// SPDX-License-Identifier: Apache-2.0
#include "Bambu.hpp"
#include "Board.hpp"
#include "cJSON.h"
#include "esp_log.h"
#include "esp_timer.h"
#include "freertos/FreeRTOS.h"
#include "freertos/queue.h"
#include "mqtt_client.h"
#include <atomic>
#include <cmath>
#include <cstdio>
#include <cstring>
#include <initializer_list>

#if __has_include("bambu_secrets.h")
#include "bambu_secrets.h"
#endif

namespace stackchan::bambu {
namespace {
#if defined(BAMBU_HOST)
constexpr char TAG[] = "BambuMQTT";
constexpr char REPORT[] = "device/" BAMBU_SERIAL "/report";
constexpr char REQUEST[] = "device/" BAMBU_SERIAL "/request";
constexpr char PUSH_ALL[] =
    R"({"pushing":{"sequence_id":"0","command":"pushall"}})";
constexpr int64_t REFRESH_US = 30'000'000;
constexpr int64_t STALE_US = 90'000'000;
struct Snapshot {
  const char *state = nullptr;
  int progress = -1;
  int minutes = -1;
  double nozzle = NAN;
  double bed = NAN;
  double chamber = NAN;
  uint32_t error = 0;
  bool error_reported = false;
  bool hms_reported = false;
  uint64_t hms[16]{};
  size_t hms_count = 0;
  int64_t updated = 0;
};
struct Notice {
  char text[256];
};
Snapshot snapshot;
portMUX_TYPE lock = portMUX_INITIALIZER_UNLOCKED;
std::atomic<QueueHandle_t> notices{nullptr};
esp_mqtt_client_handle_t client;
std::atomic<bool> subscribed{false};
int subscription_id = -1;
int64_t refresh_at;
char payload[32769];
int expected, received;

const cJSON *field(const cJSON *json, const char *name) {
  return cJSON_GetObjectItemCaseSensitive(json, name);
}

void notice(const char *message, const char *value = "") {
  Notice next{};
  snprintf(next.text, sizeof(next.text), message, value);
  const auto queue = notices.load();
  if (xQueueSend(queue, &next, 0) != pdTRUE) {
    Notice oldest{};
    xQueueReceive(queue, &oldest, 0);
    xQueueSend(queue, &next, 0);
  }
}

bool number(const cJSON *value, double min, double max) {
  return cJSON_IsNumber(value) && value->valuedouble >= min &&
         value->valuedouble <= max;
}

void report() {
  cJSON *root = cJSON_ParseWithOpts(payload, nullptr, true);
  const auto *print = field(root, "print");
  const auto *command = field(print, "command");
  if (!cJSON_IsString(command) || strcmp(command->valuestring, "push_status")) {
    cJSON_Delete(root);
    return;
  }
  portENTER_CRITICAL(&lock);
  const auto previous = snapshot;
  portEXIT_CRITICAL(&lock);
  auto next = previous.updated ? previous : Snapshot{};
  const auto *state = field(print, "gcode_state");
  if (cJSON_IsString(state)) {
    next.state = "UNKNOWN";
    for (const auto *known :
         {"IDLE", "PREPARE", "RUNNING", "PAUSE", "FINISH", "FAILED", "SLICING",
          "INIT"}) {
      if (!strcmp(state->valuestring, known))
        next.state = known;
    }
    if (previous.state && strcmp(previous.state, next.state) &&
        strcmp(next.state, "UNKNOWN"))
      notice("H3C の印刷状態が %s に変わりました。", next.state);
  }
  const auto *progress = field(print, "mc_percent");
  if (number(progress, 0, 100))
    next.progress = progress->valueint;
  const auto *minutes = field(print, "mc_remaining_time");
  if (number(minutes, 0, 100000))
    next.minutes = minutes->valueint;
  const auto *nozzle = field(print, "nozzle_temper");
  if (number(nozzle, 0, 500))
    next.nozzle = nozzle->valuedouble;
  const auto *bed = field(print, "bed_temper");
  if (number(bed, 0, 200))
    next.bed = bed->valuedouble;
  const auto *chamber = field(print, "chamber_temper");
  if (number(chamber, 0, 200))
    next.chamber = chamber->valuedouble;
  const auto *error = field(print, "print_error");
  if (number(error, 0, UINT32_MAX)) {
    next.error_reported = true;
    next.error = static_cast<uint32_t>(error->valuedouble);
    if (next.error && next.error != previous.error) {
      char code[16];
      snprintf(code, sizeof(code), "%08lX", (unsigned long)next.error);
      notice("H3C で印刷エラーが発生しました。コードは %s です。", code);
    }
  }
  const auto *hms = field(print, "hms");
  if (cJSON_IsArray(hms)) {
    next.hms_reported = true;
    next.hms_count = 0;
    const cJSON *entry;
    cJSON_ArrayForEach(entry, hms) {
      if (next.hms_count == sizeof(next.hms) / sizeof(next.hms[0]))
        break;
      const auto *attr = field(entry, "attr");
      const auto *code = field(entry, "code");
      if (!number(attr, 0, UINT32_MAX) || !number(code, 1, UINT32_MAX))
        continue;
      const auto a = static_cast<uint32_t>(attr->valuedouble);
      const auto c = static_cast<uint32_t>(code->valuedouble);
      const uint64_t key = (uint64_t(a) << 32) | c;
      bool known = false;
      for (size_t i = 0; i < next.hms_count; ++i)
        known |= key == next.hms[i];
      if (known)
        continue;
      next.hms[next.hms_count++] = key;
      for (size_t i = 0; i < previous.hms_count; ++i)
        known |= key == previous.hms[i];
      if (!known) {
        char id[24];
        snprintf(id, sizeof(id), "%04lX-%04lX-%04lX-%04lX",
                 (unsigned long)(a >> 16), (unsigned long)(a & 0xffff),
                 (unsigned long)(c >> 16), (unsigned long)(c & 0xffff));
        notice("H3C から HMS 警告が届きました。コードは %s です。", id);
      }
    }
  }
  next.updated = esp_timer_get_time();
  portENTER_CRITICAL(&lock);
  snapshot = next;
  portEXIT_CRITICAL(&lock);
  cJSON_Delete(root);
}

void event(void *, esp_event_base_t, int32_t id, void *data) {
  const auto msg = static_cast<esp_mqtt_event_handle_t>(data);
  switch (id) {
  case MQTT_EVENT_CONNECTED:
    subscription_id = esp_mqtt_client_subscribe(msg->client, REPORT, 0);
    ESP_LOGI(TAG, "Connected to printer MQTT");
    break;
  case MQTT_EVENT_SUBSCRIBED:
    if (msg->msg_id == subscription_id)
      subscribed = true;
    break;
  case MQTT_EVENT_DISCONNECTED:
    subscribed = false;
    expected = received = 0;
    portENTER_CRITICAL(&lock);
    snapshot.updated = 0;
    portEXIT_CRITICAL(&lock);
    ESP_LOGI(TAG, "Disconnected from printer MQTT");
    break;
  case MQTT_EVENT_DATA:
    if (msg->current_data_offset == 0) {
      received = expected = 0;
      if (msg->topic_len == sizeof(REPORT) - 1 &&
          !memcmp(msg->topic, REPORT, sizeof(REPORT) - 1) &&
          msg->total_data_len > 0 && msg->total_data_len < int(sizeof(payload)))
        expected = msg->total_data_len;
    }
    if (!expected || msg->current_data_offset != received ||
        msg->data_len <= 0 || msg->data_len > expected - received) {
      expected = received = 0;
      break;
    }
    memcpy(payload + received, msg->data, msg->data_len);
    received += msg->data_len;
    if (received == expected) {
      payload[received] = 0;
      expected = received = 0;
      report();
    }
    break;
  case MQTT_EVENT_ERROR:
    ESP_LOGW(TAG, "Printer MQTT connection error");
    break;
  default:
    break;
  }
}
#endif
} // namespace

void poll() {
#if defined(BAMBU_HOST)
  if (!board::wifi_ready())
    return;
  if (!client) {
    static bool attempted;
    if (attempted)
      return;
    attempted = true;
    notices = xQueueCreate(12, sizeof(Notice));
    if (!notices) {
      ESP_LOGE(TAG, "Could not allocate printer notification queue");
      return;
    }
    esp_mqtt_client_config_t config{};
    config.broker.address.uri = "mqtts://" BAMBU_HOST ":8883";
    config.broker.verification.certificate = BAMBU_CA_PEM;
    config.broker.verification.common_name = BAMBU_CERT_NAME;
    config.credentials.username = "bblp";
    config.credentials.authentication.password = BAMBU_ACCESS_CODE;
    config.task.stack_size = 6144;
    client = esp_mqtt_client_init(&config);
    if (!client) {
      ESP_LOGE(TAG, "Could not initialize printer MQTT");
      return;
    }
    if (esp_mqtt_client_register_event(client, MQTT_EVENT_ANY, event, nullptr) !=
            ESP_OK ||
        esp_mqtt_client_start(client) != ESP_OK) {
      ESP_LOGE(TAG, "Could not start printer MQTT");
      esp_mqtt_client_destroy(client);
      client = nullptr;
    }
  }
  if (!subscribed) {
    refresh_at = 0;
    return;
  }
  const auto now = esp_timer_get_time();
  if (now >= refresh_at) {
    esp_mqtt_client_enqueue(client, REQUEST, PUSH_ALL, 0, 0, 0, true);
    refresh_at = now + REFRESH_US;
  }
#endif
}

bool pending() {
#if defined(BAMBU_HOST)
  const auto queue = notices.load();
  return queue && uxQueueMessagesWaiting(queue);
#else
  return false;
#endif
}

bool commentary(void *, char *text, size_t size) {
#if defined(BAMBU_HOST)
  Notice next{};
  const auto queue = notices.load();
  if (queue && xQueueReceive(queue, &next, 0) == pdTRUE) {
    snprintf(text, size, "%s", next.text);
    return true;
  }
#else
  (void)text;
  (void)size;
#endif
  return false;
}

esp_err_t status(const char *arguments, char *out, size_t size) {
  cJSON *args = cJSON_ParseWithOpts(arguments, nullptr, true);
  const bool valid = cJSON_IsObject(args) && !args->child;
  cJSON_Delete(args);
  if (!valid) {
    snprintf(out, size, "get_printer_status requires {}.");
    return ESP_ERR_INVALID_ARG;
  }
#if defined(BAMBU_HOST)
  portENTER_CRITICAL(&lock);
  const auto data = snapshot;
  portEXIT_CRITICAL(&lock);
  if (!data.updated || esp_timer_get_time() - data.updated > STALE_US) {
    snprintf(out, size, "H3C: no current MQTT status available.");
    return ESP_OK;
  }
  char hms[128]{};
  size_t used = 0;
  for (size_t i = 0; i < data.hms_count && i < 5; ++i) {
    const auto key = data.hms[i];
    used += snprintf(hms + used, sizeof(hms) - used, "%s%016llX",
                     i ? ", " : "", (unsigned long long)key);
  }
  char error[16] = "unknown";
  if (data.error_reported)
    snprintf(error, sizeof(error), "%08lX", (unsigned long)data.error);
  snprintf(out, size,
           "H3C: state=%s, progress=%d%%, remaining=%d min, nozzle=%.1f C, "
           "bed=%.1f C, chamber=%.1f C, print_error=%s, HMS count=%d [%s]. "
           "-1, nan and UNKNOWN mean unavailable; zero error code means no "
           "reported print error.",
           data.state ? data.state : "UNKNOWN", data.progress, data.minutes,
           data.nozzle, data.bed, data.chamber, error,
           data.hms_reported ? int(data.hms_count) : -1, hms);
#else
  snprintf(out, size, "H3C MQTT is not configured.");
#endif
  return ESP_OK;
}
} // namespace stackchan::bambu
