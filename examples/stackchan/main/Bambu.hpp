// SPDX-License-Identifier: Apache-2.0
#pragma once

#include "esp_err.h"
#include <cstddef>

namespace stackchan::bambu {
void poll();
bool pending();
bool commentary(void *, char *text, size_t size);
esp_err_t status(const char *arguments, char *out, size_t size);
} // namespace stackchan::bambu
