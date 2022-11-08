/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

#pragma once

// This header is auto-generated. Do not edit directly!

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>

typedef struct qdb_pystr_buf qdb_pystr_buf;

typedef struct
{
    size_t chain;
    size_t string;
} qdb_pystr_pos;

#ifdef __cplusplus
extern "C" {
#endif // __cplusplus

/**
 * Prepare a new buffer. The buffer must be freed with `qdb_pystr_free`.
 * The `qdb_ucsX_to_utf8` functions will write to this buffer.
 */
qdb_pystr_buf *qdb_pystr_buf_new(void);

/**
 * Get current position. Use in conjunction with `truncate`.
 */
qdb_pystr_pos qdb_pystr_buf_tell(qdb_pystr_buf *b);

/**
 * Trim the buffer to the given length. Use in conjunction with `tell`.
 */
void qdb_pystr_buf_truncate(qdb_pystr_buf *b,
                            qdb_pystr_pos pos);

/**
 * Reset the converter's buffer to zero length.
 */
void qdb_pystr_buf_clear(qdb_pystr_buf *b);

/**
 * Free the buffer. Must be called after `qdb_pystr_buf_new`.
 */
void qdb_pystr_buf_free(qdb_pystr_buf *b);

/**
 * Convert a Py_UCS1 string to UTF-8.
 * Returns a `buf_out` borrowed ptr of `size_out` len.
 * The buffer is borrowed from `b`.
 */
void qdb_ucs1_to_utf8(qdb_pystr_buf *b,
                      size_t count,
                      const uint8_t *input,
                      size_t *size_out,
                      const char **buf_out);

/**
 * Convert a Py_UCS2 string to UTF-8.
 * Returns a `buf_out` borrowed ptr of `size_out` len.
 * The buffer is borrowed from `b`.
 * In case of errors, returns `false` and the buffer is an error message.
 */
bool qdb_ucs2_to_utf8(qdb_pystr_buf *b,
                      size_t count,
                      const uint16_t *input,
                      size_t *size_out,
                      const char **buf_out);

/**
 * Convert a Py_UCS4 string to UTF-8.
 * Returns a `buf_out` borrowed ptr of `size_out` len.
 * The buffer is borrowed from `b`.
 * In case of errors, returns `false` and the buffer is an error message.
 */
bool qdb_ucs4_to_utf8(qdb_pystr_buf *b,
                      size_t count,
                      const uint32_t *input,
                      size_t *size_out,
                      const char **buf_out);

#ifdef __cplusplus
} // extern "C"
#endif // __cplusplus
