################################################################################
##     ___                  _   ____  ____
##    / _ \ _   _  ___  ___| |_|  _ \| __ )
##   | | | | | | |/ _ \/ __| __| | | |  _ \
##   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
##    \__\_\\__,_|\___||___/\__|____/|____/
##
##  Copyright (c) 2014-2019 Appsicle
##  Copyright (c) 2019-2022 QuestDB
##
##  Licensed under the Apache License, Version 2.0 (the "License");
##  you may not use this file except in compliance with the License.
##  You may obtain a copy of the License at
##
##  http://www.apache.org/licenses/LICENSE-2.0
##
##  Unless required by applicable law or agreed to in writing, software
##  distributed under the License is distributed on an "AS IS" BASIS,
##  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
##  See the License for the specific language governing permissions and
##  limitations under the License.
##
################################################################################

from libc.stdint cimport int64_t, uint16_t, uint64_t

cdef extern from "questdb/ilp/line_sender.h":

  cdef enum line_sender_error_code:
    line_sender_error_could_not_resolve_addr,
    line_sender_error_invalid_api_call,
    line_sender_error_socket_error,
    line_sender_error_invalid_utf8,
    line_sender_error_invalid_name,
    line_sender_error_invalid_timestamp,
    line_sender_error_auth_error,
    line_sender_error_tls_error,

  cdef struct line_sender:
    pass

  cdef struct line_sender_buffer:
    pass

  cdef struct line_sender_error:
    pass

  cdef struct line_sender_opts:
    pass

  cdef struct line_sender_utf8:
    size_t len
    const char *buf

  cdef struct line_sender_table_name:
    size_t len
    const char *buf

  cdef struct line_sender_column_name:
    size_t len
    const char *buf

  line_sender_error_code line_sender_error_get_code(const line_sender_error *error)

  const char *line_sender_error_msg(const line_sender_error *error, size_t *len_out)

  void line_sender_error_free(line_sender_error *error)

  bint line_sender_utf8_init(line_sender_utf8 *string,
                             size_t len,
                             const char *buf,
                             line_sender_error **err_out)

  line_sender_utf8 line_sender_utf8_assert(size_t len, const char *buf)

  bint line_sender_table_name_init(line_sender_table_name *name,
                                   size_t len,
                                   const char *buf,
                                   line_sender_error **err_out)

  line_sender_table_name line_sender_table_name_assert(size_t len, const char *buf)

  bint line_sender_column_name_init(line_sender_column_name *name,
                                    size_t len,
                                    const char *buf,
                                    line_sender_error **err_out)

  line_sender_table_name line_sender_column_name_assert(size_t len, const char *buf)

  line_sender_opts *line_sender_opts_new(line_sender_utf8 host, uint16_t port)

  line_sender_opts *line_sender_opts_new_service(line_sender_utf8 host, line_sender_utf8 port)

  void line_sender_opts_capacity(line_sender_opts *opts, size_t capacity)

  void line_sender_opts_net_interface(line_sender_opts *opts, line_sender_utf8 net_interface)

  void line_sender_opts_auth(line_sender_opts *opts,
                             line_sender_utf8 key_id,
                             line_sender_utf8 priv_key,
                             line_sender_utf8 pub_key_x,
                             line_sender_utf8 pub_key_y)

  void line_sender_opts_tls(line_sender_opts *opts)

  void line_sender_opts_tls_ca(line_sender_opts *opts, line_sender_utf8 ca_path)

  void line_sender_opts_tls_insecure_skip_verify(line_sender_opts *opts)

  void line_sender_opts_read_timeout(line_sender_opts *opts, uint64_t timeout_millis)

  line_sender_opts *line_sender_opts_clone(const line_sender_opts *opts)

  void line_sender_opts_free(line_sender_opts *opts)

  line_sender_buffer *line_sender_buffer_new()

  line_sender_buffer *line_sender_buffer_with_max_name_len(size_t max_name_len)

  void line_sender_buffer_free(line_sender_buffer *buffer)

  line_sender_buffer *line_sender_buffer_clone(const line_sender_buffer *buffer)

  void line_sender_buffer_reserve(line_sender_buffer *buffer, size_t additional)

  size_t line_sender_buffer_capacity(const line_sender_buffer *buffer)

  bint line_sender_buffer_set_marker(line_sender_buffer* buffer,
                                     line_sender_error** err_out)

  bint line_sender_buffer_rewind_to_marker(line_sender_buffer* buffer,
                                           line_sender_error** err_out)

  void line_sender_buffer_clear_marker(line_sender_buffer* buffer)

  void line_sender_buffer_clear(line_sender_buffer *buffer)

  size_t line_sender_buffer_size(const line_sender_buffer *buffer)

  const char *line_sender_buffer_peek(const line_sender_buffer *buffer, size_t *len_out)

  bint line_sender_buffer_table(line_sender_buffer *buffer,
                                line_sender_table_name name,
                                line_sender_error **err_out)

  bint line_sender_buffer_symbol(line_sender_buffer *buffer,
                                 line_sender_column_name name,
                                 line_sender_utf8 value,
                                 line_sender_error **err_out)

  bint line_sender_buffer_column_bool(line_sender_buffer *buffer,
                                      line_sender_column_name name,
                                      bint value,
                                      line_sender_error **err_out)

  bint line_sender_buffer_column_i64(line_sender_buffer *buffer,
                                     line_sender_column_name name,
                                     int64_t value,
                                     line_sender_error **err_out)

  bint line_sender_buffer_column_f64(line_sender_buffer *buffer,
                                     line_sender_column_name name,
                                     double value,
                                     line_sender_error **err_out)

  bint line_sender_buffer_column_str(line_sender_buffer *buffer,
                                     line_sender_column_name name,
                                     line_sender_utf8 value,
                                     line_sender_error **err_out)

  bint line_sender_buffer_column_ts(line_sender_buffer *buffer,
                                    line_sender_column_name name,
                                    int64_t micros,
                                    line_sender_error **err_out)

  bint line_sender_buffer_at(line_sender_buffer *buffer,
                             int64_t epoch_nanos,
                             line_sender_error **err_out)

  bint line_sender_buffer_at_now(line_sender_buffer *buffer, line_sender_error **err_out)

  line_sender *line_sender_connect(const line_sender_opts *opts, line_sender_error **err_out)

  bint line_sender_must_close(const line_sender *sender)

  void line_sender_close(line_sender *sender)

  bint line_sender_flush(line_sender *sender,
                         line_sender_buffer *buffer,
                         line_sender_error **err_out)

  bint line_sender_flush_and_keep(line_sender *sender,
                                  const line_sender_buffer *buffer,
                                  line_sender_error **err_out)
