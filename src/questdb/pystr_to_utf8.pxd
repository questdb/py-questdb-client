from libc.stdint cimport int8_t, int16_t, int32_t, int64_t, intptr_t
from libc.stdint cimport uint8_t, uint16_t, uint32_t, uint64_t, uintptr_t

cdef extern from "pystr_to_utf8.h":

  cdef struct qdb_pystr_buf:
    pass

  cdef struct qdb_pystr_pos:
    size_t chain
    size_t string

  # Prepare a new buffer. The buffer must be freed with `qdb_pystr_free`.
  # The `qdb_ucsX_to_utf8` functions will write to this buffer.
  qdb_pystr_buf *qdb_pystr_buf_new()

  # Get current position. Use in conjunction with `truncate`.
  qdb_pystr_pos qdb_pystr_buf_tell(const qdb_pystr_buf *b)

  # Trim the buffer to the given position. Use in conjunction with `tell`.
  void qdb_pystr_buf_truncate(qdb_pystr_buf *b, qdb_pystr_pos pos)

  # Reset the converter's buffer to zero length.
  void qdb_pystr_buf_clear(qdb_pystr_buf *b)

  # Free the buffer. Must be called after `qdb_pystr_buf_new`.
  void qdb_pystr_buf_free(qdb_pystr_buf *b)

  # Convert a Py_UCS1 string to UTF-8.
  # Returns a `buf_out` borrowed ptr of `size_out` len.
  # The buffer is borrowed from `b`.
  void qdb_ucs1_to_utf8(qdb_pystr_buf *b,
                        size_t count,
                        const uint8_t *input,
                        size_t *size_out,
                        const char **buf_out)

  # Convert a Py_UCS2 string to UTF-8.
  # Returns a `buf_out` borrowed ptr of `size_out` len.
  # The buffer is borrowed from `b`.
  # In case of errors, returns `false` and bad_codepoint_out is set to the
  # offending codepoint.
  bint qdb_ucs2_to_utf8(qdb_pystr_buf *b,
                        size_t count,
                        const uint16_t *input,
                        size_t *size_out,
                        const char **buf_out,
                        uint32_t *bad_codepoint_out)

  # Convert a Py_UCS4 string to UTF-8.
  # Returns a `buf_out` borrowed ptr of `size_out` len.
  # The buffer is borrowed from `b`.
  # In case of errors, returns `false` and bad_codepoint_out is set to the
  # offending codepoint.
  bint qdb_ucs4_to_utf8(qdb_pystr_buf *b,
                        size_t count,
                        const uint32_t *input,
                        size_t *size_out,
                        const char **buf_out,
                        uint32_t *bad_codepoint_out)
