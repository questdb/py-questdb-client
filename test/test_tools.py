
import struct
import datetime
import numpy as np
import questdb.ingress as qi

ARRAY_TYPE_TAGS = {
    np.float64: 10,
}

import math
import struct

def _float_binary_bytes(value: float, text_format: bool = False) -> bytes:
    if text_format:
        if math.isnan(value):
            return b'=NaN'
        elif math.isinf(value):
            return f'={"-Infinity" if value < 0 else "Infinity"}'.encode('utf-8')
        else:
            return f'={value}'.encode('utf-8').replace(b'+', b'')
    else:
        return b'==' + struct.pack('<B', 16) + struct.pack('<d', value)


def _array_binary_bytes(value: np.ndarray) -> bytes:
    header = b'='
    format_type = struct.pack('<B', 14)
    try:
        type_tag = struct.pack('<B', ARRAY_TYPE_TAGS[value.dtype.type])
    except KeyError:
        raise ValueError(f"Unsupported dtype: {value.dtype}")

    ndim = struct.pack('<B', value.ndim)
    shape_bytes = b''.join(struct.pack('<i', dim) for dim in value.shape)

    dtype_le = value.dtype.newbyteorder('<')
    arr_view = value.astype(dtype_le, copy=False)

    if value.ndim != 0 and value.nbytes != 0:
        data_body = b''.join(
            elem.tobytes()
            for elem in np.nditer(arr_view, order='C')
        )
    else:
        data_body = b''

    return (
            header +
            format_type +
            type_tag +
            ndim +
            shape_bytes +
            data_body
    )


class TimestampEncodingMixin:
    def enc_ts_t(self, num):
        return f'{num}t'
    
    def enc_ts_n(self, num, v=None):
        protocol_version = v or self.version
        if protocol_version == 1:
            num = num // 1000
            suffix = 't'
        else:
            suffix = 'n'
        return f'{num}{suffix}'

    def enc_ts(self, ts, v=None):
        """encode a non-designated timestamp in ILP"""
        if isinstance(ts, datetime.datetime):
            return self.enc_ts_t(
                qi.TimestampMicros.from_datetime(ts).value)
        elif isinstance(ts, qi.TimestampMicros):
            return self.enc_ts_t(ts.value)
        elif isinstance(ts, qi.TimestampNanos):
            return self.enc_ts_n(ts.value, v=v)
        else:
            raise ValueError(f'unsupported ts {ts!r}')

    def enc_des_ts_t(self, num, v=None):
        protocol_version = v or self.version
        if protocol_version == 1:
            num = num * 1000
            suffix = ''
        else:
            suffix = 't'
        return f'{num}{suffix}'
    
    def enc_des_ts_n(self, num, v=None):
        protocol_version = v or self.version
        if protocol_version == 1:
            suffix = ''
        else:
            suffix = 'n'
        return f'{num}{suffix}'

    def enc_des_ts(self, ts, v=None):
        """encode a designated timestamp in ILP"""
        if isinstance(ts, datetime.datetime):
            return self.enc_des_ts_t(
                qi.TimestampMicros.from_datetime(ts).value, v=v)
        elif isinstance(ts, qi.TimestampMicros):
            return self.enc_des_ts_t(ts.value, v=v)
        elif isinstance(ts, qi.TimestampNanos):
            return self.enc_des_ts_n(ts.value, v=v)
        else:
            raise ValueError(f'unsupported ts {ts!r}')
