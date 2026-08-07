################################################################################
##     ___                  _   ____  ____
##    / _ \ _   _  ___  ___| |_|  _ \| __ )
##   | | | | | | |/ _ \/ __| __| | | |  _ \
##   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
##    \__\_\\__,_|\___||___/\__|____/|____/
##
##  Copyright (c) 2014-2019 Appsicle
##  Copyright (c) 2019-2024 QuestDB
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

__all__ = [
    "ConnectionEvent",
    "ConnectionEventKind",
    "PooledReader",
    "PooledSender",
    "Protocol",
    "QueryResult",
    "QuestDB",
    "QuestDBError",
    "QuestDBErrorCode",
    "QuestDBServerRejectionError",
    "QwpWsProgress",
    "Sender",
    "SenderError",
    "SenderErrorCategory",
    "SenderErrorPolicy",
    "SenderTransaction",
    "ServerInfo",
    "ServerRole",
    "ServerTimestamp",
    "ServerTimestampType",
    "TimestampMicros",
    "TimestampNanos",
    "TlsCa",
    "UnsupportedDataFrameShapeError",
    "WARN_HIGH_RECONNECTS",
]

from datetime import datetime, timedelta
from enum import Enum
from dataclasses import dataclass
from typing import Any, Callable, Dict, Iterator, List, Optional, Union

import numpy as np
import pandas as pd
from decimal import Decimal

class QuestDBErrorCode(Enum):
    """Category of Error."""

    CouldNotResolveAddr = ...
    InvalidApiCall = ...
    SocketError = ...
    InvalidUtf8 = ...
    InvalidName = ...
    InvalidTimestamp = ...
    AuthError = ...
    TlsError = ...
    HttpNotSupported = ...
    ServerFlushError = ...
    ServerRejection = ...
    RoleMismatch = ...
    ConfigError = ...
    ArrayError = ...
    ProtocolVersionError = ...
    DecimalError = ...
    ArrowUnsupportedColumnKind = ...
    ArrowIngest = ...
    FailoverRetry = ...
    ConnectTimeout = ...
    HandshakeError = ...
    UnsupportedServer = ...
    ProtocolError = ...
    InvalidBind = ...
    ServerSchemaMismatch = ...
    ServerParseError = ...
    ServerInternalError = ...
    ServerSecurityError = ...
    LimitExceeded = ...
    ServerLimitExceeded = ...
    Cancelled = ...
    FailoverWouldDuplicate = ...
    SchemaDrift = ...
    NoSchema = ...
    ArrowExport = ...
    BatchTooLarge = ...
    StoreResendRequired = ...
    SymbolDictFull = ...
    BadDataFrame = ...


class QuestDBError(Exception):
    """An error whilst using the QuestDB client."""

    @property
    def code(self) -> QuestDBErrorCode:
        """Return the error code."""

    @property
    def in_doubt(self) -> bool:
        """
        Whether the failed operation may already have delivered its input.

        Retrying the same input when this is true can duplicate rows unless the
        destination table has an appropriate deduplication guarantee.
        """

    @property
    def sender_error(self) -> Optional["SenderError"]:
        """
        Return the structured QWP/WebSocket diagnostic, if this error carries
        one from a QWP/WebSocket sender failure.
        """


class QuestDBServerRejectionError(QuestDBError):
    """
    A terminal QWP/WebSocket server rejection.

    The structured server payload is available through
    :attr:`QuestDBError.sender_error`.
    """


class UnsupportedDataFrameShapeError(QuestDBError):
    """
    A DataFrame shape is not supported by the optimized columnar client path.
    """

    column_failures: tuple


class ConnectionEventKind(Enum):
    """Connection-state transitions observed by the ingress pool."""

    Connected = ...
    Disconnected = ...
    Reconnected = ...
    FailedOver = ...
    EndpointAttemptFailed = ...
    AllEndpointsUnreachable = ...
    AuthFailed = ...

    @property
    def tag(self) -> str: ...
    @property
    def c_value(self) -> int: ...
    @classmethod
    def parse(
        cls, tag: Union[str, "ConnectionEventKind", None]
    ) -> Optional["ConnectionEventKind"]: ...


@dataclass(frozen=True)
class ConnectionEvent:
    """One connection-state transition delivered to a connection listener.

    Successful events are queued only after negotiated connection state,
    including the server-advertised frame cap, is committed. They are not
    data-delivery or acknowledgement barriers.
    """

    kind: ConnectionEventKind
    host: Optional[str]
    port: Optional[str]
    previous_host: Optional[str]
    previous_port: Optional[str]
    attempt_number: Optional[int]
    cause_code: Optional[QuestDBErrorCode]
    cause_msg: Optional[str]
    timestamp_millis: int


class ServerRole(Enum):
    """Cluster role advertised by the server's ``SERVER_INFO`` handshake."""

    Standalone = ...
    Primary = ...
    Replica = ...
    PrimaryCatchup = ...
    Other = ...

    @property
    def tag(self) -> str: ...
    @property
    def c_value(self) -> int: ...
    @classmethod
    def parse(cls, tag: Union[str, "ServerRole", None]) -> Optional["ServerRole"]: ...


@dataclass(frozen=True)
class ServerInfo:
    """Snapshot of the server's ``SERVER_INFO`` handshake."""

    role: ServerRole
    role_byte: int
    epoch: int
    capabilities: int
    server_wall_ns: int
    cluster_id: str
    node_id: str
    zone_id: Optional[str]


class ServerTimestampType:
    """
    A placeholder value to indicate using a server-generated-timestamp.
    """


ServerTimestamp: ServerTimestampType

VERSION: str

WARN_HIGH_RECONNECTS: bool


class TimestampMicros:
    """
    A timestamp in microseconds since the UNIX epoch (UTC).

    You may construct a ``TimestampMicros`` from an integer or a
    ``datetime.datetime``, or simply call the :func:`TimestampMicros.now`
    method.

    .. code-block:: python

        # Recommended way to get the current timestamp.
        TimestampMicros.now()

        # The above is equivalent to:
        TimestampMicros(time.time_ns() // 1000)

        # You can provide a numeric timestamp too. It can't be negative.
        TimestampMicros(1657888365426838)

    ``TimestampMicros`` can also be constructed from a ``datetime.datetime``
    object.

    .. code-block:: python

        TimestampMicros.from_datetime(
            datetime.datetime.now(tz=datetime.timezone.utc))

    We recommend that when using ``datetime`` objects, you explicitly pass in
    the timezone to use. A ``datetime`` object without an associated timezone
    is interpreted as UTC (a ``UserWarning`` is emitted once per process).
    Note that ``datetime.datetime.now()`` is your local wall clock: use
    ``datetime.datetime.now(datetime.timezone.utc)`` or ``now()`` on this
    class for the current instant.
    """

    def __init__(self, value: int): ...
    @classmethod
    def from_datetime(cls, dt: datetime) -> TimestampMicros:
        """
        Construct a ``TimestampMicros`` from a ``datetime.datetime`` object.
        """

    @classmethod
    def now(cls) -> TimestampMicros:
        """
        Construct a ``TimestampMicros`` from the current time as UTC.
        """

    @property
    def value(self) -> int:
        """Number of microseconds (Unix epoch timestamp, UTC)."""


class TimestampNanos:
    """
    A timestamp in nanoseconds since the UNIX epoch (UTC).

    You may construct a ``TimestampNanos`` from an integer or a
    ``datetime.datetime``, or simply call the :func:`TimestampNanos.now`
    method.

    .. code-block:: python

        # Recommended way to get the current timestamp.
        TimestampNanos.now()

        # The above is equivalent to:
        TimestampNanos(time.time_ns())

        # You can provide a numeric timestamp too. It can't be negative.
        TimestampNanos(1657888365426838016)

    ``TimestampNanos`` can also be constructed from a ``datetime`` object.

    .. code-block:: python

        TimestampNanos.from_datetime(
            datetime.datetime.now(tz=datetime.timezone.utc))

    We recommend that when using ``datetime`` objects, you explicitly pass in
    the timezone to use. A ``datetime`` object without an associated timezone
    is interpreted as UTC (a ``UserWarning`` is emitted once per process).
    Note that ``datetime.datetime.now()`` is your local wall clock: use
    ``datetime.datetime.now(datetime.timezone.utc)`` or ``now()`` on this
    class for the current instant.
    """

    def __init__(self, value: int): ...
    @classmethod
    def from_datetime(cls, dt: datetime) -> TimestampNanos:
        """
        Construct a ``TimestampNanos`` from a ``datetime.datetime`` object.
        """

    @classmethod
    def now(cls) -> TimestampNanos:
        """
        Construct a ``TimestampNanos`` from the current time as UTC.
        """

    @property
    def value(self) -> int:
        """Number of nanoseconds (Unix epoch timestamp, UTC)."""

class SenderTransaction:
    """
    A transaction for a specific table.

    Transactions are not supported with ILP/TCP, only ILP/HTTP.

    The sender API can only operate on one transaction at a time.

    To create a transaction:

    .. code-block:: python

        with sender.transaction('table_name') as txn:
            txn.row(..)
            txn.dataframe(..)
    """

    def __init__(self, sender: Sender, table_name: str): ...
    def __enter__(self) -> SenderTransaction: ...
    def __exit__(self, exc_type, _exc_value, _traceback) -> bool: ...
    def row(
        self,
        *,
        symbols: Optional[Dict[str, Optional[str]]] = None,
        columns: Optional[
            Dict[str, Union[None, bool, int, float, str, TimestampMicros, TimestampNanos, datetime, np.ndarray, Decimal]]
        ] = None,
        at: Union[ServerTimestampType, TimestampNanos, datetime],
    ) -> SenderTransaction:
        """
        Write a row for the table in the transaction.

        The table name is taken from the transaction.

        **Note**: Support for NumPy arrays (``np.array``) requires QuestDB server version 9.0.0 or higher.
        """

    def dataframe(
        self,
        df: pd.DataFrame,
        *,
        symbols: Union[str, bool, List[int], List[str]] = "auto",
        at: Union[ServerTimestampType, int, str, TimestampNanos, datetime],
    ) -> SenderTransaction:
        """
        Write a dataframe for the table in the transaction.

        The table name is taken from the transaction.
        """

    def commit(self):
        """
        Commit the transaction.

        A commit is also automatic at the end of a successful `with` block.

        This will flush the buffer.
        """

    def rollback(self):
        """
        Roll back the transaction.

        A rollback is also automatic at the end of a failed `with` block.

        This will clear the buffer.
        """


class Buffer:
    """
    Internal row-serialization buffer, managed by :class:`Sender`.

    Kept importable as ``questdb.ingress.Buffer`` for legacy ILP/HTTP and
    ILP/TCP code that constructs buffers explicitly and flushes them via
    ``sender.flush(buffer)``.
    """

    def __init__(
            self,
            protocol_version: int,
            init_buf_size: int = 65536,
            max_name_len: int = 127):
        ...

    @property
    def init_buf_size(self) -> int:
        """
        The initial capacity of the buffer when first created.

        This may grow over time, see ``capacity()``.
        """

    @property
    def max_name_len(self) -> int:
        """Maximum length of a table or column name."""

    def reserve(self, additional: int):
        """
        Ensure the buffer has at least `additional` bytes of future capacity.

        :param int additional: Additional bytes to reserve.
        """

    def capacity(self) -> int:
        """The current buffer capacity."""

    def clear(self):
        """
        Reset the buffer.

        Note that flushing a buffer will (unless otherwise specified)
        also automatically clear it.

        This method is designed to be called only in conjunction with
        ``sender.flush(buffer, clear=False)``.
        """

    def __len__(self) -> int:
        """
        The current number of bytes currently in the buffer.

        Equivalent (but cheaper) to ``len(bytes(buffer))``.
        """

    def  __bytes__(self) -> bytes:
        """Return the constructed buffer as bytes. Use for debugging."""

    def row(
        self,
        table_name: str,
        *,
        symbols: Optional[Dict[str, Optional[str]]] = None,
        columns: Optional[
            Dict[str, Union[None, bool, int, float, str, TimestampMicros, TimestampNanos, datetime, np.ndarray, Decimal]]
        ] = None,
        at: Union[ServerTimestampType, TimestampNanos, datetime],
    ) -> Buffer:
        """
        Add a single row (line) to the buffer.

        .. code-block:: python

            # All fields specified.
            buffer.row(
                'table_name',
                symbols={'sym1': 'abc', 'sym2': 'def', 'sym3': None},
                columns={
                    'col1': True,
                    'col2': 123,
                    'col3': 3.14,
                    'col4': 'xyz',
                    'col5': TimestampMicros(123456789),
                    'col6': datetime(2019, 1, 1, 12, 0, 0),
                    'col7': np.array([[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]]),
                    'col8': None,
                    'col9': Decimal('123.456')},
                at=TimestampNanos(123456789))

            # Only symbols specified. Designated timestamp assigned by the db.
            buffer.row(
                'table_name',
                symbols={'sym1': 'abc', 'sym2': 'def'}, at=Server.Timestamp)

            # Float columns and timestamp specified as `datetime.datetime`.
            # Pay special attention to the timezone, which if unspecified is
            # interpreted as UTC.
            buffer.row(
                'sensor data',
                columns={
                    'temperature': 24.5,
                    'humidity': 0.5},
                at=datetime.datetime.now(tz=datetime.timezone.utc))


        Python strings passed as values to ``symbols`` are going to be encoded
        as the ``SYMBOL`` type in QuestDB, whilst Python strings passed as
        values to ``columns`` are going to be encoded as the ``STRING`` type.

        Refer to the
        `QuestDB documentation <https://questdb.com/docs/concept/symbol/>`_ to
        understand the difference between the ``SYMBOL`` and ``STRING`` types
        (TL;DR: symbols are interned strings).

        Column values can be specified with Python types directly and map as so:

        .. list-table::
            :header-rows: 1

            * - Python type
              - Serialized as ILP type
            * - ``bool``
              - `BOOLEAN <https://questdb.com/docs/reference/api/ilp/columnset-types#boolean>`_
            * - ``int``
              - `INTEGER <https://questdb.com/docs/reference/api/ilp/columnset-types#integer>`_
            * - ``float``
              - `FLOAT <https://questdb.com/docs/reference/api/ilp/columnset-types#float>`_
            * - ``str``
              - `STRING <https://questdb.com/docs/reference/api/ilp/columnset-types#string>`_
            * - ``np.ndarray``
              - `ARRAY <https://questdb.com/docs/reference/api/ilp/columnset-types#array>`_
            * - ``datetime.datetime`` and ``TimestampMicros``
              - `TIMESTAMP <https://questdb.com/docs/reference/api/ilp/columnset-types#timestamp>`_
            * - ``Decimal``
              - `DECIMAL <https://questdb.com/docs/reference/api/ilp/columnset-types#decimal>`_
            * - ``None``
              - *Column is skipped and not serialized.*

        **Note**: Support for NumPy arrays (``np.array``) requires QuestDB server version 9.0.0 or higher.

        If the destination table was already created, then the columns types
        will be cast to the types of the existing columns whenever possible
        (Refer to the QuestDB documentation pages linked above).

        Adding a row can trigger auto-flushing behaviour.

        :param table_name: The name of the table to which the row belongs.
        :param symbols: A dictionary of symbol column names to ``str`` values.
            As a convenience, you can also pass a ``None`` value which will
            have the same effect as skipping the key: If the column already
            existed, it will be recorded as ``NULL``, otherwise it will not be
            created.
        :param columns: A dictionary of column names to ``bool``, ``int``,
            ``float``, ``str``, ``TimestampMicros`` or ``datetime`` values.
            As a convenience, you can also pass a ``None`` value which will
            have the same effect as skipping the key: If the column already
            existed, it will be recorded as ``NULL``, otherwise it will not be
            created.
        :param at: The timestamp of the row. This is required!
            If ``ServerTimestamp``, timestamp is assigned by QuestDB.
            If ``datetime``, the timestamp is converted to nanoseconds.
            A nanosecond unix epoch timestamp can be passed
            explicitly as a ``TimestampNanos`` object.
        """

    def dataframe(
        self,
        df: pd.DataFrame,
        *,
        table_name: Optional[str] = None,
        table_name_col: Union[None, int, str] = None,
        symbols: Union[str, bool, List[int], List[str]] = "auto",
        at: Union[ServerTimestampType, int, str, TimestampNanos, datetime],
    ) -> Buffer:
        """
        Add a pandas DataFrame to the buffer.

        Also see the :func:`Sender.dataframe` method if you're
        not using the buffer explicitly. It supports the same parameters
        and also supports auto-flushing.

        This feature requires the ``pandas``, ``numpy`` and ``pyarrow``
        package to be installed.

        Adding a dataframe can trigger auto-flushing behaviour,
        even between rows of the same dataframe. To avoid this, you can
        use HTTP and transactions (see :func:`Sender.transaction`).

        :param df: The pandas DataFrame to serialize to the buffer.
        :type df: pandas.DataFrame

        :param table_name: The name of the table to which the rows belong.

            If ``None``, the table name is taken from the ``table_name_col``
            parameter. If both ``table_name`` and ``table_name_col`` are
            ``None``, the table name is taken from the DataFrame's index
            name (``df.index.name`` attribute).
        :type table_name: str or None

        :param table_name_col: The name or index of the column in the DataFrame
            that contains the table name.

            If ``None``, the table name is taken
            from the ``table_name`` parameter. If both ``table_name`` and
            ``table_name_col`` are ``None``, the table name is taken from the
            DataFrame's index name (``df.index.name`` attribute).

            If ``table_name_col`` is an integer, it is interpreted as the index
            of the column starting from ``0``. The index of the column can be
            negative, in which case it is interpreted as an offset from the end
            of the DataFrame. E.g. ``-1`` is the last column.
        :type table_name_col: str or int or None

        :param symbols: The columns to be serialized as symbols.

            If ``'auto'`` (default), all columns of dtype ``'categorical'`` are
            serialized as symbols. If ``True``, all ``str`` columns are
            serialized as symbols. If ``False``, no columns are serialized as
            symbols.

            The list of symbols can also be specified explicitly as a ``list``
            of column names (``str``) or indices (``int``). Integer indices
            start at ``0`` and can be negative, offset from the end of the
            DataFrame. E.g. ``-1`` is the last column.

            Only columns containing strings can be serialized as symbols.

        :type symbols: str or bool or list[str] or list[int]

        :param at: The designated timestamp of the rows.

            You can specify a single value for all rows or column name or index.
            If ``ServerTimestamp``, timestamp is assigned by the server for all rows.
            To pass in a timestamp explicitly as an integer use the
            ``TimestampNanos`` wrapper type. To get the current timestamp,
            use ``TimestampNanos.now()``.
            When passing a ``datetime.datetime`` object, the timestamp is
            converted to nanoseconds.
            A naive ``datetime`` object is interpreted as UTC — never
            your machine's local timezone — and a ``UserWarning`` is
            emitted once per process
            (call ``datetime.datetime.now(tz=datetime.timezone.utc)``
            for the current timestamp to
            avoid bugs).

            To specify a different timestamp for each row, pass in a column name
            (``str``) or index (``int``, 0-based index, negative index
            supported): In this case, the column needs to be of dtype
            ``datetime64[ns]`` (assumed to be in the **UTC timezone** and not
            local, due to differences in Pandas and Python datetime handling) or
            ``datetime64[ns, tz]``. When a timezone is specified in the column,
            it is converted to UTC automatically.

            A timestamp column can also contain ``None`` values. The server will
            assign the current timestamp to those rows.

            **Note**: All timestamps are always converted to nanoseconds and in
            the UTC timezone. Timezone information is dropped before sending and
            QuestDB will not store any timezone information.
        :type at: TimestampNanos, datetime.datetime, int or str or None

        **Note**: It is an error to specify both ``table_name`` and
        ``table_name_col``.

        **Note**: The "index" column of the DataFrame is never serialized,
        even if it is named.

        Example:

        .. code-block:: python

            import pandas as pd
            import questdb as qi

            buf = qi.ingress.Buffer(protocol_version=2)
            # ...

            df = pd.DataFrame({
                'location': ['London', 'Managua', 'London'],
                'temperature': [24.5, 35.0, 25.5],
                'humidity': [0.5, 0.6, 0.45],
                'ts': pd.date_range('2021-07-01', periods=3)})
            buf.dataframe(
                df, table_name='weather', at='ts', symbols=['location'])

            # ...
            sender.flush(buf)

        **Pandas to ILP datatype mappings**

        .. seealso:: https://questdb.com/docs/reference/api/ilp/columnset-types/

        .. list-table:: Pandas Mappings
            :header-rows: 1

            * - Pandas ``dtype``
              - Nulls
              - ILP Datatype
            * - ``'bool'``
              - N
              - ``BOOLEAN``
            * - ``'boolean'``
              - N **α**
              - ``BOOLEAN``
            * - ``'object'`` (``bool`` objects)
              - N **α**
              - ``BOOLEAN``
            * - ``'uint8'``
              - N
              - ``INTEGER``
            * - ``'int8'``
              - N
              - ``INTEGER``
            * - ``'uint16'``
              - N
              - ``INTEGER``
            * - ``'int16'``
              - N
              - ``INTEGER``
            * - ``'uint32'``
              - N
              - ``INTEGER``
            * - ``'int32'``
              - N
              - ``INTEGER``
            * - ``'uint64'``
              - N
              - ``INTEGER`` **β**
            * - ``'int64'``
              - N
              - ``INTEGER``
            * - ``'UInt8'``
              - Y
              - ``INTEGER``
            * - ``'Int8'``
              - Y
              - ``INTEGER``
            * - ``'UInt16'``
              - Y
              - ``INTEGER``
            * - ``'Int16'``
              - Y
              - ``INTEGER``
            * - ``'UInt32'``
              - Y
              - ``INTEGER``
            * - ``'Int32'``
              - Y
              - ``INTEGER``
            * - ``'UInt64'``
              - Y
              - ``INTEGER`` **β**
            * - ``'Int64'``
              - Y
              - ``INTEGER``
            * - ``'object'`` (``int`` objects)
              - Y
              - ``INTEGER`` **β**
            * - ``'float32'`` **γ**
              - Y (``NaN``)
              - ``FLOAT``
            * - ``'float64'``
              - Y (``NaN``)
              - ``FLOAT``
            * - ``'object'`` (``float`` objects)
              - Y (``NaN``)
              - ``FLOAT``
            * - ``'string'`` (``str`` objects)
              - Y
              - ``STRING`` (default), ``SYMBOL`` via ``symbols`` arg. **δ**
            * - ``'string[pyarrow]'``
              - Y
              - ``STRING`` (default), ``SYMBOL`` via ``symbols`` arg. **δ**
            * - ``'category'`` (``str`` objects) **ε**
              - Y
              - ``SYMBOL`` (default), ``STRING`` via ``symbols`` arg. **δ**
            * - ``'object'`` (``str`` objects)
              - Y
              - ``STRING`` (default), ``SYMBOL`` via ``symbols`` arg. **δ**
            * - ``'object' (``numpy.ndarray[numpy.float64]``)``
              - Y
              - ``ARRAY[DOUBLE]``
            * - ``'datetime64[ns]'``
              - Y
              - ``TIMESTAMP`` **ζ**
            * - ``'datetime64[ns, tz]'``
              - Y
              - ``TIMESTAMP`` **ζ**
            * - ``'object'`` (``Decimal`` objects)
              - Y (``NaN``)
              - ``DECIMAL``

        .. note::

            * **α**: Note some pandas dtypes allow nulls (e.g. ``'boolean'``),
              where the QuestDB database does not.

            * **β**: The valid range for integer values is -2^63 to 2^63-1.
              Any ``'uint64'``, ``'UInt64'`` or python ``int`` object values
              outside this range will raise an error during serialization.

            * **γ**: Upcast to 64-bit float during serialization.

            * **δ**: Columns containing strings can also be used to specify the
              table name. See ``table_name_col``.

            * **ε**: We only support categories containing strings. If the
              category contains non-string values, an error will be raised.

            * **ζ**: The '.dataframe()' method only supports datetimes with
              nanosecond precision. The designated timestamp column (see ``at``
              parameter) maintains the nanosecond precision, whilst values
              stored as columns have their precision truncated to microseconds.
              All dates are sent as UTC and any additional timezone information
              is dropped. If no timezone is specified, we follow
              the pandas convention of assuming the timezone is UTC.
              Datetimes before 1970-01-01 00:00:00 UTC are not supported.
              If a datetime value is specified as ``None`` (``NaT``), it is
              interpreted as the current QuestDB server time set on receipt of
              message.

            * **η**: Support for NumPy arrays (``np.array``) requires QuestDB
              server version 9.0.0 or higher.

        **Error Handling and Recovery**

        In case an exception is raised during dataframe serialization, the
        buffer is left in its previous state.
        The buffer remains in a valid state and can be used for further calls
        even after an error.

        For clarification, as an example, if an invalid ``None``
        value appears at the 3rd row for a ``bool`` column, neither the 3rd nor
        the preceding rows are added to the buffer.

        **Note**: This differs from the :func:`Sender.dataframe` method, which
        modifies this guarantee due to its ``auto_flush`` logic.

        **Performance Considerations**

        The Python GIL is released during serialization if it is not needed.
        If any column requires the GIL, the entire serialization is done whilst
        holding the GIL.

        Column types that require the GIL are:

        * Columns of ``str``, ``float`` or ``int`` or ``float`` Python objects.
        * The ``'string[python]'`` dtype.
        """

class TaggedEnum(Enum):
    """
    Base class for tagged enums.
    """

    @property
    def tag(self) -> str:
        """
        Short name.
        """

    @property
    def c_value(self) -> Any: ...
    @classmethod
    def parse(cls, tag) -> TaggedEnum:
        """
        Parse from the tag name.
        """

class Protocol(TaggedEnum):
    """
    Protocol to use for sending data to QuestDB.

    See :ref:`sender_which_protocol` for more information.
    """

    Tcp = ...
    Tcps = ...
    Http = ...
    Https = ...
    Udp = ...
    Ws = ...
    Wss = ...

    @property
    def tls_enabled(self) -> bool: ...

class QwpWsProgress(TaggedEnum):
    """
    Progress mode for QWP/WebSocket senders.
    """

    Background = ...
    Manual = ...

class SenderErrorCategory(TaggedEnum):
    """
    Category of a structured QWP/WebSocket diagnostic.
    """

    SchemaMismatch = ...
    ParseError = ...
    InternalError = ...
    SecurityError = ...
    WriteError = ...
    NotWritable = ...
    ProtocolViolation = ...
    Unknown = ...

class SenderErrorPolicy(TaggedEnum):
    """
    Applied policy for a structured QWP/WebSocket diagnostic.
    """

    Retriable = ...
    RetriableOther = ...
    Terminal = ...

@dataclass(frozen=True)
class SenderError:
    """
    Structured QWP/WebSocket diagnostic.
    """

    category: SenderErrorCategory
    applied_policy: SenderErrorPolicy
    status: Optional[int]
    message: str
    message_sequence: Optional[int]
    from_fsn: int
    to_fsn: int

class TlsCa(TaggedEnum):
    """
    Verification mechanism for the server's certificate.

    Here ``webpki`` refers to the
    `WebPKI library <https://github.com/rustls/webpki-roots>`_ and
    ``os`` refers to the operating system's certificate store.

    See :ref:`sender_conf_tls` for more information.
    """

    WebpkiRoots = ...
    OsRoots = ...
    WebpkiAndOsRoots = ...
    PemFile = ...

class PooledSender:
    """
    A row-building sender borrowed from a :class:`QuestDB` pool.

    Obtain a lease with :meth:`QuestDB.sender`; ``close()`` returns the
    native sender to the pool. Rows go through ``row()``, ``dataframe()``,
    ``flush()``, ``wait()`` and ``close()``; ``len(sender)`` is the number
    of buffered rows. Frame-level delivery tracking is available through
    :meth:`flush_and_get_fsn`, :meth:`flush_and_keep_and_get_fsn`,
    :meth:`published_fsn`, :meth:`acked_fsn` and :meth:`await_acked_fsn`,
    and per-lease server diagnostics through :meth:`poll_error` and
    :meth:`error_events_dropped`. ``dataframe()`` routes to the same
    direct columnar path as :meth:`QuestDB.dataframe`, borrowing a direct
    connection from the pool for that call. Row auto-flush is enabled by
    default at 1,000 rows, 100 milliseconds, or a cap-derived byte threshold,
    and can be configured through the ``auto_flush`` settings on the parent
    :class:`QuestDB` configuration.
    """

    def __enter__(self) -> PooledSender: ...

    def row(
        self,
        table_name: str,
        *,
        symbols: Optional[Dict[str, Optional[str]]] = None,
        columns: Optional[
            Dict[
                str,
                Union[
                    None,
                    bool,
                    int,
                    float,
                    str,
                    TimestampMicros,
                    TimestampNanos,
                    datetime,
                    np.ndarray,
                    Decimal,
                ],
            ]
        ] = None,
        at: Union[ServerTimestampType, TimestampNanos, datetime],
    ) -> PooledSender:
        """Append one row to this sender's QWP buffer. If a configured
        auto-flush threshold is breached, this publishes without waiting for
        an acknowledgement; a publish error propagates from ``row()``."""

    def dataframe(
        self,
        df: Any,
        *,
        table_name: Optional[str] = None,
        table_name_col: Union[None, int, str] = None,
        symbols: Union[str, bool, List[int], List[str]] = "auto",
        at: Union[ServerTimestampType, int, str, TimestampNanos, datetime],
        max_rows_per_batch: int = 16384,
        schema_overrides: Optional[Dict[str, object]] = None,
    ) -> PooledSender:
        """
        Bulk-load a DataFrame over a direct columnar connection borrowed
        from the pool for this call. Prefer :meth:`QuestDB.dataframe`:
        this convenience is not part of the sender's row stream and has
        no ordering relationship with rows buffered via :meth:`row`.
        Mirrors :meth:`QuestDB.dataframe`.
        """

    def __len__(self) -> int:
        """Number of buffered (unpublished) rows."""

    def flush(self, *, wait: bool = False) -> PooledSender:
        """Publish buffered rows; ``wait=True`` waits for the server OK ack
        of everything published through this lease. The wait is a pure ack
        barrier — only a terminal connection failure raises; server
        rejections go to the pool's ``error_handler`` (default:
        the ``questdb`` logger)."""

    def wait(self, timeout_millis: int = 0) -> PooledSender:
        """Wait for everything published through this lease to receive an
        OK ack; returns immediately if the lease published nothing. Only a
        terminal connection failure raises; server rejections go to the
        pool's ``error_handler``."""

    def flush_and_get_fsn(self) -> Optional[int]:
        """Publish and clear buffered rows, returning the published
        frame's FSN (``None`` for an empty buffer). FSNs are watermarks of
        the lease's pooled connection — use them while the lease is held;
        they are not portable across leases. Configure ``auto_flush=off``
        when this call must publish and identify one whole application
        batch."""

    def published_fsn(self) -> Optional[int]:
        """Highest FSN published locally on the lease's connection."""

    def acked_fsn(self) -> Optional[int]:
        """Highest FSN completed on the lease's connection by ACK or drop-and-continue rejection."""

    def await_acked_fsn(self, fsn: int, timeout_millis: int = 0) -> bool:
        """Wait until the ack watermark reaches ``fsn``; ``False`` on
        no-progress timeout. Only a terminal connection failure raises."""

    def flush_and_keep_and_get_fsn(self) -> Optional[int]:
        """Publish without clearing the buffer; returns the frame's FSN."""

    def poll_error(self) -> Optional[SenderError]:
        """Next rejection recorded on the lease's connection since this
        borrow, or ``None``. The pool ``error_handler`` independently
        receives every rejection at record time."""

    def error_events_dropped(self) -> int:
        """Diagnostics dropped from the lease's connection ring."""

    def close(self, flush: bool = True, wait: bool = False) -> None:
        """Return this sender to its pool. Idempotent. Without
        ``wait=True`` a later server rejection of this lease's rows is
        reported through the pool's ``error_handler``."""

    def __exit__(self, exc_type, exc_val, exc_tb): ...


class PooledReader:
    """
    A reader lease borrowed from a :class:`QuestDB` pool.

    The read-side twin of :meth:`QuestDB.sender`: obtain a lease with
    :meth:`QuestDB.reader`; it holds one pooled reader connection for
    its lifetime and runs queries on it sequentially via :meth:`query`.
    ``close()`` (or leaving the ``with`` block) releases the
    connection: back to the pool if the last query was drained cleanly,
    dropped otherwise.

    Queries are strictly sequential. Closing an undrained result
    terminates the lease; call ``cancel()`` before ``close()`` to
    preserve it. Use one lease per thread, on the thread that created
    it.
    """

    def __enter__(self) -> PooledReader: ...

    def query(
        self,
        sql: str,
        binds: Optional[Union[list, tuple]] = None,
        *,
        reset_symbol_dict: bool = True,
    ) -> QueryResult:
        """
        Execute a SQL query on the lease's connection and return a
        :class:`QueryResult`.

        Arguments behave exactly as on :meth:`QuestDB.query`, except the
        query runs on the reader this lease holds instead of a per-call
        pool borrow. ``reset_symbol_dict=False`` reuses the connection's
        SYMBOL dictionary built up by the lease's earlier queries.
        """

    def execute(
        self,
        sql: str,
        binds: Optional[Union[list, tuple]] = None,
    ) -> None:
        """
        Run a statement on the lease's connection and discard whatever
        it returns. Mirrors :meth:`QuestDB.execute`; the lease stays
        usable for the next call.
        """

    def close(self) -> None:
        """Release the lease's reader connection. Idempotent."""

    def __exit__(self, exc_type, exc_val, exc_tb): ...


class QuestDB:
    """
    Handle to a QuestDB deployment over QWP/WebSocket.

    Owns the connection pool; lends row-building senders via
    :meth:`sender`, bulk-loads DataFrames via :meth:`dataframe`, runs
    queries via :meth:`query`, and lends reader leases via
    :meth:`reader`. Construct with :func:`questdb.connect`.
    """

    @staticmethod
    def from_conf(
        conf_str: str,
        *,
        connection_listener: Optional[Callable[[ConnectionEvent], None]] = None,
        connection_event_inbox_capacity: int = 0,
        error_handler: Optional[Callable[[SenderError], None]] = None,
        error_event_inbox_capacity: int = 0,
    ) -> QuestDB:
        """
        Construct a handle from a QWP/WebSocket configuration string.

        Prefer the :func:`questdb.connect` module-level factory.

        By default construction connects eagerly: it pre-opens the warm
        minimums (``sender_pool_min`` senders, ``query_pool_min`` readers),
        failing fast when the server is unreachable. ``lazy_connect=true``
        opens nothing at construction: senders buffer locally and connect
        in the background, readers connect on first use.

        Pooled row auto-flush is enabled by default at 1,000 rows, 100
        milliseconds, or a cap-derived byte threshold. Set
        ``auto_flush_bytes=off`` to disable only the byte trigger, or
        ``auto_flush=off`` to disable all automatic publishing. The mode is
        shared by all leases; a lease's interval starts when its first row
        enters an empty buffer.

        ``connection_listener`` receives one :class:`ConnectionEvent` per
        connection-state transition, on a dedicated dispatcher thread.
        Successful events are queued only after negotiated state, including
        the server frame cap, is committed; they do not acknowledge data.

        ``error_handler`` receives one :class:`SenderError` per
        server rejection recorded by any of the pool's connections —
        including rejections for rows published through an
        already-closed :class:`PooledSender` — on its own dispatcher
        thread. Without it every rejection is logged through the
        ``questdb`` logger (``ERROR`` for terminal rejections,
        ``WARNING`` for retriable ones, which are replayed), so
        rejections are never silent. A terminal rejection is queued only
        after the connection's terminal latch and pollable diagnostic have
        been committed.

        When a handler or listener closes over the returned handle, call
        :meth:`close` explicitly (or use ``with``) rather than leaving
        the handle to the garbage collector.
        """

    def __enter__(self) -> QuestDB: ...

    def sender(self) -> PooledSender:
        """Borrow a context-managed row-building :class:`PooledSender` from the pool."""

    def dataframe(
        self,
        df: Any,
        *,
        table_name: Optional[str] = None,
        table_name_col: Union[None, int, str] = None,
        symbols: Union[str, bool, List[int], List[str]] = "auto",
        at: Union[ServerTimestampType, int, str, TimestampNanos, datetime],
        max_rows_per_batch: int = 16384,
        schema_overrides: Optional[Dict[str, object]] = None,
    ) -> QuestDB:
        """
        Ingest a dataframe through the pooled columnar QWP path.

        Ingestion always uses the direct (non-store-and-forward) column
        sender, independent of ``sf_dir``. On success, the call returns only
        after every DataFrame batch has been committed. Most loads queue their
        batches and commit once at the end. Large Arrow inputs checkpoint about
        every 100 batches to keep memory bounded. The client may checkpoint
        earlier if the connection cannot queue another batch or if a batch must
        be split to fit. If a later batch fails, the exception means that the
        load did not finish, not necessarily that no rows landed. Any already
        committed prefix from this call remains, and retrying the whole
        DataFrame can duplicate it unless the destination table uses suitable
        ``DEDUP UPSERT KEYS``.

        ``at`` names the designated timestamp column (by name or index),
        or a fixed ``TimestampNanos`` / ``datetime`` shared by every row,
        or the explicit ``ServerTimestamp`` sentinel to let the server
        assign each row's timestamp on arrival.

        The columnar path loads one table per call: name it via
        ``table_name`` — or, for NumPy-backed pandas input, the
        dataframe's index name (``df.index.name``); Arrow-native input
        (polars, pyarrow, pyarrow-backed pandas) requires an explicit
        ``table_name``. ``table_name_col`` raises
        :class:`UnsupportedDataFrameShapeError` — split multi-table frames
        (e.g. ``df.groupby(col)``) and load each group.

        ``df`` accepts any of:

        - **pandas** ``pandas.DataFrame`` (NumPy-backed columns route
          through the legacy planner; pyarrow-backed columns route
          through the Arrow C Stream capsule path).
        - **polars** ``polars.DataFrame`` and ``polars.LazyFrame``.
          ``LazyFrame`` is materialised via
          ``.collect(engine='streaming')`` (eager ``.collect()`` on
          polars < 1.0).
        - **pyarrow** ``pa.Table``, ``pa.RecordBatch``, and
          ``pa.RecordBatchReader``.
        - Any object exposing the Arrow C Data Interface — i.e. with
          ``__arrow_c_stream__`` (duckdb / cudf / modin / pyarrow-backed
          pandas 2.2+) or ``__arrow_c_array__`` (single Arrow array
          exporters, wrapped into a one-batch ``pa.Table``).

        ``max_rows_per_batch`` sets the pipelining granularity, not a
        safety limit: any batch exceeding the negotiated per-batch byte
        cap is split regardless of it, and a single row is never bounded
        by it. Each batch is one unit of client memory and server-side
        apply, and a commit checkpoint fires every ~100 batches, so
        ``max_rows_per_batch * 100`` rows is the replay window on a
        transient failover. Raise it for narrow numeric rows; lower it
        for very wide rows or tight memory. Streaming Arrow input
        (``pa.RecordBatchReader``) is not re-batched — the producer's
        batch size governs.
        """

    def query(
        self,
        sql: str,
        binds: Optional[Union[list, tuple]] = None,
        *,
        reset_symbol_dict: bool = True,
    ) -> QueryResult:
        """
        Execute a SQL query and return a :class:`QueryResult`.

        When ``reset_symbol_dict`` is ``True`` (the default), the server resets
        the connection's SYMBOL dictionary before this query (query-scoped
        dict), so it never inherits symbols from earlier queries on the pooled
        connection — avoiding cross-query dictionary bloat in ``to_polars()`` /
        ``to_pandas()``. Set ``False`` to keep the dictionary warm across
        repeated identical queries. No-op against servers that predate the
        capability.
        """

    def execute(
        self,
        sql: str,
        binds: Optional[Union[list, tuple]] = None,
    ) -> None:
        """
        Run a statement and discard whatever it returns.

        Executes ``sql`` like :meth:`query`, drains the result to its
        clean end and returns the pooled connection. Statement output
        (a ``COPY`` status row, admin-function rows, a stray
        ``SELECT``) is discarded; use :meth:`query` when you want the
        result. Returns ``None``: the protocol carries no
        rows-affected count.
        """

    def reader(self) -> PooledReader:
        """
        Borrow a :class:`PooledReader` lease holding one pooled reader
        connection, for running several queries in a row on that same
        connection (``with db.reader() as r: r.query(sql)``).
        """

    def server_info(self) -> ServerInfo:
        """
        Snapshot the pooled connection's ``SERVER_INFO`` handshake: role,
        failover epoch, capabilities, handshake wall-clock, cluster/node ids.
        """

    @property
    def connection_events_dropped(self) -> int:
        """Events discarded by the listener inbox's drop-oldest policy."""

    @property
    def connection_events_delivered(self) -> int:
        """Events delivered to the connection listener."""

    @property
    def error_events_delivered(self) -> int:
        """Server rejections delivered to the ``error_handler``
        (or to the default logging handler when none was registered)."""

    @property
    def error_events_dropped(self) -> int:
        """Server rejections discarded by the handler inbox's drop-oldest
        policy."""

    def reap_idle(self) -> int:
        """
        Manually reap idle above-pool-size connections.
        """

    def close(self):
        """
        Close the client and its connection pool.

        Idempotent. When called from inside one of this handle's own
        ``error_handler`` / ``connection_listener`` callbacks, it does
        not wait for a concurrent ``close()`` on another thread to
        finish; the in-flight callback completes after that close
        returns.
        """

    def __exit__(self, exc_type, _exc_val, _exc_tb): ...


class QueryResult:
    """
    Result of :meth:`QuestDB.query`. Single-use: each materialisation
    method consumes the underlying cursor.

    Consumption rule: fully drain the result, use it as a context
    manager (``with db.query(...) as result:``), or call :meth:`close`.
    Closing a partial result drops its connection. Call :meth:`cancel`
    before closing to preserve it.

    SYMBOL columns: :meth:`to_polars` / :meth:`to_pandas` build the
    Categorical directly, interning the connection dictionary once;
    :meth:`to_arrow` / :meth:`iter_arrow` / :meth:`__arrow_c_stream__` give a
    generic compact-dictionary Arrow form a consumer reconciles. When the
    target is a polars / pandas frame, the dedicated methods avoid the
    re-reconciliation that ``polars.from_arrow(result)`` /
    ``to_arrow().to_pandas()`` pay on SYMBOL-heavy results.
    """

    def __arrow_c_stream__(self, requested_schema: Any = None) -> Any:
        """Arrow C stream PyCapsule protocol (no pyarrow needed). SYMBOL
        columns arrive compact — each batch's dictionary holds only the values
        it references — so a consumer that unifies per-batch dictionaries
        (e.g. ``polars.from_arrow``) reconciles them."""

    def to_arrow(self) -> Any:
        """Read the full result into a ``pyarrow.Table``. Requires pyarrow."""

    def to_pandas(
        self,
        *,
        dtype_backend: Optional[str] = None,
        types_mapper: Optional[Callable[[Any], Any]] = None,
    ) -> pd.DataFrame:
        """Read the full result into a ``pandas.DataFrame``. With no arguments
        the result is materialised via numpy (pyarrow-free); passing
        ``dtype_backend`` or ``types_mapper`` selects the pyarrow path."""

    def to_polars(self) -> Any:
        """Read the full result into a ``polars.DataFrame``. Requires polars
        and pyarrow."""

    def iter_arrow(self) -> Iterator[Any]:
        """Iterate result batches as ``pyarrow.RecordBatch``."""

    def iter_polars(self) -> Iterator[Any]:
        """Iterate result batches as ``polars.DataFrame`` (streaming /
        low-peak-memory). Batches share one ``Categories`` identity so
        ``polars.concat`` over them stitches cleanly. Requires polars and
        pyarrow."""

    def iter_pandas(
        self,
        *,
        dtype_backend: Optional[str] = None,
        types_mapper: Optional[Callable[[Any], Any]] = None,
    ) -> Iterator[pd.DataFrame]:
        """Iterate result batches as ``pandas.DataFrame``. With no arguments
        the batches are materialised via numpy (pyarrow-free); passing
        ``dtype_backend`` or ``types_mapper`` selects the pyarrow path."""

    def cancel(self) -> None:
        """Cancel the query and drain to terminal. The result remains
        open until :meth:`close` or context exit. On failure, it is
        closed. Idempotent after success."""

    def close(self) -> None:
        """Release the cursor and reader. Terminal connections return
        to the pool; others are dropped. Idempotent."""

    def __enter__(self) -> QueryResult: ...

    def __exit__(self, exc_type, exc_val, exc_tb): ...


class Sender:
    """
    Ingest data into QuestDB over a single connection.

    This is the connection-level API: one sender drives exactly one
    connection (ILP/HTTP, ILP/TCP, QWP/UDP, or a single QWP/WebSocket
    connection) and carries the point-to-point capabilities the
    deployment-level handle does not: HTTP transactions, UDP datagrams,
    and manual ws progress and buffer control. For pooled ingestion and
    queries, prefer :func:`questdb.connect`.

    See the :ref:`sender` documentation for more information.
    """

    def __init__(
        self,
        protocol: Protocol,
        host: str,
        port: Union[int, str],
        *,
        bind_interface: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        token: Optional[str] = None,
        token_x: Optional[str] = None,
        token_y: Optional[str] = None,
        auth_timeout: int = 15000,
        tls_verify: bool = True,
        tls_ca: Optional[TlsCa] = None,
        tls_roots=None,
        tls_roots_password: Optional[str] = None,
        max_buf_size: int = 104857600,
        retry_timeout: Union[int, timedelta] = 10000,
        retry_max_backoff: Union[int, timedelta] = 1000,
        request_min_throughput: int = 102400,
        request_timeout=None,
        auto_flush: bool = True,
        auto_flush_rows: Optional[int] = None,
        auto_flush_bytes: Union[int, bool, None] = None,
        auto_flush_interval: Union[int, timedelta, bool] = 1000,
        max_datagram_size: Optional[int] = None,
        multicast_ttl: Optional[int] = None,
        qwp_ws_progress: Optional[QwpWsProgress] = None,
        error_handler: Optional[Callable[["SenderError"], None]] = None,
        connection_listener: Optional[Callable[[ConnectionEvent], None]] = None,
        connection_event_inbox_capacity: int = 0,
        protocol_version=None,
        init_buf_size: int = 65536,
        max_name_len: int = 127,
    ): ...
    @staticmethod
    def from_conf(
        conf_str: str,
        *,
        bind_interface: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        token: Optional[str] = None,
        token_x: Optional[str] = None,
        token_y: Optional[str] = None,
        auth_timeout: int = 15000,
        tls_verify: bool = True,
        tls_ca: Optional[TlsCa] = None,
        tls_roots=None,
        tls_roots_password: Optional[str] = None,
        max_buf_size: int = 104857600,
        retry_timeout: Union[int, timedelta] = 10000,
        retry_max_backoff: Union[int, timedelta] = 1000,
        request_min_throughput: int = 102400,
        request_timeout=None,
        auto_flush: bool = True,
        auto_flush_rows: Optional[int] = None,
        auto_flush_bytes: Union[int, bool, None] = None,
        auto_flush_interval: Union[int, timedelta, bool] = 1000,
        max_datagram_size: Optional[int] = None,
        multicast_ttl: Optional[int] = None,
        qwp_ws_progress: Optional[QwpWsProgress] = None,
        error_handler: Optional[Callable[["SenderError"], None]] = None,
        connection_listener: Optional[Callable[[ConnectionEvent], None]] = None,
        connection_event_inbox_capacity: int = 0,
        protocol_version=None,
        init_buf_size: int = 65536,
        max_name_len: int = 127,
    ) -> Sender:
        """
        Construct a sender from a :ref:`configuration string <sender_conf>`.

        The additional arguments are used to specify additional parameters
        which are not present in the configuration string.

        Note that any parameters already present in the configuration string
        cannot be overridden.
        """
        ...

    @staticmethod
    def from_env(
        *,
        bind_interface: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        token: Optional[str] = None,
        token_x: Optional[str] = None,
        token_y: Optional[str] = None,
        auth_timeout: int = 15000,
        tls_verify: bool = True,
        tls_ca: Optional[TlsCa] = None,
        tls_roots=None,
        tls_roots_password: Optional[str] = None,
        max_buf_size: int = 104857600,
        retry_timeout: Union[int, timedelta] = 10000,
        retry_max_backoff: Union[int, timedelta] = 1000,
        request_min_throughput: int = 102400,
        request_timeout=None,
        auto_flush: bool = True,
        auto_flush_rows: Optional[int] = None,
        auto_flush_bytes: Union[int, bool, None] = None,
        auto_flush_interval: Union[int, timedelta, bool] = 1000,
        max_datagram_size: Optional[int] = None,
        multicast_ttl: Optional[int] = None,
        qwp_ws_progress: Optional[QwpWsProgress] = None,
        error_handler: Optional[Callable[["SenderError"], None]] = None,
        connection_listener: Optional[Callable[[ConnectionEvent], None]] = None,
        connection_event_inbox_capacity: int = 0,
        protocol_version=None,
        init_buf_size: int = 65536,
        max_name_len: int = 127,
    ) -> Sender:
        """
        Construct a sender from the ``QDB_CLIENT_CONF`` environment variable.

        The environment variable must be set to a valid
        :ref:`configuration string <sender_conf>`.

        The additional arguments are used to specify additional parameters
        which are not present in the configuration string.

        Note that any parameters already present in the configuration string
        cannot be overridden.
        """

    def new_buffer(self) -> Buffer:
        """
        Make a new configured buffer.

        The buffer is set up with the configured `init_buf_size` and
        `max_name_len`, and matches the sender's protocol.

        Must be called after :func:`Sender.establish` and before
        :func:`Sender.close`; otherwise raises
        :class:`QuestDBError` (``InvalidApiCall``).
        """

    @property
    def init_buf_size(self) -> int:
        """The initial capacity of the sender's internal buffer."""

    @property
    def auto_flush(self) -> bool:
        """
        Auto-flushing is enabled.

        Consult the `.auto_flush_rows`, `.auto_flush_bytes` and
        `.auto_flush_interval` properties for the current active thresholds.
        """

    @property
    def auto_flush_rows(self) -> Optional[int]:
        """
        Row count threshold for the auto-flush logic, or None if disabled.
        """

    @property
    def auto_flush_bytes(self) -> Optional[int]:
        """
        Byte-count threshold for the auto-flush logic, or None if disabled.
        """

    @property
    def auto_flush_interval(self) -> Optional[timedelta]:
        """
        Time interval threshold for the auto-flush logic, or None if disabled.
        """

    @property
    def protocol_version(self) -> int:
        """
        The protocol version used by the sender.

        Protocol version 1 is retained for backwards compatibility with
        older QuestDB versions.

        Protocol version 2 introduces binary floating point support and
        the array datatype.
        """

    @property
    def max_name_len(self) -> int:
        """
        Returns the sender's maximum-configured maximum name length for table
        names and column names.
        """

    def establish(self):
        """
        Prepare the sender for use.

        If using ILP/HTTP this will initialize the HTTP connection pool.

        If using ILP/TCP this will cause connection to the server and
        block until the connection is established.

        If the TCP connection is set up with authentication and/or TLS, this
        method will return only *after* the handshake(s) is/are complete.
        """

    def __enter__(self) -> Sender:
        """Call :func:`Sender.establish` at the start of a ``with`` block."""

    def __len__(self) -> int:
        """
        Number of bytes of unsent data in the internal buffer.

        Equivalent (but cheaper) to ``len(bytes(sender))``.
        """

    def __bytes__(self) -> bytes:
        """
        Inspect the contents of the internal buffer.

        The ``bytes`` value returned represents the unsent data.

        Also see :func:`Sender.__len__`.
        """

    def transaction(self, table_name: str) -> SenderTransaction:
        """
        Start a :ref:`sender_transaction` block.
        """

    def row(
        self,
        table_name: str,
        *,
        symbols: Optional[Dict[str, Optional[str]]] = None,
        columns: Optional[
            Dict[str, Union[None, bool, int, float, str, TimestampMicros, TimestampNanos, datetime, np.ndarray, Decimal]]
        ] = None,
        at: Union[TimestampNanos, datetime, ServerTimestampType],
    ) -> Sender:
        """
        Write a row to the internal buffer.

        This may be sent automatically depending on the ``auto_flush`` setting
        in the constructor.

        Refer to the :func:`Buffer.row` documentation for details on arguments.

        **Note**: Support for NumPy arrays (``np.array``) requires QuestDB server version 9.0.0 or higher.
        """

    def dataframe(
        self,
        df: pd.DataFrame,
        *,
        table_name: Optional[str] = None,
        table_name_col: Union[None, int, str] = None,
        symbols: Union[str, bool, List[int], List[str]] = "auto",
        at: Union[ServerTimestampType, int, str, TimestampNanos, datetime],
        max_rows_per_batch: int = 16384,
        schema_overrides: Optional[Dict[str, object]] = None,
    ) -> Sender:
        """
        Write a Pandas DataFrame to QuestDB.

        Over ILP/HTTP, ILP/TCP and QWP/UDP the frame is serialized through the
        row buffer. Over QWP/WebSocket it is bulk-loaded through a poolless
        direct columnar connection opened from this sender's own configuration
        (the same direct path as :meth:`QuestDB.dataframe`, carrying the
        sender's auth/TLS regardless of how it was built; ``max_rows_per_batch``
        and ``schema_overrides`` apply only there). The direct load has no
        ordering relationship with rows buffered via :meth:`row` and does not
        flush them.

        Example:

        .. code-block:: python

            import pandas as pd
            import questdb as qi

            df = pd.DataFrame({
                'car': pd.Categorical(['Nic 42', 'Eddi', 'Nic 42', 'Eddi']),
                'position': [1, 2, 1, 2],
                'speed': [89.3, 98.2, 3, 4],
                'lat_gforce': [0.1, -0.2, -0.6, 0.4],
                'accelleration': [0.1, -0.2, 0.6, 4.4],
                'tyre_pressure': [2.6, 2.5, 2.6, 2.5],
                'ts': [
                    pd.Timestamp('2022-08-09 13:56:00'),
                    pd.Timestamp('2022-08-09 13:56:01'),
                    pd.Timestamp('2022-08-09 13:56:02'),
                    pd.Timestamp('2022-08-09 13:56:03')]})

            with qi.Sender.from_env() as sender:
                sender.dataframe(df, table_name='race_metrics', at='ts')

        This method builds on top of the :func:`Buffer.dataframe` method.
        See its documentation for details on arguments.

        Additionally, this method also supports auto-flushing the buffer
        as specified in the ``Sender``'s ``auto_flush`` constructor argument.
        Auto-flushing is implemented incrementally, meanting that when
        calling ``sender.dataframe(df)`` with a large ``df``, the sender may
        have sent some of the rows to the server already whist the rest of the
        rows are going to be sent at the next auto-flush or next explicit call
        to :func:`Sender.flush`.

        In case of data errors with auto-flushing enabled, some of the rows
        may have been transmitted to the server already.
        """

    def flush(
        self,
        buffer: Optional[Buffer] = None,
        clear: bool = True,
        transactional: bool = False,
    ):
        """
        If called with no arguments, immediately flushes the internal buffer.

        Alternatively you can flush a buffer that was constructed explicitly
        by passing ``buffer``.

        The buffer will be cleared by default, unless ``clear`` is set to
        ``False``.

        This method does nothing if the provided or internal buffer is empty.

        :param buffer: The buffer to flush. If ``None``, the internal buffer
            is flushed.

        With QWP/WebSocket, this publishes the buffer into the local sender
        queue and returns before the server necessarily ACKs the frame. Later
        terminal diagnostics fail subsequent sender calls and are available as
        :attr:`QuestDBError.sender_error`. Server diagnostics are also
        available through :func:`Sender.poll_error`.

        :param clear: If ``True``, the flushed buffer is cleared (default).
            If ``False``, the flushed buffer is left in the internal buffer.
            Note that ``clear=False`` is only supported if ``buffer`` is also
            specified.

        :param transactional: If ``True`` ensures that the flushed buffer
            contains row for a single table, ensuring all data can be written
            transactionally. This feature requires ILP/HTTP and is not available
            when connecting over TCP. *Default: False.*

        The Python GIL is released during the network IO operation.
        """

    def flush_and_get_fsn(self, buffer: Optional[Buffer] = None) -> Optional[int]:
        """
        Publish a QWP/WebSocket buffer locally, clear it on success, and return
        the assigned frame sequence number.
        """

    def flush_and_keep_and_get_fsn(
        self, buffer: Optional[Buffer] = None
    ) -> Optional[int]:
        """
        Publish a QWP/WebSocket buffer locally without clearing it and return
        the assigned frame sequence number.
        """

    def published_fsn(self) -> Optional[int]:
        """
        Highest QWP/WebSocket frame sequence number published locally.
        """

    def acked_fsn(self) -> Optional[int]:
        """
        Highest QWP/WebSocket frame sequence number completed by ACK or
        drop-and-continue rejection.
        """

    def await_acked_fsn(self, fsn: int, timeout_millis: int = 0) -> bool:
        """
        Wait until the QWP/WebSocket completion watermark reaches ``fsn``.
        """

    def drive_once(self) -> bool:
        """
        Drive one QWP/WebSocket progress step for manual progress senders.
        """

    def poll_error(self) -> Optional[SenderError]:
        """
        Poll the next structured QWP/WebSocket diagnostic.
        """

    def error_events_dropped(self) -> int:
        """
        Number of QWP/WebSocket diagnostics dropped from the bounded ring.
        """

    @property
    def connection_events_dropped(self) -> int:
        """
        Total connection events discarded by the listener inbox's
        drop-oldest policy. ``0`` when no listener is registered.
        """

    @property
    def connection_events_delivered(self) -> int:
        """
        Total connection events delivered to the listener. ``0`` when no
        listener is registered.
        """

    def close_drain(self):
        """
        Stop accepting new QWP/WebSocket publications and wait for already
        published frames to resolve.
        """

    def close(self, flush: bool = True):
        """
        Disconnect.

        This method is idempotent and can be called repeatedly.

        Once a sender is closed, it can't be re-used.

        :param bool flush: If ``True``, flush the internal buffer before closing.
            For QWP/WebSocket, this also drains already-published frames before
            closing.
        """

    def __exit__(self, exc_type, _exc_val, _exc_tb):
        """
        Flush pending and disconnect at the end of a ``with`` block.

        If the ``with`` block raises an exception, any pending data will
        *NOT* be flushed.

        This is implemented by calling :func:`Sender.close`.
        """
