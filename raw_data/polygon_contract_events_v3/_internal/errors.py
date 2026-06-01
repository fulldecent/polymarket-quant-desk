"""Exception types raised by this library."""

from __future__ import annotations


class V3Error(Exception):
    """Base class for every error raised by this library."""


class SchemaMismatchError(V3Error):
    """The hot DB on disk does not match the bundled ``schema.sql``.

    Raised by ``HotStore.__init__`` when an existing hot DB is missing a
    table that the schema defines, or has a column with a different
    name or type. Indicates the schema has evolved; the caller must
    re-create the DB.
    """


class DuplicateRowError(V3Error):
    """A duplicate ``(transaction_hash, log_index)`` row was found in the
    rows being sunk to a Parquet partition.

    Fatal and unrecoverable by contract: the application is designed so
    that the same block range is never loaded twice (atomic
    INSERT + ``loaded_block_ranges`` update), so the only way duplicates
    can arise is a logic error inside a decoder. The caller must abort and
    investigate.

    Raised by ``parquet_sink.write_partition_files``: the COPY uses
    ``SELECT DISTINCT ON`` to collapse duplicates, and a post-write
    row-count check detects the collapse and aborts the partition.
    """


class PartitionFrontierError(V3Error):
    """A partition was requested that does not extend the existing sunk
    frontier.

    The cold tier must advance one 10K partition at a time starting at
    ``floor(SCRAPE_START_BLOCK / 10_000) * 10_000``, so writing partition
    ``P`` is allowed only when every partition below ``P`` has already
    been sunk. Trying to write out of order raises this error.
    """


class OperationCancelled(V3Error):
    """A caller-supplied ``stop_event`` was set while a long-running
    operation was in flight. The library makes a best-effort attempt to
    leave durable state consistent before raising.
    """
