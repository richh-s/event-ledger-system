class LedgerError(Exception):
    """Base class for all exceptions raised by the Ledger."""
    pass


class OptimisticConcurrencyError(LedgerError):
    """
    Raised when an append operation fails because the stream's current version
    does not match the expected version provided by the client.
    """
    def __init__(self, stream_id: str, expected_version: int, actual_version: int):
        self.stream_id = stream_id
        self.expected_version = expected_version
        self.actual_version = actual_version
        super().__init__(
            f"Optimistic concurrency conflict on stream '{stream_id}'. "
            f"Expected version: {expected_version}, Actual version: {actual_version}"
        )


class StreamArchivedError(LedgerError):
    """Raised when attempting to append events to an archived stream."""
    def __init__(self, stream_id: str):
        self.stream_id = stream_id
        super().__init__(f"Cannot append to archived stream '{stream_id}'.")


class DomainError(LedgerError):
    """Raised when a generic domain invariant is violated."""
    pass
