class NotFoundError(LookupError):
    """Requested entity does not exist (-> HTTP 404)."""


class ConflictError(RuntimeError):
    """Request conflicts with current state (-> HTTP 409)."""
