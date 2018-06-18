"""
Classes for the different exceptions that the dxlstreamingclient APIs can raise.
"""

class TemporaryError(Exception):
    """
    Exception raised when an unexpected/unknown (but possibly recoverable)
    error occurs.
    """
    pass


class PermanentError(Exception):
    """
    Exception raised for an operation which would not be expected to
    succeed even if the operation were retried.
    """
    pass


class StopError(Exception):
    """
    Exception raised for an operation which is interrupted due to the channel
    being stopped.
    """
    pass
