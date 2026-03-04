class PersistenceError(Exception):
    """Base class for exceptions in this module."""
    pass

class DatabaseConnectionError(PersistenceError):
    """Exception raised for errors in the database connection."""
    pass

class DocumentNotFoundError(PersistenceError):
    """Exception raised when a document is not found in the database."""
    pass

class DuplicateDocumentError(PersistenceError):
    """Exception raised when attempting to insert a duplicate document."""
    pass