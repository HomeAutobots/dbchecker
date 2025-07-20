"""
Custom exceptions for the database comparison module.
"""


class DatabaseComparisonError(Exception):
    """Base exception for database comparison errors"""
    pass


class SchemaExtractionError(DatabaseComparisonError):
    """Raised when schema cannot be extracted"""
    pass


class DataComparisonError(DatabaseComparisonError):
    """Raised during data comparison"""
    pass


class UUIDDetectionError(DatabaseComparisonError):
    """Raised when UUID detection fails"""
    pass


class DatabaseConnectionError(DatabaseComparisonError):
    """Raised when database connection fails"""
    pass


class InvalidConfigurationError(DatabaseComparisonError):
    """Raised when configuration is invalid"""
    pass
