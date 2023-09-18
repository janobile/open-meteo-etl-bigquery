class ETLException(Exception):
    """
    Custom exception class for ETL-related errors.
    """
    def __init__(self, message="An error occurred during the ETL process"):
        super().__init__(message)


class DataValidationException(Exception):
    """
    Custom exception class for data validation errors.
    """
    def __init__(self, message):
        super().__init__(message)