class ProbeScraperError(Exception):
    """Exception type for returning errors in push mode."""

    def __init__(self, message, status_code):
        self.status_code = status_code
        self.message = message


class ProbeScraperInvalidRequest(ProbeScraperError):
    """Exception type for returning HTTP 4XX in push mode."""

    def __init__(self, message, status_code=400):
        super().__init__(message, status_code)


class ProbeScraperServerError(ProbeScraperError):
    """Exception type for returning HTTP 5XX in push mode."""

    def __init__(self, message, status_code=500):
        super().__init__(message, status_code)
