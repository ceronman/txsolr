"""
Errors
"""

class WrongHTTPStatus(ValueError):
    pass

class SolrResponseError(Exception):
    pass
