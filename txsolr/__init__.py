"""
Solr Bindings for Python and Twisted
"""

##########################################################################
# Logging configuration
import logging

# NOTE: this is not necessary in Python 2.7
class _NullHandler(logging.Handler):

    def emit(self, record):
        pass

_logger = logging.getLogger('txsolr')
_logger.addHandler(_NullHandler())

def logToStderr(level=logging.DEBUG):
    global _logger
    _logger.addHandler(logging.StreamHandler())
    _logger.setLevel(level)



