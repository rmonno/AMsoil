from amsoil.core.exception import CoreException

"""
Plugin resource exceptions.
"""

class ONSException(CoreException):
    def __init__(self, desc):
        self._desc = desc

    def __str__(self):
        return "ONS error: %s" % (self._desc,)


class ONSResourceNotFound(ONSException):
    def __init__(self, error):
        super(ONSResourceNotFound, self).__init__("ResourceNotFound [error=%s]" %
                                                  (error,))


class ONSResourceNotAvailable(ONSException):
    def __init__(self, error):
        super(ONSResourceNotAvailable, self).__init__("ResourceNotAvailable [error=%s]" %
                                                      (error,))
