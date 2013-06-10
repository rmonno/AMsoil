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
    def __init__(self, slice_name, resource_name):
        super(ONSResourceNotFound, self).__init__("Resource NOT found (slice=%s, resource=%s)" %
                                                  (slice_name, resource_name,))


class ONSResourceNotAvailable(ONSException):
    def __init__(self, slice_name, resource_name):
        super(ONSResourceNotAvailable, self).__init__("Resource NOT available (slice=%s, resource=%s)" %
                                                      (slice_name, resource_name,))
