from amsoil.core.exception import CoreException

"""
Plugin exceptions.
"""

class ONSException(CoreException):
    def __init__(self, desc):
        self._desc = desc

    def __str__(self):
        return "ONS: %s" % (self._desc,)


class ONSSwitchNotFound(ONSException):
    def __init__(self, dpid):
        super(ONSSwitchNotFound, self).__init__("Switch NOT found (%s)" % (dpid,))
