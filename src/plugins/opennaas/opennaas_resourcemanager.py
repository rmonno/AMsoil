import amsoil.core.pluginmanager as pm
import amsoil.core.log
logger=amsoil.core.log.getLogger('ons_resourcemanager')

from amsoil.core import serviceinterface

ons_ex = pm.getService('opennaas_exceptions')
ons_models = pm.getService('opennaas_models')

from abc import ABCMeta, abstractmethod

"""
OpenNaas Resource Manager.
"""

class ResourceManagerInterface(object):
    __metaclass__ = ABCMeta

    @abstractmethod
    def get_resources(self, slice_name=None, resource_id=None):
        """ Get all managed resources
        :param slice_name: the name of the slice (optional)
        :param resource_id: the resource identifier (optional)
        :return: list of Resources type
        """
        pass


class OpenNaasResourceManager(ResourceManagerInterface):

    def __init__(self):
        super(OpenNaasResourceManager, self).__init__()
        logger.debug("Init openNaas resource manager...")

    @serviceinterface
    def get_resources(self, slice_name=None, resource_id=None):
        return [ons_models.RoadmResource(name="Test-1", special="Try this..."),
                ons_models.Resource("Test-2", ons_models.ALLOCATION.PROVISIONED)]
