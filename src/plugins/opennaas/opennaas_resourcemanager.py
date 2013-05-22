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
    def get_resources(self):
        """ Get all managed resources

        :return: list of Resources type
        """
        pass


class OpenNaasResourceManager(ResourceManagerInterface):

    def __init__(self):
        super(OpenNaasResourceManager, self).__init__()
        logger.debug("Init openNaas resource manager...")

    @serviceinterface
    def get_resources(self):
        return [ons_models.RoadmResource(available=True, special="Try this..."),
                ons_models.Resource(available=False)]
