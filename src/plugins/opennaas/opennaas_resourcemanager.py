import amsoil.core.pluginmanager as pm
import amsoil.core.log
logger=amsoil.core.log.getLogger('ons_resourcemanager')

from amsoil.core import serviceinterface
from opennaas_exceptions import *

"""
OpenNaas Resource Manager.
"""

class OpenNaasResourceManager(object):

    def __init__(self):
        super(OpenNaasResourceManager, self).__init__()
        logger.debug("Init openNaas resource manager...")

    @serviceinterface
    def get_version(self):
        logger.info("get_version called...")
        return "4.15"
