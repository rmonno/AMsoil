import amsoil.core.pluginmanager as pm
import amsoil.core.log
logger=amsoil.core.log.getLogger('ons_rm')

from amsoil.core import serviceinterface

ons_ex = pm.getService('opennaas_exceptions')
ons_models = pm.getService('opennaas_models')

from abc import ABCMeta, abstractmethod
import sqlalchemy as sqla
from sqlalchemy.orm import sessionmaker

"""
OpenNaas Resource Manager.
"""

class RMInterface(object):
    __metaclass__ = ABCMeta

    @abstractmethod
    def get_resources(self, slice_name=None, resource_name=None):
        """ Get all managed resources
        :param slice_name: the name of the slice (optional)
        :param resource_name: the resource name (optional)
        :return: list of Resources type
        """
        pass


class RMRoadmManager(RMInterface):

    def __init__(self):
        super(RMRoadmManager, self).__init__()
        ons_models.meta.create_all()
        logger.debug("Init openNaas ROADM resource manager")

    @serviceinterface
    def get_resources(self, slice_name=None, resource_name=None):
        s_ = sessionmaker(bind=ons_models.engine)()

        if (slice_name is not None) and (resource_name is not None):
            logger.debug("get_resources: slice_name=%s, resource_name=%s" % (slice_name, resource_name,))
            return s_.query(ons_models.Roadm).filter(sqla.and_(ons_models.Roadm.slice_name == slice_name,
                                                               ons_models.Roadm.resource_name == resource_name))

        elif (slice_name is not None):
            logger.debug("get_resources: slice_name=%s" % (slice_name,))
            return s_.query(ons_models.Roadm).filter(ons_models.Roadm.slice_name == slice_name)

        elif (resource_name is not None):
            logger.debug("get_resources: resource_name=%s" % (resource_name,))
            return s_.query(ons_models.Roadm).filter(ons_models.Roadm.resource_name == resource_name)

        else:
            logger.debug("get_resources")
            return s_.query(ons_models.Roadm).all()
