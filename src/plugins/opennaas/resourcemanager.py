import amsoil.core.pluginmanager as pm
import amsoil.core.log
logger=amsoil.core.log.getLogger('ons_rm')

from amsoil.core import serviceinterface

ons_ex = pm.getService('opennaas_exceptions')
ons_models = pm.getService('opennaas_models')
config = pm.getService("config")

from abc import ABCMeta, abstractmethod
import datetime as dt
import sqlalchemy as sqla
import sqlalchemy.orm.exc as sqla_ex
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

    @abstractmethod
    def reserve_resources(self, resources_name, slice_name, end_time=None,
                          client_name="", client_id="", client_mail=""):
        """ Reserve resource
        :param resources_name: the name of the resources
        :param slice_name: the name of the slice
        :param end_time: end time of reservation
        :param client_name: client name
        :param client_id: client identifier
        :param client_mail: client email
        :return: Resource type
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
                                                               ons_models.Roadm.resource_name == resource_name)).one()

        elif (slice_name is not None):
            logger.debug("get_resources: slice_name=%s" % (slice_name,))
            return s_.query(ons_models.Roadm).filter(ons_models.Roadm.slice_name == slice_name).all()

        elif (resource_name is not None):
            logger.debug("get_resources: resource_name=%s" % (resource_name,))
            return s_.query(ons_models.Roadm).filter(ons_models.Roadm.resource_name == resource_name).all()

        else:
            logger.debug("get_resources")
            return s_.query(ons_models.Roadm).all()

    @serviceinterface
    def reserve_resources(self, resources_name, slice_name, end_time=None,
                          client_name="", client_id="", client_mail=""):
        s_ = sessionmaker(bind=ons_models.engine)()

        rs_ = []
        for r_name_ in resources_name:
            logger.debug("reserve_resource: slice name=%s, resource name=%s" % (slice_name, r_name_,))
            r_ = None
            try:
                r_ = s_.query(ons_models.Roadm).filter(sqla.and_(ons_models.Roadm.slice_name == slice_name,
                                                                 ons_models.Roadm.resource_name == r_name_)).one()
            except sqla_ex.NoResultFound as e:
                raise ons_ex.ONSResourceNotFound(slice_name, r_name_)

            logger.debug("Resource=%s" % (str(r_)))
            if not r_.available():
                raise ons_ex.ONSResourceNotAvailable(slice_name, r_name_)

            rs_.append(r_)

        if not end_time:
            end_time = dt.datetime.now() + dt.timedelta(minutes=config.get("opennaas.reservation_timeout"))

        elif end_time <= dt.datetime.now():
            raise ons_ex.ONSException("End-Time is in the PAST!")

        try:
            values = {'end_time': end_time,
                      'allocation': ons_models.ALLOCATION.ALLOCATED,
                      'client_name': client_name,
                      'client_id': client_id,
                      'client_email': client_mail}
            for r_name_ in resources_name:
                stmt = ons_models.roadm.update().where(sqla.and_(ons_models.Roadm.slice_name == slice_name,
                                                                 ons_models.Roadm.resource_name == r_name_)).values(values)
                s_.execute(stmt)

            s_.commit()

        except sqla.exc.SQLAlchemyError as e:
            s_.rollback()
            raise ons_ex.ONSException(str(e))

        return rs_
