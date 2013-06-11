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
import urllib2

"""
OpenNaas Resource Manager.
"""
def send_to(url, data):
    try:
        request = urllib2.Request(url, data)
        request.add_header('Content-Type', 'application/xml')
        response = urllib2.urlopen(request)
        return response.read()

    except urllib2.HTTPError as error:
        raise ons_ex.ONSException(str(error))


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
        """ Reserve resources
        :param resources_name: the name of the resources
        :param slice_name: the name of the slice
        :param end_time: end time of reservation
        :param client_name: client name
        :param client_id: client identifier
        :param client_mail: client email
        :return: list of Resources type
        """
        pass

    @abstractmethod
    def renew_resources(self, resources, slices, end_time):
        """ Renew resources (throw exception if any check fails)
        :param resources: dict with resource name (key) and client information
        :param slices: dict with slice name (key) and client information
        :param end_time: end time of reservation
        :return: list of Resources type
        """
        pass

    @abstractmethod
    def force_renew_resources(self, resources, slices, end_time):
        """ Renew resources (skips all checks)
        :param resources: dict with resource name (key) and client information
        :param slices: dict with slice name (key) and client information
        :param end_time: end time of reservation
        :return: list of Resources type
        """
        pass

    @abstractmethod
    def delete_resources(self, resources, slices):
        """ Delete resources (throw exception if any check fails)
        :param resources: dict with resource name (key) and client information
        :param slices: dict with slice name (key) and client information
        :return: list of Resources type
        """
        pass

    @abstractmethod
    def force_delete_resources(self, resources, slices):
        """ Delete resources (skips all checks)
        :param resources: dict with resource name (key) and client information
        :param slices: dict with slice name (key) and client information
        :return: list of Resources type
        """
        pass


class RMRoadmManager(RMInterface):

    def __init__(self):
        super(RMRoadmManager, self).__init__()
        ons_models.meta.create_all()
        logger.debug("Init openNaas ROADM resource manager")

        if config.get("opennaas.tests"): RMTests()

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
                      'modified_time': dt.datetime.now(),
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

    @serviceinterface
    def renew_resources(self, resources, slices, end_time):
        raise ons_ex.ONSException("renew_resources: NOT implemented yet!")

    @serviceinterface
    def force_renew_resources(self, resources, slices, end_time):
        s_ = sessionmaker(bind=ons_models.engine)()
        values = {'end_time': end_time,
                  'modified_time': dt.datetime.now()}
        rs_ = []

        for res_ in resources:
            try:
                rs_.extend(s_.query(ons_models.Roadm).filter(ons_models.Roadm.resource_name == res_).all())

                stmt = ons_models.roadm.update().where(sqla.and_(ons_models.Roadm.resource_name == res_)).values(values)
                s_.execute(stmt)
                s_.commit()

            except sqla.exc.SQLAlchemyError as e:
                logger.warning(str(e))

        for sli_ in slices:
            try:
                rs_.extend(s_.query(ons_models.Roadm).filter(ons_models.Roadm.slice_name == sli_).all())

                stmt = ons_models.roadm.update().where(sqla.and_(ons_models.Roadm.slice_name == sli_)).values(values)
                s_.execute(stmt)
                s_.commit()

            except sqla.exc.SQLAlchemyError as e:
                logger.warning(str(e))

        logger.debug("force_renew_resources=%s" % (rs_,))
        return rs_

    @serviceinterface
    def delete_resources(self, resources, slices):
        s_ = sessionmaker(bind=ons_models.engine)()
        rs_ = []

        try:
            for res_ in resources:
                rs_.extend(s_.query(ons_models.Roadm).filter(ons_models.Roadm.resource_name == res_).all())
                s_.query(ons_models.Roadm).filter(ons_models.Roadm.resource_name == res_).delete()

            for sli_ in slices:
                rs_.extend(s_.query(ons_models.Roadm).filter(ons_models.Roadm.slice_name == sli_).all())
                s_.query(ons_models.Roadm).filter(ons_models.Roadm.slice_name == sli_).delete()

            s_.commit()

        except sqla.exc.SQLAlchemyError as e:
            s_.rollback()
            raise ons_ex.ONSException(str(e))

        logger.debug("delete_resources=%s" % (rs_,))
        return rs_

    @serviceinterface
    def force_delete_resources(self, resources, slices):
        raise ons_ex.ONSException("force_delete_resources: NOT implemented yet!")


class RMTests:
    def __init__(self):
        try: # db tests
            s_ = sessionmaker(bind=ons_models.engine)()
            rs = s_.query(ons_models.Roadm).all()
            logger.debug("TESTs: resources=%s" % (str(rs)))

            s_.add(ons_models.Roadm(sname='urn:publicid:IDN+geni:gpo:gcf+slice+pippuzzoSlice', rname='pippo'))
            s_.add(ons_models.Roadm(sname='urn:publicid:IDN+geni:gpo:gcf+slice+pippuzzoSlice', rname='pluto'))
            s_.add(ons_models.Roadm(sname='urn:publicid:IDN+geni:gpo:gcf+slice+pippuzzoSlice', rname='paperino'))
            s_.add(ons_models.Roadm(sname='urn:publicid:IDN+geni:gpo:gcf+slice+pippuzzoSlice', rname='topolo'))
            s_.commit()

        except Exception as e:
            logger.error("Tests error: %s" % str(e))

        try: # opennaas communication tests
            url_ = 'http://' + config.get("opennaas.server_address") + ':' +\
                   str(config.get("opennaas.server_port")) + '/opennaas/resources/create'
            descr = open('/home/ofelia-cf/opennaas/utils/examples/descriptors/roadm.descriptor', 'r')
            data = descr.read()

            r = send_to(url_, data)
            logger.debug("TESTs: Response=%s" % str(r))

        except Exception as e:
            logger.error("Tests error: %s" % str(e))
