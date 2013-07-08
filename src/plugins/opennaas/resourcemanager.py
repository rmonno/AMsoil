import amsoil.core.pluginmanager as pm
import amsoil.core.log
logger=amsoil.core.log.getLogger('ons_rm')

from amsoil.core import serviceinterface

ons_ex = pm.getService('opennaas_exceptions')
ons_models = pm.getService('opennaas_models')
ons_fsm = pm.getService('opennaas_fsm')
config = pm.getService("config")
worker = pm.getService('worker')

from abc import ABCMeta, abstractmethod
import datetime as dt
import sqlalchemy as sqla
from sqlalchemy.orm import sessionmaker


"""
OpenNaas Resource Manager.
"""
class RMInterface(object):
    __metaclass__ = ABCMeta

    @abstractmethod
    def get_resources(self):
        """ Get all managed resources
        :return: list of GeniResources
        """
        pass

    @abstractmethod
    def reserve_resources(self, resources, slice_name, end_time=None,
                          client_name="", client_id="", client_mail=""):
        """ Reserve resources
        :param resources: list of dict conteining resources information
        :param slice_name: the name of the slice
        :param end_time: end time of reservation
        :param client_name: client name
        :param client_id: client identifier
        :param client_mail: client email
        :return: list of GeniResources
        """
        pass

    @abstractmethod
    def get_slice_resources(self, slice_name):
        """ Get all managed resources
        :param slice_name: the name of the slice
        :return: list of GeniResources
        """
        pass

    @abstractmethod
    def renew_resources(self, slices, end_time):
        """ Renew resources (throw exception if any check fails)
        :param slices: dict with slice name (key) and client information
        :param end_time: end time of reservation
        :return: list of GeniResources
        """
        pass

    @abstractmethod
    def force_renew_resources(self, slices, end_time):
        """ Renew resources (skips all checks)
        :param slices: dict with slice name (key) and client information
        :param end_time: end time of reservation
        :return: list of GeniResources
        """
        pass

    @abstractmethod
    def start_slices(self, slices):
        """ Start slices (throw exception if any check fails)
        :param slices: dict with slice name (key) and client information
        :return: list of GeniResources
        """
        pass

    @abstractmethod
    def force_start_slices(self, slices):
        """ Start slices (skips all checks)
        :param slices: dict with slice name (key) and client information
        :return: list of GeniResources
        """
        pass

    @abstractmethod
    def stop_slices(self, slices):
        """ Stop slices (throw exception if any check fails)
        :param slices: dict with slice name (key) and client information
        :return: list of GeniResources
        """
        pass

    @abstractmethod
    def force_stop_slices(self, slices):
        """ Stop slices (skips all checks)
        :param slices: dict with slice name (key) and client information
        :return: list of GeniResources
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
        (ret, error) = ons_models.roadmsDBM.create_all()
        if not ret:
            logger.warning("DBM error: %s" % (error,))

        worker.addAsReccurring('opennaas_resourcemanager', 'update_resources',
                               None, config.get("opennaas.update_timeout"))
        worker.addAsReccurring('opennaas_resourcemanager', 'check_resources_expiration',
                               None, config.get("opennaas.check_expire_timeout"))
        logger.debug("Init openNaas ROADM resource manager")

    def __create_manifest(self, resources, sname, endt, alloc):
        ret_ = []
        for r in resources:
            in_urn = ons_models.create_roadm_urn(r['gen']['name'],
                                                 r['spec']['in_endpoint'],
                                                 r['spec']['in_label'])
            out_urn = ons_models.create_roadm_urn(r['gen']['name'],
                                                  r['spec']['out_endpoint'],
                                                  r['spec']['out_label'])

            ret_.append(ons_models.GeniResource(in_urn, sname, endt, r['gen']['type'], alloc))
            ret_.append(ons_models.GeniResource(out_urn, sname, endt, r['gen']['type'], alloc))

        return ret_

    def __create_detailed_manifest(self, resources):
        ret_ = []
        for (r_in, r_out, conns) in resources:
            logger.debug("In=%s, Out=%s, Conns=%s" % (r_in, r_out, conns,))

            in_urn = ons_models.create_roadm_urn(r_in.name, r_in.endpoint, r_in.label)
            out_urn = ons_models.create_roadm_urn(r_out.name, r_out.endpoint, r_out.label)

            geni_in_ = ons_models.GeniResource(in_urn, conns.slice_urn, conns.end_time,
                                               r_in.type, r_in.allocation)
            geni_in_.roadm_details(conns.client_name, conns.client_id, conns.client_email,
                                   connected_out_urn=out_urn)
            ret_.append(geni_in_)

            geni_out_ = ons_models.GeniResource(out_urn, conns.slice_urn, conns.end_time,
                                                r_in.type, r_in.allocation)
            geni_out_.roadm_details(conns.client_name, conns.client_id, conns.client_email,
                                    connected_in_urn=in_urn)
            ret_.append(geni_out_)

        return ret_

    def create_geni_resource(self, resource_name, endpoint, label,
                             slice_name, end_time, resource_type, allocation):
        urn_ = ons_models.create_roadm_urn(resource_name, endpoint, label)
        return ons_models.GeniResource(urn_, slice_name, end_time, resource_type, allocation)

    @worker.outsideprocess
    def update_resources(self, params):
        ons_fsm.fsmMngr.action()

    @worker.outsideprocess
    def check_resources_expiration(self, params):
        pass

    @serviceinterface
    def get_resources(self):
        try:
            ons_models.roadmsDBM.open_session()
            rs_ = ons_models.roadmsDBM.get_resources()

            return [self.create_geni_resource(rname, ep, lb, sname, endt, rtype, alloc)
                    for (rname, ep, lb, sname, endt, rtype, alloc) in rs_]
        finally:
            ons_models.roadmsDBM.close_session()

    @serviceinterface
    def reserve_resources(self, resources, slice_name, end_time=None,
                          client_name="", client_id="", client_mail=""):
        try:
            ons_models.roadmsDBM.open_session()
            conns_ = [ons_models.roadmsDBM.check_to_reserve(r['gen'], r['spec']) for r in resources]

            if not end_time:
                minutes = config.get("opennaas.reservation_timeout")
                end_time = dt.datetime.now() + dt.timedelta(minutes)

            elif end_time <= dt.datetime.now():
                raise ons_ex.ONSException("End-Time is in the PAST!")

            values = {'slice_name': slice_name,
                      'end_time': end_time,
                      'client_name': client_name,
                      'client_id': client_id,
                      'client_email': client_mail}

            for ingress_id, egress_id in conns_:
                ons_models.roadmsDBM.make_connection(ingress_id, egress_id, values)

            return self.__create_manifest(resources, slice_name, end_time, ons_models.ALLOCATION.ALLOCATED)

        finally:
            ons_models.roadmsDBM.close_session()

    @serviceinterface
    def get_slice_resources(self, slice_name):
        try:
            ons_models.roadmsDBM.open_session()
            rs_ = ons_models.roadmsDBM.get_slice(slice_name)

            return self.__create_detailed_manifest(rs_)

        finally:
            ons_models.roadmsDBM.close_session()

    @serviceinterface
    def renew_resources(self, slices, end_time):
        raise ons_ex.ONSException("renew_resources: NOT implemented yet!")

    @serviceinterface
    def force_renew_resources(self, slices, end_time):
        try:
            ons_models.roadmsDBM.open_session()
            rs_ = []
            for s_urn in slices.keys():
                logger.debug("Slice urn=%s" % (s_urn,))
                try:
                    ons_models.roadmsDBM.renew_slice(s_urn, end_time, slices[s_urn])
                    r_info_ = ons_models.roadmsDBM.get_slice(s_urn)
                    rs_.extend(self.__create_detailed_manifest(r_info_))

                except sqla.exc.SQLAlchemyError as e:
                    logger.error(str(e))

            return rs_

        finally:
            ons_models.roadmsDBM.close_session()

    @serviceinterface
    def start_slices(self, slices):
        raise ons_ex.ONSException("start_slices: NOT implemented yet!")

    @serviceinterface
    def force_start_slices(self, slices):
        raise ons_ex.ONSException("force_start_slices: NOT implemented yet!")

    @serviceinterface
    def stop_slices(self, slices):
        raise ons_ex.ONSException("stop_slices: NOT implemented yet!")

    @serviceinterface
    def force_stop_slices(self, slices):
        raise ons_ex.ONSException("force_stop_slices: NOT implemented yet!")

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
