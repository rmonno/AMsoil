import amsoil.core.pluginmanager as pm
import amsoil.core.log
logger=amsoil.core.log.getLogger('ons_rm')

from amsoil.core import serviceinterface

ons_ex = pm.getService('opennaas_exceptions')
ons_models = pm.getService('opennaas_models')
ons_fsm = pm.getService('opennaas_fsm')
config = pm.getService("config")
worker = pm.getService('worker')
ons_comms = pm.getService('opennaas_commands')

from abc import ABCMeta, abstractmethod
import datetime as dt

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
    def delete_slices(self, slices):
        """ Remove slices (throw exception if any check fails)
        :param slices: dict with slice name (key) and client information
        :return: list of GeniResources
        """
        pass

    @abstractmethod
    def force_delete_slices(self, slices):
        """ Remove slices (skips all checks)
        :param slices: dict with slice name (key) and client information
        :return: list of GeniResources
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

    def __create_manifest(self, resources, sname, endt, alloc, oper):
        ret_ = []
        for r in resources:
            in_urn = ons_models.create_roadm_urn(r['gen']['name'],
                                                 r['spec']['in_endpoint'],
                                                 r['spec']['in_label'])
            out_urn = ons_models.create_roadm_urn(r['gen']['name'],
                                                  r['spec']['out_endpoint'],
                                                  r['spec']['out_label'])

            ret_.append(ons_models.GeniResource(in_urn, sname, endt, r['gen']['type'], alloc, oper))
            ret_.append(ons_models.GeniResource(out_urn, sname, endt, r['gen']['type'], alloc, oper))

        return ret_

    def __create_detailed_manifest(self, resources):
        ret_ = []
        for (r_in, r_out, conns) in resources:
            logger.debug("In=%s, Out=%s, Conns=%s" % (r_in, r_out, conns,))

            in_urn = ons_models.create_roadm_urn(r_in.name, r_in.endpoint, r_in.label)
            out_urn = ons_models.create_roadm_urn(r_out.name, r_out.endpoint, r_out.label)

            geni_in_ = ons_models.GeniResource(in_urn, conns.slice_urn, conns.end_time,
                                               r_in.type, r_in.allocation, r_in.operational)
            geni_in_.roadm_details(conns.client_name, conns.client_id, conns.client_email,
                                   connected_out_urn=out_urn)
            ret_.append(geni_in_)

            geni_out_ = ons_models.GeniResource(out_urn, conns.slice_urn, conns.end_time,
                                                r_out.type, r_out.allocation, r_out.operational)
            geni_out_.roadm_details(conns.client_name, conns.client_id, conns.client_email,
                                    connected_in_urn=in_urn)
            ret_.append(geni_out_)

        return ret_

    def __operation_slices(self, slices, call_func):
        try:
            ons_models.roadmsDBM.open_session()
            exec_ = set()
            rs_ = []
            for s_urn in slices.keys():
                logger.debug("Slice urn=%s" % (s_urn,))
                r_info_ = ons_models.roadmsDBM.get_slice(s_urn)
                e_info_ = [call_func(rin, rout, conn) for (rin, rout, conn) in r_info_]

                [exec_.add(e) for e in e_info_]
                rs_.extend(self.__create_detailed_manifest(r_info_))

            [ons_comms.commandsMngr.execute(rtype, rname) for (rtype, rname) in exec_]
            return rs_

        finally:
            ons_models.roadmsDBM.close_session()

    def __start_conn(self, r_in, r_out, conns):
        if (r_in.type != r_out.type) or (r_in.name != r_out.name):
            raise ons_ex.ONSException("Mismatch between ingress/egress openNaas resources!")

        ons_models.roadmsDBM.oper_connection(conns.ingress, conns.egress,
                                             ons_models.OPERATIONAL.READY_BUSY)
        ons_comms.commandsMngr.makeXConnection(r_in.type, r_in.name, conns.xconn_id,
                                               r_in.endpoint, r_in.label,
                                               r_out.endpoint, r_out.label)
        return (r_in.type, r_in.name)

    def __stop_conn(self, r_in, r_out, conns):
        if (r_in.type != r_out.type) or (r_in.name != r_out.name):
            raise ons_ex.ONSException("Mismatch between ingress/egress openNaas resources!")

        ons_models.roadmsDBM.oper_connection(conns.ingress, conns.egress,
                                             ons_models.OPERATIONAL.READY)
        ons_comms.commandsMngr.removeXConnection(r_in.type, r_in.name, conns.xconn_id)
        return (r_in.type, r_in.name)

    def __release_conn(self, r_in, r_out, conns):
        if (r_in.type != r_out.type) or (r_in.name != r_out.name):
            raise ons_ex.ONSException("Mismatch between ingress/egress openNaas resources!")

        ons_models.roadmsDBM.destroy_connection(conns.ingress, conns.egress)
        ons_comms.commandsMngr.removeXConnection(r_in.type, r_in.name, conns.xconn_id)
        return (r_in.type, r_in.name)

    def create_geni_resource(self, resource_name, endpoint, label, slice_name,
                             end_time, resource_type, allocation, operational):
        urn_ = ons_models.create_roadm_urn(resource_name, endpoint, label)
        return ons_models.GeniResource(urn_, slice_name, end_time,
                                       resource_type, allocation, operational)

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

            return [self.create_geni_resource(rname, ep, lb, sname, endt, rtype, alloc, oper)
                    for (rname, ep, lb, sname, endt, rtype, alloc, oper) in rs_]
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

            for ingress_id, egress_id, xconn_id in conns_:
                ons_models.roadmsDBM.make_connection(ingress_id, egress_id, xconn_id, values)

            return self.__create_manifest(resources, slice_name, end_time,
                                          ons_models.ALLOCATION.ALLOCATED,
                                          ons_models.OPERATIONAL.READY)
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

                except ons_ex.ONSException as e:
                    logger.error(str(e))

            return rs_

        finally:
            ons_models.roadmsDBM.close_session()

    @serviceinterface
    def start_slices(self, slices):
        return self.__operation_slices(slices, self.__start_conn)

    @serviceinterface
    def force_start_slices(self, slices):
        raise ons_ex.ONSException("force_start_slices: NOT implemented yet!")

    @serviceinterface
    def stop_slices(self, slices):
        return self.__operation_slices(slices, self.__stop_conn)

    @serviceinterface
    def force_stop_slices(self, slices):
        raise ons_ex.ONSException("force_stop_slices: NOT implemented yet!")

    @serviceinterface
    def delete_slices(self, slices):
        return self.__operation_slices(slices, self.__release_conn)

    @serviceinterface
    def force_delete_slices(self, slices):
        raise ons_ex.ONSException("force_delete_slices: NOT implemented yet!")
