import amsoil.core.pluginmanager as pm
import amsoil.core.log
logger=amsoil.core.log.getLogger('ons_fsm')

ons_models = pm.getService('opennaas_models')
ons_comms = pm.getService('opennaas_commands')
ons_ex = pm.getService('opennaas_exceptions')
config = pm.getService("config")

from fysom import Fysom

"""
OpenNaas FSM Manager.
"""
class FSM(Fysom):
    def __init__(self):
        self.step = config.get("opennaas.update_step")
        self.resources = []
        self.roadms = []
        self.xconns = []

        super(FSM, self).__init__({
            'initial': 'get',
            'events': [{'name': 'action', 'src': 'get',    'dst': 'update'},
                       {'name': 'action', 'src': 'update', 'dst': 'clean'},
                       {'name': 'action', 'src': 'clean',  'dst': 'get'}]
        })

    def __xconns(self, resources):
        return [(rtype, rname, ons_comms.commandsMngr.getXConnection(rtype, rname, xc))
                for (rtype, rname) in self.resources
                for xc in ons_comms.commandsMngr.getXConnections(rtype, rname)]

    def __ep_labels(self, resources):
        return [(rtype, rname, ep, label)
                for (rtype, rname) in self.resources
                for ep in ons_comms.commandsMngr.getEndPoints(rtype, rname)
                for label in ons_comms.commandsMngr.getLabels(rtype, rname, ep)]

    def __update(self, call_func, info):
        try:
            ons_models.roadmsDBM.open_session()
            call_func(info)

        except ons_ex.ONSException as e:
            logger.error(str(e))

        finally:
            ons_models.roadmsDBM.close_session()

    def onbeforeaction(self, e):
        if e.src == 'get' and (not len(self.resources) and not len(self.roadms) and not len(self.xconns)):
            logger.debug("Do not leave get-state, not information are available!")
            self.onget(e)
            return False

        if e.src == 'update' and (len(self.resources) or len(self.roadms) or len(self.xconns)):
            logger.debug("Do not leave update-state, ongoing db-update procedure!")
            self.onupdate(e)
            return False

        return True

    def onget(self, e):
        logger.debug("FSM-get: src=%s, dst=%s" % (e.src, e.dst,))
        try:
            self.resources = ons_comms.commandsMngr.getResources()
            self.roadms = self.__ep_labels(self.resources)
            self.xconns = self.__xconns(self.resources)

            logger.info("Resources=%d" % (len(self.resources),))
            logger.info("Roadms=%d" % (len(self.roadms),))
            logger.info("XConns=%d" % (len(self.xconns),))

        except ons_ex.ONSException as e:
            logger.error(str(e))

    def onupdate(self, e):
        logger.debug("FSM-update: src=%s, dst=%s" % (e.src, e.dst,))

        if len(self.resources):
            info_ = [self.resources.pop() for x in range(self.step) if len(self.resources)]
            self.__update(ons_models.roadmsDBM.audit_resources, info_)
            logger.debug("Missing resources=%d" % (len(self.resources),))

        elif len(self.roadms):
            info_ = [self.roadms.pop() for x in range(self.step) if len(self.roadms)]
            self.__update(ons_models.roadmsDBM.audit_roadms, info_)
            logger.debug("Missing roadms=%d" % (len(self.roadms),))

        elif len(self.xconns):
            info_ = [self.xconns.pop() for x in range(self.step) if len(self.xconns)]
            self.__update(ons_models.roadmsDBM.audit_connections, info_)
            logger.debug("Missing connections=%d" % (len(self.xconns),))

    def onclean(self, e):
        logger.debug("FSM-clean: src=%s, dst=%s" % (e.src, e.dst,))

        del self.resources[:]
        del self.roadms[:]
        del self.xconns[:]

        try:
            ons_models.roadmsDBM.open_session()
            ons_models.roadmsDBM.audit_terminated()

        except ons_ex.ONSException as e:
            logger.error(str(e))

        finally:
            ons_models.roadmsDBM.close_session()


fsmMngr = FSM()
