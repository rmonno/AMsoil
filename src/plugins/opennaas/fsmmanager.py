import amsoil.core.pluginmanager as pm
import amsoil.core.log
logger=amsoil.core.log.getLogger('ons_fsm')

ons_models = pm.getService('opennaas_models')
ons_comms = pm.getService('opennaas_commands')
ons_ex = pm.getService('opennaas_exceptions')

from fysom import Fysom

"""
OpenNaas FSM Manager.
"""
class FSM(Fysom):
    def __init__(self):
        self.data = []
        self.xcs_data = []

        super(FSM, self).__init__({
            'initial': 'clean',
            'events': [{'name': 'action', 'src': 'get',    'dst': 'update'},
                       {'name': 'action', 'src': 'update', 'dst': 'clean'},
                       {'name': 'action', 'src': 'clean',  'dst': 'get'}],
            'callbacks': {'onget':    self.on_get,
                          'onupdate': self.on_update,
                          'onclean':  self.on_clean}
        })

    def __xconns(self, rtype, rname):
        return [ons_comms.commandsMngr.getXConnection(rtype, rname, xc)
                for xc in ons_comms.commandsMngr.getXConnections(rtype, rname)]

    def __ep_labels(self, rtype, rname):
        return [{'ep':ep, 'labels':ons_comms.commandsMngr.getLabels(rtype, rname, ep)}
                for ep in ons_comms.commandsMngr.getEndPoints(rtype, rname)]

    def on_get(self, e):
        logger.debug("FSM-GET: src=%s, dst=%s" % (e.src, e.dst,))
        try:
            rs = ons_comms.commandsMngr.getResources()
            logger.debug("Resources=%s" % (rs,))

            self.data = [{'type':rtype, 'name':rname, 'res':self.__ep_labels(rtype, rname)}
                         for (rtype, rname) in rs]

            self.xcs_data = [{'type':rtype, 'name':rname, 'xconns':self.__xconns(rtype, rname)}
                             for (rtype, rname) in rs]

            logger.info("Data=%s" % (self.data,))
            logger.info("XConns=%s" % (self.xcs_data,))

        except ons_ex.ONSException as e:
            logger.error(str(e))

    def on_update(self, e):
        logger.debug("FSM-UPDATE: src=%s, dst=%s" % (e.src, e.dst,))

    def on_clean(self, e):
        logger.debug("FSM-CLEAN: src=%s, dst=%s" % (e.src, e.dst,))

        del self.data[:]
        del self.xcs_data[:]


fsmMngr = FSM()
