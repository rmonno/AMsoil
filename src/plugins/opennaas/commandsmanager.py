import amsoil.core.pluginmanager as pm
import amsoil.core.log
logger=amsoil.core.log.getLogger('ons_comms')

ons_ex = pm.getService('opennaas_exceptions')
config = pm.getService("config")

import urllib2

"""
OpenNaas Commands Manager.
"""
class CommandsManager(object):
    def __init__(self, host, port):
        self._base_url = 'http://' + host + ':' + port + '/opennaas/resources/'

    def __send_to(self, url, data):
        try:
            request = urllib2.Request(url, data)
            request.add_header('Content-Type', 'application/xml')
            response = urllib2.urlopen(request)
            return response.read()

        except urllib2.HTTPError as error:
            raise ons_ex.ONSException(str(error))

    def create(self):
        try:
            descr = open('/home/ofelia-cf/opennaas/utils/examples/descriptors/roadm.descriptor', 'r')
            data = descr.read()

            command = 'create'
            r = self.__send_to(self._base_url + command, data)
            logger.debug("CommandsManager: response=%s" % str(r))

        except Exception as e:
            logger.error("CommandsManager ERROR: %s" % str(e))

    def makeXConnection(self, instance_id, src_ep_id, src_label_id,
                        dst_ep_id, dst_label_id):
        """
        <xConnection>
            <instanceID>id</instanceID>
            <srcEndPointId>srcEP</srcEndPointId>
            <srcLabelId>srcL</srcLabelId>
            <dstEndPointId>dstEP</dstEndPointId>
            <dstLabelId>dstL</dstLabelId>
        </xConnection>
        """
        ingress = (instance_id, src_ep_id, src_label_id, dst_ep_id, dst_label_id)
        logger.debug("Ingress=%s" % (ingress,))

    def removeXConnection(self, ident):
        ingress = (ident,)
        logger.debug("Ingress=%s" % (ingress,))

    def getXConnections(self):
        ingress = ()
        logger.debug("Ingress=%s" % (ingress,))

    def getXConnection(self, ident):
        ingress = (ident,)
        logger.debug("Ingress=%s" % (ingress,))

    def getEndPoints(self):
        ingress = ()
        logger.debug("Ingress=%s" % (ingress,))

    def getLabels(self, ident):
        ingress = (ident,)
        logger.debug("Ingress=%s" % (ingress,))


commandsMngr = CommandsManager(host=config.get("opennaas.server_address"),
                               port=str(config.get("opennaas.server_port")))
