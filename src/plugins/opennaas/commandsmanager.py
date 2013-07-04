import amsoil.core.pluginmanager as pm
import amsoil.core.log
logger=amsoil.core.log.getLogger('ons_comms')

ons_ex = pm.getService('opennaas_exceptions')
config = pm.getService("config")

import requests

"""
OpenNaas Commands Manager.
"""
class CommandsManager(object):
    """ Resource commands """

    def __init__(self, host, port):
        self._base_url = 'http://' + host + ':' + port + '/opennaas/resources/'

    def post(self, url, xml_data):
        try:
            return requests.post(url=url, headers={'Content-Type': 'application/xml'},
                                 data=xml_data).text

        except requests.exceptions.RequestException as e:
            raise ons_ex.ONSException(str(e))

    def get(self, url):
        try:
            return requests.get(url=url).text

        except requests.exceptions.RequestException as e:
            raise ons_ex.ONSException(str(e))

    def resource_create(self):
        try:
            descr = open('/home/ofelia-cf/opennaas-1/utils/examples/descriptors/roadm.descriptor', 'r')
            data = descr.read()

            command = 'create'
            r = self.post(self._base_url + command, data)
            logger.debug("CommandsManager: response=%s" % str(r))

        except Exception as e:
            logger.error("CommandsManager ERROR: %s" % str(e))

    def resource_list(self):
        try:
            command = 'getResourceTypes'
            r = self.get(self._base_url + command)
            logger.debug("CommandsManager: response=%s" % str(r))

        except Exception as e:
            logger.error("CommandsManager ERROR: %s" % str(e))


class RoadmCM(CommandsManager):
    """ Roadm specific commands """

    def __init__(self, host, port):
        super(RoadmCM, self).__init__(host, port)

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
        try:
            command = 'getXConnections'
            r = self.get(self._base_url + command)
            logger.debug("CommandsManager: response=%s" % str(r))

        except Exception as e:
            logger.error("CommandsManager ERROR: %s" % str(e))

    def getXConnection(self, ident):
        ingress = (ident,)
        logger.debug("Ingress=%s" % (ingress,))

    def getEndPoints(self):
        try:
            command = 'getEndPoints'
            r = self.get(self._base_url + command)
            logger.debug("CommandsManager: response=%s" % str(r))

        except Exception as e:
            logger.error("CommandsManager ERROR: %s" % str(e))

    def getLabels(self, ident):
        ingress = (ident,)
        logger.debug("Ingress=%s" % (ingress,))


commandsMngr = RoadmCM(host=config.get("opennaas.server_address"),
                       port=str(config.get("opennaas.server_port")))
