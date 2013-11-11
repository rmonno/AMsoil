import amsoil.core.pluginmanager as pm
import amsoil.core.log
logger=amsoil.core.log.getLogger('ons_comms')

ons_ex = pm.getService('opennaas_exceptions')
config = pm.getService("config")

import requests
import xml.etree.ElementTree as ET

"""
OpenNaas Commands Manager.
"""
class CommandsManager(object):
    """ Resource commands """

    def __init__(self, host, port):
        self._base_url = 'http://' + host + ':' + port + '/opennaas/'
        self._auth = (config.get("opennaas.user"), config.get("opennaas.password"))

    def post(self, url, xml_data):
        try:
            logger.debug("POST url=%s, data=%s" % (url, xml_data,))
            resp_ = requests.post(url=url, headers={'Content-Type': 'application/xml'},
                                  auth=self._auth, data=xml_data).text
            logger.debug("POST resp=%s" % (resp_,))
            return resp_

        except requests.exceptions.RequestException as e:
            raise ons_ex.ONSException(str(e))

    def get(self, url):
        try:
            logger.debug("GET url=%s" % (url,))
            resp_ = requests.get(url=url, auth=self._auth).text
            logger.debug("GET resp=%s" % (resp_,))
            return resp_

        except requests.exceptions.RequestException as e:
            raise ons_ex.ONSException(str(e))

    def delete(self, url):
        try:
            logger.debug("DELETE url=%s" % (url,))
            resp_ = requests.delete(url=url, auth=self._auth).text
            logger.debug("DELETE resp=%s" % (resp_,))
            return resp_

        except requests.exceptions.RequestException as e:
            raise ons_ex.ONSException(str(e))

    def decode_xml_entry(self, xml_data):
        try:
            return [e.text.strip() for e in ET.fromstring(xml_data).findall('entry')]

        except ET.ParseError as e:
            logger.error("XML ParseError: %s" % (str(e),))
            return []

    def resource_create(self):
        try:
            descr = open('/home/ofelia-cf/opennaas-1/utils/examples/descriptors/roadm.descriptor', 'r')
            data = descr.read()

            command = 'resources/create'
            r = self.post(self._base_url + command, data)
            logger.debug("CM: response=%s" % str(r))

        except Exception as e:
            logger.error("CM: error=%s" % str(e))

    def getResources(self):
        ret_ = []
        command = 'resources/getResourceTypes'
        ts_ = self.get(self._base_url + command)
        for t in self.decode_xml_entry(ts_):
            command = 'resources/listResourcesByType/' + t
            ns_ = self.get(self._base_url + command)
            ret_.extend([(t, n) for n in self.decode_xml_entry(ns_)])

        return ret_

class RoadmCM(CommandsManager):
    """ Roadm specific commands """

    def __init__(self, host, port):
        super(RoadmCM, self).__init__(host, port)
        self.error_code = ['ERROR', 'error']

    def decode_xml_conn(self, xml_data):
        try:
            return (ET.fromstring(xml_data).find('instanceID').text,
                    ET.fromstring(xml_data).find('srcEndPointId').text,
                    ET.fromstring(xml_data).find('srcLabelId').text,
                    ET.fromstring(xml_data).find('dstEndPointId').text,
                    ET.fromstring(xml_data).find('dstLabelId').text)

        except ET.ParseError as e:
            logger.error("XML ParseError: %s" % (str(e),))
            return None

    def encode_xml_conn(self, x_id, src_ep, src_label, dst_ep, dst_label):
        root = ET.Element('xConnection')
        ET.SubElement(root, 'instanceID').text = x_id
        ET.SubElement(root, 'srcEndPointId').text = src_ep
        ET.SubElement(root, 'srcLabelId').text = src_label
        ET.SubElement(root, 'dstEndPointId').text = dst_ep
        ET.SubElement(root, 'dstLabelId').text = dst_label

        return ET.tostring(root)

    def check_queue_error(self, response):
        root = ET.fromstring(response)
        for responses in root.findall('responses'):
            status = responses.find('status').text
            if status in self.error_code:
                return True, responses.find('actionID').text

        return False, None

    def makeXConnection(self, r_type, r_name, instance_id,
                        src_ep_id, src_label_id,
                        dst_ep_id, dst_label_id):
        data = self.encode_xml_conn(instance_id, src_ep_id, src_label_id,
                                    dst_ep_id, dst_label_id)
        command = r_type + '/' + r_name + '/xconnect/'
        r = self.post(self._base_url + command, data)

        if r != instance_id:
            raise ons_ex.ONSException('InstanceId mismatch!')

    def removeXConnection(self, r_type, r_name, instance_id):
        command = r_type + '/' + r_name + '/xconnect/' + instance_id
        self.delete(self._base_url + command)

    def getXConnections(self, r_type, r_name):
        command = r_type + '/' + r_name + '/xconnect/'
        cs_ = self.get(self._base_url + command)
        return self.decode_xml_entry(cs_)

    def getXConnection(self, r_type, r_name, xconn_id):
        command = r_type + '/' + r_name + '/xconnect/' + xconn_id
        eps_ = self.get(self._base_url + command)
        return self.decode_xml_conn(eps_)

    def getEndPoints(self, r_type, r_name):
        command = r_type + '/' + r_name + '/xconnect/getEndPoints'
        eps_ = self.get(self._base_url + command)
        return self.decode_xml_entry(eps_)

    def getLabels(self, r_type, r_name, ep_id):
        command = r_type + '/' + r_name + '/xconnect/getLabels/' + ep_id
        ls_ = self.get(self._base_url + command)
        return self.decode_xml_entry(ls_)

    def execute(self, r_type, r_name):
        command = r_type + '/' + r_name + '/queue/execute'
        r = self.post(self._base_url + command, None)

        error, reason = self.check_queue_error(r)
        if error:
            raise ons_ex.ONSException(reason)


commandsMngr = RoadmCM(host=config.get("opennaas.server_address"),
                       port=str(config.get("opennaas.server_port")))
