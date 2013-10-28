import amsoil.core.pluginmanager as pm
import amsoil.core.log
logger=amsoil.core.log.getLogger('ons_delegate')

GENIv3DelegateBase = pm.getService('geniv3delegatebase')
geni_ex = pm.getService('geniv3exceptions')
ons_ex = pm.getService('opennaas_exceptions')
ons_models = pm.getService('opennaas_models')
config = pm.getService("config")

"""
GENI delegate.
"""
class OpenNaasGENI3Delegate(GENIv3DelegateBase):
    URN_PREFIX = 'urn:OPENNAAS_AM'
    NAMESPACE_PREFIX = 'opennaas'
    NAMESPACE_URI = 'http://example.com/' + NAMESPACE_PREFIX

    def __init__(self):
        super(OpenNaasGENI3Delegate, self).__init__()
        self._resource_manager = pm.getService("opennaas_resourcemanager")
        logger.info("OpenNaas delegate created...")

    def enter_method_log(f):
        as_ = f.func_code.co_varnames[:f.func_code.co_argcount]
        def wrapper(*args, **kwargs):
            ass_ = ', '.join('%s=%r' % e for e in zip(as_, args) + kwargs.items())
            logger.debug("Calling %s with args=%s" % (f.func_name, ass_,))
            return f(*args, **kwargs)
        return wrapper

    def get_request_extensions_mapping(self):
        return {self.NAMESPACE_PREFIX: self.NAMESPACE_URI}

    def get_manifest_extensions_mapping(self):
        return {self.NAMESPACE_PREFIX: self.NAMESPACE_URI}

    def get_ad_extensions_mapping(self):
        return {self.NAMESPACE_PREFIX: self.NAMESPACE_URI}

    def is_single_allocation(self):
        return True

    def get_allocation_mode(self):
        return 'geni_many'

    @enter_method_log
    def list_resources(self, client_cert, credentials, geni_available):
        self.__authenticate(client_cert, credentials, None, ('listslices',))

        rn_ = self.lxml_ad_root()
        em_ = self.lxml_ad_element_maker(self.NAMESPACE_PREFIX)

        for r in self._resource_manager.get_resources():
            if geni_available and not r.available(): continue

            res_ = em_.resource()
            if r.type == 'roadm':
                (name, endpoint, label) = ons_models.decode_roadm_urn(r.urn)
                res_.append(em_.name(name))
                res_.append(em_.type(r.type))
                res_.append(em_.endpoint(endpoint))
                res_.append(em_.label(label))
            else:
                res_.append(em_.name(r.urn))
                res_.append(em_.type(r.type))

            res_.append(em_.available('True' if r.available() else 'False'))

            rn_.append(res_)

        return self.lxml_to_string(rn_)

    def describe(self, urns, client_cert, credentials):
        return self.status(urns, client_cert, credentials)[0]

    @enter_method_log
    def status(self, urns, client_cert, credentials):
        rs_ = []
        for u_ in urns:
            self.__authenticate(client_cert, credentials, u_, ('sliverstatus',))

            if self.urn_type(u_) != 'slice':
                raise geni_ex.GENIv3OperationUnsupportedError('Only slice URNs can be given to this aggregate')

            rs_slice_ = self._resource_manager.get_slice_resources(slice_name=u_)
            rs_.extend(rs_slice_)

        if not len(rs_):
            raise geni_ex.GENIv3SearchFailedError("There are no resources in the given slice(s)")

        logger.debug("Resources=%s" % (rs_,))

        slivers_ = [self.__format_sliver_status(r, True, True, r.error) for r in rs_]
        slice_urn_ = self.lxml_to_string(self.__format_manifest_rspec(rs_))
        return (slice_urn_, slivers_)

    @enter_method_log
    def allocate(self, slice_urn, client_cert, credentials, rspec, end_time=None):
        c_urn_, c_uuid_, c_email_ = self.__authenticate(client_cert, credentials, slice_urn, ('createsliver',))

        logger.debug("client_urn=%s, client_uuid=%s, client_email=%s" % (str(c_urn_), str(c_uuid_), str(c_email_),))

        resources_ = []
        for em in self.lxml_parse_rspec(rspec).getchildren():
            r_ = {}
            self.__verify_resource_tag(em)
            (r_['gen'], em_spec_) = self.__extract_gen_resource_info(em.getchildren())

            if r_['gen']['type'] == 'roadm':
                r_['spec'] = self.__extract_roadm_conn_info(em_spec_)

            resources_.append(r_)

        rs_ = []
        try:
            logger.debug("Resources=%s" % str(resources_))
            rs_ = self._resource_manager.reserve_resources(resources=resources_,
                                                           slice_name=slice_urn,
                                                           end_time=end_time,
                                                           client_name=str(c_urn_),
                                                           client_id=str(c_uuid_),
                                                           client_mail=str(c_email_))
        except ons_ex.ONSResourceNotFound as e:
            logger.error(str(e))
            raise geni_ex.GENIv3SearchFailedError(str(e))

        except ons_ex.ONSResourceNotAvailable as e:
            logger.error(str(e))
            raise geni_ex.GENIv3AlreadyExistsError(str(e))

        except ons_ex.ONSException as e:
            logger.error(str(e))
            raise geni_ex.GENIv3GeneralError(str(e))

        logger.debug("Reserved=%s" % str(rs_))

        slivers_ = [self.__format_sliver_status(r, True, True, r.error) for r in rs_]
        slice_urn_ = self.lxml_to_string(self.__format_manifest_rspec(rs_))
        return (slice_urn_, slivers_)

    @enter_method_log
    def renew(self, urns, client_cert, credentials, expiration_time, best_effort):
        (slices_, slivers_) = self.__get_slices_slivers_from_urns(urns, client_cert, credentials, 'renewsliver', 'renewsliver')

        if len(slivers_):
            raise geni_ex.GENIv3OperationUnsupportedError('Only slice URNs can be given to this aggregate')

        rs_ = []
        try:
            logger.debug("Best=%s, Slices=%s, Slivers=%s" % (best_effort, slices_, slivers_,))
            if best_effort == False:
                # all included slivers to be renewed or none
                rs_ = self._resource_manager.renew_resources(slices=slices_,
                                                             end_time=expiration_time)
            else: # partial success if possible
                rs_ = self._resource_manager.force_renew_resources(slices=slices_,
                                                                   end_time=expiration_time)
        except ons_ex.ONSResourceNotFound as e:
            logger.error(str(e))
            raise geni_ex.GENIv3SearchFailedError(str(e))

        except ons_ex.ONSException as e:
            logger.error(str(e))
            raise geni_ex.GENIv3GeneralError(str(e))

        if (not len(rs_)) and (best_effort == False):
            raise geni_ex.GENIv3SearchFailedError("There are no resources in the given slice(s)")

        return [self.__format_sliver_status(r, True, True, r.error) for r in rs_]

    @enter_method_log
    def provision(self, urns, client_cert, credentials, best_effort, end_time, geni_users):
        raise geni_ex.GENIv3GeneralError("Method not implemented yet")

    @enter_method_log
    def perform_operational_action(self, urns, client_cert, credentials, action, best_effort):
        if action == 'geni_start':
            (slices_, slivers_) = self.__get_slices_slivers_from_urns(urns, client_cert, credentials, 'startslice', None)

        elif action == 'geni_stop':
            (slices_, slivers_) = self.__get_slices_slivers_from_urns(urns, client_cert, credentials, 'stopslice', None)
        else:
            raise geni_ex.GENIv3OperationUnsupportedError('Only geni_start|stop can be given to this aggregate')

        if len(slivers_):
            raise geni_ex.GENIv3OperationUnsupportedError('Only slice URNs can be given to this aggregate')

        rs_ = []
        try:
            logger.debug("Best=%s, Action=%s, Slices=%s, Slivers=%s" % (best_effort, action, slices_, slivers_,))
            if best_effort == False and action == 'geni_start':
                rs_ = self._resource_manager.start_slices(slices=slices_)

            elif best_effort == False and action == 'geni_stop':
                rs_ = self._resource_manager.stop_slices(slices=slices_)

            elif best_effort == True and action == 'geni_start':
                rs_ = self._resource_manager.force_start_slices(slices=slices_)

            elif best_effort == True and action == 'geni_stop':
                rs_ = self._resource_manager.force_stop_slices(slices=slices_)

        except ons_ex.ONSResourceNotFound as e:
            logger.error(str(e))
            raise geni_ex.GENIv3SearchFailedError(str(e))

        except ons_ex.ONSException as e:
            logger.error(str(e))
            raise geni_ex.GENIv3GeneralError(str(e))

        if (not len(rs_)) and (best_effort == False):
            raise geni_ex.GENIv3SearchFailedError("There are no resources in the given slice(s)")

        return [self.__format_sliver_status(r, True, True, r.error) for r in rs_]

    @enter_method_log
    def delete(self, urns, client_cert, credentials, best_effort):
        (slices_, slivers_) = self.__get_slices_slivers_from_urns(urns, client_cert, credentials, 'deleteslice', 'deletesliver')

        if len(slivers_):
            raise geni_ex.GENIv3OperationUnsupportedError('Only slice URNs can be given to this aggregate')

        rs_ = []
        try:
            logger.debug("Best=%s, Slices=%s, Slivers=%s" % (best_effort, slices_, slivers_,))
            if best_effort == False: # all included slivers to be removed or none
                rs_ = self._resource_manager.delete_slices(slices=slices_)

            else: # partial success if possible
                rs_ = self._resource_manager.force_delete_slices(slices=slices_)

        except ons_ex.ONSResourceNotFound as e:
            logger.error(str(e))
            raise geni_ex.GENIv3SearchFailedError(str(e))

        except ons_ex.ONSException as e:
            logger.error(str(e))
            raise geni_ex.GENIv3GeneralError(str(e))

        if (not len(rs_)) and (best_effort == False):
            raise geni_ex.GENIv3SearchFailedError("There are no resources in the given slice(s)")

        return [self.__format_sliver_status(r, True, True, r.error) for r in rs_]

    @enter_method_log
    def shutdown(self, slice_urn, client_cert, credentials):
        raise geni_ex.GENIv3GeneralError("Method not implemented yet")

    #private
    def __authenticate(self, client_cert, credentials, slice_urn=None, privileges=()):
        user_urn, user_uuid, user_mail = None, None, None
        if config.get('opennaas.check_credentials'):
            user_urn, user_uuid, user_mail = self.auth(client_cert, credentials, slice_urn, privileges)

        return user_urn, user_uuid, user_mail

    def __convert_allocation_2geni(self, state):
        if state == ons_models.ALLOCATION.FREE:
            return self.ALLOCATION_STATE_UNALLOCATED

        elif state == ons_models.ALLOCATION.ALLOCATED:
            return self.ALLOCATION_STATE_ALLOCATED

        elif state == ons_models.ALLOCATION.PROVISIONED:
            return self.ALLOCATION_STATE_PROVISIONED

        raise geni_ex.GENIv3BadArgsError("Unknown allocation state!")

    def __convert_operational_2geni(self, state):
        if state == ons_models.OPERATIONAL.READY:
            return self.OPERATIONAL_STATE_READY

        elif state == ons_models.OPERATIONAL.READY_BUSY:
            return self.OPERATIONAL_STATE_READY_BUSY

        raise geni_ex.GENIv3BadArgsError("Unknown operational state!")

    def __format_sliver_status(self, resource, allocation=False, operational=False, error=None):
        status_ = {'geni_sliver_urn': resource.urn,
                   'geni_expires'   : resource.end_time}

        if allocation:
            status_['geni_allocation_status'] = self.__convert_allocation_2geni(resource.allocation)

        if operational:
            status_['geni_operational_status'] = self.__convert_operational_2geni(resource.operational)

        if error:
            status_['geni_error'] = error

        return status_

    def __format_manifest_rspec(self, resources):
        manifest_ = self.lxml_manifest_root()
        em_ = self.lxml_manifest_element_maker(self.NAMESPACE_PREFIX)

        for resource in resources:
            r = em_.resource()
            r.append(em_.type(resource.type))
            r.append(em_.slice(resource.slice_urn))
            r.append(em_.name(resource.urn))
            r.append(em_.available('True' if resource.available() else 'False'))
            r.append(em_.end(str(resource.end_time)))

            if (resource.type == 'roadm') and 'roadm' in resource.details:
                r.append(em_.client(resource.details['roadm'].client))
                r.append(em_.client_mail(resource.details['roadm'].client_mail))
                r.append(em_.client_id(resource.details['roadm'].client_id))

                if resource.details['roadm'].connected_in_urn:
                    r.append(em_.to_ingress(resource.details['roadm'].connected_in_urn))

                if resource.details['roadm'].connected_out_urn:
                    r.append(em_.to_egress(resource.details['roadm'].connected_out_urn))

            manifest_.append(r)

        return manifest_

    def __get_slices_slivers_from_urns(self, urns, cert, credentials, slice_op, sliver_op):
        (slices_, slivers_) = ({}, {})
        for u_ in urns:
            urn_type_ = self.urn_type(u_)
            if urn_type_ == 'slice':
                slices_[u_] = self.__authenticate(cert, credentials, u_, (slice_op,))

            elif urn_type_ == 'sliver':
                slivers_[u_] = self.__authenticate(cert, credentials, u_, (sliver_op,))

            else:
                raise geni_ex.GENIv3OperationUnsupportedError('Bad URN type (%s)' % (urn_type_,))

        return (slices_, slivers_)

    def __verify_resource_tag(self, xml_elem):
        if not self.lxml_elm_has_request_prefix(xml_elem, 'opennaas'):
            raise geni_ex.GENIv3BadArgsError("Only `opennaas` RSpec prefix is supported!")

        if not self.lxml_elm_equals_request_tag(xml_elem, 'opennaas', 'resource'):
            raise geni_ex.GENIv3BadArgsError("Only `resource` RSpec tag is supported!")

    def __extract_gen_resource_info(self, xml_resource_descr):
        info_ = {}
        child_ = None
        for i in xml_resource_descr:
            if self.lxml_elm_equals_request_tag(i, 'opennaas', 'name'):
                info_['name'] = i.text.strip()

            if self.lxml_elm_equals_request_tag(i, 'opennaas', 'type'):
                info_['type'] = i.text.strip()

            if self.lxml_elm_equals_request_tag(i, 'opennaas', 'roadm'):
                child_ = i.getchildren()

        return (info_, child_)

    def __extract_roadm_conn_info(self, xml_connection_descr):
        info_ = {}
        for i in xml_connection_descr:
            if self.lxml_elm_equals_request_tag(i, 'opennaas', 'in_endpoint'):
                info_['in_endpoint'] = i.text.strip()

            if self.lxml_elm_equals_request_tag(i, 'opennaas', 'in_label'):
                info_['in_label'] = i.text.strip()

            if self.lxml_elm_equals_request_tag(i, 'opennaas', 'out_endpoint'):
                info_['out_endpoint'] = i.text.strip()

            if self.lxml_elm_equals_request_tag(i, 'opennaas', 'out_label'):
                info_['out_label'] = i.text.strip()

        return info_
