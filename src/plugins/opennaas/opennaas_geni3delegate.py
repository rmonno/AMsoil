import amsoil.core.pluginmanager as pm
import amsoil.core.log
logger=amsoil.core.log.getLogger('ons_geniv3delegate')

GENIv3DelegateBase = pm.getService('geniv3delegatebase')
geni_ex = pm.getService('geniv3exceptions')
ons_ex = pm.getService('opennaas_exceptions')
ons_models = pm.getService('opennaas_models')

"""
GENI delegate.
"""

class OpenNaasGENI3Delegate(GENIv3DelegateBase):
    """ OpenNaas GENI version 3 delegate. """

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

    @enter_method_log
    def get_request_extensions_mapping(self):
        """Should return a dict of namespace names and request extensions (XSD schema's URLs as string).
        Format: {xml_namespace_prefix : namespace_uri, ...}
        """
        return {OpenNaasGENI3Delegate.NAMESPACE_PREFIX: OpenNaasGENI3Delegate.NAMESPACE_URI}

    @enter_method_log
    def get_manifest_extensions_mapping(self):
        """Should return a dict of namespace names and manifest extensions (XSD schema's URLs as string).
        Format: {xml_namespace_prefix : namespace_uri, ...}
        """
        return {OpenNaasGENI3Delegate.NAMESPACE_PREFIX: OpenNaasGENI3Delegate.NAMESPACE_URI}

    @enter_method_log
    def get_ad_extensions_mapping(self):
        """Should return a dict of namespace names and advertisement extensions (XSD schema URLs as string) to be sent back by GetVersion.
        Format: {xml_namespace_prefix : namespace_uri, ...}
        """
        return {OpenNaasGENI3Delegate.NAMESPACE_PREFIX: OpenNaasGENI3Delegate.NAMESPACE_URI}

    @enter_method_log
    def is_single_allocation(self):
        """Shall return a True or False. When True (not default), and performing one of (Describe, Allocate, Renew, Provision, Delete),
        such an AM requires you to include either the slice urn or the urn of all the slivers in the same state.
        """
        return True

    @enter_method_log
    def get_allocation_mode(self):
        """Shall return a either 'geni_single', 'geni_disjoint', 'geni_many'.
        It defines whether this AM allows adding slivers to slices at an AM (i.e. calling Allocate multiple times,
        without first deleting the allocated slivers).
        """
        return 'geni_many'

    @enter_method_log
    def list_resources(self, client_cert, credentials, geni_available):
        """Shall return an RSpec version 3 (advertisement) or raise an GENIv3...Error.
        If {geni_available} is set, only return availabe resources.
        """
        self.__authenticate(client_cert, credentials, None, ('listslices',))

        rn_ = self.lxml_ad_root()
        em_ = self.lxml_ad_element_maker(OpenNaasGENI3Delegate.NAMESPACE_PREFIX)

        for r in self._resource_manager.get_resources():
            if geni_available and not r.available(): continue

            res_ = em_.resource()
            res_.append(em_.type(r.human_type()))
            res_.append(em_.name(r.name))
            res_.append(em_.available('True' if r.available() else 'False'))

            rn_.append(res_)

        return self.lxml_to_string(rn_)

    def describe(self, urns, client_cert, credentials):
        """Shall return an RSpec version 3 (manifest) or raise an GENIv3...Error.
           {urns} contains a list of slice identifiers (e.g. ['urn:publicid:IDN+ofelia:eict:gcf+slice+myslice']).
        """
        return self.status(urns, client_cert, credentials)[0]

    @enter_method_log
    def status(self, urns, client_cert, credentials):
        """Shall return the two following values or raise an GENIv3...Error.
        - a slice urn
        - a list of slivers of the format:
            [{'geni_sliver_urn'         : String,
              'geni_allocation_status'  : one of the ALLOCATION_STATE_xxx,
              'geni_operational_status' : one of the OPERATIONAL_STATE_xxx,
              'geni_expires'            : Python-Date,
              'geni_error'              : optional String},
            ...]
        Please return like so: "return slice_urn, slivers"
        {urns} contains a list of slice/resource identifiers (e.g. ['urn:publicid:IDN+ofelia:eict:gcf+slice+myslice']).
        """
        rs_ = []
        for u_ in urns:
            self.__authenticate(client_cert, credentials, u_, ('sliverstatus',))
            if self.urn_type(u_) == 'slice':
                rs_slice_ = self._resource_manager.get_resources(slice_name=u_)
                rs_.extend(rs_slice_)

            elif self.urn_type(u_) == 'sliver':
                r_ = self._resource_manager.get_resources(resource_id=u_)
                rs_.append(r_)

            else:
                raise geni_ex.GENIv3OperationUnsupportedError('Only slice or sliver URNs can be given to status in this aggregate')

        if not len(rs_):
            raise geni_ex.GENIv3SearchFailedError("There are no resources in the given slice(s)")

        slivers_ = [self.__format_sliver_status(r, True, True) for r in rs_]
        slice_urn_ = self.lxml_to_string(self.__format_manifest_rspec(rs_))
        return (slice_urn_, slivers_)

    @enter_method_log
    def allocate(self, slice_urn, client_cert, credentials, rspec, end_time=None):
        """Shall return the two following values or raise an GENIv3...Error.
        - a RSpec version 3 (manifest) of newly allocated slivers
        - a list of slivers of the format:
            [{'geni_sliver_urn' : String,
              'geni_expires'    : Python-Date,
              'geni_allocation_status' : one of the ALLOCATION_STATE_xxx},
            ...]
        Please return like so: "return respecs, slivers"
        {slice_urn} contains a slice identifier (e.g. 'urn:publicid:IDN+ofelia:eict:gcf+slice+myslice').
        {end_time} Optional. A python datetime object which determines the desired expiry date of this allocation
        """
        raise geni_ex.GENIv3GeneralError("Method not implemented yet")

    @enter_method_log
    def renew(self, urns, client_cert, credentials, expiration_time, best_effort):
        """Shall return a list of slivers of the following format or raise an GENIv3...Error:
        [{'geni_sliver_urn'         : String,
          'geni_allocation_status'  : one of the ALLOCATION_STATE_xxx,
          'geni_operational_status' : one of the OPERATIONAL_STATE_xxx,
          'geni_expires'            : Python-Date,
          'geni_error'              : optional String}, 
        ...]

        {urns} contains a list of slice identifiers (e.g. ['urn:publicid:IDN+ofelia:eict:gcf+slice+myslice']).
        {expiration_time} is a python datetime object
        {best_effort} determines if the method shall fail in case that not all of the urns can be renewed (best_effort=False).

        If the transactional behaviour of {best_effort}=False can not be provided, throw a GENIv3OperationUnsupportedError.
        For more information on possible {urns} see http://groups.geni.net/geni/wiki/GAPI_AM_API_V3/CommonConcepts#urns

        For full description see http://groups.geni.net/geni/wiki/GAPI_AM_API_V3#Renew
        """
        raise geni_ex.GENIv3GeneralError("Method not implemented yet")

    @enter_method_log
    def provision(self, urns, client_cert, credentials, best_effort, end_time, geni_users):
        """Shall return the two following values or raise an GENIv3...Error.
        - a RSpec version 3 (manifest) of slivers 
        - a list of slivers of the format:
            [{'geni_sliver_urn'         : String,
              'geni_allocation_status'  : one of the ALLOCATION_STATE_xxx,
              'geni_operational_status' : one of the OPERATIONAL_STATE_xxx,
              'geni_expires'            : Python-Date,
              'geni_error'              : optional String}, 
            ...]
        Please return like so: "return respecs, slivers"

        {urns} contains a list of slice/resource identifiers (e.g. ['urn:publicid:IDN+ofelia:eict:gcf+slice+myslice']).
        {best_effort} determines if the method shall fail in case that not all of the urns can be provisioned (best_effort=False)
        {end_time} Optional. A python datetime object which determines the desired expiry date of this provision (see http://groups.geni.net/geni/wiki/GAPI_AM_API_V3/CommonConcepts#geni_end_time).
        {geni_users} is a list of the format: [ { 'urn' : ..., 'keys' : [sshkey, ...]}, ...]

        If the transactional behaviour of {best_effort}=False can not be provided, throw a GENIv3OperationUnsupportedError.
        For more information on possible {urns} see http://groups.geni.net/geni/wiki/GAPI_AM_API_V3/CommonConcepts#urns

        For full description see http://groups.geni.net/geni/wiki/GAPI_AM_API_V3#Provision
        """
        raise geni_ex.GENIv3GeneralError("Method not implemented yet")

    @enter_method_log
    def perform_operational_action(self, urns, client_cert, credentials, action, best_effort):
        """Shall return a list of slivers of the following format or raise an GENIv3...Error:
        [{'geni_sliver_urn'         : String,
          'geni_allocation_status'  : one of the ALLOCATION_STATE_xxx,
          'geni_operational_status' : one of the OPERATIONAL_STATE_xxx,
          'geni_expires'            : Python-Date,
          'geni_error'              : optional String}, 
        ...]

        {urns} contains a list of slice or sliver identifiers (e.g. ['urn:publicid:IDN+ofelia:eict:gcf+slice+myslice']).
        {action} an arbitraty string, but the following should be possible: "geni_start", "geni_stop", "geni_restart"
        {best_effort} determines if the method shall fail in case that not all of the urns can be changed (best_effort=False)

        If the transactional behaviour of {best_effort}=False can not be provided, throw a GENIv3OperationUnsupportedError.
        For more information on possible {urns} see http://groups.geni.net/geni/wiki/GAPI_AM_API_V3/CommonConcepts#urns

        For full description see http://groups.geni.net/geni/wiki/GAPI_AM_API_V3#PerformOperationalAction
        """
        raise geni_ex.GENIv3GeneralError("Method not implemented yet")

    @enter_method_log
    def delete(self, urns, client_cert, credentials, best_effort):
        """Shall return a list of slivers of the following format or raise an GENIv3...Error:
        [{'geni_sliver_urn'         : String,
          'geni_allocation_status'  : one of the ALLOCATION_STATE_xxx,
          'geni_expires'            : Python-Date,
          'geni_error'              : optional String}, 
        ...]

        {urns} contains a list of slice/resource identifiers (e.g. ['urn:publicid:IDN+ofelia:eict:gcf+slice+myslice']).
        {best_effort} determines if the method shall fail in case that not all of the urns can be deleted (best_effort=False)

        If the transactional behaviour of {best_effort}=False can not be provided, throw a GENIv3OperationUnsupportedError.
        For more information on possible {urns} see http://groups.geni.net/geni/wiki/GAPI_AM_API_V3/CommonConcepts#urns

        For full description see http://groups.geni.net/geni/wiki/GAPI_AM_API_V3#Delete
        """
        raise geni_ex.GENIv3GeneralError("Method not implemented yet")

    @enter_method_log
    def shutdown(self, slice_urn, client_cert, credentials):
        """Shall return True or False or raise an GENIv3...Error.

        For full description see http://groups.geni.net/geni/wiki/GAPI_AM_API_V3#Shutdown
        """
        raise geni_ex.GENIv3GeneralError("Method not implemented yet")

    #private
    def __authenticate(self, client_cert, credentials, slice_urn=None, privileges=()):
        return self.auth(client_cert, credentials, slice_urn, privileges)

    def __convert_allocation_2geni(self, state):
        if state == ons_models.ALLOCATION.FREE:
            return self.ALLOCATION_STATE_UNALLOCATED

        elif state == ons_models.ALLOCATION.ALLOCATED:
            return self.ALLOCATION_STATE_ALLOCATED

        elif state == ons_models.ALLOCATION.PROVISIONED:
            return self.ALLOCATION_STATE_PROVISIONED

        raise geni_ex.GENIv3GeneralError("Unknown allocation state!")

    def __convert_operational_2geni(self, state):
        if state == ons_models.OPERATIONAL.FAILED:
            return self.OPERATIONAL_STATE_FAILED

        raise geni_ex.GENIv3GeneralError("Unknown operational state!")

    def __format_sliver_status(self, resource, allocation=False, operational=False, error=None):
        status_ = {'geni_sliver_urn' : resource.id(),
                   'geni_expires'    : resource.expire()}

        if allocation:
            status_['geni_allocation_status'] = self.__convert_allocation_2geni(resource.allocation_state)

        if operational:
            status_['geni_operational_status'] = self.__convert_operational_2geni(resource.operational_state)

        if (error):
            status_['geni_error'] = error

        return status_

    def __format_manifest_rspec(self, resources):
        manifest_ = self.lxml_manifest_root()
        em_ = self.lxml_manifest_element_maker(OpenNaasGENI3Delegate.NAMESPACE_PREFIX)

        for resource in resources:
            r = em_.resource()
            r.append(em_.type(resource.human_type()))
            r.append(em_.name(resource.name))
            r.append(em_.identifier(resource.id()))
            r.append(em_.available('True' if resource.available() else 'False'))

            if resource.type() == ons_models.Resource.ROADM_RESOURCE:
                r.append(em_.special(resource.special))

            manifest_.append(r)

        return manifest_
