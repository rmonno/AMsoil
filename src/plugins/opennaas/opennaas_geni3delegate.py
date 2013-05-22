import amsoil.core.pluginmanager as pm
import amsoil.core.log
logger=amsoil.core.log.getLogger('ons_geniv3delegate')

GENIv3DelegateBase = pm.getService('geniv3delegatebase')
geni_ex = pm.getService('geniv3exceptions')
ons_ex = pm.getService('opennaas_exceptions')

"""
GENI delegate.
"""

class OpenNaasGENI3Delegate(GENIv3DelegateBase):
    """ OpenNaas GENI version 3 delegate. """

    URN_PREFIX = 'urn:OPENNAAS_AM'

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
        return {}

    @enter_method_log
    def get_manifest_extensions_mapping(self):
        """Should return a dict of namespace names and manifest extensions (XSD schema's URLs as string).
        Format: {xml_namespace_prefix : namespace_uri, ...}
        """
        return {}

    @enter_method_log
    def get_ad_extensions_mapping(self):
        """Should return a dict of namespace names and advertisement extensions (XSD schema URLs as string) to be sent back by GetVersion.
        Format: {xml_namespace_prefix : namespace_uri, ...}
        """
        return {}

    @enter_method_log
    def is_single_allocation(self):
        """Shall return a True or False. When True (not default), and performing one of (Describe, Allocate, Renew, Provision, Delete), such an AM requires you to include either the slice urn or the urn of all the slivers in the same state.
        see http://groups.geni.net/geni/wiki/GAPI_AM_API_V3/CommonConcepts#OperationsonIndividualSlivers
        """
        return True

    @enter_method_log
    def get_allocation_mode(self):
        """Shall return a either 'geni_single', 'geni_disjoint', 'geni_many'.
        It defines whether this AM allows adding slivers to slices at an AM (i.e. calling Allocate multiple times, without first deleting the allocated slivers).
        For description of the options see http://groups.geni.net/geni/wiki/GAPI_AM_API_V3/CommonConcepts#OperationsonIndividualSlivers"""
        return 'geni_many'

    @enter_method_log
    def list_resources(self, client_cert, credentials, geni_available):
        """Shall return an RSpec version 3 (advertisement) or raise an GENIv3...Error.
        If {geni_available} is set, only return availabe resources.
        For full description see http://groups.geni.net/geni/wiki/GAPI_AM_API_V3#ListResources
        """
        raise geni_ex.GENIv3GeneralError("Method not implemented yet")

    @enter_method_log
    def describe(self, urns, client_cert, credentials):
        """Shall return an RSpec version 3 (manifest) or raise an GENIv3...Error.
        {urns} contains a list of slice identifiers (e.g. ['urn:publicid:IDN+ofelia:eict:gcf+slice+myslice']).
        For more information on possible {urns} see http://groups.geni.net/geni/wiki/GAPI_AM_API_V3/CommonConcepts#urns
        For full description see http://groups.geni.net/geni/wiki/GAPI_AM_API_V3#Describe
        """
        raise geni_ex.GENIv3GeneralError("Method not implemented yet")

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
        {end_time} Optional. A python datetime object which determines the desired expiry date of this allocation (see http://groups.geni.net/geni/wiki/GAPI_AM_API_V3/CommonConcepts#geni_end_time).
        This is the first part of what CreateSliver used to do in previous versions of the AM API. The second part is now done by Provision, and the final part is done by PerformOperationalAction.

        For full description see http://groups.geni.net/geni/wiki/GAPI_AM_API_V3#Allocate
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
        For more information on possible {urns} see http://groups.geni.net/geni/wiki/GAPI_AM_API_V3/CommonConcepts#urns

        For full description see http://groups.geni.net/geni/wiki/GAPI_AM_API_V3#Status
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
