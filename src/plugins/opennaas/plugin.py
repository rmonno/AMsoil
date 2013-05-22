import amsoil.core.pluginmanager as pm

"""
OpenNaas plugin.

"""

def setup():
    # setup config keys
    config = pm.getService("config")
    config.install("opennaas.auth", False, "Enable/Disable client authentication phase")

    # resource manager
    import opennaas_exceptions as ons_exceptions_package
    pm.registerService('opennaas_exceptions', ons_exceptions_package)
    import opennaas_models as ons_models_package
    pm.registerService('opennaas_models', ons_models_package)
    from opennaas_resourcemanager import OpenNaasResourceManager
    pm.registerService('opennaas_resourcemanager', OpenNaasResourceManager())

    # delegate
    from opennaas_geni3delegate import OpenNaasGENI3Delegate
    handler = pm.getService('geniv3handler')
    handler.setDelegate(OpenNaasGENI3Delegate())
