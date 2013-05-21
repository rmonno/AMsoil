import amsoil.core.pluginmanager as pm

"""
OpenNaas plugin.

"""

def setup():
    # setup config keys
    config = pm.getService("config")
    config.install("opennass.test_config_value", 123, "Only a test configuration value")

    # resource manager
    from opennaas_resourcemanager import OpenNaasResourceManager
    import opennaas_exceptions as ons_exceptions_package
    pm.registerService('opennaas_resourcemanager', OpenNaasResourceManager())
    pm.registerService('opennaas_exceptions', ons_exceptions_package)

    # delegate
    from opennaas_geni3delegate import OpenNaasGENI3Delegate
    handler = pm.getService('geniv3handler')
    handler.setDelegate(OpenNaasGENI3Delegate())
