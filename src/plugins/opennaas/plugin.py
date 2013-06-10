import amsoil.core.pluginmanager as pm

"""
OpenNaas plugin.

"""

def setup():
    # setup config keys
    config = pm.getService("config")
    config.install("opennaas.db_dir", "/tmp", "(Sqlite) database directory")
    config.install("opennaas.db_dump_stat", True, "(Sqlite) database dump statements")
    config.install("opennaas.reservation_timeout", 5, "Default reservation timeout")

    # resource manager
    import resourceexceptions as ons_exceptions_package
    pm.registerService('opennaas_exceptions', ons_exceptions_package)
    import models as ons_models_package
    pm.registerService('opennaas_models', ons_models_package)
    from resourcemanager import RMRoadmManager
    pm.registerService('opennaas_resourcemanager', RMRoadmManager())

    # delegate
    from geni3delegate import OpenNaasGENI3Delegate
    handler = pm.getService('geniv3handler')
    handler.setDelegate(OpenNaasGENI3Delegate())
