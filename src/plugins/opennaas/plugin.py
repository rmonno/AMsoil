import amsoil.core.pluginmanager as pm

"""
OpenNaas plugin.

"""

def setup():
    # setup config keys
    config = pm.getService("config")
    config.install("opennaas.db_dir", "/opt/amsoil", "(Sqlite) database directory")
    config.install("opennaas.db_dump_stat", False, "(Sqlite) database dump statements")
    config.install("opennaas.reservation_timeout", 600, "Reservation timeout (minutes)")
    config.install("opennaas.server_address", "localhost", "OpenNaas server address")
    config.install("opennaas.server_port", 8888, "OpenNaas server port")
    config.install("opennaas.user", "admin", "OpenNaas user")
    config.install("opennaas.password", "123456", "OpenNaas password")
    config.install("opennaas.update_timeout", 5, "Update resources timeout (secs)")
    config.install("opennaas.update_step", 100, "Update resources step")
    config.install("opennaas.check_expire_timeout", 60, "Check resources expiration timeout (secs)")
    config.install("opennaas.check_credentials", False, "Check credentials for incoming requests")

    # resource manager
    import resourceexceptions as ons_exceptions_package
    pm.registerService('opennaas_exceptions', ons_exceptions_package)
    import models as ons_models_package
    pm.registerService('opennaas_models', ons_models_package)
    import commandsmanager as ons_commands_mngr_package
    pm.registerService('opennaas_commands', ons_commands_mngr_package)
    import fsmmanager as ons_fsm_mngr_package
    pm.registerService('opennaas_fsm', ons_fsm_mngr_package)
    from resourcemanager import RMRoadmManager
    pm.registerService('opennaas_resourcemanager', RMRoadmManager())

    # delegate
    from geni3delegate import OpenNaasGENI3Delegate
    handler = pm.getService('geniv3handler')
    handler.setDelegate(OpenNaasGENI3Delegate())
