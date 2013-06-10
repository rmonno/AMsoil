import amsoil.core.pluginmanager as pm
from datetime import datetime

import sqlalchemy as sqla
from sqlalchemy.orm import mapper

"""
OpenNaas Data Models.
"""
config = pm.getService("config")
engine = sqla.create_engine('sqlite:///' + config.get("opennaas.db_dir") + '/opennaas.db',
                            echo=config.get("opennaas.db_dump_stat"))
meta = sqla.MetaData(bind=engine)


class ALLOCATION:
    FREE, ALLOCATED, PROVISIONED = range(3)

class OPERATIONAL:
    FAILED = range(1)

roadm = sqla.Table('roadm', meta,
                   sqla.Column('slice_name', sqla.String, primary_key=True),
                   sqla.Column('resource_name', sqla.String, primary_key=True),
                   sqla.Column('allocation', sqla.Integer, default=ALLOCATION.FREE),
                   sqla.Column('modified_time', sqla.DateTime, default=datetime.now),
                   sqla.Column('end_time', sqla.DateTime, default=datetime.now),
                   sqla.Column('client_name', sqla.String, default=''),
                   sqla.Column('client_id', sqla.String, default=''),
                   sqla.Column('client_email', sqla.String, default=''),
                  )

class Roadm(object):
    def __init__(self, sname, rname):
        self.slice_name = sname
        self.resource_name = rname

    def __repr__(self):
        return "Roadm: [%s, %s, %d, %s, %s, %s, %s, %s]" %\
               (self.slice_name, self.resource_name, self.allocation,
                str(self.modified_time), str(self.end_time),
                self.client_name, self.client_id, self.client_email,)

    def available(self):
        return (True if (self.allocation == ALLOCATION.FREE) else False)

    def type(self):
        return 'roadm'

    def state(self):
        if self.allocation == ALLOCATION.FREE:
            return 'free'
        elif self.allocation == ALLOCATION.ALLOCATED:
            return 'allocated'
        else:
            return 'provisioned'

mapper(Roadm, roadm)
