import amsoil.core.pluginmanager as pm
from datetime import datetime, timedelta

import sqlalchemy as sqla
from sqlalchemy import event
from sqlalchemy.orm import mapper, sessionmaker

"""
OpenNaas Data Models.
"""
config = pm.getService("config")
engine = sqla.create_engine('sqlite:///' + config.get("opennaas.db_dir") + '/opennaas.db',
                            echo=config.get("opennaas.db_dump_stat"))
meta = sqla.MetaData(bind=engine)

@event.listens_for(sqla.engine.Engine, 'connect')
def set_sqlite_pragma(dbapi_connection, connection_record):
    cursor = dbapi_connection.cursor()
    cursor.execute("PRAGMA foreign_keys=ON")
    cursor.close()


class ALLOCATION:
    FREE, ALLOCATED, PROVISIONED, AUDIT_TRANS = range(4)

class OPERATIONAL:
    FAILED = range(1)


resources = sqla.Table('Resources', meta,
                       sqla.Column('id', sqla.Integer, primary_key=True, autoincrement=True),
                       sqla.Column('name', sqla.String),
                       sqla.Column('type', sqla.String),
                       sqla.Column('audit_time', sqla.DateTime, default=datetime.utcnow),
                       sqla.UniqueConstraint('name', 'type'),
                      )

roadms = sqla.Table('Roadms', meta,
                    sqla.Column('id', sqla.Integer, primary_key=True, autoincrement=True),
                    sqla.Column('resource_id', sqla.Integer),
                    sqla.Column('endpoint', sqla.String),
                    sqla.Column('label', sqla.String),
                    sqla.Column('allocation', sqla.Integer, default=ALLOCATION.AUDIT_TRANS),
                    sqla.Column('audit_time', sqla.DateTime, default=datetime.utcnow),
                    sqla.UniqueConstraint('endpoint', 'label', 'resource_id'),
                    sqla.ForeignKeyConstraint(['resource_id'], ['Resources.id'],
                                              onupdate="CASCADE", ondelete="CASCADE"),
                   )

connections = sqla.Table('RoadmsConnections', meta,
                         sqla.Column('ingress', sqla.Integer, primary_key=True, unique=True),
                         sqla.Column('egress', sqla.Integer, primary_key=True, unique=True),
                         sqla.Column('xconn_id', sqla.String),
                         sqla.Column('slice_urn', sqla.String),
                         sqla.Column('end_time', sqla.DateTime, default=datetime.utcnow),
                         sqla.Column('client_name', sqla.String, default=''),
                         sqla.Column('client_id', sqla.String, default=''),
                         sqla.Column('client_email', sqla.String, default=''),
                         sqla.Column('audit_time', sqla.DateTime, default=datetime.utcnow),
                         sqla.ForeignKeyConstraint(['ingress'], ['Roadms.id'],
                                                   onupdate="CASCADE", ondelete="CASCADE"),
                         sqla.ForeignKeyConstraint(['egress'], ['Roadms.id'],
                                                   onupdate="CASCADE", ondelete="CASCADE"),
                        )


class Resources(object):
    def __init__(self, rname, rtype):
        self.name = rname
        self.type = rtype

    def __repr__(self):
        return "(id=%d, name=%s, type=%s, audit=%s)" %\
               (self.id, self.name, self.type, str(self.audit_time),)

class Roadms(object):
    def __init__(self, rid, rep, rlabel):
        self.resource_id = rid
        self.endpoint = rep
        self.label = rlabel

    def __repr__(self):
        return "(id=%d, resource-id=%d, endpoint=%s, label=%s, alloc=%d, audit=%s)" %\
               (self.id, self.resource_id, self.endpoint, self.label, self.allocation,
                str(self.audit_time),)

class RoadmsConns(object):
    def __init__(self, ingress, egress, slice_urn):
        self.ingress = ingress
        self.egress = egress
        self.slice_urn = slice_urn

    def __repr__(self):
        return "(in=%d, out=%d, xid=%s, slice=%s, end=%s, cname=%s, cid=%s, cmail=%s, audit=%s)" %\
               (self.ingress, self.egress, self.xconn_id, self.slice_urn, str(self.end_time),
                self.client_name, self.client_id, self.client_email, str(self.audit_time))


mapper(Resources, resources)
mapper(Roadms, roadms)
mapper(RoadmsConns, connections)

def create_xconn_id(src_ep, src_label, dst_ep, dst_label):
    return src_ep + ':' + src_label + '::' + dst_ep + ':' + dst_label


class RoadmsDBM(object):
    ons_ex = pm.getService('opennaas_exceptions')

    def __init__(self):
        self.__s = None

    def create_all(self):
        try:
            meta.create_all()
            return (True, None)

        except sqla.exc.SQLAlchemyError as e:
            return (False, str(e))

    def open_session(self):
        if self.__s:
            raise self.ons_ex.ONSException('Session already opened!')

        self.__s = sessionmaker(bind=engine)()

    def close_session(self):
        if self.__s:
            self.__s.close()

        self.__s = None

    def check_to_reserve(self, resource, roadm):
        try:
            re_ = self.__s.query(Resources).filter(sqla.and_(Resources.name == resource['name'],
                                                             Resources.type == resource['type'])).one()

            rin_ = self.__s.query(Roadms).filter(sqla.and_(Roadms.resource_id == re_.id,
                                                           Roadms.endpoint == roadm['in_endpoint'],
                                                           Roadms.label == roadm['in_label'])).one()
            if rin_.allocation != ALLOCATION.FREE:
                raise self.ons_ex.ONSResourceNotAvailable('Ingress not available (id=%s, ep=%s, lab=%s)' %\
                                                          (re_.id, roadm['in_endpoint'], roadm['in_label'],))

            rout_ = self.__s.query(Roadms).filter(sqla.and_(Roadms.resource_id == re_.id,
                                                            Roadms.endpoint == roadm['out_endpoint'],
                                                            Roadms.label == roadm['out_label'])).one()
            if rout_.allocation != ALLOCATION.FREE:
                raise self.ons_ex.ONSResourceNotAvailable('Egress not available (id=%s, ep=%s, lab=%s)' %\
                                                          (re_.id, roadm['out_endpoint'], roadm['out_label'],))

            return (rin_.id, rout_.id, create_xconn_id(rin_.endpoint, rin_.label, rout_.endpoint, rout_.label))

        except sqla.exc.SQLAlchemyError as e:
            raise self.ons_ex.ONSResourceNotFound(str(e))

    def make_connection(self, ingress, egress, conn_id, values):
        try:
            stmt_ = connections.insert().values(ingress=ingress, egress=egress, xconn_id=conn_id,
                                                slice_urn=values['slice_name'], end_time=values['end_time'],
                                                client_name=values['client_name'], client_id=values['client_id'],
                                                client_email=values['client_email'])
            self.__s.execute(stmt_)

            stmt_ = roadms.update().where(roadms.c.id==ingress).values(allocation=ALLOCATION.ALLOCATED)
            self.__s.execute(stmt_)

            stmt_ = roadms.update().where(roadms.c.id==egress).values(allocation=ALLOCATION.ALLOCATED)
            self.__s.execute(stmt_)

            self.__s.commit()

        except sqla.exc.SQLAlchemyError as e:
            self.__s.rollback()
            raise self.ons_ex.ONSException(str(e))

    def get_resources(self):
        try:
            rall_ = self.__s.query(Resources.name, Resources.type, Roadms.endpoint,
                                   Roadms.label, Roadms.allocation, Roadms.id).\
                             join(Roadms, Resources.id==Roadms.resource_id).all()
            ret_ = []
            for r_ in rall_:
                if r_.allocation == ALLOCATION.ALLOCATED:
                    conn_ = self.__s.query(RoadmsConns).filter(sqla.or_(RoadmsConns.ingress == r_.id,
                                                                        RoadmsConns.egress == r_.id)).one()

                    ret_.append((r_.name, r_.endpoint, r_.label, conn_.slice_urn, conn_.end_time,\
                                 r_.type, r_.allocation))
                else:
                    ret_.append((r_.name, r_.endpoint, r_.label, None, None,\
                                 r_.type, r_.allocation))

            return ret_

        except sqla.exc.SQLAlchemyError as e:
            raise self.ons_ex.ONSException(str(e))

    def get_slice(self, slice_urn):
        try:
            rall_ = self.__s.query(RoadmsConns).filter(RoadmsConns.slice_urn == slice_urn).all()
            ret_ = []
            for r_ in rall_:
                r_in_ = self.__s.query(Roadms.endpoint, Roadms.label, Roadms.allocation,
                                       Resources.name, Resources.type).\
                                 join(Resources, Roadms.resource_id==Resources.id).\
                                 filter(Roadms.id == r_.ingress).one()

                r_out_ = self.__s.query(Roadms.endpoint, Roadms.label, Roadms.allocation,
                                        Resources.name, Resources.type).\
                                  join(Resources, Roadms.resource_id==Resources.id).\
                                  filter(Roadms.id == r_.egress).one()

                ret_.append((r_in_, r_out_, r_))

            return ret_

        except sqla.exc.SQLAlchemyError as e:
            raise self.ons_ex.ONSException(str(e))

    def renew_slice(self, slice_urn, end_time, client_info):
        try:
            client, client_id, client_mail = client_info
            stmt_ = connections.update().where(connections.c.slice_urn==slice_urn).\
                        values(end_time=end_time, client_name=client, client_id=client_id,
                               client_email=client_mail)
            self.__s.execute(stmt_)

            self.__s.commit()

        except sqla.exc.SQLAlchemyError as e:
            self.__s.rollback()
            raise self.ons_ex.ONSException(str(e))

    # audit procedures
    def __audit_resource(self, rtype, rname):
        try:
            stmt_ = resources.insert().values(name=rname, type=rtype)
            self.__s.execute(stmt_)

        except sqla.exc.SQLAlchemyError:
            stmt_ = resources.update().where(sqla.and_(resources.c.name==rname,
                                                       resources.c.type==rtype)).\
                        values(audit_time=datetime.utcnow())
            self.__s.execute(stmt_)

    def __audit_roadm(self, rid, ep, label):
        try:
            stmt_ = roadms.insert().values(resource_id=rid,endpoint=ep,label=label)
            self.__s.execute(stmt_)

        except sqla.exc.SQLAlchemyError:
            stmt_ = roadms.update().where(sqla.and_(roadms.c.resource_id==rid,
                                                    roadms.c.endpoint==ep,
                                                    roadms.c.label==label)).\
                        values(audit_time=datetime.utcnow())
            self.__s.execute(stmt_)

    def __audit_connection(self, ingr, egr, xid):
        try:
            stmt_ = connections.insert().values(ingress=ingr,egress=egr,xconn_id=xid)
            self.__s.execute(stmt_)

        except sqla.exc.SQLAlchemyError:
            stmt_ = connections.update().where(sqla.and_(connections.c.ingress==ingr,
                                                         connections.c.egress==egr)).\
                        values(audit_time=datetime.utcnow())
            self.__s.execute(stmt_)

        stmt_ = roadms.update().where(sqla.or_(roadms.c.id==ingr,
                                               roadms.c.id==egr)).\
                    values(allocation=ALLOCATION.ALLOCATED)
        self.__s.execute(stmt_)

    def audit_resources(self, info):
        try:
            for (rtype, rname) in info:
                self.__audit_resource(rtype, rname)

            self.__s.commit()

        except sqla.exc.SQLAlchemyError as e:
            self.__s.rollback()
            raise self.ons_ex.ONSException(str(e))

    def audit_roadms(self, info):
        try:
            r_id, r_type, r_name = (None, None, None)
            for (rtype, rname, ep, label) in info:
                if rtype != r_type or rname != r_name:
                    r_id, r_type, r_name = self.__s.query(Resources.id, Resources.type, Resources.name).\
                                                filter(sqla.and_(Resources.name==rname,
                                                                 Resources.type==rtype)).one()

                self.__audit_roadm(r_id, ep, label)

            self.__s.commit()

        except sqla.exc.SQLAlchemyError as e:
            self.__s.rollback()
            raise self.ons_ex.ONSException(str(e))

    def audit_connections(self, info):
        try:
            r_id, r_type, r_name = (None, None, None)
            for (rtype, rname, xconn) in info:
                x_id, xsrc_ep, xsrc_label, xdst_ep, xdst_label = xconn
                if rtype != r_type or rname != r_name:
                    r_id, r_type, r_name = self.__s.query(Resources.id, Resources.type, Resources.name).\
                                                filter(sqla.and_(Resources.name==rname,
                                                                 Resources.type==rtype)).one()

                rin = self.__s.query(Roadms.id).filter(sqla.and_(Roadms.resource_id==r_id,
                                                                 Roadms.endpoint==xsrc_ep,
                                                                 Roadms.label==xsrc_label)).one()
                rout = self.__s.query(Roadms.id).filter(sqla.and_(Roadms.resource_id==r_id,
                                                                  Roadms.endpoint==xdst_ep,
                                                                  Roadms.label==xdst_label)).one()
                self.__audit_connection(rin.id, rout.id, x_id)

            self.__s.commit()

        except sqla.exc.SQLAlchemyError as e:
            self.__s.rollback()
            raise self.ons_ex.ONSException(str(e))

    def audit_terminated(self):
        try:
            old_time = datetime.utcnow() - timedelta(days=1)

            stmt_ = resources.delete(resources.c.audit_time < old_time)
            self.__s.execute(stmt_)

            stmt_ = roadms.delete(roadms.c.audit_time < old_time)
            self.__s.execute(stmt_)

            stmt_ = connections.delete(connections.c.audit_time < old_time)
            self.__s.execute(stmt_)

            stmt_ = roadms.update().where(roadms.c.allocation==ALLOCATION.AUDIT_TRANS).\
                        values(allocation=ALLOCATION.FREE)
            self.__s.execute(stmt_)

            self.__s.commit()

        except sqla.exc.SQLAlchemyError as e:
            self.__s.rollback()
            raise self.ons_ex.ONSException(str(e))


roadmsDBM = RoadmsDBM()


def create_roadm_urn(name, endpoint, label):
    return name + ':' + endpoint + ':' + label

def decode_roadm_urn(resource_urn):
    ret_ = resource_urn.split(':', 3)
    return (ret_[0], ret_[1], ret_[2])


class GeniRoadmDetails(object):
    def __init__(self, client, client_id, client_mail,
                 connected_in_urn=None, connected_out_urn=None):
        self.client = client
        self.client_id = client_id
        self.client_mail = client_mail if client_mail else 'empty'
        self.connected_in_urn = connected_in_urn
        self.connected_out_urn = connected_out_urn

    def __repr__(self):
        return "(client=%s, c_id=%s, c_mail=%s, conn_in_urn=%s, conn_out_urn=%s)" %\
               (self.client, self.client_id, self.client_mail,
                self.connected_in_urn, self.connected_out_urn)

class GeniResource(object):
    def __init__(self, urn, slice_urn, end_time, type_, allocation):
        self.urn = urn
        self.end_time = end_time
        self.slice_urn = slice_urn
        self.type = type_
        self.allocation = allocation
        self.details = {}

    def __repr__(self):
        return "(urn=%s, end-time=%s, slice=%s, type=%s, alloc=%s, details=%s)" %\
               (self.urn, str(self.end_time), self.slice_urn, self.type,
                self.allocation, self.details)

    def available(self):
        return True if self.allocation == ALLOCATION.FREE else False

    def roadm_details(self, c_name, c_id, c_mail, connected_in_urn=None, connected_out_urn=None):
        self.details['roadm'] = GeniRoadmDetails(c_name, c_id, c_mail,
                                                 connected_in_urn, connected_out_urn)
