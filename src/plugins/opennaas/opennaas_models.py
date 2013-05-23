from datetime import datetime
"""
OpenNaas Data Models.
"""

class ALLOCATION:
    FREE, ALLOCATED, PROVISIONED = range(3)


class OPERATIONAL:
    FAILED = range(1)


class Resource(object):
    RESOURCE = 0
    ROADM_RESOURCE = 1

    def __init__(self, name, allocation_state=ALLOCATION.FREE, operational_state=OPERATIONAL.FAILED):
        super(Resource, self).__init__()
        self.name = name
        self.allocation_state = allocation_state
        self.operational_state = operational_state

    def type(self):
        return Resource.RESOURCE

    def id(self):
        return str(self.type()) + ":" + self.name

    def available(self):
        if self.allocation_state == ALLOCATION.FREE:
            return True

        return False

    def expire(self):
        return datetime.utcnow()


class RoadmResource(Resource):
    def __init__(self, special, name, allocation_state=ALLOCATION.FREE, operational_state=OPERATIONAL.FAILED):
        super(RoadmResource, self).__init__(name, allocation_state, operational_state)
        self.special = special

    def type(self):
        return Resource.ROADM_RESOURCE
