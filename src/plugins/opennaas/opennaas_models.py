"""
OpenNaas Data Models.
"""

class Resource(object):
    RESOURCE = 0
    ROADM_RESOURCE = 1

    def __init__(self, available):
        super(Resource, self).__init__()
        self.available = available

    def type(self):
        return Resource.RESOURCE


class RoadmResource(Resource):
    def __init__(self, available, special):
        super(RoadmResource, self).__init__(available)
        self.special = special

    def type(self):
        return Resource.ROADM_RESOURCE
