from datetime import datetime, date
import calendar
import json

from bson.objectid import ObjectId


class JSONEncoder(json.JSONEncoder):

    def default(self, obj):
        if isinstance(obj, (datetime, date)):
            return int(calendar.timegm(obj.timetuple()))
        elif isinstance(obj, ObjectId):
            return str(obj)
        elif isinstance(obj, set):
            return list(obj)
        return json.JSONEncoder.default(self, obj)


def json_decoder(obj):
    for key, val in obj.items():
        if key in ('_id', 'user', 'org', 'cust'):
            if val is not None:
                val = ObjectId(val)
        elif key in ('users',):
            if isinstance(val, list):
                val = [ObjectId(k) for k in val]
        elif key in ('created', 'modified', 'last_login',):
            if isinstance(val, (float, int)):
                val = datetime.utcfromtimestamp(val)

        obj[key] = val

    return obj


class JSONSerializer(object):

    def encode(self, obj, fd=None):
        try:
            if fd:
                return json.dump(obj, fd, cls=JSONEncoder)
            else:
                return json.dumps(obj, cls=JSONEncoder)
        except (TypeError, ValueError), e:
            raise Exception(str(e))

    def decode(self, msg=None, fd=None):
        try:
            if msg:
                return json.loads(msg, object_hook=json_decoder)
            elif fd:
                return json.load(fd, object_hook=json_decoder)
        except (TypeError, ValueError), e:
            raise Exception(str(e))


def serialize(obj):
    return JSONSerializer().encode(obj)
