
# -*- coding: ascii -*-

"""
In-memory hierarchical key-value store.
"""

import struct

ERRORS = {
    'NOKEY': (1, 'No such key'),
    'BADTYPE': (2, 'Invalid value type'),
    'BADPATH': (3, 'Path too short')
    }

ERROR_CODES = {code: name for code, (name, desc) in ERRORS.items()}

INTEGER = struct.Struct('!I')

class HKVError(Exception):
    @classmethod
    def for_error(cls, name):
        return cls(*ERRORS[name])

    @classmethod
    def for_code(cls, code):
        return cls(ERRORS[ERROR_CODES[code]])

    def __init__(self, code, message):
        Exception.__init__(self, 'code %s: %s' % (code, message))
        self.code = code

class DataStore:
    def __init__(self):
        self.data = {}

    def _follow_path(self, path, create=False):
        cur = self.data
        for ent in path:
            try:
                cur = cur[ent]
            except KeyError:
                if create:
                    new = {}
                    cur[ent] = new
                    cur = new
                else:
                    raise HKVError.for_error('NOKEY')
        return cur

    def _split_follow_path(self, path, create=False):
        if not path: raise HKVError.for_error('BADPATH')
        prefix, last = path[:-1], path[-1]
        return self._follow_path(prefix, create), last

    def get(self, path):
        ret = self._follow_path(path)
        if isinstance(ret, dict): raise HKVError.for_error('BADTYPE')
        return ret

    def get_all(self, path):
        ret = self._follow_path(path)
        if not isinstance(ret, dict): raise HKVError.for_error('BADTYPE')
        return ret

    def put(self, path, value):
        record, key = self._split_follow_path(path, True)
        record[key] = value

    def put_all(self, path, values):
        self.put(path, values)

    def delete(self, path):
        record, key = self._split_follow_path(path)
        try:
            del record[key]
        except KeyError:
            raise HKVError.for_error('NOKEY')

    def delete_all(self, path):
        record = self._follow_path(path)
        if not isinstance(record, dict): raise HKVError.for_error('BADTYPE')
        record.clear()

class Codec:
    def __init__(self, rfile, wfile):
        self.rfile = rfile
        self.wfile = wfile

    def read_char(self):
        return self.rfile.read(1)

    def write_char(self, item):
        self.wfile.write(item)

    def read_int(self):
        data = self.rfile.read(INTEGER.size)
        return INTEGER.unpack(data)[0]

    def write_int(self, item):
        self.wfile.write(INTEGER.pack(item))

    def read_bytes(self):
        length = self.read_int()
        return self.rfile.read(length)

    def write_bytes(self, data):
        self.write_int(len(data))
        self.wfile.write(data)

    def read_bytelist(self):
        length = self.read_int()
        ret = []
        while length:
            ret.append(self.read_bytes())
            length -= 1
        return ret

    def write_bytelist(self, data):
        self.write_int(len(data))
        for item in data:
            self.write_bytes(item)

    def read_bytedict(self):
        length = self.read_int()
        ret = {}
        while length:
            key = self.read_bytes()
            value = self.read_bytes()
            ret[key] = value
            length -= 1
        return ret
