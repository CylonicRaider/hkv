
# -*- coding: ascii -*-

"""
In-memory hierarchical key-value store.
"""

class HKVError(Exception): pass

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
                    raise
        return cur

    def _split_follow_path(self, path, create=False):
        if not path: raise HKVError('Path too short')
        prefix, last = path[:-1], path[-1]
        return self._follow_path(prefix, create), last

    def get(self, path):
        ret = self._follow_path(path)
        if isinstance(ret, dict): raise TypeError
        return ret

    def get_all(self, path):
        ret = self._follow_path(path)
        if not isinstance(ret, dict): raise TypeError
        return ret

    def put(self, path, value):
        record, key = self._split_follow_path(path, True)
        record[key] = value

    def put_all(self, path, values):
        self.put(path, values)

    def delete(self, path):
        record, key = self._split_follow_path(path)
        del record[key]

    def delete_all(self, path):
        record = self._follow_path(path)
        if not isinstance(record, dict): raise TypeError
        record.clear()
