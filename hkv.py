
# -*- coding: ascii -*-

"""
In-memory hierarchical key-value store.
"""

import struct
import threading
import socket

ERRORS = {
    'UNKNOWN': (1, 'Unknown error'),
    'NOCMD': (2, 'No such command'),
    'NOKEY': (3, 'No such key'),
    'BADTYPE': (4, 'Invalid value type'),
    'BADPATH': (5, 'Path too short'),
    'BADLCLASS': (6, 'Bad listing class')
    }

ERROR_CODES = {code: name for code, (name, desc) in ERRORS.items()}

LCLASS_BYTES = 1
LCLASS_SUBKEYS = 2
LCLASS_ANY = 3

INTEGER = struct.Struct('!I')

class HKVError(Exception):
    @classmethod
    def for_name(cls, name):
        return cls(*ERRORS[name])

    @classmethod
    def for_code(cls, code):
        return cls(ERRORS[ERROR_CODES[code]])

    def __init__(self, code, message):
        Exception.__init__(self, 'code %s: %s' % (code, message))
        self.code = code

def spawn_thread(func, *args, **kwds):
    thr = threading.Thread(target=func, args=args, kwargs=kwds)
    thr.setDaemon(True)
    thr.start()
    return thr

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
                    raise HKVError.for_name('NOKEY')
        return cur

    def _split_follow_path(self, path, create=False):
        if not path: raise HKVError.for_name('BADPATH')
        prefix, last = path[:-1], path[-1]
        return self._follow_path(prefix, create), last

    def close(self):
        self.data = None

    def get(self, path):
        ret = self._follow_path(path)
        if isinstance(ret, dict): raise HKVError.for_name('BADTYPE')
        return ret

    def get_all(self, path):
        record = self._follow_path(path)
        if not isinstance(ret, dict): raise HKVError.for_name('BADTYPE')
        return {k: v for k, v in record.items() if not isinstance(v, dict)}

    def list(self, path, lclass):
        record = self._follow_path(path)
        if lclass == LCLASS_BYTES:
            return [k for k, v in record.items() if not isinstance(v, dict)]
        elif lclass == LCLASS_SUBKEYS:
            return [k for k, v in record.items() if isinstance(v, dict)]
        elif lclass == LCLASS_ANY:
            return list(record)
        else:
            raise HKVError.for_name('BADLCLASS')

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
            raise HKVError.for_name('NOKEY')

    def delete_all(self, path):
        record = self._follow_path(path)
        if not isinstance(record, dict): raise HKVError.for_name('BADTYPE')
        record.clear()

class Codec:
    def __init__(self, rfile, wfile):
        self.rfile = rfile
        self.wfile = wfile

    def close(self):
        try:
            self.rfile.close()
        except Exception:
            pass
        try:
            self.wfile.close()
        except Exception:
            pass

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

class Server:
    class ClientHandler:
        def __init__(self, parent, conn, addr):
            self.parent = parent
            self.conn = conn
            self.addr = addr
            self.codec = Codec(self.conn.makefile('rb'),
                               self.conn.makefile('wb'))

        def close(self):
            try:
                self.conn.shutdown(socket.SHUT_RDWR)
            except Exception:
                pass
            try:
                self.codec.close()
            except Exception:
                pass
            try:
                self.conn.close()
            except Exception:
                pass

        def write_error(self, exc):
            self.codec.write_byte(b'e')
            if isinstance(exc, HKVError):
                self.codec.write_int(exc.code)
            else:
                self.codec.write_int(ERRORS['UNKNOWN'][0])

        def main(self):
            try:
                while 1:
                    cmd = self.codec.read_byte()
                    if not cmd or cmd == b'q':
                        break
                    else:
                        self.write_error(HKVError.for_name('NOCMD'))
            finally:
                self.close()

    def __init__(self, addr, addrfamily=None):
        if addrfamily is None: addrfamily = socket.AF_INET
        self.addr = addr
        self.addrfamily = addrfamily
        self.socket = None
        self.datastores = {}

    def listen(self):
        self.socket = socket.socket(self.addrfamily)
        self.socket.bind(self.addr)
        self.socket.listen()

    def accept(self):
        conn, addr = self.socket.accept()
        handler = self.ClientHandler(self, conn, addr)
        spawn_thread(handler.main)

    def close(self):
        try:
            self.socket.close()
        except Exception:
            pass

    def main(self):
        while 1:
            try:
                self.accept()
            except Exception:
                pass
