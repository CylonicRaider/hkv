
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
    'NORESP': (3, 'Unknown response'),
    'NOSTORE': (4, 'No datastore opened'),
    'NOKEY': (5, 'No such key'),
    'BADTYPE': (6, 'Invalid value type'),
    'BADPATH': (7, 'Path too short'),
    'BADLCLASS': (8, 'Bad listing class'),
    'BADUNLOCK': (9, 'Unpaired unlock')
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
        try:
            description = ERRORS[ERROR_CODES[code]][1]
        except KeyError:
            description = 'Unknown error?!'
        return cls(code, ERRORS[ERROR_CODES[code]])

    def __init__(self, code, message):
        Exception.__init__(self, 'code %s: %s' % (code, message))
        self.code = code

def spawn_thread(func, *args, **kwds):
    thr = threading.Thread(target=func, args=args, kwargs=kwds)
    thr.setDaemon(True)
    thr.start()
    return thr

class DataStore:
    _OPERATIONS = {
        b'g': ('l', 'get', 's'),
        b'G': ('l', 'get_all', 'm'),
        b'l': ('li', 'list', 'l'),
        b'p': ('ls', 'put', '-'),
        b'P': ('ld', 'put_all', '-'),
        b'd': ('l', 'delete', '-'),
        b'D': ('l', 'delete_all', '-')
        }

    def __init__(self):
        self.data = {}
        self.lock = threading.RLock()
        self._operations = {k: (i, getattr(self, m), o)
                            for k, (i, m, o) in self._OPERATIONS}

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

    def lock(self):
        self.lock.acquire()

    def unlock(self):
        try:
            self.lock.release()
        except RuntimeError:
            raise HKVError.for_name('BADUNLOCK')

    def close(self):
        self.data = None

    def get(self, path):
        with self.lock:
            ret = self._follow_path(path)
            if isinstance(ret, dict): raise HKVError.for_name('BADTYPE')
            return ret

    def get_all(self, path):
        with self.lock:
            record = self._follow_path(path)
            if not isinstance(record, dict):
                raise HKVError.for_name('BADTYPE')
            return {k: v for k, v in record.items()
                    if not isinstance(v, dict)}

    def list(self, path, lclass):
        with self.lock:
            record = self._follow_path(path)
            if lclass == LCLASS_BYTES:
                return [k for k, v in record.items()
                        if not isinstance(v, dict)]
            elif lclass == LCLASS_SUBKEYS:
                return [k for k, v in record.items() if isinstance(v, dict)]
            elif lclass == LCLASS_ANY:
                return list(record)
            else:
                raise HKVError.for_name('BADLCLASS')

    def put(self, path, value):
        with self.lock:
            record, key = self._split_follow_path(path, True)
            record[key] = value

    def put_all(self, path, values):
        self.put(path, values)

    def delete(self, path):
        with self.lock:
            record, key = self._split_follow_path(path)
            try:
                del record[key]
            except KeyError:
                raise HKVError.for_name('NOKEY')

    def delete_all(self, path):
        with self.lock:
            record = self._follow_path(path)
            if not isinstance(record, dict):
                raise HKVError.for_name('BADTYPE')
            record.clear()

class Codec:
    def __init__(self, rfile, wfile):
        self.rfile = rfile
        self.wfile = wfile
        self._rmap = {
            '-': self.read_nothing,
            'c': self.read_char,
            'i': self.read_int,
            's': self.read_bytes,
            'l': self.read_bytelist,
            'm': self.read_bytedict}
        self._wmap = {
            '-': self.write_nothing,
            'c': self.write_char,
            'i': self.write_int,
            's': self.write_bytes,
            'l': self.write_bytelist,
            'm': self.write_bytedict}

    def close(self):
        try:
            self.rfile.close()
        except Exception:
            pass
        try:
            self.wfile.close()
        except Exception:
            pass

    def read_nothing(self):
        return None

    def write_nothing(self, value):
        if value is not None:
            raise TypeError('Non-None value passed to write_nothing()')
        # NOP

    def read_char(self):
        ret = self.rfile.read(1)
        if len(ret) != 1: raise EOFError('Received end-of-file')
        return ret

    def write_char(self, item):
        self.wfile.write(item)

    def read_int(self):
        data = self.rfile.read(INTEGER.size)
        if len(data) != INTEGER.size: raise EOFError('Short read')
        return INTEGER.unpack(data)[0]

    def write_int(self, item):
        self.wfile.write(INTEGER.pack(item))

    def read_bytes(self):
        length = self.read_int()
        ret = self.rfile.read(length)
        if len(ret) != length: raise EOFError('Short read')
        return ret

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

    def write_bytedict(self, data):
        self.write_int(len(data))
        for k, v in data.items():
            self.write_bytes(k)
            self.write_bytes(v)

    def readf(self, format):
        if format.startswith('*'):
            single = False
            format = format[1:]
        else:
            single = True
            if len(format) != 1:
                raise TypeError('* format string must contain exactly one '
                    'character')
        ret = []
        for t in format:
            ret.append(self._rmap[t]())
        if single: ret = ret[0]
        return ret

    def writef(self, format, *args):
        if format.startswith('*'):
            format = format[1:]
            if len(args) != 1:
                raise TypeError('Need exactly one additional argument for * '
                    'format string')
            args = args[0]
        if len(args) != len(format):
            raise TypeError('Invalid argument count for format string')
        for t, a in zip(format, args):
            self._wmap[t](a)

class Server:
    class ClientHandler:
        def __init__(self, parent, conn, addr):
            self.parent = parent
            self.conn = conn
            self.addr = addr
            self.codec = Codec(self.conn.makefile('rb'),
                               self.conn.makefile('wb'))
            self.datastore = None
            self.locked = 0

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

        def lock(self):
            if self.locked == 0:
                self.datastore.lock()
            self.locked += 1

        def unlock(self, full=False):
            if full:
                if self.locked > 0 and self.datastore:
                    self.datastore.unlock()
                self.locked = 0
            elif self.locked == 0:
                raise HKVError.for_name('BADUNLOCK')
            elif self.locked == 1:
                self.locked = 0
                self.datastore.unlock()
            else:
                self.locked -= 1

        def write_error(self, exc):
            if isinstance(exc, str):
                code = ERRORS[exc][0]
            elif isinstance(exc, HKVError):
                code = exc.code
            else:
                code = ERRORS['UNKNOWN'][0]
            self.codec.writef('ci', b'e', code)

        def main(self):
            try:
                while 1:
                    try:
                        cmd = self.codec.read_char()
                    except EOFError:
                        break
                    if cmd == b'q':
                        self.write_char(b'-')
                        break
                    elif cmd == b'o':
                        self.unlock(True)
                        name = self.codec.read_bytes()
                        self.datastore = self.parent.get_datastore(name)
                        self.write_char(b'-')
                    elif cmd == b'c':
                        self.unlock(True)
                        self.datastore = None
                        self.write_char(b'-')
                    elif cmd == b'l':
                        if self.datastore is None:
                            self.write_error('NOSTORE')
                        self.lock()
                        self.write_char(b'-')
                    elif cmd == b'u':
                        if self.datastore is None:
                            self.write_error('NOSTORE')
                        try:
                            self.unlock()
                            self.write_char(b'-')
                        except HKVError as exc:
                            self.write_error(exc)
                    elif cmd in DataStore._OPERATIONS:
                        if self.datastore is None:
                            self.write_error('NOSTORE')
                            continue
                        operation = self.datastore._operations[cmd]
                        args = self.codec.readf(operation[0])
                        result = operation[1](*args)
                        self.codec.write_char(operation[2].encode('ascii'))
                        self.codec.writef(operation[2], result)
                    else:
                        self.write_error('NOCMD')
            finally:
                self.unlock(True)
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

    def get_datastore(self, name):
        try:
            return self.datastores[name]
        except KeyError:
            ret = DataStore()
            self.datastores[name] = ret
            return ret

    def main(self):
        while 1:
            try:
                self.accept()
            except Exception:
                pass

class RemoteDataStore:
    def __init__(self, addr, dsname, addrfamily=None):
        if addrfamily is None: addrfamily = socket.AF_INET
        self.addr = addr
        self.dsname = dsname
        self.addrfamily = addrfamily
        self.socket = None
        self.codec = None
        self.lock = threading.RLock()

    def connect(self):
        self.socket = socket.socket(self.addrfamily)
        self.socket.connect(self.addr)
        self.codec = Codec(self.socket.makefile('rb'),
                           self.socket.makefile('wb'))

    def close(self):
        try:
            self.socket.shutdown(socket.SHUT_RDWR)
        except Exception:
            pass
        try:
            self.codec.close()
        except Exception:
            pass
        try:
            self.socket.close()
        except Exception:
            pass

    def _run_command(self, cmd, format, *args):
        with self.lock:
            self.codec.write_char(cmd)
            self.codec.writef(format, *args)
            resp = self.codec.read_char()
            if resp == b'e':
                code = self.codec.read_int()
                raise HKVError.for_code(code)
            elif resp in b'slm-':
                return self.codec.readf(resp)
            else:
                raise HKVError.for_name('NORESP')

    def _run_operation(self, opname, *args):
        operation = DataStore._OPERATIONS[opname]
        return self._run_command(opname, operation[0], *args)

    def lock(self):
        return self._run_command(b'l', '')

    def unlock(self):
        return self._run_command(b'u', '')

    def get(self, path):
        return self._run_operation(b'g', path)

    def get_all(self, path):
        return self._run_operation(b'G', path)

    def list(self, path, lclass):
        return self._run_operation(b'l', path, lclass)

    def put(self, path, value):
        return self._run_operation(b'p', path, value)

    def put_all(self, path, values):
        return self._run_operation(b'P', path, values)

    def delete(self, path):
        return self._run_operation(b'd', path)

    def delete_all(self, path):
        return self._run_operation(b'D', path)
