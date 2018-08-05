#!/usr/bin/env python3
# -*- coding: ascii -*-

"""
In-memory hierarchical key-value store.
"""

import struct
import threading
import socket
import logging

try:
    from urllib.parse import urlsplit
except ImportError:
    from urlparse import urlsplit

ERRORS = {
    'UNKNOWN': (1, 'Unknown error'),
    'NOCMD': (2, 'No such command'),
    'NORESP': (3, 'Unknown response'),
    'NOSTORE': (4, 'No datastore opened'),
    'NOKEY': (5, 'No such key'),
    'BADTYPE': (6, 'Invalid value type'),
    'BADPATH': (7, 'Path too short'),
    'BADLCLASS': (8, 'Invalid listing class'),
    'BADUNLOCK': (9, 'Unpaired unlock')
    }

ERROR_CODES = {code: name for name, (code, desc) in ERRORS.items()}

LCLASS_BYTES = 1
LCLASS_SUBKEYS = 2
LCLASS_ANY = 3

# 8311 is delta-encoded from the alphabet indices of H, K, and V.
DEFAULT_ADDRESS = ('localhost', 8311)

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
        return cls(code, description)

    def __init__(self, code, message):
        Exception.__init__(self, 'code %s: %s' % (code, message))
        self.code = code

def parse_url(url):
    parts = urlsplit(url)
    if parts.scheme != 'hkv':
        raise ValueError('Invalid hkv:// URL')
    host, port = parts.hostname, parts.port
    if host is None:
        host = DEFAULT_ADDRESS[0]
    if port is None:
        port = DEFAULT_ADDRESS[1]
    if host.startswith('[') and host.endswith(']') or ':' in host:
        addrfamily = socket.AF_INET6
    else:
        addrfamily = socket.AF_INET
    pathparts = parts.path.split('/')
    if len(pathparts) > 2:
        raise ValueError('Invalid hkv:// URL')
    elif pathparts[0] != '':
        raise RuntimeError('URL parsing failed?!')
    elif len(pathparts) == 1 or not pathparts[1]:
        dsname = None
    else:
        dsname = pathparts[1].encode('utf-8')
    ret = {'addrfamily': addrfamily, 'addr': (host, port)}
    if dsname is not None: ret['dsname'] = dsname
    return ret

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
        self._lock = threading.RLock()
        self._operations = {k: (i, getattr(self, m), o)
                            for k, (i, m, o) in self._OPERATIONS.items()}

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
        self._lock.acquire()

    def unlock(self):
        try:
            self._lock.release()
        except RuntimeError:
            raise HKVError.for_name('BADUNLOCK')

    def close(self):
        self.data = None

    def get(self, path):
        with self._lock:
            ret = self._follow_path(path)
            if isinstance(ret, dict): raise HKVError.for_name('BADTYPE')
            return ret

    def get_all(self, path):
        with self._lock:
            record = self._follow_path(path)
            if not isinstance(record, dict):
                raise HKVError.for_name('BADTYPE')
            return {k: v for k, v in record.items()
                    if not isinstance(v, dict)}

    def list(self, path, lclass):
        with self._lock:
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
        with self._lock:
            record, key = self._split_follow_path(path, True)
            record[key] = value

    def put_all(self, path, values):
        self.put(path, values)

    def delete(self, path):
        with self._lock:
            record, key = self._split_follow_path(path)
            try:
                del record[key]
            except KeyError:
                raise HKVError.for_name('NOKEY')

    def delete_all(self, path):
        with self._lock:
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

    def flush(self):
        self.wfile.flush()

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
        if format.startswith('@'):
            single = True
            if len(format) != 2:
                raise TypeError('@ format string must contain exactly '
                    'one format character')
            format = format[1:]
        else:
            single = False
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
        def __init__(self, parent, id, conn, addr):
            self.parent = parent
            self.id = id
            self.conn = conn
            self.addr = addr
            self.codec = Codec(self.conn.makefile('rb'),
                               self.conn.makefile('wb'))
            self.datastore = None
            self.locked = 0
            self.logger = logging.getLogger('client/%s' % self.id)

        def init(self):
            self.logger.info('Connection from %s', self.addr)

        def close(self):
            self.logger.info('Closing')
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
                        self.codec.write_char(b'-')
                        break
                    elif cmd == b'o':
                        self.unlock(True)
                        name = self.codec.read_bytes()
                        self.datastore = self.parent.get_datastore(name)
                        self.codec.write_char(b'-')
                    elif cmd == b'c':
                        self.unlock(True)
                        self.datastore = None
                        self.codec.write_char(b'-')
                    elif cmd == b'l':
                        if self.datastore is None:
                            self.write_error('NOSTORE')
                        self.lock()
                        self.codec.write_char(b'-')
                    elif cmd == b'u':
                        if self.datastore is None:
                            self.write_error('NOSTORE')
                        try:
                            self.unlock()
                            self.codec.write_char(b'-')
                        except HKVError as exc:
                            self.write_error(exc)
                    elif cmd in DataStore._OPERATIONS:
                        try:
                            operation = DataStore._OPERATIONS[cmd]
                            args = self.codec.readf(operation[0])
                            if self.datastore is None:
                                raise HKVError.from_name('NOSTORE')
                            result = self.datastore._operations[cmd][1](*args)
                        except HKVError as exc:
                            self.write_error(exc)
                        else:
                            resp = operation[2].encode('ascii')
                            self.codec.write_char(resp)
                            self.codec.writef(operation[2], result)
                    else:
                        self.write_error('NOCMD')
                    self.codec.flush()
            finally:
                self.codec.flush()
                self.unlock(True)
                self.close()

    def __init__(self, addr, addrfamily=None):
        if addrfamily is None: addrfamily = socket.AF_INET
        self.addr = addr
        self.addrfamily = addrfamily
        self.socket = None
        self.datastores = {}
        self._next_id = 1
        self.logger = logging.getLogger('server')

    def listen(self):
        self.logger.info('Listening on %s', self.addr)
        self.socket = socket.socket(self.addrfamily)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket.bind(self.addr)
        self.socket.listen(5)

    def accept(self):
        conn, addr = self.socket.accept()
        handler = self.ClientHandler(self, self._next_id, conn, addr)
        self._next_id += 1
        handler.init()
        spawn_thread(handler.main)

    def close(self):
        self.logger.info('Closing')
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
            except IOError:
                pass

class RemoteDataStore:
    def __init__(self, addr, dsname=None, addrfamily=None):
        if addrfamily is None: addrfamily = socket.AF_INET
        self.addr = addr
        self.dsname = dsname
        self.addrfamily = addrfamily
        self.socket = None
        self.codec = None
        self._lock = threading.RLock()

    def __enter__(self):
        self._lock.__enter__()
        self.lock()

    def __exit__(self, *args):
        try:
            self.unlock()
        finally:
            self._lock.__exit__(*args)

    def connect(self):
        self.socket = socket.socket(self.addrfamily)
        self.socket.connect(self.addr)
        self.codec = Codec(self.socket.makefile('rb'),
                           self.socket.makefile('wb'))
        if self.dsname is not None: self.open(self.dsname)

    def open(self, dsname):
        self._run_command(b'o', 's', dsname)

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
        with self._lock:
            self.codec.write_char(cmd)
            self.codec.writef(format, *args)
            self.codec.flush()
            resp = self.codec.read_char()
            if resp == b'e':
                code = self.codec.read_int()
                raise HKVError.for_code(code)
            elif resp in b'slm-':
                return self.codec.readf(resp.decode('ascii'))
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

def main_listen(params, no_timestamps, loglevel):
    if 'dsname' in params:
        raise SystemExit('ERROR: Must not specify datastore name when '
            'listening')
    if no_timestamps:
        logging.basicConfig(format='[%(name)s %(levelname)s] %(message)s',
                            level=loglevel)
    else:
        logging.basicConfig(format='[%(asctime)s %(name)s %(levelname)s] '
            '%(message)s', datefmt='%Y-%m-%d %H:%M:%S', level=loglevel)
    server = Server(**params)
    server.listen()
    try:
        server.main()
    except KeyboardInterrupt:
        pass

def main_command(params, command, *args):
    def ensure_args(min=1, max=None):
        if len(args) < min:
            raise SystemExit('ERROR: Too few arguments for %s' % command)
        if max is not None and len(args) > max:
            raise SystemExit('ERROR: Too many arguments for %s' % command)
    if 'dsname' not in params:
        raise SystemExit('ERROR: Must specify datastore name when connecting')
    # Parse command line
    if command in ('get', 'get_all', 'delete', 'delete_all'):
        ensure_args(1, 1)
        cmdargs = ()
    elif command == 'put':
        ensure_args(2, 2)
        cmdargs = (args[1].encode('utf-8'),)
    elif command == 'put_all':
        ensure_args(1)
        values = {}
        for item in args[1:]:
            k, _, v = item.encode('utf-8').partition(b'=')
            values[k] = v
        cmdargs = (values,)
    else:
        raise SystemExit('ERROR: Unknown command: %s' % command)
    path = args[0].encode('utf-8').split(b'/')
    # Create client and execute command
    client = RemoteDataStore(**params)
    try:
        client.connect()
    except IOError as exc:
        raise SystemExit('ERROR: %s' % exc)
    try:
        getattr(client, command)(path, *cmdargs)
    except HKVError as exc:
        raise SystemExit('ERROR: %s' % exc)
    finally:
        client.close()

def main():
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument('--listen', '-l', action='store_true',
                   help='Enter server mode (instead of client mode)')
    p.add_argument('--url', '-u',
                   help='URL to serve on / to connect to')
    p.add_argument('--datastore', '-d',
                   help='Datastore to use (client mode only)')
    p.add_argument('--no-timestamps', action='store_true',
                   help='no timestamps on logs')
    p.add_argument('--loglevel', default='INFO',
                   help='Logging level (defaults to INFO)')
    p.add_argument('command', nargs='?',
                   help='Command to execute (client mode only)')
    p.add_argument('arg', nargs='*',
                   help='Additional arguments to the command')
    result = p.parse_args()
    if bool(result.listen) == bool(result.command):
        raise SystemExit('ERROR: Must specify either -l or command.')
    try:
        if result.url is None:
            params = {'addr': DEFAULT_ADDRESS}
        else:
            params = parse_url(result.url)
    except ValueError:
        raise SystemExit('ERROR: Invalid hkv:// URL: %s' % result.url)
    if result.datastore is not None:
        params['dsname'] = result.datastore.encode('utf-8')
    if result.listen:
        main_listen(params, result.no_timestamps, result.loglevel)
    else:
        main_command(params, result.command, *result.arg)

if __name__ == '__main__': main()
