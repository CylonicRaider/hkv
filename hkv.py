#!/usr/bin/env python3
# -*- coding: ascii -*-

"""
In-memory hierarchical key-value store.

A typical usage scenario -- aside from using a DataStore instance as a
glorified dictionary -- would be to run a server (using the DataStoreServer
class, or by running this module as a script) and to connect to it using a
RemoteDataStore instance; this allows sharing data across process lifetimes
and possibly across language boundaries. In all cases, a ConvertingDataStore
subclass may be employed to convert the values (which may only be byte strings
when using most classes here) to some more palatable application-defined type
(or types) transparently.

A DataStore holds pairs of keys and values (with unique keys), where each
value can be either a "scalar" or a nested collection of key-value pairs with
the same semantics. Each value is reachable through a path, which corresponds
to a sequence of keys; if the path points to a key-value collection, the keys
of the latter are said to be "nested" below the path.
The concrete types of keys and values are not defined; the classes provided in
module use byte strings as keys and scalar values, Python sequences (e.g.
tuples) of byte strings as paths, and Python dictionaries as key-value
collections. The ConvertingDataStore class allows using arbitrary types for
keys, scalar values, and paths (key-value collections remain Python
dictionaries).
"""

import os
import struct
import threading
import socket
import logging

try:
    from urllib.parse import urlsplit
except ImportError:
    from urlparse import urlsplit

__all__ = ['ERRORS', 'ERROR_CODES', 'LCLASS_SCALAR', 'LCLASS_NESTED',
           'LCLASS_ANY', 'HKVError', 'BaseDataStore', 'DataStore',
           'NullDataStore', 'ConvertingDataStore', 'DataStoreServer',
           'RemoteDataStore']

# Mapping from error names to codes and descriptions.
ERRORS = {
    'UNKNOWN': (1, 'Unknown error'),
    'NOCMD': (2, 'No such command'),
    'NORESP': (3, 'Unknown response'),
    'NOSTORE': (4, 'No datastore opened'),
    'NOKEY': (5, 'No such key'),
    'BADNEST': (6, 'Prefix of path corresponds to scalar'),
    'BADTYPE': (7, 'Invalid value type'),
    'BADPATH': (8, 'Path too short'),
    'BADLCLASS': (9, 'Invalid listing class'),
    'BADUNLOCK': (10, 'Unpaired unlock')}

# Mapping from error codes to names and descriptions.
ERROR_CODES = {code: (name, desc) for name, (code, desc) in ERRORS.items()}

# Possible lclass argments to DataStore.list().
LCLASS_NONE   = 0 # List nothing.
LCLASS_SCALAR = 1 # List values contained in this key.
LCLASS_NESTED = 2 # List nested keys.
LCLASS_ANY    = 3 # List both contained values and nested keys.

# The default address on which to listen on / connect to.
# 8311 is delta-encoded from the alphabet indices of H, K, and V.
DEFAULT_ADDRESS = ('localhost', 8311)

# Helper object for Codec.
INTEGER = struct.Struct('!I')

class HKVError(Exception):
    """
    HKVError(code, message) -> new instance

    General exception for this module.

    code is a numeric error code; message is a human-readable description of
    the error; the two are combined into the final exception message by the
    constructor. Users are encouraged to use the factory functions provided on
    the class instead of calling the constructor directly.

    Instances have a "code" attribute that is the numeric error code; it can
    be mapped to a tuple of a symbolic name and a human-readable description
    via the module-level ERROR_CODES mapping.
    """

    @classmethod
    def for_name(cls, name):
        """
        Create a HKVError instance from a symbolic error name.
        """
        return cls(*ERRORS[name])

    @classmethod
    def for_code(cls, code):
        """
        Create a HKVError instance from a numeric code.
        """
        try:
            description = ERRORS[ERROR_CODES[code][0]][1]
        except KeyError:
            description = 'Unknown error?!'
        return cls(code, description)

    def __init__(self, code, message):
        "Initializer. See class docstring for details."
        super(HKVError, self).__init__('code %s: %s' % (code, message))
        self.code = code

def parse_url(url):
    """
    Utility function converting a URL into keyword arguments for
    DataStoreServer or RemoteDataStore constructor keyword arguments.
    """
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
    """
    Utility function for creating and starting a daemonic thread.
    """
    thr = threading.Thread(target=func, args=args, kwargs=kwds)
    thr.setDaemon(True)
    thr.start()
    return thr

class BaseDataStore(object):
    """
    An abstract class defining the operations DataStore et al. support.

    See the module docstring for the terms used.

    The operations may raise HKVError instances whose code corresponds to
    some of the symbolic names defined in the ERRORS mapping; an error
    corresponding to symbolic name X is abbreviatedly referred to as an "X
    error" henceforth.

    Most operations that take a path as an argument require that all of it
    should exist, unless otherwise noted; if that is not the case, they raise
    a NOKEY error.
    Any operation that takes a path raises a BADNEST error if any prefix of
    the path refers to scalar value.
    Unless otherwise noted, paths must be nonempty; this implies that it is
    impossible to perform most operations on the datastore itself.
    These errors are not explicitly noted below. Some implementations may
    raise less errors than noted here.

    DataStore objects support the context management protocol; when used in a
    with statement, the DataStore is locked before entering the code block and
    unlocked after exiting the code block.
    """

    def __enter__(self):
        """
        Context manager entry.

        See the class docstring for details.

        The default implementation calls the lock() method and returns the
        DataStore object it is called upon.
        """
        self.lock()
        return self

    def __exit__(self, *args):
        """
        Context manager exit.

        See the class docstring for details.

        The default implementation calls the unlock() method.
        """
        self.unlock()

    def lock(self):
        """
        Acquire an exclusive lock on the datastore.

        The lock is reentrant; i.e., lock() may be called multiple times
        without causing a deadlock. If another user of the datastore already
        holds a lock, this blocks until the lock is fully released.
        """
        raise NotImplementedError

    def unlock(self):
        """
        Release a level of locking on the datastore.

        If the caller does not hold the lock of the datastore, thus causes
        a BADUNLOCK error.
        """
        raise NotImplementedError

    def close(self):
        """
        Dispose of any low-level resources associated with this datastore.

        The datastore object is unusable after this (note, however, that it is
        not necessarily identical with the underlying storage: a remote
        datastore will remain intact); further attempts to access it may
        result in unspecified errors.
        """
        raise NotImplementedError

    def get(self, path):
        """
        Retrieve the scalar value at the given path.

        If the path does not refer to a scalar value, this results in a
        BADTYPE error.
        """
        raise NotImplementedError

    def get_all(self, path):
        """
        Retrieve all keys nested immediately under path and their values.

        If the path has a scalar value, this results in a BADTYPE error. path
        may be empty. Note that an empty mapping may be a valid result.
        """
        raise NotImplementedError

    def list(self, path, lclass):
        """
        Enumerate all keys nested below path, filtering by the given listing
        class.

        path may be the empty path; if it refers to a scalar, a BADTYPE error
        is raised. lclass is a bitmask of LCLASS_* constants; if the
        LCLASS_SCALAR bit is set, keys corresponding to scalar values are
        included in the result; if the LCLASS_NESTED bit is set, nested keys
        are included.

        Note that an empty sequence may be a valid result.
        """
        raise NotImplementedError

    def put(self, path, value):
        """
        Store the given value at the given path.

        value must be a scalar value.

        If path does not exist, it is created (recursively). Otherwise, the
        value replaces whatever has been at the path previously (which may be
        a nested subtree of key-value pairs).
        """
        raise NotImplementedError

    def put_all(self, path, values):
        """
        Store all key-value pairs from values below path.

        path may be the empty path. The values in values must be scalars.

        If path does not exist, it is created (recursively). If it exists and
        refers to a scalar value, a BADTYPE error is raised.
        """
        raise NotImplementedError

    def replace(self, path, values):
        """
        Store the given key-value pairs as descendants of path.

        This is functionally equivalent to put(), but takes a different
        argument type. The values in values must be scalars.
        """
        raise NotImplementedError

    def delete(self, path):
        """
        Delete the value residing at path, regardless of its type.

        If path does already not exist, this produces the appropriate error.
        """
        raise NotImplementedError

    def delete_all(self, path):
        """
        Delete all descendants of the value residing at path.

        If path does not refer to a nested key-value collection, this raises
        a BADTYPE error.
        """
        raise NotImplementedError

class DataStore(BaseDataStore):
    """
    DataStore() -> new instance

    This is an in-memory implementation of the DataStore interface.
    """

    _OPERATIONS = {
        b'g': ('a', 'get', 's'),
        b'G': ('a', 'get_all', 'm'),
        b'l': ('ai', 'list', 'a'),
        b'p': ('as', 'put', '-'),
        b'P': ('am', 'put_all', '-'),
        b'r': ('am', 'replace', '-'),
        b'd': ('a', 'delete', '-'),
        b'D': ('a', 'delete_all', '-')}

    def __init__(self):
        "Initializer; see class docstring for details."
        self.data = {}
        self._lock = threading.RLock()
        self._operations = {k: (i, getattr(self, m), o)
                            for k, (i, m, o) in self._OPERATIONS.items()}

    def _follow_path(self, path, create=False):
        "Internal helper method."
        cur = self.data
        for ent in path:
            if not isinstance(cur, dict):
                raise HKVError.for_name('BADNEST')
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
        "Internal helper method."
        if not path: raise HKVError.for_name('BADPATH')
        prefix, last = path[:-1], path[-1]
        res = self._follow_path(prefix, create)
        if not isinstance(res, dict): raise HKVError.for_name('BADNEST')
        return res, last

    def lock(self):
        "Lock this DataStore; see BaseDataStore for details."
        self._lock.acquire()

    def unlock(self):
        "Unlock this DataStore; see BaseDataStore for details."
        try:
            self._lock.release()
        except RuntimeError:
            raise HKVError.for_name('BADUNLOCK')

    def close(self):
        "Dispose of this DataStore; see BaseDataStore for details."
        self.data = None

    def get(self, path):
        "Retrieve a scalar at path; see BaseDataStore for details."
        with self._lock:
            ret = self._follow_path(path)
            if isinstance(ret, dict): raise HKVError.for_name('BADTYPE')
            return ret

    def get_all(self, path):
        "Retrieve key-value pairs below path; see BaseDataStore for details."
        with self._lock:
            record = self._follow_path(path)
            if not isinstance(record, dict):
                raise HKVError.for_name('BADTYPE')
            return {k: v for k, v in record.items()
                    if not isinstance(v, dict)}

    def list(self, path, lclass):
        "List some keys below path; see BaseDataStore for details."
        with self._lock:
            record = self._follow_path(path)
            if not isinstance(record, dict):
                raise HKVError.for_name('BADTYPE')
            elif lclass == LCLASS_SCALAR:
                return [k for k, v in record.items()
                        if not isinstance(v, dict)]
            elif lclass == LCLASS_NESTED:
                return [k for k, v in record.items() if isinstance(v, dict)]
            elif lclass == LCLASS_ANY:
                return list(record)
            else:
                raise HKVError.for_name('BADLCLASS')

    def put(self, path, value):
        "Store value at path; see BaseDataStore for details."
        with self._lock:
            record, key = self._split_follow_path(path, True)
            record[key] = value

    def put_all(self, path, values):
        "Merge pairs from values below path; see BaseDataStore for details."
        with self._lock:
            record = self._follow_path(path, True)
            if not isinstance(record, dict):
                raise HKVError.for_name('BADTYPE')
            for k, v in values.items():
                record[k] = v

    def replace(self, path, values):
        "Store values at path; see BaseDataStore for details."
        self.put(path, values)

    def delete(self, path):
        "Delete the value at path; see BaseDataStore for details."
        with self._lock:
            record, key = self._split_follow_path(path)
            try:
                del record[key]
            except KeyError:
                raise HKVError.for_name('NOKEY')

    def delete_all(self, path):
        "Delete everything below path; see BaseDataStore for details."
        with self._lock:
            record = self._follow_path(path)
            if not isinstance(record, dict):
                raise HKVError.for_name('BADTYPE')
            record.clear()

class NullDataStore(BaseDataStore):
    """
    NullDataStore() -> new instance

    A DataStore implementation that does not retain any data.

    All reading requests (get(), get_all(), list()) raise a NOKEY error, while
    all other operations do nothing.
    """

    def lock(self):
        pass

    def unlock(self):
        pass

    def close(self):
        pass

    def get(self, path):
        raise HKVError.for_name('NOKEY')

    def get_all(self, path):
        raise HKVError.for_name('NOKEY')

    def list(self, path, lclass):
        raise HKVError.for_name('NOKEY')

    def put(self, path, value):
        pass

    def put_all(self, path, values):
        pass

    def replace(self, path, values):
        pass

    def delete(self, path):
        pass

    def delete_all(self, path):
        pass

class ConvertingDataStore(BaseDataStore):
    """
    ConvertingDataStore(wrapped) -> new instance

    A wrapper around another DataStore that converts keys and values on the
    fly.

    An "internal" format for keys and values is used when speaking with the
    wrapped instance, while an "external" format is accepted and returned by
    the public methods. The import_*() method convert from the external to the
    internal format; the export_*() methods undo this conversion; these need
    to be implemented in a subclass in order to use this.

    The value conversion methods are not passed the key the value corresponds
    to (if any); in particular, the internal format must be self-describing
    enough to accommodate that. Where a key-value mapping is passed or
    returned, the individual keys and values are converted rather than the
    whole mapping.
    """

    def __init__(self, wrapped):
        "Instance initializer; see class docstring for details."
        self.wrapped = wrapped

    def import_key(self, key, fragment):
        """
        Convert the given key from the external to the internal format.

        If fragment is true, the key is a key proper; otherwise, it actually
        is a path.
        """
        raise NotImplementedError

    def export_key(self, key, fragment):
        """
        Convert the given key from the internal to the external format.

        If fragment is true, the key is a key proper; otherwise, it actually
        is a path.
        """
        raise NotImplementedError

    def import_value(self, value):
        """
        Convert the given scalar value from the internal to the external
        format.
        """
        raise NotImplementedError

    def export_value(self, value):
        """
        Convert the given scalar value from the external to the internal
        format.
        """
        raise NotImplementedError

    def lock(self):
        "Lock this DataStore; see BaseDataStore for details."
        self.wrapped.lock()

    def unlock(self):
        "Unlock this DataStore; see BaseDataStore for details."
        self.wrapped.unlock()

    def close(self):
        "Dispose of this DataStore; see BaseDataStore for details."
        self.wrapped.close()

    def get(self, path):
        "Retrieve a scalar at path; see BaseDataStore for details."
        path = self.import_key(path, False)
        return self.export_value(self.wrapped.get(path))

    def get_all(self, path):
        "Retrieve key-value pairs below path; see BaseDataStore for details."
        res = self.wrapped.get_all(self.import_key(path, False))
        ek, ev = self.export_key, self.export_value
        return {ek(k, True): ev(v) for k, v in res.items()}

    def list(self, path, lclass):
        "List some keys below path; see BaseDataStore for details."
        items = self.wrapped.list(self.import_key(path, False))
        ek = self.export_key
        return [ek(i, True) for i in items]

    def put(self, path, value):
        "Store value at path; see BaseDataStore for details."
        self.wrapped.put(self.import_key(path, False),
                         self.import_value(value))

    def put_all(self, path, values):
        "Merge pairs from values below path; see BaseDataStore for details."
        ik, iv = self.import_key, self.import_value
        ivalues = {ik(k, True): iv(v) for k, v in values.items()}
        self.wrapped.put_all(self.import_key(path, False), ivalues)

    def replace(self, path, values):
        "Store values at path; see BaseDataStore for details."
        ik, iv = self.import_key, self.import_value
        ivalues = {ik(k, True): iv(v) for k, v in values.items()}
        self.wrapped.replace(self.import_key(path, False), ivalues)

    def delete(self, path):
        "Delete the value at path; see BaseDataStore for details."
        self.wrapped.delete(self.import_key(path, False))

    def delete_all(self, path):
        "Delete everything below path; see BaseDataStore for details."
        self.wrapped.delete_all(self.import_key(path, False))

class Codec(object):
    """
    Codec(rfile, wfile) -> new instance

    A utility class for serializing (and deserializing) data for the remote
    API.

    The read_*() methods read bytes from rfile (which must be a binary stream
    open for reading) and convert them to the corresponding type; the
    write_*() methods convert the value passed to them into a byte stream and
    write it to wfile (which must be a binary stream open for writing). If
    wfile is buffering internally, use the flush() method to flush it. close()
    closes both rfile and wfile.

    readf() and writef() methods read or write values according to format
    strings passed to them. Each format string consists of an optional leading
    modifier, which is followed by a sequence of format units. Whitespace etc.
    is not allowed. The following modifiers and format units are defined:
    "@": (modifier; readf() only) Return a single value rather than a list of
         values. The remainder of the format string must consist of exactly
         one character; the value corresponding to it is read and returned.
    "*": (modifier; writef() only) Instead of multiple positional arguments
         following the format string, accept only one positional argument,
         which then must be a sequence from where the values to be encoded
         are taken.
    "-": A value of None; nothing is actually read/written from/to the
         underlying files.
    "c": A single byte.
    "i": A Python integer; mapped to an unsigned 32-bit integer.
    "s": A single byte string (at most 2**32-1 bytes large; may contain
         arbitrary byte values).
    "a": A list of at most 2**32-1 byte strings as for format unit "s".
    "m": A mapping with at most 2**32-1 pairs of keys and values, both of
         which may be arbitrary byte strings as above.
    """

    def __init__(self, rfile, wfile):
        "Instance initializer; see class docstring for details."
        self.rfile = rfile
        self.wfile = wfile
        self._rmap = {
            '-': self.read_nothing,
            'c': self.read_char,
            'i': self.read_int,
            's': self.read_bytes,
            'a': self.read_bytelist,
            'm': self.read_bytedict}
        self._wmap = {
            '-': self.write_nothing,
            'c': self.write_char,
            'i': self.write_int,
            's': self.write_bytes,
            'a': self.write_bytelist,
            'm': self.write_bytedict}

    def close(self):
        """
        Close the underlying streams.
        """
        try:
            self.rfile.close()
        finally:
            self.wfile.close()

    def flush(self):
        """
        Flush the underlying *writing* stream.

        Use this to actually send data if wfile is buffering.
        """
        self.wfile.flush()

    def read_nothing(self):
        """
        Read nothing and return None.

        This is provided for symmetry with the other read_*() methods and used
        internally.
        """
        return None

    def write_nothing(self, value):
        """
        Ensure the given argument is None and write nothing.

        See the notes for read_nothing().
        """
        if value is not None:
            raise TypeError('Non-None value passed to write_nothing()')
        # NOP

    def read_char(self):
        """
        Read a single character.
        """
        ret = self.rfile.read(1)
        if len(ret) != 1: raise EOFError('Received end-of-file')
        return ret

    def write_char(self, item):
        """
        Write a single character.
        """
        self.wfile.write(item)

    def read_int(self):
        """
        Read a 32-bit unsigned integer and return a Python integer.
        """
        data = self.rfile.read(INTEGER.size)
        if len(data) != INTEGER.size: raise EOFError('Short read')
        return INTEGER.unpack(data)[0]

    def write_int(self, item):
        """
        Write a Python integer as an unsigned 32-bit integer.
        """
        self.wfile.write(INTEGER.pack(item))

    def read_bytes(self):
        """
        Read a byte string.
        """
        length = self.read_int()
        ret = self.rfile.read(length)
        if len(ret) != length: raise EOFError('Short read')
        return ret

    def write_bytes(self, data):
        """
        Write a byte string.
        """
        self.write_int(len(data))
        self.wfile.write(data)

    def read_bytelist(self):
        """
        Read a list of byte strings.
        """
        length = self.read_int()
        ret = []
        while length:
            ret.append(self.read_bytes())
            length -= 1
        return ret

    def write_bytelist(self, data):
        """
        Write a sequence of byte strings.
        """
        self.write_int(len(data))
        for item in data:
            self.write_bytes(item)

    def read_bytedict(self):
        """
        Read a dictionary with byte strings as keys and values.
        """
        length = self.read_int()
        ret = {}
        while length:
            key = self.read_bytes()
            value = self.read_bytes()
            ret[key] = value
            length -= 1
        return ret

    def write_bytedict(self, data):
        """
        Write a mapping with byte strings as keys and values.
        """
        self.write_int(len(data))
        for k, v in data.items():
            self.write_bytes(k)
            self.write_bytes(v)

    def readf(self, format):
        """
        Read a sequence of values as indicated by the format string.

        Unless the "@" modifier is specified, the return value is a list of
        the values read. See the class docstring for format string details.
        """
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
        """
        Write a sequence of values as indicated by the format string.

        If the "*" modifier is specified, there must be exactly one additional
        argument; otherwise, args contains the values to be written. See the
        class docstring for format string details.
        """
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

class DataStoreServer(object):
    """
    DataStoreServer(addr, addrfamily=None) -> new instance

    The server part of remote datastores.

    addr is the socket address to bind to; addrfamily is the address family
    for it (defaulting to socket.AF_INET).

    In order to use a server, create an instance and call its main() method
    (potentially in a background thread).
    """

    class ClientHandler(object):
        """
        ClientHandler(parent, id, conn, addr) -> new instance

        A class for serving individual connections to a datastore server.

        parent is the DataStoreServer this instance belongs to; id is a
        numerical ID of this client handler (for disambiguation purposes);
        conn is the socket representing the connection to the client; address
        is the address of the client.

        Normally, users do not need to instantiate this class directly;
        DataStoreServer does that.
        """

        def __init__(self, parent, id, conn, addr):
            "Instance initializer; see class docstring for details."
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
            """
            Perform initialization tasks for this ClientHandler.

            This is invoked synchronously in the context of the server's main
            loop and should exit quickly.
            """
            self.logger.info('Connection from %s', self.addr)

        def close(self):
            """
            Clean up this client handler and all associated resources.

            The underlying socket is shut down and closed.
            """
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
            """
            Utility method for locking the underlying datastore.

            Do not call this is no datastore has been opened.
            """
            if self.locked == 0:
                self.datastore.lock()
            self.locked += 1

        def unlock(self, full=False):
            """
            Utility method for unlocking the underlying datastore.

            If full is true, any nesting level of locking is undone and the
            datastore is actually unlocked (this is useful when closing a
            datastore).
            """
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
            """
            Convenience method for writing an error message to the client.

            exc is either an error name, or a HKVError instance, or anything
            else. The corresponding error code is sent in the first two cases,
            and a generic error in the latter case.
            """
            if isinstance(exc, str):
                code = ERRORS[exc][0]
            elif isinstance(exc, HKVError):
                code = exc.code
            else:
                code = ERRORS['UNKNOWN'][0]
            self.codec.writef('ci', b'e', code)

        def main(self):
            """
            Run the main loop of this client handler.
            """
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
                    elif cmd == b'b':
                        if self.datastore is None:
                            self.write_error('NOSTORE')
                        self.lock()
                        self.codec.write_char(b'-')
                    elif cmd == b'f':
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
                                raise HKVError.for_name('NOSTORE')
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
        "Instance initializer; see the class docstring for details."
        if addrfamily is None: addrfamily = socket.AF_INET
        self.addr = addr
        self.addrfamily = addrfamily
        self.socket = None
        self.datastores = {}
        self._next_id = 1
        self._lock = threading.RLock()
        self.logger = logging.getLogger('server')

    def listen(self):
        """
        Actually create the socket of the server and start listening on it.

        Called by main().
        """
        self.logger.info('Listening on %s', self.addr)
        self.socket = socket.socket(self.addrfamily)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket.bind(self.addr)
        self.socket.listen(5)

    def accept(self):
        """
        Accept a single connection and spawn a handler thread for it.

        Called by main() repeatedly.
        """
        conn, addr = self.socket.accept()
        handler = self.ClientHandler(self, self._next_id, conn, addr)
        self._next_id += 1
        handler.init()
        spawn_thread(handler.main)

    def close(self):
        """
        Clean up the server's socket.

        Client handler threads continue working in the background. Called by
        main() after the main loop is interrupted.
        """
        self.logger.info('Closing')
        try:
            self.socket.close()
        except Exception:
            pass

    def get_datastore(self, name):
        """
        Retrieve a datastore for the given name or return a new one.

        Used by ClientHandler.
        """
        with self._lock:
            try:
                return self.datastores[name]
            except KeyError:
                ret = DataStore()
                self.datastores[name] = ret
                return ret

    def main(self):
        """
        Run the main loop of the server.
        """
        self.listen()
        try:
            while 1:
                try:
                    self.accept()
                except IOError:
                    pass
        finally:
            self.close()

class RemoteDataStore(BaseDataStore):
    """
    RemoteDataStore(addr, dsname=None, addrfamily=None) -> new instance

    A proxy for a remote DataStore.

    addr is the socket address to connect to; dsname is the name of the remote
    datastore to use (if omitted, a datastore must be explicitly opened using
    open() before use); addrfamily is the address family for the socket to be
    created (defaulting to socket.AF_INET).

    Prior to use, the connect() method has to be called; if no datastore name
    is configured when it is called, open() has to be called in addition after
    it but before use.
    """

    def __init__(self, addr, dsname=None, addrfamily=None):
        "Instance initializer; see the class docstring for details."
        if addrfamily is None: addrfamily = socket.AF_INET
        self.addr = addr
        self.dsname = dsname
        self.addrfamily = addrfamily
        self.socket = None
        self.codec = None
        self._lock = threading.RLock()

    def connect(self):
        """
        Establish a connection to the datastore server.

        If the dsname attribute is not None, an open() call is automatically
        performed after successfully connecting.
        """
        self.socket = socket.socket(self.addrfamily)
        self.socket.connect(self.addr)
        self.codec = Codec(self.socket.makefile('rb'),
                           self.socket.makefile('wb'))
        if self.dsname is not None: self.open(self.dsname)

    def open(self, dsname):
        """
        Open the named remote datastore.

        Any previously opened datastore is automatically detached from.
        """
        self._run_command(b'o', 's', dsname)

    def close(self):
        "Dispose of this DataStore; see BaseDataStore for details."
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
        """
        Helper method for executing a remote API command.

        cmd is the byte denoting the command to execute; format and args
        define values to send as arguments (the receiving side must read
        the arguments in a compatible manner; otherwise, both sides will lose
        synchronization and things will fall apart).

        After submitting the command, a response is received and decoded
        according to the data type indicated along with the response; if the
        response is an error or invalid, an HKVError exception is raised;
        otherwise, the value responded with is returned.
        """
        with self._lock:
            self.codec.write_char(cmd)
            self.codec.writef(format, *args)
            self.codec.flush()
            resp = self.codec.read_char()
            if resp == b'e':
                code = self.codec.read_int()
                raise HKVError.for_code(code)
            elif resp in b'sam-':
                return self.codec.readf('@' + resp.decode('ascii'))
            else:
                raise HKVError.for_name('NORESP')

    def _run_operation(self, opname, *args):
        "Helper method performing a remote DataStore operation."
        operation = DataStore._OPERATIONS[opname]
        return self._run_command(opname, operation[0], *args)

    def lock_remote(self):
        """
        Lock the remote datastore.

        Differently to lock(), this only performs the corresponding remote API
        command and does not acquire this object's local lock in addition;
        hence, multiple threads can use the object concurrently after this
        has been called.
        """
        return self._run_command(b'b', '')

    def unlock_remote(self):
        """
        Unlock the remote datastore.

        See the nodes to lock_remote() for details.
        """
        return self._run_command(b'f', '')

    def lock(self):
        "Lock this DataStore; see BaseDataStore for details."
        self._lock.acquire()
        self.lock()

    def unlock(self):
        "Unlock this DataStore; see BaseDataStore for details."
        try:
            self.unlock_remote()
        finally:
            self._lock.release()


    def get(self, path):
        "Retrieve a scalar at path; see BaseDataStore for details."
        return self._run_operation(b'g', path)

    def get_all(self, path):
        "Retrieve key-value pairs below path; see BaseDataStore for details."
        return self._run_operation(b'G', path)

    def list(self, path, lclass):
        "List some keys below path; see BaseDataStore for details."
        return self._run_operation(b'l', path, lclass)

    def put(self, path, value):
        "Store value at path; see BaseDataStore for details."
        return self._run_operation(b'p', path, value)

    def put_all(self, path, values):
        "Merge pairs from values below path; see BaseDataStore for details."
        return self._run_operation(b'P', path, values)

    def replace(self, path, values):
        "Store values at path; see BaseDataStore for details."
        return self._run_operation(b'r', path, values)

    def delete(self, path):
        "Delete the value at path; see BaseDataStore for details."
        return self._run_operation(b'd', path)

    def delete_all(self, path):
        "Delete everything below path; see BaseDataStore for details."
        return self._run_operation(b'D', path)

class TextDataStore(ConvertingDataStore):
    """
    TextDataStore(wrapped, nulldelim=False) -> new instance

    A sample implementation of ConvertingDataStore that converts data into
    Unicode strings.

    Paths delimited by slash characters (in particular, keys that contain
    slashes themselves cannot be used), or if nulldelim is true, NUL
    characters (in that case, keys containing NUL characters are unusable).

    Because of this ambiguity, this class is not recommended for general use
    and not included in the module's default export list.
    """

    def __init__(self, wrapped, nulldelim=False):
        super(TextDataStore, self).__init__(wrapped)
        self.delimiter = '\0' if nulldelim else '/'

    def import_key(self, key, fragment):
        "Import the given key; see ConvertingDataStore for details."
        if self.delimiter in key and fragment:
            raise ValueError('Non-path keys may not contain delimiters')
        key = key.encode('utf-8')
        if not fragment: key = key.split(self.delimiter.encode('utf-8'))
        return key

    def export_key(self, key, fragment):
        "Export the given key; see ConvertingDataStore for details."
        if not fragment: key = self.delimiter.encode('utf-8').join(key)
        return key.decode('utf-8', errors='replace')

    def import_value(self, value):
        "Import the given value; see ConvertingDataStore for details."
        return value.encode('utf-8')

    def export_value(self, value):
        "Export the given value; see ConvertingDataStore for details."
        return value.decode('utf-8', errors='replace')

def main_listen(params, no_timestamps, loglevel):
    """
    Helper function for running a server from the command line.

    Invoked by main().
    """
    if 'dsname' in params:
        raise SystemExit('ERROR: Must not specify datastore name when '
            'listening')
    if no_timestamps:
        logging.basicConfig(format='[%(name)s %(levelname)s] %(message)s',
                            level=loglevel)
    else:
        logging.basicConfig(format='[%(asctime)s %(name)s %(levelname)s] '
            '%(message)s', datefmt='%Y-%m-%d %H:%M:%S', level=loglevel)
    server = DataStoreServer(**params)
    try:
        server.main()
    except KeyboardInterrupt:
        pass

def main_command(params, nullterm, command, *args):
    """
    Helper function for running a single-command client from the command line.

    Invoked by main().
    """
    def ensure_args(min=1, max=None):
        "Helper function for checking the argument count of a command,"
        if len(args) < min:
            raise SystemExit('ERROR: Too few arguments for %s' % command)
        if max is not None and len(args) > max:
            raise SystemExit('ERROR: Too many arguments for %s' % command)
    if 'dsname' not in params:
        raise SystemExit('ERROR: Must specify datastore name when connecting')
    # Parse command line
    if command in ('get', 'get_all', 'delete', 'delete_all'):
        ensure_args(1, 1)
        cmdargs = args
    elif command == 'list':
        ensure_args(2, 2)
        flags = 0
        for char in args[1]:
            if char == 's':
                flags |= LCLASS_SCALAR
            elif char == 'n':
                flags |= LCLASS_NESTED
            elif char == 'a':
                flags |= LCLASS_ANY
            else:
                raise SystemExit('ERROR: Unrecognized listing class '
                    'character: %s' % char)
        cmdargs = (args[0], flags)
    elif command == 'put':
        ensure_args(2, 2)
        cmdargs = args
    elif command in ('put_all', 'replace'):
        ensure_args(1)
        values = {}
        for item in args[1:]:
            k, _, v = item.partition('=')
            values[k] = v
        cmdargs = (args[0], values)
    else:
        raise SystemExit('ERROR: Unknown command: %s' % command)
    # Create client and execute command
    client = RemoteDataStore(**params)
    try:
        client.connect()
    except IOError as exc:
        raise SystemExit('ERROR: %s' % exc)
    wrapper = TextDataStore(client, nullterm)
    try:
        result = getattr(wrapper, command)(*cmdargs)
    except (HKVError, ValueError) as exc:
        raise SystemExit('ERROR: %s' % exc)
    finally:
        client.close()
    if result is None:
        pass
    elif isinstance(result, str):
        print (result)
    elif isinstance(result, list):
        for item in result:
            print (item)
    elif isinstance(result, dict):
        sep = '\0' if nullterm else '='
        for key, value in result.items():
            print (key + sep + value)
    else:
        raise RuntimeError('Unrecognized result: %r' % (result,))

def main():
    """
    Main function for execution as a script.
    """
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument('--listen', '-l', action='store_true',
                   help='Enter server mode (instead of client mode)')
    p.add_argument('--url', '-u',
                   help='URL to serve on / to connect to')
    p.add_argument('--datastore', '-d', metavar='NAME',
                   help='Datastore to use (client mode only)')
    p.add_argument('--no-timestamps', '-T', action='store_true',
                   help='No timestamps on logs')
    p.add_argument('--loglevel', '-L', default='INFO', metavar='LEVEL',
                   help='Logging level (defaults to INFO)')
    p.add_argument('--null', '-0', action='store_true',
                   help='Null-terminate data blocks on output instead of '
                       'using human-readable characters (note that both keys '
                       'and values can contain null characters, so that this '
                       'does not ensure an unambiguous output)')
    p.add_argument('command', nargs='?',
                   help='Command to execute (client mode only)')
    p.add_argument('arg', nargs='*',
                   help='Additional arguments to the command')
    result = p.parse_args()
    if bool(result.listen) == bool(result.command):
        raise SystemExit('ERROR: Must specify either -l or command.')
    try:
        if result.url is not None:
            params = parse_url(result.url)
        else:
            url_string = os.environ.get('HKV_URL')
            if url_string:
                params = parse_url(url_string)
            else:
                params = {'addr': DEFAULT_ADDRESS}
    except ValueError:
        raise SystemExit('ERROR: Invalid hkv:// URL: %s' % result.url)
    if result.datastore is not None:
        params['dsname'] = result.datastore.encode('utf-8')
    else:
        dsname_string = os.environ.get('HKV_DATASTORE')
        if dsname_string:
            params['dsname'] = dsname_string.encode('utf-8')
    if result.listen:
        main_listen(params, result.no_timestamps, result.loglevel)
    else:
        main_command(params, result.null, result.command, *result.arg)

if __name__ == '__main__': main()
