# -*- coding: utf-8 -*-
"""Lowest-level BaseX client-server communication"""

import re
import io
import socket

C00 = b'\x00'
CFF = b'\xFF'
I00 = 0
IFF = 255

r_escapeable = re.compile(b'[{}{}]'.format(C00, CFF))
r_escaped = re.compile(b'{}[{}{}]'.format(CFF, C00, CFF))
r_unescaped_null = re.compile(b'(?<!{}){}'.format(CFF, C00))


class Commands(object):
    QUERY = b'\0'
    CLOSE = b'\2'
    BIND = b'\3'
    RESULTS = b'\4'
    EXECUTE = b'\5'
    INFO = b'\6'
    OPTIONS = b'\7'
    CREATE = b'\8'
    ADD = b'\9'
    WATCH = b'\10'
    UNWATCH = b'\11'
    REPLACE = b'\12'
    STORE = b'\13'
    CONTEXT = b'\14'
    UPDATING = b'\30'
    FULL = b'\31'


class BoundedBuffer(bytearray):
    def __new__(cls, size=io.DEFAULT_BUFFER_SIZE):
        return bytearray.__new__(cls, size)

    def __init__(self):
        bytearray.__init__(self)
        self.reset()

    def reset(self):
        self.dataslice = slice(0, 0)

    @property
    def datalen(self):
        ds = self.dataslice
        return ds.stop-ds.start

    def slide_to(self, idx):
        self.dataslice = slice(self.dataslice.start + idx, self.dataslice.stop)

    def view(self, endat=None):
        if endat is None:
            endat = len(self)
        endidx = min(self.dataslice.stop, endat)
        return memoryview(self)[self.dataslice.start:endidx]

    def isempty(self):
        return not self.datalen

    def readintome(self, readintomethod):
        bytesread = readintomethod(self)
        self.dataslice = slice(0, bytesread)


class BufferedSocket(object):
    """Buffers a socket

    This is *not* a threadsafe object--do not share!
    """
    def __init__(self, socketfactory):
        """Initialize the socket wrapper

        socketfactory -- a callable which returns a socket object
                         which this object will wrap
        """
        self._soc = socketfactory()
        self._buf = BoundedBuffer()

    @classmethod
    def connect(cls, address):
        """Connect to address (host, port)"""
        return cls(lambda: socket.create_connection(address))

    def close(self):
        return self._soc.close()

    def __enter__(self):
        return self

    def __exit__(self, type_, value, traceback):
        self.close()
        return False

    def _recv_next_iter(self):
        """Yield memoryviews of buffers up to and including a null terminator.

        Used to implement recv_next_iter() and recv_next(). Use carefully:
        returned memoryviews must be materialized before the next yield!
        """
        do_readinto = self._soc.recv_into
        buf = self._buf
        while True:
            if buf.isempty():
                buf.readintome(do_readinto)
            found = next_null(buf.view())
            assert found != 0
            # if found is None, whole buffer is copied
            yield buf.view(found)
            if found:
                buf.slide_to(found)
                return

    def recv_next_iter(self):
        """Yield bytes from socket up to and including a null terminator.

        This method is the same as recv_next() except it yields bytes as they
        are available instead of buffering them all into a bytearray. This
        can reduce latency for large responses, but it will be less efficient.
        """
        for mv in self._recv_next_iter():
            yield mv.tobytes()

    def recv_next(self):
        """Receive bytes up to and including an unescaped null terminator"""
        return bytearray(self._recv_next_iter())


def next_null(ba, r_null=r_unescaped_null.search):
    """Return a memoryview of bytes up to and including the next unescaped \\0 or None if not found"""
    m = r_null(ba)
    if not m:
        return None
    return m.end()


def escape_bytearray(ba, start=0, end=None, r_escapeable=r_escapeable, IFF=IFF):
    """Prefix escapeable characters in bytearray in-place; return number of escapes done

    If start or end are supplied, will escape only data in that slice. Any
    bytes before or after will be untouched, but the index of following bytes
    may increase.

    Return value will be number of bytes added.

    """
    if end is None:
        end = len(ba)
    findescapeable = lambda s, e, ba=ba, srch=r_escapeable.search: srch(ba, s, e)
    n_matches = 0
    while True:
        m = findescapeable(start, end)
        if m is None:
            break
        ridx, start = m.span()
        ba.insert(ridx, IFF)
        start += 1
        end += 1
        n_matches += 1
    return n_matches


def unescape_bytearray(ba, r_escaped=r_escaped):
    """Remove the escape characters from a bytearray in-place; return length of valid data"""
    # need at least two characters for an escape sequence
    if len(ba) < 2:
        return len(ba)
    toremove = [m.start() for m in r_escaped.finditer(ba)]
    return repack(ba, toremove)


def repack(ba, holes):
    """Repack bytes in a bytearray removing indexes in sorted list "holes"

    Returns the slice offset where the packed data ends, which is always
    len(ba)-len(holes).
    """
    lenba = len(ba)
    if not holes:
        return lenba
    mv = memoryview(ba)
    dslices = dataslices(holes, lenba)
    pstart = 0
    for i, (dstart, dend) in enumerate(dslices):
        pend = pstart + dend - dstart
        mv[pstart:pend] = mv[dstart:dend]
        pstart = pend
    return pend


def dataslices(holes, clen=None):
    """Return a list of slices between sorted list of hole indexes

    holes must be pre-sorted!

    clen is the length of the collection. If not provided a slice of (_, None)
    will always be appended.
    """
    if clen <= 0 and clen is not None:
        return []
    if not holes:
        return [(0, clen)]
    orig_holes = holes
    dslices = []
    if holes[0] > 0:
        dslices.append((0, holes[0]))
        lasthole = holes[0] + 1
        holes = holes[1:]
    else:
        lasthole = 1
    for hole in holes:
        if hole > lasthole:
            dslices.append((lasthole, hole))
        elif hole < lasthole - 1:
            raise ValueError('holes argument is not sorted: {!r}'.format(orig_holes))
        lasthole = hole + 1
    if lasthole < clen or clen is None:
        dslices.append((lasthole, clen))
    return dslices
