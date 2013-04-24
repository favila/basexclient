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
    QUERY = b'\x00'
    CLOSE = b'\x02'
    BIND = b'\x03'
    RESULTS = b'\x04'
    EXECUTE = b'\x05'
    INFO = b'\x06'
    OPTIONS = b'\x07'
    CREATE = b'\x08'
    ADD = b'\x09'
    WATCH = b'\x0A'
    UNWATCH = b'\x0B'
    REPLACE = b'\x0C'
    STORE = b'\x0D'
    CONTEXT = b'\x0E'
    UPDATING = b'\x1E'
    FULL = b'\x1F'


class BoundedBuffer(bytearray):
    def __init__(self, size=io.DEFAULT_BUFFER_SIZE):
        super(BoundedBuffer, self).__init__(size)
        self.dataslice = slice(0, 0)

    def reset(self):
        self.dataslice = slice(0, 0)

    @property
    def datalen(self):
        ds = self.dataslice
        return ds.stop-ds.start

    def slide_to(self, idx):
        if idx is None:
            self.reset()
        else:
            self.dataslice = slice(self.dataslice.start + idx, self.dataslice.stop)

    def view(self, nbytes=None):
        """Return memoryview of next bytes without consuming them.

        ``nbytes`` is the max number of bytes to return (maybe fewer).
        """
        if nbytes is None:
            nbytes = len(self)
        endidx = min(self.dataslice.stop, self.dataslice.start + nbytes)
        return memoryview(self)[self.dataslice.start:endidx]

    def view_buf(self, nbytes=None):
        """Return buffer of next bytes without consuming them.

        Only needed by re objects in Python 2.7, which can't search
        a memoryview
        """
        if nbytes is None:
            nbytes = len(self)
        maxlen = min(self.dataslice.stop-self.dataslice.start, nbytes)
        return buffer(self, self.dataslice.start, maxlen)

    def isempty(self):
        return not self.datalen

    def readintome(self, readintomethod):
        bytesread = readintomethod(self)
        #pylint: disable=W0201
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

    def _read_next_iter(self):
        """Yield memoryviews of buffers up to and including a null terminator.

        Used to implement recv_next_iter() and recv_next(). Use carefully:
        returned memoryviews must be materialized before the next yield!
        """
        do_readinto = self._soc.recv_into
        buf = self._buf
        while True:
            if buf.isempty():
                buf.readintome(do_readinto)
            found = next_null(buf.view_buf())
            assert found != 0
            # if found is None, whole buffer is copied
            yield buf.view(found)
            buf.slide_to(found)
            if found is not None:
                return

    def read_next_iter(self):
        """Yield bytes from socket up to and including a null terminator.

        This method is the same as recv_next() except it yields bytes as they
        are available instead of buffering them all into a bytearray. This
        can reduce latency for large responses, but it will be less efficient.
        """
        for mv in self._read_next_iter():
            yield mv.tobytes()

    def read_next(self):
        """Receive bytes up to and including an unescaped null terminator"""
        # This looks strange, but it's to reduce fragmentation
        # Almost always we will only have to grab one chunk, so we allocate
        # its memory all at once instead of growing an empty bytearray.
        ba = None
        for chunk in self._read_next_iter():
            if ba is None:
                ba = bytearray(chunk)
            else:
                ba.extend(chunk)
        return ba if ba is not None else bytearray()

    def read_byte(self):
        """Return the next byte in the socket"""
        do_readinto = self._soc.recv_into
        buf = self._buf
        if buf.isempty():
            buf.readintome(do_readinto)
        nextbyte = buf.view(1).tobytes()
        buf.slide_to(1)
        return nextbyte

    def write(self, data):
        """Return bytes of data sent in one system call."""
        return self._soc.send(data)

    def write_all(self, data):
        """Write all data."""
        self._soc.sendall(data)


def next_null(ba, r_null=r_unescaped_null.search):
    """Return memoryview of bytes up to and including the next unescaped null

    Returns None if not found."""
    m = r_null(ba)
    if not m:
        return None
    return m.end()


def escape_bytearray(ba, start=0, end=None, r_escapeable=r_escapeable, IFF=IFF):
    """Return number of escaped bytes after in-place escape of characters

    If start or end are supplied, will escape only data in that slice. Any
    bytes before or after will be untouched, but the index of following bytes
    may increase.

    Return value will be number of bytes added.

    """
    #pylint: disable=W0621
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
    """Return length of data after in-place removal of escape characters"""
    #pylint: disable=W0621
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
    for dstart, dend in dslices:
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
    #pylint: disable=C0301
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
