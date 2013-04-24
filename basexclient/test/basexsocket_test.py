# -*- coding: utf-8 -*-
"""Test basexsocket code"""
from .. import basexsocket
from StringIO import StringIO
from nose.tools import eq_, ok_, raises
from itertools import chain


def test_dataslices():
    # cases = [(holes, collection_length, expected), ...]
    cases = [
        ([1], 0, []),             # zero length
        ([], 10, [(0, 10)]),      # no holes
        ([], None, [(0, None)]),  # no holes, no length
        ([0], 11, [(1, 11)]),     # hole at start
        ([10], 11, [(0, 10)]),    # hole at end
        ([10], None, [(0, 10), (11, None)]),  # no length
        ([1], 11, [(0, 1), (2, 11)]),         # hole in middle
        # holes at beginning, middle, and end:
        ([0, 5, 8, 13], 14, [(1, 5), (6, 8), (9, 13)]),
        # consecutive holes:
        ([1, 2, 6, 10, 11, 12, 16], 17, [(0, 1), (3, 6), (7, 10), (13, 16)]),
        # consecutive and terminal holes:
        ([0, 1, 3, 4, 8, 12, 13, 14, 18, 19, 20], 21,
            [(2, 3), (5, 8), (9, 12), (15, 18)]),
    ]
    for holes, clen, expect in cases:
        yield check_dataslices, holes, clen, expect


def check_dataslices(holes, clen, expect):
    got = basexsocket.dataslices(holes, clen)
    eq_(got, expect)


@raises(ValueError)
def test_dataslices_unsorted_holes():
    holes = [2, 1]
    basexsocket.dataslices(holes)


def test_repack():
    # expected value for each string is a bytearray with underscores removed
    cases = [
        b'0123456789',
        b'_0123456789',
        b'0123456789_',
        b'0_123456789',
        b'0_123456789',
        b'0_123_456789',
        b'0_123_45_6789_',
        b'012345678_9_',
        b'__0123456789',
        b'0__123456789',
        b'__0__123_456___789__',
    ]
    for case in cases:
        ba = bytearray(case)
        holes = [i for i, c in enumerate(case) if c == b'_']
        expected_packed = case.replace(b'_', b'')
        expected_retval = len(expected_packed)
        yield check_repack, ba, holes, expected_packed, expected_retval


def check_repack(ba, holes, expected_packed, expected_retval):
    ba_copy = ba[:]
    got = basexsocket.repack(ba_copy, holes)
    eq_(got, expected_retval)
    eq_(ba_copy[:expected_retval], expected_packed)


def test_unescape_bytearray():
    cases = [
        (b'1', b'1'),                # no escapes
        (b'\xFF', b'\xFF'),          # false positive
        (b'\xFF1234', b'\xFF1234'),  # false positive
        (b'\xFF\x001234', b'\x001234'),
        (b'12\xFF4\x00\xFF\x007', b'12\xFF4\x00\x007'),
    ]
    for arg, rv in cases:
        yield check_unescape_bytearray, bytearray(arg), rv


def check_unescape_bytearray(ba, expected):
    ba_copy = ba[:]
    packedlen = basexsocket.unescape_bytearray(ba_copy)
    eq_(ba_copy[:packedlen], expected)


def test_escape_bytearray():
    cases = [
        (b'', b''),
        (b'12345', b'12345'),  # no escapes
        (b'\xFF', b'\xFF\xFF'),
        (b'\x001234', b'\xFF\x001234'),
        (b'\xFF\x001234', b'\xFF\xFF\xFF\x001234'),
        (b'12\xFF4\x00\xFF\x007', b'12\xFF\xFF4\xFF\x00\xFF\xFF\xFF\x007'),
    ]
    for arg, expected in cases:
        yield check_escape_bytearray, bytearray(arg), 0, None, expected


def check_escape_bytearray(ba, start, end, expected_escaped):
    ba_copy = ba[:]
    replacements = basexsocket.escape_bytearray(ba_copy, start, end)
    eq_(replacements, len(ba_copy) - len(ba))
    eq_(ba_copy, expected_escaped)


def test_escape_bytearray_slice():
    # input, start, end, expected
    cases = [
        (b'', 1, 0, b''),  # impossible slicing
        (b'012\x0045\xFF', 3, None, b'012\xFF\x0045\xFF\xFF'),
        (b'\xFF12\x0045\xFF', 0, 4, b'\xFF\xFF12\xFF\x0045\xFF'),
        (b'\x0012\xFF\xFF45\x00', 3, 5, b'\x0012\xFF\xFF\xFF\xFF45\x00'),
    ]
    for arg, start, end, expected in cases:
        yield check_escape_bytearray, bytearray(arg), start, end, expected


def test_next_null():
    cases = [
        (b'12345', None),
        (b'\xFF\x00345', None),
        (b'12\xFF\x005', None),
        (b'123\xFF\x00', None),
        (b'\x002345', 1),
        (b'1\x00345', 2),
        (b'1234\x00', 5),
        (b'\xFF\x003\x005', 4),
        (b'\xFF\x0034\x00', 5),
        (b'1\x0034\x00', 2),
        (b'\x002\xFF\x005\x00', 1),
    ]
    for s, expect in cases:
        yield check_next_null, s, expect


def check_next_null(s, expected):
    found = basexsocket.next_null(s)
    eq_(found, expected)


def test_bounded_buffer():
    s = b'0123456789'

    def readinto(buf):
        buf[:len(s)] = s
        return len(s)

    bb = basexsocket.BoundedBuffer()
    assert isinstance(bb, bytearray)
    assert bb.isempty()
    bb.readintome(readinto)
    eq_(bb.datalen, 10)
    assert not bb.isempty()
    assert isinstance(bb.view(), (buffer, memoryview))
    eq_(bb.view(),  s)
    eq_(bb.view(5), s[:5])
    assert bb.view(100) == s
    bb.slide_to(4)
    eq_(bb.datalen,  6)
    eq_(bb.view(),  s[4:])
    eq_(bb.view(100),  s[4:])
    eq_(bb.view(1),  s[4])
    assert not bb.isempty()
    bb.readintome(readinto)
    eq_(bb.view(), s)
    bb.reset()
    eq_(bb.view(), b'')
    assert bb.isempty()


class MockSocket(object):
    def __init__(self, data):
        self._data = StringIO(data)
        self._closed = False

    def recv_into(self, buf):
        maxbytes = len(buf)
        chunk = self._data.read(maxbytes)
        buf[:len(chunk)] = chunk
        return len(chunk)

    def close(self):
        self._closed = True


def create_bufferedsocket(data):
    soc = lambda: MockSocket(data)
    return basexsocket.BufferedSocket(soc)


def test_bufferedsocket_read_next():
    data = b'0123456789\x00\x01'
    with create_bufferedsocket(data) as bs:
        eq_(bs.read_byte(), b'0')
        eq_(bs.read_next(), b'123456789\x00')
        eq_(bs.read_byte(), b'\x01')


def test_bufferedsocket_multiboundary():
    """Test how bufferedsocket works when multiple socket reads are needed"""
    trailer =  b'\x0012\xFF\xFF345\x00\x01'
    datagens = chain(
        (b'_' for i in xrange(0xfff)),
        b'X',
        (b'_' for i in xrange(0xfff)),
        b'X',
        (b'_' for i in xrange(0xfff)),
        b'X',
        (b'_' for i in xrange(0xfff)),
        b'X',
        trailer
    )
    data = bytearray(datagens)
    with create_bufferedsocket(data) as bs:
        eq_(bs.read_next(), data[:0x4001])
        eq_(bs.read_next(), trailer[1:9])

def test_bufferedsocket_read_iter():
    data = b'01234\xFF\x00\x0056789\x00\x01'
    with create_bufferedsocket(data) as bs:
        eq_(list(bs.read_next_iter()), [b'01234\xFF\x00\x00'])

    data = bytearray(chain((b'X' for i in xrange(0x4000)), b'\x00\x00'))
    with create_bufferedsocket(data) as bs:
        chunkiter = bs.read_next_iter()
        chunk = chunkiter.next()
        ok_(len(chunk) < 0x4000)
        ok_(all(c == b'X' for c in chunk))
        # round 2
        chunk = chunkiter.next()
        ok_(len(chunk) < 0x4000)
        ok_(all(c == b'X' for c in chunk))
        # last chunk
        for c in chunkiter:
            chunk = c
        ok_(len(chunk) < 0x4000)
        ok_(all(c == b'X' for c in chunk[:-1]))
        ok_(c[-1] == b'\x00')





