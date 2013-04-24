import socket
import hashlib
import sys

def authenticate(soc, username, password):
    def md5(data):
        return hashlib.md5(data).hexdigest()
        
    timestamp = soc.recv(100)[:-1]
    authreply = username+chr(0)+md5(md5(password) + timestamp)+chr(0)
    soc.sendall(authreply)
    assert soc.recv(1) == chr(0)

def send(soc, opcode, *args):
    tosend = bytearray()
    tosend.append(opcode)
    for arg in args:
        tosend.extend(arg+chr(0))
    soc.sendall(tosend)
    return bytes(tosend)

def recv(soc):
    b = bytearray(100000)
    nbytes = soc.recv_into(b)
    return bytes(b[:nbytes])

def connect(host, port):
    soc = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    soc.connect((host, port))
    return soc

import re
quotedstring = re.compile(r"(\d+)|(['\"])(.*?)(?<!\\)\2")


def parse_str(s):
    parts = [chr(int(m.group(1))) if m.group(1) else m.group(3) for m in quotedstring.finditer(s)]
    if not len(parts):
        return None
    if len(parts[0]) > 1:
        parts = [parts[0][0], parts[0][1:]] + parts[1:]
    return parts
   
soc = connect('localhost',1984)
authenticate(soc, 'admin', 'admin')
while True:
    input = raw_input('? ')
    parsed = parse_str(input)
    if parsed:
        sent = send(soc, *parsed)
        print '>', repr(sent)
    print '<', repr(recv(soc))
    
    