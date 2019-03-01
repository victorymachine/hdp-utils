#!/usr/bin/env python

import argparse
import socket
import time
import subprocess

CARBON_SERVER = 'hostname'
CARBON_PORT = 2003

HOST = "hdpmaster1"

cmd = "for USER in $(ps haux | awk '{print $1}' | sort -u); do ps haux | awk -v user=$USER '$1 ~ user { sum += $4} END { print user, sum; }' ; done"
p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
result = p.stdout.read().rstrip()
for user in result.split('\n'):
        usage=user.split(' ')
        timestamp = int(time.time())
        time.sleep(2)
        if float(usage[1]) > 0:
                message = 'server.%s.memory.user.%s %s %d\n' % (HOST,usage[0], float(usage[1])/100 * 251, timestamp)

                #print 'sending message:\n%s' % message
                sock = socket.socket()
                sock.connect((CARBON_SERVER, CARBON_PORT))
                sock.sendall(message)
                sock.close()

