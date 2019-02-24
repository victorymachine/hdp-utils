#!/usr/bin/env python

import argparse
import socket
import time
import subprocess

CARBON_SERVER = '192.168.0.2'
CARBON_PORT = 2003

HOST = "hdpmaster1"

cmd = "hdfs dfs -ls /user/vicmac/source_files/ |awk '{print $8}'"
p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
result = p.stdout.read().rstrip()
timestamp = int(time.time())

for d in result.split("\n")[1:]:
        cmd = "hdfs dfs -du -s %s" % (d)
        p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        result = p.stdout.read().rstrip()
        result = result.split(" ")
        message = 'hdfs.source_files.datasource.%s %s %s\n' % (result[2].split("/")[-1],result[0], timestamp)

        sock = socket.socket()
        sock.connect((CARBON_SERVER, CARBON_PORT))
        sock.sendall(message)
        sock.close()

cmd = "hdfs dfs -ls /user/vicmac/source/ |awk '{print $8}'"
p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
result = p.stdout.read().rstrip()
timestamp = int(time.time())
for d in result.split("\n")[1:]:
        cmd = "hdfs dfs -du -s %s" % (d)
        p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        result = p.stdout.read().rstrip()
        result = result.split(" ")
        message = 'hdfs.source.datasource.%s %s %s\n' % (result[2].split("/")[-1],result[0], timestamp)
        sock = socket.socket()
        sock.connect((CARBON_SERVER, CARBON_PORT))
        sock.sendall(message)
        sock.close()

cmd = "hdfs dfs -ls /apps/hive/warehouse/ |awk '{print $8}'"
p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
result = p.stdout.read().rstrip()
timestamp = int(time.time())
for d in result.split("\n")[1:]:
        cmd = "hdfs dfs -du -s %s" % (d)
        p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        result = p.stdout.read().rstrip()
        result = result.split(" ")
        message = 'hdfs.hive.warehouse.%s %s %s\n' % (result[2].split("/")[-1].replace(".db",""),result[0], timestamp)
        sock = socket.socket()
        sock.connect((CARBON_SERVER, CARBON_PORT))
        sock.sendall(message)
        sock.close()

cmd = "hdfs dfs -ls /user/ |awk '{print $8}'"
p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
result = p.stdout.read().rstrip()
timestamp = int(time.time())
for d in result.split("\n")[1:]:
        cmd = "hdfs dfs -du -s %s" % (d)
        p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        result = p.stdout.read().rstrip()
        result = result.split(" ")
        message = 'hdfs.user.%s %s %s\n' % (result[2].split("/")[-1],result[0], timestamp)
        sock = socket.socket()
        sock.connect((CARBON_SERVER, CARBON_PORT))
        sock.sendall(message)
