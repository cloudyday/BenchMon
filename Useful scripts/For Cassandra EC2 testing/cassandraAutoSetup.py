#!/usr/bin/env python

import socket
import re
import subprocess
import shlex
import os

host = socket.gethostbyname(socket.gethostname())

f = open("/etc/cassandra/cassandra.yaml","r")
cass = f.read()
cass = re.sub(r"listen_address: \d+\.\d+\.\d+\.\d+", "listen_address: "+host, cass)
f.close()
f = open("/etc/cassandra/cassandra.yaml","w")
f.write(cass)
f.close()

args = shlex.split("service cassandra start")
subprocess.Popen(args, stdout=open("/dev/null","wb"))