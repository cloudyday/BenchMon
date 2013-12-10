#!/usr/bin/env python

__author__ = 'Oliver Roess'

import shlex, subprocess
import os
import ConfigParser
import boto # AWS Library
import logging
import pika
from cStringIO import StringIO
import threading
import re
import socket
import time
import sys


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("CassandraMonitoring")
#hostname = socket.gethostname()
hostname = socket.gethostbyname(socket.gethostname())


##### Configuration params START #####
mqHost = "141.52.218.31"
routingKeys = ["all"]
home = os.path.expanduser("~")
outputDir = home+"/MonitoringScripts/output/"
##### Configuration params END #####

##### Bootstrap START #####
if len(sys.argv[1:]) != 1:
    sys.exit("Usage: nodegroup")
routingKeys.append(sys.argv[1])

f = open(outputDir+"../boto.cfg", "r")
contents = f.read()
f.close()
f = open(home+"/.boto", "w")
f.write(contents)
f.close()

conf = ConfigParser.RawConfigParser()
conf.read(outputDir+"../awsSettings.cfg")
cred = conf.get("Credentials", "aws_access_key_id"), conf.get("Credentials", "aws_secret_access_key")

connection = pika.BlockingConnection(pika.ConnectionParameters(
        host=mqHost))
channel = connection.channel()

channel.exchange_declare(exchange='OCcass',
                         type='direct')

result = channel.queue_declare(exclusive=True)
queue_name = result.method.queue

for route in routingKeys:
    channel.queue_bind(exchange='OCcass',
                       queue=queue_name,
                       routing_key=route)

monitoringOn = True

# run in loop, so that this program is always ready to start a monitoring session
# there is no break inside the while-loop. this loop will only exit by forced interruption or on error
while True:
    while monitoringOn:

        logger.info("Switched ON")

        bucketName = None
        monitoringDataDir = None
        startOfTest = None
        samplingInterval = None
        afterPeriod = None

        ## IDLE-state START
        # wait for configuration params
        for method, header, body in channel.consume(queue_name):
            if body.find("<OFF>") > -1:
                monitoringOn = False
                channel.basic_ack(method.delivery_tag)
                break
            if body.find("<CONFIGURATION>") == -1:
                channel.basic_ack(method.delivery_tag)
                continue

            body = body.replace("<CONFIGURATION>","")
            f = StringIO(body)
            conf = ConfigParser.RawConfigParser()
            conf.readfp(f)
            bucketName = conf.get("S3", "bucketName")
            monitoringDataDir = conf.get("S3", "monitoringDataDir")
            startOfTest = conf.get("test", "startOfTest")
            samplingInterval = int(conf.get("test", "samplingInterval"))
            afterPeriod = int(conf.get("test", "afterPeriod"))


            f.close()
            channel.basic_ack(method.delivery_tag)
            logger.info("Received CONFIGURATION")
            break

        
        ##### Bootstrap END #####
        if monitoringOn == False: break

        # wait for START command
        for method, header, body in channel.consume(queue_name):
            if body.find("<START>") == -1:
                channel.basic_ack(method.delivery_tag)
                continue

            channel.basic_ack(method.delivery_tag)
            break

        logger.info("Received START Command")
        ## IDLE-state END

        # getting cfstats (just txt output)
        command_line = "nodetool cfstats"
        args = shlex.split(command_line)
        p = subprocess.Popen(args, stdout=subprocess.PIPE)
        p.wait()
        output = p.stdout.read()
        f = open(outputDir+"nodetool-cfstats-before.txt", "w")
        print >>f, output
        f.close()

        logger.info("Doing Sampling")
        times = []
        cacheHitRate = ([],[]) # key cache, row cache
        compactionStats = ([],[],[]) # pending tasks, sum-over-tasks(completed bytes), sum-over-tasks(total size)

        txtInfo = open(outputDir+"nodetool-info.txt","w")
        txtCompaction = open(outputDir+"nodetool-compactionstats.txt","w")

        # no threadsafe implementation // just making it work, quick and dirty
        # every thread/function that uses the above defined variables, has access to them at any time
        def runMonitor(stop_event):
            i = 0
            while not stop_event.is_set():
                i += 1
                times.append(samplingInterval*i)
                print >>txtInfo, "time: "+str(samplingInterval*i)
                print >>txtCompaction, "time: "+str(samplingInterval*i)

                # run nodetool processes
                command_line = "nodetool info"
                args = shlex.split(command_line)
                p1 = subprocess.Popen(args, stdout=subprocess.PIPE)
                command_line = "nodetool compactionstats"
                args = shlex.split(command_line)
                p2 = subprocess.Popen(args, stdout=subprocess.PIPE)

                # Parsing nodetool info output
                keyHitRate = ''
                rowHitRate = ''
                while True:
                    line = p1.stdout.readline()
                    if line == '':
                        break
                    # if line = '', the stdout is empty
                    if re.match(r"Key", line):
                        line = line.strip()
                        print >>txtInfo, line
                        line = re.sub(r"K.*requests, ", "", line)
                        line = re.sub(r" recent.*", "", line)
                        if line != "NaN":
                            keyHitRate = line

                    if re.match(r"Row", line):
                        line = line.strip()
                        print >>txtInfo, line
                        line = re.sub(r"R.*requests, ", "", line)
                        line = re.sub(r" recent.*", "", line)
                        if line != "NaN":
                            rowHitRate = line

                cacheHitRate[0].append(keyHitRate)
                cacheHitRate[1].append(rowHitRate)

                # Parsing nodetool compactionstats output
                pendingTasks = "0"
                sumCompactionStats = 0 # in MB
                sumCompactionStatsTotal = 0 # in MB
                while True:
                    line = p2.stdout.readline()
                    if line == '':
                        break
                    # if line = '', the stdout is empty
                    if re.match(r"pending tasks: 0", line):
                        print >>txtCompaction, "tasks: 0"
                        compactionStats[0].append('0')
                        #break # no need to go further down the file. there's nothing interesting in case of 0 tasks

                    elif re.match(r"pending tasks: [1-9]", line):
                        line = line.strip()
                        line = re.sub(r"pending tasks: ", "", line)
                        print >>txtCompaction, "tasks: "+line
                        pendingTasks = line

                    elif re.match(r"\s+C", line):
                        line = line.strip()
                        print >>txtCompaction, line
                        split = re.split(r"\s+", line)
                        sumCompactionStats += float(split[3])/1000000
                        sumCompactionStatsTotal += float(split[4])/1000000

                compactionStats[0].append(pendingTasks)
                compactionStats[1].append(str(sumCompactionStats))
                compactionStats[2].append(str(sumCompactionStatsTotal))

                stop_event.wait(samplingInterval)

        # run the thread
        monitorStop = threading.Event()
        t = threading.Thread(target=runMonitor, args=(monitorStop,))
        t.start()

        # wait for STOP command
        for method, header, body in channel.consume(queue_name):
            if body.find("<STOP>") == -1:
                channel.basic_ack(method.delivery_tag)
                continue
            channel.basic_ack(method.delivery_tag)
            break


        logger.info("Received STOP Command")
        logger.info("After Period....")
        time.sleep(afterPeriod)
        monitorStop.set()
        t.join()
        logger.info("STOPPED Monitoring")
        txtInfo.close()
        txtCompaction.close()

        print str(len(times))+" "+str(len(cacheHitRate[0]))+" "+str(len(compactionStats[0]))

        logger.info("WRITING CSV")
        csv = open(outputDir+"nodetool.csv", "w")
        print >> csv, "time (s), KeyCache Hitrate, RowCache Hitrate, Compaction Tasks, Sum(Migrated of Total) (MB), Sum(Total of Tasks) (MB)"

        for i in range(len(times)):
            csv.write(str(times[i])+","+cacheHitRate[0][i]+","+cacheHitRate[1][i]+","+compactionStats[0][i]+","+compactionStats[1][i]+","+compactionStats[2][i]+"\n")

        csv.close()

        # getting cfstats (just txt output)
        command_line = "nodetool cfstats"
        args = shlex.split(command_line)
        p = subprocess.Popen(args, stdout=subprocess.PIPE)
        p.wait()
        output = p.stdout.read()
        f = open(outputDir+"nodetool-cfstats-after.txt", "w")
        print >>f, output
        f.close()


        logger.info("CONNECTING TO S3")
        s3 = boto.connect_s3(cred[0], cred[1])
        bucket = s3.get_bucket(bucketName)

        logger.info("START uploading data to S3")
        from boto.s3.key import Key
        k = Key(bucket, monitoringDataDir+"/"+startOfTest+"/raw/"+startOfTest+"--"+hostname+"---nodetool-cfstats-before.txt")
        k.set_contents_from_filename(outputDir+"nodetool-cfstats-before.txt")
        k = Key(bucket, monitoringDataDir+"/"+startOfTest+"/raw/"+startOfTest+"--"+hostname+"---nodetool-cfstats-after.txt")
        k.set_contents_from_filename(outputDir+"nodetool-cfstats-after.txt")
        k = Key(bucket, monitoringDataDir+"/"+startOfTest+"/raw/"+startOfTest+"--"+hostname+"---nodetool-info.txt")
        k.set_contents_from_filename(outputDir+"nodetool-info.txt")
        k = Key(bucket, monitoringDataDir+"/"+startOfTest+"/raw/"+startOfTest+"--"+hostname+"---nodetool-compactionstats.txt")
        k.set_contents_from_filename(outputDir+"nodetool-compactionstats.txt")
        k = Key(bucket, monitoringDataDir+"/"+startOfTest+"/"+startOfTest+"--"+hostname+"---nodetool.csv")
        k.set_contents_from_filename(outputDir+"nodetool.csv")
        logger.info("FINISHED uploading data to S3")
        s3.close()

    # this code will only be reached if "monitoringOn" was set to false
    logger.info("Switched OFF")

    channel.basic_publish(exchange='OCcass',
                      routing_key='controller',
                      body="<LOG>"+hostname+": CassandraMonitoring OFF")


    for method, header, body in channel.consume(queue_name):
        if body.find("<ON>") == -1:
            channel.basic_ack(method.delivery_tag)
            continue

        monitoringOn = True

        channel.basic_publish(exchange='OCcass',
                  routing_key='controller',
                  body="<LOG>"+hostname+": CassandraMonitoring ON")
        channel.basic_ack(method.delivery_tag)
        break


connection.close()
