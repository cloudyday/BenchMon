#!/usr/bin/env python

__author__ = 'Oliver Roess'

import shlex, subprocess
import os
import ConfigParser
import boto # AWS Library
import YCSBOutputParser
import logging
import pika
from cStringIO import StringIO
import time


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("YCSBMonitoring")

##### Configuration params START #####
mqHost = "141.52.218.31"
home = os.path.expanduser("~")
outputDir = home+"/MonitoringScripts/output/"
ycsbDirectory = home+"/YCSB"

ycsbCommandInitial = ycsbDirectory+"/bin/ycsb run cassandra-10 -s" \
                            " -P "+outputDir+"../ycsb-workload"
##### Configuration params END #####

##### Bootstrap START #####
f = open(outputDir+"../boto.cfg", "r")
contents = f.read()
f.close()
f = open(home+"/.boto", "w")
f.write(contents)
f.close()

conf = ConfigParser.RawConfigParser()
conf.read(outputDir+"../awsSettings.cfg")
cred = conf.get("Credentials", "aws_access_key_id"), conf.get("Credentials", "aws_secret_access_key")


connection = None
channel = None
queue_name = None

def connect_queue():
    global connection, channel, queue_name
    connection = pika.BlockingConnection(pika.ConnectionParameters(
        host=mqHost))
    channel = connection.channel()

    channel.exchange_declare(exchange='OCcass',
                             type='direct')

    result = channel.queue_declare(exclusive=True)
    queue_name = result.method.queue

    channel.queue_bind(exchange='OCcass',
                       queue=queue_name,
                       routing_key='all')

connect_queue()

# run in loop, so that this program is always ready to start a monitoring session
while True:
    bucketName = None
    monitoringDataDir = None
    startOfTest = None
    beforePeriod = None
    ycsbCommand = ycsbCommandInitial

    ## wait for configuration params
    for method, header, body in channel.consume(queue_name):
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
        beforePeriod = int(conf.get("test", "beforePeriod"))


        ycsbCommand += " -p hosts="+conf.get("ycsb","hosts")
        ycsbCommand += " -p timeseries.granularity="+str(int(conf.get("test","samplingInterval"))*1000)

        #save workload file from configuration to disk, so that ycsb can load the workload-file
        workload = conf.get("ycsb", "workload")
        workload = workload.replace(";","\n")

        wfile = open(outputDir+"../ycsb-workload", "w")
        wfile.write(workload)
        wfile.close()

        # measurementType = conf.get("ycsb", "measurementtype")
        # if measurementType == "histogram":
        #     ycsbCommand += " -p histogram.buckets="+conf.get("ycsb", "histogram.buckets")
        # elif measurementType == "timeseries":
        #     ycsbCommand += " -p measurementtype=timeseries -p timeseries.granularity="+str(int(conf.get("test","samplingInterval"))*1000)
        f.close()
        channel.basic_ack(method.delivery_tag)
        break

    logger.info("Received CONFIGURATION")
    ##### Bootstrap END #####

    time.sleep(2)

    # send START command
    channel.basic_publish(exchange='OCcass',
                              routing_key='all',
                              body="<START>")

    logger.info("Sent START Command")

    #beforePeriod
    logger.info("Before Period. Waiting...")
    time.sleep(beforePeriod)

    #running YCSB
    logger.info("RUNNING YCSB")
    command_line = ycsbCommand
    args = shlex.split(command_line)
    p = subprocess.call(args, stdout=open(outputDir+"stdout.txt", "w"), stderr=open(outputDir+"stderr.txt", "w"))
    logger.info("END YCSB")

    # YCSB is done. send STOP command
    try:
        channel.basic_publish(exchange='OCcass',
                              routing_key='all',
                              body="<STOP>")
    except:
        connect_queue()
        channel.basic_publish(exchange='OCcass',
                              routing_key='all',
                              body="<STOP>")

    logger.info("Sent STOP Command")

    # run the parser, which generates csv
    logger.info("RUNNING YCSB parser")
    f = open(outputDir+"stdout.txt", "r")
    csvString = YCSBOutputParser.parseHistogram(f)
    f.close()
    f = open(outputDir+"ycsb-histogram.csv","w")
    f.write(csvString.getvalue())
    f.close()
    f = open(outputDir+"stdout.txt", "r")
    csvString = YCSBOutputParser.parseTimeseries(f)
    f.close()
    f = open(outputDir+"ycsb-timeseries.csv","w")
    f.write(csvString.getvalue())
    f.close()
    f = open(outputDir+"stderr.txt", "r")
    csvString = YCSBOutputParser.parseStderr(f)
    f.close()
    f = open(outputDir+"ycsb-stderr.csv","w")
    f.write(csvString.getvalue())
    f.close()
    logger.info("END YCSB parser")


    # data upload to S3
    logger.info("CONNECTING TO S3")
    s3 = boto.connect_s3(cred[0], cred[1])
    bucket = s3.get_bucket(bucketName)

    logger.info("START uploading data to S3")
    from boto.s3.key import Key
    k = Key(bucket, monitoringDataDir+"/"+startOfTest+"/raw/"+startOfTest+"---ycsb-stdout.txt")
    k.set_contents_from_filename(outputDir+"stdout.txt")
    k = Key(bucket, monitoringDataDir+"/"+startOfTest+"/raw/"+startOfTest+"---ycsb-stderr.txt")
    k.set_contents_from_filename(outputDir+"stderr.txt")
    k = Key(bucket, monitoringDataDir+"/"+startOfTest+"/"+startOfTest+"---ycsb-histogram.csv")
    k.set_contents_from_filename(outputDir+"ycsb-histogram.csv")
    k = Key(bucket, monitoringDataDir+"/"+startOfTest+"/"+startOfTest+"---ycsb-timeseries.csv")
    k.set_contents_from_filename(outputDir+"ycsb-timeseries.csv")
    k = Key(bucket, monitoringDataDir+"/"+startOfTest+"/"+startOfTest+"---ycsb-stderr.csv")
    k.set_contents_from_filename(outputDir+"ycsb-stderr.csv")
    logger.info("FINISHED uploading data to S3")
    s3.close()

connection.close()