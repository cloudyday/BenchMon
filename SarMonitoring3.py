#!/usr/bin/env python

__author__ = 'Oliver Roess'


import shlex, subprocess
import os
import ConfigParser
import boto # AWS Library
import logging
import pika
from cStringIO import StringIO
import signal
import fileinput
import socket
import time
import sys

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("SarMonitoring")
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
        ## wait for configuration params
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
            samplingInterval = conf.get("test", "samplingInterval")
            afterPeriod = int(conf.get("test", "afterPeriod"))
            f.close()

            channel.basic_ack(method.delivery_tag)
            logger.info("Received CONFIGURATION")
            break    
        ##### Bootstrap END #####

        if monitoringOn == False: break

        try:
            os.remove(outputDir+"binary.txt")
        except:
            pass
        try:
            os.remove(outputDir+"summary.pdf")
        except:
            pass


        # wait for START command
        for method, header, body in channel.consume(queue_name):
            if body.find("<START>") == -1:
                channel.basic_ack(method.delivery_tag)
                continue
            channel.basic_ack(method.delivery_tag)
            break

        logger.info("Received START Command")

        # run SAR monitoring
        logger.info("RUNNING sar")
        command_line = "sar -A "+samplingInterval+" -o "+outputDir+"binary.txt"
        args = shlex.split(command_line)
        p = subprocess.Popen(args, stdout=open(os.devnull, "wb"))



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

        p.send_signal(signal.SIGINT)
        p.wait()

        logger.info("STOPPED sar")

        # conversion of binary data to txt, CSV and PDF
        logger.info("RUNNING Converting to human readable txt file / Parsable for kSar")
        my_env = os.environ.copy()
        my_env["LC_ALL"] = "C"
        command_line = "sar -f "+outputDir+"binary.txt -bBdqrRSvwWy -n DEV -n EDEV -n SOCK -I SUM -u ALL"
        args = shlex.split(command_line)
        p = subprocess.call(args, stdout=open(outputDir+"summary.txt", "w"), env=my_env)

        logger.info("RUNNING converting to csv and pdf (kSar)")
        command_line = "java -jar "+outputDir+"../kSar/kSar.jar -input "+outputDir+"summary.txt -showCPUstacked -outputCSV "+outputDir+"summary.csv -outputPDF "+outputDir+"summary.pdf"
        args = shlex.split(command_line)
        p = subprocess.call(args)
        logger.info("FINISHED converting to csv and pdf")

        # replace all ";" with "," in CSV, so that the CSV can be instantly used with gnumeric or converted to XLS via ssconvert
        for line in fileinput.input(outputDir+"summary.csv", inplace=1):
            line = line.replace(";",",")
            line = line.strip()
            print line


        # data upload to S3
        logger.info("CONNECTING TO S3")
        s3 = boto.connect_s3(cred[0], cred[1])
        bucket = s3.get_bucket(bucketName)

        logger.info("START uploading data to S3")
        from boto.s3.key import Key
        k = Key(bucket, monitoringDataDir+"/"+startOfTest+"/raw/"+startOfTest+"--"+hostname+"---sar.txt")
        k.set_contents_from_filename(outputDir+"summary.txt")
        k = Key(bucket, monitoringDataDir+"/"+startOfTest+"/"+startOfTest+"--"+hostname+"---sar.csv")
        k.set_contents_from_filename(outputDir+"summary.csv")
        k = Key(bucket, monitoringDataDir+"/"+startOfTest+"/"+startOfTest+"--"+hostname+"---sar.pdf")
        k.set_contents_from_filename(outputDir+"summary.pdf")
        logger.info("FINISHED uploading data to S3")
        s3.close()

    # this code will only be reached if "monitoringOn" was set to false
    logger.info("Switched OFF")

    channel.basic_publish(exchange='OCcass',
                      routing_key='controller',
                      body="<LOG>"+hostname+": SarMonitoring OFF")


    for method, header, body in channel.consume(queue_name):
        if body.find("<ON>") == -1:
            channel.basic_ack(method.delivery_tag)
            continue
            
        monitoringOn = True

        channel.basic_publish(exchange='OCcass',
                  routing_key='controller',
                  body="<LOG>"+hostname+": SarMonitoring ON")
        channel.basic_ack(method.delivery_tag)
        break




connection.close()