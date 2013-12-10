__author__ = 'Oliver Roess'

from datetime import datetime
import pika
import time
from cStringIO import StringIO

mqHost = "ec2-54-235-208-11.compute-1.amazonaws.com"

# S3 settings
bucketName = "or_masterarbeit" # bucket must exist!
monitoringDataDir = "monitoring"

# benchmark-related settings
testName = "Vertical-ELB--target6000"
startOfTest = datetime.now().strftime("%m-%d---%H-%M-%S")+"-"+testName
samplingInterval = 5 # sec
beforePeriod = 60 # sec /// periods that monitoring tools begin to monitor Before and After the benchmark (ycsb) starts/finishes
afterPeriod = 60 # sec

# ycsb workload file
wfile = open("ycsb-workload", "r")
workload = wfile.read()
wfile.close()

# further ycsb settings
#hosts = "\"10.145.205.132,10.147.174.202,10.165.0.240\""
hosts = "\"Cassandra-1875041769.us-east-1.elb.amazonaws.com\""


# send workload file with configuration data with ";"-separated settings
workload = workload.strip()
workload = workload.replace("\r\n",";") # just in case... (the original yahoo workload files use \r\n)
workload = workload.replace("\n",";")

# benchmark configuration - sent to all nodes
f = StringIO()
print >>f, "[S3]"
print >>f, "bucketName = "+bucketName
print >>f, "monitoringDataDir = "+monitoringDataDir
print >>f, "[test]"
print >>f, "startOfTest = "+startOfTest
print >>f, "samplingInterval = "+str(samplingInterval)
print >>f, "beforePeriod ="+str(beforePeriod)
print >>f, "afterPeriod = "+str(afterPeriod)
print >>f, "[ycsb]"
print >>f, "hosts = "+hosts
print >>f, "workload = "+workload

connection = pika.BlockingConnection(pika.ConnectionParameters(
        host=mqHost))
channel = connection.channel()

channel.exchange_declare(exchange='OCcass',
                         type='direct')

message = "<CONFIGURATION>"+f.getvalue()
f.close()
channel.basic_publish(exchange='OCcass',
                      routing_key='all',
                      body=message)
print "SENT Configuration"


# bind to exchange... only for waiting for a stop command
result = channel.queue_declare(exclusive=True)
queue_name = result.method.queue

channel.queue_bind(exchange='OCcass',
                   queue=queue_name,
                   routing_key='all')

try:
  for method, properties, body in channel.consume(queue_name):
      if body.find("<STOP>") == -1:
          channel.basic_ack(method.delivery_tag)
          continue


      channel.basic_ack(method.delivery_tag)
      break

  time.sleep(afterPeriod)
  print "ITS OVER"

except KeyboardInterrupt:
  pass

connection.close()

