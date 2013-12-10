#!/usr/bin/env python

__author__ = 'Oliver Roess'

print "Loading..."

import pika
import threading
import os

#mqHost = "141.52.218.31"
mqHost = "54.235.208.11"

connection = pika.BlockingConnection(pika.ConnectionParameters(
        host=mqHost))
channel = connection.channel()

channel.exchange_declare(exchange='OCcass',
                         type='direct')

result = channel.queue_declare(exclusive=True)
queue_name = result.method.queue
channel.queue_bind(exchange='OCcass',
                       queue=queue_name,
                       routing_key='controller')


def runMonitor(stop_event):
	i = 0
	while not stop_event.is_set():

		print "\nUsage: routing_key ON/OFF [routing_key ON/OFF]*"
		print "CTRL-C to exit"

		userInput = raw_input("\n> ")
		command = userInput.split(" ")

		if (len(command) % 2) != 0:
			continue

		exit = False

		i = 1
		while i < len(command):
			if not (command[i] in ["ON", "OFF"]):
				exit = True
			i += 2
		if exit: continue


		i = 0
		while i < len(command):
			channel.basic_publish(exchange='OCcass',
		                      routing_key=command[i],
		                      body="<"+command[i+1]+">")	
			i += 2

		stop_event.wait(2)

        

# run the thread
monitorStop = threading.Event()
t = threading.Thread(target=runMonitor, args=(monitorStop,))
t.start()

try:
	while True:
		for method, properties, body in channel.consume(queue_name):
			if body.find("<LOG>") == -1:
			  channel.basic_ack(method.delivery_tag)
			  continue

			channel.basic_ack(method.delivery_tag)
			print body
			break
except KeyboardInterrupt:
	pass
	
print "Exiting"
connection.close()
os._exit(1)



