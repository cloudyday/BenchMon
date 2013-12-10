__author__ = 'Oliver Roess'

import pika

mqHost = "ec2-54-235-208-11.compute-1.amazonaws.com"



connection = pika.BlockingConnection(pika.ConnectionParameters(
        host=mqHost))
channel = connection.channel()

channel.exchange_declare(exchange='OCcass',
                         type='direct')


channel.basic_publish(exchange='OCcass',
                      routing_key='all',
                      body="<STOP>")


connection.close()

