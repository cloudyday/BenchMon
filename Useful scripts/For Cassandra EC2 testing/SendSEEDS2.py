__author__ = 'Oliver Roess'

import pika

mqHost = "54.235.208.11"

seeds = "10.145.208.29"


connection = pika.BlockingConnection(pika.ConnectionParameters(
        host=mqHost))
channel = connection.channel()

channel.exchange_declare(exchange='OCcass',
                         type='direct')


channel.basic_publish(exchange='OCcass',
                      routing_key='all',
                      body="<CASS_SEEDS>"+seeds)

connection.close()