from flask import Flask, render_template, jsonify, request, abort, g,request
import requests
#import sqlite3
#import status
from werkzeug.exceptions import BadRequest
#from models import sessions
app = Flask(__name__)
from sqlalchemy import create_engine, Sequence
from sqlalchemy import String, Integer, Float, Boolean, Column, ForeignKey, DateTime
from sqlalchemy.orm import sessionmaker
import random
from datetime import datetime
from multiprocessing import Value
import csv
import json
import pika
import sys
import uuid

#connection = pika.BlockingConnection(
#pika.ConnectionParameters(host='rabbitmq'))
#channel = connection.channel()



class OrchestratorRpcClient(object):

    def __init__(self):
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='rabbitmq'))

        self.channel = self.connection.channel()

        result = self.channel.queue_declare(queue='rpc_queue')
        self.callback_queue = result.method.queue

        self.channel.basic_consume(
            queue=self.callback_queue,
            on_message_callback=self.on_response,
            auto_ack=True)

    def on_response(self, ch, method, props, body):
        if self.corr_id == props.correlation_id:
            self.response = body

    def call(self, n):
        self.response = None
        self.corr_id = str(uuid.uuid4())
        self.channel.basic_publish(
            exchange='',
            routing_key='READ_queue',
            properties=pika.BasicProperties(
                reply_to=self.callback_queue,
                correlation_id=self.corr_id,
            ),
            body=n)
        while self.response is None:
            self.connection.process_data_events()
        return self.response


orchestrator_rpc = OrchestratorRpcClient()

"""def write_to_queue(queue_name,message) :
    channel.queue_declare(queue=queue_name, durable=True)
    channel.basic_publish(
        exchange='',
        routing_key=queue_name,
        body=message,
        properties=pika.BasicProperties(
            delivery_mode=2,  # make message persistent
        ))


@app.route('/api/v1/db/write', methods=["POST"])
def writetodb():
    queue_name = 'WRITE_queue'
    user_details = request.json
    if user_details['isPut']==1:
        write_message = 'INSERT INTO ' + user_details['table'] + ' VALUES(' + user_details['insert'] + ')'
        write_to_queue(queue_name,write_message)
        
    else:
        write_message = 'DELETE FROM ' + user_details['table'] + ' WHERE  ' + user_details['column'] + '=' '"' + user_details['value'] + '"'
        write_to_queue(queue_name,write_message)
    return "written"
"""


# 9
@app.route('/api/v1/db/read', methods=["POST"])
def readfromdb():
    queue_name = 'READ_queue'
    user_details = dict(request.json)
    read_message = 'SELECT '+ user_details['columns'] + ' FROM ' + user_details['table'] + ' WHERE ' + user_details['where']
    response = orchestrator_rpc.call(read_message)
    return response
    
@app.route('/api/v1/db/clear',methods=["POST"])
def cleardb():
    queue_name = 'WRITE_queue'
    write_to_queue('DELETE FROM Riders')
    write_to_queue('DELETE FROM Ride')
    write_to_queue('DELETE FROM User')
    return {},200
    
if __name__ == '__main__':
    app.run(debug=True,host='0.0.0.0',port=8000)
