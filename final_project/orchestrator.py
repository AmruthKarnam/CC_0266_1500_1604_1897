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
import time
import csv
import json
import pika
import sys
import uuid
import threading
import math
import docker
counter = Value('i', 0)
spawners = 1
#connection = pika.BlockingConnection(
#pika.ConnectionParameters(host='rabbitmq'))
#channel = connection.channel()

def http_count():
    with counter.get_lock():
        counter.value += 1

def timer():
    print("hmm\n")
    global counter
    while(True):
        print("hello im here\n")
        time.sleep(5)
        no_of_req = counter.value
        containers =  math.ceil(no_of_req/20)
        print("ajeya=",containers)
        if containers == 0:
            containers = 1
        client = docker.from_env()
        
        res=requests.delete('http://localhost:8000/api/v1/_count')

@app.route('/api/v1/_count',methods=["GET"])
def http_count1():
    list1 = []
    list1.append(counter.value)

    return json.dumps(list1),200

@app.route('/api/v1/_count',methods=["DELETE"])
def http_count_reset():
    with counter.get_lock():
        counter.value = 0
    return {},200

class OrchestratorRpcClient(object):

    def __init__(self):
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='rabbitmq'))

        self.channel = self.connection.channel()

        result = self.channel.queue_declare(queue='')
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
            routing_key='rpc_queue',
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
    http_count()
    print("heellloo im here12132\n")
    queue_name = 'READ_queue'
    print("heellloo im here1341\n")
    user_details = dict(request.json)
    print("heellloo im here1342\n")
    read_message = 'SELECT '+ user_details['columns'] + ' FROM ' + user_details['table'] + ' WHERE ' + user_details['where']
    print("heellloo im here1343\n")
    response = orchestrator_rpc.call(json.dumps(user_details))
    print("heellloo im here134\n")
    return response
    
    
@app.route('/api/v1/db/clear',methods=["POST"])
def cleardb():
    '''queue_name = 'WRITE_queue'
    write_to_queue('DELETE FROM Riders')
    write_to_queue('DELETE FROM Ride')
    write_to_queue('DELETE FROM User')'''
    return {},200
    
if __name__ == '__main__':
    print("check1")
    t1 = threading.Thread(target=timer, args=())
    print("check2")
    t1.start()
    print("check3")
    app.run(debug=True,host='0.0.0.0',port=8000)