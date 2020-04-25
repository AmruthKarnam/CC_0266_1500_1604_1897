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
from kazoo.exceptions import ConnectionLossException
from kazoo.exceptions import NoAuthException
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
connection = pika.BlockingConnection(
pika.ConnectionParameters(host='rabbitmq'))
channel = connection.channel()
client = docker.from_env()
client = docker.DockerClient(base_url='unix://var/run/docker.sock')
#container = client.containers.run("final_project_slave","python slave.py",links={"rabbitmq":"rabbitmq"},network="final_project_default")

#container = client.containers.create("final_project_slave","python slave.py")
#container1 = client.containers.run("final_project_slave","python slave.py")
#print("container_id:",container)
#for container in client.containers.list():
#   print("container_id1:",container.name)

from kazoo.client import KazooClient
from kazoo.handlers.gevent import SequentialGeventHandler
import sys

from kazoo.exceptions import ConnectionLossException
from kazoo.exceptions import NoAuthException
print("before client")
zk = KazooClient(hosts='zookeeper:2181',handler=SequentialGeventHandler())
print("after client")
# returns immediately
event = zk.start_async()
print("after start")
# Wait for 30 seconds and see if we're connected
event.wait(timeout=20)
print("after wait")
if not zk.connected:
    # Not connected, stop trying to connect
    zk.stop()
    raise Exception("Unable to connect.")

print("after function")
zk.delete("/producer", recursive=True)
zk.ensure_path("/producer")
if zk.exists("/producer/node_1"):
    print("Node already exists")
else:
    print("in else")
    zk.create("/producer/node_1", b"demo producer node")
data, stat = zk.get("/producer/node_1")
print("Version: %s, data: %s" % (stat.version, data.decode("utf-8")))
zk.create("/producer/node_3", b"demo producer node")
@zk.ChildrenWatch('/producer')
def demo_func(event):
    print("inside demo_func")
    # Create a node with data
    if zk.exists("/producer/node_2"):
        print("Hello World")
    else:
        print("something else")
        zk.ensure_path("/producer/node_2")
    print("Event=",event)
    children = zk.get_children("/producer")
    print(" %s children with names %s" % (len(children), children))
children = zk.get_children("/producer", watch=demo_func)
print("There are %s children with names %s" % (len(children), children))
zk.delete("/producer", recursive=True)
# Both these statements return immediately, the second sets a callback
# that will be run when get_children_async has its return value
#async_obj = zk.get_children_async("/")
#async_obj.rawlink(my_callback)

"""import logging

from kazoo.client import KazooClient
from kazoo.handlers.gevent import SequentialGeventHandler
from kazoo.client import KazooState
print("before client")
zk = KazooClient(hosts='zookeeper:2181',handler=SequentialGeventHandler())
print("after client")
zk.start()
print("after start")
def my_listener(state):
    print("into function")
    if state == KazooState.LOST:
        print("STATE:",state)
    elif state == KazooState.SUSPENDED:
        print("STATE:",state)
    else:
        print("STATE",state)

zk.add_listener(my_listener)
print("after listner")
zk.stop()"""
"""zk = KazooClient(hosts="zookeeper:2181",handler=SequentialGeventHandler())
# returns immediately
event = zk.start_async()
# Can also use the @DataWatch and @ChildrenWatch decorators for the same
def my_callback(async_obj):
    try:
        children = async_obj.get()
        print("something")
    except (ConnectionLossException, NoAuthException):
        sys.exit(1)
async_obj = zk.get_children_async("producer")
async_obj.rawlink(my_callback)
zk.create_async("/producer",b'producer')
zk.create_async("/producer/node1",b"child1")
zk.create_async("/producer/node2",b"child2")
event.wait(timeout=10)
if not zk.connected:
   raise Exception
# List the children"""


def http_count():
    with counter.get_lock():
        counter.value += 1

def timer():
    print("hmm\n")
    global counter
    while(True):
        print("hello im here\n")
        no_of_req = counter.value
        containers =  math.ceil(no_of_req/20)
        print("ajeya=",containers)
        if containers == 0:
            containers = 1
        res1 = requests.get("http://localhost:8000/api/v1/worker/list")
        length = len(res1.json())

        if length>containers:
            for i in range(length-containers):
                requests.post("http://localhost:8000/api/v1/crash/slave")
            client.containers.prune()
            res1=requests.get("http://localhost:8000/api/v1/worker/list")
            print("after pruning = ",len(res1.json()))

             
        res=requests.delete('http://localhost:8000/api/v1/_count')
        time.sleep(1200)

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

@app.route('/api/v1/worker/list',methods=["GET"])
def list_worker():
    pid_list = []
    
    for container in client.containers.list():
        print(client.inspect_container(container.id))
        if "slave" in container.name:
            pid_list.append(container.id)
    return json.dumps(sorted(pid_list)),200

@app.route('/api/v1/crash/master',methods=["POST"])
def crash_master():
    for container in client.containers.list():
        if "master" in container.name:
            container.stop()
            client.containers.prune()
    for container in client.containers.list():
        print("container_id2:",container.name)
    return {},200


@app.route('/api/v1/crash/slave',methods=["POST"])
def crash_slave():
    #print("something Start\n")
    res1 = requests.get("http://localhost:8000/api/v1/worker/list")
    l = res1.json()
    #print("list is = ",l)
    if(len(l)>1):
        delete_id = l[-1]
        #print("the delete id = ",delete_id)
        for container in client.containers.list():
            if container.id==delete_id:
                #print("something inside\n")
                container.stop()
        #for container in client.containers.list():
            #print("container_id2:",container.name)
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

def write_to_queue(queue_name,message) :
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



# 9
@app.route('/api/v1/db/read', methods=["POST"])
def readfromdb():
    #http_count()
    queue_name = 'READ_queue'
    user_details = dict(request.json)
    print(user_details)
    read_message = 'SELECT '+ user_details['columns'] + ' FROM ' + user_details['table'] + ' WHERE ' + user_details['where']
    print(str(user_details))
    response = orchestrator_rpc.call(json.dumps(user_details))
    return response
    
    
@app.route('/api/v1/db/clear',methods=["POST"])
def cleardb():
    queue_name = 'WRITE_queue'
    write_to_queue(queue_name,'DELETE FROM Riders')
    write_to_queue(queue_name,'DELETE FROM Ride')
    write_to_queue(queue_name,'DELETE FROM User')
    return {},200
    
if __name__ == '__main__':
    
    print("check1")
    t1 = threading.Thread(target=timer, args=())
    print("check2")
    t1.start()
    print("check3")
    
    #container = client.containers.run("final_project_slave","python slave.py",links={"rabbitmq":"rabbitmq"},network="final_project_default")
    app.run(debug=True,host='0.0.0.0',port=8000)
    