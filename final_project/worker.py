import pika
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
import csv
import json
import threading
import os

from sqlalchemy.ext.declarative import declarative_base
Base = declarative_base()

class User(Base):
    __tablename__ = 'User'
    username = Column(String(8080), primary_key=True)
    password = Column(String(40))

    def init(self, username, password):
        self.username = username
        self.password = password



class Ride(Base):
    __tablename__ = 'Ride'
    rideid = Column(Integer, primary_key=True)
    createdby = Column(String(8000), nullable=False)
    source = Column(String(80), nullable=False)
    dest = Column(String(80), nullable=False)
    timestamp = Column(DateTime,nullable=False)

class Riders(Base):
    __tablename__ = 'Riders'
    rideid = Column(Integer, ForeignKey('Ride.rideid',ondelete = 'CASCADE'),primary_key=True)
    username = Column(String(8000), primary_key = True)

engine = create_engine('sqlite:///ride_share.db', connect_args={'check_same_thread': False}, echo=True,pool_pre_ping=True)
con = engine.connect()
Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)
session=Session()


def writetodb(str1):
    str1 = str1[2:-1]
    con.execute(str1)


def callback(ch, method, properties, body):
    print(" [x] Received %r" % body)
    abc=str(body)
    writetodb(abc)
    print(" [x] Done")
       
def readfromdb(str1):
    str1=str1[2:-1]
    str1 = str1.replace('\\', '')
    user_details=json.loads(str1)
    rs = con.execute('SELECT '+ user_details['columns'] + ' FROM ' + user_details['table'] + ' WHERE ' + user_details['where'])
    list1=[]
    for row in rs:
        d={}
        l=user_details['columns'].split(',')
        if len(row):
            for colNo in range(0,len(row)):
                d[l[colNo]]=row[colNo]
            list1.append(d)
    return json.dumps(list1)


def on_request(ch, method, properties, body):
    n = str(body)
    response = readfromdb(n)
    ch.basic_publish(exchange='',
                     routing_key=properties.reply_to,
                     properties=pika.BasicProperties(correlation_id = \
                                                         properties.correlation_id),
                     body=response)





def writer():
    connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='0.0.0.0'))
    channel = connection.channel()
    channel.queue_declare(queue='WRITE_queue', durable=True)
    channel.basic_qos(prefetch_count=30)
    channel.basic_consume(queue='WRITE_queue', on_message_callback=callback,auto_ack=True)
    channel.start_consuming()


def reader():
    connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='0.0.0.0'))
    channel = connection.channel()
    channel.queue_declare(queue='rpc_queue')
    channel.basic_qos(prefetch_count=30)
    channel.basic_consume(queue='rpc_queue', on_message_callback=on_request,auto_ack=True)
    print(" [x] Awaiting RPC requests")
    channel.start_consuming()




if __name__ == '__main__':
    t1 = threading.Thread(target=writer, args=())
    #t1.start()
    t2 = threading.Thread(target=reader, args=())
    #t2.start()
    if os.environ["container_type"] == "master" :
        t1.start()
    else :
        t2.start()
        
    app.run(debug=True,host='0.0.0.0',port=8000)