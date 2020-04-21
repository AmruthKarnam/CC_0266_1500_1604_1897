#!/usr/bin/env python
import pika
import time
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

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='rmq'))
channel = connection.channel()

channel.queue_declare(queue='WRITE_queue', durable=True)
print(' [*] Waiting for messages.')

def writetodb(str1):
    con.execute(str1)

def callback(ch, method, properties, body):
    print(" [x] Received %r" % body)
    abc=str(body)
    print(abc)
    writetodb(abc)
    print(" [x] Done")
    ch.basic_ack(delivery_tag=method.delivery_tag)
channel.basic_qos(prefetch_count=10)
channel.basic_consume(queue='WRITE_queue', on_message_callback=callback)

channel.start_consuming()

if __name__ == '__main__':
    app.run(debug=True,host='0.0.0.0',port=8000)
