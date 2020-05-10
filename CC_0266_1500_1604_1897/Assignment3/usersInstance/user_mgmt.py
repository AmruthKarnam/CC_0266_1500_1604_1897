rom flask import Flask, render_template, jsonify, request, abort, g,request
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
dict1 = dict()
comma = ','
quotes = '"'
counter = Value('i', 0)
def parse(datetime) :
    #print(datetime)
    date,time = datetime.split(':')
    dd,momo,yy = date.split('-')
    ss,mm,hh = time.split('-')
    #print(yy,momo,dd,hh,mm,ss)
    return (int(yy),int(momo),int(dd),int(hh),int(mm),int(ss))

with open('AreaNameEnum.csv') as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=',')
    line_count = 0

    for row in csv_reader:
        dict1[row[0]] = row[1]
#print(dict1)
from sqlalchemy.ext.declarative import declarative_base
Base = declarative_base()

class User(Base):
    __tablename__ = 'User'
    username = Column(String(8080), primary_key=True)
    password = Column(String(40))

    def __init__(self, username, password):
        self.username = username
        self.password = password

engine = create_engine('sqlite:///user_mgmt.db', connect_args={'check_same_thread': False}, echo=True,pool_pre_ping=True)

con = engine.connect()
Base.metadata.create_all(engine)


Session = sessionmaker(bind=engine)
session=Session()

def sha(password) :
    hexa_decimal = ['1','2','3','4','5','6','7','8','9','0','a','b','c','d','e','f','A','B','C','D','E','F']
    if len(password) == 40 :
        for i in password :
            if i not in hexa_decimal :
                return False
        return True
    return False

def http_count():
    with counter.get_lock():
        counter.value += 1

@app.route('/',methods=["GET"])
def dummy_api():
    return {},200
# 1
@app.route('/api/v1/users', methods=["PUT","POST"])
def adduser():
    http_count()
    if request.method == "POST" :
        return {},405
    print("aaaaa\n")
    user = dict(request.json)

    pwd = user["password"]

    #print(user["username"]) 

    user['table']='User'
    user['where']='username='+ "'" + user['username']+ "'"
    user['columns']='username'
    results=requests.post('http://localhost:8000/api/v1/db/read', json = user).json()
    #print(user["username"])
    user['isPut'] = 1
    user['table'] = 'User'
    user['insert'] = '"' + user['username'] + '"' + ',' + '"' + user['password'] + '"'
    count = 0
    for row in results :
        count += 1
    if count == 0:
        if sha(pwd) == False :
            return {},400
        res = requests.post('http://localhost:8000/api/v1/db/write', json = user)
        return {},201
    elif count>0:
        return {},400
    else:
        return {},405




# 2
@app.route('/api/v1/users/<username>', methods=["DELETE"])
def removeuser(username):
    http_count()
    if(username == ""):
        return {},400
    userForRead={}
    userForRead['table']='User'
    userForRead['columns']='username'
    userForRead['where']='username'+ '=' + "'"  + username+"'"
    results=requests.post('http://localhost:8000/api/v1/db/read',json=userForRead).json()
    count = 0
    for _ in results :
        count += 1
    if count == 0:
        return {},400
    elif count>0:
        d={}
        d['table'] = 'User'
        d['value']=username
        d['isPut']=0
        d['column']= 'username'
        requests.post('http://localhost:8000/api/v1/db/write', json = d)
        d['isPut'] = 0
        d['table'] = 'Ride'
        d['column']= 'createdby'
        requests.post('http://localhost:8000/api/v1/db/write', json = d)
        d['isPut'] = 0
        d['table'] = 'Riders'
        d['column']= 'username'
        requests.post('http://localhost:8000/api/v1/db/write', json = d)
        return {},200
    else:
        return {},405
    #return {}

@app.route('/api/v1/users', methods=["GET"])
def listallusers():
    http_count()
    print(request,request.headers.get('Origin'))
    ride = {}
    l=[]
    ride['table']='User'
    ride['where']='1=1'
    ride['columns']='username'
    res_ride = requests.post('http://localhost:8000/api/v1/db/read', json = ride).json()
    for i in res_ride:
        l1=i.values()
        l.extend(l1)
        #l.append(i.values())
    if len(l)==0:
        return json.dumps(l),204
    return json.dumps(l),200

# 8
@app.route('/api/v1/db/write', methods=["POST"])
def writetodb():
    user_details = request.json
    if user_details['isPut']==1:
        rs = con.execute('INSERT INTO ' + user_details['table'] + ' VALUES(' + user_details['insert'] + ')')
        return str(rs)
    else:
        rs=con.execute('DELETE FROM ' + user_details['table'] + ' WHERE  ' + user_details['column'] + '=' '"' + user_details['value'] + '"')
        return str(rs)


# 9
@app.route('/api/v1/db/read', methods=["POST"])
def readfromdb():
    user_details = dict(request.json)
    #print("AJEya\n")
    rs = con.execute('SELECT '+ user_details['columns'] + ' FROM ' + user_details['table'] + ' WHERE ' + user_details['where'])
    #print("ajeya BS")
    list1=[]
    for row in rs:
        d={}
        l=user_details['columns'].split(',')
        if len(row):
            for colNo in range(0,len(row)):
                d[l[colNo]]=row[colNo]
            list1.append(d)
    return json.dumps(list1)

@app.route('/api/v1/db/clear',methods=["POST"])
def cleardb():
    con.execute('DELETE FROM User')
    return {},200

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

if __name__ == '__main__':
    app.run(debug=True,host='0.0.0.0',port=8000)


                                                                                                                                                                                          1,1           T
