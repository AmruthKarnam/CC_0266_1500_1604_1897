from flask import Flask, render_template, jsonify, request, abort, g
#from flask_cors import CORS
import requests
#import sqlite3
#import status
from werkzeug.exceptions import BadRequest
#from models import sessions
app = Flask(__name__)
#cors = CORS(app)
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

engine = create_engine('sqlite:///ride_mgmt.db', connect_args={'check_same_thread': False}, echo=True,pool_pre_ping=True)
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


@app.route('/',methods=["GET"])
def dummy_api():
    return  {},200

def http_count():
    with counter.get_lock():
        counter.value += 1


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



@app.route('/api/v1/rides/count',methods=["GET"])
def ride_count():
     http_count()
     list1 = []
     ride = {}
     ride['table']='Ride'
     ride['where']="1=1"
     ride['columns']='rideid'
     res=requests.post('http://localhost:8000/api/v1/db/read', json = ride).json()
     ride_count = 0
     for row in res :
         ride_count += 1
     list1.append(ride_count)
     return json.dumps(list1),200

# 3
@app.route('/api/v1/rides', methods=["POST","PUT"])
def createride():
    http_count()
    if request.method == "PUT" :
        return {},405
    ride = dict(request.json)
    count = 0
    #= ride['password']
    if 'source' not in ride.keys() or 'destination' not in ride.keys() or 'created_by' not in ride.keys() or 'timestamp' not in ride.keys() :
        return {},400
    date = parse(ride['timestamp'])
    d = datetime(date[0],date[1],date[2],date[3],date[4],date[5])
    if d < datetime.now() :
        return {},400

    #print(user["username"])

    results=requests.get('http://ajeyaexponential-982805114.us-east-1.elb.amazonaws.com/api/v1/users',headers = {'Origin':'http://35.171.66.199'})

    for _ in results:
        count+=1
    if count==0:
        return {},400
    results=results.json()
    #print("dsasdaads")
    print("sdasd=",results)
    l=results
    a=ride['created_by']
    print(a)
    count=0
    if a in l:
        count=1
    if count == 1:
        if str(ride['source']) in dict1.keys() and str(ride['destination']) in dict1.keys():
            ride_id=0
            ride['table']='Ride'
            ride['where']="1=1"
            ride['columns']='rideid'
            res=requests.post('http://localhost:8000/api/v1/db/read', json = ride).json()
            #print(res)
            ride_count = 0
            for row in res :
                ride_count += 1
            if ride_count >= 1 :
                for row in res :
                    if ride_id<row['rideid']:
                        ride_id = row['rideid']
                #print(ride_id)                 
                ride_id += 1
            else :
                ride_id = 1
            #print(ride_id)
            ''' TIMESTAMP CASE IS NOT COVERED'''
            ride['isPut'] = 1
            ride['table'] = 'Ride'
            ride['insert'] = str(ride_id) + comma + quotes + ride['created_by'] + quotes + comma + str(ride['source']) + comma + str(ride['destination']) + comma + quotes + ride['timestamp'] + quotes
            res = requests.post('http://localhost:8000/api/v1/db/write', json = ride)
            ride['isPut'] = 1
            ride['table'] = 'Riders'
            ride['insert'] = str(ride_id) + comma + quotes + ride['created_by'] + quotes
            #print(ride_id)
            #print("create"+ride['timestamp'])
            res = requests.post('http://localhost:8000/api/v1/db/write', json = ride)
            return {},201
        else :
            return {},400
    elif count!=1:
        return {},400
    else :
        return {},405

# 4
@app.route('/api/v1/rides', methods=["GET"])
def listupcomingride():
    http_count()
    #print("entered")
    source = request.args.get('source')
    #print("Source:" + source)
    destination = request.args.get('destination')
    #print(source)
    if source == None or destination == None or source == '' or destination == '':
        return {},400
    try:
        if int(source) not in range(1,199) or int(destination) not in range(1,199) :
            return {},400
    except:
        return {},400

    res = {}
    res['columns'] = 'rideid,createdby,source,dest,timestamp'
    res['table'] = 'Ride'
    res['where'] = 'source = ' + source + ' and dest = ' + destination
    res = requests.post('http://localhost:8000/api/v1/db/read', json = res)
    #print(res)
    res = res.json()
    #print(res)
    #return
    #return jsonify(res)
    #return res.json()

    l = []
    #print("entry")
    for row in res :
        #print('here')
        date = parse(row['timestamp'])
        #print(date)
        d = datetime(date[0],date[1],date[2],date[3],date[4],date[5])
        if d > datetime.now() :
            l.append({'rideId' : row['rideid'],'username' : row['createdby'],'timestamp' : row['timestamp']})
        #l.append({'rideId' : row['rideid'],'username' : row['createdby'],'timestamp' : row['timestamp']})
    if l == [] :
        return {},204

    return jsonify(l),200


# 5
@app.route('/api/v1/rides/<rideId>', methods=["GET"])
def listride(rideId):
    http_count()
    try:
        if int(rideId):
            pass
    except:
        return {},400
    ride = {}
    ride['table']='Ride'
    ride['where']='rideid='+rideId
    ride['columns']='createdby,timestamp,source,dest'
    res_ride=requests.post('http://localhost:8000/api/v1/db/read', json = ride).json()
    response = {}
    response['rideId'] = rideId
    #print(res_ride)
    count = 0
    for row in res_ride :
        count += 1
    if count == 0 :
        return {},204
    elif count>0:
        res_ride = requests.post('http://localhost:8000/api/v1/db/read', json = ride).json()
        ride['columns']='username'
        ride['table']='Riders'
        res_rider = requests.post('http://localhost:8000/api/v1/db/read', json = ride).json()
        for row in res_ride :
            response["createdby"] = row['createdby']
            response["timestamp"] = row['timestamp']
            response["source"] = row['source']
            response["destination"] = row['dest']
            #print(response)
        response["users"] = []
        for row in res_rider :
            response["users"].append(row['username'])

        #print(response)
        return response,200
    else:
        return {},405

# 6
@app.route('/api/v1/rides/<rideId>', methods=["POST"])
def joinride(rideId):
    http_count()
    user = dict(request.json)
    print("asa")
    results=requests.get('http://ajeyaexponential-982805114.us-east-1.elb.amazonaws.com/api/v1/users',headers = {'Origin':'http://35.171.66.199'})
    first_count = 0
    for _ in results :
        first_count += 1
    print("hdh")
    print(first_count)
    if first_count == 0 :
        return {},400
    results=results.json()
    l=results
    count = 0
    a=user["username"]
    if a in l:
        count=1
    print(results)
    print(count)
    if count == 0 :
        return {},400
    user['table']='Ride'
    user['where']='rideid='+rideId
    user['columns']='rideid,timestamp'
    print("1234")
    res= requests.post('http://localhost:8000/api/v1/db/read', json = user)
    count = 0
    ride = {}
    for _ in res :
        count += 1
    print(count)
    if count == 0 :
        return {},400

    else :
        ride['table'] = 'Riders'
        ride['insert'] = rideId + comma + quotes + user["username"] + quotes
        ride['isPut'] = 1
        res = requests.post('http://localhost:8000/api/v1/db/write', json = ride)
        return {},200


# 7
@app.route('/api/v1/rides/<rideId>', methods=["DELETE"])
def deleteride(rideId):
    http_count()
    ride = {}
    ride['table']='Ride'
    ride['where']='rideid='+rideId
    ride['columns']='createdby,timestamp,source,dest'
    res= requests.post('http://localhost:8000/api/v1/db/read', json = ride).json()
    count = 0
    ride = {}
    for _ in res :
        count += 1
    if count == 0 :
        return {},400

    else :
        ride['table'] = 'Riders'
        ride['column'] = 'rideid'
        ride['isPut'] = 0
        ride['value']=rideId
        #ride['insert'] = rideId + comma + quotes + user + quotes
        res = requests.post('http://localhost:8000/api/v1/db/write', json = ride)
        ride['table'] = 'Ride'

        #ride['insert'] = rideId + comma + quotes + user + quotes

        res = requests.post('http://localhost:8000/api/v1/db/write', json = ride)
        return {},200

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

# 10
@app.route('/api/v1/db/clear',methods=["POST"])
def cleardb():
    con.execute('DELETE FROM Riders')
    con.execute('DELETE FROM Ride')
    return {},200

if __name__ == '__main__':
    app.run(debug=True,host='0.0.0.0',port=8000)

