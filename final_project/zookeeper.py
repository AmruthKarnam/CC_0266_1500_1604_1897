import logging

from kazoo.client import KazooClient
from kazoo.client import KazooState

logging.basicConfig()

zk = KazooClient(hosts='127.0.0.1:2181')
zk.start()

def my_listener(state):
    print("here")
    if state == KazooState.LOST:
        print(state)
    elif state == KazooState.SUSPENDED:    
        print(state)
    else:
        print(state)

zk.add_listener(my_listener)