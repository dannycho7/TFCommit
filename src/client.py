#! /usr/bin/env python3

import json
import sys
import socket
import threading
import random
import time
import os
sys.path.insert(0, './lib')
from blockchain import RWSet
from messenger import Messenger
from msg_types import MessageManager, MSG
from constants import Const

class Transaction:
    def __init__(self, txn_id, ts, updates, rw_set_list):
        self.id = txn_id
        self.ts = ts
        self.updates = updates
        self.rw_set_list = rw_set_list

class Client:
    def __init__(self, config):
        self.config = config
        self.shard_config = self.config['shards'][0]
        client_config = self.config['client']
        self.msg_mgr = MessageManager((client_config['ip_addr'], client_config['port']))
        self.cntr = 0

    def createTxn(self):
        txn_id = time.time()
        ts = time.time()
        updates = []
        rw_set_list = []
        for i in range(Const.NUM_OPS):
            p_id = random.randint(0, Const.NUM_PARTITIONS-1)
            if i < Const.NUM_PARTITIONS:
                p_id = i
            v = random.randint(0, Const.NUM_ELEMENTS)
            val = bytes('v' + str(v), 'utf-8')
            strt = p_id * Const.NUM_ELEMENTS
            k = random.randint(strt, strt + Const.NUM_ELEMENTS)
            key = bytes('k' + str(k), 'utf-8')
            updates.append((key, val))
            rw_set_list.append(RWSet({}, {key: hash(val)}))
        return Transaction(txn_id, ts, updates, rw_set_list)

    def performTransaction(self):
        self.cntr += 1
        txn = self.createTxn()
        msg = self.msg_mgr.create_end_transaction_msg(txn)
        Messenger.get().send(msg, (self.shard_config['ip_addr'], self.shard_config['port']))

    def recvDecision(self, final_decision):
        self.performTransaction()

    def logResults(self):
        timeElapsed = time.time() - globalTime
        txnRate = Const.NUM_TXNS/timeElapsed
        msg = str(timeElapsed) + ':' + str(txnRate)
        msg += str(Const.NUM_PARTITIONS) + '\n'
        
        if not os.path.exists('./results'):
            os.mkdir('results')
        file_path = './results/results.txt'
        fd = open(file_path,'a')
        fd.write(msg)
        fd.close()

def handleConnection(cl, req_sock):
    while True:
        req = Messenger.recv(req_sock)
        if req == None:
            break
        print("Recv msg {0}\n".format(req))
        body = req['body']
        if req['msg_type'] == MSG.DECISION:
            if cl.cntr < Const.NUM_TXNS:
                cl.recvDecision(body['final_decision'])
            else:
                cl.logResults()
                break

if __name__ == "__main__":
    if len(sys.argv) != 2:
    	print("Correct Usage: {0} <config_file_path>".format(sys.argv[0]))
    	sys.exit()
    
    config = json.load(open(sys.argv[1]))
    cl = Client(config)

    client_config = config['client']
    print(client_config)
    client_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    client_sock.bind((client_config['ip_addr'], client_config['port']))
    client_sock.listen(5)
    globalTime = time.time()
    txn_t = threading.Thread(target=cl.performTransaction(), args=(cl,))
    txn_t.start()
    (req_sock, addr) = client_sock.accept()
    t = threading.Thread(target=handleConnection, args=(cl, req_sock))
    t.start()
    