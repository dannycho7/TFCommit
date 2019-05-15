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

OPS_PER_TXN = 5
class Transaction:
    def __init__(self, txn_id, ts, updates, rw_set_list):
        self.id = txn_id
        self.ts = ts
        self.updates = updates
        self.rw_set_list = rw_set_list

class Client:
    def __init__(self, global_config):
        self.global_config = global_config
        client_config = self.global_config['client']
        self.msg_mgr = MessageManager((client_config['ip_addr'], client_config['port']))
        self.cntr = 0

    def createTxn(self):
        txn_id = time.time()
        ts = txn_id
        updates = []
        rw_set_list = []
        for i in range(self.global_config['client']['num_ops']):
            shard_i = random.randint(0, len(self.global_config['shards']) - 1)
            if i < len(self.global_config['shards']):
                shard_i = i
            v = random.randint(1, self.global_config['num_elements'])
            val = bytes('v' + str(v), 'utf-8')
            strt = shard_i * self.global_config['num_elements'] + 1
            k = random.randint(strt, strt + self.global_config['num_elements'])
            key = bytes('k' + str(k), 'utf-8')
            updates.append((key, val))
            rw_set_list.append(RWSet({}, {key: hash(val)}))
        return Transaction(txn_id, ts, updates, rw_set_list)

    def handleConnection(self, req_sock):
        while True:
            req = Messenger.recv(req_sock)
            if req == None:
                break
            if verbose:
                print("Recv msg {0}\n".format(req))
            body = req['body']
            if req['msg_type'] == MSG.DECISION:
                if self.cntr < self.global_config['client']['num_txns']:
                    self.recvDecision(body['final_decision'])
                else:
                    self.logResults()
                    break
                    
    def logResults(self):
        print('Done!')
        num_txns = self.global_config['client']['num_txns']
        num_txns *= int(self.global_config['client']['num_ops'] / OPS_PER_TXN)
        timeElapsed = time.time() - globalTime
        latency = timeElapsed/num_txns
        txnRate = num_txns/timeElapsed
        msg = str(latency) + ':' + str(txnRate) + ':' + str(num_txns)
        msg += ':' + str(len(self.global_config['shards'])) + '\n'
        
        if not os.path.exists('./results'):
            os.mkdir('results')
        file_path = './results/results.txt'
        fd = open(file_path,'a')
        fd.write(msg)
        fd.close()

    def performTransaction(self):
        self.cntr += 1
        txn = self.createTxn()
        msg = self.msg_mgr.create_end_transaction_msg(txn)
        Messenger.get().send(msg, (self.global_config['shards'][0]['ip_addr'], self.global_config['shards'][0]['port']))

    def recvDecision(self, final_decision):
        self.performTransaction()

if __name__ == "__main__":
    if len(sys.argv) != 3:
    	print("Correct Usage: {0} <config_file_path> <verbose>".format(sys.argv[0]))
    	sys.exit()
    
    config = json.load(open(sys.argv[1]))
    cl = Client(config)
    verbose = bool(int(sys.argv[2]))
    client_config = config['client']
    client_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    client_sock.bind((client_config['ip_addr'], client_config['port']))
    client_sock.listen(5)
    globalTime = time.time()
    txn_t = threading.Thread(target=cl.performTransaction)
    txn_t.start()
    (req_sock, addr) = client_sock.accept()
    t = threading.Thread(target=cl.handleConnection, args=(req_sock,))
    t.start()
    