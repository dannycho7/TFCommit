#! /usr/bin/env python3

import json
import pickle
import socket
import sys
import threading
import time
sys.path.insert(0, './lib')
from blockchain import *
from collections import deque
from merkle_tree import MerkleTree, VO_C
from messenger import Messenger
from msg_types import MSG, TwoPCMessageManager
from cosi import *

class CurrentExecution:
    def __init__(self, block):
        self.block = block
        self.final_decision = ''
        self.vote_list = []
        self.updates = []

class Shard:
    def __init__(self, global_config, shard_i, data, data_ts, bch = Blockchain()):
        self.global_config = global_config
        self.shard_i = shard_i
        self.data = data
        self.data_ts = data_ts
        self.bch = bch
        self.pending_block = None
        self.current_execution = None
        self.lock = threading.Lock()
        shard_config = self.global_config['shards'][self.shard_i]
        self.msg_mgr = TwoPCMessageManager((shard_config['ip_addr'], shard_config['port']))
        self.req_q = deque()

    def handleReq(self, req):
        if verbose:
            print("Recv msg {0}\n".format(req))
        body = req['body']
        self.lock.acquire()
        if req['msg_type'] == MSG.END_TRANSACTION:              
            rw_set_list = pickle.loads(body['rw_set_list'])
            self.recvEndTransaction(req, body['txn_id'], body['ts'], rw_set_list, body['updates'])
        elif req['msg_type'] == MSG.GET_VOTE:
            block = pickle.loads(body['block'])
            self.recvGetVote(req, block, body['updates'])
        elif req['msg_type'] == MSG.VOTE:
            self.recvVote(body)
        elif req['msg_type'] == MSG.DECISION:
            self.recvDecision(body['final_decision'], body)
        q_req = self.req_q.pop() if self.current_execution is None and len(self.req_q) > 0 else None
        self.lock.release()
        if q_req:
            self.handleReq(q_req)

    def handleConnection(self, client_sock):
        while True:
            req = Messenger.recv(client_sock)
            if req is None:
                raise ConnectionError('Something went wrong with the connection')
            t = threading.Thread(target=self.handleReq, args=(req,))
            t.start()

    def recvEndTransaction(self, req, txn_id, ts, rw_set_list, updates):
        if self.current_execution:
            self.req_q.appendleft(req)
            return
        txns = [Transaction(rw_set) for rw_set in rw_set_list]
        self.pending_block = self.bch.createBlock(ts, txns, {})
        self.current_execution = CurrentExecution(self.pending_block)
        msg = self.msg_mgr.create_get_vote_msg(self.pending_block, updates)
        Messenger.get().broadcast(msg, self.global_config['shards'])

    def recvGetVote(self, req, block, updates):
        if self.current_execution and self.current_execution.block.bid != block.bid:
            self.req_q.append(req)
            return
        self.current_execution = CurrentExecution(block) if self.current_execution is None else self.current_execution
        self.current_execution.updates = updates
        msg = self.msg_mgr.create_vote_msg(self.shard_i, 'commit')
        Messenger.get().send(msg, req['addr'])

    def recvVote(self, v):
        self.current_execution.vote_list.append(v)
        if len(self.current_execution.vote_list) == len(self.global_config['shards']):
            final_decision = 'commit'
            for vote in self.current_execution.vote_list:
                if vote['decision'] == 'abort':
                    final_decision = 'abort'
            self.current_execution.final_decision = final_decision
            msg = self.msg_mgr.create_decision_msg(final_decision, self.pending_block)
            Messenger.get().send(msg, (self.global_config["client"]["ip_addr"], self.global_config["client"]["port"]))
            Messenger.get().broadcast(msg, self.global_config['shards'])

    def recvDecision(self, final_decision, body):
        if final_decision == 'commit':
            block = pickle.loads(body['block'])
            if not self.current_execution or self.current_execution.block.bid != block.bid:
                raise ValueError('Invalid CurrentExecution. Decision invalid.')
            for k, new_v in self.current_execution.updates:
                self.data[k] =  new_v
            self.bch.appendBlock(block)
            for txn in block.txns:
                for k in txn.rw_set.write_set.keys():
                    if k in self.data_ts:
                        self.data_ts[k] = (block.bid, block.bid)
            self.current_execution = None

def createData(shard_i, num_elements):
    strt = shard_i * num_elements + 1
    kv_map = { bytes('k'+str(i), 'utf-8'): bytes('v'+str(i), 'utf-8') for i in range(strt, strt + num_elements) }
    return kv_map

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Correct Usage: {0} <config_file_path> <shard_i> <verbose>".format(sys.argv[0]))
        sys.exit()
    config = json.load(open(sys.argv[1]))
    shard_i = int(sys.argv[2])
    data = createData(shard_i, config['num_elements'])
    data_ts = {k: (0, 0) for k in data.keys()}
    sh = Shard(config, shard_i, data, data_ts)

    shard_config = config['shards'][shard_i]
    verbose = bool(int(sys.argv[3]))
    server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_sock.bind((shard_config['ip_addr'], shard_config['port']))
    server_sock.listen(5)
    while True:
        (client_sock, addr) = server_sock.accept()
        t = threading.Thread(target=sh.handleConnection, args=(client_sock,))
        t.start()
