#! /usr/bin/env python3

import json
import pickle
import socket
import sys
import threading
sys.path.insert(0, './lib')
from blockchain import *
from messenger import Messenger
from msg_types import MSG, TwoPCMessageManager

class CurrentExecution:
    def __init__(self, bid):
        self.bid = bid
        self.vote_decisions = []
        self.updates = []

class Shard:
    def __init__(self, global_config, shard_i, data, bch = Blockchain()):
        self.global_config = global_config
        self.bch = bch
        self.current_transaction = None
        self.current_execution = None
        self.lock = threading.Lock()
        self.data = data
        shard_config = self.global_config['shards'][shard_i]
        self.msg_mgr = TwoPCMessageManager((shard_config['ip_addr'], shard_config['port']))

    def recvEndTransaction(self, req, txn_id, ts, rw_set, updates):
        self.current_transaction = self.bch.createBlock(ts, rw_set, [])
        self.current_execution = CurrentExecution(ts)
        msg = self.msg_mgr.create_get_vote_msg(self.current_transaction, updates)
        Messenger.get().broadcast(msg, self.global_config['shards'])

    def recvGetVote(self, req, block, updates):
        if not self.current_execution or self.current_execution.bid != block.bid:
            self.current_execution = CurrentExecution(block.bid)
        self.current_execution.updates = updates
        msg = self.msg_mgr.create_vote_msg('commit')
        Messenger.get().send(msg, req['addr'])

    def recvVote(self, vote):
        self.current_execution.vote_decisions.append(vote)
        if len(self.current_execution.vote_decisions) == len(self.global_config['shards']):
            final_decision = 'commit'
            for vote_decision in self.current_execution.vote_decisions:
                if vote_decision['decision'] == 'abort':
                    final_decision = 'abort'
            msg = self.msg_mgr.create_decision_msg(final_decision, self.current_transaction)
            Messenger.get().send(msg, (self.global_config["client"]["ip_addr"], self.global_config["client"]["port"]))
            Messenger.get().broadcast(msg, self.global_config['shards'])

    def recvDecision(self, final_decision, body):
        if final_decision == 'commit':
            for k, new_v in self.current_execution.updates:
                self.data[k] =  new_v
            block = pickle.loads(body['block'])
            self.bch.appendBlock(block)
            
def handleConnection(sh, client_sock):
    while True:
        req = Messenger.recv(client_sock)
        if req == None:
            break
        print("Recv msg {0}\n".format(req))
        body = req['body']
        sh.lock.acquire()
        if req['msg_type'] == MSG.END_TRANSACTION:
            rw_set = pickle.loads(body['rw_set'])
            sh.recvEndTransaction(req, body['txn_id'], body['ts'], rw_set, body['updates'])
        elif req['msg_type'] == MSG.GET_VOTE:
            block = pickle.loads(body['block'])
            sh.recvGetVote(req, block, body['updates'])
        elif req['msg_type'] == MSG.VOTE:
            sh.recvVote(body)
        elif req['msg_type'] == MSG.DECISION:
            sh.recvDecision(body['final_decision'], body)
        sh.lock.release()

def initData():
    data = {}
    for i in range(1, 1000):
        data['k'+str(i)] = 'v'+str(i)
    return data

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Correct Usage: {0} <config_file_path> <shard_i>".format(sys.argv[0]))
        sys.exit()
    config = json.load(open(sys.argv[1]))
    shard_i = int(sys.argv[2])
    data = initData()
    sh = Shard(config, shard_i, data)

    shard_config = config['shards'][shard_i]

    server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_sock.bind((shard_config['ip_addr'], shard_config['port']))
    server_sock.listen(5)
    while True:
        (client_sock, addr) = server_sock.accept()
        t = threading.Thread(target=handleConnection, args=(sh, client_sock))
        t.start()
