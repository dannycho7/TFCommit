#! /usr/bin/env python3

import json
import sys
import socket
import threading
sys.path.insert(0, './lib')
from blockchain import RWSet
from messenger import Messenger
from msg_types import MessageManager, MSG

totalTxns = 10
class Client:
    def __init__(self, config):
        self.config = config
        self.shard_config = self.config['shards'][0]
        client_config = self.config['client']
        self.msg_mgr = MessageManager((client_config['ip_addr'], client_config['port']))
        self.cntr = 0

    def performTransaction(self):
        rw_set_list = [RWSet([], [b'k1', hash(b'v2')])]
        updates = [(b'k1', b'v2')]

        msg = self.msg_mgr.create_end_transaction_msg(1, 1, rw_set_list, updates)
        Messenger.get().send(msg, (self.shard_config['ip_addr'], self.shard_config['port']))

    def recvDecision(self, final_decision):
        if self.cntr < totalTxns:
            self.cntr += 1
            self.performTransaction()

def handleConnection(cl, req_sock):
    while True:
        req = Messenger.recv(req_sock)
        if req == None:
            break
        print("Recv msg {0}\n".format(req))
        body = req['body']
        if req['msg_type'] == MSG.DECISION:
            cl.recvDecision(body['final_decision'])

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
    cl.performTransaction()
    while True:
        (req_sock, addr) = client_sock.accept()
        t = threading.Thread(target=handleConnection, args=(cl, req_sock))
        t.start()
    