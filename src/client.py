#! /usr/bin/env python3

import json
import sys
sys.path.insert(0, './lib')
from blockchain import RWSet
import messaging
from msg_types import MessageManager

if len(sys.argv) != 2:
	print("Correct Usage: {0} <config_file_path>".format(sys.argv[0]))
	sys.exit()
config = json.load(open(sys.argv[1]))
client_config = config['client']
shard_config = config['shards'][0]
rw_set = RWSet([], ['k1', hash('v2')])
updates = [('k1', 'v2')]

msg_mgr = MessageManager((client_config['ip_addr'], client_config['port']))
messaging.send(msg_mgr.create_end_transaction_msg(1, 1, rw_set, updates), (shard_config['ip_addr'], shard_config['port']))
