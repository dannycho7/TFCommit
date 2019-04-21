#! /usr/bin/env python3

import json
import socket
import sys
sys.path.insert(0, './lib')
from blockchain import *
from merkle_tree import MerkleTree, VO_C
import messaging
from msg_types import MSG, create_get_vote_msg, create_vote_msg, create_prepare_msg

class Shard:
	def __init__(self, global_conf, shard_i, mht, bch = Blockchain()):
		self.global_conf = global_conf
		self.bch = bch
		self.mht = mht
		self.current_transaction = None
		self.vote_decisions = []
	def recvEndTransaction(self, req, txn_id, ts, rwset):
		self.current_transaction = self.bch.createBlock(txn_id, rwset, [])
		messaging.broadcast(create_get_vote_msg(self.current_transaction), self.global_conf['servers'])
	def recvGetVote(self, req, b_i):
		modded_mht = MerkleTree.copyCreate(self.mht)
		# TODO: modify modded_mht based on rw-set in b_i
		ip_addr, port = req['addr']
		messaging.send(create_vote_msg('commit', VO_i, modded_mht.getRoot()), ip_addr, port)
		# TODO: free modded_mht?
	def recvVote(self, req, vote):
		self.vote_decisions.append(vote)
		if len(self.vote_decisions) == len(self.global_conf['servers']):
			final_decision = 'commit'
			for vote_decision in self.vote_decisions:
				if vote_decision['decision'] == 'commit':
					self.current_transaction.signed_roots.append(vote_decision['root'])
				else:
					final_decision = 'abort'
			messaging.broadcast(create_prepare_msg(final_decision, self.current_transaction), self.global_conf['servers'])
	def recvPrepare(self, req, decision, ch, b_i):
		if decision == 'commit':
			self.bch.appendBlock(b_i)
		# send ack to coordinator with schnorr response
	def recvAck(self, res):
		"""
		aggregate all schnorr-responses to form collective signature
		send b_i and multisig to client and cohorts.
		"""
		pass

if __name__ == "__main__":
	if len(sys.argv) != 3:
		print("Correct Usage: {0} <config_file_path> <shard_i>".format(sys.argv[0]))
		sys.exit()
	config = json.load(open(sys.argv[1]))
	shard_i = int(sys.argv[2])
	mht = MerkleTree([b'val1', b'val2'])
	sh = Shard(config, shard_i, mht)

	shard_config = config['shards'][shard_i]

	server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	server_sock.bind((shard_config['ip_addr'], shard_config['port']))
	server_sock.listen(5)
	while True:
		(client_sock, addr) = server_sock.accept()
		req = messaging.parse(client_sock.recv(2048), addr)
		if req['msg_type'] == MSG.END_TRANSACTION:
			pass
		elif req['msg_type'] == MSG.GET_VOTE:
			pass
		elif req['msg_type'] == MSG.VOTE:
			pass
		elif req['msg_type'] == MSG.PREPARE:
			pass