#! /usr/bin/env python3

import json
import pickle
import socket
import sys
sys.path.insert(0, './lib')
from blockchain import *
from merkle_tree import MerkleTree, VO_C
import messaging
from msg_types import MSG, MessageManager

class Shard:
	def __init__(self, global_config, shard_i, mht, bch = Blockchain()):
		self.global_config = global_config
		shard_config = self.global_config['shards'][shard_i]
		self.msg_mgr = MessageManager((shard_config['ip_addr'], shard_config['port']))
		self.bch = bch
		self.mht = mht
		self.current_transaction = None
		self.vote_decisions = []
	def recvEndTransaction(self, req, txn_id, ts, rw_set, updates):
		self.current_transaction = self.bch.createBlock(txn_id, rw_set, [])
		msg = self.msg_mgr.create_get_vote_msg(self.current_transaction, updates)
		messaging.broadcast(msg, self.global_config['shards'])
	def recvGetVote(self, req, b_i, updates):
		modded_mht = MerkleTree.copyCreate(self.mht)
		for k, new_v in updates:
			modded_mht.update(k, new_v)
		msg = self.msg_mgr.create_vote_msg('commit', modded_mht.getRoot())
		messaging.send(msg, req['addr'])
		# TODO: free modded_mht?
	def recvVote(self, req, vote):
		self.vote_decisions.append(vote)
		if len(self.vote_decisions) == len(self.global_config['shards']):
			final_decision = 'commit'
			for vote_decision in self.vote_decisions:
				if vote_decision['decision'] == 'commit':
					self.current_transaction.signed_roots.append(vote_decision['root'])
				else:
					final_decision = 'abort'
			msg = self.msg_mgr.create_prepare_msg(final_decision, self.current_transaction)
			messaging.broadcast(msg, self.global_config['shards'])
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
	mht = MerkleTree([b'k1', b'v1'])
	sh = Shard(config, shard_i, mht)

	shard_config = config['shards'][shard_i]

	server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	server_sock.bind((shard_config['ip_addr'], shard_config['port']))
	server_sock.listen(5)
	while True:
		(client_sock, addr) = server_sock.accept()
		req = messaging.parse(client_sock.recv(2048))
		body = req['body']
		if req['msg_type'] == MSG.END_TRANSACTION:
			print("Recv end_transaction {0}".format(req))
			rw_set = pickle.loads(body['rw_set'])
			sh.recvEndTransaction(req, body['txn_id'], body['ts'], rw_set, body['updates'])
		elif req['msg_type'] == MSG.GET_VOTE:
			print("Recv get_vote {0}".format(req))
			block = pickle.loads(body['block'])
			sh.recvGetVote(req, block, body['updates'])
		elif req['msg_type'] == MSG.VOTE:
			print("Recv vote {0}".format(req))
		elif req['msg_type'] == MSG.PREPARE:
			pass