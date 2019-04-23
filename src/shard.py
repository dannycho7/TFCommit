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
from cosi import *

class CurrentExecution:
	def __init__(self, txn_id):
		self.txn_id = txn_id
		self.vote_decisions = []
		self.ack_resps = []
		self.sch_challenge = None

class Shard:
	def __init__(self, global_config, shard_i, mht, bch = Blockchain()):
		self.global_config = global_config
		shard_config = self.global_config['shards'][shard_i]
		self.msg_mgr = MessageManager((shard_config['ip_addr'], shard_config['port']))
		self.bch = bch
		self.mht = mht
		self.current_transaction = None
		self.current_execution = None
		self.cosi = CoSi()

	def recvEndTransaction(self, req, txn_id, ts, rw_set, updates):
		self.current_transaction = self.bch.createBlock(txn_id, rw_set, [])
		self.current_execution = CurrentExecution(txn_id)
		msg = self.msg_mgr.create_get_vote_msg(self.current_transaction, updates)
		messaging.broadcast(msg, self.global_config['shards'])

	def recvGetVote(self, req, block, updates):
		modded_mht = MerkleTree.copyCreate(self.mht)
		for k, new_v in updates:
			modded_mht.update(k, new_v)
		sch_commitment = self.cosi.commitment()
		msg = self.msg_mgr.create_vote_msg('commit', modded_mht.getRoot(), sch_commitment)
		messaging.send(msg, req['addr'])
		# TODO: free modded_mht?

	def recvVote(self, vote):
		self.current_execution.vote_decisions.append(vote)
		if len(self.current_execution.vote_decisions) == len(self.global_config['shards']):
			final_decision = 'commit'
			sch_commits = []
			for vote_decision in self.current_execution.vote_decisions:
				if vote_decision['decision'] == 'commit':
					self.current_transaction.roots.append(vote_decision['root'])
					sch_commits.append(vote_decision['sch_commitment'])
				else:
					final_decision = 'abort'
			sch_challenge = self.cosi.challenge(str(self.current_transaction), sch_commits)
			self.current_execution.sch_challenge = sch_challenge
			msg = self.msg_mgr.create_prepare_msg(final_decision, self.current_transaction, 
												  sch_challenge)
			messaging.broadcast(msg, self.global_config['shards'])

	def recvPrepare(self, req, body):
		block = pickle.loads(body['block'])
		#TODO: If decision = commit, Verify if the block is correct
		sch_challenge = body['ch']
		sch_response = self.cosi.response(sch_challenge)
		msg = self.msg_mgr.create_ack_msg(sch_response)
		messaging.send(msg, req['addr'])

	def recvAck(self, res):
		curr_exec = self.current_execution
		curr_exec.ack_resps.append(res)
		if len(curr_exec.ack_resps) == len(self.global_config['shards']):
			aggrR = self.cosi.aggr_response(curr_exec.ack_resps)
			self.current_transaction.cosign = tuple((curr_exec.sch_challenge, aggrR))
			msg = self.msg_mgr.create_decision_msg(self.current_transaction)
			#Respond to client
			messaging.broadcast(msg, self.global_config['shards'])

	def recvDecision(self, body):
		block = pickle.loads(body['block'])
		self.bch.appendBlock(block)

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
		print("Recv msg {0}\n".format(req))
		body = req['body']
		if req['msg_type'] == MSG.END_TRANSACTION:
			rw_set = pickle.loads(body['rw_set'])
			sh.recvEndTransaction(req, body['txn_id'], body['ts'], rw_set, body['updates'])
		elif req['msg_type'] == MSG.GET_VOTE:
			block = pickle.loads(body['block'])
			sh.recvGetVote(req, block, body['updates'])
		elif req['msg_type'] == MSG.VOTE:
			sh.recvVote(body)
		elif req['msg_type'] == MSG.PREPARE:
			sh.recvPrepare(req, body)
		elif req['msg_type'] == MSG.ACK:
			sh.recvAck(body['sch_response'])
		elif req['msg_type'] == MSG.DECISION:
			sh.recvDecision(body)
