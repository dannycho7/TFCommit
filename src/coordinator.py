#! /usr/bin/env python3

class Coordinator:
	def __init__(self, init_roots: list[str]):
		self.last_roots = init_roots

	@staticmethod
	def init() -> 'Coordinator':
		# for each shard, add init root to last_roots
		pass
	def endTransaction(self, T_i):
		# send signed(get_vote(T_i)) to all shards in T_i
		pass
	def recvVote(self, vote):
		"""
		add to voted store
		check if all involved shards voted
			final_decision <- commit
			corrupted_shards <- {}
			for each shard S_j do
				fetch the last known root last_roots[S_j]
				if root_{i-1} =/= last_roots[S_j] then
					final_decision <- abort
					append S_j to corrupted shards
				else if decision = abort then
					final_decision <- abort
			if final_decision = commit then
				update last_roots[S_j] for each shard S_j
			send signed(decision(T_i, final_decision)) to all shards
		"""
		pass