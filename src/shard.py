class Shard:
	def __init__(self, i):
		self.i = i
	def getVote(self, vote_req):
		"""
		if message contains valid signature then
			decide to commit or abort T_i
			VO_i <- null
			if decision = commit then
				construct VO_i for the tuples accessed in T_i
			send(signed(vote(decision, VO_i, root_{i-1}))) to vote requester (Coordinator)
		"""
		pass