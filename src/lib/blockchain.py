from typing import List

class ModSet(list):
	def __str__(self):
		return ''.join(map(str, self))

class RWSet:
	def __init__(self, read_set: ModSet, write_set: ModSet):
		self.read_set = read_set
		self.write_set = write_set
	def __hash__(self):
		return hash(str(self.read_set) + str(self.write_set))

class Block:
	def __init__(self, txn_id: int, rw_set: RWSet, signed_roots: List[str], prev_block_hash: str):
		self.txn_id = txn_id
		self.rw_set = rw_set
		self.signed_roots = signed_roots
		self.prev_block_hash = prev_block_hash
	def __hash__(self):
		return hash(str(self.txn_id) + str(RWSet) + ''.join(signed_roots) + prev_block_hash)

class Blockchain:
	def __init__(self):
		init_block = Block(0, RWSet(ModSet(), ModSet()), [], '')
		self.chain = [init_block]
	def appendBlock(self, block: Block):
		self.chain.append(block)
	def createBlock(self, txn_id, rw_set, signed_roots) -> Block:
		prev_block_hash = hash(self.chain[-1])
		return Block(txn_id, rw_set, signed_roots, prev_block_hash)
