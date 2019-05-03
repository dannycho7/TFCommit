from typing import Dict, List, TypeVar

Keyable = TypeVar('Keyable', int, str)

class ModSet(list):
	def __init__(self, *args, **kwargs):
		super().__init__(*args, **kwargs)
		for item in self:
			if len(item) != 2:
				raise ValueError('Expected 2-tuple list.')

class RWSet:
	def __init__(self, read_set: ModSet, write_set: ModSet):
		self.read_set = read_set
		self.write_set = write_set
	def __str__(self):
		return str({
			'read_set': self.read_set,
			'write_set': self.write_set
		})

class Transaction:
	def __init__(self, rw_set: RWSet):
		self.rw_set = rw_set
	def __str__(self):
		return str({
			'rw_set': str(self.rw_set)
		})

class Block:
	def __init__(self, bid: int, txns: List[Transaction], roots: Dict[Keyable, str], prev_block_hash: str):
		self.bid = bid
		self.txns = txns
		self.roots = roots
		self.prev_block_hash = prev_block_hash
		self.cosign = ()
	def __hash__(self):
		return hash(str(self))
	def __str__(self):
		return str({
			'bid': self.bid,
			'txns': str(self.txns),
			'roots': self.roots,
			'prev_block_hash': self.prev_block_hash,
			'cosign': self.cosign
		})

class Blockchain:
	def __init__(self):
		init_block = Block(0, [], {}, '')
		self.chain = [init_block]
	def appendBlock(self, block: Block):
		self.chain.append(block)
	def createBlock(self, bid, txns, roots) -> Block:
		prev_block_hash = hash(self.chain[-1])
		return Block(bid, txns, roots, prev_block_hash)
