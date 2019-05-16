from typing import Dict, List, TypeVar

Keyable = TypeVar('Keyable', int, str)

"""
Format: { key: (hash(value), r_max, w_max }
"""
class ReadSet(dict):
	def __init__(self, *args, **kwargs):
		super().__init__(*args, **kwargs)
		for k, v in self.items():
			if len(v) != 3:
				raise ValueError('Expected 3-tuple values.')

"""
Format: { key: hash(value) }
"""
class WriteSet(dict):
	def __init__(self, *args, **kwargs):
		super().__init__(*args, **kwargs)

class RWSet:
	def __init__(self, read_set: ReadSet, write_set: WriteSet):
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
		self.mht_update_time = 0
	def __hash__(self):
		return hash(str(self))
	def __str__(self):
		return str({
			'bid': self.bid,
			'txns': str(self.txns),
			'roots': self.roots,
			'prev_block_hash': self.prev_block_hash,
			'cosign': self.cosign,
			'mht_update_time': self.mht_update_time
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
