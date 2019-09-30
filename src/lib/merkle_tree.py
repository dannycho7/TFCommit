#! /usr/bin/env python3

from ctypes import *
from typing import Dict, List

class VO_C(Structure):
	_fields_ = [("val", c_char_p), ("mSiblingPath", POINTER(c_char_p)), ("size", c_int)]

HASH_FUNC_TYPE = CFUNCTYPE(c_char_p, c_char_p)
HASH_FUNC_OUT_NBYTES = 9

mht_so = cdll.LoadLibrary("../mht-cpp/src/mhtc.so")
mht_so.mhtCreate.argtypes = [POINTER(c_char_p), c_int, HASH_FUNC_TYPE]
mht_so.mhtCreate.restype = c_void_p
mht_so.mhtGetRoot.argtypes = [c_void_p]
mht_so.mhtGetRoot.restype= c_char_p
mht_so.mhtUpdate.argtypes = [c_void_p, c_char_p, c_char_p]
mht_so.mhtGetVO.argtypes = [c_void_p, c_char_p]
mht_so.mhtGetVO.restype = VO_C

pythonapi.PyMem_RawMalloc.restype = c_void_p

@CFUNCTYPE(c_char_p, c_char_p)
def h(x):
	out = pythonapi.PyMem_RawMalloc(HASH_FUNC_OUT_NBYTES)
	hash_bytes = hash(x).to_bytes(8, byteorder='big', signed=True)
	memmove(out, hash_bytes + b'\x00', HASH_FUNC_OUT_NBYTES)
	return out

class MerkleTree:
	def __init__(self, kv_map: Dict[bytes, bytes]):
		self.kv_map = kv_map
		raw_data = [d for k, v in self.kv_map.items() for d in [k, v]]
		data = (c_char_p * len(raw_data))(*raw_data)
		self.mht_obj = mht_so.mhtCreate(data, len(data), h)
	@classmethod
	def copyCreate(cls, rh_mht: 'MerkleTree'):
		return cls(rh_mht.kv_map)
	def update(self, k: bytes, v: bytes) -> None:
		mht_so.mhtUpdate(self.mht_obj, c_char_p(k), c_char_p(v))
		self.kv_map[k] = v
	def getRoot(self) -> bytes:
		return mht_so.mhtGetRoot(self.mht_obj)
	def getVO(self, k: bytes) -> 'VO_C':
		return mht_so.mhtGetVO(self.mht_obj, c_char_p(k))
