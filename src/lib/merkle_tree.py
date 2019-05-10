#! /usr/bin/env python3

from ctypes import *
from typing import Dict, List

class VO_C(Structure):
	_fields_ = [("val", c_char_p), ("sibling_path", POINTER(c_char_p)), ("size", c_int)]

mht_so = cdll.LoadLibrary("../mht-cpp/src/mhtc.so")
mht_so.mht_create.argtypes = [POINTER(c_char_p), c_int]
mht_so.mht_create.restype = c_void_p
mht_so.mht_get_root.argtypes = [c_void_p]
mht_so.mht_get_root.restype= c_char_p
mht_so.mht_update.argtypes = [c_void_p, c_char_p, c_char_p]
mht_so.mht_get_vo.argtypes = [c_void_p, c_char_p]
mht_so.mht_get_vo.restype = VO_C

class MerkleTree:
	def __init__(self, kv_map: Dict[bytes, bytes]):
		self.kv_map = kv_map
		raw_data = [d for k, v in self.kv_map.items() for d in [k, v]]
		data = (c_char_p * len(raw_data))(*raw_data)
		self.mht_obj = mht_so.mht_create(data, len(data))
	@classmethod
	def copyCreate(cls, rh_mht: 'MerkleTree'):
		return cls(rh_mht.kv_map)
	def update(self, k: bytes, v: bytes) -> None:
		mht_so.mht_update(self.mht_obj, c_char_p(k), c_char_p(v))
		self.kv_map[k] = v
	def getRoot(self) -> bytes:
		return mht_so.mht_get_root(self.mht_obj)
	def getVO(self, k: bytes) -> 'VO_C':
		return mht_so.mht_get_vo(self.mht_obj, c_char_p(k))
