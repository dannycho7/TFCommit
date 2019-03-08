from ctypes import *

so = cdll.LoadLibrary("./mht-cpp/src/mhtc.so")
dat = (c_char_p * 4)(c_char_p(b'k1'), c_char_p(b'v1'), c_char_p(b'k2'), c_char_p(b'v2'))
so.mht_create.argtypes = [POINTER(c_char_p), c_int]
so.mht_create.restype = c_void_p # why does this prevent the bug?
mht = so.mht_create(dat, 4)


so.mht_get_root.argtypes = [c_void_p]
so.mht_get_root.restype= c_char_p
print(so.mht_get_root(mht))

"""
class VO_C(Structure):
	_fields_ = [("val", c_char_p), ("sibling_path", POINTER(c_char_p)), ("size", c_int)]

so.mht_get_vo.argtypes = [c_void_p, c_char_p]
so.mht_get_vo.restype = VO_C
vo = so.mht_get_vo(mht, c_char_p(b'k1'))

print(vo.val)
print(vo.sibling_path[0])
print(vo.size)
"""

so.mht_update.argtypes = [c_void_p, c_char_p, c_char_p]
so.mht_update(mht, c_char_p(b'k1'), c_char_p(b'v5'))

print(so.mht_get_root(mht))
