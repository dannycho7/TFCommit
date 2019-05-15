from fastecdsa import curve, keys, point
from fastecdsa.curve import P256
import hashlib

class CoSi():

    def __init__(self):
        self.sec_key = keys.gen_private_key(curve.P256)
        self.pub_key = keys.get_public_key(self.sec_key, curve.P256)
        self.v = 0


    def public_key(self):
        return self.pub_key

    def commitment(self):
        if self.v == 0:
            self.v = keys.gen_private_key(curve.P256)
            self.V = keys.get_public_key(self.v, curve.P256) # V = G^v
        return self.V

    def response(self, ch): #ch --> str
        ch = int(ch, 16)
        r = self.v -  ch * self.sec_key
        return r

    def extract_points_from_str(self, strPoint):
        x = strPoint.split('\n')[0][3:]
        y = strPoint.split('\n')[1][3:]
        return point.Point(int(x, 16), int(y, 16))

    def aggr_commits(self, commits):
        if not commits or len(commits) == 0:
            return
        aggrV = self.extract_points_from_str(commits.pop(0))
        for c in commits:
            aggrV += self.extract_points_from_str(c)
        return aggrV

    def challenge(self, msg, commits):
        aggrV = self.aggr_commits(commits)
        if not aggrV:
            return
        aggrMsg = msg + str(aggrV)
        encodedMsg = aggrMsg.encode('utf-8')
        ch = hashlib.sha256(encodedMsg)
        return ch.hexdigest()

    def aggr_response(self, responses):
        aggrR = 0
        if not responses or len(responses) == 0:
            return aggrR
        for r in responses:
            aggrR += r
        return aggrR

    @staticmethod
    def aggr_public_keys(public_keys):
        if not public_keys or len(public_keys) == 0:
            return
        aggrP = public_keys.pop(0)
        for pk in public_keys:
            aggrP += pk
        return aggrP

    @staticmethod
    def verify(ch, aggrR, aggrPK, msg):
        if not (aggrR and aggrPK):
            return
        Gr = keys.get_public_key(aggrR, curve.P256) # G^aggrR
        aggrV_1 = Gr + ch * aggrPK
        aggrMsg = msg + str(aggrV_1)
        encodedMsg = aggrMsg.encode('utf-8')
        ch_1 = hashlib.sha256(encodedMsg)
        ch_1 = int(ch_1.hexdigest(), 16)
        assert ch == ch_1

