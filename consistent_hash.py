import bisect
import hashlib
import csv
import urllib.request
import json

# list of server endpoints
servers = ['http://localhost:5000','http://localhost:5001','http://localhost:5002','http://localhost:5003']

"""
Implement a consistent hashing ring.
"""
class ConsistentHashRing(object):
    
    """
    Create and initialize ConsistentHashRing.

    @param replicas number of replicas node replicas
    """
    def __init__(self, replicas=100):
        self.replicas_ = replicas
        self.keys_ = []
        self.nodes_ = {}

    """
    Returns hash for key.

    @param key the key to be
    """
    def _hash(self, key_):
        #print("_hash[{}]".format(key))
        return int(hashlib.md5(key_.encode()).hexdigest(), 16)

    """
    Returns iterable of replica hashes for nodename passed.

    @param nodename the name of the node to be added to ring
    """
    def _replica_iterator(self, nodename_):
        #print("_replica_iterator[{}]".format(nodename))
        return (self._hash("%s:%s" % (nodename_, i))
                for i in range(self.replicas_))

    """
    Instance method that is invoke when object is accessed in dict set mode.
    Creates hashes equal to replicas and node is assigned as value.

    @param nodename the node name (key) 
    @param node the node value (value)
    """
    def __setitem__(self, nodename, node):
        #print("__setitem__[{}, {}]".format(nodename, node))
        for hash_ in self._replica_iterator(nodename):
            if hash_ in self.nodes_:
                raise ValueError("Node name %r is already present" % nodename)
            self.nodes_[hash_] = node
            # insort inserts hash_ keeping sort order of keys_
            bisect.insort(self.keys_, hash_)
    
    """
    Instance method that is invoked when object is accessed in dict get mode.
    The node replica with hash value nearest but not less than that of the given 
    name is required. If the has of the given name is greated than the greatest hash,
    returns the lowest hashed node.

    @param key the data to be mapped to a node in the ring
    """
    def __getitem__(self, key):
        #print("__getitem__[{}]".format(key))
        hash_ = self._hash(key)
        # returns the right index position of hash_ in keys_ (no data inserted, just index returned)
        index_ = bisect.bisect(self.keys_, hash_)
        if index_ == len(self.keys_):
            index_ = 0
        return self.nodes_[self.keys_[index_]]

"""
1. Places nodes on the ring equal to the number of replicas.
2. Reads the csv file passed as program arg, parses csv data and identified the node for data hash.
3. HTTP post the csv data to the node identified
"""
def client():
    # creating hash ring with 100 replicas for each server in the list
    chr_ = ConsistentHashRing(100)

    # position servers on the hash ring
    for i in range(len(servers)):
        chr_["%s-%s" % ('server', i)] = servers[i]

    # now reading data in the csv and pushing it to servers
    with open('causes-of-death.csv', mode='r') as csvfile:
        csvreader_ = csv.reader(csvfile, delimiter=',')
        row_number_ = 0
        for row_ in csvreader_:
            if row_number_ == 0:
                # skip header
                row_number_ += 1
                continue
            #print(', '.join(row_))
            # generate data hash
            hash_ = chr_._hash("%s:%s:%s" % (row_[0], row_[2], row_[3]))
            # identify node on the hash ring based on data hash
            node_ = chr_["%s:%s:%s" % (row_[0], row_[2], row_[3])]
            #print("{} - {} - {}".format(hash_, node_, ','.join(row_)))
            # http request post payload
            payload_ = {'{}'.format(hash_):','.join(row_)}
            #print(payload_)
            # prepare http request and post payload
            req_ = urllib.request.Request("{}/api/v1/entries".format(node_))
            req_.add_header('Content-Type', 'application/json; charset=utf-8')
            json_data_ = json.dumps(payload_)
            json_data_bytes_ = json_data_.encode('utf-8')
            req_.add_header('Content-Length', len(json_data_bytes_))
            resp_ = urllib.request.urlopen(req_, json_data_bytes_)
            print("{} - {}".format(resp_.reason, payload_))

"""
Program entry point
"""
if __name__ == '__main__':
    client()