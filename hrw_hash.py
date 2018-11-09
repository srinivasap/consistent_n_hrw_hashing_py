import csv
import urllib.request
import json
import mmh3
import math

# list of server endpoints
servers = ['http://localhost:5000','http://localhost:5001','http://localhost:5002','http://localhost:5003']

# Class representing a node that is assigned keys as part of a weighted rendezvous hash
"""
Model to maintain node metadata - name, seed, weight
"""
class Node(object):
    """
    create and intialize Node instance

    @param name the node name
    @param seed the seed unique to each node
    @param weight the node given to the node
    """
    def __init__(self, name, seed, weight):
        self.name_ = name
        self.seed_ = seed
        self.weight_ = weight

    """
    string representation of the object
    """
    def __str__(self):
        return "[" + self.name_ + " (" + str(self.seed_) + ", " + str(self.weight_) + ")]"

    """
    Computes the weight of node for the key

    @param key the key of data
    """
    def compute_weighted_score(self, key):
        hash_1, hash_2 = mmh3.hash64(str(key), 0xFFFFFFFF & self.seed_)
        hash_f = int_to_float(hash_2)
        score = 1.0 / -math.log(hash_f)
        return self.weight_ * score

"""
Converts a uniformly random 64-bit integer to uniformly random floating point number on interval [0, 1)
"""
def int_to_float(value):
  fifty_three_ones = (0xFFFFFFFFFFFFFFFF >> (64 - 53))
  fifty_three_zeros = float(1 << 53)
  return (value & fifty_three_ones) / fifty_three_zeros

"""
Determines which node, of a set of nodes of various weights, is responsible for the provided key
"""
def determine_responsible_node(nodes, key):
  highest_score, champion = -1, None
  for node in nodes:
    score = node.compute_weighted_score(key)
    if score > highest_score:
      champion, highest_score = node, score
  return champion

"""
1. Places nodes on the ring equal to the number of replicas.
2. Reads the csv file passed as program arg, parses csv data and identified the node for data hash.
3. HTTP post the csv data to the node identified
"""
def client():
    nodes_ = []
    # position servers on the hash ring
    for i in range(len(servers)):
        nodes_.append(Node(servers[i], 123 * i, 100))
    
    # now reading data in the csv and pushing it to selected server
    with open('causes-of-death.csv', mode='r') as csvfile:
        csvreader_ = csv.reader(csvfile, delimiter=',')
        row_number_ = 0
        for row_ in csvreader_:
            if row_number_ == 0:
                # skip header
                row_number_ += 1
                continue
            #print(', '.join(row_))
            key_ = "%s:%s:%s" % (row_[0], row_[2], row_[3])
            hash_ = mmh3.hash64(key_)[0]
            node_ = determine_responsible_node(nodes_, key_)
            #print("{} - {} - {}".format(hash_, str(node_), ','.join(row_)))
            payload_ = {'{}'.format(hash_):','.join(row_)}
            #print(payload_)
            # prepare http request and post payload
            req_ = urllib.request.Request("{}/api/v1/entries".format(node_.name_))
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

