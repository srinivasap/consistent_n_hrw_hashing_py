# consistent_n_hrw_hashing_py

##Prerequisites:
Listed in requirements.txt

##Instructions:
1. Run api servers
    - $ python3 api.py 5000
    - $ python3 api.py 5001
    - $ python3 api.py 5002
    - $ python3 api.py 5003
2. Run consistent hashing client
    - $ python3 consistent_hash.py
3. Verify data distribution in each server
    - $ curl http://localhost:5000/api/v1/entries > ch_5000.out
    - $ curl http://localhost:5001/api/v1/entries > ch_5001.out
    - $ curl http://localhost:5002/api/v1/entries > ch_5002.out
    - $ curl http://localhost:5003/api/v1/entries > ch_5003.out
4. Run hrw client
    - $ python3 hrw_hash.py
5. Verify data distribution in each server
    - $ curl http://localhost:5000/api/v1/entries > hrw_5000.out
    - $ curl http://localhost:5001/api/v1/entries > hrw_5001.out
    - $ curl http://localhost:5002/api/v1/entries > hrw_5002.out
    - $ curl http://localhost:5003/api/v1/entries > hrw_5003.out