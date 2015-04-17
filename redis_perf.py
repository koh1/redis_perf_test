import redis
import sys
import datetime
import random
import string
import pandas as pd

def test_list(redis, keys, length, retry):
    print "generating data..."
    for i in range(length):
        redis.lpush("key", ''.join([random.choice(string.ascii_letters + string.digits) for i in range(8)]))
    for i in range(keys - 1):
        redis.lpush(''.join([random.choice(string.ascii_letters + string.digits) for i in range(8)]),
                    ''.join([random.choice(string.ascii_letters + string.digits) for i in range(8)]))
    delays = []
    print "testing..."
    for i in range(retry):
        t1 = datetime.datetime.now()
        redis.lrange("key", 0, -1)
        t2 = datetime.datetime.now()
        td = t2 - t1
        delays.append(td.days * 24 * 3600 * 1000 + td.seconds * 1000 + td.microseconds / 1000.0)

    result = pd.Series(delays)
    result.to_csv("list_%d_%d.csv" % (length, retry))
    print result.describe()
    

def test_hash(redis, keys, fields, retry):
    print "generating data..."
    for i in range(fields):
        redis.hset("key", ''.join([random.choice(string.ascii_letters + string.digits) for i in range(8)]), 1)
    for i in range(keys - 1):
        redis.hset(''.join([random.choice(string.ascii_letters + string.digits) for i in range(8)]),
                   ''.join([random.choice(string.ascii_letters + string.digits) for i in range(8)]), 1)

    delays = []
    print "testing..."
    for i in range(retry):
        t1 = datetime.datetime.now()
        redis.hgetall("key")
        t2 = datetime.datetime.now()
        td = t2 - t1
        delays.append(td.days * 24 * 3600 * 1000 + td.seconds * 1000 + td.microseconds / 1000.0)

    result = pd.Series(delays)
    result.to_csv("hash_%d_%d.csv" % (fields, retry))
    print result.describe()

if __name__ == '__main__':
    p = sys.argv
    r = redis.Redis(host=p[2],
                    port=int(p[3]),
                    db=int(p[4]))
    r.flushdb()
    
    if p[1] == 'list':
        test_list(r, int(p[5]), int(p[6]), int(p[7]))
    elif p[1] == 'hash':
        test_hash(r, int(p[5]), int(p[6]), int(p[7]))
