import redis, sys, time, json
from redis.sentinel import Sentinel

def get_master(sentinel):
    begin = int(time.time())
    while True:
        master = sentinel.master_for(service_name='mymaster', db=0)
        try:
            if master.ping() and master.info()['role'] == 'master':
                print time.asctime(), 'master_info:', sentinel.discover_master('mymaster')  
                return master
        except (redis.exceptions.TimeoutError,
                redis.exceptions.BusyLoadingError,
                redis.exceptions.ConnectionError,
                redis.sentinel.MasterNotFoundError, ), e:
            print time.asctime(), e, 'time cost:', str(int(time.time())-begin), 's'
            time.sleep(1)
            continue

if __name__ == '__main__':
    sentinel = Sentinel([('r0', 26379),('r1', 26379),('r2', 26379),], socket_timeout=0.1)
    master = get_master(sentinel) 
    loop_count = 100000000
    pad_length = 16
    ok_till = 0
    try:
        pad_string = "_"*(pad_length-16)
        for x in xrange(loop_count):
            key = '%s_%010d' % (sys.argv[1], x)
            val = '%016d' % (x)
            try:
                master.set(key, ''.join([val, pad_string,]))
                ok_till = x
                time.sleep(0.01)
            except (redis.exceptions.TimeoutError,
                    redis.exceptions.BusyLoadingError,
                    redis.exceptions.ConnectionError,
                    redis.sentinel.MasterNotFoundError, ), e:
                print time.asctime(), 'exception caught.', e
                del master
                master = get_master(sentinel)
                master.set(key, ''.join([val, pad_string,]))
            if 0 == (x + 1) % 100:
                print time.asctime(), x+1, 'finished', sys.argv[1]
    except:
        print ok_till
        raise
