import time
import redis
import sys


# Sample command to use reids_status.py
# python3 reids_status.py redis_host

args = sys.argv[1:]
if args:
    redis_host = args[0]


class redisCli:

#     redis_host = '172.16.221.234'
    redis_host = redis_host
    redis_port = 6379

    REDIS_CLI = redis.StrictRedis(
        host=redis_host, port=redis_port, decode_responses=True)

    def get_status(self):
        return self.REDIS_CLI.get('state')

    def length_of_queue(self, key):
        return self.REDIS_CLI.llen(key)

    def get_process_count(self):
        return self.REDIS_CLI.get('process')

redisClient = redisCli()

while True:

    time.sleep(1)

    job_len = redisClient.length_of_queue('job_queue')
    page_len = redisClient.length_of_queue('page_queue')
    data_len = redisClient.length_of_queue('data')
    state = redisClient.get_status()
    count = redisClient.get_process_count()
    print(f'job_len:{job_len} page_len:{page_len} count:{count} data_len:{data_len} state:{state}')