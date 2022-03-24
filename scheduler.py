import paramiko
from threading import Thread
import redis
import sys

# sample command to use this scheduler
# python3 scheduler.py no_of_executor redis_host spider_url
# python3 scheduler.py 3 172.16.221.234 https://gist.githubusercontent.com/firedrak/9bd24ba6a1f42d864c8a98b94cde4f36/raw/d4297a1025a270f3d61aadebd5dccfff29fac56b

args = sys.argv[1:]
if len(args) < 2:
    print('Provide redis host and spider url')


else:

    no_of_executor = int(args[0])
    redis_host = args[1]
    spider_url = args[2]
    # redis_host = '172.16.221.234'
    # spider_url = 'https://gist.githubusercontent.com/firedrak/9bd24ba6a1f42d864c8a98b94cde4f36/raw/d4297a1025a270f3d61aadebd5dccfff29fac56b'

    redis_port = 6379


    def start_executor(server):
        client = paramiko.client.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        host = server
        port = 5190
        user = 'ubuntu'
        client.connect(host, port, username=user)
        print(f'{host} => started')
        _stdin, _stdout,_stderr = client.exec_command("rm -rf shell")
        _stdout.read().decode()
        _stdin, _stdout,_stderr = client.exec_command("git clone https://github.com/firedrak/shell.git")
        _stdout.read().decode()
        print(f'{host} => cloned to https://github.com/firedrak/shell.git')
        _stdin, _stdout,_stderr = client.exec_command(f"bash shell/shell.sh {redis_host} {spider_url}")
        print(f'{host} => crawling started')
        print((_stdout.read().decode()))
        print(f'{host} => crawling completed')
        client.close()
        REDIS_CLI.lpush('server_list', server)

    # server_list = [{'host':"172.16.221.235", 'username':"ubuntu", 'port':5190}, {'host':"172.16.221.233", 'username':"ubuntu",  'port':5190}]
    
    REDIS_CLI = redis.StrictRedis(
        host=redis_host, port=redis_port, decode_responses=True)

    available_servers = int(REDIS_CLI.llen('server_list'))

    threads = []
    if no_of_executor <= available_servers:
        for i in range(no_of_executor):
            server = REDIS_CLI.rpop('server_list')
            print(server)
            t = Thread(target = start_executor, args = (server,))
            t.start()
            threads.append(t)

    else:
        print(f'server not available')
    # while True:
    #     for i in range(len(threads)):
    #         if not threads[i].is_alive():
    #             print(f'server {i +1} inactive')
    # ssh ubuntu@172.16.221.235  -p5190 'rm -rf shell | git clone https://github.com/firedrak/shell.git | bash shell/shell.sh'
