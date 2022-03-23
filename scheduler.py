import paramiko
from threading import Thread
import time
import sys

# sample command to use this scheduler
# python3 scheduler.py redis_host spider_url
# python3 scheduler.py 172.16.221.234 https://gist.githubusercontent.com/firedrak/9bd24ba6a1f42d864c8a98b94cde4f36/raw/d4297a1025a270f3d61aadebd5dccfff29fac56b

args = sys.argv[1:]
if len(args) < 2:
    print('Provide redis host and spider url')

else:

    redis_host = args[0]
    spider_url = args[1]
    # redis_host = '172.16.221.234'
    # spider_url = 'https://gist.githubusercontent.com/firedrak/9bd24ba6a1f42d864c8a98b94cde4f36/raw/d4297a1025a270f3d61aadebd5dccfff29fac56b'

    def start_crawling(server):
        client = paramiko.client.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        host = server['host']
        port = server['port']
        user = server['username']
        client.connect(host, port, username=user)
        print(f'{host} => started')
        _stdin, _stdout,_stderr = client.exec_command("rm -rf shell")
        _stdout.read().decode()
        _stdin, _stdout,_stderr = client.exec_command("git clone https://github.com/firedrak/shell.git")
        _stdout.read().decode()
        print(f'{host} => cloned to https://github.com/firedrak/shell.git')
        _stdin, _stdout,_stderr = client.exec_command(f"bash shell/shell.sh {redis_host} {spider_url}")
        print(f'{host} => crawling started')
        (_stdout.read().decode())
        print(f'{host} => crawling completed')
        client.close()

    server_list = [{'host':"172.16.221.235", 'username':"ubuntu", 'port':5190}, {'host':"172.16.221.233", 'username':"ubuntu",  'port':5190}]

    threads = []
    for server in server_list:
        t = Thread(target = start_crawling, args = (server,))
        t.start()
        threads.append(t)

    while True:
        for i in len(threads):
            if not threads[i].is_alive():
                print(f'server {i +1} inactive')
    # ssh ubuntu@172.16.221.235  -p5190 'rm -rf shell | git clone https://github.com/firedrak/shell.git | bash shell/shell.sh'
