#! /usr/bin/python

import pexpect
import sys
import re

repo_name = 'CHost'
PROG_NAME = 'chserver'

def read_all(handler):
    s = ''
    try:
        while True:
            s += handler.read_nonblocking(size = 100, timeout = 0.1)
    except pexpect.TIMEOUT:
        pass
    return s

def restart_server(ip, pem):
    done = False
    ssh = pexpect.spawn('ssh -i ' + pem + ' ec2-user@' + ip)

    print 'Connecting to ' + ip
    i = ssh.expect(['continue connecting (yes/no)?', 'Last login', pexpect.EOF, pexpect.TIMEOUT], timeout = 5)
    if i == 0:
        ssh.sendline('yes')
    elif i > 1:
        return False

    read_all(ssh)

    try:

        print 'Killing server'
        ssh.sendline('ps aux | grep ' + PROG_NAME)

        r  = read_all(ssh)

        for m in re.findall('.*' + PROG_NAME + '.*', r):
            if 'grep' not in m:
                print 'Found one...'
                tokens = re.split(r'\s+', m)
                if len(tokens) > 2:
                    ssh.sendline('kill -9 ' + tokens[1])
                print 'Killed!'

        ssh.sendline("mkdir -p ." + repo_name)
        ssh.sendline('cd ' + repo_name)

        print 'Starting server'
        ssh.sendline('./bin/' + PROG_NAME + ' > ~/.' + repo_name + '/server.log &')

        ssh.sendline('exit')
        r = ssh.read()

        done = True
    except pexpect.EOF:
        print 'Fail on ' + ip + ': unexpected EOF'
    except pexpect.TIMEOUT:
        print 'Fail on ' + ip + ': operation timeout'

    ssh.close()
    return done

def printHelp(name):
    print 'Usage: python ' + name + '[configuration file] [.pem file]'

if __name__ == '__main__':
    if len(sys.argv) < 3:
        printHelp(sys.argv[0])
    else:
        with open(sys.argv[1]) as confFile:
            for ip in confFile:
                if ('#' in ip) or ('.' not in ip):
                    continue
                restart_server(ip, sys.argv[2])
