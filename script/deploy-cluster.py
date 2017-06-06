#! /usr/bin/python

import pexpect
import sys
import argparse

cHadoop_repo_name = 'cHadoop'
cHadoop_git_https = 'https://github.com/lidajian/' + cHadoop_repo_name
CXX = 'gcc-c++'
MAX_TRY = 5

def deploy_on(ip, pem):
    done = False
    ssh = pexpect.spawn('ssh -i ' + pem + ' ec2-user@' + ip)
    try:
        print 'Connecting to ' + ip
        i = ssh.expect(['continue connecting (yes/no)?', 'Last login'], timeout = 5)
        if i == 0:
            ssh.sendline('yes')

        print 'Installing git'
        ssh.sendline('sudo yum install git')
        i = ssh.expect(['Is this ok', 'Nothing to do'], timeout = None)
        if i == 0:
            ssh.sendline('y')

        print 'Cloning repository'
        ssh.sendline('git clone ' + cHadoop_git_https)

        print 'Installing ' + CXX
        ssh.sendline('sudo yum install ' + CXX)
        i = ssh.expect(['Is this ok', 'Nothing to do'], timeout = None)
        if i == 0:
            ssh.sendline('y')

        ssh.sendline('mkdir .cHadoop')
        ssh.sendline('cd ' + cHadoop_repo_name)

        print 'Compiling'
        ssh.sendline('make')

        print 'Running server'
        ssh.sendline('nohup ./bin/cHadoopServer > ~/.cHadoop/server.log &')

        ssh.sendline('exit')
        r = ssh.read()
        print 'Done on ' + ip
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
        failedIP = []
        with open(sys.argv[1]) as confFile:
            selfIP = confFile.readline()
            for ip in confFile:
                tries = 0
                while tries < MAX_TRY and deploy_on(ip, sys.argv[2]):
                    tries += 1
                if tries == MAX_TRY:
                    print 'Fail to finish on ' + ip
                    failedIP.append(ip)

        if len(failedIP) == 0:
            print 'Deployed on all machines'
        else:
            print 'Servers on the following machines may not run properly:'
            for ip in failedIP:
                print '\t' + ip

