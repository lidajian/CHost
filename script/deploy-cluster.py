#! /usr/bin/python

import pexpect
import sys
import argparse

repo_name = 'CHost'
git_https = 'https://github.com/lidajian/' + repo_name + '.git'
CXX = 'gcc-c++'
MAX_TRY = 5

def deploy_on(ip, pem, git_user, git_password):
    done = False
    ssh = pexpect.spawn('ssh -i ' + pem + ' ec2-user@' + ip)
    try:
        i = ssh.expect(['continue connecting (yes/no)?', 'Last login'], timeout = 5)
        if i == 0:
            ssh.sendline('yes')

        print 'Installing git'
        ssh.sendline('sudo yum -y install git')

        print 'Installing ' + CXX
        ssh.sendline('sudo yum -y install ' + CXX)

        print 'Cloning repository'
        ssh.sendline('git clone ' + git_https)
        i = ssh.expect(['Username', 'already exists'], timeout = 10)
        if i == 0:
            ssh.sendline(git_user)
            i = ssh.expect(['Password'], timeout = 10)
            if i == 0:
                ssh.sendline(git_password)
                ssh.expect(['done.'], timeout = 20)

        # create temp files
        ssh.sendline('mkdir -p .' + repo_name)

        # Enter repository directory
        ssh.sendline('cd ' + repo_name)

        print 'Compiling'
        ssh.sendline('make')

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
    print 'Usage: python ' + name + '[configuration file] [.pem file] [git username] [git password]'

if __name__ == '__main__':
    if len(sys.argv) < 5:
        printHelp(sys.argv[0])
    else:
        failedIP = []
        with open(sys.argv[1]) as confFile:
            for ip in confFile:
                if ('#' in ip) or ('.' not in ip):
                    continue
                tries = 0
                while tries < MAX_TRY:
                    if deploy_on(ip, sys.argv[2], sys.argv[3], sys.argv[4]):
                        break
                    tries += 1
                if tries == MAX_TRY:
                    print 'Fail to finish on ' + ip
                    failedIP.append(ip)
                else:
                    print 'Done on ' + ip

        if len(failedIP) == 0:
            print 'Deployed on all machines'
        else:
            print 'Servers on the following machines may not run properly:'
            for ip in failedIP:
                print ip

