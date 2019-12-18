#!/opt/rackware/vendor/python/bin/python
#===================================================================================================
# Imports
#===================================================================================================
import sys
import os
import subprocess
import stat
import re
import argparse
import random
import socket

from datetime import datetime, date, time, timedelta
from time import sleep, time
from subprocess import PIPE
from multiprocessing import Pool


#-----------------------------------------------------------------------------
def print_sep():

    proc = CliExec('stty size'.split())
    proc.run()
    out = proc.getOutput()
    rows, columns = out.split()
    print int(columns) * '-'

#-----------------------------------------------------------------------------
def print_center(l):

    proc = CliExec('stty size'.split())
    proc.run()
    out = proc.getOutput()
    rows, columns = out.split()

    print " " * (int(columns)/2 - len(l)/2) + l
    print int(columns) * '-'

#-----------------------------------------------------------------------------
class CliExec:

    def __init__(self, cmd, ip=None, user=None, port=22, debug=False):
        self._cmd = cmd
        self._p = None
        self.retCode = None
        self._output = ""
        self._startTime = None
        self._ip = ip
        self._user = user
        self._debug = debug
        self._port = str(port)

    def getOutput(self):
        return self._output

    def getRetcode(self):
        return self._retcode

    def start(self):
        if self._ip is not None:
            cmd = ['ssh', '-p', self._port, self._user + '@' + self._ip]
            cmd.extend(self._cmd) 
        else:
            cmd = self._cmd

        if self._debug:
            print "CMD: {0}".format(cmd)
        self._p = subprocess.Popen(cmd, stdout=PIPE, stderr=PIPE)
        self._startTime = datetime.now()

    def execute(self):
        (result, error) = self._p.communicate()
        self._output = result
        self._output += error
        self._retcode = self._p.returncode

    def run(self):

        self.start()
        self.execute()
        if self._debug:
            print "OUTPUT: {0}".format(self._output)

#-----------------------------------------------------------------------------
def dd(src, dst, bs, count, flags, ip=None, user=None ):

    cmd = '/bin/dd if={0} of={1} bs={2}'.format(src, dst, bs)
    if count > 0:
        cmd += ' count={0}'.format(count)
    if len(flags) > 0:
        cmd += " " + flags

    print cmd

    b = ""
    t = ""
    proc = CliExec(cmd.split(), ip, user)
    proc.run()
    out = proc.getOutput()
    for l in out.split(','):
        if any(speed in l.lower() for speed in ['b/s', 'kb/s', 'mb/s', 'gb/s', 'tb/s']):
            #print "Throughput: {0}".format( l.strip() )
            b = l.strip()
        if ' s' in l:
            #print "Time:       {0}".format( l.strip() )
            t = l.strip()
    
    return b,t

#-----------------------------------------------------------------------------
def dd_remote_pipe_local(srcip, port, user, src, dst, bs, count, iflags, oflags):

    retry = 3
    while retry > 0:
        rem_cmd = ' dd if=' + src + ' bs={0}'.format(bs) + ' count={0}'.format(count) + ' ' + iflags
        if srcip is not None:
            rem_cmd = 'ssh ' + user + '@' + srcip + rem_cmd
        loc_cmd = 'dd of=' + dst + ' ' + oflags + ' bs={0}'.format(bs)

        print rem_cmd
        print loc_cmd

        remote = subprocess.Popen(rem_cmd.split(), stdout=subprocess.PIPE)
        local = subprocess.Popen(loc_cmd.split(), stdin=remote.stdout, stderr=subprocess.STDOUT, stdout=subprocess.PIPE)
        remote.wait()
        ret_l = local.communicate()

        if remote.returncode or local.returncode:
            print "FAILED to measure network throughput"
            print_sep()
            retry += 1
            sleep(60)
            continue
        if remote.returncode == 0 and local.returncode == 0:
            break

    band = ret_l[0].split(',')[2].strip()
    t = ret_l[0].split(',')[1].strip()

    #print "Network bandwidth: {0}".format(band)
    #print "Elapsed time     : {0}".format(t)
    #print_sep()

#-----------------------------------------------------------------------------
def dd_remote_pipe_remote(srcip, dstip, port, user, src, dst, bs, count, iflags, oflags):

    retry = 3
    while retry > 0:
        rem_cmd = 'ssh ' + user + '@' + srcip + ' dd if=' + src + ' bs={0}'.format(bs) + ' count={0}'.format(count) + ' ' + iflags
        loc_cmd = 'ssh ' + user + '@' + dstip + ' dd of=' + dst + ' ' + oflags + ' bs={0}'.format(bs)

        print rem_cmd
        print loc_cmd

        remote = subprocess.Popen(rem_cmd.split(), stdout=subprocess.PIPE)
        local = subprocess.Popen(loc_cmd.split(), stdin=remote.stdout, stderr=subprocess.STDOUT, stdout=subprocess.PIPE)
        remote.wait()
        ret_l = local.communicate()

        if remote.returncode or local.returncode:
            print "FAILED to measure network throughput"
            print_sep()
            retry += 1
	    sleep(60)
            continue
        if remote.returncode == 0 and local.returncode == 0:
            break

    band = ret_l[0].split(',')[2].strip()
    t = ret_l[0].split(',')[1].strip()

#-----------------------------------------------------------------------------
def getSize(ip, user, port, path):

    cmd = 'ls -l {0}'.format(path)

    proc = CliExec(cmd.split(), ip, user, port, False)
    proc.run()
    out = proc.getOutput()
    if proc.getRetcode() > 0:
        raise Exception("Failed to get size of the file.")

    return int(out.split()[4])

#-----------------------------------------------------------------------------
class Chunk:
    def __init__(self):
        self._start = 0
        self._end = 0
        self._user = None
        self._ip = None
        self._port = None
        self._srcpath = None
        self._dstpath = None
        self._bs = None

#-----------------------------------------------------------------------------
def ddWorker(c):

    if c._dstip is None:
        print 'ddWorker remote->local'
        dd_remote_pipe_local( srcip=c._srcip, 
                          port=c._port,
                          user=c._user, 
                          src=c._srcpath, 
                          dst=c._dstpath, 
                          bs=c._bs, 
                          count=c._count, 
                          iflags='skip=' + str(c._offset), 
                          oflags='seek={0} conv=nocreat'.format(c._offset) )
    else:
        print 'ddWorker remote->remote'
        dd_remote_pipe_remote( srcip=c._srcip, 
                          dstip=c._dstip,  
                          port=c._port,
                          user=c._user, 
                          src=c._srcpath, 
                          dst=c._dstpath, 
                          bs=c._bs, 
                          count=c._count, 
                          iflags="skip={0} iflag=fullblock".format(str(c._offset)), 
                          #iflags="skip={0}".format(str(c._offset)), 
                          oflags='seek={0} conv=nocreat conv=notrunc '.format(c._offset) )


#-----------------------------------------------------------------------------
def pdd(srcip, dstip, user, port, srcpath, dstpath, size, parallel, verbose):
    
    chunkSize = size / parallel
    offset = 0
    offsetCount = 0
    count = 1
    bs = chunkSize

    print "chunkSize = {0}".format(chunkSize)

    if chunkSize > 8 * 1024 * 1024: # 32MB blocks
        bs = 8 * 1024 * 1024
        count = size / bs / parallel 

    chunks = []
    
    while size - offset >= 0:
        c = Chunk()
        c._user = user
        c._srcip = srcip
        c._dstip = dstip
        c._port = port
        c._srcpath = srcpath
        c._dstpath = dstpath
        c._offset = offsetCount
        c._bs = bs
        c._count = count
        offset += bs * count
        offsetCount = offsetCount + count
        chunks.append(c)
    #if size - offset > 0:
    #    print "LAST CHUNK"
    #    c = Chunk()
    #    c._user = user
    #    c._srcip = srcip
    #    c._dstip = dstip
    #    c._port = port
    #    c._srcpath = srcpath
    #    c._dstpath = dstpath
    #    c._offset = offset
    #    c._size = size - offset
    #    c._bs = 1
    #    c._count = c._size

    #    chunks.append(c)

    pool = Pool(processes=parallel)  # start worker processes

    ret = []

    # Transfers
    ret += pool.map(ddWorker, chunks)

#-----------------------------------------------------------------------------
def createSparse(ip, user, path, size):

    dd('/dev/zero', path, 1, 1, 'seek={0}'.format(size - 1), ip, user )

    cmd = 'ls -l {0}'.format(path)
    if ip is not None:
        cmd = 'ssh {0}@{1} '.format(user, ip) + cmd

    print cmd

    proc = CliExec(cmd.split())
    proc.run()
    out = proc.getOutput()
    print out

    return int(out.split()[4])

#-----------------------------------------------------------------------------
def printTime(d):

    secs = d.seconds
    days = d.days

    seconds = secs % 60
    minutes = (secs / 60) % 60
    hours = ( secs / 60 / 60 )

    print "Elapsed time: {0}d {1}h {2}m {3}s".format(days, hours, minutes, seconds)

#-----------------------------------------------------------------------------
def getMD5(path, ip=None, user=None, port=None):

    cmd = 'md5sum {0}'.format(path)

    proc = CliExec(cmd.split(), ip, user, port, False)
    proc.run()
    out = proc.getOutput()

    if proc.getRetcode() != 0:
        raise Exception("Failed to calculate MD5")

    return out.split()[0]

#-----------------------------------------------------------------------------
def humanReadable(s):
    if s < 1024:
        return "{0}B".format(s)
    s = s/1024
    if s < 1024:
        return "{0}KB".format(s)
    s = s/1024
    if s < 1024:
        return "{0}MB".format(s)
    s = s/1024
    if s < 1024:
        return "{0}GB".format(s)


def isRemoteDir(args):

    cmd = "test -d {0} && echo 'dir'".format(args.dstpath)
    proc = CliExec(cmd.split(), args.dstip, args.dstuser, args.port, False)
    proc.run()
    out = proc.getOutput()

    if 'dir' in out:
        return True
    else:
        return False

#-----------------------------------------------------------------------------
class Transfer:

    def run(self, args):

        res = []

        if args.filelist is not None:
            fp = open(args.filelist, 'r')
            files = fp.readlines()
            fp.close()

            # Check dst dir exists
            dstdir = args.dstpath
            #if not os.path.isdir(dstdir):
            #    print "{0} is not a directory".format(dstdir)
            #    quit()

            for f in files:    
                f = f.strip()
                if len(f) == 0:
                    continue
                args.srcpath = f
                filename = f.split('/')[-1]
                args.dstpath = dstdir + '/' + filename
                try:
                    self.transfer_file(args)
                    print 'after transfer_file'
                    res.append( (f, 'Success') )
                except:
                    print "{0} FAILED TO TRANSFER".format(f) 
                    res.append( (f, 'FAILED') )

        else:
            filename = args.srcpath.split('/')[-1]
            if args.dstip is not None:
                if isRemoteDir(args):
                    args.dstpath += '/' + filename
            elif os.path.isdir(args.dstpath):
                args.dstpath += '/' + filename


            self.transfer_file(args)

        for f in res:
            print "{0} {1}".format(f[0].strip(), f[1])


    def transfer_file(self, args):

        self.args = args

        size = getSize(args.srcip, args.srcuser, args.port, args.srcpath)
        print "File size: {0} ({1})".format(size, humanReadable(size))

        if not args.nochecksum:
            md5src = getMD5(args.srcpath, args.srcip, args.srcuser, args.port)
            print "MD5: {0}".format(md5src)

        createSparse(args.dstip, args.dstuser, args.dstpath, size)

        print 'After createSparse'

        t1 = datetime.now()
        pdd( args.srcip, args.dstip, args.srcuser, args.port, args.srcpath, args.dstpath, size, int(args.j), args.verbose)
        t2 = datetime.now()
        d = t2 - t1
        printTime(d)

        if not args.nochecksum:
            md5dst = getMD5(args.dstpath, args.dstip, args.dstuser, 22)
            print "MD5: {0}".format(md5dst)
            if md5src == md5dst:
                print "Copy successful"
                print "File size: {0} ({1})".format(size, humanReadable(size))
            else:
                print "Copy FAILED. Checksums don't match."

#-----------------------------------------------------------------------------
def parse():

    parser = argparse.ArgumentParser()
    parser.add_argument("-S", "--srcip", help="Specify source host IP", required=False)
    parser.add_argument("-D", "--dstip", help="Specify destination host IP", required=False)
    parser.add_argument("-su", "--srcuser", help="Specify source host user name", required=False)
    parser.add_argument("-du", "--dstuser", help="Specify destination host user name", required=False)
    parser.add_argument("-p", "--port", help="Port to be used for network latency check", required=True)
    parser.add_argument("-src", "--srcpath", help="Source path.", required=False)
    parser.add_argument("-dst", "--dstpath", help="Destination path.", required=True)
    parser.add_argument("-j", "--j", help="Concurrency level.", required=True )
    parser.add_argument("-v", "--verbose", help="Verbose log output.", required=False)
    parser.add_argument("-nocheck", "--nochecksum", help="Do not check MD5 checksum.", required=False, action='store_true')
    parser.add_argument("-fl", "--filelist", help="Verbose log output.", required=False)

    args = parser.parse_args()

    if args.srcpath is None and args.filelist is None:
        print "Either specify --srcpath or --filelist"
        quit()

    if args.dstuser is None and '@' in args.dstpath:
        args.dstuser = args.dstpath[:args.dstpath.find('@')]
        args.dstpath = args.dstpath[args.dstpath.find('@') + 1:]

    if args.dstip is None and ':' in args.dstpath:
        args.dstip = args.dstpath[:args.dstpath.find(':')]
        args.dstpath = args.dstpath[args.dstpath.find(':')+1:]
    elif args.dstip is not None and ':' in args.dstpath:
        args.dstpath = args.dstpath[args.dstpath.find(':')+1:]

    if args.srcuser is None and '@' in args.srcpath:
        args.srcuser = args.srcpath[:args.srcpath.find('@')]
        args.srcpath = args.srcpath[args.srcpath.find('@') + 1:]

    if args.srcip is None and ':' in args.srcpath:
        args.srcip = args.srcpath[:args.srcpath.find(':')]
        args.srcpath = args.srcpath[args.srcpath.find(':')+1:]

    return args

#-----------------------------------------------------------------------------
if __name__ == "__main__":

    t = Transfer()
    t.run( parse() )


