#!/usr/bin/env python
import sys
import redis
import argparse
#import urlparse
from urllib.parse import urlparse
import time
import os
import curses
import signal

def fail(msg):
    print(msg)
    exit(1)

def redisHost(r):
    return r.connection_pool.connection_kwargs['host']

def redisPort(r):
    return r.connection_pool.connection_kwargs['port']

def redisPassword(r):
    return r.connection_pool.connection_kwargs['password']

def getRedisList(urls):
    res = []
    for srcUrl in urls:
        url = urlparse(srcUrl)
        if not url.scheme:
            srcUrl = 'redis://' + srcUrl
            url = urlparse.urlparse(srcUrl)
        if url.scheme != 'redis':
            fail('Invalid scheme %s for %s, aborting. url example: redis://127.0.0.1:6379'%(url.scheme,srcUrl))
        r = redis.Redis(host=url.hostname, port=(url.port if url.port else 6379), password=url.password)
        try:
            ver = r.info()['redis_version']
            r.ver = ver
        except redis.ConnectionError as e:
            fail('Failed connecting (%s) to %s, aborting'%(e,srcUrl))
        res.append(r)            
    return res
   

def writeLn(y, x, txt, attr=0):
    stdscr.move(y,0)
    stdscr.clrtoeol()
    stdscr.move(y,x)
    stdscr.addstr(txt, attr)
    stdscr.refresh()
    
def checkInput():
    c = stdscr.getch()
    if c < 0 or c > 255:
        return None
    c = chr(c).lower()
    if c == 'q':
        sys.exit()
    return c

def compareVersion(va, vb):
    for vaPart,vbPart in zip([int(x) for x in va.split('.')], [int(x) for x in vb.split('.')]):
        if vaPart > vbPart:
            return 1
        elif vaPart < vbPart:
            return -1
    return 0

        
def signalWinch(signum, frame):
    pass
    
def valOrNA(x):
    return x if x != None else 'N/A'

def bytesToStr(bytes):
    if bytes < 1024:
        return '%dB'%bytes
    if bytes < 1024*1024:
        return '%dKB'%(bytes/1024)
    if bytes < 1024*1024*1024:
        return '%dMB'%(bytes/(1024*1024))
    return '%dGB'%(bytes/(1024*1024*1024))
    
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Interactively migrate a bunch of redis servers to another bunch of redis servers.')
    parser.add_argument('--src', metavar='src_url', nargs='+', required=True, help='list of source redises to sync from')
    parser.add_argument('--dst', metavar='dst_url', nargs='+', required=True, help='list of destination redises to sync to')
    
    args = parser.parse_args()
    
    if len(args.src) != len(args.dst):
        fail('Number of sources must match number of destinations')

    srcs = getRedisList(args.src)
    dsts = getRedisList(args.dst)

    stdscr = curses.initscr()
    curses.halfdelay(10)
    curses.noecho()
    curses.curs_set(0)
    
    signal.signal(signal.SIGWINCH, signalWinch)
    
    try:
        # Get aggregate sizes from sources
        keys = None
        mem = 0
        for r in srcs:
            info = r.info()
            mem += info['used_memory']
            if compareVersion(r.ver, '2.6') >= 0:
                ks = r.info('keyspace')
                if keys == None:
                    keys = 0
                for db in ks:
                    keys += ks[db]['keys']
                

        writeLn(0, 0, 'Syncing %.2fMB and %s keys from %d redises'%(float(mem)/(1024*1024), valOrNA(keys), len(srcs)))
        writeLn(1, 0, 'q - Quit, s - Start', curses.A_BOLD)
        while checkInput() != 's':
            pass
        writeLn(1, 0, 'q - Quit', curses.A_BOLD)
        
        # Start replication from all slaves
        for sr,dr in zip(srcs,dsts):
            if compareVersion(dr.ver, '2.6') >= 0:
                dr.config_set('slave-read-only', 'yes')
            drAuth = dr.config_get('masterauth')['masterauth']
            if redisPassword(sr) != drAuth: # Avoid setting the master auth if not required since on redis 2.2 theres no way to set a null password
                dr.config_set('masterauth', redisPassword(sr) or '')
            dr.slaveof(redisHost(sr), redisPort(sr))

        # Wait for dsts to be in sync
        while True:
            synced = 0
            y = 2
            for dr,sr in zip(dsts,srcs):
                y += 1
                info = dr.info()
                if info['role'] != 'slave':
                    writeLn(y, 1, 'Error: dest %s:%s configured as %s'%(redisHost(dr), redisPort(dr), info['role']))
                    continue
                writeLn(y, 1, '%s:%s ==> %s:%s - link status: %s, sync in progress: %s, %s left, used memory %.2fMB'%(redisHost(sr), redisPort(sr), redisHost(dr), redisPort(dr), info['master_link_status'], 'yes' if info['master_sync_in_progress'] else 'no', bytesToStr(info.get('master_sync_left_bytes', 0)), float(info['used_memory'])/(1024*1024)))
                if info['master_link_status'] == 'up':
                    synced += 1
            if synced == len(dsts):
                stdscr.move(3,0)
                stdscr.clrtobot()
                writeLn(3, 1, 'Replication links are up, wait for master replication buffers to flush before disconnecting from sources')
                writeLn(1, 0, 'q - Quit, e - Enable writes on destinations', curses.A_BOLD)
                break
            checkInput()

        # Wait for master client buffers to flush
        while True:
            y = 5
            for dr,sr in zip(dsts,srcs):
                maxOutBuff = None
                maxOutBuffCommands = None
                if compareVersion(sr.ver, '2.4') >= 0:
                    slaves = [client for client in sr.client_list() if 'S' in client['flags']]
                    if compareVersion(sr.ver, '2.6') >= 0:
                        maxOutBuff = max([int(slave['omem']) for slave in slaves])
                    maxOutBuffCommands = max([(1 if int(slave['obl']) > 0 else 0) + int(slave['oll']) for slave in slaves])
                readonly = dr.config_get('slave-read-only').get('slave-read-only') if compareVersion(dr.ver, '2.6') >= 0 else 'N/A'
                writeLn(y, 1, '%s:%s ==> %s:%s: replication buf size %s, replication buf commands: %s, dst readonly: %s  '%(redisHost(sr), redisPort(sr), redisHost(dr), redisPort(dr), bytesToStr(maxOutBuff) if maxOutBuff != None else 'N/A', valOrNA(maxOutBuffCommands), readonly))
                y += 1
            c = checkInput()
            if c == 'e':
                for dr in dsts:
                    if compareVersion(dr.ver, '2.6') >= 0:
                        dr.config_set('slave-read-only', 'no')
                writeLn(3, 1, 'Replication links are up and writes enabled on destinations, wait for master replication buffers to flush before disconnecting from sources')
                writeLn(1, 0, 'q - quit, e - Enable writes on destinations, m - Make destinations masters and quit', curses.A_BOLD)
            if c == 'm':
                for dr in dsts:
                    dr.slaveof('no','one')
                    if compareVersion(dr.ver, '2.6') >= 0:
                        dr.config_set('slave-read-only', 'no')
                    if dr.config_get('masterauth')['masterauth']: # Avoid zeroing the master auth if not required, becaues of bug in v2.2 where you can put a null value in the mastaer auth
                        dr.config_set('masterauth', '')
                sys.exit()
            
    finally:
        curses.nocbreak()
        curses.echo()
        curses.curs_set(1)
        curses.endwin()
            
        
