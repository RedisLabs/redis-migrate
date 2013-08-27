#!/usr/bin/python
import sys
import redis
import argparse
import urlparse
import time
import os
import curses
import signal

def fail(msg):
    print >> sys.stderr, msg
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
        url = urlparse.urlparse(srcUrl)
        if not url.scheme:
            srcUrl = 'redis://' + srcUrl
            url = urlparse.urlparse(srcUrl)
        if url.scheme != 'redis':
            fail('Invalid scheme %s for %s, aborting'%(url.scheme,srcUrl))
        r = redis.Redis(host=url.hostname, port=(url.port if url.port else 6379), password=url.password)
        try:
            r.ping()
        except redis.ConnectionError as e:
            fail('Failed connecting (%s) to %s, aborting'%(e,srcUrl))
        res.append(r)            
    return res
   

def updateStatus(txt):
    i, s = txt.split(',',1)
    syncProgressWidgets[int(i)].set_text(s)
    
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
        
def signalWinch(signum, frame):
    pass
    
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
        keys = 0
        mem = 0
        for r in srcs:
            mem += r.info('memory')['used_memory']
            ks = r.info('keyspace')
            for db in ks:
                keys += ks[db]['keys']
                

        writeLn(0, 0, 'Syncing %.2fMB and %d keys from %d redises'%(float(mem)/(1024*1024), keys, len(srcs)))
        writeLn(1, 0, 'q - Quit, s - Start', curses.A_BOLD)
        while checkInput() != 's':
            pass
        writeLn(1, 0, 'q - Quit', curses.A_BOLD)
        
        # Start replication from all slaves
        for sr,dr in zip(srcs,dsts):
            dr.config_set('slave-read-only', 'yes')
            dr.config_set('masterauth', redisPassword(sr) or '')
            dr.slaveof(redisHost(sr), redisPort(sr))

        # Wait for dsts to be in sync
        while True:
            synced = 0
            y = 2
            for dr,sr in zip(dsts,srcs):
                y += 1
                replInfo = dr.info('replication')
                memInfo = dr.info('memory')
                if replInfo['role'] != 'slave':
                    writeLn(y, 1, 'Error: dest %s:%s configured as %s'%(redisHost(dr), redisPort(dr), replInfo['role']))
                    continue
                writeLn(y, 1, '%s:%s ==> %s:%s - link status: %s, sync in progress: %s, %s left, used memory %.2fMB'%(redisHost(sr), redisPort(sr), redisHost(dr), redisPort(dr), replInfo['master_link_status'], 'yes' if replInfo['master_sync_in_progress'] else 'no', bytesToStr(replInfo.get('master_sync_left_bytes', 0)), float(memInfo['used_memory'])/(1024*1024)))
                if replInfo['master_link_status'] == 'up':
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
                slaves = [client for client in sr.client_list() if 'S' in client['flags']]
                maxOutBuff = max([int(slave['omem']) for slave in slaves])
                readonly = dr.config_get('slave-read-only').get('slave-read-only')
                writeLn(y, 1, '%s:%s ==> %s:%s: replication buf: %s, dst readonly: %s  '%(redisHost(sr), redisPort(sr), redisHost(dr), redisPort(dr), bytesToStr(maxOutBuff), readonly))
                y += 1
            c = checkInput()
            if c == 'e':
                for dr in dsts:
                    dr.config_set('slave-read-only', 'no')
                writeLn(3, 1, 'Replication links are up and writes enabled on destinations, wait for master replication buffers to flush before disconnecting from sources')
                writeLn(1, 0, 'q - quit, e - Enable writes on destinations, m - Make destinations masters and quit', curses.A_BOLD)
            if c == 'm':
                for dr in dsts:
                    dr.slaveof('no','one')
                    dr.config_set('slave-read-only', 'no')
                    dr.config_set('masterauth', '')
                sys.exit()
            
    finally:
        curses.nocbreak()
        curses.echo()
        curses.curs_set(1)
        curses.endwin()
            
        
