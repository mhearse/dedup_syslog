#!/usr/bin/env python

try:
    import sys
    import socket
    import logging
    from dedup_syslog import *
except ImportError, err:
    print "Error Importing module. %s" % (err)
    sys.exit()

##############################################
if __name__=='__main__':
##############################################
    logfile_path = '/tmp/dedup_syslog.log'
    # Set up logging to file
    logging.basicConfig(
        level    = logging.DEBUG,
        format   = '%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
        datefmt  = '%m-%d %H:%M',
        filename = logfile_path,
        filemode = 'w'
    )

    # Define a Handler which writes INFO messages or higher to the sys.stderr
    console = logging.StreamHandler()
    console.setLevel(logging.DEBUG)

    # Set a format which is simpler for console use
    formatter = logging.Formatter('%(name)-12s: %(levelname)-8s %(message)s')

    # Tell the handler to use this format
    console.setFormatter(formatter)

    # Add the handler to the root logger
    logging.getLogger('').addHandler(console)

    logger = logging.getLogger('__main__')
    logger.info('Creating dedup_syslog object')

    args = {
        'fifo_path'   : '/tmp/dedup_syslog.fifo',
        'log_level'   : 'INFO',
        'redis_host'  : 'localhost',
        'redis_port'  : 6379,
        'redis_dbs'   : {
            'sum2msg' : 0,
            'msg2sum' : 1,
        },
        'bdb_paths'   : [
            '/tmp/dedup_syslog_sum2msg.db',
            '/tmp/dedup_syslog_msg2sum.db',
        ]
    }

    obj = dedup_syslog(args)

    redis_dbs = getattr(obj, 'redis_dbs')
    fifo_path = getattr(obj, 'fifo_path')
    socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    with open(fifo_path, "r") as ourfifo:
        while True:
            line = ourfifo.readline()
            parts = line.split()
            (month, date, tod, host, service) = parts[0:5]
            msg = ' '.join(parts[5:])

            # Check bdb cache for msg2sum.
            shasum = obj.bdb_get('msg2sum', msg)

            # Check redis msg2sum db.
            if not shasum:
                shasum = obj.redis_get('msg2sum', msg)

            # Calculate our own sum.
            if not shasum:
                shasum = hashlib.sha1(msg).hexdigest()
                # Store sum to bdb and redis
                for db_name in redis_dbs:
                    obj.bdb_put(db_name, msg, shasum)
                    obj.redis_set(db_name, msg, shasum)

            socket.sendto(
                "%s %s %s %s %s %s%s" % (month, date, tod, host, service, '', shasum),
                ('localhost', 514)
            )

# TODO
# exception handle for splitting syslog lines
# ensure function prototypes validate object types
# on startup, iterate over contents of bdb on startup.  redis setnx for all entries
# Use select.poll (epoll) instead of reading from pipe in infinite loope
# use additional redis db to track number of shasum hits.  determine cache time based on number of hits.... higher hits yields lower cache time????? 

