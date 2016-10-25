#!/usr/bin/env python

try:
    import os
    import sys
    import stat
    import time
    import redis
    import hashlib
    import logging
    from bsddb3 import db as bsddb
except ImportError, err:
    print "Error Importing module. %s" % (err)
    sys.exit()

class dedup_syslog:
    ##############################################
    def __init__(self, args):
    ##############################################
        logger = logging.getLogger('__init__')
        logger.info('')

        defaults = {
            # Human 'must' pass these.
            'requiredargs' : [
                'fifo_path',
                'bdb_paths',
                'redis_host',
                'redis_port',
                'redis_dbs',
            ],
            # Human 'may' pass these.
            'redis_cons' : {},
            'bdb_cons' : {},
            'fifo_mode'  : '0600',
            'redis_dbs'  : {
                'sum2msg' : 0,
                'msg2sum' : 1,
            },
            'bdb_paths'          : [
                '/tmp/dedup_syslog_sum2msg.db',
                '/tmp/dedup_syslog_msg2sum.db',
            ]
        }

        # Apply class defaults.
        for key in defaults.keys():
            setattr(self, key, defaults[key])
    
        # Apply arguments passed by human.  They may clobber our defaults.
        for key in args.keys():
            setattr(self, key, args[key])

        # Ensure human passed necessary arguments to constructor.
        self.validateArgs(getattr(self, 'requiredargs'))

        # Create fifo if needed.
        self._createFifo(
            getattr(self, 'fifo_path'),
            getattr(self, 'fifo_mode')
        )

        redis_dbs = getattr(self, 'redis_dbs')
        for db_name, db_number in redis_dbs.iteritems():
            # Initialize Redis sum2msg connection.
            self._initRedis(
                getattr(self, 'redis_host'),
                getattr(self, 'redis_port'),
                db_number,
                db_name
            )
            # Initialize BerkeleyDB.
            self._initBdb(
                self.bdb_paths[db_number],
                db_name,
            )
            
    ######################################################
    def validateArgs(self, requiredArgs):
    ######################################################
        logger = logging.getLogger('validateArgs')
        logger.info('')

        missing_args = []
        for arg in requiredArgs:
            try:
                getattr(self, arg)
            except:
                missing_args.append(arg)

        if missing_args:
            logger.error("Required arguments missing: %s" % ', '.join(missing_args))
            sys.exit()

    ######################################################
    def _initRedis(self, host, port, db, redis_con_name):
    ######################################################
        logger = logging.getLogger('_initRedis')
        logger.info('')

        try:
            logger.info("Initiating Redis connection, host=%s port=%s, db=%s" % (host, port, db))
            redis_con = redis.StrictRedis(host=host, port=port, db=db)
            self.redis_cons[redis_con_name] = redis_con
        except redis.RedisError, err:
            logger.error(err)
            sys.exit()
        else:
            logger.info("redis connection successful")

    ######################################################
    def _initBdb(self, bdb_path, bdb_con_name):
    ######################################################
        logger = logging.getLogger('_initBdb')
        logger.info('')

        try:
            logger.info("Initializing bdb: %s" % bdb_path)
            bdb_obj = bsddb.DB()
            bdb_obj.open(bdb_path, None, bsddb.DB_HASH, bsddb.DB_CREATE)
            self.bdb_cons[bdb_con_name] = bdb_obj
        except bsddb.DBError, err:
            logger.error(err)
            sys.exit()
        else:
            logger.info("bdb initialization complete")

    ######################################################
    def _createFifo(self, fifo_path, fifo_mode):
    ######################################################
        logger = logging.getLogger('createFifo')
        logger.info('')
        
        try:
            logger.info('Checkfing filesystem for fifo at: %s' % fifo_path)
            stat.S_ISFIFO(os.stat(fifo_path).st_mode)
        except:
            logger.info('Not found, attempting to create')
            try:
                os.mkfifo(fifo_path)
            except OSError, e:
                logger.error("mkfifo creation failure, path: %s mode: %s.  Error: %s" % (fifo_path, fifo_mode, e))
                sys.exit()
            else:
                logger.info('fifo created: %s' % fifo_path)
        else:
            logger.info('Existing fifo found at: %s' %fifo_path)

    ######################################################
    def redis_get(self, redis_con_name, key, ourval=None):
    ######################################################
        logger = logging.getLogger('redis_get')
        logger.info('')

        redis_con = self.redis_cons[redis_con_name]

        try:
            logger.info('Attempting redis set, key: %s' % key)
            ourval = redis_con.get(key, value)
        except redis.RedisError, err:
            logger.error(err)
        else:
            logger.info("redis set complete")
        finally:
            return ourval

    ######################################################
    def redis_set(self, redis_con_name, key, value):
    ######################################################
        logger = logging.getLogger('redis_set')
        logger.info('')

        redis_con = self.redis_cons[redis_con_name]

        try:
            logger.info('Attempting redis set, key: %s value: %s' % (key, value))
            redis_con.set(key, value)
        except redis.RedisError, err:
            logger.error(err)
        else:
            logger.info("redis set complete")

    ######################################################
    def redis_setnx(self, redis_con_name, key, value):
    ######################################################
        logger = logging.getLogger('redis_setnx')
        logger.info('')

        redis_con = self.redis_cons[redis_con_name]

        try:
            logger.info('Attempting redis setnx, key: %s value: %s' % (key, value))
            redis_con.setnx(key, value)
        except redis.RedisError, err:
            logger.error(err)
        else:
            logger.info("redis setnx complete")

    ######################################################
    def bdb_get(self, bdb_con_name, key, ourval=None):
    ######################################################
        logger = logging.getLogger('bdb_get')
        logger.info('')

        bdb_con = self.bdb_cons[bdb_con_name]

        try:
            logger.info('Attempting bdb_get, key: %s' % key)
            ourval = bdb_con.get(key)
        except:
            logger.info('bdb_get failed, no results found')
        else:
            logger.info('bdb_get successful, value: %s' % ourval)
            return ourval

    ######################################################
    def bdb_put(self, bdb_con_name, key, value):
    ######################################################
        logger = logging.getLogger('bdb_put')
        logger.info('')

        bdb_con = self.bdb_cons[bdb_con_name]

        # Add exception handling
        bdb_con.put(key, value)
