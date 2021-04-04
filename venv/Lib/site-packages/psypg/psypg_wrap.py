#!/usr/bin/env python
# -*- coding: utf-8 -*-

try:
    from collections.abc import Iterable
except ImportError:
    from collections import Iterable
try:
    import configparser
except ImportError:
    import ConfigParser as configparser
import logging
import os.path
import psycopg2
import psycopg2.extensions
import psycopg2.extras

try:
  basestring
except NameError:
  basestring = str

# Always return Unicode strings
psycopg2.extensions.register_type(psycopg2.extensions.UNICODE)
psycopg2.extensions.register_type(psycopg2.extensions.UNICODEARRAY)

logger = logging.getLogger('psypg')


def pg_commit(dbh):
    '''Commit a transaction'''
    dbh.commit()


def pg_rollback(dbh):
    '''Rollback a transaction'''
    dbh.rollback()


def pg_notify(dbh, channel, payload=None):
    '''Send a notify message and an optional payload'''
    query = 'select * from pg_notify(%s, %s)'
    return pg_query(dbh, query, [channel, payload], fetch_one=True)


def pg_query(dbh, query, query_parms=None, autocommit=True,
             real_dict_cursor=True, fetch_one=False):
    '''A [hopefully not-too-naive] Psycopg2 query wrapper function'''
    rows = None
    cursor_factory = None
    if real_dict_cursor:
        cursor_factory = psycopg2.extras.RealDictCursor
    curs = dbh.cursor(cursor_factory=cursor_factory)

    try:
        curs.execute(query, query_parms)
        # https://stackoverflow.com/questions/38657566/detect-whether-to-fetch-from-psycopg2-cursor-or-not
        # http://initd.org/psycopg/docs/cursor.html#cursor.description
        if curs.description:
            rows = curs.fetchone() if fetch_one else curs.fetchall()
        curs.close()
        if autocommit:
            dbh.commit()
        return rows
    except Exception as e:
        logging.exception(str(e))  # This needs more attention
        dbh.rollback()
        raise


class PgConfig(object):
    '''
    A simple class to return a Postgres DSN from a ConfigParser config file
    '''

    def __init__(self, dbschema, configfiles):
        self.schemaopts = dict()
        self.dbschema = dbschema
        self.cfp = configparser.ConfigParser()
        # Permit the caller to pass in a list of file names or a single
        # file name, which is checked first because it, too, is an iterable
        if isinstance(configfiles, basestring):
            self.cfp.read(configfiles)
        elif isinstance(configfiles, Iterable):
            self.cfp.read([os.path.expanduser(x) for x in configfiles])
        if self.dbschema not in self.cfp.sections():
            exception_msg = 'Unknown schema: {0}'.format(dbschema)
            raise Exception(exception_msg)
        else:
            for o in self.cfp.options(dbschema):
                self.schemaopts[o] = self.cfp.get(dbschema, o)
                setattr(self, o, self.cfp.get(dbschema, o))
            if 'user' not in self.schemaopts:
                setattr(self, 'user', dbschema)

    def dsn(self):
        '''Build a Pg DSN'''
        dsn = []
        # Permit the caller to override the user name, otherwise
        # use the ConfigParser section.
        if 'user' not in self.schemaopts:
            self.schemaopts['user'] = self.dbschema
        for opt in self.schemaopts:
            dsn.append('='.join([opt, self.schemaopts[opt]]))

        return ' '.join(dsn)

    def peewee(self):
        '''Generate a connect tuple for the Peewee ORM'''
        dbopts = {}
        for opt in self.schemaopts:
            dbopts[opt] = self.schemaopts[opt]
        if 'user' not in dbopts:
            dbopts['user'] = self.dbschema
        return (self.dbname, dbopts)

    def sa(self, dialect=None, **kwargs):
        '''Generate a SQLAlchemy URI'''
        uri = 'postgresql'
        if dialect:
            uri = uri + '+' + dialect
        uri = uri + '://'
        if 'user' in self.schemaopts:
            uri = uri + self.schemaopts['user']
        else:
            uri = uri + self.dbschema
        if 'password' in self.schemaopts:
            uri = uri + ':' + self.schemaopts['password']
        if 'host' in self.schemaopts:
            uri = uri + '@' + self.schemaopts['host']
        if 'port' in self.schemaopts:
            uri = uri + ':' + self.schemaopts['port']
        uri = uri + '/' + self.schemaopts['dbname']
        if kwargs:
            uri = uri + '?' + '&'.join(
                    ['%s=%s' % arg for arg in kwargs.items()])
        logger.debug('uri: {}'.format(uri))
        return uri

    def get_handle(self):
        '''Return a database handle/connection'''
        return psycopg2.connect(self.dsn())


if __name__ == '__main__':
    d = PgConfig('testdb', ['db.ini'])
    print(d.dsn())
    print (d.sa())
