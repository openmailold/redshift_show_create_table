#!/usr/bin/env python

"""
'show create table' equivalent for aws redshift
 xiuming chen <cc@cxm.cc>
"""

import psycopg2

__all__ = ['show_create_table']

def get_table_defs(conn, schemaname, tablename):
    cur = conn.cursor()
    if schemaname:
        cur.execute('SET SEARCH_PATH = %s;', (schemaname, ))
    if tablename:
        cur.execute(
            'SELECT * FROM pg_table_def WHERE tablename = %s',
            (tablename, ))
    else:
        cur.execute('SELECT * FROM pg_table_def')
    defs = cur.fetchall()
    cur.close()
    return defs

def get_table_name(r):
    return '%s.%s' % (r[0], r[1])

def group_table_defs(table_defs):
    curr_table = None
    defs = []
    for r in table_defs:
        table = get_table_name(r)
        if curr_table and curr_table != table:
            yield defs
            defs = []
        curr_table = table
        defs.append(r)
    if defs:
        yield defs

def build_stmts(table_defs):
    for defs in group_table_defs(table_defs):
        table = get_table_name(defs[0])
        s = 'CREATE TABLE "%s" (\n' % table
        cols = []
        for d in defs:
            c = []
            c.append('"%s"' % d[2]) # column
            c.append(d[3]) # type
            if d[4] != 'none': # encode
                c.append('encode')
                c.append(d[4])
            if d[5]: # distkey
                c.append('DISTKEY')
            if d[6]: # sortkey
                c.append('SORTKEY')
            if d[7]: # notnull
                c.append('NOT NULL')
            cols.append(' '.join(c))
        s += ',\n'.join(map(lambda c:'    '+c, cols))
        s += '\n);'
        yield table, s

def show_create_table(host, user, password, dbname, schemaname=None, tablename=None, port=5432):
    conn = psycopg2.connect(
        host=host, port=port, database=dbname, user=user, password=password)
    table_defs = get_table_defs(conn, schemaname, tablename)
    statements = build_stmts(table_defs)
    return statements

def main(host, user, password, dbname, schemaname=None, tablename=None, port=5432):
    for table, stmt in show_create_table(
        host, user, password, dbname, schemaname, tablename, port):
        print '-- Table:', table
        print stmt
        print

if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument('-h', '--host', required=True, dest='host')
    parser.add_argument('-U', '--user', required=True, dest='user')
    parser.add_argument('-d', '--dbname', required=True, dest='dbname')
    parser.add_argument('-W', '--password', required=True, dest='password')
    parser.add_argument('-p', '--port', default=5432, dest='port')
    parser.add_argument('--schema', dest='schemaname')
    parser.add_argument('--table', dest='tablename')

    args = parser.parse_args()
    main(
        args.host,
        args.user,
        args.password,
        args.dbname,
        args.schemaname,
        args.tablename,
        args.port
    )

