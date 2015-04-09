#!/usr/bin/env python

"""
'show create table' equivalent for aws redshift
 xiuming chen <cc@cxm.cc>
"""

import psycopg2

__all__ = ['show_create_table']

def add_where_stmts(schemaname, tablename):
    wheres = []
    if tablename:
        wheres.append('tablename = %(table)s')
    if schemaname:
        wheres.append('schemaname = %(schema)s')
    return ' AND '.join(wheres)

def get_table_diststyles(cur, schemaname, tablename):
    sql = '''
    SELECT n.nspname AS schemaname, c.relname AS tablename, c.reldiststyle AS diststyle
    FROM pg_namespace n, pg_class c
    WHERE n.oid = c.relnamespace AND pg_table_is_visible(c.oid)
    '''
    where = add_where_stmts(schemaname, tablename)
    if where:
        sql += ' AND ' + where
    cur.execute(sql, dict(table=tablename, schema=schemaname))
    d = {}
    for r in cur.fetchall():
        table = get_table_name(r)
        if r[2] == 0:
            d[table] = 'EVEN'
        elif r[2] == 1:
            d[table] = 'KEY'
        elif r[2] == 8:
            d[table] = 'ALL'
        else:
            d[table] = 'UNKNOWN'
    return d

def get_table_defs(cur, schemaname, tablename):
    sql = 'SELECT * FROM pg_table_def'
    where = add_where_stmts(schemaname, tablename)
    if where:
        sql += ' WHERE ' + where
    cur.execute(sql, dict(table=tablename, schema=schemaname))
    defs = cur.fetchall()
    return defs

def get_table_name(r):
    if '.' not in r[0] and '.' not in r[1]:
        return '%s.%s' % (r[0], r[1])
    return '"%s"."%s"' % (r[0], r[1])

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

def build_stmts(table_defs, table_diststyles):
    for defs in group_table_defs(table_defs):
        table = get_table_name(defs[0])
        s = 'CREATE TABLE %s (\n' % table
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
        s += '\n)'
        if table_diststyles.get(table) == 'ALL':
            s += ' DISTSTYLE ALL '
        s += ';'
        yield table, s

def show_create_table(host, user, password, dbname, schemaname=None, tablename=None, port=5432):
    conn = psycopg2.connect(
        host=host, port=port, database=dbname, user=user, password=password)
    cur = conn.cursor()
    if schemaname:
        cur.execute('SET SEARCH_PATH = %s;', (schemaname, ))
    table_defs = get_table_defs(cur, schemaname, tablename)
    table_diststyles = get_table_diststyles(cur, schemaname, tablename)
    cur.close()
    statements = build_stmts(table_defs, table_diststyles)
    return statements

def main(host, user, password, dbname, schemaname=None, tablename=None, port=5432):
    for table, stmt in show_create_table(
        host, user, password, dbname, schemaname, tablename, port):
        print ('-- Table: %s\n%s\n' % (table, stmt))

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

