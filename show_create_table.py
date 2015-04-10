#!/usr/bin/env python

"""
'show create table' equivalent for aws redshift
 xiuming chen <cc@cxm.cc>
"""

import psycopg2
from os import path, makedirs

__all__ = ['show_create_table']


def add_where_stmts(schemaname, tablename):
    wheres = []
    if tablename:
        wheres.append('tablename = %(table)s')
    if schemaname:
        wheres.append('schemaname = %(schema)s')
    return ' AND '.join(wheres)


def get_table_infos(cur, schemaname, tablename):
    sql = '''
SELECT schemaname, tablename, tableowner, tablespace
FROM pg_tables
'''
    where = add_where_stmts(schemaname, tablename)
    if where:
        sql += ' WHERE ' + where
    cur.execute(sql, dict(table=tablename, schema=schemaname))
    d = {}
    for r in cur.fetchall():
        table = get_table_name(r[0], r[1])
        d[table] = {
            'owner': r[2],
            'space': r[3],
        }
    return d


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
        table = get_table_name(r[0], r[1])
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
    sql = '''
SELECT
    n.nspname AS "schemaname",
    c.relname AS "tablename",
    a.attname AS "column",
    format_type(a.atttypid, a.atttypmod) AS "type",
    format_encoding(a.attencodingtype::integer) AS "encoding",
    a.attisdistkey AS "distkey",
    a.attsortkeyord AS "sortkey",
    a.attnotnull AS "notnull",
    a.atthasdef AS "hasdef",
    d.adsrc as "default"
FROM pg_attribute a
JOIN pg_class c ON c.oid = a.attrelid
JOIN pg_namespace n ON n.oid = c.relnamespace
LEFT JOIN pg_attrdef d ON d.adrelid = c.oid AND d.adnum = a.attnum
WHERE a.attnum > 0 AND NOT a.attisdropped AND pg_table_is_visible(c.oid) AND c.relkind = 'r'
'''
    where = add_where_stmts(schemaname, tablename)
    if where:
        sql += ' AND ' + where
    sql += ' ORDER BY n.nspname, c.relname, a.attnum;'
    cur.execute(sql, dict(table=tablename, schema=schemaname))
    out = []
    for r in cur.fetchall():
        out.append(dict(zip(
            ['schemaname', 'tablename', 'column', 'type',
             'encoding', 'distkey', 'sortkey', 'notnull',
             'hasdef', 'default'], r)))
    return out


def get_table_name(schema, table):
    if '.' not in schema and '.' not in table:
        return '%s.%s' % (schema, table)
    return '"%s"."%s"' % (schema, table)


def group_table_defs(table_defs):
    curr_table = None
    defs = []
    for r in table_defs:
        table = get_table_name(r['schemaname'], r['tablename'])
        if curr_table and curr_table != table:
            yield defs
            defs = []
        curr_table = table
        defs.append(r)
    if defs:
        yield defs


def build_stmts(table_defs, table_diststyles, table_infos):
    for defs in group_table_defs(table_defs):
        schemaname = defs[0]['schemaname']
        tablename = defs[0]['tablename']
        table = get_table_name(schemaname, tablename)
        table_info = table_infos.get(table)
        if table_info:
            owner = table_info['owner'] or ''
            space = table_info['space'] or ''
        else:
            owner = space = ''
        s = ('--\n'
             '-- Name: %(table)s; Type: TABLE; Schema: %(schema)s; Owner: %(owner)s; Tablespace: %(space)s\n'
             '--\n\n') % {
                'table': tablename,
                'schema': schemaname,
                'owner': owner,
                'space': space,
            }
        s += 'CREATE TABLE %s (\n' % table
        cols = []
        for d in defs:
            c = []
            c.append('"%s"' % d['column'])
            c.append(d['type'])
            if d['encoding'] != 'none':
                c.append('ENCODE')
                c.append(d['encoding'])
            if d['distkey']:
                c.append('DISTKEY')
            if d['sortkey']:
                c.append('SORTKEY')
            if d['notnull']:
                c.append('NOT NULL')
            if d['hasdef']:
                c.append('DEFAULT %s' % d['default'])
            cols.append(' '.join(c))
        s += ',\n'.join(map(lambda c: '    ' + c, cols))
        s += '\n)'
        diststyle = table_diststyles.get(table)
        if diststyle:
            s += ' DISTSTYLE ' + diststyle
        s += ';\n'
        yield table, s


def show_create_table(host, user, password, dbname, schemaname=None, tablename=None, port=5432):
    conn = psycopg2.connect(
        host=host, port=port, database=dbname, user=user, password=password)
    cur = conn.cursor()
    try:
        if schemaname:
            cur.execute('SET SEARCH_PATH = %s;', (schemaname,))
        table_diststyles = get_table_diststyles(cur, schemaname, tablename)
        table_defs = get_table_defs(cur, schemaname, tablename)
        table_infos = get_table_infos(cur, schemaname, tablename)
        statements = build_stmts(table_defs, table_diststyles, table_infos)
        return statements
    finally:
        cur.close()


# TODO: deal with case where no schemaname specified
def main(host, user, password, dbname, filename, format, schemaname=None, tablename=None, port=5432):
    for table, stmt in show_create_table(
            host, user, password, dbname, schemaname, tablename, port):
        if filename:
            if format == 'directory':
                basedir = filename
                if not path.exists(basedir):
                    makedirs(basedir)
                schemadir = path.join(basedir, schemaname)
                if not path.exists(schemadir):
                    makedirs(schemadir)
                full_filename = path.join(schemadir, table + '.sql')
                with open(full_filename, 'w') as f:
                    f.write(stmt + '\n')
            else:
                raise RuntimeError('Invalid format: ' + format)
        else:
            print(stmt)


if __name__ == '__main__':
    import argparse

    # arguments similar to those for pg_dump
    parser = argparse.ArgumentParser(add_help=False)  # add_help=False because of conflict with '-h'
    parser.add_argument('-h', '--host', required=True, dest='host')
    parser.add_argument('-U', '--user', required=True, dest='user')
    parser.add_argument('-d', '--dbname', required=True, dest='dbname')
    parser.add_argument('-W', '--password', required=True, dest='password')
    parser.add_argument('-p', '--port', default=5432, dest='port')
    parser.add_argument('-f', '--file', default=False, dest='file',
                        help='file/directory to write output to, defaults to standard output')
    parser.add_argument('-F', '--format', default='directory', dest='format',
                        choices=['directory'],
                        help='Requires --file, valid options: directory')
    parser.add_argument('-n', '--schema', dest='schemaname',
                        help='Name of schema to show tables from, if not provided it will iterate over all non-system'
                             'schemas')
    parser.add_argument('-t', '--table', dest='tablename')

    args = parser.parse_args()
    main(
        args.host,
        args.user,
        args.password,
        args.dbname,
        args.file,
        args.format,
        args.schemaname,
        args.tablename,
        args.port,
    )
