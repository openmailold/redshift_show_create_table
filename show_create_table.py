#!/usr/bin/env python

"""
'show create table' equivalent for aws redshift

 Authors:
 xiuming chen <cc@cxm.cc>
 Neil Halelamien
"""

from os import path, makedirs

import psycopg2


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


DISTSTYLES = {
    0: 'EVEN',
    1: 'KEY',
    8: 'ALL',
}

SYSTEM_SCHEMAS = ['information_schema', 'pg_catalog', 'sys']


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
        d[table] = DISTSTYLES.get(r[2])
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


def format_comment(table, schema, owner, tablespace, model_type='TABLE'):
    comment = ('--\n'
               '-- Name: %(table)s; Type: %(model_type)s; Schema: %(schema)s; Owner: %(owner)s; Tablespace: %(tablespace)s\n'
               '--\n\n') \
              % {
                  'table': table,
                  'schema': schema,
                  'owner': owner,
                  'model_type': model_type,
                  'tablespace': tablespace,
              }
    return comment


def build_table_stmts(table_defs, table_diststyles, table_infos):
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
        s = format_comment(tablename, schemaname, owner, space)
        s += 'CREATE TABLE %s (\n' % table
        cols = []
        sk = {}
        interleaved = False
        for d in defs:
            c = [
                '"%s"' % d['column'],
                d['type'],
            ]
            if d['encoding'] != 'none':
                c.append('ENCODE')
                c.append(d['encoding'])
            if d['distkey']:
                c.append('DISTKEY')
            if d['sortkey']:
                if d['sortkey'] < 0:
                    interleaved = True;
                sk[int(abs(d['sortkey']))] = d['column']
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
        if sk:
            if interleaved:
                s += ' INTERLEAVED'
            s += ' SORTKEY ("%s")' % '", "'.join([sk[k] for k in sorted(sk)])
        s += ';\n'
        yield schemaname, table, s


def build_view_stmts_for_schema(cur, schema):
    sql = '''
    SELECT c.relname, pg_get_userbyid(c.relowner) AS owner, pg_get_viewdef(c.oid) AS definition
    FROM pg_class c
    LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relkind = 'v'::"char" and nspname = %(schema)s
    '''
    cur.execute(sql, {'schema': schema})
    for v in cur.fetchall():
        view_name = v[0]
        owner = v[1]
        base_statement = v[2]
        s = format_comment(view_name, schema, owner, tablespace='', model_type='VIEW')
        s += 'CREATE OR REPLACE VIEW %s AS' % get_table_name(schema, view_name)
        s += '\n' + base_statement + '\n'
        yield schema, view_name, s


# gets list of all non-system schemas
def get_all_schemas(cur):
    sql = 'SELECT schemaname FROM pg_stat_all_tables GROUP BY schemaname'
    cur.execute(sql)
    schemas = []
    for s in cur.fetchall():
        schema = s[0]
        if schema not in SYSTEM_SCHEMAS:
            schemas.append(schema)
    return schemas


def show_create_table(host, user, dbname, schemaname=None, tablename=None, port=5432, password=None):
    if password:
        conn = psycopg2.connect(
            host=host, port=port, database=dbname, user=user, password=password)
    else:
        conn = psycopg2.connect(
            host=host, port=port, database=dbname, user=user)
    cur = conn.cursor()
    try:
        if schemaname is None and tablename is None:  # scan all non-system schemas and tables
            schema_list = get_all_schemas(cur)
            search_path_sql = 'SET SEARCH_PATH = ' + (','.join(schema_list)) + ';'
            cur.execute(search_path_sql)
        elif schemaname:
            cur.execute('SET SEARCH_PATH = %s;', (schemaname,))
            schema_list = [schemaname]
        else:
            raise RuntimeError('If passing a table name, schema name must also be provided')

        statements = []
        for schema in schema_list:
            table_diststyles = get_table_diststyles(cur, schema, tablename)
            table_defs = get_table_defs(cur, schema, tablename)
            table_infos = get_table_infos(cur, schema, tablename)
            for s in build_table_stmts(table_defs, table_diststyles, table_infos):
                statements.append(s)
            for s in build_view_stmts_for_schema(cur, schema):
                statements.append(s)
        return statements
    finally:
        cur.close()


def main(host, user, dbname, filename, file_format, schemaname=None, tablename=None, port=5432, password=None):
    for schema, table, stmt in show_create_table(
            host, user, dbname, schemaname, tablename, port, password):
        if filename:
            if file_format == 'directory':
                basedir = filename
                if not path.exists(basedir):
                    makedirs(basedir)
                schemadir = path.join(basedir, schema)
                if not path.exists(schemadir):
                    makedirs(schemadir)
                full_filename = path.join(schemadir, table + '.sql')
                with open(full_filename, 'w') as f:
                    f.write(stmt + '\n')
            else:
                raise RuntimeError('Invalid format: ' + file_format)
        else:
            print(stmt)


if __name__ == '__main__':
    import argparse

    # arguments similar to those for pg_dump
    parser = argparse.ArgumentParser(add_help=False)  # add_help=False because of conflict with '-h'
    parser.add_argument('-h', '--host', required=True, dest='host')
    parser.add_argument('-U', '--user', required=True, dest='user')
    parser.add_argument('-d', '--dbname', required=True, dest='dbname')
    parser.add_argument('-W', '--password', required=False, dest='password',
                        help='If no password is provided, the connector will attempt to authorize with .pgpass file in user\'s home directory, '
                        'or the file defined in PGPASSFILE system variable')
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
        args.dbname,
        args.file,
        args.format,
        args.schemaname,
        args.tablename,
        args.port,
        args.password,
    )
