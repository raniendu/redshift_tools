import psycopg2
from networkx import Graph, OrderedGraph
from flask import Flask
import json
from flask import make_response

app = Flask(__name__)

__docformat__ = "restructuredtext en"


class PrintGraph(OrderedGraph):
    """
    Example subclass of the Graph class.

    Prints activity log to file or standard output.
    """

    def __init__(self, data=None, name='', file=None, **attr):
        OrderedGraph.__init__(self, data=data, name=name, **attr)
        if file is None:
            import sys
            self.fh = sys.stdout
        else:
            self.fh = open(file, 'w')

    def add_node(self, n, attr_dict=None, **attr):
        OrderedGraph.add_node(self, n, attr_dict=attr_dict, **attr)
        self.fh.write("--Add node: {}\n".format(n))

    def add_nodes_from(self, nodes, **attr):
        for n in nodes:
            self.add_node(n, **attr)

    def remove_node(self, n):
        OrderedGraph.remove_node(self, n)
        self.fh.write("--Remove node: {}\n".format(n))

    def remove_nodes_from(self, nodes):
        for n in nodes:
            self.remove_node(n)

    def add_edge(self, u, v, attr_dict=None, **attr):
        OrderedGraph.add_edge(self, u, v, attr_dict=attr_dict, **attr)
        self.fh.write("--Add edge: {}-{}\n".format(u, v))

    def add_edges_from(self, ebunch, attr_dict=None, **attr):
        for e in ebunch:
            u, v = e[0:2]
            self.add_edge(u, v, attr_dict=attr_dict, **attr)

    def remove_edge(self, u, v):
        OrderedGraph.remove_edge(self, u, v)
        self.fh.write("--Remove edge: {}-{}\n".format(u, v))

    def remove_edges_from(self, ebunch):
        for e in ebunch:
            u, v = e[0:2]
            self.remove_edge(u, v)

    def clear(self):
        OrderedGraph.clear(self)
        self.fh.write("--Clear graph\n")


g = PrintGraph()
grants = []
HOST=''
PORT=
USER=''
PASSWORD=''
DATABASE='''


def get_dependent_objects(schema, table):
    connection = psycopg2.connect(host=HOST, port=PORT,
                                  user=USER, password=PASSWORD, database=DATABASE, )
    cursor = connection.cursor()
    query = '''SELECT distinct source_ns.nspname AS source_schema,
               source_table.relname AS source_table,
               dependent_ns.nspname AS dependent_schema,
               dependent_view.relname AS dependent_view
               FROM pg_depend
               JOIN pg_rewrite ON pg_depend.objid = pg_rewrite.oid
               JOIN pg_class AS dependent_view ON pg_rewrite.ev_class = dependent_view.oid
               JOIN pg_class AS source_table ON pg_depend.refobjid = source_table.oid
               JOIN pg_attribute
               ON pg_depend.refobjid = pg_attribute.attrelid
               AND pg_depend.refobjsubid = pg_attribute.attnum
               JOIN pg_namespace dependent_ns ON dependent_ns.oid = dependent_view.relnamespace
               JOIN pg_namespace source_ns ON source_ns.oid = source_table.relnamespace
               WHERE source_ns.nspname = '{0}'
               AND source_table.relname = '{1}'
               AND   pg_attribute.attnum > 0
               --AND   pg_attribute.attname = 'my_column'
               ORDER BY 1,2;'''
    cursor.execute(query.format(schema, table))
    result_set = cursor.fetchall()
    cursor.close()
    connection.close()
    return result_set


def get_acl(schema, table):
    connection = psycopg2.connect(host=HOST, port=PORT,
                                  user=USER, password=PASSWORD, database=DATABASE, )
    cursor = connection.cursor()
    query = '''select nspname,relname,relacl,nspacl
    from (select * from pg_class c
      left join pg_namespace nsp
        on (c.relnamespace = nsp.oid)
    where nspname
          not in ('pg_catalog','information_schema')and nspname not ilike 'pg_t%'
                                and c.relname not in (select
                                indexname
                                from pg_indexes))
                                where relname = '{1}' and nspname='{0}'
                                ;'''
    cursor.execute(query.format(schema, table))
    result_set = cursor.fetchall()
    cursor.close()
    connection.close()
    return result_set[0]


def add_nodes(dependent_objects):
    for (src_schema, src_object, tgt_schema, tgt_object) in dependent_objects:
        g.add_node((src_schema, src_object))
        g.add_node((tgt_schema, tgt_object))
        g.add_edge((src_schema, src_object), (tgt_schema, tgt_object))
        add_nodes(get_dependent_objects(tgt_schema, tgt_object))
    return g


def generate_grant_statements(privilege, actor, is_group, schema, object, grant_option, is_relation):
    if is_group == True and grant_option == True and is_relation == True:
        statement = 'GRANT {0} ON {1}.{2} TO GROUP {3} WITH GRANT OPTION;'.format(privilege, schema, object, actor)
    elif is_group == True and grant_option == True and is_relation == False:
        statement = 'GRANT {0} ON SCHEMA {1} TO GROUP {3} WITH GRANT OPTION;'.format(privilege, schema, object, actor)
    elif is_group == True and grant_option == False and is_relation == True:
        statement = 'GRANT {0} ON {1}.{2} TO GROUP {3};'.format(privilege, schema, object, actor)
    elif is_group == False and grant_option == True and is_relation == True:
        statement = 'GRANT {0} ON {1}.{2} TO {3} WITH GRANT OPTION;'.format(privilege, schema, object, actor)
    elif is_group == True and grant_option == False and is_relation == False:
        statement = 'GRANT {0} ON SCHEMA {1} TO GROUP {3};'.format(privilege, schema, object, actor)
    elif is_group == False and grant_option == True and is_relation == False:
        statement = 'GRANT {0} ON SCHEMA {1} TO {3} WITH GRANT OPTION;'.format(privilege, schema, object, actor)
    elif is_group == False and grant_option == False and is_relation == True:
        statement = 'GRANT {0} ON {1}.{2} TO {3};'.format(privilege, schema, object, actor)
    elif is_group == False and grant_option == False and is_relation == False:
        statement = 'GRANT {0} ON SCHEMA {1} TO {3};'.format(privilege, schema, object, actor)
    else:
        statement = 'Exception: Grant {} ON {}.{} TO {}'.format(privilege, schema, object, actor)

    #print('-- {}'.format(statement))

    return statement


def grants_from_acl(schema, object, acl_rules, is_relation):
    # https://www.postgresql.org/docs/9.1/static/sql-grant.html
    is_all = False
    if acl_rules is None:
        grants.extend([])
        return grants
    for acl_rule in acl_rules.split(','):
        with_grant_option = False
        is_group = False

        acl_rule = acl_rule.strip('''{''').strip('''}''')

        if 'group' in acl_rule:
            acl_rule = acl_rule.strip('''"''').lstrip('''group''').strip()
            is_group = True

        grantee = acl_rule[0:acl_rule.find('=')]
        granter = acl_rule[acl_rule.find('/') + 1:]
        privileges = acl_rule[acl_rule.find('=') + 1:acl_rule.find('/')]
        last_privilege = ''

        print('--Generating grants for {}'.format(acl_rule))

        if privileges == 'a*r*w*d*R*x*t*' or privileges == 'arwdRxt':
            privilege = 'ALL'
            is_all = True
            if privileges == 'a*r*w*d*R*x*t*':
                with_grant_option = True
            grants.append(
                generate_grant_statements(privilege, grantee, is_group, schema, object, with_grant_option, is_relation))

        if is_all == False:
            for privilege in list(privileges):
                if privilege == 'U':
                    privilege = 'USAGE'
                elif privilege == 'r':
                    privilege = 'SELECT'
                elif privilege == 'a':
                    privilege = 'INSERT'
                elif privilege == 'w':
                    privilege = 'UPDATE'
                elif privilege == 'd':
                    privilege = 'DELETE'
                elif privilege == 'D':
                    privilege = 'TRUNCATE'
                elif privilege == 'x':
                    privilege = 'REFERENCES'
                elif privilege == 'X':
                    privilege = 'EXECUTE'
                elif privilege == 't':
                    privilege = 'TRIGGER'
                elif privilege == 'C':
                    privilege = 'CREATE'
                elif privilege == 'c':
                    privilege = 'CONNECT'
                elif privilege == 't':
                    privilege = 'TEMPORARY'
                elif privilege == 'R':
                    privilege = 'RULE'
                elif privilege == '*':
                    privilege = last_privilege
                    with_grant_option = True
                    grants.pop()

                last_privilege = privilege

                grants.append(generate_grant_statements(privilege, grantee, is_group, schema, object, with_grant_option,
                                                        is_relation))

        is_all = False

    return grants


def get_view_def(schema, object):
    connection = psycopg2.connect(host=HOST, port=PORT,
                                  user=USER, password=PASSWORD, database=DATABASE, )
    cursor = connection.cursor()
    query = '''select pg_get_viewdef('{}.{}',TRUE)'''
    cursor.execute(query.format(schema, object))
    result_set = cursor.fetchall()
    cursor.close()
    connection.close()
    return result_set[0][0]

def get_view_owner(schema, object):
    connection = psycopg2.connect(host=HOST, port=PORT,
                                  user=USER, password=PASSWORD, database=DATABASE, )
    cursor = connection.cursor()
    query = '''select viewowner from pg_views where schemaname = '{}' and viewname = '{}';'''
    cursor.execute(query.format(schema, object))
    result_set = cursor.fetchall()
    cursor.close()
    connection.close()
    return result_set[0][0]

def jsonify(status=200, indent=4, sort_keys=True, **kwargs):
  response = make_response(json.dumps(dict(**kwargs), indent=indent, sort_keys=sort_keys))
  response.headers['Content-Type'] = 'application/json; charset=utf-8'
  response.headers['mimetype'] = 'application/json'
  response.status_code = status
  return response

def main():
    all_grants = []

    table_list = (('schema','table'),)

    for (base_schema, base_table) in table_list:
        g.clear()
        graph = add_nodes(get_dependent_objects(base_schema, base_table))

        print('--Total Nodes = {}'.format(graph.number_of_nodes()))

        reversed_graph = []

        for (schema, table) in graph.nodes:
            reversed_graph.append((schema, table))

        reversed_graph.reverse()

        for (schema, table) in reversed_graph:
            if not get_view_def(schema, table)=='Not a view':
                print('\nDROP VIEW IF EXISTS {}.{};'.format(schema, table))

        for (schema, table) in graph.nodes:
            print('--Processing Node : {},{}'.format(schema, table))

            if not get_view_def(schema,table)=='Not a view':
                print('\nCREATE VIEW {0}.{1} AS '.format(schema,table) + get_view_def(schema,table))
                print('''ALTER VIEW {0}.{1} OWNER TO {2};'''.format(schema,table,get_view_owner(schema,table)))


            (schema, table, relacl, nspacl) = get_acl(schema, table)

            all_grants.extend(grants_from_acl(schema, table, relacl, True))
            all_grants.extend(grants_from_acl(schema, table, nspacl, False))

    for grants in set(all_grants):
        print(grants)

    return json.dumps(all_grants)

if __name__ == '__main__':
    main()
