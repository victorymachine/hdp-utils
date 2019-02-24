#notes          :# python bin/sqlserver_profiler.py dataset/data_sources_SQLSERVER.csv
#==============================================================================

import sys
import csv
import traceback
from datetime import datetime
from pprint import pprint
import getpass
import os
import json
import pymssql

if len(sys.argv) != 2:
   print "%s <data_sources_list.csv>\n\nExpected format:\nname,ip,port,tns,schema,login,password" % sys.argv[0]

def now():
    return datetime.now().strftime('%Y-%m-%d-%H-%M-%S')

def query(conn_detail,query):
        conn = pymssql.connect(conn_detail['ip'], conn_detail['login'], conn_detail['password'], conn_detail['instance'])
        cursor = conn.cursor(as_dict=True)
        cursor.execute(query)
        return cursor.fetchall()

def main():
        profiler= {
                "datasource" : []
        }
        #with open("dataset/data_sources_SQLSERVER.csv", 'rb') as csvfile:
        with open(sys.argv[1], 'rb') as csvfile:

                spamreader = csv.DictReader(csvfile, delimiter='\t', quotechar='"')
                for conn_detail in spamreader:
                        source_profile = {
                                "source_name": conn_detail['name'],
                                "connection_string" : "jdbc:sqlserver://%s:%s;databaseName=%s" % (conn_detail['ip'], conn_detail['port'], conn_detail['instance']),
                                "username" : conn_detail['login'],
                                "password" : conn_detail['password'],
                                "db_name" : conn_detail['instance'],
                                "schema" : conn_detail['schema'],
                                "tables":[]
                        }

                        table_list=query(conn_detail,"select table_name from INFORMATION_SCHEMA.TABLES")
                        for table in table_list:
                                column=query(conn_detail,"select data_type,column_name from INFORMATION_SCHEMA.COLUMNS where table_name = '%s'" % (table['table_name']))

                                table.update({'columns':column})
                                source_profile['tables'].append(table)
                        profiler['datasource'].append(source_profile)

        fname = 'profiler/profiler-SQLSERVER-output-%s.json' % now()
        with open(fname, "w") as write_file:
                write_file.write(json.dumps(profiler, sort_keys=True,indent=4))
        print "WRITTEN %s" % fname

if __name__ == '__main__':
    main()

