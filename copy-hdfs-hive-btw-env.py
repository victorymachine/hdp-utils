
import csv
import argparse

user='vicmac'
parser = argparse.ArgumentParser()
parser.add_argument("--envsource", help="envsource",dest="envsource")
parser.add_argument("--envdest", help="envdest",dest="envdest")
parser.add_argument("--hivesource", help="hivesource",dest="hivesource")
parser.add_argument("--hivedest", help="hivedest",dest="hivedest")
parser.add_argument("--hiveuser", help="hiveuser",dest="hiveuser")
parser.add_argument("--hivepassword", help="hivepassword",dest="hivepassword")
args = parser.parse_args()
with open('table_list.csv', 'rb') as csvfile:
        spamreader = csv.reader(csvfile, delimiter='|', quotechar='"')
        data = []
        command=''
        for line in spamreader:
                command+= "\n\n"
                command+= "hdfs dfs -rm -r hdfs://%s/user/%s/source/%s/%s_%s/CURRENT\n" % (args.envdest,user,line[0],line[1],line[2])
                #command+= "hdfs dfs -mkdir -p hdfs://%s/user/%s/source/%s/%s_%s/CURRENT\n" % (args.envdest,user,line[0],line[1],line[2])
                command+= "hadoop distcp -Dmapred.job.queue.name=distcp hdfs://%s/user/%s/source/%s/%s_%s/CURRENT hdfs://%s/user/%s/source/%s/%s_%s/\n" % (args.envsource,user,line[0],line[1],line[2],args.envdest,user,line[0],line[1],line[2])
                command+= "hdfs dfs -rm -r -f hdfs://%s/tmp/%s.%s_%s\n" % (args.envdest,user,line[0],line[1],line[2])

                query= "export table %s.%s_%s to 'hdfs://%s/tmp/%s.%s_%s';" % (line[0],line[1],line[2],args.envdest,user,line[0],line[1],line[2])

                command+='''beeline -u "jdbc:hive2://%s:10000/default;AuthMech=3;tez.queue.name=batch;" -n %s -p %s -e "%s"\n''' % (args.hivesource,args.hiveuser,args.hivepassword,query)

                query= "drop table if exists %s.%s_%s;import table %s.%s_%s from 'hdfs://%s/tmp/%s.%s_%s';" % (line[0],line[1],line[2],line[0],line[1],line[2],args.envdest,line[0],line[1],line[2])

                command+='''beeline -u "jdbc:hive2://%s:10000/default;AuthMech=3;tez.queue.name=batch;" -n %s -p %s -e "%s"\n''' % (args.hivedest,user,args.hiveuser,args.hivepassword,query)
                command+= "hdfs dfs -rm -r -f hdfs://%s/tmp/%s.%s_%s\n" % (args.envdest,user,line[0],line[1],line[2])
with open('bashcopy.sh', 'w') as s:
        s.write(command)
