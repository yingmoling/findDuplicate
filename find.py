'''
@Author: moling
@Date: 2018-11-21 10:35:51
@LastEditors: moling
@LastEditTime: 2018-11-21 22:37:46
@Description: 整理find内的代码，用来查找指定目录下的重复文件。
'''
import re
import os
import os.path
import sys
import sqlite3
import hashlib
from queue import Queue

DBPATH="d:\\"
DBNAME="eTDB.db"
FTABLE="file_t"
DTABLE="directory_t"
SUFFIX=["pdf","epub","mobi"]
OPENMODE="rb"
WRITEMODE="a+"
CODING="utf-8"
DEFAULTPATH="E:"
SQL_CREATE_FTABLE='create table {} (id integer primary key,name nvarchar(20),path nvarchar(255),fsize varchar(10),mtime char(19),hash char(40))'
SQL_CREATE_INDEX='create index {}_index on {} ({}})'

DUPLICATEFILES = "duplicates.dp"

def db_connect(dbpath=None,dbname=None):
    conn = False

    if(dbpath is None and dbname is None):
        dbfullpath = os.path.join(DBPATH,DBNAME)
    elif(dbpath is None):
        dbfullpath = os.path.join(dbpath,DBNAME)
    elif(dbname is None):
        dbfullpath = os.path.join(DBPATH,dbname)

    if(os.path.isfile(dbfullpath) and not os.path.islink(dbfullpath)):
        try:
            conn = sqlite3.connect(dbfullpath)
        except:
            conn = False
    elif(os.path.islink(dbfullpath)):
        os.remove(dbfullpath)
        try:
            conn = sqlite3.connect(dbfullpath)
        except:
            conn = False
    else:
        try:
            conn = sqlite3.connect(dbfullpath)
        except:
            conn = False
    return conn       


def db_connect_close(conn):
    if(conn is not False):
        conn.commit()
        conn.close()
        return True
    else:
        return False



def is_table_exists(cur,table_name):
    if(cur.execute("select name from sqlite_master where type='table' and name ='{}'".format(table_name))):
        return True
    else:
        return False


def create_table(cur,table_name,sql_struct):
    try:
        cur.execute(sql_struct.format(table_name))
        return True
    except:
        return False

def create_index(cur,table_name,key,sql_struct):
    try:
        cur.execute(sql_struct.format(key,table_name,key))
        return True
    except:
        return False

def add_info(cur,table_name,item_info,sql_add):
    try:
        cur.execute(sql_add.format(table_name,item_info))
        return True
    except:
        return False

def del_info(cur,table_name,item_id,sql_del):
    try:
        cur.execute(sql_del.format(table_name,item_id))
        return True
    except:
        return False

def search_info(cur,table_name,item_info,sql_search):
    try:
        cur.execute(sql_search.format(table_name,item_info))
        return True
    except:
        return False

def find_duplicate(cur,table_name,key,sql_duplicate):
    try:
        cur.execute(sql_duplicate.format(table_name,key,key,table_name,key))
    except:
        print("Failed to find duplicates.")
        return False
    try:
        fullpath = os.path.join(DBPATH,DUPLICATEFILES)
        with open(fullpath,WRITEMODE,encoding=CODING) as f:
            line = cur.fetchone()
            while(line):
                s=str(line[0])+"|"+"|".join(line[1:])+'\n'
                f.write(s)
                line=cur.fetchone()
        return True
    except:
        print("Failed to Write to file!")
        return False

def is_file_exists(fullpath):
    if(os.path.isfile(fullpath) and not os.path.islink(fullpath)):
        return True
    else:
        return False

def is_directory_exists(fullpath):
    return os.path.isdir(fullpath)

def record_format(suffix=None):
    if suffix is None:
        suffix = SUFFIX
    pattern ='(\S*)'+'('+("|".join(suffix))+')$'
    judge = re.compile(pattern,flags=re.IGNORECASE)
    return judge

def is_need_record(judge,name):
    return judge.match(name)

def sha1sum(fullpath):
    if(is_file_exists(fullpath)):
        filehash = hashlib.sha1()
        with open(fullpath,OPENMODE) as f:
            for line in f.readlines():
                filehash.update(line)
        return filehash
    else:
        return False
   
def exit(message="Done!"):
        input("{} Enter any key to Exit...".format(message))
        sys.exit()



if __name__ == "__main__":
    conn = db_connect()
    if(conn is False):
        exit("Can't connect database.")

    print("Connect to DB.....")

    if(is_directory_exists(DEFAULTPATH) is False):
        exit("The directory {} is not exists.".format(DEFAULTPATH))
    print("The scanpath is exists......")
    cur = conn.cursor()

    if create_table(cur,FTABLE,SQL_CREATE_FTABLE) is True:
        print("Create table {}".format(FTABLE))
    else:
        print("Can't creat tabe {}".format(FTABLE))

    print("Files table is exists....")


    scandirs=os.path.join(DEFAULTPATH,'\\')
    dir_queue = Queue()
    dir_queue.put(scandirs)
    file_judge = record_format()

    print("Start scanning.....")
    while(not dir_queue.empty()):
        directory = dir_queue.get()
        try:
            with os.scandir(directory) as it:
                for entry in it:
                    fullpath = os.path.join(directory,entry.name)
                    if entry.is_dir():
                        dir_queue.put(fullpath)
                        continue
                    if is_need_record(file_judge,entry.name):
                        sha1 = sha1sum(fullpath)
                        if sha1 is False:
                            print("sha1sum failed!!! {}".format(fullpath))
                            continue
                        stat = entry.stat()
                        insert_content="(name,path,fsize,mtime,hash) values ('{}','{}','{}','{}','{}')".format(entry.name,directory,stat.st_size,stat.st_mtime_ns,sha1.hexdigest())
                        sql_statement = "insert into {} {}"
                        if add_info(cur,FTABLE,insert_content,sql_statement):
                            conn.commit()
                        else:
                            exit("add_info failed!!! {}".format(fullpath))
        except:
            print("Something happened when scanning {}.".format(directory))


    sql_statement = 'select * from {} as a where ({} in (select {} from {} as b where a.id <> b.id)) order by {}'
    if(find_duplicate(cur,FTABLE,"hash",sql_statement) is False):
        exit("While find duplicates,Some err happened.")

    cur.close()
    if db_connect_close(conn):
        exit()
    else:
        exit("DB cannt be close.")
