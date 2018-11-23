'''
@Author: moling
@Date: 2018-11-21 10:35:51
@LastEditors: moling
@LastEditTime: 2018-11-23 12:12:56
@Description: 整理find内的代码，用来查找指定目录下的重复文件。
'''
import re
import os
import os.path
import sys
import sqlite3
import hashlib
from queue import Queue
import argparse

DATABASE="d:\\TDB.db"
DBPATH=sys.path[0]
DBNAME="TDB.db"
SUFFIX=["pdf","epub","mobi"]
DEFAULTPATH=sys.path[0]

CODING="utf-8"
OPENMODE="rb"
WRITEMODE="a+"
FTABLE="file_t"
DTABLE="directory_t"
SQL_CREATE_FTABLE='create table {} (id integer primary key,name nvarchar(20),path nvarchar(255),fsize varchar(10),mtime char(19),hash char(40))'
SQL_CREATE_INDEX='create index {}_index on {} ({}})'
DUPLICATEFILES = "duplicates.dp"

def db_connect(dbpath=None,dbname=None):
    conn = False

    if(dbpath is None and dbname is None):
        dbfullpath = os.path.join(DBPATH,DBNAME)
    elif(dbpath is None):
        dbfullpath = os.path.join(DBPATH,dbname)
    elif(is_directory_exists(dbpath) and dbname is not None):
        dbfullpath = os.path.join(dbpath,dbname)
    else:
        dbfullpath = os.path.join(DBPATH,DBNAME)
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

def del_info(id,dbpath=None,dbname=None):
    conn =db_connect(dbpath,dbname)
    if(conn is False):
        exit("Can't connect database.")
    print("Connect to DB.....")
    cur = conn.cursor()
    sql_search = "SELECT * from {} where id={}"
    sql_del = "DELETE from {} where id={}"
    try:
        if(search_info(cur,FTABLE,id,sql_search) is not False):
            item=cur.fetchone()
            os.remove(os.path.join(item[2],item[1]))
        else:
            cur.close()
            db_connect_close(conn)
            exit("Can't delete the file which id is {}.".format(id))
        try:
            cur.execute(sql_del.format(FTABLE,id))
            cur.close()
            db_connect_close(conn)
        except:
            cur.close()
            db_connect_close(conn)
            exit("Can't delete the file info which id is {}.".format(id))
        return True
    except:
        cur.close()
        db_connect_close(conn)
        return False

def search_info(cur,table_name,item_info,sql_search):
    try:
        cur.execute(sql_search.format(table_name,item_info))
        return cur
    except:
        return False

def find_duplicate(cur,table_name,key,sql_duplicate,dbpath=None):
    if(dbpath is None):
        dbpath = DBPATH
    try:
        cur.execute(sql_duplicate.format(table_name,key,key,table_name,key))
    except:
        print("Failed to find duplicates.")
        return False
    try:
        fullpath = os.path.join(dbpath,DUPLICATEFILES)
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
    else:
        suffix=suffix.split()
    pattern =r"(\S*)"+'('+("|".join(suffix))+')$'
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
        return filehash.hexdigest()
    else:
        return False


def scan_dirs(cur,suffix=None,fullpath=None):
    if fullpath is None:
        fullpath=DEFAULTPATH
    elif (not os.path.isdir(fullpath)):
        fullpath=DEFAULTPATH
    if len(fullpath)==2 and fullpath[1]==":":
        fullpath=os.path.join(fullpath,'\\')
    if(is_directory_exists(fullpath) is False):
            exit("The directory {} is not exists.".format(fullpath))
    print("The scanpath is exists......")
    dir_queue = Queue()
    dir_queue.put(fullpath)
    file_judge = record_format(suffix)

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
                        insert_content="(name,path,fsize,mtime,hash) values ('{}','{}','{}','{}','{}')".format(entry.name,directory,stat.st_size,stat.st_mtime_ns,sha1)
                        sql_statement = "insert into {} {}"
                        if add_info(cur,FTABLE,insert_content,sql_statement):
                            conn.commit()
                        else:
                            exit("add_info failed!!! {}".format(fullpath))
        except:
            print("Something happened when scanning {}.".format(directory))

def is_in_table(cur,table_name,item_info,sql_in):
    try:
        cur.execute(sql_in.format(table_name,item_info))
        return cur
    except:
        return False

def check_file(file,dbpath=None,dbname=None):
    conn= db_connect(dbpath,dbname)
    if(conn is False):
        exit("Can't connect database.")
    print("Connect to DB.....")
    cur = conn.cursor()
    if(is_file_exists(file)):
        sha1=sha1sum(file)
        sql_statement="select * from {} where hash='{}'"
        if(is_in_table(cur,FTABLE,sha1,sql_statement) is not False):
            line=cur.fetchone()
            while(line):
                print(line)
                line=cur.fetchone()
            cur.close()
            db_connect_close(conn)
            return True
        else:
            cur.close()
            db_connect_close(conn)
            return False
    else:
        cur.close()
        db_connect_close(conn)
        exit("Enter full path of the file,please.File dosen't exists.")
  
def exit(message="Done!"):
        input("{}\nEnter any key to Exit...".format(message))
        sys.exit()



if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Find duplicate files.")
    parser.add_argument("-s","--suffix",help="Define the file suffixe that need record. Example:\"txt pdf epub\"")
    parser.add_argument("-d","--directory",help="Where the script to scan. Default directory is where the script file store.")
    parser.add_argument("--dbpath",help="specify the database path.")
    parser.add_argument("--dbname",help="Name the database.")
    parser.add_argument("--file",help="Check the file is or not in database.")
    parser.add_argument("--delete",type=int,help="Delete the file and it's record in database.")
    args=parser.parse_args()
    
    suffix = args.suffix
    fullpath = args.directory
    dbpath = args.dbpath
    dbname = args.dbname
    file = args.file
    id=args.delete
    if(file is not None):
        if(check_file(file,dbpath,dbname)):
            exit()
        else:
            exit("File dose not exists.")

    if(id is not None):
        if(del_info(id,dbpath,dbname)):
            exit()
        else:
            exit("DELETE ERROR!")

    conn = db_connect(dbpath,dbname)
    if(conn is False):
        exit("Can't connect database.")

    print("Connect to DB.....")
    cur = conn.cursor()

    if create_table(cur,FTABLE,SQL_CREATE_FTABLE) is True:
        print("Create table {}".format(FTABLE))
    else:
        print("Can't creat table {}".format(FTABLE))
    print("Files table is exists....")

    scan_dirs(cur,suffix,fullpath)
    print("Scan finished.")
    print("****************")

    sql_statement = 'select * from {} as a where ({} in (select {} from {} as b where a.id <> b.id)) order by {}'
    print("Start to write info to file.")
    if(find_duplicate(cur,FTABLE,"hash",sql_statement,dbpath) is False):
        exit("While find duplicates,Some err happened.")
    print("Finished!")
    cur.close()
    if db_connect_close(conn):
        exit()
    else:
        exit("DB can't be close.")
