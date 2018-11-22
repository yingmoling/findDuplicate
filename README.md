# 目的
用来查找目录下的重复文件  
- 根据后缀名来记录文件
- 查找重复文件
- 查看是否包含该文件
## 用法
```
usage: find.py [-h] [-s SUFFIX] [-d DIRECTORY] [--dbpath DBPATH]
               [--dbname DBNAME] [--file FILE] [--delete DELETE]

Find duplicate files.

optional arguments:
  -h, --help            show this help message and exit
  -s SUFFIX, --suffix SUFFIX
                        Define the file suffixe that need record. 
                        Example:"txt pdf epub"
  -d DIRECTORY, --directory DIRECTORY
                        Where the script to scan. Default directory is where
                        the script file store.
  --dbpath DBPATH       specify the database path.
  --dbname DBNAME       Name the database.
  --file FILE           Check the file is or not in database.
  --delete DELETE       Delete the file and it's record in database.

```
```
usage: find.py [-h] [-s SUFFIX] [-d DIRECTORY] [--dbpath DBPATH]
               [--dbname DBNAME] [--file FILE] [--delete DELETE]

查找重复文件。将重复文件的信息写入到duplicates.dp文件。直接用文本编辑器打开就可以查看。

optional arguments:
  -h, --help            show this help message and exit
  -s SUFFIX, --suffix SUFFIX
                        指定需要记录的文件后缀名。
                        例如："txt pdf epub"
                        默认为"pdf epub mobi"
  -d DIRECTORY, --directory DIRECTORY
                        指定要搜索的目录。
                        默认为脚本所在的目录为起始目录。
  --dbpath DBPATH       指定结果的存放位置.
                        默认目录与脚本所在目录相同
  --dbname DBNAME       为存储结果的数据库命名。
                        默认为TDB.db。
  --file FILE           查看指定文件是否已经记录。
  --delete DELETE       删除指定文件的记录信息。

```
**示例**
```
不带参数，将find.py放到，需要扫描的目录。比如E盘根目录。在终端里运行
python e:\find.py

带参数的。将终端切换到find.py所在的目录运行
python find.py -s "txt epub mobi pdf azw3" -d e: --dbpath d: --dbname E.db
```
