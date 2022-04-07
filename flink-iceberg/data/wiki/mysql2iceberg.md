# mysql-cdc 2 iceberg

## 1. 查看mysql binlog是否开启

```shell
show variables like '%bin%';
```

![image-20220407095238974](pic\image-20220407095238974.png)

## 2. 建MYSQL测试表

```mysql
CREATE DATABASE rison_db;

USE rison_db;

CREATE TABLE student (
       id INTEGER NOT NULL AUTO_INCREMENT PRIMARY KEY,
       name VARCHAR(255) NOT NULL,
       description VARCHAR(512)
);

INSERT INTO student VALUES (default,"rison","description_01");
```

## 3. 启动Flink SQL Client 执行DDL和查询

```mysql
SET execution.checkpointing.interval = 3s;

# 创建mysql-cdc source
CREATE TABLE student (
     id INT,
     name STRING,
     description STRING,
     PRIMARY KEY (id) NOT ENFORCED
   ) WITH (
     'connector' = 'mysql-cdc',
     'hostname' = 'tbds-172-16-16-142',
     'port' = '3306',
     'username' = 'root',
     'password' = 'portal@Tbds.com',
     'database-name' = 'rison_db',
     'table-name' = 'student'
   );
   
 # 查表 
 SELECT * FROM student;
 
 # 创建iceberg sink 表
 CREATE TABLE student_sink (
 id int,
 name STRING,
 description STRING,
 PRIMARY KEY (id) NOT ENFORCED
 ) WITH (
 'connector'='iceberg',
 'catalog-name'='iceberg_catalog',
 'catalog-type'='hive',  
 'uri'='thrift://tbds-172-16-16-41:9083',
 'warehouse'='hdfs:///apps/hive/warehouse',
 'format-version'='2',
 'database-name' = 'iceberg_db'
 );
   
 # 插入数据  
 insert into student_sink select * from student;
 # 设置streaming流
 SET execution.type = streaming ;
 # 允许dynamic table 的参数设置
 SET table.dynamic-table-options.enabled=true;
 # 查iceberg表，设置两个参数：'streaming'='true', 'monitor-interval'='1s'
 SELECT * FROM student_sink /*+ OPTIONS('streaming'='true', 'monitor-interval'='1s')*/ ;
```

![image-20220407144925518](pic\image-20220407144925518.png)

![image-20220407145446746](pic\image-20220407145446746.png)

## 4. Mysql实时插入数据，查看iceberg动态表数据变化

```mysql
 INSERT INTO student VALUES (default,"lisi","description2");
 INSERT INTO student VALUES (default,"zhangsan","description3");
```

![image-20220407145955432](pic\image-20220407145955432.png)

查看iceberg表变化：

![image-20220407150117373](pic\image-20220407150117373.png)

## 5. Flink 执行脚本

```shell
/usr/hdp/2.2.0.0-2041/flink/bin/flink run \
-c com.rison.iceberg.flink.cdc.mysql.Mysql2Iceberg \
-m yarn-cluster \
/root/flink-dir/original-flink-iceberg-1.0-SNAPSHOT.jar
```

## 6. MysqlClient 插入数据

```sql
INSERT INTO student VALUES (default,"数据1","description_数据1");
INSERT INTO student VALUES (default,"数据2","description_数据2");
```

![image-20220407172137741](pic\image-20220407172137741.png)

## 7. Flink client 查看数据变化

```sql
 USE catalog iceberg_catalog;
 USE iceberg_db;
 
 # 设置streaming流
 SET execution.type = streaming ;
 # 允许dynamic table 的参数设置
 SET table.dynamic-table-options.enabled=true;
 # 查iceberg表，设置两个参数：'streaming'='true', 'monitor-interval'='1s'
 SELECT * FROM student_sink /*+ OPTIONS('streaming'='true', 'monitor-interval'='1s')*/ ;
```

![image-20220407172240661](pic\image-20220407172240661.png)