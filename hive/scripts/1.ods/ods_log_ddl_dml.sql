drop table if exists ods_log;
CREATE EXTERNAL TABLE ods_log
(
    `line` string
)
    PARTITIONED BY (`dt` string) -- 按照时间创建分区
    STORED AS -- 指定存储方式，读数据采用LzoTextInputFormat；
        INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
        OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
    LOCATION '/warehouse/gmall/ods/ods_log' -- 指定数据在hdfs上的存储位置
;
-- 2）加载数据
-- 说明Hive的LZO压缩：https://cwiki.apache.org/confluence/display/Hive/LanguageManual+LZO
load data inpath '/origin_data/gmall/log/topic_log/2021-05-03' into table ods_log partition (dt = '2021-05-03');

-- 简单读取，扫描HDFS，不启动MR
select *from ods_log;

-- 启动MR任务/SparkJobs
select count(*)from ods_log alias;
-- 查看建表语句
show create table ods_log;

-- 为lzo压缩文件创建索引
-- hadoop jar /opt/module/hadoop-3.1.4/share/hadoop/common/hadoop-lzo-0.4.20.jar com.hadoop.compression.lzo.DistributedLzoIndexer -Dmapreduce.job.queuename=hive /warehouse/gmall/ods/ods_log/dt=2021-05-03