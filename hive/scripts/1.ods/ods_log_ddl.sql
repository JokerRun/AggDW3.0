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