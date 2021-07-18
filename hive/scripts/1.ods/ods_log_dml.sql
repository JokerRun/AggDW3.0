-- 说明Hive的LZO压缩：https://cwiki.apache.org/confluence/display/Hive/LanguageManual+LZO
load data inpath '/origin_data/gmall/log/topic_log/2021-05-03' into table ods_log partition (dt = '2021-05-03');

-- 为lzo压缩文件创建索引
-- hadoop jar /opt/module/hadoop-3.1.4/share/hadoop/common/hadoop-lzo-0.4.20.jar com.hadoop.compression.lzo.DistributedLzoIndexer -Dmapreduce.job.queuename=hive /warehouse/gmall/ods/ods_log/dt=2021-05-03