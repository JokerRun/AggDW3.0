schematool -initSchema -dbType mysql -verbose
#  修改表字段注解和表注解
#alter table COLUMNS_V2 modify column COMMENT varchar(256) character set utf8
#alter table TABLE_PARAMS modify column PARAM_VALUE varchar(4000) character set utf8

#  修改分区字段注解
#alter table PARTITION_PARAMS modify column PARAM_VALUE varchar(4000) character set utf8 ;
#alter table PARTITION_KEYS modify column PKEY_COMMENT varchar(4000) character set utf8;

#  修改索引注解
#alter table INDEX_PARAMS modify column PARAM_VALUE varchar(4000) character set utf8;