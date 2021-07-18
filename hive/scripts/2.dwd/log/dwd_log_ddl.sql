-- 说明：数据采用parquet存储方式，是可以支持切片的，不需要再对数据创建索引。
-- 如果单纯的text方式存储数据，需要采用支持切片的，lzop压缩方式并创建索引。

-- 4.1.3 启动日志表
drop table if exists dwd_start_log;
CREATE EXTERNAL TABLE dwd_start_log
(
    `area_code`       string COMMENT '地区编码',
    `brand`           string COMMENT '手机品牌',
    `channel`         string COMMENT '渠道',
    `model`           string COMMENT '手机型号',
    `mid_id`          string COMMENT '设备id',
    `os`              string COMMENT '操作系统',
    `user_id`         string COMMENT '会员id',
    `version_code`    string COMMENT 'app版本号',
    `entry`           string COMMENT ' icon手机图标  notice 通知   install 安装后启动',
    `loading_time`    bigint COMMENT '启动加载时间',
    `open_ad_id`      string COMMENT '广告页ID ',
    `open_ad_ms`      bigint COMMENT '广告总共播放时间',
    `open_ad_skip_ms` bigint COMMENT '用户跳过广告时点',
    `ts`              bigint COMMENT '时间'
) COMMENT '启动日志表'
    PARTITIONED BY (dt string) -- 按照时间创建分区
    stored as parquet -- 采用parquet列式存储
    LOCATION '/warehouse/gmall/dwd/dwd_start_log' -- 指定在HDFS上存储位置
    TBLPROPERTIES ('parquet.compression' = 'lzo') -- 采用LZO压缩
;

-- 4.1.4 页面日志表
drop table if exists dwd_page_log;
CREATE EXTERNAL TABLE dwd_page_log(
    `area_code` string COMMENT '地区编码',
    `brand` string COMMENT '手机品牌',
    `channel` string COMMENT '渠道',
    `model` string COMMENT '手机型号',
    `mid_id` string COMMENT '设备id',
    `os` string COMMENT '操作系统',
    `user_id` string COMMENT '会员id',
    `version_code` string COMMENT 'app版本号',
    `during_time` bigint COMMENT '持续时间毫秒',
    `page_item` string COMMENT '目标id ',
    `page_item_type` string COMMENT '目标类型',
    `last_page_id` string COMMENT '上页类型',
    `page_id` string COMMENT '页面ID ',
    `source_type` string COMMENT '来源类型',
    `ts` bigint
) COMMENT '页面日志表'
PARTITIONED BY (dt string)
stored as parquet
LOCATION '/warehouse/gmall/dwd/dwd_page_log'
TBLPROPERTIES('parquet.compression'='lzo');

-- 4.1.5 动作日志表
drop table if exists dwd_action_log;
CREATE EXTERNAL TABLE dwd_action_log
(
    `area_code`      string COMMENT '地区编码',
    `brand`          string COMMENT '手机品牌',
    `channel`        string COMMENT '渠道',
    `model`          string COMMENT '手机型号',
    `mid_id`         string COMMENT '设备id',
    `os`             string COMMENT '操作系统',
    `user_id`        string COMMENT '会员id',
    `version_code`   string COMMENT 'app版本号',
    `during_time`    bigint COMMENT '持续时间毫秒',
    `page_item`      string COMMENT '目标id ',
    `page_item_type` string COMMENT '目标类型',
    `last_page_id`   string COMMENT '上页类型',
    `page_id`        string COMMENT '页面id ',
    `source_type`    string COMMENT '来源类型',
    `action_id`      string COMMENT '动作id',
    `item`           string COMMENT '目标id ',
    `item_type`      string COMMENT '目标类型',
    `ts`             bigint COMMENT '时间'
) COMMENT '动作日志表'
    PARTITIONED BY (dt string)
    stored as parquet
    LOCATION '/warehouse/gmall/dwd/dwd_action_log'
    TBLPROPERTIES ('parquet.compression' = 'lzo');

drop function if exists explode_json_array;
create function explode_json_array
    as 'com.atguigu.hive.udtf.ExplodeJSONArray'
    using jar 'hdfs://node1.hdp:8020/user/hive/jars/hivefunction-1.0-SNAPSHOT.jar';

-- 4.1.6 曝光日志表
drop table if exists dwd_display_log;
CREATE EXTERNAL TABLE dwd_display_log(
    `area_code` string COMMENT '地区编码',
    `brand` string COMMENT '手机品牌',
    `channel` string COMMENT '渠道',
    `model` string COMMENT '手机型号',
    `mid_id` string COMMENT '设备id',
    `os` string COMMENT '操作系统',
    `user_id` string COMMENT '会员id',
    `version_code` string COMMENT 'app版本号',
    `during_time` bigint COMMENT 'app版本号',
    `page_item` string COMMENT '目标id ',
    `page_item_type` string COMMENT '目标类型',
    `last_page_id` string COMMENT '上页类型',
    `page_id` string COMMENT '页面ID ',
    `source_type` string COMMENT '来源类型',
    `ts` bigint COMMENT 'app版本号',
    `display_type` string COMMENT '曝光类型',
    `item` string COMMENT '曝光对象id ',
    `item_type` string COMMENT 'app版本号',
    `order` bigint COMMENT '出现顺序'
) COMMENT '曝光日志表'
PARTITIONED BY (dt string)
stored as parquet
LOCATION '/warehouse/gmall/dwd/dwd_display_log'
TBLPROPERTIES('parquet.compression'='lzo');

-- 4.1.7 错误日志表
drop table if exists dwd_error_log;
CREATE EXTERNAL TABLE dwd_error_log(
    `area_code` string COMMENT '地区编码',
    `brand` string COMMENT '手机品牌',
    `channel` string COMMENT '渠道',
    `model` string COMMENT '手机型号',
    `mid_id` string COMMENT '设备id',
    `os` string COMMENT '操作系统',
    `user_id` string COMMENT '会员id',
    `version_code` string COMMENT 'app版本号',
    `page_item` string COMMENT '目标id ',
    `page_item_type` string COMMENT '目标类型',
    `last_page_id` string COMMENT '上页类型',
    `page_id` string COMMENT '页面ID ',
    `source_type` string COMMENT '来源类型',
    `entry` string COMMENT ' icon手机图标  notice 通知 install 安装后启动',
    `loading_time` string COMMENT '启动加载时间',
    `open_ad_id` string COMMENT '广告页ID ',
    `open_ad_ms` string COMMENT '广告总共播放时间',
    `open_ad_skip_ms` string COMMENT '用户跳过广告时点',
    `actions` string COMMENT '动作',
    `displays` string COMMENT '曝光',
    `ts` string COMMENT '时间',
    `error_code` string COMMENT '错误码',
    `msg` string COMMENT '错误信息'
) COMMENT '错误日志表'
PARTITIONED BY (dt string)
stored as parquet
LOCATION '/warehouse/gmall/dwd/dwd_error_log'
TBLPROPERTIES('parquet.compression'='lzo');
