-- 4.4.1 商品维度表（全量）
DROP TABLE IF EXISTS `dwd_dim_sku_info`;
CREATE EXTERNAL TABLE `dwd_dim_sku_info`
(
    `id`             string COMMENT '商品id',
    `spu_id`         string COMMENT 'spuid',
    `price`          decimal(16, 2) COMMENT '商品价格',
    `sku_name`       string COMMENT '商品名称',
    `sku_desc`       string COMMENT '商品描述',
    `weight`         decimal(16, 2) COMMENT '重量',
    `tm_id`          string COMMENT '品牌id',
    `tm_name`        string COMMENT '品牌名称',
    `category3_id`   string COMMENT '三级分类id',
    `category2_id`   string COMMENT '二级分类id',
    `category1_id`   string COMMENT '一级分类id',
    `category3_name` string COMMENT '三级分类名称',
    `category2_name` string COMMENT '二级分类名称',
    `category1_name` string COMMENT '一级分类名称',
    `spu_name`       string COMMENT 'spu名称',
    `create_time`    string COMMENT '创建时间'
) COMMENT '商品维度表'
    PARTITIONED BY (`dt` string)
    stored as parquet
    location '/warehouse/gmall/dwd/dwd_dim_sku_info/'
    tblproperties ("parquet.compression" = "lzo");
SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table dwd_dim_sku_info partition (dt = '2020-06-14')
select sku.id,
       sku.spu_id,
       sku.price,
       sku.sku_name,
       sku.sku_desc,
       sku.weight,
       sku.tm_id,
       ob.tm_name,
       sku.category3_id,
       c2.id   category2_id,
       c1.id   category1_id,
       c3.name category3_name,
       c2.name category2_name,
       c1.name category1_name,
       spu.spu_name,
       sku.create_time
from (
         select *
         from ods_sku_info
         where dt = '2020-06-14'
     ) sku
         join
     (
         select *
         from ods_base_trademark
         where dt = '2020-06-14'
     ) ob on sku.tm_id = ob.tm_id
         join
     (
         select *
         from ods_spu_info
         where dt = '2020-06-14'
     ) spu on spu.id = sku.spu_id
         join
     (
         select *
         from ods_base_category3
         where dt = '2020-06-14'
     ) c3 on sku.category3_id = c3.id
         join
     (
         select *
         from ods_base_category2
         where dt = '2020-06-14'
     ) c2 on c3.category2_id = c2.id
         join
     (
         select *
         from ods_base_category1
         where dt = '2020-06-14'
     ) c1 on c2.category1_id = c1.id;
-- 4.4.2 优惠券维度表（全量）
-- 把ODS层ods_coupon_info表数据导入到DWD层优惠卷维度表，在导入过程中可以做适当的清洗。
drop table if exists dwd_dim_coupon_info;
create external table dwd_dim_coupon_info
(
    `id`               string COMMENT '购物券编号',
    `coupon_name`      string COMMENT '购物券名称',
    `coupon_type`      string COMMENT '购物券类型 1 现金券 2 折扣券 3 满减券 4 满件打折券',
    `condition_amount` decimal(16, 2) COMMENT '满额数',
    `condition_num`    bigint COMMENT '满件数',
    `activity_id`      string COMMENT '活动编号',
    `benefit_amount`   decimal(16, 2) COMMENT '减金额',
    `benefit_discount` decimal(16, 2) COMMENT '折扣',
    `create_time`      string COMMENT '创建时间',
    `range_type`       string COMMENT '范围类型 1、商品 2、品类 3、品牌',
    `spu_id`           string COMMENT '商品id',
    `tm_id`            string COMMENT '品牌id',
    `category3_id`     string COMMENT '品类id',
    `limit_num`        bigint COMMENT '最多领用次数',
    `operate_time`     string COMMENT '修改时间',
    `expire_time`      string COMMENT '过期时间'
) COMMENT '优惠券维度表'
    PARTITIONED BY (`dt` string)
    stored as parquet
    location '/warehouse/gmall/dwd/dwd_dim_coupon_info/'
    tblproperties ("parquet.compression" = "lzo");
SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table dwd_dim_coupon_info partition (dt = '2020-06-14')
select id,
       coupon_name,
       coupon_type,
       condition_amount,
       condition_num,
       activity_id,
       benefit_amount,
       benefit_discount,
       create_time,
       range_type,
       spu_id,
       tm_id,
       category3_id,
       limit_num,
       operate_time,
       expire_time
from ods_coupon_info
where dt = '2020-06-14';


-- 4.4.3 活动维度表（全量）
drop table if exists dwd_dim_activity_info;
create external table dwd_dim_activity_info
(
    `id`            string COMMENT '编号',
    `activity_name` string COMMENT '活动名称',
    `activity_type` string COMMENT '活动类型',
    `start_time`    string COMMENT '开始时间',
    `end_time`      string COMMENT '结束时间',
    `create_time`   string COMMENT '创建时间'
) COMMENT '活动信息表'
    PARTITIONED BY (`dt` string)
    stored as parquet
    location '/warehouse/gmall/dwd/dwd_dim_activity_info/'
    tblproperties ("parquet.compression" = "lzo");
SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table dwd_dim_activity_info partition (dt = '2020-06-14')
select id,
       activity_name,
       activity_type,
       start_time,
       end_time,
       create_time
from ods_activity_info
where dt = '2020-06-14';
-- 4.4.4 地区维度表（特殊）
DROP TABLE IF EXISTS `dwd_dim_base_province`;
CREATE EXTERNAL TABLE `dwd_dim_base_province`
(
    `id`            string COMMENT 'id',
    `province_name` string COMMENT '省市名称',
    `area_code`     string COMMENT '地区编码',
    `iso_code`      string COMMENT 'ISO编码',
    `region_id`     string COMMENT '地区id',
    `region_name`   string COMMENT '地区名称'
) COMMENT '地区维度表'
    stored as parquet
    location '/warehouse/gmall/dwd/dwd_dim_base_province/'
    tblproperties ("parquet.compression" = "lzo");
SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table dwd_dim_base_province
select bp.id,
       bp.name,
       bp.area_code,
       bp.iso_code,
       bp.region_id,
       br.region_name
from (
         select *
         from ods_base_province
     ) bp
         join
     (
         select *
         from ods_base_region
     ) br
     on bp.region_id = br.id;
-- 4.4.5 时间维度表（特殊）
DROP TABLE IF EXISTS `dwd_dim_date_info`;
CREATE EXTERNAL TABLE `dwd_dim_date_info`
(
    `date_id`    string COMMENT '日',
    `week_id`    string COMMENT '周',
    `week_day`   string COMMENT '周的第几天',
    `day`        string COMMENT '每月的第几天',
    `month`      string COMMENT '第几月',
    `quarter`    string COMMENT '第几季度',
    `year`       string COMMENT '年',
    `is_workday` string COMMENT '是否是周末',
    `holiday_id` string COMMENT '是否是节假日'
) COMMENT '时间维度表'
    stored as parquet
    location '/warehouse/gmall/dwd/dwd_dim_date_info/'
    tblproperties ("parquet.compression" = "lzo");
-- 2）把date_info.txt文件上传到hadoop102的/opt/module/db_log/路径
-- 3）数据装载 注意：由于dwd_dim_date_info是列式存储+LZO压缩。直接将date_info.txt文件导入到目标表，并不会直接转换为列式存储+LZO压缩。我们需要创建一张普通的临时表dwd_dim_date_info_tmp，将date_info.txt加载到该临时表中。最后通过查询临时表数据，把查询到的数据插入到最终的目标表中。
-- （1）创建临时表，非列式存储
DROP TABLE IF EXISTS `dwd_dim_date_info_tmp`;
CREATE EXTERNAL TABLE `dwd_dim_date_info_tmp`
(
    `date_id`    string COMMENT '日',
    `week_id`    string COMMENT '周',
    `week_day`   string COMMENT '周的第几天',
    `day`        string COMMENT '每月的第几天',
    `month`      string COMMENT '第几月',
    `quarter`    string COMMENT '第几季度',
    `year`       string COMMENT '年',
    `is_workday` string COMMENT '是否是周末',
    `holiday_id` string COMMENT '是否是节假日'
) COMMENT '时间临时表'
    row format delimited fields terminated by '\t'
    location '/warehouse/gmall/dwd/dwd_dim_date_info_tmp/';
-- （2）将数据导入临时表
load data local inpath '/opt/module/db_log/date_info.txt' into table dwd_dim_date_info_tmp;
-- （3）将数据导入正式表
insert overwrite table dwd_dim_date_info
select *
from dwd_dim_date_info_tmp;
-- 4）查询加载结果


-- 4.4.6 支付事实表（事务型事实表）
drop table if exists dwd_fact_payment_info;
create external table dwd_fact_payment_info
(
    `id`              string COMMENT 'id',
    `out_trade_no`    string COMMENT '对外业务编号',
    `order_id`        string COMMENT '订单编号',
    `user_id`         string COMMENT '用户编号',
    `alipay_trade_no` string COMMENT '支付宝交易流水编号',
    `payment_amount`  decimal(16, 2) COMMENT '支付金额',
    `subject`         string COMMENT '交易内容',
    `payment_type`    string COMMENT '支付类型',
    `payment_time`    string COMMENT '支付时间',
    `province_id`     string COMMENT '省份ID'
) COMMENT '支付事实表表'
    PARTITIONED BY (`dt` string)
    stored as parquet
    location '/warehouse/gmall/dwd/dwd_fact_payment_info/'
    tblproperties ("parquet.compression" = "lzo");
SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table dwd_fact_payment_info partition (dt = '2020-06-14')
select pi.id,
       pi.out_trade_no,
       pi.order_id,
       pi.user_id,
       pi.alipay_trade_no,
       pi.total_amount,
       pi.subject,
       pi.payment_type,
       pi.payment_time,
       oi.province_id
from (
         select *
         from ods_payment_info
         where dt = '2020-06-14'
     ) pi
         join
     (
         select id, province_id
         from ods_order_info
         where dt = '2020-06-14'
     ) oi
     on pi.order_id = oi.id;
-- 4.4.7 退款事实表（事务型事实表）
drop table if exists dwd_fact_order_refund_info;
create external table dwd_fact_order_refund_info
(
    `id`                 string COMMENT '编号',
    `user_id`            string COMMENT '用户ID',
    `order_id`           string COMMENT '订单ID',
    `sku_id`             string COMMENT '商品ID',
    `refund_type`        string COMMENT '退款类型',
    `refund_num`         bigint COMMENT '退款件数',
    `refund_amount`      decimal(16, 2) COMMENT '退款金额',
    `refund_reason_type` string COMMENT '退款原因类型',
    `create_time`        string COMMENT '退款时间'
) COMMENT '退款事实表'
    PARTITIONED BY (`dt` string)
    stored as parquet
    location '/warehouse/gmall/dwd/dwd_fact_order_refund_info/'
    tblproperties ("parquet.compression" = "lzo");
SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table dwd_fact_order_refund_info partition (dt = '2020-06-14')
select id,
       user_id,
       order_id,
       sku_id,
       refund_type,
       refund_num,
       refund_amount,
       refund_reason_type,
       create_time
from ods_order_refund_info
where dt = '2020-06-14';
-- 4.4.8 评价事实表（事务型事实表）
drop table if exists dwd_fact_comment_info;
create external table dwd_fact_comment_info
(
    `id`          string COMMENT '编号',
    `user_id`     string COMMENT '用户ID',
    `sku_id`      string COMMENT '商品sku',
    `spu_id`      string COMMENT '商品spu',
    `order_id`    string COMMENT '订单ID',
    `appraise`    string COMMENT '评价',
    `create_time` string COMMENT '评价时间'
) COMMENT '评价事实表'
    PARTITIONED BY (`dt` string)
    stored as parquet
    location '/warehouse/gmall/dwd/dwd_fact_comment_info/'
    tblproperties ("parquet.compression" = "lzo");
SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table dwd_fact_comment_info partition (dt = '2020-06-14')
select id,
       user_id,
       sku_id,
       spu_id,
       order_id,
       appraise,
       create_time
from ods_comment_info
where dt = '2020-06-14';
-- 4.4.9 订单明细事实表（事务型事实表）
drop table if exists dwd_fact_order_detail;
create external table dwd_fact_order_detail
(
    `id`                      string COMMENT '订单编号',
    `order_id`                string COMMENT '订单号',
    `user_id`                 string COMMENT '用户id',
    `sku_id`                  string COMMENT 'sku商品id',
    `sku_name`                string COMMENT '商品名称',
    `order_price`             decimal(16, 2) COMMENT '商品价格',
    `sku_num`                 bigint COMMENT '商品数量',
    `create_time`             string COMMENT '创建时间',
    `province_id`             string COMMENT '省份ID',
    `source_type`             string COMMENT '来源类型',
    `source_id`               string COMMENT '来源编号',
    `original_amount_d`       decimal(20, 2) COMMENT '原始价格分摊',
    `final_amount_d`          decimal(20, 2) COMMENT '购买价格分摊',
    `feight_fee_d`            decimal(20, 2) COMMENT '分摊运费',
    `benefit_reduce_amount_d` decimal(20, 2) COMMENT '分摊优惠'
) COMMENT '订单明细事实表表'
    PARTITIONED BY (`dt` string)
    stored as parquet
    location '/warehouse/gmall/dwd/dwd_fact_order_detail/'
    tblproperties ("parquet.compression" = "lzo");
SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table dwd_fact_order_detail partition (dt = '2020-06-14')
select id,
       order_id,
       user_id,
       sku_id,
       sku_name,
       order_price,
       sku_num,
       create_time,
       province_id,
       source_type,
       source_id,
       original_amount_d,
       if(rn = 1, final_total_amount - (sum_div_final_amount - final_amount_d), final_amount_d),
       if(rn = 1, feight_fee - (sum_div_feight_fee - feight_fee_d), feight_fee_d),
       if(rn = 1, benefit_reduce_amount - (sum_div_benefit_reduce_amount - benefit_reduce_amount_d),
          benefit_reduce_amount_d)
from (
         select od.id,
                od.order_id,
                od.user_id,
                od.sku_id,
                od.sku_name,
                od.order_price,
                od.sku_num,
                od.create_time,
                oi.province_id,
                od.source_type,
                od.source_id,
                round(od.order_price * od.sku_num, 2)                                                    original_amount_d,
                round(od.order_price * od.sku_num / oi.original_total_amount * oi.final_total_amount, 2) final_amount_d,
                round(od.order_price * od.sku_num / oi.original_total_amount * oi.feight_fee, 2)         feight_fee_d,
                round(od.order_price * od.sku_num / oi.original_total_amount * oi.benefit_reduce_amount,
                      2)                                                                                 benefit_reduce_amount_d,
                row_number() over (partition by od.order_id order by od.id desc)                         rn,
                oi.final_total_amount,
                oi.feight_fee,
                oi.benefit_reduce_amount,
                sum(round(od.order_price * od.sku_num / oi.original_total_amount * oi.final_total_amount, 2))
                    over (partition by od.order_id)                                                      sum_div_final_amount,
                sum(round(od.order_price * od.sku_num / oi.original_total_amount * oi.feight_fee, 2))
                    over (partition by od.order_id)                                                      sum_div_feight_fee,
                sum(round(od.order_price * od.sku_num / oi.original_total_amount * oi.benefit_reduce_amount, 2))
                    over (partition by od.order_id)                                                      sum_div_benefit_reduce_amount
         from (
                  select *
                  from ods_order_detail
                  where dt = '2020-06-14'
              ) od
                  join
              (
                  select *
                  from ods_order_info
                  where dt = '2020-06-14'
              ) oi
              on od.order_id = oi.id
     ) t1;

-- 4.4.10 加购事实表（周期型快照事实表，每日快照）
drop table if exists dwd_fact_cart_info;
create external table dwd_fact_cart_info
(
    `id`           string COMMENT '编号',
    `user_id`      string COMMENT '用户id',
    `sku_id`       string COMMENT 'skuid',
    `cart_price`   string COMMENT '放入购物车时价格',
    `sku_num`      string COMMENT '数量',
    `sku_name`     string COMMENT 'sku名称 (冗余)',
    `create_time`  string COMMENT '创建时间',
    `operate_time` string COMMENT '修改时间',
    `is_ordered`   string COMMENT '是否已经下单。1为已下单;0为未下单',
    `order_time`   string COMMENT '下单时间',
    `source_type`  string COMMENT '来源类型',
    `srouce_id`    string COMMENT '来源编号'
) COMMENT '加购事实表'
    PARTITIONED BY (`dt` string)
    stored as parquet
    location '/warehouse/gmall/dwd/dwd_fact_cart_info/'
    tblproperties ("parquet.compression" = "lzo");
SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table dwd_fact_cart_info partition (dt = '2020-06-14')
select id,
       user_id,
       sku_id,
       cart_price,
       sku_num,
       sku_name,
       create_time,
       operate_time,
       is_ordered,
       order_time,
       source_type,
       source_id
from ods_cart_info
where dt = '2020-06-14';


-- 4.4.11 收藏事实表（周期型快照事实表，每日快照）
drop table if exists dwd_fact_favor_info;
create external table dwd_fact_favor_info
(
    `id`          string COMMENT '编号',
    `user_id`     string COMMENT '用户id',
    `sku_id`      string COMMENT 'skuid',
    `spu_id`      string COMMENT 'spuid',
    `is_cancel`   string COMMENT '是否取消',
    `create_time` string COMMENT '收藏时间',
    `cancel_time` string COMMENT '取消时间'
) COMMENT '收藏事实表'
    PARTITIONED BY (`dt` string)
    stored as parquet
    location '/warehouse/gmall/dwd/dwd_fact_favor_info/'
    tblproperties ("parquet.compression" = "lzo");
SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table dwd_fact_favor_info partition (dt = '2020-06-14')
select id,
       user_id,
       sku_id,
       spu_id,
       is_cancel,
       create_time,
       cancel_time
from ods_favor_info
where dt = '2020-06-14';
-- 4.4.12 优惠券领用事实表（累积型快照事实表）
drop table if exists dwd_fact_coupon_use;
create external table dwd_fact_coupon_use
(
    `id`            string COMMENT '编号',
    `coupon_id`     string COMMENT '优惠券ID',
    `user_id`       string COMMENT 'userid',
    `order_id`      string COMMENT '订单id',
    `coupon_status` string COMMENT '优惠券状态',
    `get_time`      string COMMENT '领取时间',
    `using_time`    string COMMENT '使用时间(下单)',
    `used_time`     string COMMENT '使用时间(支付)'
) COMMENT '优惠券领用事实表'
    PARTITIONED BY (`dt` string)
    stored as parquet
    location '/warehouse/gmall/dwd/dwd_fact_coupon_use/'
    tblproperties ("parquet.compression" = "lzo");
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table dwd_fact_coupon_use partition (dt)
select if(new.id is null, old.id, new.id),
       if(new.coupon_id is null, old.coupon_id, new.coupon_id),
       if(new.user_id is null, old.user_id, new.user_id),
       if(new.order_id is null, old.order_id, new.order_id),
       if(new.coupon_status is null, old.coupon_status, new.coupon_status),
       if(new.get_time is null, old.get_time, new.get_time),
       if(new.using_time is null, old.using_time, new.using_time),
       if(new.used_time is null, old.used_time, new.used_time),
       date_format(if(new.get_time is null, old.get_time, new.get_time), 'yyyy-MM-dd')
from (
         select id,
                coupon_id,
                user_id,
                order_id,
                coupon_status,
                get_time,
                using_time,
                used_time
         from dwd_fact_coupon_use
         where dt in
               (
                   select date_format(get_time, 'yyyy-MM-dd')
                   from ods_coupon_use
                   where dt = '2020-06-14'
               )
     ) old
         full outer join
     (
         select id,
                coupon_id,
                user_id,
                order_id,
                coupon_status,
                get_time,
                using_time,
                used_time
         from ods_coupon_use
         where dt = '2020-06-14'
     ) new
     on old.id = new.id;
-- 4.4.13 系统函数（concat、concat_ws、collect_set、STR_TO_MAP）
-- 1）concat函数
-- concat函数在连接字符串的时候，只要其中一个是NULL，那么将返回NULL
select concat('a', 'b');
select concat('a', 'b', null);
-- 2）concat_ws函数
select concat_ws('-', 'a', 'b');
select concat_ws('-', 'a', 'b', null);
select concat_ws('', 'a', 'b', null);
-- 3）collect_set函数
drop table if exists stud;
create table stud
(
    name   string,
    area   string,
    course string,
    score  int
);
insert into table stud
values ('zhang3', 'bj', 'math', 88);
insert into table stud
values ('li4', 'bj', 'math', 99);
insert into table stud
values ('wang5', 'sh', 'chinese', 92);
insert into table stud
values ('zhao6', 'sh', 'chinese', 54);
insert into table stud
values ('tian7', 'bj', 'chinese', 91);
select course, collect_set(area), avg(score)
from stud
group by course;
select course, collect_set(area)[0], avg(score)
from stud
group by course;
-- 3）STR_TO_MAP函数
-- select * str_to_map('1001=2020-06-14,1002=2020-06-14',  ','  ,  '=');
-- {"1001":"2020-06-14","1002":"2020-06-14"}

-- 4.4.14 订单事实表（累积型快照事实表）
drop table if exists dwd_fact_order_info;
create external table dwd_fact_order_info
(
    `id`                    string COMMENT '订单编号',
    `order_status`          string COMMENT '订单状态',
    `user_id`               string COMMENT '用户id',
    `out_trade_no`          string COMMENT '支付流水号',
    `create_time`           string COMMENT '创建时间(未支付状态)',
    `payment_time`          string COMMENT '支付时间(已支付状态)',
    `cancel_time`           string COMMENT '取消时间(已取消状态)',
    `finish_time`           string COMMENT '完成时间(已完成状态)',
    `refund_time`           string COMMENT '退款时间(退款中状态)',
    `refund_finish_time`    string COMMENT '退款完成时间(退款完成状态)',
    `province_id`           string COMMENT '省份ID',
    `activity_id`           string COMMENT '活动ID',
    `original_total_amount` decimal(16, 2) COMMENT '原价金额',
    `benefit_reduce_amount` decimal(16, 2) COMMENT '优惠金额',
    `feight_fee`            decimal(16, 2) COMMENT '运费',
    `final_total_amount`    decimal(16, 2) COMMENT '订单金额'
) COMMENT '订单事实表'
    PARTITIONED BY (`dt` string)
    stored as parquet
    location '/warehouse/gmall/dwd/dwd_fact_order_info/'
    tblproperties ("parquet.compression" = "lzo");
select order_id, concat(order_status, '=', operate_time)
from ods_order_status_log
where dt = '2020-06-14';
select order_id, collect_set(concat(order_status, '=', operate_time))
from ods_order_status_log
where dt = '2020-06-14'
group by order_id;
select order_id, concat_ws(',', collect_set(concat(order_status, '=', operate_time)))
from ods_order_status_log
where dt = '2020-06-14'
group by order_id;
select order_id, str_to_map(concat_ws(',', collect_set(concat(order_status, '=', operate_time))), ',', '=')
from ods_order_status_log
where dt = '2020-06-14'
group by order_id;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table dwd_fact_order_info partition (dt)
select if(new.id is null, old.id, new.id),
       if(new.order_status is null, old.order_status, new.order_status),
       if(new.user_id is null, old.user_id, new.user_id),
       if(new.out_trade_no is null, old.out_trade_no, new.out_trade_no),
       if(new.tms['1001'] is null, old.create_time, new.tms['1001']),--1001对应未支付状态
       if(new.tms['1002'] is null, old.payment_time, new.tms['1002']),
       if(new.tms['1003'] is null, old.cancel_time, new.tms['1003']),
       if(new.tms['1004'] is null, old.finish_time, new.tms['1004']),
       if(new.tms['1005'] is null, old.refund_time, new.tms['1005']),
       if(new.tms['1006'] is null, old.refund_finish_time, new.tms['1006']),
       if(new.province_id is null, old.province_id, new.province_id),
       if(new.activity_id is null, old.activity_id, new.activity_id),
       if(new.original_total_amount is null, old.original_total_amount, new.original_total_amount),
       if(new.benefit_reduce_amount is null, old.benefit_reduce_amount, new.benefit_reduce_amount),
       if(new.feight_fee is null, old.feight_fee, new.feight_fee),
       if(new.final_total_amount is null, old.final_total_amount, new.final_total_amount),
       date_format(if(new.tms['1001'] is null, old.create_time, new.tms['1001']), 'yyyy-MM-dd')
from (
         select id,
                order_status,
                user_id,
                out_trade_no,
                create_time,
                payment_time,
                cancel_time,
                finish_time,
                refund_time,
                refund_finish_time,
                province_id,
                activity_id,
                original_total_amount,
                benefit_reduce_amount,
                feight_fee,
                final_total_amount
         from dwd_fact_order_info
         where dt
                   in
               (
                   select date_format(create_time, 'yyyy-MM-dd')
                   from ods_order_info
                   where dt = '2020-06-14'
               )
     ) old
         full outer join
     (
         select info.id,
                info.order_status,
                info.user_id,
                info.out_trade_no,
                info.province_id,
                act.activity_id,
                log.tms,
                info.original_total_amount,
                info.benefit_reduce_amount,
                info.feight_fee,
                info.final_total_amount
         from (
                  select order_id,
                         str_to_map(concat_ws(',', collect_set(concat(order_status, '=', operate_time))), ',', '=') tms
                  from ods_order_status_log
                  where dt = '2020-06-14'
                  group by order_id
              ) log
                  join
              (
                  select *
                  from ods_order_info
                  where dt = '2020-06-14'
              ) info
              on log.order_id = info.id
                  left join
              (
                  select *
                  from ods_activity_order
                  where dt = '2020-06-14'
              ) act
              on log.order_id = act.order_id
     ) new
     on old.id = new.id;


-- 4.4.15 用户维度表（拉链表）
-- 拉链表制作过程
-- 步骤0：初始化拉链表（首次独立执行）
-- （1）建立拉链表
drop table if exists dwd_dim_user_info_his;
create external table dwd_dim_user_info_his
(
    `id`           string COMMENT '用户id',
    `name`         string COMMENT '姓名',
    `birthday`     string COMMENT '生日',
    `gender`       string COMMENT '性别',
    `email`        string COMMENT '邮箱',
    `user_level`   string COMMENT '用户等级',
    `create_time`  string COMMENT '创建时间',
    `operate_time` string COMMENT '操作时间',
    `start_date`   string COMMENT '有效开始日期',
    `end_date`     string COMMENT '有效结束日期'
) COMMENT '用户拉链表'
    stored as parquet
    location '/warehouse/gmall/dwd/dwd_dim_user_info_his/'
    tblproperties ("parquet.compression" = "lzo");
-- （2）初始化拉链表
SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table dwd_dim_user_info_his
select id,
       name,
       birthday,
       gender,
       email,
       user_level,
       create_time,
       operate_time,
       '2020-06-14',
       '9999-99-99'
from ods_user_info oi
where oi.dt = '2020-06-14';

-- 步骤1：制作当日变动数据（包括新增，修改）每日执行
-- （1）如何获得每日变动表
-- a.最好表内有创建时间和变动时间（Lucky!）
-- b.如果没有，可以利用第三方工具监控比如canal，监控MySQL的实时变化进行记录（麻烦）。
-- c.逐行对比前后两天的数据，检查md5(concat(全部有可能变化的字段))是否相同(low)
-- d.要求业务数据库提供变动流水（人品，颜值）
-- （2）因为ods_user_info本身导入过来就是新增变动明细的表，所以不用处理
-- a）数据库中新增2020-06-15一天的数据
-- b）通过Sqoop把2020-06-15日所有数据导入
-- mysql_to_hdfs.sh all 2020-06-15
-- c）ods层数据导入
-- hdfs_to_ods_db.sh all 2020-06-15
-- 步骤2：先合并变动信息，再追加新增信息，插入到临时表中
-- 1）建立临时表
drop table if exists dwd_dim_user_info_his_tmp;
create external table dwd_dim_user_info_his_tmp
(
    `id`           string COMMENT '用户id',
    `name`         string COMMENT '姓名',
    `birthday`     string COMMENT '生日',
    `gender`       string COMMENT '性别',
    `email`        string COMMENT '邮箱',
    `user_level`   string COMMENT '用户等级',
    `create_time`  string COMMENT '创建时间',
    `operate_time` string COMMENT '操作时间',
    `start_date`   string COMMENT '有效开始日期',
    `end_date`     string COMMENT '有效结束日期'
) COMMENT '订单拉链临时表'
    stored as parquet
    location '/warehouse/gmall/dwd/dwd_dim_user_info_his_tmp/'
    tblproperties ("parquet.compression" = "lzo");
-- 2）导入脚本
SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table dwd_dim_user_info_his_tmp
select *
from (
         select id,
                name,
                birthday,
                gender,
                email,
                user_level,
                create_time,
                operate_time,
                '2020-06-15' start_date,
                '9999-99-99' end_date
         from ods_user_info
         where dt = '2020-06-15'
         union all
         select uh.id,
                uh.name,
                uh.birthday,
                uh.gender,
                uh.email,
                uh.user_level,
                uh.create_time,
                uh.operate_time,
                uh.start_date,
                if(ui.id is not null and uh.end_date = '9999-99-99', date_add(ui.dt, -1), uh.end_date) end_date
         from dwd_dim_user_info_his uh
                  left join
              (
                  select *
                  from ods_user_info
                  where dt = '2020-06-15'
              ) ui on uh.id = ui.id
     ) his
order by his.id, start_date;
-- 步骤3：把临时表覆盖给拉链表
-- 1）导入数据
insert overwrite table dwd_dim_user_info_his
select *
from dwd_dim_user_info_his_tmp;
