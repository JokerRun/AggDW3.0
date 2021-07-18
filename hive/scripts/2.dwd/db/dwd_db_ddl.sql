-- 说明：数据采用parquet存储方式，是可以支持切片的，不需要再对数据创建索引。
-- 如果单纯的text方式存储数据，需要采用支持切片的，lzop压缩方式并创建索引。

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
