# MOCK数据流

## 1.日志数据

### 1.1.生成log(指定日期)

日志生成命令`lg.sh`

```bash
#!/bin/bash
for i in node1 node2
do
    echo "========== $i =========="
    ssh $i "cd /opt/module/applog/; java -jar gmall2020-mock-log-2020-05-10.jar >/dev/null 2>&1 &"
done 
```

日志生成参数`/opt/module/applog/application.properties`

```properties
# 外部配置打开
logging.config=./logback.xml
#业务日期
mock.date=2021-05-03

#模拟数据发送模式
mock.type=log
#mock.type=http
#http模式下，发送的地址
mock.url=http://localhost:8080/applog

#启动次数
mock.startup.count=100
#设备最大值
mock.max.mid=50
#会员最大值
mock.max.uid=500
#商品最大值
mock.max.sku-id=10
#页面平均访问时间
mock.page.during-time-ms=20000
#错误概率 百分比
mock.error.rate=3
#每条日志发送延迟 ms
mock.log.sleep=10
#商品详情来源  用户查询，商品推广，智能推荐, 促销活动
mock.detail.source-type-rate=40:25:15:20
```



### 1.2. FLUME后台监听日志并打入KAFKA

Flume 数据源采集配置`file-flume-kafka.conf` 

```properties
#为各组件命名
a1.sources = r1
a1.channels = c1

#描述source
a1.sources.r1.type = TAILDIR
a1.sources.r1.filegroups = f1
a1.sources.r1.filegroups.f1 = /opt/module/applog/log/app.*
a1.sources.r1.positionFile = /opt/module/flume/taildir_position.json
a1.sources.r1.interceptors =  i1
a1.sources.r1.interceptors.i1.type = com.atguigu.flume.interceptor.ETLInterceptor$Builder

#描述channel
a1.channels.c1.type = org.apache.flume.channel.kafka.KafkaChannel
a1.channels.c1.kafka.bootstrap.servers = node1:9092,node2:9092,node3:9092
a1.channels.c1.kafka.topic = topic_log
a1.channels.c1.parseAsFlumeEvent = false

#绑定source和channel以及sink和channel的关系
a1.sources.r1.channels = c1

```

### 1.3.FLUME消费KAFKA数据，写入HDFS

Flume 数据源输出配置`kafka-flume-hdfs.conf` 

```properties
## 组件
a1.sources=r1
a1.channels=c1
a1.sinks=k1
## source1
a1.sources.r1.type = org.apache.flume.source.kafka.KafkaSource
a1.sources.r1.batchSize = 5000
a1.sources.r1.batchDurationMillis = 2000
a1.sources.r1.kafka.bootstrap.servers = node2:9092,node3:9092,node1:9092
a1.sources.r1.kafka.topics=topic_log
a1.sources.r1.interceptors = i1
a1.sources.r1.interceptors.i1.type = com.atguigu.flume.interceptor.TimeStampInterceptor$Builder
## channel1
a1.channels.c1.type = file
a1.channels.c1.checkpointDir = /opt/module/flume/checkpoint/behavior1
a1.channels.c1.dataDirs = /opt/module/flume/data/behavior1/
a1.channels.c1.maxFileSize = 2146435071
a1.channels.c1.capacity = 1000000
a1.channels.c1.keep-alive = 6


## sink1
a1.sinks.k1.type = hdfs
a1.sinks.k1.hdfs.path = /origin_data/gmall/log/topic_log/%Y-%m-%d
# a1.sinks.k1.hdfs.path = hdfs://node1.hdp:8020/origin_data/gmall/log/topic_log/%Y-%m-%d
a1.sinks.k1.hdfs.filePrefix = log-
a1.sinks.k1.hdfs.round = false


a1.sinks.k1.hdfs.rollInterval = 3600
a1.sinks.k1.hdfs.rollSize = 134217728
a1.sinks.k1.hdfs.rollCount = 0

## 控制输出文件是原生文件。
a1.sinks.k1.hdfs.fileType = CompressedStream
a1.sinks.k1.hdfs.codeC = lzop

## 拼装
a1.sources.r1.channels = c1
a1.sinks.k1.channel= c1

```



### 1.4.利用HIVE从HDFS导入HIVE表(指定日期)

`hdfs_to_ods_log.sh`

```sh
#!/bin/bash
# 定义变量方便修改
APP=gmall
hive=/opt/module/hive/bin/hive
hadoop=/opt/module/hadoop-3.1.4/bin/hadoop

# 如果是输入的日期按照取输入日期；如果没输入日期取当前时间的前一天
if [ -n "$1" ] ;then
   do_date=$1
else
   do_date=$(date -d "-1 day" +%F)
fi

echo ================== 日志日期为 "$do_date" ==================
sql="
load data inpath '/origin_data/$APP/log/topic_log/$do_date' into table ${APP}.ods_log partition(dt='$do_date');
"

$hive -e "$sql"

$hadoop jar /opt/module/hadoop-3.1.3/share/hadoop/common/hadoop-lzo-0.4.20.jar com.hadoop.compression.lzo.DistributedLzoIndexer -Dmapreduce.job.queuename=hive /warehouse/$APP/ods/ods_log/dt="$do_date"

```





## 2.业务数据

### 2.1.生成Mysql业务数据(指定日期)

`java -jar /opt/module/db_log/gmall2020-mock-db-2020-05-18.jar`

`/opt/module/db_log/application.properties`

```properties
logging.level.root=info
spring.datasource.driver-class-name=com.mysql.jdbc.Driver
spring.datasource.url=jdbc:mysql://node1.hdp:3306/gmall?characterEncoding=utf-8&useSSL=false&serverTimezone=GMT%2B8
spring.datasource.username=root
spring.datasource.password=000000
logging.pattern.console=%m%n
mybatis-plus.global-config.db-config.field-strategy=not_null
#业务日期
mock.date=2021-05-03
#是否重置
mock.clear=0
#是否重置用户
mock.clear.user=0
#生成新用户数量
mock.user.count=100
#男性比例
mock.user.male-rate=20
#用户数据变化概率
mock.user.update-rate:10

#收藏取消比例
mock.favor.cancel-rate=10
#收藏数量
mock.favor.count=100

#购物车数量
mock.cart.count=300
#每个商品最多购物个数
mock.cart.sku-maxcount-per-cart=3
#购物车来源  用户查询，商品推广，智能推荐, 促销活动
mock.cart.source-type-rate=60:20:10:10

#用户下单比例
mock.order.user-rate=20
#用户从购物中购买商品比例
mock.order.sku-rate=50
#是否参加活动
mock.order.join-activity=1
#是否使用购物券
mock.order.use-coupon=1
#购物券领取人数
mock.coupon.user-count=100

#支付比例
mock.payment.rate=70
#支付方式 支付宝：微信 ：银联
mock.payment.payment-type=30:60:10


#评价比例 好：中：差：自动
mock.comment.appraise-rate=30:10:10:50

#退款原因比例：质量问题 商品描述与实际描述不一致 缺货 号码不合适 拍错 不想买了 其他
mock.refund.reason-rate=30:10:20:5:15:5:5

```



### 2.2.Sqoop批量导入HDFS(指定日期)

`mysql_to_hdfs.sh`

```bash
#!/bin/bash
# 3）初次导入 mysql_to_hdfs.sh first 2020-06-14
# 4）每日导入 mysql_to_hdfs.sh all 2020-06-15


APP=gmall
sqoop=/opt/module/sqoop/bin/sqoop

if [ -n "$2" ] ;then
    do_date=$2
else
    do_date=$(date -d '-1 day' +%F)
fi

import_data(){
$sqoop import \
--connect jdbc:mysql://node1.hdp:3306/$APP \
--username root \
--password 000000 \
--target-dir /origin_data/$APP/db/"$1"/"$do_date" \
--delete-target-dir \
--query "$2 and  \$CONDITIONS" \
--num-mappers 1 \
--fields-terminated-by '\t' \
--compress \
--compression-codec lzop \
--null-string '\\N' \
--null-non-string '\\N'

hadoop jar /opt/module/hadoop-3.1.4/share/hadoop/common/hadoop-lzo-0.4.20.jar com.hadoop.compression.lzo.DistributedLzoIndexer /origin_data/$APP/db/"$1"/"$do_date"
}

import_order_info(){
  import_data order_info "select
                            id, 
                            final_total_amount, 
                            order_status, 
                            user_id, 
                            out_trade_no, 
                            create_time, 
                            operate_time,
                            province_id,
                            benefit_reduce_amount,
                            original_total_amount,
                            feight_fee      
                        from order_info
                        where (date_format(create_time,'%Y-%m-%d')='$do_date' 
                        or date_format(operate_time,'%Y-%m-%d')='$do_date')"
}

import_coupon_use(){
  import_data coupon_use "select
                          id,
                          coupon_id,
                          user_id,
                          order_id,
                          coupon_status,
                          get_time,
                          using_time,
                          used_time
                        from coupon_use
                        where (date_format(get_time,'%Y-%m-%d')='$do_date'
                        or date_format(using_time,'%Y-%m-%d')='$do_date'
                        or date_format(used_time,'%Y-%m-%d')='$do_date')"
}

import_order_status_log(){
  import_data order_status_log "select
                                  id,
                                  order_id,
                                  order_status,
                                  operate_time
                                from order_status_log
                                where date_format(operate_time,'%Y-%m-%d')='$do_date'"
}

import_activity_order(){
  import_data activity_order "select
                                id,
                                activity_id,
                                order_id,
                                create_time
                              from activity_order
                              where date_format(create_time,'%Y-%m-%d')='$do_date'"
}

import_user_info(){
  import_data "user_info" "select 
                            id,
                            name,
                            birthday,
                            gender,
                            email,
                            user_level, 
                            create_time,
                            operate_time
                          from user_info 
                          where (DATE_FORMAT(create_time,'%Y-%m-%d')='$do_date' 
                          or DATE_FORMAT(operate_time,'%Y-%m-%d')='$do_date')"
}

import_order_detail(){
  import_data order_detail "select 
                              od.id,
                              order_id, 
                              user_id, 
                              sku_id,
                              sku_name,
                              order_price,
                              sku_num, 
                              od.create_time,
                              source_type,
                              source_id  
                            from order_detail od
                            join order_info oi
                            on od.order_id=oi.id
                            where DATE_FORMAT(od.create_time,'%Y-%m-%d')='$do_date'"
}

import_payment_info(){
  import_data "payment_info"  "select 
                                id,  
                                out_trade_no, 
                                order_id, 
                                user_id, 
                                alipay_trade_no, 
                                total_amount,  
                                subject, 
                                payment_type, 
                                payment_time 
                              from payment_info 
                              where DATE_FORMAT(payment_time,'%Y-%m-%d')='$do_date'"
}

import_comment_info(){
  import_data comment_info "select
                              id,
                              user_id,
                              sku_id,
                              spu_id,
                              order_id,
                              appraise,
                              comment_txt,
                              create_time
                            from comment_info
                            where date_format(create_time,'%Y-%m-%d')='$do_date'"
}

import_order_refund_info(){
  import_data order_refund_info "select
                                id,
                                user_id,
                                order_id,
                                sku_id,
                                refund_type,
                                refund_num,
                                refund_amount,
                                refund_reason_type,
                                create_time
                              from order_refund_info
                              where date_format(create_time,'%Y-%m-%d')='$do_date'"
}

import_sku_info(){
  import_data sku_info "select 
                          id,
                          spu_id,
                          price,
                          sku_name,
                          sku_desc,
                          weight,
                          tm_id,
                          category3_id,
                          create_time
                        from sku_info where 1=1"
}

import_base_category1(){
  import_data "base_category1" "select 
                                  id,
                                  name 
                                from base_category1 where 1=1"
}

import_base_category2(){
  import_data "base_category2" "select
                                  id,
                                  name,
                                  category1_id 
                                from base_category2 where 1=1"
}

import_base_category3(){
  import_data "base_category3" "select
                                  id,
                                  name,
                                  category2_id
                                from base_category3 where 1=1"
}

import_base_province(){
  import_data base_province "select
                              id,
                              name,
                              region_id,
                              area_code,
                              iso_code
                            from base_province
                            where 1=1"
}

import_base_region(){
  import_data base_region "select
                              id,
                              region_name
                            from base_region
                            where 1=1"
}

import_base_trademark(){
  import_data base_trademark "select
                                tm_id,
                                tm_name
                              from base_trademark
                              where 1=1"
}

import_spu_info(){
  import_data spu_info "select
                            id,
                            spu_name,
                            category3_id,
                            tm_id
                          from spu_info
                          where 1=1"
}

import_favor_info(){
  import_data favor_info "select
                          id,
                          user_id,
                          sku_id,
                          spu_id,
                          is_cancel,
                          create_time,
                          cancel_time
                        from favor_info
                        where 1=1"
}

import_cart_info(){
  import_data cart_info "select
                        id,
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
                      from cart_info
                      where 1=1"
}

import_coupon_info(){
  import_data coupon_info "select
                          id,
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
                        from coupon_info
                        where 1=1"
}

import_activity_info(){
  import_data activity_info "select
                              id,
                              activity_name,
                              activity_type,
                              start_time,
                              end_time,
                              create_time
                            from activity_info
                            where 1=1"
}

import_activity_rule(){
    import_data activity_rule "select
                                    id,
                                    activity_id,
                                    condition_amount,
                                    condition_num,
                                    benefit_amount,
                                    benefit_discount,
                                    benefit_level
                                from activity_rule
                                where 1=1"
}

import_base_dic(){
    import_data base_dic "select
                            dic_code,
                            dic_name,
                            parent_code,
                            create_time,
                            operate_time
                          from base_dic
                          where 1=1"
}

case $1 in
  "order_info")
     import_order_info
;;
  "base_category1")
     import_base_category1
;;
  "base_category2")
     import_base_category2
;;
  "base_category3")
     import_base_category3
;;
  "order_detail")
     import_order_detail
;;
  "sku_info")
     import_sku_info
;;
  "user_info")
     import_user_info
;;
  "payment_info")
     import_payment_info
;;
  "base_province")
     import_base_province
;;
  "base_region")
     import_base_region
;;
  "base_trademark")
     import_base_trademark
;;
  "activity_info")
      import_activity_info
;;
  "activity_order")
      import_activity_order
;;
  "cart_info")
      import_cart_info
;;
  "comment_info")
      import_comment_info
;;
  "coupon_info")
      import_coupon_info
;;
  "coupon_use")
      import_coupon_use
;;
  "favor_info")
      import_favor_info
;;
  "order_refund_info")
      import_order_refund_info
;;
  "order_status_log")
      import_order_status_log
;;
  "spu_info")
      import_spu_info
;;
  "activity_rule")
      import_activity_rule
;;
  "base_dic")
      import_base_dic
;;

"first")
   import_base_category1
   import_base_category2
   import_base_category3
   import_order_info
   import_order_detail
   import_sku_info
   import_user_info
   import_payment_info
   import_base_province
   import_base_region
   import_base_trademark
   import_activity_info
   import_activity_order
   import_cart_info
   import_comment_info
   import_coupon_use
   import_coupon_info
   import_favor_info
   import_order_refund_info
   import_order_status_log
   import_spu_info
   import_activity_rule
   import_base_dic
;;
"all")
   import_base_category1
   import_base_category2
   import_base_category3
   import_order_info
   import_order_detail
   import_sku_info
   import_user_info
   import_payment_info
   import_base_trademark
   import_activity_info
   import_activity_order
   import_cart_info
   import_comment_info
   import_coupon_use
   import_coupon_info
   import_favor_info
   import_order_refund_info
   import_order_status_log
   import_spu_info
   import_activity_rule
   import_base_dic
;;
esac

```

