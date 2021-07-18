use gmall;

load data inpath '/origin_data/$APP/db/order_info/$do_date' OVERWRITE into table ods_order_info partition (dt = '$do_date');

load data inpath '/origin_data/$APP/db/order_detail/$do_date' OVERWRITE into table ods_order_detail partition (dt = '$do_date');

load data inpath '/origin_data/$APP/db/sku_info/$do_date' OVERWRITE into table ods_sku_info partition (dt = '$do_date');

load data inpath '/origin_data/$APP/db/user_info/$do_date' OVERWRITE into table ods_user_info partition (dt = '$do_date');

load data inpath '/origin_data/$APP/db/payment_info/$do_date' OVERWRITE into table ods_payment_info partition (dt = '$do_date');

load data inpath '/origin_data/$APP/db/base_category1/$do_date' OVERWRITE into table ods_base_category1 partition (dt = '$do_date');

load data inpath '/origin_data/$APP/db/base_category2/$do_date' OVERWRITE into table ods_base_category2 partition (dt = '$do_date');

load data inpath '/origin_data/$APP/db/base_category3/$do_date' OVERWRITE into table ods_base_category3 partition (dt = '$do_date');

load data inpath '/origin_data/$APP/db/base_trademark/$do_date' OVERWRITE into table ods_base_trademark partition (dt = '$do_date');

load data inpath '/origin_data/$APP/db/activity_info/$do_date' OVERWRITE into table ods_activity_info partition (dt = '$do_date');

load data inpath '/origin_data/$APP/db/activity_order/$do_date' OVERWRITE into table ods_activity_order partition (dt = '$do_date');

load data inpath '/origin_data/$APP/db/cart_info/$do_date' OVERWRITE into table ods_cart_info partition (dt = '$do_date');

load data inpath '/origin_data/$APP/db/comment_info/$do_date' OVERWRITE into table ods_comment_info partition (dt = '$do_date');

load data inpath '/origin_data/$APP/db/coupon_info/$do_date' OVERWRITE into table ods_coupon_info partition (dt = '$do_date');

load data inpath '/origin_data/$APP/db/coupon_use/$do_date' OVERWRITE into table ods_coupon_use partition (dt = '$do_date');

load data inpath '/origin_data/$APP/db/favor_info/$do_date' OVERWRITE into table ods_favor_info partition (dt = '$do_date');

load data inpath '/origin_data/$APP/db/order_refund_info/$do_date' OVERWRITE into table ods_order_refund_info partition (dt = '$do_date');

load data inpath '/origin_data/$APP/db/order_status_log/$do_date' OVERWRITE into table ods_order_status_log partition (dt = '$do_date');

load data inpath '/origin_data/$APP/db/spu_info/$do_date' OVERWRITE into table ods_spu_info partition (dt = '$do_date');

load data inpath '/origin_data/$APP/db/activity_rule/$do_date' OVERWRITE into table ods_activity_rule partition (dt = '$do_date');

load data inpath '/origin_data/$APP/db/base_dic/$do_date' OVERWRITE into table ods_base_dic partition (dt = '$do_date');
