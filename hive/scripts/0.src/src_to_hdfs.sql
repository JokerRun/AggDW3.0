select id,
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
where (date_format(create_time, '%Y-%m-%d') = ${do_date}
    or date_format(operate_time, '%Y-%m-%d') = ${do_date});
select id,
       coupon_id,
       user_id,
       order_id,
       coupon_status,
       get_time,
       using_time,
       used_time
from coupon_use
where (date_format(get_time, '%Y-%m-%d') = ${do_date}
    or date_format(using_time, '%Y-%m-%d') = ${do_date}
    or date_format(used_time, '%Y-%m-%d') = ${do_date});
select id,
       order_id,
       order_status,
       operate_time
from order_status_log
where date_format(operate_time, '%Y-%m-%d') = ${do_date};
select id,
       activity_id,
       order_id,
       create_time
from activity_order
where date_format(create_time, '%Y-%m-%d') = ${do_date};
select id,
       name,
       birthday,
       gender,
       email,
       user_level,
       create_time,
       operate_time
from user_info
where (DATE_FORMAT(create_time, '%Y-%m-%d') = ${do_date}
    or DATE_FORMAT(operate_time, '%Y-%m-%d') = ${do_date});
select od.id,
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
              on od.order_id = oi.id
where DATE_FORMAT(od.create_time, '%Y-%m-%d') = ${do_date};
select id,
       out_trade_no,
       order_id,
       user_id,
       alipay_trade_no,
       total_amount,
       subject,
       payment_type,
       payment_time
from payment_info
where DATE_FORMAT(payment_time, '%Y-%m-%d') = ${do_date};
select id,
       user_id,
       sku_id,
       spu_id,
       order_id,
       appraise,
       comment_txt,
       create_time
from comment_info
where date_format(create_time, '%Y-%m-%d') = ${do_date};
select id,
       user_id,
       order_id,
       sku_id,
       refund_type,
       refund_num,
       refund_amount,
       refund_reason_type,
       create_time
from order_refund_info
where date_format(create_time, '%Y-%m-%d') = ${do_date};
select id,
       spu_id,
       price,
       sku_name,
       sku_desc,
       weight,
       tm_id,
       category3_id,
       create_time
from sku_info
where 1 = 1;
select id,
       name
from base_category1
where 1 = 1;
select id,
       name,
       category1_id
from base_category2
where 1 = 1;
select id,
       name,
       category2_id
from base_category3
where 1 = 1;
select id,
       name,
       region_id,
       area_code,
       iso_code
from base_province
where 1 = 1;
select id,
       region_name
from base_region
where 1 = 1;
select tm_id,
       tm_name
from base_trademark
where 1 = 1;
select id,
       spu_name,
       category3_id,
       tm_id
from spu_info
where 1 = 1;
select id,
       user_id,
       sku_id,
       spu_id,
       is_cancel,
       create_time,
       cancel_time
from favor_info
where 1 = 1;
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
from cart_info
where 1 = 1;
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
from coupon_info
where 1 = 1;
select id,
       activity_name,
       activity_type,
       start_time,
       end_time,
       create_time
from activity_info
where 1 = 1;
select id,
       activity_id,
       condition_amount,
       condition_num,
       benefit_amount,
       benefit_discount,
       benefit_level
from activity_rule
where 1 = 1;
select dic_code,
       dic_name,
       parent_code,
       create_time,
       operate_time
from base_dic
where 1 = 1
;