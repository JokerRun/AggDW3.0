-- 1.商品维度表
-- insert into dwd_dim_sku_info partition (dt= ${loaddate} )
select sku.id          as id,
       sku.spu_id      as spu_id,
       sku.price       as price,
       sku.sku_name    as sku_name,
       sku.sku_desc    as sku_desc,
       sku.weight      as weight,
       sku.tm_id       as tm_id,
       tm.tm_name      as tm_name,
       c3.id           as category3_id,
       c2.id           as category2_id,
       c1.id           as category1_id,
       c3.name         as category3_name,
       c2.name         as category2_name,
       c1.name         as category1_name,
       spu.spu_name    as spu_name,
       sku.create_time as create_time
from ods_sku_info sku
         join ods_spu_info spu on sku.spu_id = spu.id
         join ods_base_trademark tm on sku.tm_id = tm.tm_id
         join ods_base_category3 c3 on sku.category3_id = c3.id
         join ods_base_category2 c2 on c3.category2_id = c2.id
         join ods_base_category1 c1 on c2.category1_id = c1.id
where sku.dt = ${loaddate}
;