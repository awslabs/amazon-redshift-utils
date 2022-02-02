/* These are some of aqua eligible test queries*/
select  count(*) from amazon_reviews 
where product_title SIMILAR TO  '%lap%' or
      product_title SIMILAR TO  '%iph%' or 
      product_title SIMILAR TO  '%soa%' or 
      product_title SIMILAR TO '%nice%' or 
      product_title SIMILAR TO '%hope%';

select count(*) from amazon_reviews where product_title SIMILAR TO '%lap%';

select count(*) from amazon_reviews where product_title SIMILAR TO '%lap%' group by star_rating order by star_rating desc;

select count(customer_id), count(review_ID) from amazon_reviews where  review_body like '%one%car%';

select count(*) from amazon_reviews where product_title ilike '%lap%' or product_title ilike '%e%|%E%' or customer_ID like '3%__%45__3';

select customer_id , product_title , review_id  from amazon_reviews where product_title SIMILAR TO ‘L%’ order by customer_id  LIMIT 20;

select customer_id , product_title  from amazon_reviews where review_body SIMILAR TO ‘%(good|bad)%’;

select count(*) from amazon_reviews
 where product_title SIMILAR TO  '%lap%' or
         product_title SIMILAR TO  '%iph%'  or review_body SIMILAR TO '%not%'; 

select  count(*) from amazon_reviews where product_title SIMILAR TO '%comp%' or product_title SIMILAR TO  '%iph%';

select  count(*) from amazon_reviews 
where product_title like  '%lap%' or
      product_title like  '%iph%' or 
      product_title like  '%soa%' or 
      product_title like  '%nice%' or 
      product_title like '%hope%';

 select  product_title || 'Test ' from amazon_reviews where product_title ilike '%lap%' or product_title ilike '%e%|%E%' or customer_ID like '3%__%45__3';

 select product_title like '%lap%' from amazon_reviews where product_title like  '%nice%';
