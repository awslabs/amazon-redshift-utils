/* These are some of aqua eligible test queries*/
select count(*) 
from amazon_reviews 
where product_title similar to '%lap%' or
      product_title similar to '%iph%' or 
      product_title similar to '%soa%' or 
      product_title similar to '%nice%' or 
      product_title similar to '%hope%';

select count(*) 
from amazon_reviews 
where product_title similar to '%lap%';

select count(*) 
from amazon_reviews 
where product_title similar to '%lap%' 
group by star_rating 
order by star_rating desc;

select count(customer_id),
       count(review_ID) 
from  amazon_reviews 
where review_body like '%one%car%';

select count(*) 
from  amazon_reviews 
where product_title ilike '%lap%' or 
      product_title ilike '%e%|%E%' or 
      customer_ID like '3%__%45__3';

select customer_id , 
       product_title , 
       review_id  
from  amazon_reviews 
where product_title similar to 'L%' 
order by customer_id  
limit 20;

select customer_id , 
       product_title 
from  amazon_reviews 
where review_body like '%(good|bad)%';

select count(*) 
from  amazon_reviews
where product_title similar to  '%lap%' or
      product_title similar to  '%iph%'  or 
      review_body similar to '%not%'; 

select count(*) 
from amazon_reviews 
where product_title similar to '%comp%' or 
      product_title similar to  '%iph%';

select count(*) 
from amazon_reviews 
where product_title like '%lap%' or
      product_title like '%iph%' or 
      product_title like '%soa%' or 
      product_title like '%nice%' or 
      product_title like '%hope%';

select  product_title 
from amazon_reviews 
where product_title ilike '%lap%' or 
       product_title ilike '%e%|%E%' or 
       customer_ID like '3%__%45__3';

select product_title like '%lap%' 
from amazon_reviews 
where product_title like  '%nice%';
 
 select count(*) 
 from amazon_reviews 
 where product_title ilike '%toy%';
