/*creates table in default dev db and loaded data into the table*/
CREATE TABLE amazon_reviews(
  marketplace char(2), 
  customer_id varchar(10), 
  review_id varchar(20), 
  product_id varchar(15), 
  product_parent varchar(15), 
  product_title varchar(500), 
  product_category varchar(30), 
  star_rating int, 
  helpful_votes int, 
  total_votes int, 
  vine char(1), 
  verified_purchase char(1), 
  review_headline varchar(200), 
  review_body varchar(max), 
  review_date date);

/* Note: A role must be associated with your cluster and should have AmazonS3ReadOnlyAccess policy associated with it */ 

copy amazon_reviews from 's3://amazon-reviews-pds/tsv/amazon_reviews' \
credentials 'aws_iam_role=<arn_of_role_with_policy_AmazonS3ReadOnlyAccess>'
gzip delimiter '\t' ignoreheader as 1;

/*In case you are using an access key/secret pair*/

copy amazon_reviews from 's3://amazon-reviews-pds/tsv/amazon_reviews' \
credentials 'aws_access_key_id= aws_secret_access_key= ' \
gzip delimiter '\t' ignoreheader as 1; 
