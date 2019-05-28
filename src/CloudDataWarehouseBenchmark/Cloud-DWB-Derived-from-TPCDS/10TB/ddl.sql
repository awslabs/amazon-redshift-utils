create table dbgen_version
(
    dv_version                varchar(32),
    dv_create_date            date       ,
    dv_create_time            timestamp,
    dv_cmdline_args           varchar(200)                  
);

create table customer_address
(
 ca_address_sk int4 not null ,
  ca_address_id char(16) not null ,
  ca_street_number char(10) ,      
  ca_street_name varchar(60) ,   
  ca_street_type char(15) ,     
  ca_suite_number char(10) ,    
  ca_city varchar(60) ,         
  ca_county varchar(30) ,       
  ca_state char(2) ,            
  ca_zip char(10) ,             
  ca_country varchar(20) ,      
  ca_gmt_offset numeric(5,2) ,  
  ca_location_type char(20)     
  ,primary key (ca_address_sk)
) distkey(ca_address_sk);

create table customer_demographics
(
  cd_demo_sk int4 not null ,   
  cd_gender char(1) ,          
  cd_marital_status char(1) ,   
  cd_education_status char(20) , 
  cd_purchase_estimate int4 ,   
  cd_credit_rating char(10) ,   
  cd_dep_count int4 ,             
  cd_dep_employed_count int4 ,    
  cd_dep_college_count int4       
  ,primary key (cd_demo_sk)
)distkey (cd_demo_sk);

create table date_dim
(
    d_date_sk                 integer               not null,
    d_date_id                 char(16)              not null,
    d_date                    date,
    d_month_seq               integer                       ,
    d_week_seq                integer                       ,
    d_quarter_seq             integer                       ,
    d_year                    integer                       ,
    d_dow                     integer                       ,
    d_moy                     integer                       ,
    d_dom                     integer                       ,
    d_qoy                     integer                       ,
    d_fy_year                 integer                       ,
    d_fy_quarter_seq          integer                       ,
    d_fy_week_seq             integer                       ,
    d_day_name                char(9)                       ,
    d_quarter_name            char(6)                       ,
    d_holiday                 char(1)                       ,
    d_weekend                 char(1)                       ,
    d_following_holiday       char(1)                       ,
    d_first_dom               integer                       ,
    d_last_dom                integer                       ,
    d_same_day_ly             integer                       ,
    d_same_day_lq             integer                       ,
    d_current_day             char(1)                       ,
    d_current_week            char(1)                       ,
    d_current_month           char(1)                       ,
    d_current_quarter         char(1)                       ,
    d_current_year            char(1)                       ,
    primary key (d_date_sk)
) diststyle all;

create table warehouse
(
    w_warehouse_sk            integer               not null,
    w_warehouse_id            char(16)              not null,
    w_warehouse_name          varchar(20)                   ,
    w_warehouse_sq_ft         integer                       ,
    w_street_number           char(10)                      ,
    w_street_name             varchar(60)                   ,
    w_street_type             char(15)                      ,
    w_suite_number            char(10)                      ,
    w_city                    varchar(60)                   ,
    w_county                  varchar(30)                   ,
    w_state                   char(2)                       ,
    w_zip                     char(10)                      ,
    w_country                 varchar(20)                   ,
    w_gmt_offset              decimal(5,2)                  ,
    primary key (w_warehouse_sk)
) diststyle all;

create table ship_mode
(
    sm_ship_mode_sk           integer               not null,
    sm_ship_mode_id           char(16)              not null,
    sm_type                   char(30)                      ,
    sm_code                   char(10)                      ,
    sm_carrier                char(20)                      ,
    sm_contract               char(20)                      ,
    primary key (sm_ship_mode_sk)
) diststyle all;

create table time_dim
(
    t_time_sk                 integer               not null,
    t_time_id                 char(16)              not null,
    t_time                    integer                       ,
    t_hour                    integer                       ,
    t_minute                  integer                       ,
    t_second                  integer                       ,
    t_am_pm                   char(2)                       ,
    t_shift                   char(20)                      ,
    t_sub_shift               char(20)                      ,
    t_meal_time               char(20)                      ,
    primary key (t_time_sk)
) diststyle all;

create table reason
(
    r_reason_sk               integer               not null,
    r_reason_id               char(16)              not null,
    r_reason_desc             char(100)                     ,
    primary key (r_reason_sk)
) diststyle all ;

create table income_band
(
    ib_income_band_sk         integer               not null,
    ib_lower_bound            integer                       ,
    ib_upper_bound            integer                       ,
    primary key (ib_income_band_sk)
) diststyle all;

create table item
(
i_item_sk int4 not null,                     
  i_item_id char(16) not null ,      
  i_rec_start_date date,             
  i_rec_end_date date,               
  i_item_desc varchar(200) ,         
  i_current_price numeric(7,2),      
  i_wholesale_cost numeric(7,2),     
  i_brand_id int4,                   
  i_brand char(50) ,                 
  i_class_id int4,                   
  i_class char(50) ,                 
  i_category_id int4,                
  i_category char(50) ,              
  i_manufact_id int4,                
  i_manufact char(50) ,              
  i_size char(20) ,                  
  i_formulation char(20) ,           
  i_color char(20) ,            
  i_units char(10),             
  i_container char(10),         
  i_manager_id int4,            
  i_product_name char(50)       
  ,primary key (i_item_sk)
) distkey(i_item_sk) sortkey(i_category);

create table store
(
    s_store_sk                integer               not null,
    s_store_id                char(16)              not null,
    s_rec_start_date          date,
    s_rec_end_date            date,
    s_closed_date_sk          integer                       ,
    s_store_name              varchar(50)                   ,
    s_number_employees        integer                       ,
    s_floor_space             integer                       ,
    s_hours                   char(20)                      ,
    s_manager                 varchar(40)                   ,
    s_market_id               integer                       ,
    s_geography_class         varchar(100)                  ,
    s_market_desc             varchar(100)                  ,
    s_market_manager          varchar(40)                   ,
    s_division_id             integer                       ,
    s_division_name           varchar(50)                   ,
    s_company_id              integer                       ,
    s_company_name            varchar(50)                   ,
    s_street_number           varchar(10)                   ,
    s_street_name             varchar(60)                   ,
    s_street_type             char(15)                      ,
    s_suite_number            char(10)                      ,
    s_city                    varchar(60)                   ,
    s_county                  varchar(30)                   ,
    s_state                   char(2)                       ,
    s_zip                     char(10)                      ,
    s_country                 varchar(20)                   ,
    s_gmt_offset              decimal(5,2)                  ,
    s_tax_precentage          decimal(5,2)                  ,
    primary key (s_store_sk)
) diststyle all;

create table call_center
(
    cc_call_center_sk         integer               not null,
    cc_call_center_id         char(16)              not null,
    cc_rec_start_date         date,
    cc_rec_end_date           date,
    cc_closed_date_sk         integer                       ,
    cc_open_date_sk           integer                       ,
    cc_name                   varchar(50)                   ,
    cc_class                  varchar(50)                   ,
    cc_employees              integer                       ,
    cc_sq_ft                  integer                       ,
    cc_hours                  char(20)                      ,
    cc_manager                varchar(40)                   ,
    cc_mkt_id                 integer                       ,
    cc_mkt_class              char(50)                      ,
    cc_mkt_desc               varchar(100)                  ,
    cc_market_manager         varchar(40)                   ,
    cc_division               integer                       ,
    cc_division_name          varchar(50)                   ,
    cc_company                integer                       ,
    cc_company_name           char(50)                      ,
    cc_street_number          char(10)                      ,
    cc_street_name            varchar(60)                   ,
    cc_street_type            char(15)                      ,
    cc_suite_number           char(10)                      ,
    cc_city                   varchar(60)                   ,
    cc_county                 varchar(30)                   ,
    cc_state                  char(2)                       ,
    cc_zip                    char(10)                      ,
    cc_country                varchar(20)                   ,
    cc_gmt_offset             decimal(5,2)                  ,
    cc_tax_percentage         decimal(5,2)                  ,
    primary key (cc_call_center_sk)
) diststyle all;

create table customer
(
  c_customer_sk int4 not null ,                 
  c_customer_id char(16) not null ,             
  c_current_cdemo_sk int4 ,   
  c_current_hdemo_sk int4 ,   
  c_current_addr_sk int4 ,    
  c_first_shipto_date_sk int4 ,                 
  c_first_sales_date_sk int4 ,
  c_salutation char(10) ,     
  c_first_name char(20) ,     
  c_last_name char(30) ,      
  c_preferred_cust_flag char(1) ,               
  c_birth_day int4 ,          
  c_birth_month int4 ,        
  c_birth_year int4 ,         
  c_birth_country varchar(20) ,                 
  c_login char(13) ,          
  c_email_address char(50) ,  
  c_last_review_date_sk int4 ,
  primary key (c_customer_sk)
) distkey(c_customer_sk);

create table web_site
(
    web_site_sk               integer               not null,
    web_site_id               char(16)              not null,
    web_rec_start_date        date,
    web_rec_end_date          date,
    web_name                  varchar(50)                   ,
    web_open_date_sk          integer                       ,
    web_close_date_sk         integer                       ,
    web_class                 varchar(50)                   ,
    web_manager               varchar(40)                   ,
    web_mkt_id                integer                       ,
    web_mkt_class             varchar(50)                   ,
    web_mkt_desc              varchar(100)                  ,
    web_market_manager        varchar(40)                   ,
    web_company_id            integer                       ,
    web_company_name          char(50)                      ,
    web_street_number         char(10)                      ,
    web_street_name           varchar(60)                   ,
    web_street_type           char(15)                      ,
    web_suite_number          char(10)                      ,
    web_city                  varchar(60)                   ,
    web_county                varchar(30)                   ,
    web_state                 char(2)                       ,
    web_zip                   char(10)                      ,
    web_country               varchar(20)                   ,
    web_gmt_offset            decimal(5,2)                  ,
    web_tax_percentage        decimal(5,2)                  ,
    primary key (web_site_sk)
) diststyle all;

create table store_returns
(
sr_returned_date_sk int4 ,    
  sr_return_time_sk int4 ,    
  sr_item_sk int4 not null ,  
  sr_customer_sk int4 ,       
  sr_cdemo_sk int4 ,          
  sr_hdemo_sk int4 ,          
  sr_addr_sk int4 ,           
  sr_store_sk int4 ,          
  sr_reason_sk int4 ,         
  sr_ticket_number int8 not null,               
  sr_return_quantity int4 ,   
  sr_return_amt numeric(7,2) ,
  sr_return_tax numeric(7,2) ,
  sr_return_amt_inc_tax numeric(7,2) ,          
  sr_fee numeric(7,2) ,       
  sr_return_ship_cost numeric(7,2) ,            
  sr_refunded_cash numeric(7,2) ,               
  sr_reversed_charge numeric(7,2) ,             
  sr_store_credit numeric(7,2) ,                
  sr_net_loss numeric(7,2)                      
  ,primary key (sr_item_sk, sr_ticket_number)
) distkey(sr_item_sk) sortkey(sr_returned_date_sk);

create table household_demographics
(
    hd_demo_sk                integer               not null,
    hd_income_band_sk         integer                       ,
    hd_buy_potential          char(15)                      ,
    hd_dep_count              integer                       ,
    hd_vehicle_count          integer                       ,
    primary key (hd_demo_sk)
) diststyle all;

create table web_page
(
    wp_web_page_sk            integer               not null,
    wp_web_page_id            char(16)              not null,
    wp_rec_start_date         date,
    wp_rec_end_date           date,
    wp_creation_date_sk       integer                       ,
    wp_access_date_sk         integer                       ,
    wp_autogen_flag           char(1)                       ,
    wp_customer_sk            integer                       ,
    wp_url                    varchar(100)                  ,
    wp_type                   char(50)                      ,
    wp_char_count             integer                       ,
    wp_link_count             integer                       ,
    wp_image_count            integer                       ,
    wp_max_ad_count           integer                       ,
    primary key (wp_web_page_sk)
) diststyle all;

create table promotion
(
    p_promo_sk                integer               not null,
    p_promo_id                char(16)              not null,
    p_start_date_sk           integer                       ,
    p_end_date_sk             integer                       ,
    p_item_sk                 integer                       ,
    p_cost                    decimal(15,2)                 ,
    p_response_target         integer                       ,
    p_promo_name              char(50)                      ,
    p_channel_dmail           char(1)                       ,
    p_channel_email           char(1)                       ,
    p_channel_catalog         char(1)                       ,
    p_channel_tv              char(1)                       ,
    p_channel_radio           char(1)                       ,
    p_channel_press           char(1)                       ,
    p_channel_event           char(1)                       ,
    p_channel_demo            char(1)                       ,
    p_channel_details         varchar(100)                  ,
    p_purpose                 char(15)                      ,
    p_discount_active         char(1)                       ,
    primary key (p_promo_sk)
) diststyle all;

create table catalog_page
(
    cp_catalog_page_sk        integer               not null,
    cp_catalog_page_id        char(16)              not null,
    cp_start_date_sk          integer                       ,
    cp_end_date_sk            integer                       ,
    cp_department             varchar(50)                   ,
    cp_catalog_number         integer                       ,
    cp_catalog_page_number    integer                       ,
    cp_description            varchar(100)                  ,
    cp_type                   varchar(100)                  ,
    primary key (cp_catalog_page_sk)
) diststyle all;

create table inventory
(
 inv_date_sk int4 not null , 
  inv_item_sk int4 not null ,
  inv_warehouse_sk int4 not null ,
  inv_quantity_on_hand int4
  ,primary key (inv_date_sk, inv_item_sk, inv_warehouse_sk)
) distkey(inv_item_sk) sortkey(inv_date_sk);

create table catalog_returns
(
 cr_returned_date_sk int4 ,  
  cr_returned_time_sk int4 , 
  cr_item_sk int4 not null , 
  cr_refunded_customer_sk int4 ,
  cr_refunded_cdemo_sk int4 ,   
  cr_refunded_hdemo_sk int4 ,   
  cr_refunded_addr_sk int4 ,    
  cr_returning_customer_sk int4 ,
  cr_returning_cdemo_sk int4 ,   
  cr_returning_hdemo_sk int4 ,  
  cr_returning_addr_sk int4 ,   
  cr_call_center_sk int4 ,      
  cr_catalog_page_sk int4 ,     
  cr_ship_mode_sk int4 ,        
  cr_warehouse_sk int4 ,        
  cr_reason_sk int4 ,           
  cr_order_number int8 not null,
  cr_return_quantity int4 ,     
  cr_return_amount numeric(7,2) ,
  cr_return_tax numeric(7,2) ,   
  cr_return_amt_inc_tax numeric(7,2) ,
  cr_fee numeric(7,2) ,         
  cr_return_ship_cost numeric(7,2) , 
  cr_refunded_cash numeric(7,2) ,    
  cr_reversed_charge numeric(7,2) ,  
  cr_store_credit numeric(7,2) ,
  cr_net_loss numeric(7,2)      
  ,primary key (cr_item_sk, cr_order_number)
) distkey(cr_item_sk) sortkey(cr_returned_date_sk);

create table web_returns
(
wr_returned_date_sk int4 ,   
  wr_returned_time_sk int4 , 
  wr_item_sk int4 not null , 
  wr_refunded_customer_sk int4 ,
  wr_refunded_cdemo_sk int4 ,   
  wr_refunded_hdemo_sk int4 ,   
  wr_refunded_addr_sk int4 ,    
  wr_returning_customer_sk int4 ,
  wr_returning_cdemo_sk int4 ,   
  wr_returning_hdemo_sk int4 ,  
  wr_returning_addr_sk int4 ,   
  wr_web_page_sk int4 ,         
  wr_reason_sk int4 ,           
  wr_order_number int8 not null,
  wr_return_quantity int4 ,     
  wr_return_amt numeric(7,2) ,  
  wr_return_tax numeric(7,2) ,  
  wr_return_amt_inc_tax numeric(7,2) ,
  wr_fee numeric(7,2) ,         
  wr_return_ship_cost numeric(7,2) ,
  wr_refunded_cash numeric(7,2) ,   
  wr_reversed_charge numeric(7,2) ,  
  wr_account_credit numeric(7,2) ,   
  wr_net_loss numeric(7,2)           
  ,primary key (wr_item_sk, wr_order_number)
) distkey(wr_order_number) sortkey(wr_returned_date_sk);

create table web_sales
(
 ws_sold_date_sk int4 ,             
  ws_sold_time_sk int4 ,        
  ws_ship_date_sk int4 ,        
  ws_item_sk int4 not null ,    
  ws_bill_customer_sk int4 ,    
  ws_bill_cdemo_sk int4 ,       
  ws_bill_hdemo_sk int4 ,       
  ws_bill_addr_sk int4 ,        
  ws_ship_customer_sk int4 ,    
  ws_ship_cdemo_sk int4 ,       
  ws_ship_hdemo_sk int4 ,       
  ws_ship_addr_sk int4 ,        
  ws_web_page_sk int4 ,         
  ws_web_site_sk int4 ,         
  ws_ship_mode_sk int4 ,        
  ws_warehouse_sk int4 ,        
  ws_promo_sk int4 ,            
  ws_order_number int8 not null,
  ws_quantity int4 ,            
  ws_wholesale_cost numeric(7,2) ,                
  ws_list_price numeric(7,2) ,  
  ws_sales_price numeric(7,2) , 
  ws_ext_discount_amt numeric(7,2) ,              
  ws_ext_sales_price numeric(7,2) ,
  ws_ext_wholesale_cost numeric(7,2) ,               
  ws_ext_list_price numeric(7,2) , 
  ws_ext_tax numeric(7,2) ,     
  ws_coupon_amt numeric(7,2) ,  
  ws_ext_ship_cost numeric(7,2) ,                 
  ws_net_paid numeric(7,2) ,    
  ws_net_paid_inc_tax numeric(7,2) ,              
  ws_net_paid_inc_ship numeric(7,2) ,             
  ws_net_paid_inc_ship_tax numeric(7,2) ,         
  ws_net_profit numeric(7,2)                      
  ,primary key (ws_item_sk, ws_order_number)
) distkey(ws_order_number) sortkey(ws_sold_date_sk);

create table catalog_sales
(
 cs_sold_date_sk int4 ,          
  cs_sold_time_sk int4 ,        
  cs_ship_date_sk int4 ,        
  cs_bill_customer_sk int4 ,    
  cs_bill_cdemo_sk int4 ,       
  cs_bill_hdemo_sk int4 ,       
  cs_bill_addr_sk int4 ,        
  cs_ship_customer_sk int4 ,    
  cs_ship_cdemo_sk int4 ,       
  cs_ship_hdemo_sk int4 ,       
  cs_ship_addr_sk int4 ,        
  cs_call_center_sk int4 ,      
  cs_catalog_page_sk int4 ,     
  cs_ship_mode_sk int4 ,        
  cs_warehouse_sk int4 ,        
  cs_item_sk int4 not null ,    
  cs_promo_sk int4 ,            
  cs_order_number int8 not null ,                 
  cs_quantity int4 ,            
  cs_wholesale_cost numeric(7,2) ,                
  cs_list_price numeric(7,2) ,  
  cs_sales_price numeric(7,2) , 
  cs_ext_discount_amt numeric(7,2) ,              
  cs_ext_sales_price numeric(7,2) ,               
  cs_ext_wholesale_cost numeric(7,2) ,            
  cs_ext_list_price numeric(7,2) ,
  cs_ext_tax numeric(7,2) ,     
  cs_coupon_amt numeric(7,2) , 
  cs_ext_ship_cost numeric(7,2) ,                
  cs_net_paid numeric(7,2) ,   
  cs_net_paid_inc_tax numeric(7,2) ,             
  cs_net_paid_inc_ship numeric(7,2) ,            
  cs_net_paid_inc_ship_tax numeric(7,2) ,        
  cs_net_profit numeric(7,2)                     
  ,primary key (cs_item_sk, cs_order_number)
) distkey(cs_item_sk) sortkey(cs_sold_date_sk);

create table store_sales
(
ss_sold_date_sk int4 ,            
  ss_sold_time_sk int4 ,     
  ss_item_sk int4 not null ,      
  ss_customer_sk int4 ,           
  ss_cdemo_sk int4 ,              
  ss_hdemo_sk int4 ,         
  ss_addr_sk int4 ,               
  ss_store_sk int4 ,           
  ss_promo_sk int4 ,           
  ss_ticket_number int8 not null,        
  ss_quantity int4 ,           
  ss_wholesale_cost numeric(7,2) ,          
  ss_list_price numeric(7,2) ,              
  ss_sales_price numeric(7,2) ,
  ss_ext_discount_amt numeric(7,2) ,             
  ss_ext_sales_price numeric(7,2) ,              
  ss_ext_wholesale_cost numeric(7,2) ,           
  ss_ext_list_price numeric(7,2) ,               
  ss_ext_tax numeric(7,2) ,                 
  ss_coupon_amt numeric(7,2) , 
  ss_net_paid numeric(7,2) ,   
  ss_net_paid_inc_tax numeric(7,2) ,             
  ss_net_profit numeric(7,2)                     
  ,primary key (ss_item_sk, ss_ticket_number)
) distkey(ss_item_sk) sortkey(ss_sold_date_sk);

/*
	To load the sample data, you must provide authentication for your cluster to access Amazon S3 on your behalf.
	You can provide either role-based authentication or key-based authentication.

	Text files needed to load test data under s3://redshift-downloads/TPC-DS/10TB are publicly available.
	Any valid credentials should have read access.

	The COPY commands include a placeholder for the aws_access_key_id and aws_secret_access_key.
	User must update the credentials clause below with valid credentials or the command will fail.

	For more information check samples in https://docs.aws.amazon.com/redshift/latest/gsg/rs-gsg-create-sample-db.html
*/

copy store_sales from 's3://redshift-downloads/TPC-DS/10TB/store_sales/' credentials 'aws_access_key_id=<USER_ACCESS_KEY_ID> ;aws_secret_access_key=<USER_SECRET_ACCESS_KEY>' gzip delimiter '|' COMPUPDATE ON region 'us-east-1';
copy catalog_sales from 's3://redshift-downloads/TPC-DS/10TB/catalog_sales/' credentials 'aws_access_key_id=<USER_ACCESS_KEY_ID> ;aws_secret_access_key=<USER_SECRET_ACCESS_KEY>' gzip delimiter '|' COMPUPDATE ON region 'us-east-1' ;
copy web_sales from 's3://redshift-downloads/TPC-DS/10TB/web_sales/' credentials 'aws_access_key_id=<USER_ACCESS_KEY_ID> ;aws_secret_access_key=<USER_SECRET_ACCESS_KEY>' gzip delimiter '|' COMPUPDATE ON region 'us-east-1' ;
copy web_returns from 's3://redshift-downloads/TPC-DS/10TB/web_returns/' credentials 'aws_access_key_id=<USER_ACCESS_KEY_ID> ;aws_secret_access_key=<USER_SECRET_ACCESS_KEY>' gzip delimiter '|' COMPUPDATE ON region 'us-east-1' ;
copy store_returns from 's3://redshift-downloads/TPC-DS/10TB/store_returns/' credentials 'aws_access_key_id=<USER_ACCESS_KEY_ID> ;aws_secret_access_key=<USER_SECRET_ACCESS_KEY>' gzip delimiter '|' COMPUPDATE ON region 'us-east-1' ;
copy catalog_returns from 's3://redshift-downloads/TPC-DS/10TB/catalog_returns/' credentials 'aws_access_key_id=<USER_ACCESS_KEY_ID> ;aws_secret_access_key=<USER_SECRET_ACCESS_KEY>' gzip delimiter '|' COMPUPDATE ON region 'us-east-1' ;
copy call_center from 's3://redshift-downloads/TPC-DS/10TB/call_center/' credentials 'aws_access_key_id=<USER_ACCESS_KEY_ID> ;aws_secret_access_key=<USER_SECRET_ACCESS_KEY>' gzip delimiter '|' COMPUPDATE ON region 'us-east-1' ;
copy catalog_page from 's3://redshift-downloads/TPC-DS/10TB/catalog_page/' credentials 'aws_access_key_id=<USER_ACCESS_KEY_ID> ;aws_secret_access_key=<USER_SECRET_ACCESS_KEY>' gzip delimiter '|' COMPUPDATE ON region 'us-east-1' ;
copy customer_address from 's3://redshift-downloads/TPC-DS/10TB/customer_address/' credentials 'aws_access_key_id=<USER_ACCESS_KEY_ID> ;aws_secret_access_key=<USER_SECRET_ACCESS_KEY>' gzip delimiter '|' COMPUPDATE ON region 'us-east-1' ;
copy customer from 's3://redshift-downloads/TPC-DS/10TB/customer/' credentials 'aws_access_key_id=<USER_ACCESS_KEY_ID> ;aws_secret_access_key=<USER_SECRET_ACCESS_KEY>' gzip delimiter '|' COMPUPDATE ON region 'us-east-1' ;
copy customer_demographics from 's3://redshift-downloads/TPC-DS/10TB/customer_demographics/' credentials 'aws_access_key_id=<USER_ACCESS_KEY_ID> ;aws_secret_access_key=<USER_SECRET_ACCESS_KEY>' gzip delimiter '|' COMPUPDATE ON region 'us-east-1' ;
copy date_dim from 's3://redshift-downloads/TPC-DS/10TB/date_dim/' credentials 'aws_access_key_id=<USER_ACCESS_KEY_ID> ;aws_secret_access_key=<USER_SECRET_ACCESS_KEY>' gzip delimiter '|' COMPUPDATE ON region 'us-east-1' ;
copy household_demographics from 's3://redshift-downloads/TPC-DS/10TB/household_demographics/' credentials 'aws_access_key_id=<USER_ACCESS_KEY_ID> ;aws_secret_access_key=<USER_SECRET_ACCESS_KEY>' gzip delimiter '|' COMPUPDATE ON region 'us-east-1' ;
copy income_band from 's3://redshift-downloads/TPC-DS/10TB/income_band/' credentials 'aws_access_key_id=<USER_ACCESS_KEY_ID> ;aws_secret_access_key=<USER_SECRET_ACCESS_KEY>' gzip delimiter '|' COMPUPDATE ON region 'us-east-1' ;
copy inventory from 's3://redshift-downloads/TPC-DS/10TB/inventory/' credentials 'aws_access_key_id=<USER_ACCESS_KEY_ID> ;aws_secret_access_key=<USER_SECRET_ACCESS_KEY>' gzip delimiter '|' COMPUPDATE ON region 'us-east-1' ;
copy item from 's3://redshift-downloads/TPC-DS/10TB/item/' credentials 'aws_access_key_id=<USER_ACCESS_KEY_ID> ;aws_secret_access_key=<USER_SECRET_ACCESS_KEY>' gzip delimiter '|' COMPUPDATE ON region 'us-east-1' ;
copy promotion from 's3://redshift-downloads/TPC-DS/10TB/promotion/' credentials 'aws_access_key_id=<USER_ACCESS_KEY_ID> ;aws_secret_access_key=<USER_SECRET_ACCESS_KEY>' gzip delimiter '|' COMPUPDATE ON region 'us-east-1' ;
copy reason from 's3://redshift-downloads/TPC-DS/10TB/reason/' credentials 'aws_access_key_id=<USER_ACCESS_KEY_ID> ;aws_secret_access_key=<USER_SECRET_ACCESS_KEY>' gzip delimiter '|' COMPUPDATE ON region 'us-east-1' ;
copy ship_mode from 's3://redshift-downloads/TPC-DS/10TB/ship_mode/' credentials 'aws_access_key_id=<USER_ACCESS_KEY_ID> ;aws_secret_access_key=<USER_SECRET_ACCESS_KEY>' gzip delimiter '|' COMPUPDATE ON region 'us-east-1' ;
copy store from 's3://redshift-downloads/TPC-DS/10TB/store/' credentials 'aws_access_key_id=<USER_ACCESS_KEY_ID> ;aws_secret_access_key=<USER_SECRET_ACCESS_KEY>' gzip delimiter '|' COMPUPDATE ON region 'us-east-1' ;
copy time_dim from 's3://redshift-downloads/TPC-DS/10TB/time_dim/' credentials 'aws_access_key_id=<USER_ACCESS_KEY_ID> ;aws_secret_access_key=<USER_SECRET_ACCESS_KEY>' gzip delimiter '|' COMPUPDATE ON region 'us-east-1' ;
copy warehouse from 's3://redshift-downloads/TPC-DS/10TB/warehouse/' credentials 'aws_access_key_id=<USER_ACCESS_KEY_ID> ;aws_secret_access_key=<USER_SECRET_ACCESS_KEY>' gzip delimiter '|' COMPUPDATE ON region 'us-east-1' ;
copy web_page from 's3://redshift-downloads/TPC-DS/10TB/web_page/' credentials 'aws_access_key_id=<USER_ACCESS_KEY_ID> ;aws_secret_access_key=<USER_SECRET_ACCESS_KEY>' gzip delimiter '|' COMPUPDATE ON region 'us-east-1' ;
copy web_site from 's3://redshift-downloads/TPC-DS/10TB/web_site/' credentials 'aws_access_key_id=<USER_ACCESS_KEY_ID> ;aws_secret_access_key=<USER_SECRET_ACCESS_KEY>' gzip delimiter '|' COMPUPDATE ON region 'us-east-1' ;

select count(*) from call_center; -- 54
select count(*) from catalog_page; -- 40000
select count(*) from catalog_returns; -- 1440033112
select count(*) from catalog_sales; -- 14399964710
select count(*) from customer_address; -- 32500000
select count(*) from customer_demographics; -- 1920800
select count(*) from customer; -- 65000000
select count(*) from date_dim; -- 73049
select count(*) from household_demographics; -- 7200
select count(*) from income_band; -- 20
select count(*) from inventory; -- 1311525000
select count(*) from item; -- 402000
select count(*) from promotion; -- 2000
select count(*) from reason; -- 70
select count(*) from ship_mode; -- 20
select count(*) from store_returns; -- 2880015105
select count(*) from store_sales; -- 28799289409
select count(*) from store; -- 1500
select count(*) from time_dim; -- 86400
select count(*) from warehouse; -- 25
select count(*) from web_page; -- 4002
select count(*) from web_returns; -- 720020485
select count(*) from web_sales; -- 7199963324
select count(*) from web_site; -- 78