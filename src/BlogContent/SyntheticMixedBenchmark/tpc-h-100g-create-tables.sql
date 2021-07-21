create schema if not exists tpch100g;

set search_path=tpch100g;

drop table if exists customer;

drop table if exists lineitem;

                                        
drop table if exists nation;
                                        
drop table if exists orders;
                                        
drop table if exists part;
                                        
drop table if exists partsupp;

drop table if exists region;
                                        
drop table if exists supplier;

drop table if exists delete_ids;



create table customer (
 c_custkey int8 not null ,
 c_name varchar(25) not null,
 c_address varchar(40) not null,
 c_nationkey int4 not null,
 c_phone char(15) not null,
 c_acctbal numeric(12,2) not null,
 c_mktsegment char(10) not null,
 c_comment varchar(117) not null,
 Primary Key(C_CUSTKEY)
) distkey(c_custkey) sortkey(c_custkey);

create table lineitem (
 l_orderkey int8 not null ,
 l_partkey int8 not null,
 l_suppkey int4 not null,
 l_linenumber int4 not null,
 l_quantity numeric(12,2) not null,
 l_extendedprice numeric(12,2) not null,
 l_discount numeric(12,2) not null,
 l_tax numeric(12,2) not null,
 l_returnflag char(1) not null,
 l_linestatus char(1) not null,
 l_shipdate date not null ,
 l_commitdate date not null,
 l_receiptdate date not null,
 l_shipinstruct char(25) not null,
 l_shipmode char(10) not null,
 l_comment varchar(44) not null,
 Primary Key(L_ORDERKEY, L_LINENUMBER)
) distkey(l_orderkey) sortkey(l_shipdate,l_orderkey)  ;

create table nation (
 n_nationkey int4 not null,
 n_name char(25) not null ,
 n_regionkey int4 not null,
 n_comment varchar(152) not null,
 Primary Key(N_NATIONKEY)                                
) distkey(n_nationkey) sortkey(n_nationkey) ;

create table orders (
 o_orderkey int8 not null,
 o_custkey int8 not null,
 o_orderstatus char(1) not null,
 o_totalprice numeric(12,2) not null,
 o_orderdate date not null,
 o_orderpriority char(15) not null,
 o_clerk char(15) not null,
 o_shippriority int4 not null,
 o_comment varchar(79) not null,
 Primary Key(O_ORDERKEY)
) distkey(o_orderkey) sortkey(o_orderdate, o_orderkey) ;

create table part (
 p_partkey int8 not null ,
 p_name varchar(55) not null,
 p_mfgr char(25) not null,
 p_brand char(10) not null,
 p_type varchar(25) not null,
 p_size int4 not null,
 p_container char(10) not null,
 p_retailprice numeric(12,2) not null,
 p_comment varchar(23) not null,
 PRIMARY KEY (P_PARTKEY)
) distkey(p_partkey) sortkey(p_partkey);

create table partsupp (
 ps_partkey int8 not null,
 ps_suppkey int4 not null,
 ps_availqty int4 not null,
 ps_supplycost numeric(12,2) not null,
 ps_comment varchar(199) not null,
 Primary Key(PS_PARTKEY, PS_SUPPKEY)
) distkey(ps_partkey) sortkey(ps_partkey);

create table region (
 r_regionkey int4 not null,
 r_name char(25) not null ,
 r_comment varchar(152) not null,
 Primary Key(R_REGIONKEY)                             
) distkey(r_regionkey) sortkey(r_regionkey);

create table supplier (
 s_suppkey int4 not null,
 s_name char(25) not null,
 s_address varchar(40) not null,
 s_nationkey int4 not null,
 s_phone char(15) not null,
 s_acctbal numeric(12,2) not null,
 s_comment varchar(101) not null,
 Primary Key(S_SUPPKEY)
) distkey(s_suppkey) sortkey(s_suppkey)
;


grant all on schema tpch100g to datascienceuser,reportuser,dashboarduser,copyuser;

grant all on all tables in schema tpch100g to datascienceuser,reportuser,dashboarduser, copyuser;


copy customer from 's3://tpc-h/100/customer.tbl.' CREDENTIALS 'aws_access_key_id=<aaceeskeyid> ;aws_secret_access_key=<accesskey>' gzip delimiter '|';
copy lineitem from 's3://tpc-h/100/lineitem.tbl.' CREDENTIALS 'aws_access_key_id=<aaceeskeyid> ;aws_secret_access_key=<accesskey>' gzip delimiter '|';
copy nation from 's3://tpc-h/100/nation.tbl.' CREDENTIALS 'aws_access_key_id=<aaceeskeyid> ;aws_secret_access_key=<accesskey>' gzip delimiter '|';
copy orders from 's3://tpc-h/100/orders.tbl.' CREDENTIALS 'aws_access_key_id=<aaceeskeyid> ;aws_secret_access_key=<accesskey>' gzip delimiter '|';
copy part from 's3://tpc-h/100/part.tbl.' CREDENTIALS 'aws_access_key_id=<aaceeskeyid> ;aws_secret_access_key=<accesskey>' gzip delimiter '|';
copy partsupp from 's3://tpc-h/100/partsupp.tbl.' CREDENTIALS 'aws_access_key_id=<aaceeskeyid> ;aws_secret_access_key=<accesskey>'  gzip delimiter '|';
copy region from 's3://tpc-h/100/region.tbl.' CREDENTIALS 'aws_access_key_id=<aaceeskeyid> ;aws_secret_access_key=<accesskey>' gzip delimiter '|';
copy supplier from 's3://tpc-h/100/supplier.tbl.' CREDENTIALS 'aws_access_key_id=<aaceeskeyid> ;aws_secret_access_key=<accesskey>' gzip delimiter '|';





