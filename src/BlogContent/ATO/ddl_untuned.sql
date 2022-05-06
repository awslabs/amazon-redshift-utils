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
) ;

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
) ;

create table nation (
  n_nationkey int4 not null,
  n_name char(25) not null ,
  n_regionkey int4 not null,
  n_comment varchar(152) not null,
  Primary Key(N_NATIONKEY)
) ;

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
) ;

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
) ;

create table partsupp (
  ps_partkey int8 not null,
  ps_suppkey int4 not null,
  ps_availqty int4 not null,
  ps_supplycost numeric(12,2) not null,
  ps_comment varchar(199) not null,
  Primary Key(PS_PARTKEY, PS_SUPPKEY)
) ;

create table region (
  r_regionkey int4 not null,
  r_name char(25) not null ,
  r_comment varchar(152) not null,
  Primary Key(R_REGIONKEY)
) ;

create table supplier (
  s_suppkey int4 not null,
  s_name char(25) not null,
  s_address varchar(40) not null,
  s_nationkey int4 not null,
  s_phone char(15) not null,
  s_acctbal numeric(12,2) not null,
  s_comment varchar(101) not null,
  Primary Key(S_SUPPKEY)
)  ;

copy region from 's3://redshift-downloads/TPC-H/2.18/30TB/region/' iam_role 'arn:aws:iam::<account_no>:role/RedshiftCopyRole' delimiter '|' EMPTYASNULL region 'us-east-1';
copy nation from 's3://redshift-downloads/TPC-H/2.18/30TB/nation/' iam_role 'arn:aws:iam::<account_no>:role/RedshiftCopyRole' delimiter '|' EMPTYASNULL region 'us-east-1';
copy lineitem from 's3://redshift-downloads/TPC-H/2.18/30TB/lineitem/' iam_role 'arn:aws:iam::<account_no>:role/RedshiftCopyRole' gzip delimiter '|' EMPTYASNULL region 'us-east-1';
copy orders from 's3://redshift-downloads/TPC-H/2.18/30TB/orders/' iam_role 'arn:aws:iam::<account_no>:role/RedshiftCopyRole' gzip delimiter '|' EMPTYASNULL region 'us-east-1';
copy part from 's3://redshift-downloads/TPC-H/2.18/30TB/part/' iam_role 'arn:aws:iam::<account_no>:role/RedshiftCopyRole' gzip delimiter '|' EMPTYASNULL region 'us-east-1';
copy supplier from 's3://redshift-downloads/TPC-H/2.18/30TB/supplier/' iam_role 'arn:aws:iam::<account_no>:role/RedshiftCopyRole' gzip delimiter '|' EMPTYASNULL region 'us-east-1';
copy partsupp from 's3://redshift-downloads/TPC-H/2.18/30TB/partsupp/' iam_role 'arn:aws:iam::<account_no>:role/RedshiftCopyRole' gzip delimiter '|' EMPTYASNULL region 'us-east-1';
copy customer from 's3://redshift-downloads/TPC-H/2.18/30TB/customer/' iam_role 'arn:aws:iam::<account_no>:role/RedshiftCopyRole' gzip delimiter '|' EMPTYASNULL region 'us-east-1';
