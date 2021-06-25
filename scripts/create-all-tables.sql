define uri_landing = 'https://objectstorage.us-ashburn-1.oraclecloud.com/n/adwc4pm/b/moviestream_landing/o'
define uri_gold = 'https://objectstorage.us-ashburn-1.oraclecloud.com/n/adwc4pm/b/moviestream_gold/o'
define csv_format = '{"dateformat":"YYYYMMDD", "skipheaders":"1", "delimiter":",", "ignoreblanklines":"true", "removequotes":"true", "blankasnull":"true", "trimspaces":"lrtrim", "truncatecol":"true", "ignoremissingcolumns":"true"}'
define pipe_format = '{"dateformat":"YYYYMMDD", "skipheaders":"1", "delimiter":"|", "ignoreblanklines":"true", "removequotes":"true", "blankasnull":"true", "trimspaces":"lrtrim", "truncatecol":"true", "ignoremissingcolumns":"true"}'
define json_format = '{"skipheaders":"0", "delimiter":"\n", "ignoreblanklines":"true"}'
define parquet_format = '{"type":"parquet",  "schema": "all"}'
define db_user = 'moviework'

-- initialize
drop table ext_customer;
drop table ext_genre;
drop table ext_movie;
drop table ext_pageviews;
drop table ext_custsales;
drop table ext_customer_segment;
drop table pizza_locations;
drop table customer;
drop table genre;
drop table movie;
drop table custsales;
drop table customer_segment;
drop table pizza_locations;
drop table time;

-- Grant roles to user
/*grant DWROLE TO &db_user;
grant UNLIMITED TABLESPACE TO &db_user;
grant OML_DEVELOPER to &db_user;
grant GRAPH_DEVELOPER to &db_user;
grant CREATE MATERIALIZED VIEW to &db_user;

BEGIN  
    ords_admin.enable_schema (
        p_enabled               => TRUE,
        p_schema                => '&db_user',
        p_url_mapping_type      => 'BASE_PATH',
        p_auto_rest_auth        => TRUE   -- this flag says, don't expose my REST APIs
    );
    COMMIT;
END;
/
*/

-- Test:  query object storage with the credential
-- SELECT * 
-- FROM DBMS_CLOUD.LIST_OBJECTS('OBJ_STORE_CRED', '&uri_gold/');


-- Create a time table  over 2 years.  Used to densify time series calculations
exec dbms_output.put_line(systimestamp || ' - create time table')
create table TIME as
SELECT TRUNC (to_date('20210101','YYYYMMDD')) as day
  FROM DUAL CONNECT BY ROWNUM < 732;

-- Using public buckets  so credentials are not required
-- Create external tables then do a CTAS
exec dbms_output.put_line(systimestamp || ' - create external tables')
begin
    dbms_cloud.create_external_table(
        table_name => 'ext_genre',
        file_uri_list => '&uri_gold/genre/genre.csv',
        format => '&csv_format',
        column_list => 'genre_id number, name varchar2(30)'
        );
end;
/

begin
    dbms_cloud.create_external_table(
        table_name => 'ext_customer_segment', 
        file_uri_list => '&uri_gold/customer_segment/customer_segment.csv', 
        format => '&csv_format', 
        column_list => 'segment_id number, name varchar2(100), short_name varchar2(100)' 
        );        
end;
/

begin
    dbms_cloud.create_external_table( 
        table_name => 'ext_movie', 
        file_uri_list => '&uri_gold/movie/movies.json', 
        format => '&json_format', 
        column_list => 'doc varchar2(30000)' 
        );
end;
/        

begin 
    dbms_cloud.create_external_table( 
        table_name => 'ext_custsales', 
        file_uri_list => '&uri_gold/custsales/*.parquet', 
        format => '&parquet_format',
        column_list => 'MOVIE_ID NUMBER(20,0), 
                        LIST_PRICE BINARY_DOUBLE, 
                        DISCOUNT_TYPE VARCHAR2(4000 BYTE), 
                        PAYMENT_METHOD VARCHAR2(4000 BYTE), 
                        GENRE_ID NUMBER(20,0), 
                        DISCOUNT_PERCENT BINARY_DOUBLE, 
                        ACTUAL_PRICE BINARY_DOUBLE, 
                        DEVICE VARCHAR2(4000 BYTE), 
                        CUST_ID NUMBER(20,0), 
                        OS VARCHAR2(4000 BYTE), 
                        DAY VARCHAR2(4000 BYTE), 
                        APP VARCHAR2(4000 BYTE)'
    );  
end;
/

begin        
    dbms_cloud.create_external_table( 
        table_name => 'ext_pizza_locations', 
        file_uri_list => '&uri_gold/pizza-locations/*.csv', 
        format => '&csv_format', 
        column_list => 'PIZZA_LOC_ID NUMBER, 
                        LAT NUMBER, 
                        LON NUMBER, 
                        CHAIN_ID NUMBER, 
                        CHAIN VARCHAR2(30 BYTE), 
                        ADDRESS VARCHAR2(250 BYTE), 
                        CITY VARCHAR2(250 BYTE), 
                        STATE VARCHAR2(26 BYTE), 
                        POSTAL_CODE VARCHAR2(38 BYTE), 
                        COUNTY VARCHAR2(250 BYTE)'
        );  
end;
/

begin        
    dbms_cloud.create_external_table( 
        table_name => 'ext_customer', 
        file_uri_list => '&uri_gold/customer/*.csv', 
        format => '&csv_format', 
        column_list => 'CUST_ID	NUMBER,
                        LAST_NAME	VARCHAR2(200 BYTE),
                        FIRST_NAME	VARCHAR2(200 BYTE),
                        STREET_ADDRESS	VARCHAR2(400 BYTE),
                        POSTAL_CODE	VARCHAR2(10 BYTE),
                        CITY	VARCHAR2(100 BYTE),
                        STATE_PROVINCE	VARCHAR2(100 BYTE),
                        COUNTRY	VARCHAR2(400 BYTE),
                        COUNTRY_CODE	VARCHAR2(2 BYTE),
                        CONTINENT	VARCHAR2(400 BYTE),
                        AGE	NUMBER,
                        COMMUTE_DISTANCE	NUMBER,
                        CREDIT_BALANCE	NUMBER,
                        EDUCATION	VARCHAR2(40 BYTE),
                        EMAIL	VARCHAR2(500 BYTE),
                        FULL_TIME	VARCHAR2(40 BYTE),
                        GENDER	VARCHAR2(20 BYTE),
                        HOUSEHOLD_SIZE	NUMBER,
                        INCOME	NUMBER,
                        INCOME_LEVEL VARCHAR2(20 BYTE),
                        INSUFF_FUNDS_INCIDENTS	NUMBER,
                        JOB_TYPE	VARCHAR2(200 BYTE),
                        LATE_MORT_RENT_PMTS	NUMBER,
                        MARITAL_STATUS	VARCHAR2(8 BYTE),
                        MORTGAGE_AMT	NUMBER,
                        NUM_CARS	NUMBER,
                        NUM_MORTGAGES	NUMBER,
                        PET	VARCHAR2(40 BYTE),
                        PROMOTION_RESPONSE	NUMBER,
                        RENT_OWN	VARCHAR2(40 BYTE),
                        SEGMENT_ID	NUMBER,
                        WORK_EXPERIENCE	NUMBER,
                        YRS_CURRENT_EMPLOYER	NUMBER,
                        YRS_CUSTOMER	NUMBER,
                        YRS_RESIDENCE	NUMBER,
                        LOC_LAT	NUMBER,
                        LOC_LONG	NUMBER'
        );  
end;
/

/*
    Create tables from external tables
*/
exec dbms_output.put_line(systimestamp || ' - create custsales')
create table custsales as select * from ext_custsales;

exec dbms_output.put_line(systimestamp || ' - create movie')
create table movie as
select 
    cast(m.doc.movie_id as number) as movie_id,
    cast(m.doc.title as varchar2(200 byte)) as title,    
    cast(m.doc.budget as number) as budget,
    cast(m.doc.gross as number) gross,
    cast(m.doc.list_price as number) as list_price,
    cast(m.doc.genre as varchar2(4000)) as genre,
    cast(m.doc.sku as varchar2(30 byte)) as sku,    
    cast(m.doc.year as number) as year,
    to_date(m.doc.opening_date, 'YYYY-MM-DD') as opening_date,
    cast(m.doc.views as number) as views,
    cast(m.doc.cast as varchar2(4000 byte)) as cast, 
    cast(m.doc.crew as varchar2(4000 byte)) as crew,
    cast(m.doc.studio as varchar2(4000 byte)) as studio,
    cast(m.doc.main_subject as varchar2(4000 byte)) as main_subject,
    cast(m.doc.awards as varchar2(4000 byte)) as awards,
    cast(m.doc.nominations as varchar2(4000 byte)) as nominations,
    cast(m.doc.runtime as number) as runtime,
    substr(cast(m.doc.summary as varchar2(4000 byte)),1, 4000) as summary
from ext_movie m
where rownum < 10;

alter table movie add constraint pk_movie_cust_id primary key("MOVIE_ID");
alter table movie add CONSTRAINT movie_cast_json CHECK (cast IS JSON);
alter table movie add CONSTRAINT movie_genre_json CHECK (genre IS JSON);
alter table movie add CONSTRAINT movie_crew_json CHECK (crew IS JSON);
alter table movie add CONSTRAINT movie_studio_json CHECK (studio IS JSON);
alter table movie add CONSTRAINT movie_awards_json CHECK (awards IS JSON);
alter table movie add CONSTRAINT movie_nominations_json CHECK (nominations IS JSON);

exec dbms_output.put_line(systimestamp || ' - create genre')
create table genre as select * from ext_genre;

exec dbms_output.put_line(systimestamp || ' - create customer_segment')
create table customer_segment as select * from ext_customer_segment;

exec dbms_output.put_line(systimestamp || ' - create customer')
create table customer as select * from ext_customer;
alter table customer add constraint pk_cust_cust_id primary key("CUST_ID");


-- View combining data
exec dbms_output.put_line(systimestamp || ' - create view v_custsales')
CREATE OR REPLACE VIEW v_custsales AS
SELECT
    cs.day,
    c.cust_id,
    c.last_name,
    c.first_name,
    c.city,
    c.state_province,
    c.country,
    c.continent,
    c.age,
    c.commute_distance,
    c.credit_balance,
    c.education,
    c.full_time,
    c.gender,
    c.household_size,
    c.income,
    c.income_level,
    c.insuff_funds_incidents,
    c.job_type,
    c.late_mort_rent_pmts,
    c.marital_status,
    c.mortgage_amt,
    c.num_cars,
    c.num_mortgages,
    c.pet,
    c.promotion_response,
    c.rent_own,
    c.work_experience,
    c.yrs_current_employer,
    c.yrs_customer,
    c.yrs_residence,
    c.loc_lat,
    c.loc_long,    
    cs.app,
    cs.device,
    cs.os,
    cs.payment_method,
    cs.list_price,
    cs.discount_type,
    cs.discount_percent,
    cs.actual_price,
    1 as transactions,
    s.short_name as segment,
    g.name as genre,
    m.title,
    m.budget,
    m.gross,
    m.genre as genre_list,
    m.sku,
    m.year,
    m.opening_date,
    m.cast,
    m.crew,
    m.studio,
    m.main_subject,
    nvl(json_value(m.awards,'$.size()'),0) awards,
    nvl(json_value(m.nominations,'$.size()'),0) nominations,
    m.runtime
FROM
    genre g, customer c, custsales cs, customer_segment s, movie m
WHERE
     cs.movie_id = m.movie_id
AND  cs.genre_id = g.genre_id
AND  cs.cust_id = c.cust_id
AND  c.segment_id = s.segment_id;


-- Create MV over custsales
exec dbms_output.put_line(systimestamp || ' - create materialized view')
drop materialized view mv_custsales;
create materialized view mv_custsales
build immediate
refresh complete
as SELECT
    cs.day,
    c.cust_id,
    c.last_name,
    c.first_name,
    c.city,
    c.state_province,
    c.country,
    c.continent,
    c.age,
    c.commute_distance,
    c.credit_balance,
    c.education,
    c.full_time,
    c.gender,
    c.household_size,
    c.income,
    c.income_level,
    c.insuff_funds_incidents,
    c.job_type,
    c.late_mort_rent_pmts,
    c.marital_status,
    c.mortgage_amt,
    c.num_cars,
    c.num_mortgages,
    c.pet,
    c.promotion_response,
    c.rent_own,
    c.work_experience,
    c.yrs_current_employer,
    c.yrs_customer,
    c.yrs_residence,
    c.loc_lat,
    c.loc_long,    
    cs.app,
    cs.device,
    cs.os,
    cs.payment_method,
    cs.list_price,
    cs.discount_type,
    cs.discount_percent,
    cs.actual_price,
    1 as transactions,
    s.short_name as segment,
    g.name as genre,
    m.title,
    m.budget,
    m.gross,
    m.genre as genre_list,
    m.sku,
    m.year,
    m.opening_date,
    m.cast,
    m.crew,
    m.studio,
    m.main_subject,
    nvl(json_value(m.awards,'$.size()'),0) awards,
    nvl(json_value(m.nominations,'$.size()'),0) nominations,
    m.runtime
FROM
    genre g, customer c, custsales cs, customer_segment s, movie m
WHERE
     cs.movie_id = m.movie_id
AND  cs.genre_id = g.genre_id
AND  cs.cust_id = c.cust_id
AND  c.segment_id = s.segment_id;
 
alter table mv_custsales add CONSTRAINT cs_cast_json CHECK (cast IS JSON);
alter table mv_custsales add CONSTRAINT cs_crew_json CHECK (crew IS JSON);
alter table mv_custsales add CONSTRAINT cs_studio_json CHECK (studio IS JSON);
