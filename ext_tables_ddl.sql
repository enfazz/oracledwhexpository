ALTER SESSION SET "_ORACLE_SCRIPT" = true;
CREATE USER sa_olist_dataset IDENTIFIED BY 2334
    DEFAULT TABLESPACE users
    TEMPORARY TABLESPACE temp
    PROFILE default;
GRANT connect, resource TO sa_olist_dataset;
GRANT UNLIMITED TABLESPACE TO sa_olist_dataset;
COMMIT;

GRANT READ, WRITE ON DIRECTORY ext_tab TO kiryl_navitski; 
COMMIT;

CONNECT kiryl_navitski;

CREATE TABLE sa_olist_dataset.seller_ext
(SELLER_ID_SRC varchar (255),
SELLER_TELEPHONE_NUM varchar (255),
SELLER_EMAIL varchar (255)
)
ORGANIZATION external (
        DEFAULT DIRECTORY ext_tab
         ACCESS PARAMETERS 
         (RECORDS DELIMITED BY NEWLINE SKIP 1
         FIELDS TERMINATED BY ';'
        )
LOCATION ('olist_sellers_dataset.csv'));
COMMIT;

-- DROP TABLE sa_olist_dataset.seller_ext;
-- SELECT * FROM sa_olist_dataset.seller_ext;

CREATE TABLE sa_olist_dataset.customer_ext
(customer_unique_id varchar (255),
customer_first_name varchar (255),
customer_last_name varchar (255),
customer_telephone_num varchar (255),
customer_email varchar (255)
)
ORGANIZATION external (
        DEFAULT DIRECTORY ext_tab
         ACCESS PARAMETERS 
         (RECORDS DELIMITED BY NEWLINE SKIP 1
         FIELDS TERMINATED BY ';'
        )
LOCATION ('olist_customers_dataset.csv'));
COMMIT;

-- DROP TABLE sa_olist_dataset.customer_ext;
-- SELECT * FROM sa_olist_dataset.customer_ext;

CREATE TABLE sa_olist_dataset.country_ext
(country_id varchar (255),
country_name varchar (255),
country_abbrev varchar (255),
country_emerg_id varchar (255)
)
ORGANIZATION external (
        DEFAULT DIRECTORY ext_tab
         ACCESS PARAMETERS 
         (RECORDS DELIMITED BY NEWLINE SKIP 1
         FIELDS TERMINATED BY ';'
        )
LOCATION ('geo_country.csv'));
COMMIT;

-- DROP TABLE sa_olist_dataset.country_ext;
-- SELECT * FROM sa_olist_dataset.country_ext;

CREATE TABLE sa_olist_dataset.geolocation_ext
(geolocation_country_id varchar (255),
geolocation_country_name varchar (255),
geolocation_zip_code_prefix varchar (255),
geolocation_lat varchar (255),
geolocation_lng varchar (255),
geolocation_city varchar (255)
)
ORGANIZATION external (
        DEFAULT DIRECTORY ext_tab
         ACCESS PARAMETERS 
         (RECORDS DELIMITED BY NEWLINE SKIP 1
         FIELDS TERMINATED BY ';'
        )
LOCATION ('olist_geolocation_dataset.csv'));
COMMIT;

-- DROP TABLE sa_olist_dataset.geolocation_ext;
-- SELECT * FROM sa_olist_dataset.geolocation_ext;

CREATE TABLE sa_generated_dataset.employees_ext
(employee_id varchar (255),
first_name varchar (255),
last_name varchar (255),
telephone_num varchar (255),
position_name varchar (255)
)
ORGANIZATION external (
        DEFAULT DIRECTORY ext_tab
         ACCESS PARAMETERS 
         (RECORDS DELIMITED BY NEWLINE SKIP 1
         FIELDS TERMINATED BY ';'
        )
LOCATION ('generated_employees_dataset.csv'));
COMMIT;

CREATE TABLE sa_generated_dataset.stores_ext
(store_id varchar (255),
store_name varchar (255),
tel_num varchar(255),
location_id varchar(255)
)
ORGANIZATION external (
        DEFAULT DIRECTORY ext_tab
         ACCESS PARAMETERS 
         (RECORDS DELIMITED BY NEWLINE SKIP 1
         FIELDS TERMINATED BY ';'
        )
LOCATION ('generated_stores_dataset.csv'));
COMMIT;

-- DROP TABLE sa_generated_dataset.stores_ext;

CREATE TABLE sa_olist_dataset.product_ext
(product_id varchar (255),
product_name varchar (255),
product_category_name varchar (255),
product_weight_g varchar (255),
product_length_cm varchar (255),
product_height_cm varchar (255),
product_width_cm varchar (255)
)
ORGANIZATION external (
        DEFAULT DIRECTORY ext_tab
         ACCESS PARAMETERS 
         (RECORDS DELIMITED BY NEWLINE SKIP 1
         FIELDS TERMINATED BY ';'
        )
LOCATION ('olist_products_dataset.csv'));
COMMIT;

-- DROP TABLE sa_olist_dataset.product_ext;
-- SELECT * FROM sa_olist_dataset.product_ext;

CREATE TABLE sa_olist_dataset.order_item_ext
(order_id varchar (255),
order_item_id varchar (255),
product_id varchar (255),
seller_id varchar (255),
shipping_limit_date  varchar (255),
price varchar (255),
geolocation_lat varchar (255),
geolocation_lng varchar (255),
store_id varchar (255),
employee_id varchar (255),
customer_id varchar (255),
customer_unique_id varchar (255),
order_purchase_timestamp varchar (255)
)
ORGANIZATION external (
        DEFAULT DIRECTORY ext_tab
         ACCESS PARAMETERS 
         (RECORDS DELIMITED BY NEWLINE SKIP 1
         FIELDS TERMINATED BY ','
        )
LOCATION ('order_item.csv'));
COMMIT;

CREATE TABLE sa_olist_dataset.olist_cities_ext
(geolocation_city varchar (255),
geolocation_state varchar (255)
)
ORGANIZATION external (
        DEFAULT DIRECTORY ext_tab
         ACCESS PARAMETERS 
         (RECORDS DELIMITED BY NEWLINE SKIP 1
         FIELDS TERMINATED BY ';'
        )
LOCATION ('olist_cities.csv'));
COMMIT;

CREATE TABLE sa_olist_dataset.olist_regions_ext
(geolocation_state varchar (255),
geolocation_country_name varchar (255),
country_emerg_id varchar (255)
)
ORGANIZATION external (
        DEFAULT DIRECTORY ext_tab
         ACCESS PARAMETERS 
         (RECORDS DELIMITED BY NEWLINE SKIP 1
         FIELDS TERMINATED BY ';'
        )
LOCATION ('olist_regions.csv'));
COMMIT;

CREATE TABLE sa_olist_dataset.customer_unique_ext
(customer_id varchar (255),
customer_unique_id varchar (255)
)
ORGANIZATION external (
        DEFAULT DIRECTORY ext_tab
         ACCESS PARAMETERS 
         (RECORDS DELIMITED BY NEWLINE SKIP 1
         FIELDS TERMINATED BY ';'
        )
LOCATION ('olist_customer_to_customer_unique_dataset.csv'));
COMMIT;