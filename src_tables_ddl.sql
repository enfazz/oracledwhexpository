CREATE TABLE sa_olist_dataset.src_seller
(SELLER_ID_SRC varchar (255),
SELLER_TELEPHONE_NUM varchar (255),
SELLER_EMAIL varchar (255));
COMMIT;

-- DROP TABLE sa_olist_dataset.src_seller;

CREATE TABLE sa_olist_dataset.src_customer
(customer_unique_id varchar (255),
customer_first_name varchar (255),
customer_last_name varchar (255),
customer_telephone_num varchar (255),
customer_email varchar (255));
COMMIT;

-- DROP TABLE sa_olist_dataset.src_customer;

CREATE TABLE sa_olist_dataset.src_country
(country_id varchar (255),
country_name varchar (255),
country_abbrev varchar (255),
geolocation_emerg_id varchar (255));
COMMIT;

-- DROP TABLE sa_olist_dataset.src_country;

CREATE TABLE sa_olist_dataset.src_cities
(geolocation_city varchar (255),
geolocation_state varchar (255)
);
COMMIT;

CREATE TABLE sa_olist_dataset.src_regions
(geolocation_state varchar (255),
geolocation_country_name varchar (255),
geolocation_emerg_id varchar (255)
);
COMMIT;

CREATE TABLE sa_olist_dataset.src_geolocation
(geolocation_country_id varchar (255),
geolocation_country_name varchar (255),
geolocation_zip_code_prefix varchar (255),
geolocation_lat varchar (255),
geolocation_lng varchar (255),
geolocation_city varchar (255)
);
COMMIT;

-- DROP TABLE sa_olist_dataset.src_geolocation;

CREATE TABLE sa_generated_dataset.src_employees
(employee_id varchar (255),
employee_first_name varchar (255),
employee_last_name varchar (255),
employee_telephone_num varchar (255),
employee_position_name varchar (255));
COMMIT;

CREATE TABLE sa_generated_dataset.src_stores
(store_id varchar (255),
store_name varchar (255),
tel_num varchar (255),
location_id varchar(255));
COMMIT;

-- DROP TABLE sa_generated_dataset.src_stores;

CREATE TABLE sa_olist_dataset.src_product
(product_id varchar (255),
product_name varchar (255),
product_category_name varchar (255),
product_weight_g varchar (255),
product_length_cm varchar (255),
product_height_cm varchar (255),
product_width_cm varchar (255)
);
COMMIT;

-- DROP TABLE sa_olist_dataset.src_product;

CREATE TABLE sa_olist_dataset.src_order_item
(order_id varchar (255),
order_item_id varchar (255),
product_id varchar (255),
seller_id varchar (255),
shipping_limit_date  varchar (255),
price varchar(255),
geolocation_lat varchar (255),
geolocation_lng varchar (255),
store_id varchar (255),
employee_id varchar (255),
customer_id varchar (255),
customer_unique_id varchar (255),
order_purchase_timestamp varchar (255)
);

CREATE TABLE sa_olist_dataset.src_order_customer
(
order_id varchar (255),
customer_id varchar (255),
order_status varchar (255),
order_purchase_timestamp varchar (255),
order_approved_at varchar (255),
order_delivered_carrier_date varchar (255),
order_delivered_customer_date varchar (255),
order_estimated_delivery_date varchar (255)
);

CREATE TABLE sa_olist_dataset.src_order_payment
(
order_id varchar (255),
payment_sequential varchar (255),
payment_type varchar (255),
payment_installments varchar (255),
payment_value varchar (255)
);

CREATE TABLE sa_olist_dataset.src_customer_unique
(customer_id varchar (255),
customer_unique_id varchar (255)
);