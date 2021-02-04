CREATE SEQUENCE bl_dm.seq_dim_customer_id ORDER;
COMMIT;

CREATE TABLE dim_customer (
    customer_id             NUMBER DEFAULT bl_dm.seq_dim_customer_id.NEXTVAL NOT NULL,
    customer_id_src         VARCHAR2(255),
    customer_first_name     VARCHAR2(255),
    customer_last_name      VARCHAR2(255),
    customer_telephone_num  VARCHAR2(255),
    customer_email          VARCHAR2(255),
    customer_source         VARCHAR2(255),
    insert_dt               DATE DEFAULT sysdate,
    update_dt               DATE
);
ALTER TABLE dim_customer ADD CONSTRAINT dim_customer_pk PRIMARY KEY ( customer_id );

CREATE OR REPLACE TRIGGER trg_dim_customer_update_dt
  BEFORE INSERT OR UPDATE ON bl_dm.dim_customer
  FOR EACH ROW
BEGIN
  :new.update_dt := sysdate;
END;

CREATE SEQUENCE bl_dm.seq_dim_location_id ORDER;
COMMIT;

CREATE TABLE dim_location (
    location_id              NUMBER DEFAULT bl_dm.seq_dim_location_id.NEXTVAL,
    location_id_src          NUMBER,
    location_lat             VARCHAR2(255),
    location_ltd             VARCHAR2(255),
    location_postal_code     VARCHAR2(255),
    location_city_id         NUMBER,
    location_city_name       VARCHAR2(255),
    location_region_id       NUMBER,
    location_region_name     VARCHAR2(255),
    location_country_id      NUMBER,
    location_country_id_src  VARCHAR2(255),
    location_country_name    VARCHAR2(255),
    location_source          VARCHAR2(255),
    insert_dt                DATE DEFAULT SYSDATE,
    update_dt                DATE
);
ALTER TABLE dim_location ADD CONSTRAINT dim_geo_pk PRIMARY KEY ( location_id );

CREATE OR REPLACE TRIGGER trg_dim_location_update_dt
  BEFORE INSERT OR UPDATE ON bl_dm.dim_location
  FOR EACH ROW
BEGIN
  :new.update_dt := sysdate;
END;

-- DROP TABLE dim_location;

CREATE SEQUENCE bl_dm.seq_dim_seller_id ORDER;
COMMIT;

CREATE TABLE dim_seller (
    seller_id             NUMBER DEFAULT bl_dm.seq_dim_seller_id.NEXTVAL,
    seller_id_src         VARCHAR2 (255),
    seller_telephone_num  VARCHAR2(255),
    seller_email          VARCHAR2(255),
    seller_source         VARCHAR2(255),
    insert_dt             DATE DEFAULT SYSDATE,
    update_dt             DATE
);
ALTER TABLE dim_seller ADD CONSTRAINT dim_payment_pk PRIMARY KEY ( seller_id );

CREATE OR REPLACE TRIGGER trg_dim_seller_update_dt
  BEFORE INSERT OR UPDATE ON bl_dm.dim_seller
  FOR EACH ROW
BEGIN
  :new.update_dt := sysdate;
END;

CREATE TABLE bl_dm.dim_time (
    time_date_id NUMBER, 
	time_date DATE, 
	time_day_of_week NUMBER, 
	time_day_num_in_month NUMBER, 
	time_day_num_overall NUMBER, 
	time_day_name VARCHAR2(100 BYTE), 
	time_day_abbrev VARCHAR2(3 BYTE), 
	time_weekday_flag VARCHAR2(100 BYTE), 
	time_week_num_in_year NUMBER, 
	time_week_num_overall NUMBER, 
	time_week_begin_date DATE, 
	time_week_begin_date_id NUMBER, 
	time_month NUMBER, 
	time_month_num_overall NUMBER, 
	time_month_name VARCHAR2(100 BYTE), 
	time_month_abbrev VARCHAR2(3 BYTE), 
    time_quarter NUMBER, 
	time_year NUMBER, 
	time_yearmo NUMBER, 
	time_fiscal_month NUMBER, 
	time_fiscal_quarter NUMBER, 
	time_month_end_flag VARCHAR2(100 BYTE), 
	time_same_day_year_ago DATE
   );
ALTER TABLE dim_time ADD CONSTRAINT dim_time_pk PRIMARY KEY ( time_date_id );

CREATE SEQUENCE bl_dm.seq_dim_store_id ORDER;
COMMIT;

CREATE TABLE dim_store (
    store_id             NUMBER DEFAULT bl_dm.seq_dim_store_id.NEXTVAL NOT NULL,
    store_id_src         VARCHAR2(255),
    store_name           VARCHAR2(255),
    store_telephone_num  VARCHAR2(255),
    store_source         VARCHAR2(255),
    insert_dt            DATE DEFAULT SYSDATE,
    update_dt            DATE
);
ALTER TABLE dim_store ADD CONSTRAINT dim_store_pk PRIMARY KEY ( store_id );

CREATE SEQUENCE bl_dm.seq_dim_employee_id ORDER;
COMMIT;

CREATE SEQUENCE bl_dm.seq_dim_position_id ORDER;
COMMIT;

CREATE TABLE dim_employee_scd (
    employee_id                NUMBER DEFAULT bl_dm.seq_dim_employee_id.NEXTVAL NOT NULL,
    employee_id_src            VARCHAR2(255),
    employee_first_name        VARCHAR2(255),
    employee_last_name         VARCHAR2(255) ,
    employee_telephone_num     VARCHAR2(255),
    employee_position_id       NUMBER DEFAULT bl_dm.seq_dim_position_id.NEXTVAL NOT NULL,
    employee_position_name     VARCHAR2(255),
    employee_eff_sop           DATE,
    employee_eff_eop           DATE,
    employee_source_name       VARCHAR2(255),
    is_active                  VARCHAR2(255) DEFAULT 'ACTIVE',
    insert_dt                  DATE DEFAULT SYSDATE,
    update_dt                  DATE
);
ALTER TABLE dim_employee_scd ADD CONSTRAINT dim_employees_pk PRIMARY KEY ( employee_id );

CREATE OR REPLACE TRIGGER trg_dim_employee_update_dt
  BEFORE INSERT OR UPDATE ON bl_dm.dim_employee_scd
  FOR EACH ROW
BEGIN
  :new.update_dt := sysdate;
END;

CREATE SEQUENCE bl_dm.seq_dim_product_id ORDER;
COMMIT;

CREATE SEQUENCE bl_dm.seq_dim_category_id ORDER;
COMMIT;

CREATE TABLE dim_product_scd (
    product_id               NUMBER DEFAULT bl_dm.seq_dim_product_id.NEXTVAL NOT NULL,
    product_id_src           VARCHAR2(255),
    product_name             VARCHAR2(255),
    product_category_id      NUMBER DEFAULT bl_dm.seq_dim_category_id.NEXTVAL,
    product_category_name    VARCHAR2(255),
    product_lenght           VARCHAR2(255),
    product_width            VARCHAR2(255),
    product_height           VARCHAR2(255),
    product_weight           VARCHAR2(255),
    product_sop              DATE,
    product_eop              DATE,
    product_source           VARCHAR2(255),
    is_active                VARCHAR2(255) DEFAULT 'ACTIVE',
    insert_dt                DATE DEFAULT SYSDATE,
    update_dt                DATE
);
ALTER TABLE dim_product_scd ADD CONSTRAINT dim_product_pk PRIMARY KEY ( product_id ); 

CREATE OR REPLACE TRIGGER trg_dim_product_scd_update_dt
  BEFORE INSERT OR UPDATE ON bl_dm.dim_product_scd
  FOR EACH ROW
BEGIN
  :new.update_dt := sysdate;
END;

CREATE SEQUENCE bl_dm.seq_dim_sales_id ORDER;
COMMIT; 

CREATE TABLE fct_sales (
    sale_id         NUMBER DEFAULT bl_dm.seq_dim_sales_id.NEXTVAL NOT NULL,
    sale_id_src     VARCHAR2(255),
    product_id      NUMBER NOT NULL,
    customer_id     NUMBER NOT NULL,
    store_id        NUMBER NOT NULL,
    seller_id       NUMBER NOT NULL,
    location_id     NUMBER NOT NULL,
    employee_id     NUMBER NOT NULL,
    unit_price      VARCHAR2(255),
    sale_timestamp  TIMESTAMP, 
    sale_source     VARCHAR2(255),
    insert_dt       DATE DEFAULT SYSDATE,
    update_dt       DATE
)
PARTITION BY RANGE (sale_timestamp)
INTERVAL (NUMTOYMINTERVAL  (3, 'MONTH'))
    (
     PARTITION y2016q4 VALUES LESS THAN (TO_DATE('01-01-2017', 'MM-DD-YYYY')),
     PARTITION y2017q1 VALUES LESS THAN (TO_DATE('04-01-2017', 'MM-DD-YYYY')),
     PARTITION y2017q2 VALUES LESS THAN (TO_DATE('07-01-2017', 'MM-DD-YYYY')),
     PARTITION y2017q3 VALUES LESS THAN (TO_DATE('10-01-2017', 'MM-DD-YYYY')),
     PARTITION y2017q4 VALUES LESS THAN (TO_DATE('01-01-2018', 'MM-DD-YYYY')),
     PARTITION y2018q1 VALUES LESS THAN (TO_DATE('04-01-2018', 'MM-DD-YYYY')),
     PARTITION y2018q2 VALUES LESS THAN (TO_DATE('07-01-2018', 'MM-DD-YYYY')),
     PARTITION y2018q3 VALUES LESS THAN (TO_DATE('10-01-2018', 'MM-DD-YYYY')),
     PARTITION y2018q4 VALUES LESS THAN (TO_DATE('01-01-2019', 'MM-DD-YYYY'))
    );
ALTER TABLE fct_sales ADD CONSTRAINT fct_sales_pk PRIMARY KEY ( sale_id );
ALTER TABLE fct_sales
    ADD CONSTRAINT sales_location_fk FOREIGN KEY ( location_id )
        REFERENCES dim_location ( location_id );
ALTER TABLE fct_sales
    ADD CONSTRAINT sales_customer_fk FOREIGN KEY ( customer_id )
        REFERENCES dim_customer ( customer_id );
ALTER TABLE fct_sales
    ADD CONSTRAINT sales_employee_fk FOREIGN KEY ( employee_id )
        REFERENCES dim_employee_scd ( employee_id );
ALTER TABLE fct_sales
    ADD CONSTRAINT sales_product_fk FOREIGN KEY ( product_id )
        REFERENCES dim_product_scd ( product_id );
ALTER TABLE fct_sales
    ADD CONSTRAINT sales_store_fk FOREIGN KEY ( store_id )
        REFERENCES dim_store ( store_id );
ALTER TABLE fct_sales
    ADD CONSTRAINT sales_seller_fk FOREIGN KEY ( seller_id )
        REFERENCES dim_seller ( seller_id );
        
CREATE OR REPLACE TRIGGER trg_fct_sales_update_dt
  BEFORE INSERT OR UPDATE ON bl_dm.fct_sales
  FOR EACH ROW
BEGIN
  :new.update_dt := sysdate;
END;    

CREATE TABLE fct_sales_exchange (
    sale_id         NUMBER DEFAULT bl_dm.seq_dim_sales_id.NEXTVAL NOT NULL,
    sale_id_src     VARCHAR2(255),
    product_id      NUMBER NOT NULL,
    customer_id     NUMBER NOT NULL,
    store_id        NUMBER NOT NULL,
    seller_id       NUMBER NOT NULL,
    location_id     NUMBER NOT NULL,
    employee_id     NUMBER NOT NULL,
    unit_price      VARCHAR2(255),
    sale_timestamp  TIMESTAMP, 
    sale_source     VARCHAR2(255),
    insert_dt       DATE DEFAULT SYSDATE,
    update_dt       DATE
);
    
CREATE OR REPLACE TRIGGER trg_bl_dm_fct_sales_update_dt
  BEFORE INSERT OR UPDATE ON bl_dm.fct_sales_exchange
  FOR EACH ROW
BEGIN
  :new.update_dt := sysdate;
END;    
