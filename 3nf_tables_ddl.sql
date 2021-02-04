CREATE SEQUENCE tab_3NF.seq_country_id ORDER;
COMMIT;

-- DROP SEQUENCE tab_3NF.seq_country_id;

CREATE TABLE tab_3NF.ce_country (
    country_id      NUMBER DEFAULT tab_3NF.seq_country_id.NEXTVAL,
    country_id_src  VARCHAR2(100),
    country_name    VARCHAR2(100),
    country_emerg_id VARCHAR2(100),
    country_source  VARCHAR2(100),
    insert_dt       DATE DEFAULT sysdate NOT NULL,
    update_dt       DATE
);
ALTER TABLE tab_3NF.ce_country ADD CONSTRAINT country_pk PRIMARY KEY ( country_id );
COMMIT;

CREATE OR REPLACE TRIGGER trg_ce_country_update_dt
  BEFORE INSERT OR UPDATE ON tab_3NF.ce_country
  FOR EACH ROW
BEGIN
  :new.update_dt := sysdate;
END;

-- DROP TABLE tab_3NF.ce_country;

CREATE SEQUENCE tab_3NF.seq_region_id1 ORDER;
COMMIT;

-- DROP SEQUENCE tab_3NF.seq_tegion_id1;

CREATE TABLE tab_3NF.ce_region (
    region_id          NUMBER DEFAULT tab_3NF.seq_region_id1.NEXTVAL,
    region_abbrev      VARCHAR2(255),
    region_country_id  NUMBER NOT NULL,
    region_source      VARCHAR2(255),
    insert_dt          DATE DEFAULT sysdate NOT NULL,
    update_dt          DATE
);
ALTER TABLE ce_region ADD CONSTRAINT region_pk PRIMARY KEY ( region_id );
ALTER TABLE ce_region
    ADD CONSTRAINT region_country_fk FOREIGN KEY ( region_country_id )
        REFERENCES ce_country ( country_id );
COMMIT;

CREATE OR REPLACE TRIGGER trg_ce_region_update_dt
  BEFORE INSERT OR UPDATE ON tab_3NF.ce_region
  FOR EACH ROW
BEGIN
  :new.update_dt := sysdate;
END;

-- DROP TRIGGER trg_ce_region_update_dt;
-- DROP TABLE tab_3NF.ce_region;

CREATE SEQUENCE tab_3NF.seq_city_id ORDER;
COMMIT;

CREATE TABLE tab_3NF.ce_city (
    city_id                NUMBER DEFAULT tab_3NF.seq_city_id.NEXTVAL,
    city_name              VARCHAR2(255),
    city_region_id         NUMBER NOT NULL,
    city_source            VARCHAR2(255),
    insert_dt              DATE DEFAULT sysdate NOT NULL,
    update_dt              DATE
);
ALTER TABLE ce_city ADD CONSTRAINT city_pk PRIMARY KEY ( city_id );
ALTER TABLE ce_city
    ADD CONSTRAINT city_region_fk FOREIGN KEY ( city_region_id )
        REFERENCES ce_region ( region_id );
COMMIT;

CREATE OR REPLACE TRIGGER trg_ce_city_update_dt
  BEFORE INSERT OR UPDATE ON tab_3NF.ce_city
  FOR EACH ROW
BEGIN
  :new.update_dt := sysdate;
END;

-- DROP TRIGGER trg_ce_city_update_dt;
-- DROP TABLE tab_3NF.ce_city;

CREATE SEQUENCE tab_3NF.seq_location_id;
COMMIT;

CREATE TABLE tab_3NF.ce_location (
    location_id           NUMBER DEFAULT tab_3NF.seq_location_id.NEXTVAL,
    location_lat          VARCHAR2(100),
    location_ltd          VARCHAR2(100),
    location_postal_code  VARCHAR2(100),
    location_city_id      NUMBER NOT NULL,
    location_source       VARCHAR2(100),
    insert_dt             DATE DEFAULT sysdate NOT NULL,
    update_dt             DATE
);
ALTER TABLE ce_location ADD CONSTRAINT address_pk PRIMARY KEY ( location_id );
ALTER TABLE ce_location
    ADD CONSTRAINT address_city_fk FOREIGN KEY ( location_city_id )
        REFERENCES ce_city ( city_id );
COMMIT;


CREATE OR REPLACE TRIGGER trg_ce_location_update_dt
  BEFORE INSERT OR UPDATE ON tab_3NF.ce_location
  FOR EACH ROW
BEGIN
  :new.update_dt := sysdate;
END;

-- DROP TRIGGER trg_ce_location_update_dt;
-- DROP TABLE tab_3NF.ce_location;

CREATE SEQUENCE tab_3NF.seq_customer_id;
COMMIT;

CREATE TABLE tab_3NF.ce_customer (
    customer_id             NUMBER DEFAULT tab_3NF.seq_customer_id.NEXTVAL,
    customer_id_src         VARCHAR2(100),
    customer_first_name     VARCHAR2(100), 
    customer_last_name      VARCHAR2(100), 
    customer_telephone_num  VARCHAR2(100), 
    customer_email          VARCHAR2(100),
    customer_source         VARCHAR2(100),
    insert_dt               DATE DEFAULT sysdate NOT NULL,
    update_dt               DATE
);
ALTER TABLE ce_customer ADD CONSTRAINT customer_pk PRIMARY KEY ( customer_id );
COMMIT;

CREATE OR REPLACE TRIGGER trg_ce_customer_update_dt
  BEFORE INSERT OR UPDATE ON tab_3NF.ce_customer
  FOR EACH ROW
BEGIN
  :new.update_dt := sysdate;
END;

-- DROP TABLE tab_3NF.ce_customer;
-- DROP TRIGGER trg_ce_customer_update_dt;


CREATE SEQUENCE seq_product_id;
COMMIT;

CREATE SEQUENCE tab_3Nf.seq_seller_id;
COMMIT;

CREATE TABLE ce_seller (
    seller_id             NUMBER DEFAULT tab_3Nf.seq_seller_id.NEXTVAL,
    seller_id_src         VARCHAR2(100),
    seller_telephone_num  VARCHAR2(100),
    seller_email          VARCHAR2(100),
    seller_source         VARCHAR2(100),
    insert_dt             DATE DEFAULT sysdate NOT NULL,
    update_dt             DATE
);
ALTER TABLE ce_seller ADD CONSTRAINT warehouse_pk PRIMARY KEY ( seller_id );
COMMIT;

CREATE OR REPLACE TRIGGER trg_ce_seller_update_dt
  BEFORE INSERT OR UPDATE ON tab_3NF.ce_seller
  FOR EACH ROW
BEGIN
  :new.update_dt := sysdate;
END;

-- DROP TABLE ce_seller;

CREATE SEQUENCE tab_3NF.seq_position_id ORDER;
COMMIT;

CREATE TABLE ce_position (
    position_id       NUMBER DEFAULT tab_3NF.seq_position_id.NEXTVAL,
    position_name     VARCHAR2(255), 
    position_source   VARCHAR2(255),
    insert_dt         DATE DEFAULT sysdate,
    update_dt         DATE
);
ALTER TABLE ce_position ADD CONSTRAINT position_pk PRIMARY KEY ( position_id );
COMMIT;

CREATE OR REPLACE TRIGGER trg_ce_position_update_dt
  BEFORE INSERT OR UPDATE ON tab_3NF.ce_position
  FOR EACH ROW
BEGIN
  :new.update_dt := sysdate;
END;

-- DROP TABLE ce_position;

CREATE SEQUENCE tab_3NF.seq_employee_id ORDER;
COMMIT;

CREATE TABLE ce_employee (
    employee_id                NUMBER DEFAULT tab_3NF.seq_employee_id.NEXTVAL,
    employee_id_src            VARCHAR2(255),
    employee_first_name        VARCHAR2(255), 
    employee_last_name         VARCHAR2(255), 
    employee_telephone_num     VARCHAR2(255), 
    employee_position_id       NUMBER NOT NULL,
    employee_source            VARCHAR2(255),
    employee_eff_sop           DATE,
    employee_eff_eop           DATE,
    is_active                  VARCHAR2(255) DEFAULT 'Active',
    insert_dt                  DATE DEFAULT sysdate,
    update_dt                  DATE
);
ALTER TABLE ce_employee ADD CONSTRAINT employee_pk PRIMARY KEY ( employee_id );
ALTER TABLE ce_employee
    ADD CONSTRAINT employee_position_fk FOREIGN KEY ( employee_position_id )
        REFERENCES ce_position ( position_id );
COMMIT;

CREATE OR REPLACE TRIGGER trg_ce_employee_update_dt
  BEFORE INSERT OR UPDATE ON tab_3NF.ce_employee
  FOR EACH ROW
BEGIN
  :new.update_dt := sysdate;
END;

-- DROP TABLE ce_employee;

CREATE SEQUENCE tab_3NF.seq_store_id ORDER;
COMMIT;

CREATE TABLE ce_store (
    store_id                NUMBER DEFAULT tab_3NF.seq_store_id.NEXTVAL,
    store_id_src            VARCHAR2(255),
    store_name              VARCHAR2(255), 
    store_telephone_number  VARCHAR2(255),
    store_source            VARCHAR2(255),
    insert_dt               DATE DEFAULT sysdate,
    update_dt               DATE
);
ALTER TABLE ce_store ADD CONSTRAINT store_pk PRIMARY KEY ( store_id );
        
CREATE OR REPLACE TRIGGER trg_ce_store_update_dt
  BEFORE INSERT OR UPDATE ON tab_3NF.ce_store
  FOR EACH ROW
BEGIN
  :new.update_dt := sysdate;
END;

-- DROP TABLE ce_store;

CREATE SEQUENCE tab_3NF.seq_category_id;
COMMIT;

CREATE TABLE tab_3NF.ce_category (
    category_id      NUMBER DEFAULT tab_3NF.seq_category_id.NEXTVAL,
    category_name    VARCHAR2(255), 
    category_source  VARCHAR2(255), 
    insert_dt        DATE DEFAULT sysdate NOT NULL,
    update_dt        DATE
);
ALTER TABLE ce_category ADD CONSTRAINT category_pk PRIMARY KEY ( category_id );
COMMIT;

CREATE OR REPLACE TRIGGER trg_ce_category_update_dt
  BEFORE INSERT OR UPDATE ON tab_3NF.ce_category
  FOR EACH ROW
BEGIN
  :new.update_dt := sysdate;
END;

-- DROP TABLE tab_3NF.ce_category;
-- DROP TRIGGER trg_ce_category_update_dt;

CREATE SEQUENCE tab_3NF.seq_product_id ORDER;
COMMIT;

CREATE TABLE ce_product (
    product_id           NUMBER DEFAULT tab_3NF.seq_product_id.NEXTVAL,
    product_id_src       VARCHAR2(255),
    product_name         VARCHAR2(255),
    product_cat_id       NUMBER NOT NULL,
    product_len          VARCHAR2(255),
    product_wid          VARCHAR2(255),
    product_hei          VARCHAR2(255),
    product_wei          VARCHAR2(255),
    product_sop          DATE,
    product_eop          DATE,
    product_is_active    VARCHAR2(255) DEFAULT 'Active',
    product_source       VARCHAR2(255),
    insert_dt            DATE DEFAULT sysdate NOT NULL,
    update_dt            DATE
);
ALTER TABLE ce_product ADD CONSTRAINT product_pk PRIMARY KEY ( product_id );
ALTER TABLE ce_product
    ADD CONSTRAINT product_category_fk FOREIGN KEY ( product_cat_id )
        REFERENCES ce_category ( category_id );
COMMIT;
        
CREATE OR REPLACE TRIGGER trg_ce_product_update_dt
  BEFORE INSERT OR UPDATE ON tab_3NF.ce_product
  FOR EACH ROW
BEGIN
  :new.update_dt := sysdate;
END;

-- DROP TABLE tab_3NF.ce_product;

CREATE SEQUENCE tab_3NF.seq_sale_id ORDER;
COMMIT;

CREATE SEQUENCE tab_3NF.seq_payment_id ORDER;
COMMIT;

CREATE TABLE ce_sales (
    sale_id         NUMBER DEFAULT tab_3NF.seq_sale_id.NEXTVAL NOT NULL,
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
ALTER TABLE ce_sales ADD CONSTRAINT sales_pk PRIMARY KEY ( sale_id );
ALTER TABLE ce_sales
    ADD CONSTRAINT sales_location_fk FOREIGN KEY ( location_id )
        REFERENCES ce_location ( location_id );
ALTER TABLE ce_sales
    ADD CONSTRAINT sales_customer_fk FOREIGN KEY ( customer_id )
        REFERENCES ce_customer ( customer_id );
ALTER TABLE ce_sales
    ADD CONSTRAINT sales_employee_fk FOREIGN KEY ( employee_id )
        REFERENCES ce_employee ( employee_id );
ALTER TABLE ce_sales
    ADD CONSTRAINT sales_product_fk FOREIGN KEY ( product_id )
        REFERENCES ce_product ( product_id );
ALTER TABLE ce_sales
    ADD CONSTRAINT sales_store_fk FOREIGN KEY ( store_id )
        REFERENCES ce_store ( store_id );
ALTER TABLE ce_sales
    ADD CONSTRAINT sales_seller_fk FOREIGN KEY ( seller_id )
        REFERENCES ce_seller ( seller_id );
        
CREATE OR REPLACE TRIGGER trg_ce_sales_update_dt
  BEFORE INSERT OR UPDATE ON tab_3NF.ce_sales
  FOR EACH ROW
BEGIN
  :new.update_dt := sysdate;
END;