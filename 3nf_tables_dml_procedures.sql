GRANT CREATE ANY PROCEDURE TO bl_cl;
GRANT SELECT, INSERT, UPDATE, DELETE ON sa_olist_dataset.src_country TO bl_cl;
GRANT SELECT, INSERT, UPDATE, DELETE ON sa_olist_dataset.src_customer TO bl_cl;
GRANT SELECT, INSERT, UPDATE, DELETE ON sa_olist_dataset.src_geolocation TO bl_cl;
GRANT SELECT, INSERT, UPDATE, DELETE ON sa_olist_dataset.src_product TO bl_cl;
GRANT SELECT, INSERT, UPDATE, DELETE ON sa_olist_dataset.src_seller TO bl_cl;
GRANT SELECT, INSERT, UPDATE, DELETE ON sa_olist_dataset.src_stores TO bl_cl;
GRANT SELECT, INSERT, UPDATE, DELETE ON sa_olist_dataset.src_cities TO bl_cl;
GRANT SELECT, INSERT, UPDATE, DELETE ON sa_olist_dataset.src_regions TO bl_cl;
GRANT SELECT, INSERT, UPDATE, DELETE ON sa_generated_dataset.src_stores TO bl_cl;
GRANT SELECT, INSERT, UPDATE, DELETE ON sa_generated_dataset.src_employees TO bl_cl;
GRANT SELECT, INSERT, UPDATE, DELETE ON tab_3NF.ce_country TO bl_cl;
GRANT SELECT, INSERT, UPDATE, DELETE ON tab_3NF.ce_customer TO bl_cl;
GRANT SELECT, INSERT, UPDATE, DELETE ON tab_3NF.ce_city TO bl_cl;
GRANT SELECT, INSERT, UPDATE, DELETE ON tab_3NF.ce_category TO bl_cl;
GRANT SELECT, INSERT, UPDATE, DELETE ON tab_3NF.ce_employee TO bl_cl;
GRANT SELECT, INSERT, UPDATE, DELETE ON tab_3NF.ce_location TO bl_cl;
GRANT SELECT, INSERT, UPDATE, DELETE ON tab_3NF.ce_position TO bl_cl;
GRANT SELECT, INSERT, UPDATE, DELETE ON tab_3NF.ce_product TO bl_cl;
GRANT SELECT, INSERT, UPDATE, DELETE ON tab_3NF.ce_region TO bl_cl;
GRANT SELECT, INSERT, UPDATE, DELETE ON tab_3NF.ce_seller TO bl_cl;
GRANT SELECT, INSERT, UPDATE, DELETE ON tab_3NF.ce_store TO bl_cl;
GRANT SELECT, INSERT, UPDATE, DELETE ON tab_3NF.ce_sales TO bl_cl;
GRANT SELECT ON tab_3NF.seq_country_id TO bl_cl;
GRANT SELECT ON tab_3NF.seq_region_id1 TO bl_cl;
GRANT SELECT ON tab_3NF.seq_city_id TO bl_cl;
GRANT SELECT ON tab_3NF.seq_location_id TO bl_cl;
GRANT SELECT ON tab_3NF.seq_customer_id TO bl_cl;
GRANT SELECT ON tab_3Nf.seq_seller_id TO bl_cl;
GRANT SELECT ON tab_3NF.seq_position_id TO bl_cl;
GRANT SELECT ON tab_3NF.seq_employee_id TO bl_cl;
GRANT SELECT ON tab_3NF.seq_store_id TO bl_cl;
GRANT SELECT ON tab_3NF.seq_category_id TO bl_cl;
GRANT SELECT ON tab_3NF.seq_product_id TO bl_cl;
GRANT SELECT ANY DICTIONARY TO bl_cl;


CREATE OR REPLACE PACKAGE nf3_load_pkg AS 
    PROCEDURE ce_region_load (source_name IN VARCHAR2);
    PROCEDURE ce_city_load (source_name IN VARCHAR2);
    PROCEDURE ce_location_load (source_name IN VARCHAR2);
    PROCEDURE ce_store_load (source_name IN VARCHAR2);
    PROCEDURE ce_category_load (source_name IN VARCHAR2);
    PROCEDURE ce_product_load (source_name IN VARCHAR2);
    PROCEDURE ce_seller_load (source_name IN VARCHAR2);
    PROCEDURE ce_customer_load (source_name IN VARCHAR2);
    PROCEDURE ce_position_load (source_name IN VARCHAR2);
    PROCEDURE ce_employee_load (source_name IN VARCHAR2);
END nf3_load_pkg;
--------------------------------------------------------------------------------
CREATE OR REPLACE PACKAGE BODY nf3_load_pkg AS

    PROCEDURE logger_procedure (exec_user IN VARCHAR2,
                                exec_time IN TIMESTAMP WITH TIME ZONE,
                                exec_process_desc IN VARCHAR2,
                                pop_object_name IN VARCHAR2) 
    IS
    PRAGMA AUTONOMOUS_TRANSACTION;
    BEGIN 
      INSERT INTO bl_cl.logs (username, execution_time, process_desc, object_name)
      VALUES (exec_user, exec_time, exec_process_desc, pop_object_name);
      COMMIT;
    END logger_procedure;

    PROCEDURE ce_country_load (source_name IN VARCHAR2) IS
    olist_dataset CONSTANT VARCHAR2(100) := 'OLIST_DATASET';
    gen_dataset CONSTANT VARCHAR2(100) := 'GENERATED_DATASET';
    invalid_dataset EXCEPTION;
    CURSOR curs IS 
                    SELECT src_country.country_id AS country_id_src,
                           src_country.country_name AS country_name,
                           source_name AS country_source
                    FROM sa_olist_dataset.src_country
                    WHERE src_country.country_id NOT IN (
                                                         SELECT country_id_src
                                                         FROM tab_3NF.ce_country
                                                         )
                    ;
    ce_country_record curs%ROWTYPE;
    BEGIN
        OPEN curs;
          LOOP 
              FETCH curs INTO ce_country_record;
              IF UPPER(ce_country_record.country_source) IN (gen_dataset, olist_dataset)
                THEN
                  EXIT WHEN curs%NOTFOUND;
                  INSERT INTO tab_3NF.ce_country (
                                                  ce_country.country_id_src, 
                                                  ce_country.country_name, 
                                                  ce_country.country_source
                                                  )
                  VALUES (
                          ce_country_record.country_id_src,
                          ce_country_record.country_name,
                          ce_country_record.country_source
                          );
              ELSE 
                  RAISE invalid_dataset;
                  EXIT;
              END IF;
          END LOOP;
        CLOSE curs;
        COMMIT;
        bl_cl.logger_procedure(USER, CURRENT_TIMESTAMP, 'Procedure succesfull', 'tab_3NF.ce_country');
    EXCEPTION 
        WHEN invalid_dataset
          THEN bl_cl.logger_procedure(USER, CURRENT_TIMESTAMP, 'Invalid Dataset', 'tab_3NF.ce_country');
          ROLLBACK;
        WHEN OTHERS 
          THEN bl_cl.logger_procedure(USER, CURRENT_TIMESTAMP, 'Unexpected error', 'tab_3NF.ce_country');
          ROLLBACK;
    END ce_country_load;

    PROCEDURE ce_region_load (source_name IN VARCHAR2) IS
    olist_dataset CONSTANT VARCHAR2(100) := 'OLIST_DATASET';
    gen_dataset CONSTANT VARCHAR2(100) := 'GENERATED_DATASET';
    invalid_dataset EXCEPTION;
    CURSOR curs IS 
                    SELECT DISTINCT src_regions.geolocation_state AS region_abbrev, 
                                    ce_country.country_id AS region_country_id, 
                                    source_name AS region_source
                    FROM sa_olist_dataset.src_regions
                    INNER JOIN tab_3NF.ce_country ON src_regions.geolocation_country_name =
                                                        ce_country.country_name
                    WHERE src_regions.geolocation_state NOT IN (
                                                                    SELECT ce_region.region_abbrev
                                                                    FROM tab_3NF.ce_region
                                                                    )
                    ;
                    
    ce_region_record curs%ROWTYPE;
    BEGIN
        OPEN curs;
          LOOP 
            FETCH curs INTO ce_region_record;
            IF UPPER(ce_region_record.region_source) IN (gen_dataset, olist_dataset)
              THEN
                EXIT WHEN curs%NOTFOUND;
                INSERT INTO tab_3NF.ce_region (
                                               ce_region.region_abbrev, 
                                               ce_region.region_country_id, 
                                               ce_region.region_source
                                               )
                VALUES (
                        ce_region_record.region_abbrev,
                        ce_region_record.region_country_id,
                        ce_region_record.region_source
                        );
            ELSE
                RAISE invalid_dataset;
                EXIT;
            END IF;
        END LOOP;
        CLOSE curs;
        COMMIT;
        bl_cl.logger_procedure(USER, CURRENT_TIMESTAMP, 'Procedure succesfull', 'tab_3NF.ce_region');
    EXCEPTION 
        WHEN invalid_dataset
          THEN bl_cl.logger_procedure(USER, CURRENT_TIMESTAMP, 'Invalid Dataset', 'tab_3NF.ce_region');
          ROLLBACK;
        WHEN OTHERS 
          THEN bl_cl.logger_procedure(USER, CURRENT_TIMESTAMP, 'Unexpected error', 'tab_3NF.ce_region');
          ROLLBACK;
    END ce_region_load;

    PROCEDURE ce_city_load (source_name IN VARCHAR2) IS
    olist_dataset CONSTANT VARCHAR2(100) := 'OLIST_DATASET';
    gen_dataset CONSTANT VARCHAR2(100) := 'GENERATED_DATASET';
    invalid_dataset EXCEPTION;
    CURSOR curs IS 
                    SELECT DISTINCT src_cities.geolocation_city AS city_name,
                                    ce_region.region_id AS city_region_id,
                                    source_name AS city_source
                    FROM sa_olist_dataset.src_cities
                    INNER JOIN tab_3NF.ce_region ON src_cities.geolocation_state = 
                                                    ce_region.region_abbrev
                    WHERE src_cities.geolocation_city NOT IN (
                                                                   SELECT ce_city.city_name
                                                                   FROM tab_3NF.ce_city
                                                                   )
                    ;
    ce_city_record curs%ROWTYPE;
    BEGIN
        OPEN curs;
          LOOP 
            FETCH curs INTO ce_city_record;
            IF UPPER(ce_city_record.city_source) IN (gen_dataset, olist_dataset)
                THEN
                  EXIT WHEN curs%NOTFOUND;
                  INSERT INTO tab_3NF.ce_city (
                                               ce_city.city_name, 
                                               ce_city.city_region_id, 
                                               ce_city.city_source
                                               )
                  VALUES (
                          ce_city_record.city_name,
                          ce_city_record.city_region_id,
                          ce_city_record.city_region_id
                          );
            ELSE
                RAISE invalid_dataset;
                EXIT;
            END IF;
          END LOOP;        
        CLOSE curs;
        COMMIT;
        bl_cl.logger_procedure(USER, CURRENT_TIMESTAMP, 'Procedure succesfull', 'tab_3NF.ce_city');
    EXCEPTION
        WHEN invalid_dataset
          THEN bl_cl.logger_procedure(USER, CURRENT_TIMESTAMP, 'Invalid Dataset', 'tab_3NF.ce_city');
          ROLLBACK;
        WHEN OTHERS 
          THEN bl_cl.logger_procedure(USER, CURRENT_TIMESTAMP, 'Unexpected error', 'tab_3NF.ce_city');
          ROLLBACK;
    END ce_city_load;
    
    PROCEDURE ce_location_load (source_name IN VARCHAR2) IS
    olist_dataset CONSTANT VARCHAR2(100) := 'OLIST_DATASET';
    gen_dataset CONSTANT VARCHAR2(100) := 'GENERATED_DATASET';
    invalid_dataset EXCEPTION;
    BEGIN
      IF UPPER(source_name) IN (olist_dataset, gen_dataset)
      THEN
          MERGE INTO tab_3NF.ce_location loc
          USING 
               (
                SELECT DISTINCT src_geolocation.geolocation_lat AS location_lat,
                                src_geolocation.geolocation_lng AS location_ltd,
                                src_geolocation.geolocation_zip_code_prefix AS location_postal_code,
                                ce_city.city_id AS location_city_id,
                                source_name AS location_source
                FROM sa_olist_dataset.src_geolocation
                INNER JOIN tab_3NF.ce_city ON src_geolocation.geolocation_city = ce_city.city_name
          ) src
          ON (
              src.location_lat = loc.location_lat AND
              src.location_ltd = loc.location_ltd
             )
          WHEN NOT MATCHED THEN 
          INSERT (
                  loc.location_lat,
                  loc.location_ltd,
                  loc.location_postal_code,
                  loc.location_city_id,
                  loc.location_source
                  )
          VALUES (
                  src.location_lat,
                  src.location_ltd,
                  src.location_postal_code,
                  src.location_city_id,
                  src.location_source
                  );
          COMMIT;
          bl_cl.logger_procedure(USER, CURRENT_TIMESTAMP, 'Procedure succesfull', 'tab_3NF.ce_location');
      ELSE 
          RAISE invalid_dataset;
      END IF;
    EXCEPTION
      WHEN invalid_dataset
          THEN bl_cl.logger_procedure(USER, CURRENT_TIMESTAMP, 'Invalid Dataset', 'tab_3NF.ce_location');
          ROLLBACK;
        WHEN OTHERS 
          THEN bl_cl.logger_procedure(USER, CURRENT_TIMESTAMP, 'Unexpected error', 'tab_3NF.ce_location');
          ROLLBACK;
    END ce_location_load;

    PROCEDURE ce_store_load (source_name IN VARCHAR2) IS
    olist_dataset CONSTANT VARCHAR2(100) := 'OLIST_DATASET';
    gen_dataset CONSTANT VARCHAR2(100) := 'GENERATED_DATASET';
    row_num NUMBER;
    curr_user VARCHAR(255);
    invalid_dataset EXCEPTION;
    BEGIN
      IF UPPER(source_name) IN (olist_dataset, gen_dataset)
      THEN
        MERGE INTO tab_3NF.ce_store str
        USING 
             (
              SELECT src_stores.store_id AS store_id_src,
                     src_stores.store_name AS store_name,
                     src_stores.tel_num AS store_telephone_number,
                     source_name AS store_source
              FROM sa_generated_dataset.src_stores
             ) src
        ON (
            src.store_id_src = str.store_id_src
           )
        WHEN NOT MATCHED THEN 
        INSERT (
                str.store_id_src,
                str.store_name,
                str.store_telephone_number,
                str.store_source
                )
        VALUES (
                src.store_id_src,
                src.store_name,
                src.store_telephone_number,
                src.store_source
                );
        COMMIT;
        bl_cl.logger_procedure(USER, CURRENT_TIMESTAMP, 'Procedure succesfull', 'tab_3NF.ce_store');
      ELSE 
        RAISE invalid_dataset;
      END IF;
    EXCEPTION
      WHEN invalid_dataset
          THEN bl_cl.logger_procedure(USER, CURRENT_TIMESTAMP, 'Invalid Dataset', 'tab_3NF.ce_store');
          ROLLBACK;
        WHEN OTHERS 
          THEN bl_cl.logger_procedure(USER, CURRENT_TIMESTAMP, 'Unexpected error', 'tab_3NF.ce_store');
          ROLLBACK;
    END ce_store_load;
    
    PROCEDURE ce_category_load (source_name IN VARCHAR2) IS
    olist_dataset CONSTANT VARCHAR2(100) := 'OLIST_DATASET';
    gen_dataset CONSTANT VARCHAR2(100) := 'GENERATED_DATASET';
    invalid_dataset EXCEPTION;
    BEGIN
      IF UPPER(source_name) IN (olist_dataset, gen_dataset)
      THEN
        MERGE INTO tab_3NF.ce_category cat
        USING 
             (
              SELECT DISTINCT src_product.product_category_name AS category_name,
                              source_name AS category_source
              FROM sa_olist_dataset.src_product
              WHERE src_product.product_category_name IS NOT NULL
             ) src
        ON (
            src.category_name = cat.category_name
           )
        WHEN NOT MATCHED THEN 
        INSERT (
                cat.category_name,
                cat.category_source
                )
        VALUES (
                src.category_name,
                src.category_source
                );
        COMMIT;
        bl_cl.logger_procedure(USER, CURRENT_TIMESTAMP, 'Procedure succesfull', 'tab_3NF.ce_category');
      ELSE 
        RAISE invalid_dataset;
      END IF;
    EXCEPTION
      WHEN invalid_dataset
          THEN bl_cl.logger_procedure(USER, CURRENT_TIMESTAMP, 'Invalid Dataset', 'tab_3NF.ce_category');
          ROLLBACK;
        WHEN OTHERS 
          THEN bl_cl.logger_procedure(USER, CURRENT_TIMESTAMP, 'Unexpected error', 'tab_3NF.ce_category');
          ROLLBACK;
    END ce_category_load;
    
    PROCEDURE ce_product_load (source_name IN VARCHAR2) IS
    olist_dataset CONSTANT VARCHAR2(100) := 'OLIST_DATASET';
    gen_dataset CONSTANT VARCHAR2(100) := 'GENERATED_DATASET';
    invalid_dataset EXCEPTION;
    BEGIN
      IF UPPER(source_name) IN (olist_dataset, gen_dataset)
      THEN
        MERGE INTO tab_3NF.ce_product prod
        USING 
             (
              SELECT src_product.product_id AS product_id_src,
                     src_product.product_name AS product_name,
                     ce_category.category_id AS product_cat_id,
                     src_product.PRODUCT_LENGTH_CM AS product_len,
                     src_product.product_width_cm AS product_wid,
                     src_product.product_height_cm AS product_hei,
                     src_product.product_weight_g AS product_wei,
                     sysdate AS product_sop,
                     '31.12.9999' AS product_eop,
                     'Olist_Dataset' AS product_source
              FROM sa_olist_dataset.src_product
              INNER JOIN tab_3NF.ce_category ON src_product.product_category_name 
                                                = ce_category.category_name 
             ) src
        ON (
            src.product_id_src = prod.product_id_src
           )
        WHEN NOT MATCHED THEN 
        INSERT (
                prod.product_id_src, 
                prod.product_name, 
                prod.product_cat_id, 
                prod.product_len, 
                prod.product_wid, 
                prod.product_hei, 
                prod.product_wei, 
                prod.product_sop, 
                prod.product_eop, 
                prod.product_source
                )
        VALUES (
                src.product_id_src, 
                src.product_name, 
                src.product_cat_id, 
                src.product_len, 
                src.product_wid, 
                src.product_hei, 
                src.product_wei, 
                src.product_sop, 
                src.product_eop, 
                src.product_source
                );
        COMMIT;
        bl_cl.logger_procedure(USER, CURRENT_TIMESTAMP, 'Procedure succesfull', 'tab_3NF.ce_product');
      ELSE 
        RAISE invalid_dataset;
      END IF;
    EXCEPTION
      WHEN invalid_dataset
          THEN bl_cl.logger_procedure(USER, CURRENT_TIMESTAMP, 'Invalid Dataset', 'tab_3NF.ce_product');
          ROLLBACK;
        WHEN OTHERS 
          THEN bl_cl.logger_procedure(USER, CURRENT_TIMESTAMP, 'Unexpected error', 'tab_3NF.ce_product');
          ROLLBACK;
    END ce_product_load;
    
    PROCEDURE ce_seller_load (source_name IN VARCHAR2) IS
    olist_dataset CONSTANT VARCHAR2(100) := 'OLIST_DATASET';
    gen_dataset CONSTANT VARCHAR2(100) := 'GENERATED_DATASET';
    invalid_dataset EXCEPTION;
    BEGIN
      IF UPPER(source_name) IN (olist_dataset, gen_dataset)
      THEN
        MERGE INTO tab_3NF.ce_seller slr
        USING 
             (
              SELECT src_seller.seller_id_src AS seller_id_src,
                     src_seller.seller_telephone_num AS seller_telephone_num,
                     src_seller.seller_email AS seller_email,
                     source_name AS seller_source
              FROM sa_olist_dataset.src_seller
             ) src
        ON (
            src.seller_id_src = slr.seller_id_src
           )
        WHEN NOT MATCHED THEN 
        INSERT (
                slr.seller_id_src, 
                slr.seller_telephone_num,
                slr.seller_email, 
                slr.seller_source
                )
        VALUES (
                src.seller_id_src, 
                src.seller_telephone_num,
                src.seller_email, 
                src.seller_source
                );
        COMMIT;
        bl_cl.logger_procedure(USER, CURRENT_TIMESTAMP, 'Procedure succesfull', 'tab_3NF.ce_seller');
      ELSE 
        RAISE invalid_dataset;
      END IF;
    EXCEPTION
      WHEN invalid_dataset
          THEN bl_cl.logger_procedure(USER, CURRENT_TIMESTAMP, 'Invalid Dataset', 'tab_3NF.ce_seller');
          ROLLBACK;
        WHEN OTHERS 
          THEN bl_cl.logger_procedure(USER, CURRENT_TIMESTAMP, 'Unexpected error', 'tab_3NF.ce_seller');
          ROLLBACK;
    END ce_seller_load;
    
    PROCEDURE ce_customer_load (source_name IN VARCHAR2) IS
    olist_dataset CONSTANT VARCHAR2(100) := 'OLIST_DATASET';
    gen_dataset CONSTANT VARCHAR2(100) := 'GENERATED_DATASET';
    invalid_dataset EXCEPTION;
    BEGIN
      IF UPPER(source_name) IN (olist_dataset, gen_dataset)
      THEN
        MERGE INTO tab_3NF.ce_customer cust
        USING 
             (
              SELECT DISTINCT src_customer.customer_unique_id AS customer_id_src,
                              src_customer.customer_first_name AS customer_first_name,
                              src_customer.customer_last_name AS customer_last_name,
                              src_customer.customer_telephone_num AS customer_telephone_num,
                              src_customer.customer_email AS customer_email,
                              source_name AS customer_source
              FROM sa_olist_dataset.src_customer
             ) src
        ON (
            src.customer_id_src = cust.customer_id_src
           )
        WHEN NOT MATCHED THEN 
        INSERT (
                cust.customer_id_src, 
                cust.customer_first_name, 
                cust.customer_last_name,
                cust.customer_telephone_num, 
                cust.customer_email, 
                cust.customer_source
                )
        VALUES (
                src.customer_id_src, 
                src.customer_first_name, 
                src.customer_last_name,
                src.customer_telephone_num, 
                src.customer_email, 
                src.customer_source
                );
        COMMIT;
        bl_cl.logger_procedure(USER, CURRENT_TIMESTAMP, 'Procedure succesfull', 'tab_3NF.ce_customer');
      ELSE 
        RAISE invalid_dataset;
      END IF;
    EXCEPTION
      WHEN invalid_dataset
          THEN bl_cl.logger_procedure(USER, CURRENT_TIMESTAMP, 'Invalid Dataset', 'tab_3NF.ce_customer');
          ROLLBACK;
        WHEN OTHERS 
          THEN bl_cl.logger_procedure(USER, CURRENT_TIMESTAMP, 'Unexpected error', 'tab_3NF.ce_customer');
          ROLLBACK;
    END ce_customer_load;
    
    PROCEDURE ce_position_load (source_name IN VARCHAR2) IS
    olist_dataset CONSTANT VARCHAR2(100) := 'OLIST_DATASET';
    gen_dataset CONSTANT VARCHAR2(100) := 'GENERATED_DATASET';
    invalid_dataset EXCEPTION;
    BEGIN
      IF UPPER(source_name) IN (olist_dataset, gen_dataset)
      THEN
        MERGE INTO tab_3NF.ce_position pos
        USING 
             (
              SELECT DISTINCT src_employees.employee_position_name AS position_name,
                              source_name AS position_source
              FROM sa_generated_dataset.src_employees
             ) src
        ON (
            src.position_name = pos.position_name
           )
        WHEN NOT MATCHED THEN 
        INSERT (
                pos.position_name,
                pos.position_source
                )
        VALUES (
                src.position_name,
                src.position_name
                );
        COMMIT;
        bl_cl.logger_procedure(USER, CURRENT_TIMESTAMP, 'Procedure succesfull', 'tab_3NF.ce_position');
      ELSE 
        RAISE invalid_dataset;
      END IF;
    EXCEPTION
      WHEN invalid_dataset
          THEN bl_cl.logger_procedure(USER, CURRENT_TIMESTAMP, 'Invalid Dataset', 'tab_3NF.ce_position');
          ROLLBACK;
        WHEN OTHERS 
          THEN bl_cl.logger_procedure(USER, CURRENT_TIMESTAMP, 'Unexpected error', 'tab_3NF.ce_position');
          ROLLBACK;
    END ce_position_load;
    
    PROCEDURE ce_employee_load (source_name IN VARCHAR2) IS
    olist_dataset CONSTANT VARCHAR2(100) := 'OLIST_DATASET';
    gen_dataset CONSTANT VARCHAR2(100) := 'GENERATED_DATASET';
    invalid_dataset EXCEPTION;
    BEGIN
      IF UPPER(source_name) IN (olist_dataset, gen_dataset)
      THEN
        MERGE INTO tab_3NF.ce_employee empl
        USING 
             (
              SELECT src_employees.employee_id AS employee_id_src,
                     src_employees.employee_first_name AS employee_first_name,
                     src_employees.employee_last_name AS employee_last_name,
                     src_employees.employee_telephone_num AS employee_telephone_num,
                     ce_position.position_id AS employee_position_id,
                     source_name AS employee_source,
                     sysdate AS employee_eff_sop,
                     '31.12.9999' AS employee_eff_eop
              FROM sa_generated_dataset.src_employees
              INNER JOIN tab_3NF.ce_position ON src_employees.employee_position_name =
                                  ce_position.position_name
             ) src
        ON (
            src.employee_id_src = empl.employee_id_src
           )
        WHEN NOT MATCHED THEN 
        INSERT (
                empl.employee_id_src, 
                empl.employee_first_name,
                empl.employee_last_name, 
                empl.employee_telephone_num,
                empl.employee_position_id, 
                empl.employee_source,
                empl.employee_eff_sop,
                empl.employee_eff_eop
                )
        VALUES (
                src.employee_id_src, 
                src.employee_first_name,
                src.employee_last_name, 
                src.employee_telephone_num,
                src.employee_position_id, 
                src.employee_source,
                src.employee_eff_sop,
                src.employee_eff_eop
                );
        COMMIT;
        bl_cl.logger_procedure(USER, CURRENT_TIMESTAMP, 'Procedure succesfull', 'tab_3NF.ce_employee');
      ELSE 
        RAISE invalid_dataset;
      END IF;
    EXCEPTION
      WHEN invalid_dataset
          THEN bl_cl.logger_procedure(USER, CURRENT_TIMESTAMP, 'Invalid Dataset', 'tab_3NF.ce_employee');
          ROLLBACK;
        WHEN OTHERS 
          THEN bl_cl.logger_procedure(USER, CURRENT_TIMESTAMP, 'Unexpected error', 'tab_3NF.ce_employee');
          ROLLBACK;
    END ce_employee_load;
    
    PROCEDURE ce_sales_load (source_name IN VARCHAR2) IS
    olist_dataset CONSTANT VARCHAR2(100) := 'OLIST_DATASET';
    gen_dataset CONSTANT VARCHAR2(100) := 'GENERATED_DATASET';
    invalid_dataset EXCEPTION;
    BEGIN
      IF UPPER(source_name) IN (olist_dataset, gen_dataset)
      THEN
        INSERT INTO tab_3NF.ce_sales (sale_id_src, product_id, customer_id, 
                              store_id, seller_id, location_id, employee_id, 
                              unit_price, sale_timestamp, sale_source)
        SELECT CONCAT(src_order_item.order_id, src_order_item.order_item_id) AS sale_id_src,
               ce_product.product_id,
               ce_customer.customer_id,
               ce_store.store_id,
               ce_seller.seller_id,
               ce_location.location_id,
               ce_employee.employee_id,
               TO_NUMBER(regexp_replace(trim(src_order_item.price),'[^0-9.,]',''), '99999.99') AS sale_price,
               RTRIM(TO_TIMESTAMP(src_order_item.order_purchase_timestamp, 'DD.MM.YYYY HH24:MI:SS'), ':,0') AS sale_purchase_timestamp,
               source_name
        FROM sa_olist_dataset.src_order_item
        INNER JOIN tab_3NF.ce_product ON regexp_replace(src_order_item.product_id,'[^a-zA-Z0-9]','') = regexp_replace(ce_product.product_id_src,'[^a-zA-Z0-9]','')
        INNER JOIN tab_3NF.ce_store ON regexp_replace(src_order_item.store_id,'[^a-zA-Z0-9]','') = regexp_replace(ce_store.store_id_src,'[^a-zA-Z0-9]','')
        INNER JOIN tab_3NF.ce_seller ON regexp_replace(src_order_item.seller_id,'[^a-zA-Z0-9]','') = regexp_replace(ce_seller.seller_id_src,'[^a-zA-Z0-9]','')
        INNER JOIN tab_3NF.ce_location ON regexp_replace(src_order_item.geolocation_lat,'[^a-zA-Z0-9]','') = regexp_replace(ce_location.location_lat,'[^a-zA-Z0-9]','') AND
                                          regexp_replace(src_order_item.geolocation_lng,'[^a-zA-Z0-9]','') = regexp_replace(ce_location.location_ltd,'[^a-zA-Z0-9]','')
        INNER JOIN tab_3NF.ce_employee ON regexp_replace(src_order_item.employee_id,'[^a-zA-Z0-9]','') = regexp_replace(ce_employee.employee_id_src,'[^a-zA-Z0-9]','')
        INNER JOIN tab_3NF.ce_customer ON regexp_replace(src_order_item.customer_unique_id,'[^a-zA-Z0-9]','') = regexp_replace(ce_customer.customer_id_src,'[^a-zA-Z0-9]','');
        COMMIT;
        bl_cl.logger_procedure(USER, CURRENT_TIMESTAMP, 'Procedure succesfull', 'tab_3NF.ce_employee');
      ELSE 
        RAISE invalid_dataset;
      END IF;
    EXCEPTION
      WHEN invalid_dataset
          THEN bl_cl.logger_procedure(USER, CURRENT_TIMESTAMP, 'Invalid Dataset', 'tab_3NF.ce_employee');
          ROLLBACK;
        WHEN OTHERS 
          THEN bl_cl.logger_procedure(USER, CURRENT_TIMESTAMP, 'Unexpected error', 'tab_3NF.ce_employee');
          ROLLBACK;
    END ce_sales_load;
    
END nf3_load_pkg;