GRANT SELECT, INSERT, UPDATE, DELETE ON bl_dm.dim_customer TO bl_cl;
GRANT SELECT, INSERT, UPDATE, DELETE ON bl_dm.dim_location TO bl_cl;
GRANT SELECT, INSERT, UPDATE, DELETE ON bl_dm.dim_seller TO bl_cl;
GRANT SELECT, INSERT, UPDATE, DELETE ON bl_dm.dim_time TO bl_cl;
GRANT SELECT, INSERT, UPDATE, DELETE ON bl_dm.dim_store TO bl_cl;
GRANT SELECT, INSERT, UPDATE, DELETE ON bl_dm.dim_employee_scd TO bl_cl;
GRANT SELECT, INSERT, UPDATE, DELETE ON bl_dm.dim_product_scd TO bl_cl;
GRANT SELECT, INSERT, UPDATE, DELETE ON bl_dm.fct_sales TO bl_cl;
GRANT SELECT, INSERT, UPDATE, DELETE ON bl_dm.fct_sales_exchange TO bl_cl;
GRANT SELECT ON bl_dm.seq_dim_store_id TO bl_cl;
GRANT SELECT ON bl_dm.seq_dim_location_id TO bl_cl;
GRANT SELECT ON bl_dm.seq_dim_seller_id TO bl_cl;
GRANT SELECT ON bl_dm.seq_dim_employee_id TO bl_cl;
GRANT SELECT ON bl_dm.seq_dim_position_id TO bl_cl;
GRANT SELECT ON bl_dm.seq_dim_product_id TO bl_cl;
GRANT SELECT ON bl_dm.seq_dim_category_id TO bl_cl;
GRANT SELECT ON bl_dm.seq_dim_customer_id TO bl_cl;
GRANT SELECT, INSERT, UPDATE, DELETE ON kiryl_navitski.dim_time_test TO bl_cl;

CREATE PACKAGE dm_load_pkg AS 
    PROCEDURE dim_customer_load;
    PROCEDURE dim_location_load;
    PROCEDURE dim_time_load;
    PROCEDURE dim_store_load;
    PROCEDURE dim_seller_load;
    PROCEDURE dim_employee_load (active_status IN VARCHAR);
    PROCEDURE dim_product_load (active_status IN VARCHAR);
    PROCEDURE fct_sales_initial_load;
    PROCEDURE fct_sales_incremental_load;
END dm_load_pkg;

--------------------------------------------------------------------------------

CREATE PACKAGE BODY dm_load_pkg AS

    PROCEDURE dm_load_logger (exec_user IN VARCHAR2,
                                exec_time IN TIMESTAMP WITH TIME ZONE,
                                exec_process_desc IN VARCHAR2,
                                pop_object_name IN VARCHAR2) 
    IS
    PRAGMA AUTONOMOUS_TRANSACTION;
    BEGIN 
      INSERT INTO bl_cl.logs (username, execution_time, process_desc, object_name)
      VALUES (exec_user, exec_time, exec_process_desc, pop_object_name);
      COMMIT;
    END dm_load_logger;

    CREATE OR REPLACE PROCEDURE dim_customer_load IS
    CURSOR curs IS 
                    SELECT ce_customer.customer_id AS customer_id,
                           ce_customer.customer_id_src AS customer_id_src,
                           ce_customer.customer_first_name AS customer_first_name,
                           ce_customer.customer_last_name AS customer_last_name,
                           ce_customer.customer_telephone_num AS customer_telephone_num,
                           ce_customer.customer_email AS customer_email,
                           ce_customer.customer_source AS customer_source
                    FROM tab_3NF.ce_customer
                    WHERE ce_customer.customer_id NOT IN (
                                                         SELECT dim_customer.customer_id
                                                         FROM bl_dm.dim_customer
                                                         )
                    ;
    dim_customer_record curs%ROWTYPE;
    BEGIN
        OPEN curs;
          LOOP 
              FETCH curs INTO dim_customer_record;
                  EXIT WHEN curs%NOTFOUND;
                  INSERT INTO bl_dm.dim_customer (
                                                  dim_customer.customer_id_src, 
                                                  dim_customer.customer_first_name,
                                                  dim_customer.customer_last_name, 
                                                  dim_customer.customer_telephone_num, 
                                                  dim_customer.customer_email,
                                                  dim_customer.customer_source
                                                  )
                  VALUES (
                          dim_customer_record.customer_id,
                          dim_customer_record.customer_first_name,
                          dim_customer_record.customer_last_name,
                          dim_customer_record.customer_telephone_num,
                          dim_customer_record.customer_email,
                          dim_customer_record.customer_source
                          ); 
          END LOOP;
        CLOSE curs;
        COMMIT;
        bl_cl.dm_load_logger(USER, CURRENT_TIMESTAMP, 'Procedure succesfull', 'bl_dm.dim_customer');
    EXCEPTION 
        WHEN OTHERS
        THEN bl_cl.dm_load_logger(USER, CURRENT_TIMESTAMP, 'Unexpected error', 'bl_dm.dim_customer');
             ROLLBACK;
    END dim_customer_load;
    
    PROCEDURE dim_location_load IS
    BEGIN
        MERGE INTO bl_dm.dim_location loc
        USING 
            (
            SELECT  ce_location.location_id AS location_id_src,            
                    ce_location.location_lat AS location_lat,
                    ce_location.location_ltd AS location_ltd,
                    ce_location.location_postal_code AS location_postal_code,
                    ce_location.location_city_id AS location_city_id,
                    ce_city.city_name AS location_city_name,
                    ce_city.city_region_id AS location_region_id,
                    ce_region.region_abbrev AS location_region_name,
                    ce_region.region_country_id AS location_country_id,
                    ce_country.country_id_src AS location_country_id_src,
                    ce_country.country_name AS location_country_name,
                    ce_location.location_source AS location_source
            FROM tab_3NF.ce_location
            INNER JOIN tab_3NF.ce_city ON ce_location.location_city_id = ce_city.city_id
            INNER JOIN tab_3NF.ce_region ON ce_city.city_region_id = ce_region.region_id
            INNER JOIN tab_3NF.ce_country ON ce_region.region_country_id = ce_country.country_id
            ) src
        ON (
            src.location_lat = loc.location_lat AND 
            src.location_ltd = loc.location_ltd
            )
        WHEN NOT MATCHED THEN 
            INSERT (
                    loc.location_id_src,
                    loc.location_lat,
                    loc.location_ltd,
                    loc.location_postal_code,
                    loc.location_city_id,
                    loc.location_city_name,
                    loc.location_region_id,
                    loc.location_region_name,
                    loc.location_country_id,
                    loc.location_country_id_src,
                    loc.location_country_name,
                    loc.location_source
                    )
            VALUES (
                    src.location_id_src,
                    src.location_lat,
                    src.location_ltd,
                    src.location_postal_code,
                    src.location_city_id,
                    src.location_city_name,
                    src.location_region_id,
                    src.location_region_name,
                    src.location_country_id,
                    src.location_country_id_src,
                    src.location_country_name,
                    src.location_source
                    );
            COMMIT;
          bl_cl.dm_load_logger(USER, CURRENT_TIMESTAMP, 'Procedure succesfull', 'bl_dm.dim_location');
    EXCEPTION
      WHEN OTHERS
        THEN bl_cl.dm_load_logger(USER, CURRENT_TIMESTAMP, 'Procedure failed', 'bl_dm.dim_location');
             ROLLBACK;
    END dim_location_load;
    
    PROCEDURE dim_time_load AS
    TYPE time_load_tab IS TABLE OF kiryl_navitski.dim_time_test%ROWTYPE;
    l_tab   time_load_tab;
    sql_stmt  VARCHAR2(2000);
    BEGIN
        SELECT dim_time_test.time_date_id AS time_date_id, 
               dim_time_test.time_date AS time_date, 
               dim_time_test.time_day_of_week AS time_day_of_week, 
               dim_time_test.time_day_num_in_month AS time_day_num_in_month, 
               dim_time_test.time_day_num_overall AS time_day_num_overall, 
               dim_time_test.time_day_name AS time_day_name, 
               dim_time_test.time_day_abbrev AS time_day_abbrev, 
               dim_time_test.time_weekday_flag AS time_weekday_flag, 
               dim_time_test.time_week_num_in_year AS time_week_num_in_year, 
               dim_time_test.time_week_num_overall AS time_week_num_overall, 
               dim_time_test.time_week_begin_date AS time_week_begin_date, 
               dim_time_test.time_week_begin_date_id AS time_week_begin_date_id, 
               dim_time_test.time_month AS time_month, 
               dim_time_test.time_month_num_overall AS time_month_num_overall, 
               dim_time_test.time_month_name AS time_month_name, 
               dim_time_test.time_month_abbrev AS time_month_abbrev, 
               dim_time_test.time_quarter AS time_quarter, 
               dim_time_test.time_year AS time_year, 
               dim_time_test.time_yearmo AS time_yearmo, 
               dim_time_test.time_fiscal_month AS time_fiscal_month, 
               dim_time_test.time_fiscal_quarter AS time_fiscal_quarter, 
               dim_time_test.time_month_end_flag AS time_month_end_flag, 
               dim_time_test.time_same_day_year_ago AS time_same_day_year_ago
        BULK COLLECT INTO l_tab
        FROM   kiryl_navitski.dim_time_test
        WHERE dim_time_test.time_date NOT IN (SELECT dim_time.time_date
                                         FROM bl_dm.dim_time);
        sql_stmt := 'INSERT INTO bl_dm.dim_time (time_date_id,
                                                 time_date,
                                                 time_day_of_week,
                                                 time_day_num_in_month,
                                                 time_day_num_overall,
                                                 time_day_name,
                                                 time_day_abbrev,
                                                 time_weekday_flag,
                                                 time_week_num_in_year,
                                                 time_week_num_overall,
                                                 time_week_begin_date,
                                                 time_week_begin_date_id,
                                                 time_month,
                                                 time_month_num_overall,
                                                 time_month_name,
                                                 time_month_abbrev,
                                                 time_quarter,
                                                 time_year,
                                                 time_yearmo,
                                                 time_fiscal_month,
                                                 time_fiscal_quarter,
                                                 time_month_end_flag,
                                                 time_same_day_year_ago)
            VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14, 
                    :15, :16, :17, :18, :19, :20, :21, :22, :23)';
        FORALL i IN l_tab.first .. l_tab.last
        EXECUTE IMMEDIATE
            sql_stmt
            USING l_tab(i).time_date_id,
                  l_tab(i).time_date,
                  l_tab(i).time_day_of_week,
                  l_tab(i).time_day_num_in_month,
                  l_tab(i).time_day_num_overall,
                  l_tab(i).time_day_name,
                  l_tab(i).time_day_abbrev,
                  l_tab(i).time_weekday_flag,
                  l_tab(i).time_week_num_in_year,
                  l_tab(i).time_week_num_overall,
                  l_tab(i).time_week_begin_date,
                  l_tab(i).time_week_begin_date_id,
                  l_tab(i).time_month,
                  l_tab(i).time_month_num_overall,
                  l_tab(i).time_month_name,
                  l_tab(i).time_month_abbrev,
                  l_tab(i).time_quarter,
                  l_tab(i).time_year,
                  l_tab(i).time_yearmo,
                  l_tab(i).time_fiscal_month,
                  l_tab(i).time_fiscal_quarter,
                  l_tab(i).time_month_end_flag,
                  l_tab(i).time_same_day_year_ago;
            COMMIT;
            bl_cl.dm_load_logger(USER, CURRENT_TIMESTAMP, 'Procedure succesfull', 'bl_dm.dim_location');
    EXCEPTION
      WHEN OTHERS
        THEN bl_cl.dm_load_logger(USER, CURRENT_TIMESTAMP, 'Procedure failed', 'bl_dm.dim_location');
             ROLLBACK;
    END dim_time_load;
    
    PROCEDURE dim_seller_load IS
    BEGIN
        MERGE INTO bl_dm.dim_seller sel
        USING 
            (
            SELECT  ce_seller.seller_id,
                    ce_seller.seller_telephone_num,
                    ce_seller.seller_email,
                    ce_seller.seller_source
            FROM tab_3NF.ce_seller
            ) src
        ON (
            src.seller_id = sel.seller_id_src
            )
        WHEN NOT MATCHED THEN 
            INSERT (
                    sel.seller_id_src,
                    sel.seller_telephone_num,
                    sel.seller_email,
                    sel.seller_source
                    )
            VALUES (
                    src.seller_id,
                    src.seller_telephone_num,
                    src.seller_email,
                    src.seller_source
                    );
            COMMIT;
          bl_cl.dm_load_logger(USER, CURRENT_TIMESTAMP, 'Procedure succesfull', 'bl_dm.dim_location');
    EXCEPTION
      WHEN OTHERS
        THEN bl_cl.dm_load_logger(USER, CURRENT_TIMESTAMP, 'Procedure failed', 'bl_dm.dim_location');
             ROLLBACK;
    END dim_seller_load;

    PROCEDURE dim_store_load IS
    BEGIN
        MERGE INTO bl_dm.dim_store str
        USING 
            (
            SELECT  ce_store.store_id,
                    ce_store.store_name,
                    ce_store.store_telephone_number,
                    ce_store.store_source
            FROM tab_3NF.ce_store
            ) src
        ON (
            src.store_id = str.store_id_src
            )
        WHEN NOT MATCHED THEN 
            INSERT (
                    str.store_id_src,
                    str.store_name,
                    str.store_telephone_num,
                    str.store_source
                    )
            VALUES (
                    src.store_id,
                    src.store_name,
                    src.store_telephone_number,
                    src.store_source
                    );
            COMMIT;
          bl_cl.dm_load_logger(USER, CURRENT_TIMESTAMP, 'Procedure succesfull', 'bl_dm.dim_location');
    EXCEPTION
      WHEN OTHERS
        THEN bl_cl.dm_load_logger(USER, CURRENT_TIMESTAMP, 'Procedure failed', 'bl_dm.dim_location');
             ROLLBACK;
    END dim_store_load;
    
    CREATE OR REPLACE PROCEDURE dim_employee_load IS
    BEGIN
    MERGE INTO bl_dm.dim_employee_scd prod
        USING (
               SELECT src.employee_id,
                      src.employee_id_src,
                      src.employee_first_name,
                      src.employee_last_name,
                      src.employee_telephone_num,
                      src.employee_position_id,
                      ce_position.position_name,
                      src.employee_eff_eop,
                      src.employee_eff_sop,
                      src.employee_source,
                      src.is_active
               FROM tab_3nf.ce_employee src
               INNER JOIN tab_3nf.ce_position ON src.employee_position_id = ce_position.position_id
               WHERE src.insert_dt >= (
                                       SELECT COALESCE(MAX(src.insert_dt), TO_DATE('01-12-2020','DD-MM-YYYY'))
                                       FROM bl_dm.dim_employee_scd
                                       )
                 OR src.update_dt >= (
                                      SELECT COALESCE(MAX(src.update_dt), TO_DATE('01-12-2020','DD-MM-YYYY'))
                                      FROM bl_dm.dim_employee_scd
                                      )
                ) load
        ON (prod.employee_id_src = load.employee_id AND prod.employee_eff_sop = load.employee_eff_sop)
        WHEN MATCHED THEN
            UPDATE
            SET prod.employee_first_name = (CASE WHEN prod.employee_first_name <> load.employee_first_name THEN load.employee_first_name ELSE prod.employee_first_name END),
                prod.employee_last_name = (CASE WHEN prod.employee_last_name <> load.employee_last_name THEN load.employee_last_name ELSE prod.employee_last_name END),
                prod.employee_telephone_num = (CASE WHEN prod.employee_telephone_num <> load.employee_telephone_num THEN load.employee_telephone_num ELSE prod.employee_telephone_num END),
                prod.employee_position_id = (CASE WHEN prod.employee_position_id <> load.employee_position_id THEN load.employee_position_id ELSE prod.employee_position_id END),
                prod.employee_position_name = (CASE WHEN prod.employee_position_name <> load.position_name THEN load.position_name ELSE prod.employee_position_name END),
                prod.employee_eff_eop = (CASE WHEN prod.employee_eff_eop <> load.employee_eff_eop THEN load.employee_eff_eop ELSE prod.employee_eff_eop END),
                prod.is_active = (CASE WHEN prod.is_active <> load.is_active THEN load.is_active ELSE prod.is_active END),
                prod.update_dt = sysdate
            WHERE prod.employee_first_name <> load.employee_first_name
               OR prod.employee_last_name <> load.employee_last_name
               OR prod.employee_telephone_num <> load.employee_telephone_num
               OR prod.employee_position_id <> load.employee_position_id
               OR prod.employee_position_name <> load.position_name
               OR prod.employee_eff_eop <> load.employee_eff_eop
               OR prod.is_active <> load.is_active 
        WHEN NOT MATCHED THEN
            INSERT (prod.employee_id_src,
                    prod.employee_first_name,
                    prod.employee_last_name,
                    prod.employee_telephone_num,
                    prod.employee_position_id,
                    prod.employee_position_name,
                    prod.employee_eff_sop,
                    prod.employee_eff_eop,
                    prod.employee_source_name,
                    prod.is_active)
            VALUES (load.employee_id, 
                    load.employee_first_name,
                    load.employee_last_name,
                    load.employee_telephone_num,
                    load.employee_position_id,
                    load.position_name,
                    load.employee_eff_sop,
                    load.employee_eff_eop,
                    load.employee_source,
                    load.is_active);
                    COMMIT;
    EXCEPTION
      WHEN OTHERS
        THEN bl_cl.dm_load_logger(USER, CURRENT_TIMESTAMP, 'Procedure failed', 'bl_dm.dim_location');
            ROLLBACK;
    END dim_employee_load;
    
    PROCEDURE dim_product_load IS
    invalid_status EXCEPTION;
    BEGIN
      MERGE INTO bl_dm.dim_product_scd prod
        USING (
               SELECT src.product_id,
                      src.product_name,
                      src.product_cat_id,
                      ce_category.category_name,
                      src.product_len,
                      src.product_wid,
                      src.product_hei,
                      src.product_wei,
                      src.product_sop,
                      src.product_eop,
                      src.product_is_active,
                      src.product_source
               FROM tab_3nf.ce_product src
               INNER JOIN tab_3nf.ce_category ON src.product_cat_id = ce_category.category_id
               WHERE src.insert_dt >= (
                                       SELECT COALESCE(MAX(src.insert_dt), TO_DATE('01-12-2020','DD-MM-YYYY'))
                                       FROM bl_dm.dim_product_scd
                                       )
                 OR src.update_dt >= (
                                      SELECT COALESCE(MAX(src.update_dt), TO_DATE('01-12-2020','DD-MM-YYYY'))
                                      FROM bl_dm.dim_product_scd
                                      )
                ) load
        ON (prod.product_id_src = load.product_id AND prod.product_sop = load.product_sop)
        WHEN MATCHED THEN
            UPDATE
            SET prod.product_name = (CASE WHEN prod.product_name <> load.product_name THEN load.product_name ELSE prod.product_name END),
                prod.product_category_id = (CASE WHEN prod.product_category_id <> load.product_cat_id THEN load.product_cat_id ELSE prod.product_category_id END),
                prod.product_category_name = (CASE WHEN prod.product_category_name <> load.category_name THEN load.category_name ELSE prod.product_category_name END),
                prod.product_lenght = (CASE WHEN prod.product_lenght <> load.product_len THEN load.product_len ELSE prod.product_lenght END),
                prod.product_width = (CASE WHEN prod.product_width <> load.product_wid THEN load.product_wid ELSE prod.product_width END),
                prod.product_height = (CASE WHEN prod.product_height <> load.product_hei THEN load.product_hei ELSE prod.product_height END),
                prod.product_weight = (CASE WHEN prod.product_weight <> load.product_wei THEN load.product_wei ELSE prod.product_weight END),
                prod.product_eop = (CASE WHEN prod.product_eop <> load.product_eop THEN load.product_eop ELSE prod.product_eop END),
                prod.is_active = (CASE WHEN prod.is_active <> load.product_is_active THEN load.product_is_active ELSE prod.is_active END),
                prod.update_dt = sysdate
            WHERE prod.product_name <> load.product_name
               OR prod.product_category_id <> load.product_cat_id
               OR prod.product_lenght <> load.product_len
               OR prod.product_width <> load.product_wid
               OR prod.product_height <> load.product_hei
               OR prod.product_weight <> load.product_wei
               OR prod.product_eop <> load.product_eop
               OR prod.is_active <> load.product_is_active
        WHEN NOT MATCHED THEN
            INSERT (prod.product_id_src,
                    prod.product_name,
                    prod.product_category_id,
                    prod.product_category_name,
                    prod.product_lenght,
                    prod.product_width,
                    prod.product_height,
                    prod.product_weight,
                    prod.product_sop,
                    prod.product_eop,
                    prod.product_source,
                    prod.is_active)
            VALUES (load.product_id, 
                    load.product_name,
                    load.product_cat_id,
                    load.category_name,
                    load.product_len,
                    load.product_wid,
                    load.product_hei,
                    load.product_wei,
                    load.product_sop,
                    load.product_eop,
                    load.product_source,
                    load.product_is_active);
            COMMIT;
    EXCEPTION
      WHEN OTHERS
        THEN bl_cl.dm_load_logger(USER, CURRENT_TIMESTAMP, 'Procedure failed', 'bl_dm.dim_location');
            ROLLBACK;
    END dim_product_load;  
    
    PROCEDURE fct_sales_initial_load IS
    invalid_status EXCEPTION;
    BEGIN
    MERGE INTO bl_dm.fct_sales prod
        USING 
            (
            SELECT  src.sale_id,
                    dim_product_scd.product_id,
                    dim_customer.customer_id,
                    dim_store.store_id,
                    dim_seller.seller_id,
                    dim_location.location_id,
                    dim_employee_scd.employee_id,
                    src.unit_price,
                    src.sale_timestamp,
                    src.sale_source
            FROM tab_3NF.ce_sales src
            INNER JOIN bl_dm.dim_product_scd ON regexp_replace(src.product_id, '[^a-zA-Z0-9]','') = regexp_replace(dim_product_scd.product_id_src, '[^a-zA-Z0-9]','')
            INNER JOIN bl_dm.dim_customer ON regexp_replace(src.customer_id, '[^a-zA-Z0-9]','') = regexp_replace(dim_customer.customer_id_src, '[^a-zA-Z0-9]','')
            INNER JOIN bl_dm.dim_store ON regexp_replace(src.store_id, '[^a-zA-Z0-9]','') = regexp_replace(dim_store.store_id_src, '[^a-zA-Z0-9]','')
            INNER JOIN bl_dm.dim_seller ON regexp_replace(src.seller_id, '[^a-zA-Z0-9]','') = regexp_replace(dim_seller.seller_id_src, '[^a-zA-Z0-9]','')
            INNER JOIN bl_dm.dim_location ON regexp_replace(src.location_id, '[^a-zA-Z0-9]','') = regexp_replace(dim_location.location_id_src, '[^a-zA-Z0-9]','')
            INNER JOIN bl_dm.dim_employee_scd ON regexp_replace(src.employee_id, '[^a-zA-Z0-9]','') = regexp_replace(dim_employee_scd.employee_id_src, '[^a-zA-Z0-9]','')
            ) load
        ON (
            prod.sale_id_src = load.sale_id
            )
        WHEN NOT MATCHED THEN 
            INSERT (
                    prod.sale_id_src,
                    prod.product_id,
                    prod.customer_id,
                    prod.store_id,
                    prod.seller_id,
                    prod.location_id,
                    prod.employee_id,
                    prod.unit_price,
                    prod.sale_timestamp,
                    prod.sale_source
                    )
            VALUES (
                    load.sale_id,
                    load.product_id,
                    load.customer_id,
                    load.store_id,
                    load.seller_id,
                    load.location_id,
                    load.employee_id,
                    load.unit_price,
                    load.sale_timestamp,
                    load.sale_source
                   );
            COMMIT;
    EXCEPTION
      WHEN OTHERS
        THEN bl_cl.dm_load_logger(USER, CURRENT_TIMESTAMP, 'Procedure failed', 'bl_dm.fct_sales');
            ROLLBACK;
    END fct_sales_initial_load;  
                  
    CREATE OR REPLACE PROCEDURE fct_sales_incremental_load IS               
    invalid_status EXCEPTION;
    BEGIN
      EXECUTE IMMEDIATE 'TRUNCATE TABLE bl_dm.fct_sales_exchange';
      MERGE INTO bl_dm.fct_sales_exchange prod
        USING 
            (
            SELECT  src.sale_id,
                    dim_product_scd.product_id,
                    dim_customer.customer_id,
                    dim_store.store_id,
                    dim_seller.seller_id,
                    dim_location.location_id,
                    dim_employee_scd.employee_id,
                    src.unit_price,
                    src.sale_timestamp,
                    src.sale_source
            FROM tab_3NF.ce_sales src
            INNER JOIN bl_dm.dim_product_scd ON regexp_replace(src.product_id, '[^a-zA-Z0-9]','') = regexp_replace(dim_product_scd.product_id_src, '[^a-zA-Z0-9]','')
            INNER JOIN bl_dm.dim_customer ON regexp_replace(src.customer_id, '[^a-zA-Z0-9]','') = regexp_replace(dim_customer.customer_id_src, '[^a-zA-Z0-9]','')
            INNER JOIN bl_dm.dim_store ON regexp_replace(src.store_id, '[^a-zA-Z0-9]','') = regexp_replace(dim_store.store_id_src, '[^a-zA-Z0-9]','')
            INNER JOIN bl_dm.dim_seller ON regexp_replace(src.seller_id, '[^a-zA-Z0-9]','') = regexp_replace(dim_seller.seller_id_src, '[^a-zA-Z0-9]','')
            INNER JOIN bl_dm.dim_location ON regexp_replace(src.location_id, '[^a-zA-Z0-9]','') = regexp_replace(dim_location.location_id_src, '[^a-zA-Z0-9]','')
            INNER JOIN bl_dm.dim_employee_scd ON regexp_replace(src.employee_id, '[^a-zA-Z0-9]','') = regexp_replace(dim_employee_scd.employee_id_src, '[^a-zA-Z0-9]','')
            WHERE src.sale_timestamp > TO_TIMESTAMP('10-01-2018 00:00:00.000000', 'MM-DD-YYYY HH24:MI:SS.FF') AND 
                  src.sale_timestamp < TO_TIMESTAMP('01-01-2019 00:00:00.000000', 'MM-DD-YYYY HH24:MI:SS.FF')
            ) load
        ON (
            prod.sale_id_src = load.sale_id
            )
        WHEN NOT MATCHED THEN 
            INSERT (
                    prod.sale_id_src,
                    prod.product_id,
                    prod.customer_id,
                    prod.store_id,
                    prod.seller_id,
                    prod.location_id,
                    prod.employee_id,
                    prod.unit_price,
                    prod.sale_timestamp,
                    prod.sale_source
                    )
            VALUES (
                    load.sale_id,
                    load.product_id,
                    load.customer_id,
                    load.store_id,
                    load.seller_id,
                    load.location_id,
                    load.employee_id,
                    load.unit_price,
                    load.sale_timestamp,
                    load.sale_source
                   );
        EXECUTE IMMEDIATE  'ALTER TABLE bl_dm.fct_sales 
                            EXCHANGE PARTITION y2018q4 
                            WITH TABLE bl_dm.fct_sales_exchange
                            INCLUDING INDEXES WITHOUT VALIDATION';
        COMMIT;
    EXCEPTION
      WHEN OTHERS 
        THEN bl_cl.logger_procedure(USER, CURRENT_TIMESTAMP, 'Unexpected error', 'tab_3NF.ce_employee');
             ROLLBACK;   
    END fct_sales_incremental_load;
    
END dm_load_pkg;
