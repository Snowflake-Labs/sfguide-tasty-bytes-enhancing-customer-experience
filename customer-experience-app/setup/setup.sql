/***************************************************************************************************
  _______           _            ____          _             
 |__   __|         | |          |  _ \        | |            
    | |  __ _  ___ | |_  _   _  | |_) | _   _ | |_  ___  ___ 
    | | / _` |/ __|| __|| | | | |  _ < | | | || __|/ _ \/ __|
    | || (_| |\__ \| |_ | |_| | | |_) || |_| || |_|  __/\__ \
    |_| \__,_||___/ \__| \__, | |____/  \__, | \__|\___||___/
                          __/ |          __/ |               
                         |___/          |___/            
Quickstart:   Tasty Bytes - Enhancing Customer Experience - Email Support App
Version:      v1     
Author:       Charlie Hammond
Copyright(c): 2024 Snowflake Inc. All rights reserved.
****************************************************************************************************
SUMMARY OF CHANGES
Date(yyyy-mm-dd)    Author              Comments
------------------- ------------------- ------------------------------------------------------------
2024-06-25          Charlie Hammond     Initial Release
***************************************************************************************************/

USE ROLE sysadmin;

CREATE OR REPLACE DATABASE tasty_bytes_enhancing_customer_experience;

--Schema
CREATE OR REPLACE SCHEMA tasty_bytes_enhancing_customer_experience.raw_doc;
CREATE OR REPLACE SCHEMA tasty_bytes_enhancing_customer_experience.raw_pos;
CREATE OR REPLACE SCHEMA tasty_bytes_enhancing_customer_experience.harmonized;
CREATE OR REPLACE SCHEMA tasty_bytes_enhancing_customer_experience.analytics;
CREATE OR REPLACE SCHEMA tasty_bytes_enhancing_customer_experience.app;

--Warehouse
CREATE OR REPLACE WAREHOUSE tasty_bytes_enhancing_customer_experience_wh with
WAREHOUSE_SIZE = LARGE
AUTO_SUSPEND = 60;

-- create roles
USE ROLE securityadmin;

-- functional roles
CREATE ROLE IF NOT EXISTS enhancing_customer_expereience_role
    COMMENT = 'app user for tasty bytes support app';
    
-- role hierarchy
GRANT ROLE enhancing_customer_expereience_role TO ROLE sysadmin;

GRANT USAGE ON DATABASE tasty_bytes_enhancing_customer_experience TO ROLE enhancing_customer_expereience_role;
GRANT USAGE ON ALL SCHEMAS IN DATABASE tasty_bytes_enhancing_customer_experience TO ROLE enhancing_customer_expereience_role;
GRANT ALL ON SCHEMA tasty_bytes_enhancing_customer_experience.raw_doc TO ROLE enhancing_customer_expereience_role;
GRANT ALL ON SCHEMA tasty_bytes_enhancing_customer_experience.raw_pos TO ROLE enhancing_customer_expereience_role;
GRANT ALL ON SCHEMA tasty_bytes_enhancing_customer_experience.harmonized TO ROLE enhancing_customer_expereience_role;
GRANT ALL ON SCHEMA tasty_bytes_enhancing_customer_experience.analytics TO ROLE enhancing_customer_expereience_role;
GRANT ALL ON SCHEMA tasty_bytes_enhancing_customer_experience.app TO ROLE enhancing_customer_expereience_role;

-- warehouse grants
GRANT OWNERSHIP ON WAREHOUSE tasty_bytes_enhancing_customer_experience_wh TO ROLE enhancing_customer_expereience_role COPY CURRENT GRANTS;
GRANT ALL ON WAREHOUSE tasty_bytes_enhancing_customer_experience_wh TO ROLE enhancing_customer_expereience_role;

-- future grants
GRANT ALL ON FUTURE TABLES IN SCHEMA tasty_bytes_enhancing_customer_experience.raw_doc TO ROLE enhancing_customer_expereience_role;
GRANT ALL ON FUTURE TABLES IN SCHEMA tasty_bytes_enhancing_customer_experience.raw_pos TO ROLE enhancing_customer_expereience_role;
GRANT ALL ON FUTURE TABLES IN SCHEMA tasty_bytes_enhancing_customer_experience.harmonized TO ROLE enhancing_customer_expereience_role;
GRANT ALL ON FUTURE TABLES IN SCHEMA tasty_bytes_enhancing_customer_experience.analytics TO ROLE enhancing_customer_expereience_role;
GRANT ALL ON FUTURE TABLES IN SCHEMA tasty_bytes_enhancing_customer_experience.app TO ROLE enhancing_customer_expereience_role;

GRANT ALL ON FUTURE VIEWS IN SCHEMA tasty_bytes_enhancing_customer_experience.raw_doc TO ROLE enhancing_customer_expereience_role;
GRANT ALL ON FUTURE VIEWS IN SCHEMA tasty_bytes_enhancing_customer_experience.raw_pos TO ROLE enhancing_customer_expereience_role;
GRANT ALL ON FUTURE VIEWS IN SCHEMA tasty_bytes_enhancing_customer_experience.harmonized TO ROLE enhancing_customer_expereience_role;
GRANT ALL ON FUTURE VIEWS IN SCHEMA tasty_bytes_enhancing_customer_experience.analytics TO ROLE enhancing_customer_expereience_role;
GRANT ALL ON FUTURE VIEWS IN SCHEMA tasty_bytes_enhancing_customer_experience.app TO ROLE enhancing_customer_expereience_role;

USE ROLE sysadmin;

CREATE OR REPLACE FILE FORMAT tasty_bytes_enhancing_customer_experience.raw_doc.csv_ff 
TYPE = 'csv';

CREATE OR REPLACE STAGE tasty_bytes_enhancing_customer_experience.raw_doc.s3load
COMMENT = 'Quickstarts S3 Stage Connection'
url = 's3://sfquickstarts/tastybytes-ece/'
file_format = tasty_bytes_enhancing_customer_experience.raw_doc.csv_ff;

-- stages
CREATE OR REPLACE STAGE tasty_bytes_enhancing_customer_experience.raw_doc.inspection_reports
COMMENT = 'Inspection reports images'
DIRECTORY = (ENABLE = TRUE) 
ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE');

CREATE OR REPLACE STAGE tasty_bytes_enhancing_customer_experience.app.enhance_customer_experience_app 
DIRECTORY = (ENABLE = TRUE) 
ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE');

-- raw_doc
CREATE OR REPLACE TABLE tasty_bytes_enhancing_customer_experience.raw_doc.inspection_report_raw_extract (
	FILE_NAME VARCHAR(16777216),
	EXTRACTION_TIMESTAMP TIMESTAMP_LTZ(9),
	INSPECTION_REPORT_OBJECT VARIANT
);

CREATE OR REPLACE TABLE tasty_bytes_enhancing_customer_experience.raw_doc.inspection_report_ref (
	IRR_ID NUMBER(38,0),
	COLUMN_NAME VARCHAR(16777216),
	DESCRIPTION VARCHAR(16777216),
	CATEGORY VARCHAR(16777216)
);

-- raw_pos
CREATE OR REPLACE TABLE tasty_bytes_enhancing_customer_experience.raw_pos.truck (
	TRUCK_ID NUMBER(38,0),
	MENU_TYPE_ID NUMBER(38,0),
	PRIMARY_CITY VARCHAR(16777216),
	REGION VARCHAR(16777216),
	ISO_REGION VARCHAR(16777216),
	COUNTRY VARCHAR(16777216),
	ISO_COUNTRY_CODE VARCHAR(16777216),
	FRANCHISE_FLAG NUMBER(38,0),
	YEAR NUMBER(38,0),
	MAKE VARCHAR(16777216),
	MODEL VARCHAR(16777216),
	EV_FLAG NUMBER(38,0),
	FRANCHISE_ID NUMBER(38,0),
	TRUCK_OPENING_DATE DATE
);

CREATE OR REPLACE TABLE tasty_bytes_enhancing_customer_experience.raw_pos.menu (
	MENU_ID NUMBER(19,0),
	MENU_TYPE_ID NUMBER(38,0),
	MENU_TYPE VARCHAR(16777216),
	TRUCK_BRAND_NAME VARCHAR(16777216),
	MENU_ITEM_ID NUMBER(38,0),
	MENU_ITEM_NAME VARCHAR(16777216),
	ITEM_CATEGORY VARCHAR(16777216),
	ITEM_SUBCATEGORY VARCHAR(16777216),
	COST_OF_GOODS_USD NUMBER(38,4),
	SALE_PRICE_USD NUMBER(38,4),
	MENU_ITEM_HEALTH_METRICS_OBJ VARIANT
);

-- harmonized
CREATE OR REPLACE VIEW tasty_bytes_enhancing_customer_experience.harmonized.inspection_reports_v(
	DATE,
	TRUCK_ID,
	TRUCK_BRAND_NAME,
	CITY,
	COUNTRY,
	PIC_PRESENT,
	PIC_KNOWLEDGE,
	HANDS_CLEAN_WASHED,
	ADEQUATE_HW_FACILITIES,
	FOOD_APPROVED_SOURCE,
	FOOD_PROPER_TEMP,
	THERMOMETERS_ACCURATE,
	FOOD_PROTECTED,
	FOOD_SURFACE_CLEAN,
	NONFOOD_SURFACE_CLEAN,
	WATER_ICE_SOURCE_APPROVED,
	WATER_ICE_CONTAMINANT_FREE,
	UTENSILS_PROPER_STORAGE,
	GLOVES_USE_PROPER,
	WATER_PRESSURE_ADEQUATE,
	SEWAGE_DISPOSED_PROPERLY,
	GARBAGE_DISPOSED_PROPERLY,
	VENTILATION_LIGHTING_ADEQUATE,
	TIRES_ADEQUATE,
	VEHICLE_WELL_MAINTAINED,
	FILE_NAME,
	SCOPED_URL,
	PRESIGNED_URL
) AS (
WITH _menu_and_truck_name AS (SELECT DISTINCT menu_type_id, truck_brand_name FROM tasty_bytes_enhancing_customer_experience.raw_pos.menu)
SELECT 
    TO_DATE(REPLACE(inspection_report_object:"DATE"[0].value::varchar,'-','/')) AS date,
    inspection_report_object:"TRUCK_ID"[0].value::integer AS truck_id,
    mt.truck_brand_name,
    INITCAP(inspection_report_object:"CITY"[0].value::varchar) AS city,
    CASE 
        WHEN inspection_report_object:"COUNTRY"[0].value::varchar IN ('USA','US') THEN 'United States'
        ELSE inspection_report_object:"COUNTRY"[0].value::varchar
    END AS country,
    CASE 
        WHEN inspection_report_object:"PIC_PRESENT"[0].value::varchar = 'Y' THEN 'Pass'
        WHEN inspection_report_object:"PIC_PRESENT"[0].value::varchar = 'N' THEN 'Fail'
        WHEN inspection_report_object:"PIC_PRESENT"[0].value::varchar = 'X' THEN 'Not Observed'
        ELSE 'Not Observed'
    END AS pic_present,
    CASE
        WHEN inspection_report_object:"PIC_KNOWLEDGE"[0].value::varchar = 'Y' THEN 'Pass'
        WHEN inspection_report_object:"PIC_KNOWLEDGE"[0].value::varchar = 'N' THEN 'Fail'
        WHEN inspection_report_object:"PIC_KNOWLEDGE"[0].value::varchar = 'X' THEN 'Not Observed'
        ELSE 'Not Observed'
    END AS pic_knowledge,
    CASE 
        WHEN inspection_report_object:"HANDS_CLEAN_WASHED"[0].value::varchar = 'Y' THEN 'Pass'
        WHEN inspection_report_object:"HANDS_CLEAN_WASHED"[0].value::varchar = 'N' THEN 'Fail'
        WHEN inspection_report_object:"HANDS_CLEAN_WASHED"[0].value::varchar = 'X' THEN 'Not Observed'
        ELSE 'Not Observed'
    END AS hands_clean_washed,
    CASE 
        WHEN inspection_report_object:"ADEQUATE_HW_FACILITIES"[0].value::varchar  = 'Y' THEN 'Pass'
        WHEN inspection_report_object:"ADEQUATE_HW_FACILITIES"[0].value::varchar  = 'N' THEN 'Fail'
        WHEN inspection_report_object:"ADEQUATE_HW_FACILITIES"[0].value::varchar  = 'X' THEN 'Not Observed'
        ELSE 'Not Observed'
    END AS adequate_hw_facilities,
    CASE
        WHEN inspection_report_object:"FOOD_APPROVED_SOURCE"[0].value::varchar = 'Y' THEN 'Pass'
        WHEN inspection_report_object:"FOOD_APPROVED_SOURCE"[0].value::varchar = 'N' THEN 'Fail'
        WHEN inspection_report_object:"FOOD_APPROVED_SOURCE"[0].value::varchar = 'X' THEN 'Not Observed'
        ELSE 'Not Observed'
    END AS food_approved_source,
    CASE
        WHEN inspection_report_object:"FOOD_PROPER_TEMP"[0].value::varchar = 'Y' THEN 'Pass'
        WHEN inspection_report_object:"FOOD_PROPER_TEMP"[0].value::varchar = 'N' THEN 'Fail'
        WHEN inspection_report_object:"FOOD_PROPER_TEMP"[0].value::varchar = 'X' THEN 'Not Observed'
        ELSE 'Not Observed'
    END AS food_proper_temp,
    CASE
        WHEN inspection_report_object:"THERMOMETERS_ACCURATE"[0].value::varchar  = 'Y' THEN 'Pass'
        WHEN inspection_report_object:"THERMOMETERS_ACCURATE"[0].value::varchar  = 'N' THEN 'Fail'
        WHEN inspection_report_object:"THERMOMETERS_ACCURATE"[0].value::varchar  = 'X' THEN 'Not Observed'
        ELSE 'Not Observed'
    END AS thermometers_accurate,
    CASE
        WHEN inspection_report_object:"FOOD_PROTECTED"[0].value::varchar = 'Y' THEN 'Pass'
        WHEN inspection_report_object:"FOOD_PROTECTED"[0].value::varchar = 'N' THEN 'Fail'
        WHEN inspection_report_object:"FOOD_PROTECTED"[0].value::varchar = 'X' THEN 'Not Observed'
        ELSE 'Not Observed'
    END AS food_protected,
    CASE
        WHEN inspection_report_object:"FOOD_SURFACE_CLEAN"[0].value::varchar = 'Y' THEN 'Pass'
        WHEN inspection_report_object:"FOOD_SURFACE_CLEAN"[0].value::varchar = 'N' THEN 'Fail'
        WHEN inspection_report_object:"FOOD_SURFACE_CLEAN"[0].value::varchar = 'X' THEN 'Not Observed'
        ELSE 'Not Observed'
    END AS food_surface_clean,
    CASE
        WHEN inspection_report_object:"NONFOOD_SURFACE_CLEAN"[0].value::varchar = 'Y' THEN 'Pass'
        WHEN inspection_report_object:"NONFOOD_SURFACE_CLEAN"[0].value::varchar = 'N' THEN 'Fail'
        WHEN inspection_report_object:"NONFOOD_SURFACE_CLEAN"[0].value::varchar = 'X' THEN 'Not Observed'
        ELSE 'Not Observed'
    END AS nonfood_surface_clean,
    CASE
        WHEN inspection_report_object:"WATER_ICE_SOURCE_APPROVED"[0].value::varchar = 'Y' THEN 'Pass'
        WHEN inspection_report_object:"WATER_ICE_SOURCE_APPROVED"[0].value::varchar = 'N' THEN 'Fail'
        WHEN inspection_report_object:"WATER_ICE_SOURCE_APPROVED"[0].value::varchar = 'X' THEN 'Not Observed'
        ELSE 'Not Observed'
    END AS water_ice_source_approved,
    CASE
        WHEN inspection_report_object:"WATER_ICE_CONTAMINANT_FREE"[0].value::varchar = 'Y' THEN 'Pass'
        WHEN inspection_report_object:"WATER_ICE_CONTAMINANT_FREE"[0].value::varchar = 'N' THEN 'Fail'
        WHEN inspection_report_object:"WATER_ICE_CONTAMINANT_FREE"[0].value::varchar = 'X' THEN 'Not Observed'
        ELSE 'Not Observed' 
    END AS water_ice_contaminant_free,
    CASE
        WHEN inspection_report_object:"UTENSILS_PROPER_STORAGE"[0].value::varchar = 'Y' THEN 'Pass'
        WHEN inspection_report_object:"UTENSILS_PROPER_STORAGE"[0].value::varchar = 'N' THEN 'Fail'
        WHEN inspection_report_object:"UTENSILS_PROPER_STORAGE"[0].value::varchar = 'X' THEN 'Not Observed'
        ELSE 'Not Observed' 
    END AS utensils_proper_storage,
    CASE
        WHEN inspection_report_object:"GLOVES_USE_PROPER"[0].value::varchar = 'Y' THEN 'Pass'
        WHEN inspection_report_object:"GLOVES_USE_PROPER"[0].value::varchar = 'N' THEN 'Fail'
        WHEN inspection_report_object:"GLOVES_USE_PROPER"[0].value::varchar = 'X' THEN 'Not Observed'
        ELSE 'Not Observed' 
    END AS gloves_use_proper,
    CASE
        WHEN inspection_report_object:"WATER_PRESSURE_ADEQUATE"[0].value::varchar = 'Y' THEN 'Pass'
        WHEN inspection_report_object:"WATER_PRESSURE_ADEQUATE"[0].value::varchar = 'N' THEN 'Fail'
        WHEN inspection_report_object:"WATER_PRESSURE_ADEQUATE"[0].value::varchar = 'X' THEN 'Not Observed'
        ELSE 'Not Observed' 
    END AS water_pressure_adequate,
    CASE
        WHEN inspection_report_object:"SEWAGE_DISPOSED_PROPERLY"[0].value::varchar = 'Y' THEN 'Pass'
        WHEN inspection_report_object:"SEWAGE_DISPOSED_PROPERLY"[0].value::varchar = 'N' THEN 'Fail'
        WHEN inspection_report_object:"SEWAGE_DISPOSED_PROPERLY"[0].value::varchar = 'X' THEN 'Not Observed'
        ELSE 'Not Observed' 
    END AS sewage_disposed_properly,
    CASE
        WHEN inspection_report_object:"GARBAGE_DISPOSED_PROPERLY"[0].value::varchar = 'Y' THEN 'Pass'
        WHEN inspection_report_object:"GARBAGE_DISPOSED_PROPERLY"[0].value::varchar = 'N' THEN 'Fail'
        WHEN inspection_report_object:"GARBAGE_DISPOSED_PROPERLY"[0].value::varchar = 'X' THEN 'Not Observed'
        ELSE 'Not Observed'
    END AS garbage_disposed_properly,
    CASE
        WHEN inspection_report_object:"VENTILATION_LIGHTING_ADEQUATE"[0].value::varchar = 'Y' THEN 'Pass'
        WHEN inspection_report_object:"VENTILATION_LIGHTING_ADEQUATE"[0].value::varchar = 'N' THEN 'Fail'
        WHEN inspection_report_object:"VENTILATION_LIGHTING_ADEQUATE"[0].value::varchar = 'X' THEN 'Not Observed'
        ELSE 'Not Observed'
    END AS ventilation_lighting_adequate,
    CASE
        WHEN inspection_report_object:"TIRES_ADEQUATE"[0].value::varchar = 'Y' THEN 'Pass'
        WHEN inspection_report_object:"TIRES_ADEQUATE"[0].value::varchar = 'N' THEN 'Fail'
        WHEN inspection_report_object:"TIRES_ADEQUATE"[0].value::varchar = 'X' THEN 'Not Observed'
        ELSE 'Not Observed'
    END AS tires_adequate,
    CASE
        WHEN inspection_report_object:"VEHICLE_WELL_MAINTAINED"[0].value::varchar  = 'Y' THEN 'Pass'
        WHEN inspection_report_object:"VEHICLE_WELL_MAINTAINED"[0].value::varchar  = 'N' THEN 'Fail'
        WHEN inspection_report_object:"VEHICLE_WELL_MAINTAINED"[0].value::varchar  = 'X' THEN 'Not Observed'
        ELSE 'Not Observed'
    END AS vehicle_well_maintained,
    file_name,
    BUILD_SCOPED_FILE_URL(@tasty_bytes_enhancing_customer_experience.raw_doc.inspection_reports, file_name) AS scoped_url,
    GET_PRESIGNED_URL(@tasty_bytes_enhancing_customer_experience.raw_doc.inspection_reports, file_name) AS presigned_url
FROM tasty_bytes_enhancing_customer_experience.raw_doc.inspection_report_raw_extract ir
JOIN tasty_bytes_enhancing_customer_experience.raw_pos.truck t
    ON ir.inspection_report_object:"TRUCK_ID"[0].value::integer = t.truck_id
JOIN _menu_and_truck_name mt
    ON t.menu_type_id = mt.menu_type_id
  );
CREATE OR REPLACE VIEW tasty_bytes_enhancing_customer_experience.harmonized.inspection_report_ref_v(
	IRR_ID,
	COLUMN_NAME,
	DESCRIPTION,
	CATEGORY
) as (
SELECT * FROM tasty_bytes_enhancing_customer_experience.raw_doc.inspection_report_ref
  );

-- analytics
CREATE OR REPLACE VIEW tasty_bytes_enhancing_customer_experience.analytics.inspection_reports_v(
	DATE,
	TRUCK_ID,
	TRUCK_BRAND_NAME,
	CITY,
	COUNTRY,
	PIC_PRESENT,
	PIC_KNOWLEDGE,
	HANDS_CLEAN_WASHED,
	ADEQUATE_HW_FACILITIES,
	FOOD_APPROVED_SOURCE,
	FOOD_PROPER_TEMP,
	THERMOMETERS_ACCURATE,
	FOOD_PROTECTED,
	FOOD_SURFACE_CLEAN,
	NONFOOD_SURFACE_CLEAN,
	WATER_ICE_SOURCE_APPROVED,
	WATER_ICE_CONTAMINANT_FREE,
	UTENSILS_PROPER_STORAGE,
	GLOVES_USE_PROPER,
	WATER_PRESSURE_ADEQUATE,
	SEWAGE_DISPOSED_PROPERLY,
	GARBAGE_DISPOSED_PROPERLY,
	VENTILATION_LIGHTING_ADEQUATE,
	TIRES_ADEQUATE,
	VEHICLE_WELL_MAINTAINED,
	FILE_NAME,
	SCOPED_URL,
	PRESIGNED_URL,
	OVERALL_RESULT
) as (
    
        
SELECT *,
   CASE
       WHEN (
             CASE WHEN pic_present = 'Fail' THEN 1 ELSE 0 END +
             CASE WHEN pic_knowledge = 'Fail' THEN 1 ELSE 0 END +
             CASE WHEN hands_clean_washed = 'Fail' THEN 1 ELSE 0 END +
             CASE WHEN adequate_hw_facilities = 'Fail' THEN 1 ELSE 0 END +
             CASE WHEN food_approved_source = 'Fail' THEN 1 ELSE 0 END +
             CASE WHEN food_proper_temp = 'Fail' THEN 1 ELSE 0 END +
             CASE WHEN thermometers_accurate = 'Fail' THEN 1 ELSE 0 END +
             CASE WHEN food_protected = 'Fail' THEN 1 ELSE 0 END +
             CASE WHEN food_surface_clean = 'Fail' THEN 1 ELSE 0 END +
             CASE WHEN nonfood_surface_clean = 'Fail' THEN 1 ELSE 0 END +
             CASE WHEN water_ice_source_approved = 'Fail' THEN 1 ELSE 0 END +
             CASE WHEN water_ice_contaminant_free = 'Fail' THEN 1 ELSE 0 END +
             CASE WHEN utensils_proper_storage = 'Fail' THEN 1 ELSE 0 END +
             CASE WHEN gloves_use_proper = 'Fail' THEN 1 ELSE 0 END +
             CASE WHEN water_pressure_adequate = 'Fail' THEN 1 ELSE 0 END +
             CASE WHEN sewage_disposed_properly = 'Fail' THEN 1 ELSE 0 END +
             CASE WHEN garbage_disposed_properly = 'Fail' THEN 1 ELSE 0 END +
             CASE WHEN ventilation_lighting_adequate = 'Fail' THEN 1 ELSE 0 END +
             CASE WHEN tires_adequate = 'Fail' THEN 1 ELSE 0 END +
             CASE WHEN vehicle_well_maintained = 'Fail' THEN 1 ELSE 0 END) >= 5 THEN 'Fail'
       ELSE 'Pass'
       END AS overall_result
FROM tasty_bytes_enhancing_customer_experience.harmonized.inspection_reports_v
  );

CREATE OR REPLACE TABLE tasty_bytes_enhancing_customer_experience.analytics.review_analysis_output (
	TRUCK_ID NUMBER(38,0),
	REVIEW_ID NUMBER(18,0),
	REVIEW VARCHAR(16777216),
	SENTIMENT FLOAT,
	TRANSLATED_REVIEW VARCHAR(16777216),
	RATING VARCHAR(16777216),
	CLEAN_RATING VARCHAR(16777216),
	RECOMMEND VARCHAR(16777216),
	CLEAN_RECOMMEND VARCHAR(16777216),
	CATEGORY_SENTIMENT VARCHAR(16777216)
);

CREATE OR REPLACE VIEW tasty_bytes_enhancing_customer_experience.analytics.inspection_reports_unpivot_v(
	DATE,
	TRUCK_ID,
	CATEGORY,
	INSPECTION_ITEM,
	DESCRIPTION,
	RESULT
) as (
WITH _unpivot_inspections AS (
    SELECT 
        date, 
        truck_id,
        inspection_item,
        result
    FROM 
        (SELECT 
            date, 
            truck_id,
            pic_present, 
            pic_knowledge, 
            hands_clean_washed, 
            adequate_hw_facilities, 
            food_approved_source, 
            food_proper_temp, 
            thermometers_accurate, 
            food_protected, 
            food_surface_clean, 
            nonfood_surface_clean, 
            water_ice_source_approved, 
            water_ice_contaminant_free, 
            utensils_proper_storage, 
            gloves_use_proper, 
            water_pressure_adequate, 
            sewage_disposed_properly, 
            garbage_disposed_properly, 
            ventilation_lighting_adequate, 
            tires_adequate, 
            vehicle_well_maintained
        FROM tasty_bytes_enhancing_customer_experience.harmonized.inspection_reports_v
        ) UNPIVOT (
            result FOR inspection_item IN (
                pic_present, 
                pic_knowledge, 
                hands_clean_washed, 
                adequate_hw_facilities, 
                food_approved_source, 
                food_proper_temp, 
                thermometers_accurate, 
                food_protected, 
                food_surface_clean, 
                nonfood_surface_clean, 
                water_ice_source_approved, 
                water_ice_contaminant_free, 
                utensils_proper_storage, 
                gloves_use_proper, 
                water_pressure_adequate, 
                sewage_disposed_properly, 
                garbage_disposed_properly, 
                ventilation_lighting_adequate, 
                tires_adequate, 
                vehicle_well_maintained
            )
        )
)
SELECT 
    u.date,
    u.truck_id,
    r.category,
    u.inspection_item,
    r.description,
    u.result
FROM _unpivot_inspections u
JOIN  tasty_bytes_enhancing_customer_experience.harmonized.inspection_report_ref_v r
    ON u.inspection_item = r.column_name
ORDER BY u.date, u.truck_id, u.inspection_item
  );

CREATE OR REPLACE VIEW tasty_bytes_enhancing_customer_experience.analytics.review_analysis_output_v(
	TRUCK_ID,
	REVIEW_ID,
	REVIEW,
	SENTIMENT,
	TRANSLATED_REVIEW,
	RATING,
	CLEAN_RATING,
	RECOMMEND,
	CLEAN_RECOMMEND,
	CATEGORY,
	CATEGORY_SENTIMENT,
	DETAILS
) AS (
SELECT 
    rao.* EXCLUDE category_sentiment,
    value:category::STRING as category,
    value:sentiment::STRING as category_sentiment,
    value:details::STRING as details
FROM tasty_bytes_enhancing_customer_experience.analytics.review_analysis_output rao,
LATERAL FLATTEN (PARSE_JSON(TO_VARIANT(rao.category_sentiment))) cs
  );

-- insert data
COPY INTO tasty_bytes_enhancing_customer_experience.raw_doc.inspection_report_raw_extract
FROM @tasty_bytes_enhancing_customer_experience.raw_doc.s3load/raw_doc/inspection_report_raw_extract/;

COPY INTO tasty_bytes_enhancing_customer_experience.raw_doc.inspection_report_ref
FROM @tasty_bytes_enhancing_customer_experience.raw_doc.s3load/raw_doc/inspection_report_ref/;

COPY INTO tasty_bytes_enhancing_customer_experience.raw_pos.truck
FROM @tasty_bytes_enhancing_customer_experience.raw_doc.s3load/raw_pos/truck/;

COPY INTO tasty_bytes_enhancing_customer_experience.raw_pos.menu
FROM @tasty_bytes_enhancing_customer_experience.raw_doc.s3load/raw_pos/menu/;

COPY INTO tasty_bytes_enhancing_customer_experience.analytics.review_analysis_output
FROM @tasty_bytes_enhancing_customer_experience.raw_doc.s3load/analytics/review_analysis_output/;

-- copy image files into stage
COPY FILES
  INTO @tasty_bytes_enhancing_customer_experience.raw_doc.inspection_reports
  FROM @tasty_bytes_enhancing_customer_experience.raw_doc.s3load/raw_doc/inspection_report/;