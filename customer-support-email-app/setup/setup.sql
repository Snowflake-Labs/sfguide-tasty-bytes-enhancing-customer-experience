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
2024-06-23 4        Charlie Hammond     Initial Release
***************************************************************************************************/

USE ROLE sysadmin;

CREATE OR REPLACE DATABASE tasty_bytes_customer_support_email;

--Schema
CREATE OR REPLACE SCHEMA tasty_bytes_customer_support_email.raw_support;
CREATE OR REPLACE SCHEMA tasty_bytes_customer_support_email.harmonized;
CREATE OR REPLACE SCHEMA tasty_bytes_customer_support_email.app;


--Warehouse
CREATE OR REPLACE WAREHOUSE tasty_bytes_customer_support_email_wh with
WAREHOUSE_SIZE = LARGE
AUTO_SUSPEND = 60;

-- create roles
USE ROLE securityadmin;

-- functional roles
CREATE ROLE IF NOT EXISTS customer_support_email_role
    COMMENT = 'app user for tasty bytes support app';
    
-- role hierarchy
GRANT ROLE customer_support_email_role TO ROLE sysadmin;

GRANT USAGE ON DATABASE tasty_bytes_customer_support_email TO ROLE customer_support_email_role;
GRANT USAGE ON ALL SCHEMAS IN DATABASE tasty_bytes_customer_support_email TO ROLE customer_support_email_role;
GRANT ALL ON SCHEMA tasty_bytes_customer_support_email.raw_support TO ROLE customer_support_email_role;
GRANT ALL ON SCHEMA tasty_bytes_customer_support_email.harmonized TO ROLE customer_support_email_role;
GRANT ALL ON SCHEMA tasty_bytes_customer_support_email.app TO ROLE customer_support_email_role;

-- warehouse grants
GRANT OWNERSHIP ON WAREHOUSE tasty_bytes_customer_support_email_wh TO ROLE customer_support_email_role COPY CURRENT GRANTS;
GRANT ALL ON WAREHOUSE tasty_bytes_customer_support_email_wh TO ROLE customer_support_email_role;

-- future grants
GRANT ALL ON FUTURE TABLES IN SCHEMA tasty_bytes_customer_support_email.raw_support TO ROLE customer_support_email_role;

GRANT ALL ON FUTURE TABLES IN SCHEMA tasty_bytes_customer_support_email.harmonized TO ROLE customer_support_email_role;
GRANT ALL ON FUTURE TABLES IN SCHEMA tasty_bytes_customer_support_email.app TO ROLE customer_support_email_role;

GRANT ALL ON FUTURE VIEWS IN SCHEMA tasty_bytes_customer_support_email.raw_support TO ROLE customer_support_email_role;

GRANT ALL ON FUTURE VIEWS IN SCHEMA tasty_bytes_customer_support_email.harmonized TO ROLE customer_support_email_role;
GRANT ALL ON FUTURE VIEWS IN SCHEMA tasty_bytes_customer_support_email.app TO ROLE customer_support_email_role;

USE ROLE sysadmin;

-- file format for loading data
CREATE OR REPLACE FILE FORMAT tasty_bytes_customer_support_email.raw_support.csv_ff 
TYPE = 'csv';

-- stage to link S3 bucket
CREATE OR REPLACE STAGE tasty_bytes_customer_support_email.raw_support.s3load
COMMENT = 'Quickstarts S3 Stage Connection'
url = 's3://sfquickstarts/tastybytes-ece/'
file_format = tasty_bytes_customer_support_email.raw_support.csv_ff;

-- stage for app files
CREATE OR REPLACE STAGE tasty_bytes_customer_support_email.app.customer_support_email_app 
DIRECTORY = (ENABLE = TRUE) 
ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE');

-- raw support tables
CREATE OR REPLACE TABLE tasty_bytes_customer_support_email.raw_support.email_status_app (
	EMAIL_ID NUMBER(38,0) NOT NULL autoincrement start 1 increment 1 noorder,
	EMAIL_STATUS_CODE NUMBER(38,0),
	EMAIL_STATUS_DEF VARCHAR(255),
	RESPONDED_FLAG BOOLEAN,
	LAST_UPDATED_TS TIMESTAMP_NTZ(9),
	EMAIL_OBJECT VARIANT
);

CREATE OR REPLACE TABLE tasty_bytes_customer_support_email.raw_support.email_response_app (
	EMAIL_R_ID NUMBER(38,0) NOT NULL autoincrement start 1000 increment 1 noorder,
	EMAIL_ID NUMBER(38,0),
	EMAIL_RESPONSE VARIANT,
	SENT_TS TIMESTAMP_NTZ(9)
);

-- harmonized tables
CREATE OR REPLACE TABLE tasty_bytes_customer_support_email.harmonized.chunk_text (
	SOURCE VARCHAR(6),
	SOURCE_DESC VARCHAR(16777216),
	FULL_TEXT VARCHAR(16777216),
	SIZE NUMBER(18,0),
	CHUNK VARCHAR(16777216),
	META VARCHAR(16777216),
	INPUT_TEXT VARCHAR(16777216)
);

CREATE OR REPLACE TABLE tasty_bytes_customer_support_email.harmonized.documents (
	RELATIVE_PATH VARCHAR(16777216),
	RAW_TEXT VARCHAR(16777216)
);

-- https://docs.snowflake.com/en/sql-reference/data-types-vector#loading-and-unloading-vector-data
CREATE OR REPLACE TABLE tasty_bytes_customer_support_email.harmonized.array_table (
  SOURCE VARCHAR(6),
	SOURCE_DESC VARCHAR(16777216),
	FULL_TEXT VARCHAR(16777216),
	SIZE NUMBER(18,0),
	CHUNK VARCHAR(16777216),
	INPUT_TEXT VARCHAR(16777216),
	CHUNK_EMBEDDING ARRAY
);

CREATE OR REPLACE FUNCTION tasty_bytes_customer_support_email.harmonized.text_chunker("TEXT" VARCHAR(16777216))
RETURNS TABLE ("CHUNK" VARCHAR(16777216), "META" VARCHAR(16777216))
LANGUAGE PYTHON
RUNTIME_VERSION = '3.9'
PACKAGES = ('snowflake-snowpark-python','langchain')
HANDLER = 'text_chunker'
AS '
from langchain.text_splitter import RecursiveCharacterTextSplitter
import pandas as pd

class text_chunker:

    def process(self,text):        
        text_raw=[text]
        
        text_splitter = RecursiveCharacterTextSplitter(
            separators = ["\\n"],
            chunk_size = 5000, # Adjust chunk size
            chunk_overlap  = 3000, # Adjust overlap
            length_function = len,
            add_start_index = True,
        )
    
        chunks = text_splitter.create_documents(text_raw)
        
	# Extract attributes from documents
        data = [{"content": chunk.page_content, "metadata": chunk.metadata["start_index"]} for chunk in chunks]

        # Convert the data into a pandas DataFrame
        df = pd.DataFrame(data)
        
        yield from df.itertuples(index=False, name=None)
';

-- procedures
CREATE OR REPLACE PROCEDURE tasty_bytes_customer_support_email.raw_support.insert_new_email_app("email_str" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS '
    // Escape single quotes in email string
    var temp = email_str.replace(/''''/g, "''''''''");
    
    // Construct JSON string
    var jsonStr = `{"Request": "${temp.replace(/"/g, ''\\\\"'')}"}`;
    
    // Create temporary table and insert email data
    var query1 = "\\
        CREATE OR REPLACE TEMPORARY TABLE raw_support.email_data_gen_tt_app \\
        ( \\
            email_object VARIANT \\
        ) \\
        AS \\
        SELECT \\
            PARSE_JSON(:1) AS email_object \\
    ";
    var statement1 = snowflake.createStatement({sqlText: query1, binds: [jsonStr]});
    statement1.execute();
    
    // Insert email status for new emails
    var query2 = `
        INSERT INTO raw_support.email_status_app
        (email_status_code, email_status_def, responded_flag, last_updated_ts, email_object)
        SELECT
            ''1'' AS email_status_code,
            ''new'' AS email_status_def,
            ''false'' AS responded_flag,
            CURRENT_TIMESTAMP() AS last_updated_ts,
            email_object
        FROM raw_support.email_data_gen_tt_app
    `;
    var statement2 = snowflake.createStatement({sqlText: query2});
    statement2.execute();

    return "Email Sent successfully";
';

CREATE OR REPLACE PROCEDURE tasty_bytes_customer_support_email.raw_support.process_auto_responses_app()
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS '
    var sql1, sql2, sql3, sql4, sql5, sql6, stmt1, stmt2, stmt3, stmt4, stmt5, stmt6;
    var responderValue, resultSet;
    // Create or replace the transient table
    sql1 = `
        CREATE OR REPLACE TEMPORARY TABLE raw_support.auto_response_tt_app
            AS
        WITH _new_email_embedding AS 
        (
            SELECT 
                 email_id,
                 email_object AS email,
                 snowflake.cortex.EMBED_TEXT_768(''e5-base-v2'', email_object) AS email_embedding
            FROM raw_support.email_status_app 
            WHERE email_status_code = 1
        ),
        _ranked_chunks AS 
        (
            SELECT
                e.email_id,
                e.email,
                vs.input_text,
                VECTOR_COSINE_SIMILARITY(e.email_embedding, vs.chunk_embedding) AS similarity
            FROM _new_email_embedding e
            CROSS JOIN harmonized.vector_store vs
            WHERE vs.source IN (''PUBLIC'', ''EMAIL'')
            ORDER BY similarity desc
            limit 1
        ),
        _responses AS (
            SELECT
                email_id,
                email,
                input_text AS context,
                snowflake.cortex.complete(''mistral-large'', TO_VARIANT(CONCAT(
                    ''Provide a response to this customer email only if you can fully address it with the provided documentation, do not provide generic responses. If you can address fully and response is available, follow Answer Format provided Strictly and Provide a valid JSON. If not, return NULL. Nothing else. Email: '',
                    email,
                    ''. Context from documentation: '',
                    context,
                    '' Answer Format: {"Response": "{"responder":"<agent or auto>", "subject": "<subject_line_of_email>", "body": "<email_body>"}"}.''
                ))) AS response
            FROM _ranked_chunks
        )
        SELECT
            email_id,
            email AS original_email,
            CASE
                WHEN TRIM(TRIM(response, ''\\\\t\\\\n''), '' '') = ''NULL'' THEN NULL
                ELSE TRIM(TRIM(response, ''\\\\t\\\\n''), '' '')
            END AS auto_response
        FROM _responses
        WHERE auto_response IS NOT NULL;
    `;
    stmt1 = snowflake.createStatement({sqlText: sql1});
    stmt1.execute();

    // Insert into email_response table
    sql2 = `
        INSERT INTO raw_support.email_response_app (email_id, sent_ts)
        SELECT email_id, CURRENT_TIMESTAMP() FROM raw_support.auto_response_tt_app;
    `;
    stmt2 = snowflake.createStatement({sqlText: sql2});
    stmt2.execute();

    // Update email_response table with responses
    sql3 = `
        UPDATE raw_support.email_response_app AS er
        SET er.email_response = PARSE_JSON(ar.auto_response)
        FROM raw_support.auto_response_tt_app AS ar
        WHERE er.email_id = ar.email_id;
    `;
    stmt3 = snowflake.createStatement({sqlText: sql3});
    stmt3.execute();

    // Update email statuses
    sql4 = `
        UPDATE raw_support.email_status_app
        SET 
            email_status_code = 2,
            email_status_def = ''auto'',
            last_updated_ts = CURRENT_TIMESTAMP(),
            responded_flag = TRUE
        WHERE email_id IN (SELECT email_id FROM raw_support.auto_response_tt_app);
    `;
    stmt4 = snowflake.createStatement({sqlText: sql4});
    stmt4.execute();

    sql5 = `
        UPDATE raw_support.email_status_app
        SET 
            email_status_code = 3,
            email_status_def = ''agent'',
            last_updated_ts = CURRENT_TIMESTAMP()
        WHERE email_id IN (SELECT email_id FROM raw_support.email_status_app WHERE email_status_code = 1);
    `;
    stmt5 = snowflake.createStatement({sqlText: sql5});
    stmt5.execute();

    sql6 = `
        select email_id
        FROM raw_support.auto_response_tt_app;
    `;
    stmt6 = snowflake.createStatement({sqlText: sql6});
    resultSet = stmt6.execute();
    
    if (resultSet.next()) {
        return "Processed Successfullly and Auto Responded"
    } else {
        return "Processed Successfullly and Routed to Agent"
    }

    return responderValue;
';

CREATE OR REPLACE PROCEDURE tasty_bytes_customer_support_email.raw_support.insert_response_app("EMAIL_ID" VARCHAR(16777216), "RESPONSE" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS '
try {
    var temp = RESPONSE.replace(/''''''''/g, "''''''''''''''''");
    
    // Construct JSON string
    var jsonStr = `{"Response": "${temp.replace(/"/g, ''\\\\"'')}"}`;
    
    // Create temporary table and insert email data
    var query1 = `
        CREATE OR REPLACE TEMPORARY TABLE raw_support.email_data_gen_tt_response_app
        (
            email_id VARCHAR(16777216),
            email_response VARIANT,
            sent_ts TIMESTAMP_NTZ(9)
        )
        AS
        SELECT
            :1 AS email_id,
            PARSE_JSON(:2) AS email_response,
            current_timestamp() as sent_ts
    `;
    var statement1 = snowflake.createStatement({sqlText: query1, binds: [EMAIL_ID, jsonStr]});
    var result1 = statement1.execute();
    if (!result1.next()) {
        throw new Error("Failed to create temporary table or insert data");
    }
    
    // Insert email status for new emails
    var query2 = `
        INSERT INTO raw_support.email_response_app
        (EMAIL_ID, EMAIL_RESPONSE, SENT_TS)
        SELECT
            email_id,
            email_response,
            sent_ts
        FROM raw_support.email_data_gen_tt_response_app
    `;
    var statement2 = snowflake.createStatement({sqlText: query2});
    var result2 = statement2.execute();
    if (!result2.next()) {
        throw new Error("Failed to insert email response");
    }

    // Update the EMAIL_STATUS table setting RESPONDED_FLAG to True and last_updated_ts to CURRENT_TIMESTAMP
    var update_status_command = "UPDATE raw_support.email_status_app SET RESPONDED_FLAG = TRUE, last_updated_ts = CURRENT_TIMESTAMP() WHERE EMAIL_ID = ?";
    var update_status_stmt = snowflake.createStatement({
        sqlText: update_status_command,
        binds: [EMAIL_ID]
    });
    var update_status_result = update_status_stmt.execute();
    if (update_status_result.getRowCount() == 0) {
        throw new Error("Email status update failed: no rows affected");
    }

    // Return success message after successful updates
    return "Email Sent Successfully";

} catch (err) {
    // Error handling
    return "Failed to insert: " + err.message;
}
';

-- load data from S3
COPY INTO tasty_bytes_customer_support_email.raw_support.email_status_app
FROM @tasty_bytes_customer_support_email.raw_support.s3load/raw_support/email_status_app/;

COPY INTO tasty_bytes_customer_support_email.raw_support.email_response_app
FROM @tasty_bytes_customer_support_email.raw_support.s3load/raw_support/email_response_app/;

COPY INTO tasty_bytes_customer_support_email.harmonized.documents
FROM @tasty_bytes_customer_support_email.raw_support.s3load/harmonized/documents/;

COPY INTO tasty_bytes_customer_support_email.harmonized.array_table
FROM @tasty_bytes_customer_support_email.raw_support.s3load/harmonized/vector_store/;

COPY INTO tasty_bytes_customer_support_email.harmonized.chunk_text
FROM @tasty_bytes_customer_support_email.raw_support.s3load/harmonized/chunk_text/;

CREATE OR REPLACE TABLE tasty_bytes_customer_support_email.harmonized.vector_store (
	SOURCE VARCHAR(6),
	SOURCE_DESC VARCHAR(16777216),
	FULL_TEXT VARCHAR(16777216),
	SIZE NUMBER(18,0),
	CHUNK VARCHAR(16777216),
	INPUT_TEXT VARCHAR(16777216),
	CHUNK_EMBEDDING VECTOR(FLOAT, 768)
) AS
SELECT 
  source,
	source_desc,
	full_text,
	size,
	chunk,
	input_text,
  chunk_embedding::VECTOR(FLOAT, 768)
FROM tasty_bytes_customer_support_email.harmonized.array_table;

