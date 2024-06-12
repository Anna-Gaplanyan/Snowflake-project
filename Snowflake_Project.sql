use warehouse WAREHOUSE;

use DATABASE DATABASE;



CREATE SCHEMA bronze;
USE SCHEMA bronze;

select current_schema();



CREATE STAGE external_bucket
  URL='s3:URL'
  CREDENTIALS = (AWS_KEY_ID = 'AWS-Key_ID' AWS_SECRET_KEY = AWS_Secret_Key');



CREATE TABLE stg_molecules(json_data VARIANT, filename VARCHAR, amnd_user VARCHAR DEFAULT CURRENT_USER(), amnd_date DATETIME DEFAULT CURRENT_TIMESTAMP);
CREATE TABLE stg_batches(json_data VARIANT, filename VARCHAR, amnd_user VARCHAR DEFAULT CURRENT_USER(), amnd_date DATETIME DEFAULT CURRENT_TIMESTAMP);
CREATE TABLE stg_projects(id NUMBER, project_name VARCHAR, filename VARCHAR, amnd_user VARCHAR DEFAULT CURRENT_USER(), amnd_date DATETIME DEFAULT CURRENT_TIMESTAMP);

CREATE STREAM stream_stg_molecules
ON TABLE stg_molecules
APPEND_ONLY = FALSE;


CREATE STREAM stream_stg_molecules_projects
ON TABLE stg_molecules
append_only=FALSE;

CREATE STREAM stream_stg_molecules_batches
ON TABLE stg_molecules
append_only=FALSE;


CREATE STREAM stream_stg_batches
ON TABLE stg_batches
APPEND_ONLY = FALSE;

CREATE STREAM stream_stg_projects
ON TABLE stg_projects
APPEND_ONLY = FALSE;


CREATE OR REPLACE FILE FORMAT JSON_DATA
TYPE = 'JSON'
STRIP_OUTER_ARRAY = TRUE;

CREATE OR REPLACE FILE FORMAT CSV_DATA
  TYPE = 'CSV'
  FIELD_DELIMITER = '^'
  SKIP_HEADER = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = NONE;



CREATE OR REPLACE PIPE stg_molecules_pipe AUTO_INGEST = TRUE AS
COPY INTO stg_molecules(json_data, filename)
FROM (SELECT $1, METADATA$FILENAME FROM @external_bucket/data/molecules/)
PATTERN =  'molecules.*\.json$'
FILE_FORMAT = (FORMAT_NAME = JSON_DATA)
ON_ERROR = 'CONTINUE';


ALTER PIPE stg_molecules_pipe REFRESH;
select * from stg_molecules;
select * from stream_stg_molecules_batches;

CREATE OR REPLACE PIPE stg_batches_pipe AUTO_INGEST = TRUE AS
COPY INTO stg_batches(json_data, filename)
FROM (SELECT $1, METADATA$FILENAME FROM  @external_bucket/data/batches/)
PATTERN =  'batches.*\.json$'
FILE_FORMAT = (FORMAT_NAME = JSON_DATA)
ON_ERROR = 'CONTINUE';


ALTER PIPE stg_batches_pipe REFRESH;

select * from stg_batches;

select * from stream_stg_batches;



CREATE OR REPLACE PIPE stg_projects_pipe AUTO_INGEST = TRUE AS
COPY INTO stg_projects(id, project_name, filename)
FROM (
    SELECT
        $1::NUMBER AS id,
        $2::VARCHAR AS project_name,
        METADATA$FILENAME
    FROM  @EXTERNAL_BUCKET/data/projects/
)
PATTERN = 'projects.*\.csv$'
FILE_FORMAT = (FORMAT_NAME = 'CSV_DATA')
ON_ERROR = 'CONTINUE';

show pipes;

ALTER PIPE stg_projects_pipe REFRESH;

select * from stg_projects;

select * from stream_stg_projects;



CREATE OR REPLACE FUNCTION add_comment(input_string VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
AS $$
    input_string || ' added by Anna_Gaplanyan'
$$;



CREATE SCHEMA silver;

USE SCHEMA silver;

select current_schema();

CREATE TABLE molecules (
    seq_id INT AUTOINCREMENT START = 1 INCREMENT = 1,
    molecule_id NUMBER,
    class VARCHAR,
    created_at DATETIME,
    modified_at DATETIME,
    name VARCHAR,
    owner VARCHAR,
    molecular_weight NUMBER,
    formula VARCHAR,
    filename VARCHAR,
    amnd_user VARCHAR DEFAULT CURRENT_USER(),
    amnd_date DATETIME DEFAULT CURRENT_TIMESTAMP
);


CREATE TABLE molecules_projects (
    seq_id INT AUTOINCREMENT START = 1 INCREMENT = 1,
    molecule_id NUMBER,
    project_id NUMBER,
    filename VARCHAR,
    amnd_user VARCHAR DEFAULT CURRENT_USER(),
    amnd_date DATETIME DEFAULT CURRENT_TIMESTAMP
);


CREATE TABLE molecules_batches (
    seq_id INT AUTOINCREMENT START = 1 INCREMENT = 1,
    molecule_id NUMBER,
    batch_id NUMBER,
    filename VARCHAR,
    amnd_user VARCHAR DEFAULT CURRENT_USER(),
    amnd_date DATETIME DEFAULT CURRENT_TIMESTAMP
);


CREATE TABLE batches (
    seq_id INT AUTOINCREMENT START = 1 INCREMENT = 1,
    id NUMBER,
    class VARCHAR,
    created_at DATETIME,
    modified_at DATETIME,
    name VARCHAR,
    owner VARCHAR,
    formula_weight NUMBER,
    filename VARCHAR,
    amnd_user VARCHAR DEFAULT CURRENT_USER(),
    amnd_date DATETIME DEFAULT CURRENT_TIMESTAMP
);



CREATE TABLE projects (
    seq_id INT AUTOINCREMENT START = 1 INCREMENT = 1,
    id NUMBER,
    project_name VARCHAR,
    comment VARCHAR,
    filename VARCHAR,
    amnd_user VARCHAR DEFAULT CURRENT_USER(),
    amnd_date DATETIME DEFAULT CURRENT_TIMESTAMP
);



CREATE OR REPLACE PROCEDURE bronze.load_molecules()
RETURNS VARCHAR
LANGUAGE SQL
AS $$
BEGIN
    INSERT INTO silver.molecules (
        molecule_id, class, created_at, modified_at, name, owner, molecular_weight, formula,
        filename, amnd_user, amnd_date
    )
    SELECT DISTINCT
        json_data:id::NUMBER,
        json_data:class::VARCHAR,
        json_data:created_at::TIMESTAMP_LTZ,
        json_data:modified_at::TIMESTAMP_LTZ,
        json_data:name::VARCHAR,
        json_data:owner::VARCHAR,
        json_data:molecular_weight::FLOAT,
        json_data:formula::VARCHAR,
        filename,
        CURRENT_USER(),
        CURRENT_TIMESTAMP
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY json_data:id ORDER BY json_data:modified_at DESC) AS rn
        FROM bronze.stream_stg_molecules
    )
    WHERE rn = 1;
    RETURN 'Molecules data loaded into molecules table';
END;
$$;

call bronze.load_molecules();

select * from silver.molecules;

CREATE OR REPLACE PROCEDURE bronze.load_molecules_projects()
RETURNS VARCHAR
LANGUAGE SQL
AS $$
BEGIN
    INSERT INTO silver.molecules_projects (
        molecule_id, project_id, filename, amnd_user, amnd_date
    )
    SELECT DISTINCT
        json_data:id::NUMBER AS molecule_id,
        p.value:id::NUMBER AS project_id,
        filename,
        CURRENT_USER(),
        CURRENT_TIMESTAMP
    FROM
        bronze.stream_stg_molecules_projects,
        LATERAL FLATTEN(input => json_data:projects) AS p
    QUALIFY
        ROW_NUMBER() OVER (PARTITION BY json_data:id::NUMBER, p.value:id::NUMBER ORDER BY json_data:modified_at DESC) = 1;
    RETURN 'Molecules projects data loaded into molecules_projects table';
END;
$$;


call bronze.load_molecules_projects();

select * from silver.molecules_projects;




CREATE OR REPLACE PROCEDURE bronze.load_molecules_batches()
RETURNS VARCHAR
LANGUAGE SQL
AS $$
BEGIN
    INSERT INTO silver.molecules_batches (
        molecule_id, batch_id, filename, amnd_user, amnd_date
    )
    SELECT DISTINCT
        json_data:id::NUMBER,
        b.value:id::NUMBER AS batch_id,
        filename,
        CURRENT_USER(),
        CURRENT_TIMESTAMP
        FROM bronze.stream_stg_molecules_batches,
        LATERAL FLATTEN(input => json_data:batches) b
    QUALIFY ROW_NUMBER() OVER (PARTITION BY json_data:id, b.value:id ORDER BY json_data:modified_at DESC) = 1;
    RETURN 'Molecules batches data loaded into molecules_batches table';
END;
$$;


call bronze.load_molecules_batches();

select * from silver.molecules_batches;


CREATE OR REPLACE PROCEDURE bronze.load_projects()
RETURNS VARCHAR
LANGUAGE SQL
AS $$
BEGIN
    INSERT INTO silver.projects (
         id, project_name, comment, filename, amnd_user, amnd_date
    )
    SELECT
        id, project_name, bronze.add_comment(project_name),
        filename, CURRENT_USER(), CURRENT_TIMESTAMP
    FROM (
        SELECT DISTINCT *
        FROM bronze.stream_stg_projects
    );
    RETURN 'Projects data loaded into Silver schema';
END;
$$;


call bronze.load_projects();

select * from silver.projects;


CREATE OR REPLACE PROCEDURE bronze.load_batches()
RETURNS VARCHAR
LANGUAGE SQL
AS $$
BEGIN
    INSERT INTO silver.batches (
        id, class, created_at, modified_at, name,owner, formula_weight,filename, amnd_user,amnd_date)
    SELECT DISTINCT
        json_data:id::NUMBER,
        json_data:class::VARCHAR,
        json_data:created_at::TIMESTAMP_LTZ,
        json_data:modified_at::TIMESTAMP_LTZ,
        json_data:name::VARCHAR,
        json_data:owner::VARCHAR,
        json_data:formula_weight::FLOAT,
        filename,
        CURRENT_USER(),
        CURRENT_TIMESTAMP
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY json_data:id ORDER BY json_data:modified_at DESC) AS rn
        FROM bronze.stream_stg_batches
    )
    WHERE rn = 1;
    RETURN 'Molecules batches data loaded into molecules_batches table';
END;
$$;


call bronze.load_batches();

select * from silver.batches;


CREATE OR REPLACE TASK bronze.task_move_batches
WAREHOUSE = student_warehouse
SCHEDULE = 'USING CRON 0 0 * * * UTC'  
WHEN SYSTEM$STREAM_HAS_DATA('bronze.stream_stg_batches')
AS
    CALL bronze.load_batches();


CREATE OR REPLACE TASK bronze.task_move_projects
WAREHOUSE = student_warehouse
SCHEDULE = 'USING CRON 0 0 * * * UTC'  
WHEN SYSTEM$STREAM_HAS_DATA('bronze.stream_stg_projects')
AS
    CALL bronze.load_projects();


CREATE OR REPLACE TASK bronze.task_move_molecules
WAREHOUSE = student_warehouse
SCHEDULE = 'USING CRON 0 0 * * * UTC'  
WHEN SYSTEM$STREAM_HAS_DATA('bronze.stream_stg_molecules')
AS
    CALL bronze.load_molecules();



CREATE SCHEMA gold;
USE SCHEMA gold;

select current_schema();

CREATE OR REPLACE VIEW gold.v_molecules AS
    SELECT
        m.MOLECULE_ID AS molecule_id,
        m.CLASS AS molecule_class,
        m.OWNER AS molecule_owner,
        m.MOLECULAR_WEIGHT AS molecule_molecular_weight,
        m.FORMULA AS molecule_formula,
        p.COMMENT AS project_comment,
        b.NAME AS batch_name
    FROM silver.molecules m
    JOIN silver.molecules_projects mp ON m.MOLECULE_ID = mp.MOLECULE_ID
    JOIN silver.projects p ON mp.PROJECT_ID = p.ID
    JOIN silver.molecules_batches mb ON m.MOLECULE_ID = mb.MOLECULE_ID
    JOIN silver.batches b ON mb.BATCH_ID = b.ID;
