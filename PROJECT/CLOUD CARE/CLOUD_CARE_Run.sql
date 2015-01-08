
WHENEVER OSERROR EXIT;
WHENEVER SQLERROR EXIT SQL.SQLCODE;

EXECUTE dbms_output.put_line('');
EXECUTE dbms_output.put_line('Workflow CLOUD_CARE started: ' || SYSTIMESTAMP);
EXECUTE dbms_output.put_line('');

ALTER SESSION set "_optimizer_reuse_cost_annotations"=false;
ALTER SESSION set NLS_NUMERIC_CHARACTERS=".,";

-- SQL Plus Script Commands Section:
SET SERVEROUTPUT ON
SET VERIFY OFF

-- Target Database and Repository Version the Generated Scripts are Compatible with
DEFINE TARGET_DB_VERSION = '11.2.0.1.0'
DEFINE TARGET_REPOS_VERSION = '12.1.0.1.5'

-- Subsitution Variable Definition Section: Override default object names here
DEFINE WORKFLOW_OUTPUT = 'CLOUD_CARE'
-- Drop user named objects (e.g. model, output table)?  TRUE or FALSE
DEFINE DROP_EXISTING_OBJECTS = 'TRUE'
-- From Node: "CUSTOMER"
DEFINE DATA_SOURCE_1 = '"CLOUDKIDS"."CUSTOMER"'
-- From Node: "CUST_REQ_SCHEDULE_FACTORS"
DEFINE DATA_SOURCE_2 = '"CLOUDKIDS"."CUST_REQ_SCHEDULE_FACTORS"'
-- From Node: "PRIORITY_ASSESSMENT"
DEFINE MODEL_1 = '"CLOUDKIDS"."CLAS_SVM_2_6"'
-- From Node: "PRIORITY_HYPOTHESIS"
DEFINE CREATE_TABLE_3 = '"CLOUDKIDS"."OUTPUT_1_6"'
-- From Node: "Explore Data"
DEFINE CREATE_TABLE_4 = '"CLOUDKIDS"."OUTPUT_4_6"'
-- From Node: "NEW_FACTS"
DEFINE CREATE_TABLE_5 = '"CLOUDKIDS"."OUTPUT_3_6"'


DECLARE
  TYPE NUM_NTAB_TYPE IS TABLE OF NUMBER;
  v_db_ver          VARCHAR2(30);
  v_compatibility   VARCHAR2(30);
  v_repos_ver       VARCHAR2(30);
  table_cnt         NUMBER;
  v_timestamp       TIMESTAMP := SYSTIMESTAMP;

  FUNCTION parseVersion(p_version in varchar2) return NUM_NTAB_TYPE is
    /*  
    The significance of the version numbers are (lets use 11.2.0.2.0 as an example):
      11 is the version number
      2 is the new features release number
      0 is the maintenance release number
      2 is the generic patch set number
      0 is the platform-specific patch set number
    */
    v_versions  NUM_NTAB_TYPE := NUM_NTAB_TYPE();
    v_start_pos integer := 1;
    v_end_pos   integer;
  BEGIN
    FOR i IN 1..5 LOOP -- assume version contains at most five numbers
      v_versions.extend;
      IF (v_start_pos > 0) THEN
        v_end_pos := INSTR(p_version, '.', v_start_pos);
        IF (v_end_pos > 0) THEN
          v_versions(i) := (SUBSTR(p_version, v_start_pos, v_end_pos-v_start_pos));
          v_start_pos := v_end_pos+1;
        ELSE
          v_versions(i) := SUBSTR(p_version, v_start_pos, (Length(p_version)-v_start_pos)+1);
          v_start_pos := 0; -- end of search
        END IF;
      ELSE
        v_versions(i) := 0; -- assume 0 value
      END IF;
    END LOOP;
    RETURN v_versions;
  END parseVersion;

  FUNCTION isCompatible(p_target_version IN VARCHAR2, p_cur_version IN VARCHAR2) return BOOLEAN is
    /*  
    The significance of the version numbers are (lets use 11.2.0.2.0 as an example):
      11 is the version number
      2 is the new features release number
      0 is the maintenance release number
      2 is the generic patch set number
      0 is the platform-specific patch set number
    */
    v_target_ver NUM_NTAB_TYPE := NUM_NTAB_TYPE();
    v_cur_ver NUM_NTAB_TYPE := NUM_NTAB_TYPE();
  BEGIN
    v_target_ver := parseVersion(p_target_version);
    v_cur_ver := parseVersion(p_cur_version);
    -- compare version number
    FOR i IN 1..5 LOOP -- assume version contains at most five numbers
      IF (v_cur_ver(i) != v_target_ver(i)) THEN
        RETURN (v_cur_ver(i) - v_target_ver(i) >= 0);
      END IF;
    END LOOP;    
    RETURN TRUE; -- exact same version, so it's compatible
  EXCEPTION WHEN OTHERS THEN
    RETURN FALSE; -- assume incompatible if any error
  END isCompatible;

BEGIN
  -- db and repository version validation
  DBMS_UTILITY.DB_VERSION(v_db_ver, v_compatibility);
  IF (isCompatible('&TARGET_DB_VERSION', v_db_ver) = FALSE) THEN
    RAISE_APPLICATION_ERROR(-20999, 'Database validation failed: generated scripts can only run against database version &TARGET_DB_VERSION or later.');
  END IF;
  SELECT PROPERTY_STR_VALUE INTO v_repos_ver FROM ODMR_REPOSITORY_PROPERTIES WHERE PROPERTY_NAME = 'VERSION';
  IF (isCompatible('&TARGET_REPOS_VERSION', v_repos_ver) = FALSE) THEN
    RAISE_APPLICATION_ERROR(-20999, 'Dataminer repository validation failed: generated scripts can only run against dataminer repository version &TARGET_REPOS_VERSION or later.');
  END IF;
  -- create the workflow table
  SELECT count(*) INTO table_cnt FROM user_tables WHERE table_name='&WORKFLOW_OUTPUT';
  IF (table_cnt = 0) THEN
    EXECUTE IMMEDIATE
      'CREATE TABLE "&WORKFLOW_OUTPUT"
      (
        NODE_ID VARCHAR2(30) NOT NULL,
        NODE_NAME VARCHAR2(30) NOT NULL,
        NODE_TYPE VARCHAR2(30) NOT NULL,
        MODEL_ID VARCHAR2(30),
        MODEL_NAME VARCHAR2(65),
        MODEL_TYPE VARCHAR2(35),
        OUTPUT_NAME VARCHAR2(65) NOT NULL,
        OUTPUT_TYPE VARCHAR2(30) NOT NULL, -- TABLE, VIEW, POLICY, LEXER, STOPLIST, MODEL
        ADDITIONAL_INFO VARCHAR2(65),
        CREATION_TIME TIMESTAMP(6) NOT NULL,
        COMMENTS VARCHAR2(4000 CHAR)
      )';
  ELSE
    RAISE_APPLICATION_ERROR(-20999, 'Workflow output table exists, please run cleanup first');
  END IF;
END;
/

-- Workflow run
@@"CUSTOMER.sql";

@@"CUST_REQ_SCHEDULE_FACTORS.sql";

@@"CLOUD_PERCEPTION.sql";

@@"PRIORITY_ASSESSMENT.sql";

@@"CLOUD_APPLICATION.sql";

@@"PRIORITY_HYPOTHESIS.sql";

@@"Explore Data.sql";

@@"DIRECTIVE.sql";

@@"NEW_FACTS.sql";



EXECUTE dbms_output.put_line('');
EXECUTE dbms_output.put_line('Workflow CLOUD_CARE completed: ' || SYSTIMESTAMP);
EXECUTE dbms_output.put_line('Please refer to table "&WORKFLOW_OUTPUT" for all generated output');
EXECUTE dbms_output.put_line('');
