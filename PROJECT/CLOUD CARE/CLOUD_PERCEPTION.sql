
EXECUTE dbms_output.put_line('');
EXECUTE dbms_output.put_line('CLOUD_PERCEPTION node started: ' || SYSTIMESTAMP);
EXECUTE dbms_output.put_line('');

DECLARE
  v_view_name     VARCHAR2(30);
  v_sql           CLOB;
  v_sql2          CLOB;
  v_user_session  VARCHAR2(30) := SYS_CONTEXT ('USERENV', 'SESSION_USER');

  FUNCTION generateUniqueName RETURN VARCHAR2 IS
    v_uniqueName  VARCHAR2(30);
  BEGIN
    SELECT 'ODMR$'||TO_CHAR(SYSTIMESTAMP,'HH24_MI_SS_FF')||dbms_random.string(NULL, 7) INTO v_uniqueName FROM dual;
    RETURN v_uniqueName;
  END;
    
  FUNCTION getInputSource(p_nodeId VARCHAR2) RETURN VARCHAR2 IS
    v_output  VARCHAR2(30);
  BEGIN
    SELECT OUTPUT_NAME INTO v_output FROM "&WORKFLOW_OUTPUT" WHERE NODE_ID = p_nodeId AND COMMENTS = 'Output Data';
    RETURN v_output;
  END;

  PROCEDURE recordOutput(p_NODE_ID VARCHAR2, p_NODE_NAME VARCHAR2, p_NODE_TYPE VARCHAR2, 
                         p_MODEL_ID VARCHAR2, p_MODEL_NAME VARCHAR2, p_MODEL_TYPE VARCHAR2, 
                         p_OUTPUT_NAME VARCHAR2, p_OUTPUT_TYPE VARCHAR2, p_ADDITIONAL_INFO VARCHAR2, p_COMMENTS VARCHAR2) IS
  BEGIN
    INSERT INTO "&WORKFLOW_OUTPUT" VALUES (p_NODE_ID, p_NODE_NAME, p_NODE_TYPE, p_MODEL_ID, REPLACE(REPLACE(p_MODEL_NAME,'"',''), (v_user_session||'.'), ''), p_MODEL_TYPE, p_OUTPUT_NAME, p_OUTPUT_TYPE, p_ADDITIONAL_INFO, SYSTIMESTAMP, p_COMMENTS);
    COMMIT;
  END;

  PROCEDURE execSQL(p_sql CLOB) IS
    curid         INTEGER;
    ignoreid      INTEGER;    
  BEGIN
    curid := DBMS_SQL.OPEN_CURSOR;
    DBMS_SQL.PARSE(curid, p_sql, DBMS_SQL.NATIVE);
    ignoreid := DBMS_SQL.EXECUTE(curid);
    DBMS_SQL.CLOSE_CURSOR(curid);
  EXCEPTION WHEN OTHERS THEN
    IF DBMS_SQL.IS_OPEN(curid) THEN
      DBMS_SQL.CLOSE_CURSOR(curid);
    END IF;
    RAISE;
  END;

  FUNCTION formatErrorStack(
    p_node_name IN VARCHAR2,
    p_sqlerr        IN VARCHAR2,
    p_error_stack   IN VARCHAR2 ) RETURN VARCHAR2
  IS
  BEGIN
    RETURN SUBSTR('Error in ' || p_node_name || ': ' || CHR(13) || CHR(10) || p_sqlerr || 
                   CHR(13) || CHR(10) || p_error_stack, 1, 4000);
  END;

BEGIN
  v_view_name := generateUniqueName;
  
  v_sql := 
    'CREATE VIEW '||v_view_name||' AS SELECT  * FROM
    (
      WITH /* Start of sql for node: CLOUD_PERCEPTION */
"N$10009" as (SELECT /*+ inline */
"CUSTOMER"."PRIORITY"
, "CUSTOMER"."ID"
, "CUST_REQ_SCHEDULE_FACTORS"."EXECUTION_TIME"
, "CUST_REQ_SCHEDULE_FACTORS"."CUST_ID"
, "CUST_REQ_SCHEDULE_FACTORS"."ID" "ID1"
, "CUST_REQ_SCHEDULE_FACTORS"."RESOURCES_REQUIRED"
, "CUST_REQ_SCHEDULE_FACTORS"."WAIT_TIME"
FROM
'||getInputSource('10007')||' "CUSTOMER"
INNER JOIN '||getInputSource('10008')||' "CUST_REQ_SCHEDULE_FACTORS"
ON "CUSTOMER"."ID" = "CUST_REQ_SCHEDULE_FACTORS"."CUST_ID"
 )
/* End of sql for node: CLOUD_PERCEPTION */
SELECT * FROM "N$10009"
    )';
  execSQL(v_sql);
  
  recordOutput('10009', 'CLOUD_PERCEPTION', 'JoinNode', NULL, NULL, NULL, v_view_name, 'VIEW', NULL, 'Output Data');
EXCEPTION WHEN OTHERS THEN
  RAISE_APPLICATION_ERROR(-20999, formatErrorStack('CLOUD_PERCEPTION', SQLERRM, DBMS_UTILITY.FORMAT_ERROR_BACKTRACE()));
END;
/
EXECUTE dbms_output.put_line('');
EXECUTE dbms_output.put_line('CLOUD_PERCEPTION node completed: ' || SYSTIMESTAMP);
EXECUTE dbms_output.put_line('');
