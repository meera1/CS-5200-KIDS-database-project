
EXECUTE dbms_output.put_line('');
EXECUTE dbms_output.put_line('NEW_FACTS node started: ' || SYSTIMESTAMP);
EXECUTE dbms_output.put_line('');

DECLARE
  v_table_name    VARCHAR2(65);
  v_sql           CLOB;
  v_sql2          CLOB;
  v_user_session  VARCHAR2(30) := SYS_CONTEXT ('USERENV', 'SESSION_USER');
  v_drop          VARCHAR2(30) := '&DROP_EXISTING_OBJECTS';

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
  v_table_name := '&CREATE_TABLE_5';
  
  IF (v_drop = 'TRUE') THEN -- delete existing table? 
    BEGIN 
      v_sql := 'DROP TABLE '||v_table_name||' PURGE'; 
      execSQL(v_sql); 
    EXCEPTION WHEN OTHERS THEN 
      NULL;
    END; 

    BEGIN 
      v_sql := 'DROP VIEW '||v_table_name; 
      execSQL(v_sql); 
    EXCEPTION WHEN OTHERS THEN 
      NULL;
    END; 

  END IF; 

  
  v_sql := 'CREATE TABLE &CREATE_TABLE_5 (
    "CUST_ID"                       NUMBER)
    ';
  execSQL(v_sql);
  
  
  
  v_sql := 'INSERT  INTO &CREATE_TABLE_5
    SELECT /*+ inline  */ 
    "CUST_ID"
    FROM (SELECT * FROM ' ||getInputSource('10050')||')';
  execSQL(v_sql);
  
  recordOutput('10052', 'NEW_FACTS', 'CreateTableNode', NULL, NULL, NULL, v_table_name, 'TABLE', NULL, 'Output Data');
EXCEPTION WHEN OTHERS THEN
  RAISE_APPLICATION_ERROR(-20999, formatErrorStack('NEW_FACTS', SQLERRM, DBMS_UTILITY.FORMAT_ERROR_BACKTRACE()));
END;
/
EXECUTE dbms_output.put_line('');
EXECUTE dbms_output.put_line('NEW_FACTS node completed: ' || SYSTIMESTAMP);
EXECUTE dbms_output.put_line('');
