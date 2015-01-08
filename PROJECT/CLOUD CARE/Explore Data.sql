
EXECUTE dbms_output.put_line('');
EXECUTE dbms_output.put_line('Explore Data node started: ' || SYSTIMESTAMP);
EXECUTE dbms_output.put_line('');

DECLARE
  v_data_view_name          VARCHAR2(30);
  v_explore_sampled_data    VARCHAR2(30);
  v_histogram_sampled_data  VARCHAR2(30);
  v_sql             CLOB;
  v_user_session    VARCHAR2(30) := SYS_CONTEXT ('USERENV', 'SESSION_USER');
  v_drop            VARCHAR2(30) := '&DROP_EXISTING_OBJECTS';

  TYPE  
    LSTMT_REC_TYPE                IS RECORD (
      lstmt                          dbms_sql.VARCHAR2A,
      lb                             BINARY_INTEGER DEFAULT 1,
      ub                             BINARY_INTEGER DEFAULT 0);

  v_columns         ODMR_OBJECT_NAMES := ODMR_OBJECT_NAMES();
  v_attributes      ODMR_OBJECT_NAMES := ODMR_OBJECT_NAMES();
  v_aliases         ODMR_OBJECT_NAMES := ODMR_OBJECT_NAMES();
  v_attrDataTypes   ODMR_OBJECT_NAMES := ODMR_OBJECT_NAMES();

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

  PROCEDURE drop_view (view_name IN VARCHAR2) IS
    v_stmt            VARCHAR2(4000);
  BEGIN
    v_stmt := 'DROP VIEW '|| SYS.DBMS_ASSERT.ENQUOTE_NAME(view_name,FALSE);
    EXECUTE  IMMEDIATE v_stmt;
  EXCEPTION WHEN OTHERS THEN
   NULL;
  END;
  
  FUNCTION ls_clob(p_lstmt IN OUT NOCOPY LSTMT_REC_TYPE)
  RETURN CLOB
  IS
    v_clob   CLOB;
  BEGIN
    FOR i IN p_lstmt.lb..p_lstmt.ub LOOP
      v_clob := v_clob || p_lstmt.lstmt(i);
    END LOOP;
    RETURN v_clob;
  END;

  PROCEDURE ls_append( p_lstmt IN OUT NOCOPY LSTMT_REC_TYPE,
                       p_txt VARCHAR2) IS
  BEGIN
    p_lstmt.ub := p_lstmt.ub + 1;
    p_lstmt.lstmt(p_lstmt.ub) := p_txt;
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
  v_data_view_name := generateUniqueName;

  IF (v_drop = 'TRUE') THEN -- delete existing table? 
    BEGIN 
      v_sql := 'DROP TABLE &CREATE_TABLE_4 PURGE'; 
      execSQL(v_sql); 
    EXCEPTION WHEN OTHERS THEN 
      NULL; 
    END; 
    
  END IF; 
  
  v_sql := 'CREATE VIEW '||v_data_view_name||' AS SELECT  * FROM (
  WITH  INPUT_DATA_VIEW AS 
     (SELECT * FROM ' ||getInputSource('10038')||') 
SELECT * FROM INPUT_DATA_VIEW)';
  execSQL(v_sql);

  recordOutput('10054', 'Explore Data', 'DataProfileNode', NULL, NULL, NULL, v_data_view_name, 'VIEW', NULL, 'Input Data');
  v_explore_sampled_data := ODMR_UTIL.CREATE_SAMPLE_DATA (
     v_data_view_name,
     1,
     2000,
     0,
     null,
     1,
     12345,
     0,
     0,
     '',
     '',
     '');

  recordOutput('10054', 'Explore Data', 'DataProfileNode', NULL, NULL, NULL, v_explore_sampled_data, 'TABLE', NULL, 'Sampled Data');
  v_columns.EXTEND(1);
  v_columns(v_columns.COUNT) := 'CLAS_SVM_2_6_PRED';
  v_aliases.EXTEND(1);
  v_aliases(v_aliases.COUNT) := null;
  v_attrDataTypes.EXTEND(1);
  v_attrDataTypes(v_attrDataTypes.COUNT) := 'NUMBER';

  v_columns.EXTEND(1);
  v_columns(v_columns.COUNT) := 'CLAS_SVM_2_6_PROB';
  v_aliases.EXTEND(1);
  v_aliases(v_aliases.COUNT) := null;
  v_attrDataTypes.EXTEND(1);
  v_attrDataTypes(v_attrDataTypes.COUNT) := 'NUMBER';

  v_columns.EXTEND(1);
  v_columns(v_columns.COUNT) := 'CUST_ID';
  v_aliases.EXTEND(1);
  v_aliases(v_aliases.COUNT) := null;
  v_attrDataTypes.EXTEND(1);
  v_attrDataTypes(v_attrDataTypes.COUNT) := 'NUMBER';

  v_columns.EXTEND(1);
  v_columns(v_columns.COUNT) := 'EXECUTION_TIME';
  v_aliases.EXTEND(1);
  v_aliases(v_aliases.COUNT) := null;
  v_attrDataTypes.EXTEND(1);
  v_attrDataTypes(v_attrDataTypes.COUNT) := 'NUMBER';

  v_columns.EXTEND(1);
  v_columns(v_columns.COUNT) := 'RESOURCES_REQUIRED';
  v_aliases.EXTEND(1);
  v_aliases(v_aliases.COUNT) := null;
  v_attrDataTypes.EXTEND(1);
  v_attrDataTypes(v_attrDataTypes.COUNT) := 'NUMBER';

  v_columns.EXTEND(1);
  v_columns(v_columns.COUNT) := 'WAIT_TIME';
  v_aliases.EXTEND(1);
  v_aliases(v_aliases.COUNT) := null;
  v_attrDataTypes.EXTEND(1);
  v_attrDataTypes(v_attrDataTypes.COUNT) := 'NUMBER';

  v_columns.EXTEND(1);
  v_columns(v_columns.COUNT) := 'PRIORITY';
  v_aliases.EXTEND(1);
  v_aliases(v_aliases.COUNT) := null;
  v_attrDataTypes.EXTEND(1);
  v_attrDataTypes(v_attrDataTypes.COUNT) := 'NUMBER';

  FOR j IN 1..v_columns.COUNT LOOP
    v_attributes.EXTEND(1);
    IF ( v_aliases(j) IS NOT NULL ) THEN
      v_attributes(v_attributes.COUNT) := v_aliases(j);
    ELSE 
      v_attributes(v_attributes.COUNT) := v_columns(j);
    END IF;
  END LOOP;


  ODMR_UTIL.CREATE_EXPLORE_NODE_STATISTICS(
    p_input_view              => v_explore_sampled_data,
    p_statistics_table_name   => '&CREATE_TABLE_4',
    p_attributes              => v_attributes,
    p_attrDataTypes           => v_attrDataTypes,
    p_calc_percent_distinct   => true,
    p_calc_percent_null       => true,
    p_calc_max                => true,
    p_calc_min                => true,
    p_calc_avg                => true,
    p_calc_stddev             => true,
    p_calc_variance           => true, 
    p_calc_kurtosis           => false,
    p_calc_median             => true, 
    p_calc_skewness           => false,
    p_calc_mode               => true, 
    p_calc_mode_sampled       => true, 
    p_calc_mode_all           => false,
    p_parallel_query_hint     => '',
    p_parallel_insert_hint    => '',
    p_parallel_table_hint     => '');

  v_histogram_sampled_data := v_explore_sampled_data;


  ODMR_UTIL.CLIENT_CALCULATE_HISTOGRAMS (
     v_histogram_sampled_data,
      '&CREATE_TABLE_4',
     10,
     10,
     10,
     'PRIORITY',
     'NUMBER',
     v_attributes,
     v_attrDataTypes,
     true,
     '',
     '',
     '');


  recordOutput('10054', 'Explore Data', 'DataProfileNode', NULL, NULL, NULL, '&CREATE_TABLE_4', 'TABLE', NULL, 'Output Data');

EXCEPTION WHEN OTHERS THEN
  RAISE_APPLICATION_ERROR(-20999, formatErrorStack('Explore Data', SQLERRM, DBMS_UTILITY.FORMAT_ERROR_BACKTRACE()));
END;
/
EXECUTE dbms_output.put_line('');
EXECUTE dbms_output.put_line('Explore Data node completed: ' || SYSTIMESTAMP);
EXECUTE dbms_output.put_line('');
