-- Function: connectby(text, text, text, text, text)
CREATE OR REPLACE FUNCTION connectby(tb_nm text, p_key text, c_key text, insql text, ward text)
  RETURNS text AS
$BODY$
DECLARE
  v_sql text;
  v_table_name text;
  v_condition text;
  v_sessid text;
  node1 RECORD;
  result int;
  v_lev int;
  v_istable int;
  root_key text;
  connect_key text;
BEGIN
 SET enable_nestloop='on';
 SET random_page_cost=5;
 --SELECT 'temp_' || lower(ward) || '_' || replace(tb_nm,'.','_') || '_' || pg_backend_pid() INTO v_table_name;
 --SELECT count(*) INTO v_istable FROM pg_class WHERE relname = v_table_name ;
 v_table_name := 'temp_' || lower(ward) || '_' || replace(tb_nm,'.','_');
 SELECT count(*) FROM pg_tables INTO v_istable WHERE schemaname IN ( SELECT 'pg_temp_'||sess_id FROM pg_stat_activity WHERE procpid = pg_backend_pid() ) AND tablename = v_table_name;
        IF upper(ward) = 'UPWARD' THEN
  root_key := c_key;
  connect_key := p_key;
        ELSE
  root_key := p_key;
  connect_key := c_key;
 END IF;
 IF insql is null THEN
  v_condition := c_key || ' IS NULL';
 ELSE
  v_condition := c_key || ' IN (' || insql || ')';
 END IF;
 
 IF v_istable = 0 THEN
  execute 'create temp table ' || v_table_name || ' as ' ||
  ' select ' || c_key || ' as root_id, 1::integer as lev, ' || c_key || '::text as trace, c.* from '|| tb_nm || ' c ' ||
  ' WHERE  ' || v_condition ||
  ' DISTRIBUTED BY (' || connect_key || ')';
 ELSE
  execute 'truncate ' || v_table_name;
  execute 'insert into ' || v_table_name ||
  ' select ' || c_key || ' as root_id, 1 as lev, c.' || c_key || ' as trace, c.* from '|| tb_nm || ' c ' ||
  ' WHERE ' || v_condition;
 END IF;
 result := 1;
 v_lev := 1;
 WHILE result >= 1 LOOP
  select schyms.connectby_engine (v_lev, tb_nm,  v_table_name, root_key, connect_key) into result;
  v_lev := v_lev + 1;
 END LOOP;

 RESET enable_nestloop;
 RESET random_page_cost;
 RETURN v_table_name;
/*
 FOR node1 IN  execute 'SELECT * FROM ' || v_table_name || ' order by trace'
 LOOP
  RETURN NEXT node1;
 END LOOP;
*/
END
$BODY$
  LANGUAGE plpgsql VOLATILE;

-- Function: connectby_engine(integer, text, text, text, text)
CREATE OR REPLACE FUNCTION connectby_engine(priolev integer, tablename text, v_table_name text, root_key text, connect_key text)
  RETURNS integer AS
$BODY$
DECLARE
  v_sql text;
  n int;
BEGIN
 v_sql := 'INSERT INTO ' || v_table_name ||
 ' SELECT b.root_id, '|| (priolev + 1) || ' as lev, b.trace || ''/'' || c.' || connect_key || ',  c.* ' ||
 ' FROM ' || tablename || ' c, ' || v_table_name ||' b' ||
 ' WHERE c.' || root_key || '=b.' || connect_key || ' AND b.lev=' || priolev || ' AND b.' || root_key || ' <> c.' || connect_key;
 RAISE NOTICE 'v_sql here is %', v_sql;
        EXECUTE v_sql ;
 GET DIAGNOSTICS n = ROW_COUNT ;
 RETURN n;
END
$BODY$
  LANGUAGE plpgsql VOLATILE;
