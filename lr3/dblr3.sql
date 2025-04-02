CREATE OR REPLACE PROCEDURE compare_schemas_tables (
    dev_schema_name IN VARCHAR2,
    prod_schema_name IN VARCHAR2
)
AUTHID CURRENT_USER
AS
    CURSOR dev_tables IS
        SELECT table_name
        FROM all_tables
        WHERE owner = UPPER(dev_schema_name);

    CURSOR prod_tables IS
        SELECT table_name
        FROM all_tables
        WHERE owner = UPPER(prod_schema_name);

    TYPE table_diff_rec IS RECORD (
        table_name VARCHAR2(128),
        issue_type VARCHAR2(50)
    );
    TYPE table_diff_tab IS TABLE OF table_diff_rec;
    v_diff_tables table_diff_tab := table_diff_tab();

    TYPE dependency_rec IS RECORD (
        table_name VARCHAR2(128),
        referenced_table VARCHAR2(128)
    );
    TYPE dependency_tab IS TABLE OF dependency_rec;
    v_dependencies dependency_tab := dependency_tab();

    TYPE loop_tables_tab IS TABLE OF VARCHAR2(128);
    v_loop_tables loop_tables_tab := loop_tables_tab();

    FUNCTION is_table_in_list (
        p_table_name IN VARCHAR2,
        p_table_list IN loop_tables_tab
    ) RETURN BOOLEAN IS
    BEGIN
        FOR i IN 1..p_table_list.COUNT LOOP
            IF p_table_list(i) = p_table_name THEN
                RETURN TRUE;
            END IF;
        END LOOP;
        RETURN FALSE;
    END is_table_in_list;

BEGIN
    FOR dev_rec IN dev_tables LOOP
        DECLARE
            v_prod_exists NUMBER;
            v_col_match NUMBER;
        BEGIN
            SELECT COUNT(*)
            INTO v_prod_exists
            FROM all_tables
            WHERE owner = UPPER(prod_schema_name)
            AND table_name = dev_rec.table_name;

            IF v_prod_exists = 0 THEN
                v_diff_tables.EXTEND;
                v_diff_tables(v_diff_tables.LAST).table_name := dev_rec.table_name;
                v_diff_tables(v_diff_tables.LAST).issue_type := 'DOESNT_EXIST_IN_PROD';
            ELSE
                SELECT COUNT(*)
                INTO v_col_match
                FROM (
                    SELECT column_name, data_type, data_length
                    FROM all_tab_columns
                    WHERE owner = UPPER(dev_schema_name) AND table_name = dev_rec.table_name
                    MINUS
                    SELECT column_name, data_type, data_length
                    FROM all_tab_columns
                    WHERE owner = UPPER(prod_schema_name) AND table_name = dev_rec.table_name
                );

                IF v_col_match > 0 THEN
                    v_diff_tables.EXTEND;
                    v_diff_tables(v_diff_tables.LAST).table_name := dev_rec.table_name;
                    v_diff_tables(v_diff_tables.LAST).issue_type := 'DIFFERENT_STRUCTURE';
                END IF;
            END IF;
        END;
    END LOOP;

    FOR fk_rec IN (
        SELECT ac.table_name, 
               ac2.table_name AS referenced_table
        FROM all_constraints ac
        JOIN all_constraints ac2
          ON ac.r_constraint_name = ac2.constraint_name
          AND ac2.owner = UPPER(dev_schema_name)
        WHERE ac.owner = UPPER(dev_schema_name)
          AND ac.constraint_type = 'R'
    ) LOOP
        v_dependencies.EXTEND;
        v_dependencies(v_dependencies.LAST).table_name := fk_rec.table_name;
        v_dependencies(v_dependencies.LAST).referenced_table := fk_rec.referenced_table;
    END LOOP;

    FOR i IN 1..v_dependencies.COUNT LOOP
        FOR j IN 1..v_dependencies.COUNT LOOP
            IF i != j 
               AND v_dependencies(i).table_name = v_dependencies(j).referenced_table 
               AND v_dependencies(j).table_name = v_dependencies(i).referenced_table THEN

                IF NOT is_table_in_list(v_dependencies(i).table_name, v_loop_tables) THEN
                    v_loop_tables.EXTEND;
                    v_loop_tables(v_loop_tables.LAST) := v_dependencies(i).table_name;
                END IF;
                IF NOT is_table_in_list(v_dependencies(j).table_name, v_loop_tables) THEN
                    v_loop_tables.EXTEND;
                    v_loop_tables(v_loop_tables.LAST) := v_dependencies(j).table_name;
                END IF;
            END IF;
        END LOOP;
    END LOOP;

    IF v_loop_tables.COUNT > 0 THEN
        DBMS_OUTPUT.PUT_LINE('Error: Looped foreign key dependencies detected in the following tables:');
        FOR i IN 1..v_loop_tables.COUNT LOOP
            DBMS_OUTPUT.PUT_LINE('- ' || v_loop_tables(i));
        END LOOP;
    END IF;

    DBMS_OUTPUT.PUT_LINE('Tables to update in ' || prod_schema_name || ':');
    FOR i IN 1..v_diff_tables.COUNT LOOP
        DBMS_OUTPUT.PUT_LINE(v_diff_tables(i).table_name || ' - ' || v_diff_tables(i).issue_type);
    END LOOP;
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Error: ' || SQLERRM);
END compare_schemas_tables;
/



CREATE OR REPLACE PROCEDURE compare_schemas_objects (
    dev_schema_name IN VARCHAR2,
    prod_schema_name IN VARCHAR2
) 
AUTHID CURRENT_USER
AS
    TYPE obj_diff_rec IS RECORD (
        object_name VARCHAR2(128),
        object_type VARCHAR2(30),
        issue_type VARCHAR2(50)
    );
    TYPE obj_diff_tab IS TABLE OF obj_diff_rec;
    v_diff_objects obj_diff_tab := obj_diff_tab();

    v_sql VARCHAR2(4000);
BEGIN
    FOR obj_type IN (
        SELECT 'TABLE' AS object_type FROM dual UNION ALL
        SELECT 'PROCEDURE' FROM dual UNION ALL
        SELECT 'FUNCTION' FROM dual UNION ALL
        SELECT 'PACKAGE' FROM dual UNION ALL
        SELECT 'INDEX' FROM dual
    ) LOOP
        FOR dev_rec IN (
            SELECT object_name
            FROM all_objects
            WHERE owner = UPPER(dev_schema_name)
            AND object_type = obj_type.object_type
            AND object_name NOT LIKE 'SYS_%'
        ) LOOP
            DECLARE
                v_prod_exists NUMBER;
                v_source_match NUMBER;
            BEGIN
                SELECT COUNT(*)
                INTO v_prod_exists
                FROM all_objects
                WHERE owner = UPPER(prod_schema_name)
                AND object_type = obj_type.object_type
                AND object_name = dev_rec.object_name;

                IF v_prod_exists = 0 THEN
                    v_diff_objects.EXTEND;
                    v_diff_objects(v_diff_objects.LAST).object_name := dev_rec.object_name;
                    v_diff_objects(v_diff_objects.LAST).object_type := obj_type.object_type;
                    v_diff_objects(v_diff_objects.LAST).issue_type := 'DOESNT_EXIST_IN_PROD';
                ELSIF obj_type.object_type != 'TABLE' THEN
                    SELECT COUNT(*)
                    INTO v_source_match
                    FROM (
                        SELECT text
                        FROM all_source
                        WHERE owner = UPPER(dev_schema_name)
                        AND name = dev_rec.object_name
                        AND type = obj_type.object_type
                        MINUS
                        SELECT text
                        FROM all_source
                        WHERE owner = UPPER(prod_schema_name)
                        AND name = dev_rec.object_name
                        AND type = obj_type.object_type
                    );
                    IF v_source_match > 0 THEN
                        v_diff_objects.EXTEND;
                        v_diff_objects(v_diff_objects.LAST).object_name := dev_rec.object_name;
                        v_diff_objects(v_diff_objects.LAST).object_type := obj_type.object_type;
                        v_diff_objects(v_diff_objects.LAST).issue_type := 'DIFFERENT_CODE';
                    END IF;
                END IF;

                IF obj_type.object_type = 'TABLE' THEN
                    SELECT COUNT(*)
                    INTO v_source_match
                    FROM (
                        SELECT column_name, data_type, data_length
                        FROM all_tab_columns
                        WHERE owner = UPPER(dev_schema_name) AND table_name = dev_rec.object_name
                        MINUS
                        SELECT column_name, data_type, data_length
                        FROM all_tab_columns
                        WHERE owner = UPPER(prod_schema_name) AND table_name = dev_rec.object_name
                    );
                    IF v_source_match > 0 THEN
                        v_diff_objects.EXTEND;
                        v_diff_objects(v_diff_objects.LAST).object_name := dev_rec.object_name;
                        v_diff_objects(v_diff_objects.LAST).object_type := obj_type.object_type;
                        v_diff_objects(v_diff_objects.LAST).issue_type := 'DIFFERENT_STRUCTURE';
                    END IF;
                END IF;
            END;
        END LOOP;
    END LOOP;

    DBMS_OUTPUT.PUT_LINE('Objects to update in ' || prod_schema_name || ':');
    FOR i IN 1..v_diff_objects.COUNT LOOP
        DBMS_OUTPUT.PUT_LINE(v_diff_objects(i).object_type || ' ' ||
                             v_diff_objects(i).object_name || ' - ' ||
                             v_diff_objects(i).issue_type);
    END LOOP;
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Error: ' || SQLERRM);
END compare_schemas_objects;
/


CREATE OR REPLACE PROCEDURE sync_schemas (
    dev_schema_name IN VARCHAR2,
    prod_schema_name IN VARCHAR2
) 
AUTHID CURRENT_USER
AS
    TYPE obj_diff_rec IS RECORD (
        object_name VARCHAR2(128),
        object_type VARCHAR2(30),
        issue_type VARCHAR2(50),
        ddl_script CLOB
    );
    TYPE obj_diff_tab IS TABLE OF obj_diff_rec;
    v_diff_objects obj_diff_tab := obj_diff_tab();

    v_dev_schema VARCHAR2(128) := UPPER(dev_schema_name);
    v_prod_schema VARCHAR2(128) := UPPER(prod_schema_name);
    v_ddl CLOB;

    FUNCTION get_ddl_safe(p_obj_type IN VARCHAR2, p_obj_name IN VARCHAR2, p_schema IN VARCHAR2) 
    RETURN CLOB IS
        v_ddl CLOB;
    BEGIN
        DBMS_OUTPUT.PUT_LINE('Attempting to get DDL for ' || p_schema || '.' || p_obj_name || ' (' || p_obj_type || ')');
        
        v_ddl := DBMS_METADATA.GET_DDL(
            object_type => p_obj_type,
            name => p_obj_name,
            schema => p_schema
        );
        
        v_ddl := REPLACE(v_ddl, '"' || v_dev_schema || '".', '');
        v_ddl := REPLACE(v_ddl, '"' || v_prod_schema || '".', '');
        v_ddl := REPLACE(v_ddl, '"', '');
        
        DBMS_OUTPUT.PUT_LINE('Successfully retrieved DDL for ' || p_schema || '.' || p_obj_name);
        RETURN v_ddl;
    EXCEPTION
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('Error getting DDL for ' || p_schema || '.' || p_obj_name || ': ' || SQLERRM);
            RETURN '/* Error: ' || SQLERRM || ' */';
    END get_ddl_safe;

    FUNCTION tables_differ(p_table_name IN VARCHAR2) RETURN BOOLEAN IS
        v_col_diff NUMBER := 0;
        v_real_diff BOOLEAN := FALSE;
        v_dev_count NUMBER;
        v_prod_count NUMBER;
        v_dev_ddl CLOB;
        v_prod_ddl CLOB;
    BEGIN
        SELECT COUNT(*) INTO v_col_diff
        FROM (
            SELECT column_name, data_type, data_length, data_precision, data_scale, nullable
            FROM all_tab_columns
            WHERE owner = v_dev_schema AND table_name = p_table_name
            MINUS
            SELECT column_name, data_type, data_length, data_precision, data_scale, nullable
            FROM all_tab_columns
            WHERE owner = v_prod_schema AND table_name = p_table_name
            UNION ALL
            SELECT column_name, data_type, data_length, data_precision, data_scale, nullable
            FROM all_tab_columns
            WHERE owner = v_prod_schema AND table_name = p_table_name
            MINUS
            SELECT column_name, data_type, data_length, data_precision, data_scale, nullable
            FROM all_tab_columns
            WHERE owner = v_dev_schema AND table_name = p_table_name
        );
        
        IF v_col_diff > 0 THEN
            DBMS_OUTPUT.PUT_LINE('Table ' || p_table_name || ' has column differences: ' || v_col_diff);
            RETURN TRUE;
        END IF;
        
        BEGIN
            v_dev_ddl := TRIM(get_ddl_safe('TABLE', p_table_name, v_dev_schema));
            v_prod_ddl := TRIM(get_ddl_safe('TABLE', p_table_name, v_prod_schema));
            
            v_dev_ddl := REGEXP_REPLACE(v_dev_ddl, 'SEGMENT CREATION (IMMEDIATE|DEFERRED)', '');
            v_prod_ddl := REGEXP_REPLACE(v_prod_ddl, 'SEGMENT CREATION (IMMEDIATE|DEFERRED)', '');
            
            v_dev_ddl := REGEXP_REPLACE(v_dev_ddl, 'PCTFREE \d+ PCTUSED \d+ INITRANS \d+ MAXTRANS \d+', '');
            v_prod_ddl := REGEXP_REPLACE(v_prod_ddl, 'PCTFREE \d+ PCTUSED \d+ INITRANS \d+ MAXTRANS \d+', '');
            
            v_dev_ddl := REGEXP_REPLACE(v_dev_ddl, 'COMPUTE STATISTICS', '');
            v_prod_ddl := REGEXP_REPLACE(v_prod_ddl, 'COMPUTE STATISTICS', '');
            
            v_dev_ddl := REGEXP_REPLACE(v_dev_ddl, '\s+', ' ');
            v_prod_ddl := REGEXP_REPLACE(v_prod_ddl, '\s+', ' ');
            
            v_real_diff := (v_dev_ddl != v_prod_ddl);
            
            DBMS_OUTPUT.PUT_LINE('Table ' || p_table_name || ' DDL comparison: ' || 
                                 CASE WHEN v_real_diff THEN 'DIFFERENT' ELSE 'SAME' END);
            
            RETURN v_real_diff;
        EXCEPTION
            WHEN OTHERS THEN
                DBMS_OUTPUT.PUT_LINE('Error comparing DDL for table ' || p_table_name || ': ' || SQLERRM);

                SELECT COUNT(*) INTO v_dev_count 
                FROM all_constraints 
                WHERE owner = v_dev_schema AND table_name = p_table_name;
                
                SELECT COUNT(*) INTO v_prod_count 
                FROM all_constraints 
                WHERE owner = v_prod_schema AND table_name = p_table_name;
                
                IF v_dev_count != v_prod_count THEN
                    DBMS_OUTPUT.PUT_LINE('Table ' || p_table_name || ' has different constraint count: DEV=' || 
                                        v_dev_count || ', PROD=' || v_prod_count);
                    RETURN TRUE;
                END IF;
                
                SELECT COUNT(*) INTO v_dev_count 
                FROM all_indexes 
                WHERE owner = v_dev_schema AND table_name = p_table_name;
                
                SELECT COUNT(*) INTO v_prod_count 
                FROM all_indexes 
                WHERE owner = v_prod_schema AND table_name = p_table_name;
                
                IF v_dev_count != v_prod_count THEN
                    DBMS_OUTPUT.PUT_LINE('Table ' || p_table_name || ' has different index count: DEV=' || 
                                        v_dev_count || ', PROD=' || v_prod_count);
                    RETURN TRUE;
                END IF;
                
                RETURN FALSE;
        END;
    END tables_differ;

BEGIN
    DBMS_OUTPUT.ENABLE;

    DECLARE
        v_count NUMBER;
    BEGIN
        SELECT COUNT(*)
        INTO v_count
        FROM all_users
        WHERE username IN (v_dev_schema, v_prod_schema);

        IF v_count != 2 THEN
            DBMS_OUTPUT.PUT_LINE('Error: One or both schemas (' || v_dev_schema || ', ' || v_prod_schema || ') do not exist.');
            RETURN;
        END IF;
    END;

    FOR obj_type IN (
        SELECT 'TABLE' AS object_type FROM dual UNION ALL
        SELECT 'PROCEDURE' FROM dual UNION ALL
        SELECT 'FUNCTION' FROM dual UNION ALL
        SELECT 'PACKAGE' FROM dual UNION ALL
        SELECT 'INDEX' FROM dual
    ) LOOP
        FOR dev_rec IN (
            SELECT object_name
            FROM all_objects
            WHERE owner = v_dev_schema
            AND object_type = obj_type.object_type
            AND object_name NOT LIKE 'SYS_%'
        ) LOOP
            DECLARE
                v_prod_exists NUMBER;
                v_source_diff NUMBER;
                v_table_diff BOOLEAN := FALSE;
            BEGIN
                SELECT COUNT(*)
                INTO v_prod_exists
                FROM all_objects
                WHERE owner = v_prod_schema
                AND object_type = obj_type.object_type
                AND object_name = dev_rec.object_name;

                IF v_prod_exists = 0 THEN
                    v_diff_objects.EXTEND;
                    v_diff_objects(v_diff_objects.LAST).object_name := dev_rec.object_name;
                    v_diff_objects(v_diff_objects.LAST).object_type := obj_type.object_type;
                    v_diff_objects(v_diff_objects.LAST).issue_type := 'DOESNT_EXIST_IN_PROD';
                    v_diff_objects(v_diff_objects.LAST).ddl_script := get_ddl_safe(obj_type.object_type, dev_rec.object_name, v_dev_schema);
                ELSE
                    IF obj_type.object_type = 'TABLE' THEN
                        v_table_diff := tables_differ(dev_rec.object_name);
                        
                        IF v_table_diff THEN
                            v_diff_objects.EXTEND;
                            v_diff_objects(v_diff_objects.LAST).object_name := dev_rec.object_name;
                            v_diff_objects(v_diff_objects.LAST).object_type := obj_type.object_type;
                            v_diff_objects(v_diff_objects.LAST).issue_type := 'DIFFERENT_STRUCTURE';
                            v_diff_objects(v_diff_objects.LAST).ddl_script := 'DROP TABLE ' || dev_rec.object_name || ';' || CHR(10) ||
                                                                              get_ddl_safe(obj_type.object_type, dev_rec.object_name, v_dev_schema);
                        END IF;
                    ELSIF obj_type.object_type IN ('PROCEDURE', 'FUNCTION', 'PACKAGE') THEN
                        SELECT COUNT(*)
                        INTO v_source_diff
                        FROM (
                            SELECT text
                            FROM all_source
                            WHERE owner = v_dev_schema
                            AND name = dev_rec.object_name
                            AND type = obj_type.object_type
                            MINUS
                            SELECT text
                            FROM all_source
                            WHERE owner = v_prod_schema
                            AND name = dev_rec.object_name
                            AND type = obj_type.object_type
                        );
                        IF v_source_diff > 0 THEN
                            v_diff_objects.EXTEND;
                            v_diff_objects(v_diff_objects.LAST).object_name := dev_rec.object_name;
                            v_diff_objects(v_diff_objects.LAST).object_type := obj_type.object_type;
                            v_diff_objects(v_diff_objects.LAST).issue_type := 'DIFFERENT_CODE';
                            v_diff_objects(v_diff_objects.LAST).ddl_script := get_ddl_safe(obj_type.object_type, dev_rec.object_name, v_dev_schema);
                        END IF;
                    END IF;
                END IF;
            END;
        END LOOP;
    END LOOP;

    FOR prod_rec IN (
        SELECT object_name, object_type
        FROM all_objects
        WHERE owner = v_prod_schema
        AND object_type IN ('TABLE', 'PROCEDURE', 'FUNCTION', 'PACKAGE', 'INDEX')
        AND object_name NOT LIKE 'SYS_%'
    ) LOOP
        DECLARE
            v_dev_exists NUMBER;
        BEGIN
            SELECT COUNT(*)
            INTO v_dev_exists
            FROM all_objects
            WHERE owner = v_dev_schema
            AND object_type = prod_rec.object_type
            AND object_name = prod_rec.object_name;

            IF v_dev_exists = 0 THEN
                v_diff_objects.EXTEND;
                v_diff_objects(v_diff_objects.LAST).object_name := prod_rec.object_name;
                v_diff_objects(v_diff_objects.LAST).object_type := prod_rec.object_type;
                v_diff_objects(v_diff_objects.LAST).issue_type := 'DELETE_FROM_PROD';
                v_diff_objects(v_diff_objects.LAST).ddl_script := 'DROP ' || prod_rec.object_type || ' ' || prod_rec.object_name || ';';
            END IF;
        END;
    END LOOP;

    DBMS_OUTPUT.PUT_LINE('Synchronization script for ' || prod_schema_name || ':');
    IF v_diff_objects.COUNT = 0 THEN
        DBMS_OUTPUT.PUT_LINE('No differences found.');
    ELSE
        FOR i IN 1..v_diff_objects.COUNT LOOP
            DBMS_OUTPUT.PUT_LINE('/* ' || v_diff_objects(i).object_type || ' ' ||
                                 v_diff_objects(i).object_name || ' - ' ||
                                 v_diff_objects(i).issue_type || ' */');
            DBMS_OUTPUT.PUT_LINE(v_diff_objects(i).ddl_script);
            DBMS_OUTPUT.PUT_LINE('/');
        END LOOP;
    END IF;
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Unexpected error in sync_schemas: ' || SQLERRM);
END sync_schemas;
/ 

EXEC compare_schemas_tables('DEV', 'PROD');
EXEC compare_schemas_objects('DEV', 'PROD');
EXEC sync_schemas('DEV', 'PROD');

SELECT table_name
        FROM all_tables
        WHERE owner = 'DEV';