/*task1*/
CREATE TABLE MyTable (
    id NUMBER,
    val NUMBER
);
/*task2*/
CREATE SEQUENCE mytable_seq START WITH 1 INCREMENT BY 1;
BEGIN
    FOR i IN 1..10000 LOOP
        INSERT INTO MyTable (id, val)
        VALUES (mytable_seq.NEXTVAL, TRUNC(DBMS_RANDOM.VALUE(1, 1000)));
    END LOOP;
    COMMIT;
END;
/
/*task3*/
CREATE OR REPLACE FUNCTION check_even_odd RETURN VARCHAR2 IS
    even_count NUMBER := 0;
    odd_count NUMBER := 0;
BEGIN
    SELECT COUNT(*) INTO even_count FROM MyTable WHERE MOD(val, 2) = 0;
    SELECT COUNT(*) INTO odd_count FROM MyTable WHERE MOD(val, 2) = 1;

    IF even_count > odd_count THEN
        RETURN 'TRUE';
    ELSIF odd_count > even_count THEN
        RETURN 'FALSE';
    ELSE
        RETURN 'EQUAL';
    END IF;
END;
/
SELECT check_even_odd FROM dual;

/*task4*/
CREATE OR REPLACE FUNCTION generate_insert_statement(p_id IN NUMBER) RETURN VARCHAR2 IS
    v_val NUMBER;
    v_sql VARCHAR2(200);
BEGIN
    SELECT val INTO v_val FROM MyTable WHERE id = p_id;

    v_sql := 'INSERT INTO MyTable (id, val) VALUES (' || p_id || ', ' || v_val || ');';

    RETURN v_sql;
EXCEPTION
    WHEN NO_DATA_FOUND THEN
        RETURN 'No record found for ID: ' || p_id;
END;
/
SELECT generate_insert_statement(5) FROM dual;

/*task5*/
CREATE OR REPLACE PROCEDURE insert_mytable(p_id IN NUMBER, p_val IN NUMBER) IS
BEGIN
    INSERT INTO MyTable (id, val) VALUES (p_id, p_val);
    COMMIT;
END;
/

CREATE OR REPLACE PROCEDURE update_mytable(p_id IN NUMBER, p_new_val IN NUMBER) IS
BEGIN
    UPDATE MyTable SET val = p_new_val WHERE id = p_id;
    COMMIT;
EXCEPTION
    WHEN NO_DATA_FOUND THEN
        DBMS_OUTPUT.PUT_LINE('No record found for ID: ' || p_id);
END;
/

CREATE OR REPLACE PROCEDURE delete_mytable(p_id IN NUMBER) IS
BEGIN
    DELETE FROM MyTable WHERE id = p_id;
    COMMIT;
EXCEPTION
    WHEN NO_DATA_FOUND THEN
        DBMS_OUTPUT.PUT_LINE('No record found for ID: ' || p_id);
END;
/
EXEC delete_mytable(6857684);
EXEC insert_mytable(10001,123);
EXEC update_mytable(10001,124);

/*task6*/
CREATE OR REPLACE FUNCTION calculate_annual_compensation(
    p_monthly_salary IN NUMBER,
    p_bonus_percent IN NUMBER
) RETURN NUMBER IS
    v_total_compensation NUMBER;
BEGIN
    -- Проверка на корректность данных
    IF p_monthly_salary <= 0 OR p_bonus_percent < 0 THEN
        RAISE_APPLICATION_ERROR(-666, 'Invalid input data');
    END IF;

    -- Преобразование процента в дробное число
    v_total_compensation := (1 + p_bonus_percent / 100) * 12 * p_monthly_salary;

    RETURN v_total_compensation;
END;
/
SELECT calculate_annual_compensation(666, 6) FROM dual;
select * from dual;