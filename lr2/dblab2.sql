/*Построить две таблицы STUDENTS и GROUPS 
реализующих соответственно 
справочник студентов и справочник групп*/
CREATE TABLE GROUPS (
    ID NUMBER PRIMARY KEY,
    NAME VARCHAR2(255) NOT NULL,
    C_VAL NUMBER DEFAULT 0 NOT NULL
);

CREATE TABLE STUDENTS (
    ID NUMBER PRIMARY KEY,
    NAME VARCHAR2(255) NOT NULL,
    GROUP_ID NUMBER NOT NULL,
    CONSTRAINT fk_group FOREIGN KEY (GROUP_ID) REFERENCES GROUPS(ID)
);

DROP TABLE GROUPS;
DROP TABLE STUDENTS;

/*Реализовать триггеры для таблиц задания 1 проверку целостности 
(проверка на уникальность полей ID), генерацию автоинкрементного ключа 
и проверку уникальности для поля GROUP.NAME*/
CREATE SEQUENCE groups_seq START WITH 1 INCREMENT BY 1;
CREATE SEQUENCE students_seq START WITH 1 INCREMENT BY 1;
DROP SEQUENCE students_seq;

CREATE OR REPLACE TRIGGER groups_unique_name
BEFORE INSERT OR UPDATE ON GROUPS
FOR EACH ROW
DECLARE
    v_id_count NUMBER;
BEGIN
    IF UPDATING AND :OLD.C_VAL != :NEW.C_VAL THEN
        RETURN;
    END IF;

    IF INSERTING THEN
        :NEW.ID := groups_seq.NEXTVAL;
    END IF;

    IF INSERTING THEN
        SELECT COUNT(*) INTO v_id_count FROM GROUPS WHERE ID = :NEW.ID;
    ELSIF UPDATING THEN
        SELECT COUNT(*) INTO v_id_count FROM GROUPS WHERE ID = :NEW.ID AND ID != :OLD.ID;
    END IF;

    IF v_id_count > 0 THEN
        RAISE_APPLICATION_ERROR(
            -20000, 
            'Такой ID уже существует в таблице GROUPS'
        );
    END IF;

    IF INSERTING THEN
        SELECT COUNT(*) INTO v_id_count FROM GROUPS WHERE NAME = :NEW.NAME;
    ELSIF UPDATING THEN
        SELECT COUNT(*) INTO v_id_count FROM GROUPS WHERE NAME = :NEW.NAME AND NAME != :OLD.NAME;
    END IF;
    
    IF v_id_count > 0 THEN
        RAISE_APPLICATION_ERROR(-20001, 'Группа с таким названием уже существует');
    END IF;
END;
/

CREATE OR REPLACE TRIGGER students_unique_name
BEFORE INSERT OR UPDATE ON STUDENTS
FOR EACH ROW
DECLARE
    PRAGMA AUTONOMOUS_TRANSACTION; 
    v_id_count NUMBER;
BEGIN
    IF INSERTING THEN
        :NEW.ID := students_seq.NEXTVAL;
    END IF;

    IF INSERTING THEN
        SELECT COUNT(*) INTO v_id_count FROM STUDENTS WHERE ID = :NEW.ID;
    ELSIF UPDATING THEN
        SELECT COUNT(*) INTO v_id_count FROM STUDENTS WHERE ID = :NEW.ID AND ID != :OLD.ID;
    END IF;

    IF v_id_count > 0 THEN
        RAISE_APPLICATION_ERROR(
            -20002, 
            'ID  уже существует в таблице STUDENTS'
        );
    END IF;
END;
/
-- Пример
INSERT INTO GROUPS (NAME) VALUES ('Group A');
INSERT INTO GROUPS (NAME) VALUES ('Group A');
SELECT * FROM groups;
INSERT INTO STUDENTS (NAME, GROUP_ID) VALUES ('Alice', 1);
INSERT INTO STUDENTS (NAME, GROUP_ID) VALUES ('Bob', 1);

--Реализовать триггер реализующий Foreign Key с каскадным удалением между таблицами STUDENTS и GROUPS
CREATE OR REPLACE TRIGGER groups_cascade
BEFORE DELETE ON GROUPS
FOR EACH ROW
BEGIN

    DELETE FROM STUDENTS WHERE GROUP_ID = :OLD.ID;
END;
/
-- Пример
ALTER TRIGGER update_group_count DISABLE;
INSERT INTO GROUPS (NAME) VALUES ('Group B');
SELECT * FROM GROUPS;
select * from students;
INSERT INTO STUDENTS (NAME, GROUP_ID) VALUES ('G2', 62);
INSERT INTO STUDENTS (NAME, GROUP_ID) VALUES ('G3', 62);
DELETE FROM GROUPS WHERE ID = 62;
ALTER TRIGGER update_group_count ENABLE;

-- Реализовать триггер реализующий журналирование всех действий над данными таблицы STUDENTS
CREATE TABLE STUDENTS_LOGER (
    LOGER_ID NUMBER PRIMARY KEY,
    ACTION_TYPE VARCHAR2(10),
    OLD_ID NUMBER,
    NEW_ID NUMBER,
    OLD_NAME VARCHAR2(255),
    NEW_NAME VARCHAR2(255),
    OLD_GROUP_ID NUMBER,
    NEW_GROUP_ID NUMBER,
    ACTION_TIME TIMESTAMP DEFAULT SYSTIMESTAMP
);
CREATE SEQUENCE loger_seq START WITH 1 INCREMENT BY 1;

CREATE OR REPLACE TRIGGER students_dml
AFTER INSERT OR UPDATE OR DELETE ON STUDENTS
FOR EACH ROW
BEGIN
    IF INSERTING THEN
        INSERT INTO STUDENTS_LOGER (LOGER_ID, ACTION_TYPE, NEW_ID, NEW_NAME, NEW_GROUP_ID)
        VALUES (loger_seq.NEXTVAL, 'INSERT', :NEW.ID, :NEW.NAME, :NEW.GROUP_ID);
    ELSIF UPDATING THEN
        INSERT INTO STUDENTS_LOGER (LOGER_ID, ACTION_TYPE, 
            OLD_ID, NEW_ID, OLD_NAME, NEW_NAME, OLD_GROUP_ID, NEW_GROUP_ID)
        VALUES (loger_seq.NEXTVAL, 'UPDATE', 
            :OLD.ID, :NEW.ID, :OLD.NAME, :NEW.NAME, :OLD.GROUP_ID, :NEW.GROUP_ID);
    ELSIF DELETING THEN
        INSERT INTO STUDENTS_LOGER (LOGER_ID, ACTION_TYPE, OLD_ID, OLD_NAME, OLD_GROUP_ID)
        VALUES (loger_seq.NEXTVAL, 'DELETE', :OLD.ID, :OLD.NAME, :OLD.GROUP_ID);
    END IF;
END;
/
-- Пример
INSERT INTO STUDENTS (NAME, GROUP_ID) VALUES ('Charlie', 1);
UPDATE STUDENTS SET NAME = 'Charles' WHERE ID = 7;
DELETE FROM STUDENTS WHERE ID = 6; 
SELECT * FROM STUDENTS;
SELECT * FROM students_loger;
SELECT * FROM groups;

/*Исходя из данных предыдущей задачи, реализовать процедуру для восстановления информации 
на указанный временной момент и на временное смещение*/
CREATE OR REPLACE PROCEDURE restore_students(
    p_restore_time TIMESTAMP
) AS
BEGIN
    FOR loger_rec IN (
        SELECT * FROM STUDENTS_LOGER
        WHERE ACTION_TIME >= p_restore_time
        ORDER BY ACTION_TIME DESC
    ) LOOP
        IF loger_rec.ACTION_TYPE = 'INSERT' THEN
            DELETE FROM STUDENTS WHERE ID = loger_rec.NEW_ID;
        ELSIF loger_rec.ACTION_TYPE = 'UPDATE' THEN
            UPDATE STUDENTS SET
                NAME = loger_rec.OLD_NAME,
                GROUP_ID = loger_rec.OLD_GROUP_ID
            WHERE ID = loger_rec.OLD_ID;
        ELSIF loger_rec.ACTION_TYPE = 'DELETE' THEN
            INSERT INTO STUDENTS (ID, NAME, GROUP_ID)
            VALUES (loger_rec.OLD_ID, loger_rec.OLD_NAME, loger_rec.OLD_GROUP_ID);
        END IF;
    END LOOP;
END;
/

CREATE OR REPLACE PROCEDURE restore_students_by_offset(
    p_interval VARCHAR2
) AS
    v_restore_time TIMESTAMP;
BEGIN
    v_restore_time := SYSTIMESTAMP - TO_DSINTERVAL(p_interval);
    restore_students(v_restore_time);
END;
/
-- Пример
INSERT INTO STUDENTS (NAME, GROUP_ID) VALUES ('test7', 1);
INSERT INTO STUDENTS (NAME, GROUP_ID) VALUES ('test8', 1);
select * from students_loger;
SELECT * from students;
select * from groups;
CALL restore_students_by_offset('0 00:01:00');

/*Реализовать триггер, который в случае изменения данных в таблице STUDENTS 
будет соответственно обновлять информацию C_VAL таблицы GROUPS*/
CREATE OR REPLACE TRIGGER update_group_count
AFTER INSERT OR UPDATE OR DELETE ON STUDENTS
FOR EACH ROW
BEGIN
    IF INSERTING THEN
        UPDATE GROUPS SET C_VAL = C_VAL + 1 WHERE ID = :NEW.GROUP_ID;
    ELSIF UPDATING THEN
        UPDATE GROUPS SET C_VAL = C_VAL - 1 WHERE ID = :OLD.GROUP_ID;
        UPDATE GROUPS SET C_VAL = C_VAL + 1 WHERE ID = :NEW.GROUP_ID;
ELSIF DELETING THEN
    BEGIN
        UPDATE GROUPS SET C_VAL = C_VAL - 1 WHERE ID = :OLD.GROUP_ID;
        -- если ошибка
    EXCEPTION
        WHEN OTHERS THEN
            NULL;
    END;
    END IF;
END;
/
select * from students;
select * from groups;

