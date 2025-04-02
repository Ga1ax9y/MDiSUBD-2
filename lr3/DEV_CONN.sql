CREATE TABLE books (
    book_id NUMBER PRIMARY KEY,
    title VARCHAR2(200) NOT NULL,
    author VARCHAR2(100) NOT NULL,
    is_available CHAR(1) DEFAULT 'Y' CHECK (is_available IN ('Y', 'N'))
);

CREATE TABLE readers (
    reader_id NUMBER PRIMARY KEY,
    reader_name VARCHAR2(100) NOT NULL,
    book_id NUMBER,
    CONSTRAINT fk_book FOREIGN KEY (book_id) REFERENCES books(book_id)
);

CREATE INDEX IDX_READER_NAME ON readers(reader_name);

CREATE TABLE loans (
    loan_id NUMBER PRIMARY KEY,
    book_id NUMBER,
    reader_id NUMBER,
    loan_date DATE DEFAULT SYSDATE,
    return_date DATE,
    CONSTRAINT fk_loan_book FOREIGN KEY (book_id) REFERENCES books(book_id),
    CONSTRAINT fk_loan_reader FOREIGN KEY (reader_id) REFERENCES readers(reader_id)
);

CREATE OR REPLACE PROCEDURE GREET_USER (p_name IN VARCHAR2) AS
BEGIN
    DBMS_OUTPUT.PUT_LINE('Hello, ' || p_name || ' from Dev Library!');
END;
/

CREATE OR REPLACE FUNCTION CALC_FINE (p_days_late IN NUMBER) 
RETURN NUMBER AS
BEGIN
    RETURN p_days_late * 10;
END;
/

CREATE OR REPLACE PACKAGE LIBRARY_PKG AS
    PROCEDURE add_book(p_title IN VARCHAR2, p_author IN VARCHAR2);
    FUNCTION get_book_count RETURN NUMBER;
END LIBRARY_PKG;
/

CREATE OR REPLACE PACKAGE BODY LIBRARY_PKG AS
    PROCEDURE add_book(p_title IN VARCHAR2, p_author IN VARCHAR2) AS
    BEGIN
        INSERT INTO books (book_id, title, author)
        VALUES (books_seq.NEXTVAL, p_title, p_author);
    END;

    FUNCTION get_book_count RETURN NUMBER AS
        v_count NUMBER;
    BEGIN
        SELECT COUNT(*) INTO v_count FROM books;
        RETURN v_count;
    END;
END LIBRARY_PKG;
/
CREATE INDEX IDX_READER_NAME ON READERS (READER_NAME) 
  PCTFREE 10 INITRANS 2 MAXTRANS 255 COMPUTE STATISTICS 
  TABLESPACE USERS; 
/

CREATE SEQUENCE books_seq START WITH 1;

DROP TABLE books CASCADE CONSTRAINTS;
DROP TABLE A CASCADE CONSTRAINTS;
DROP TABLE B CASCADE CONSTRAINTS;
DROP TABLE readers CASCADE CONSTRAINTS;
DROP TABLE loans CASCADE CONSTRAINTS;
DROP INDEX idx_reader_name;
DROP PROCEDURE greet_user;
DROP FUNCTION calc_fine;
DROP PACKAGE LIBRARY_PKG;
DROP SEQUENCE books_seq;
PURGE RECYCLEBIN;

CREATE TABLE A (
    id NUMBER PRIMARY KEY,
    b_id NUMBER
);
CREATE TABLE B (
    id NUMBER PRIMARY KEY,
    a_id NUMBER
);
ALTER TABLE A
ADD CONSTRAINT fk_a_b FOREIGN KEY (b_id) REFERENCES B(id);
ALTER TABLE B
ADD CONSTRAINT fk_b_a FOREIGN KEY (a_id) REFERENCES A(id);