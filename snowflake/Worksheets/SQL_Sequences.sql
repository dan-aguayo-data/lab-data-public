USE MANAGE_DB.PUBLIC;

CREATE OR REPLACE SEQUENCE my_seq
START = 1
INCREMENT = 1;


SELECT my_seq.nextval;
SELECT my_seq.nextval,my_seq.nextval;  --- once you run it it will stay recorded in which sequence number you are standing at

CREATE OR REPLACE TABLE sequence_test(
id int DEFAULT my_seq.nextval, 
first_name varchar);

INSERT INTO sequence_test(first_name)
VALUES
('Maria'),
('Frank');

SELECT * FROM sequence_test;