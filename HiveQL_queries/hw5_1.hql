USE dualcore;

CREATE TABLE loyalty_program
    (cust_id INT,
     f_name STRING,
     l_name STRING,
     email STRING,
     loyalty_lvl STRING,
     phone_numbers MAP<STRING,STRING>,
     order_id ARRAY<INT>,
     order_data STRUCT<min: INT,
                       max: INT,
                       avg: INT,
                       total: INT>
)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '|'
COLLECTION ITEMS TERMINATED BY ','
MAP KEYS TERMINATED BY ':';
