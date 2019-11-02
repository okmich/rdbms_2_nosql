-- show databases
show databases;

-- create a database
create database helloworld;

-- switch a namespace
use helloworld;

-- lets see all the measurements in this database
show measurements;

-- lets see all the series in this database
show  series;

-- lets see all the field in this database
show field keys;

-- lets see all the tag in this database
show tag keys;


-- body temperature query
CREATE CONTINUOUS QUERY temp_measure_ma ON hellodb BEGIN  SELECT mean(reading) INTO temp_meausre_ma FROM temp_measure GROUP BY time(1m, 2m) END;






