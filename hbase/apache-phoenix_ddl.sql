-- /usr/hdp/current/phoenix-client/bin/sqlline.py sandbox-hdp.hortonworks.com

-- !help

create schema mot;
use mot;
create table user (
	id integer PRIMARY KEY,
	name varchar(40),
	city varchar(20),
	age smallint,
	gender varchar(10)
);

-- write a record 
UPSERT INTO user VALUES (1,'Melissa Noel','Wolfsberg',19,'Male');
UPSERT INTO user VALUES (2, 'Casey Ewing','Essen',39,'Female');
UPSERT INTO user VALUES (3,'Brenda Morgan','Smoky Lake',37,'Female');
UPSERT INTO user VALUES (4,'Norman Phillips','Maidenhead',26,'Female');
UPSERT INTO user VALUES (5,'Octavius Cobb','Etobicoke',35,'Female');
UPSERT INTO user VALUES (6,'Kyle Mathis','Temuco',38,'Male');
-- query your table
select * from user;
select * from user where gender= 'Female';

-- lets do some ACID
!autocommit off
UPSERT INTO user VALUES (12,'Luke French','Varena',36,'Female');
UPSERT INTO user VALUES (13,'Kuame Barry','Saint-Martin',22,'Female');
UPSERT INTO user VALUES (14,'Jermaine Cantrell','Moliterno',25,'Male');
UPSERT INTO user VALUES (15,'Clark Wright','Schwaz',38,'Male');
UPSERT INTO user VALUES (16,'Brennan Lloyd','Bergeggi',23,'Female');
UPSERT INTO user VALUES (17,'Troy Petersen','Galzignano Terme',19,'Male');
UPSERT INTO user VALUES (18,'Cassidy Franklin','Khandwa',30,'Male');
UPSERT INTO user VALUES (19,'Tobias Evans','Perpignan',25,'Male');
UPSERT INTO user VALUES (20,'Kelly Norton','Bridlington',35,'Female');
UPSERT INTO user VALUES (21,'Channing Sherman','Sherbrooke',19,'Female');
select * from user where id = 21;
!commit

-- lets try a rollback
!autocommit off
UPSERT INTO user VALUES (22,'Hadley Webster','Olsztyn',22,'Male');
select * from user where id = 22;
!rollback

select * from user where id = 22;

-- aggregation query
select gender, avg(age) from user group by gender;

-- copy this records into another table
create table user_backup (
id integer PRIMARY KEY,
name varchar(40),
city varchar(20),
age smallint,
gender varchar(10)
);

UPSERT INTO user_backup SELECT * FROM user;

-- to delete a record 
DELETE FROM user WHERE age >= 25;

--
-- if you are using namespace, please check 
-- https://community.hortonworks.com/articles/89464/create-phoenix-schemas-in-hdp-25.html on
-- how to enable the phoenix property phoenix.schema.isNamespaceMappingEnabled=true using ambari
--
-- if you are not using ambari, you can add the property setting directly to hbase-site.xml
--
-- see all phoenix data types - https://phoenix.apache.org/language/datatypes.html
--
-- create a phoenix view that reads off our hbase tables
--

CREATE VIEW "movie" (
id UNSIGNED_INT PRIMARY KEY, 
"a"."title" varchar(100), 
"a"."ryear" varchar(4),
"a"."actn" boolean,
"a"."advnt" boolean,
"a"."anmtn" boolean,
"a"."chldrn" boolean,
"a"."comdy" boolean,
"a"."crime" boolean,
"a"."dcmtry" boolean,
"a"."drama" boolean,
"a"."fntsy" boolean,
"a"."film" boolean,
"a"."horro" boolean,
"a"."music" boolean,
"a"."mysty" boolean,
"a"."rmance" boolean,
"a"."scifi" boolean,
"a"."thrllr" boolean,
"a"."war" boolean,
"a"."wstrn" boolean
);

CREATE VIEW "altmovie" (
id varchar(50) PRIMARY KEY, 
"gm"."mid" UNSIGNED_INT,
"gm"."title" varchar(100), 
"gm"."genre" varchar(20), 
"gm"."ryear" varchar(6)
);


CREATE VIEW "rating" (
id varchar(20) PRIMARY KEY, 
"a"."uid" UNSIGNED_INT, 
"a"."mid" UNSIGNED_INT,
"a"."rate" UNSIGNED_FLOAT,
"a"."ts" varchar(30)
);

CREATE VIEW "denom_rating" (
id varchar(20) PRIMARY KEY, 
"r"."uid" UNSIGNED_INT, 
"r"."mid" UNSIGNED_INT,
"r"."title" varchar(100),
"r"."ryear" varchar(4),
"r"."age" varchar(30),
"r"."occ" varchar(30),
"r"."gndr" varchar(30),
"r"."rate" UNSIGNED_FLOAT,
"r"."ts" varchar(30)
);

CREATE VIEW "user" (
id UNSIGNED_INT PRIMARY KEY,
"u"."gndr" varchar(30), 
"u"."ageid" varchar(4), 
"u"."age" varchar(15),
"u"."occid" varchar(4),
"u"."occ" varchar(30),
"u"."zip" varchar(10)
);
