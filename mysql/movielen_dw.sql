create database movielens_dw;
use movielens_dw;

-- create the procedures to load the tables from the movielens schema
DROP PROCEDURE IF EXISTS load_date_dim;
DELIMITER $
CREATE PROCEDURE load_date_dim (start_date DATE, stop_date DATE)
BEGIN
    DECLARE last_date date;
    DECLARE i_date date;
    
    DELETE FROM dim_date;

    SET i_date = start_date;
    WHILE i_date <= stop_date DO
        INSERT INTO dim_date (`date`, day_of_week, dayname, is_weekend, month, monthname, day_of_month, quarter, `year`, year_quarter) 
            VALUES (
                i_date,
                dayofweek(i_date),
                dayname(i_date),
                weekday(i_date) = 5 or weekday(i_date) = 6,
                month(i_date),
                monthname(i_date),
                dayofmonth(i_date),
                concat('Q', quarter(i_date)),
                year(i_date),
                concat(year(i_date), '_Q', quarter(i_date))
            );

        SET i_date = DATE_ADD(i_date, INTERVAL 1 DAY);
    END WHILE;
END $
DELIMITER ;


--- create a function to get the time of day
DROP FUNCTION IF EXISTS get_time_of_day;
DELIMITER $$
CREATE FUNCTION get_time_of_day(t_day TIME) RETURNS char(10) DETERMINISTIC
BEGIN
  DECLARE time_of_day CHAR(10);
  DECLARE vtime INT;
    
  SET vtime = hour(t_day);
    
  IF vtime = 0 THEN
    SET time_of_day = 'Mid-Night';
  ELSEIF (vtime > 0 AND vtime < 6) THEN 
    SET time_of_day ='Night';
  ELSEIF (vtime > 6 AND vtime < 12) THEN 
    SET time_of_day ='Morning';
  ELSEIF (vtime = 12) THEN 
    SET time_of_day ='Noon';
  ELSEIF (vtime > 12 AND vtime < 16) THEN 
    SET time_of_day ='Afternoon';
  ELSEIF (vtime > 16 AND vtime < 20) THEN 
    SET time_of_day ='Evening';
  ELSE
    SET time_of_day ='Late Night';
  END IF;
    
  RETURN time_of_day;
END $$
DELIMITER ;

--- create the procedures to load the time dimension 
DROP PROCEDURE IF EXISTS load_time_dim;
DELIMITER $
CREATE PROCEDURE load_time_dim ()
BEGIN
    DECLARE last_time time;
    DECLARE start_time time;
    DECLARE i_t time;
    
    DELETE FROM dim_time;

    SET last_time = '23:59:59';
    SET i_t = '00:00:00';
    WHILE i_t <= last_time DO
        INSERT INTO dim_time (full_time, hour, hour24, minute, ampm, timeofdayname) 
            VALUES (
                i_t,
                TIME_FORMAT(i_t, '%h'),
                TIME_FORMAT(i_t, '%H'),
                minute(i_t),
                TIME_FORMAT(i_t, '%p'),
                get_time_of_day(i_t)
            );

        SET i_t = DATE_ADD(i_t, INTERVAL 1 MINUTE);
    END WHILE;
END $
DELIMITER ;




DROP TABLE IF EXISTS dim_date;
CREATE TABLE dim_date (
  date_pk         bigint primary key auto_increment, 
  date            date not null,
  day_of_week     char(10) not null,
  dayname         char(12) not null,
  is_weekend      boolean not null default false,
  month           tinyint not null,
  monthname       char(10) not null,
  day_of_month    tinyint not null, 
  quarter         char(2) not null,
  year            int not null,
  year_quarter    char(7) not null,
  unique key dim_date_date_idx (date)
);
-- call procedure
call load_date_dim ('2000-01-01', '2010-12-31');

DROP TABLE IF EXISTS dim_time;
CREATE TABLE dim_time (
  time_pk         bigint primary key auto_increment, 
  full_time       time not null,
  hour            char(2) not null,
  hour24          char(2) not null,
  minute          char(2) not null,
  ampm            char(2) not null, 
  timeofdayname   varchar(10) not null comment 'morning, noon, afternoon, evening, midnight, night',
  unique key dim_time_time_unique_idx (full_time)
);

-- load data 
call load_time_dim();

drop table if exists dim_movie;
create table dim_movie (
  movie_pk        bigint primary key,  -- map to movie_id
  title           varchar(100) not null,
  release_year    char(4),
  insert_date     datetime
);

-- load dim_movie
INSERT INTO dim_movie (movie_pk, title,release_year, insert_date)
SELECT id,
       title,
       release_year,
       curdate()
FROM movielens.movies;



drop table if exists dim_movie_genre;
create table dim_movie_genre (
  movie_genre_pk  bigint primary key auto_increment, 
  movie_pk        bigint not null,
  movie_genre_id  bigint not null,
  genre_id        bigint not null,
  genre_name      varchar(20) not null,
  insert_date     datetime,
  unique key dim_movie_genre_unique_idx (movie_pk, genre_id),
  foreign key dim_movie_genre_dim_movie_fk (movie_pk) REFERENCES dim_movie (movie_pk)
);

-- load dim_movie
INSERT INTO dim_movie_genre (movie_pk, movie_genre_id, genre_id, genre_name, insert_date)
SELECT gm.movie_id,
       gm.id AS movie_genre_id,
       g.id AS genre_id,
       g.name,
       curdate()
FROM movielens.genres_movies gm
JOIN movielens.genres g ON g.id = gm.genre_id;


drop table if exists dim_user;
create table dim_user (
  user_pk         bigint primary key auto_increment, 
  user_id         bigint not null,
  age_group_id    bigint not null,
  age_group       char(15),
  occupation_id   bigint not null,
  occupation      char(20),
  gender          char(6),
  zip_code        char(12),
  insert_date     datetime
);

-- load dim_movie
INSERT INTO dim_user (user_id, age_group_id, age_group, occupation_id, occupation, gender, zip_code, insert_date)
SELECT u.id,
       u.age,
       ag.name AS age_group,
       u.occupation_id,
       oc.name AS occuption,
       CASE u.gender
           WHEN 'F' THEN 'Female'
           WHEN 'M' THEN 'Male'
           ELSE 'na'
       END,
       u.zip_code,
       curdate()
FROM movielens.users u
LEFT JOIN movielens.age_group ag ON ag.id = u.age
LEFT JOIN movielens.occupations oc ON oc.id = u.occupation_id;


-- fact_rating
drop table if exists fact_rating;
create table fact_rating (
  rating_pk       bigint primary key auto_increment, 
  movie_pk        bigint not null,
  user_pk         bigint not null,
  date_pk         bigint not null,
  time_pk         bigint not null,
  rating          float,
  insert_date     datetime,
  foreign key fact_rating_dim_date_fk (date_pk) REFERENCES dim_date (date_pk),
  foreign key fact_rating_dim_time_fk (time_pk) REFERENCES dim_time (time_pk),
  foreign key fact_rating_dim_movie_fk (movie_pk) REFERENCES dim_movie (movie_pk),
  foreign key fact_rating_dim_user_fk (user_pk) REFERENCES dim_user (user_pk)
);


INSERT INTO fact_rating (rating_pk, movie_pk, user_pk, date_pk, time_pk, rating, insert_date)
SELECT r.id,
       m.movie_pk,
       u.user_pk,
       d.date_pk,
       t.time_pk,
       r.rating,
       curdate()
FROM movielens.ratings r
JOIN movielens_dw.dim_movie m ON m.movie_pk = r.movie_id
JOIN movielens_dw.dim_user u ON u.user_pk = r.user_id
JOIN movielens_dw.dim_date d ON DATE(d.date) = DATE(r.rated_at)
JOIN movielens_dw.dim_time t ON t.full_time = time(DATE_FORMAT(rated_at, '%H:%i:00'));

