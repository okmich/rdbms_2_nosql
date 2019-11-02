create database movielens;
 
use movielens;
-- create the raw table for ratings
create external table tmp_ratings (
  userId bigint,
  movieId bigint,
  rate float,
  ts bigint
)
row format delimited 
fields terminated by ","
location '/user/hadoop/raw/movielens/latest/ratings';


-- create the raw table for movies
create external table tmp_movie(
  movieId bigint,
  title string,
  genres string
)
row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
with serdeproperties (
  "separatorChar" = ",",
  "quoteChar"     = "\"",
  "escapeChar"    = "\\"
) location '/user/hadoop/raw/movielens/latest/movies';


--
-- create processsed tables
-- 
create external table movies(
  movieId bigint,
  title string,
  genres array<string>
)
stored as orc
location '/user/hadoop/processed/movielens/latest/movies';

create external table movies_genres (
  movieId bigint,
  genre string
)
stored as orc
location '/user/hadoop/processed/movielens/latest/movies_genres';

create external table ratings (
  userId bigint,
  movieId bigint,
  rate float,
  ts bigint
)
stored as orc
location '/user/hadoop/processed/movielens/latest/ratings';

insert overwrite table movies 
select cast(movieid as bigint) movieid, title, split(genres, '\\|') genres from tmp_movie where genres != 'genres';

insert overwrite table movies_genres 
select movieid, genre from movies lateral view explode(genres) genres as genre where title != 'title';

insert overwrite table ratings 
select * from tmp_ratings;

-- drop table tmp_ratings;

select count(1) from movies_genres;
select count(1) from movies;
select count(1) from ratings;

