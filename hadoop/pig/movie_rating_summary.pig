register '/home/hadoop/datafu-pig-incubating-1.3.1.jar'

define VAR datafu.pig.stats.VAR();

raw_ratings = LOAD '/user/hadoop/raw/movielens/latest/ratings' USING PigStorage(',') as (userId: long, movieId: long, rating:float, ts:long);

filtered_ratings = FILTER raw_ratings BY userId is not null;
ratings = FOREACH filtered_ratings GENERATE movieId, rating, ToDate(ts * 1000) AS rating_dt;

grouped_ratings = GROUP ratings BY movieId;
agg_ratings = FOREACH grouped_ratings GENERATE group as movieId, COUNT(ratings.rating) as no_ratings, SUM(ratings.rating) as total_ratings, AVG(ratings.rating) as avg_ratings, VAR(ratings.rating) as var_ratings, MIN(ratings.rating_dt) AS earliest, MAX(ratings.rating_dt) as latest;

STORE agg_ratings INTO '/user/hadoop/output/movielens/pig/agg_ratings_pipe' using PigStorage('|');
