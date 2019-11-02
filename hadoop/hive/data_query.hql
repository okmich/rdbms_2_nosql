use movielens;

-- 
SELECT genre, count(1) AS COUNT
FROM
    (SELECT explode(genres) AS genre FROM movies) v
GROUP BY genre
ORDER BY COUNT DESC;

-- 
SELECT genre, count(1) AS COUNT
FROM movies_genres v
GROUP BY genre
ORDER BY COUNT DESC;

-- the rating summary 
SELECT m.movieid,
       m.title,
       count(1) AS count_rating,
       sum(r.rate) AS total_rating,
       avg(r.rate) AS average_rating,
       stddev_pop(r.rate) AS std_deviation,
       min(ts) AS earliest_ts,
       max(ts) AS latest_ts
FROM
    (SELECT movieid,
            rate,
            from_unixtime(ts) AS ts
     FROM movielens.ratings) r
JOIN movielens.movies m ON m.movieid = r.movieid
GROUP BY m.movieid,
         m.title
LIMIT 100;