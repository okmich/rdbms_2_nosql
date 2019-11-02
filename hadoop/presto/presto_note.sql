
-- show to connect directly to a catalog
-- presto --server <server:port> --catalog <catalog_name>;
presto --server sandbox-hdp.hortonworks.com:7001 --catalog hive

-- show all catalogs
show catalogs;

-- show schema
show schemas;

-- from a catalog and schema show tables
show schemas from hive;
-- can also use in 
show tables IN hive.movielens;

-- run the genres_count exercise with unnest
SELECT genre, count(1) COUNT
FROM movies m
CROSS JOIN UNNEST(genres) AS t (genre)
GROUP BY genre
ORDER BY COUNT DESC;

-- run the genres_count exercise with denormalized table
SELECT genre, count(1) AS COUNT
FROM movies_genres v
GROUP BY genre
ORDER BY COUNT DESC;

-- run the rating summary
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
