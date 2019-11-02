use movielens_dw;

-- genre movie counting for the year 2000
SELECT gm.genre_name,
       count(1)
FROM dim_movie_genre gm
JOIN dim_movie m ON m.movie_pk = gm.movie_pk
WHERE release_year = '2000'
GROUP BY gm.genre_name;


-- total rating summary for each movie
SELECT m.title,
       m.release_year,
       count(f.rating) rating_count,
       sum(f.rating) total_rating,
       avg(f.rating) avg_rating,
       variance(f.rating)
FROM fact_rating f
JOIN dim_movie m ON f.movie_pk = m.movie_pk
GROUP BY m.title,
         m.release_year;


-- total rating summary for genres for years
SELECT gm.genre_name,
       count(1) rating_count,
       sum(r.rating) total_rating,
       avg(r.rating) avg_rating,
       variance(r.rating)
FROM fact_rating r
JOIN dim_movie_genre gm ON gm.movie_pk = r.movie_pk
GROUP BY gm.genre_name;


-- number of count for each rating class (1-5) for a certain genre on a monthly basis
SELECT gm.genre_name,
       d.year event_year,
       d.monthname event_month,
       r.rating stars,
       count(1) event_count
FROM fact_rating r
JOIN dim_movie_genre gm ON gm.movie_pk = r.movie_pk
JOIN dim_date d ON d.date_pk = r.date_pk
GROUP BY gm.genre_name,
         d.year,
         d.monthname,
         r.rating;
