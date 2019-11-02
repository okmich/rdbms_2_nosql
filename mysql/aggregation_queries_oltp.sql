use movielens;

-- genre movie counting for the year 2000
SELECT g.name,
       count(movie_id)
FROM genres_movies gm
JOIN genres g ON g.id =gm.genre_id
JOIN movies m ON m.id = gm.movie_id
WHERE release_year = '2000'
GROUP BY g.name;


-- total rating summary for each movie
SELECT m.title,
       m.release_year,
       count(r.rating) rating_count,
       sum(r.rating) total_rating,
       avg(r.rating) avg_rating,
       variance(r.rating)
FROM movies m
JOIN ratings r ON r.movie_id = m.id
GROUP BY m.title,
         m.release_year;


-- total rating summary for genres for years
SELECT g.name genre,
       m.release_year,
       count(1) rating_count,
       sum(r.rating) total_rating
FROM ratings r
JOIN movies m ON m.id = r.movie_id
JOIN genres_movies gm ON gm.movie_id = m.id
JOIN genres g ON g.id = gm.genre_id
GROUP BY g.name,
         m.release_year;


-- number of count for each rating class (1-5) for a certain genre on a monthly basis
SELECT g.name genre,
       r.event_year,
       r.event_month,
       r.stars,
       count(1) event_count
FROM
    (SELECT movie_id,
            year(rated_at) event_year,
            month(rated_at) event_month,
            rating stars
     FROM ratings) r
JOIN genres_movies gm ON gm.movie_id = r.movie_id
JOIN genres g ON g.id = gm.genre_id
GROUP BY g.name,
         r.event_year,
         r.event_month,
         r.stars;

