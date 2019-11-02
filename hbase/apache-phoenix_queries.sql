-- https://phoenix.apache.org/language/functions.htm
--
-- genre movie counting
-- 
select m."genre", count(1) counts
from "rating" r JOIN "altmovie" m on m."mid" = r."mid" 
group by m."genre";

--
-- total rating summary for each movie
-- 
SELECT "mid", "title", COUNT(1) rating_count, SUM("rate") total_rating, AVG("rate") avg_rating, STDDEV_POP("rate")
FROM "denom_rating" 
GROUP BY "mid", "title";

--
-- total rating summary for genres for years
-- 
-- SELECT g.name genre,
--        m.release_year,
--        count(1) rating_count,
--        sum(r.rating) total_rating
-- FROM ratings r
-- JOIN movies m ON m.id = r.movie_id
-- JOIN genres_movies gm ON gm.movie_id = m.id
-- JOIN genres g ON g.id = gm.genre_id
-- GROUP BY g.name,
--          m.release_year;
SELECT "genre", "ryear", COUNT(1) rating_count, SUM("rate") total_rating
FROM "rating" 
GROUP BY "mid", "title";



