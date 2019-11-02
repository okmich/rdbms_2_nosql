use movielens_dw;
-- find the percentage of ratings for genres made for each month of a specific year
select v.*, 
    v.rating_counts / SUM(v.rating_counts) OVER (PARTITION BY v.year,v.monthname) AS factor, 
    (v.rating_counts / SUM(v.rating_counts) OVER (PARTITION BY v.year,v.monthname)) * 100 AS percentage
    ,RANK() OVER (PARTITION BY v.year,v.monthname ORDER BY v.rating_counts DESC) AS irank
FROM ( 
    SELECT d.year,
        d.monthname,
        gm.genre_name,
        COUNT(1) rating_counts
    FROM fact_rating f
    JOIN dim_movie_genre gm ON gm.movie_pk = f.movie_pk
    JOIN dim_date d ON d.date_pk = f.date_pk
    WHERE d.year = 2001 and d. month = 1 -- change the year in focus
    GROUP BY d.year,
        d.monthname,
        gm.genre_name
) AS v;
  

-- fetch the top 3 genres for each demographics (gender, occupation) of viewers
WITH my_query as (
    SELECT genre_name, gender, occupation, 
        ROW_NUMBER() OVER(PARTITION BY gender, occupation ORDER BY avg_rating DESC) as position
    FROM
        (SELECT u.gender,
            u.occupation,
            mg.genre_name,
            avg(f.rating) avg_rating
        FROM fact_rating f
        JOIN dim_movie m ON m.movie_pk = f.movie_pk
        JOIN dim_user u ON u.user_pk = f.user_pk
        JOIN dim_movie_genre mg ON mg.movie_pk = f.movie_pk
        GROUP BY u.gender,
            u.occupation,
            mg.genre_name
        HAVING COUNT(1) > 200) v
) 
select * from my_query q
where q.position  <= 3;