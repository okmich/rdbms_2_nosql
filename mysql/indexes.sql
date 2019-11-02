use movielens;

-- explain statement
explain select * from ratings where user_id=20;
explain select * from movies where release_year = 1990;

-- show indexes 
show indexes from ratings; -- show index from ratings will also work
show keys from ratings;

-- create a table without index
create table ratings_without_index as
select * from ratings;

show keys from ratings_without_index;

--explain on the new table
explain select * from ratings_without_index where user_id = 20;

-- let do more complex queries
explain SELECT g.name genre,
       m.release_year,
       count(1) rating_count,
       sum(r.rating) total_rating
FROM ratings r
JOIN movies m ON m.id = r.movie_id
JOIN genres_movies gm ON gm.movie_id = m.id
JOIN genres g ON g.id = gm.genre_id
GROUP BY g.name,
         m.release_year;

-- unique index
CREATE unique index genre_name_indx on genre(name);

-- full text index
explain select m.title, t.tags from tags t join movies m on m.id = t.movie_id 
-- where match (t.tags) against('super heroes');
where t.tags = 'super heroes';

-- create a full text index
create fulltext index tag_tag_ft_indx on tags(tags);

-- now use the same full text search function. It should work now.
select * from tags t join movies m on m.id = t.movie_id 
where match (t.tags) against('super hero');

--lets explain the full text query
explain select * from tags t join movies m on m.id = t.movie_id 
where match (t.tags) against('super hero');

-- show indexs on the tag table
show keys from tags;

-- drop indexes 
drop index movie_release_year_indx on movies;
