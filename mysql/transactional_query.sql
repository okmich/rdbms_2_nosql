use movielens;

-- Signing up a new user
select * from age_group;
select * from occupations where id = 3;
insert into users(age, gender, occupation_id, zip_code) values(35, 'F', 3, '00000'); -- 6041

-- Adding a new movie
START TRANSACTION;
insert into movies(title, release_year) values('Toy story 4', 2020); 

-- find the just inserted movies
select LAST_INSERT_ID(); -- 3953

select * from genres;

-- choose Animation, Adventure, Comedy 
insert into genres_movies(movie_id, genre_id) values(3953, 4);
insert into genres_movies(movie_id, genre_id) values(3953, 2);
insert into genres_movies values(null, 3953, 8);

COMMIT;

-- Updating movie information
update movies set title='Toy Story 4' where id=3953;

-- Delete a movie
delete from movies where id=3953;

-- get the cause of the foreign constraint violation message
select * from genres_movies where movie_id = 3953;

START TRANSACTION;

delete from genres_movies where movie_id=3953;
delete from movies where id=3953;

ROLLBACK;

-- Paginated query of movie
SELECT * from movies order by title desc limit 1, 100;
SELECT * from movies limit 101, 200;
