-- genre stats
-- input is 
-- 1,Toy Story (1995),Adventure|Animation|Children|Comedy|Fantasy
-- output should be
-- Adventure, 1
-- Animation, 1
-- Children, 1
-- Comedy, 1
-- Fantasy, 1

register '/usr/hdp/current/pig-client/lib/piggybank.jar';

DEFINE myCSVLoader org.apache.pig.piggybank.storage.CSVLoader();
-- DEFINE myXlsStorage org.apache.pig.piggybank.storage.CSVExcelStorage();

-- load the movie data
raw_movie_full = LOAD '/user/maria_dev/raw/movielens/latest/movies' USING myCSVLoader() as (movieId:chararray, title:chararray, genres:chararray);
--remove the header
raw_movie = FILTER raw_movie_full BY (movieId != 'movieId');

-- project the movieId and genre
movie_genre = FOREACH raw_movie GENERATE FLATTEN(TOKENIZE(genres, '|')) as genre;

grp_movie_genre = GROUP movie_genre BY genre;
agg_data = FOREACH grp_movie_genre GENERATE group as genre, COUNT(movie_genre) as num_movies;
--sorting
sorted = ORDER agg_data BY genre;

STORE sorted INTO '/user/maria_dev/output/movielens/pig/genre-dist-text' USING  PigStorage('|'); 
STORE sorted INTO '/user/maria_dev/output/movielens/pig/genre-dist-avro' USING  AvroStorage(); 
STORE sorted INTO '/user/maria_dev/output/movielens/pig/genre-dist-orc' USING  OrcStorage(); 
STORE sorted INTO '/user/maria_dev/output/movielens/pig/genre-dist-json' USING  JsonStorage(); 
