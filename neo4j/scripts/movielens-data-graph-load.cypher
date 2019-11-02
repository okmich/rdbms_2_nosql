CREATE CONSTRAINT ON (u:User) ASSERT u.userId IS UNIQUE;
CREATE CONSTRAINT ON (m:Movie) ASSERT m.movieId IS UNIQUE;
CREATE CONSTRAINT ON (g:Genre) ASSERT g.name IS UNIQUE;

CREATE INDEX ON :Rating(userId);
CREATE INDEX ON :Rating(movieId);

//load predefined genres
CREATE (: Genre {name: "Action"}),
		(: Genre {name: "Adventure"}),
		(: Genre {name: "Animation"}),
		(: Genre {name: "Children's"}),
		(: Genre {name: "Comedy"}),
		(: Genre {name: "Crime"}),
		(: Genre {name: "Documentary"}),
		(: Genre {name: "Drama"}),
		(: Genre {name: "Fantasy"}),
		(: Genre {name: "Film-Noir"}),
		(: Genre {name: "Horror"}),
		(: Genre {name: "Musical"}),
		(: Genre {name: "Mystery"}),
		(: Genre {name: "Romance"}),
		(: Genre {name: "Sci-Fi"}),
		(: Genre {name: "Thriller"}),
		(: Genre {name: "War"}),
		(: Genre {name: "Western"});
		

//load movies
USING PERIODIC COMMIT 100
LOAD CSV WITH HEADERS FROM 'file:///home/dataeng/rdbms_2_nosql/datasets/movielens/movies.csv' AS line
CREATE (m:Movie {movieId: TOINT(line.id)})
SET m.title=line.title
SET m.genres=line.genres;


//load users
USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM 'file:///home/dataeng/rdbms_2_nosql/datasets/movielens/users.csv' AS line
CREATE (u:User {userId: TOINT(line.id), age: line.age_group, gender: line.gender, occupation: line.occupation, zipCode: line.zip_code});


//load rating and rating relationship
USING PERIODIC COMMIT 10000 //without this line, we get an outofmemory exception
LOAD CSV WITH HEADERS FROM 'file:///home/dataeng/rdbms_2_nosql/datasets/movielens/ratings.csv' AS line
MATCH (m:Movie {movieId : TOINT(line.mId)})
MATCH (u:User {userId: TOINT(line.uId)})
CREATE (r:Rating {rate: TOFLOAT(line.rate), ts: line.ts}),
	   (u)-[:GAVE]->(r),
       (r)-[:TO]->(m);


//evolve the data to have a genre with relationship to the movie
MATCH (movie:Movie)
WHERE coalesce(movie.genres, "-") <> "-"
WITH SPLIT(movie.genres, "|") as parts, movie as m
UNWIND parts as x
MATCH (g: Genre {name: x})
MERGE (m)-[:IS_A]->(g)
REMOVE m.genres;
