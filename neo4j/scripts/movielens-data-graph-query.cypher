// Return the top x most rated movies
MATCH (u:User)-[:GAVE]->(r:Rating)-[t:TO]->(m:Movie)
WITH m.movieId as movieId, m.title as title, count(r) as count, AVG(r.rate) as avg_rate
WHERE count > 2000
RETURN movieId, title, count, avg_rate
ORDER BY avg_rate DESC //which should we use? count or rating
LIMIT 15;

// Return other movies that belong to the same genre as movie x
MATCH (m:Movie {movieId : 5})-[:IS_A]-(g)-[:IS_A]-(otherMovies)
return m, g, otherMovies


// Return 10 most rated movies that were rated by other people who rated movie X positively
MATCH (x:Movie {movieId : 122})<-[:TO]-(r)<-[:GAVE]-(u:User)
WHERE r.rate > 3 
WITH u as users
MATCH (users)-[:GAVE]-(r)-[:TO]-(m:Movie)
WITH m, AVG(r.rate) AS score, , COUNT(r) AS scount
ORDER BY score DESC, scount DESC
RETURN m, score , scount
LIMIT 10;

//similar to the query above. We only removed the first WITH conjunction
MATCH (x:Movie {movieId : 122})<-[:TO]-(xr:Rating)<-[:GAVE]-(us:User)-[:GAVE]->(r2:Rating)-[:TO]->(m:Movie)
WHERE xr.rate > 3 
WITH m, AVG(r2.rate) AS score, COUNT(r2) AS scount
ORDER BY score DESC, scount DESC
RETURN m, score, scount
LIMIT 10;


// Return 10 most rated movies by people like our user x
MATCH (u:User {userId : 700})-[:GAVE]->(r)-[:TO]->(m:Movie)<-[:TO]-(r2)-[:GAVE]-(others:User)-[:GAVE]->(r3)-[:TO]->(m2:Movie)
WHERE r.rate >= 3 AND r2.rate >=3 AND r3.rate >= 3 AND u.gender = others.gender and u.age=others.age 
WITH m2 AS movie, AVG(r3.rate) AS score, count(r3)  AS ratings
RETURN movie
ORDER BY ratings DESC, score DESC
LIMIT 10
