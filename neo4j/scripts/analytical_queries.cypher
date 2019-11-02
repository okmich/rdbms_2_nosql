// get the schema in your neo4j instance
call db.schema();

//use the apoc help function to find functions or procedures
call apoc.help('path');


///////////////////////////////////////////////////
////////////////  PERSONAL REFERRAL
//////////////////////////////////////////////////

//generate page rank score for all nodes in the person network
CALL algo.pageRank.stream('Person', 'REFERS', {iterations:50, dampingFactor:0.95})
YIELD nodeId, score
CALL apoc.nodes.get(nodeId) YIELD node
SET node.rankscore = score;

//generate centrality score for all nodes in the person network
CALL algo.closeness.stream('Person', 'REFERS')
YIELD nodeId, centrality
WITH algo.asNode(nodeId) AS node, centrality
SET node.closecenterscore = centrality;


//let us checkout our new state of network
MATCH (p:Person) 
RETURN p.id, p.firstname, p.lastame, p.gender, p.age, p.rankscore, p.closecenterscore;



///////////////////////////////////////////////////
////////////////  MOVIELENS
//////////////////////////////////////////////////
//what is common between two users.
MATCH (u:User {userId: 2334}) 
MATCH (v:User {userId: 1})
CALL apoc.algo.dijkstra(u, v, 'GAVE|TO', 'weight', 1.0, 5) YIELD path, weight
return path, weight;

//similarity calculation using jaccard similarity function
MATCH (u:User {userId: 2334})-[:GAVE]->()-[:TO]->(movies)
WITH u, collect(id(movies)) AS user1Movies
MATCH (v:User)-[:GAVE]->()-[:TO]->(movies2) WHERE v <> u
WITH u, user1Movies, v, collect(id(movies2)) AS user2Movies2
RETURN u.userId AS from,
       v.userId AS to,
       algo.similarity.jaccard(user1Movies, user2Movies2) AS similarity
ORDER BY similarity DESC
LIMIT 20;
