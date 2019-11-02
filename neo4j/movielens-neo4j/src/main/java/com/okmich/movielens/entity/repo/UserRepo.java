/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.okmich.movielens.entity.repo;

import com.okmich.movielens.entity.Movie;
import com.okmich.movielens.entity.User;
import java.util.List;
import org.springframework.data.neo4j.annotation.Query;
import org.springframework.data.neo4j.repository.Neo4jRepository;
import org.springframework.stereotype.Repository;

/**
 *
 * @author michael.enudi
 */
@Repository
public interface UserRepo extends Neo4jRepository<User, Long> {

    @Query("MATCH (u:User {userId : {0}})-[:GAVE]->(r)-[:TO]->(m:Movie)<-[:TO]-(r2)-[]-(others:User)-[:GAVE]->(r3)-[:TO]->(m2:Movie)\n"
            + "WHERE r.rate >= 3 AND r2.rate >=3 AND r3.rate >= 4 AND u.gender = others.gender and u.age=others.age \n"
            + "WITH m2 AS movie, AVG(r3.rate) AS score, count(r3)  AS ratings\n"
            + "RETURN movie\n"
            + "ORDER BY ratings DESC, score DESC\n"
            + "LIMIT 10")
    List<Movie> recommendByUserRatingHistory(long userId);
    
    User findByUserId(Long userId);

}