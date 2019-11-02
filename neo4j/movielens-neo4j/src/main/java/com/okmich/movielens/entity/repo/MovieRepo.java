/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.okmich.movielens.entity.repo;

import com.okmich.movielens.entity.Movie;
import java.util.List;
import org.springframework.data.neo4j.annotation.Query;
import org.springframework.data.neo4j.repository.Neo4jRepository;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

/**
 *
 * @author michael.enudi
 */
@Repository
public interface MovieRepo extends Neo4jRepository<Movie, Long> {

    Movie findByMovieId(long movieId);

    @Query("MATCH (x:Movie {movieId : {movieId}})<-[]-(xr:Rating)<-[:GAVE]-(us:User)-[]->(r2:Rating)-[:TO]->(m:Movie) "
            + "WHERE xr.rate > 3 "
            + "WITH m, AVG(r2.rate) AS score, COUNT(r2) AS scount "
            + "ORDER BY score DESC, scount DESC "
            + "RETURN m LIMIT 10")
    List<Movie> getMoviesByMovieId(@Param("movieId") Long movieId);
}
