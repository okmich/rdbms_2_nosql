/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.okmich.movielens.entity.repo;

import com.okmich.movielens.entity.Genre;
import org.springframework.data.neo4j.repository.Neo4jRepository;
import org.springframework.stereotype.Repository;

/**
 *
 * @author michael.enudi
 */
@Repository
public interface GenreRepo extends Neo4jRepository<Genre, Long> {

}
