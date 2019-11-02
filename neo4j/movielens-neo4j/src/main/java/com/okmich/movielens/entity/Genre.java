/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.okmich.movielens.entity;

import java.io.Serializable;
import org.neo4j.ogm.annotation.Id;
import org.neo4j.ogm.annotation.NodeEntity;

/**
 *
 * @author michael.enudi
 */
@NodeEntity
public class Genre implements Serializable {

    @Id
    private Long id;
    private String name;

    public Genre() {
    }

    /**
     * @return the id
     */
    public Long getId() {
        return id;
    }

    /**
     * @param id the id to set
     */
    public void setId(Long id) {
        this.id = id;
    }

    /**
     * @return the name
     */
    public String getName() {
        return name;
    }

    /**
     * @param tag the name to set
     */
    public void setName(String tag) {
        this.name = tag;
    }

    @Override
    public String toString() {
        return "Genre{" + "name=" + name + '}';
    }
}
