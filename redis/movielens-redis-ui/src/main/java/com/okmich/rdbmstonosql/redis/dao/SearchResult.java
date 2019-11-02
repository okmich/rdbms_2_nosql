/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.okmich.rdbmstonosql.redis.dao;

import java.util.List;

/**
 *
 * @author Michael Enudi
 */
public class SearchResult<T> {
    private final String cursor;
    private final List<T> results;
    private final boolean lastIteration;

    public SearchResult(String cursor, List<T> results, boolean lastIteration) {
        this.cursor = cursor;
        this.results = results;
        this.lastIteration = lastIteration;
    }

    public String getCursor() {
        return cursor;
    }

    public List<T> getResults() {
        return results;
    }

    public boolean isLastIteration() {
        return lastIteration;
    }
    
    
}
