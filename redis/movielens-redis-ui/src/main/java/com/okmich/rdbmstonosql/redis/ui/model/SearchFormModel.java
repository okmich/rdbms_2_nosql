/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.okmich.rdbmstonosql.redis.ui.model;

import com.okmich.rdbmstonosql.redis.dao.SearchResult;

/**
 *
 * @author Michael Enudi
 */
public interface SearchFormModel<T> {
    
    int DEFAULT_FETCH_SIZE = 25;
    
    void update(Object... args);
    
    SearchResult<T> getSearchResult(String... ts);
    
    Object[][] tableData();
    
    String currentCursor();
    
    boolean hasNext();
    
    boolean hasPrevious();
    
    void next();
    
    void previous();
    
    boolean isLastIteration();
    
    default String[] searchCriteria() {
        return new String[0];
    }
    
    default String[][] searchCriteriaArray(){
        return new String[0][0];
    }
    
    long recordCount();
    
    String[] columnNames();
        
}
