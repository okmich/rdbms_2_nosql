/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.okmich.rdbmstonosql.redis.ui.model;

import com.okmich.rdbmstonosql.redis.dao.SearchResult;
import com.okmich.rdbmstonosql.redis.dao.RatingDao;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author Michael Enudi
 */
public class RatingsSearchFormModel implements SearchFormModel<String> {

    private final RatingDao dao;
    private SearchResult<String> searchResult;
    private long recordCount;

    private final List<String> cursorList;
    private int cursorIndx = 0;

    private static final String[] COL_NAME = new String[]{"Data"};

    public RatingsSearchFormModel(RatingDao dao) {
        this.dao = dao;
        this.cursorList = new ArrayList();
        cursorList.add("0");
    }

    @Override
    public void update(Object... args) {
        String cursor = cursorList.get(cursorIndx);
        this.searchResult = dao.findAllRatings(Integer.parseInt(cursor), 50);
        //update the list of visited cursor
        if (!cursorList.contains(searchResult.getCursor())) {
            cursorList.add(searchResult.getCursor());
        }
        this.recordCount = dao.countAllRatings();
    }

    @Override
    public SearchResult<String> getSearchResult(String... ts) {
        return this.searchResult;
    }

    @Override
    public Object[][] tableData() {
        List<String> result = searchResult.getResults();
        Object[][] data = new Object[result.size()][columnNames().length];
        for (int i = 0; i < data.length; i++) {
            data[i][0] = result.get(i);
        }
        return data;
    }

    @Override
    public String currentCursor() {
        return searchResult.getCursor();
    }

    @Override
    public boolean hasNext() {
        return !searchResult.isLastIteration();
    }

    @Override
    public boolean hasPrevious() {
        return cursorIndx > 0;
    }

    @Override
    public boolean isLastIteration() {
        return searchResult.isLastIteration();
    }

    @Override
    public String[] searchCriteria() {
        return new String[]{};
    }

    @Override
    public long recordCount() {
        return this.recordCount;
    }

    @Override
    public String[] columnNames() {
        return COL_NAME;
    }

    @Override
    public void next() {
        this.cursorIndx++;
    }

    @Override
    public void previous() {
        this.cursorIndx--;
    }

}
