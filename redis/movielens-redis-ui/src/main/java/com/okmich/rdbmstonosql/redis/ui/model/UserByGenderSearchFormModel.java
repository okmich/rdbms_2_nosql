/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.okmich.rdbmstonosql.redis.ui.model;

import com.okmich.rdbmstonosql.redis.dao.SearchResult;
import com.okmich.rdbmstonosql.redis.dao.UserDao;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author Michael Enudi
 */
public class UserByGenderSearchFormModel implements SearchFormModel<String> {

    private final UserDao dao;
    private SearchResult<String> searchResult;
    private long recordCount;

    private final List<String> cursorList;
    private int cursorIndx = 0;

    private static final String[] COL_NAME = new String[]{"Data"};

    public UserByGenderSearchFormModel(UserDao dao) {
        this.dao = dao;
        this.cursorList = new ArrayList();
        cursorList.add("0");
    }

    @Override
    public void update(Object... args) {
        String cursor = cursorList.get(cursorIndx);
        this.searchResult = dao.findUsersByGender(args[0].toString(),
                cursor, DEFAULT_FETCH_SIZE * 2,
                !cursor.equals("0"));
        //update the list of visited cursor
        if (!cursorList.contains(searchResult.getCursor())) {
            cursorList.add(searchResult.getCursor());
        }
        this.recordCount = dao.countUsersByGender(args[0].toString());
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
    public void next() {
        this.cursorIndx++;
    }

    @Override
    public void previous() {
        this.cursorIndx--;
    }

    @Override
    public boolean isLastIteration() {
        return searchResult.isLastIteration();
    }

    @Override
    public String[] searchCriteria() {
        List<String> list = dao.getAllGenders();
        String[] temps = new String[list.size()];
        return list.toArray(temps);
    }

    @Override
    public long recordCount() {
        return this.recordCount;
    }

    @Override
    public String[] columnNames() {
        return COL_NAME;
    }
}
