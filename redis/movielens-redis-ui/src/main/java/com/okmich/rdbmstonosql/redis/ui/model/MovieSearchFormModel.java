/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.okmich.rdbmstonosql.redis.ui.model;

import com.okmich.rdbmstonosql.redis.dao.SearchResult;
import com.okmich.rdbmstonosql.redis.dao.MovieDao;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 *
 * @author Michael Enudi
 */
public class MovieSearchFormModel implements SearchFormModel<String> {

    private final MovieDao dao;
    private SearchResult<String> searchResult;
    private long recordCount;

    private final List<String> cursorList;
    private int cursorIndx = 0;

    private static final String[] COL_NAME = new String[]{"Data"};

    public MovieSearchFormModel(MovieDao dao) {
        this.dao = dao;
        this.cursorList = new ArrayList();
        cursorList.add("0");
    }

    @Override
    public void update(Object... args) {
        boolean criteriaChanged = (Boolean) args[2];

        if (criteriaChanged) {
            this.cursorList.clear();
            this.cursorList.add("0");
            this.cursorIndx = 0;
        }
        String cursor = cursorList.get(cursorIndx);
        this.searchResult = dao.findMoviesByGenreYear(args[0].toString(), args[1].toString(),
                cursor, DEFAULT_FETCH_SIZE,
                !cursor.equals("0"));
        //if the 3 argument is true, reset the cursorlist
        if (!cursorList.contains(searchResult.getCursor())) {
            cursorList.add(searchResult.getCursor());
        }

        this.recordCount = dao.countMoviesByGenreYear(args[0].toString(), args[1].toString());
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
    public String[][] searchCriteriaArray() {
        List<String> list = dao.findAllGenres();
        String[] temps = new String[list.size()];

        List<String> list2 = dao.findAllYears();
        Collections.sort(list2);
        String[] temps2 = new String[list.size()];
        return new String[][]{list.toArray(temps), list2.toArray(temps2)};
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
