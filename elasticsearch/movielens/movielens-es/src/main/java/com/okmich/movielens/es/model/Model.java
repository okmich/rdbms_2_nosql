/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.okmich.movielens.es.model;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

/**
 *
 * @author michael.enudi
 */
public abstract class Model {

    public abstract Map<String, Object> toMap();

    protected final DateFormat DF = new SimpleDateFormat("dd/MM/yyyy HH:mm");

    protected void joinMovie(Movie movie, Map<String, Object> model) {
        model.put("title", movie.getTitle());
        model.put("year", movie.getYear());
        model.put("genres", movie.getGenres());
    }

    public final Date secondsTsToDate(long secondTs) {
        return new Date(secondTs * 1000);
    }

    public final String secondsTsToDateString(long secondTs) {
        return DF.format(secondsTsToDate(secondTs));
    }
}
