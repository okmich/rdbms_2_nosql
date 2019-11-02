/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.okmich.movielens.es.model;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 *
 * @author michael.enudi
 */
public class Movie extends Model {

    private final long movieId;
    private final String title;
    private final String year;
    private final String[] genres;

    public Movie(long movieId, String title, String year, String[] genres) {
        this.movieId = movieId;
        this.title = title;
        this.year = year;
        this.genres = genres;
    }

    /**
     * @return the movieId
     */
    public long getMovieId() {
        return movieId;
    }

    /**
     * @return the title
     */
    public String getTitle() {
        return title;
    }

    /**
     * @return the year
     */
    public String getYear() {
        return year;
    }

    /**
     * @return the genres
     */
    public String[] getGenres() {
        return genres;
    }

    @Override
    public Map<String, Object> toMap() {
        Map<String, Object> map = new LinkedHashMap<>(4);

        map.put("movieId", this.getMovieId());
        map.put("title", this.getTitle());
        map.put("year", this.getYear());
        map.put("genres", this.getGenres());

        return map;
    }

    @Override
    public String toString() {
        return "Movie{" + "movieId=" + movieId + ", title=" + title + ", year=" + year + ", genres=" + Arrays.toString(genres) + '}';
    }

}
