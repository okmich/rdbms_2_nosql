/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.okmich.movielens.es.model;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 *
 * @author michael.enudi
 */
public class Tag extends Model {

    private final long userId;
    private final long movieId;
    private final String tag;
    private final String timestamp;

    private Movie movie;

    public Tag(long userId, long movieId, String tag, long timestamp) {
        this.userId = userId;
        this.movieId = movieId;
        this.tag = tag;
        this.timestamp = secondsTsToDateString(timestamp);
    }

    /**
     * @return the userId
     */
    public long getUserId() {
        return userId;
    }

    /**
     * @return the movieId
     */
    public long getMovieId() {
        return movieId;
    }

    /**
     * @return the tag
     */
    public String getTag() {
        return tag;
    }

    /**
     * @return the timestamp
     */
    public String getTimestamp() {
        return timestamp;
    }

    /**
     * @param movie
     */
    public void joinMovie(Movie movie) {
        this.movie = movie;
    }

    /**
     * @return the movie
     */
    public Movie getMovie() {
        return movie;
    }

    @Override
    public Map<String, Object> toMap() {
        Map<String, Object> map = new LinkedHashMap<>(4);

        map.put("userId", this.getUserId());
        map.put("movieId", this.getMovieId());
        if (this.movie != null) {
            joinMovie(movie, map);
        }
        map.put("tag", this.getTag());
        map.put("ts", this.getTimestamp());

        return map;
    }

    @Override
    public String toString() {
        return "Tag{" + "userId=" + userId + ", movieId=" + movieId + ", tag=" + tag + ", timestamp=" + timestamp + ", movie=" + movie + '}';
    }

}
