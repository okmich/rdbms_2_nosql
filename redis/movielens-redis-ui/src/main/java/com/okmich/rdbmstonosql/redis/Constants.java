/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.okmich.rdbmstonosql.redis;

/**
 *
 * @author Michael Enudi
 */
public interface Constants {

    String FIELD_DELIMITER = "---";

    String BASE_PREFIX = "app.ml.";

    String MOVIE_PREFIX = "movie.";
    String RATING_PREFIX = "rating.";
    String USER_PREFIX = "user.";

    String CHANNEL_PREFIX = "app.msg.chnl.new.";

    String MOVIE_CHANNEL = CHANNEL_PREFIX + "movie";
    String RATING_CHANNEL = CHANNEL_PREFIX + "rating";
    String USER_CHANNEL = CHANNEL_PREFIX + "user";

    default boolean isEmpty(String arg) {
        return arg == null || arg.trim().isEmpty();
    }

}
