/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.okmich.rdbmstonosql.redis.dao;

import com.okmich.rdbmstonosql.redis.Constants;
import static com.okmich.rdbmstonosql.redis.Constants.BASE_PREFIX;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Tuple;

/**
 *
 * @author Michael Enudi
 */
public class RatingDao implements Constants {

    private final Jedis jedis;

    private static final Logger LOG = Logger.getLogger("RatingDao");
    public static final String KEY_PREFIX = BASE_PREFIX + RATING_PREFIX;

    public static final String ALL_RATING_KEY = KEY_PREFIX + "alll";
    public static final String RATING_BY_MOVIE_KEY = KEY_PREFIX + "bymovie#";
    public static final String RATING_USER_KEY = KEY_PREFIX + "usercountHLL";
    public static final String RATING_SCORE_KEY = KEY_PREFIX + "summaryboardz";

    public static final String FIELD_CNT_PREFIX = "cnt-";
    public static final String FIELD_TOTAL_PREFIX = "total-";

    public RatingDao(Jedis jedis) {
        this.jedis = jedis;
    }

    public void createRating(String payload) {
        //write to list the rating
        //increment the movie rating_cnt and the total_rating
        //put the user who rated into hyperloglog for user rated.
        //calculate the new rating score and add that to movie_rating_board

        String fields[] = payload.split(FIELD_DELIMITER);
        String movieId = fields[1], userId = fields[0];
        float rate = Float.parseFloat(fields[2]);

        String countFieldKey = FIELD_CNT_PREFIX + movieId;
        String sumFieldKey = FIELD_TOTAL_PREFIX + movieId;

        long icount;
        double sum;

        //Transaction transaction = this.jedis.multi();
        try {
            //write to list the rating
            jedis.lpush(ALL_RATING_KEY, payload);

            //increment the movie rating_cnt and the total_rating
            icount = jedis.hincrBy(RATING_BY_MOVIE_KEY, countFieldKey, 1);
            sum = jedis.hincrByFloat(RATING_BY_MOVIE_KEY, sumFieldKey, rate);

            double score = (sum / icount);
            jedis.zadd(RATING_SCORE_KEY, score, movieId);

            //put the user who rated into hyperloglog for user rated.
            jedis.pfadd(RATING_USER_KEY, userId);

            jedis.publish(RATING_CHANNEL, "xxxxxxxx");
            //transaction.exec();
        } catch (Exception ex) {
            //transaction.discard();
            LOG.log(Level.SEVERE, ex.getMessage(), ex);
            throw new RuntimeException(ex);
        } finally {
            //transaction.close();
        }

    }

    public Map<String, Double> findTopNRatedMovies(int n) {
        //get the top elements from the movie_rating_board
        Set<Tuple> tupleSet = this.jedis.zrevrangeWithScores(RATING_SCORE_KEY, 0, n);
        Map<String, Double> result = new LinkedHashMap(tupleSet.size());
        for (Tuple t : tupleSet) {
            result.put(getMovieTitle(t.getElement()), t.getScore());
        }
        return result;
    }

    public long getActiveRatingUser() {
        //get the top elements from the movie_rating_board
        return this.jedis.pfcount(RATING_USER_KEY);
    }

    public SearchResult<String> findAllRatings(int page, int fetchSize) {
        //get the top elements from the movie_rating_board
        List<String> list = this.jedis.lrange(ALL_RATING_KEY, page * fetchSize, (fetchSize * (page + 1)) - 1);
        return new SearchResult(String.valueOf(page + 1), list, list.isEmpty() || list.size() < fetchSize);
    }

    public long countAllRatings() {
        return this.jedis.llen(ALL_RATING_KEY);
    }

    private String getMovieTitle(String id) {
        String pyload = jedis.hget(MovieDao.ALL_MOVIE_HASH_KEY, id);
        if (pyload == null) return id;
       
        return id + " - " + pyload.split(FIELD_DELIMITER)[1];
    }
}
