/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.okmich.rdbmstonosql.redis.dao;

import com.okmich.rdbmstonosql.redis.Constants;
import static com.okmich.rdbmstonosql.redis.dao.RatingDao.RATING_BY_MOVIE_KEY;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;
import redis.clients.jedis.Transaction;

/**
 *
 * @author Michael Enudi
 */
public class MovieDao implements Constants {

    private final Jedis jedis;
    private static final Logger LOG = Logger.getLogger("MovieDao");
    public static final String KEY_PREFIX = BASE_PREFIX + MOVIE_PREFIX;

    public static final String ALL_MOVIE_KEY = KEY_PREFIX + "alll";
    public static final String ALL_MOVIE_HASH_KEY = KEY_PREFIX + "all#";
    public static final String MOVIE_BY_YEAR_KEY = KEY_PREFIX + "byyear.";
    public static final String MOVIE_BY_GENRE_KEY = KEY_PREFIX + "bygenre.";

    private final Pattern yearPattern = Pattern.compile("^.+ \\((\\d{4})\\)$");

    public MovieDao(Jedis jedis) {
        this.jedis = jedis;
    }

    public void createMovie(String payload) {
        //write to a list  movie properties
        //write to a set for the year -> movieId
        //write to a set for the genre -> movieId

        Transaction transaction = jedis.multi();
        String parts[] = payload.split(FIELD_DELIMITER);
        String movieId = parts[0];
        String year = getYear(parts[1]);
        String genres = parts[parts.length - 1];

        try {
            transaction.rpush(ALL_MOVIE_KEY, payload);
            transaction.hset(ALL_MOVIE_HASH_KEY, movieId, payload);
            if (!year.trim().isEmpty()) {
                transaction.sadd(MOVIE_BY_YEAR_KEY + year.trim(), payload);
            }
            if (!genres.trim().isEmpty()) {
                String[] genreArr = genres.split("\\|");
                for (String genre : genreArr) {
                    transaction.sadd(MOVIE_BY_GENRE_KEY + genre.trim(), payload);
                }
            }
            transaction.publish(MOVIE_CHANNEL, "a new movie is created");

            transaction.exec();
        } catch (Exception e) {
            transaction.discard();
            LOG.log(Level.SEVERE, e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    public List<String> listMovies(int page, int pagesize) {
        int start = (page - 1) * pagesize;
        int stop = (pagesize * page) - 1;
        return this.jedis.lrange(ALL_MOVIE_KEY, start, stop);
    }

    public SearchResult<String> findMoviesByGenreYear(String genre, String year, String cursor, int fetchSize, boolean isCursorFectch) {
        if (isEmpty(genre) && isEmpty(year)) {
            //load all 
            int page = Integer.parseInt(cursor);
            List<String> list = this.jedis.lrange(ALL_MOVIE_KEY, page * fetchSize, (fetchSize * (page + 1)) - 1);
            return new SearchResult(String.valueOf(page + 1), list, list.isEmpty() || list.size() < fetchSize);
        }

        //return a cursor of user age set 
        ScanParams scanParam = new ScanParams().count(fetchSize);
        String iCursor = "0";
        if (isCursorFectch) {
            iCursor = cursor;
        }
        if (!isEmpty(genre) && isEmpty(year)) {
            //load by genre
            ScanResult<String> scanResult = jedis.sscan(MOVIE_BY_GENRE_KEY + genre, iCursor, scanParam);
            return new SearchResult(scanResult.getCursor(), scanResult.getResult(), scanResult.isCompleteIteration());

        } else if (isEmpty(genre) && !isEmpty(year)) {
            //load by year
            ScanResult<String> scanResult = jedis.sscan(MOVIE_BY_YEAR_KEY + year, iCursor, scanParam);
            return new SearchResult(scanResult.getCursor(), scanResult.getResult(), scanResult.isCompleteIteration());

        } else {
            //load by both
            List<String> joinedSet = new ArrayList(jedis.sinter(MOVIE_BY_GENRE_KEY + genre, MOVIE_BY_YEAR_KEY + year));
            Collections.sort(joinedSet);
            return new SearchResult(iCursor, joinedSet, true);
        }

    }

    public long countMoviesByGenreYear(String genre, String year) {
        if (isEmpty(genre) && isEmpty(year)) {
            return jedis.llen(ALL_MOVIE_KEY);
        } else if (!isEmpty(genre) && isEmpty(year)) {
            //load by genre
            return jedis.scard(MOVIE_BY_GENRE_KEY + genre);
        } else if (isEmpty(genre) && !isEmpty(year)) {
            //load by year
            return jedis.scard(MOVIE_BY_YEAR_KEY + genre);
        } else {
            return jedis.sinter(MOVIE_BY_GENRE_KEY + genre, MOVIE_BY_YEAR_KEY + year).size();
        }
    }

    public Map<String, Long> findGenreMovieDist() {
        //read all keys for genres
        //for each key, read the length of the set
        Set<String> keys = this.jedis.keys(MOVIE_BY_GENRE_KEY + "*");
        Map<String, Long> dist = new HashMap<>(keys.size());
        keys.forEach((key) -> {
            dist.put(key.replace(MOVIE_BY_GENRE_KEY, ""), jedis.scard(key));
        });
        return dist;
    }

    public Map<String, Object> findMovieById(String id) {
        String payload = this.jedis.hget(ALL_MOVIE_HASH_KEY, id);
        if (payload == null) {
            return null;
        }

        Map<String, Object> res = new HashMap<>();
        String totalRating = this.jedis.hget(RATING_BY_MOVIE_KEY, RatingDao.FIELD_TOTAL_PREFIX + id);
        String countRating = this.jedis.hget(RATING_BY_MOVIE_KEY, RatingDao.FIELD_CNT_PREFIX + id);

        Long rank = this.jedis.zrevrank(RatingDao.RATING_SCORE_KEY, id);
        Double score = this.jedis.zscore(RatingDao.RATING_SCORE_KEY, id);

        res.put("payload", payload);
        res.put("count", countRating);
        res.put("sum", totalRating);
        res.put("score", score);
        res.put("rank", rank);

        return res;
    }

    public List<String> findAllGenres() {
        //read all keys for genres
        //for each key, read the length of the set
        Set<String> keys = this.jedis.keys(MOVIE_BY_GENRE_KEY + "*");
        return keys.stream().map((key) -> {
            return key.replace(MOVIE_BY_GENRE_KEY, "");
        }).collect(Collectors.toList());
    }

    public List<String> findAllYears() {
        //read all keys for genres
        //for each key, read the length of the set
        Set<String> keys = this.jedis.keys(MOVIE_BY_YEAR_KEY + "*");
        return keys.stream().map((key) -> {
            return key.replace(MOVIE_BY_YEAR_KEY, "");
        }).collect(Collectors.toList());
    }

    private String getYear(String s) {
        Matcher m = yearPattern.matcher(s);
        if (m.find()) {
            return m.group(1);
        } else {
            return "";
        }
    }
}
