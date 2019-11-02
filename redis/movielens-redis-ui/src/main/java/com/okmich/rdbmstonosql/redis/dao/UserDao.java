/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.okmich.rdbmstonosql.redis.dao;

import com.okmich.rdbmstonosql.redis.Constants;
import static com.okmich.rdbmstonosql.redis.Constants.BASE_PREFIX;
import static com.okmich.rdbmstonosql.redis.Constants.USER_PREFIX;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;
import redis.clients.jedis.Transaction;

/**
 *
 * @author Michael Enudi
 */
public class UserDao implements Constants {

    private final Jedis jedis;

    private static final Logger LOG = Logger.getLogger("UserDao");
    public static final String KEY_PREFIX = BASE_PREFIX + USER_PREFIX;

    public static final String ALL_USER_KEY = KEY_PREFIX + "all#";
    public static final String USERS_BY_AGE_KEY = KEY_PREFIX + "byage.";
    public static final String USERS_BY_GENDER_KEY = KEY_PREFIX + "bysex.";

    public UserDao(Jedis jedis) {
        this.jedis = jedis;
    }

    public void createUser(String payload) {
        //save to user list
        //save to user_age set
        //save to user_gender set

        Transaction transaction = jedis.multi();
        String parts[] = payload.split(FIELD_DELIMITER);
        String userId = parts[0];
        String ageGroup = parts[1];
        String gender = parts[2];

        try {
            transaction.hset(ALL_USER_KEY, userId, payload);
            transaction.sadd(USERS_BY_AGE_KEY + ageGroup.trim(), userId);
            transaction.sadd(USERS_BY_GENDER_KEY + gender.trim(), userId);
            
            transaction.publish(USER_CHANNEL, "xxxxxxxx");
            transaction.exec();
        } catch (Exception e) {
            transaction.discard();
            LOG.log(Level.SEVERE, e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    public void updateUser() {
        //remove from list and write back to list head
        //remove from user_age and replace
        //replace in user_occupation
    }

    public String findUserById(String id) {
        //return user at index from user_list
        return this.jedis.hget(ALL_USER_KEY, id);
    }

    public Long getUserCount() {
        //return number of all users
        return this.jedis.hlen(ALL_USER_KEY);
    }

    public SearchResult<String> findUsersByAgeGroup(String ageGroup, String cursor, int fetchSize, boolean isCursorFetch) {
        //return a cursor of user age set 
        ScanParams scanParam = new ScanParams().count(fetchSize);
        String iCursor = "0";
        if (isCursorFetch) {
            iCursor = cursor;
        }
        ScanResult<String> scanResult = jedis.sscan(USERS_BY_AGE_KEY + ageGroup, iCursor, scanParam);
        List<String> fllRs = scanResult.getResult().stream().map((key) -> {
            return jedis.hget(ALL_USER_KEY, key);
        }).collect(Collectors.toList());

        return new SearchResult(scanResult.getCursor(), fllRs, scanResult.isCompleteIteration());
    }

    public Long countUsersByAgeGroup(String ageGroup) {
        return jedis.scard(USERS_BY_AGE_KEY + ageGroup);
    }

    public SearchResult<String> findUsersByGender(String gender, String cursor, int fetchSize, boolean isCursorFetch) {
        //return a cursor of user gender 
        ScanParams scanParam = new ScanParams().count(fetchSize);
        String iCursor = "0";
        if (isCursorFetch) {
            iCursor = cursor;
        }

        ScanResult<String> scanResult = jedis.sscan(USERS_BY_GENDER_KEY + gender, iCursor, scanParam);
        List<String> fllRs = scanResult.getResult().stream().map((key) -> {
            return jedis.hget(ALL_USER_KEY, key);
        }).collect(Collectors.toList());

        return new SearchResult(scanResult.getCursor(), fllRs, scanResult.isCompleteIteration());
    }

    public Long countUsersByGender(String gender) {
        return jedis.scard(USERS_BY_GENDER_KEY + gender);
    }

    public List<String> getAllAgeGroups() {
        Set<String> keys = this.jedis.keys(USERS_BY_AGE_KEY + "*");
        return keys.stream().map((key) -> {
            return key.replace(USERS_BY_AGE_KEY, "");
        }).collect(Collectors.toList());
    }

    public List<String> getAllGenders() {
        Set<String> keys = this.jedis.keys(USERS_BY_GENDER_KEY + "*");
        return keys.stream().map((key) -> {
            return key.replace(USERS_BY_GENDER_KEY, "");
        }).collect(Collectors.toList());
    }

}
