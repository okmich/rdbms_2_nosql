/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.okmich.movielens.hbase.migrate;

import static com.okmich.movielens.hbase.migrate.MoviesMigration.*;
import static com.okmich.movielens.hbase.migrate.UsersMigration.USER_COLUMN_FAMILY_QUALIFIER;
import static com.okmich.movielens.hbase.migrate.UsersMigration.USER_TABLE;
import java.io.IOException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;

/**
 *
 * @author michael.enudi
 */
public class DenomRatingsMigration extends BaseMigration {

    private static final String TABLE_NAME = "denom_rating";
    private static final Logger LOG = Logger.getLogger(DenomRatingsMigration.class.getName());
    //caching to reduce the number of calls to hbase
    //a production system to implement this in a key-value store
    private static final Map<Integer, Result> userCache = new HashMap<>();
    private static final Map<Integer, Result> movieCache = new HashMap<>();
    //simple date format
    private final SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy'T'hh:mm:ss.SSSSS");

    /**
     *
     *
     * @param fileToIngest
     * @param zkQuorum
     * @param clientPort
     */
    public DenomRatingsMigration(String fileToIngest,
            String zkQuorum, String clientPort) {
        super(zkQuorum, clientPort, fileToIngest);
    }

    @Override
    public void doEachLine(String line) throws IOException {
        String fields[] = line.split("::");
        Integer userId = Integer.valueOf(fields[0]);
        Integer movieId = Integer.valueOf(fields[1]);

        try (Table table = initHBase(zkQuorum, zkClientPort, TABLE_NAME);
                Table userTable = initHBase(zkQuorum, zkClientPort, USER_TABLE);
                Table movieTable = initHBase(zkQuorum, zkClientPort, MOVIE_TABLE);) {
            Put put = new Put(as(String.format("%s-%s", userId, movieId)));
            put.addColumn(as("r"), as("uid"), as(userId));
            put.addColumn(as("r"), as("mid"), as(movieId));
            put.addColumn(as("r"), as("rate"), as(Float.valueOf(fields[2])));
            Timestamp t = new Timestamp(Long.valueOf(fields[3]) * 1000);
            put.addColumn(as("r"), as("ts"), as(sdf.format(t)));

            Result movieRow = getMovie(movieId, movieTable);
            if (movieRow == null) {
                throw new IllegalArgumentException("Cannot find movie record with id " + movieId);
            }
            put.addColumn(as("r"), as("title"), movieRow.getValue(MOVIE_COLUMN_FAMILY_QUALIFIER, as("title")));
            put.addColumn(as("r"), as("ryear"), movieRow.getValue(MOVIE_COLUMN_FAMILY_QUALIFIER, as("ryear")));

            Result userRow = getUser(userId, userTable);
            if (userRow == null) {
                throw new IllegalArgumentException("Cannot find user record with id " + userId);
            }
            put.addColumn(as("r"), as("age"), userRow.getValue(USER_COLUMN_FAMILY_QUALIFIER, as("age")));
            put.addColumn(as("r"), as("occ"), userRow.getValue(USER_COLUMN_FAMILY_QUALIFIER, as("occ")));
            put.addColumn(as("r"), as("gndr"), userRow.getValue(USER_COLUMN_FAMILY_QUALIFIER, as("gndr")));

            //save to hbase table
            table.put(put);
        } catch (IOException ex) {
            Logger.getLogger(MoviesMigration.class.getName()).log(Level.SEVERE, null, ex);
            throw ex;
        }
    }

    @Override
    public void processBatch(List<String> lines) throws IOException {
        List<Put> puts = lines.stream().map((String line) -> {
            try (Table userTable = initHBase(zkQuorum, zkClientPort, USER_TABLE);
                    Table movieTable = initHBase(zkQuorum, zkClientPort, MOVIE_TABLE);) {
                String fields[] = line.split("::");
                Integer userId = Integer.valueOf(fields[0]);
                Integer movieId = Integer.valueOf(fields[1]);

                Put put = new Put(as(String.format("%s-%s", userId, movieId)));
                put.addColumn(as("a"), as("uid"), as(userId));
                put.addColumn(as("a"), as("mid"), as(movieId));
                put.addColumn(as("a"), as("rate"), as(Float.valueOf(fields[2])));
                Timestamp t = new Timestamp(Long.valueOf(fields[3]) * 1000);
                put.addColumn(as("a"), as("ts"), as(sdf.format(t)));

                Result movieRow = getMovie(movieId, movieTable);
                if (movieRow == null) {
                    throw new IllegalArgumentException("Cannot find movie record with id " + movieId);
                }
                put.addColumn(as("a"), as("title"), movieRow.getValue(MOVIE_COLUMN_FAMILY_QUALIFIER, as("title")));
                put.addColumn(as("a"), as("ryear"), movieRow.getValue(MOVIE_COLUMN_FAMILY_QUALIFIER, as("ryear")));

                Result userRow = getUser(userId, userTable);
                if (userRow == null) {
                    throw new IllegalArgumentException("Cannot find user record with id " + userId);
                }
                put.addColumn(as("a"), as("age"), userRow.getValue(USER_COLUMN_FAMILY_QUALIFIER, as("age")));
                put.addColumn(as("a"), as("occ"), userRow.getValue(USER_COLUMN_FAMILY_QUALIFIER, as("occ")));
                put.addColumn(as("a"), as("gndr"), userRow.getValue(USER_COLUMN_FAMILY_QUALIFIER, as("gndr")));

                return put;
            } catch (IOException ex) {
                Logger.getLogger(MoviesMigration.class.getName()).log(Level.SEVERE, null, ex);
                throw new RuntimeException(ex);
            }
        }).collect(Collectors.toList());

        try (Table table = initHBase(zkQuorum, zkClientPort, TABLE_NAME);) {
            //save to hbase table
            table.put(puts);
        } catch (IOException ex) {
            Logger.getLogger(MoviesMigration.class.getName()).log(Level.SEVERE, null, ex);
            throw new RuntimeException(ex);
        }
    }

    Result getUser(Integer uId, Table userTable) throws IOException {
        if (userCache.containsKey(uId)) {
            return userCache.get(uId);
        }
        LOG.log(Level.INFO, "Looking up User with Id {0}", uId);
        Result result = userTable.get(new Get(as(uId)));
        userCache.put(uId, result);
        return result;
    }

    Result getMovie(Integer mId, Table movieTable) throws IOException {
        if (movieCache.containsKey(mId)) {
            return movieCache.get(mId);
        }
        Result result = movieTable.get(new Get(as(mId)));
        if (result != null) {
            movieCache.put(mId, result);
        }
        return result;
    }

}
