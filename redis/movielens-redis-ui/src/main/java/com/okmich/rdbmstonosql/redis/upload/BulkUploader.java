/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.okmich.rdbmstonosql.redis.upload;

import com.okmich.rdbmstonosql.redis.dao.MovieDao;
import com.okmich.rdbmstonosql.redis.dao.RatingDao;
import com.okmich.rdbmstonosql.redis.dao.UserDao;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;
import redis.clients.jedis.Jedis;

/**
 *
 * @author Michael Enudi
 */
public class BulkUploader implements AutoCloseable {

    private final Function<String, Void> movieFunction,
            createRatingFunction,
            createUserFunction;

    private final Jedis jedis;

    private static final Logger LOG = Logger.getLogger("BulkUploader");

    public BulkUploader(String host, int port) {
        jedis = new Jedis(host, port);
        LOG.info("Redis connection established");

        movieFunction = new Function<String, Void>() {
            MovieDao movieDao = new MovieDao(jedis);

            @Override
            public Void apply(String payload) {
                movieDao.createMovie(payload);
                return null;
            }
        };

        createRatingFunction = new Function<String, Void>() {
            RatingDao ratingDao = new RatingDao(jedis);

            @Override
            public Void apply(String payload) {
                ratingDao.createRating(payload);
                return null;
            }
        };

        createUserFunction = new Function<String, Void>() {
            UserDao userDao = new UserDao(jedis);

            @Override
            public Void apply(String payload) {
                userDao.createUser(payload);
                return null;
            }
        };
    }

    void run(String entity, String fileName, boolean truncate) {
        FileReaderAndExec exec = new FileReaderAndExec(Paths.get(fileName), true);

        if (truncate) {
            LOG.log(Level.INFO, "Truncating all keys related to {0}", entity);
            Set<String> keys = jedis.keys(String.format("app.ml.%s.*", entity));
            for (String key : keys) {
                jedis.del(key);
            }
            LOG.log(Level.INFO, "Truncating completed");
        }

        try {
            switch (entity.toLowerCase()) {
                case "movie":
                    LOG.log(Level.INFO, "Importing records for movies");
                    exec.executeForEachRecord(movieFunction);
                    break;
                case "rating":
                    LOG.log(Level.INFO, "Importing records for rating");
                    exec.executeForEachRecord(createRatingFunction);
                    break;
                case "user":
                    LOG.log(Level.INFO, "Importing records for user");
                    exec.executeForEachRecord(createUserFunction);
                    break;
                default:
                throw new IllegalArgumentException("Unknown entity type");
            }

        } catch (IOException ex) {
            Logger.getLogger(BulkUploader.class.getName()).log(Level.SEVERE, null, ex);
            throw new RuntimeException(ex.getMessage());
        }
    }

    public static void main(String[] args) throws IOException {
        Map<String, String> params = getArguments(args);

        if (params.isEmpty()) {
            System.err.println("\nArgument usage: \n\t-file=file_to_upload\n\t-host=[redis_host]\n\t-port=[redis_port]\n\t-type=[movielens file type]\n\t-truncate=[true|false] - should the related redis keys be removed before import");
            System.exit(0);
        }

        String fileToUpload = params.get("file");
        String redisHost = params.getOrDefault("host", "localhost");
        int redisPort = Integer.parseInt(params.getOrDefault("port", "6379"));
        String entityType = params.getOrDefault("type", "");
        boolean truncateExisting = Boolean.parseBoolean(params.getOrDefault("truncate", "false"));

        try (BulkUploader uploader = new BulkUploader(redisHost, redisPort);) {
            LOG.log(Level.INFO, "Import starting .....");
            uploader.run(entityType, fileToUpload, truncateExisting);

            LOG.log(Level.INFO, "Import Done. .....");
        } catch (Exception e) {
            LOG.log(Level.SEVERE, e.getMessage(), e);
        }

    }

    private static Map<String, String> getArguments(String[] args) {
        Map<String, String> params = new HashMap<>(args.length);
        for (String arg : args) {
            String[] parts = arg.split("=");
            params.put(parts[0].toLowerCase().substring(1), parts[1]);
        }
        return params;
    }

    @Override
    public void close() throws Exception {
        if (jedis != null) {
            jedis.close();
        }
    }
}
