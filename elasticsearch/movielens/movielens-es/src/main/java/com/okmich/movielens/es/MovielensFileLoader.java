/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.okmich.movielens.es;

import com.okmich.movielens.es.db.MovieReader;
import com.okmich.movielens.es.db.RatingReader;
import com.okmich.movielens.es.db.TagReader;
import com.okmich.movielens.es.db.UserReader;
import com.okmich.movielens.es.model.Movie;
import com.okmich.movielens.es.model.Rating;
import com.okmich.movielens.es.model.Tag;
import com.okmich.movielens.es.model.User;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 *
 * @author michael.enudi
 */
public class MovielensFileLoader implements AutoCloseable {

    private static final Logger LOG = Logger.getLogger(MovielensFileLoader.class.getName());

    private Map<Long, Movie> movieMap = null;
    private final ESHighLevelRestClient highLevelRestClient;

    public MovielensFileLoader() {
        this.highLevelRestClient = new ESHighLevelRestClient();
    }

    public void loadMovies(Path movieFile) {
        LOG.info("Preparing to load movies ....");
        MovieReader movieReader = new MovieReader(movieFile, true);
        LOG.info("Loading movies ....\n");
        try {
            this.movieMap = movieReader.readMovieMap();
        } catch (IOException ex) {
            throw new RuntimeException(ex.getMessage(), ex);
        }

        List<Map<String, Object>> fullRecords = this.movieMap
                .values()
                .stream()
                .map((Movie t) -> {
                    return t.toMap();
                })
                .collect(Collectors.toList());

        try {
            this.highLevelRestClient.performBulkIndexRequest(
                    "ml-movies", "movie", "movieId", fullRecords);
        } catch (Exception ex) {
            throw new RuntimeException(ex.getMessage(), ex);
        }
    }

    public void loadRatings(Path ratingFile, int batchSize) throws IOException {
        LOG.info("Preparing to load ratings ....");
        RatingReader ratingReader = new RatingReader(ratingFile, true, batchSize);
        LOG.info("Loading ratings ....\n");
        List<Rating> ratings = ratingReader.read();
        do {
            List<Map<String, Object>> ratingData = ratings
                    .stream()
                    .map((Rating r) -> {
                        r.joinMovie(this.movieMap.get(r.getMovieId()));
                        return r.toMap();
                    })
                    .collect(Collectors.toList());

            this.highLevelRestClient.performBulkIndexRequest(
                    "ml-ratings", "rating", "", ratingData);
            //read more records
            ratings = ratingReader.read();
        } while (!ratings.isEmpty());
    }

    public void loadTags(Path tagFile, int batchSize) throws IOException {
        LOG.info("Preparing to load tags ....");
        TagReader tagReader = new TagReader(tagFile, true, batchSize);
        LOG.info("Loading tags ....\n");
        List<Tag> tags = tagReader.read();
        do {
            List<Map<String, Object>> tagData = tags
                    .stream()
                    .map((Tag t) -> {
                        t.joinMovie(this.movieMap.get(t.getMovieId()));
                        return t.toMap();
                    })
                    .collect(Collectors.toList());

            this.highLevelRestClient.performBulkIndexRequest(
                    "ml-tags", "tag", "", tagData);

            //read more records
            tags = tagReader.read();
        } while (!tags.isEmpty());
    }

    public void loadUsers(Path userFile, int batchSize) throws IOException {
        LOG.info("Preparing to load users ....");
        UserReader userReader = new UserReader(userFile, true, batchSize);
        LOG.info("Loading users ....\n");
        List<User> users = userReader.read();

        List<Map<String, Object>> userData = users
                .stream()
                .map((User t) -> {
                    return t.toMap();
                })
                .collect(Collectors.toList());

        this.highLevelRestClient.performBulkIndexRequest(
                "ml-users", "user", "", userData);
    }

    @Override
    public void close() throws Exception {
        this.movieMap = null;
        if (highLevelRestClient != null) {
            this.highLevelRestClient.close();
        }
    }
}
