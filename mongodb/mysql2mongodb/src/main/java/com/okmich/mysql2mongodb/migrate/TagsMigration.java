/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.okmich.mysql2mongodb.migrate;

import com.mongodb.DBRef;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import java.sql.ResultSet;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import org.bson.BsonArray;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.Document;

/**
 *
 * @author michael.enudi
 */
public class TagsMigration extends BaseMigration {

    private static final String MAIN_QUERY = "SELECT t.id, t.user_id, m.id as movie_id, m.title, "
            + "m.release_year, q.genres, t.tags, t.created_at FROM tags t "
            + "LEFT JOIN movies m on m.id = t.movie_id "
            + "LEFT JOIN users u on u.id = t.user_id "
            + "LEFT JOIN (select gm.movie_id, group_concat(g.name) genres from genres_movies gm "
            + "JOIN genres g on g.id = gm.genre_id group by gm.movie_id) q on q.movie_id = t.movie_id";

    /**
     *
     * @param dbServerUrl
     * @param dbUser
     * @param dbPassword
     * @param mongoServerUrl
     * @param mongoDbName
     */
    public TagsMigration(String dbServerUrl, String dbUser, String dbPassword,
            String mongoServerUrl, String mongoDbName) {
        super(dbServerUrl, dbUser, dbPassword, mongoServerUrl, mongoDbName);
    }

    @Override
    protected Document rowToDocument(Object... row) {
        Document object = new Document();
        object.put("user_id", new DBRef("users", row[1]));

        Document movieObj = new Document();
        movieObj.put("movie_id", row[2]);
        movieObj.put("title", row[3]);
        movieObj.put("release_year", row[4]);
        if (row[5] != null) {
            String[] genres = row[5].toString().split(",");
            List<BsonValue> genresValue = Arrays.asList(genres).stream().map((String s) -> {
                return new BsonString(s);
            }).collect(Collectors.toList());
            movieObj.put("genres", new BsonArray(genresValue));
        }
        object.put("movie", movieObj);
        object.put("tag", row[6]);
        object.put("created_at", row[7]);

        return object;
    }

    @Override
    public void migrate() {
        MongoDatabase mongoDB = getMongoDatabase(mongoDbUrl, mongoDbName);
        MongoCollection collection = mongoDB.getCollection("tags");
        try (ResultSet rs = getConnection(jdbcServerUrl, jdbcUsername, jdbcPassword)
                .createStatement().executeQuery(MAIN_QUERY);) {
            while (rs.next()) {
                collection.insertOne(rowToDocument(rs.getInt("id"),
                        rs.getInt("user_id"),
                        rs.getInt("movie_id"),
                        rs.getString("title"),
                        rs.getInt("release_year"),
                        rs.getString("genres"),
                        rs.getString("tags"),
                        new Date(rs.getDate("created_at").getTime())
                ));
            }
        } catch (Exception ex) {
            Logger.getLogger(TagsMigration.class.getName()).log(Level.SEVERE, null, ex);
            throw new RuntimeException(ex.getMessage(), ex);
        }
    }
}
