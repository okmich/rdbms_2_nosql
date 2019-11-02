/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.okmich.mysql2mongodb.migrate;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import java.sql.ResultSet;
import java.util.Arrays;
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
public class MoviesMigration extends BaseMigration {

    /**
     * MAIN_QUERY
     */
    private static final String MAIN_QUERY = "select m.id, title, release_year, group_concat(g.name) genres from movies m  "
            + "left join genres_movies gm on gm.movie_id = m.id left "
            + "join genres g on g.id = gm.genre_id group by m.id, title, release_year";

    /**
     *
     * @param dbServerUrl
     * @param dbUser
     * @param dbPassword
     * @param mongoServerUrl
     * @param mongoDbName
     */
    public MoviesMigration(String dbServerUrl, String dbUser, String dbPassword,
            String mongoServerUrl, String mongoDbName) {
        super(dbServerUrl, dbUser, dbPassword, mongoServerUrl, mongoDbName);
    }

    @Override
    protected Document rowToDocument(Object... row) {
        Document object = new Document();
        object.put("_id", row[0]); //mongodb will create if dont
        object.put("title", row[1]);
        object.put("release_year", row[2]);
        if (row[3] != null) {
            String[] genres = row[3].toString().split(",");
            List<BsonValue> genresValue = Arrays.asList(genres).stream().map((String s) -> {
                return new BsonString(s);
            }).collect(Collectors.toList());
            object.put("genres", new BsonArray(genresValue));
        }
        return object;
    }

    @Override
    public void migrate() {
        MongoDatabase mongoDB = getMongoDatabase(mongoDbUrl, mongoDbName);
        MongoCollection collection = mongoDB.getCollection("movies");
        try (ResultSet rs = getConnection(jdbcServerUrl, jdbcUsername, jdbcPassword)
                .createStatement().executeQuery(MAIN_QUERY);) {
            while (rs.next()) {
                collection.insertOne(rowToDocument(rs.getInt("id"),
                        rs.getString("title"),
                        rs.getInt("release_year"),
                        rs.getString("genres")
                ));
            }
        } catch (Exception ex) {
            Logger.getLogger(MoviesMigration.class.getName()).log(Level.SEVERE, null, ex);
            throw new RuntimeException(ex.getMessage(), ex);
        }
    }
}
