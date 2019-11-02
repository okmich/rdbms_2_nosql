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
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.bson.Document;

/**
 *
 * @author michael.enudi
 */
public class RatingsMigration extends BaseMigration {

    private static final String MAIN_QUERY = "SELECT r.id, r.user_id, m.id as movie_id, m.title, m.release_year, r.rating, r.rated_at FROM ratings r "
            + "LEFT JOIN movies m on m.id = r.movie_id LEFT JOIN users u on u.id = r.user_id";

    /**
     *
     * @param dbServerUrl
     * @param dbUser
     * @param dbPassword
     * @param mongoServerUrl
     * @param mongoDbName
     */
    public RatingsMigration(String dbServerUrl, String dbUser, String dbPassword,
            String mongoServerUrl, String mongoDbName) {
        super(dbServerUrl, dbUser, dbPassword, mongoServerUrl, mongoDbName);
    }

    @Override
    protected Document rowToDocument(Object... row) {
        Document object = new Document();
        object.put("user_id", row[1]);
        object.put("movie_id", row[2]);
        object.put("rating", row[3]);
        object.put("rated_at", row[4]);

        return object;
    }

    @Override
    public void migrate() {
        MongoDatabase mongoDB = getMongoDatabase(mongoDbUrl, mongoDbName);
        MongoCollection collection = mongoDB.getCollection("ratings");
        try (ResultSet rs = getConnection(jdbcServerUrl, jdbcUsername, jdbcPassword)
                .createStatement().executeQuery(MAIN_QUERY);) {
            while (rs.next()) {
                collection.insertOne(rowToDocument(rs.getInt("id"),
                        rs.getInt("user_id"),
                        rs.getInt("movie_id"),
                        rs.getFloat("rating"),
                        new Date(rs.getDate("rated_at").getTime())
                ));
            }
        } catch (Exception ex) {
            Logger.getLogger(RatingsMigration.class.getName()).log(Level.SEVERE, null, ex);
            throw new RuntimeException(ex.getMessage(), ex);
        }
    }
}
