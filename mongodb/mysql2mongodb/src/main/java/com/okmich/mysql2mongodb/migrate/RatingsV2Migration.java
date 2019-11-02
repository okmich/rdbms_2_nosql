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
public class RatingsV2Migration extends BaseMigration {

    private static final String MAIN_QUERY = "SELECT r.id, r.user_id, m.id as movie_id, m.title, m.release_year, "
            + "q.genres, ag.name age_group, o.name occupation , u.gender, r.rating, r.rated_at FROM ratings r "
            + "LEFT JOIN movies m on m.id = r.movie_id LEFT JOIN (select gm.movie_id, group_concat(g.name) genres from genres_movies gm "
            + "JOIN genres g on g.id = gm.genre_id group by gm.movie_id) q on q.movie_id = m.id LEFT JOIN users u on u.id = r.user_id "
            + "LEFT JOIN age_group ag on ag.id = u.age LEFT JOIN occupations o on o.id = u.occupation_id";

    /**
     *
     * @param dbServerUrl
     * @param dbUser
     * @param dbPassword
     * @param mongoServerUrl
     * @param mongoDbName
     */
    public RatingsV2Migration(String dbServerUrl, String dbUser, String dbPassword,
            String mongoServerUrl, String mongoDbName) {
        super(dbServerUrl, dbUser, dbPassword, mongoServerUrl, mongoDbName);
    }

    @Override
    protected Document rowToDocument(Object... row) {
        Document object = new Document();

        Document movieObj = new Document();
        movieObj.put("movie_id", row[1]);
        movieObj.put("title", row[4]);
        movieObj.put("release_year", row[3]);
        if (row[5] != null) {
            String[] genres = row[5].toString().split(",");
            List<BsonValue> genresValue = Arrays.asList(genres).stream().map((String s) -> {
                return new BsonString(s);
            }).collect(Collectors.toList());
            movieObj.put("genres", new BsonArray(genresValue));
        }
        object.put("movie", movieObj);

        Document userObj = new Document();
        userObj.put("user_id", row[0]);
        userObj.put("age_group", row[6]);
        userObj.put("occupation", row[7]);
        userObj.put("gender", row[8]);

        object.put("user", userObj);

        object.put("rating", row[2]);
        object.put("rated_at", row[9]);

        return object;
    }

    @Override
    public void migrate() {
        MongoDatabase mongoDB = getMongoDatabase(mongoDbUrl, mongoDbName);
        MongoCollection collection = mongoDB.getCollection("ratings_v2");
        try (ResultSet rs = getConnection(jdbcServerUrl, jdbcUsername, jdbcPassword)
                .createStatement().executeQuery(MAIN_QUERY);) {
            while (rs.next()) {
                collection.insertOne(rowToDocument(
                        rs.getInt("user_id"),
                        rs.getInt("movie_id"),
                        rs.getFloat("rating"),
                        rs.getInt("release_year"),
                        rs.getString("title"),
                        rs.getString("genres"),
                        rs.getString("age_group"),
                        rs.getString("occupation"),
                        rs.getString("gender"),
                        new Date(rs.getDate("rated_at").getTime())
                ));
            }
        } catch (Exception ex) {
            Logger.getLogger(RatingsV2Migration.class.getName()).log(Level.SEVERE, null, ex);
            throw new RuntimeException(ex.getMessage(), ex);
        }
    }
}
