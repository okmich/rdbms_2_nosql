/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.okmich.mysql2mongodb.migrate;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import java.sql.ResultSet;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.bson.Document;

/**
 *
 * @author michael.enudi
 */
public class UsersMigration extends BaseMigration {

    private static final String QUERY = "SELECT u.id, u.age age_id, a.name age_group, gender, occupation_id, "
            + "o.name occupation, zip_code FROM users  u left join age_group a on u.age = a.id "
            + "left join occupations o on u.occupation_id = o.id";

    /**
     *
     * @param dbServerUrl
     * @param dbUser
     * @param dbPassword
     * @param mongoServerUrl
     * @param mongoDbName
     */
    public UsersMigration(String dbServerUrl, String dbUser, String dbPassword,
            String mongoServerUrl, String mongoDbName) {
        super(dbServerUrl, dbUser, dbPassword, mongoServerUrl, mongoDbName);
    }

    @Override
    protected Document rowToDocument(Object... row) {
        Document object = new Document();
        object.put("_id", row[0]);
        object.put("age_id", row[1]);
        object.put("age_group", row[2]);
        object.put("gender", row[3]);
        object.put("occupation_id", row[4]);
        object.put("occupation", row[5]);
        object.put("zip_code", row[6]);

        return object;
    }

    @Override
    public void migrate() {
        MongoDatabase mongoDB = getMongoDatabase(mongoDbUrl, mongoDbName);
        MongoCollection collection = mongoDB.getCollection("users");
        try (ResultSet rs = getConnection(jdbcServerUrl, jdbcUsername, jdbcPassword)
                .createStatement().executeQuery(QUERY);) {
            while (rs.next()) {
                collection.insertOne(rowToDocument(rs.getInt("id"),
                                rs.getInt("age_id"),
                                rs.getString("age_group"),
                                rs.getString("gender"),
                                rs.getInt("occupation_id"),
                                rs.getString("occupation"),
                                rs.getString("zip_code")));
            }
        } catch (Exception ex) {
            Logger.getLogger(BaseMigration.class.getName()).log(Level.SEVERE, null, ex);
            throw new RuntimeException(ex.getMessage(), ex);
        }
    }
}
