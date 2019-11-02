/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.okmich.mysql2mongodb;

import com.okmich.mysql2mongodb.migrate.BaseMigration;
import com.okmich.mysql2mongodb.migrate.MoviesMigration;
import com.okmich.mysql2mongodb.migrate.RatingsMigration;
import com.okmich.mysql2mongodb.migrate.RatingsV2Migration;
import com.okmich.mysql2mongodb.migrate.TagsMigration;
import com.okmich.mysql2mongodb.migrate.UsersMigration;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author michael.enudi
 */
public class Main {

    public static void main(String[] args) {
        Map<String, String> params = getArguments(args);
        String entity = params.get("tblname");
        String jdbcUrl = params.getOrDefault("mysqlurl", "jdbc:mysql://localhost:3306/movielens");
        String jdbcUsername = params.getOrDefault("mysqluser", "root");
        String jdbcPassword = params.getOrDefault("mysqlpassword", "password");
        String mongoUrl = params.getOrDefault("mongourl", "mongodb://localhost:27017/movielens");
        String mongoDBName = params.getOrDefault("mongodb", "movielens");

        if (entity == null || entity.isEmpty()) {
            System.err.println("Specify at least the table you wish to migrate ");
            System.err.println("Usage: Main -tblname=[users|movies|tags|ratings|ratings2] -mysqluser=[username] "
                    + "-mysqlpassword=[password] -mysqlurl=[jdbc-url] -mongourl=[mongourl] -mongodb=[mongodb]");
            System.exit(-1);
        }

        BaseMigration migrationUtil;
        switch (entity.toLowerCase()) {
            case "users":
                migrationUtil = new UsersMigration(jdbcUrl, jdbcUsername, jdbcPassword, mongoUrl, mongoDBName);
                break;
            case "movies":
                migrationUtil = new MoviesMigration(jdbcUrl, jdbcUsername, jdbcPassword, mongoUrl, mongoDBName);
                break;
            case "tags":
                migrationUtil = new TagsMigration(jdbcUrl, jdbcUsername, jdbcPassword, mongoUrl, mongoDBName);
                break;
            case "ratings":
                migrationUtil = new RatingsMigration(jdbcUrl, jdbcUsername, jdbcPassword, mongoUrl, mongoDBName);
                break;
            case "ratings2":
                migrationUtil = new RatingsV2Migration(jdbcUrl, jdbcUsername, jdbcPassword, mongoUrl, mongoDBName);
                break;
            default:
                throw new IllegalArgumentException("Unknown entity");
        }

        migrationUtil.migrate();

        System.out.println(">>>>>>>>>>>>> Done");
    }

    private static Map<String, String> getArguments(String[] args) {
        Map<String, String> params = new HashMap<>(args.length);
        for (String arg : args) {
            String[] parts = arg.split("=");
            params.put(parts[0].toLowerCase().substring(1), parts[1]);
        }
        return params;
    }
}
