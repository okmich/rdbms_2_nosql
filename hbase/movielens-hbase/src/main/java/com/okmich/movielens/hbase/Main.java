/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.okmich.movielens.hbase;

import com.okmich.movielens.hbase.migrate.BaseMigration;
import com.okmich.movielens.hbase.migrate.MoviesMigration;
import com.okmich.movielens.hbase.migrate.DenomRatingsMigration;
import com.okmich.movielens.hbase.migrate.RatingsMigration;
import com.okmich.movielens.hbase.migrate.UsersMigration;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author michael.enudi
 */
public class Main {

    public static void main(String[] args) {
        Map<String, String> params = getArguments(args);
        String entity = params.getOrDefault("tblname", "movies");
        String fileToIngest = params.get("file");
        String zkQuorum = params.getOrDefault("zkhost", "sandbox-hdp.hortonworks.com");
        String zkClientPort = params.getOrDefault("zkport", "2181");

        if (params.isEmpty()) {
            System.err.println("Specify at least the table you wish to migrate ");
            System.err.println("Usage: Main -tblname=[users|movies|ratings|denom_ratings|find_genre_movies -file=[file-path]"
                    + "-zkhost=[Zookeeper quorum] -zkport=[Zookeeper client port]");
            System.exit(-1);
        }

        BaseMigration migrationUtil;
        switch (entity.toLowerCase()) {
            case "users":
                new UsersMigration(fileToIngest, zkQuorum, zkClientPort).migrate(1000);
                break;
            case "movies":
                new MoviesMigration(fileToIngest, zkQuorum, zkClientPort).migrate(1000);
                break;
            case "ratings":
                new RatingsMigration(fileToIngest, zkQuorum, zkClientPort).migrate(1000);
                break;
            case "denom_ratings":
                new DenomRatingsMigration(fileToIngest, zkQuorum, zkClientPort).migrate();
                break;
            case "find_genre_movies":
                new GenreMovieFinder(zkQuorum, zkClientPort).find();
                break;
            default:
                throw new IllegalArgumentException("Unknown entity");
        }
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
