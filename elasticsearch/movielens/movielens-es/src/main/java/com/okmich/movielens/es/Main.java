/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.okmich.movielens.es;

import java.nio.file.Paths;

/**
 * @author michael.enudi
 */
public class Main {

    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Please specify the location of all csv files");
            System.exit(-1);
        }
        String folder = args[0];

        try (MovielensFileLoader movielensFileLoader = new MovielensFileLoader();) {

            movielensFileLoader.loadUsers(Paths.get(folder, "users.csv"), 10000);

            movielensFileLoader.loadMovies(Paths.get(folder, "movies.csv"));
            
            movielensFileLoader.loadTags(Paths.get(folder, "tags.csv"), 20000);

            movielensFileLoader.loadRatings(Paths.get(folder, "ratings.csv"), 20000);


        } catch (Exception ex) {
            ex.printStackTrace();
        }

    }

}
