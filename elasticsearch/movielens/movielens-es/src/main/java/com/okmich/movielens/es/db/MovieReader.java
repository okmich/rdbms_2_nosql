/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.okmich.movielens.es.db;

import com.okmich.movielens.es.model.Movie;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 * @author michael.enudi
 */
public class MovieReader extends BaseReader<Movie> {

    private static final String REGEX = "^.+ \\((\\d{4})\\)$";
    private final Pattern yearPattern;

    public MovieReader(Path file, boolean header) {
        super(file, header);
        this.yearPattern = Pattern.compile(REGEX);
    }

    public List<Movie> read() throws IOException {
        List<String[]> payload = super.readFile();
        List<Movie> movies = new ArrayList<>(payload.size());
        String year = "";
        for (String[] rec : payload) {
            year = getYear(rec[1]);
            movies.add(new Movie(Long.parseLong(rec[0]),
                    rec[1].replace("(" + year + ")", ""), year, getGenres(rec[2])));
        }
        return movies;
    }

    public Map<Long, Movie> readMovieMap() throws IOException {
        List<String[]> payload = super.readFile();
        Map<Long, Movie> movies = new HashMap<>(payload.size());
        String year = "";
        Long movieId = null;
        for (String[] rec : payload) {
            year = getYear(rec[1]);
            movieId = Long.parseLong(rec[0]);
            movies.put(movieId, new Movie(movieId,
                    rec[1].replace("(" + year + ")", ""), year, getGenres(rec[2])));
        }
        return movies;
    }

    private String getYear(String s) {
        Matcher m = yearPattern.matcher(s);
        if (m.find()) {
            return m.group(1);
        } else {
            return "";
        }
    }

    private String[] getGenres(String genres) {
        if (genres == null || genres.isEmpty()) {
            return new String[0];
        } else {
            return genres.split("\\|");
        }
    }

}
