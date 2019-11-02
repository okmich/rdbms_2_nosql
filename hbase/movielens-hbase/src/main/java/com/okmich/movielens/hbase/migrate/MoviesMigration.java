/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.okmich.movielens.hbase.migrate;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;

/**
 *
 * @author michael.enudi
 */
public class MoviesMigration extends BaseMigration {

    public static final String MOVIE_TABLE = "movie";
    public static final String ALT_MOVIE_TABLE = "altmovie";
    public static final byte[] MOVIE_COLUMN_FAMILY_QUALIFIER = as("a");
    public static final byte[] ALT_MOVIE_COLUMN_FAMILY_QUALIFIER = as("gm");

    private final Pattern yearPattern = Pattern.compile("^.+ \\((\\d{4})\\)$");

    /**
     *
     *
     * @param fileToIngest
     * @param zkQuorum
     * @param clientPort
     */
    public MoviesMigration(String fileToIngest,
            String zkQuorum, String clientPort) {
        super(zkQuorum, clientPort, fileToIngest);
    }

    @Override
    public void doEachLine(String line) {
        String fields[] = line.split("::");
        Integer id = Integer.valueOf(fields[0]);
        try (Table table = initHBase(zkQuorum, this.zkClientPort, MOVIE_TABLE);
                Table altTable = initHBase(zkQuorum, this.zkClientPort, ALT_MOVIE_TABLE);) {
            Put put = getPutForMovie(line);
            //save to hbase table
            table.put(put);

            //save into the second table
            String releaseYear = getReleaseYear(fields[1]);
            altTable.put(rowToGenreMovies(String.valueOf(id), fields[1], releaseYear, fields[2]));
        } catch (IOException ex) {
            Logger.getLogger(MoviesMigration.class.getName()).log(Level.SEVERE, null, ex);
            throw new RuntimeException(ex);
        }
    }

    @Override
    public void processBatch(List<String> lines) throws IOException {

        List<Put> puts = lines.stream().map((String line) -> {
            String fields[] = line.split("::");

            Integer id = Integer.valueOf(fields[0]);
            return getPutForMovie(line);
        }).collect(Collectors.toList());

        List<Put> altPuts = lines.stream().flatMap((String line) -> {
            String fields[] = line.split("::");

            Integer id = Integer.valueOf(fields[0]);//save into the second table
            String releaseYear = getReleaseYear(fields[1]);
            return rowToGenreMovies(String.valueOf(id), fields[1], releaseYear, fields[2]).stream();
        }).collect(Collectors.toList());

        try (Table table = initHBase(zkQuorum, this.zkClientPort, MOVIE_TABLE);
                Table altTable = initHBase(zkQuorum, this.zkClientPort, ALT_MOVIE_TABLE);) {

            //save to hbase table
            table.put(puts);
            altTable.put(altPuts);
        } catch (IOException ex) {
            Logger.getLogger(MoviesMigration.class.getName()).log(Level.SEVERE, null, ex);
            throw new RuntimeException(ex);
        }
    }

    private void addGenresToPut(Put put, String genres) {
        List<String> genreList = Arrays.asList(genres.split(","));

        setIfGenreApplies(genreList, "Action", put, "actn");
        setIfGenreApplies(genreList, "Adventure", put, "advnt");
        setIfGenreApplies(genreList, "Animation", put, "anmtn");
        setIfGenreApplies(genreList, "Children's", put, "chldrn");
        setIfGenreApplies(genreList, "Comedy", put, "comdy");
        setIfGenreApplies(genreList, "Crime", put, "crime");
        setIfGenreApplies(genreList, "Documentary", put, "dcmtry");
        setIfGenreApplies(genreList, "Drama", put, "drama");
        setIfGenreApplies(genreList, "Fantasy", put, "fntsy");
        setIfGenreApplies(genreList, "Film-Noir", put, "film");
        setIfGenreApplies(genreList, "Horror", put, "horro");
        setIfGenreApplies(genreList, "Musical", put, "music");
        setIfGenreApplies(genreList, "Mystery", put, "mysty");
        setIfGenreApplies(genreList, "Romance", put, "rmance");
        setIfGenreApplies(genreList, "Sci-Fi", put, "scifi");
        setIfGenreApplies(genreList, "Thriller", put, "thrllr");
        setIfGenreApplies(genreList, "War", put, "war");
        setIfGenreApplies(genreList, "Western", put, "wstrn");
    }

    private void setIfGenreApplies(List<String> genreList, String genre, Put put, String colName) {
        if (genreList.contains(genre)) {
            put.addColumn(MOVIE_COLUMN_FAMILY_QUALIFIER, as(colName), as(true));
        }
    }

    private List<Put> rowToGenreMovies(String id, String title, String releaseYear, String genres) {
        List<Put> putList = new ArrayList();
        Put put;
        for (String g : genres.split("\\|")) {
            put = new Put(as(new StringBuilder(g).append(":").append(id).toString()));
            put.addColumn(ALT_MOVIE_COLUMN_FAMILY_QUALIFIER, as("mid"), as(Integer.parseInt(id)));
            put.addColumn(ALT_MOVIE_COLUMN_FAMILY_QUALIFIER, as("title"), as(title));
            put.addColumn(ALT_MOVIE_COLUMN_FAMILY_QUALIFIER, as("genre"), as(g));
            put.addColumn(ALT_MOVIE_COLUMN_FAMILY_QUALIFIER, as("ryear"), as(releaseYear));
            putList.add(put);
        }
        return putList;
    }

    private String getReleaseYear(String field) {
        Matcher m = yearPattern.matcher(field);
        if (m.find()) {
            return m.group(1);
        } else {
            return "";
        }
    }

    private Put getPutForMovie(String record) {
        String fields[] = record.split("::");
        Integer id = Integer.valueOf(fields[0]);

        Put put = new Put(as(id));
        String releaseYear = getReleaseYear(fields[1]);
        put.addColumn(MOVIE_COLUMN_FAMILY_QUALIFIER, as("title"), as(fields[1]));
        put.addColumn(MOVIE_COLUMN_FAMILY_QUALIFIER, as("ryear"), as(releaseYear));
        addGenresToPut(put, (String) fields[2]);

        return put;
    }
}
