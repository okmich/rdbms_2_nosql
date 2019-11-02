/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.okmich.movielens.hbase.migrate;

import static com.okmich.movielens.hbase.migrate.MoviesMigration.*;
import java.io.IOException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;

/**
 *
 * @author michael.enudi
 */
public class RatingsMigration extends BaseMigration {

    private static final String TABLE_NAME = "rating";
    private static final Logger LOG = Logger.getLogger(MoviesMigration.class.getName());
    //simple date format
    private final SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy'T'hh:mm:ss.SSSSS");

    /**
     *
     *
     * @param fileToIngest
     * @param zkQuorum
     * @param clientPort
     */
    public RatingsMigration(String fileToIngest,
            String zkQuorum, String clientPort) {
        super(zkQuorum, clientPort, fileToIngest);
    }

    @Override
    public void doEachLine(String line) throws IOException {
        String fields[] = line.split("::");
        Integer userId = Integer.valueOf(fields[0]);
        Integer movieId = Integer.valueOf(fields[1]);

        try (Table table = initHBase(zkQuorum, zkClientPort, TABLE_NAME);) {
            Put put = new Put(as(String.format("%s-%s", userId, movieId)));
            put.addColumn(as("r"), as("uid"), as(userId));
            put.addColumn(as("r"), as("mid"), as(movieId));
            put.addColumn(as("r"), as("rate"), as(Float.valueOf(fields[2])));
            Timestamp t = new Timestamp(Long.valueOf(fields[3]) * 1000);
            put.addColumn(as("r"), as("ts"), as(sdf.format(t)));

            //save to hbase table
            table.put(put);
        } catch (IOException ex) {
            Logger.getLogger(MoviesMigration.class.getName()).log(Level.SEVERE, null, ex);
            throw ex;
        }
    }

    @Override
    public void processBatch(List<String> lines) throws IOException {
        List<Put> puts = lines.stream().map((String line) -> {
            String fields[] = line.split("::");
            Integer userId = Integer.valueOf(fields[0]);
            Integer movieId = Integer.valueOf(fields[1]);

            Put put = new Put(as(String.format("%s-%s", userId, movieId)));
            put.addColumn(as("a"), as("uid"), as(userId));
            put.addColumn(as("a"), as("mid"), as(movieId));
            put.addColumn(as("a"), as("rate"), as(Float.valueOf(fields[2])));
            Timestamp t = new Timestamp(Long.valueOf(fields[3]) * 1000);
            put.addColumn(as("a"), as("ts"), as(sdf.format(t)));

            return put;
        }).collect(Collectors.toList());

        try (Table table = initHBase(zkQuorum, zkClientPort, TABLE_NAME);) {
            //save to hbase table
            table.put(puts);
        } catch (IOException ex) {
            LOG.log(Level.SEVERE, null, ex);
            throw new RuntimeException(ex);
        }
    }

}
