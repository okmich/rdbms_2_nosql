/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.okmich.movielens.hbase;

import com.okmich.movielens.hbase.migrate.MoviesMigration;
import static com.okmich.movielens.hbase.migrate.MoviesMigration.ALT_MOVIE_TABLE;
import java.io.IOException;
import java.util.Scanner;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;
import static org.apache.hadoop.hbase.util.Bytes.toBytes;

/**
 *
 * @author michael.enudi
 */
public class GenreMovieFinder {

    private static final Logger LOG = Logger.getLogger(GenreMovieFinder.class.getName());
    private Table genreMovieTable;

    public GenreMovieFinder(String zkQuorum, String clientPort) {
        try {
            Configuration conf = HBaseConfiguration.create();
            conf.set(HConstants.ZOOKEEPER_QUORUM, zkQuorum);
            conf.set(HConstants.ZOOKEEPER_CLIENT_PORT, clientPort);
            conf.set(HConstants.ZOOKEEPER_ZNODE_PARENT, "/hbase-unsecure");
            org.apache.hadoop.hbase.client.Connection hbaseConnection
                    = ConnectionFactory.createConnection(conf);
            this.genreMovieTable = hbaseConnection.getTable(TableName.valueOf(ALT_MOVIE_TABLE));
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    public void find() {
        Scanner scanner = new Scanner(System.in);
        System.out.print("We will return all movies for a specified genre: ");
        String genre = scanner.nextLine();
        LOG.log(Level.INFO, "The genre specified is : {0}", genre);
        Scan scan = new Scan();
        scan.setFilter(new PrefixFilter(Bytes.toBytes(genre + ":")));
        long count = 0l;

        try (ResultScanner resultScanner = this.genreMovieTable.getScanner(scan);) {
            Result result;
            while ((result = resultScanner.next()) == null) {
                byte[] mIdBytes = result.getValue(MoviesMigration.ALT_MOVIE_COLUMN_FAMILY_QUALIFIER, toBytes("mid"));
                byte[] titleBytes = result.getValue(MoviesMigration.ALT_MOVIE_COLUMN_FAMILY_QUALIFIER, toBytes("title"));
                byte[] yearBytes = result.getValue(MoviesMigration.ALT_MOVIE_COLUMN_FAMILY_QUALIFIER, toBytes("ryear"));
                byte[] genreBytes = result.getValue(MoviesMigration.ALT_MOVIE_COLUMN_FAMILY_QUALIFIER, toBytes("genre"));

                System.out.println(
                        String.format("%-6s | %-40s | %-6s | %-25s |", new String(mIdBytes),
                                new String(titleBytes),
                                new String(yearBytes),
                                new String(genreBytes)));
                count++;
            }
            LOG.log(Level.INFO, ">>>>>> {0} records", count);
        } catch (IOException ex) {
            Logger.getLogger(GenreMovieFinder.class.getName()).log(Level.SEVERE, null, ex);
            throw new RuntimeException(ex);
        } finally {
            try {
                this.genreMovieTable.close();
            } catch (IOException ex) {
                Logger.getLogger(GenreMovieFinder.class.getName()).log(Level.SEVERE, null, ex);
            }
        }

    }

}
