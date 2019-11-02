/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.okmich.movielens.hbase.migrate;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

/**
 *
 * @author michael.enudi
 */
public abstract class BaseMigration {

    protected String zkQuorum;
    protected String zkClientPort;
    private final String fileToIngest;

    private static final Logger LOG = Logger.getLogger(BaseMigration.class.getName());

    /**
     *
     * @param zookeeperQuorum
     * @param zookeeperPort
     * @param fileToIngest
     * @throws java.lang.RuntimeException
     */
    protected BaseMigration(
            String zookeeperQuorum, String zookeeperPort, String fileToIngest) throws RuntimeException {
        this.fileToIngest = fileToIngest;
        this.zkClientPort = zookeeperPort;
        this.zkQuorum = zookeeperQuorum;
    }

    protected final Table initHBase(String zookeeperQuorum, String zookeeperPort, String tableName) {

        try {
            Configuration conf = HBaseConfiguration.create();
            conf.set(HConstants.ZOOKEEPER_QUORUM, zookeeperQuorum);
            conf.set(HConstants.ZOOKEEPER_CLIENT_PORT, zookeeperPort);
            conf.set(HConstants.ZOOKEEPER_ZNODE_PARENT, "/hbase-unsecure");
            org.apache.hadoop.hbase.client.Connection hbaseConnection
                    = ConnectionFactory.createConnection(conf);
            return hbaseConnection.getTable(TableName.valueOf(tableName));
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    public void migrate() throws RuntimeException {
        String line;
        try (BufferedReader reader = new BufferedReader(new FileReader(fileToIngest))) {
            while ((line = reader.readLine()) != null) {
                doEachLine(line);
                LOG.log(Level.INFO, line);
            }
        } catch (IOException ex) {
            throw new RuntimeException(ex.getMessage(), ex);
        }
    }

    public void migrate(int batchSize) throws RuntimeException {
        List<String> lines = new ArrayList<>(batchSize);
        try (BufferedReader reader = new BufferedReader(new FileReader(fileToIngest))) {
            String line;
            while ((line = reader.readLine()) != null) {
                lines.add(line);
                LOG.log(Level.INFO, line);

                if (lines.size() == batchSize) {
                    processBatchLines(lines);
                }
            }
            if (!lines.isEmpty()) {
                processBatchLines(lines);
            }
        } catch (IOException ex) {
            throw new RuntimeException(ex.getMessage(), ex);
        }
    }

    public abstract void doEachLine(String line) throws IOException;

    public abstract void processBatch(List<String> lines) throws IOException;

    protected static byte[] as(Boolean arg) {
        return Bytes.toBytes(arg);
    }

    protected static byte[] as(String arg) {
        return Bytes.toBytes(arg);
    }

    protected static byte[] as(Long arg) {
        return Bytes.toBytes(arg);
    }

    protected static byte[] as(Integer arg) {
        return Bytes.toBytes(arg);
    }

    protected static byte[] as(Float arg) {
        return Bytes.toBytes(arg);
    }

    private void processBatchLines(List<String> lines) {
        try {
            LOG.log(Level.INFO, "Processing writes..... {0} records\n", lines.size());
            processBatch(lines);
            lines.clear();
        } catch (IOException ex) {
            LOG.log(Level.SEVERE, null, ex);

        }
    }
}
