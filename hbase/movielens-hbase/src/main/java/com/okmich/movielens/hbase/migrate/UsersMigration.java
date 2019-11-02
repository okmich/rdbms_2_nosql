/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.okmich.movielens.hbase.migrate;

import static com.okmich.movielens.hbase.migrate.BaseMigration.as;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;

/**
 *
 * @author michael.enudi
 */
public class UsersMigration extends BaseMigration {

    public static final String USER_TABLE = "user";
    public static final byte[] USER_COLUMN_FAMILY_QUALIFIER = as("u");

    private final static Map<String, String> AGE_GROUP_LOOKUP = new HashMap<>();
    private final static Map<String, String> OCCUPATION_LOOKUP = new HashMap<>();

    /**
     *
     * @param fileToIngest
     * @param zkQuorum
     * @param clientPort
     */
    public UsersMigration(String fileToIngest,
            String zkQuorum, String clientPort) {
        super(zkQuorum, clientPort, fileToIngest);
        init();
    }

    @Override
    public void doEachLine(String line) throws IOException {
        String fields[] = line.split("::");
        Integer id = Integer.valueOf(fields[0]);
        String ageId = fields[2];
        String occId = fields[3];
        try (Table table = initHBase(zkQuorum, this.zkClientPort, USER_TABLE);) {
            Put put = new Put(as(id));
            put.addColumn(USER_COLUMN_FAMILY_QUALIFIER, as("gndr"), as(fields[1]));
            put.addColumn(USER_COLUMN_FAMILY_QUALIFIER, as("ageid"), as(ageId));
            put.addColumn(USER_COLUMN_FAMILY_QUALIFIER, as("age"), as(AGE_GROUP_LOOKUP.get(ageId)));
            put.addColumn(USER_COLUMN_FAMILY_QUALIFIER, as("occid"), as(occId));
            put.addColumn(USER_COLUMN_FAMILY_QUALIFIER, as("occ"), as(OCCUPATION_LOOKUP.get(occId)));
            put.addColumn(USER_COLUMN_FAMILY_QUALIFIER, as("zip"), as(fields[4]));

            table.put(put);
        } catch (IOException ex) {
            Logger.getLogger(MoviesMigration.class.getName()).log(Level.SEVERE, ex.getMessage(), ex);
            throw ex;
        }
    }

    @Override
    public void processBatch(List<String> lines) throws IOException {
        List<Put> puts = lines.stream().map((String line) -> {
            String fields[] = line.split("::");
            Integer id = Integer.valueOf(fields[0]);
            String ageId = fields[2];
            String occId = fields[3];

            Put put = new Put(as(id));
            put.addColumn(USER_COLUMN_FAMILY_QUALIFIER, as("gndr"), as(fields[1]));
            put.addColumn(USER_COLUMN_FAMILY_QUALIFIER, as("ageid"), as(ageId));
            put.addColumn(USER_COLUMN_FAMILY_QUALIFIER, as("age"), as(AGE_GROUP_LOOKUP.get(ageId)));
            put.addColumn(USER_COLUMN_FAMILY_QUALIFIER, as("occid"), as(occId));
            put.addColumn(USER_COLUMN_FAMILY_QUALIFIER, as("occ"), as(OCCUPATION_LOOKUP.get(occId)));
            put.addColumn(USER_COLUMN_FAMILY_QUALIFIER, as("zip"), as(fields[4]));

            return put;
        }).collect(Collectors.toList());

        try (Table table = initHBase(zkQuorum, this.zkClientPort, USER_TABLE);) {

            //save to hbase table
            table.put(puts);
        } catch (IOException ex) {
            Logger.getLogger(MoviesMigration.class.getName()).log(Level.SEVERE, null, ex);
            throw new RuntimeException(ex);
        }
    }

    private static void init() {
        AGE_GROUP_LOOKUP.put("1", "Under 18");
        AGE_GROUP_LOOKUP.put("18", "18-24");
        AGE_GROUP_LOOKUP.put("25", "25-34");
        AGE_GROUP_LOOKUP.put("35", "35-44");
        AGE_GROUP_LOOKUP.put("45", "45-49");
        AGE_GROUP_LOOKUP.put("50", "50-55");
        AGE_GROUP_LOOKUP.put("56", "56+");

        OCCUPATION_LOOKUP.put("0", "other or not specified");
        OCCUPATION_LOOKUP.put("1", "academic/educator");
        OCCUPATION_LOOKUP.put("2", "artist");
        OCCUPATION_LOOKUP.put("3", "clerical/admin");
        OCCUPATION_LOOKUP.put("4", "college/grad student");
        OCCUPATION_LOOKUP.put("5", "customer service");
        OCCUPATION_LOOKUP.put("6", "doctor/health care");
        OCCUPATION_LOOKUP.put("7", "executive/managerial");
        OCCUPATION_LOOKUP.put("8", "farmer");
        OCCUPATION_LOOKUP.put("9", "homemaker");
        OCCUPATION_LOOKUP.put("10", "K-12 student");
        OCCUPATION_LOOKUP.put("11", "lawyer");
        OCCUPATION_LOOKUP.put("12", "programmer");
        OCCUPATION_LOOKUP.put("13", "retired");
        OCCUPATION_LOOKUP.put("14", "sales/marketing");
        OCCUPATION_LOOKUP.put("15", "scientist");
        OCCUPATION_LOOKUP.put("16", "self-employed");
        OCCUPATION_LOOKUP.put("17", "technician/engineer");
        OCCUPATION_LOOKUP.put("18", "tradesman/craftsman");
        OCCUPATION_LOOKUP.put("19", "unemployed");
        OCCUPATION_LOOKUP.put("20", "writer");
    }
}
