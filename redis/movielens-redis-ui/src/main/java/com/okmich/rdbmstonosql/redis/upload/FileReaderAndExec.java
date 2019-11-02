/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.okmich.rdbmstonosql.redis.upload;

import com.okmich.rdbmstonosql.redis.Constants;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 *
 * @author michael.enudi
 */
public class FileReaderAndExec {

    private boolean headerExist = false;
    private final Path file;

    public FileReaderAndExec(Path file, boolean header) {
        this.file = file;
        this.headerExist = header;
    }

    public void executeForEachRecord(Function<String, Void> function) throws IOException {
        try (Reader reader = Files.newBufferedReader(this.file);
                CSVReader csvReader = new CSVReaderBuilder(reader).withSkipLines(1).build();) {
            String[] nextRecord;

            while ((nextRecord = csvReader.readNext()) != null) {
                //call on this method
                function.apply(mkString(nextRecord, "---"));
            }

        } catch (IOException ex) {
            throw ex;
        }
    }

    /**
     * @return the headerExist
     */
    public boolean isHeaderExist() {
        return headerExist;
    }

    String mkString(String[] strArr, String delim) {
        return Arrays.asList(strArr).stream().collect(Collectors.joining(Constants.FIELD_DELIMITER));
    }
}
