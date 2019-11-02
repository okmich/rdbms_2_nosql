/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.okmich.movielens.es.db;

import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author michael.enudi
 */
public abstract class BaseReader<T> {

    private String[] header = null;
    private boolean headerExist = false;
    private final Path file;

    private int pointerMark;

    public BaseReader(Path file, boolean header) {
        this.file = file;
        this.headerExist = header;
        this.pointerMark = 0;
    }

    public List<String[]> readFile() throws IOException {
        try (Reader reader = Files.newBufferedReader(this.file);
                CSVReader csvReader = new CSVReader(reader, ',', '"');) {

            List<String[]> payload = new ArrayList<>();
            String[] nextRecord;
            if (this.headerExist) {
                this.header = csvReader.readNext();
            }

            while ((nextRecord = csvReader.readNext()) != null) {
                payload.add(nextRecord);
            }
            return payload;
        } catch (IOException ex) {
            throw ex;
        }
    }

    public List<String[]> readFile(int number) throws IOException {
        int i = 0;
        try (Reader reader = Files.newBufferedReader(this.file);
                CSVReader csvReader = new CSVReaderBuilder(reader).withSkipLines(1 + this.pointerMark).build();) {

            List<String[]> payload = new ArrayList<>();

            String[] nextRecord;
            if (this.headerExist) {
                this.header = csvReader.readNext();
            }
            while ((nextRecord = csvReader.readNext()) != null) {
                payload.add(nextRecord);
                this.pointerMark++;
                i++;
                if (i == number) {
                    break;
                }
            }
            return payload;
        } catch (IOException ex) {
            throw ex;
        }
    }

    /**
     * @return the header
     */
    public String[] getHeader() {
        return header;
    }

    /**
     * @return the headerExist
     */
    public boolean isHeaderExist() {
        return headerExist;
    }
}
