/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.okmich.movielens.es.db;

import com.okmich.movielens.es.model.Rating;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author michael.enudi
 */
public class RatingReader extends BaseReader<Rating> {

    private final int pageSize;

    /**
     *
     * @param file
     * @param header
     * @param pageSize
     */
    public RatingReader(Path file, boolean header, int pageSize) {
        super(file, header);
        this.pageSize = pageSize;
    }

    public List<Rating> read() throws IOException {
        List<String[]> payload = super.readFile(this.pageSize);
        List<Rating> ratings = new ArrayList<>(this.pageSize);
        payload.forEach((rec) -> {
            ratings.add(new Rating(
                    Long.parseLong(rec[0]),
                    Long.parseLong(rec[1]),
                    Float.parseFloat(rec[2]),
                    Long.parseLong(rec[3])
            ));
        });
        return ratings;
    }
}
