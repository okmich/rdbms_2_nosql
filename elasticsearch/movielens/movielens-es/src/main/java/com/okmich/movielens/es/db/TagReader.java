/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.okmich.movielens.es.db;

import com.okmich.movielens.es.model.Tag;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 *
 * @author michael.enudi
 */
public class TagReader extends BaseReader<Tag> {

    private final int pageSize;

    /**
     *
     * @param file
     * @param header
     * @param pageSize
     */
    public TagReader(Path file, boolean header, int pageSize) {
        super(file, header);
        this.pageSize = pageSize;
    }

    public List<Tag> read() throws IOException {
        List<String[]> payload = super.readFile(this.pageSize);
        List<Tag> tags = new ArrayList<>(this.pageSize);
        payload.forEach((rec) -> {
            tags.add(new Tag(
                    Long.parseLong(rec[1]),
                    Long.parseLong(rec[2]),
                    rec[3],
                    Long.parseLong(rec[4])
            ));
        });
        return tags;
    }
}
