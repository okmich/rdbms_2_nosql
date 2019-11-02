/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.okmich.movielens.es.db;

import com.okmich.movielens.es.model.User;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author michael.enudi
 */
public class UserReader extends BaseReader<User> {

    private final int pageSize;

    /**
     *
     * @param file
     * @param header
     * @param pageSize
     */
    public UserReader(Path file, boolean header, int pageSize) {
        super(file, header);
        this.pageSize = pageSize;
    }

    public List<User> read() throws IOException {
        List<String[]> payload = super.readFile(this.pageSize);
        List<User> users = new ArrayList<>(this.pageSize);
        payload.forEach((rec) -> {
            users.add(new User(
                    Long.parseLong(rec[0]),
                    rec[1],
                    rec[2],
                    rec[3],
                    rec[4]
            ));
        });
        return users;
    }
}
