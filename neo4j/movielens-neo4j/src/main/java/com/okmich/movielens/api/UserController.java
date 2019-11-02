/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.okmich.movielens.api;

import com.okmich.movielens.entity.Movie;
import com.okmich.movielens.entity.User;
import com.okmich.movielens.entity.repo.UserRepo;
import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 *
 * @author michael.enudi
 */
@RestController
@RequestMapping("/service/users")
public class UserController {

    @Autowired
    private UserRepo userRepo;

    public UserController() {
    }

    @RequestMapping(method = RequestMethod.GET)
    public Page<User> getUsers(@RequestParam(name = "pageCount", required = false, defaultValue = "0") int pageCount,
            @RequestParam(name = "pageSize", required = false, defaultValue = "25") int pageSize) {
        Pageable pgbl = new PageRequest(pageCount, pageSize);
        return userRepo.findAll(pgbl);
    }

    @RequestMapping(value = "/{userId}", method = RequestMethod.GET)
    public User getUser(@PathVariable long userId) {
        return userRepo.findById(userId).get();
    }

    @RequestMapping(value = "/", method = RequestMethod.GET)
    public User getUserByUserId(@RequestParam long userId) {
        return userRepo.findByUserId(userId);
    }

    @RequestMapping(method = RequestMethod.POST)
    public User createUser(@RequestBody User user) {
        return userRepo.save(user);
    }

    @RequestMapping(value = "/{userId}", method = RequestMethod.PUT)
    public User updateUser(@PathVariable long userId, @RequestBody User user) {
        return userRepo.save(user);
    }

    @RequestMapping(value = "/{userId}/movierecommended", method = RequestMethod.GET)
    public List<Movie> recommendMovie(@PathVariable long userId) {
        return userRepo.recommendByUserRatingHistory(userId);
    }
}
