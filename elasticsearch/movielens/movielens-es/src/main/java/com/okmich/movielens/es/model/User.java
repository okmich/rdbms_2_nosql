/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.okmich.movielens.es.model;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 *
 * @author michael.enudi
 */
public class User extends Model {

    private final long id;
    private final String age;
    private final String occupation;
    private final String gender;
    private final String zipCode;

    public User(long userId, String age, String gender, String occupation, String zipCode) {
        this.id = userId;
        this.age = age;
        this.gender = gender;
        this.occupation = occupation;
        this.zipCode = zipCode;
    }

    /**
     * @return the id
     */
    public long getId() {
        return id;
    }

    /**
     * @return the age
     */
    public String getAge() {
        return age;
    }

    /**
     * @return the occupation
     */
    public String getOccupation() {
        return occupation;
    }

    /**
     * @return the gender
     */
    public String getGender() {
        return gender;
    }

    /**
     * @return the zipCode
     */
    public String getZipCode() {
        return zipCode;
    }

    @Override
    public Map<String, Object> toMap() {
        Map<String, Object> map = new LinkedHashMap<>(4);

        map.put("id", this.id);
        map.put("age", this.age);
        map.put("gender", this.gender);
        map.put("occupation", this.occupation);
        map.put("zipcode", this.zipCode);

        return map;
    }

    @Override
    public String toString() {
        return "User{" + "id=" + id + ", age=" + age + ", occupation=" + occupation
                + ", gender=" + gender + ", zipcode=" + zipCode + '}';
    }
}
