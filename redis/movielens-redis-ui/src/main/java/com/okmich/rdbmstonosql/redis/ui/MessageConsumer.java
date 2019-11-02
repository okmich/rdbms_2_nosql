/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.okmich.rdbmstonosql.redis.ui;

/**
 *
 * @author Michael Enudi
 */
public interface MessageConsumer {
    
   void applyMessage(Object... args);
}
