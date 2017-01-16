/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.jc.zk.process;

import java.util.concurrent.Callable;

/**
 *
 * @author cespedjo
 * @param <G> type of value returned by Callable.
 */
public interface Actions<G> extends Callable<G> {
    
    void updateZnode();
}
