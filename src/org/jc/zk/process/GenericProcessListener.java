/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.jc.zk.process;

/**
 *
 * @author cespedjo
 */
public interface GenericProcessListener {
    
    void connected();
    
    void disconnected();
    
    void pHeartBeatZnodeCreated();
    
    void pHeartBeatZnodeRemoved();
    
    void pHeartBeatZnodeUpdated(boolean error, String data);
    
    void pHeartBeatZnodeDataRead(boolean error, String data);
}
