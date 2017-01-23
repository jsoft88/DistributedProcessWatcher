/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.jc.zk.Util;

import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

/**
 *
 * @author cespedjo
 */
public class AsyncZnodeModifier implements Runnable {
    
    private final String znode;
    
    private final byte[] data;
    
    private final ZooKeeper zk;
    
    private final long modifyWaitMillis;
    
    private long timeStarted;
    
    public AsyncZnodeModifier(String znode, byte[] data, ZooKeeper zk, long modifyWaitMillis) {
        this.znode = znode;
        this.data = data;
        this.zk = zk;
        this.modifyWaitMillis = modifyWaitMillis;
    }
    
    @Override
    public void run() {
        this.timeStarted = System.currentTimeMillis();
        synchronized (this) {
            while (true) {
                try {
                    wait(this.modifyWaitMillis);
                    break;
                } catch (InterruptedException ex) {
                    Logger.getLogger(AsyncZnodeModifier.class.getName()).log(Level.SEVERE, null, ex);
                    if (System.currentTimeMillis() - this.timeStarted > this.modifyWaitMillis) {
                        break;
                    }
                }
            }
            try {
                this.zk.setData(this.znode, data, -1);
            } catch (KeeperException ex) {
                Logger.getLogger(AsyncZnodeModifier.class.getName()).log(Level.SEVERE, null, ex);
            } catch (InterruptedException ex) {
                Logger.getLogger(AsyncZnodeModifier.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }
}
