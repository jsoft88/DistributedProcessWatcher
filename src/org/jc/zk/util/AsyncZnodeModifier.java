/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.jc.zk.util;

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
    
    private static final org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger(AsyncZnodeModifier.class);
    
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
                    logger.info("Waiting " + this.modifyWaitMillis + " millis before restoring " + this.znode + " znode to FREE.");
                    wait(this.modifyWaitMillis);
                    break;
                } catch (InterruptedException ex) {
                    logger.error("Thread interrupted while waiting " + this.modifyWaitMillis + " millis to set " + this.znode + " to FREE.", ex);
                    if (System.currentTimeMillis() - this.timeStarted > this.modifyWaitMillis) {
                        logger.error("Interrupted while waiting but finished waiting and now it is time to set " + this.znode + " to FREE. Breaking.");
                        break;
                    }
                }
            }
            try {
                logger.info("Restoring " + this.znode + " to FREE now.");
                this.zk.setData(this.znode, data, -1);
            } catch (KeeperException ex) {
                logger.error("Failed to restore " + this.znode + " to FREE.", ex);
            } catch (InterruptedException ex) {
                logger.error("Thread interrupted while waiting for ZK to finish setting znode to FREE.", ex);
            }
        }
    }
}
