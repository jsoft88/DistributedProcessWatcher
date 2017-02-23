/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.jc.zk.util;

import java.util.Iterator;
import org.apache.log4j.Logger;
import org.jc.zk.dpw.TimeDataMonitor;

/**
 *
 * @author cespedjo
 */
public class AsyncResponseConsumer implements Runnable {
    
    private final long timeout;
    
    private final TimeDataMonitor tdm;
    
    private static final Logger logger = Logger.getLogger(AsyncResponseConsumer.class);
    
    public AsyncResponseConsumer(long timeout, TimeDataMonitor tdm) {
        this.timeout = timeout;
        this.tdm = tdm;
    }
    
    @Override
    public void run() {
        synchronized (this) {
            try {
                wait(this.timeout);
            } catch (InterruptedException ex) {
                logger.error("Aynchronous response consumer for IMWs was interrupted before: " + this.timeout + " millis.", ex);
            } finally {
                int count = 0;
                Iterator<byte[]> it = AuthorizationQueue.consumeQueue().iterator();
                while (it.hasNext()) {
                    ++count;
                    byte[] data = it.next();
                    logger.info("Now authorizing AMW kill request with: " + Utils.requestAMWKillZnodeDataToString(data));
                    it.remove();
                    this.tdm.setRequestAMWKillZnodeData(data);
                }
                logger.info("All authorizations for kill requests were sent - total: " + count);
            }
        }
    }
}
