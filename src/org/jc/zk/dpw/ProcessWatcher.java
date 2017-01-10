/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.jc.zk.dpw;

import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.jc.zk.util.Utils;

/**
 *
 * @author cespedjo
 */
@Deprecated
public abstract class ProcessWatcher 
    implements Watcher, Runnable, DataMonitorProcesses.DataMonitorListenerProcesses {
    
    private String zkHost;
    
    private int zkPort;
    
    private String observedZnode;
    
    private String timeZnode;
    
    private DataMonitorProcesses dmp;
    
    private boolean alive;
    
    private String pwId;
    
    public ProcessWatcher(String pwId, String zkHost, int zkPort, String observedZnode, String timeZnode) {
        this.zkHost = zkHost;
        this.zkPort = zkPort;
        this.observedZnode = observedZnode;
        this.timeZnode = timeZnode;
        this.dmp = new DataMonitorProcesses();
        this.alive = true;
        this.pwId = pwId;
    }

    @Override
    public void process(WatchedEvent event) {
        this.dmp.process(event);
    }

    @Override
    public void run() {
        this.execute();
        synchronized (this) {
            while (this.alive) {
                try {
                    wait();
                } catch (InterruptedException ex) {
                    Logger.getLogger(ProcessWatcher.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        }
        this.onCancelled();
    }

    @Override
    public void closing(int rc) {
        if (KeeperException.Code.get(rc) == KeeperException.Code.SESSIONEXPIRED ||
                KeeperException.Code.get(rc) == KeeperException.Code.CONNECTIONLOSS) {
            synchronized (this) {
                this.alive = false;
                notify();
            }
        }
    }

    @Override
    public void updateStatus(byte[] data) {
        if (data == null) {
            this.dmp.readObservedZnode();
            return;
        }        
        
        String pWatcherId = Utils.getIdOfProcessWatcherFromZnode(data);
        if (this.pwId.equals(pWatcherId)) {
            this.dmp.updateObservedZnode(data);
        }
    }

    @Override
    public void timeUpdated(byte[] data) {
        if (data == null) {
            this.dmp.readTimeZnode();
            return;
        }
        
        long time = Utils.getTimeFromZnode(data);
        //Update observed znode.
        this.dmp.updateObservedZnode(Utils.generateDataForObservedZnode(this.pwId,"", time));
    }

    @Override
    public void statusUpdated(byte[] data) {
        if (data == null) {
            long time = Utils.getTimeFromZnode(data);
            //Update observed znode.
            this.dmp.updateObservedZnode(Utils.generateDataForObservedZnode(this.pwId, "", time));
        }
        
        //If updated, there's nothing else to do. Everything went fine.
    }
    
    public void execute() {
        
    }
    
    public void onCancelled() {
        
    }
    
    public void endExecution() {
        synchronized (this) {
            this.alive = false;
            notify();
        }
    }
}
