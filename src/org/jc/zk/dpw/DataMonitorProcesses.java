/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.jc.zk.dpw;

import java.util.HashMap;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

/**
 *
 * @author cespedjo
 */
@Deprecated
public class DataMonitorProcesses implements Watcher, AsyncCallback.StatCallback,
        AsyncCallback.DataCallback {
    
    private ZooKeeper zk;
    
    private String znodeObserved;
    
    private String znodeTime;
    
    private Watcher chainedWatcher;
    
    boolean dead;
    
    private DataMonitorListenerProcesses listener;
    
    private byte[] prevData;
    
    private static final String TIME_FOR_UPDATE = "tfu";
    
    private HashMap<String, String> ctx;
    
    public DataMonitorProcesses() {
        this.ctx = new HashMap<>();
    }
    
    public interface DataMonitorListenerProcesses {
        void closing(int rc);
        void updateStatus(byte[] data);
        void timeUpdated(byte[] data);
        void statusUpdated(byte[] data);
    }

    @Override
    public void process(WatchedEvent event) {
        switch (event.getType()) {
            case NodeCreated:
                if (event.getPath().equals(this.znodeObserved)) {
                    
                }
                break;
            case NodeDataChanged:
                if (event.getPath().equals(this.znodeTime)) {
                    this.readTimeZnode();
                }
                break;
            case None:
                if (event.getState() == Event.KeeperState.Disconnected || 
                        event.getState() == Event.KeeperState.Expired) {
                    this.listener.closing(KeeperException.Code.CONNECTIONLOSS.intValue());
                }
                break;
            case NodeDeleted:
                if (event.getPath().equals(this.znodeObserved) || 
                        event.getPath().equals(this.znodeTime)) {
                    this.listener.closing(KeeperException.Code.CONNECTIONLOSS.intValue());
                }
                break;
        }
    }
    
    public void readTimeZnode() {
        this.zk.getData(this.znodeTime, this, this, null);
    }
    
    public void readObservedZnode() {
        this.zk.getData(this.znodeObserved, this, this, null);
    }
    
    public void updateObservedZnode(byte[] data) {
        this.ctx.clear();
        this.ctx.put(TIME_FOR_UPDATE, new String(data));
        this.zk.setData(this.znodeObserved, data, -1, this, null);
    }

    @Override
    public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
        switch (KeeperException.Code.get(rc)) {
            case OK:
                if (path.equals(this.znodeTime)) {
                    this.listener.timeUpdated(data);
                } else if (path.equals(this.znodeObserved)) {
                    //We're passing the data that is contained in observed znode
                    //since we need to verify the master id to know if we're 
                    //supposed to be alive.
                    this.listener.updateStatus(data);
                }
                break;
            case CONNECTIONLOSS:
            case SESSIONEXPIRED:
                this.listener.closing(rc);
                break;
            case OPERATIONTIMEOUT:
            case NOAUTH:
                if (path.equals(this.znodeTime)) {
                    this.listener.timeUpdated(null);
                } else if (path.equals(this.znodeObserved)) {
                    this.listener.updateStatus(null);
                }
                break;
        }
    }

    @Override
    public void processResult(int rc, String path, Object ctx, Stat stat) {
        switch (KeeperException.Code.get(rc)) {
            case OK:
                this.listener.statusUpdated(this.ctx.get(TIME_FOR_UPDATE).getBytes());
                break;
            case CONNECTIONLOSS:
            case SESSIONEXPIRED:
                this.listener.closing(rc);
                break;
            case OPERATIONTIMEOUT:
            case AUTHFAILED:
                this.listener.statusUpdated(null);
                break;
        }
    }
}
