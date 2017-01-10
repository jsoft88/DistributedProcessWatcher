/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.jc.zk.process;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.jc.zk.util.Utils;

/**
 *
 * @author cespedjo
 */
public class ProcessWrapperMonitor<P extends GenericProcessListener> implements Watcher, AsyncCallback.StatCallback, AsyncCallback.DataCallback {

    protected ZooKeeper zk;
    
    protected final String zkConnectString;
    
    private final String heartBeatZnode;
    
    protected final P listener;
    
    public static final String PAYLOAD = "payload";
    
    public ProcessWrapperMonitor(String zkConnectString, String znode, P listener) throws IOException {
        this.zkConnectString = zkConnectString;
        this.heartBeatZnode = znode;
        this.listener = listener;
        
        zkConnect();
    }
    
    public final void zkConnect() throws IOException {
        this.zk = new ZooKeeper(this.zkConnectString, 120000, this);
    }
    
    public final void bindHeartBeatZnode() {
        this.zk.exists(this.heartBeatZnode, this, this, null);
    }
    
    @Override
    public void process(WatchedEvent event) {
        switch (event.getType()) {
            case None:
                if (event.getState() == Event.KeeperState.Disconnected ||
                        event.getState() == Event.KeeperState.Expired) {
                    this.listener.disconnected();
                } else if (event.getState() == Event.KeeperState.SyncConnected) {
                    this.listener.connected();
                }
                break;
            case NodeCreated:
                if (event.getPath().equals(this.heartBeatZnode)) {
                    this.listener.pHeartBeatZnodeCreated();
                }
                break;
            case NodeDeleted:
                if (event.getPath().equals(this.heartBeatZnode)) {
                    this.listener.pHeartBeatZnodeRemoved();
                }
                break;
        }
    }
    
    public void updateZnode(String data) {
        Map<String, String> ctx = new HashMap<>();
        ctx.put(PAYLOAD, data);
        
        this.zk.setData(this.heartBeatZnode, data.getBytes(Charset.forName("UTF-8")), -1, this, ctx);
    }
    
    public void readHeartBeatData() {
        this.zk.getData(this.heartBeatZnode, this, this, null);
    }

    @Override
    public void processResult(int rc, String path, Object ctx, Stat stat) {
        switch (KeeperException.Code.get(rc)) {
            case OK:
                if (ctx != null) {
                    if (path.equals(this.heartBeatZnode)) {
                        this.listener.pHeartBeatZnodeUpdated(false, null);
                    }
                }
                break;
            case AUTHFAILED:
            case OPERATIONTIMEOUT:
                if (ctx != null) {
                    if (path.equals(this.heartBeatZnode)) {
                        Map<String, String> mCtx = (Map<String, String>)ctx;
                        this.listener.pHeartBeatZnodeUpdated(true, mCtx.get(PAYLOAD));
                    }
                } else {
                    if (path.equals(this.heartBeatZnode)) {
                        this.bindHeartBeatZnode();
                    }
                }
                break;
            case SESSIONEXPIRED:
            case CONNECTIONLOSS:
                this.listener.disconnected();
                break;
        }
    }

    @Override
    public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
        switch (KeeperException.Code.get(rc)) {
            case OK:
                if (path.equals(this.heartBeatZnode)) {
                    String sData = Utils.processObservedZnodeDataToString(data);
                    this.listener.pHeartBeatZnodeDataRead(false, sData);
                }
                break;
            case SESSIONEXPIRED:
            case CONNECTIONLOSS:
                this.listener.disconnected();
                break;
            case AUTHFAILED:
            case OPERATIONTIMEOUT:
                if (path.equals(this.heartBeatZnode)) {
                    this.listener.pHeartBeatZnodeDataRead(true, null);
                }
                break;
        }
    }
}
