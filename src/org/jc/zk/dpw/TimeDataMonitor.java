/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.jc.zk.dpw;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.jc.zk.util.AsyncZnodeModifier;
import org.jc.zk.util.Utils;

/**
 *
 * @author cespedjo
 */
public class TimeDataMonitor implements 
        Watcher, AsyncCallback.StringCallback, AsyncCallback.DataCallback, 
        AsyncCallback.StatCallback, AsyncCallback.VoidCallback {

    private final String timeZnode;
    
    private final String zkHost;
    
    private final String zkPort;
    
    private final ZooKeeper zk;
    
    private final String[] ntpServers;
    
    private final String requestAMWKillZnode;
    
    private final String znodeForTimeListeners;
    
    private final TimeDataMonitor.TMInterfaceListener listener;
    
    private final String ownerId;
    
    private final String timeZnodeRemovedFlagZnode;
    
    private static final String REQTR_MASTER_ID = "rqmsid";
    
    @Deprecated
    private static final String WRITE_TIME_TO_PROCESS_MASTERS = "wtpm";
    
    private static final String TIME_MASTER_KEEP_ALIVE_ZNODE = "tmkazn";
    
    private static final String TIME_LISTENERS_ZNODE = "tlzn";
    
    private static final String REQUEST_AMW_KILL_ZNODE = "ramwzn";
    
    private static final String PATH_TO_ZNODE_MASTER_PROC = "ptzmp";
    
    private static final String TIME_PAYLOAD = "tpay";
    
    private static final String ZNODE_TYPE = "zntype";
    
    public TimeDataMonitor(String masterId,
            String zkHost, 
            String zkPort, 
            String timeZnode,
            String znodeForTimeListeners,
            String timeZnodeRemovedFlagZnode,
            String requestAMWKillZnode,
            String[] ntpServers,
            TimeDataMonitor.TMInterfaceListener listener) 
            throws IOException {
        this.ownerId = masterId;
        this.timeZnode = timeZnode;
        this.zkHost = zkHost;
        this.zkPort = zkPort;
        this.znodeForTimeListeners = znodeForTimeListeners;
        this.ntpServers = ntpServers;
        this.listener = listener;
        this.zk = new ZooKeeper(this.zkHost + ":" + this.zkPort, 60000, this);
        this.timeZnodeRemovedFlagZnode = timeZnodeRemovedFlagZnode;
        this.requestAMWKillZnode = requestAMWKillZnode;
    }
    
    public static interface TMInterfaceListener {
        //void masterElected(String idOfMasterElected);
        
        //void closing(int rc);
        /**
         * Callback invoked to notify ITMs that ATM pushed an update to time
         * masters' keep alive znode.
         * @param data byte array representing the data that was pushed by ATM.
         * @param error true if there was an error, false otherwise.
         */
        void activeMasterPushedUpdate(byte[] data, boolean error);
        
        /**
         * Callback invoked to notify ATM that the update was pushed to masters' 
         * keep alive znode.
         * @param data byte array representing the data for update.
         * @param error true if an error occurred when pushing update, false otherwise.
         */
        void updatePushedToTimeZnode(byte[] data, boolean error);
        
        /**
         * Callback invoked to indicate that data was read from time masters' 
         * keep alive znode.
         * @param data byte array representing the data read from znode. This
         * might be null if an error does not occurr.
         * @param error true if an error occurred when reading znode, false otherwise.
         */
        void retrievedTimeZnodeLastUpdate(byte[] data, boolean error);
        
        /**
         * Callback invoked to indicate that update was pushed to time listeners'
         * znode.
         * 
         * @param data byte array representing data that was pushed to znode. This
         * might be null if an error does not occurr.
         * @param error true if an error occurred when pushing update, false otherwise.
         */
        void updatePushedToTimeListenersZnode(byte[] data, boolean error);
        
        /**
         * Callback invoked to notify that the client connected to ZooKeeper. 
         */
        void connected();
        
        /**
         * Callback invoked to notify that time masters' keep alive znode was
         * modified by ATM.
         */
        void timeZnodeChanged();
        
        /**
         * Callback invoked to notify that time listeners' znode was modified
         * by ATM.
         */
        void timeListenersZnodeChanged();
        
        /**
         * Callback invoked to notify that time masters' keep alive znode was 
         * removed.
         */
        void timeZnodeDeleted();
        
        /**
         * Callback invoked to notify that time listeners' znode was removed.
         */
        void timeListenersZnodeDeleted();
        
        /**
         * Callback invoked to notify time masters that time masters' keep alive
         * was created.
         * 
         * @param creatorId String representing the id of the TM which created znode.
         */
        void timeZnodeCreated(String creatorId);
        
        /**
         * Callback invoked to notify that time listeners' znode was created.
         */
        void timeListenersZnodeCreated();
        
        /**
         * Callback invoked to notify that client disconnected from ZooKeeper.
         * @param rc integer representing the reason code.
         */
        void disconnected(int rc);
        
        /**
         * Callback invoked to notify which ITM is allowed to remove time masters'
         * keep alive znode.
         * @param allowedToAttempTimeZnodeRemoval true if the ITM is allowed to 
         * remove znode, false otherwise.
         */
        void recreateTimeZnode(boolean allowedToAttempTimeZnodeRemoval);
        
        /**
         * Callback invoked to notify that notification znode was removed.
         */
        void notificationZnodeRemoved();
        
        /**
         * Callback invoked to notify that notification znode was created.
         */
        void notificationZnodeCreated();
        
        /**
         * Callback invoked to notify about a change under request AMW kill znode.
         */
        void requestAMWKillZnodeChanged();
        
        /**
         * Callback invoked to notify TMs that data has been retrieved from 
         * amw kill request znode.
         * @param data byte array representing data that was retrieved from znode.
         * @param error true if an error occurred while reading data, false otherwise.
         */
        void requestAMWKillZnodeDataRead(byte[] data, boolean error);
        
        /**
         * Callback invoked to notify that amw kill request znode has been updated
         * with the provided data.
         * @param data byte array representing data that was written to znode.
         * @param error true if an error occurred while setting data under znode,
         * false otherwise.
         */
        void requestAMWKillZnodeDataSet(byte[] data, boolean error);
    }
    
    @Override
    public void process(WatchedEvent event) {
        System.out.print(event.toString());
        switch (event.getType()) {
            case NodeCreated:
                if (event.getPath().equals(this.znodeForTimeListeners)) {
                    this.listener.timeListenersZnodeCreated();
                } else if (event.getPath().equals(this.timeZnodeRemovedFlagZnode)) {
                    this.listener.notificationZnodeCreated();
                }
                break;
            case NodeDeleted:
                if (event.getPath().equals(this.timeZnode)) {
                    this.listener.timeZnodeDeleted();
                } else if (event.getPath().equals(this.znodeForTimeListeners)) {
                    this.listener.timeListenersZnodeDeleted();
                } else if (event.getPath().equals(this.timeZnodeRemovedFlagZnode)) {
                    this.listener.notificationZnodeRemoved();
                }
                break;
            case NodeDataChanged:
                if (event.getPath().equals(this.timeZnode)) {
                    this.listener.timeZnodeChanged();
                    //this.zk.getData(this.timeZnode, null, this, this.ctx);
                } else if (event.getPath().equals(this.znodeForTimeListeners)) {
                    this.listener.timeListenersZnodeChanged();
                } else if (event.getPath().equals(this.requestAMWKillZnode)) {
                    this.listener.requestAMWKillZnodeChanged();
                }
                break;
            case None:
                if (event.getState() == Event.KeeperState.SyncConnected) {
                    this.listener.connected();
                } else if (event.getState() == Event.KeeperState.Disconnected ||
                        event.getState() == Event.KeeperState.Expired) {
                    this.listener.disconnected(KeeperException.Code.CONNECTIONLOSS.intValue());
                }
                break;
        }
    }
    
    /**
     * Invoke this method to create the notification flag znode.
     */
    public void createTimeZnodeRemovedFlag() {
        this.zk.create(this.timeZnodeRemovedFlagZnode, "0".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL, this, null);
    }
    
    public void removeTimeZnodeRemovedFlag() {
        this.zk.delete(this.timeZnodeRemovedFlagZnode, -1, this, null);
    }
    
    /**
     * Invoke this method to remove the time znode (best-effort).
     */
    public void removeTimeZnode() {
        this.zk.delete(this.timeZnode, -1, this, null);
    }
    
    public void testBindToZnodeListener(boolean bindToTimeZnode) {
        this.zk.exists(this.znodeForTimeListeners, this, this, null);
        this.zk.exists(this.requestAMWKillZnode, this, this, null);
        if (bindToTimeZnode) {
            this.zk.exists(this.timeZnode, this, this, null);
        }
    }
    /**
     * Method to retrieve data from AMW kill request znode.
     */
    public void getRequestAMWKillZnodeData() {
        this.zk.getData(this.requestAMWKillZnode, this, this, new HashMap<>());
    }
    
    /**
     * Method to set data under AMW kill request znode.
     * @param data byte array representing data to be written.
     */
    public void setRequestAMWKillZnodeData(byte[] data) {
        Map<String, String> ctx = new HashMap<>();
        ctx.put(ZNODE_TYPE, REQUEST_AMW_KILL_ZNODE);
        ctx.put(TIME_PAYLOAD, Utils.requestAMWKillZnodeDataToString(data));
        
        this.zk.setData(this.requestAMWKillZnode, data, -1, this, ctx);
    }
    
    /**
     * Method to bind temporarily to a znode.
     */
    public void bindOnceToNotificationZnode() {
        this.zk.exists(this.timeZnodeRemovedFlagZnode, this, this, null);
    }
    
    /**
     * Invoke this method to update the znode which lets ITMs know that ATM is
     * active.
     * @param data byte array representing the data to be written.
     */
    public void updateZNode(byte[] data) {
        HashMap<String, String> ctx = new HashMap<>();
        ctx.put(WRITE_TIME_TO_PROCESS_MASTERS, "false");
        ctx.put(ZNODE_TYPE, TIME_MASTER_KEEP_ALIVE_ZNODE);
        ctx.put(TIME_PAYLOAD, Utils.timeMasterDataForTimeZnodeToString(data));
        this.zk.setData(this.timeZnode, data, -1, this, ctx);
    }
    
    /**
     * Invoke this method to read data from the znode for time listeners.
     */
    public void getDataFromZnodeForTimeListeners() {
        this.zk.getData(this.znodeForTimeListeners, this, this, new HashMap<String, String>());
    }
    
    /**
     * This method will generate the znode that will be used to allow the ATM
     * notify ITMs about its status.
     * @param data 
     */
    public void createTimeZnode(byte[] data) {
        HashMap<String, String> ctx = new HashMap<>();
        ctx.put(REQTR_MASTER_ID, this.ownerId);
        this.zk.create(
                this.timeZnode, 
                data, 
                ZooDefs.Ids.OPEN_ACL_UNSAFE, 
                CreateMode.EPHEMERAL, 
                this, ctx);
    }
    
    public void triggerAsyncAMWRequestKillZnodeRestore(byte[] data, long modifyWaitMillis) {
        AsyncZnodeModifier azm = new AsyncZnodeModifier(this.requestAMWKillZnode, data, this.zk, modifyWaitMillis);
        new Thread(azm).start();
    }
    
    /**
     * This method will query the status notification znode, that is, the znode
     * that ATM uses to communicate its status to ITMs, to retrieve the last
     * updated piece of information.
     */
    public void getDataFromTimeZnode() {
        this.zk.getData(this.timeZnode, this, this, new HashMap<String, String>());
    }
    
    /**
     * This method will push an update to the znode for time listeners. That znode
     * is used by AMW to get clock ticks and inquire PWs about their statuses. When
     * the AMW that created this znode dies, the clock tick stops.
     * @param data byte array that will be written to znode.
     */
    public void writeTimeForProcessMasters(byte[] data) {
        HashMap<String, String> ctx = new HashMap<>();
        ctx.put(WRITE_TIME_TO_PROCESS_MASTERS, "true");
        ctx.put(ZNODE_TYPE, TIME_LISTENERS_ZNODE);
        ctx.put(PATH_TO_ZNODE_MASTER_PROC, this.znodeForTimeListeners);
        ctx.put(TIME_PAYLOAD, Utils.timeMasterDataForTimeListenersToString(data));
        this.zk.setData(this.znodeForTimeListeners, data, -1, this, ctx);
    }

    /**
     * Callback for znode data read.
     * @param rc
     * @param path
     * @param ctx
     * @param data
     * @param stat 
     */
    @Override
    public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
        switch (KeeperException.Code.get(rc)) {
            case OK:
                if (path.equals(this.timeZnode)) {
                    this.listener.retrievedTimeZnodeLastUpdate(data, false);
                } else if (path.equals(this.znodeForTimeListeners)) {
                    this.listener.activeMasterPushedUpdate(data, false);
                } else if (path.equals(this.requestAMWKillZnode)) {
                    this.listener.requestAMWKillZnodeDataRead(data, false);
                }
                
                break;
            case SESSIONEXPIRED:
            case CONNECTIONLOSS:
                this.listener.disconnected(rc);
                
                
                break;
            case NOAUTH:
            case OPERATIONTIMEOUT:
                if (path.equals(this.timeZnode)) {
                    this.listener.retrievedTimeZnodeLastUpdate(null, true);
                } else if (path.equals(this.znodeForTimeListeners)) {
                    this.listener.activeMasterPushedUpdate(null, true);
                } else if (path.equals(this.requestAMWKillZnode)) {
                    this.listener.requestAMWKillZnodeDataRead(null, true);
                }
                
                break;
        }
    }

    /**
     * Callback for znode creation.
     * @param rc
     * @param path
     * @param ctx
     * @param name 
     */
    @Override
    public void processResult(int rc, String path, Object ctx, String name) {
        switch (KeeperException.Code.get(rc)) {
            case OK:
                //Everything went fine, just break.
                if (path.equals(this.timeZnode)) {
                    //this.listener.masterElected(((HashMap<String, String>)ctx).get(REQTR_MASTER_ID));
                    this.listener.timeZnodeCreated(((HashMap<String, String>)ctx).get(REQTR_MASTER_ID));
                } else if (path.equals(this.timeZnodeRemovedFlagZnode)) {
                    new Thread(new Runnable() {

                        @Override
                        public void run() {
                            TimeDataMonitor.this.listener.recreateTimeZnode(true);
                        }
                    }).start();
                }
                break;
            case NODEEXISTS:
                if (path.equals(this.timeZnode)) {
                    //this.listener.masterElected("-9999999999");
                    this.listener.timeZnodeCreated("-9999999999");
                } else if (path.equals(this.timeZnodeRemovedFlagZnode)) {
                    new Thread(new Runnable() {

                        @Override
                        public void run() {
                            TimeDataMonitor.this.listener.recreateTimeZnode(false);
                        }
                    }).start();
                }
                break;
            case SESSIONEXPIRED:
            case CONNECTIONLOSS:
                this.listener.disconnected(rc);
                break;
            case NOAUTH:
            case OPERATIONTIMEOUT:
                //If anything fails, the other TMs will create a node.
                break;
        }
        
    }

    /**
     * Callback for setting znode data.
     * @param rc
     * @param path
     * @param ctx
     * @param stat 
     */
    @Override
    public void processResult(int rc, String path, Object ctx, Stat stat) {
        switch (KeeperException.Code.get(rc)) {
            case OK:
                if (ctx != null) {
                    //We're trying to set data.
                    switch (((Map<String, String>)ctx).get(ZNODE_TYPE)) {
                        case TIME_LISTENERS_ZNODE:
                            byte[] tlData = Utils.timeMasterDataForTimeZnodeToBytes(((HashMap<String, String>)ctx).get(TIME_PAYLOAD));
                            this.listener.updatePushedToTimeListenersZnode(tlData, false);
                            break;
                        case TIME_MASTER_KEEP_ALIVE_ZNODE:
                            byte[] tkData = Utils.timeMasterDataForTimeZnodeToBytes(((HashMap<String, String>)ctx).get(TIME_PAYLOAD));
                            this.listener.updatePushedToTimeZnode(tkData, false);
                            break;
                        case REQUEST_AMW_KILL_ZNODE:
                            String rkPartData = Utils.getDataFromRequestAmwKillZnodeData(((HashMap<String, String>)ctx).get(TIME_PAYLOAD));
                            String rkPartType = Utils.getTypeFromRequestAmwKillZnodeData(((HashMap<String, String>)ctx).get(TIME_PAYLOAD));
                            String rkPartRequester = Utils.getRequesterFromRequestAmwKillZnodeData(((HashMap<String, String>)ctx).get(TIME_PAYLOAD));
                            byte[] rkData = Utils.requestAMWKillZnodeDataToBytes(rkPartData, rkPartType, rkPartRequester);
                            this.listener.requestAMWKillZnodeDataSet(rkData, false);
                            break;
                    }
                }
                //If ctx is null, what we're trying to achieve here is a bind.
                break;
            case SESSIONEXPIRED:
            case CONNECTIONLOSS:
                this.listener.disconnected(rc);
                break;
            case OPERATIONTIMEOUT:
            case AUTHFAILED:
                //Something went wrong
                if (ctx != null) {
                    //we signal that the write failed.
                    //if context is empty, then requestAMWKillZnode update failed.
                    switch (((Map<String, String>)ctx).get(ZNODE_TYPE)) {
                        case TIME_LISTENERS_ZNODE:
                            byte[] tlData = Utils.timeMasterDataForTimeZnodeToBytes(((HashMap<String, String>)ctx).get(TIME_PAYLOAD));
                            this.listener.updatePushedToTimeListenersZnode(tlData, true);
                            break;
                        case TIME_MASTER_KEEP_ALIVE_ZNODE:
                            byte[] tkData = Utils.timeMasterDataForTimeZnodeToBytes(((HashMap<String, String>)ctx).get(TIME_PAYLOAD));
                            this.listener.updatePushedToTimeZnode(tkData, true);
                            break;
                        case REQUEST_AMW_KILL_ZNODE:
                            String rkPartData = Utils.getDataFromRequestAmwKillZnodeData(((HashMap<String, String>)ctx).get(TIME_PAYLOAD));
                            String rkPartType = Utils.getTypeFromRequestAmwKillZnodeData(((HashMap<String, String>)ctx).get(TIME_PAYLOAD));
                            String rkPartRequester = Utils.getRequesterFromRequestAmwKillZnodeData(((HashMap<String, String>)ctx).get(TIME_PAYLOAD));
                            byte[] rkData = Utils.requestAMWKillZnodeDataToBytes(rkPartData, rkPartType, rkPartRequester);
                            this.listener.requestAMWKillZnodeDataSet(rkData, true);
                            break;
                    }
                } else {
                    //We were trying to achieve a bind, not a write, so we retry.
                    this.zk.exists(path, this, this, null);
                }
                break;
        }
    }

    /**
     * Callback for processing znode remove result.
     * @param rc
     * @param path
     * @param ctx 
     */
    @Override
    public void processResult(int rc, String path, Object ctx) {
        switch (KeeperException.Code.get(rc)) {
            case OK:
                this.listener.notificationZnodeRemoved();
                break;
            case OPERATIONTIMEOUT:
            case AUTHFAILED:
                break;
            case CONNECTIONLOSS:
            case SESSIONEXPIRED:
                this.listener.disconnected(rc);
                break;
        }
    }
}
