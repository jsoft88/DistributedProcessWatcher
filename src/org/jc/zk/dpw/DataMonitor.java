/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.jc.zk.dpw;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.log4j.Logger;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.jc.zk.util.Utils;

/**
 *
 * @author cespedjo
 */
public class DataMonitor implements Watcher, AsyncCallback.StatCallback, AsyncCallback.StringCallback,
        AsyncCallback.DataCallback, AsyncCallback.VoidCallback {
    
    private static final Logger logger = Logger.getLogger(DataMonitor.class);
    
    ZooKeeper zk;
    
    String znodeMaster;
    
    String znodeTime;
    
    String znodeProcessObserved;
    
    String znodeToCreateForUpdates;
    
    Watcher chainedWatcher;
    
    DataMonitorListenerMaster listener;
    
    private final String ownCMWFailoverZnode;
    
    private final List<String> cmwUpdateZnodes;
    
    private final String safeZnodeRemovalNotificationZnode;
    
    private final String processHeartBeatZnode;
    
    private boolean isActiveMaster;
    
    //Context key to store the MW requesting an operation.
    private static final String REQSTR_MASTER_ID = "rqmsid";
    
    //Context key to store the type of znode is being: created, read from, updated or removed.
    private static final String ZNODE_TYPE = "zntype";
    
    //Context value for key ZNODE_TYPE, indicating that it refers to KEEP ALIVE ZNODE (znodeMaster).
    private static final String KEEP_ALIVE_NODE = "kaln";
    
    //Context value for key ZNODE_TYPE, indicating that it refers to PROCESS KEEP ALIVE NODE (znodeProcessObserved)
    private static final String PROCESS_KEEP_ALIVE_NODE = "pkaln";
    
    //Context value for key ZNODE_TYPE, indicating that it refers to TIME ZNODE (znodeTime).
    private static final String TIME_NODE = "tmnode";
    
    //Context key to store a znode's payload encoded as a string.
    private static final String ZNODE_PAYLOAD = "znpyl";
    
    //Context value for key ZNODE_TYPE, indicating that it refers to UPDATE ZNODE (znodeToCreateForUpdates)
    private static final String CMW_UPDATE_NODE = "cmwupn";
    
    //Context value for key ZNODE_TYPE, indicating that it refers to NOTIFICATION ZNODE (safeZnodeRemovalNotificationZnode)
    private static final String NOTIFICATION_ZNODE = "nozn";
    
    //Context key to store the type of event associated to CMW_UPDATE_NODE.
    private static final String UPDATE_NODE_EVENT = "upnev";
    
    //Context value for key UPDATE_NODE_EVENT, indicating that CMW_UPDATE_NODE is being created.
    private static final String UPDATE_EVENT_IS_CREATE = "upiscrt";
    
    //Context value for key UPDATE_NODE_EVENT, indicating that CMW_UPDATE_NODE is being changed.
    private static final String UPDATE_EVENT_IS_CHANGED = "upischn";
    
    /**
     * Masters' Data Monitor constructor.
     * @param zk instance of zookeeper.
     * @param znodeMaster path to masters' znode.
     * @param znodeTime path to time masters' time update znode.
     * @param znodeProcessObserved path to observed znode.
     * @param znodeToCreateForUpdates path to znode that a CMW will have to update
     * when requested by ATM. This is null when instantiating a monitor for a MW.
     * @param ownCMWFailoverZnode failover znode that will be created by active
     * CMW to indicate its inactive pair to activate itself.
     * @param safeZnodeRemovalNotificationZnode
     * @param cmwUpdateZnodes
     * @param processHeartBeatZnode path to znode that CMW will create to request
     * heart beat from ProcessWrappers.
     * @param chainedWatcher another watcher to which events will be forwarded
     * after processing locally.
     * @param listener instance of class implementing listener interface.
     */
    public DataMonitor(
            ZooKeeper zk, 
            String znodeMaster,
            String znodeTime,
            String znodeProcessObserved,
            String znodeToCreateForUpdates,
            String ownCMWFailoverZnode,
            String safeZnodeRemovalNotificationZnode,
            List<String> cmwUpdateZnodes,
            String processHeartBeatZnode,
            Watcher chainedWatcher,
            DataMonitorListenerMaster listener) {
        this.zk = zk;
        this.znodeTime = znodeTime;
        this.znodeMaster = znodeMaster;
        this.chainedWatcher = chainedWatcher;
        this.listener = listener;
        this.znodeProcessObserved = znodeProcessObserved;
        this.znodeToCreateForUpdates = znodeToCreateForUpdates;
        this.ownCMWFailoverZnode = ownCMWFailoverZnode;
        this.cmwUpdateZnodes = cmwUpdateZnodes;
        this.safeZnodeRemovalNotificationZnode = safeZnodeRemovalNotificationZnode;
        this.processHeartBeatZnode = processHeartBeatZnode;
        this.isActiveMaster = false;
    }
    
    public interface DataMonitorListenerMaster {
        /**
         * Invoke this method to handle an event like session expired or some error.
         * @param rc reason code for closing.
         */
        //void closing(int rc);
        
        /**
         * The first time an active master writes to a znode, this is the method
         * invoked by callback to notify status of first write.
         * @param error true if something went wrong, false otherwise.
         */
        //void dataWrittenToZnode(boolean error);
        
        /**
         * This callback is invoked when data is retrieved from znode.
         * The master will probably have to take time information from it.
         * @param data data retrieved from znode.
         */
        void dataReadFromZnode(byte[] data);
        
        /**
         * This callback allows masters find out who the elected master is.
         * @param masterId Id of the selected master.
         */
        void masterElected(String masterId);
        
        /**
         * This callback is invoked to indicate the result of the masters' keep
         * alive znode.
         * @param error true to indicate that there was an error or false otherwise.
         * @param data byte array containing the data that was used to create znode.
         * This is null if creation was completed successfully.
         */
        void masterWatcherZnodeCreated(boolean error, byte[] data);
        
        /**
         * This callback is invoked to indicate the result of the znode that 
         * tells CMWs to deploy their processes.
         * @param error true to indicate that there was an error or false otherwise.
         * @param data byte array containing the data that was used to create znode.
         * This is null if creation was completed successfully.
         */
        void processDataNodeCreated(boolean error, byte[] data);
        
        /**
         * This callback is invoked when the znode for time listeners receives
         * an update from TM.
         * @param data the data that was written to time znode by the TM.
         */
        void timeUpdated(byte[] data);
        
        /**
         * Callback invoked when data from processes' znode has been read.
         * @param data byte array representing the data stored in znode.
         */
        void dataReadFromProcessZnode(byte[] data);
        
        /**
         * Callback invoked to notify the result of the update issued by CMW to its update znode 
         * @param znode the znode assigned to CMW that must be created to ack 
         * that a CMW is alive.
         * @param data the data that was written to znode or null if an error occurred
         * while creating update znode.
         */
        void childMasterWatcherUpdatedZnode(String znode, byte[] data);
        
        //void childMasterWatcherZnodeData(byte[] data);
        
        //void znodeRemoved(String path, boolean error);
        
        /**
         * Callback invoked when client is connected to ZooKeeper.
         */
        void connected();
        
        /**
         * Callback invoked when a client disconnects.
         * @param rc an int representing reason code.
         */
        void disconnected(int rc);
        
        /**
         * This method will be invoked to notify which MW will be allowed to remove
         * znodes created by former AMW and who will be able to remove notification znode.
         * @param allowedToAttemptZnodesRemoval true if the MW will be allowed to remove
         * znodes or false if it cannot remove znodes.
         */
        void recreateMasterZnode(boolean allowedToAttemptZnodesRemoval);
        
        /**
         * Callback to notify clients that masters' keep alive znode changed.
         */
        void masterZnodeChanged();
        
        /**
         * Callback to notify clients that masters' keep alive was removed.
         */
        void masterZnodeRemoved();
        
        /**
         * Callback to notify clients that masters' keep alive znode was created.
         */
        void masterZnodeCreated();
        
        /**
         * Callback to notify clients that znode for time listeners changed.
         */
        void timeZnodeChanged();
        
        /**
         * Callback to notify clients that znode for time listeners changed.
         */
        void timeZnodeRemoved();
        
        /**
         * Callback to notify that CMW created its assigned update znode.
         * @param znode path to znode.
         */
        void cmwUpdatedUpdateZnode(String znode);
        
        /**
         * Callback to notify clients that update znode was removed.
         */
        void cmwUpdateZnodeRemoved();
        
        /**
         * Callback invoked when a new failover znode was created.
         * @param znode failover znode that was created.
         */
        void cmwFailoverZnodeCreated(String znode);
        
        /**
         * Callback invoked when process observed znode was created.
         */
        void processObservedZnodeCreated();
        
        /**
         * Callback invoked when process observed znode changed.
         */
        void processObservedZnodeChanged();
        
        /**
         * Callback invoked when process observed znode is removed.
         */
        void processObservedZnodeRemoved();
        
        /**
         * Callback invoked to notify that data set to masters' keep alive znode
         * is done.
         * 
         * @param data byte array representing the data that was written to znode.
         * @param error boolean indicating if there was an error. True if an error
         * occurred, false otherwise.
         */
        void masterZnodeDataSetCompleted(byte[] data, boolean error);
        
        /**
         * Callback invoked to notify listeners that znode for time listeners has
         * been created.
         */
        void timeZnodeCreated();
        
        /**
         * Callback invoked to notify ProcessWrappers that an update has been 
         * requested by CMW.
         */
        void processHeartBeatZnodeUpdate();
        
        /**
         * Callback invoked to notify CMWs that the AMW created update znode and
         * it is expecting CMW to update it.
         * @param time the time contained in update znode created by AMW.
         */
        void updateZnodeCreated(long time);
        
        /**
         * Callback invoked to notify the AMW the result of the creation of the
         * update znode.
         * 
         * @param znode path of znode.
         * @param data byte array representing the data that must have been written 
         * to znode on creation time. This will probably be null if the creation succeeded.
         * @param error boolean indicating whether an error occurred or not during
         * creation of znode.
         */
        void updateZnodeCreatedByMaster(String znode, byte[] data, boolean error);
    }
    
    /**
     * Method to be invoked to bind/re-bind to existing znodes
     * Remember that MWs also have CMWs, and depending on which type of MW 
     * will be assigned this DM, some will bind to certain nodes while others
     * won't.
     * @param isActiveMaster boolean indicating whether the MW trying to bind is
     * an active master or not.
     */
    public void bindToZnodes(boolean isActiveMaster) {
        this.isActiveMaster = isActiveMaster;
        this.zk.exists(this.znodeMaster, this, this, null);
        
        //A null znodeToCreateForUpdates refers to a MW trying to bind to znodes,
        //a non-null znode, indicates that a CMW is trying to bind.
        //CMWs only need to bind to their own update znode.
        if (this.znodeToCreateForUpdates != null) {
            this.zk.exists(this.ownCMWFailoverZnode, this, this, null);
            this.zk.exists(this.znodeProcessObserved, this, this, null);
            this.zk.exists(this.znodeToCreateForUpdates, this, this, null);
        } else {
            if (this.isActiveMaster) {
                for (String znForUpdate : this.cmwUpdateZnodes) {
                    logger.info("MW binding to CMW Update: " + znForUpdate);
                    this.zk.exists(znForUpdate, this, this, null);
                }
            }
        }
        this.zk.exists(this.znodeTime, this, this, null);
    }
    
    /**
     * Invoke this method to create the notification flag znode.
     */
    public void createTimeZnodeRemovedFlag() {
        HashMap<String, String> ctx = new HashMap<>();
        ctx.put(ZNODE_TYPE, NOTIFICATION_ZNODE);
        
        this.zk.create(this.safeZnodeRemovalNotificationZnode, "0".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL, this, ctx);
    }
    
    /**
     * Invoke this method when MWs are starting for electing the active master. The
     * masterId that gets to create the time znode first becomes the active master.
     * @param masterId String representing the identifier of a MW.
     * @param znode the znode that will be created for listening to time ticks.
     * @param data byte array representing the data to be stored under the znode.
     */
    public void createTimeZnode(String masterId, String znode, byte[] data) {
        HashMap<String, String> ctx = new HashMap<>();
        ctx.put(REQSTR_MASTER_ID, masterId);
        ctx.put(ZNODE_TYPE, TIME_NODE);
        this.zk.create(znode, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL, this, ctx);
    }
    
    /**
     * Active MW invokes this method to create the keep alive znode, where it will
     * report its health status and the IMWs can poll about active's health status
     * to decide whether a new active master is needed.
     * @param masterId String representing the identifier of the AMW.
     * @param znode String representing the znode that will be created for health
     * status updates.
     * @param data byte array representing the data
     */
    public void createKeepAliveZnode(String masterId, String znode, byte[] data) {
        HashMap<String, String> ctx = new HashMap<>();
        ctx.put(ZNODE_TYPE, KEEP_ALIVE_NODE);
        ctx.put(ZNODE_PAYLOAD, Utils.masterWatcherZnodeDataToString(data));
        
        this.zk.create(znode, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL, this, ctx);
    }
    
    /**
     * Active MW invokes this method to create the process observed znode. It signals
     * active CMWs that it is time for them to deploy their child processes.
     * @param znode String representing the znode to be used to indicate deployment time.
     * @param data byte array representing the data to be written to process observed znode.
     */
    public void createProcessKeepAliveZnode(String znode, byte[] data) {
        HashMap<String, String> ctx = new HashMap<>();
        
        ctx.put(ZNODE_TYPE, PROCESS_KEEP_ALIVE_NODE);
        ctx.put(ZNODE_PAYLOAD, Utils.processObservedZnodeDataToString(data));
        
        this.zk.create(znode, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL, this, ctx);
    }
    
    /**
     * This method is invoked by Active MW to update data in znode, that is, report
     * its health status to IMWs.
     * @param data byte array representing the data to be written to znode.
     */
    public void updateZnodesData(byte[] data) {
        HashMap<String, String> ctx = new HashMap<>();
        ctx.put(ZNODE_TYPE, KEEP_ALIVE_NODE);
        ctx.put(ZNODE_PAYLOAD, Utils.masterWatcherZnodeDataToString(data));
        
        this.zk.setData(this.znodeMaster, data, -1, this, ctx);
    }
    
    /**
     * This method is invoked to read the last update that was pushed to keep alive
     * znode by the Active MW. It will allow the current active MW identify whether
     * it is still the active MW and if it is allow to push a new update. On the other
     * hand, it will allow IMWs identify whether the current active MW is alive or
     * not.
     */
    public void readLastUpdate() {
        HashMap<String, String> ctx = new HashMap<>();
        ctx.put(ZNODE_TYPE, KEEP_ALIVE_NODE);
        this.zk.getData(this.znodeMaster, null, this, ctx);
    }
    
    /**
     * This method is invoked to retrieve the last time tick issued by active
     * TM.
     */
    public void readTimesZnodeLastUpdate() {
        HashMap<String, String> ctx = new HashMap<>();
        ctx.put(ZNODE_TYPE, TIME_NODE);
        this.zk.getData(this.znodeTime, null, this, ctx);
    }
    
    /**
     * This method is invoked by CMWs to update the update znode that was assigned
     * to them and where the AMW is expecting for updates.
     * @param data byte array representing the data that will be written to update znode.
     */
    public void setChildMasterWatcherZnode(byte[] data) {
        HashMap<String, String> ctx = new HashMap<>();
        ctx.put(ZNODE_TYPE, CMW_UPDATE_NODE);
        ctx.put(ZNODE_PAYLOAD, Utils.childMasterWatcherDataToString(data));
        ctx.put(UPDATE_NODE_EVENT, UPDATE_EVENT_IS_CHANGED);
        //this.zk.create(this.znodeToCreateForUpdates, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL, this, ctx);
        this.zk.setData(this.znodeToCreateForUpdates, data, -1, this, ctx);
    }
    
    /**
     * Method invoked by current active MW to create a given update znode and to
     * allow CMW to report its health status.
     * @param znode String representing the update znode that will be created.
     * @param data byte array representing the data that will be set under znode.
     */
    public void createChildMasterWatcherZnodeByActiveMaster(String znode, byte[] data) {
         HashMap<String, String> ctx = new HashMap<>();
        ctx.put(ZNODE_TYPE, CMW_UPDATE_NODE);
        ctx.put(ZNODE_PAYLOAD, Utils.childMasterWatcherDataToString(data));
        ctx.put(UPDATE_NODE_EVENT, UPDATE_EVENT_IS_CREATE);
        this.zk.create(znode, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL, this, ctx);
    }
    
    /**
     * Method invoked by CMWs to temporarily bind to a given heart beat znode. It
     * is normally used by a CMW to obtain an update from a ProcessWrapper.
     * @param heartBeatZnode String representing the znode that will be created.
     */
    public void temporaryBindToHeartBeat(String heartBeatZnode) {
        this.zk.exists(heartBeatZnode, this, this, null);
    }
    
    /**
     * Method invoked by CMWs to create the znode that will be used
     * to signal ProcessWrappers that it is time to report their health statuses. 
     * @param heartBeatZnode String representing the znode where a heart beat will
     * be placed.
     * @param dummyData byte array representing the data. It is not so dummy since
     * it contains a flag indicating whether the CMW is trying to destroy the ProcessWrapper
     * or just trying to get an update from them.
     */
    public void createProcessHeartBeatZnode(String heartBeatZnode, byte[] dummyData) {
        Map<String, String> ctx = new HashMap<>();
        ctx.put(ZNODE_TYPE, "");
        logger.info("Creating heart beat znode: " + heartBeatZnode);
        this.zk.create(heartBeatZnode, dummyData, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL, this, ctx);
    }
    
    /**
     * Method invoked by CMWs to remove heart beat znodes.
     */
    public void removeProcessHeartBeatZnode() {
        this.zk.delete(this.processHeartBeatZnode, -1, this, null);
    }
    
    /**
     * Method invoked by current active MW to read the update pushed by CMW to
     * later add it to its internal global updates queue. NOTE: using this leads
     * to unexpected behavior. Please use the method readCMWUpdateZnodeData.
     */
    @Deprecated
    public void readChildMasterWatcherZnode() {
        HashMap<String, String> ctx = new HashMap<>();
        ctx.put(ZNODE_TYPE, CMW_UPDATE_NODE);
        this.zk.getData(this.znodeToCreateForUpdates, null, this, ctx);
    }
    
    /**
     * Method invoked to remove a znode.
     * @param path String representing the znode to be removed.
     */
    public void removeZnode(String path) {
        this.zk.delete(path, -1, this, null);
    }
    /*
    public void createFailoverZnode(String path) {
        this.zk.create(path, "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL, this, null);
    }*/
    
    /**
     * Method invoked by current active MW to read the update pushed by CMW to
     * later add it to its internal global updates queue.
     * @param path String representing the znode that MW needs to read from.
     */
    public void readCMWUpdateZnodeData(String path) {
        HashMap<String, String> ctx = new HashMap<>();
        ctx.put(ZNODE_TYPE, CMW_UPDATE_NODE);
        ctx.put(UPDATE_NODE_EVENT, UPDATE_EVENT_IS_CHANGED);
        this.zk.getData(path, this, this, ctx);
    }

    @Override
    public void process(WatchedEvent event) {
        String path = event.getPath();
        switch (event.getType()) {
            case NodeDataChanged:
                if (path.equals(this.znodeTime)) {
                    //Time has been updated
                    this.listener.timeZnodeChanged();
                } else if (path.equals(this.znodeMaster)) {
                    this.listener.masterZnodeChanged();
                } else if (path.equals(this.processHeartBeatZnode)) {
                    this.listener.processHeartBeatZnodeUpdate();
                } else if (this.znodeToCreateForUpdates == null && this.cmwUpdateZnodes.contains(event.getPath())) {
                    this.listener.cmwUpdatedUpdateZnode(event.getPath());
                } else if (this.znodeToCreateForUpdates != null && this.znodeToCreateForUpdates.equals(event.getPath())) {
                    this.bindToZnodes(this.isActiveMaster);
                }
                break;
            case None:
                if (event.getState() == Event.KeeperState.Expired ||
                        event.getState() == Event.KeeperState.Disconnected) {
                    this.listener.disconnected(KeeperException.Code.CONNECTIONLOSS.intValue());
                } else if (event.getState() == Event.KeeperState.SyncConnected) {
                    this.listener.connected();
                }
                break;
            case NodeDeleted:
                if (event.getPath().equals(this.znodeTime)) {
                    this.listener.timeZnodeRemoved();
                } else if (event.getPath().equals(this.znodeProcessObserved)) {
                    this.listener.processObservedZnodeRemoved();
                } else if (this.cmwUpdateZnodes.contains(event.getPath())) {
                    this.listener.cmwUpdateZnodeRemoved();
                } 
                break;
            case NodeCreated:
                if (this.ownCMWFailoverZnode != null && this.ownCMWFailoverZnode.equals(event.getPath())) {
                    this.listener.cmwFailoverZnodeCreated(event.getPath());
                } else if (this.znodeToCreateForUpdates != null && this.znodeToCreateForUpdates.equals(event.getPath())) {
                    /**
                     * To comply with the old model, where CMWs created update
                     * znode, instead of waiting for AMW to create them, once CMW
                     * detects that update znode was created by AMW, it will 
                     * automatically read the data from znode, that is, instead
                     * of invoking callback to notify about the creation of znode
                     * by master, it will proceed to read the content and later
                     * notify CMW about the new data.
                     */
                    Map<String, String> ctx = new HashMap<>();
                    ctx.put(ZNODE_TYPE, CMW_UPDATE_NODE);
                    ctx.put(UPDATE_NODE_EVENT, UPDATE_EVENT_IS_CREATE);
                    this.zk.getData(path, this, this, ctx);
                } else if (this.znodeToCreateForUpdates == null && this.cmwUpdateZnodes.contains(event.getPath())) {
                    this.bindToZnodes(this.isActiveMaster);
                } else if (event.getPath().equals(this.znodeMaster)) {
                    this.listener.masterZnodeCreated();
                } else if (event.getPath().equals(this.znodeTime)) {
                    this.listener.timeZnodeCreated();
                } else if (event.getPath().equals(this.znodeProcessObserved)) {
                    this.listener.processObservedZnodeCreated();
                }
                break;
        }
    }
    
    

    /**
     * Callback when deleting znode.
     * @param rc
     * @param path
     * @param ctx 
     */
    @Override
    public void processResult(int rc, String path, Object ctx) {
        switch (KeeperException.Code.get(rc)) {
            case OK:
                //this.listener.znodeRemoved(path, false);
                break;
            case CONNECTIONLOSS:
            case SESSIONEXPIRED:
                this.listener.disconnected(rc);
                break;
            case OPERATIONTIMEOUT:
            case AUTHFAILED:
                //this.listener.znodeRemoved(path, true);
                break;
        }
    }

    /**
     * Callback for setting/binding data.
     * @param rc
     * @param path
     * @param ctx
     * @param stat 
     */
    @Override
    public void processResult(int rc, String path, Object ctx, Stat stat) {
        switch (KeeperException.Code.get(rc)) {
            case OK:
                if (ctx == null) {
                    //We're just binding
                    //Nothing else needs to be done here.
                } else {
                    HashMap<String, String> mCtx = (HashMap<String, String>)ctx;
                    switch (mCtx.get(ZNODE_TYPE)) {
                        case KEEP_ALIVE_NODE:
                            this.listener.masterZnodeDataSetCompleted(
                                    Utils.masterWatcherZnodeDataToBytes(mCtx.get(ZNODE_PAYLOAD)), 
                                    false);
                            break;
                        case CMW_UPDATE_NODE:
                            //Everything went fine, nothing to do.
                            break;
                    }
                }
                break;
            case SESSIONEXPIRED:
            case CONNECTIONLOSS:
                this.listener.disconnected(rc);
                break;
            case OPERATIONTIMEOUT:
            case AUTHFAILED:
                if (ctx == null) {
                    //Bind failed, retry.
                    this.bindToZnodes(this.isActiveMaster);
                } else {
                    HashMap<String, String> mCtx = (HashMap<String, String>)ctx;
                    switch (mCtx.get(ZNODE_TYPE)) {
                        case KEEP_ALIVE_NODE:
                            this.listener.masterZnodeDataSetCompleted(
                                    Utils.masterWatcherZnodeDataToBytes(mCtx.get(ZNODE_PAYLOAD)), 
                                    true);
                            break;
                        case CMW_UPDATE_NODE:
                            this.setChildMasterWatcherZnode(Utils.childMasterWatcherDataToBytes(mCtx.get(ZNODE_PAYLOAD)));
                            break;
                    }
                }
                break;
        }
    }

    /**
     * Callback when creating a znode
     * @param rc
     * @param path
     * @param ctx
     * @param name 
     */
    @Override
    public void processResult(int rc, String path, Object ctx, String name) {
        if (ctx == null) {
            //Operation result is not important, exiting...
            return;
        }
        Map<String, String> cCtx = (HashMap<String, String>)ctx;
        switch (KeeperException.Code.get(rc)) {
            case OK:
                switch (cCtx.get(ZNODE_TYPE)) {
                    case KEEP_ALIVE_NODE:
                        final byte[] dataKA = ((HashMap<String, String>)ctx).get(ZNODE_PAYLOAD).getBytes();
                        new Thread(new Runnable() {

                            @Override
                            public void run() {
                                DataMonitor.this.listener.masterWatcherZnodeCreated(false, dataKA);
                            }
                        }).start();
                        //this.listener.masterWatcherZnodeCreated(false, dataKA);
                        break;
                    case PROCESS_KEEP_ALIVE_NODE:
                        byte[] dataPKA = Utils.processObservedZnodeDataToBytes(cCtx.get(ZNODE_PAYLOAD));
                        this.listener.processDataNodeCreated(false, dataPKA);
                        break;
                    case TIME_NODE:
                        this.listener.masterElected(cCtx.get(REQSTR_MASTER_ID));
                        break;
                    case CMW_UPDATE_NODE:
                        //this.zk.getData(path, null, this, ((HashMap<String, String>)ctx));
                        byte[] dataCMU = Utils.updateZnodeCreatedByMastersDataToBytes(Long.valueOf(cCtx.get(ZNODE_PAYLOAD)));
                        this.listener.updateZnodeCreatedByMaster(path, dataCMU, false);
                        break;
                    case NOTIFICATION_ZNODE:
                        new Thread(new Runnable() {

                            @Override
                            public void run() {
                                DataMonitor.this.listener.recreateMasterZnode(true);
                            }
                        }).start();
                        break;
                }
                break;
            case NODEEXISTS:
                //Master already elected, just return a dummy id
                switch (cCtx.get(ZNODE_TYPE)) {
                    case KEEP_ALIVE_NODE:
                        this.listener.masterElected("-9999999");
                        break;
                    case NOTIFICATION_ZNODE:
                        new Thread(new Runnable() {

                            @Override
                            public void run() {
                                DataMonitor.this.listener.recreateMasterZnode(false);
                            }
                        }).start();
                        break;
                }
                
                break;
            case CONNECTIONLOSS:
            case SESSIONEXPIRED:
                this.listener.disconnected(rc);
                break;
            case OPERATIONTIMEOUT:
            case NOAUTH:
                switch (((HashMap<String, String>)ctx).get(ZNODE_TYPE)) {
                    case KEEP_ALIVE_NODE:
                        byte[] eDataKA = cCtx.get(ZNODE_PAYLOAD).getBytes();
                        this.listener.masterWatcherZnodeCreated(true, eDataKA);
                        break;
                    case PROCESS_KEEP_ALIVE_NODE:
                        byte[] eDataPKA = cCtx.get(ZNODE_PAYLOAD).getBytes();
                        this.listener.processDataNodeCreated(true, eDataPKA);
                        break;
                    case TIME_NODE:
                        this.listener.masterElected(null);
                        break;
                    case CMW_UPDATE_NODE:
                        byte[] eDataCMU = Utils.updateZnodeCreatedByMastersDataToBytes(Long.valueOf(cCtx.get(ZNODE_PAYLOAD)));
                        this.listener.updateZnodeCreatedByMaster(path, eDataCMU, true);
                        break;
                }
                break;
        }
    }    

    /**
     * Callback when reading data
     * @param rc
     * @param path
     * @param ctx
     * @param data
     * @param stat 
     */
    @Override
    public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
        Map<String, String> cCtx = (HashMap<String, String>)ctx;
        switch (KeeperException.Code.get(rc)) {
            case OK:
                switch (cCtx.get(ZNODE_TYPE)) {
                    case KEEP_ALIVE_NODE:
                        this.listener.dataReadFromZnode(data);
                        break;
                    case PROCESS_KEEP_ALIVE_NODE:
                        this.listener.dataReadFromProcessZnode(data);
                        
                        break; 
                    case TIME_NODE:
                        /*
                        Note that it is absolutely necessary to wrap this 
                        callback into a new thread since the method blocks until
                        all CMWs send a heartbeat. Normally, every callback invocation
                        should be wrapped inside a new thread, however now we
                        can just wrap this unique callback.
                        */
                        final byte[] fData = data;
                        new Thread(
                            new Runnable() {

                                @Override
                                public void run() {
                                    DataMonitor.this.listener.timeUpdated(fData);
                                }
                            }
                        ).start();
                        break;
                    case CMW_UPDATE_NODE:
                        if (cCtx.get(UPDATE_NODE_EVENT).equals(UPDATE_EVENT_IS_CREATE)) {
                            final byte[] uData = data;
                            new Thread(new Runnable() {

                                @Override
                                public void run() {
                                    DataMonitor.this.listener.updateZnodeCreated(Long.valueOf(Utils.updateZnodeCreatedByMastersDataToString(uData)));
                                }
                            }).start();
                            
                        } else if (cCtx.get(UPDATE_NODE_EVENT).equals(UPDATE_EVENT_IS_CHANGED)) {
                            this.listener.childMasterWatcherUpdatedZnode(path, data);
                        }
                        
                        break;
                }
                break;
            case CONNECTIONLOSS:
            case SESSIONEXPIRED:
                this.listener.disconnected(rc);
                break;
                
            case OPERATIONTIMEOUT:
            case NOAUTH:
                switch (cCtx.get(ZNODE_TYPE)) {
                    case KEEP_ALIVE_NODE:
                        this.listener.dataReadFromZnode(null);
                        break;
                    case PROCESS_KEEP_ALIVE_NODE:
                        this.listener.dataReadFromProcessZnode(null);
                        break;
                    case TIME_NODE:
                        this.listener.timeUpdated(null);
                        break;
                    case CMW_UPDATE_NODE:
                        if (cCtx.get(UPDATE_NODE_EVENT).equals(UPDATE_EVENT_IS_CREATE)) {
                            this.zk.getData(path, this, this, cCtx);
                        } else if (cCtx.get(UPDATE_NODE_EVENT).equals(UPDATE_EVENT_IS_CHANGED)) {
                            this.listener.childMasterWatcherUpdatedZnode(path, null);
                        }
                        break;
                }
                break;
        }
    }
}
