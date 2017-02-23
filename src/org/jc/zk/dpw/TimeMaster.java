/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.jc.zk.dpw;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import org.apache.log4j.Logger;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.jc.zk.util.AsyncResponseConsumer;
import org.jc.zk.util.AuthorizationQueue;
import org.jc.zk.util.Utils;

/**
 *
 * @author cespedjo
 */
public class TimeMaster implements Watcher, Runnable, TimeDataMonitor.TMInterfaceListener {

    private final long intervalMillis;
    
    private final TimeDataMonitor tdm;
    
    private final String[] ntpServers;
    
    private final String zkHost;
    
    private final String zkPort;
    
    private final String zkNodeForTimeListeners;
    
    private final String masterId;
    
    private boolean imMaster;
    
    private final String zkNodeForTime;
    
    private boolean killSelf;
    
    private long lastUpdate;
    
    private boolean waitingToPushUpdate;
    
    private boolean waitingToReplaceActive;
    
    private boolean clockTicking;
    
    private boolean shouldBindToTimeZnode;
    
    private long prevLastUpdate;
    
    private final CountDownLatch cdl;
    
    private final long maxUpdateMiss;
    
    private boolean ignoreTimeUpdate;
    
    private static final long INITIAL_TIME = Long.MIN_VALUE;
    
    private boolean runningElection;
    
    private long cummulativeTime;
    
    private final String requestAMWKillZkNode;
    
    private long timeAMWFirstKillArrived = 0L;
    
    private static final String TIME_ZNODE_REMOVED_NOTIF_ZNODE = "/dpw0001241564/tm_tzrzn";
    
    private static final long NOTIFICATION_ZNODE_MAX_CREATION_OFFSET = 30000L;
    
    private static final long VERY_LONG_TIME = NOTIFICATION_ZNODE_MAX_CREATION_OFFSET * 15;
    
    public static final String AMW_REQUEST_KILL_CODE_KILL = "kill";
    
    public static final String AMW_REQUEST_KILL_CODE_ALLOW = "allow";
    
    public static final String AMW_REQUEST_KILL_CODE_DENIED = "denied";
    
    public static final String AMW_REQUEST_KILL_FREE = "free";
    
    public static final String AMW_REQUEST_KILL_BUSY = "busy";
    
    public static final long FIXED_AUTHORIZATION_WAIT_TIME = 600000L;
    
    private static final Logger logger = Logger.getLogger(TimeMaster.class);
    
    public TimeMaster (
            String masterId,
            String zkHost,
            String zkPort,
            String zkNode,
            String zkNodeForTimeListeners,
            String requestAMWKillZkNode,
            long intervalMillis, 
            String[] ntpServers) throws IOException {
        
        this.masterId = masterId;
        this.zkHost = zkHost;
        this.zkPort = zkPort;
        this.zkNodeForTime = zkNode;
        this.zkNodeForTimeListeners = zkNodeForTimeListeners;
        this.intervalMillis = intervalMillis;
        this.ntpServers = ntpServers;
        this.requestAMWKillZkNode = requestAMWKillZkNode;
        this.tdm = new TimeDataMonitor(
                this.masterId, 
                this.zkHost, 
                this.zkPort, 
                this.zkNodeForTime, 
                this.zkNodeForTimeListeners,
                TIME_ZNODE_REMOVED_NOTIF_ZNODE,
                this.requestAMWKillZkNode,
                this.ntpServers, 
                this);
        this.killSelf = false;
        this.waitingToPushUpdate = false;
        this.lastUpdate = INITIAL_TIME;
        this.prevLastUpdate = INITIAL_TIME;
        this.clockTicking = false;
        this.cdl = new CountDownLatch(1);
        this.shouldBindToTimeZnode = false;
        this.maxUpdateMiss = this.intervalMillis * 2;
        this.runningElection = false;
        this.ignoreTimeUpdate = false;
        this.waitingToReplaceActive = false;
        this.cummulativeTime = 0L;
    }

    @Override
    public void connected() {
        if (this.cdl.getCount() > 0) {
            this.cdl.countDown();
        }
    }

    private void setWatchers() {
        this.tdm.testBindToZnodeListener(this.shouldBindToTimeZnode);
        this.shouldBindToTimeZnode = false;
    }

    @Override
    public void run() {
        try {
            this.cdl.await();
        } catch (InterruptedException ex) {
            logger.error("Time Master interrupted while waiting for connection with ZK.");
        }
        try {
            logger.info("Time Masters connected to zk, now proceeding to create Time Masters' Keep alive znode.");
            this.tdm.createTimeZnode(
                    Utils.generateDataForTimeZnode(
                            this.masterId, 
                            this.lastUpdate,
                            false,
                            this.ntpServers));
            synchronized (this) {
                logger.info("Waiting for Time Masters' Keep alive creation to be finished.");
                wait();
            }    
        } catch (InterruptedException ex) {
            logger.error("Time Master interrupted while waiting for Keep Alive znode creation to be finished.");
        }
        synchronized (this) {
            while (!this.killSelf) {
                if (!clockTicking) {
                    try {
                        logger.info("Clock not ticking so Time Master will wait.");
                        wait();
                    } catch (InterruptedException ex) {
                        logger.error("Time Master interrupted while waiting for clock tick.", ex);
                        continue;
                    }
                }
                
                try {
                    logger.info("Clock ticking, Time Masters will wait " + this.intervalMillis + " millis before 2 things: Active Master pushing an update and Inactive Masters read update.");
                    wait(this.intervalMillis);
                } catch (InterruptedException ex) {
                    logger.error("Time Master interrupted while waiting before checking/pushing update.", ex);
                    continue;
                }
                
                //If cummulativeTime is not negative, it means that I was allowed
                //to create notification znode. But if it is, it means
                //that I was not allowed to create it. So I wait until a notification
                //znode is removed.
                if (this.cummulativeTime < 0) {
                    try {
                        logger.info("This Time master was not allowed to create notification znode, so it waits until it goes away.");
                        wait(VERY_LONG_TIME);
                        //This means that we waited for a very long time, and notification flag
                        //was never removed (possibly, never created) so we proceed to unlock ourself.
                        if (this.cummulativeTime < 0) {
                            logger.info("Time Master still has negative cummulative time. It is possible that notification znode was long gone (or never created). Proceed to reset status.");
                            this.cummulativeTime = 0L;
                            this.ignoreTimeUpdate = false;
                            this.prevLastUpdate = this.lastUpdate++;
                        }
                    } catch (InterruptedException ex) {
                        logger.error("Time Master interrupted while waiting for notification znode to go away.", ex);
                    }
                }
                
                if (this.ignoreTimeUpdate || this.killSelf) {
                    logger.info("Ignore time update: " + this.ignoreTimeUpdate + ", killSelf: " + this.killSelf);
                    continue;
                }
                
                if (this.lastUpdate == INITIAL_TIME) {
                    if (!this.imMaster) {
                        try {
                            logger.info("Time is initial and this is Inactive Master Watcher, it waits until Keep Alive znode is updated.");
                            wait();
                            this.prevLastUpdate = this.lastUpdate;
                        } catch (InterruptedException ex) {
                            logger.info("Inactive Time Master interrupted while waiting for initial update", ex);
                            continue;
                        }
                    }
                }
                
                this.cummulativeTime += this.intervalMillis;
                
                if (this.imMaster) {
                    this.waitingToPushUpdate = true;
                    //Get data to verify active master.
                    this.tdm.getDataFromTimeZnode();
                    logger.info("Active Time Master will retrieve data from keep alive znode to verify it is still active.");
                    try {
                        logger.info("Active Time Master now waiting until zk returns data from keep alive znode.");
                        wait();
                    } catch (InterruptedException ex) {
                        logger.error("Active Time Master interrupted while retrieving keep alive's data from zk.", ex);
                    }
                } else {
                    long timeDiff;
                    logger.info("Inactive Time Master checking if last update minus previous last update is within time constraint.");
                    try {
                        timeDiff = this.lastUpdate > this.prevLastUpdate ?
                                this.lastUpdate - this.prevLastUpdate :
                                Utils.getNetworkTime(this.ntpServers) - this.lastUpdate;
                    } catch (Exception ex) {
                        logger.error("Error while retrieving network time", ex);
                        timeDiff = this.lastUpdate - this.prevLastUpdate;
                    }
                    logger.info("Inactive Time Master inner clock difference is: " + timeDiff);
                    if (timeDiff > this.maxUpdateMiss) {
                        logger.info("Inactive Time Master will verify that it isn't him that's failing.");
                        //Verify that it isn't me the one thread that failed to 
                        //regularly update inner clock before accusing ATM of being dead.
                        this.waitingToReplaceActive = true;
                        this.tdm.getDataFromZnodeForTimeListeners();
                        
                        try {
                            logger.info("Inactive Time Master waiting until data from znode for time listeners returns data.");
                            wait();
                        } catch (InterruptedException ex) {
                            logger.error("Inactive Time Master interrupted while waiting for data from time listeners znode.", ex);
                        }
                    }
                    logger.info("Inactive Time Master set previous last update to last update.");
                    this.prevLastUpdate = this.lastUpdate;
                    //Everything is fine, go back to sleep.
                    
                }
            }
        }
    }

    @Override
    public void process(WatchedEvent event) {
        this.tdm.process(event);
    }

    public void masterElected(String idOfMasterElected) {
        logger.info("Inactive Time Master with ID: " + this.masterId + ", received new master is: " + idOfMasterElected);
        this.imMaster = this.masterId.equals(idOfMasterElected);
        this.shouldBindToTimeZnode = true;
        this.setWatchers();
        
        if (this.imMaster) {
            logger.info("ATM now initializing " + this.requestAMWKillZkNode + "with code " + AMW_REQUEST_KILL_FREE);
            this.tdm.setRequestAMWKillZnodeData(Utils.requestAMWKillZnodeDataToBytes(AMW_REQUEST_KILL_FREE, Utils.AMW_PAYLOAD_TYPE_INIT, this.masterId));
        }
        logger.info("After initializing " + this.requestAMWKillZkNode + " ATM continues...");
        
        synchronized (this) {
            notify();
        }
    }

    @Override
    public void activeMasterPushedUpdate(byte[] data, boolean error) {
        this.shouldBindToTimeZnode = true;
        this.setWatchers();
        if (!this.imMaster) {
            boolean notifyThread = this.lastUpdate == INITIAL_TIME;
            if (error) {
                logger.info("Inactive Time Master will retry to get data from time listeners znode cause zk failed.");
                this.tdm.getDataFromZnodeForTimeListeners();
            } else {
                if (this.waitingToReplaceActive) {
                    logger.info("Inactive Time Master is now checking if Active Time Master pushed a heart beat or not, with data from time listeners znode.");
                    long timeSentByActive = Utils.getTimeFromTimeZnode(data);
                    try {
                        long currenTime = Utils.getNetworkTime(this.ntpServers);
                        if (currenTime - timeSentByActive > this.maxUpdateMiss) {
                            logger.info("Active Time Master failed to push heart beats within time constraints. Competition for new active master begins.");
                            synchronized (this) {
                                //The ATM didn't update its status, it might be down.
                                this.ignoreTimeUpdate = true;
                                this.noLongerMaster();
                            }
                        }
                    } catch (Exception ex) {
                        logger.error("Inactive Time Master failed retrieving time from NTP", ex);
                    } finally {
                        synchronized (this) {
                            this.waitingToReplaceActive = false;
                            notify();
                        }
                    }
                } else {
                    this.cummulativeTime = 0L;
                    this.lastUpdate = this.ignoreTimeUpdate ? this.lastUpdate : Utils.getTimeFromTimeZnode(data);

                    if (notifyThread) {
                        synchronized (this) {
                            notify();
                        }
                    }
                }
            }
        }
    }
    
    @Override
    public void updatePushedToTimeListenersZnode(byte[] data, boolean error) {
        this.shouldBindToTimeZnode = true;
        this.setWatchers();
        if (this.imMaster) {
            //boolean notifyThread = this.lastUpdate == INITIAL_TIME;
            if (error) {
                logger.info("Active Time Master failed to push an update to time listeners due to zk error. Retrying.");
                //If I'm master, write update to znode where master watchers are observing.
                this.tdm.writeTimeForProcessMasters(data);
            } else {
                logger.info("Active Time Master succesfully pushed an update to time listeners znode.");
                this.cummulativeTime = 0L;
                this.lastUpdate = Utils.getTimeFromTimeZnode(data);
                if (this.prevLastUpdate == INITIAL_TIME) {
                    this.prevLastUpdate = this.lastUpdate;
                }
                /*if (notifyThread) {
                    synchronized (this) {
                        notify();
                    }
                }*/
                synchronized(this) {
                    notify();
                }
            }
        }
    }

    @Override
    public void updatePushedToTimeZnode(byte[] data, boolean error) {
        if (error) {
            if (this.imMaster) {
                logger.info("Active Time Master failed to push an update to keep alive znode. Retrying.");
                this.tdm.updateZNode(data);
            }
        } else {
            if (this.imMaster) {
                synchronized (this) {
                    if (this.timeAMWFirstKillArrived != 0L) {
                        logger.info("Active Time Master successfully pushed an update to keep alive znode, but an election is in progress for a new AMW, so ATM will not push an update to time listeners' znode.");
                        this.lastUpdate = Utils.getTimeFromTimeZnode(data);
                        this.cummulativeTime = 0L;
                        logger.info("New lastUpdate is: " + this.lastUpdate);
                        notify();
                        return;
                    }
                }
                logger.info("Active Time Master successfully pushed an update to keep alive znode, now writing update to time listeners znode.");
                this.tdm.writeTimeForProcessMasters(data);
            }
        }
    }

    @Override
    public void retrievedTimeZnodeLastUpdate(byte[] data, boolean error) {
        if (error) {
            //Get data to verify active master.
            logger.info("Time Master was waiting for data from Keep alive znode to come from zk, but an error occurred. Retrying.");
            this.tdm.getDataFromTimeZnode();
            return;
        }
        
        if (this.waitingToPushUpdate) {
            this.waitingToPushUpdate = false;
            
            long time;
            try {
                time = Utils.getNetworkTime(this.ntpServers);
            } catch (Exception ex) {
                logger.error("Active Time Master failed to retrieve time from znode.", ex);
                //Valid because datanode has its time synced with NTP.
                time = System.currentTimeMillis();
                /*
                The only problem that may arise here is that, because current
                time couldn't be retrieved, the time znode will be updated
                regardless of the max threshold. This will cause the master to
                push its update when it was supposed to kill itself, however,
                ITMs detect this when they wake up to check their inner clock.
                If the update was pushed later than the max threshold, they will
                dismiss this because the ignoreTimeUpdate flag is set to true.
                */
            }
            
            if (this.lastUpdate == INITIAL_TIME) {
                logger.info("Active Time Master initializing inner clock.");
                this.tdm.updateZNode(Utils.generateDataForTimeZnode(
                                    this.masterId, 
                                    time,
                                    false,
                                    this.ntpServers));
                this.lastUpdate = time;
            } else  {
                String activeId = Utils.getIdOfMasterFromTimeZnode(data);
                if (!activeId.equals(this.masterId)) {
                    this.imMaster = false;
                    logger.info("Active Time Master verified that the checksum from Keep Alive znode does not match his. It is no longer active and it is killing itself.");
                    this.noLongerMaster();

                } else {
                    logger.info("Active Time Master verified that the checksum from Keep Alive znode matches his. Now it will verify that it has time to push an update.");
                
                    if ((time - this.lastUpdate) >= this.maxUpdateMiss) {
                        //Verify if this master should remain active.
                        logger.info("Active Time Master failed to push an update within time constraints. It is no longer active master. Timediff is:" + (time - this.lastUpdate));
                        this.noLongerMaster();
                        synchronized (this) {
                            notify();
                        }
                    } else {
                        //Now push an update to let ITMs know that you're still an active master.
                        logger.info("Pushing update to keep alive znode. Time: " + time);
                        synchronized (this) {
                            this.tdm.updateZNode(
                                        Utils.generateDataForTimeZnode(
                                                this.masterId, 
                                                time,
                                                this.timeAMWFirstKillArrived != 0L,
                                                this.ntpServers));
                        }
                    }
                }
            }
        } else if (!this.imMaster) {
            logger.info("IMWs have to verify whether the ATM is pushing an update to time listeners or not. If it is not, they must update their inner clock now and must not wait for time listeners' znode to be updated.");
            if (Utils.imwMustUpdateInnerClock(data)) {
                synchronized (this) {
                    this.lastUpdate = Utils.getTimeFromTimeZnode(data);
                    logger.info("IMWs updating inner clock now. Time is: " + this.lastUpdate);
                }
            } else {
                logger.info("IMWs do not need to update inner clock now, ATM is pushing an update to time listeners.");
            }
        }
    }

    public void noLongerMaster() {
        synchronized (this) {
            if (this.imMaster) {
                logger.info("Active Time Master is no longer active, it will kill itself.");
                this.killSelf = true;
                notify();
            } else {
                try {
                    //The wait that ends first will try to create the master's znode.
                    logger.info("Inactive Time masters wait " + this.intervalMillis + " millis before proceeding to competition.");
                    wait(this.intervalMillis);
                } catch (InterruptedException ex) {
                    logger.error("Inactive Time Master interrupted while waiting to compete for active mastership.", ex);
                } finally {
                    try {
                        long currentTime = Utils.getNetworkTime(this.ntpServers);
                        long timeDiff = (currentTime - this.cummulativeTime) - (this.lastUpdate + NOTIFICATION_ZNODE_MAX_CREATION_OFFSET);
                        if (timeDiff < 0) {
                            logger.info("Inactive Time Master still has time to compete for active mastership. TimeDiff is " + timeDiff);
                            this.runningElection = true;
                            this.tdm.createTimeZnodeRemovedFlag();
                        } else {
                            logger.info("Inactive Time Master does not have time to compete for active mastership. TimeDiff is " + timeDiff);
                            this.tdm.bindOnceToNotificationZnode();
                            this.cummulativeTime = -1;
                        }
                    } catch (Exception ex) {
                        logger.error("Inactive Time Master failed to retrieve NTP time.", ex);
                        //Assume that it is late to run for master, so he does not participate
                        //in the competition.
                    }
                }
            }
        }
    }

    public void stopTimeTick() {
        synchronized (this) {
            logger.info("Stopping time tick.");
            this.clockTicking = false;
        }
    }

    public void startTimeTick() {
        synchronized(this) {
            logger.info("Starting time tick.");
            this.clockTicking = true;
            notify();
        }
    }

    @Override
    public void timeZnodeChanged() {
        this.shouldBindToTimeZnode = true;
        this.setWatchers();
        logger.info("Keep Alive znode changed.");
        //If time znode changed, ITMs don't need to do anything else. They just
        //update their inner clock once the timeListenersZnode changes. Verify
        //UPDATE: Since the inclusion of a new field in masters znode payload,
        //IMWs must verify update to update the inner clock if the master is not
        //pushing an update to time listeners znode.
        this.tdm.getDataFromTimeZnode();
    }

    @Override
    public void timeListenersZnodeChanged() {
        //Rebind watchers.
        this.shouldBindToTimeZnode = true;
        this.setWatchers();
        logger.info("Time Listeners znode changed.");
        if (!this.imMaster) {
            //Remember that master has access to the data since it is the one
            //pushing the update. ITMs are the ones who need to retrieve the update.
            logger.info("Inactive Time Masters retrieving data from znode from time listeners.");
            this.tdm.getDataFromZnodeForTimeListeners();
        }
    }

    @Override
    public void disconnected(int rc) {
        logger.info("Time Master with ID: " + this.masterId + ", disconnected.");
        synchronized (this) {
            this.killSelf = true;
            notify();
        }
    }

    @Override
    public void timeZnodeDeleted() {
        logger.info("Keep Alive znode removed.");
        this.shouldBindToTimeZnode = true;
        this.setWatchers();
        if (!this.runningElection) {
            logger.info("Keep Alive znode was removed, but an election is not in progress. Invoke active mastership competition.");
            this.noLongerMaster();
        }
        //If already competing for new Master, ignore this callback. Why?
        //Because it was possibly triggered by the thread which was allowed to
        //remove the existing time znode.
    }

    @Override
    public void timeListenersZnodeDeleted() {
        logger.info("Time Listeners znode removed.");
        this.shouldBindToTimeZnode = true;
        this.setWatchers();
        if (this.timeAMWFirstKillArrived == 0L) {
            logger.info("An election is not in progress stopping time tick.");
            this.stopTimeTick();
        } else {
            logger.info("An election is in progress ignoring removal of time listeners' znode.");
        }
    }

    @Override
    public void timeZnodeCreated(String creatorId) {
        logger.info("Keep Alive znode created by this id: " + creatorId);
        //This would normally be handled by the processResult method, since
        //every TM will issue a create command. However, on master elected,
        //bind watchers.
        this.prevLastUpdate = this.lastUpdate;
        //Make at least a second later since the ITMs will compare it against 
        //current time when the execution resumes and it is going to believe it is
        //outdated, causing a new mastership competition. If they're different,
        //no comparison against current time will be performed.
        if (this.ignoreTimeUpdate) {
            ++this.lastUpdate;
            this.cummulativeTime = 0L;
        }
        this.ignoreTimeUpdate = false;
        this.runningElection = false;
        this.masterElected(creatorId);
    }

    @Override
    public void timeListenersZnodeCreated() {
        logger.info("Verify if this is a re-bind to time listeners znode or is actually being created.");
        if (!this.clockTicking) {
            logger.info("Time Listeners znode created.");
            this.shouldBindToTimeZnode = true;
            this.setWatchers();
            this.startTimeTick();
        } else {
            logger.info("Re-binding to Time listeners znode or election is finished.");
        }
    }

    @Override
    public void recreateTimeZnode(boolean allowedToAttempTimeZnodeRemoval) {
        logger.info("Inactive Time Master with ID: " + this.masterId + ", is allowed to create keep alive znode: " + allowedToAttempTimeZnodeRemoval);
        synchronized (this) {
            if (allowedToAttempTimeZnodeRemoval) {
                //this.tdm.removeTimeZnode();
                try {
                    logger.info("Waiting 5000 millis before proceeding to create keep alive znode.");
                    wait(5000);
                } catch (InterruptedException ex) {
                    logger.error("Interrupted while waiting to remove keep alive znode.", ex);
                }
            } else {
                try {
                    //This thread is not allowed to remove the time znode, so just give
                    //a few seconds to the thread that is allowed to do this before
                    //attempting to create.
                    wait(10000);
                    logger.info("Waiting 10000 millis, until keep alive znode is removed by another master.");
                } catch (InterruptedException ex) {
                    logger.error("Interrupted while waiting for another master to remove keep alive znode.", ex);
                }
            }
            this.timeZnodeCreated(allowedToAttempTimeZnodeRemoval ? this.masterId : "-9999999");
            try {
                wait();
            } catch (InterruptedException ex) {
                logger.error("Interrupted while waiting for new keep alive znode to be finished.", ex);
            }
            this.tdm.removeTimeZnodeRemovedFlag();
            logger.info("Removing notification flag znode.");
        }
    }
    
    @Override
    public void notificationZnodeRemoved() {
        logger.info("Notification flag znode removed.");
        this.shouldBindToTimeZnode = true;
        this.tdm.testBindToZnodeListener(this.shouldBindToTimeZnode);
        if (this.cummulativeTime < 0) {
            this.cummulativeTime = 0L;
            this.ignoreTimeUpdate = false;
            this.prevLastUpdate = this.lastUpdate++;
            synchronized (this) {
                notify();
            }
        }
    }
    
    @Override
    public void notificationZnodeCreated() {
        logger.info("Notification flag znode created.");
        this.tdm.bindOnceToNotificationZnode();
    }

    @Override
    public void requestAMWKillZnodeChanged() {
        this.tdm.testBindToZnodeListener(this.shouldBindToTimeZnode);
        if (this.imMaster) {
            this.tdm.getRequestAMWKillZnodeData();
        }
    }

    @Override
    public void requestAMWKillZnodeDataRead(byte[] data, boolean error) {
        final Object innerLock = new Object();
        
        if (error) {
            logger.info("An error occurred while reading update from: " + this.requestAMWKillZkNode + ". Retrying read.");
            this.tdm.getRequestAMWKillZnodeData();
            return;
        }
        
        String type = Utils.getTypeFromRequestAmwKillZnodeData(data);
        if (type.equals(Utils.AMW_PAYLOAD_TYPE_RESTORE) && Utils.getDataFromRequestAmwKillZnodeData(data).equals(AMW_REQUEST_KILL_FREE)) {
            logger.info("TM just got notified that " + this.requestAMWKillZkNode + " got restored. Resetting requests' time.");
            this.timeAMWFirstKillArrived = 0L;
            return;
        }
        
        if (type.equals(Utils.AMW_PAYLOAD_TYPE_INIT) || type.equals(Utils.AMW_PAYLOAD_TYPE_RESTORE) 
                || type.equals(Utils.AMW_PAYLOAD_TYPE_RESPONSE)) {
            logger.info("Time Master got a type: " + type + " but we're only interested in REQUEST: " + Utils.AMW_PAYLOAD_TYPE_REQUEST);
            return;
        }   
        
        if (this.imMaster) {
            logger.info("ATM is now checking content from: " + this.requestAMWKillZkNode + ".");
            String sData = Utils.requestAMWKillZnodeDataToString(data);
            String dataPayload = Utils.getDataFromRequestAmwKillZnodeData(sData);
            
            if (this.timeAMWFirstKillArrived == 0L) {
                logger.info("Spawning response's consumer thread.");
                AsyncResponseConsumer arc = new AsyncResponseConsumer(FIXED_AUTHORIZATION_WAIT_TIME, this.tdm);
                
                new Thread(arc).start();
            }
            
            if (dataPayload.equals(AMW_REQUEST_KILL_CODE_KILL) && this.timeAMWFirstKillArrived == 0L) {
                logger.info(this.requestAMWKillZkNode + " has a request KILL and time is 0L. Granting permission.");
                this.timeAMWFirstKillArrived = System.currentTimeMillis();
                AuthorizationQueue.enqueueResponse(Utils.requestAMWKillZnodeDataToBytes(AMW_REQUEST_KILL_CODE_ALLOW, Utils.AMW_PAYLOAD_TYPE_RESPONSE, Utils.getRequesterFromRequestAmwKillZnodeData(sData)));
                //this.tdm.setRequestAMWKillZnodeData(Utils.requestAMWKillZnodeDataToBytes(AMW_REQUEST_KILL_CODE_ALLOW, Utils.AMW_PAYLOAD_TYPE_RESPONSE, Utils.getRequesterFromRequestAmwKillZnodeData(sData)));
                logger.info("ATM is spawning thread that will clear " + this.requestAMWKillZkNode + "'s status and set it to FREE.");
                this.tdm.triggerAsyncAMWRequestKillZnodeRestore(Utils.requestAMWKillZnodeDataToBytes(AMW_REQUEST_KILL_FREE, Utils.AMW_PAYLOAD_TYPE_RESTORE, this.masterId), FIXED_AUTHORIZATION_WAIT_TIME + 60000);
            } else if (dataPayload.equals(AMW_REQUEST_KILL_CODE_KILL) && this.timeAMWFirstKillArrived != 0L){
                logger.info("ATM received a request to KILL AMW, however, this is not first request. Time is: " + this.timeAMWFirstKillArrived + ". Denying with DENIED.");
                
                AuthorizationQueue.enqueueResponse(Utils.requestAMWKillZnodeDataToBytes(AMW_REQUEST_KILL_CODE_DENIED, Utils.AMW_PAYLOAD_TYPE_RESPONSE, Utils.getRequesterFromRequestAmwKillZnodeData(sData)));
                //this.tdm.setRequestAMWKillZnodeData(Utils.requestAMWKillZnodeDataToBytes(AMW_REQUEST_KILL_CODE_DENIED, Utils.AMW_PAYLOAD_TYPE_RESPONSE, Utils.getRequesterFromRequestAmwKillZnodeData(sData)));
            } else if (dataPayload.equals(AMW_REQUEST_KILL_FREE)) {
                logger.info("TM just got notified that " + this.requestAMWKillZkNode + " got restored. Resetting requests' time.");
                this.timeAMWFirstKillArrived = 0L;
            }
            //Ignores if the call is caused by a BUSY set.
            logger.info("ATM will ignore the permission request since IMW just set it to BUSY.");
        }
    }

    @Override
    public void requestAMWKillZnodeDataSet(byte[] data, boolean error) {
        if (error) {
            logger.info("An error occurred while ATM was trying to set: " + this.requestAMWKillZkNode + ". Retrying.");
            this.tdm.setRequestAMWKillZnodeData(data);
        }
        logger.info("ATM set data of: " + this.requestAMWKillZkNode + " correctly.");
    }
}
