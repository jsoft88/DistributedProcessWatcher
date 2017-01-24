/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.jc.zk.dpw;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import org.apache.log4j.Logger;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.jc.zk.process.ProcessWrapper;
import org.jc.zk.util.ProcessStreamConsumer;
import org.jc.zk.util.Utils;

/**
 *
 * @author cespedjo
 */
public class Master implements 
        Watcher, Runnable, DataMonitor.DataMonitorListenerMaster {
    
    private final DataMonitor dm;
    
    private final String zkHost;
    
    private final String zkPort;
    
    private final ZooKeeper zk;
    
    private final String zkMasterStatusNode;
    
    private final String programToWatch;
    
    private final String[] argsForProgram;
    
    private final String zkWatchedProgramNode;
    
    private final String zkTimeNode;
    
    private final String zkNodeToCreateForUpdate;
    
    private boolean active;
    
    private long lastUpdate;
    
    private final String masterIdentifier;
    
    private final String[] ntpServers;
    
    private byte[] prevData;
    
    private boolean killSelf;
    
    private final boolean child;
    
    private boolean activeChild;
    
    private final String[] cmwsZnodesToListenTo;
    
    private final int numberOfCMW;
    
    private CountDownLatch cdl;
    
    private final CountDownLatch connCd;
    
    private final long timeTickInterval;
    
    private final StringBuilder updateQueueAsString;
    
    private Process p;
    
    private final List<String> cmwsThatUpdated;
    
    private final List<String> cmwsFailingToUpdate;
    
    private final List<String> cmwsInDanger;
    
    private boolean runningElection;
    
    private boolean ignoreTimeTicks;
    
    private long waitTimeToCheckActiveMastersUpdate;
    
    private CountDownLatch processUpdateWaitCountdown;
    
    private final String heartBeatZnode;
    
    private final String parentMasterWatcherId;
    
    private int heartBeatMisses;
    
    private String activeMasterId;
    
    private final long maxProcessHeartBeatWait;
    
    private CountDownLatch itmWaitCountdown;
    
    private CountDownLatch perUpdateZnodeWaitCountdown;
    
    private final long maxForgiveMeMillis;
    
    private final String hardKillScript;
    
    private final String amwRequestKillZnode;
    
    private static final int MAX_HEARTBEAT_MISS = 3;
    
    //private static final long MAX_CHECKUP_MISS = 95000L;
    
    private static final long SAFETY_ELECTION_WAIT_TIME_MILLIS = 20000L;
    
    private static final long INITIAL_TIME = Long.MIN_VALUE;
    
    private static final String FAILOVER_ZNODE_SUFFIX = "_failover";
    
    private static final String FAILOVER_NAME_PATTERN = "#_" + FAILOVER_ZNODE_SUFFIX;
    
    private static final String TIME_ZNODE_REMOVED_NOTIF_ZNODE = "/dpw0001241564/mw_tzrzn";
    
    public static final String ZNODE_FOR_UPDATE_REQUEST = "/dpw0001241564/pr_upd_now_#";
    
    private static final long NOTIFICATION_ZNODE_MAX_CREATION_OFFSET = 30000L;
    
    private static final Logger logger = Logger.getLogger(Master.class);
    
    public Master (
            String identifier, 
            String zkHost, 
            String zkPort,
            String zkTimeNode,
            String zkMasterStatusNode,
            String zkWatchedProgramNode,
            String programToWatch,
            String[] argsForProgram,
            boolean child,
            boolean activeChild,
            int numberOfCMW,
            long timeTickInterval,
            String parentMasterWatcherId,
            long maxForgiveMeMillis,
            String hardKillScript,
            String amwRequestKillZnode,
            String[] cmwsZnodesToListenTo,
            String zkNodeToCreateForUpdate,
            String[] ntpServers) throws IOException, Exception {
        this.lastUpdate = INITIAL_TIME;
        this.zkHost = zkHost;
        this.zkPort = zkPort;
        this.zk = new ZooKeeper(zkHost + ":" + zkPort, 3000, this);
        this.active = false;
        this.child = child;
        this.zkMasterStatusNode = zkMasterStatusNode;
        this.zkWatchedProgramNode = zkWatchedProgramNode;
        this.programToWatch = programToWatch;
        this.argsForProgram = argsForProgram;
        this.masterIdentifier = identifier;
        this.parentMasterWatcherId = parentMasterWatcherId;
        this.ntpServers = ntpServers;
        this.zkTimeNode = zkTimeNode;
        this.zkNodeToCreateForUpdate = zkNodeToCreateForUpdate;
        this.heartBeatZnode = this.child ? 
                ZNODE_FOR_UPDATE_REQUEST.replace("#", this.masterIdentifier) :
                null;
        this.amwRequestKillZnode = amwRequestKillZnode;
        this.dm = new DataMonitor(
                this.zk, 
                this.zkMasterStatusNode, 
                this.zkTimeNode, 
                this.zkWatchedProgramNode, 
                this.zkNodeToCreateForUpdate,
                this.child ?
                        FAILOVER_NAME_PATTERN.replace("#", this.zkNodeToCreateForUpdate) :
                        null,
                TIME_ZNODE_REMOVED_NOTIF_ZNODE,
                Arrays.asList(cmwsZnodesToListenTo),
                this.heartBeatZnode,
                this.amwRequestKillZnode,
                this, 
                this);
        this.killSelf = false;
        this.numberOfCMW = numberOfCMW;
        this.cmwsZnodesToListenTo = cmwsZnodesToListenTo;
        this.updateQueueAsString = new StringBuilder();
        this.timeTickInterval = timeTickInterval;
        this.cmwsThatUpdated = new ArrayList<>();
        //Initially all znodes that will be created by CMWs are failing to update.
        this.cmwsFailingToUpdate = new ArrayList<>(Arrays.asList(this.cmwsZnodesToListenTo));
        this.cmwsInDanger = new ArrayList<>();
        //If this isn't an instance of CMW, always pass false as an argument.
        this.activeChild = activeChild;
        this.connCd = new CountDownLatch(1);
        this.runningElection = false;
        this.ignoreTimeTicks = false;
        this.waitTimeToCheckActiveMastersUpdate = this.timeTickInterval + this.timeTickInterval / 4L;
        this.heartBeatMisses = 0;
        //When deploying a CMW, timeTickInterval should be equals to time interval,
        //that AMW will wait for update queue to fill up, divided by number of CMWs.
        this.maxProcessHeartBeatWait = timeTickInterval;
        //this.timeTickInterval / Math.max(this.numberOfCMW, 1);
        this.activeMasterId = null;
        this.p = null;
        this.itmWaitCountdown = new CountDownLatch(1);
        this.maxForgiveMeMillis = maxForgiveMeMillis;
        this.hardKillScript = hardKillScript;
    }

    @Override
    public void process(WatchedEvent event) {
        this.dm.process(event);
    }

    @Override
    public void connected() {
        this.connCd.countDown();
        this.setWatchers();
    }
    
    public void setWatchers() {
        this.dm.bindToZnodes(this.active);
    }

    @Override
    public void run() {
        try {
            this.connCd.await();
        } catch (InterruptedException ex) {
            logger.error("Error while waiting for zk connection: " + ex.getMessage());
        }
        
        if (!this.child) {
            try {
                logger.info("Master Watcher sleep for 5000 millis...");
                Thread.sleep(5000);
            } catch (InterruptedException ex) {
                logger.error("Master Watcher 5000 Millis sleep interrupted...");
            }
            logger.info("Competition for mastership will start");
            //Remember CMW cannot compete to become ATM.
            this.dm.createTimeZnode(
                    this.masterIdentifier, this.zkTimeNode, 
                    Utils.generateDataForTimeZnode(this.masterIdentifier, INITIAL_TIME, this.ntpServers));
        }
        
        synchronized(this) {
            try {
                while (!this.killSelf) {
                    logger.info("Master Watcher going to wait until killself is set to true");
                    wait();
                }
            } catch (InterruptedException ex) {
                logger.info("Master Watcher interrupted while waiting in main loop...", ex);
            }
        }
    }

    public void closing(int rc) {
        //notify someone that master will be unable to verify status of other
        //masters and that some action should be taken.
        synchronized(this) {
            logger.info("Master Watcher now set to killself with code: " + rc);
            this.killSelf = true;
            notify();
        }
    }

    @Override
    public void timeUpdated(byte[] data) {
        synchronized (this) {
            if (this.killSelf) {
                return;
            }
            
            if (this.runningElection) {
                return;
            }
        }
        
        if (data == null) {
            logger.info("Tried to read last update from masters' keep alive, but failed. Retrying now.");
            this.dm.readTimesZnodeLastUpdate();
            return;
        }
        
        if (!this.child && !this.active) {
            this.itmWaitCountdown = new CountDownLatch(1);
        }
        
        long currentTime = Utils.getTimeFromTimeZnode(data);
        
        if (this.lastUpdate == INITIAL_TIME) {
            if (this.active) {
                try {
                    this.prevData = 
                                Utils.generateDataForZNode(
                                        currentTime, 
                                        this.updateQueueAsString.toString(), 
                                        this.masterIdentifier, 
                                        this.ntpServers);
                    logger.info("First time tick received, creating masters' keep alive znode.");
                    this.dm.createKeepAliveZnode(this.masterIdentifier, this.zkMasterStatusNode, this.prevData);
                } catch (Exception ex) {
                    logger.error("Failed to prepare data for masters' keep alive znode to be created.", ex);
                }
            }/* else if (this.child) {
                this.lastUpdate = currentTime;
            }*/
        } else {
            if (this.active) {
                long waitMillisPerZnode = this.timeTickInterval / this.numberOfCMW;
                boolean allOk = true;
                for (String aZnode : this.cmwsZnodesToListenTo) {
                    this.perUpdateZnodeWaitCountdown = new CountDownLatch(1);
                    this.dm.createChildMasterWatcherZnodeByActiveMaster(
                            aZnode,
                            Utils.updateZnodeCreatedByMastersDataToBytes(currentTime));
                    try {
                        boolean expired = !this.perUpdateZnodeWaitCountdown.await(waitMillisPerZnode, TimeUnit.MILLISECONDS);
                        if (expired) {
                            allOk = false;
                        }
                    } catch (InterruptedException ex) {
                        logger.error("Master interrupted while waiting for CMW to update: " + aZnode, ex);
                    }
                }
                
                for (String  aZnode : this.cmwsZnodesToListenTo) {
                    this.dm.removeZnode(aZnode);
                }
                
                if (!allOk) {
                    logger.info("Active Master's wait time exhausted and some CMWs failed to push update.");
                    //The waiting ended but not due to countdown reaching 0.
                    //It probably just expired and we didn't receive updates from CMWs.
                    //We cannot push an update for IMWs to know that we're ok, thus
                    //we don't retrieve the last update.
                    Iterator<String> itUpdated = this.cmwsFailingToUpdate.iterator();
                    while (itUpdated.hasNext()) {
                        String zn = itUpdated.next();
                        for (int i = 0; i < this.cmwsThatUpdated.size(); ++i) {
                            if (zn.equals(this.cmwsThatUpdated.get(i))) {
                                itUpdated.remove();
                                break;
                            }
                        }
                    }
                    //Add all CMWs that fail to update.
                    this.cmwsInDanger.addAll(this.cmwsFailingToUpdate);
                    logger.info("Number of CMWs that failed to update: " + this.cmwsInDanger.size());
                    if (currentTime - this.lastUpdate > this.maxForgiveMeMillis) {
                        //Your children caused you to fail. Kill'em all and then yourself.
                        logger.info("Active Master Watcher order to kill itself due to children failing to update.");
                        /*********************************************************************************
                        for (String znodeInDanger : this.cmwsInDanger) {
                            this.dm.createFailoverZnode(FAILOVER_NAME_PATTERN.replace("#", znodeInDanger));
                        }*********************************************************************************/
                        this.electNewMaster();
                    }
                    this.cmwsFailingToUpdate.clear();
                    this.cmwsFailingToUpdate.addAll(Arrays.asList(this.cmwsZnodesToListenTo));
                    
                    return;
                }
                
                logger.info("Active Master Watcher managed to obtain full update from CMWs.");
                //Countdown reached zero, all CMWs reported their status, we're clear
                //to update our heart beat.
                //Read the last time that an update was sent by the AMW.
                logger.info("Active Master Watcher retrieve the last update you pushed.");
                this.lastUpdate = currentTime;
                this.dm.readLastUpdate();
            } else if (!this.child && !this.active) {
                this.waitTimeToCheckActiveMastersUpdate = this.timeTickInterval + this.timeTickInterval / 4L;
                logger.info("Inactive Master Watcher will wait: " + this.waitTimeToCheckActiveMastersUpdate + " before checking if Active Master Watcher pushed an update within time constraints.");
                //Do not update inner clock of IMWs if they want to compete for mastership.
                if (this.ignoreTimeTicks) {
                    logger.info("Inactive Master Watcher now ignoring time ticks because it is competing for mastership.");
                } else {
                    this.lastUpdate = currentTime;
                    logger.info("Inactive Master Watcher updated inner clock.");
                }
                
                try {
                    //wait(waitTimeToCheckActiveMastersUpdate);
                    this.itmWaitCountdown.await(this.waitTimeToCheckActiveMastersUpdate, TimeUnit.MILLISECONDS);
                } catch (InterruptedException ex) {
                    logger.info("Inactive Master Watcher was interrupted while waiting to read update from Active Master Watcher, it will skip checking update until next time tick.", ex);
                } finally {
                    //When the waiting is done, request last update.
                    //Bare in mind that this is all best effort check.
                    logger.info("Inactive Master Watcher waiting done, it will now read update from Active Master Watcher");
                    this.dm.readLastUpdate();
                }
                
            }
        }
    }

    @Override
    public void childMasterWatcherUpdatedZnode(String znode, byte[] data) {
        synchronized (this) {
            if (this.killSelf) {
                return;
            }
            
            if (this.runningElection) {
                return;
            }
        }
        if (!this.child && this.active) {
            if (data == null) {
                logger.info("Active Master Watcher noted that something went wrong with Child Master Watcher creating update znode, so it will read data from update.");
                this.dm.readCMWUpdateZnodeData(znode);
            }
            else {
                //If I'm not child, I might be the active master being notified about
               //an update being available.
               if (this.active && !this.child) {
                   logger.info("Active Master Watcher received an update from Child Master Watcher and will add it to update queue: " + znode);
                   //this.dm.removeZnode(znode);
                   this.cmwsThatUpdated.add(znode);
                   Utils.addUpdateToCMWUpdatesQueue(this.updateQueueAsString, data);
                   //this.cdl.countDown();
                   this.perUpdateZnodeWaitCountdown.countDown();
               }
            }
        }
    }

    @Override
    public void dataReadFromZnode(byte[] data) {
        if (data == null) {
            logger.info("Something went wrong while reading Masters' keep alive znode, retrying.");
            this.dm.readLastUpdate();
        }
        
        synchronized (this) {
            if (this.killSelf) {
                return;
            }
            if (this.runningElection) {
                return;
            }
        }
        if (!this.child && this.active) {
            logger.info("Active Master Watcher received the content for Master's keep alive znode.");
            if (this.masterIdentifier.equals(Utils.getIdOfMasterWatcherFromZnode(data))) {
                logger.info("Active Master Watcher verified that Masters' znode's data checksum matches its id");
                if (this.lastUpdate - Utils.getTimeFromZnode(data) < this.maxForgiveMeMillis) {
                    logger.info("Active Master Watcher verified that it is updating well (within time constraints).");
                    try {
                        this.prevData = 
                                Utils.generateDataForZNode(
                                        this.lastUpdate, 
                                        this.updateQueueAsString.toString(), 
                                        this.masterIdentifier, 
                                        this.ntpServers);
                        this.updateQueueAsString.delete(0, this.updateQueueAsString.length());
                        logger.info("Clearing update queue and pushing new update to Masters' Keep Alive Znode.");
                        this.dm.updateZnodesData(this.prevData);
                    } catch (Exception ex) {
                        logger.info("Active Master Watcher failed to push update to Keep Alive znode: " + ex.getMessage(), ex);
                    }
                } else {
                    logger.info("Active Master Watcher noticed that it is failing to update Keep Alive znode within time constraints...");
                    if (this.cmwsInDanger.isEmpty()) {
                        logger.info("Active Master Watcher noticed that failing to update Keep Alive znode is its fault, not its children's.");
                        //It is you the one who is failing to push update, not
                        //CMWs, so kill yourself and let those kids alone.
                        logger.info("Active Master Watcher killing itself now, allowing new mastership competition.");
                        this.electNewMaster();
                    } else {
                        //Ok, you failed to push an update because some of your
                        //CMWs fail to push updates. Create as many failovers
                        //znodes as there are failing CMWs. Once you're done,
                        //kill yourself. This is just best effort failover znode
                        //creation. Which means that we don't make sure that things
                        //actually are created. We issue the command but do not
                        //check for response.
                        logger.info("Active Master Watcher noticed that failing to update is being caused by its children.");
                        /**********************************************************************************
                        for (String znodeInDanger : this.cmwsInDanger) {
                            this.dm.createFailoverZnode(FAILOVER_NAME_PATTERN.replace("#", znodeInDanger));
                        }**********************************************************************************/
                        logger.info("Number of CMWs that failed: " + this.cmwsInDanger.size() + ". Active Master killing itself now.");
                        this.electNewMaster();
                    }
                }
            } else {
                logger.info("Active Master Watcher was replaced by another master, just kill yourself.");
                synchronized (this) {
                    this.killSelf = true;
                    notify();
                }
            }
        } else if (this.child && this.activeMasterId == null){
            if (this.lastUpdate == INITIAL_TIME) {
                this.lastUpdate = Utils.getTimeFromZnode(data);
            }
            
            this.activeMasterId = Utils.getIdOfMasterWatcherFromZnode(data);
            logger.info("CMW initializing active master id information to parent id: " + this.activeMasterId);
            synchronized (this) {
                notify();
            }
        } else if (!this.child && !this.active) {
            logger.info("Inactive Master Watcher received data from Masters' Keep Alive znode.");
            if (this.lastUpdate == INITIAL_TIME) {
                logger.info("Inactive Master Watcher initializing inner clock.");
                this.lastUpdate = Utils.getTimeFromZnode(data);
            } else if (this.lastUpdate - Utils.getTimeFromZnode(data) >= this.maxForgiveMeMillis) {
                logger.info("Inactive Master Watcher noticed that Active master Watcher failed to update within time constraints. Competition for mastership begins.");
                this.ignoreTimeTicks = true;
                this.electNewMaster();
            }
        }
    }

    @Override
    public void processDataNodeCreated(boolean error, byte[] data) {
        if (!this.child && this.active) {
            if (error) {
                logger.info("Active Master Watcher failed to create processes' keep alive znode, and it will retry.");
                this.dm.createProcessKeepAliveZnode(
                        zkTimeNode, 
                        Utils.generateDataForObservedZnode(this.masterIdentifier,
                                this.masterIdentifier + System.currentTimeMillis(), 
                                Utils.getTimeFromProcessWatcherZnode(data)));
            }
            logger.info("Active Master Watcher created processes' keep alive znode.");
        }
    }

    public void childMasterWatcherSleep() {
        if (this.child && this.activeChild && this.p != null) {
            byte[] hbkillData = Utils.processHeartBeatDataToBytes(ProcessWrapper.FLAG_KILLSELF);
            this.dm.createProcessHeartBeatZnode(this.heartBeatZnode, hbkillData);
            synchronized (this) {
                try {
                    wait(8000L);
                } catch (InterruptedException ex) {
                    logger.error("CMW interrupted while waiting grace period before destroying ProcessWrapper", ex);
                }
            }
            logger.info("Child Master Watcher was told to destroy its process and wait.");
            this.p.destroy();
            //Assemble hard kill script
            ProcessBuilder kpb = new ProcessBuilder(this.hardKillScript.split("\\s"));
            try {
                Process kp = kpb.start();
                ProcessStreamConsumer scInfo = new ProcessStreamConsumer(kp.getInputStream(), logger);
                ProcessStreamConsumer scError = new ProcessStreamConsumer(kp.getErrorStream(), logger);
                scInfo.start();
                scError.start();
                int exitStatus = kp.waitFor();
                scInfo.join();
                scError.join();
                
                if (exitStatus == 0) {
                    logger.info("Hard kill script completed successfully.");
                } else {
                    logger.info("Hard kill script failed to complete correctly. Exit code is: " + exitStatus);
                }
            } catch (IOException | InterruptedException ex) {
                if (ex instanceof IOException) {
                    logger.error("Hard kill Script not found. There's a possibility that when new AMW is elected, the process will not be deployed.", ex);
                } else if (ex instanceof InterruptedException) {
                    logger.error("CMW interrupted while waiting for hard kill script to complete. Best effort kill in progress, that is, no way of knowing if things went well.");
                }
            } finally {
                this.closing(0);
            }
        }
    }

    @Override
    public void masterWatcherZnodeCreated(boolean error, byte[] data) {
        this.setWatchers();
        if (this.active) {
            if (error) {
                logger.info("Active Master Watcher failed to create masters' keep alive znode and now it will retry.");
                this.dm.createKeepAliveZnode(this.masterIdentifier, this.zkMasterStatusNode, data);
            } else {
                this.lastUpdate = Utils.getTimeFromZnode(data);
                logger.info("Active Master Watcher created masters' keep alive znode and now it will create processes' keep alive znode.");
                synchronized (this) {
                    try {
                        wait(5000);
                    } catch (InterruptedException ex) {
                        java.util.logging.Logger.getLogger(Master.class.getName()).log(Level.SEVERE, null, ex);
                    }
                }
                this.dm.createProcessKeepAliveZnode(
                        this.zkWatchedProgramNode, 
                        Utils.generateDataForObservedZnode(
                                this.masterIdentifier,
                                this.masterIdentifier + System.currentTimeMillis(), 
                                this.lastUpdate));
                
            }
        }
    }

    @Override
    public void masterElected(String masterId) {
        if (masterId == null) {
            logger.info("Rebooting master election.");
            this.electNewMaster();
            return;
        }
        logger.info("Master Elected: " + masterId + ", setting watchers.");
        this.active = this.masterIdentifier.equals(masterId);
        //nothing went wrong
        this.lastUpdate = INITIAL_TIME;
        this.setWatchers();
    }

    /**
    * Method that is invoked to cause inactive master watchers to compete until
    * another IMW becomes AMW.
    */
    public void electNewMaster() {
        if (!this.runningElection) {
            logger.info("An election is not in progress, we can proceed.");
            synchronized (this) {
                if (!this.child && this.active) {
                    logger.info("I, as the AMW, am not allowed to compete. Thus, I kill myself.");
                    this.killSelf = true;
                    notify();
                } else if (!this.child && !this.active){
                    logger.info("I am an IMW, and will ask for permission to compete. " + this.amwRequestKillZnode);
                    this.runningElection = true;
                    this.dm.readAmwRequestKillZnodeData();/*
                    try {
                        //The wait that ends first will try to create the master's znode.
                        logger.info("Will wait " + SAFETY_ELECTION_WAIT_TIME_MILLIS + " millis before proceeding to election.");
                        wait(SAFETY_ELECTION_WAIT_TIME_MILLIS);
                    } catch (InterruptedException ex) {
                        logger.error("Inactive Master Watcher interrupted while waiting for election", ex);
                    } finally {
                        try {
                            long currentTime = Utils.getNetworkTime(this.ntpServers);
                            long timeDiff = (currentTime - this.waitTimeToCheckActiveMastersUpdate) - (this.lastUpdate + NOTIFICATION_ZNODE_MAX_CREATION_OFFSET);
                            
                            if (timeDiff < 0) {
                                //Still has time to run for master.
                                logger.info("Inactive Master Watcher with a timeDiff of " + timeDiff + " has time to compete for mastership.");
                                this.dm.createTimeZnodeRemovedFlag();
                            }
                        } catch (Exception ex) {
                            logger.info("Something went wrong: ", ex);
                            //Assume that it is late to run for master, so he does not participate
                            //in the competition.
                        }
                    }*/
                }
            }
        } else {
            logger.info("Election is in progress, cannot start a competition now.");
        }
    }

    @Override
    public void dataReadFromProcessZnode(byte[] data) {
        if (this.active) {
            if (this.lastUpdate - Utils.getTimeFromProcessWatcherZnode(data) >= this.maxForgiveMeMillis) {
                //Process failed to update heartbeat, deploy a new one.
                this.processDataNodeCreated(true, null);
            }
        }
    }

    @Override
    public void disconnected(int rc) {
        if (this.child && this.activeChild && this.activeMasterId.equals(this.parentMasterWatcherId)) {
            //Best effort znode creation.
            logger.info("Child Master Watcher disconnected, creating failover znode.");
            /********************************************************************************************
            this.dm.createFailoverZnode(FAILOVER_NAME_PATTERN.replace("#", this.zkNodeToCreateForUpdate));
            *********************************************************************************************/
        }
        
        closing(rc);
    }

    @Override
    public void timeZnodeChanged() {
        this.setWatchers();
        logger.info("New time tick, read value.");
        this.dm.readTimesZnodeLastUpdate();
    }

    @Override
    public void masterZnodeChanged() {
        this.setWatchers();
        if (!this.child && !this.active) {
            logger.info("Active Master Watcher pushed an update, read value.");
            //this.dm.readLastUpdate();
            this.itmWaitCountdown.countDown();
        }
    }

    @Override
    public void timeZnodeRemoved() {
        this.setWatchers();
        logger.info("Time znode was removed, possibly due to Active Master Watcher killing itself. Run mastership competition.");
        if (!this.child && !this.active) {
            synchronized (this) {
                if (!this.runningElection) {
                    this.electNewMaster();
                }
            }
        } 
    }

    @Override
    public void masterZnodeCreated() {
        this.setWatchers();
        //Active Master Watcher will handle this through creation callback.
        //The AMW needs to ignore this callback because it handles it through the
        //other callback.
        //ITMs, however, need to act accordingly. In this case, they update their
        //inner clock.
        if ((!this.child && !this.active) || this.child){
            this.dm.readLastUpdate();
        }
    }

    @Override
    public void processObservedZnodeRemoved() {
        this.setWatchers();
        if (this.child && this.activeChild && this.activeMasterId.equals(this.parentMasterWatcherId)) {
            logger.info("Active Child Master Watcher going to sleep because process observed znode was removed.");
            this.childMasterWatcherSleep();
        } else if (this.child) {
            this.activeMasterId = null;
            this.lastUpdate = INITIAL_TIME;
        } else if (!this.child && !this.active) { 
            logger.info("Inactive Master Watcher will compete for mastership because Process Observed znode was removed.");
            synchronized (this) {
                if (!this.runningElection) {
                    this.electNewMaster();
                }
            }
        }
    }

    @Override
    public void cmwFailoverZnodeCreated(String znode) {
        this.setWatchers();
        
        if (this.child && !this.activeChild) {
            if (znode.equals(FAILOVER_NAME_PATTERN.replace("#", this.zkNodeToCreateForUpdate))) {
                //Wait until a new active master is elected, if it happens to be this CMW's parent
                //it will continue its activation. Otherwise, won't be active CMW.
                synchronized (this) {
                    try {
                        logger.info("Wait until a new active master is elected, to match the id of this CMW's parent: " + this.activeMasterId + ". CMW Id is: " + this.masterIdentifier);
                        wait();
                    } catch (InterruptedException ex) {
                        logger.error("CMW interrupted while waiting to activate itself. Id is: " + this.masterIdentifier, ex);
                    }
                }
                if (this.activeMasterId != null && this.activeMasterId.equals(this.parentMasterWatcherId)) {
                    //I'm an inactive copy of a CMW and I should activate myself now.
                    this.activeChild = true;
                    logger.info("Failover child Master Watcher will now make itself active");
                } else if (this.activeMasterId == null) {
                    logger.error("CMW cannot identify who the new active master is. It will not activate itself causing its parent to fail later.");
                }
            }
        } else if (this.child && this.activeChild) {
            if (znode.equals(FAILOVER_NAME_PATTERN.replace("#", this.zkNodeToCreateForUpdate))) {
                this.activeChild = false;
                logger.info("Active Child Master Watcher will make itself inactive.");
            }
        }
        logger.info("Removing failover znode");
        this.dm.removeZnode(FAILOVER_NAME_PATTERN.replace("#", this.zkNodeToCreateForUpdate));
    }

    @Override
    public void cmwUpdatedUpdateZnode(String znode) {
        this.setWatchers();
        if (!this.child && this.active) {
            this.dm.readCMWUpdateZnodeData(znode);
        }
    }

    @Override
    public void masterZnodeRemoved() {
        this.setWatchers();
        if (!this.child && this.active) {
            logger.info("Masters' Keep alive znode removed, Active Master Watcher kills itself.");
            this.closing(0);
        } else if (!this.child) {
            synchronized (this) {
                if (!this.runningElection) {
                    this.electNewMaster();
                    logger.info("Masters' Keep alive znode removed, inactive master watchers competing for mastership.");
                } else {
                    logger.info("Masters' Keep alive znode removed, inactive master watchers already competing for mastership.");
                }
            }
        }
    }

    @Override
    public void processObservedZnodeCreated() {
        synchronized (this) {
            if (this.killSelf) {
                return;
            }
        }
        
        this.setWatchers();
        if (this.child && this.activeChild && this.parentMasterWatcherId.equals(this.activeMasterId)) {
            //If no errors, launch process that will be monitored by masters.
            //ProcessBuilder does not handle well spaces, so we split string to copy it into the array.
            String[] processBuilderFormattedProgram = this.programToWatch.split("\\s");
            int sizeOfProgramArray = processBuilderFormattedProgram.length;
            String[] pbArgs = new String[this.argsForProgram.length + 3 + sizeOfProgramArray];
            System.arraycopy(processBuilderFormattedProgram, 0, pbArgs, 0, sizeOfProgramArray);
            //pbArgs[0] = this.programToWatch;
            
            //Args required by process wrapper. Remember that each watched process must extend
            //ProcessWrapper.
            pbArgs[sizeOfProgramArray] = this.zkHost;
            pbArgs[sizeOfProgramArray + 1] = this.zkPort;
            pbArgs[sizeOfProgramArray + 2] = this.heartBeatZnode;
            
            System.arraycopy(this.argsForProgram, 0, pbArgs, sizeOfProgramArray + 3, this.argsForProgram.length);
            logger.info("Running process with: " + Arrays.toString(pbArgs));
            ProcessBuilder pb = new ProcessBuilder(pbArgs);
            try {
                this.p = pb.start();
                ProcessStreamConsumer scInfo = new ProcessStreamConsumer(this.p.getInputStream(), logger);
                ProcessStreamConsumer scError = new ProcessStreamConsumer(this.p.getErrorStream(), logger);
                scInfo.start();
                scError.start();
            } catch (IOException ex) {
                logger.error("Child Master Watcher tried to start process, but failed.", ex);
                this.killSelf = true;
            }
        }
    }

    @Override
    public void processObservedZnodeChanged() {
        this.setWatchers();
        //Nothing to do here
    }

    @Deprecated
    @Override
    public void recreateMasterZnode(boolean allowedToAttemptZnodesRemoval) {
        if (allowedToAttemptZnodesRemoval) {
            logger.info("Inactive Master Watcher allowed to remove masters', time and observed process znodes.");
            this.dm.removeZnode(this.zkTimeNode);
            this.dm.removeZnode(this.zkMasterStatusNode);
            this.dm.removeZnode(this.zkWatchedProgramNode);
            
            synchronized (this) {
                try {
                    wait(5000);
                } catch (InterruptedException ex) {
                    logger.error("Inactive Master Watcher interrupted while waiting to remove masters', time and observed znodes.", ex);
                }
            }
        } else {
            logger.info("Inactive Master Watcher NOT allowed to remove masters', time and observed process znode.");
            synchronized (this) {
                try {
                    wait(7000);
                } catch (InterruptedException ex) {
                    logger.error("Inactive master watcher interrupted while waiting for inactive watchers to finish removing znodes.", ex);
                }
            }
        }
        try {
            //Every thread has a chance to remove the notification flag, if
            //their inner clock plus the time offset is later than current time.
            long currentTime = Utils.getNetworkTime(this.ntpServers);
            long timediff = currentTime - (this.lastUpdate + NOTIFICATION_ZNODE_MAX_CREATION_OFFSET);
            if (timediff > 0) {
                //Time up. We know that no other thread will attempt to create 
                //a notification znode after the grace period. So if time is up,
                //go ahead and delete notification znode.
                logger.info("Inactive master watcher removing notification znode.");
                this.dm.removeZnode(TIME_ZNODE_REMOVED_NOTIF_ZNODE);
            }
        } catch (Exception ex) {
            logger.error("Error while retrieving time: ", ex);
        }
        logger.info("Creating time znode, to see who the next active master will be.");
        this.lastUpdate = INITIAL_TIME;
        this.dm.createTimeZnode(
                this.masterIdentifier, 
                this.zkTimeNode, 
                Utils.generateDataForTimeZnode(this.masterIdentifier, INITIAL_TIME, this.ntpServers));
    }

    @Override
    public void masterZnodeDataSetCompleted(byte[] data, boolean error) {
        if (!this.child && this.active) {
            if (error) {
                this.dm.updateZnodesData(data);
            } else {
                //this.lastUpdate = Utils.getTimeFromZnode(data);
            }
        }
    }

    @Override
    public void timeZnodeCreated() {
        synchronized (this) {
            if (this.killSelf) {
                notify();
                return;
            }
        }
        
        if (this.child) {
            this.setWatchers();
        }
        this.ignoreTimeTicks = false;
        this.runningElection = false;
        this.lastUpdate = INITIAL_TIME;
        logger.info("Time znode created.");
        if (this.cdl != null && this.cdl.getCount() > 0L) {
            this.cdl.countDown();
        }
    }

    @Override
    public void cmwUpdateZnodeRemoved() {
        synchronized (this) {
            if (this.killSelf) {
                return;
            }
        }
        this.setWatchers();
    }

    @Override
    public void processHeartBeatZnodeUpdate() {
        logger.info("Child Master Watcher received an update from Process Watcher.");
        if (this.child && this.activeChild && this.activeMasterId.equals(this.parentMasterWatcherId)) {
            this.processUpdateWaitCountdown.countDown();
        }
    }

    @Override
    public void updateZnodeCreated(long time) {
        synchronized (this) {
            if (this.killSelf) {
                return;
            }
        }
        
        this.setWatchers();
        if (this.child && this.activeChild && this.parentMasterWatcherId.equals(this.activeMasterId)) {
            try {
                //create znode to tell ProcessWrapper to report itself.
                logger.info("Child Master Watcher creating heartbeat znode: " + this.heartBeatZnode + " with dummy data");
                byte[] hbData = Utils.processHeartBeatDataToBytes(ProcessWrapper.FLAG_UPDATE);
                this.dm.createProcessHeartBeatZnode(this.heartBeatZnode, hbData);
                //bind to this znode
                logger.info("Child Master Watcher temporarily binding to " + this.heartBeatZnode);
                this.dm.temporaryBindToHeartBeat(this.heartBeatZnode);
                //Now wait for a maximum of time
                logger.info("Now waiting: " + this.maxProcessHeartBeatWait + " millis for new updates from ProcessWatcher");
                this.processUpdateWaitCountdown = new CountDownLatch(1);
                boolean expired = 
                        !this.processUpdateWaitCountdown.await(this.maxProcessHeartBeatWait, TimeUnit.MILLISECONDS);

                if (expired) {
                    logger.info("Child Master Watcher waiting exhausted before receiving an update from ProcessWatcher");
                    ++this.heartBeatMisses;
                    logger.info("Child Master Watcher number of times missing heart beat from ProcessWatcher: " + this.heartBeatMisses);
                } else {
                    logger.info("Child Master Watcher received an update from ProcessWatcher before wating time exhausted");
                    this.heartBeatMisses = 0;
                }

                if (this.heartBeatMisses <= MAX_HEARTBEAT_MISS) {
                    logger.info("Child Master Watcher creating update znode for Active Master Watcher now.");
                    this.dm.setChildMasterWatcherZnode(
                            Utils.generateDataForChildMasterWatcher(
                                    time,
                                    this.programToWatch,
                                    this.argsForProgram,
                                    this.ntpServers));
                }

                //remove znode
                logger.info("Removing heart beat znode: " + this.heartBeatZnode + " now.");
                this.dm.removeProcessHeartBeatZnode();
                //update inner clock.
                this.lastUpdate = time;
            } catch (Exception ex) {
                logger.info("Child Master Watcher exception when receiving time update: " + ex.getMessage());
            }
        }
    }
    /*
    @Override
    public void updateZnodeChanged(String znode) {
        synchronized (this) {
            if (this.killSelf) {
                return;
            }
            
            if (this.runningElection) {
                return;
            }
        }
        this.setWatchers();
        if (!this.child && this.active) {
            this.dm.readCMWUpdateZnodeData(znode);
        }
    }*/

    @Override
    public void updateZnodeCreatedByMaster(String znode, byte[] data, boolean error) {
        synchronized (this) {
            if (this.killSelf) {
                return;
            }
            
            if (this.runningElection) {
                return;
            }
        }
        
        this.setWatchers();
        if (!this.child && this.active) {
            if (error) {
                this.dm.createChildMasterWatcherZnodeByActiveMaster(znode, data);
            }
        }
    }

    @Override
    public void amwRequestKillZnodeUpdated(byte[] data, boolean error) {
        if (error) {
            logger.error("Error occurred while setting data to amw kill request znode. Retrying.");
            this.dm.setAmwRequestKillZnodeData(data);
            return;
        }
        
        logger.info("AMW kill request znode data set succesfully.");
    }

    @Override
    public void amwRequestKillZnodeChanged() {
        logger.info("AMW kill request znode changed.");
        this.dm.bindToZnodes(this.active);
        if (!this.child && !this.active) {
            logger.info("IMW is going to read AMW kill request znode because it changed.");
            this.dm.readAmwRequestKillZnodeData();
        } else {
            logger.info("CMW or AMW ignoring fact that AMW kill request znode changed.");
        }
    }

    @Override
    public void dataReadFromAmwRequestKillZnode(byte[] data) {
        if (data == null) {
            logger.error("Error occurred while attempting to read AMW kill request znode. Retrying.");
            this.dm.readAmwRequestKillZnodeData();
            return;
        }
        
        //Active master does not need to worry about being granted permission.
        if (!this.child && this.active) {
            return;
        }
        
        //What has been written to znode.
        String amwZnodeData = Utils.requestAMWKillZnodeDataToString(data);
        //Type of data that was written to znode.
        String type = Utils.getTypeFromRequestAmwKillZnodeData(amwZnodeData);
        //If it is of type RESTORE or REQUEST, ignore.
        if (type.equals(Utils.AMW_PAYLOAD_TYPE_RESTORE)
                || type.equals(Utils.AMW_PAYLOAD_TYPE_REQUEST)) {
            logger.info("Received a payload type: " + type + ", but we're only interested in RESPONSE: " + Utils.AMW_PAYLOAD_TYPE_RESPONSE);
            return;
        }
        //To whom the permission is granted.
        String toWhom = Utils.getRequesterFromRequestAmwKillZnodeData(amwZnodeData);
        //Note that we need to make sure that this is a RESPONSE, because the thread
        //gets notified about REQUEST events as well.
        if (!toWhom.equals(this.masterIdentifier) && type.equals(Utils.AMW_PAYLOAD_TYPE_RESPONSE)) {
            logger.info("I'm not requester of permission to kill AMW. I am: " + this.masterIdentifier + ", requester is: " + toWhom);
            return;
        }
        logger.info("Current data in " + this.amwRequestKillZnode + " is: " + amwZnodeData);
        //Finally, extract payload.
        String dataPayload = Utils.getDataFromRequestAmwKillZnodeData(amwZnodeData);
        
        //We shall continue verifying the granted permission only if it is a response,
        //or if we need to see which MW will be the next AMW and this IMW is the
        //first one to request for permission. If it is the first one to request 
        //permission, TYPE of content in znode might be INIT or RESTORE. Those
        //are valid statuses only if running election, otherwise, when MWs get
        //notified about it, they must ignore it.
        boolean continuePermissionCheck = type.equals(Utils.AMW_PAYLOAD_TYPE_RESPONSE) || 
                (this.runningElection && (type.equals(Utils.AMW_PAYLOAD_TYPE_INIT) || type.equals(Utils.AMW_PAYLOAD_TYPE_RESTORE)));
        if (continuePermissionCheck && dataPayload.equals(TimeMaster.AMW_REQUEST_KILL_FREE)) {
            synchronized (this) {
                if (this.runningElection) {
                    logger.info("AMW request znode status is FREE. IMW is allowed to request a new master.");
                    this.dm.setAmwRequestKillZnodeData(Utils.requestAMWKillZnodeDataToBytes(TimeMaster.AMW_REQUEST_KILL_CODE_KILL, Utils.AMW_PAYLOAD_TYPE_REQUEST, this.masterIdentifier));
                } else {
                    logger.info("MW just got notified about initialization of kill request znode.");
                }
            }
        } else if (continuePermissionCheck && dataPayload.equals(TimeMaster.AMW_REQUEST_KILL_CODE_ALLOW)) {
            this.dm.setAmwRequestKillZnodeData(Utils.requestAMWKillZnodeDataToBytes(TimeMaster.AMW_REQUEST_KILL_BUSY, Utils.AMW_PAYLOAD_TYPE_REQUEST, this.masterIdentifier));
            logger.info("IMW is allowed to remove znodes and will become the next AMW. Current ITM id is: " + this.masterIdentifier);
            this.dm.removeZnode(this.zkTimeNode);
            this.dm.removeZnode(this.zkMasterStatusNode);
            this.dm.removeZnode(this.zkWatchedProgramNode);
            
            synchronized (this) {
                try {
                    wait(5000);
                } catch (InterruptedException ex) {
                    logger.error("Inactive Master Watcher interrupted while waiting to remove masters', time and observed znodes.", ex);
                } finally {
                    logger.info("Creating time znode, to see who the next active master will be.");
                    this.lastUpdate = INITIAL_TIME;
                    this.dm.createTimeZnode(
                            this.masterIdentifier, 
                            this.zkTimeNode, 
                            Utils.generateDataForTimeZnode(this.masterIdentifier, INITIAL_TIME, this.ntpServers));
                }
            }
        } else if (continuePermissionCheck && (dataPayload.equals(TimeMaster.AMW_REQUEST_KILL_CODE_DENIED) 
                || dataPayload.equals(TimeMaster.AMW_REQUEST_KILL_BUSY))) {
            this.cdl = new CountDownLatch(1);
            while (true) {
                try {
                    logger.info("IMW asked for permission to compete for AMW, but got denied or busy. Will wait a max of " + this.timeTickInterval * 4 + " millis until new time znode for time listeners is created.");
                    boolean expired = !this.cdl.await(this.timeTickInterval * 4, TimeUnit.MILLISECONDS);
                    if (!expired) {
                        logger.info("IMW just got notified that a new time znode for time listeners has been created.");
                    } else {
                        logger.info("IMW never got notified about new AMW, that is, no time znode was created.");
                        if (this.runningElection) {
                            logger.info("IMW was expecting to compete for new AMW to be elected, but time znode was never created. Requesting permission to kill AMW now.");
                            this.dm.setAmwRequestKillZnodeData(Utils.requestAMWKillZnodeDataToBytes(TimeMaster.AMW_REQUEST_KILL_CODE_KILL, Utils.AMW_PAYLOAD_TYPE_REQUEST, this.masterIdentifier));
                        } else {
                            logger.error("IMW was waiting for time znode to be added by new AMW, but never happened. Also, it was not running election... is that even possible??");
                        }
                    }
                    break;
                } catch (InterruptedException ex) {
                    logger.error("ITM interrupted while waiting for new time znode to be created by new AMW.", ex);
                    if (this.cdl.getCount() == 0L) {
                        break;
                    }
                }
            }
        }
    }
}
