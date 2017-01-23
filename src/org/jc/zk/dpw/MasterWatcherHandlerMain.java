/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.jc.zk.dpw;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author cespedjo
 */
public class MasterWatcherHandlerMain {
    
    public static void main(String[] args) throws Exception {
        
        if (args == null || args.length < 19) {
            System.out.println("Invoke: hadoop jar /path/to/DPW.jar org.jc.zk.dpw.MasterWatcherHandlerMain " + 
                    "uniqueId zkHost zkPort zkTimeListenerZnode zkKeepAliveZnode zkProcessObservedZnode " +
                    "/absolute/path/program/to/run arg1,arg2,...,argn isChild isActiveChild numberOfChildren " +
                    "intervalToWaitForUpdate childUpdateZnode1,childUpdateZnode2... znodeToCreateForUpdate " + 
                    "ntpServer1,ntpServer2 idOfParentMasterWatcher maxForgiveMeMillis hardKillScript amwKillZnode");
            System.out.println("------------------");
            System.out.println("Common args");
            System.out.println("------------------");
            System.out.println("uniqueId: an unique identifier. Could a number.");
            System.out.println("zkHost: ZooKeeper Host");
            System.out.println("zkPort: ZooKeeper Port");
            System.out.println("zkTimeListenerZnode: znode where time updates will be placed by Time Master");
            System.out.println("zkKeepAliveZnode: znode where Active Master Watcher will push heart beat updates");
            System.out.println("zkProcessObservedZnode: znode to signal Child Master Watchers to deploy processes");
            System.out.println("ntpServer1,ntpServer2: List of NTP servers separated by comma");
            System.out.println("maxForgiveMeMillis: the number of milliseconds IMWs will forgive ATM lack of updates. This should be greater than TMs interval millis.");
            System.out.println("---------------------------------");
            System.out.println("When launching a master watcher:");
            System.out.println("---------------------------------");
            System.out.println("isChild: flag to signal whether this is a CMW or a MW. In this case set to false.");
            System.out.println("isActiveChild: flag to signal that a CMW is active or inactive. In this case, set to false");
            System.out.println("numberOfChildren: how many CMWs are being deployed");
            System.out.println("intervalToWaitForUpdate: how many millis will an Active Master Watcher wait for all CMWs to push their updates");
            System.out.println("childUpdateZnode1,childUpdateZnode2: list of znodes that CMWs will create to push updates separated by comma. Each cmw has an unique znode");
            System.out.println("znodeToCreateForUpdate: set to null when deploying master");
            System.out.println("hardKillScript: use keyword null.");
            System.out.println("amwKillZnode: znode where IMWs will request for permission to run a new competition for the new AMW to be elected.");
            System.out.println("---------------------------------------");
            System.out.println("When launching a Child Master Watcher:");
            System.out.println("---------------------------------------");
            System.out.println("isChild: flag to signal whether this is a CMW or a MW. In this case set to true");
            System.out.println("isActiveChild: flag to signal that a CMW is active or inactive");
            System.out.println("numberOfChildren: how many CMWs are being deployed. In this case set to 0");
            System.out.println("intervalToWaitForUpdate: how many millis will an Active Child Master Watcher wait for its PW to push an update. Divide Master's interval millis by the number of deployed CMWs.");
            System.out.println("childUpdateZnode1,childUpdateZnode2: list of znodes that CMWs will create to push updates separated by comma. In this case, set to null");
            System.out.println("znodeToCreateForUpdate: an unique znode that this CMW will create to push update");
            System.out.println("idOfParentMasterWatcher: id of parent Master Watcher.");
            System.out.println("hardKillScript: include command plus script to be run when the CMW is told to kill itself due to failure. Ex: bash /path/to/command.sh");
            System.out.println("amwKillZnode: znode where IMWs will request for permission to run a new competition for the new AMW to be elected. Use keyword null for CMWs.");
            System.exit(1);
        }
        
        String masterId = args[0];
        String zkHost = args[1];
        String zkPort = args[2];
        String zkTimeListenerZnode = args[3];
        String zkKeepAliveZnode = args[4];
        String zkProcessObserved = args[5];
        String programToWatch = args[6];
        if (programToWatch.toLowerCase().equals("null")) {
            programToWatch = null;
        }
        String[] receivedArgs = programToWatch == null? new String[0] : args[7].split(",");
        String[] programArgs;
        if (receivedArgs.length == 1 && receivedArgs[0].equals("null")) {
            programArgs = new String[0];
        } else {
            programArgs = new String[receivedArgs.length];
            for (int i = 0; i < receivedArgs.length; ++i) {
                programArgs[i] = receivedArgs[i].trim();
            }
        }
        String child = args[8];
        String activeChild = args[9];
        String numberOfCmws = args[10];
        String interval = args[11];
        String[] receivedZnodesForUpdate = args[12].split(",");
        String[] znodesForUpdate = new String[receivedZnodesForUpdate.length];
        for (int i = 0; i < receivedZnodesForUpdate.length; ++i) {
            znodesForUpdate[i] = receivedZnodesForUpdate[i].trim();
        }
        String znodeToCreateForUpdate = args[13];
        if (znodeToCreateForUpdate.toLowerCase().equals("null")) {
            znodeToCreateForUpdate = null;
        }
        String[] receivedNtpServers = args[14].split(",");
        String[] ntpServers = new String[receivedNtpServers.length];
        for (int i = 0; i < receivedNtpServers.length; ++i) {
            ntpServers[i] = receivedNtpServers[i].trim();
        }
        String idOfParentMasterWatcher = args[15].trim();
        String maxForgiveMeMillis = args[16].trim();
        String aHardKillScript = args[17].trim();
        String hardKillScript = null;
        if (!aHardKillScript.toLowerCase().equals("null")) {
            hardKillScript = aHardKillScript;
        }
        
        String amwKillRequestZnode = args[18].trim();
        if (amwKillRequestZnode.toLowerCase().equals("null")) {
            amwKillRequestZnode = null;
        }
        
        ExecutorService es = Executors.newFixedThreadPool(1);
        
        Master m = new Master(
                masterId, 
                zkHost, 
                zkPort, 
                zkTimeListenerZnode, 
                zkKeepAliveZnode, 
                zkProcessObserved, 
                programToWatch, 
                programArgs, 
                Boolean.parseBoolean(child), 
                Boolean.parseBoolean(activeChild), 
                Integer.parseInt(numberOfCmws), 
                Long.valueOf(interval),
                idOfParentMasterWatcher,
                Long.parseLong(maxForgiveMeMillis),
                hardKillScript,
                amwKillRequestZnode,
                znodesForUpdate, 
                znodeToCreateForUpdate, 
                ntpServers);
        Future f = es.submit(m);
        try {
            f.get();
        } catch (ExecutionException | InterruptedException ex) {
            Logger.getLogger(MasterWatcherHandlerMain.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}
