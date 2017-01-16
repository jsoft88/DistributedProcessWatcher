/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.jc.zk.dpw;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author cespedjo
 */
public class TimeHandlerMain {
    
    public static void main(String[] args) {
        if (args.length < 7) {
            System.out.println("Usage: hadoop jar /path/to/DPW.jar org.jc.zk.dpw.TimeHandlerMain uniqueIdentifier zkHost zkPort zkTimeNode zkTimeListenersNode intervalMillis ntpserver1,ntpserver2");
            System.out.println("---------------------------------------------------------------------");
            System.out.println("uniqueIdentifier: an unique identifier for this instance of Time Master.");
            System.out.println("zkHost: zookeeper host");
            System.out.println("zkPort: zookeeper port");
            System.out.println("zkTimeNode: znode where Active Time Master will push Keep Alive heart beat");
            System.out.println("zkTimeListenersNode: znode where time ticks will be placed. This znode must match the one used by MWs and CMWs.");
            System.out.println("intervalMillis: interval millis of ticks being sent to time listeners znode. Remember that this must be long enough to allow AMW retrieve updates from CMWs.");
            System.out.println("ntpServer1,ntpServer2: list of ntp servers separated by comma.");
            
            System.exit(1);
        }
        
        String masterIdentifier = args[0];
        String zkHost = args[1];
        String zkPort = args[2];
        String zkTimeNode = args[3];
        String zkTimeListenersNode = args[4];
        String interval = args[5];
        String[] receivedNtpServers = args[6].split(",");
        String[] ntpServers = new String[receivedNtpServers.length];
        for (int i = 0; i < receivedNtpServers.length; ++i) {
            ntpServers[i] = receivedNtpServers[i].trim();
        }
        
        ExecutorService es = Executors.newFixedThreadPool(1);
        try {
            TimeMaster tm = new TimeMaster(masterIdentifier, zkHost, zkPort, zkTimeNode, zkTimeListenersNode, Long.parseLong(interval), ntpServers);
            es.submit(tm);
        } catch (IOException ex) {
            Logger.getLogger(TimeHandlerMain.class.getName()).log(Level.SEVERE, null, ex);
            throw new UnsupportedOperationException(ex.getMessage());
        }
        
    }
}
