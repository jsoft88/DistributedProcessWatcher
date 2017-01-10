/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.jc.zk.process;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

/**
 *
 * @author cespedjo
 * Base abstract class which acts as a wrapper of processes that must be deployed
 * by CMWs. This abstract class provides hooks that allow processes to communicate
 * with upper level components, for example, communicate health status and receive
 * events.
 * 
 * @param <T> Class of Data Monitor extending Base Process Wrapper Monitor.
 * @param <G> Type of value returned by Callable.
 */
public abstract class ProcessWrapper<T extends ProcessWrapperMonitor, G> implements Actions<G>, GenericProcessListener, Watcher {

    private static final org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger(ProcessWrapper.class);
    
    protected final String zkConnect;
    
    protected final String znode;
    
    protected T pwm;
    
    protected boolean killSelf;
    
    private final ExecutorService es;
    
    private Future<G> futureTask;
    
    public static final String FLAG_UPDATE = "100";
    
    public static final String FLAG_KILLSELF = "101";
    
    /**
     * Default constructor for class.
     * @param zkHost String representing ZooKeeper host.
     * @param zkPort String representing ZooKeeper port.
     * @param znode String representing heartbeat znode that will be created by 
     * CMW and that this ProcessWrapper will be bound to.
     * @throws IOException If connection to ZooKeeper fails.
     */
    public ProcessWrapper(String zkHost, String zkPort, String znode) throws IOException {
        this.zkConnect = zkHost + ":" + zkPort;
        this.znode = znode;
        this.es = Executors.newFixedThreadPool(1);
        this.futureTask = null;
    }
    
    @Override
    public void updateZnode() {
        this.pwm.updateZnode("0");
    }

    @Override
    public void connected() {
        this.pwm.bindHeartBeatZnode();
        logger.info("ProcessWrapper connected. Launching user code in new thread.");
        //Launch user code in another thread.
        this.futureTask = this.es.submit(new Callable<G>(){

            @Override
            public G call() {
                return userCode();
            }
        });
    }
    
    /**
     * Method which allows user code execute certain operations before terminating
     * process wrapper.
     */
    public void userHandleTermination() {
        throw new UnsupportedOperationException("Override method first.");
    }
    
    /**
     * Method invoked to terminate ProcessWrapper.
     */
    public void terminateProcessWrapper(){
        logger.info("ProcessWrapper was requested to terminate itself.");
        this.userHandleTermination();
        synchronized (this) {
            this.killSelf = true;
            notify();
        }
    }

    /**
     * Callback indicating that a connection to ZooKeeper was lost. Let implementations
     * define behavior.
     */
    @Override
    public void disconnected() {
        throw new UnsupportedOperationException("Override method disconnected()");
    }

    @Override
    public void process(WatchedEvent event) {
        this.pwm.process(event);
    }

    @Override
    public void pHeartBeatZnodeUpdated(boolean error, String data) {
        this.pwm.bindHeartBeatZnode();
        if (error) {
            this.pwm.updateZnode(data);
        }
    }

    @Override
    public void pHeartBeatZnodeCreated() {
        this.pwm.bindHeartBeatZnode();
        this.pwm.readHeartBeatData();
    }

    /**
     * When heartbeat znode is created, the ProcessWrapper identifies whether this
     * event is requesting a health report from the wrapper or it is requesting
     * the wrapper to end.
     * 
     * @param error true if an error occurred when reading heartbeat znode, false otherwise.
     * @param data String representing data read from heartbeat znode.
     */
    @Override
    public void pHeartBeatZnodeDataRead(boolean error, String data) {
        if (error) {
            this.pwm.readHeartBeatData();
        } else {
            if (data.equals(FLAG_UPDATE)) {
                this.pwm.updateZnode("0");
            } else if (data.equals(FLAG_KILLSELF)){
                if (this.futureTask != null) {
                    this.terminateProcessWrapper();
                }
            }
        }
    }

    @Override
    public void pHeartBeatZnodeRemoved() {
        this.pwm.bindHeartBeatZnode();
    }
    
    public G userCode(){
        throw new UnsupportedOperationException("Override method in extending class.");
    }
    
    public boolean isThreadInterrupted() {
        synchronized (this) {
            return this.killSelf;
        }
    }

    @Override
    public G call() {
        synchronized (this) {
            while (!this.killSelf) {
                try {
                    wait();
                } catch (InterruptedException ex) {
                    logger.error("ProcessWrapper interrupted while waiting...", ex);
                }
            }
            this.es.shutdown();
            try {
                wait(5000);
                this.es.shutdownNow();
                return this.futureTask.get();
            } catch (InterruptedException | ExecutionException ex) {
                if (ex instanceof InterruptedException) {
                    logger.error("ProcessWrapper interrupted while waiting for ExecutorService to shutdown.", ex);
                } else if (ex instanceof ExecutionException) {
                    logger.error("ProcessWrapper failed while shutting down ExecutorService", ex);
                }
                //Thread would be ended by this time, exception cannot be thrown.
                return null;
            }
        }
    }
}
