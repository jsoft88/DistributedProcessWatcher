/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.jc.zk.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import org.apache.log4j.Logger;

/**
 *
 * @author cespedjo
 * 
 * Thread in charge of consuming error stream and info stream from spawned process.
 */
public class ProcessStreamConsumer extends Thread {
    private final InputStream in;
    private final Logger logger;
    
    public ProcessStreamConsumer(InputStream in, Logger logger) {
        this.in = in;
        this.logger = logger;
    }

    @Override
    public void run() {
        BufferedReader br = null;
        try {
            br = new BufferedReader(new InputStreamReader(this.in));
            String line;
            
            while ((line = br.readLine()) != null) {
                this.logger.info(line);
            }
        } catch (Exception ex) {
            this.logger.error("Error processing stream", ex);
        } finally {
            try {
                if (br != null) {
                    br.close();
                }
            } catch (IOException ex) {
                this.logger.error("Error closing buffered reader", ex);
            }
        }
    }
}
