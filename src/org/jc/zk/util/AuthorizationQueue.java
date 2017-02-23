/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.jc.zk.util;

import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author cespedjo
 */
public class AuthorizationQueue {
    
    private final static List<byte[]> queue = new ArrayList<>();
    
    private static boolean consumed = false;
    
    public static boolean enqueueResponse(byte[] data) {
        synchronized (AuthorizationQueue.queue) {
            if (AuthorizationQueue.consumed) {
                return false;
            }
            
            AuthorizationQueue.queue.add(data);
            return true;
        }
    }
    
    public static List<byte[]> consumeQueue() {
        synchronized (AuthorizationQueue.queue) {
            AuthorizationQueue.consumed = true;
            return AuthorizationQueue.queue;
        }
    }
}
