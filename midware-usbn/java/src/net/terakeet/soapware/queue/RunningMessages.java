/*
 * RunningMessages.java
 * Created on October 23, 2007, 2:28 PM
 */

package net.terakeet.soapware.queue;

import java.util.HashMap;
import net.terakeet.util.MidwareLogger;

/**
 *
 * @author Alex
 */
public class RunningMessages {
    private static RunningMessages ref;
    private static final Object lock = new Object();
    
    private HashMap<String,Boolean> msgs;
    
    /**
     * Creates a new instance of RunningMessages
     */
    private RunningMessages() {
        // dissallow instantiation
        msgs = new HashMap<String,Boolean>(100);
    }
    
    public static RunningMessages getInstance() {
        synchronized (lock) {
            if (null == ref) {
                ref = new RunningMessages();
            }
        }
        return ref;
    }
    
    // returns false if already present
    public boolean add(String name) {
        boolean success = false;
        synchronized (lock) {
            if (!msgs.containsKey(name)) {
                msgs.put(name, true);
                success = true;
            }
        }
        return success;
    }
    
    // returns false if oldName is still present or if newName exists
    public boolean rename(String oldName, String newName) {
        boolean success = false;
        synchronized (lock) {
            if (!msgs.containsKey(oldName) && !msgs.containsKey(newName)) {
                msgs.put(newName, true);
                success = true;
            }
        }
        return success;
    }
    
    public void remove(String name) {
        synchronized (lock) {
            msgs.remove(name);
        }
    }
    
    public boolean contains(String name) {
        boolean val = false;
        synchronized (lock) {
            val = msgs.containsKey(name);
        }
        return val;
    }
    
    public Object clone() throws CloneNotSupportedException {
        throw new CloneNotSupportedException();
    }
    
    
}
