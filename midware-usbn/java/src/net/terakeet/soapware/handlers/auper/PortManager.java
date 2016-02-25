/*
 * PortManager.java
 *
 * Created on September 14, 2005, 5:04 PM
 *
 */

package net.terakeet.soapware.handlers.auper;

import java.util.*;
import javax.comm.*;

/** PortManager
 *
 */
public class PortManager {
    
    Map<String,Boolean> openPorts;
    Map<String,SerialPort> ports;
    Map<String,SynchedSerialPort> loanedPorts;
    Map<String,Date> checkoutTimes;
    private Object lock;
    
    /** Creates a new instance of PortManager */
    public PortManager() {
        ports = new HashMap<String,SerialPort>();
    }
    
    private SynchedSerialPort openPort(String name) {
        return null;
    }
    
    public SynchedSerialPort requestPort(String name) {
        synchronized(lock) {
            if (openPorts.get(name).booleanValue()) {
                 return openPort(name);
            } else {
                return null;
            }
        }
    }
    
    public SynchedSerialPort stealPort(String name) {
        synchronized(lock) {          
            if (openPorts.get(name).booleanValue()) {
                loanedPorts.get(name).makePortUnavailable();
            }
        }
        return null;
    }
    
    
}
