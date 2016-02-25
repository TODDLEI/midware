/*
 * Scheduler.java
 *
 * Created on September 9, 2005, 12:07 PM
 *
 */

package net.terakeet.soapware.handlers.auper;

import java.util.*;

/** Scheduler
 *
 */
public class Scheduler {
    
    /** Creates a new instance of Scheduler */
    public Scheduler() {
    }
    
    private static long seconds(int s) {
        return 1000 * s;
    }
    
    private static long minutes(int m) {
        return 60 * seconds(m);
    } 
    
    private static long hours(int h) {
        return 60 * minutes(h);
    }
    
    
    static Timer timer;

    public static void main(String[] args) {
        timer = new Timer();
        timer.schedule(new MeterReader(),
                       seconds(10),        //initial delay
                       minutes(5));  //subsequent rate
    }
    
}
