/*
 * ElapsedTime.java
 *
 * Created on January 3, 2007, 2:49 PM
 *
 * (c) 2007 Terakeet Corp.
 */

package net.terakeet.soapware.handlers.report;

import java.text.DecimalFormat;

/** Holds a number of seconds and a label.  This class
 *  is not synchronized.
 */
public class ElapsedTime {
   
    private double time;
    private String label;
    private boolean running;
    private long startTime; // in mills
    
    private static final DecimalFormat twoPlaces = new DecimalFormat("0.00");           
    
    /** Get an instance of a completed elapsed time, with the time provided. 
     *  The timer will not be running. 
     *  @param seconds the time the task took
     *  @param a description of the task
     */
    public ElapsedTime(double seconds, String description) {
        time = seconds;
        label = description;
        running = false;
        startTime = 0;
    }
    
    /** Start a running timer for this ET.
     *  @param a description of the task;
     */
    public ElapsedTime(String description) {
        label = description;
        startTime = System.currentTimeMillis();
        running = true;
    }
    
    /** Stop a running ET.  The timer cannot be started again.
     */
    public void stopTimer() {
        if (running) {
            long endTime = System.currentTimeMillis();
            time = (endTime - startTime)/1000.0;
            running = false;
        }
    }
    /** Returns true if this ET has a running timer, or false if its finished
     */
    public boolean isRunning() {
        return running;
    }
    public String getDescription() {
        return label;
    }
    /** Get the elapsed seconds of this ET.  The timer may still be running
     */
    public double getTime() {
        return running ? ((System.currentTimeMillis()-startTime)/1000.0) : time;
    }
    public String toString() {
        return label+" ("+twoPlaces.format(getTime())+" sec)";
    }
    
}
