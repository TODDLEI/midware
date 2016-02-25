/*
 * AuperReading.java
 *
 * Created on August 18, 2005, 4:15 PM
 *
 * To change this template, choose Tools | Options and locate the template under
 * the Source Creation and Management node. Right-click the template and choose
 * Open. You can then make changes to the template in the Source Editor.
 */

package net.terakeet.soapware.handlers.auper;

/**
 *
 * @author Administrator
 */
public class AuperReading {
    
    private final int pid;
    private final AuperKegs reading;
    
    /** Creates a new instance of AuperReading */
    public AuperReading(int pid, AuperKegs reading) {
        this.pid = pid;
        this.reading = reading;
    }
    
    public int getPid() {
        return pid;
    }
    
    public AuperKegs getKegs() {
        return reading;
    }
               
    public static AuperReading mostRecent(int pid) {
        //TODO: Connect to db for lookup
        return new AuperReading(pid, new AuperKegs(-1.0));
    }
    
    public static AuperKegs difference(AuperReading recent, AuperReading old) {
        return new AuperKegs(old.reading.asKegs() - recent.reading.asKegs());
    }
    
}
