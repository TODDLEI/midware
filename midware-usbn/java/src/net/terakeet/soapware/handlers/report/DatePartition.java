/*
 * DatePartition.java
 *
 * Created on August 29, 2005, 1:39 PM
 *
 */

package net.terakeet.soapware.handlers.report;

import java.util.Date;

/** DatePartition
 * A lightweight wrapper for an immutable tuple of Date and int.  
 * The methods compareTo,equals,and hashCode are passed
 * directly to the encapsulated Date.   
 */
public class DatePartition implements Comparable {
    
    private final Date d;
    private final int i;
    
    /** Creates a new instance of DatePartition */
    public DatePartition(Date d, int i) {
        this.d = new Date(d.getTime());
        this.i = i;
    }
    
    /** Creates a new instance of DatePartition without an index */
    public DatePartition(Date d) {
        this(d, -1);
    }
    
    public Date getDate() {
        return new Date(d.getTime());
    }
    
    public int getIndex() {
        return i;
    }
    
    public int compareTo(Object o) {
        if (o instanceof DatePartition) { 
            DatePartition d2 = (DatePartition) o;
            return d.compareTo(d2.d);
        } else {
            throw new ClassCastException("Argument to compareTo is not a DatePartition");
        }
    }
    
    public boolean equals(Object o) {
        return d.equals(o);
    }
    
    public int hashCode() {
        return d.hashCode();
    }
    
    public String toString() {
        return "("+String.valueOf(i)+") "+d.toString();
    }
    
}
