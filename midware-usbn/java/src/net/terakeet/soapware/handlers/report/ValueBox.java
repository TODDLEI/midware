/*
 * ValueBox.java
 *
 * Created on January 10, 2007, 3:31 PM
 *
 * (c) 2007 Terakeet Corp.
 */

package net.terakeet.soapware.handlers.report;

import java.util.HashMap;
import java.util.Set;

/**  Value boxes hold a running count of all summaries passed to it, grouped by ounce types
 */
public class ValueBox {
    
    private HashMap<OunceType,Double> counters;
    
    /** Create a new value box with counters at zero
     */
    public ValueBox() {
        // set counters to zero
        counters = new HashMap<OunceType, Double>();
        for(OunceType type : OunceType.values()) {
            counters.put(type,new Double(0.0));
        }
    }
    
    /**  Process a summary
     *  @param summary to process
     */
    public void consume(ReportSummary summary) {
        OunceType type = summary.getOunceType();
        double value = counters.get(type).doubleValue();
        value += summary.getOunces();
        counters.put(type,new Double(value));
    }
    
    /** get the counter for an ounce type
     *  @param type poured or sold
     *  @return the count in ounces
     */
    public double get(OunceType type) {
        return counters.get(type).doubleValue();
    }
    
    /**
     * Output all the types and their values.
     */
    public String toString(){
        StringBuilder sb = new StringBuilder();
        Set<OunceType> keys = counters.keySet();
        for(OunceType type : keys){
            sb.append("Type: " + String.valueOf(type));
            sb.append("Value: " + String.valueOf(counters.get(type)));
        }
        
        return sb.toString();
    }
}
