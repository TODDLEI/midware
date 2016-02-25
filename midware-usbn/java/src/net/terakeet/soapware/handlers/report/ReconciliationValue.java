/*
 * ReconciliationValue.java
 *
 * Created on October 3, 2005, 11:33 AM
 *
 */

package net.terakeet.soapware.handlers.report;

/** ReconciliationValue
 *
 */
public class ReconciliationValue {
    
    String type;
    double value;
    
    /** Creates a new instance of ReconciliationValue */
    public ReconciliationValue(String t, double v) {
        type = t;
        value = v;
    }
    
    public double getOunces() {
        return value;
    }
    
}
