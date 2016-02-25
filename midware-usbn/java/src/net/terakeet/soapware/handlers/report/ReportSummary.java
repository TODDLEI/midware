/*
 * ReportSummary.java
 *
 * Created on January 2, 2007, 4:14 PM
 *
 * (c) 2007 Terakeet Corp.
 */

package net.terakeet.soapware.handlers.report;

import java.util.Date;
import java.text.DecimalFormat;

/**
 */
public class ReportSummary {
    
    int location;
    int product;
    double ounces;
    String timestamp;
    OunceType type;
    
    static final DecimalFormat twoPlaces = new DecimalFormat("0.00");
    
    /** Create a new ReportSummary
     *  @param loc location ID
     *  @param pr product ID
     *  @param oz ounces recorded
     *  @param date timestamp
     *  @param ozType type of summary
     */
    public ReportSummary(int loc, int pr, double oz, String date, OunceType ozType) {
        location = loc;
        product = pr;
        ounces = oz;
        timestamp = date;
        type = ozType;
    }
    
    public int getLocation() {
        return location;
    }
    public int getProduct() {
        return product;
    }
    public double getOunces() {
        return ounces;
    }
    public String getTimestamp() {
        return timestamp;
    }
    public OunceType getOunceType() {
        return type;
    }
    public boolean isPoured() {
        return OunceType.POURED == type;
    }
    public boolean isSold() {
        return OunceType.SOLD == type;
    }
    
    /** Outputs this summary in the format:
     *     "T: L#X P#Y: ZZZ.ZZ oz"
     *  Where T is the type, P for poured or S for sold
     *  X is the location ID
     *  Y is the product ID
     *  ZZZ.ZZ is the ounces, with two decimal places
     */
    public String toString() {
        StringBuilder result = new StringBuilder();
        result.append(type == OunceType.POURED ? "P: ":"S: ");
        result.append(timestamp);
        result.append(" L#");
        result.append(location);
        result.append(" P#");
        result.append(product);
        result.append(": ");
        result.append(twoPlaces.format(ounces)); 
        result.append(" oz");
        return result.toString();        
    }
    
}
