/*
 * ReportDescriptor.java
 *
 * Created on January 2, 2007, 3:59 PM
 *
 */

package net.terakeet.soapware.handlers.report;

import net.terakeet.soapware.*;


/**
 */
public class SummaryStructure {
    
    private int location = 0;
    private int bar = 0;
    private int product = 0;
    private double value;
    
    /** Create a new ReportDescriptor
     *  @param groupSetType column grouping
     *  @param valueSetType row grouping
     *  @param typeOfFilter filter
     *  @param filterValue value
     *  @param dateStart the start date ('YYYY-MM-DD')
     *  @param dateEnd the end date ('YYYY-MM-DD')
     *  @param includePoured a poured report
     *  @param includeSold a sold report
     *  @param includeVaraince a variance report
     */
    
    public SummaryStructure() {
        
    }
    public SummaryStructure(int location, int bar, int product, double value) {
        this.location = location;
        this.bar = bar;
        this.product = product;
        this.value = value;
    }
    
    public int getLocation() {
        return location;
    }
    public int getBar() {
        return bar;
    }
    public int getProduct() {
        return product;
    }
    public double getValue() {
        return value;
    }
}
