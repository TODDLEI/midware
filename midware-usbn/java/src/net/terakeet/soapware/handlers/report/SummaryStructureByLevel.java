/*
 * SummaryStructureByLevel.java
 *
 * Created on August 17, 2009, 3:59 PM
 *
 */

package net.terakeet.soapware.handlers.report;

import net.terakeet.soapware.*;


/**
 */
public class SummaryStructureByLevel {
    
    private int childLevel = 0;
    private int product = 0;
    private double value;
    
    public SummaryStructureByLevel() {
        
    }
    public SummaryStructureByLevel(int childLevel, int product, double value) {
        this.childLevel = childLevel;
        this.product = product;
        this.value = value;
    }
    
    public int getChildLevel() {
        return childLevel;
    }
    public int getProduct() {
        return product;
    }
    public double getValue() {
        return value;
    }
}
