/*
 * SummaryStructureSimpleData.java
 *
 * Created on August 17, 2009, 3:59 PM
 *
 */

package net.terakeet.soapware.handlers.report;


/**
 */
public class SummaryStructureSimpleData {

    private int parentLevel = 0;
    private int childLevel = 0;
    private int product = 0;
    private double value;

    public SummaryStructureSimpleData() {

    }
    public SummaryStructureSimpleData(int parentLevel, int childLevel, int product, double value) {
        this.parentLevel = parentLevel;
        this.childLevel = childLevel;
        this.product = product;
        this.value = value;
    }

    public int getChildLevel() {
        return childLevel;
    }
    public int getParentLevel() {
        return parentLevel;
    }
    public int getProduct() {
        return product;
    }
    public double getValue() {
        return value;
    }
}
