/*
 * TierSummaryStructure.java
 *
 * Created on April 1, 2011, 2:59 PM
 *
 */

package net.terakeet.soapware.handlers.report;
import net.terakeet.soapware.*;


/**
 */
public class TierSummaryStructure {
    
    private int tier            = 0;
    private double count        = 0.0;
    private double var          = 0.0;
    private double poured       = 0.0;
    private double sold         = 0.0;
    private int location        = 0;
    
    
    public TierSummaryStructure() { }

    public TierSummaryStructure(int tier, double count, double var, double poured, double sold, int location) {
        this.tier               = tier;
        this.count              = count;
        this.var                = var;
        this.poured             = poured;
        this.sold               = sold;
        this.location           = location;
    }

    public TierSummaryStructure(int tier, double count, double var) {
        this.tier               = tier;
        this.count              = count;
        this.var                = var;
    }

    public void setComplexData(double poured, double sold) {
        this.poured             = poured;
        this.sold               = sold;
    }

    public void setLocation(int location) {
        this.location           = location;
    }
    
    public int getTier() {
        return tier;
    }
    public double getCount() {
        return count;
    }
    public double getVar() {
        return var;
    }
    public double getPoured() {
        return poured;
    }
    public double getSold() {
        return sold;
    }
    public int getLocation() {
        return location;
    }
}
