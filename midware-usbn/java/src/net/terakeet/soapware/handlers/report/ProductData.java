/*
 * ProductData.java
 *
 * Created on September 14, 2005, 11:20 AM
 *
 */

package net.terakeet.soapware.handlers.report;

/** ProductData
 *
 */
public class ProductData {
    
    private final int location;
    private final int id;
    private final double poured;
    private final String name;
    private final double units;
    private static final ProductData unknown = new ProductData(0,0.0,"Unknown Product", 20);
    
    /** Creates a new instance of ProductData */
    public ProductData(int id, double poured, String name, double units) {
        this.id = id;
        this.location = 0;
        this.poured = poured;
        if (name != null) {
            this.name = name;
        } else {
            this.name = "";
        }
        this.units = units;
       
    }

    /** Creates a new instance of ProductData */
    public ProductData(int location, int id, double poured, String name, double units) {
        this.id = id;
        this.location = location;
        this.poured = poured;
        if (name != null) {
            this.name = name;
        } else {
            this.name = "";
        }
        this.units = units;

    }
    
    public int getId() {
        return id;
    }

    public int getLocation() {
        return location;
    }
    
    public double getPoured() {
        return poured;
    }
    
    public String getName() {
        return name;
    }
    
    public double getUnits() {
        return units;
    }
        
    public static ProductData getUnknownInstance() {
        return unknown;
    }
}
