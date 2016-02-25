/*
 * AuperKegs.java
 *
 * Created on August 18, 2005, 4:40 PM
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
public class AuperKegs {
    
    private final static double CONVERSION = 1920; // Ounces in a keg
    private final double k;
    
    /** Creates an AuperKegs object from a keg reading
     * @param k the number of kegs
     */
    public AuperKegs(double k) {
        this.k = k;
    }
    
    /** Get the number of kegs this object represents
     * @return the value in kegs
     */
    public double asKegs() {
        return k;
    }
    
    /** Get the number of fluid ounces this object represents
     * @return the value in fl.oz
     */
    public double asOz() {
        return k * CONVERSION;
    }
    
    /** Creates an AuperKegs object from a fluid ounces reading
     * @param oz the number of fluid ounces
     * @return a new instance
     */
    public static AuperKegs ozToKegs(double oz) {
     
        return new AuperKegs(oz / CONVERSION);
    }
}
