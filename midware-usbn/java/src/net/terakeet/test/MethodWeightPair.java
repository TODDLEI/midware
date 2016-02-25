/**
 * MethodWeightPair.java
 *
 * @author Ben Ransford
 * @version $Id: MethodWeightPair.java,v 1.1.1.1 2005/08/26 19:16:01 anonymous Exp $
 */

package net.terakeet.test;

public class MethodWeightPair implements Comparable {
    private String method = null;
    private double weight = 0.0;

    public MethodWeightPair (String method, double weight) {
	this.method = method;
	this.weight = weight;
    }

    public void setWeight (double d) {
	this.weight = d;
    }

    public String getMethod () {
	return this.method;
    }

    public double getWeight () {
	return this.weight;
    }

    public String toString () {
	return method + ": " + weight;
    }
    
    private static final double tinyDouble = 0.0000000000001;
    
    /** Sorts MethodWeightPairs in descending order by weight
     *  @throws ClassCastException if obj is not a MethodWeightPair
     */
    public int compareTo(Object obj) {
        if (obj instanceof MethodWeightPair) {
            MethodWeightPair mwp = (MethodWeightPair) obj;
            if ((mwp.getWeight() - this.weight) < tinyDouble) {
                return 0;
            } else if (mwp.getWeight() > this.weight) {
                return 1;
            } else {
                return -1;
            }
        } else {
            throw new ClassCastException(
                "Paramter obj must be of type MethodWeightPair");
        }
    }
    
    public boolean equals(Object obj) {
        if (obj instanceof MethodWeightPair) {
            MethodWeightPair mwp = (MethodWeightPair) obj;
            if (this.method.equals(mwp.getMethod()) && 
                (this.weight - mwp.getWeight() < tinyDouble)) {
                    return true;
            }
        }
        return false;
    }
    
    public int hashCode() {
        long w = Double.doubleToLongBits(weight);
        return (31 + (method.hashCode() * 17)) +
            ((int)(w^(w>>>32)));
    }
    
}
