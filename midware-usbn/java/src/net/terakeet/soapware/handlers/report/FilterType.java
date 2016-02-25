/*
 * FilterType.java
 *
 * Created on January 2, 2007, 4:03 PM
 *
 * (c) 2007 Terakeet Corp.
 */

package net.terakeet.soapware.handlers.report;

/**
 */
public enum FilterType {
    
    CUSTOMER,LOCATION,PRODUCT,STATE;
    
    public static FilterType instanceOf(String s) {
        String str = s.toLowerCase();
        if ("customer".equals(str)) {
            return CUSTOMER;
        } else if ("location".equals(str)) {
            return LOCATION;
        } else if ("product".equals(str)) {
            return PRODUCT;
        } else if ("state".equals(str)) {
            return STATE;
        }
        return null;
    }
    
    public String toString(){
        switch(this){
            case CUSTOMER: return "customer";
            case LOCATION: return "location";
            case PRODUCT: return "product";
            case STATE: return "state";
            default: return "";
        }
    }
}
