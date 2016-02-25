/*
 * GroupingType.java
 *
 * Created on January 2, 2007, 4:02 PM
 *
 * (c) 2007 Terakeet Corp.
 */

package net.terakeet.soapware.handlers.report;

/**
 * List of supported types in grouping.
 */
public enum GroupingType {
    
    DAY,WEEK,MONTH,DAY_OF_WEEK,PRODUCT,LOCATION,CUSTOMER, NONE, UNKNOWN;
    
    public static GroupingType instanceOf(String s) {
        String str = s.toLowerCase();
        if ("product".equals(str)) {
            return PRODUCT;
        } else if ("location".equals(str)) {
            return LOCATION;
        } else if ("day".equals(str)) {
            return DAY;
        } else if ("week".equals(str)) {
            return WEEK;
        } else if ("month".equals(str)) {
            return MONTH;
        } else if ("day of week".equals(str) || "day_of_week".equals(str)) {
            return DAY_OF_WEEK;
        } else if ("customer".equals(str)) {
            return CUSTOMER;
        } else if ("none".equals(str)){
            return NONE;
        }
        return UNKNOWN;
    }
    
    public String toString(){
        switch(this){
            case DAY:   return "day";
            case WEEK:  return "week";
            case MONTH: return "month";
            case DAY_OF_WEEK: return "day_of_week";
            case PRODUCT:   return "product";
            case LOCATION: return "location";
            case CUSTOMER: return "customer";
            case NONE: return "none";
            case UNKNOWN:
            default:
                return "unknown";
        }
    }
    
}
