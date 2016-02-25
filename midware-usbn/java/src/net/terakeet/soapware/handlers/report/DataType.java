/*
 * DataType.java
 */

package net.terakeet.soapware.handlers.report;

/**
 * List of supported types for period shifts.
 */
public enum DataType {
    
    Poured,Sold,UNKNOWN;
    
    public static DataType instanceOf(String s) {
        String str = s.toLowerCase();
        if ("poured".equals(str)) {
            return Poured;
        } else if ("sold".equals(str)) {
            return Sold;
        } else {
            return UNKNOWN;
        }
    }
    
    public String toString(){
        switch(this){
            case Poured:   return "";
            case Sold:  return "Sold";
            case UNKNOWN:
            default:
                return "unknown";
        }
    }

    public int toSQLQueryInt(){
        switch(this){
            case Poured:   return 1;
            case Sold:  return 2;
            case UNKNOWN:
            default:
                return 0;
        }
    }

    public int toExclusionInt(){
        switch(this){
            case Poured:   return 2;
            case Sold:  return 1;
            case UNKNOWN:
            default:
                return 0;
        }
    }
    
}
