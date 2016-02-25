/*
 * PeriodShiftType.java
 */

package net.terakeet.soapware.handlers.report;

/**
 * List of supported types for period shifts.
 */
public enum PeriodShiftType {
    
    PreOpen,Open,AfterHours,EntireDay,None,BevSync,UNKNOWN;
    
    public static PeriodShiftType instanceOf(String s) {
        String str = s.toLowerCase();
        if ("preopen".equals(str)) {
            return PreOpen;
        } else if ("open".equals(str)) {
            return Open;
        } else if ("afterhours".equals(str)) {
            return AfterHours;
        } else if ("entireday".equals(str)) {
            return EntireDay;
        } else if ("none".equals(str)){
            return None;
        } else if ("bevsync".equals(str)){
            return BevSync;
        } else {
            return UNKNOWN;
        }
    }
    
    public String toString(){
        switch(this){
            case PreOpen:   return "PreOpen";
            case Open:  return "Open";
            case AfterHours: return "AfterHours";
            case EntireDay: return "EntireDay";
            case None: return "none";
            case BevSync: return "bevsync";
            case UNKNOWN:
            default:
                return "unknown";
        }
    }

    public String toHoursString(){
        switch(this){
            case PreOpen:   return "PreOpenHours";
            case Open:  return "OpenHours";
            case AfterHours: return "AfterHours";
            case EntireDay: return "EntireDay";
            case None: return "none";
            case BevSync: return "bevsync";
            case UNKNOWN:
            default:
                return "unknown";
        }
    }

    public int toSQLQueryInt(){
        switch(this){
            case PreOpen:   return 1;
            case Open:  return 2;
            case AfterHours: return 3;
            case EntireDay: return 0;
            case None: return 0;
            case BevSync: return 4;
            case UNKNOWN:
            default:
                return 0;
        }
    }
    
}
