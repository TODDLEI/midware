/*
 * SummaryType.java
 */

package net.terakeet.soapware.handlers.report;

/**
 * List of supported types for period shifts.
 */
public enum SummaryType {
    
    Poured,Sold,PreOpenHours,PreOpenHoursSold,OpenHours,OpenHoursTest,OpenHoursSold,AfterHours,AfterHoursSold,Exclusion,LineCleaning,EventPreOpenHours,EventPreOpenHoursSold,EventOpenHours,
    EventOpenHoursSold,EventAfterHours,EventAfterHoursSold,BevSync,Inventory,Projection,UNKNOWN;
    
    public static SummaryType instanceOf(String s) {
        String str = s.toLowerCase();
        if ("poured".equals(str)) {
            return Poured;
        } else if ("sold".equals(str)) {
            return Sold;
        } else if ("preopenhours".equals(str)) {
            return PreOpenHours;
        } else if ("preopenhourssold".equals(str)) {
            return PreOpenHoursSold;
        } else if ("openhours".equals(str)) {
            return OpenHours;
        } else if ("openhourstest".equals(str)) {
            return OpenHoursTest;
        } else if ("openhourssold".equals(str)) {
            return OpenHoursSold;
        } else if ("afterhours".equals(str)) {
            return AfterHours;
        } else if ("afterhourssold".equals(str)) {
            return AfterHoursSold;
        } else if ("exclusion".equals(str)) {
            return Exclusion;
        } else if ("linecleaning".equals(str)) {
            return LineCleaning;
        } else if ("eventpreopenhours".equals(str)) {
            return EventPreOpenHours;
        } else if ("eventpreopenhourssold".equals(str)) {
            return EventPreOpenHoursSold;
        } else if ("eventopenhours".equals(str)) {
            return EventOpenHours;
        } else if ("eventopenhourssold".equals(str)) {
            return EventOpenHoursSold;
        } else if ("eventafterhours".equals(str)) {
            return EventAfterHours;
        } else if ("eventafterhourssold".equals(str)) {
            return EventAfterHoursSold;
        } else if ("bevsync".equals(str)) {
            return BevSync;
        } else if ("projection".equals(str)) {
            return Projection;
        } else if ("inventory".equals(str)) {
            return Inventory;
        } else {
            return UNKNOWN;
        }
    }
    
    public String toString(){
        switch(this){
            case Poured:   return "poured";
            case Sold:  return "sold";
            case PreOpenHours:   return "preOpenHours";
            case PreOpenHoursSold:  return "preOpenHoursSold";
            case OpenHours:   return "openHours";
            case OpenHoursTest:   return "openHoursTest";
            case OpenHoursSold:  return "openHoursSold";
            case AfterHours:   return "afterHours";
            case AfterHoursSold:  return "afterHoursSold";
            case Exclusion:   return "exclusion";
            case LineCleaning:  return "lineCleaning";
            case EventPreOpenHours:   return "eventPreOpenHours";
            case EventPreOpenHoursSold:  return "eventPreOpenHoursSold";
            case EventOpenHours:   return "eventOpenHours";
            case EventOpenHoursSold:  return "eventOpenHoursSold";
            case EventAfterHours:   return "eventAfterHours";
            case EventAfterHoursSold:  return "eventAfterHoursSold";
            case BevSync:  return "bevSync";
            case Projection:  return "bevSync";
            case Inventory:  return "inventory";
            case UNKNOWN:
            default:
                return "unknown";
        }
    }

    public int toSQLQueryInt(){
        switch(this){
            case Poured:   return 1;
            case Sold:  return 2;
            case PreOpenHours:   return 3;
            case PreOpenHoursSold:  return 4;
            case OpenHours:   return 5;
            case OpenHoursTest:   return 20;
            case OpenHoursSold:  return 6;
            case AfterHours:   return 7;
            case AfterHoursSold:  return 8;
            case Exclusion:   return 9;
            case LineCleaning:  return 10;
            case EventPreOpenHours:   return 11;
            case EventPreOpenHoursSold:  return 12;
            case EventOpenHours:   return 13;
            case EventOpenHoursSold:  return 14;
            case EventAfterHours:   return 15;
            case EventAfterHoursSold:  return 16;
            case BevSync:  return 17;
            case Projection:  return 18;
            case Inventory:  return 19;
            case UNKNOWN:
            default:
                return 0;
        }
    }

    public int toLocationType(){
        switch(this){
            case Poured:   return 1;
            case Sold:  return 1;
            case PreOpenHours:   return 1;
            case PreOpenHoursSold:  return 1;
            case OpenHours:   return 1;
            case OpenHoursTest:   return 1;
            case OpenHoursSold:  return 1;
            case AfterHours:   return 1;
            case AfterHoursSold:  return 1;
            case Exclusion:   return 1;
            case LineCleaning:  return 1;
            case EventPreOpenHours:   return 2;
            case EventPreOpenHoursSold:  return 2;
            case EventOpenHours:   return 2;
            case EventOpenHoursSold:  return 2;
            case EventAfterHours:   return 2;
            case EventAfterHoursSold:  return 2;
            case BevSync:  return 3;
            case Projection:  return 4;
            case Inventory:  return 5;
            case UNKNOWN:
            default:
                return 0;
        }
    }
    
}
