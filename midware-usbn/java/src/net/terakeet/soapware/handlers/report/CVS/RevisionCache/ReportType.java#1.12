/*
 * ReportType.java
 */

package net.terakeet.soapware.handlers.report;
import java.util.Date;
import java.util.Calendar;
/**
 * List of supported types for reports.
 */
public enum ReportType {
    
    Today,Yesterday,Week,Monthly,Quarterly,Halfyearly,Yearly,YTD;
    
    public static ReportType instanceOf(String s) {
        String str = s.toLowerCase();
        if ("today".equals(str)) {
            return Today;
        } else if ("yesterday".equals(str)) {
            return Yesterday;
        } else if ("week".equals(str)) {
            return Week;
        } else if ("monthly".equals(str)) {
            return Monthly;
        } else if ("quarterly".equals(str)) {
            return Quarterly;
        } else if ("halfyearly".equals(str)) {
            return Halfyearly;
        } else if ("yearly".equals(str)) {
            return Yearly;
        } else if ("ytd".equals(str)) {
            return YTD;
        } else {
            return Today;
        }
    }
    
    public String toString(){
        switch(this){
            case Today:         return "Today";
            case Yesterday:     return "Yesterday";
            case Week:          return "Week";
            case Monthly:       return "Monthly";
            case Quarterly:     return "Quarterly";
            case Halfyearly:    return "Halfyearly";
            case Yearly:        return "Yearly";
            case YTD:           return "YTD";
            default:            return "Today";
        }
    }

    public int toDays(){
        switch(this){
            case Today:         return 1;
            case Yesterday:     return 1;
            case Week:          return 7;
            case Monthly:       return 30;
            case Quarterly:     return 90;
            case Halfyearly:    return 180;
            case Yearly:        return 365;
            case YTD:
                Calendar c1                 = Calendar.getInstance();
                return c1.get(Calendar.DAY_OF_YEAR);
            default:            return 1;
        }
    }

    public Date toStartDate(){
        Calendar c1 = Calendar.getInstance();
        c1.set(Calendar.HOUR_OF_DAY, 7);
        c1.set(Calendar.MINUTE, 0);
        c1.set(Calendar.SECOND, 0);
        switch(this){
            case Today:         
                return  c1.getTime();
            case Yesterday:
                c1.add(Calendar.DATE, -1);
                return c1.getTime();
            case Week:
                c1.add(Calendar.DATE, -7);
                return c1.getTime();
            case Monthly:
                c1.add(Calendar.MONTH, -1);
                return c1.getTime();
            case Quarterly:
                c1.add(Calendar.MONTH, -3);
                return c1.getTime();
            case Halfyearly:
                c1.add(Calendar.MONTH, -6);
                return c1.getTime();
            case Yearly:
                c1.add(Calendar.MONTH, -12);
                return c1.getTime();
            case YTD:
                c1.set(Calendar.MONTH, 1);
                c1.set(Calendar.DAY_OF_MONTH, 1);
                return c1.getTime();
            default:
                return  c1.getTime();
        }
    }

    public Date toEndDate(){
        Calendar c1 = Calendar.getInstance();
        c1.set(Calendar.HOUR_OF_DAY, 7);
        c1.set(Calendar.MINUTE, 0);
        c1.set(Calendar.SECOND, 0);
        switch(this){
            case Today:
                c1.add(Calendar.DATE, 1);
                return c1.getTime();
            case Yesterday:     
                return c1.getTime();
            case Week:        
                return c1.getTime();
            case Monthly:
                return c1.getTime();
            case Quarterly:
                return c1.getTime();
            case Halfyearly:
                return c1.getTime();
            case Yearly:
                return c1.getTime();
            case YTD:
                return c1.getTime();
            default:
                c1.add(Calendar.DATE, 1);
                return c1.getTime();
        }
    }

    public Date toDayOfWeek(Date d, int dayOfWeek){
        Calendar c1 = Calendar.getInstance();
        c1.setTime(d);
        c1.set(Calendar.DAY_OF_WEEK, dayOfWeek);
        c1.set(Calendar.HOUR_OF_DAY, 7);
        c1.set(Calendar.MINUTE, 0);
        c1.set(Calendar.SECOND, 0);
        return c1.getTime();
    }

    public Date addDays(Date d, int days){
        Calendar c1 = Calendar.getInstance();
        c1.setTime(d);
        c1.add(Calendar.DATE, (days*-1));
        c1.set(Calendar.HOUR_OF_DAY, 7);
        c1.set(Calendar.MINUTE, 0);
        c1.set(Calendar.SECOND, 0);
        return c1.getTime();
    }
    
}
