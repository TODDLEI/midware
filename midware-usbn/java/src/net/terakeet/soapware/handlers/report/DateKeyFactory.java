/*
 * DateKeyFactory.java
 *
 * Created on January 15, 2007, 11:32 AM
 *
 * (c) 2007 Terakeet Corp.
 */

package net.terakeet.soapware.handlers.report;

import java.util.*;
import java.text.*;
import net.terakeet.util.LRUCache;

/**   A GroupingKey Factory that can generate keys for grouping types DAY, WEEK, MONTH, AND DAY_OF_WEEK
 */
public class DateKeyFactory extends GroupingKeyFactory {
    private GroupingType type;
    private static final long DAYINTERVAL = 86400000;
    private static final String DATEPATTERN = "(\\d{4})-(\\d{1,2})-(\\d{1,2}) (\\d{1,2}):(\\d{1,2}):(\\d{1,2})";
    private static final String DATEPATTERN2 = "(\\d{4})-(\\d{1,2})-(\\d{1,2})";
    private static final DateFormat format = new SimpleDateFormat("yyyy-MM-dd");
    private LRUCache<String,String> dayToWeekCache;
    
    public DateKeyFactory(GroupingType myType) {
        type = myType;
        dayToWeekCache = new LRUCache<String,String>(50);
    }
    
    /** Get a GroupingKey for the supplied summary, of the supported-type of this factory.
     */
    public GroupingKey getKey(ReportSummary summary) {
        switch(type){
            case DAY:
                return new GroupingKey(type, getDateString(getTimestamp(summary.getTimestamp())));
            case WEEK:
                return new GroupingKey(type, getWeekNumber(summary.getTimestamp()));
            case MONTH:{
                Calendar ca = getTimestamp(summary.getTimestamp());
                if(null == ca){
                    break;
                }
                return new GroupingKey(type, getMonthString(ca.get(Calendar.MONTH)));
            }
            case DAY_OF_WEEK:{
                Calendar ca = getTimestamp(summary.getTimestamp());
                if(null == ca){
                    break;
                }
                return new GroupingKey(type, getDayOfWeekString(ca.get(Calendar.DAY_OF_WEEK)));
            }
            default:
                throw new UnsupportedOperationException("SimpleGroupingFactory can't make keys of type: "+type);
        }
        return new GroupingKey(GroupingType.UNKNOWN,"");
    }
    
    /** Get the month string from the Calendar month const */
    private String getMonthString(int month){
        switch(month){
            case 0: return "January";
            case 1: return "February";
            case 2: return "March";
            case 3: return "April";
            case 4: return "May";
            case 5: return "June";
            case 6: return "July";
            case 7: return "August";
            case 8: return "September";
            case 9: return "October";
            case 10: return "November";
            case 11: return "December";
            default: return null;
        }
    }
    
    /** Get the day of week string according to the Calendar setting */
    private String getDayOfWeekString(int dayOfWeek){
        switch(dayOfWeek){
            case 1: return "Sunday";
            case 2: return "Monday";
            case 3: return "Tuesday";
            case 4: return "Wednesday";
            case 5: return "Thursday";
            case 6: return "Friday";
            case 7: return "Saturday";
            default: return null;
        }
    }
    
    /** Get the date string from the Calendar object. This format is used to be displayed. */
    private String getDateString(Calendar ca){
        if(null == ca){
            return "";
        }
        return format.format(ca.getTime());
//      return (String.valueOf(ca.get(Calendar.YEAR)) + "-" + String.valueOf(ca.get(Calendar.MONTH)) + "-" +
//              String.valueOf(ca.get(Calendar.DATE)));
    }
    
    /**
     * Get the week string.
     *
     * The last week of the year, if the year is not ended in Sunday, will be rounded up to the first week of next year.
     * @author Qi Liu
     */
    private String getWeekNumber(String date){
        if(null != dayToWeekCache){
            String cacheResult = dayToWeekCache.get(stripDate(date));
            if (null != cacheResult) {
                return cacheResult;
            }
        }
        
        Calendar ca = getTimestamp(date);
        if(null == ca){
            return "";
        }
        
        int dayOfWeek = ca.get(Calendar.DAY_OF_WEEK);
        long currentFig = ca.getTimeInMillis();
        long newFig = currentFig - DAYINTERVAL * ((dayOfWeek == 1)?6:(dayOfWeek - 2));
        ca.setTimeInMillis(newFig);
        
        String result = getDateString(ca);
        if(null != dayToWeekCache){
            dayToWeekCache.put(stripDate(date),result);
        }
        return result;
    }
    
    
    /**
     * @deprecated Pass String instead of Date.
     */
    private String getWeekNumber(java.util.Date date){
//        if(null != dayToWeekCache){
//            String cacheResult = dayToWeekCache.get(date);
//            if (null != cacheResult) {
//                return cacheResult;
//            }
//        }
        
        Calendar ca = getTimestamp(date);
        if(null == ca){
            return "";
        }
        
//      int weekOfYear = ca.get(Calendar.WEEK_OF_YEAR);
        int dayOfWeek = ca.get(Calendar.DAY_OF_WEEK);
        long currentFig = ca.getTimeInMillis();
        long newFig = currentFig - DAYINTERVAL * ((dayOfWeek == 1)?6:(dayOfWeek - 2));
        ca.setTimeInMillis(newFig);
        
        String result = getDateString(ca);
//        if(null != dayToWeekCache){
//            dayToWeekCache.put(date,result);
//        }
        return result;
    }
    
    /**  removes the spaces from a date
     */
    private String stripDate(String date){
        int space = date.indexOf(" ");
        if(-1 != space){
            return date.substring(0, space);
        }
        
        return date;
    }
    
    /**
     * Return the Calendar class from the timestamp string in the ReportSummary class.
     * @author Qi Liu
     */
    private Calendar getTimestamp(String ts){
        try{
            format.parse(ts);
            Calendar ca = format.getCalendar();
            ca.setFirstDayOfWeek(Calendar.MONDAY);
            return ca;
        } catch(Exception exp) {
            return null;
        }
    }
    
    /**
     * @deprecated Pass String instead of Date.
     */
    private Calendar getTimestamp(java.util.Date date){
        Calendar ca = Calendar.getInstance();
        ca.setTime(date);
        ca.setFirstDayOfWeek(Calendar.MONDAY);
        return ca;
    }
    
//    public void logDayToWeekCache() {
//        if (dayToWeekCache != null) {
//            for (Map.Entry<String,String> e : dayToWeekCache.getAll()) {
//                logger.debug(" Day to Week Cache: '"+e.getKey()+"' : '"+e.getValue()+"'");
//            }
//        }
//    }

//    /**
//     * Convert the day of week returned by the Calendar.get method which starts from
//     * Sunday to our system which starts from Monday.
//     */
//    private int convertDayOfWeek(int caDow){
//        if( (caDow > 7) || (caDow < 0) ){
//            return -1;
//        }
//        
//        int result = caDow - 1;
//        return result == 0 ? 7 : result;
//    }
    
    
}
