/*
 * DateSet.java
 *
 * Created on June 21, 2010, 10:06 AM
 *
 */

package net.terakeet.soapware.handlers.report;

import net.terakeet.util.MidwareLogger;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;

/** DateSet
 *  An immutable object that contains information about a report's date-interval.  
 */
public class DateSet {
    
    private final String sd; //start date
    private final String ed; //end date
    private SimpleDateFormat newDateFormat  = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private static MidwareLogger logger     = new MidwareLogger(DateSet.class.getName());
    
    /** Creates a new instance of DateSet */
    public DateSet() {
        sd  = null;
        ed = null;
    }

    public DateSet(String startDate, String endDate) {
        
        if (startDate == null || endDate == null ) {
            throw new NullPointerException("Tried to construct ReportPeriod with one or more null arguments");
        }
        
        sd = startDate;
        ed = endDate;
    }

    public HashMap<Integer, DateSet> getDateSets(String start, String end) {

        HashMap<Integer, DateSet> dateSets  = new HashMap<Integer, DateSet>();
        try {
            DateSet dates                   = new DateSet(start, end);
            java.util.Date startDate        = newDateFormat.parse(start);
            java.util.Date endDate          = newDateFormat.parse(end);

            if (startDate.getYear() < endDate.getYear()) {
                Calendar c1                 = Calendar.getInstance();
                c1.setTime(startDate);
                c1.set(Calendar.MONTH, 11);
                c1.set(Calendar.DAY_OF_MONTH, 31);
                c1.set(Calendar.HOUR_OF_DAY, 23);
                c1.set(Calendar.MINUTE, 59);
                c1.set(Calendar.SECOND, 59);
                dates                       = new DateSet(start, newDateFormat.format(c1.getTime()));
                dateSets.put(1, dates);

                c1.setTime(endDate);
                c1.set(Calendar.MONTH, 1);
                c1.set(Calendar.DAY_OF_MONTH, 1);
                c1.set(Calendar.HOUR_OF_DAY, 0);
                c1.set(Calendar.MINUTE, 0);
                c1.set(Calendar.SECOND, 0);
                dates                       = new DateSet(newDateFormat.format(c1.getTime()), end);
                dateSets.put(2, dates);
            } else {
                dates                       = new DateSet(start, end);
                dateSets.put(1, dates);
            }

        } catch (Exception pe) { }
        return dateSets;
    }
    
    public String getStartDate() {
        return sd;
    }
    
    public String getEndDate() {
        return ed;
    }

    public String getStartYear() {
        Calendar c1                         = Calendar.getInstance();
        try {
            c1.setTime(newDateFormat.parse(sd));
            if (c1.getTime().getYear() == Calendar.getInstance().getTime().getYear()) {
                return "";
            }
        } catch (Exception pe) { }
        return String.valueOf(1900 + c1.getTime().getYear());
    }

    public String getEndYear() {
        Calendar c1                         = Calendar.getInstance();
        try {
            c1.setTime(newDateFormat.parse(ed));
            if (c1.getTime().getYear() == Calendar.getInstance().getTime().getYear()) {
                return "";
            }
        } catch (Exception pe) { }
        return String.valueOf(1900 + c1.getTime().getYear());
    }
}
