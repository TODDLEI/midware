/*
 * ReportPeriod.java
 *
 * Created on August 29, 2005, 10:06 AM
 *
 */

package net.terakeet.soapware.handlers.report;

import java.util.Date;
import java.util.Calendar;
import java.util.GregorianCalendar;
import net.terakeet.util.MidwareLogger;

/** ReportPeriod
 *  An immutable object that contains information about a report's date-interval and period.  
 */
public class ReportPeriod {
    
    private final PeriodType p; //period
    private final String pd; //period detail
    private final Date sd; //start date
    private final Date ed; //end date
    private MidwareLogger logger;
    
    /** Creates a new instance of ReportPeriod */
    public ReportPeriod(PeriodType period, String periodDetail, Date startDate, Date endDate) {
        logger                              = new MidwareLogger(ReportPeriod.class.getName());
        if (period == null || periodDetail == null || startDate == null || endDate == null ) {
            throw new NullPointerException("Tried to construct ReportPeriod with one or more null arguments");
        }
        // TODO: check date
        
        if (period == PeriodType.WEEKLY) {
            String detail = periodDetail.toLowerCase();
            if (detail.length() > 3) {
                detail = detail.substring(0,3);
            }
            if (detail.equals("mon") || detail.equals("m")) {
                pd = String.valueOf(Calendar.MONDAY);
            } else if (detail.equals("sun") || detail.equals("su")) {
                pd = String.valueOf(Calendar.SUNDAY);
            } else if (detail.equals("sat") || detail.equals("sa")) {
                pd = String.valueOf(Calendar.SATURDAY);
            } else if (detail.equals("tue") || detail.equals("t")) {
                pd = String.valueOf(Calendar.TUESDAY);
            } else if (detail.equals("wed") || detail.equals("w")) {
                pd = String.valueOf(Calendar.WEDNESDAY);
            } else if (detail.equals("thu") || detail.equals("r")) {
                pd = String.valueOf(Calendar.THURSDAY);
            } else if (detail.equals("fri") || detail.equals("f")) {
                pd = String.valueOf(Calendar.FRIDAY);
            } else {
                throw new IllegalArgumentException("Illegal periodDetail: "+periodDetail);
            }
        } else {
            pd = periodDetail;
        }
        // TODO: Add parameter-checks
        p = period;
        sd = startDate;
        ed = endDate;
    }
    
    public PeriodType getPeriod() {
        return p;
    }
    
    public String getPeriodDetail() {
        return pd;
    }
    
    public int getOffsetWeekly() {
        //add checking
        return Integer.parseInt(pd);
    }
    
    public int getOffsetHourly() {
        // add checking
        return Integer.parseInt(pd);
    }
    
     public int getOffsetMinutely() {
        // add checking
        return Integer.parseInt(pd);
    }
    
    public int getOffsetDaily() {
        // add checking
        return Integer.parseInt(pd);
    }
    
    public int getOffsetMonthly() {
        //add checking
        return Integer.parseInt(pd);
    }
    
    
    public Date getStartDate() {
        return sd;
    }
    
    public Date getEndDate() {
        return ed;
    }
    
    
    /** Create a new Calendar that is set to the earliest aligned date AFTER the 
     *  argument, c. The alignment is based on the information contained in
     *  this ReportPeriod. Calendar c is unchanged by this method.
     *
     */
    public Calendar alignToCalendar(Calendar c) {
        Calendar result;
        if (p == PeriodType.MINUTELY) {
            result = nextMinute(c, getOffsetMinutely());
        } else if (p == PeriodType.HOURLY) {
            result = nextHour(c, getOffsetHourly());
        } else if (p == PeriodType.DAILY) {
            result = nextDay(c, getOffsetDaily());
        } else if (p == PeriodType.WEEKLY) {
            result = nextWeek(c, getOffsetWeekly());
        } else if (p == PeriodType.MONTHLY) {
            result = nextMonth(c, getOffsetWeekly());
        } else if (p == PeriodType.FULL) {
            result = c;
        } else {
            throw new UnsupportedOperationException("No alignment available for type "+p.toString());
        }   
        return result;
    }
    
    /** Increment the calendar by the amount described by this period.  
     *
     */
    public void incrementCalendar(Calendar c) {
        if (getPeriod() == PeriodType.MINUTELY) {
            //logger.debug("PeriodDetail TEst:"+getPeriodDetail());
            c.add(Calendar.MINUTE,Integer.parseInt(getPeriodDetail()));
        } else if (getPeriod() == PeriodType.HOURLY) {
            c.add(Calendar.HOUR_OF_DAY,1);
        } else if (getPeriod() == PeriodType.DAILY) {
            c.add(Calendar.DAY_OF_MONTH,1);
        } else if (getPeriod() == PeriodType.WEEKLY) {
            c.add(Calendar.DAY_OF_MONTH,7);
        } else if (getPeriod() == PeriodType.MONTHLY) {
            c.add(Calendar.MONTH,1);
        } else if (getPeriod() == PeriodType.FULL) {
            c.add(Calendar.YEAR,100);
        } else {
            throw new UnsupportedOperationException("No increment available for type "+p.toString());
        }
    }
    
    
    private Calendar nextMonth(Calendar c, int firstDay) {
        Calendar cNew = new GregorianCalendar(c.get(Calendar.YEAR),c.get(Calendar.MONTH),firstDay);
        if (c.get(Calendar.DAY_OF_MONTH) >= firstDay) {
            cNew.add(Calendar.MONTH,1);
        }
        return cNew;
    }
   
    
    private Calendar nextWeek(Calendar c, int firstDay) {
        Calendar cNew = new GregorianCalendar(c.get(Calendar.YEAR),c.get(Calendar.MONTH),c.get(Calendar.DAY_OF_MONTH));

        // Next line is needed to force the calendar to get it self together and stop slacking off
        int updateYourself = cNew.get(Calendar.DAY_OF_WEEK);
        cNew.set(Calendar.DAY_OF_WEEK,firstDay);
        
        if (cNew.compareTo(c) <= 0) {
            cNew.add(Calendar.DAY_OF_MONTH,7);
        }
        return cNew;
    }
    
    public static Calendar nextDay(Calendar c, int firstHour) {
        Calendar cNew = new GregorianCalendar(c.get(Calendar.YEAR),c.get(Calendar.MONTH),c.get(Calendar.DAY_OF_MONTH));

        // Next line is needed to force the calendar to get it self together and stop slacking off
        int updateYourself = cNew.get(Calendar.HOUR_OF_DAY);
        cNew.set(Calendar.HOUR_OF_DAY,firstHour);
        
        if (cNew.compareTo(c) <= 0) {
            cNew.add(Calendar.DAY_OF_MONTH,1);
        }
        return cNew;
    }
    
    public static Calendar nextHour(Calendar c, int firstHour) {
        Calendar cNew = new GregorianCalendar(c.get(Calendar.YEAR),c.get(Calendar.MONTH),c.get(Calendar.DAY_OF_MONTH));

        // Next line is needed to force the calendar to get it self together and stop slacking off
        int updateYourself = cNew.get(Calendar.HOUR_OF_DAY);
        cNew.set(Calendar.HOUR_OF_DAY,firstHour);
        
        if (cNew.compareTo(c) <= 0) {
            cNew.add(Calendar.DAY_OF_MONTH,1);
        }
        return cNew;
    }
    
    public static Calendar nextMinute(Calendar c, int firstMinute) {
        Calendar cNew = new GregorianCalendar(c.get(Calendar.YEAR),c.get(Calendar.MONTH),c.get(Calendar.DAY_OF_MONTH));

        // Next line is needed to force the calendar to get it self together and stop slacking off
        int updateYourself = cNew.get(Calendar.HOUR_OF_DAY);
        cNew.set(Calendar.HOUR_OF_DAY,c.get(Calendar.HOUR_OF_DAY));
        cNew.set(Calendar.MINUTE,c.get(Calendar.MINUTE) + firstMinute);
        
        if (cNew.compareTo(c) <= 0) {
            cNew.add(Calendar.DAY_OF_MONTH,1);
        }
        return cNew;
    }
    
}
