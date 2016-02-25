/*
 * DatePartitionFactory.java
 *
 * Created on September 1, 2005, 9:36 AM
 *
 */

package net.terakeet.soapware.handlers.report;

import java.util.SortedSet;
import java.util.TreeSet;
import java.util.Date;
import java.util.Iterator;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.text.SimpleDateFormat;

/** DatePartitionFactory
 *
 */
public class DatePartitionFactory {
    
    /** Static class, not instantiable */
    private DatePartitionFactory() {
    }
    
    /**  for testing.  Preferred method is createPartitions(ReportPeriod rp)
     */
    private static SortedSet<DatePartition> createPartitions(Date startDate,Date endDate, int offset, int calField) {
        TreeSet<DatePartition> ts = new TreeSet<DatePartition>();
        int indexCounter = 0;
        
        ts.add(new DatePartition(startDate,indexCounter++));
        //Test case: always weekly
        Calendar c0 = new GregorianCalendar();
        c0.setTime(startDate);
        Calendar c = nextWeek(c0, offset);
        
        while (c.getTime().before(endDate) ) {
            ts.add(new DatePartition(c.getTime(),indexCounter++));
            // c.add(calField,1);  //Flexible way
            c.add(Calendar.DAY_OF_MONTH,7);
        }
        
        return ts;
    }
    
    /** create a set of DatePartitions for a report period */
    public static SortedSet<DatePartition> createPartitions(ReportPeriod rp) {
        TreeSet<DatePartition> ts = new TreeSet<DatePartition>();
        int indexCounter = 0;
        
        ts.add(new DatePartition(rp.getStartDate(),indexCounter++));
        Calendar c0 = new GregorianCalendar();
        c0.setTime(rp.getStartDate());
        Calendar c = rp.alignToCalendar(c0);
        
        while (c.getTime().before(rp.getEndDate())) {
            ts.add(new DatePartition(c.getTime(),indexCounter++));
            rp.incrementCalendar(c);
        }
        return ts;
    }
    
    public static void printPartitions(SortedSet<DatePartition> dps) {
        System.out.println(partitionReport(dps));
    }
    
    public static String partitionReport(SortedSet<DatePartition> dps) {
        StringBuilder result = new StringBuilder();
        Iterator<DatePartition> i = dps.iterator();
        while (i.hasNext()) {
            DatePartition dp = i.next();
            result.append(dp.toString());
            result.append("\n");
        }
        return result.toString();
    }
    
    
    
    
    /** MOVED TO ReportPeriod CLASS 
     */
    public static Calendar nextMonth(Calendar c, int firstDay) {
        Calendar cNew = new GregorianCalendar(c.get(Calendar.YEAR),c.get(Calendar.MONTH),firstDay);
        if (c.get(Calendar.DAY_OF_MONTH) >= firstDay) {
            cNew.add(Calendar.MONTH,1);
        }
        return cNew;
    }
   
    /** MOVED TO ReportPeriod CLASS 
     */
    public static Calendar nextWeek(Calendar c, int firstDay) {
        Calendar cNew = new GregorianCalendar(c.get(Calendar.YEAR),c.get(Calendar.MONTH),c.get(Calendar.DAY_OF_MONTH));

        // Next line is needed to force the calendar to get it self together and stop slacking off
        int updateYourself = cNew.get(Calendar.DAY_OF_WEEK);
        cNew.set(Calendar.DAY_OF_WEEK,firstDay);
        
        if (cNew.compareTo(c) <= 0) {
            cNew.add(Calendar.DAY_OF_MONTH,7);
        }
        return cNew;
    }
    
    /** MOVED TO ReportPeriod CLASS 
     */
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
    
    public static void testCal() {
        try {
            Calendar c = new GregorianCalendar();
            SimpleDateFormat f = new SimpleDateFormat("MM/dd/yy HH:mm");
            Date midMay = f.parse("05/03/04 04:56");
            Date midDec = f.parse("12/18/04 04:56");
            Date mayFirst = f.parse("05/01/04 00:00");
            Date morning = f.parse("05/05/04 07:30");
            Date afternoon = f.parse("05/05/04 14:19");
            Date evening = f.parse("05/05/04 18:00");
            
            c.setTime(midDec);
            
            // Try hitting the next month
            Calendar testCal = new GregorianCalendar();          
            int calHour = 8;
            testCal.setTime(mayFirst);
            pr("00:00 to next day "+nextWeek(testCal,calHour).getTime().toString());
            testCal.setTime(morning);
            pr("07:30 to next day: "+nextWeek(testCal,calHour).getTime().toString());
            testCal.setTime(afternoon);
            pr("14:19 to next day: "+nextWeek(testCal,calHour).getTime().toString());
            testCal.setTime(evening);
            pr("18:00 to next day: "+nextWeek(testCal,calHour).getTime().toString());
            
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    public static void testDps() {
        try {
            // DEBUG TESTS
            SimpleDateFormat f = new SimpleDateFormat("MM/dd/yy");
            Date startDate = f.parse("01/03/04");
            Date endDate = f.parse("07/07/04");
            Date midDate1 = f.parse("03/08/04");
            Date midDate2 = f.parse("05/03/04");
            Date midDate3 = f.parse("02/18/04");
            SortedSet<DatePartition> dps = createPartitions(startDate,endDate,Calendar.SUNDAY,Calendar.MONTH);
            printPartitions(dps);
            DatePartitionTree tree = new DatePartitionTree(dps);
            System.out.println("Index for "+midDate1.toString()+" : "+tree.getIndex(midDate1));
            System.out.println("Index for "+midDate2.toString()+" : "+tree.getIndex(midDate2));
            System.out.println("Index for "+midDate3.toString()+" : "+tree.getIndex(midDate3));
            System.out.println("Finished");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    public static void testDpsCompact() {
        SimpleDateFormat f = new SimpleDateFormat("MM/dd/yy HH:mm");
        try {
            SortedSet<DatePartition> dps = createPartitions(
                    new ReportPeriod(PeriodType.MONTHLY,"1", 
                        f.parse("09/09/03 23:15"),f.parse("09/30/05 21:30")));
            printPartitions(dps);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    private static void pr(String s){
        System.out.println(s);
    }
    
    public static void main(String[] args) {
        testDpsCompact();
    }
    
}
