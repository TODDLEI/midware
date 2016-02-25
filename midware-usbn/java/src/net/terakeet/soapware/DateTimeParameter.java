/*
 * DateParameter.java
 *
 * Created on February 19, 2007, 11:44 AM
 *
 * (c) 2007 Terakeet Corp.
 */

package net.terakeet.soapware;

import java.util.regex.*;
import java.util.Date;
import java.text.SimpleDateFormat;

/**  Parses a string into a date or a date string.  
 *  Acceptable input formats are "yyyy-MM-dd HH:mm:ss" or "MM-dd-yyyy HH:mm:ss"
 *  The time can be empty, HH:mm:ss, or HH:mm
 *  The dash may be replaced by any non-numeric character (like \ or /)
 *  After attempting to parse a date, check that this object is valid using the
 *  isValid method before calling toString to toDate
 */
public class DateTimeParameter {
    
    final boolean valid;
    String year;
    String month;
    String day;
    String hour;
    String minute;
    String second;
    
    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private static final Pattern p1 = Pattern.compile(
            "^\\s*(\\d{4})\\D(\\d{1,2})\\D(\\d{1,2})( (\\d{2}):(\\d{2})(:(\\d{2}))?)?.*");
    private static final Pattern p2 = Pattern.compile(
            "^\\s*(\\d{1,2})\\D(\\d{1,2})\\D(\\d{4})( (\\d{2}):(\\d{2})(:(\\d{2}))?)?.*");
    
    /**  Attempt to parse a date string.  
     *  Acceptable input formats are "yyyy-MM-dd HH:mm:ss" or "MM-dd-yyyy HH:mm:ss"
     *  The time can be empty, HH:mm:ss, or HH:mm
     *  The dash may be replaced by any non-numeric character (like \ or /)
     *  If the input is not a valid date, 
     *  this method will not throw an exception, but the method "isValid" will return false;
     */
    public DateTimeParameter(String input) {
         Matcher m = p1.matcher(input);
         if (m.matches()) {
             year = m.group(1);
             month = m.group(2);
             day = m.group(3);
             hour = m.group(5);
             minute = m.group(6);
             second = m.group(8);
             valid = true;
         } else {
             m = p2.matcher(input);
             if (m.matches()) {
                 year = m.group(3);
                 month = m.group(1);
                 day = m.group(2);
                 hour = m.group(5);
                 minute = m.group(6);
                 second = m.group(8);
                 valid = true;
             } else {
                 year = "";
                 month = "";
                 day = "";
                 hour = "";
                 minute = "";
                 second = "";
                 valid = false;
             }
         }
         if (valid && hour == null || hour.equals("")) {
             hour = "00";
         }
         if (valid && minute == null || minute.equals("")) {
             minute = "00";
         }
         if (valid && second == null || second.equals("")) {
             second = "00";
         }
         if (day.length() == 1) {
             day = "0"+day;
         }
         if (month.length() == 1) {
             month = "0"+month;
         }
    }

    public DateTimeParameter(Date input) {

        year = String.valueOf(input.getYear() + 1900);
        month = String.valueOf(input.getMonth() + 1);
        day = String.valueOf(input.getDate());
        hour = String.valueOf(input.getHours());
        minute = String.valueOf(input.getMinutes());
        second = String.valueOf(input.getSeconds());
        valid = true;
    }
    
    /**  Returns true if the date contained within is valid, false otherwise.  If the date is not valid, 
     * then no other member methods should be called; this instance should be discarded.
     */
    public boolean isValid() {
        return valid;
    }

    public void addHour(int hourVal) {
        int curHour = Integer.parseInt(hour) + hourVal;
        if (curHour > 23) {
            curHour = curHour - 24;
        }
        if (String.valueOf(curHour).length() == 1) {
            hour = "0" + String.valueOf(curHour);
         } else {
            hour = String.valueOf(curHour);
         }
    }

    public void setHour(int hourVal) {
        if (String.valueOf(hourVal).length() == 1) {
            hour = "0" + String.valueOf(hourVal);
         } else {
            hour = String.valueOf(hourVal);
         }
    }

    public void setMinute(int minuteVal) {
        if (String.valueOf(minuteVal).length() == 1) {
            minute = "0" + String.valueOf(minuteVal);
         } else {
            minute = String.valueOf(minuteVal);
         }
    }

    public void setSeconds(int secVal) {
        if (String.valueOf(secVal).length() == 1) {
            second = "0" + String.valueOf(secVal);
         } else {
            second = String.valueOf(secVal);
         }
    }
    
    /** Returns dates in the "YYYY-MM-DD HH:mm:ss" format if this date is valid, otherwise the string "INVALID"
     */
    public String toString() {
        if (valid) {
            StringBuilder result = new StringBuilder(20);
            result.append(year);
            result.append("-");
            result.append(month);
            result.append("-");
            result.append(day);
            result.append(" ");
            result.append(hour);
            result.append(":");
            result.append(minute);
            result.append(":");
            result.append(second);
            return result.toString();
        } else {
            return ("INVALID");                
        }
    }

    /** Returns dates in the "YYYY-MM-DD HH:mm:ss" format if this date is valid, otherwise the string "INVALID"
     */
    public String toDateString() {
        if (valid) {
            StringBuilder result = new StringBuilder(10);
            result.append(year);
            result.append("-");
            result.append(month);
            result.append("-");
            result.append(day);
            return result.toString();
        } else {
            return ("INVALID");
        }
    }
    
    /**  Returns the java Date of this date if its valid, or today's today otherwise.
     */
    public Date toDate() {
        if (valid) {
            try {
                return dateFormat.parse(this.toString());
            } catch (Exception e) {
                //ignore
                return new Date();
            }
        } else {
            return new Date();
        }
    }
    
}
