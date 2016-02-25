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
 *  Acceptable input formats are "yyyy-MM-dd" or "MM-dd-yyyy"
 *  The dash may be replaced by any non-numeric character (like \ or /)
 *  After attempting to parse a date, check that this object is valid using the
 *  isValid method before calling toString to toDate
 */
public class DateParameter {
    
    final boolean valid;
    String year;
    String month;
    String day;
    
    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
    private static final Pattern p1 = Pattern.compile("^\\s*(\\d{4})\\D(\\d{1,2})\\D(\\d{1,2}).*");
    private static final Pattern p2 = Pattern.compile("^\\s*(\\d{1,2})\\D(\\d{1,2})\\D(\\d{4}).*");
    
    /**  Attempt to parse a date string.  
     *  Acceptable input formats are "yyyy-MM-dd" or "MM-dd-yyyy"
     *  The dash may be replaced by any non-numeric character (like \ or /)
     *  If the input is not a valid date, 
     *  this method will not throw an exception, but the method "isValid" will return false;
     */
    public DateParameter(String input) {
         Matcher m = p1.matcher(input);
         if (m.matches()) {
             year = m.group(1);
             month = m.group(2);
             day = m.group(3);
             valid = true;
         } else {
             m = p2.matcher(input);
             if (m.matches()) {
                 year = m.group(3);
                 month = m.group(1);
                 day = m.group(2);
                 valid = true;
             } else {
                 year = "";
                 month = "";
                 day = "";   
                 valid = false;
             }
         }
         if (day.length() == 1) {
             day = "0"+day;
         }
         if (month.length() == 1) {
             month = "0"+month;
         }
    }

    /**  Attempt to parse a date string.
     *  Acceptable input formats are "yyyy-MM-dd" or "MM-dd-yyyy"
     *  The dash may be replaced by any non-numeric character (like \ or /)
     *  If the input is not a valid date,
     *  this method will not throw an exception, but the method "isValid" will return false;
     */
    public DateParameter(Date input) {

        year = String.valueOf(input.getYear() + 1900);
        month = String.valueOf(input.getMonth() + 1);
        day = String.valueOf(input.getDate());
        valid = true;

        if (day.length() == 1) {
            day = "0"+day;
        }
        if (month.length() == 1) {
            month = "0"+month;
        }
    }
    
    /**  Returns true if the date contained within is valid, false otherwise.  If the date is not valid, 
     * then no other member methods should be called; this instance should be discarded.
     */
    public boolean isValid() {
        return valid;
    }
    
    /** Returns dates in the YYYY-MM-DD format if this date is valid, otherwise the string "INVALID"
     */
    public String toString() {
        if (valid) {
            StringBuilder result = new StringBuilder(12);
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
