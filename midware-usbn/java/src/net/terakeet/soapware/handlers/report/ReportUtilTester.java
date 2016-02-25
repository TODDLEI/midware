/*
 * ReportUtilTester.java
 *
 * Created on February 19, 2007, 1:33 PM
 *
 * (c) 2007 Terakeet Corp.
 */

package net.terakeet.soapware.handlers.report;

import net.terakeet.soapware.DateParameter;
import net.terakeet.soapware.DateTimeParameter;

/**
 */
public class ReportUtilTester {
    
    private ReportUtilTester() {
    }
    
    public static void testD(String toTest) {
        DateParameter date = new DateParameter(toTest);
        if (date.isValid()) {
            System.out.println("PASSED '"+toTest+"' to '"+date.toString()+"' java date: "+date.toDate().toString());
        } else {
            System.out.println("FAILED '"+toTest+"'");
        }
    }
    public static void testDT(String toTest) {
        DateTimeParameter date = new DateTimeParameter(toTest);
        if (date.isValid()) {
            System.out.println("PASSED '"+toTest+"' to '"+date.toString()+"' java date: "+date.toDate().toString());
        } else {
            System.out.println("FAILED '"+toTest+"'");
        }
    }
    
    public static void main(String[] args) {
        testD("          2006-04-12 12:43");
        testD(" 2006-06-12 12:43");
        testD(" x  2006-12-01 12:43");
        testD("  2006-01-12 12:43:00");
        testD("1994-04-12");
        testD("2006/1X/19");
        testD("10-23-2004");
        testD("1/1/1999");
        testD("10/20/2000 12:20");
        
        testDT("          2006-04-12 12:43");
        testDT(" 2006-06-12 12:43");
        testDT(" x  2006-12-01 12:43");
        testDT("  2006-01-12 12:43:00");
        testDT("1994-04-12");
        testDT("2006/1X/19");
        testDT("10-23-2004");
        testDT("1/1/1999");
        testDT("10/20/2000 12:20");
    }
    
}
