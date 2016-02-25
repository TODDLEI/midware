package net.terakeet.soapware.handlers;
/*
 * DigitalDiningHandler.java
 *
 * Created on October 24, 2005, 1:00 PM
 */

import java.io.*;
import com.linuxense.javadbf.*;
import net.terakeet.soapware.Handler;
import net.terakeet.soapware.HandlerException;
import net.terakeet.soapware.HandlerUtils;
import net.terakeet.soapware.SOAPMessage;
import net.terakeet.soapware.RegisteredConnection;
import net.terakeet.soapware.DatabaseConnectionManager;
import net.terakeet.util.MidwareLogger;
import org.apache.log4j.Logger;
import org.dom4j.Element;
import java.util.Date;
import java.util.Calendar;
import java.util.Collection;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;
import java.util.SortedSet;
import java.util.Iterator;
import java.text.SimpleDateFormat;
import java.text.ParseException;
import java.sql.*;
import java.lang.Integer;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.CallableStatement;
import java.util.Set;
import java.text.DecimalFormat;

public class DigitalDiningHandler implements Handler {
    
    private MidwareLogger logger = new MidwareLogger(DigitalDiningHandler.class.getName());
    private static final String connName = "auper";
    static final long DD_DB_TIMEOUT_MILLIS = 10 * 60 * 1000; // 10 minute timeout
    private RegisteredConnection conn;
    private static SimpleDateFormat dateFormat =
            new SimpleDateFormat("MM/dd/yyyy HH:mm");

    
    public DigitalDiningHandler() throws HandlerException {
        logger = new MidwareLogger(DigitalDiningHandler.class.getName());
        conn = null;
    }
    
    public void handle(Element toHandle, Element toAppend) throws HandlerException{
        
        String function = toHandle.getName();
        String responseNamespace = (String)SOAPMessage.getURIMap().get("tkmsg");
        
        logger = new MidwareLogger(DigitalDiningHandler.class.getName(), function);
        logger.debug("DigitalDiningHandler processing method: "+function);
        logger.xml("request: " + toHandle.asXML());
        
        conn = DatabaseConnectionManager.getNewConnection(connName,
                function+" (DigitalDiningHandler)",DD_DB_TIMEOUT_MILLIS);
        
        try {
            if ("parseDBFFile".equals(function)) {
                parseDBFFile(toHandle, responseFor(function, toAppend));
            } else if ("getSales".equals(function)) {
                getSales(toHandle, responseFor(function, toAppend));
            } else {
                logger.generalWarning("Unknown function '" + function + "'.");
            }
        } catch (Exception e) {
            if (e instanceof HandlerException) {
                throw (HandlerException) e;
            } else {
                logger.midwareError("Non-handler exception thrown in ReportHandler: "+e.toString());
                logger.midwareError("XML: " + toHandle.asXML());
                throw new HandlerException(e);
            }
        } finally {
            int queryCount = conn.getQueryCount();
            logger.dbAction("Executed "+queryCount+" quer"+(queryCount==1 ? "y" : "ies"));
            conn.close();
        }
        
        logger.xml("response: " + toAppend.asXML());
        
    }
    
    private Element responseFor(String s, Element e) {
        String responseNamespace = (String)SOAPMessage.getURIMap().get("tkmsg");
        return e.addElement("m:"+s+"Response",responseNamespace);
    }
    private void close(Statement s) {
        if (s != null) {
            try { s.close(); } catch (SQLException sqle) { }
        }
    }
    private void close(ResultSet rs) {
        if (rs != null) {
            try { rs.close(); } catch (SQLException sqle) { }
        }
    }
    private void close(Connection c) {
        if (c != null) {
            try { c.close(); } catch (SQLException sqle) { }
        }
    }
    private void close(RegisteredConnection c) {
        if (c != null) {
            c.close();
        }
    }
    
    /**   Reads sales data from a dbf file into the sales database.  Calling this
     * method will cause the middleware to look for the appropriate dbf file for the
     * specified location (the path is listed in the database), and attempt to parse
     * it for the day.   If the file is not present, not readable, or contains fewer
     * records than the database already holds for the day, then the contents of the
     * file will be ignored and this method will have no effect.
     */
    private void parseDBFFile(Element toHandle, Element toAppend) throws HandlerException {
        
        int location = HandlerUtils.getRequiredInteger(toHandle, "locationId");
        String month = HandlerUtils.getOptionalString(toHandle, "month");
        String year = HandlerUtils.getOptionalString(toHandle, "year");
        String day = HandlerUtils.getOptionalString(toHandle, "day");
        String digitalDiningFolder = null;
        
        String getFilePath = "SELECT directory FROM digitalDining WHERE location = ?";
        String insertSale = "INSERT INTO sales (pluNumber, quantity, date, location, reportRecordId)" +
                " VALUES (?,?,?,?,?)";
        String getPlus = "SELECT plu FROM beverage WHERE location=?";
        String checkSalesRecord = "SELECT sid,quantity FROM sales WHERE location=? " +
                " AND reportRecordId=? AND date>=?";

        
        PreparedStatement stmt = null;
        ResultSet rs = null;
        
        /* maps plu-strings to an integer code to indicate if the plu in on file
        *   1 = We have the plu, and the sale should be recorded
        *   -1 = Not in our system, ignore it.  Probably a burger or something.
        */
        Set<String> validPlus = new HashSet<String>();      
        try {
            // Formatting date to 'yyyy-mm-dd 00:00:00'
            Calendar rightNow = Calendar.getInstance();
            long nowMillis = rightNow.getTimeInMillis();
            
            // DUMAC runs on a 4AM rotation - So, if it's between midnight and 3:59AM we're still on
            // the current day. We'll fix by subtracting 4 hours from the current server time.
            rightNow.add(Calendar.HOUR, -4);
            
            DecimalFormat myFormatter = new DecimalFormat("00");
            if (month == null) {
                month = myFormatter.format((rightNow.get(Calendar.MONTH)+1));
            }
            if (day == null) {
                day = myFormatter.format(rightNow.get(Calendar.DAY_OF_MONTH));
            }
            if (year == null) {
                year = myFormatter.format(rightNow.get(Calendar.YEAR));
            }
            String rightNowFormatted = rightNow.get(Calendar.YEAR) + "-" + month + "-" + day + " 00:00:00";
            
            
            
            // Pull digital dining directory for location.
            try {
                stmt = null;
                stmt = conn.prepareStatement(getFilePath);
                stmt.setInt(1, location);
                rs = stmt.executeQuery();
                if (rs.next()) {
                    digitalDiningFolder = rs.getString(1);
                } else {
                    logger.midwareError("No directory path for Loc#"+location);
                }
            } catch (SQLException sqle) {
                logger.dbError("Database error pulling DD location: " + sqle.getMessage());
                throw new HandlerException(sqle);
            } finally {
                close(rs);
            }
            
            //loggingCounters
            int logValidPlus = 0;
            int logTotalReportRecords = 0;
            int logIgnoredPluRecords = 0;
            int logAlreadyRecorded = 0;
            int logSales = 0;
            int logSqlProblems = 0;
            int logVoids = 0;
            
            //Read the file and build a set of sales records
            InputStream inputStream = null;
            if (digitalDiningFolder != null) {
                try {
                    
                    //build our set of valid PLUS
                    logger.debug("Creating PLU Set");
                    stmt = conn.prepareStatement(getPlus);
                    stmt.setInt(1,location);
                    rs = stmt.executeQuery();
                    while (rs.next()) {
                        validPlus.add(rs.getString(1));
                    }
                    logValidPlus = validPlus.size();
                    logger.debug("PLU Set created");
                    
                    File inputFile = new File("/home/dumac/" +
                        digitalDiningFolder + "/OR" + month + day + year.substring(2,4) + ".dbf");
                    inputStream  = new FileInputStream(inputFile);
                    DBFReader reader = new DBFReader(inputStream);
                    Object []rowObjects;    

                    //boolean first = true;
                    while((rowObjects = reader.nextRecord()) != null) {  
                        logTotalReportRecords++;
                        
                        //if (first) {logger.debug("pulling row");}
                        // Assigning relevant fields from dbf row
                        Integer pluNumber = new Integer((int) ((Double) rowObjects[11]).doubleValue());
                        Double quantity = (Double) rowObjects[14]; 
                        Date date = (Date) rowObjects[0];
                        String time = (String) rowObjects[1];
                        Integer rowId = new Integer((int) ((Double) rowObjects[32]).doubleValue());
                        //if (first) {logger.debug("row pulled");}
                        
                        // find out if the PLU is something that we track                        
                        if (validPlus.contains(String.valueOf(pluNumber))) {
                            //lets set up the two date strings we need, the day and the time.
                            
                            // Another DUMAC fix
                            // If it's before 4AM, increment the day. This will properly set the sale for
                            // the current day.
                            Calendar dumacCal = Calendar.getInstance();
                            dumacCal.setTime(date);
                            int hourDumac = Integer.valueOf(time.substring(0,2));
                            int minuteDumac = Integer.valueOf(time.substring(3,5));
                            if (hourDumac < 4) {
                                dumacCal.add(Calendar.DAY_OF_MONTH, 1);
                            }
                            String dumacDay =  // "yyyy-MM-dd "
                                    String.valueOf(dumacCal.get(Calendar.YEAR)) + "-" +
                                    String.valueOf(dumacCal.get(Calendar.MONTH)+1) + "-" +
                                    String.valueOf(dumacCal.get(Calendar.DAY_OF_MONTH)) + " ";
                            String dumacTime = // "hh:mm:ss"                                 
                                    String.valueOf(hourDumac) + ":" + String.valueOf(minuteDumac) +
                                        ":00";
                            
                            //see if this rowId already exists
                            //if (first) {logger.debug("checking row");}
                            stmt = conn.prepareStatement(checkSalesRecord);
                            stmt.setInt(1,location);
                            stmt.setInt(2, rowId);
                            stmt.setString(3,dumacDay);
                            rs = stmt.executeQuery();

                            boolean matches = false;
                            boolean hasResults = false;
                            while (rs.next() && !matches) {
                                hasResults = true;
                                //see if any records match the quantity
                                double difference = Math.abs(rs.getDouble(2)-quantity.doubleValue());
                                matches = matches || (difference < 0.1);
                            }
                            if (matches) {    
                                //if (first) {logger.debug("row exists");}
                                logAlreadyRecorded++;
                            } else {
                                //if (first) {logger.debug("row doesn't exist");}        
                                //we need to insert this record
                                try {
                                    stmt = conn.prepareStatement(insertSale);
                                    stmt.setInt(1, pluNumber);
                                    stmt.setDouble(2, quantity);
                                    stmt.setString(3, dumacDay+dumacTime);
                                    stmt.setInt(4, location);
                                    stmt.setInt(5, rowId);
                                    stmt.executeUpdate();
  
                                    if (hasResults) {
                                        logVoids++;
                                    } else {
                                        logSales++;
                                    }
                                    //if (first) {logger.debug("row inserted");}
                                } catch (SQLException sqle) {
                                    logger.dbError("SQL Error during row insert ("+rowId+"): "+sqle.toString());
                                    logSqlProblems++;
                                }
                            }
                            //first = false;
                        } else {
                            // the plu isn't on file, so we don't want this reading
                            logIgnoredPluRecords++;
                        }  
                    }
 
                    // record that a sales reading happened in the location table
                    String sql = " UPDATE location SET lastSold=GREATEST(lastSold,?) WHERE id=?";
                    stmt = conn.prepareStatement(sql);
                    stmt.setTimestamp(1,new Timestamp(nowMillis));
                    stmt.setInt(2,location);
                    stmt.executeUpdate();

                } catch( DBFException e) {        
                    logger.readingWarning( e.getMessage());
                } catch( IOException e) {
                    logger.readingWarning( e.getMessage());
                } finally {
                    try {inputStream.close();} catch (Exception e) {}
                }
                logger.debug("Parse DBF Complete for '"+digitalDiningFolder+"': \n"+
                        " Valid PLUS: "+logValidPlus+" \n"+
                        " TotalRecords: "+logTotalReportRecords+" \n"+
                        "   > Added   : "+logSales+" sale(s) "+logVoids+" void(s) \n"+
                        "   > Bad PLU : "+logIgnoredPluRecords+" \n"+
                        "   > Not new : "+logAlreadyRecorded+" \n"+
                        "   > SQL Err : "+logSqlProblems+" \n"+
                        "   > Other(?): "+
                           (logTotalReportRecords
                           -logSales
                           -logVoids
                           -logIgnoredPluRecords
                           -logAlreadyRecorded
                           -logSqlProblems)
                        );
            } 
        } catch (SQLException sqle) {
            logger.dbError("SQL Exception in parsedbf "+sqle.toString());
        } finally {
            close(rs);
            close(stmt);
        }
    }
    
    private void getSales(Element toHandle, Element toAppend)
    throws HandlerException{
        int location = HandlerUtils.getRequiredInteger(toHandle,"locationId");
        String start = HandlerUtils.getRequiredString(toHandle, "startDate");
        String end = HandlerUtils.getRequiredString(toHandle, "endDate");
        
        String SQL = "SELECT pluNumber, quantity FROM sales WHERE location = ? "	+
                " AND date BETWEEN ? AND ?";
              
        PreparedStatement stmt = null;
        ResultSet rs = null;
        
        try {
            stmt = conn.prepareStatement(SQL);
            stmt.setInt(1, location);
            stmt.setString(2, start);
            stmt.setString(3, end);
            rs = stmt.executeQuery();
            while (rs.next()) {
                Element sale = toAppend.addElement("sale");
                sale.addElement("pluNumber").addText(HandlerUtils.nullToEmpty(rs.getString(1)));
                sale.addElement("quantity").addText(HandlerUtils.nullToEmpty(rs.getString(2)));
            }
            
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(rs);
            close(stmt);
        }      
    }
}


