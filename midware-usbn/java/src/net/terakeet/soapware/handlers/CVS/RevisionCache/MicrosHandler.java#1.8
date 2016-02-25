package net.terakeet.soapware.handlers;
/*
 * DigitalDiningHandler.java
 *
 * Created on October 24, 2005, 1:00 PM
 */

import java.io.*;
import net.terakeet.soapware.*;
import net.terakeet.util.MidwareLogger;
import org.apache.log4j.Logger;
import org.dom4j.Element;
import java.text.ParseException;
import java.util.*;
import java.sql.*;
import java.util.Set;

public class MicrosHandler implements Handler {
    private MidwareLogger logger;
    private static final String connName = "auper";
    
    
    public MicrosHandler() {
        logger = new MidwareLogger(MicrosHandler.class.getName());
    }
    
    public void handle(Element toHandle, Element toAppend) throws HandlerException{
        String function = toHandle.getName();
        String responseNamespace = (String)SOAPMessage.getURIMap().get("tkmsg");
        
        logger = new MidwareLogger(MicrosHandler.class.getName(), function);
        logger.debug("MicrosHandler processing method: "+function);
        logger.xml("request: " + toHandle.asXML());
        
        if ("sendDetail".equals(function)) {
            sendDetail(toHandle, responseFor(function, toAppend));
        } else {
            logger.generalWarning("Unknown function '" + function + "'.");
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
        c.close();
    }
    
    private void sendDetail(Element toHandle, Element toAppend) throws HandlerException {
        
        int location = HandlerUtils.getRequiredInteger(toHandle, "location");

        List itemList = toHandle.elements("item");
        int qty;
        long epoch;
        String objectId, date, price, name;
        
        RegisteredConnection conn = DatabaseConnectionManager.getNewConnection(connName, 
                "sendDetail (MicrosHandler)");
        PreparedStatement stmt = null;
        
        String sql = null;
        boolean oldAutoCommit = true;
        
        try {
            oldAutoCommit = conn.getAutoCommit();
            
            sql = "INSERT INTO sales (pluNumber, price, quantity, date, location)" +
                    " values (?,?,?,?,?)";
            //logger.dbAction("Insert Statement: " + sql);
            
            conn.setAutoCommit(false);
            
            stmt = conn.prepareStatement(sql);
            
            Timestamp tstamp = null;
            
            for (Object o : itemList) {
                Element item = (Element) o;
                // convert from seconds to milliseconds
                epoch = HandlerUtils.getRequiredLong(item, "epoch") * 1000;
                objectId = HandlerUtils.getRequiredDigitString(item, "objectId");
                qty = HandlerUtils.getRequiredInteger(item, "qty");
                price = HandlerUtils.getRequiredString(item, "price");
                name = HandlerUtils.getRequiredString(item, "name");
                tstamp = new Timestamp(epoch);
                logger.dbAction("Inserting: " + name + " (" + objectId +
                        ", " + price + ", " + qty + ", " + tstamp + ", " +
                        location + ")");
                
                stmt.setString(1, objectId);
                stmt.setString(2, price);
                stmt.setInt(3, qty);
                stmt.setTimestamp(4, tstamp);
                stmt.setInt(5, location);
                stmt.executeUpdate();
            }
            
            if (tstamp != null) {
                // record that a sales reading happened in the location table
                sql = " UPDATE location SET lastSold=GREATEST(lastSold,?) WHERE id=?";
                stmt = conn.prepareStatement(sql);
                stmt.setTimestamp(1,tstamp);
                stmt.setInt(2,location);
                stmt.executeUpdate();
            }
            
            conn.commit();
            
        } catch (SQLException sqle) {
            if (null != conn) {
                try {
                    conn.rollback();
                } catch (SQLException ignore) {}
            }
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            if (null != conn && null != sql) {
                try {
                    conn.setAutoCommit(oldAutoCommit);
                } catch (SQLException ignore) {}
            }
            close(stmt);
            close(conn);
        }
    }
    
}
