/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package net.terakeet.soapware.handlers;

/**
 *
 * @author suba
 */
import net.terakeet.soapware.*;
import net.terakeet.soapware.security.*;
import net.terakeet.util.MidwareLogger;
import org.apache.log4j.Logger;
import org.dom4j.Element;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.text.*;
import java.text.SimpleDateFormat;
import net.terakeet.util.TemplatedMessage;
import net.terakeet.util.ParameterFile;
import net.terakeet.util.MailException;
import net.terakeet.soapware.handlers.report.*;
import net.terakeet.usbn.WebPermission;


import javapns.Push;
import javapns.communication.exceptions.CommunicationException;
import javapns.communication.exceptions.KeystoreException;
import javapns.notification.PushNotificationPayload;
import javapns.notification.Payload;


import com.google.android.gcm.server.Message;
import com.google.android.gcm.server.Result;
import com.google.android.gcm.server.Sender;
import java.awt.Color;
import java.io.*;
import java.math.BigDecimal;
import java.net.URL;
import net.terakeet.util.*;
import java.awt.image.BufferedImage;
import javax.imageio.ImageIO;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;

import org.json.JSONObject;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.XML;
import javax.media.jai.JAI;
	

public class BeerBoardHandler  implements Handler{
    
    private MidwareLogger logger;
    private static final String transConnName
                                            = "auper";
    private RegisteredConnection transconn;
    private SecureSession ss;
    private DecimalFormat cf;
    private LocationMap locationMap;
    private static SimpleDateFormat dateFormat =
            new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
    private static SimpleDateFormat dbDateFormat
                                            = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private static SimpleDateFormat timeFormat
                                            = new SimpleDateFormat("h:mm a");
    private RegionProductMap regionProductMap;
    private RegionExclusionMap regionExclusionMap;
    private ParentLevelMap parentLevelMap;
    private ChildLevelMap childLevelMap;

    /**
     * Creates a new instance of BeerBoardHandler
     */
    public BeerBoardHandler() throws HandlerException {
        HandlerUtils.initializeClientKeyManager();
        logger                              = new MidwareLogger(SQLBOSSHandler.class.getName());
        transconn                           = null;
        locationMap                         = null;
        cf                                  = (DecimalFormat) NumberFormat.getInstance(Locale.US);
    }
    
    public void handle(Element toHandle, Element toAppend) throws HandlerException{
        
        String function                     = toHandle.getName();
        String responseNamespace            = (String)SOAPMessage.getURIMap().get("tkmsg");
        
        String clientKey                    = HandlerUtils.getOptionalString(toHandle,"clientKey");
        ss                                  = ClientKeyManager.getSession(clientKey);
        
        logger                              = new MidwareLogger(SQLBOSSHandler.class.getName(), function);
        logger.debug("BeerBoardHandler processing method: "+function);
        logger.xml("request: " + toHandle.asXML());
        
        transconn                           = DatabaseConnectionManager.getNewConnection(transConnName, function + " (BeerBoardHandler)");
        
        cf.applyPattern("#.####");
        try {
            // All methods require an admin client key
            if (ss.getLocation() == 0 && ss.getClientId() == 1 && ss.getSecurityLevel().canAdmin()) {
                if ("addBevSyncCustomer".equals(function)) {
                    addBevSyncCustomer(toHandle, responseFor(function,toAppend));
                } else if ("updateBevSyncCustomer".equals(function)) {
                    updateBevSyncCustomer(toHandle, responseFor(function,toAppend));
                } else if ("getBevSyncCustomers".equals(function)) {
                    getBevSyncCustomers(toHandle, responseFor(function,toAppend));
                } else if ("addBevSyncUser".equals(function)) {
                    addBevSyncUser(toHandle, responseFor(function,toAppend));
                }else if ("updateBevSyncUser".equals(function)) {
                    updateBevSyncUser(toHandle, responseFor(function,toAppend));
                }else if ("changeBevSyncPassword".equals(function)) {
                    changeBevSyncPassword(toHandle, responseFor(function,toAppend));
                } else if ("updateBevSyncUserPermissions".equals(function)) {
                    updateBevSyncUserPermissions(toHandle, responseFor(function,toAppend));
                }else if ("moveBevSyncUser".equals(function)) {
                    moveBevSyncUser(toHandle, responseFor(function,toAppend));
                }else if ("getBevSyncUsers".equals(function)) {
                    getBevSyncUsers(toHandle, responseFor(function,toAppend));
                } else if ("getBevSyncUserDetail".equals(function)) {
                    getBevSyncUserDetail(toHandle, responseFor(function,toAppend));
                } else if ("deleteBevSyncUser".equals(function)) {
                    deleteBevSyncUser(toHandle, responseFor(function,toAppend));
                } else if ("addUpdateDeleteCreatives".equals(function)) {
                    addUpdateDeleteCreatives(toHandle, responseFor(function,toAppend));
                } else if ("getCreatives".equals(function)) {
                    getCreatives(toHandle, responseFor(function,toAppend));
                } else if ("addUpdateDeleteCampaign".equals(function)) {
                    addUpdateDeleteCampaign(toHandle, responseFor(function,toAppend));
                } else if ("getCampaignSupports".equals(function)) {
                    getCampaignSupports(toHandle, responseFor(function,toAppend));
                } else if ("getBevSyncBeerboardList".equals(function)) {
                    getBevSyncBeerboardList(toHandle, responseFor(function,toAppend));
                } else if ("getCampaign".equals(function)) {
                    getCampaign(toHandle, responseFor(function,toAppend));
                } else if ("addUpdateDeleteReward".equals(function)) {
                    addUpdateDeleteReward(toHandle, responseFor(function,toAppend));
                } else if ("getCampaignReport".equals(function)) {
                    getCampaignReport(toHandle, responseFor(function,toAppend));
                } else if ("getBevSyncRegions".equals(function)) {
                    getBevSyncRegions(toHandle, responseFor(function,toAppend));
                } else if ("getTapShare".equals(function)) {
                    getTapShare(toHandle, responseFor(function, toAppend));
                } else if ("getCampaignLogs".equals(function)) {
                    getCampaignLogs(toHandle, responseFor(function, toAppend));
                }else if ("getBevSyncDashboardReport".equals(function)) {
                    getBevSyncDashboardReport(toHandle, responseFor(function, toAppend));
                }else if ("addUpdateDeleteBBTVGraphics".equals(function)) {
                    addUpdateDeleteBBTVGraphics(toHandle, responseFor(function, toAppend));
                } else if ("getBBTVGraphics".equals(function)) {
                    getBBTVGraphics(toHandle, responseFor(function, toAppend));
                }else if ("addUpdateDeleteBBTVSchedule".equals(function)) {
                    addUpdateDeleteBBTVSchedule(toHandle, responseFor(function, toAppend));
                } else if ("getBBTVSchedule".equals(function)) {
                    getBBTVSchedule(toHandle, responseFor(function, toAppend));
                } else if ("resendPushMessage".equals(function)) {
                    resendPushMessage(toHandle, responseFor(function, toAppend));
                } else if ("getBeerBoardMobileAppReport".equals(function)) {
                    getBeerBoardMobileAppReport(toHandle, responseFor(function, toAppend));
                } else if ("getBBTVStatus".equals(function)) {
                    getBBTVStatus(toHandle, responseFor(function, toAppend));
                } else if ("getWakeupMessage".equals(function)) {
                    getWakeupMessage(toHandle, responseFor(function, toAppend));
                } else if ("addUpdateDeleteWakeupMessage".equals(function)) {
                    addUpdateDeleteWakeupMessage(toHandle, responseFor(function, toAppend));
                } else if ("addUpdateDeleteBanner".equals(function)) {
                    addUpdateDeleteBanner(toHandle, responseFor(function,toAppend));
                } else if ("getProfinityBlackList".equals(function)) {
                    getProfinityBlackList(toHandle, responseFor(function,toAppend));
                } else if ("addRemoveBlackList".equals(function)) {
                    addRemoveBlackList(toHandle, responseFor(function,toAppend));
                } else if ("addUpdateDeleteInsiderPass".equals(function)) {
                    addUpdateDeleteInsiderPass(toHandle, responseFor(function,toAppend)); 
                } else if ("sendBBMobileNotification".equals(function)) {
                    sendBBMobileNotification(toHandle, responseFor(function,toAppend));
                } else if ("addLocationPushNotification".equals(function)) {
                    addLocationPushNotification(toHandle, responseFor(function,toAppend));
                } else if ("sendBevMobileNotification".equals(function)) {
                    sendBevMobileNotification(toHandle, responseFor(function,toAppend));
                } else if ("testNotification".equals(function)) {
                    testNotification(toHandle, responseFor(function,toAppend));
                } else {
                    logger.generalWarning("Unknown function '" + function + "'.");
                }
            } else {
                // access violation
                addErrorDetail(toAppend, "Access violation: This method is not available with your client key");
                logger.portalAccessViolation("Tried to call '"+function+"' with key "+ss.toString());
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
            // Log database use
            int queryCount                  = transconn.getQueryCount();
            logger.dbAction("Executed " + queryCount + " report quer" + (queryCount == 1 ? "y" : "ies"));

            transconn.close();
        }
        
        logger.xml("response: " + toAppend.asXML());
        
    }
    
    private Element responseFor(String s, Element e) {
        String responseNamespace = (String)SOAPMessage.getURIMap().get("tkmsg");
        return e.addElement("m:"+s+"Response",responseNamespace);
    }
    
    
    private String nullToEmpty(String s) {
        return (null == s) ? "" : s;
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
    
    private void addErrorDetail(Element toAppend, String message) {
        toAppend.addElement("error").addElement("detail").addText(message);
    }
    private int getCallerId(Element toHandle) throws HandlerException {
        return HandlerUtils.getRequiredInteger(HandlerUtils.getRequiredElement(toHandle,"caller"),"callerId");
    }
    
    private boolean checkForeignKey(String table, int value) throws SQLException, HandlerException {
        return checkForeignKey(table,"id",value);
    }
    
    private boolean checkForeignKey(String table, String field, int value) throws SQLException, HandlerException {
        
        PreparedStatement stmt = null;
        ResultSet rs = null;
        boolean result = false;
        
        String select = "SELECT " + field + " FROM " + table +
                " WHERE " + field + " = ?";
        
        stmt = transconn.prepareStatement(select);
        stmt.setInt(1, value);
        rs = stmt.executeQuery();
        result = rs.next();
        
        close(rs);
        close(stmt);
        return result;
    }
    
    
    private boolean checkForeignKey(String table, int value, RegisteredConnection transconn) throws SQLException {
        return checkForeignKey(table, "id", value, transconn);
    }
    
    
    private boolean checkForeignKey(String table, String field, int value, RegisteredConnection transconn) throws SQLException {

        PreparedStatement stmt = null;
        ResultSet rs = null;
        boolean result = false;

        String select = "SELECT " + field + " FROM " + table + " WHERE " + field + "=?";

        stmt = transconn.prepareStatement(select);
        stmt.setInt(1, value);
        rs = stmt.executeQuery();
        result = rs.next();

        close(rs);
        close(stmt);

        return result;
    }
    
    
    private void getBeerBoardStates(Element toHandle, Element toAppend) throws HandlerException {
        String selectState                  = "SELECT DISTINCT(l.addrState), s.STNAME, s.country FROM location l LEFT JOIN state s ON s.USPSST=l.addrState WHERE s.country !='Brazil';";
        PreparedStatement stmt              = null;
        ResultSet rs                        = null;
        try {
            stmt                        = transconn.prepareStatement(selectState);
            rs                          = stmt.executeQuery();
            while (rs != null && rs.next()) {
                int rsIndex             = 1;
                Element locEl           = toAppend.addElement("state");
                locEl.addElement("code").addText(HandlerUtils.nullToEmpty((rs.getString(rsIndex++))));
                locEl.addElement("name").addText(HandlerUtils.nullToEmpty((rs.getString(rsIndex++))));
                locEl.addElement("country").addText(HandlerUtils.nullToEmpty((rs.getString(rsIndex++))));
        }
     } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } catch (Exception ex) {
            System.out.println("ERROR: " + ex.getClass().getName() + " " + ex.getMessage());
            ex.printStackTrace();
        } finally {
            close(rs);
            close(stmt);
        }
     }

    private void addBevSyncCustomer(Element toHandle, Element toAppend) throws HandlerException {
        
        int callerId = getCallerId(toHandle);
        
        String custName                    = HandlerUtils.getOptionalString(toHandle, "customerName");
        int custId                          = HandlerUtils.getOptionalInteger(toHandle, "customerId");        
        String addrCity                     = HandlerUtils.getRequiredString(toHandle, "addrCity");
        String addrState                    = HandlerUtils.getRequiredString(toHandle, "addrState");
        String addrZip                      = HandlerUtils.getRequiredString(toHandle, "addrZip");
        String addrStreet                   = HandlerUtils.getRequiredString(toHandle, "addrStreet");
        String addrCountry                  = HandlerUtils.getRequiredString(toHandle, "addrCountry");
        String easternOffset                = HandlerUtils.getRequiredString(toHandle, "easternOffset");        
        double latitude                     = HandlerUtils.getRequiredDouble(toHandle, "latitude");
        double longitude                    = HandlerUtils.getRequiredDouble(toHandle, "longitude");        
        int custType                        = HandlerUtils.getOptionalInteger(toHandle, "custType");
        String brewery                      = HandlerUtils.getRequiredString(toHandle, "brewery");  
        String location                     = HandlerUtils.getRequiredString(toHandle, "location"); 
        boolean otherBrands                 = HandlerUtils.getOptionalBoolean(toHandle, "otherBrands");
        

        

        if (custId <= 0 && (custName == null || custName.equals(""))) {
            throw new HandlerException("illegal arguments to addCustomer");
        }

        String insertCust = "INSERT INTO bevSyncCustomer (name,type,addrStreet,addrCity,addrState,addrZip,addrCountry,easternOffset,latitude,longitude,brewery,otherBrands) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)";
        
        
        String getLastId = "SELECT LAST_INSERT_ID()";

        PreparedStatement stmt = null;
        ResultSet rs = null;

        logger.portalAction("addCustomer");

        try {

            // If no customer Id was provided, we need to make a new customer
            if (custId <= 0) {
                stmt = transconn.prepareStatement(insertCust);
                stmt.setString(1, custName);
                stmt.setInt(2, custType);
                stmt.setString(3, addrStreet);
                stmt.setString(4, addrCity);
                stmt.setString(5, addrState);
                stmt.setString(6, addrZip);
                stmt.setString(7, addrCountry);
                stmt.setString(8, easternOffset);                
                stmt.setDouble(9, latitude);
                stmt.setDouble(10, longitude);               
                stmt.setString(11, brewery);                
                stmt.setBoolean(12, otherBrands);
                stmt.executeUpdate();

                stmt = transconn.prepareStatement(getLastId);
                rs = stmt.executeQuery();
                logger.debug("Created a new customer");

                if (rs.next()) {
                    custId = rs.getInt(1);
                } else {
                    logger.dbError("first call to LAST_INSERT_ID in addCustomer failed to return a result");
                    throw new HandlerException("Database Error");
                }
                
                String locList[]        = location.split(",");
                for(int i=0;i<locList.length;i++){
                    stmt                = transconn.prepareStatement("INSERT INTO bevSyncCustomerLocationMap ( customer,  location) VALUES (?, ?)");
                    stmt.setInt(1, custId);
                    stmt.setInt(2, Integer.parseInt(locList[i]));
                    stmt.executeUpdate();
                }

                toAppend.addElement("customerId").addText(String.valueOf(custId));
                String logMessage = "Created customer '" + custName + "'";
                logger.portalDetail(callerId, "addBevSyncCustomer", 0, "customer", custId, logMessage, transconn);
            }
            
            
            


        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
            close(rs);
        }

    }
    
    
    private void updateBevSyncCustomer(Element toHandle, Element toAppend) throws HandlerException {

        String custName                     = HandlerUtils.getRequiredString(toHandle, "customerName");
        String brewery                      = HandlerUtils.getRequiredString(toHandle, "brewery");
        int custId                          = HandlerUtils.getRequiredInteger(toHandle, "customerId");
         String addrCity                     = HandlerUtils.getRequiredString(toHandle, "addrCity");
        String addrState                    = HandlerUtils.getRequiredString(toHandle, "addrState");
        String addrZip                      = HandlerUtils.getRequiredString(toHandle, "addrZip");
        String addrStreet                   = HandlerUtils.getRequiredString(toHandle, "addrStreet");
        String addrCountry                  = HandlerUtils.getRequiredString(toHandle, "addrCountry");
        String easternOffset                = HandlerUtils.getRequiredString(toHandle, "easternOffset");        
        double latitude                     = HandlerUtils.getRequiredDouble(toHandle, "latitude");
        double longitude                    = HandlerUtils.getRequiredDouble(toHandle, "longitude");        
        int custType                        = HandlerUtils.getOptionalInteger(toHandle, "custType");
        String location                     = HandlerUtils.getRequiredString(toHandle, "location"); 
        boolean otherBrands                 = HandlerUtils.getOptionalBoolean(toHandle, "showOtherBrands");
        int callerId = getCallerId(toHandle);

        String update = "UPDATE bevSyncCustomer SET name=?, brewery=?,addrCity =?,addrState=?,addrZip = ?,addrStreet =?,addrCountry=?,easternOffset=?,latitude=?, longitude=?,type=?,otherBrands=? WHERE id=?";

        PreparedStatement stmt              = null;
        ResultSet rs                        = null;

        try {
            stmt = transconn.prepareStatement(update);
            int colCount = 1;

            stmt.setString(colCount++, custName);
            stmt.setString(colCount++, brewery);
            stmt.setString(colCount++, addrCity);
            stmt.setString(colCount++, addrState);
            stmt.setString(colCount++, addrZip);
            stmt.setString(colCount++, addrStreet);
            stmt.setString(colCount++, addrCountry);
            stmt.setString(colCount++, easternOffset);
            stmt.setDouble(colCount++, latitude);
            stmt.setDouble(colCount++, longitude);
            stmt.setInt(colCount++, custType);            
            stmt.setBoolean(colCount++, otherBrands);
            stmt.setInt(colCount++, custId);
           

            stmt.executeUpdate();
            
            stmt                       = transconn.prepareStatement("SELECT  GROUP_CONCAT(location ORDER BY location SEPARATOR ', ') FROM bevSyncCustomerLocationMap  WHERE customer = ?");
            stmt.setInt(1,custId);
            String locations            = "";
            rs                          = stmt.executeQuery();
            if(rs.next()) {              
                locations       = rs.getString(1);  
                if(locations==null || locations.equals("") || !locations.equals(location) ){
                    
                    stmt                = transconn.prepareStatement("DELETE FROM bevSyncCustomerLocationMap WHERE  customer = ?");
                    stmt.setInt(1, custId);
                    stmt.executeUpdate();
                    
                    String locList[]        = location.split(",");
                    for(int i=0;i<locList.length;i++){
                        stmt                = transconn.prepareStatement("INSERT INTO bevSyncCustomerLocationMap ( customer,  location) VALUES (?, ?)");
                        stmt.setInt(1, custId);
                        stmt.setInt(2, Integer.parseInt(locList[i]));
                        stmt.executeUpdate();
                }
                }
            } 

            String logMessage = "Updated BevSync_Customer to '" + custName + "'";
            logger.portalDetail(callerId, "updateBevSyncCustomer", 0, "customer", custId, logMessage, transconn);

        } catch (SQLException sqle) {
            logger.dbError("Database error in updateCustomer: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
        }
    }
    
    
    private void getBevSyncCustomers(Element toHandle, Element toAppend) throws HandlerException {

        int customerId                      = HandlerUtils.getOptionalInteger(toHandle, "customerId");        
        String name                         = HandlerUtils.getOptionalString(toHandle, "customerName");        
        int type                            = HandlerUtils.getOptionalInteger(toHandle, "type");
        int userId                          = HandlerUtils.getRequiredInteger(toHandle, "userId");
        int level                           = HandlerUtils.getRequiredInteger(toHandle, "securityLevel");

        int paramsSet = 0;
        if (customerId >= 0) {
            paramsSet++;
        }
        if (null != name) {
            paramsSet++;
        }
        
        if (paramsSet > 1) {
            throw new HandlerException("Only one parameter can be set for getCustomers.");
        }

        String typeString = "";
        if (type > 0) {
            typeString                      = " AND type = ?";
        }
        PreparedStatement stmt              = null;
        ResultSet rs                        = null;

        try {
            if (customerId >= 0) {
                String selectById           = "SELECT id, name, type,addrStreet,addrCity,addrState,addrZip,addrCountry,easternOffset,latitude, longitude, brewery, otherBrands FROM bevSyncCustomer WHERE isActive = 1 AND id=?" + typeString;
                stmt                        = transconn.prepareStatement(selectById);
                stmt.setInt(1, customerId);
                if (type > 0) {
                    stmt.setInt(2, type);
                }
                rs                          = stmt.executeQuery();
                getBevSyncCustomerXML(toAppend, rs);
            }  else if (null != name) {
                String selectByName         = "SELECT id, name, type,addrStreet,addrCity,addrState,addrZip,addrCountry,easternOffset,latitude, longitude,brewery, otherBrands FROM bevSyncCustomer WHERE isActive = 1 AND name LIKE = ?" + typeString;
                stmt                        = transconn.prepareStatement(selectByName);
                String subName              = '%' + name + '%';
                stmt.setString(1, subName);
                if (type > 0) {
                    stmt.setInt(2, type);
                }
                rs                          = stmt.executeQuery();
                getBevSyncCustomerXML(toAppend, rs);
            } else if(userId >0 && level !=1 ){
                String selectAll            = "SELECT c.id, c.name, c.type,c.addrStreet,c.addrCity,c.addrState,c.addrZip,c.addrCountry,c.easternOffset,c.latitude, c.longitude,c.brewery, otherBrands FROM bevSyncCustomer c"
                                            + " LEFT JOIN bevSyncUser u ON u.customer =c.id  WHERE isActive = 1 AND u.id=? " + typeString;
                stmt                        = transconn.prepareStatement(selectAll);
                stmt.setInt(1, userId);
                if (type > 0) {
                    stmt.setInt(2, type);
                }
                rs                          = stmt.executeQuery();
                getBevSyncCustomerXML(toAppend, rs);
            } else {
                String selectAll            = "SELECT id, name, type,addrStreet,addrCity,addrState,addrZip,addrCountry,easternOffset,latitude, longitude,brewery,  otherBrands FROM bevSyncCustomer WHERE isActive = 1 " + typeString;
                stmt                        = transconn.prepareStatement(selectAll);
                if (type > 0) {
                    stmt.setInt(1, type);
                }
                rs                          = stmt.executeQuery();
                getBevSyncCustomerXML(toAppend, rs);
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(rs);
            close(stmt);
        }
    }
    
    
    private void getBevSyncCustomerXML(Element toAppend, ResultSet rs) throws SQLException {
        PreparedStatement stmt              = null;
        ResultSet rsDetails                 = null;

        try {
            while (rs.next()) {
                Element customerEl          = toAppend.addElement("customer");
                customerEl.addElement("customerId").addText(String.valueOf(rs.getInt(1)));
                customerEl.addElement("name").addText(HandlerUtils.nullToEmpty(rs.getString(2)));
                customerEl.addElement("cType").addText(String.valueOf(rs.getInt(3)));
                customerEl.addElement("addrStreet").addText(HandlerUtils.nullToEmpty(rs.getString(4)));
                customerEl.addElement("addrCity").addText(HandlerUtils.nullToEmpty(rs.getString(5)));
                customerEl.addElement("addrState").addText(HandlerUtils.nullToEmpty(rs.getString(6)));
                customerEl.addElement("addrZip").addText(HandlerUtils.nullToEmpty(rs.getString(7)));
                customerEl.addElement("addrCountry").addText(HandlerUtils.nullToEmpty(rs.getString(8)));
                customerEl.addElement("easternOffset").addText(HandlerUtils.nullToEmpty(rs.getString(9)));
                customerEl.addElement("latitude").addText(HandlerUtils.nullToEmpty(rs.getString(10)));
                customerEl.addElement("longitude").addText(HandlerUtils.nullToEmpty(rs.getString(11)));

                String breweryStr           = rs.getString(12);
                for (String brew : breweryStr.split(",")) {
                    stmt                    = transconn.prepareStatement("SELECT id FROM productSet WHERE id = ?;");
                    stmt.setInt(1, Integer.valueOf(brew));
                    rsDetails               = stmt.executeQuery();
                    if (!rsDetails.next()) {
                        if (breweryStr.contains(brew + ",")) {
                            breweryStr      = breweryStr.replace(brew + ",", "");
                        } else {
                            breweryStr      = breweryStr.replace("," + brew, "");
                        }
                    }
                }
                
                stmt                    = transconn.prepareStatement("UPDATE bevSyncCustomer SET brewery = ? WHERE id = ?;");
                stmt.setString(1, breweryStr);
                stmt.setInt(2, rs.getInt(1));
                stmt.executeUpdate();
                
                customerEl.addElement("brewery").addText(HandlerUtils.nullToEmpty(breweryStr));
                customerEl.addElement("showOtherBrands").addText(String.valueOf(rs.getBoolean(13)));

                 stmt                       = transconn.prepareStatement("SELECT GROUP_CONCAT(b.location ORDER BY b.location SEPARATOR ',') FROM bevSyncCustomerLocationMap b " +
                                            " LEFT JOIN locationDetails lD ON lD.location = b.location WHERE b.customer = ? AND lD.active = 1 AND lD.beerboard=1;");
                 stmt.setInt(1, rs.getInt(1));
                 rsDetails                  = stmt.executeQuery();
                 if(rsDetails.next()){
                     customerEl.addElement("location").addText(HandlerUtils.nullToEmpty(rsDetails.getString(1)));
                 } else {
                     customerEl.addElement("location").addText(HandlerUtils.nullToEmpty("0"));
                 }
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new SQLException(sqle);
        } finally {
            close(rsDetails);
            close(stmt);
        }
    }
    
    private void addBevSyncUser(Element toHandle, Element toAppend) throws HandlerException {
        final int READ_ONLY_ACCESS = 5;
        int customer                        = HandlerUtils.getOptionalInteger(toHandle, "customerId");        
        String fullName                     = HandlerUtils.getRequiredString(toHandle, "fullName");
        String username                     = HandlerUtils.getRequiredString(toHandle, "username");
        String password                     = HandlerUtils.getRequiredString(toHandle, "password");
        String email                        = HandlerUtils.getRequiredString(toHandle, "email");
        String mobile                       = HandlerUtils.getRequiredString(toHandle, "mobile");
        String carrier                      = HandlerUtils.getRequiredString(toHandle, "carrier");
        
        int callerId                        = getCallerId(toHandle);
        int userDbId                        = 0;

        String checkUser =
                " SELECT id FROM bevSyncUser WHERE username=? LIMIT 1";
        
        String insertUser =
                " INSERT INTO bevSyncUser (name,username,password,customer,email,mobile,carrier,securityLevel) " +
                " VALUES (?,?,?,?,?,?,?,?) ";
        String getLastId =
                " SELECT LAST_INSERT_ID()";
        

        PreparedStatement stmt = null;
        ResultSet rs = null;
        
        String logMessage;
        logger.portalAction("addUser");

        try {
            //Check that the username doesn't already exist
            stmt = transconn.prepareStatement(checkUser);
            stmt.setString(1, username);
            rs = stmt.executeQuery();
            if (rs.next()) {
                logger.debug("Username already exists");
                addErrorDetail(toAppend, "Username already exists");
            } else {
               
                       
                        // add the user record
                        stmt = transconn.prepareStatement(insertUser);
                        stmt.setString(1, fullName);
                        stmt.setString(2, username);
                        stmt.setString(3, password);
                        stmt.setInt(4, customer);                       
                        stmt.setString(5, email);
                        stmt.setString(6, mobile);
                        stmt.setString(7, carrier);
                        stmt.setInt(8, READ_ONLY_ACCESS);
                        stmt.executeUpdate();

                        // get the id of the user we added
                        stmt = transconn.prepareStatement(getLastId);
                        rs = stmt.executeQuery();
                        if (rs.next()) {
                            userDbId = rs.getInt(1);
                        } else {
                            logger.dbError("SQL Last_Insert_Id FAILED in addBevSyncUser ");
                            throw new HandlerException("database error");
                        }

                        logMessage = "Added bevManager user " + fullName + " as '" + username + "'";
                        logger.portalDetail(callerId, "addBevSyncUser", 0, "user", userDbId, logMessage, transconn);
                        
                        logger.debug("User added");

                        toAppend.addElement("userFullname").addText(HandlerUtils.nullToEmpty(fullName));
                        toAppend.addElement("userId").addText(String.valueOf(userDbId));
                   
                

            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
            close(rs);
        }
    }
    
    
    private void updateBevSyncUser(Element toHandle, Element toAppend) throws HandlerException {
        int callerId                        = getCallerId(toHandle);
        int customer                        = HandlerUtils.getOptionalInteger(toHandle, "customerId");
        int targetUser                      = HandlerUtils.getRequiredInteger(toHandle, "userId");        
        String fullnameToTry                = HandlerUtils.getOptionalString(toHandle, "fullname");
        String usernameToTry                = HandlerUtils.getOptionalString(toHandle, "username");
        String emailToTry                   = HandlerUtils.getOptionalString(toHandle, "email");
        String mobileToTry                  = HandlerUtils.getOptionalString(toHandle, "mobile");
        String carrierToTry                 = HandlerUtils.getOptionalString(toHandle, "carrier");
        String select                       = "SELECT customer,name,username,email,mobile,carrier FROM bevSyncUser WHERE id=?";
        String updateUser                   = "UPDATE bevSyncUser SET username=?,name=?,email=?,mobile=?,carrier=? WHERE id=?";
        String checkUsername                = "SELECT id FROM bevSyncUser WHERE username=? LIMIT 1";                                                                    
        int customerId                      = HandlerUtils.getRequiredInteger(toHandle, "customer");                        
        WebPermission access                = new WebPermission(HandlerUtils.getRequiredInteger(toHandle, "access"));

        PreparedStatement stmt              = null;
        ResultSet rs                        = null;

        try {
            stmt = transconn.prepareStatement(select);
            stmt.setInt(1, targetUser);
            rs = stmt.executeQuery();
            if (rs.next()) {
                int userCustomer            = rs.getInt(1);
                String fullname             = rs.getString(2);
                String username             = rs.getString(3);
                String email                = rs.getString(4);
                String mobile               = rs.getString(5);
                String carrier              = rs.getString(6);
                String fullnameToUse        = fullname;
                String usernameToUse        = username;
                String emailToUse           = email;
                String mobileToUse          = mobile;
                String carrierToUse         = carrierToTry;
                String logMessage;
                if (usernameToTry != null && usernameToTry.length() > 3 && !usernameToTry.equals(username)) {
                    stmt                    = transconn.prepareStatement(checkUsername);
                    stmt.setString(1, usernameToTry);
                    rs                      = stmt.executeQuery();
                    if (!rs.next()) {
                        usernameToUse       = usernameToTry;
                    } else {
                        addErrorDetail(toAppend, "The username '" + usernameToTry + "' already exists");
                    }
                }
                if (fullnameToTry != null && fullnameToTry.length() > 3) {
                    fullnameToUse           = fullnameToTry;
                }
                if (emailToTry != null && emailToTry.length() > 3) {
                    emailToUse              = emailToTry;
                }
                if (mobileToTry != null) {
                    mobileToUse             = mobileToTry;
                }

                logMessage = "Updating " + username + " (" + fullname + ") to " + usernameToUse + " (" + fullnameToUse + ")";
                //check permission to delete.
                if ((access.canSuperAdmin()) || ((userCustomer == customerId) && (access.canWrite()))) {
                    logger.portalDetail(callerId, "updateTestUser", 0, "user", targetUser, logMessage, transconn);
                    stmt                    = transconn.prepareStatement(updateUser);
                    stmt.setString(1, usernameToUse);
                    stmt.setString(2, fullnameToUse);
                    stmt.setString(3, emailToUse);
                    stmt.setString(4, mobileToUse);
                    stmt.setString(5, carrierToUse);
                    stmt.setInt(6, targetUser);
                    stmt.executeUpdate();


                } else {
                    //permission problem
                    logger.portalAccessViolation("Update user failed by U#" + callerId + " C#" + customerId + " uC#" + userCustomer + " P#" + access + ": " + logMessage);
                    addErrorDetail(toAppend, "Unable to update that user.");
                }
                       
            } else {
                addErrorDetail(toAppend, "Unable to find that user");
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error in updateTestUser " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(rs);
            close(stmt);
        }
    }
    
    
    private void changeBevSyncPassword(Element toHandle, Element toAppend) throws HandlerException {

        String oldPass                      = HandlerUtils.getRequiredString(toHandle, "oldPassword");
        String newPass                      = HandlerUtils.getRequiredString(toHandle, "newPassword");
        int userId                          = HandlerUtils.getRequiredInteger(toHandle, "userId");        

        String update = "UPDATE bevSyncUser SET password=? WHERE id=? AND password=?";
        String select = "SELECT id FROM bevSyncUser WHERE id=? AND password=? LIMIT 1";

        PreparedStatement stmt = null;
        ResultSet rs = null;

        try {

            stmt = transconn.prepareStatement(select);
            stmt.setInt(1, userId);
            stmt.setString(2, oldPass);
            rs = stmt.executeQuery();
            if (rs.next()) {

                stmt = transconn.prepareStatement(update);
                stmt.setString(1, newPass);
                stmt.setInt(2, userId);
                stmt.setString(3, oldPass);

                stmt.executeUpdate();
                logger.portalDetail(userId, "changePassword", 0, "user", userId, "Changed password", transconn);

            } else {
                logger.portalAction("changePassword failed for U#" + userId + ", old pw didn't match");
                addErrorDetail(toAppend, "Original password isn't correct.");
            }

        } catch (SQLException sqle) {
            logger.dbError("Database error in changePassword: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
        }

    }
    
    
    private void moveBevSyncUser(Element toHandle, Element toAppend) throws HandlerException {

        int targetUser                      = HandlerUtils.getRequiredInteger(toHandle, "userId");
        int customer                        = HandlerUtils.getRequiredInteger(toHandle, "customer");
        int callerId                        = getCallerId(toHandle);

        String updateUser                   = "UPDATE bevSyncUser SET customer=? WHERE id=?";
        PreparedStatement stmt = null;
        ResultSet rs = null;

        try {
            //make sure the user and the new customer exist
            assertForeignKey("bevSyncUser", targetUser, transconn);
            assertForeignKey("bevSyncCustomer", customer, transconn);

            logger.portalDetail(callerId, "moveUser", 0, "user", targetUser, "Moving user " + targetUser + " to Customer " + customer, transconn);

            //change the user's customer
            stmt = transconn.prepareStatement(updateUser);
            stmt.setInt(1, customer);
            stmt.setInt(2, targetUser);
            stmt.executeUpdate();
           
        } catch (SQLException sqle) {
            logger.dbError("Database error in moveUser " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(rs);
            close(stmt);
        }
    }
    
    
    private void assertForeignKey(String table, int value, RegisteredConnection transconn) throws SQLException, HandlerException {
        if (!checkForeignKey(table, value, transconn)) {
            throw new HandlerException("Unknown " + table + ": " + value);
        }
    }

    
    
    private void getBevSyncUsers(Element toHandle, Element toAppend) throws HandlerException {
        
        int customerId                      = HandlerUtils.getOptionalInteger(toHandle, "customerId");
        String getCustomer                  = "SELECT id FROM bevSyncCustomer ";       
        String listSelection                = "customer";
        PreparedStatement stmt              = null;
        ResultSet rs                        = null, rs1 = null;
        if(customerId >0) {
            getCustomer                     += " WHERE id = ?;";
        }

        try {
            
            
            stmt                            = transconn.prepareStatement(getCustomer);  
            if(customerId >0) {
                stmt.setInt(1, customerId);
            }
            rs                              = stmt.executeQuery();
            while (rs.next()) {
                customerId                  = rs.getInt(1);
                String getUsers = "SELECT id, name, email, securityLevel, emailReport  FROM bevSyncUser  WHERE customer = ? ORDER BY name ";
                stmt                        = transconn.prepareStatement(getUsers);
                stmt.setInt(1, customerId);                
                rs1                          = stmt.executeQuery();
                while (rs1.next()) {                    
                    Element userEl          = toAppend.addElement("user");
                    userEl.addElement("fullName").addText(HandlerUtils.nullToEmpty(rs1.getString(2)));
                    userEl.addElement("userId").addText(String.valueOf(rs1.getInt(1)));
                    userEl.addElement("email").addText(HandlerUtils.nullToEmpty(rs1.getString(3)));
                    userEl.addElement("permission").addText(String.valueOf(rs1.getInt(4)));
                    userEl.addElement("userAuth").addText(String.valueOf(rs1.getInt(5)));
                }
            }
        

        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(rs1);
            close(rs);
            close(stmt);
        }

    }
    
    
    private void getBevSyncUserDetail(Element toHandle, Element toAppend) throws HandlerException {
        int callerId                        = getCallerId(toHandle);
        int userId                          = HandlerUtils.getRequiredInteger(toHandle, "userId");
        int level                           = HandlerUtils.getRequiredInteger(toHandle, "securityLevel");

        String getUser                      = " SELECT u.name, u.username, u.email, u.customer, u.isManager, u.mobile, u.carrier, u.securityLevel, c.name" +
                                            " FROM bevSyncUser u LEFT JOIN bevSyncCustomer c ON u.customer = c.id  WHERE u.id = ?";
        String getAdminUser                 =" SELECT name, username, email, customer,  isManager, mobile, carrier, securityLevel,'' FROM user u"
                                            + " LEFT JOIN userMap m  ON m.user=u.id WHERE u.id =? ";
       

        PreparedStatement stmt              = null;
        ResultSet rs                        = null;

        try {
            if(level ==1) {
                getUser                     = getAdminUser;
            }
            int callerCust                  = -1;

            stmt                            = transconn.prepareStatement(getUser);
            stmt.setInt(1, callerId);
            rs = stmt.executeQuery();
            if (rs.next()) {
                callerCust                  = rs.getInt(4);
                
            }
            callerCust                      = 0;
            stmt                            = transconn.prepareStatement(getUser);
            stmt.setInt(1, userId);
            rs = stmt.executeQuery();
            if (rs.next()) {
                String name                 = rs.getString(1);
                String username             = rs.getString(2);
                String email                = rs.getString(3);
                int customer                = rs.getInt(4);               
                int isManager               = rs.getInt(5);
                String mobile               = rs.getString(6);
                String carrier              = rs.getString(7);
                int securityLevel           = rs.getInt(8);
                String customerName         = rs.getString(9);

                //check that the caller should be able to see this info
                if (callerCust == 0 || callerCust == customer  ) {
                    toAppend.addElement("fullName").addText(HandlerUtils.nullToEmpty(name));
                    toAppend.addElement("username").addText(HandlerUtils.nullToEmpty(username));
                    toAppend.addElement("email").addText(HandlerUtils.nullToEmpty(email));
                    toAppend.addElement("mobile").addText(HandlerUtils.nullToEmpty(mobile));
                    toAppend.addElement("carrier").addText(HandlerUtils.nullToEmpty(carrier));
                    toAppend.addElement("isManager").addText(String.valueOf(isManager));
                    
                    if (isManager == 0) {
                        
                            Element permEl = toAppend.addElement("customer");
                            permEl.addElement("customerName").addText(HandlerUtils.nullToEmpty(customerName));
                            permEl.addElement("customerId").addText(String.valueOf(customer));
                            permEl.addElement("permission").addText(String.valueOf(securityLevel));
                        
                    }
                } else {
                    addErrorDetail(toAppend, "You don't have permission to view this user");
                    logger.portalAccessViolation("Permission problem: Tried to getUserDetail on " + userId + " by " + callerId);
                }
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(rs);
            close(stmt);
        }

    }
    
    
    /**  Updates user permissions for users location-specific access (map-level).
     *  There are two ways to call this method.  The first is to change permissions
     * for a number of users at one location, and the second it to change a number
     * of locations for a specific user.
     *
     *  Fixed location:
     *  <locationId>000
     *  <user>
     *      <userId>111
     *      <permission>7
     *  </user>
     *  <user .. />
     *  <user .. />
     *
     *  Fixed user:
     *  <userId>999
     *  <location>
     *      <locationId>111
     *      <permission>7
     *  </location>
     *  <location .. />
     *  <location .. />
     *
     *  Both of these types may be used in the same XML message.
     *
     *  The legal values of 'permission' are:
     *    5  =  Manager (Write Access)
     *    7  =  Read Only
     *    >7 =  No Access
     *  The location must belong to the customer whom is associated with each user.
     * see also net.terakeet.usbn.WebPermission
     */
    private void updateBevSyncUserPermissions(Element toHandle, Element toAppend) throws HandlerException {

        int customer                        = HandlerUtils.getOptionalInteger(toHandle, "customerId");
        int user                            = HandlerUtils.getOptionalInteger(toHandle, "userId");
        int callerId                        = getCallerId(toHandle);
        String updateMap                    = "UPDATE bevSyncUser set securityLevel=? WHERE id=? ";

        if (customer < 0 && user < 0) {
            throw new HandlerException("Either customerId or userId must be set.");
        }
         PreparedStatement stmt              = null;
        ResultSet rs                        = null;

        try {
        
            Iterator users                  = toHandle.elementIterator("user");
            while (users.hasNext()) {
                Element userEl              = (Element) users.next();
                int userId                  = HandlerUtils.getRequiredInteger(userEl, "userId");
                WebPermission permission    = new WebPermission(HandlerUtils.getRequiredInteger(userEl, "permission"));
                stmt        = transconn.prepareStatement(updateMap);
                stmt.setInt(1, permission.getLevel());
                stmt.setInt(2, userId);
                stmt.executeUpdate();
                
            }
        
        
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(rs);
            close(stmt);
        }

    }
    
    private void deleteBevSyncUser(Element toHandle, Element toAppend) throws HandlerException {

        int userId                          = HandlerUtils.getRequiredInteger(toHandle, "userId");
        int customerId                      = HandlerUtils.getRequiredInteger(toHandle, "customerId");
        WebPermission access                = new WebPermission(HandlerUtils.getRequiredInteger(toHandle, "access"));
        int callerId                        = getCallerId(toHandle);

        String select                       = "SELECT customer,name,username FROM bevSyncUser WHERE id =? LIMIT 1";
        String deleteUser                   = "DELETE FROM bevSyncUser WHERE id=?";       

        PreparedStatement stmt              = null;
        ResultSet rs                        = null;
        try {
            stmt                            = transconn.prepareStatement(select);
            stmt.setInt(1, userId);
            rs                              = stmt.executeQuery();
            if (rs.next()) {
                int userCustomer            = rs.getInt(1);
                String fullname             = rs.getString(2);
                String username             = rs.getString(3);

                String logMessage = "Deleting user " + userId + " (" + fullname + ", '" + username + "')";
                //check permission to delete.
                if ((access.canSuperAdmin()) || ((userCustomer == customerId) && (access.canCustomerAdmin()))) {
                    logger.portalDetail(callerId, "deleteUser", 0, "user", userId, logMessage, transconn);

                    stmt                    = transconn.prepareStatement(deleteUser);
                    stmt.setInt(1, userId);
                    stmt.executeUpdate();


                } else {
                    logger.portalAccessViolation("Delete user failed by U#" + callerId + " C#" + customerId + " uC#" + userCustomer + " P#" + access + ": " + logMessage);
                    addErrorDetail(toAppend, "Unable to delete that user.");
                }
            } else {
                logger.debug("Unknown user: " + callerId);
                addErrorDetail(toAppend, "Unable to delete that user.");
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error in deleteUser: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(rs);
            close(stmt);
        }

    }
    
    
    private void addUpdateDeleteCreatives(Element toHandle, Element toAppend) throws HandlerException {

        int callerId                        = getCallerId(toHandle);
        int userId                          = HandlerUtils.getRequiredInteger(toHandle, "userId");
        int customerId                      = HandlerUtils.getRequiredInteger(toHandle, "customerId");
        ResultSet rs                        = null;
        PreparedStatement stmt              = null;

        try {
           

            Iterator addCustomerCreatives   = toHandle.elementIterator("addCreatives");
            while (addCustomerCreatives.hasNext()) {
                Element customerCreatives   = (Element) addCustomerCreatives.next();
                int type                    = HandlerUtils.getRequiredInteger(customerCreatives, "type");
                String title                = HandlerUtils.getRequiredString(customerCreatives, "title");
                String file                 = HandlerUtils.getRequiredString(customerCreatives, "file");                
                String validity             = HandlerUtils.getRequiredString(customerCreatives, "validity");
                int brewery                 = HandlerUtils.getRequiredInteger(customerCreatives, "brewery");
                int product                 = HandlerUtils.getRequiredInteger(customerCreatives, "product");

                String getLastId            = " SELECT LAST_INSERT_ID()";

                stmt                        = transconn.prepareStatement("INSERT INTO bevSyncCreatives (user, customer, type, title, file,validity,brewery,product) VALUES (?, ?, ?, ?,?,?,?,?)");
                stmt.setInt(1, userId);
                stmt.setInt(2, customerId);
                stmt.setInt(3, type);
                stmt.setString(4, title);
                stmt.setString(5, file);                
                stmt.setString(6, validity);
                stmt.setInt(7, brewery);
                stmt.setInt(8, product);
                stmt.executeUpdate();

                stmt                        = transconn.prepareStatement(getLastId);
                rs                          = stmt.executeQuery();
                if (rs.next()) {
                    int id                  = rs.getInt(1);
                    String logMessage       = "Added  Creatives '" + file + "'";
                    logger.portalDetail(callerId, "addBevSyncCreatives", 0, "addCreatives", id, logMessage, transconn);
                } else {
                    logger.dbError("first call to LAST_INSERT_ID in addBevSyncCreatives failed to return a result");
                    throw new HandlerException("Database Error");
                }
            }

            Iterator updateCustomerCreatives
                                            = toHandle.elementIterator("updateCreatives");
            while (updateCustomerCreatives.hasNext()) {
                Element customerCreatives    = (Element) updateCustomerCreatives.next();
                int id                      = HandlerUtils.getRequiredInteger(customerCreatives, "id");
                int type                    = HandlerUtils.getRequiredInteger(customerCreatives, "type");
                String title                = HandlerUtils.getRequiredString(customerCreatives, "title");
                String file                 = HandlerUtils.getRequiredString(customerCreatives, "file");                
                String validity             = HandlerUtils.getRequiredString(customerCreatives, "validity");
                int brewery                 = HandlerUtils.getRequiredInteger(customerCreatives, "brewery");
                int product                 = HandlerUtils.getRequiredInteger(customerCreatives, "product");

                String update               = " UPDATE bevSyncCreatives SET type = ?, title = ?, file = ?, brewery = ?, validity = ?,product=? WHERE id = ? ";

                stmt                        = transconn.prepareStatement(update);
                stmt.setInt(1, type);
                stmt.setString(2, title);
                stmt.setString(3, file);
                stmt.setInt(4, brewery);
                stmt.setString(5, validity);
                stmt.setInt(6, product);
                stmt.setInt(7, id);
                stmt.executeUpdate();
                String logMessage           = "Updated Creatives '" + id + "'";
                logger.portalDetail(callerId, "updateBevSyncCreatives", 0, "accountEmailMap", id, logMessage, transconn);
            }

            Iterator deleteCustomerCreatives
                                            = toHandle.elementIterator("deleteCreatives");
            while (deleteCustomerCreatives.hasNext()) {
                Element customerCreatives   = (Element) deleteCustomerCreatives.next();
                int id                      = HandlerUtils.getRequiredInteger(customerCreatives, "id");

                String delete               = " DELETE FROM bevSyncCreatives WHERE id = ? ";

                stmt                        = transconn.prepareStatement(delete);
                stmt.setInt(1,id);
                stmt.executeUpdate();

                String logMessage           = "Delete Creatives '" + id + "'";
                logger.portalDetail(callerId, "deleteBevSyncCreatives", 0, "accountEmailMap", id, logMessage, transconn);
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: "+sqle.toString());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
            close(rs);
        }
   }
    
    
    private void getCreatives(Element toHandle, Element toAppend) throws HandlerException {

        int customer                        = HandlerUtils.getRequiredInteger(toHandle, "customerId");
        PreparedStatement stmt              = null;
        ResultSet rs                        = null, rsDetail= null;
        String select                       = "SELECT p.id, p.name " +
                                            " FROM product p LEFT JOIN productDescription pD ON pD.product = p.id " +
                                            " LEFT JOIN productSetMap sPSM ON sPSM.product = pD.product LEFT JOIN productSet sPS ON sPS.id = sPSM.productSet " +
                                            " LEFT JOIN productSetMap pSM ON pSM.product = pD.product LEFT JOIN productSet pS ON pS.id = pSM.productSet " +
                                            " WHERE pS.productSetType = 7 AND sPS.productSetType = 9 AND p.isActive = 1 AND pSM.productSet = ?";
        String selectType                   = "SELECT id, name FROM  bevSyncCreativesType";
        
        

        try {
            String selectCustomer           = "SELECT brewery  FROM bevSyncCustomer WHERE id = ? ";           
            stmt                            = transconn.prepareStatement(selectType);           
            rs                              = stmt.executeQuery();
            while (rs.next()) { 
                 Element creativesEl = toAppend.addElement("type");
                 creativesEl.addElement("id").addText(String.valueOf(rs.getInt(1)));                        
                 creativesEl.addElement("name").addText(rs.getString(2));
            }
            stmt                            = transconn.prepareStatement(selectCustomer);
            stmt.setInt(1,customer);
            rs                              = stmt.executeQuery();
            if (rs.next()) {                        
                String brewery[]            = rs.getString(1).split(",");
                if(brewery.length > 0) {
                    for(int i=0;i<brewery.length;i++){
                        stmt                = transconn.prepareStatement(select);
                        stmt.setInt(1, Integer.parseInt(brewery[i]));
                        rsDetail            = stmt.executeQuery();
                        while(rsDetail.next()) {
                            Element productsEl
                                            = toAppend.addElement("products");
                            productsEl.addElement("breweryId").addText(brewery[i]);        
                            productsEl.addElement("productId").addText(String.valueOf(rsDetail.getInt(1)));                                                
                            productsEl.addElement("productName").addText(HandlerUtils.nullToEmpty(rsDetail.getString(2)));                                    
                        }                              
                    }
                }                       
            }
           
            String selectCreatives          = "SELECT id, type, title, file, brewery,product, validity FROM bevSyncCreatives WHERE customer = ? AND type <5 ORDER BY validity";
                    stmt                    = transconn.prepareStatement(selectCreatives);
                    stmt.setInt(1,customer);
                    rs                      = stmt.executeQuery();
                    while (rs.next()) {                        
                        Element creativesEl = toAppend.addElement("creatives");
                        creativesEl.addElement("id").addText(String.valueOf(rs.getInt(1)));                        
                        creativesEl.addElement("type").addText(String.valueOf(rs.getInt(2)));
                        creativesEl.addElement("title").addText(HandlerUtils.nullToEmpty(rs.getString(3)));
                        creativesEl.addElement("file").addText(HandlerUtils.nullToEmpty(rs.getString(4)));                       
                        creativesEl.addElement("brewery").addText(String.valueOf(rs.getInt(5)));
                        creativesEl.addElement("product").addText(String.valueOf(rs.getInt(6)));
                        creativesEl.addElement("validity").addText(HandlerUtils.nullToEmpty(rs.getString(7)));
                        creativesEl.addElement("filePath").addText(HandlerUtils.nullToEmpty("http://beerboard.tv/USBN.BeerBoard.UI/bevSyncCustomers/" + String.valueOf(customer) + "/" + rs.getString(4).replaceAll(" ", "%20")));
                    }
        } catch (SQLException sqle) {
            logger.dbError("Database error: "+sqle.toString());
            throw new HandlerException(sqle);
        }  finally {
            close(rsDetail);
            close(rs);
            close(stmt);
        }
    }

    
    private void addUpdateDeleteCampaign(Element toHandle, Element toAppend) throws HandlerException {

        int callerId                        = getCallerId(toHandle);
        int customerId                      = HandlerUtils.getRequiredInteger(toHandle, "customerId");
        ResultSet rs                        = null, rsLast=null, rsDetails = null, rsFB = null;
        PreparedStatement stmt              = null;
        String getLastId                    = "SELECT LAST_INSERT_ID()";

        try {
            Iterator addCampaign            = toHandle.elementIterator("addCampaign");
            while (addCampaign.hasNext()) {
                Element CampaignEl          = (Element) addCampaign.next();
                String location             = HandlerUtils.getRequiredString(CampaignEl, "location");                
                String creative             = HandlerUtils.getOptionalString(CampaignEl, "creatives");
                String start                = HandlerUtils.getRequiredString(CampaignEl, "start");
                String end                  = HandlerUtils.getRequiredString(CampaignEl, "end");
                String title                = HandlerUtils.getRequiredString(CampaignEl, "title");
                int customer                = HandlerUtils.getRequiredInteger(CampaignEl, "customer");
                //int type                    = HandlerUtils.getRequiredInteger(CampaignEl, "type");   
                
                stmt                        = transconn.prepareStatement("INSERT INTO bevSyncCampaign ( customer,  title,start,end,isActive) VALUES (?, ?, ?,?,?)");
                stmt.setInt(1, customer);   
                stmt.setString(2, title);                 
                stmt.setString(3, dbDateFormat.format(dateFormat.parse(start)));
                stmt.setString(4, dbDateFormat.format(dateFormat.parse(end)));
                stmt.setInt(5, 1);                   
                stmt.executeUpdate();

                stmt                        = transconn.prepareStatement(getLastId);
                rs                          = stmt.executeQuery();
                if (rs.next()) {
                    int campaign                  = rs.getInt(1);
                    String logMessage       = "Added  Campaign '" + title + "'";
                    logger.portalDetail(callerId, "addBevSyncCreatives", 0, "accountEmailMap", campaign, logMessage, transconn);
                    String locList[]        = location.split(",");
                    for(int i=0;i<locList.length;i++){
                        stmt                = transconn.prepareStatement("INSERT INTO bevSyncCampaignLocations ( campaign,  location) VALUES (?, ?)");
                        stmt.setInt(1, campaign);
                        stmt.setInt(2, Integer.parseInt(locList[i]));
                        stmt.executeUpdate();
                    }
                    if(creative!=null && !creative.equals("") ) {
                    String creList[]        = creative.split(",");
                    for(int i=0;i<creList.length;i++){
                        stmt                = transconn.prepareStatement("INSERT INTO bevSyncCampaignCreatives ( campaign,  creatives) VALUES (?, ?)");
                        stmt.setInt(1, campaign);
                        stmt.setInt(2, Integer.parseInt(creList[i]));
                        stmt.executeUpdate();
                    }
                    }
                } else {
                    logger.dbError("first call to LAST_INSERT_ID in addBevSyncCreatives failed to return a result");
                    throw new HandlerException("Database Error");
                }
            }

            Iterator updateCampaignEl
                                            = toHandle.elementIterator("updateCampaign");
            while (updateCampaignEl.hasNext()) {
                //logger.debug("Calling updateCampaignCreatives");
                Element CampaignEl          = (Element) updateCampaignEl.next();
                int id                      = HandlerUtils.getRequiredInteger(CampaignEl, "id");
                String location             = HandlerUtils.getRequiredString(CampaignEl, "location");                
                String creative             = HandlerUtils.getOptionalString(CampaignEl, "creatives");
                String start                = HandlerUtils.getRequiredString(CampaignEl, "start");
                String end                  = HandlerUtils.getRequiredString(CampaignEl, "end");
                String title                = HandlerUtils.getRequiredString(CampaignEl, "title");
                //int type                    = HandlerUtils.getRequiredInteger(CampaignEl, "type"); 
                
                String updateCampaign       = " UPDATE bevSyncCampaign SET title=?,start=?,end=? WHERE id = ? ";

                stmt                        = transconn.prepareStatement(updateCampaign);
                stmt.setString(1, title);                
                stmt.setString(2, dbDateFormat.format(dateFormat.parse(start)));
                stmt.setString(3, dbDateFormat.format(dateFormat.parse(end)));               
                stmt.setInt(4, id);   
                stmt.executeUpdate();
                
                stmt                        = transconn.prepareStatement("DELETE FROM bevSyncCampaignLocations WHERE campaign=?;");
                stmt.setInt(1,id);
                stmt.executeUpdate();
                
                stmt                        = transconn.prepareStatement("DELETE FROM bevSyncCampaignCreatives WHERE campaign=?;");
                stmt.setInt(1,id);
                stmt.executeUpdate();
                
                String locList[]            = location.split(",");
                for(int i=0;i<locList.length;i++){
                    stmt                    = transconn.prepareStatement("INSERT INTO bevSyncCampaignLocations ( campaign,  location) VALUES (?, ?)");
                    stmt.setInt(1, id);
                    stmt.setInt(2, Integer.parseInt(locList[i]));
                    stmt.executeUpdate();
                }
                if(creative!=null && !creative.equals("") ) {
                String creList[]            = creative.split(",");
                for(int i=0; i<creList.length; i++){
                    stmt                    = transconn.prepareStatement("INSERT INTO bevSyncCampaignCreatives ( campaign,  creatives) VALUES (?, ?)");
                    stmt.setInt(1, id);
                    stmt.setInt(2, Integer.parseInt(creList[i]));
                    stmt.executeUpdate();
                    
                    int creativeId          = -1;
                    stmt                    = transconn.prepareStatement(getLastId);
                    rs                      = stmt.executeQuery();
                    if (rs.next()) {
                        creativeId          = rs.getInt(1);
                    }

                    String selectToken      = "SELECT DISTINCT p.pageid, u.user_id, u.access_token FROM usbnFacebook u LEFT JOIN usbnFacebookPage p ON u.user_id = p.fbid " +
                                            " WHERE p.location = ? AND u.user_id != '100000256298215';";

                    for(int j=0; j<locList.length; j++){
                        int locationId      = Integer.parseInt(locList[j]);
                        stmt                = transconn.prepareStatement(selectToken);
                        stmt.setInt(1, locationId);
                        rsFB                = stmt.executeQuery();
                        if (rsFB.next()) {
                            String pageId   = rsFB.getString(1);
                            String fbId     = rsFB.getString(2);
                            String locationToken
                                            = rsFB.getString(3);
                            //logger.debug(fbId + ", " + pageId + ", " + locationToken + ", " + locationId + ", " + creativeId);
                            SQLBeerBoardMobileHandler socialMedia
                                            = new SQLBeerBoardMobileHandler();
                            stmt            = transconn.prepareStatement("SELECT bC.customer, bC.file from bevSyncCreatives bC LEFT JOIN bevSyncCampaignCreatives bCC ON bCC.creatives = bC.id WHERE bCC.id = ?");
                            stmt.setInt(1, creativeId);
                            rsDetails       = stmt.executeQuery();
                            if(rsDetails.next()) {
                                String fileName
                                            = "http://beerboard.tv/USBN.BeerBoard.UI/bevSyncCustomers/" + String.valueOf(rsDetails.getInt(1)) + "/" + rsDetails.getString(2).replaceAll(" ", "%20");
                                socialMedia.saveImage(fileName, "/home/midware/facebook/reward/"+rsDetails.getString(2));
                                File file   = new File("/home/midware/facebook/reward/"+rsDetails.getString(2));
                                

                                logger.debug(rsDetails.getString(2));
                                //socialMedia.postImage(fbId, pageId, locationToken, "Come join us for 'The Just Beer Project' at Kitty Hoyne's Irish Pub on 10/28", locationId, creativeId, file, 4);

                                //logger.debug(file.getPath());
                               // socialMedia.postImage(fbId, pageId, locationToken, "", locationId, creativeId, file, 4);

                            }
                        }
                    }
                }
                String logMessage           = "Updated Campaign '" + id + "'";
                logger.portalDetail(callerId, "updateBevSyncCampaign", 0, "Update", id, logMessage, transconn);
            }
            }

            Iterator deleteCampaign
                                            = toHandle.elementIterator("deleteCampaign");
            while (deleteCampaign.hasNext()) {
                Element Campaign    = (Element) deleteCampaign.next();
                int id                      = HandlerUtils.getRequiredInteger(Campaign, "id");
                
                stmt                        = transconn.prepareStatement("DELETE FROM bevSyncCampaignLocations WHERE campaign=?;");
                stmt.setInt(1,id);
                stmt.executeUpdate();
                
                stmt                        = transconn.prepareStatement("DELETE FROM bevSyncCampaignCreatives WHERE campaign=?;");
                stmt.setInt(1,id);
                stmt.executeUpdate();

                String delete               = " DELETE FROM bevSyncCampaign WHERE id = ? ";

                stmt                        = transconn.prepareStatement(delete);
                stmt.setInt(1,id);
                stmt.executeUpdate();
               

                String logMessage           = "Delete Campaign '" + id + "'";
                logger.portalDetail(callerId, "deleteBevSyncCampaign", 0, "delete", id, logMessage, transconn);
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: "+sqle.toString());
            throw new HandlerException(sqle);
        } catch(Exception e) {
            e.printStackTrace();
        } finally {
            close(stmt);
            close(rs);
        }
   }
    
    
    private void getCampaign(Element toHandle, Element toAppend) throws HandlerException {

        int customer                        = HandlerUtils.getRequiredInteger(toHandle, "customer");
        PreparedStatement stmt              = null;
        ResultSet rs                        = null,rsDetails = null;
        

        try {
            String campaigns                = "0";
            String selectCampaign           = "SELECT id,title,start,end FROM bevSyncCampaign bSC WHERE customer = ? ORDER BY end";                        
            stmt                            = transconn.prepareStatement(selectCampaign);
            stmt.setInt(1,customer);                           
            rs                              = stmt.executeQuery();
            while (rs.next()) {                           
                Element campaignEl          = toAppend.addElement("campaign");
                int campaign                = rs.getInt(1);
                campaigns                   +=","+String.valueOf(campaign);
                campaignEl.addElement("id").addText(String.valueOf(campaign));                                                
                campaignEl.addElement("title").addText(HandlerUtils.nullToEmpty(rs.getString(2)));                        
                campaignEl.addElement("start").addText(HandlerUtils.nullToEmpty(rs.getString(3)));
                campaignEl.addElement("end").addText(HandlerUtils.nullToEmpty(rs.getString(4)));                        
               // int type                    = rs.getInt(5);
                //campaignEl.addElement("type").addText(String.valueOf(type));
                campaignEl.addElement("location").addText(HandlerUtils.nullToEmpty(getDetails(1,campaign)));
                campaignEl.addElement("creatives").addText(HandlerUtils.nullToEmpty(getDetails(2,campaign)));                         
               
            }
            String selectReward             = "SELECT bR.id,bC.brewery,bC.product,rewardText, location, startTime, endTime,campaign FROM  bevSyncCampaignReward bR"
                                            + " LEFT JOIN bevSyncCreatives bC ON bC.id=bR.creatives WHERE campaign IN ("+campaigns+") ORDER BY bR.id DESC; ";
            stmt                    = transconn.prepareStatement(selectReward);            
            rsDetails               = stmt.executeQuery();
            while (rsDetails.next()) {
                Element rewardEl    = toAppend.addElement("reward");
                rewardEl.addElement("id").addText(String.valueOf(rsDetails.getInt(1)));
                rewardEl.addElement("campaign").addText(String.valueOf(rsDetails.getInt(8))); 
                rewardEl.addElement("brewery").addText(String.valueOf(rsDetails.getInt(2)));
                rewardEl.addElement("product").addText(String.valueOf(rsDetails.getInt(3)));
                rewardEl.addElement("rewardText").addText(HandlerUtils.nullToEmpty(rsDetails.getString(4)));
                rewardEl.addElement("location").addText(String.valueOf(rsDetails.getInt(5)));
                rewardEl.addElement("startTime").addText(HandlerUtils.nullToEmpty(rsDetails.getString(6)));
                rewardEl.addElement("endTime").addText(HandlerUtils.nullToEmpty(rsDetails.getString(7)));
            }
            
            
             String selectBanner            = "SELECT bR.id,bC.brewery,bC.product,bannerText, location, startTime, endTime,campaign, bR.type, bR.link FROM  bevSyncCampaignBanner bR"
                                            + " LEFT JOIN bevSyncCreatives bC ON bC.id=bR.creatives WHERE campaign IN ("+campaigns+") ORDER BY bR.id DESC; ";
            stmt                    = transconn.prepareStatement(selectBanner);            
            rsDetails               = stmt.executeQuery();
            while (rsDetails.next()) {
                Element rewardEl    = toAppend.addElement("banner");
                rewardEl.addElement("id").addText(String.valueOf(rsDetails.getInt(1)));
                rewardEl.addElement("campaign").addText(String.valueOf(rsDetails.getInt(8))); 
                rewardEl.addElement("brewery").addText(String.valueOf(rsDetails.getInt(2)));
                rewardEl.addElement("product").addText(String.valueOf(rsDetails.getInt(3)));
                rewardEl.addElement("bannerText").addText(HandlerUtils.nullToEmpty(rsDetails.getString(4)));
                rewardEl.addElement("location").addText(String.valueOf(rsDetails.getInt(5)));
                rewardEl.addElement("startTime").addText(HandlerUtils.nullToEmpty(rsDetails.getString(6)));
                rewardEl.addElement("endTime").addText(HandlerUtils.nullToEmpty(rsDetails.getString(7)));
                rewardEl.addElement("type").addText(String.valueOf(rsDetails.getInt(9)));
                rewardEl.addElement("link").addText(HandlerUtils.nullToEmpty(rsDetails.getString(10)));
            }
             
        } catch (SQLException sqle) {
            logger.dbError("Database error: "+sqle.toString());
            throw new HandlerException(sqle);
        }  finally {
            close(rs);
            close(stmt);
        }
    }
    
    private String getDetails(int type, int campaign) {
        
    String select                           = "";
    if(type == 1) {
        select                          ="SELECT bL.location,CONCAT(l.boardName,'-', l.name) FROM bevSyncCampaignLocations bL LEFT JOIN location l ON l.id = bL.location WHERE campaign = ?";
    } else if(type == 2) {
        select                          ="SELECT c.creatives,bC.title FROM bevSyncCampaignCreatives c LEFT JOIN bevSyncCreatives bC ON bC.id=c.creatives WHERE campaign = ?";
    }
        
       String result                        = "";
       
        PreparedStatement stmt              = null;
        ResultSet rs                        = null;
        try {
            if(type == 1 || type == 2) {
                stmt                = transconn.prepareStatement(select);
                stmt.setInt(1, campaign);
                rs            = stmt.executeQuery();
                while(rs.next()) { 
                    result          +=rs.getString(1)+"_"+rs.getString(2);                       
                    result          +=",";
                } 
            } 
            if(result.length()>0){
                  result                    =result.substring(0,result.length()-1);
              }
                
            
            
         } catch (SQLException sqle) {
            logger.dbError("Database error: "+sqle.toString());            
        }  finally {            
            close(rs);
            close(stmt);
        }
        return result;
                
        
    }
    
    
    private void getCampaignSupports(Element toHandle, Element toAppend) throws HandlerException {

        int customer                        = HandlerUtils.getRequiredInteger(toHandle, "customerId");
        int type                            = HandlerUtils.getOptionalInteger(toHandle, "type");
        boolean products                    = HandlerUtils.getOptionalBoolean(toHandle, "products");
        PreparedStatement stmt              = null;
        ResultSet rs                        = null, rsDetail = null,rsProduct = null;
        String selectType                   = "SELECT id, name FROM  bevSyncCreativesType";
        
        

        try {
            
            String selectCustomer           = "SELECT brewery  FROM bevSyncCustomer WHERE id = ? ";
            String select                   = "SELECT  p.name FROM productSet p WHERE p.id = ?  ";
            if(type == 0 || type ==1) {
            stmt                            = transconn.prepareStatement(selectCustomer);
            stmt.setInt(1,customer);
            rs                              = stmt.executeQuery();
            if (rs.next()) {                        
                String brewery[]            = rs.getString(1).split(",");
                if(brewery.length > 0) {
                    for(int i=0;i<brewery.length;i++){
                        stmt                = transconn.prepareStatement(select);
                        stmt.setInt(1, Integer.parseInt(brewery[i]));
                        rsDetail            = stmt.executeQuery();
                        if(rsDetail.next()) {
                            Element breweryEl
                                            = toAppend.addElement("brewery");
                            breweryEl.addElement("id").addText(brewery[i]);                                                
                            breweryEl.addElement("name").addText(HandlerUtils.nullToEmpty(rsDetail.getString(1)));  
                            if(products) {
                                
                                stmt                = transconn.prepareStatement("SELECT p.id ,name FROM product p LEFT JOIN productSetMap pS ON pS.product=p.id WHERE pS.productSet=?;");
                                stmt.setInt(1, Integer.parseInt(brewery[i]));
                                rsProduct            = stmt.executeQuery();
                                while(rsProduct.next()) {
                                    Element productEl
                                            = toAppend.addElement("product");
                                    productEl.addElement("brewery").addText(brewery[i]);       
                                    productEl.addElement("id").addText(String.valueOf(rsProduct.getInt(1)));
                                    productEl.addElement("name").addText(HandlerUtils.nullToEmpty(rsProduct.getString(2)));  
                                    
                                }
                            }
                        }                              
                    }
                }                       
            }
            }
            if(type == 0 || type ==2) {
            String selectCreatives          = "SELECT id,title,type FROM bevSyncCreatives WHERE customer = ? and type <5;";
            stmt        = transconn.prepareStatement(selectCreatives);
            stmt.setInt(1,customer);
            rs                              = stmt.executeQuery();
            while (rs.next()) { 
                Element creativeEl          = toAppend.addElement("creatives");
                creativeEl.addElement("id").addText(String.valueOf(rs.getInt(1)));                        
                creativeEl.addElement("name").addText(HandlerUtils.nullToEmpty(rs.getString(2)));  
                creativeEl.addElement("type").addText(String.valueOf(rs.getInt(3)));                        
                                
            } 
            stmt                            = transconn.prepareStatement(selectType);           
            rs                              = stmt.executeQuery();
            while (rs.next()) { 
                 Element creativesEl = toAppend.addElement("type");
                 creativesEl.addElement("id").addText(String.valueOf(rs.getInt(1)));                        
                 creativesEl.addElement("name").addText(rs.getString(2));
            }
            
            
            }
            
             
        } catch (SQLException sqle) {
            logger.dbError("Database error: "+sqle.toString());
            throw new HandlerException(sqle);
        }  finally {
            close(rsDetail);
            close(rs);
            close(stmt);
        }
    }
    
    
    private void getBevSyncBeerboardList(Element toHandle, Element toAppend) throws HandlerException {

        
        int bevSyncCustomer                 = HandlerUtils.getOptionalInteger(toHandle, "bevSyncCustomer");
        int forAll                          = HandlerUtils.getOptionalInteger(toHandle, "forAll");
        String creatives                    = HandlerUtils.getOptionalString(toHandle, "creative");
        
        
        PreparedStatement stmt              = null;
        ResultSet rs                        = null;
        String brewStmt                     = "";
        String beerboardCondition           = "";
       
        

        try { 
            if(forAll==1) {
                
            } else {
                beerboardCondition          = " AND lD.beerboard = 1 " ;
            }
            String condition                ="  ";
            if(bevSyncCustomer>0){
                String selectCreatives      = "SELECT GROUP_CONCAT(b.location ORDER BY b.location SEPARATOR ',') FROM bevSyncCustomerLocationMap b LEFT JOIN locationDetails lD ON lD.location=b.location WHERE b.customer = ? AND lD.active = 1  "+beerboardCondition;
                stmt                        = transconn.prepareStatement(selectCreatives);
                stmt.setInt(1,bevSyncCustomer);
                rs                          = stmt.executeQuery();
                if(rs.next()) {
                    if(rs.getString(1)!=null &&!rs.getString(1).equals("")) {
                        condition               ="lo.id IN ("+rs.getString(1)+") AND ";
                    } else {
                        condition               ="lo.id IN (0) AND ";
                    }
                }
            }
            String selectLocations          = "SELECT DISTINCT lo.id, CONCAT(lo.boardName,'-', lo.name),s.STNAME, lD.beerboard FROM location lo   LEFT JOIN locationDetails lD ON lD.location = lo.id" +
                                            " LEFT Join state s on s.USPSST = lo.addrState  WHERE "+condition+" lD.active = 1 "+beerboardCondition+" AND s.country=lo.addrCountry ORDER BY lo.boardName, lo.name";
            stmt                            = transconn.prepareStatement(selectLocations);                                
            rs                              = stmt.executeQuery();
            while (rs.next()) {                        
                Element locationEl          = toAppend.addElement("location");
                locationEl.addElement("id").addText(String.valueOf(rs.getInt(1)));                                                
                locationEl.addElement("name").addText(HandlerUtils.nullToEmpty(rs.getString(2)));  
                locationEl.addElement("state").addText(HandlerUtils.nullToEmpty(rs.getString(3)));                      
                locationEl.addElement("beerboard").addText(String.valueOf(rs.getInt(4)));                                                
            }            
        
        } catch (SQLException sqle) {
            logger.dbError("Database error: "+sqle.toString());
            throw new HandlerException(sqle);
        }  finally {
            close(rs);
            close(stmt);
        }
    }
    
    
    private void addUpdateDeleteReward(Element toHandle, Element toAppend) throws HandlerException {

        int callerId                        = getCallerId(toHandle);  
        int userId                          = HandlerUtils.getRequiredInteger(toHandle, "userId");
        int customerId                      = HandlerUtils.getRequiredInteger(toHandle, "customerId");
        ResultSet rs                        = null, rsDetails = null,rsFB = null;
        PreparedStatement stmt              = null;
        Calendar currentDate                = null;

        try {
           

            Iterator addCustomerRewards   = toHandle.elementIterator("addReward");
            while (addCustomerRewards.hasNext()) {
                Element customerRewards     = (Element) addCustomerRewards.next();
                int campaign                = HandlerUtils.getRequiredInteger(customerRewards, "campaign");
                //int creatives               = HandlerUtils.getRequiredInteger(customerRewards, "creatives");
                String rewardText           = HandlerUtils.getRequiredString(customerRewards, "rewardText");
                int breweryId               = HandlerUtils.getRequiredInteger(customerRewards, "brewery");
                int productId               = HandlerUtils.getRequiredInteger(customerRewards, "product");
                int location                = HandlerUtils.getRequiredInteger(customerRewards, "location");
                String start                = HandlerUtils.getRequiredString(customerRewards, "startTime");
                String end                  = HandlerUtils.getRequiredString(customerRewards, "endTime");
             
               
                String checkReward          = "SELECT id FROM bevSyncCampaignReward WHERE location = ? AND campaign = ? AND rewardText = ? AND ? BETWEEN startTime AND endTime AND  ? BETWEEN  startTime AND endTime";
                String selectProduct        = "SELECT pS.name, IFNULL(pD.boardname, 'Unknown Product'),(SELECT CONCAT(boardname,', ',addrCity) from location where id=?) from bevSyncCreatives bC "
                                            + " LEFT JOIN product p ON p.id = bC.product LEFT JOIN productDescription pD ON pD.product = p.id LEFT JOIN productSet pS ON pS.id=bC.brewery WHERE pS.productSetType=7 AND bC.id=?;";
                String getLastId            = " SELECT LAST_INSERT_ID()";
                String selectToken          = "select Distinct p.pageid, u.user_id, u.access_token from usbnFacebook u LEFT JOIN usbnFacebookPage p ON u.user_id =p.fbid where p.location=?  and u.user_id!='5515563';";
                int rewardId                = 0;
                String brewery              = null,locationName = null,product= null, message = "";
                stmt                        = transconn.prepareStatement(checkReward);
                stmt.setInt(1, location);
                stmt.setInt(2, campaign);
                stmt.setString(3, rewardText);
                stmt.setString(4, dbDateFormat.format(dbDateFormat.parse(start)));
                stmt.setString(5, dbDateFormat.format(dbDateFormat.parse(end)));
               
                
                rs                          = stmt.executeQuery();
                if (!rs.next()) {
                    stmt                        = transconn.prepareStatement("INSERT INTO bevSyncCampaignReward (campaign, rewardText, location,startTime,endTime) VALUES (?, ?, ?, ?, ?)");
                    stmt.setInt(1, campaign);
                    stmt.setString(2, rewardText);                    
                    stmt.setInt(3, location);
                    stmt.setString(4, dbDateFormat.format(dbDateFormat.parse(start)));
                    stmt.setString(5, dbDateFormat.format(dbDateFormat.parse(end)));
                    stmt.executeUpdate();
                    
                    
                    
                    stmt                        = transconn.prepareStatement(getLastId);
                    rs                          = stmt.executeQuery();
                    if(rs.next()) {
                        rewardId                = rs.getInt(1);
                    }
                    
                    generateInsiderPass(rewardId,rewardText,breweryId,"reward"+"-"+rewardId+"-"+campaign+"-"+location);
                    Element rewardEl          = toAppend.addElement("rewardPass");
                    rewardEl.addElement("id").addText(String.valueOf(rewardId));                                                                    
                    rewardEl.addElement("path").addText("http://social.usbeveragenet.com:8080/fileUploader/Images/"+"reward"+"-"+rewardId+"-"+campaign+"-"+location+".jpg");
                    stmt                        = transconn.prepareStatement("SELECT CONCAT ('Reward','-',title, '-',(select name from location WHERE id=?)) from bevSyncCampaign WHERE id = ?;");
                    stmt.setInt(1,location);
                    stmt.setInt(2,campaign);
                    rs                          = stmt.executeQuery();
                    String title                = "Reward";
                    if(rs.next()) {
                        title                   = rs.getString(1);
                    }
                    int creatives = addCreative(userId, 5,customerId, title, "reward"+"-"+rewardId+"-"+campaign+"-"+location+".jpg", end, breweryId, productId);
                    stmt                        = transconn.prepareStatement("UPDATE bevSyncCampaignReward SET creatives =? WHERE id =?");
                    stmt.setInt(1,creatives);
                    stmt.setInt(2,rewardId);
                    stmt.executeUpdate();
                    
                    logger.debug("Reward Id: " + rewardId);
                    
                    stmt                        = transconn.prepareStatement("UPDATE bbtvMobileUser SET arrival = NOW() - INTERVAL 3 HOUR WHERE arrival > NOW() - INTERVAL 2 HOUR");
                    stmt.executeUpdate();
                    
                    stmt                        = transconn.prepareStatement(selectProduct);
                    stmt.setInt(1,location);                    
                    stmt.setInt(2,creatives);
                    rsDetails                   = stmt.executeQuery();
                    if(rsDetails.next()) {
                        brewery                 = rsDetails.getString(1);                        
                        product                 = rsDetails.getString(2);                        
                        locationName            = rsDetails.getString(3);
                        
                    }
                    String[] days               = {"Saturday", "Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday"};
                    Calendar c                  = Calendar.getInstance();
                    c.setTime(dbDateFormat.parse(start));
                    int day_of_week             = c.get(Calendar.DAY_OF_WEEK);
                    if(product.equals("Unknown Product") ){
                        message                 = "Get Your Reward for "+brewery +" @ "+locationName + " from " + days[day_of_week] + " " + timeFormat.format(dbDateFormat.parse(start)) + " to " + timeFormat.format(dbDateFormat.parse(end));
                    } else {
                        message                 = "Get Your Reward for "+product +" @ "+locationName + " from " + days[day_of_week] + " " + timeFormat.format(dbDateFormat.parse(start)) + " to " + timeFormat.format(dbDateFormat.parse(end));
                    }
                    currentDate                 = Calendar.getInstance();
                    stmt                        = transconn.prepareStatement("INSERT INTO pushMessage (message, location, reward, pushTime) VALUES (?, ?, ?, ?)");
                    stmt.setString(1, message);
                    stmt.setInt(2, location);
                    stmt.setInt(3, rewardId);
                    stmt.setString(4, dbDateFormat.format(currentDate.getTime()));
                    stmt.executeUpdate();
                    
                    stmt                        = transconn.prepareStatement(getLastId);
                    rsDetails                   = stmt.executeQuery();
                    if(rsDetails.next()) {
                        int messageId           = rsDetails.getInt(1);
                    }

                    /**/
                    /*SQLBeerBoardHandler socialMedia   = new SQLBeerBoardHandler();
                    stmt                        = transconn.prepareStatement("SELECT customer, file from bevSyncCreatives WHERE id= ?");
                    stmt.setInt(1,creatives);
                    rsDetails                   = stmt.executeQuery();
                    if(rsDetails.next()) {
                        String fileName     = "http://beerboard.tv/USBN.BeerBoard.UI/bevSyncCustomers/" + String.valueOf(rsDetails.getInt(1)) + "/" + rsDetails.getString(2).replaceAll(" ", "%20");
                        socialMedia.saveImage(fileName, "/home/midware/facebook/reward/"+rsDetails.getString(2));                    
                        File file                    = new File("/home/midware/facebook/reward/"+rsDetails.getString(2));
                        stmt                     = transconn.prepareStatement(selectToken); 
                        stmt.setInt(1,location);
                        rsFB                     = stmt.executeQuery();
                        while (rsFB.next()) {
                            String pageId        = rsFB.getString(1);
                            String fbId=rsFB.getString(2);
                            String locationToken=rsFB.getString(3);
                            socialMedia.postImage(fbId, pageId, locationToken, message, location, rewardId, file,4);
                        }
                     *
                        stmt                       = transconn.prepareStatement("Select t.consumerKey,t.consumerSecret,t.accesToken,t.tokenSecret,t.id from usbnTwitter t LEFT JOIN twitterLocationMap l ON l.twitter = t.id WHERE l.location = ?;");
                        stmt.setInt(1,location);
                        rsFB                         = stmt.executeQuery();
                        while(rsFB.next()) {
                            String consumerKey     = rsFB.getString(1);
                            String consumerSecret  = rsFB.getString(2);
                            String accesToken      = rsFB.getString(3);
                            String tokenSecret     = rsFB.getString(4);
                            int user                     = rs.getInt(5);
                            socialMedia.tweetImage(user,location,message, consumerKey, consumerSecret, accesToken, tokenSecret, file,4,rewardId);
                        }
                    }
                    */
                } else {
                    toAppend.addElement("rewardStatus").addText("Already Reward Assigned for this location");
                }
            }

            Iterator updateCustomerReward   = toHandle.elementIterator("updateReward");
            while (updateCustomerReward.hasNext()) {
                Element customerRewards     = (Element) updateCustomerReward.next();
                int id                      = HandlerUtils.getRequiredInteger(customerRewards, "id");
                int campaign                = HandlerUtils.getRequiredInteger(customerRewards, "campaign");
                //int creatives               = HandlerUtils.getRequiredInteger(customerRewards, "creatives");
                String rewardText           = HandlerUtils.getRequiredString(customerRewards, "rewardText");
                int breweryId               = HandlerUtils.getRequiredInteger(customerRewards, "brewery");
                int productId               = HandlerUtils.getRequiredInteger(customerRewards, "product");
                int location                = HandlerUtils.getRequiredInteger(customerRewards, "location");
                String start                = HandlerUtils.getRequiredString(customerRewards, "startTime");
                String end                  = HandlerUtils.getRequiredString(customerRewards, "endTime");

                String update               = " UPDATE bevSyncCampaignReward SET rewardText = ?, campaign= ?, location = ?, startTime=?, endTime = ? WHERE id = ? ";
                String checkReward          = "SELECT id, creatives FROM bevSyncCampaignReward WHERE location = ? AND campaign = ? AND rewardText = ? AND ? BETWEEN startTime AND endTime AND  ? BETWEEN  startTime AND endTime";
                stmt                        = transconn.prepareStatement(checkReward);
                stmt.setInt(1, location);
                stmt.setInt(2, campaign);
                stmt.setString(3, rewardText);
                stmt.setString(4, dbDateFormat.format(dbDateFormat.parse(start)));
                stmt.setString(5, dbDateFormat.format(dbDateFormat.parse(end)));
                rs                          = stmt.executeQuery();
                int rid                     = 0;
                int oldCreatives            = 0;
                if (rs.next()) {
                        rid                 = rs.getInt(1);
                        oldCreatives        = rs.getInt(2);
                }
                
                if(rid == 0 || rid == id) {
                stmt                        = transconn.prepareStatement(update);                
                stmt.setString(1, rewardText);                
                stmt.setInt(2, campaign);
                stmt.setInt(3, location);
                stmt.setString(4, dbDateFormat.format(dbDateFormat.parse(start)));
                stmt.setString(5, dbDateFormat.format(dbDateFormat.parse(end)));
                stmt.setInt(6, id);                
                stmt.executeUpdate();
                
                generateInsiderPass(id,rewardText,breweryId,"reward"+"-"+id+"-"+campaign+"-"+location);
                Element rewardEl          = toAppend.addElement("rewardPass");
                rewardEl.addElement("id").addText(String.valueOf(id));                                                                    
                rewardEl.addElement("path").addText("http://social.usbeveragenet.com:8080/fileUploader/Images/"+"reward"+"-"+id+"-"+campaign+"-"+location+".jpg");
                
                stmt                        = transconn.prepareStatement("DELETE FROM bevSyncCreatives WHERE id = ?");
                stmt.setInt(1,oldCreatives);
                stmt.executeUpdate();
                
                stmt                        = transconn.prepareStatement("SELECT CONCAT ('Reward','-',title, '-',(select name from location WHERE id=?)) from bevSyncCampaign WHERE id = ?;");
                stmt.setInt(1,location);
                stmt.setInt(2,campaign);
                rs                          = stmt.executeQuery();
                String title                = "Reward";
                if(rs.next()) {
                    title                   = rs.getString(1);
                }
                
                int creatives = addCreative(userId,5, customerId, title, "reward"+"-"+id+"-"+campaign+"-"+location+".jpg", end, breweryId, productId);
                stmt                        = transconn.prepareStatement("UPDATE bevSyncCampaignReward SET creatives =? WHERE id =?");
                stmt.setInt(1,creatives);
                stmt.setInt(2,id);
                stmt.executeUpdate();                
                
                
                String logMessage           = "Updated Reward '" + id + "'";                
                logger.portalDetail(callerId, "updateBevSyncReward", 0, "bevSyncCampaignReward", id, logMessage, transconn);
            } else {
                    toAppend.addElement("rewardStatus").addText("Already Reward Assigned for this location");
                    
                }
            }

            Iterator deleteCustomerReward   = toHandle.elementIterator("deleteReward");
            while (deleteCustomerReward.hasNext()) {
                Element customerReward      = (Element) deleteCustomerReward.next();
                int id                      = HandlerUtils.getRequiredInteger(customerReward, "id");
                String checkReward          = "SELECT creatives FROM bevSyncCampaignReward WHERE id= ?";
                stmt                        = transconn.prepareStatement(checkReward);
                stmt.setInt(1, id);               
                rs                          = stmt.executeQuery();               
                int creatives            = 0;
                if (rs.next()) {                     
                        creatives        = rs.getInt(1);
                        stmt                        = transconn.prepareStatement("DELETE FROM bevSyncCreatives WHERE id = ?");
                        stmt.setInt(1,creatives);
                        stmt.executeUpdate();
                }

                String delete               = " DELETE FROM bevSyncCampaignReward WHERE id = ? ";

                stmt                        = transconn.prepareStatement(delete);
                stmt.setInt(1,id);
                stmt.executeUpdate();

                String logMessage           = "Delete Reward '" + id + "'";
                logger.portalDetail(callerId, "deleteBevSyncReward", 0, "bevSyncCampaignReward", id, logMessage, transconn);
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: "+sqle.toString());
            throw new HandlerException(sqle);
        } catch(Exception e) {
            logger.debug(e.getMessage());
            e.printStackTrace();
             } finally {
            close(rsFB);
            close(rsDetails);
            close(rs);
            close(stmt);
        
        }
   }
    
    
    private void addUpdateDeleteBanner(Element toHandle, Element toAppend) throws HandlerException {

        int callerId                        = getCallerId(toHandle);  
        int userId                          = HandlerUtils.getRequiredInteger(toHandle, "userId");
        int customerId                      = HandlerUtils.getRequiredInteger(toHandle, "customerId");
        ResultSet rs                        = null, rsDetails = null,rsFB = null;
        PreparedStatement stmt              = null;
        Calendar currentDate                = null;

        try {
           

            Iterator addCustomerBanners   = toHandle.elementIterator("addBanner");
            while (addCustomerBanners.hasNext()) {
                Element customerBanners     = (Element) addCustomerBanners.next();
                int campaign                = HandlerUtils.getRequiredInteger(customerBanners, "campaign");
                //int creatives               = HandlerUtils.getRequiredInteger(customerRewards, "creatives");
                String bannerText           = HandlerUtils.getRequiredString(customerBanners, "bannerText");
                int breweryId               = HandlerUtils.getRequiredInteger(customerBanners, "brewery");
                int productId               = HandlerUtils.getRequiredInteger(customerBanners, "product");
                int location                = HandlerUtils.getRequiredInteger(customerBanners, "location");
                int type                    = HandlerUtils.getRequiredInteger(customerBanners, "type");
                String start                = HandlerUtils.getRequiredString(customerBanners, "startTime");
                String end                  = HandlerUtils.getRequiredString(customerBanners, "endTime");
                String link                 = HandlerUtils.getOptionalString(customerBanners, "link");
             
               
                String checkBanner          = "SELECT id FROM bevSyncCampaignBanner WHERE location = ? AND campaign = ? AND bannerText = ? AND ? BETWEEN startTime AND endTime AND  ? BETWEEN  startTime AND endTime";
                String selectProduct        = "SELECT pS.name, IFNULL(pD.boardname, 'Unknown Product'),(SELECT CONCAT(boardname,', ',addrCity) from location where id=?) from bevSyncCreatives bC "
                                            + " LEFT JOIN product p ON p.id = bC.product LEFT JOIN productDescription pD ON pD.product = p.id LEFT JOIN productSet pS ON pS.id=bC.brewery WHERE pS.productSetType=7 AND bC.id=?;";
                String getLastId            = " SELECT LAST_INSERT_ID()";
                String selectToken          = "select Distinct p.pageid, u.user_id, u.access_token from usbnFacebook u LEFT JOIN usbnFacebookPage p ON u.user_id =p.fbid where p.location=?  and u.user_id!='5515563';";
                int bannerId                = 0;
                String brewery              = null,locationName = null,product= null, message = "";
                stmt                        = transconn.prepareStatement(checkBanner);
                stmt.setInt(1, location);
                stmt.setInt(2, campaign);
                stmt.setString(3, bannerText);
                stmt.setString(4, dbDateFormat.format(dbDateFormat.parse(start)));
                stmt.setString(5, dbDateFormat.format(dbDateFormat.parse(end)));
               
                
                rs                          = stmt.executeQuery();
                if (!rs.next()) {
                    stmt                        = transconn.prepareStatement("INSERT INTO bevSyncCampaignBanner (campaign, bannerText, location,startTime,endTime,type, link ) VALUES (?, ?, ?, ?, ?, ?, ?)");
                    stmt.setInt(1, campaign);
                    stmt.setString(2, bannerText);                    
                    stmt.setInt(3, location);
                    stmt.setString(4, dbDateFormat.format(dbDateFormat.parse(start)));
                    stmt.setString(5, dbDateFormat.format(dbDateFormat.parse(end)));
                    stmt.setInt(6, type);
                    stmt.setString(7, HandlerUtils.nullToEmpty(link));
                    stmt.executeUpdate();
                    
                    
                    
                    stmt                        = transconn.prepareStatement(getLastId);
                    rs                          = stmt.executeQuery();
                    if(rs.next()) {
                        bannerId                = rs.getInt(1);
                    }
                    
                    generateBanner(bannerId,bannerText,breweryId,productId,"banner"+"-"+bannerId+"-"+campaign+"-"+location);
                    Element rewardEl          = toAppend.addElement("adBanner");
                    rewardEl.addElement("id").addText(String.valueOf(bannerId));                                                                    
                    rewardEl.addElement("path").addText("http://social.usbeveragenet.com:8080/fileUploader/Images/"+"banner"+"-"+bannerId+"-"+campaign+"-"+location+".jpg");
                    stmt                        = transconn.prepareStatement("SELECT CONCAT ('Banner','-',title, '-',(select name from location WHERE id=?)) from bevSyncCampaign WHERE id = ?;");
                    stmt.setInt(1,location);
                    stmt.setInt(2,campaign);
                    rs                          = stmt.executeQuery();
                    String title                = "Banner";
                    if(rs.next()) {
                        title                   = rs.getString(1);
                    }
                    int creatives = addCreative(userId,6, customerId, title, "banner"+"-"+bannerId+"-"+campaign+"-"+location+".jpg", end, breweryId, productId);
                    stmt                        = transconn.prepareStatement("UPDATE bevSyncCampaignBanner SET creatives =? WHERE id =?");
                    stmt.setInt(1,creatives);
                    stmt.setInt(2,bannerId);
                    stmt.executeUpdate();
                    
                    
                } else {
                    toAppend.addElement("bannerStatus").addText("Already Banner Assigned for this location");
                    
                }
            }

            Iterator updateCustomerBanners   = toHandle.elementIterator("updateBanner");
            while (updateCustomerBanners.hasNext()) {
                Element customerBanners     = (Element) updateCustomerBanners.next();
                int id                      = HandlerUtils.getRequiredInteger(customerBanners, "id");
                int campaign                = HandlerUtils.getRequiredInteger(customerBanners, "campaign");
                //int creatives               = HandlerUtils.getRequiredInteger(customerBanners, "creatives");
                String bannerText           = HandlerUtils.getRequiredString(customerBanners, "bannerText");
                int breweryId               = HandlerUtils.getRequiredInteger(customerBanners, "brewery");
                int productId               = HandlerUtils.getRequiredInteger(customerBanners, "product");
                int location                = HandlerUtils.getRequiredInteger(customerBanners, "location");
                String start                = HandlerUtils.getRequiredString(customerBanners, "startTime");
                String end                  = HandlerUtils.getRequiredString(customerBanners, "endTime");
                int type                    = HandlerUtils.getRequiredInteger(customerBanners, "type");
                String link                 = HandlerUtils.getOptionalString(customerBanners, "link");

                String update               = " UPDATE bevSyncCampaignBanner SET bannerText = ?, campaign= ?, location = ?, startTime=?, endTime = ?, type = ?, link = ? WHERE id = ? ";
                String checkBanner          = "SELECT id, creatives FROM bevSyncCampaignBanner WHERE location = ? AND campaign = ? AND bannerText = ? AND ? BETWEEN startTime AND endTime AND  ? BETWEEN  startTime AND endTime";
                stmt                        = transconn.prepareStatement(checkBanner);
                stmt.setInt(1, location);
                stmt.setInt(2, campaign);
                stmt.setString(3, bannerText);
                stmt.setString(4, dbDateFormat.format(dbDateFormat.parse(start)));
                stmt.setString(5, dbDateFormat.format(dbDateFormat.parse(end)));
                rs                          = stmt.executeQuery();
                int rid                     = 0;
                int oldCreatives            = 0;
                if (rs.next()) {
                        rid                 = rs.getInt(1);
                        oldCreatives        = rs.getInt(2);
                }
                
                if(rid == 0 || rid == id) {
                stmt                        = transconn.prepareStatement(update);                
                stmt.setString(1, bannerText);                
                stmt.setInt(2, campaign);
                stmt.setInt(3, location);
                stmt.setString(4, dbDateFormat.format(dbDateFormat.parse(start)));
                stmt.setString(5, dbDateFormat.format(dbDateFormat.parse(end)));
                stmt.setInt(6, type);  
                stmt.setString(7, HandlerUtils.nullToEmpty(link));
                stmt.setInt(8, id);      
                stmt.executeUpdate();
                
                generateBanner(id,bannerText,breweryId,productId,"banner"+"-"+id+"-"+campaign+"-"+location);
                Element rewardEl          = toAppend.addElement("adBanner");
                rewardEl.addElement("id").addText(String.valueOf(id));                                                                    
                rewardEl.addElement("path").addText("http://social.usbeveragenet.com:8080/fileUploader/Images/"+"banner"+"-"+id+"-"+campaign+"-"+location+".jpg");
                
                stmt                        = transconn.prepareStatement("DELETE FROM bevSyncCreatives WHERE id = ?");
                stmt.setInt(1,oldCreatives);
                stmt.executeUpdate();
                
                stmt                        = transconn.prepareStatement("SELECT CONCAT ('Banner','-',title, '-',(select name from location WHERE id=?)) from bevSyncCampaign WHERE id = ?;");
                stmt.setInt(1,location);
                stmt.setInt(2,campaign);
                rs                          = stmt.executeQuery();
                String title                = "Banner";
                if(rs.next()) {
                    title                   = rs.getString(1);
                }
                
                int creatives = addCreative(userId,6, customerId, title, "banner"+"-"+id+"-"+campaign+"-"+location+".jpg", end, breweryId, productId);
                stmt                        = transconn.prepareStatement("UPDATE bevSyncCampaignBanner SET creatives =? WHERE id =?");
                stmt.setInt(1,creatives);
                stmt.setInt(2,id);
                stmt.executeUpdate();                
                
                
                String logMessage           = "Updated Banner '" + id + "'";                
                logger.portalDetail(callerId, "updateBevSyncBanner", 0, "bevSyncCampaignBanner", id, logMessage, transconn);
            } else {
                    toAppend.addElement("bannerStatus").addText("Already Banner Assigned for this location");
                    
                }
            }

            Iterator deleteCustomerBanners   = toHandle.elementIterator("deleteBanner");
            while (deleteCustomerBanners.hasNext()) {
                Element customerBanners     = (Element) deleteCustomerBanners.next();
                int id                      = HandlerUtils.getRequiredInteger(customerBanners, "id");
                String checkBanner          = "SELECT creatives FROM bevSyncCampaignBanner WHERE id= ?";
                stmt                        = transconn.prepareStatement(checkBanner);
                stmt.setInt(1, id);               
                rs                          = stmt.executeQuery();               
                int creatives            = 0;
                if (rs.next()) {                     
                        creatives        = rs.getInt(1);
                        stmt                        = transconn.prepareStatement("DELETE FROM bevSyncCreatives WHERE id = ?");
                        stmt.setInt(1,creatives);
                        stmt.executeUpdate();
                }

                String delete               = " DELETE FROM bevSyncCampaignBanner WHERE id = ? ";

                stmt                        = transconn.prepareStatement(delete);
                stmt.setInt(1,id);
                stmt.executeUpdate();

                String logMessage           = "Delete Banner '" + id + "'";
                logger.portalDetail(callerId, "deleteBevSyncBanner", 0, "bevSyncCampaignBanner", id, logMessage, transconn);
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: "+sqle.toString());
            throw new HandlerException(sqle);
        } catch(Exception e) {
            logger.debug(e.getMessage());
            e.printStackTrace();
             } finally {
            close(rsFB);
            close(rsDetails);
            close(rs);
            close(stmt);
        
        }
   }
    
    
    private int  addCreative(int userId,int type,int customerId, String title,String file, String validity, int brewery, int product )throws HandlerException{
        
         PreparedStatement stmt              = null;
        ResultSet rs                        = null;
        int creativeId                      = 0;
        try {
         stmt                        = transconn.prepareStatement("INSERT INTO bevSyncCreatives (user, customer, type, title, file,validity,brewery,product) VALUES (?, ?, ?, ?,?,?,?,?)");
                stmt.setInt(1, userId);
                stmt.setInt(2, customerId);
                stmt.setInt(3, type);
                stmt.setString(4, title);
                stmt.setString(5, file);                
                stmt.setString(6, validity);
                stmt.setInt(7, brewery);
                stmt.setInt(8, product);
                stmt.executeUpdate();

                stmt                        = transconn.prepareStatement("SELECT LAST_INSERT_ID()");
                rs                          = stmt.executeQuery();
                if(rs.next()) {
                        creativeId = rs.getInt(1);
                    }
                
                  } catch (SQLException sqle) {
            logger.dbError("Database error: "+sqle.toString());
            throw new HandlerException(sqle);
        }  finally {           
            close(rs);
            close(stmt);
        
        }
        return creativeId;
    }
    
    
    
    
    private void getCampaignReport(Element toHandle, Element toAppend) throws HandlerException {

        int campaign                        = HandlerUtils.getOptionalInteger(toHandle, "campaign");
        String locations                    = HandlerUtils.getOptionalString(toHandle, "locationId");
        boolean graph                       = HandlerUtils.getOptionalBoolean(toHandle, "graph");
        

        
        String selectCampaign               = "SELECT c.start,c.end, c.type FROM bevSyncCampaign c WHERE c.id= ?;";
        String selectProducts               = "SELECT p.id, p.name FROM product p LEFT JOIN productSetMap pSM ON pSM.product = p.id "
                                            + " LEFT JOIN productSet pS ON pS.id = pSM.productSet " +
                                            " WHERE pS.productSetType = 7 AND p.isActive = 1 AND pSM.productSet = ?";      
        String selectReward                 = "SELECT id FROM bevSyncCampaignReward WHERE  DATE(?) BETWEEN DATE(startTime) AND DATE(endTime) AND campaign = ?";
        String selectCreatives              = "SELECT Distinct cr.brewery,cr.product from bevSyncCreatives cr LEFT JOIN bevSyncCampaignCreatives bSC ON bSC.creatives=cr.id"
                                            + " LEFT JOIN bevSyncCampaignLocations bSL ON bSL.campaign =bSC.campaign WHERE bSC.campaign=? AND bSL.location IN("+locations+");";

      
        PreparedStatement stmt              = null;
        ResultSet rs                        = null, locationRS = null, productRs =null;
        Calendar currentDate                = null;
        
        
       
        
        String startDate                    =null, endDate=null;        
        int product                         = 0, brewery=0, cType=0;
        Map<Integer, Integer> productArray  = new HashMap<Integer, Integer>();

        try {
            stmt                = transconn.prepareStatement(selectCreatives);
            stmt.setInt(1, campaign);
            rs            = stmt.executeQuery();
            int j                      = 0;      
            while(rs.next()) { 
                brewery                    = rs.getInt(1);
                product                    = rs.getInt(2);
                          
                if(product > 0) {
                    productArray.put(j,product);
                    j++;
                } else {
                    stmt                    = transconn.prepareStatement(selectProducts);
                    stmt.setInt(1, brewery);
                    productRs              = stmt.executeQuery();
                    while(productRs.next()) {
                        productArray.put(j,productRs.getInt(1));                         
                        j++;
                    }
                }
            } 
           
            
             stmt                           = transconn.prepareStatement(selectCampaign);
             stmt.setInt(1,campaign);
             rs                      = stmt.executeQuery();
             if (rs.next()) { 
                 startDate                  = rs.getString(1);
                 endDate                    = rs.getString(2);                 
                 cType                      = rs.getInt(3);
               
                 
             } 
             if(graph) {
                toAppend.addElement("startDate").addText(startDate);
                toAppend.addElement("endDate").addText(endDate);
                Date start                  = dateFormat.parse(dateFormat.format(dbDateFormat.parse(startDate)));
                currentDate                 = Calendar.getInstance();
                currentDate.setTime(start);
                currentDate.add(Calendar.DATE,-28); 
                logger.debug("loc:"+locations);
                logger.debug("startDate"+startDate);
                logger.debug("endDate"+endDate);
                logger.debug("start"+dbDateFormat.format(currentDate.getTime()));
                start                       = dateFormat.parse(dateFormat.format(currentDate.getTime()));
                Date end                    = dateFormat.parse(dateFormat.format(dbDateFormat.parse(endDate)));
                currentDate.setTime(end);
                currentDate.add(Calendar.DATE,28);                  
                end                         = dateFormat.parse(dateFormat.format(currentDate.getTime()));
                campaignGraphGenerator(dbDateFormat.format(start), dbDateFormat.format(end), productArray, locations, toAppend, campaign);
                /*logger.debug("End"+dbDateFormat.format(currentDate.getTime()));
                int campaignFlag            = 0;
                 do {
                     if(start.equals(dateFormat.parse(dateFormat.format(dbDateFormat.parse(startDate))))) {
                         campaignFlag   =1;
                     }
                     if(start.equals(dateFormat.parse(dateFormat.format(dbDateFormat.parse(endDate))))) {
                         campaignFlag   =0;
                     }
                     currentDate.setTime(start);
                     if(campaignFlag>0 && cType >3) {
                         stmt               = transconn.prepareStatement(selectReward);
                         stmt.setString(1, new DateParameter(dbDateFormat.format(currentDate.getTime())).toString());
                         stmt.setInt(2,campaign);
                         rs                 = stmt.executeQuery();
                         if (rs.next()) { 
                             campaignFlag   = 2;
                         } else {
                             campaignFlag   = 1;                             
                         }
                     }
                     
                     campaignGraphGenerator(dbDateFormat.format(currentDate.getTime()),productArray,locations,toAppend,campaignFlag);
                     
                     currentDate.add(Calendar.DATE,1);  
                     start                  = dateFormat.parse(dateFormat.format(currentDate.getTime()));                     
                     
                 } while(!start.equals(end ));    */             
                 getCampaignDetails(campaign,locations,toAppend);
                
                 
             } else {
             campaignReportGenerator(startDate,endDate,productArray,locations,toHandle,toAppend,1);
             currentDate                    = Calendar.getInstance();
             Date start                     = dateFormat.parse(dateFormat.format(dbDateFormat.parse(startDate)));
             Date end                       = dateFormat.parse(dateFormat.format(dbDateFormat.parse(endDate)));
             long diff[]                    = getTimeDifference(start,end);
             logger.debug("Date Diff"+diff[0]);
             currentDate.setTime(start);
             currentDate.add(Calendar.DATE,(Integer.parseInt(String.valueOf(diff[0]+1))*-1));      
             String previousStart           = dbDateFormat.format(currentDate.getTime());
             end                          = dateFormat.parse(dateFormat.format(dbDateFormat.parse(startDate)));
             currentDate.setTime(end);
             currentDate.add(Calendar.DATE,-1);      
             
             campaignReportGenerator(previousStart,dbDateFormat.format(currentDate.getTime()),productArray,locations,toHandle,toAppend,2);
             getCampaignDetails(campaign,locations,toAppend);
             }
       } catch (SQLException sqle) {
           logger.debug(sqle.getMessage());
                throw new HandlerException(sqle);
            }catch (Exception e) {
                logger.debug(e.getMessage());
                e.printStackTrace();
            }finally {
            close(productRs);
             close(rs);
             close(stmt);
            }
    }
    
  

    private void campaignReportGenerator( String startDate,String  endDate,Map<Integer, Integer> productArray, String locations, Element toHandle, Element toAppend,int type) throws HandlerException {  
        boolean detail                      = HandlerUtils.getRequiredBoolean(toHandle, "detail");
        PreparedStatement stmt              = null;
        ResultSet rs                        = null;
        DateParameter validatedStartDate    = new DateParameter(startDate);
        DateParameter validatedEndDate      = new DateParameter(endDate);

        if (!validatedStartDate.isValid()) {
            logger.debug("Aborted report, invalid start date '" + startDate + "'");
            addErrorDetail(toAppend, "Invalid Start Date");
        } else if (!validatedEndDate.isValid()) {
            logger.debug("Aborted report, invalid end date '" + endDate + "'");
            addErrorDetail(toAppend, "Invalid End Date");
        }

        toAppend.addElement("startDate"+type).addText(startDate);
        toAppend.addElement("endDate"+type).addText(endDate);

        
            //String soldTable = "soldSummary";
            //logger.debug(soldTable);
            

            

            String pouredTable = "openHoursSummary";
            //logger.debug(pouredTable);
            String location[]               = locations.split(",");
            
           
            

            
            String selectLocationsPoured = "SELECT  p.product, pr.name,SUM(p.value), ((SUM(p.value)/ ?)*100) AS rank FROM " + pouredTable + " p  LEFT JOIN product pr ON pr.id =p.product" +
                    " WHERE p.location IN ("+locations+") AND p.date BETWEEN DATE(?) AND DATE(?)  GROUP BY p.product ORDER BY rank DESC"   ;
            String selectLocationPoured = "SELECT p.location,l.name, p.product, pr.name,sum(p.value), ((sum(p.value)/ ?)*100) AS rank FROM " + pouredTable + " p  LEFT JOIN product pr ON pr.id = p.product" +
                    " LEFT JOIN location l ON l.id=p.location   WHERE p.location =? AND p.date BETWEEN DATE(?) AND DATE(?) GROUP BY p.product ORDER BY rank DESC" ;
            String selectLocationsPouredSum = "SELECT  sum(p.value) FROM " + pouredTable + " p  WHERE p.location IN ("+locations+") AND p.date BETWEEN DATE(?) AND DATE(?) " ;
            String selectLocationPouredSum = "SELECT  SUM(p.value) FROM " + pouredTable + " p  WHERE p.location =? AND p.date BETWEEN DATE(?) AND DATE(?) " ;
            //logger.debug(selectLocationPoured );

           

            try {
                if(!detail) {
                    double sum              = 0.0;
                stmt = transconn.prepareStatement(selectLocationsPouredSum);
                stmt.setString(1, validatedStartDate.toString());
                stmt.setString(2, validatedEndDate.toString());
                rs = stmt.executeQuery();
                if(rs.next()){
                    sum                 = rs.getDouble(1);
                }    
                //logger.debug("SUM:"+sum);           
                stmt = transconn.prepareStatement(selectLocationsPoured);
                stmt.setDouble(1,sum );
                stmt.setString(2, validatedStartDate.toString());
                stmt.setString(3, validatedEndDate.toString());
                rs = stmt.executeQuery();
                int rank           = 1;
                while(rs.next()) {                    
                    Element reportEl        = toAppend.addElement("report"+type);
                    int productId           = rs.getInt(1);
                    reportEl.addElement("productId").addText(String.valueOf(productId));                                                
                    reportEl.addElement("productName").addText(HandlerUtils.nullToEmpty(rs.getString(2))); 
                    reportEl.addElement("poured").addText(HandlerUtils.nullToEmpty(rs.getString(3))); 
                    reportEl.addElement("avg").addText(String.valueOf((rs.getDouble(4)))); 
                    reportEl.addElement("rank").addText(String.valueOf(rank++));                           
                    if(productArray.containsValue(productId)) {
                        reportEl.addElement("campaign").addText("1"); 
                    } else {
                        reportEl.addElement("campaign").addText("0"); 
                    }
                }
                } else {
                    for(int i=0;i<location.length;i++) {
                    
                    double sum              = 0.0;
                    stmt = transconn.prepareStatement(selectLocationPouredSum);  
                    stmt.setInt(1, Integer.parseInt(location[i]));
                    stmt.setString(2, validatedStartDate.toString());
                    stmt.setString(3, validatedEndDate.toString());
                    rs = stmt.executeQuery();
                    
                    if(rs.next()){
                        sum                 = rs.getDouble(1);
                    }
                     stmt = transconn.prepareStatement(selectLocationPoured);
                     stmt.setDouble(1,sum );
                     stmt.setInt(2, Integer.parseInt(location[i]));
                    stmt.setString(3, validatedStartDate.toString());
                    stmt.setString(4, validatedEndDate.toString());
                     rs = stmt.executeQuery();
                     int rank           = 1;
                     while(rs.next()) {                         
                         Element reportEl   = toAppend.addElement("report"+type);
                         int productId      = rs.getInt(3);
                         reportEl.addElement("productId").addText(String.valueOf(productId));                                                 
                         reportEl.addElement("locationName").addText(HandlerUtils.nullToEmpty(rs.getString(2))); 
                         reportEl.addElement("locationId").addText(String.valueOf(rs.getInt(1)));                                                
                         reportEl.addElement("productName").addText(HandlerUtils.nullToEmpty(rs.getString(4))); 
                         reportEl.addElement("poured").addText(HandlerUtils.nullToEmpty(rs.getString(5))); 
                         reportEl.addElement("avg").addText(String.valueOf((rs.getDouble(6))));                           
                         reportEl.addElement("rank").addText(String.valueOf(rank++));                           
                         if(productArray.containsValue(productId)) {
                             reportEl.addElement("campaign").addText("1");                              
                         } else {
                             reportEl.addElement("campaign").addText("0"); 
                         }
                     }
                }
                }
            } catch (SQLException sqle) {
                throw new HandlerException(sqle);
            } finally {
                close(rs);
                close(stmt);
            }
}
    
    private void campaignGraphGenerator( String startDate,Map<Integer, Integer> productArray, String locations, Element toAppend,int type) throws HandlerException {  
        
        PreparedStatement stmt              = null;
        ResultSet rs                        = null;
        DateParameter validatedStartDate    = new DateParameter(startDate);
        

        if (!validatedStartDate.isValid()) {
            logger.debug("Aborted report, invalid start date '" + startDate + "'");           
        } 
        

        
            //String soldTable = "soldSummary";
            //logger.debug(soldTable);
            

            

            String pouredTable = "openHoursSummary";
            //logger.debug(pouredTable);
            String selectLocationsPoured = "SELECT  p.product, pr.name,SUM(p.value) FROM " + pouredTable + " p  LEFT JOIN product pr ON pr.id =p.product" +
                    " WHERE p.location IN ("+locations+") AND p.date BETWEEN DATE(?) AND DATE(?)  GROUP BY p.product "   ;
           
            String selectLocationsPouredSum = "SELECT  sum(p.value) FROM " + pouredTable + " p  WHERE p.location IN ("+locations+") AND p.date BETWEEN DATE(?) AND DATE(?) " ;
            

           

            try {
                double sum              = 0.0;
                double  campaign        = 0.0;
                stmt = transconn.prepareStatement(selectLocationsPouredSum);
                stmt.setString(1, validatedStartDate.toString());
                stmt.setString(2, validatedStartDate.toString());
                rs = stmt.executeQuery();
                if(rs.next()){
                    sum                 = rs.getDouble(1);
                }    
               // logger.debug("SUM:"+sum);           
                stmt = transconn.prepareStatement(selectLocationsPoured);
                stmt.setString(1, validatedStartDate.toString());
                stmt.setString(2, validatedStartDate.toString());
                rs = stmt.executeQuery();
               
                while(rs.next()) {                    
                    int productId           = rs.getInt(1);
                    if(productArray.containsValue(productId)) {
                    campaign +=rs.getDouble(3);    
                    } 
                }
                //logger.debug("Campaign Value:"+campaign); 
                if(sum!=0&& campaign!=0) {
                campaign    = (campaign/sum) *100;
                }  else {
                    campaign = 0;
                }
                if(campaign >0) {
                Element reportEl        = toAppend.addElement("graph");                
                reportEl.addElement("value").addText(String.valueOf(campaign)); 
                reportEl.addElement("date").addText(startDate);
                reportEl.addElement("type").addText(String.valueOf(type));
                }
               
                
                
            } catch (SQLException sqle) {
                logger.debug(sqle.getMessage());
                throw new HandlerException(sqle);
                
            } finally {
                close(rs);
                close(stmt);
            }
    }
    
    
    private void campaignGraphGenerator( String startDate,String endDate,Map<Integer, Integer> productArray, String locations, Element toAppend,int campaignId) throws HandlerException {  
        
                
        String selectReward                 = "SELECT id FROM bevSyncCampaignReward WHERE  DATE(?) BETWEEN DATE(startTime) AND DATE(endTime) AND campaign = ?";
        PreparedStatement stmt              = null;
        ResultSet rs                        = null,rsDetail = null,rsCampaign=null,rsReward=null;
        DateParameter validatedStartDate    = new DateParameter(startDate);
        DateParameter validatedEndDate      = new DateParameter(endDate);
        int campaignFlag                    = 0;

        if (!validatedStartDate.isValid()) {
            logger.debug("Aborted report, invalid start date '" + startDate + "'");           
        } else if (!validatedEndDate.isValid()) {
            logger.debug("Aborted report, invalid end date '" + endDate + "'");           
        }

        
            //String soldTable = "soldSummary";
            //logger.debug(soldTable);
            

            

            String pouredTable = "openHoursSummary";
            //logger.debug(pouredTable);
            String selectLocationsPoured = "SELECT  p.product, pr.name, SUM(p.value) FROM " + pouredTable + " p  LEFT JOIN product pr ON pr.id =p.product" +
                    " WHERE p.location IN ("+locations+") AND p.date BETWEEN DATE(?) AND DATE(?)  GROUP BY p.product "   ;
           
            String selectLocationsPouredSum = "SELECT p.date, sum(p.value) FROM " + pouredTable + " p  WHERE p.location IN ("+locations+") AND p.date BETWEEN DATE(?) AND DATE(?)  GROUP BY p.date" ;
            

           

            try {
                
                double sum              = 0.0;
                double  campaign        = 0.0;
                stmt = transconn.prepareStatement(selectLocationsPouredSum);
                stmt.setString(1, validatedStartDate.toString());
                stmt.setString(2, validatedEndDate.toString());
                rs = stmt.executeQuery();
                while(rs.next()){
                    String date         = rs.getString(1);
                    sum                 = rs.getDouble(2);
                   
                //logger.debug("Date:"+date+" SUM:"+sum);           
                stmt = transconn.prepareStatement(selectLocationsPoured);
                stmt.setString(1, date);
                stmt.setString(2, date);
                rsDetail = stmt.executeQuery();
                campaign                = 0.0;
                while(rsDetail.next()) {                    
                    int productId           = rsDetail.getInt(1);
                    if(productArray.containsValue(productId)) {
                        campaign +=rsDetail.getDouble(3);  
                        //logger.debug("Product:"+rsDetail.getString(2));
                    } 
                }

                double campaignShare        = 0.0;
                if(campaign!=0 && sum!=0) {
                    campaignShare    = (campaign/sum) *100;
                }  else {
                    campaignShare = 0;
                }

                //logger.debug("Date:" + date + ", Campaign Volume:"+campaign+", Total Volume:"+sum+", Campaign Share:" + campaignShare);
               
                Element reportEl        = toAppend.addElement("graph");                
                reportEl.addElement("value").addText(String.valueOf(campaignShare));
                reportEl.addElement("myVolume").addText(String.valueOf(campaign));
                reportEl.addElement("totalVolume").addText(String.valueOf(sum));

                reportEl.addElement("date").addText(date);
                stmt                        = transconn.prepareStatement("SELECT id,type,title FROM bevSyncCampaign WHERE Date(?) BETWEEN DATE(start) AND DATE(end) AND id = ?");                
                stmt.setString(1, date);
                stmt.setInt(2, campaignId);
                rsCampaign            = stmt.executeQuery();               
                if(rsCampaign.next()) { 
                    if(rs.getInt(2)>3) {
                    stmt               = transconn.prepareStatement(selectReward);
                         stmt.setString(1, date);
                         stmt.setInt(2,campaignId);
                         rsReward                 = stmt.executeQuery();
                         if (rsReward.next()) {
                             campaignFlag   = 2;
                             
                         } else {
                             campaignFlag   = 1;
                    }
                         
                    } else {
                    campaignFlag            = 1;
                    }
                }else {
                    campaignFlag            = 0;
                }
                reportEl.addElement("type").addText(String.valueOf(campaignFlag));
                
                
                }
               
                
                
            } catch (SQLException sqle) {
                logger.debug(sqle.getMessage());
                throw new HandlerException(sqle);
                
            } finally {
                close(rsReward);
                close(rsCampaign);
                close(rsDetail);
                close(rs);
                close(stmt);
            }
    }
     
     
 
    
    
    private void getCampaignDetails(int campaign, String locations,Element toAppend) throws HandlerException {

        PreparedStatement stmt              = null;
        ResultSet rs                        = null, rsDetail = null,rsDate = null;
        String selectCampaignLocations      = "SELECT bC.customer, l.id, CONCAT(l.boardName,'-', l.name) Location FROM bevSyncCampaign bC " +
                                            " LEFT JOIN bevSyncCampaignLocations bSL ON bSL.campaign = bC.id LEFT JOIN location l ON l.id = bSL.location " +
                                            " WHERE bC.id = ? AND l.id IN ("+locations+");";
        String selectCampaignCreatives      = "SELECT bSC.title, bSC.file, bSCC.id, DATE(sL.date), COUNT(sL.file),bSC.brewery,bSC.product,bC.start,bC.end FROM bevSyncCampaign bC " +
                                            " LEFT JOIN bevSyncCampaignCreatives bSCC ON bSCC.campaign = bC.id LEFT JOIN bevSyncCreatives bSC ON bSC.id = bSCC.creatives " +
                                            " LEFT JOIN productSet p ON p.id = bSC.brewery LEFT JOIN sponsorLogs sL ON sL.file = bSC.file " +
                                            " WHERE bC.id = ? AND sL.location = ? AND DATE(sL.date) between DATE(bC.start) AND DATE(bC.end) GROUP BY bSC.id, DATE(sL.date);";
        String selectDates                  = "SELECT max(lS.date),min(lS.date) FROM lineSummary lS LEFT JOIN  line li ON li.id = lS.line"
                                            + " LEFT JOIN bar b ON b.id = li.bar  LEFT JOIN location l ON l.id = b.location LEFT JOIN productSetMap pSM ON pSM.product=li.product "
                                            + " LEFT JOIN productSet pS ON pS.id=pSM.productSet WHERE ";
        try {
            

            stmt                            = transconn.prepareStatement(selectCampaignLocations);
            stmt.setInt(1,campaign);
            rs                              = stmt.executeQuery();
            while (rs.next()) {
                int customer                = rs.getInt(1);
                int location                = rs.getInt(2);
                String title                = "";
                Element detailsEl           = null;
                Element locationEl          = toAppend.addElement("details");
                locationEl.addElement("location").addText(HandlerUtils.nullToEmpty(rs.getString(3)));
                locationEl.addElement("locationId").addText(String.valueOf(location));

                //logger.debug("Sending Campaign Logs for " + rs.getString(3));

                stmt                        = transconn.prepareStatement(selectCampaignCreatives);
                stmt.setInt(1,campaign);
                stmt.setInt(2,location);
                rsDetail                    = stmt.executeQuery();
                while (rsDetail.next()) {
                    
                   
                    
                    if (!title.equalsIgnoreCase(rsDetail.getString(1))) {
                        title               = rsDetail.getString(1);
                        detailsEl           = locationEl.addElement("creatives");
                        detailsEl.addElement("title").addText(HandlerUtils.nullToEmpty(title));
                        //detailsEl.addElement("file").addText(HandlerUtils.nullToEmpty(rsDetail.getString(2)));
                        //detailsEl.addElement("filePath").addText(HandlerUtils.nullToEmpty("http://bevsync.net/USBN.BevSync.UI/Images/Customers/"+ String.valueOf(customer) + "/" + rsDetail.getString(2).replaceAll(" ", "%20")));
                        detailsEl.addElement("creativeIde").addText(String.valueOf(rsDetail.getInt(3)));
                        int brewery         = rsDetail.getInt(6);
                        int product         = rsDetail.getInt(7);
                        String start        = rsDetail.getString(8);
                        String end          = rsDetail.getString(9);
                        String condition    = "";
                        if(product == 0) {
                            condition       =" pS.id="+brewery+" AND location ="+location+" AND lS.date BETWEEN DATE('"+start+"') AND DATE('"+end+"')";
                        } else {
                            condition       =" li.product="+product+" AND location ="+location+" AND lS.date BETWEEN DATE('"+start+"') AND DATE('"+end+"')";
                        }
                        
                        stmt                = transconn.prepareStatement(selectDates+condition);
                        rsDate              = stmt.executeQuery();
                        if (rsDate.next()) {
                            detailsEl.addElement("tapStart").addText(HandlerUtils.nullToEmpty(rsDate.getString(2)));
                            detailsEl.addElement("tapEnd").addText(HandlerUtils.nullToEmpty(rsDate.getString(1)));
                            //logger.debug("tapStart:"+rsDate.getString(2))  ;                          
                            //logger.debug("tapEnd:"+rsDate.getString(1))  ;
                        }
                        Element logsEl      = detailsEl.addElement("creativeLogs");
                        logsEl.addElement("date").addText(HandlerUtils.nullToEmpty(rsDetail.getString(4)));
                        logsEl.addElement("count").addText(String.valueOf(rsDetail.getInt(5)));
                    } else {
                        Element logsEl      = detailsEl.addElement("creativeLogs");
                        logsEl.addElement("date").addText(HandlerUtils.nullToEmpty(rsDetail.getString(4)));
                        logsEl.addElement("count").addText(String.valueOf(rsDetail.getInt(5)));
                    }
                    
                }
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: "+sqle.toString());
            throw new HandlerException(sqle);
        }  finally {
            close(rsDetail);
            close(rs);
            close(stmt);
        }
    }
        
     
    private void getBevSyncRegions(Element toHandle, Element toAppend) throws HandlerException {

        int customerId                      = HandlerUtils.getOptionalInteger(toHandle, "customerId");   
        
        String customerBrewery              = "SELECT brewery from bevSyncCustomer WHERE id = ?";
        String selectProduct                = "SELECT  DISTINCT pSM.product FROM  productSetMap pSM  Where pSM.productSet IN (? ) ORDER BY pSM.product";

        PreparedStatement stmt = null;        
        ResultSet rs = null;

        try {
            if (customerId >= 0) {
                String brewery              ="";
                stmt = transconn.prepareStatement(customerBrewery);
                stmt.setInt(1, customerId);
                rs                          = stmt.executeQuery();
                if(rs.next()) {
                    brewery                 = rs.getString(1);
                }
                stmt = transconn.prepareStatement("SELECT  DISTINCT pSM.product FROM  productSetMap pSM  Where pSM.productSet IN ("+brewery+" ) ORDER BY pSM.product");
                rs                = stmt.executeQuery();
                String result               = "";
                while (rs.next()) {
                    result                  +=rs.getString(1);                       
                    result                  +=",";
                }
                if(result.length()>0){
                    result                  =result.substring(0,result.length()-1);
                }
                Element regionEl            = toAppend.addElement("region");
                regionEl.addElement("regionId").addText(String.valueOf(customerId));
                regionEl.addElement("productList").addText(HandlerUtils.nullToEmpty(result));
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(rs);
            close(stmt);
        }

    }
    
    
    //SundarRavindran_28-Aug-2009_Start
    /**  Return a total tap share count for the requried drill down
     */
    private void getTapShare(Element toHandle, Element toAppend) throws HandlerException {

        int userId                          = HandlerUtils.getRequiredInteger(toHandle, "userId");
        int regionId                        = HandlerUtils.getOptionalInteger(toHandle, "regionId");
        int countyId                        = HandlerUtils.getOptionalInteger(toHandle, "countyId");
        int locationId                      = HandlerUtils.getOptionalInteger(toHandle, "locationId");
        int bevSync                         = HandlerUtils.getOptionalInteger(toHandle, "bevSync");
       // String state                        = HandlerUtils.getOptionalString(toHandle, "state");
        String locations                    = HandlerUtils.getOptionalString(toHandle, "locations");

        String startDate                    = HandlerUtils.getOptionalString(toHandle, "startDate");
        String endDate                      = HandlerUtils.getOptionalString(toHandle, "endDate");

        if (endDate == null || endDate.equals("")) {
            Calendar c1                     = Calendar.getInstance();
            java.util.Date d1               = c1.getTime();
            endDate                         = String.valueOf(d1.getYear() + 1900) + "-" + String.valueOf(d1.getMonth()+1) + "-" + String.valueOf(d1.getDate());
        }

        DateParameter validatedStartDate    = new DateParameter(startDate);
        DateParameter validatedEndDate      = new DateParameter(endDate);

        if (!validatedStartDate.isValid()) {
            logger.debug("Aborted report, invalid start date '" + startDate + "'");
            addErrorDetail(toAppend, "Invalid Start Date");
        } else if (!validatedEndDate.isValid()) {
            logger.debug("Aborted report, invalid end date '" + endDate + "'");
            addErrorDetail(toAppend, "Invalid End Date");
        }

        boolean getTapShareChange           = HandlerUtils.getOptionalBoolean(toHandle, "getTapShareChange");
        
        int parentLevel                     = 0, childLevel = 0;
        String conditionString              = " ", selectedValues = " ";

        int paramsSet                       = 0;
        int paramValue                      = 0;
        PreparedStatement stmt              = null;
        ResultSet rs                        = null;

        if(bevSync>0) {
            if (locationId > 0) {
                conditionString             += " AND lD.beerboard=1 AND l.id = ? ";
                selectedValues              = " l.id, b.id, ";
                parentLevel                 = 4;
                childLevel                  = 5;           
                paramValue                  = locationId;
            } else {
                 try {
                    String selectCreatives      = "SELECT GROUP_CONCAT(b.location ORDER BY b.location SEPARATOR ',') FROM bevSyncCustomerLocationMap b LEFT JOIN locationDetails lD ON lD.location=b.location WHERE b.customer = ? AND lD.active = 1 AND lD.beerboard = 1; ";
                    stmt                        = transconn.prepareStatement(selectCreatives);
                    stmt.setInt(1,bevSync);
                    rs                          = stmt.executeQuery();
                    if(rs.next()) {
                        if(rs.getString(1)!=null &&!rs.getString(1).equals("")) {
                            locations               =rs.getString(1);                       
                        } else {
                            locations     ="0"; 
                        }
                    }
                    
                 } catch (SQLException sqle) {
                throw new HandlerException(sqle);
            } finally {
                     close(rs);
                     close(stmt);
            }
                 conditionString            += " AND lD.beerboard=1 AND l.id IN ( "+locations+")";
                //logger.debug("Locations"+conditionString);
                selectedValues              = " l.id, b.id, ";
                parentLevel                 = 4;
                childLevel                  = 5;           
                paramValue                  = 0;
            }
        } else if (regionId > 0) {
            conditionString                 = " AND uRM.region = ? ";
            selectedValues                  = " l.countyIndex, l.id, ";
            parentLevel                     = 3;
            childLevel                      = 3;
            paramsSet++;
            paramValue                      = regionId;

        } else if (countyId > 0) {
            conditionString                 = " AND l.countyIndex = ? ";
            selectedValues                  = " l.id, b.id, ";
            parentLevel                     = 4;
            childLevel                      = 5;
            paramsSet++;
            paramValue                      = countyId;

        } else if (locationId > 0) {
            conditionString                 = " AND l.id = ? ";
            selectedValues                  = " l.id, b.id, ";
            parentLevel                     = 4;
            childLevel                      = 5;
            paramsSet++;
            paramValue                      = locationId;

        } else {
            conditionString                 = " ";
            selectedValues                  = " uRM.region, l.countyIndex, ";


            parentLevel                     = 2;
            childLevel                      = 2;
        }

        if (paramsSet > 1) {
            throw new HandlerException("Only one parameter can be set for getTapShare.");
        }
        Element endTag                      = toAppend.addElement("end");
        getTapShareDetail(userId, paramValue, parentLevel, childLevel, conditionString, selectedValues, validatedEndDate.toString(), endTag,bevSync,locations);
        
        if (getTapShareChange) {
            Element startTag                = toAppend.addElement("start");
            getTapShareDetail(userId, paramValue, parentLevel, childLevel, conditionString, selectedValues, validatedStartDate.toString(), startTag,bevSync,locations);
        }
    }
    //SundarRavindran_28-Aug-2009_End

    private void getTapShareDetail(int userId, int paramValue, int parentLevel, int childLevel, String conditionString, String selectedValues, String date, Element toAppend,int bevSync,String locations)
            throws HandlerException {

        int parent                          = 0, child = 0;
        int tapCount                        = 0, childTapCount = 0, parentTapCount = 0;
        int totalTapCount                   = 0, totalChildTapCount = 0, totalParentTapCount = 0;

        //logger.debug("Share date is: " + date);
        parentLevelMap                  = new ParentLevelMap(transconn, parentLevel);
        childLevelMap                   = new ChildLevelMap(transconn, childLevel);

        // A cache of Region Exclusion sets (maps Region -> Exclusion)
        Map<Integer, RegionExclusionMap> exclusionMapCache
                                            = new HashMap<Integer, RegionExclusionMap>();

        // A cache of Region Product sets (maps Region -> Product)
        Map<Integer, RegionProductMap> productMapCache
                                            = new HashMap<Integer, RegionProductMap>();

        Element parentTag                   = null, childTag = null;
        String userLocationExclusions       = "0";

        PreparedStatement stmt              = null;
        ResultSet rs                        = null, locationRS  = null;
        try {
            
            String userString           ="AND uRM.user = ? ";
            if (bevSync > 0) {
                userString  =" ";
            } else {

            String selectUserExclusions     = " SELECT e.tables, e.value FROM exclusion e LEFT JOIN userExclusionMap uEM ON uEM.exclusion = e.id WHERE e.type = 2 AND uEM.user = ? ";
            stmt                            = transconn.prepareStatement(selectUserExclusions);
            stmt.setInt(1, userId);
            rs                              = stmt.executeQuery();
            while (rs.next()) {
                String selectLocations      = " SELECT l.id FROM location l ";
                if (rs.getString(1).equals("county")) {
                    selectLocations         += " LEFT JOIN county c ON c.id = l.countyIndex WHERE c.id = ?";
                } else if (rs.getString(1).equals("location")) {
                    selectLocations         += " WHERE l.id = ? ";
                }
                stmt                        = transconn.prepareStatement(selectLocations);
                stmt.setInt(1, Integer.valueOf(rs.getString(2)));
                locationRS                  = stmt.executeQuery();
                while (locationRS.next()) {
                    userLocationExclusions  += ", " + locationRS.getString(1);
                }
                locationRS.close();
            }
            }
            

            String selectTapCount           = " SELECT " + selectedValues + " li.product, count(li.product) FROM lineSummary lS " +
                                            " LEFT JOIN line li ON li.id = lS.line LEFT JOIN bar b ON b.id = li.bar " +
                                            " LEFT JOIN location l ON l.id = b.location LEFT JOIN locationDetails lD ON lD.location = l.id" +
                                            " LEFT JOIN regionCountyMap rCM ON rCM.county = l.countyIndex LEFT JOIN groupRegionMap gRM ON gRM.regionMaster = rCM.region " +
                                            " LEFT JOIN region r ON r.regionGroup = gRM.id LEFT JOIN userRegionMap uRM ON uRM.region = r.id " +
                                            " WHERE lD.active = 1 AND lD.data = 1 "+userString+" AND l.id NOT IN (" + userLocationExclusions + ") AND lS.date = SUBDATE(?, INTERVAL 1 DAY) " + conditionString +
                                            " GROUP BY " + selectedValues + " li.product " +
                                            " ORDER BY " + selectedValues + " li.product ";

            stmt                            = transconn.prepareStatement(selectTapCount);
            //logger.debug("Query"+selectTapCount);
            if(bevSync >0) {
                stmt.setString(1, date);
                if(paramValue > 0) {
                    stmt.setInt(2, paramValue);
                }
            } else {
            stmt.setInt(1, userId);
            stmt.setString(2, date);
            if (paramValue > 0) {
                stmt.setInt(3, paramValue);
            }
            }
            rs                              = stmt.executeQuery();
            while (rs.next()) {

                if (parent != rs.getInt(1)) {
                    parent                  = rs.getInt(1);
                    if(bevSync <1) {
                    if (exclusionMapCache.containsKey(parent)) {
                        regionExclusionMap = exclusionMapCache.get(parent);
                    } else {
                        // we need to do a db lookup and add the region product exclusion to the cache
                        regionExclusionMap = new RegionExclusionMap(transconn, userId, parentLevel - 1, parent);
                        exclusionMapCache.put(parent, regionExclusionMap);
                    }
                    }
                    //regionExclusionMap = new RegionExclusionMap(transconn, userId, parentLevel - 1, rs.getInt(1));

                    if (productMapCache.containsKey(parent)) {
                        regionProductMap = productMapCache.get(parent);
                    } else {
                        // we need to do a db lookup and add the ingredients to the cache
                        regionProductMap = new RegionProductMap(transconn, userId, parentLevel, parent,locations);
                        productMapCache.put(parent, regionProductMap);
                    }
                    //regionProductMap = new RegionProductMap(transconn, userId, parentLevel, rs.getInt(1));

                    if (tapCount > 0) {
                        childTag.addElement("tapCount").addText(String.valueOf(tapCount));
                        tapCount = 0;
                    }
                    if (childTapCount > 0) {
                        childTag.addElement("childTapCount").addText(String.valueOf(childTapCount));
                        childTapCount = 0;
                    }
                    if (totalChildTapCount > 0) {
                        parentTag.addElement("totalChildTapCount").addText(String.valueOf(totalChildTapCount));
                        totalChildTapCount = 0;
                    }
                    if (parentTapCount > 0) {
                        parentTag.addElement("parentTapCount").addText(String.valueOf(parentTapCount));
                        parentTapCount = 0;
                    }
                    //NischaySharma_24-Aug-2009_Start
                    parentTag = toAppend.addElement("parent");
                    //NischaySharma_24-Aug-2009_End
                    parentTag.addElement("parentLevelId").addText(String.valueOf(parent));
                    parentTag.addElement("parentLevelName").addText(String.valueOf(parentLevelMap.getParentLevel(parent)));
                }

                if (child != rs.getInt(2)) {
                    if (tapCount > 0) {
                        childTag.addElement("tapCount").addText(String.valueOf(tapCount));
                        tapCount = 0;
                    }
                    if (childTapCount > 0) {
                        childTag.addElement("childTapCount").addText(String.valueOf(childTapCount));
                        childTapCount = 0;
                    }
                    child = rs.getInt(2);
                    childTag = parentTag.addElement("child");
                    childTag.addElement("childLevelId").addText(String.valueOf(rs.getInt(2)));
                    childTag.addElement("childLevelName").addText(String.valueOf(childLevelMap.getChildLevel(rs.getInt(2))));
                }
                if(bevSync > 0) {
                    tapCount += rs.getInt(4);
                    totalChildTapCount += rs.getInt(4);
                    totalTapCount += rs.getInt(4);
                    if (regionProductMap.hasProduct(rs.getInt(3))) {
                        childTapCount += rs.getInt(4);
                        parentTapCount += rs.getInt(4);
                        totalParentTapCount += rs.getInt(4);
                    }
                } else if (!regionExclusionMap.hasValue(rs.getInt(3))) {
                    tapCount += rs.getInt(4);
                    totalChildTapCount += rs.getInt(4);
                    totalTapCount += rs.getInt(4);
                    if (regionProductMap.hasProduct(rs.getInt(3))) {
                        childTapCount += rs.getInt(4);
                        parentTapCount += rs.getInt(4);
                        totalParentTapCount += rs.getInt(4);
                    }
                }
            }

            if (tapCount > 0) {
                childTag.addElement("tapCount").addText(String.valueOf(tapCount));
                tapCount = 0;
            }
            if (childTapCount > 0) {
                childTag.addElement("childTapCount").addText(String.valueOf(childTapCount));
                childTapCount = 0;
            }
            if (totalChildTapCount > 0) {
                parentTag.addElement("totalChildTapCount").addText(String.valueOf(totalChildTapCount));
                totalChildTapCount = 0;
            }
            if (parentTapCount > 0) {
                parentTag.addElement("parentTapCount").addText(String.valueOf(parentTapCount));
                parentTapCount = 0;
            }
            toAppend.addElement("totalTapCount").addText(String.valueOf(totalTapCount));
            toAppend.addElement("totalParentTapCount").addText(String.valueOf(totalParentTapCount));

            parentLevelMap = null;
            childLevelMap = null;
            regionProductMap = null;
            regionExclusionMap = null;
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(rs);
            close(stmt);
        }
    }
    
    
     private void getCampaignLogs(Element toHandle, Element toAppend) throws HandlerException {

        int campaign                        = HandlerUtils.getRequiredInteger(toHandle, "campaign");
        
        PreparedStatement stmt              = null;
        ResultSet rs                        = null;
        String selectCampaign               = "SELECT c.id,l.id,s.date From bevSyncCreatives c LEFT JOIN sponsorLogs s ON s.file=c.file"
                                            + " LEFT JOIN bevSyncCampaignCreatives bCR ON bCR.creatives=c.id LEFT JOIN bevSyncCampaign bSC ON bSC.id=bCR.campaign"
                                            + " LEFT JOIN bevSyncCampaignLocations bCL ON bCL.campaign=bSC.id LEFT JOIN location l ON l.id=s.location WHERE bCL.location=s.location AND bSC.id= ? AND s.date between bSC.start AND bSC.end;";
        
        

        try {
            stmt                            = transconn.prepareStatement(selectCampaign);
            stmt.setInt(1, campaign);
            rs                              = stmt.executeQuery();
            while(rs.next()) {
                Element logEl               = toAppend.addElement("campaignLog");
                logEl.addElement("creativeId").addText(String.valueOf(rs.getInt(1)));                                                
                logEl.addElement("locationId").addText(String.valueOf(rs.getInt(2)));                            
                logEl.addElement("date").addText(HandlerUtils.nullToEmpty(rs.getString(3))); 
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: "+sqle.toString());
            throw new HandlerException(sqle);
        }  finally {
            close(rs);
            close(stmt);
        }
    }
     
     private long[] getTimeDifference(Date d1, Date d2) {
        long[] result                       = new long[5];
        Calendar cal                        = Calendar.getInstance();
        cal.setTimeZone(TimeZone.getTimeZone("UTC"));
        cal.setTime(d1);

        long t1                             = cal.getTimeInMillis();
        cal.setTime(d2);

        long diff                           = Math.abs(cal.getTimeInMillis() - t1);
        final int ONE_DAY                   = 1000 * 60 * 60 * 24;
        final int ONE_HOUR                  = ONE_DAY / 24;
        final int ONE_MINUTE                = ONE_HOUR / 60;
        final int ONE_SECOND                = ONE_MINUTE / 60;

        long d                              = diff / ONE_DAY;
        diff                                %= ONE_DAY;

        long h                              = diff / ONE_HOUR;
        diff                                %= ONE_HOUR;

        long m                              = diff / ONE_MINUTE;
        diff                                %= ONE_MINUTE;

        long s                              = diff / ONE_SECOND;

        long ms                             = diff % ONE_SECOND;
        result[0]                           = d;
        result[1]                           = h;
        result[2]                           = m;
        result[3]                           = s;
        result[4]                           = ms;

        return result;
    }

     private void getBevSyncDashboardReport(Element toHandle, Element toAppend) throws HandlerException {

        int customerId                      = HandlerUtils.getOptionalInteger(toHandle, "customerId");   
        ReportType reportType               = ReportType.instanceOf("Yesterday");
        String reportTypeString             = HandlerUtils.getOptionalString(toHandle, "reportType");
        if (null != reportTypeString) {
            reportType                      = ReportType.instanceOf(HandlerUtils.getOptionalString(toHandle, "reportType"));
            if(HandlerUtils.getOptionalString(toHandle, "reportType").equals("SixMonth")) {
                reportType                  = ReportType.instanceOf("halfyearly");
            }
        }

        boolean shareTracker                = HandlerUtils.getOptionalBoolean(toHandle, "shareTracker");
        boolean pastCampaign                = HandlerUtils.getOptionalBoolean(toHandle, "pastCampaign");
        boolean currentCampaign             = HandlerUtils.getOptionalBoolean(toHandle, "currentCampaign");
        boolean topShare                    = HandlerUtils.getOptionalBoolean(toHandle, "topShare");
        boolean socialMedia                 = HandlerUtils.getOptionalBoolean(toHandle, "socialMedia");

        try {
            if (shareTracker) {
                getBevSyncDashboardData(reportType,customerId,toAppend,1);
            } else if(pastCampaign) {
                getBevSyncDashboardData(reportType,customerId,toAppend,2);
            } else if(currentCampaign) {
                getBevSyncDashboardData(reportType,customerId,toAppend,3);
            } else if(topShare) {
                getBevSyncDashboardData(reportType,customerId,toAppend,4);
            } else if(socialMedia) {
                getBevSyncDashboardData(reportType,customerId,toAppend,5);
            }

        } catch (Exception e) {
            logger.debug("Dashboard error: " + e.getMessage());
            throw new HandlerException(e);
        } finally {
           
        }
    }

     private void getBevSyncDashboardData(ReportType reportType,int customer,  Element toAppend,int type) throws HandlerException {
        
        String selectCampaign               = "SELECT MIN(ca.start), DATE_SUB(now(),INTERVAL 28 DAY), c.brewery FROM bevSyncCampaign ca LEFT JOIN bevSyncCustomer c ON c.id = ca.customer " +
                                            " WHERE customer = ? GROUP BY ca.customer";
        String selectBrewery                = "SELECT SUBDATE(NOW(), INTERVAL 29 DAY), SUBDATE(NOW(), INTERVAL 1 DAY), brewery FROM bevSyncCustomer WHERE id = ?";
        String selectProducts               = "SELECT p.id, p.name FROM product p LEFT JOIN productSetMap pSM ON pSM.product = p.id " +
                                            " LEFT JOIN productSet pS ON pS.id = pSM.productSet " +
                                            " WHERE pS.productSetType = 7 AND p.isActive = 1 AND pSM.productSet = ?";   
        String selectCreatives              = "SELECT cr.brewery,cr.product FROM bevSyncCreatives cr LEFT JOIN bevSyncCampaignCreatives bSC ON bSC.creatives = cr.id " +
                                            " WHERE bSC.campaign=?";
        String selectLocation               = "SELECT GROUP_CONCAT(b.location ORDER BY b.location SEPARATOR ',') FROM bevSyncCustomerLocationMap b LEFT JOIN locationDetails lD ON lD.location=b.location WHERE b.customer = ? AND lD.active = 1 AND lD.beerboard = 1; ";
        
        PreparedStatement stmt              = null;
        ResultSet rs                        = null, rsDetail = null, productRs =null;
        Calendar currentDate                = null;
        DateParameter validatedStartDate    = new DateParameter(reportType.toStartDate());
        DateParameter validatedEndDate      = new DateParameter(reportType.toEndDate());
        
        String startDate                    = null, endDate = null, sDate = null, eDate = null;
        String locations                    = "";
        Date start                          = null, end = null;
        int cType                           = 0;

        
        Map<Integer, Integer> productArray = new HashMap<Integer, Integer>();
        try {
            switch (type) {
                case 1:
                    stmt                    = transconn.prepareStatement(selectCampaign);
                    stmt.setInt(1, customer);
                    rs                      = stmt.executeQuery();
                    if(rs.next()) {
                        startDate           = rs.getString(1);
                        endDate             = rs.getString(2);

                        selectProducts      = "SELECT p.id, p.name FROM product p LEFT JOIN productSetMap pSM ON pSM.product = p.id " +
                                            " LEFT JOIN productSet pS ON pS.id = pSM.productSet " +
                                            " WHERE pS.productSetType = 7 AND p.isActive = 1 AND pSM.productSet IN ("+rs.getString(3) +")";
                        stmt                = transconn.prepareStatement(selectProducts);
                        productRs           = stmt.executeQuery();
                        int j               = 0;    
                        while(productRs.next()) {
                            productArray.put(j,productRs.getInt(1));                         
                            j++;
                        }
                        stmt                = transconn.prepareStatement("SELECT GROUP_CONCAT(DISTINCT bCL.location  ORDER BY bCL.location  SEPARATOR ', ') FROM bevSyncCampaignLocations bCL LEFT Join bevSyncCampaign c ON c.id=bCL.campaign WHERE c.Customer = ?");
                        stmt.setInt(1, customer);
                        rs                  = stmt.executeQuery();
                        if(rs.next()) {
                            locations       = rs.getString(1);
                        }
                        
                        stmt                       = transconn.prepareStatement(selectLocation);
                        stmt.setInt(1,customer);
                        rs                          = stmt.executeQuery();
                        if(rs.next()) {                            
                            locations       = rs.getString(1);  
                            if(locations==null || locations.equals("")){
                                locations   = "0";
                            }
                        } 
                        start               = dateFormat.parse(dateFormat.format(dbDateFormat.parse(startDate)));
                        currentDate         = Calendar.getInstance();
                        currentDate.setTime(start);
                        currentDate.add(Calendar.DATE,-28);
                        sDate               = dbDateFormat.format(currentDate.getTime());
                        //logger.debug("Start"+dbDateFormat.format(currentDate.getTime()));
                        end                 = dateFormat.parse(dateFormat.format(dbDateFormat.parse(endDate)));
                        currentDate.setTime(end);
                        currentDate.add(Calendar.DATE,28);
                        eDate               = dbDateFormat.format(currentDate.getTime());
                        //logger.debug("End"+dbDateFormat.format(currentDate.getTime()));
                    } else {
                         
                        stmt                = transconn.prepareStatement(selectBrewery);
                        stmt.setInt(1, customer);
                        rs                  = stmt.executeQuery();
                        if(rs.next()) {
                            startDate       = rs.getString(1);
                            endDate         = rs.getString(2);

                            selectProducts  = "SELECT p.id, p.name FROM product p LEFT JOIN productSetMap pSM ON pSM.product = p.id " +
                                            " LEFT JOIN productSet pS ON pS.id = pSM.productSet " +
                                            " WHERE pS.productSetType = 7 AND p.isActive = 1 AND pSM.productSet IN ("+rs.getString(3) +")";
                            
                            stmt            = transconn.prepareStatement(selectProducts);
                            productRs       = stmt.executeQuery();
                            int j           = 0;
                            while(productRs.next()) {
                                productArray.put(j,productRs.getInt(1));
                                j++;
                            }
                            stmt            = transconn.prepareStatement("SELECT GROUP_CONCAT(location ORDER BY location SEPARATOR ', ') FROM locationDetails WHERE active = 1 AND pouredUp = 1 AND beerboard = 1;");
                            rs              = stmt.executeQuery();
                            if(rs.next()) {
                                locations   = rs.getString(1);
                            }
                            
                            stmt                       = transconn.prepareStatement(selectLocation);
                            stmt.setInt(1,customer);
                            rs                          = stmt.executeQuery();
                            if(rs.next()) {                            
                                locations       = rs.getString(1);  
                                if(locations==null || locations.equals("")){
                                    locations   = "0";
                                }
                            } 
                            
                            start           = dateFormat.parse(dateFormat.format(dbDateFormat.parse(startDate)));
                            currentDate     = Calendar.getInstance();
                            currentDate.setTime(start);
                            sDate           = dbDateFormat.format(currentDate.getTime());
                            //logger.debug("Start" + dbDateFormat.format(currentDate.getTime()));
                            
                            end             = dateFormat.parse(dateFormat.format(dbDateFormat.parse(endDate)));
                            currentDate.setTime(end);
                            eDate           = dbDateFormat.format(currentDate.getTime());
                            //logger.debug("End" + dbDateFormat.format(currentDate.getTime()));
                        }
                    }
                    dashboardGraphGenerator(sDate, eDate, productArray, locations, toAppend, customer);
                    break;
                case 2:
                    stmt                    = transconn.prepareStatement("select title,start,end,id from bevSyncCampaign  WHERE Customer = ? AND end BETWEEN ? AND ?");
                    stmt.setInt(1, customer);
                    stmt.setString(2, validatedStartDate.toString());
                    stmt.setString(3, validatedEndDate.toString());
                    rs                      = stmt.executeQuery();
                    while(rs.next()) {
                        String CampaignName = rs.getString(1);
                        startDate           = rs.getString(2);
                        endDate             = rs.getString(3);
                        int campaign        = rs.getInt(4);
                        stmt                = transconn.prepareStatement("SELECT GROUP_CONCAT(DISTINCT bCL.location  ORDER BY bCL.location  SEPARATOR ', ') FROM bevSyncCampaignLocations bCL LEFT Join bevSyncCampaign c ON c.id=bCL.campaign WHERE c.id = ?");
                        stmt.setInt(1, campaign);
                        rsDetail            = stmt.executeQuery();
                        if(rsDetail.next()) { 
                            locations       = rsDetail.getString(1);
                        }
                        
                        stmt                = transconn.prepareStatement(selectCreatives);
                        stmt.setInt(1, campaign);
                        rsDetail            = stmt.executeQuery();
                        String products     = "0";
                        int j               = 0;    
                        while(rsDetail.next()) { 
                            int brewery     = rsDetail.getInt(1);
                            int product     = rsDetail.getInt(2);                                       
                            if(product > 0) {
                                productArray.put(j,product);
                                j++;
                                products    += String.valueOf(product);
                            } else {
                                stmt        = transconn.prepareStatement(selectProducts);
                                stmt.setInt(1, brewery);
                                productRs   = stmt.executeQuery();
                                while(productRs.next()) {
                                    productArray.put(j,productRs.getInt(1));  
                                    products+= ","+productRs.getInt(1);
                                    j++;
                                }
                            }
                        } 
                        
                        Element reportEl    = toAppend.addElement("pastCampaign");
                        reportEl.addElement("campaign").addText(String.valueOf(campaign));                
                        reportEl.addElement("startDate").addText(startDate);
                        reportEl.addElement("endDate").addText(endDate);
                        reportEl.addElement("campaignName").addText(CampaignName);
                        getDashboardPastCampaignGenerator(startDate,endDate,productArray,locations,reportEl,1,CampaignName);
                        
                        currentDate         = Calendar.getInstance();
                        start               = dateFormat.parse(dateFormat.format(dbDateFormat.parse(startDate)));
                        end                 = dateFormat.parse(dateFormat.format(dbDateFormat.parse(endDate)));
                        long diff[]         = getTimeDifference(start,end);
                       // logger.debug("Date Diff"+diff[0]);
                        currentDate.setTime(start);
                        currentDate.add(Calendar.DATE,(Integer.parseInt(String.valueOf(diff[0]+1))*-1));      
                        String previousStart= dbDateFormat.format(currentDate.getTime());
                        end                 = dateFormat.parse(dateFormat.format(dbDateFormat.parse(startDate)));
                        currentDate.setTime(end);
                        currentDate.add(Calendar.DATE,-1);                      
                        getDashboardPastCampaignGenerator(previousStart,dbDateFormat.format(currentDate.getTime()),productArray,locations,reportEl,2,CampaignName);    
                        
                        stmt                = transconn.prepareStatement("SELECT count(user) FROM bbtvMobileUserCheckin WHERE location IN("+locations+") AND date between ? AND ?;");
                        stmt.setString(1, startDate);
                        stmt.setString(2, endDate);
                        rsDetail            = stmt.executeQuery();
                        if(rsDetail.next()) { 
                            reportEl.addElement("checkin").addText(rsDetail.getString(1));
                        }
                        
                        stmt                = transconn.prepareStatement("SELECT count(id) FROM bbtvMobileUserRating WHERE location IN("+locations+") AND product IN("+products+") AND date between ? AND ?;;");
                        stmt.setString(1, startDate);
                        stmt.setString(2, endDate);
                        rsDetail            = stmt.executeQuery();
                        if(rsDetail.next()) { 
                            reportEl.addElement("likes").addText(rsDetail.getString(1));
                        }
                    }
                    break;
                case 3:
                    stmt                    = transconn.prepareStatement("select title,start,end,id from bevSyncCampaign  WHERE Customer = ? AND DATE(NOW()) BETWEEN start AND end");
                    stmt.setInt(1, customer);
                    rs                      = stmt.executeQuery();
                    while(rs.next()) {
                        String CampaignName = rs.getString(1);
                        startDate           = rs.getString(2);
                        endDate             = rs.getString(3);
                        int campaign        = rs.getInt(4);
                        stmt                = transconn.prepareStatement("SELECT GROUP_CONCAT(DISTINCT bCL.location  ORDER BY bCL.location  SEPARATOR ', ') FROM bevSyncCampaignLocations bCL LEFT Join bevSyncCampaign c ON c.id=bCL.campaign WHERE c.id = ?");
                        stmt.setInt(1, campaign);
                        rsDetail            = stmt.executeQuery();
                        if(rsDetail.next()) { 
                            locations       = rsDetail.getString(1);
                        }
                        
                        stmt                = transconn.prepareStatement(selectCreatives);
                        stmt.setInt(1, campaign);
                        rsDetail            = stmt.executeQuery();
                        int j               = 0;       
                        while(rsDetail.next()) { 
                            int brewery     = rsDetail.getInt(1);
                            int product     = rsDetail.getInt(2);                                    
                            if(product > 0) {
                                productArray.put(j,product);
                                j++;
                                
                            } else {
                                stmt        = transconn.prepareStatement(selectProducts);
                                stmt.setInt(1, brewery);
                                productRs   = stmt.executeQuery();
                                while(productRs.next()) {
                                    productArray.put(j,productRs.getInt(1));  
                                    
                                    j++;
                                }
                            }
                        } 
                        //logger.debug("Loc:"+locations);
                        getDashboardCurrentCampaignGenerator(startDate,endDate,productArray,locations,toAppend,1,CampaignName);
                        currentDate         = Calendar.getInstance();
                        start               = dateFormat.parse(dateFormat.format(dbDateFormat.parse(startDate)));
                        end                 = dateFormat.parse(dateFormat.format(dbDateFormat.parse(endDate)));
                        long diff[]         = getTimeDifference(start,end);
                        //logger.debug("Date Diff"+diff[0]);
                        currentDate.setTime(start);
                        currentDate.add(Calendar.DATE,(Integer.parseInt(String.valueOf(diff[0]+1))*-1));      
                        String previousStart= dbDateFormat.format(currentDate.getTime());
                        end                 = dateFormat.parse(dateFormat.format(dbDateFormat.parse(startDate)));
                        currentDate.setTime(end);
                        currentDate.add(Calendar.DATE,-1);      
                        getDashboardCurrentCampaignGenerator(previousStart,dbDateFormat.format(currentDate.getTime()),productArray,locations,toAppend,2,CampaignName);
                    }
                    break;
                case 4:
                    
                    stmt                       = transconn.prepareStatement(selectLocation);
                        stmt.setInt(1,customer);
                        rs                          = stmt.executeQuery();
                        if(rs.next()) {                            
                            locations       = rs.getString(1);  
                            if(locations==null || locations.equals("")){
                                locations   = "0";
                            }
                        } 
                    stmt                    = transconn.prepareStatement(selectBrewery);
                    stmt.setInt(1, customer);
                    rs                      = stmt.executeQuery();
                    if(rs.next()) {              
                        selectProducts      = "SELECT p.id, p.name FROM product p LEFT JOIN productSetMap pSM ON pSM.product = p.id "
                                            + " LEFT JOIN productSet pS ON pS.id = pSM.productSet " +
                                            " WHERE pS.productSetType = 7 AND p.isActive = 1 AND pSM.productSet IN ("+rs.getString(3) +")";   
                        stmt                = transconn.prepareStatement(selectProducts);
                        productRs           = stmt.executeQuery();
                        int j               = 0;    
                        while(productRs.next()) {
                            productArray.put(j,productRs.getInt(1));                         
                            j++;
                        }
                    }
                    getDashboardTopShareGenerator(validatedStartDate.toString(), validatedEndDate.toString(), productArray,locations, toAppend);
                    break;
                case 5:
                     stmt                       = transconn.prepareStatement(selectLocation);
                        stmt.setInt(1,customer);
                        rs                          = stmt.executeQuery();
                        if(rs.next()) {                            
                            locations       = rs.getString(1);  
                            if(locations==null || locations.equals("")){
                                locations   = "0";
                            }
                        } 
                    stmt                    = transconn.prepareStatement(selectBrewery);
                    String products         ="0";
                    stmt.setInt(1, customer);
                    rs                      = stmt.executeQuery();
                    if(rs.next()) {               
                        selectProducts      = "SELECT p.id, p.name FROM product p LEFT JOIN productSetMap pSM ON pSM.product = p.id "
                                            + " LEFT JOIN productSet pS ON pS.id = pSM.productSet " +
                                            " WHERE pS.productSetType = 7 AND p.isActive = 1 AND pSM.productSet IN ("+rs.getString(3) +")";
                        stmt                = transconn.prepareStatement(selectProducts);
                        productRs           = stmt.executeQuery();
                        while(productRs.next()) {                       
                            products        +=","+productRs.getInt(1);
                        }
                    }
                    getDashboardSocialMediaGenerator(validatedStartDate.toString(), validatedEndDate.toString(),locations, products, toAppend);
                
                    break;
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: "+sqle.toString());
            throw new HandlerException(sqle);
        } catch(Exception e) {
            logger.debug(e.getMessage());   
        }finally {
            close(rsDetail);
            close(rs);
            close(stmt);
        }
     }
     
     
     private void getDashboardPastCampaignGenerator( String startDate,String endDate,Map<Integer, Integer> productArray, String locations, Element reportEl,int type,String CampaignName) throws HandlerException {  
        
        PreparedStatement stmt              = null;
        ResultSet rs                        = null;
        DateParameter validatedStartDate    = new DateParameter(startDate);
        DateParameter validatedEndDate      = new DateParameter(endDate);

        if (!validatedStartDate.isValid()) {
            logger.debug("Aborted report, invalid start date '" + startDate + "'");           
        } else if (!validatedEndDate.isValid()) {
            logger.debug("Aborted report, invalid end date '" + endDate + "'");           
        }
            //String soldTable = "soldSummary";
            //logger.debug(soldTable);
            

            

            String pouredTable = "openHoursSummary";
            //logger.debug(pouredTable);
            String selectLocationsPoured = "SELECT  p.product, pr.name,SUM(p.value) FROM " + pouredTable + " p  LEFT JOIN product pr ON pr.id =p.product" +
                    " WHERE p.location IN ("+locations+") AND p.date BETWEEN DATE(?) AND DATE(?)  GROUP BY p.product "   ;
           
            String selectLocationsPouredSum = "SELECT  sum(p.value) FROM " + pouredTable + " p  WHERE p.location IN ("+locations+") AND p.date BETWEEN DATE(?) AND DATE(?) " ;
            

           

            try {
                double sum              = 0.0;
                double  campaign        = 0.0;
                stmt = transconn.prepareStatement(selectLocationsPouredSum);
                stmt.setString(1, validatedStartDate.toString());
                stmt.setString(2, validatedEndDate.toString());
                rs = stmt.executeQuery();
                if(rs.next()){
                    sum                 = rs.getDouble(1);
                }    
               // logger.debug("SUM:"+sum);           
                stmt = transconn.prepareStatement(selectLocationsPoured);
                stmt.setString(1, validatedStartDate.toString());
                stmt.setString(2, validatedEndDate.toString());
                rs = stmt.executeQuery();
                
                while(rs.next()) {                    
                    int productId           = rs.getInt(1);
                    if(productArray.containsValue(productId)) {
                    campaign +=rs.getDouble(3);    
                    } 
                }
                //logger.debug("Campaign Value:"+campaign); 
                if(campaign!=0&&sum!=0) {
                campaign    = (campaign/sum) *100;
                }
                if(type==1){
                reportEl.addElement("currentValue").addText(String.valueOf(campaign));                 
                } else {
                    reportEl.addElement("beforeValue").addText(String.valueOf(campaign));                 
                }
                
                
                
            } catch (SQLException sqle) {
                logger.debug(sqle.getMessage());
                throw new HandlerException(sqle);
                
            } finally {
                close(rs);
                close(stmt);
            }
    }
     
     
     private void getDashboardCurrentCampaignGenerator( String startDate,String endDate,Map<Integer, Integer> productArray, String locations, Element toAppend,int type,String CampaignName) throws HandlerException {  
        
        PreparedStatement stmt              = null;
        ResultSet rs                        = null, rsDetail = null;
        DateParameter validatedStartDate    = new DateParameter(startDate);
        DateParameter validatedEndDate      = new DateParameter(endDate);

        if (!validatedStartDate.isValid()) {
            logger.debug("Aborted report, invalid start date '" + startDate + "'");           
        } else if (!validatedEndDate.isValid()) {
            logger.debug("Aborted report, invalid end date '" + endDate + "'");           
        }
            //String soldTable = "soldSummary";
            //logger.debug(soldTable);
            

            

            String pouredTable = "openHoursSummary";
            //logger.debug(pouredTable);
            String selectLocationsPoured = "SELECT  p.product, pr.name,SUM(p.value),CONCAT(lo.boardname,'-',lo.name) FROM " + pouredTable + " p  LEFT JOIN product pr ON pr.id =p.product" +
                    "  LEFT JOIN location lo ON lo.id=p.location  WHERE p.location IN (?) AND p.date BETWEEN DATE(?) AND DATE(?)  GROUP BY p.product "   ;
           
            String selectLocationsPouredSum = "SELECT p.location, sum(p.value) FROM " + pouredTable + " p  WHERE p.location IN ("+locations+") AND p.date BETWEEN DATE(?) AND DATE(?) GROUP BY p.location " ;
            
            

           

            try {
                double sum              = 0.0;
                double  campaign        = 0.0;
                int location            = 0;
                stmt = transconn.prepareStatement(selectLocationsPouredSum);
                stmt.setString(1, validatedStartDate.toString());
                stmt.setString(2, validatedEndDate.toString());
                rs = stmt.executeQuery();
                while(rs.next()){
                    location            = rs.getInt(1);
                    sum                 = rs.getDouble(2);
                    
               // logger.debug("SUM:"+sum);           
                stmt = transconn.prepareStatement(selectLocationsPoured);
                stmt.setInt(1,location);
                stmt.setString(2, validatedStartDate.toString());
                stmt.setString(3, validatedEndDate.toString());
                rsDetail = stmt.executeQuery();
                String locationName         = "";
                campaign                    = 0.0;
                while(rsDetail.next()) {                    
                    int productId           = rsDetail.getInt(1);
                    if(productArray.containsValue(productId)) {
                    campaign +=rsDetail.getDouble(3);    
                    } 
                    locationName    = rsDetail.getString(4);
                    
                }
                //logger.debug("Campaign Value:"+campaign); 
                if(campaign!=0&&sum!=0) {
                campaign    = (campaign/sum) *100;
                }
                 Element reportEl        = toAppend.addElement("currentCampaign");
                reportEl.addElement("value").addText(String.valueOf(campaign));                 
                reportEl.addElement("type").addText(String.valueOf(type));
                reportEl.addElement("startDate").addText(startDate);
                reportEl.addElement("endDate").addText(endDate);
                reportEl.addElement("campaignName").addText(CampaignName);
                reportEl.addElement("locationName").addText(locationName);
                }
                
                
            } catch (SQLException sqle) {
                logger.debug(sqle.getMessage());
                throw new HandlerException(sqle);
                
            } finally {
                close(rsDetail);
                close(rs);
                close(stmt);
            }
    }
     
     
     private void dashboardGraphGenerator(String startDate, String endDate, Map<Integer, Integer> productArray, String locations, Element toAppend, int customer) throws HandlerException {
         
         PreparedStatement stmt              = null;
         ResultSet rs                        = null,rsDetail = null,rsCampaign=null;
         DateParameter validatedStartDate    = new DateParameter(startDate);
         DateParameter validatedEndDate      = new DateParameter(endDate);

         if (!validatedStartDate.isValid()) {
             logger.debug("Aborted report, invalid start date '" + startDate + "'");
         } else if (!validatedEndDate.isValid()) {
             logger.debug("Aborted report, invalid end date '" + endDate + "'");
         }
         
         String pouredTable                 = "openHoursSummary";
         //logger.debug(pouredTable);
         String selectLocationsPoured       = "SELECT  p.product, pr.name,SUM(p.value) FROM " + pouredTable + " p  LEFT JOIN product pr ON pr.id =p.product" +
                                            " WHERE p.location IN ("+locations+") AND p.date BETWEEN DATE(?) AND DATE(?)  GROUP BY p.product "   ;
           
        String selectLocationsPouredSum     = "SELECT p.date, sum(p.value) FROM " + pouredTable + " p  WHERE p.location IN ("+locations+") AND p.date BETWEEN DATE(?) AND DATE(?)  GROUP BY p.date" ;

        try {
            double sum                      = 0.0;
            double  campaign                = 0.0;
            double totalSum                 = 0.0;
            double  totalCampaign           = 0.0;
            stmt                            = transconn.prepareStatement(selectLocationsPouredSum);
            stmt.setString(1, validatedStartDate.toString());
            stmt.setString(2, validatedEndDate.toString());
            rs                              = stmt.executeQuery();
            while(rs.next()){
                String date                 = rs.getString(1);
                sum                         = rs.getDouble(2);
                totalSum                    += rs.getDouble(2);
                   
                //logger.debug("Date:"+date+" SUM:"+sum);           
                stmt                        = transconn.prepareStatement(selectLocationsPoured);
                stmt.setString(1, date);
                stmt.setString(2, date);
                rsDetail                    = stmt.executeQuery();
                campaign                    = 0.0;
                while(rsDetail.next()) {
                    int productId           = rsDetail.getInt(1);
                    if(productArray.containsValue(productId)) {
                        campaign            += rsDetail.getDouble(3);
                        totalCampaign       += rsDetail.getDouble(3);
                        //logger.debug("Product:"+rsDetail.getString(2)+" Value:"+campaign); 
                    } 
                }
                
                //logger.debug("Campaign Value:"+campaign); 
                if(sum!=0&& campaign!=0) {
                    campaign                = (campaign/sum) *100;
                }  else {
                    campaign                = 0;
                }
                Element reportEl            = toAppend.addElement("graph");
                reportEl.addElement("value").addText(String.valueOf(campaign)); 
                reportEl.addElement("date").addText(date);
                
                stmt                        = transconn.prepareStatement("SELECT id, type, title FROM bevSyncCampaign WHERE DATE(?) BETWEEN DATE(start) AND DATE(end) AND customer = ?");
                stmt.setString(1, date);
                stmt.setInt(2, customer);
                rsCampaign                  = stmt.executeQuery();
                boolean campaignFlag        = false;
                if(rsCampaign.next()) { 
                    campaignFlag            = true;                    
                    reportEl.addElement("type").addText(String.valueOf(rsCampaign.getInt(1)));
                    reportEl.addElement("title").addText(rsCampaign.getString(3));
                    //logger.debug("Date:"+date+"  campaign:"+rsCampaign.getString(3)); 
                }
                if(!campaignFlag){
                    //Element reportEl        = toAppend.addElement("graph");                
                    //.addElement("value").addText(String.valueOf(campaign)); 
                    //reportEl.addElement("date").addText(date);
                    reportEl.addElement("type").addText(String.valueOf(0));
                    reportEl.addElement("title").addText("Non-Campaign Period");
                }
            }

            boolean campaignFlag            = false;

            stmt                            = transconn.prepareStatement("SELECT id,type,title,start,end FROM bevSyncCampaign WHERE  customer = ?");
            stmt.setInt(1, customer);
            rsCampaign                      = stmt.executeQuery();
            while(rsCampaign.next()) {
                campaignFlag                = true;
                Element reportEl            = toAppend.addElement("details");
                reportEl.addElement("id").addText(String.valueOf(rsCampaign.getInt(1)));
                reportEl.addElement("title").addText(HandlerUtils.nullToEmpty(rsCampaign.getString(3)));
                reportEl.addElement("startDate").addText(HandlerUtils.nullToEmpty(rsCampaign.getString(4)));
                reportEl.addElement("endDate").addText(HandlerUtils.nullToEmpty(rsCampaign.getString(5)));
                getCampaignShare(rsCampaign.getInt(1),reportEl);
                
                
            }

            if(!campaignFlag){
                Element reportSubEl         = toAppend.addElement("details");
                reportSubEl.addElement("id").addText(String.valueOf(0));
                reportSubEl.addElement("title").addText("Non-Campaign Period");
                reportSubEl.addElement("startDate").addText(startDate);
                reportSubEl.addElement("endDate").addText(endDate);
                if(totalSum!=0&& totalCampaign!=0) {
                    totalCampaign           = (totalCampaign/totalSum) *100;
                }  else {
                    totalCampaign           = 0;
                }
                reportSubEl.addElement("value").addText(String.valueOf(totalCampaign));
                //getNonCampaignShare(locations, productArray, startDate, endDate, toAppend);
           }
        } catch (SQLException sqle) {
            logger.debug(sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(rsCampaign);
            close(rsDetail);
            close(rs);
            close(stmt);
        }
    }
     
     
     private void getCampaignShare(int campaign,Element toAppend) throws HandlerException {
        
        String selectCampaign               = "SELECT c.start,c.end, c.type FROM bevSyncCampaign c WHERE c.id= ?;";
        String selectProducts               = "SELECT p.id, p.name FROM product p LEFT JOIN productSetMap pSM ON pSM.product = p.id "
                                            + " LEFT JOIN productSet pS ON pS.id = pSM.productSet " +
                                            " WHERE pS.productSetType = 7 AND p.isActive = 1 AND pSM.productSet = ?";      
        String selectReward                 = "SELECT id FROM bevSyncCampaignReward WHERE  DATE(?) BETWEEN DATE(startTime) AND DATE(endTime) AND campaign = ?";
       

      
        PreparedStatement stmt              = null;
        ResultSet rs                        = null, locationRS = null, productRs =null;
        Calendar currentDate                = null;
        
        
       
        
        String startDate                    =null, endDate=null;        
        int product                         = 0, brewery=0, cType=0;
        Map<Integer, Integer> productArray  = new HashMap<Integer, Integer>();

        try {
            
            
            stmt                       = transconn.prepareStatement("SELECT  GROUP_CONCAT(location ORDER BY location SEPARATOR ', ') FROM bevSyncCampaignLocations  WHERE campaign = ?");
            stmt.setInt(1,campaign);
            String locations            = "";
            rs                          = stmt.executeQuery();
            if(rs.next()) {              
                locations       = rs.getString(1);  
                if(locations==null || locations.equals("")){
                    locations   = "0";
                }
            } 
             String selectCreatives              = "SELECT Distinct cr.brewery,cr.product from bevSyncCreatives cr LEFT JOIN bevSyncCampaignCreatives bSC ON bSC.creatives=cr.id"
                                            + " LEFT JOIN bevSyncCampaignLocations bSL ON bSL.campaign =bSC.campaign WHERE bSC.campaign=? AND bSL.location IN("+locations+");";
            stmt                = transconn.prepareStatement(selectCreatives);
            stmt.setInt(1, campaign);
            rs            = stmt.executeQuery();
            int j                      = 0;      
            while(rs.next()) { 
                brewery                    = rs.getInt(1);
                product                    = rs.getInt(2);
                          
                if(product > 0) {
                    productArray.put(j,product);
                    j++;
                } else {
                    stmt                    = transconn.prepareStatement(selectProducts);
                    stmt.setInt(1, brewery);
                    productRs              = stmt.executeQuery();
                    while(productRs.next()) {
                        productArray.put(j,productRs.getInt(1));                         
                        j++;
                    }
                }
            } 
           
            
             stmt                           = transconn.prepareStatement(selectCampaign);
             stmt.setInt(1,campaign);
             rs                      = stmt.executeQuery();
             if (rs.next()) { 
                 startDate                  = rs.getString(1);
                 endDate                    = rs.getString(2);                 
                 cType                      = rs.getInt(3);
               
                 
             } 
             
             
              String pouredTable = "openHoursSummary";
            //logger.debug(pouredTable);
            String selectLocationsPoured = "SELECT  p.product, pr.name,SUM(p.value) FROM " + pouredTable + " p  LEFT JOIN product pr ON pr.id =p.product" +
                    " WHERE p.location IN ("+locations+") AND p.date BETWEEN DATE(?) AND DATE(?)  GROUP BY p.product "   ;
           
            String selectLocationsPouredSum = "SELECT  sum(p.value) FROM " + pouredTable + " p  WHERE p.location IN ("+locations+") AND p.date BETWEEN DATE(?) AND DATE(?) " ;
            

                double sum              = 0.0;
                double  campaignValue        = 0.0;
                stmt = transconn.prepareStatement(selectLocationsPouredSum);
                stmt.setString(1, startDate);
                stmt.setString(2, endDate);
                rs = stmt.executeQuery();
                if(rs.next()){
                    sum                 = rs.getDouble(1);
                }    
               // logger.debug("SUM:"+sum);           
                stmt = transconn.prepareStatement(selectLocationsPoured);
                stmt.setString(1, startDate);
                stmt.setString(2, endDate);
                rs = stmt.executeQuery();
               
                while(rs.next()) {                    
                    int productId           = rs.getInt(1);
                    if(productArray.containsValue(productId)) {
                    campaignValue +=rs.getDouble(3);    
                    } 
                }
                //logger.debug("Campaign Value:"+campaign); 
                if(sum!=0&& campaignValue!=0) {
                campaignValue    = (campaignValue/sum) *100;
                }  else {
                    campaignValue = 0;
                }
                if(campaignValue >0) {                   
                toAppend.addElement("value").addText(String.valueOf(campaignValue)); 
               
                
                }
       } catch (SQLException sqle) {
            logger.debug(sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {            
            close(rs);
            close(stmt);
        }
    }
     
     
     private void getDashboardTopShareGenerator( String startDate,String endDate,Map<Integer, Integer> productArray, String locations, Element toAppend) throws HandlerException {  
        
        PreparedStatement stmt              = null;
        ResultSet rs                        = null, rsDetail = null;
        DateParameter validatedStartDate    = new DateParameter(startDate);
        DateParameter validatedEndDate      = new DateParameter(endDate);

        if (!validatedStartDate.isValid()) {
            logger.debug("Aborted report, invalid start date '" + startDate + "'");           
        } else if (!validatedEndDate.isValid()) {
            logger.debug("Aborted report, invalid end date '" + endDate + "'");           
        }
            //String soldTable = "soldSummary";
            //logger.debug(soldTable);
            toAppend.addElement("startDate").addText(startDate);
                toAppend.addElement("endDate").addText(endDate);               

            

           // String pouredTable = "pouredSummary";
              String pouredTable = "openHoursSummary";
            //logger.debug(pouredTable);
            String selectLocationsPoured = "SELECT  p.product, pr.name,SUM(p.value),CONCAT(lo.boardname,'-',lo.name) FROM " + pouredTable + " p  LEFT JOIN product pr ON pr.id =p.product" +
                    "  LEFT JOIN location lo ON lo.id=p.location  WHERE p.location IN (?) AND p.date BETWEEN DATE(?) AND DATE(?)  GROUP BY p.product "   ;
           
            String selectLocationsPouredSum = "SELECT p.location, sum(p.value) FROM " + pouredTable + " p  LEFT JOIN locationDetails lD ON lD.location=p.location WHERE lD.beerboard=1   AND p.date BETWEEN DATE(?) AND DATE(?) AND  p.location IN ("+locations+") GROUP BY p.location " ;
            
            

           

            try {
                double sum              = 0.0;
               
                int location            = 0;
                stmt = transconn.prepareStatement(selectLocationsPouredSum);
                stmt.setString(1, validatedStartDate.toString());
                stmt.setString(2, validatedEndDate.toString());
                rs = stmt.executeQuery();
                while(rs.next()){
                    location            = rs.getInt(1);
                    sum                 = rs.getDouble(2);
                    
                //logger.debug("SUM:"+sum);           
                stmt = transconn.prepareStatement(selectLocationsPoured);
                stmt.setInt(1,location);
                stmt.setString(2, validatedStartDate.toString());
                stmt.setString(3, validatedEndDate.toString());
                rsDetail = stmt.executeQuery();
                String locationName         = "";
                double sum1                 = 0;
                double  campaign           = 0.0;
                while(rsDetail.next()) {                    
                    int productId           = rsDetail.getInt(1);
                    if(productArray.containsValue(productId)) {
                    campaign +=rsDetail.getDouble(3);                         
                    } 
                    //logger.debug("Product:"+productId+" share:"+roundOff(rsDetail.getDouble(3)));
                    locationName            = rsDetail.getString(4);
                    sum1                    += rsDetail.getDouble(3);
                }
                //logger.debug("Location:"+locationName+" share:"+campaign);
                //logger.debug("Campaign Value:"+campaign); 
                if(campaign!=0&&sum1!=0) {
                campaign    = (campaign/sum1) *100;               
                Element reportEl        = toAppend.addElement("topShare");
                reportEl.addElement("value").addText(String.valueOf(campaign));                                                
                reportEl.addElement("locationName").addText(locationName);
                
                } else {
                    Element reportEl        = toAppend.addElement("topShare");
                    reportEl.addElement("value").addText(String.valueOf(0.0));                                                
                    reportEl.addElement("locationName").addText(locationName);
                    
                }
                }
                
                
            } catch (SQLException sqle) {
                logger.debug(sqle.getMessage());
                throw new HandlerException(sqle);
                
            } finally {
                close(rsDetail);
                close(rs);
                close(stmt);
            }
    }
     
     private double roundOff(double value) {
         BigDecimal a = new BigDecimal(value);
         BigDecimal roundOff = a.setScale(5, BigDecimal.ROUND_HALF_EVEN);
         return Double.parseDouble(roundOff.toString());
     }
     
     private void getDashboardSocialMediaGenerator(String startDate, String endDate,String locations, String  products,  Element toAppend) throws HandlerException {
        
        PreparedStatement stmt              = null;
        ResultSet rs                        = null, rsDetails = null;
        DateParameter validatedStartDate    = new DateParameter(startDate);
        DateParameter validatedEndDate      = new DateParameter(endDate);

        if (!validatedStartDate.isValid()) {
            logger.debug("Aborted report, invalid start date '" + startDate + "'");           
        } else if (!validatedEndDate.isValid()) {
            logger.debug("Aborted report, invalid end date '" + endDate + "'");           
        }
        toAppend.addElement("startDate").addText(startDate);
        toAppend.addElement("endDate").addText(endDate);     
        //logger.debug("product"+products);
        //logger.debug(startDate);
        //logger.debug(endDate);
        
        String selectProductsLikes          = "SELECT p.name,count(rating),p.id FROM bbtvMobileUserRating uR LEFT JOIN product p ON p.id = uR.product " +
                                            " LEFT JOIN locationDetails lD ON lD.location= uR.location WHERE lD.beerboard=1 AND uR.location IN ("+locations+") AND product IN (" + products + ") " +
                                            " AND date between ? AND ? GROUP BY uR.product;" ;
        String selectLocationCheckins       = "SELECT CONCAT(l.boardName,'-', l.name) Location, count(bMUR.id),l.id FROM bbtvMobileUserCheckin bMUR " +
                                            " LEFT JOIN location l ON l.id = bMUR.location LEFT JOIN locationDetails lD ON lD.location = bMUR.location " +
                                            " WHERE lD.beerboard = 1 AND bMUR.location IN ("+locations+") AND bMUR.date between ? AND ? GROUP BY bMUR.location ORDER BY count(bMUR.id) DESC, Location;" ;
        String selectCheckinDetails         = "SELECT u.username,CONCAT(DATE(c.date) , ' ',TIME(c.date) ) FROM bbtvMobileUserCheckin c LEFT JOIN bbtvMobileUser u ON u.id= c.user "
                                            + "WHERE c.location =? AND c.date BETWEEN ? AND  ?";
        String selectLikesDetails           = "SELECT u.username,CONCAT(l.boardName,'-', l.name),CONCAT(DATE(r.date) , ' ',TIME(r.date) ) FROM bbtvMobileUserRating r "
                                            + "LEFT JOIN location l ON l.id=r.location LEFT JOIN bbtvMobileUser u ON u.id=r.user "
                                            + "WHERE r.product =? AND r.date between ? AND  ? AND r.location IN ("+locations+")";

        try {
            stmt                            = transconn.prepareStatement(selectProductsLikes);
            stmt.setString(1, validatedStartDate.toString());
            stmt.setString(2, validatedEndDate.toString());
            rs = stmt.executeQuery();
            while(rs.next()){
                Element socialEl            = toAppend.addElement("socialMediaLikes");
                socialEl.addElement("product").addText(HandlerUtils.nullToEmpty(rs.getString(1))); 
                socialEl.addElement("count").addText(String.valueOf(rs.getInt(2)));
                stmt                            = transconn.prepareStatement(selectLikesDetails);
                stmt.setInt(1,rs.getInt(3) );
                stmt.setString(2, validatedStartDate.toString());
                stmt.setString(3, validatedEndDate.toString());
                rsDetails                   = stmt.executeQuery();
                 while(rsDetails.next()){
                     Element detailEl            = socialEl.addElement("details");
                     detailEl.addElement("user").addText(HandlerUtils.nullToEmpty(rsDetails.getString(1)));
                     detailEl.addElement("location").addText(HandlerUtils.nullToEmpty(rsDetails.getString(2)));
                     detailEl.addElement("date").addText(HandlerUtils.nullToEmpty(rsDetails.getString(3)));
                 }
            }

            stmt                            = transconn.prepareStatement(selectLocationCheckins);
            stmt.setString(1, validatedStartDate.toString());
            stmt.setString(2, validatedEndDate.toString());
            rs                              = stmt.executeQuery();
            while(rs.next()){
                Element socialEl            = toAppend.addElement("socialMediaCheckins");
                socialEl.addElement("location").addText(HandlerUtils.nullToEmpty(rs.getString(1)));
                socialEl.addElement("count").addText(String.valueOf(rs.getInt(2)));
                stmt                            = transconn.prepareStatement(selectCheckinDetails);
                stmt.setInt(1,rs.getInt(3) );
                stmt.setString(2, validatedStartDate.toString());
                stmt.setString(3, validatedEndDate.toString());
                rsDetails                   = stmt.executeQuery();
                 while(rsDetails.next()){
                     Element detailEl            = socialEl.addElement("details");
                     detailEl.addElement("user").addText(HandlerUtils.nullToEmpty(rsDetails.getString(1)));
                     detailEl.addElement("date").addText(HandlerUtils.nullToEmpty(rsDetails.getString(2)));
                     
                 }
            }
        } catch (SQLException sqle) {
            logger.debug(sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(rsDetails);
            close(rs);
            close(stmt);
        }
    }


     private void resendPushMessage(Element toHandle, Element toAppend) throws HandlerException {

         int messageId                      = HandlerUtils.getRequiredInteger(toHandle, "messageId");
         String userString                  = HandlerUtils.getOptionalString(toHandle, "userString");

         PreparedStatement stmt             = null;
         ResultSet rs                       = null, rsDetails = null;

         double latitude                    = 0.0,longitude = 0;
         int lastUser                       = 0;
         String logo                        = "", validity = "";
         
         String selectPushMessage           = "SELECT p.location, p.message, (SELECT endTime from bevSyncCampaignReward WHERE id=p.reward) from pushMessage p WHERE p.id = ?;";
         String selectLastUser              = "SELECT user FROM pushMessageMap WHERE message = ? ORDER BY user DESC LIMIT 1;";
         String selectLatLon                = "SELECT latitude,longitude, (SELECT logo from locationGraphics WHERE location = l.id) FROM location l WHERE l.id = ?";
         String selectUserByLoc             = "SELECT id, platform, deviceToken, username, (6371 * acos( cos( radians(?) ) * cos( radians( latitude ) ) * cos( radians( ?) - radians(longitude) ) + sin( radians(?) ) * sin( radians(latitude) ) )) AS distant"
                                            + " FROM bbtvMobileUser WHERE id > ? AND LENGTH(deviceToken) > 4 " + (userString.length() > 0 ? " AND id in (" + userString + ")" : "") +
                                            " HAVING distant < 33 ORDER BY id";
         try {

            stmt                            = transconn.prepareStatement(selectPushMessage);
            stmt.setInt(1, messageId);
            rs                              = stmt.executeQuery();
            if(rs.next()){
                int location                = rs.getInt(1);
                String message              = rs.getString(2);
                validity                    = HandlerUtils.nullToEmpty(rs.getString(3));
                
                stmt                        = transconn.prepareStatement(selectLastUser);
                stmt.setInt(1, messageId);
                rs                          = stmt.executeQuery();
                if(rs.next()){
                    lastUser                = rs.getInt(1);
                }

                stmt                        = transconn.prepareStatement(selectLatLon);
                stmt.setInt(1,location);
                rs                          = stmt.executeQuery();
                if(rs.next()){
                    latitude                = rs.getDouble(1);
                    longitude               = rs.getDouble(2);
                    logo                    = HandlerUtils.nullToEmpty(rs.getString(3));

                    stmt                    = transconn.prepareStatement(selectUserByLoc);
                    stmt.setDouble(1, latitude);
                    stmt.setDouble(2, longitude);
                    stmt.setDouble(3, latitude);
                    stmt.setInt(4, lastUser);
                    rs                      = stmt.executeQuery();
                    while(rs.next()){
                        int user            = rs.getInt(1);
                        int platform        = rs.getInt(2);
                        String deviceToken  = rs.getString(3);
                        switch (platform) {
                            case 2:
                                Sender sender
                                            = new Sender("AIzaSyDjrsCeKvnCWTTVr73bFwhVVdBibOaEfxs");
                                Message pushMessage
                                            = new Message.Builder()
                                        .delayWhileIdle(false) // Wait for device to become active before sending.
                                        .addData( "message", message)
                                        .addData( "location", String.valueOf(location) )
                                        .addData( "messageId", String.valueOf(messageId) )
                                        .addData( "logo", logo )
                                        .addData( "type", String.valueOf(1) )
                                        .addData( "validity", validity )
                                        .build();
                                Result result
                                            = sender.send(pushMessage, deviceToken, 1);

                                 stmt       = transconn.prepareStatement("INSERT INTO pushMessageMap (user, message) VALUES (?,?);");
                                 stmt.setInt(1,user);
                                 stmt.setInt(2,messageId);
                                 stmt.executeUpdate();
                                //logger.debug(result.toString());
                                break;
                            case 3:
                                PushNotificationPayload simplePayLoad
                                            = new PushNotificationPayload();
                                simplePayLoad.addAlert(message);
                                simplePayLoad.addBadge(1);
                                simplePayLoad.addSound("default");
                                simplePayLoad.addCustomDictionary("location", location);
                                simplePayLoad.addCustomDictionary("messageId", messageId);
                                simplePayLoad.addCustomDictionary("type", 1);
                                simplePayLoad.addCustomDictionary("logo", logo);
                                simplePayLoad.addCustomDictionary("validity", validity);
                                Push.payload(simplePayLoad, "/home/midware/Push Notification/usbnapnNew.p12", "usbn", true, deviceToken);
                               // Push.payload(simplePayLoad, "/home/midware/Push Notification/usbnapnDev.p12", "usbn", false, deviceToken);
                                 stmt       = transconn.prepareStatement("INSERT INTO pushMessageMap (user, message) VALUES (?,?);");
                                 stmt.setInt(1,user);
                                 stmt.setInt(2,messageId);
                                 stmt.executeUpdate();
                                break;
                         }
                    }
                }
            }
    	  } catch (Exception e) {
    		  e.printStackTrace();
    	  } finally {
            close(rsDetails);
            close(rs);
            close(stmt);
        }
     }
     
     
     private void sendPushMessage(int messageId, int location, String message,int type, String validity) throws HandlerException {
         
         PreparedStatement stmt             = null;
         ResultSet rs                       = null, rsDetails = null;
         double latitude                    = 0.0,longitude = 0;
         String selectUser                  = "SELECT u.id,platform,u.deviceToken FROM bbtvMobileUser u WHERE LENGTH(u.deviceToken) > 4";
         String selectLatLon                = "SELECT latitude,longitude, (SELECT logo from locationGraphics WHERE location =l.id) FROM location l WHERE l.id = ?";
         String selectUserByLoc             = "SELECT id, platform, deviceToken, username, (6371 * acos( cos( radians(?) ) * cos( radians( latitude ) ) * cos( radians( ?) - radians(longitude) ) + sin( radians(?) ) * sin( radians(latitude) ) )) AS distant"
                                            + " FROM bbtvMobileUser WHERE LENGTH(deviceToken) > 4 HAVING distant < 33";
        try {
            String logo                     = "";
            stmt                            = transconn.prepareStatement(selectLatLon); 
            stmt.setInt(1,location);            
            rs = stmt.executeQuery();
            if(rs.next()){
                latitude                    = rs.getDouble(1);
                longitude                   = rs.getDouble(2);
                logo                        = HandlerUtils.nullToEmpty(rs.getString(3));
            }
            
            stmt                            = transconn.prepareStatement(selectUser);    
            //stmt.setDouble(1, latitude);
            //stmt.setDouble(2, longitude);
            //stmt.setDouble(3, latitude);
            rs = stmt.executeQuery();
            while(rs.next()){
                int user                    = rs.getInt(1);
                int platform                = rs.getInt(2);
                String deviceToken          = rs.getString(3);                
                 switch (platform) {
                case 2:
                    Sender sender = new Sender("AIzaSyDjrsCeKvnCWTTVr73bFwhVVdBibOaEfxs");
                     //Sender sender = new Sender("AIzaSyB9h-gJ2k4vQ9iv9fFJ0VbS0w7luCJV0HY");
                    //Message message = new Message.Builder().addData("message", "Test Suba For Usbn").build();
                    Message pushMessage     = new Message.Builder()
                            .delayWhileIdle(false) // Wait for device to become active before sending.
                            .addData( "message", message)
                            .addData( "location", String.valueOf(location) )
                            .addData( "messageId", String.valueOf(messageId) )
                            .addData( "logo", logo )
                            .addData( "type", String.valueOf(1) )
                            .addData( "validity", validity )
                            .build();
                    Result result           = sender.send(pushMessage, deviceToken, 1);
                    logger.debug(result.toString());
                    break;
                case 3:
                    PushNotificationPayload simplePayLoad
                                            = new PushNotificationPayload();
                    simplePayLoad.addAlert(message);
                    simplePayLoad.addBadge(1);
                    simplePayLoad.addSound("default");
                    simplePayLoad.addCustomDictionary("location", location);
                    simplePayLoad.addCustomDictionary("messageId", messageId);
                    simplePayLoad.addCustomDictionary("type", 1);
                    simplePayLoad.addCustomDictionary("logo", logo);
                    simplePayLoad.addCustomDictionary("validity", validity); 
                    Push.payload(simplePayLoad, "/home/midware/Push Notification/usbnapn.p12", "usbn", true, deviceToken);
                   // Push.payload(simplePayLoad, "/home/midware/Push Notification/usbnapnDev.p12", "usbn", false, deviceToken);
                    break;
                
            }
           stmt                            = transconn.prepareStatement("INSERT INTO pushMessageMap (user,message) VALUES (?,?);");
           stmt.setInt(1,user);
           stmt.setInt(2,messageId);
           stmt.executeUpdate();
               
            }
            
            
    	  }catch (Exception e) {
    		  e.printStackTrace();
    	  }  finally {
            close(rsDetails);
            close(rs);
            close(stmt);
        }
     }
     
     private void addUpdateDeleteBBTVGraphics(Element toHandle, Element toAppend) throws HandlerException {

        int callerId                        = getCallerId(toHandle);
        int locationId                      = HandlerUtils.getRequiredInteger(toHandle, "locationId");
        ResultSet rs                        = null;
        PreparedStatement stmt              = null;
        Calendar currentDate                = Calendar.getInstance();

        try {

           

            Iterator addLocationSpecials  = toHandle.elementIterator("addSpecialsGraphics");
            while (addLocationSpecials.hasNext()) {
                Element locationSpecials  = (Element) addLocationSpecials.next();
                int type                    = HandlerUtils.getRequiredInteger(locationSpecials, "type");                
                int textsize                = HandlerUtils.getRequiredInteger(locationSpecials, "textsize");
                String bgcolor                 = HandlerUtils.getOptionalString(locationSpecials, "bgcolor");
                String bgimage              = HandlerUtils.getOptionalString(locationSpecials, "bgimage");
                String textcolor              = HandlerUtils.getOptionalString(locationSpecials, "textcolor");                
                String font                 = HandlerUtils.getOptionalString(locationSpecials, "font");
                

                String getLastId            = " SELECT LAST_INSERT_ID()";

                stmt                        = transconn.prepareStatement("INSERT INTO bbtvSpecialsGraphics (location, bgcolor, bgimage, textcolor, textsize,font) VALUES (?, ?, ?, ?, ?,?)");
                stmt.setInt(1, locationId);
                stmt.setString(2, bgcolor);
                stmt.setString(3, "");
                stmt.setString(4, textcolor);
                stmt.setInt(5, textsize);
                stmt.setString(6, font);
                stmt.executeUpdate();

                stmt                        = transconn.prepareStatement(getLastId);
                rs                          = stmt.executeQuery();
                if (rs.next()) {
                    int id                  = rs.getInt(1);                    
                } else {
                    logger.dbError("first call to LAST_INSERT_ID in addBBTVSpecialsGraphics failed to return a result");
                    throw new HandlerException("Database Error");
                }
            }

            Iterator updateLocationSpecials
                                            = toHandle.elementIterator("updateSpecialsGraphics");
            while (updateLocationSpecials.hasNext()) {
                Element locationSpecials    = (Element) updateLocationSpecials.next();
                int id                      = HandlerUtils.getRequiredInteger(locationSpecials, "id");                
                int textsize                = HandlerUtils.getRequiredInteger(locationSpecials, "textsize");
                String bgcolor                 = HandlerUtils.getOptionalString(locationSpecials, "bgcolor");
                String bgimage              = HandlerUtils.getOptionalString(locationSpecials, "bgimage");
                String textcolor              = HandlerUtils.getOptionalString(locationSpecials, "textcolor");                
                String font                 = HandlerUtils.getOptionalString(locationSpecials, "font");

                String updateAccountEmail   = " UPDATE bbtvSpecialsGraphics SET bgcolor = ?, bgimage = ?, textcolor = ?, textsize = ? ,font = ? WHERE id = ? ";

                stmt                        = transconn.prepareStatement(updateAccountEmail);
                stmt.setString(1, bgcolor);
                stmt.setString(2, "");
                stmt.setString(3, textcolor);
                stmt.setInt(4, textsize);
                stmt.setString(5, font);
                stmt.setInt(6, id);
                stmt.executeUpdate();
                String logMessage           = "Updated bbtv Specials Graphics '" + id + "'";                
            }

            Iterator deleteLocationSpecials
                                            = toHandle.elementIterator("deleteSpecialsGraphics");
            while (deleteLocationSpecials.hasNext()) {
                Element locationSpecials    = (Element) deleteLocationSpecials.next();
                int id                      = HandlerUtils.getRequiredInteger(locationSpecials, "id");

                String delete               = " DELETE FROM bbtvSpecialsGraphics WHERE id = ? ";

                stmt                        = transconn.prepareStatement(delete);
                stmt.setInt(1,id);
                stmt.executeUpdate();

                String logMessage           = "Delete bbtvSpecialsGraphics '" + id + "'";
                logger.portalDetail(callerId, "DeletebbtvSpecialsGraphics", 0, "accountEmailMap", id, logMessage, transconn);
            }
            
            
            Iterator addLocationMenu        = toHandle.elementIterator("addMenuGraphics");
            while (addLocationMenu.hasNext()) {
                Element locationMenu    = (Element) addLocationMenu.next();
                int type                    = HandlerUtils.getRequiredInteger(locationMenu, "type");                
                int textsize                = HandlerUtils.getRequiredInteger(locationMenu, "textsize");
                String bgcolor              = HandlerUtils.getOptionalString(locationMenu, "bgcolor");
                String bgimage              = HandlerUtils.getOptionalString(locationMenu, "bgimage");
                String textcolor            = HandlerUtils.getOptionalString(locationMenu, "textcolor");                
                String font                 = HandlerUtils.getOptionalString(locationMenu, "font");
                int animation               = HandlerUtils.getOptionalInteger(locationMenu, "animation");                
                

                String getLastId            = " SELECT LAST_INSERT_ID()";

                stmt                        = transconn.prepareStatement("INSERT INTO bbtvMenuGraphics (location,type, bgcolor, bgimage, textcolor, textsize,font,animation) VALUES (?, ?, ?, ?, ?,?,?,?)");
                stmt.setInt(1, locationId);
                stmt.setInt(2, type);
                stmt.setString(3, bgcolor);
                stmt.setString(4, bgimage);
                stmt.setString(5, textcolor);
                stmt.setInt(6, textsize);
                stmt.setString(7, font);
                stmt.setInt(8, animation);
                stmt.executeUpdate();

                stmt                        = transconn.prepareStatement(getLastId);
                rs                          = stmt.executeQuery();
                if (rs.next()) {
                    int id                  = rs.getInt(1);                    
                } else {
                    logger.dbError("first call to LAST_INSERT_ID in addBBTVMenuGraphics failed to return a result");
                    throw new HandlerException("Database Error");
                }
            }

            Iterator updateLocationMenu
                                            = toHandle.elementIterator("updateMenuGraphics");
            while (updateLocationMenu.hasNext()) {
                Element locationMenu        = (Element) updateLocationMenu.next();
                int id                      = HandlerUtils.getRequiredInteger(locationMenu, "id");                
               int type                    = HandlerUtils.getRequiredInteger(locationMenu, "type");                
                int textsize                = HandlerUtils.getRequiredInteger(locationMenu, "textsize");
                String bgcolor              = HandlerUtils.getOptionalString(locationMenu, "bgcolor");
                String bgimage              = HandlerUtils.getOptionalString(locationMenu, "bgimage");
                String textcolor            = HandlerUtils.getOptionalString(locationMenu, "textcolor");                
                String font                 = HandlerUtils.getOptionalString(locationMenu, "font");
                int animation               = HandlerUtils.getOptionalInteger(locationMenu, "animation");    

                String updateAccountEmail   = " UPDATE bbtvMenuGraphics SET bgcolor = ?, bgimage = ?, textcolor = ?, textsize = ? ,font = ?,type=?,animation=? WHERE id = ? ";

                stmt                        = transconn.prepareStatement(updateAccountEmail);
                stmt.setString(1, bgcolor);
                stmt.setString(2, bgimage);
                stmt.setString(3, textcolor);
                stmt.setInt(4, textsize);
                stmt.setString(5, font);
                stmt.setInt(6, type);
                stmt.setInt(7, animation);
                stmt.setInt(8, id);
                stmt.executeUpdate();
                String logMessage           = "Updated bbtv Menu Graphics '" + id + "'";                
            }

            Iterator deleteLocationMenu
                                            = toHandle.elementIterator("deleteMenuGraphics");
            while (deleteLocationMenu.hasNext()) {
                Element locationMenu        = (Element) deleteLocationMenu.next();
                int id                      = HandlerUtils.getRequiredInteger(locationMenu, "id");

                String delete               = " DELETE FROM bbtvMenuGraphics WHERE id = ? ";

                stmt                        = transconn.prepareStatement(delete);
                stmt.setInt(1,id);
                stmt.executeUpdate();

                String logMessage           = "Delete bbtvMenuGraphics '" + id + "'";
                logger.portalDetail(callerId, "DeletebbtvSpecialsGraphics", 0, "accountEmailMap", id, logMessage, transconn);
            }
            
            
             Iterator addLocationPage       = toHandle.elementIterator("addPageGraphics");
            while (addLocationPage.hasNext()) {
                Element locationMenu    = (Element) addLocationPage.next();
                int type                    = HandlerUtils.getRequiredInteger(locationMenu, "type");                
                int textsize                = HandlerUtils.getRequiredInteger(locationMenu, "textsize");
                String bgcolor              = HandlerUtils.getOptionalString(locationMenu, "bgcolor");
                String bgimage              = HandlerUtils.getOptionalString(locationMenu, "bgimage");
                String textcolor            = HandlerUtils.getOptionalString(locationMenu, "textcolor");                
                String font                 = HandlerUtils.getOptionalString(locationMenu, "font");                
                String startTime            = HandlerUtils.getOptionalString(locationMenu, "startTime");                
                String endTime              = HandlerUtils.getOptionalString(locationMenu, "endTime");                
                

                String getLastId            = " SELECT LAST_INSERT_ID()";

                stmt                        = transconn.prepareStatement("INSERT INTO bbtvPageGraphics (location,type, bgcolor, bgimage, textcolor, textsize,font,startTime,endTime) VALUES (?, ?, ?, ?, ?,?,?,?,?)");
                stmt.setInt(1, locationId);
                stmt.setInt(2, type);
                stmt.setString(3, bgcolor);
                stmt.setString(4, bgimage);
                stmt.setString(5, textcolor);
                stmt.setInt(6, textsize);
                stmt.setString(7, font);  
                stmt.setString(8, startTime);
                stmt.setString(9, endTime);
                stmt.executeUpdate();

                stmt                        = transconn.prepareStatement(getLastId);
                rs                          = stmt.executeQuery();
                if (rs.next()) {
                    int id                  = rs.getInt(1);                    
                } else {
                    logger.dbError("first call to LAST_INSERT_ID in addBBTVPageGraphics failed to return a result");
                    throw new HandlerException("Database Error");
                }
            }

            Iterator updateLocationPage
                                            = toHandle.elementIterator("updatePageGraphics");
            while (updateLocationPage.hasNext()) {
                Element locationMenu        = (Element) updateLocationPage.next();
                int id                      = HandlerUtils.getRequiredInteger(locationMenu, "id");                
               int type                    = HandlerUtils.getRequiredInteger(locationMenu, "type");                
                int textsize                = HandlerUtils.getRequiredInteger(locationMenu, "textsize");
                String bgcolor              = HandlerUtils.getOptionalString(locationMenu, "bgcolor");
                String bgimage              = HandlerUtils.getOptionalString(locationMenu, "bgimage");
                String textcolor            = HandlerUtils.getOptionalString(locationMenu, "textcolor");                
                String font                 = HandlerUtils.getOptionalString(locationMenu, "font");
                String startTime            = HandlerUtils.getOptionalString(locationMenu, "startTime");                
                String endTime              = HandlerUtils.getOptionalString(locationMenu, "endTime");                


                String updateAccountEmail   = " UPDATE bbtvPageGraphics SET bgcolor = ?, bgimage = ?, textcolor = ?, textsize = ? ,font = ?,type=?,startTime=?,endTime=? WHERE id = ? ";

                stmt                        = transconn.prepareStatement(updateAccountEmail);
                stmt.setString(1, bgcolor);
                stmt.setString(2, bgimage);
                stmt.setString(3, textcolor);
                stmt.setInt(4, textsize);
                stmt.setString(5, font);
                stmt.setInt(6, type);
                stmt.setString(7, startTime);
                stmt.setString(8, endTime);
                stmt.setInt(9, id);
                stmt.executeUpdate();
                String logMessage           = "Updated bbtv Page Graphics '" + id + "'";                
            }

            Iterator deleteLocationPage
                                            = toHandle.elementIterator("deletePageGraphics");
            while (deleteLocationPage.hasNext()) {
                Element locationPage  = (Element) deleteLocationPage.next();
                int id                      = HandlerUtils.getRequiredInteger(locationPage, "id");

                String delete               = " DELETE FROM bbtvPageGraphics WHERE id = ? ";

                stmt                        = transconn.prepareStatement(delete);
                stmt.setInt(1,id);
                stmt.executeUpdate();

                String logMessage           = "Delete bbtvMenuGraphics '" + id + "'";
                logger.portalDetail(callerId, "DeletebbtvSpecialsGraphics", 0, "accountEmailMap", id, logMessage, transconn);
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: "+sqle.toString());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
            close(rs);
        }
    }
     
     private void getBBTVGraphics(Element toHandle, Element toAppend) throws HandlerException {

        int location                        = HandlerUtils.getRequiredInteger(toHandle, "locationId");
        PreparedStatement stmt              = null;
        ResultSet rs                        = null;
        
        String selectLocationSpecials       = "SELECT id, bgcolor, bgimage, textcolor, textsize,font FROM bbtvSpecialsGraphics WHERE location = ?";
        String selectLocationMenu           = "SELECT g.id, g.type, g.bgcolor, g.bgimage, g.textcolor, g.textsize,g.font,g.animation,t.name FROM bbtvMenuGraphics g LEFT JOIN bbtvPageType t"
                                            + " ON t.id=g.type WHERE location =? ;";
        String selectLocationPage           = "SELECT g.id, g.type, g.bgcolor, g.bgimage, g.textcolor, g.textsize,g.font,t.name,g.startTime,g.endTime FROM bbtvPageGraphics g LEFT JOIN bbtvPageType t ON"
                                            + " t.id=g.type WHERE location = ?;";
        String selectMenuType               = "SELECT id, name FROM bbtvPageType WHERE type = 1";
        String selectPageType               = "SELECT id, name FROM bbtvPageType WHERE type >1 and type !=2";

        try {
            
            stmt                            = transconn.prepareStatement(selectLocationSpecials);
            stmt.setInt(1,location);
            rs                              = stmt.executeQuery();          
            if (rs.next()) {               
                Element locationSpecialsEl  = toAppend.addElement("specialsGraphics");
                locationSpecialsEl.addElement("id").addText(String.valueOf(rs.getInt(1)));                    
                locationSpecialsEl.addElement("bgcolor").addText(HandlerUtils.nullToEmpty(rs.getString(2)));
                //locationSpecialsEl.addElement("bgimage").addText(HandlerUtils.nullToEmpty(rs.getString(3)));
                locationSpecialsEl.addElement("textcolor").addText(HandlerUtils.nullToEmpty(rs.getString(4)));
                locationSpecialsEl.addElement("textsize").addText(String.valueOf(rs.getInt(5)));                    
                locationSpecialsEl.addElement("font").addText(HandlerUtils.nullToEmpty(rs.getString(6)));
               // locationSpecialsEl.addElement("bgimagePath").addText(HandlerUtils.nullToEmpty("http://beerboard.tv/USBN.BeerBoard.UI/Images/Customers/"+ String.valueOf(location) + "/" + rs.getString(3).trim().replaceAll(" ", "%20")));
                locationSpecialsEl.addElement("fontPath").addText("http://beerboard.tv/USBN.BeerBoard.UI/Fonts/"+ String.valueOf(location) + "/" + HandlerUtils.nullToEmpty(rs.getString(6)).trim().replaceAll(" ", "%20"));
                
            }else{
                stmt                            = transconn.prepareStatement(selectLocationSpecials);
                stmt.setInt(1,0);
                rs                              = stmt.executeQuery();         
                if (rs.next()) {               
                Element locationSpecialsEl  = toAppend.addElement("specialsGraphics");
                locationSpecialsEl.addElement("id").addText(String.valueOf(0));                    
                locationSpecialsEl.addElement("bgcolor").addText(HandlerUtils.nullToEmpty(rs.getString(2)));
                //locationSpecialsEl.addElement("bgimage").addText(HandlerUtils.nullToEmpty(rs.getString(3)));
                locationSpecialsEl.addElement("textcolor").addText(HandlerUtils.nullToEmpty(rs.getString(4)));
                locationSpecialsEl.addElement("textsize").addText(String.valueOf(rs.getInt(5)));                    
                locationSpecialsEl.addElement("font").addText(HandlerUtils.nullToEmpty(rs.getString(6)));
                //locationSpecialsEl.addElement("bgimagePath").addText(HandlerUtils.nullToEmpty("http://beerboard.tv/USBN.BeerBoard.UI/Images/Customers/"+ String.valueOf(location) + "/" + rs.getString(3).trim().replaceAll(" ", "%20")));
                locationSpecialsEl.addElement("fontPath").addText("http://beerboard.tv/USBN.BeerBoard.UI/Fonts/" + HandlerUtils.nullToEmpty(rs.getString(6)).trim().replaceAll(" ", "%20"));
                
            }
            
                
            }
            
            
            stmt                            = transconn.prepareStatement(selectLocationMenu);
            stmt.setInt(1,location);
            rs                              = stmt.executeQuery();
            
            if(!rs.next()){
                stmt                            = transconn.prepareStatement(selectLocationMenu);
                stmt.setInt(1,0);
                rs                              = stmt.executeQuery();            
                while (rs.next()) {
                    
                      stmt                        = transconn.prepareStatement("INSERT INTO bbtvMenuGraphics (location,type, bgcolor, bgimage, textcolor, textsize,font,animation) VALUES (?, ?, ?, ?, ?,?,?,?)");
                      stmt.setInt(1, location);
                       
                stmt.setInt(2, rs.getInt(2));
                stmt.setString(3, HandlerUtils.nullToEmpty(rs.getString(3)));
                stmt.setString(4, HandlerUtils.nullToEmpty(rs.getString(4)));
                stmt.setString(5, HandlerUtils.nullToEmpty(rs.getString(5)));
                stmt.setInt(6, rs.getInt(6));
                stmt.setString(7, HandlerUtils.nullToEmpty(rs.getString(7)));  
                stmt.setInt(8, rs.getInt(8));               
                stmt.executeUpdate();
            }
            }
            
            stmt                            = transconn.prepareStatement(selectLocationMenu);
            stmt.setInt(1,location);
            rs                              = stmt.executeQuery();            
            while (rs.next()) {              
                Element locationGraphicsEl  = toAppend.addElement("menuGraphics");
                locationGraphicsEl.addElement("id").addText(String.valueOf(rs.getInt(1)));                    
                locationGraphicsEl.addElement("type").addText(String.valueOf(rs.getInt(2)));                    
                locationGraphicsEl.addElement("bgcolor").addText(HandlerUtils.nullToEmpty(rs.getString(3)));
                locationGraphicsEl.addElement("bgimage").addText(HandlerUtils.nullToEmpty(rs.getString(4)));
                locationGraphicsEl.addElement("textcolor").addText(HandlerUtils.nullToEmpty(rs.getString(5)));
                locationGraphicsEl.addElement("textsize").addText(String.valueOf(rs.getInt(6)));                    
                locationGraphicsEl.addElement("font").addText(HandlerUtils.nullToEmpty(rs.getString(7)));                
                locationGraphicsEl.addElement("animation").addText(String.valueOf(rs.getInt(8)));                    
                locationGraphicsEl.addElement("bgimagePath").addText(HandlerUtils.nullToEmpty("http://beerboard.tv/USBN.BeerBoard.UI/Images/Customers/"+ String.valueOf(location) + "/" + HandlerUtils.nullToEmpty(rs.getString(4)).trim().replaceAll(" ", "%20")));
                locationGraphicsEl.addElement("fontPath").addText(HandlerUtils.nullToEmpty("http://beerboard.tv/USBN.BeerBoard.UI/Images/Customers/"+ String.valueOf(location) + "/" + HandlerUtils.nullToEmpty(rs.getString(7)).trim().replaceAll(" ", "%20")));
                logger.debug("location Menu Graphics"+rs.getString(7));
                locationGraphicsEl.addElement("typeName").addText(HandlerUtils.nullToEmpty(rs.getString(9)));
            }
            
          
              
            
            
            stmt                            = transconn.prepareStatement(selectLocationPage);
            stmt.setInt(1,location);
            rs                              = stmt.executeQuery();
             if(!rs.next()) {
                stmt                            = transconn.prepareStatement(selectLocationPage);
                stmt.setInt(1,0);
                rs                              = stmt.executeQuery();
           
            while (rs.next()) {
                stmt                        = transconn.prepareStatement("INSERT INTO bbtvPageGraphics (location,type, bgcolor, bgimage, textcolor, textsize,font,startTime,endTime) VALUES (?, ?, ?, ?, ?,?,?,?,?)");
                stmt.setInt(1, location);
                stmt.setInt(2, rs.getInt(2));
                stmt.setString(3, HandlerUtils.nullToEmpty(rs.getString(3)));
                stmt.setString(4, HandlerUtils.nullToEmpty(rs.getString(4)));
                stmt.setString(5, HandlerUtils.nullToEmpty(rs.getString(5)));
                stmt.setInt(6, rs.getInt(6));
                stmt.setString(7, HandlerUtils.nullToEmpty(rs.getString(7)));  
                stmt.setString(8, HandlerUtils.nullToEmpty(rs.getString(9)));
                stmt.setString(9, HandlerUtils.nullToEmpty(rs.getString(10)));
                stmt.executeUpdate();
               
               
            }
            }
            stmt                            = transconn.prepareStatement(selectLocationPage);
            stmt.setInt(1,location);
            rs                              = stmt.executeQuery();
            while (rs.next()) {               
                Element locationGraphicsEl  = toAppend.addElement("pageGraphics");
                locationGraphicsEl.addElement("id").addText(String.valueOf(rs.getInt(1)));                    
                locationGraphicsEl.addElement("type").addText(String.valueOf(rs.getInt(2)));                    
                locationGraphicsEl.addElement("bgcolor").addText(HandlerUtils.nullToEmpty(rs.getString(3)));
                locationGraphicsEl.addElement("bgimage").addText(HandlerUtils.nullToEmpty(rs.getString(4)));
                locationGraphicsEl.addElement("textcolor").addText(HandlerUtils.nullToEmpty(rs.getString(5)));
                locationGraphicsEl.addElement("textsize").addText(String.valueOf(rs.getInt(6)));                    
                locationGraphicsEl.addElement("font").addText(HandlerUtils.nullToEmpty(rs.getString(7)));                
                locationGraphicsEl.addElement("bgimagePath").addText("http://beerboard.tv/USBN.BeerBoard.UI/Images/Customers/"+ String.valueOf(location) + "/" + HandlerUtils.nullToEmpty(rs.getString(4)).trim().replaceAll(" ", "%20"));
                locationGraphicsEl.addElement("fontPath").addText("http://beerboard.tv/USBN.BeerBoard.UI/Fonts/"+ HandlerUtils.nullToEmpty(rs.getString(7)).trim().replaceAll(" ", "%20"));
                locationGraphicsEl.addElement("typeName").addText(HandlerUtils.nullToEmpty(rs.getString(8)));
                locationGraphicsEl.addElement("startTime").addText(HandlerUtils.nullToEmpty(rs.getString(9)));
                locationGraphicsEl.addElement("endTime").addText(HandlerUtils.nullToEmpty(rs.getString(10)));
            }
            
           
            logger.debug("location Page Graphics");
            
            
            
           
        } catch (SQLException sqle) {
            logger.dbError("Database error: "+sqle.toString());
            logger.debug("Graphics Error:"+sqle.getMessage());
            throw new HandlerException(sqle);
            
        } finally {
            close(rs);
            close(stmt);
        }
    }
     
     
      private void getBBTVSchedule(Element toHandle, Element toAppend) throws HandlerException {

        int location                        = HandlerUtils.getRequiredInteger(toHandle, "locationId");
        PreparedStatement stmt              = null;
        ResultSet rs                        = null;
        
        
        String selectPageType               = "SELECT Distinct type,pageName FROM bbtvPageType  ORDER BY type;";
        String selectPageSchedule           = "SELECT id,sequence, type FROM bbtvSchedule WHERE location = ? ORDER BY sequence";

        try {
           
            
            stmt                            = transconn.prepareStatement(selectPageType);            
            rs                              = stmt.executeQuery();
            while (rs.next()) {
                Element locationSpecialsEl  = toAppend.addElement("pageType");
                locationSpecialsEl.addElement("id").addText(String.valueOf(rs.getInt(1)));                    
                locationSpecialsEl.addElement("name").addText(HandlerUtils.nullToEmpty(rs.getString(2)));
            }
            
            stmt                            = transconn.prepareStatement(selectPageSchedule);
            stmt.setInt(1,location);
            rs                              = stmt.executeQuery();
            boolean schedule                = false;
            while (rs.next()) {
                schedule                    = true;
                Element locationScheduleEl  = toAppend.addElement("pageSchedule");
                locationScheduleEl.addElement("id").addText(String.valueOf(rs.getInt(1)));                    
                locationScheduleEl.addElement("sequence").addText(String.valueOf(rs.getInt(2)));                    
                locationScheduleEl.addElement("type").addText(String.valueOf(rs.getInt(3)));                                    
                
            }
            
            if(!schedule){
                stmt                            = transconn.prepareStatement(selectPageSchedule);
            stmt.setInt(1,0);
            rs                              = stmt.executeQuery();            
            while (rs.next()) {                
                Element locationScheduleEl  = toAppend.addElement("pageSchedule");
                locationScheduleEl.addElement("id").addText(String.valueOf(0));                    
                locationScheduleEl.addElement("sequence").addText(String.valueOf(rs.getInt(2)));                    
                locationScheduleEl.addElement("type").addText(String.valueOf(rs.getInt(3)));                                    
                
            }
            }
           
        } catch (SQLException sqle) {
            logger.dbError("Database error: "+sqle.toString());
            throw new HandlerException(sqle);
        } finally {
            close(rs);
            close(stmt);
        }
    }
      
      
      private void addUpdateDeleteBBTVSchedule(Element toHandle, Element toAppend) throws HandlerException {

        int callerId                        = getCallerId(toHandle);
        int locationId                      = HandlerUtils.getRequiredInteger(toHandle, "locationId");
        ResultSet rs                        = null;
        PreparedStatement stmt              = null;
        Calendar currentDate                = Calendar.getInstance();
        String updateUserTVAccess           = "UPDATE locationBeerBoardMap SET schedule = 1 WHERE location = ? ";

        try {

            boolean updateSchedule          = false;
           

            Iterator addLocationSpecials    = toHandle.elementIterator("addBBTVSchedule");
            while (addLocationSpecials.hasNext()) {
                updateSchedule              = true;
                Element locationSpecials    = (Element) addLocationSpecials.next();
                int type                    = HandlerUtils.getRequiredInteger(locationSpecials, "type");                
                int sequence                = HandlerUtils.getRequiredInteger(locationSpecials, "sequence");
                String getLastId            = " SELECT LAST_INSERT_ID()";

                stmt                        = transconn.prepareStatement("INSERT INTO bbtvSchedule (location, type, sequence) VALUES (?, ?, ?)");
                stmt.setInt(1, locationId);
                stmt.setInt(2, type);
                stmt.setInt(3, sequence);                
                stmt.executeUpdate();

                stmt                        = transconn.prepareStatement(getLastId);
                rs                          = stmt.executeQuery();
                if (rs.next()) {
                    int id                  = rs.getInt(1);                    
                } else {
                    logger.dbError("first call to LAST_INSERT_ID in addBBTVSchedule failed to return a result");
                    throw new HandlerException("Database Error");
                }
            }

            Iterator updateLocationSpecials
                                            = toHandle.elementIterator("updateBBTVSchedule");
            while (updateLocationSpecials.hasNext()) {
                updateSchedule              = true;
                Element locationSpecials    = (Element) updateLocationSpecials.next();
                int id                      = HandlerUtils.getRequiredInteger(locationSpecials, "id");
                int type                    = HandlerUtils.getRequiredInteger(locationSpecials, "type");                
                int sequence                = HandlerUtils.getRequiredInteger(locationSpecials, "sequence");                

                String updateAccountEmail   = " UPDATE bbtvSchedule SET type = ?, sequence = ? WHERE id = ? ";

                stmt                        = transconn.prepareStatement(updateAccountEmail);
                stmt.setInt(1, type);
                stmt.setInt(2, sequence);                
                stmt.setInt(3, id);
                stmt.executeUpdate();
                String logMessage           = "Updated bbtv Schedule '" + id + "'";                
            }

            Iterator deleteLocationSpecials
                                            = toHandle.elementIterator("deleteBBTVSchedule");
            while (deleteLocationSpecials.hasNext()) {
                updateSchedule              = true;
                Element locationSpecials    = (Element) deleteLocationSpecials.next();
                int id                      = HandlerUtils.getRequiredInteger(locationSpecials, "id");

                String delete               = " DELETE FROM bbtvSchedule WHERE id = ? ";

                stmt                        = transconn.prepareStatement(delete);
                stmt.setInt(1,id);
                stmt.executeUpdate();

                String logMessage           = "Delete bbtvSchedule '" + id + "'";
                logger.portalDetail(callerId, "DeletebbtvSchedule", 0, "accountEmailMap", id, logMessage, transconn);
            }

            if (updateSchedule) {
                stmt                        = transconn.prepareStatement(updateUserTVAccess);
                stmt.setInt(1, locationId);
                stmt.executeUpdate();
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: "+sqle.toString());
            throw new HandlerException(sqle);
        } catch(Exception e){
             logger.debug("ERROR:"+e.getMessage());
            
        }finally {
            close(stmt);
            close(rs);
        }
    }
      
      
       private void getBeerBoardMobileAppReport(Element toHandle, Element toAppend) throws HandlerException {
           
           DateParameter validatedStartDate = null;
           DateParameter validatedEndDate = null;
           ReportType reportType               = ReportType.instanceOf("week");
           String reportTypeString             = HandlerUtils.getOptionalString(toHandle, "reportType");
        if (null != reportTypeString) {
            reportType                      = ReportType.instanceOf(HandlerUtils.getOptionalString(toHandle, "reportType"));
            if(HandlerUtils.getOptionalString(toHandle, "reportType").equals("SixMonth")) {
                reportType                  = ReportType.instanceOf("halfyearly");
                validatedStartDate    = new DateParameter(reportType.toStartDate());
                validatedEndDate      = new DateParameter(reportType.toEndDate());
            }else if(HandlerUtils.getOptionalString(toHandle, "reportType").equals("Custom")) {
                String start                = HandlerUtils.getRequiredString(toHandle, "startDate");
                String end                  = HandlerUtils.getRequiredString(toHandle, "endDate");
                validatedStartDate    = new DateParameter(start);
                validatedEndDate      = new DateParameter(end);
            } else {
                validatedStartDate    = new DateParameter(reportType.toStartDate());
                validatedEndDate      = new DateParameter(reportType.toEndDate());
                
            }
           
        }
        int bevSyncCustomer                 = HandlerUtils.getOptionalInteger(toHandle, "customerId");
        
        int locationId                      = HandlerUtils.getOptionalInteger(toHandle, "locationId");

        boolean newDownloads                = HandlerUtils.getOptionalBoolean(toHandle, "newDownloads");
        boolean userActivity                = HandlerUtils.getOptionalBoolean(toHandle, "userActivity");
        boolean userCheckin                 = HandlerUtils.getOptionalBoolean(toHandle, "userCheckin");
        boolean userLikes                   = HandlerUtils.getOptionalBoolean(toHandle, "userLikes");  
        boolean chart                       = HandlerUtils.getOptionalBoolean(toHandle, "chart");  
        boolean bevManager                  = HandlerUtils.getOptionalBoolean(toHandle, "bevManager");  

        try {
            if (newDownloads) {
                getMobileAppData(bevManager,bevSyncCustomer,locationId,validatedStartDate.toString(),validatedEndDate.toString(),chart,toAppend,1);
            } else if(userActivity) {
                getMobileAppData(bevManager,bevSyncCustomer,locationId,validatedStartDate.toString(),validatedEndDate.toString(),chart,toAppend,2);
            } else if(userCheckin) {
                getMobileAppData(bevManager,bevSyncCustomer,locationId,validatedStartDate.toString(),validatedEndDate.toString(),chart,toAppend,3);
            } else if(userLikes) {
                getMobileAppData(bevManager,bevSyncCustomer,locationId,validatedStartDate.toString(),validatedEndDate.toString(),chart,toAppend,4);
            } 

        } catch (Exception e) {
            logger.debug("Dashboard error: " + e.getMessage());
            throw new HandlerException(e);
        } finally {
           
        }
    }
       
       
       private void getMobileAppData(boolean bevManager,int bevSyncCustomer, int locationId,String start,String end,boolean chart,  Element toAppend,int type) throws HandlerException {
        
        PreparedStatement stmt              = null;
        ResultSet rs                        = null,rsDetails =null,productRs =null;
        Calendar currentDate                = null;
        DateParameter validatedStartDate    = new DateParameter(start);
        DateParameter validatedEndDate      = new DateParameter(end);
        String locationCondition1           =" ",locationCondition2="";
        String products                     = "0";
        String productCondition1            = " ";
        String productCondition2            = " ";
        try {
            if(bevManager){
                if(locationId>0){
                    locationCondition2      = " AND l.id = " + locationId;
                    locationCondition1      = " AND uR.location = " + locationId;
                } else if(bevSyncCustomer>0){
                    String selectLocations  = "SELECT GROUP_CONCAT(l.id ORDER BY l.id SEPARATOR ',') FROM location l LEFT JOIN locationDetails lD ON lD.location=l.location WHERE l.customer = ? AND lD.active = 1 ;";
                    stmt                    = transconn.prepareStatement(selectLocations);
                    stmt.setInt(1,bevSyncCustomer);
                    rs                           = stmt.executeQuery();
                    if(rs.next()) {
                        if(rs.getString(1)!=null &&!rs.getString(1).equals("")) {                        
                            locationCondition2
                                            =" AND l.id IN("+rs.getString(1)+")";
                            locationCondition1   
                                            =" AND uR.location IN("+rs.getString(1)+")";
                        } else {
                            locationCondition2  
                                            =" AND l.id IN(0)";
                            locationCondition1 
                                            =" AND uR.location IN(0)";
                        }
                    }
                }
            } else {
                String selectbrewery        = "SELECT  brewery FROM bevSyncCustomer WHERE id = ? ";
                stmt                        = transconn.prepareStatement(selectbrewery);
                stmt.setInt(1,bevSyncCustomer);
                rs                          = stmt.executeQuery();
                if(rs.next()) {
                    String selectProducts   = "SELECT p.id, p.name FROM product p LEFT JOIN productSetMap pSM ON pSM.product = p.id "
                                            + " LEFT JOIN productSet pS ON pS.id = pSM.productSet " +
                                            " WHERE pS.productSetType = 7 AND p.isActive = 1 AND pSM.productSet IN ("+rs.getString(1) +")";
                    stmt                    = transconn.prepareStatement(selectProducts);
                    productRs               = stmt.executeQuery();
                    while(productRs.next()) {                       
                        products            +=","+productRs.getInt(1);
                    }
                }
                productCondition1           = " AND uR.product IN (" + products + ") ";
                productCondition2           = " AND bMUR.product IN (" + products + ")  ";
                
                if(locationId>0){
                    locationCondition2      = " AND l.id = " + locationId;
                    locationCondition1      = " AND uR.location = " + locationId;
                } else if(bevSyncCustomer>0){
                    String selectCreatives  = "SELECT GROUP_CONCAT(b.location ORDER BY b.location SEPARATOR ',') FROM bevSyncCustomerLocationMap b LEFT JOIN locationDetails lD ON lD.location=b.location WHERE b.customer = ? AND lD.active = 1 AND lD.beerboard = 1;";
                    stmt                    = transconn.prepareStatement(selectCreatives);
                    stmt.setInt(1,bevSyncCustomer);
                    rs                      = stmt.executeQuery();
                    if(rs.next()) {
                        if(rs.getString(1)!=null &&!rs.getString(1).equals("")) {                        
                            locationCondition2   
                                            =" AND l.id IN("+rs.getString(1)+")";
                            locationCondition1    
                                            =" AND uR.location IN("+rs.getString(1)+")";
                            
                        } else {
                            locationCondition2     
                                            =" AND l.id IN(0)";
                            locationCondition1
                                            =" AND uR.location IN(0)";
                        }
                    }
                }
            }
        
        String selectLikesGraph             = "SELECT DATE(uR.date),count(uR.id) FROM bbtvMobileUserRating uR WHERE uR.date between ? AND ? "+locationCondition1+" "+productCondition1+ " group by DATE(uR.date)";
        String selectCheckinGraph           = "SELECT DATE(uR.date),count(uR.id) FROM bbtvMobileUserCheckin uR WHERE uR.date between ? AND ? "+locationCondition1+" group by DATE(uR.date)";
        String selectAccessGraph            = "SELECT DATE(uR.lastAccess),count(uR.id) FROM bbtvMobileUsage uR WHERE uR.lastAccess between ? AND ? "+locationCondition1+" group by DATE(uR.lastAccess);";
        String selectProductsLikes          = "SELECT p.name,count(rating),p.id FROM bbtvMobileUserRating uR LEFT JOIN product p ON p.id = uR.product " +
                                            " LEFT JOIN locationDetails lD ON lD.location= uR.location WHERE lD.beerboard=1 " +
                                            " AND date between ? AND ? "+locationCondition1+" "+productCondition1+ " GROUP BY uR.product;" ;
        String selectLocationCheckins       = "SELECT CONCAT(l.boardName,'-', l.name) Location, count(bMUR.id),l.id FROM bbtvMobileUserCheckin bMUR " +
                                            " LEFT JOIN location l ON l.id = bMUR.location LEFT JOIN locationDetails lD ON lD.location = bMUR.location " +
                                            " WHERE lD.beerboard = 1 AND bMUR.date between ? AND ? "+locationCondition2+" GROUP BY bMUR.location ORDER BY count(bMUR.id) DESC, Location;" ;
        String selectMaxLocCheckin          ="SELECT 1,CONCAT(l.boardName,'-', l.name) Location, count(bMUR.id) FROM bbtvMobileUserCheckin bMUR"
                                            + " LEFT JOIN location l ON l.id = bMUR.location LEFT JOIN locationDetails lD ON lD.location = bMUR.location"
                                            + " WHERE lD.beerboard = 1 AND bMUR.date between ? AND ? "+locationCondition2+" GROUP BY bMUR.location ORDER BY count(bMUR.id) DESC, Location LIMIT 3;";
        String selectCheckinDetails         = "SELECT CONCAT(u.username, ' - ', IF(u.platform=2 ,'Android','IOS')),CONCAT(DATE(c.date) , ' ',TIME(c.date) ) FROM bbtvMobileUserCheckin c LEFT JOIN bbtvMobileUser u ON u.id= c.user "
                                            + "WHERE c.location =? AND c.date BETWEEN ? AND  ?";
        String selectMaxUserCheckin         ="SELECT 2,u.username,count(u.id) ucount FROM bbtvMobileUserCheckin uR LEFT JOIN bbtvMobileUser u ON u.id= uR.user"
                                            + " WHERE  uR.date BETWEEN ? AND ? AND u.username <> 'null' "+locationCondition1+" GROUP BY u.id ORDER BY ucount DESC LIMIT 3;";
        String selectLikesDetails           = "SELECT CONCAT(u.username, ' - ', IF(u.platform=2 ,'Android','IOS')),CONCAT(l.boardName,'-', l.name),CONCAT(DATE(r.date) , ' ',TIME(r.date) ) FROM bbtvMobileUserRating r "
                                            + "LEFT JOIN location l ON l.id=r.location LEFT JOIN bbtvMobileUser u ON u.id=r.user "
                                            + "WHERE r.product =? AND r.date between ? AND  ? "+locationCondition2;
        String selectLoginActivity          = "SELECT CONCAT(u.username, ' - ', IF(u.platform=2 ,'Android','IOS')),CONCAT(l.boardName,'-', l.name) Location,CONCAT(DATE(b.lastAccess) , ' ',TIME(b.lastAccess) ),IF(u.platform=2 ,'Android','IOS') FROM bbtvMobileUsage b LEFT JOIN bbtvMobileUser u ON u.id=b.user"
                                            + " LEFT JOIN location l ON l.id=b.location WHERE b.lastAccess between ? AND ? AND u.platform IN(2,3) "+locationCondition2;
        String selectNewDownloads           = "SELECT IF(platform=2 ,'Android','IOS'), '', DATE(registration), count(id),platform FROM bbtvMobileUser WHERE registration BETWEEN ? AND ? AND platform IN(2,3) GROUP BY DATE(registration)";
        String selectNewDownloadsChart      = "SELECT DATE(registration), count(id) FROM bbtvMobileUser WHERE registration BETWEEN ? AND ? AND platform IN(2,3) GROUP BY DATE(registration)";
        String selectNewDownloadsDetails    = "SELECT username, DATE(registration),iF(platform=2 ,'Android','IOS') FROM bbtvMobileUser WHERE DATE(registration) = ? AND platform =? ";
        
        String selectMaxLocLikes           ="SELECT 1,CONCAT(l.boardName,'-', l.name) Location, count(bMUR.id) FROM bbtvMobileUserRating bMUR"
                                            + " LEFT JOIN location l ON l.id = bMUR.location LEFT JOIN locationDetails lD ON lD.location = bMUR.location"
                                            + " WHERE lD.beerboard = 1 AND bMUR.date between ? AND ? "+locationCondition2+" "+productCondition2+ " GROUP BY bMUR.location ORDER BY count(bMUR.id) DESC, Location LIMIT 3;";
        String selectMaxUserLikes           ="SELECT 2,u.username,count(u.id) ucount FROM bbtvMobileUserRating uR LEFT JOIN bbtvMobileUser u ON u.id= uR.user"
                                            + " WHERE  uR.date BETWEEN ? AND ? AND u.username <> 'null' "+locationCondition1 +" "+productCondition1+ "  GROUP BY u.id ORDER BY ucount DESC LIMIT 3;";
        String selectMaxProductLikes        ="SELECT 3,p.name,count(rating) prating FROM bbtvMobileUserRating uR LEFT JOIN product p ON p.id = uR.product  LEFT JOIN locationDetails lD ON lD.location= uR.location WHERE lD.beerboard=1"
                                            + " AND date between ? AND ? "+locationCondition1+" "+productCondition1+ "  GROUP BY uR.product ORDER BY prating DESC LIMIT 3;";
        String selectMaxLocAccess           ="SELECT 1,CONCAT(l.boardName,'-', l.name) Location,uR.location,count(uR.id) lcount FROM bbtvMobileUsage uR LEFT JOIN location l"
                                            + " ON  l.id = uR.location  WHERE uR.lastAccess between ? AND ? "+locationCondition1+" group by uR.location ORDER BY lcount DESC LIMIT 3;";
        String selectMaxUserAccess          ="SELECT 2,l.username,uR.location,count(uR.id) lcount FROM bbtvMobileUsage uR LEFT JOIN bbtvMobileUser l"
                                            + " ON  l.id = uR.user  WHERE uR.lastAccess between ? AND ? AND l.username <> 'null' "+locationCondition1+" group by uR.user ORDER BY lcount DESC LIMIT 3;";
        
      
            switch (type) {
                case 1:
                    if(chart){
                        stmt                    = transconn.prepareStatement(selectNewDownloadsChart);
                         stmt.setString(1, validatedStartDate.toString());
                         stmt.setString(2, validatedEndDate.toString());
                         rs                      = stmt.executeQuery();
                         while(rs.next()) {
                          Element mobileChartEl  = toAppend.addElement("chart");
                          mobileChartEl.addElement("date").addText(HandlerUtils.nullToEmpty(rs.getString(1)));                    
                          mobileChartEl.addElement("count").addText(String.valueOf(rs.getInt(2)));                    
                        }
                          
                    } else {
                        
                        stmt                    = transconn.prepareStatement(selectNewDownloads);
                         stmt.setString(1, validatedStartDate.toString());
                         stmt.setString(2, validatedEndDate.toString());
                         rs                      = stmt.executeQuery();
                         while(rs.next()) {
                          Element mobileChartEl  = toAppend.addElement("newDownloads");
                          mobileChartEl.addElement("date").addText(HandlerUtils.nullToEmpty(rs.getString(3)));                    
                          mobileChartEl.addElement("count").addText(String.valueOf(rs.getInt(4)));                    
                          mobileChartEl.addElement("version").addText(HandlerUtils.nullToEmpty(rs.getString(2)));                    
                          mobileChartEl.addElement("platform").addText(HandlerUtils.nullToEmpty(rs.getString(1)));
                          stmt                            = transconn.prepareStatement(selectNewDownloadsDetails);
                          stmt.setString(1, HandlerUtils.nullToEmpty(rs.getString(3)));
                          stmt.setInt(2, rs.getInt(5));
                          rsDetails                   = stmt.executeQuery();
                          while(rsDetails.next()){
                              Element detailEl            = mobileChartEl.addElement("details");
                              detailEl.addElement("user").addText(HandlerUtils.nullToEmpty(rsDetails.getString(1)));
                              detailEl.addElement("date").addText(HandlerUtils.nullToEmpty(rsDetails.getString(2)));
                              detailEl.addElement("platform").addText(HandlerUtils.nullToEmpty(rsDetails.getString(3)));

                        }
                        }
                                                 
                    }
                    break;
                case 2:
                     if(chart){
                         stmt                    = transconn.prepareStatement(selectAccessGraph);
                         stmt.setString(1, validatedStartDate.toString());
                         stmt.setString(2, validatedEndDate.toString());
                         rs                      = stmt.executeQuery();
                         while(rs.next()) {
                             Element mobileLikeEl  = toAppend.addElement("chart");
                             mobileLikeEl.addElement("date").addText(HandlerUtils.nullToEmpty(rs.getString(1)));                    
                             mobileLikeEl.addElement("count").addText(String.valueOf(rs.getInt(2)));  
                         }
                     } else {
                         stmt                            = transconn.prepareStatement(selectMaxLocAccess);
                         stmt.setString(1, validatedStartDate.toString());
                         stmt.setString(2, validatedEndDate.toString());
                         rs                              = stmt.executeQuery();
                         while(rs.next()){
                             Element socialEl            = toAppend.addElement("topAccess");
                             socialEl.addElement("type").addText(String.valueOf(rs.getInt(1)));
                             socialEl.addElement("name").addText(HandlerUtils.nullToEmpty(rs.getString(2)));
                             socialEl.addElement("count").addText(String.valueOf(rs.getInt(3)));                           
                         }
                         stmt                            = transconn.prepareStatement(selectMaxUserAccess);
                         stmt.setString(1, validatedStartDate.toString());
                         stmt.setString(2, validatedEndDate.toString());
                         rs                              = stmt.executeQuery();
                         while(rs.next()){
                             Element socialEl            = toAppend.addElement("topAccess");
                             socialEl.addElement("type").addText(String.valueOf(rs.getInt(1)));
                             socialEl.addElement("name").addText(HandlerUtils.nullToEmpty(rs.getString(2)));
                             socialEl.addElement("count").addText(String.valueOf(rs.getInt(3)));                           
                         }
                         
                         stmt                    = transconn.prepareStatement(selectLoginActivity);
                         stmt.setString(1, validatedStartDate.toString());
                         stmt.setString(2, validatedEndDate.toString());
                         rs                      = stmt.executeQuery();
                         while(rs.next()) {
                             Element userActivityEl  = toAppend.addElement("userActivity");
                             userActivityEl.addElement("user").addText(HandlerUtils.nullToEmpty(rs.getString(1)));                    
                             userActivityEl.addElement("device").addText(HandlerUtils.nullToEmpty(rs.getString(4)));
                             userActivityEl.addElement("description").addText(HandlerUtils.nullToEmpty(rs.getString(2))); 
                             userActivityEl.addElement("time").addText(HandlerUtils.nullToEmpty(rs.getString(3)));  
                         }
                     }                    
                    break;
                case 3:
                   if(chart){
                         stmt                    = transconn.prepareStatement(selectCheckinGraph);
                         stmt.setString(1, validatedStartDate.toString());
                         stmt.setString(2, validatedEndDate.toString());
                         rs                      = stmt.executeQuery();
                         while(rs.next()) {
                             Element mobileLikeEl  = toAppend.addElement("chart");
                             mobileLikeEl.addElement("date").addText(HandlerUtils.nullToEmpty(rs.getString(1)));                    
                             mobileLikeEl.addElement("count").addText(String.valueOf(rs.getInt(2)));  
                         }
                        
                     } else {
                       stmt                            = transconn.prepareStatement(selectMaxLocCheckin);
                       stmt.setString(1, validatedStartDate.toString());
                       stmt.setString(2, validatedEndDate.toString());
                       rs                              = stmt.executeQuery();
                       while(rs.next()){
                           Element socialEl            = toAppend.addElement("topCheckins");
                            socialEl.addElement("type").addText(String.valueOf(rs.getInt(1)));
                           socialEl.addElement("name").addText(HandlerUtils.nullToEmpty(rs.getString(2)));
                           socialEl.addElement("count").addText(String.valueOf(rs.getInt(3)));                           
                       }
                       
                       stmt                            = transconn.prepareStatement(selectMaxUserCheckin);
                       stmt.setString(1, validatedStartDate.toString());
                       stmt.setString(2, validatedEndDate.toString());
                       rs                              = stmt.executeQuery();
                       while(rs.next()){
                           Element socialEl            = toAppend.addElement("topCheckins");
                            socialEl.addElement("type").addText(String.valueOf(rs.getInt(1)));
                           socialEl.addElement("name").addText(HandlerUtils.nullToEmpty(rs.getString(2)));
                           socialEl.addElement("count").addText(String.valueOf(rs.getInt(3)));                           
                       }
                       
                       
                       stmt                            = transconn.prepareStatement(selectLocationCheckins);
                       stmt.setString(1, validatedStartDate.toString());
                       stmt.setString(2, validatedEndDate.toString());
                       rs                              = stmt.executeQuery();
                       while(rs.next()){
                           Element socialEl            = toAppend.addElement("socialMediaCheckins");
                           socialEl.addElement("location").addText(HandlerUtils.nullToEmpty(rs.getString(1)));
                           socialEl.addElement("count").addText(String.valueOf(rs.getInt(2)));
                           stmt                            = transconn.prepareStatement(selectCheckinDetails);
                           stmt.setInt(1,rs.getInt(3) );
                           stmt.setString(2, validatedStartDate.toString());
                           stmt.setString(3, validatedEndDate.toString());
                           rsDetails                   = stmt.executeQuery();
                           while(rsDetails.next()){
                               Element detailEl            = socialEl.addElement("details");
                               detailEl.addElement("user").addText(HandlerUtils.nullToEmpty(rsDetails.getString(1)));
                               detailEl.addElement("date").addText(HandlerUtils.nullToEmpty(rsDetails.getString(2)));

                        }
                    }
                       
                   }           
                    
                    break;
                case 4:
                    if(chart){
                         stmt                    = transconn.prepareStatement(selectLikesGraph);
                         stmt.setString(1, validatedStartDate.toString());
                         stmt.setString(2, validatedEndDate.toString());
                         rs                      = stmt.executeQuery();
                         while(rs.next()) {
                             Element mobileLikeEl  = toAppend.addElement("chart");
                             mobileLikeEl.addElement("date").addText(HandlerUtils.nullToEmpty(rs.getString(1)));                    
                             mobileLikeEl.addElement("count").addText(String.valueOf(rs.getInt(2)));  
                         }
                     } else {
                        stmt                            = transconn.prepareStatement(selectMaxLocLikes);
                        stmt.setString(1, validatedStartDate.toString());
                        stmt.setString(2, validatedEndDate.toString());
                        rs                              = stmt.executeQuery();
                        while(rs.next()){
                            Element socialEl            = toAppend.addElement("topLikes");
                            socialEl.addElement("type").addText(String.valueOf(rs.getInt(1)));
                            socialEl.addElement("name").addText(HandlerUtils.nullToEmpty(rs.getString(2)));
                            socialEl.addElement("count").addText(String.valueOf(rs.getInt(3)));                           
                        }
                        stmt                            = transconn.prepareStatement(selectMaxUserLikes);
                        stmt.setString(1, validatedStartDate.toString());
                        stmt.setString(2, validatedEndDate.toString());
                        rs                              = stmt.executeQuery();
                        while(rs.next()){
                            Element socialEl            = toAppend.addElement("topLikes");
                            socialEl.addElement("type").addText(String.valueOf(rs.getInt(1)));
                            socialEl.addElement("name").addText(HandlerUtils.nullToEmpty(rs.getString(2)));
                            socialEl.addElement("count").addText(String.valueOf(rs.getInt(3)));                           
                        }
                        stmt                            = transconn.prepareStatement(selectMaxProductLikes);
                        stmt.setString(1, validatedStartDate.toString());
                        stmt.setString(2, validatedEndDate.toString());
                        rs                              = stmt.executeQuery();
                        while(rs.next()){
                            Element socialEl            = toAppend.addElement("topLikes");
                            socialEl.addElement("type").addText(String.valueOf(rs.getInt(1)));
                            socialEl.addElement("name").addText(HandlerUtils.nullToEmpty(rs.getString(2)));
                            socialEl.addElement("count").addText(String.valueOf(rs.getInt(3)));                           
                        }
                        stmt                            = transconn.prepareStatement(selectProductsLikes);
                        stmt.setString(1, validatedStartDate.toString());
                        stmt.setString(2, validatedEndDate.toString());
                        rs = stmt.executeQuery();
                        while(rs.next()){
                            Element socialEl            = toAppend.addElement("socialMediaLikes");
                            socialEl.addElement("product").addText(HandlerUtils.nullToEmpty(rs.getString(1)));
                            socialEl.addElement("count").addText(String.valueOf(rs.getInt(2)));
                            stmt                            = transconn.prepareStatement(selectLikesDetails);
                            stmt.setInt(1,rs.getInt(3) );
                            stmt.setString(2, validatedStartDate.toString());
                            stmt.setString(3, validatedEndDate.toString());
                            rsDetails                   = stmt.executeQuery();
                            while(rsDetails.next()){
                                Element detailEl            = socialEl.addElement("details");
                                detailEl.addElement("user").addText(HandlerUtils.nullToEmpty(rsDetails.getString(1)));
                                detailEl.addElement("location").addText(HandlerUtils.nullToEmpty(rsDetails.getString(2)));
                                detailEl.addElement("date").addText(HandlerUtils.nullToEmpty(rsDetails.getString(3)));
                            }
                        }
                        
                    }           
                    
                    break;
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: "+sqle.toString());
            throw new HandlerException(sqle);
        }  catch(Exception e) {
            logger.debug(e.getMessage());   
        }finally {  
            close(rsDetails);
            close(rs);
            close(stmt);
            
           
        }
     }

    private void generateInsiderPass(int rewardId, String rewardText, int breweryId,String fileName) throws HandlerException {
        String selectProduct                = "SELECT pS.name, CONCAT(bL.logo, IF(bL.type = 0, '.png', IF(bL.type = 1, '.jpg', '.gif'))) from bevSyncCreatives bC"
                                            + " LEFT JOIN product p ON p.id = bC.product LEFT JOIN productDescription pD ON pD.product = p.id"
                                            + " LEFT JOIN productSet pS ON pS.id=bC.brewery LEFT JOIN breweryLogo bL ON bL.brewery = pS.name"
                                            + " WHERE pS.productSetType=7 AND bC.id IN (SELECT creatives FROM  bevSyncCampaignCreatives cr WHERE campaign=?) LIMIT 1;";
        ResultSet rs                        = null;
        PreparedStatement stmt              = null;
        Calendar currentDate                = null;
        String breweryLogo                  = "",brewery="";
        try {
            stmt                            = transconn.prepareStatement("SELECT pS.name, CONCAT(bL.logo, IF(bL.type = 0, '.png', IF(bL.type = 1, '.jpg', '.gif'))) FROM productSet pS LEFT JOIN breweryLogo bL ON bL.brewery = pS.name WHERE pS.id =?");
            stmt.setInt(1,breweryId);
            rs                              = stmt.executeQuery();
            if(rs.next()) {
                brewery                     = rs.getString(1);
                breweryLogo                 = rs.getString(2);    
            }
            String brewUrl                  = "http://beerboard.tv/USBN.BeerBoard.UI/Images/FBlogo/"+breweryLogo.trim().replaceAll("\'", "%27").replaceAll(" ", "%20");
            if(!saveImage(brewUrl, "/home/midware/facebook/reward/"+brewery+".png")) {
            }
            writeHtml(rewardText, brewery+".png");
            BufferedImage  ire = HtmlToImage.create("file:////home/midware/facebook/reward/pass.htm", 400, 520);
            ImageIO.write(ire, "jpg", new File("/home/midware/facebook/reward/"+fileName+".jpg"));
            fileUpload("/home/midware/facebook/reward/"+fileName+".jpg");
        
        } catch (SQLException sqle) {
            logger.debug(sqle.getMessage());
            throw new HandlerException(sqle);
        }catch (Exception e) {
            logger.debug(e.getMessage());
            e.printStackTrace();
        }finally {            
             close(rs);
             close(stmt);
            }
        
    }
    
    
    private void generateBanner(int rewardId, String rewardText, int breweryId, int productId, String fileName) throws HandlerException {
        int styleId                         = 0;
        String conditionString              = "";
        if(productId > 0) {
            conditionString                 = " AND p.product= "+productId;
        }
        String selectStyleLogo              = "Select style, CONCAT(sL.logo, IF(sL.type = 0, '.png', IF(sL.type = 1, '.jpg', '.gif')))  FROM styleLogo sL "
                                            + " LEFT JOIN productSet pS ON pS.name=sL.style WHERE pS.id=?;";
        String selectBreweryLogo            = "Select pS.name, CONCAT(bL.logo, IF(bL.type = 0, '.png', IF(bL.type = 1, '.jpg', '.gif'))) FROM breweryLogo bL "
                                            + " LEFT JOIN productSet pS ON pS.name=bL.brewery WHERE pS.id=?;";
        String selectProductId              = "SELECT p.product,(SELECT p1.productSet FROM productSetMap p1 LEFT JOIN productSet pS ON pS.id=p1.productSet WHERE p1.product=p.product AND pS.productSetType=9 ) FROM productSetMap p WHERE productSet= ?  "+conditionString +" LIMIT 1;";
        ResultSet rs                        = null;
        PreparedStatement stmt              = null;
        Calendar currentDate                = null;
        String styleLogo                    = "", style="";
        String breweryLogo                  = "",brewery="";
        
        try {
            stmt                            = transconn.prepareStatement(selectProductId);
            stmt.setInt(1,breweryId);
            rs                              = stmt.executeQuery();
            if(rs.next()) {
                styleId                = rs.getInt(2);
                if(styleId > 0) {
                    stmt                            = transconn.prepareStatement(selectStyleLogo);
                    stmt.setInt(1,styleId);
                    rs                              = stmt.executeQuery();
                    if(rs.next()) {
                        style               = HandlerUtils.nullToString(rs.getString(1),"Steam Beer");
                        styleLogo           = HandlerUtils.nullToString(rs.getString(2),"Steam%20Beer.png");
                    }
                }
            }
            
            String styleUrl                  = "http://beerboard.tv/USBN.BeerBoard.UI/Images/FBGlass/"+styleLogo.trim().replaceAll("\'", "%27").replaceAll(" ", "%20");
            if(!saveImage(styleUrl, "/home/midware/facebook/reward/"+style+".png")) {
            }
            
            stmt                            = transconn.prepareStatement(selectBreweryLogo);
            stmt.setInt(1,breweryId);
            rs                              = stmt.executeQuery();
            if(rs.next()) {
                brewery                     = HandlerUtils.nullToString(rs.getString(1), "Custom");
                breweryLogo                 = HandlerUtils.nullToString(rs.getString(2), "Custom.png");
            }
            
            String brewUrl                  = "http://beerboard.tv/USBN.BeerBoard.UI/Images/FBLogo/"+breweryLogo.trim().replaceAll("\'", "%27").replaceAll(" ", "%20");
            if(!saveImage(brewUrl, "/home/midware/facebook/reward/"+brewery+".png")) {
            }
            
            writeBannerHtml(brewery+".png", rewardText, style+".png");
            BufferedImage  ire = HtmlToImage.create("file:////home/midware/facebook/reward/banner.htm", 400, 40);
            ImageIO.write(ire, "jpg", new File("/home/midware/facebook/reward/"+fileName+".jpg"));
            fileUpload("/home/midware/facebook/reward/"+fileName+".jpg");
        
        } catch (SQLException sqle) {
            logger.debug(sqle.getMessage());
            throw new HandlerException(sqle);
        }catch (Exception e) {
            logger.debug(e.getMessage());
            e.printStackTrace();
        }finally {            
             close(rs);
             close(stmt);
            }
        
    }
    
    
     private String  getStyleLogo(int breweryId,int productId) throws HandlerException {
         
        
         PreparedStatement stmt             = null;
         ResultSet rs                       = null;
         int styleId                        = 0;
         String conditionString             = "";
         String logo                        = "Steam%20Beer.png";
         if(productId > 0) {
             conditionString                = " AND p.product= "+productId;
         }
         String selectLogo                  = "Select CONCAT(sL.logo, IF(sL.type = 0, '.png', IF(sL.type = 1, '.jpg', '.gif')))  FROM styleLogo sL "
                                            + " LEFT JOIN productSet pS ON pS.name=sL.style WHERE pS.id=?;";
         String selectProductId             = "SELECT p.product,(SELECT p1.productSet FROM productSetMap p1 LEFT JOIN productSet pS ON pS.id=p1.productSet WHERE p1.product=p.product AND pS.productSetType=9 ) FROM productSetMap p WHERE productSet= ?  "+conditionString +" LIMIT 1;";
        try {
            stmt                            = transconn.prepareStatement(selectProductId);
            stmt.setInt(1,breweryId);
            rs                              = stmt.executeQuery();
            if(rs.next()) {
                styleId                = rs.getInt(2);
                if(styleId > 0) {
                    stmt                            = transconn.prepareStatement(selectProductId);
                    stmt.setInt(1,styleId);
                    rs                              = stmt.executeQuery();
                    if(rs.next()) {
                        logo                = HandlerUtils.nullToString(rs.getString(selectLogo),"Steam%20Beer.png");
                        
                    }
                    
                }
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: "+sqle.toString());
            throw new HandlerException(sqle);
        } finally {
            
            close(stmt);
            close(rs);
        }
        return logo;
    }
     
     
    public void writeHtml(String rewardText,String brewery) {
        try {
        StringBuffer sb                     = new StringBuffer();
        sb.append("<html><head><style type=\"text/css\">");
        sb.append(".reward{ font-family: 'PPETRIAL'; font-size:36px; font-weight:bold; color: #000000; overflow:hidden;  white-space:nowrap; }"
                + ".reward1{display:none; font-family: 'PPETRIAL'; font-size:36px; font-weight:bold; color: #f7d675; overflow:hidden;  white-space:nowrap; }"
                + ".size {width:300px; height:150px;}");
        sb.append("body { }</style></head>");
        sb.append("<body>");
        sb.append("<table background=\"background.png\" width=\"400\" height=\"540\" cellspacing='0' cellpadding='0' border='0'>"
        + "  <tr> <td height=\"200\" width=\"400\"></td></tr> <tr><td height=\"200\"><img src=\""+brewery+"\" />   </td> </tr>"
                + "  <tr>    <td align=\"center\"  ><p class=\"reward\" align=\"center\" > "+rewardText+"</p>"
                + "  <p class=\"reward1\" align=\"center\" > - </p>  </td>  </tr></table>");
        sb.append("</body></html>");
        
        
         File file                          = new File("/home/midware/facebook/reward/pass.htm");
         BufferedWriter bw                  = new BufferedWriter(new FileWriter(file));
         bw.write(sb.toString());
         bw.close();
         //logger.debug(sb.toString());
        }catch (Exception e) {
                logger.dbError("Html: "+e.toString());
                e.printStackTrace();
        }

    }
    
     public void writeBannerHtml(String brewery, String rewardText, String style) {
        try {
            StringBuffer sb                 = new StringBuffer();
            sb.append("<html><head><style type=\"text/css\">");
            sb.append(".reward{ font-family: 'PPETRIAL'; font-size:15px; font-weight:bold; color: #f06626; overflow:hidden;  white-space:nowrap; }");
            sb.append("body { }</style></head>");
            sb.append("<body>");
            sb.append("<table width=\"320px\" height=\"40px\" cellspacing='0' cellpadding='0' bgcolor=\"#000000\">"
                + "<tr><td align=\"left\" style=\" width : 20%;\"><img height=\"40\" width=\"80\" src=\""+brewery+"\" /></td>"
                + "<td align=\"left\" style=\" width : 80%;\"><p class=\"reward\"> "+rewardText+" </p></td></tr></table>");
            sb.append("</body></html>");
        
            File file                      = new File("/home/midware/facebook/reward/banner.htm");
            BufferedWriter bw              = new BufferedWriter(new FileWriter(file));
            bw.write(sb.toString());
            bw.close();
         //logger.debug(sb.toString());
        }catch (Exception e) {
                logger.dbError("Html: "+e.toString());
                e.printStackTrace();
        }

    }
    
    
    public boolean  saveImage(String imageUrl, String destinationFile)  {
        boolean visible                     = false;
        try {
            URL url                         = new URL(imageUrl);
            InputStream is                  = url.openStream();
            OutputStream os                 = new FileOutputStream(destinationFile);
            byte[] b                        = new byte[2048];
            int length;
            while ((length = is.read(b)) != -1) {
                os.write(b, 0, length);
            }
            is.close();
            os.close();
            visible                         = true;
        }catch(Exception e) {
            e.printStackTrace();
        }
        return visible;
    }
    
    
      public void convertPNGToJPG(String file, String outFile,String color) {
 
	BufferedImage bufferedImage;
 
	try {
 
	  //read image file
	  URL url = new URL(file);
          bufferedImage = JAI.create("url", url).getAsBufferedImage();

           logger.debug("Image Read from "+file);
 
	  // create a blank, RGB, same width and height, and a white background
	  BufferedImage newBufferedImage = new BufferedImage(bufferedImage.getWidth(),
			bufferedImage.getHeight(), BufferedImage.TYPE_INT_RGB);
	  Color aColor = Color.decode(color);
	  newBufferedImage.createGraphics().drawImage(bufferedImage, 0, 0, aColor, null);
 
	  // write to jpeg file
	  ImageIO.write(newBufferedImage, "jpg", new File(outFile));
           logger.debug("Image Converted to "+outFile);
 
	 
 
	} catch (IOException e) {
 
	  logger.debug(e.getMessage());
 
	}
 
   }
    
    public void fileUpload(String filePath) throws IOException {
	        // takes file path from first program's argument
        String UPLOAD_URL = "http://social.usbeveragenet.com:8080/fileUploader/upload";
        int BUFFER_SIZE = 4096;
	        File uploadFile = new File(filePath);
	 
	        System.out.println("File to upload: " + filePath);
	 
	        // creates a HTTP connection
	        URL url = new URL(UPLOAD_URL); 
	        HttpURLConnection httpConn = (HttpURLConnection) url.openConnection();
	        httpConn.setUseCaches(false);
	        httpConn.setDoOutput(true);
	        httpConn.setRequestMethod("POST");
	        // sets file name as a HTTP header
	        httpConn.setRequestProperty("fileName", uploadFile.getName()); 
	 
	        // opens output stream of the HTTP connection for writing data
	        OutputStream outputStream = httpConn.getOutputStream();
	 
	        // Opens input stream of the file for reading data
	        FileInputStream inputStream = new FileInputStream(uploadFile);
	 
	        byte[] buffer = new byte[BUFFER_SIZE];
	        int bytesRead = -1;
	 
	        System.out.println("Start writing data...");
	 
	        while ((bytesRead = inputStream.read(buffer)) != -1) {
	            outputStream.write(buffer, 0, bytesRead);
	        }
	 
	        System.out.println("Data was written.");
	        outputStream.close();
	        inputStream.close();
	 
	        // always check HTTP response code from server
	        int responseCode = httpConn.getResponseCode();
	        if (responseCode == HttpURLConnection.HTTP_OK) {
	            // reads server's response
	            BufferedReader reader = new BufferedReader(new InputStreamReader(
	                    httpConn.getInputStream()));
	            String response = reader.readLine();
	            System.out.println("Server's response: " + response);
	        } else {
	            System.out.println("Server returned non-OK code: " + responseCode);
	        }
	    }
     


     private void getBBTVStatus(Element toHandle, Element toAppend) throws HandlerException {
        
        int callerId                        = getCallerId(toHandle);
        int type                            = HandlerUtils.getRequiredInteger(toHandle, "type");
        int customerId                      = HandlerUtils.getOptionalInteger(toHandle, "customerId");
        String startDate                    = HandlerUtils.getOptionalString(toHandle, "startDate");
        String endDate                      = HandlerUtils.getOptionalString(toHandle, "endDate");
        int hidden                          = HandlerUtils.getOptionalInteger(toHandle, "hidden");
        if(hidden <0){
            hidden                          = 0;
        }
        
        String selectLog                    = "SELECT l.id, lo.name,b.name,l.timestamp,l.message FROM bbtvErrorLogs l LEFT JOIN bbtvUserMac b ON b.id=l.userId "
                                            + " LEFT JOIN location lo ON lo.id=b.location WHERE b.name <> 'null' AND l.timestamp BETWEEN ? AND ? AND b.location IN (SELECT b.location FROM bevSyncCustomerLocationMap b WHERE b.customer=?) ORDER BY l.timestamp DESC;";
        String selectBBTVLog                = "SELECT l.id, lo.name,b.name,l.timestamp,l.message FROM bbtvErrorLogs l LEFT JOIN bbtvUserMac b ON b.id=l.userId "
                                            + " LEFT JOIN location lo ON lo.id=b.location WHERE b.name <> 'null' AND l.timestamp BETWEEN ? AND ? ORDER BY l.timestamp DESC;";
        
        String selectStatus                  = "SELECT l.name,b.name,b.lastPing,b.version FROM bbtvUserMac b LEFT JOIN location l ON l.id=b.location  WHERE l.name <> 'null' "
                                            + " AND b.location IN (SELECT b.location FROM bevSyncCustomerLocationMap b WHERE b.customer=?)  ORDER BY l.name";
        String selectBBTVStatus             = "SELECT b.id,l.id, BL.id,BL.customer_id, c.name,l.name,b.name, lastLogin,b.lastPing,b.version, b.status FROM bbtvUserMac b LEFT JOIN location l ON l.id=b.location "
                                            + " LEFT JOIN customer c ON c.id =l.customer LEFT JOIN BOSS_Location BL ON BL.usbn_location = l.id  WHERE l.name <> 'null'  AND b.hidden= 0 ORDER BY c.name,l.name;";
        String selectHiddenBBTV             = "SELECT b.id,l.id, BL.id,BL.customer_id, c.name,l.name,b.name, lastLogin,b.lastPing,b.version, b.status FROM bbtvUserMac b LEFT JOIN location l ON l.id=b.location "
                                            + " LEFT JOIN customer c ON c.id =l.customer LEFT JOIN BOSS_Location BL ON BL.usbn_location = l.id  WHERE l.name <> 'null' AND b.hidden= 1 ORDER BY c.name,l.name;";
        

        PreparedStatement stmt              = null;
        ResultSet rs                        = null;
        try {
            if(type ==1){
                if(customerId > 0){
                    stmt                        = transconn.prepareStatement(selectStatus);           
                    stmt.setInt(1,customerId);
                    rs                          = stmt.executeQuery();   
                    while (rs.next()) {
                        int i                   = 1;
                        Element statusEl        = toAppend.addElement("status");                
                        statusEl.addElement("locationName").addText(HandlerUtils.nullToEmpty(rs.getString(i++)));
                        statusEl.addElement("bbtvName").addText(HandlerUtils.nullToEmpty(rs.getString(i++)));
                        statusEl.addElement("lastPing").addText(HandlerUtils.nullToEmpty(rs.getString(i++)));
                        statusEl.addElement("version").addText(HandlerUtils.nullToEmpty(rs.getString(i++)));

                    }
                } else {
                    stmt                        = transconn.prepareStatement(selectBBTVStatus);                      
                    rs                          = stmt.executeQuery();   
                    while (rs.next()) {
                        int i                   = 1;
                        Element statusEl        = toAppend.addElement("status");                
                        statusEl.addElement("id").addText(HandlerUtils.nullToEmpty(rs.getString(i++)));
                        statusEl.addElement("locationId").addText(HandlerUtils.nullToEmpty(rs.getString(i++)));
                        statusEl.addElement("bossLocationId").addText(HandlerUtils.nullToEmpty(rs.getString(i++)));
                        statusEl.addElement("bossCustomerId").addText(HandlerUtils.nullToEmpty(rs.getString(i++)));
                        statusEl.addElement("customerName").addText(HandlerUtils.nullToEmpty(rs.getString(i++)));
                        statusEl.addElement("locationName").addText(HandlerUtils.nullToEmpty(rs.getString(i++)));
                        statusEl.addElement("bbtvName").addText(HandlerUtils.nullToEmpty(rs.getString(i++)));
                        statusEl.addElement("lastLogin").addText(HandlerUtils.nullToEmpty(rs.getString(i++)));
                        statusEl.addElement("lastPing").addText(HandlerUtils.nullToEmpty(rs.getString(i++)));
                        statusEl.addElement("version").addText(HandlerUtils.nullToEmpty(rs.getString(i++)));
                        statusEl.addElement("status").addText(String.valueOf(rs.getInt(i++)));

                    }
                }
            } else if(type == 2){
                if(customerId > 0){
                    stmt                        = transconn.prepareStatement(selectLog);           
                    stmt.setString(1, startDate);
                    stmt.setString(2, endDate);
                    stmt.setInt(3,customerId);
                    rs                          = stmt.executeQuery();   
                    while (rs.next()) {
                        int i                   = 1;
                        Element eLog            = toAppend.addElement("errorLogs");
                        eLog.addElement("id").addText(HandlerUtils.nullToEmpty(rs.getString(i++)));
                        eLog.addElement("locationName").addText(HandlerUtils.nullToEmpty(rs.getString(i++)));
                        eLog.addElement("bbtvName").addText(HandlerUtils.nullToEmpty(rs.getString(i++)));
                        eLog.addElement("timestamp").addText(HandlerUtils.nullToEmpty(rs.getString(i++)));
                        eLog.addElement("message").addText(HandlerUtils.nullToEmpty(rs.getString(i++)));

                    }
                } else {
                    stmt                        = transconn.prepareStatement(selectBBTVLog);           
                    stmt.setString(1, startDate);
                    stmt.setString(2, endDate);                    
                    rs                          = stmt.executeQuery();   
                    while (rs.next()) {
                        int i                   = 1;
                        Element eLog            = toAppend.addElement("errorLogs");
                        eLog.addElement("id").addText(HandlerUtils.nullToEmpty(rs.getString(i++)));
                        eLog.addElement("locationName").addText(HandlerUtils.nullToEmpty(rs.getString(i++)));
                        eLog.addElement("bbtvName").addText(HandlerUtils.nullToEmpty(rs.getString(i++)));
                        eLog.addElement("timestamp").addText(HandlerUtils.nullToEmpty(rs.getString(i++)));
                        eLog.addElement("message").addText(HandlerUtils.nullToEmpty(rs.getString(i++)));

                    }
                    
                }
            } else if(type==3) {
                stmt                        = transconn.prepareStatement(selectHiddenBBTV);                  
                rs                          = stmt.executeQuery();   
                while (rs.next()) {
                    int i                   = 1;
                    Element statusEl        = toAppend.addElement("status");                
                    statusEl.addElement("id").addText(HandlerUtils.nullToEmpty(rs.getString(i++)));
                    statusEl.addElement("locationId").addText(HandlerUtils.nullToEmpty(rs.getString(i++)));
                    statusEl.addElement("bossLocationId").addText(HandlerUtils.nullToEmpty(rs.getString(i++)));
                    statusEl.addElement("bossCustomerId").addText(HandlerUtils.nullToEmpty(rs.getString(i++)));
                    statusEl.addElement("customerName").addText(HandlerUtils.nullToEmpty(rs.getString(i++)));
                    statusEl.addElement("locationName").addText(HandlerUtils.nullToEmpty(rs.getString(i++)));
                    statusEl.addElement("bbtvName").addText(HandlerUtils.nullToEmpty(rs.getString(i++)));
                    statusEl.addElement("lastLogin").addText(HandlerUtils.nullToEmpty(rs.getString(i++)));
                    statusEl.addElement("lastPing").addText(HandlerUtils.nullToEmpty(rs.getString(i++)));
                    statusEl.addElement("version").addText(HandlerUtils.nullToEmpty(rs.getString(i++)));
                    statusEl.addElement("status").addText(String.valueOf(rs.getInt(i++)));

                }
                
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error in getErrorLogs: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } catch(Exception e){
            logger.debug("Date Format Error: " + e.getMessage());
            
        } finally {
            close(rs);
            close(stmt);
        }

    }
     
     
     private void getWakeupMessage(Element toHandle, Element toAppend) throws HandlerException {
        
        int callerId                        = getCallerId(toHandle);
      
        
        String selectMessage                = "SELECT m.id,m.message ,(SELECT GROUP_CONCAT(p.location) FROM wakeupMessageMap p WHERE p.message=m.id ),(SELECT p.openLocation FROM wakeupMessageMap p WHERE p.message=m.id LIMIT 1 ) FROM wakeupMessage m ;";
        
        PreparedStatement stmt              = null;
        ResultSet rs                        = null;
        try {
            stmt                        = transconn.prepareStatement(selectMessage);  
            rs                          = stmt.executeQuery();   
            while (rs.next()) {
                int i                   = 1;
                Element eMsg            = toAppend.addElement("wakeupMessage");
                eMsg.addElement("id").addText(HandlerUtils.nullToEmpty(rs.getString(i++)));
                eMsg.addElement("message").addText(HandlerUtils.nullToEmpty(rs.getString(i++)));
                eMsg.addElement("location").addText(HandlerUtils.nullToString(rs.getString(i++),"0"));
                eMsg.addElement("openLocation").addText(HandlerUtils.nullToString(rs.getString(i++),"0"));
                
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error in getErrorLogs: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(rs);
            close(stmt);
        }

    }
     
     
      private void addUpdateDeleteWakeupMessage(Element toHandle, Element toAppend) throws HandlerException {

        int callerId                        = getCallerId(toHandle);        
        ResultSet rs                        = null;
        PreparedStatement stmt              = null;
        String getLastId                    = "SELECT LAST_INSERT_ID()";

        try {
            Iterator addCampaign            = toHandle.elementIterator("addMessage");
            while (addCampaign.hasNext()) {
                Element CampaignEl          = (Element) addCampaign.next();
                String location             = HandlerUtils.getRequiredString(CampaignEl, "location");                
                String message              = HandlerUtils.getOptionalString(CampaignEl, "message");
                int openLocation            = HandlerUtils.getOptionalInteger(CampaignEl, "openLocation");
                   
                
                stmt                        = transconn.prepareStatement("INSERT INTO wakeupMessage ( message) VALUES (?)");
                stmt.setString(1, message);                 
                stmt.executeUpdate();

                stmt                        = transconn.prepareStatement(getLastId);
                rs                          = stmt.executeQuery();
                if (rs.next()) {
                    int messageId           = rs.getInt(1);   
                    if(location!=null && !location.equals("")){
                    String locList[]        = location.split(",");
                    for(int i=0;i<locList.length;i++){
                        stmt                = transconn.prepareStatement("DELETE FROM  wakeupMessageMap WHERE   location = ?;");                        
                        stmt.setInt(1, Integer.parseInt(locList[i]));
                        stmt.executeUpdate();
                        stmt                = transconn.prepareStatement("INSERT INTO wakeupMessageMap ( message,  location,openLocation) VALUES (?, ?,?)");
                        stmt.setInt(1, messageId);
                        stmt.setInt(2, Integer.parseInt(locList[i]));
                        stmt.setInt(3,openLocation);
                        stmt.executeUpdate();
                    }
                    }                    
                } else {
                    logger.dbError("first call to LAST_INSERT_ID in addWakeupMessage failed to return a result");
                    throw new HandlerException("Database Error");
                }
            }

            Iterator updateCampaignEl
                                            = toHandle.elementIterator("updateMessage");
            while (updateCampaignEl.hasNext()) {
                //logger.debug("Calling updateCampaignCreatives");
                Element CampaignEl          = (Element) updateCampaignEl.next();
                int id                      = HandlerUtils.getRequiredInteger(CampaignEl, "id");
                String location             = HandlerUtils.getRequiredString(CampaignEl, "location");                
                String message              = HandlerUtils.getOptionalString(CampaignEl, "message");
                int openLocation            = HandlerUtils.getOptionalInteger(CampaignEl, "openLocation");
                
                String updateCampaign       = " UPDATE wakeupMessage SET message=? WHERE id = ? ";

                stmt                        = transconn.prepareStatement(updateCampaign);
                stmt.setString(1, message);                
                stmt.setInt(2, id);   
                stmt.executeUpdate();
                
                stmt                        = transconn.prepareStatement("DELETE FROM wakeupMessageMap WHERE message=?;");
                stmt.setInt(1,id);
                stmt.executeUpdate();
                if(location!=null && !location.equals("")){
                String locList[]            = location.split(",");
                for(int i=0;i<locList.length;i++){
                   stmt                = transconn.prepareStatement("DELETE FROM  wakeupMessageMap WHERE   location = ?;");                        
                   stmt.setInt(1, Integer.parseInt(locList[i]));
                   stmt.executeUpdate();
                   
                   stmt                = transconn.prepareStatement("INSERT INTO wakeupMessageMap ( message,  location,openLocation) VALUES (?, ?, ?)");
                   stmt.setInt(1, id);
                   stmt.setInt(2, Integer.parseInt(locList[i]));
                   stmt.setInt(3,openLocation);
                   stmt.executeUpdate();
                }
                }
                
            }

            Iterator deleteCampaign
                                            = toHandle.elementIterator("deleteMessage");
            while (deleteCampaign.hasNext()) {
                Element Campaign    = (Element) deleteCampaign.next();
                int id                      = HandlerUtils.getRequiredInteger(Campaign, "id");
                
                stmt                        = transconn.prepareStatement("DELETE FROM wakeupMessageMap WHERE message=?;");
                stmt.setInt(1,id);
                stmt.executeUpdate();
               
                String delete               = " DELETE FROM wakeupMessage WHERE id = ? ";

                stmt                        = transconn.prepareStatement(delete);
                stmt.setInt(1,id);
                stmt.executeUpdate();
               

                String logMessage           = "Delete Wakeup message '" + id + "'";
                logger.portalDetail(callerId, "deleteWakeupMessage", 0, "delete", id, logMessage, transconn);
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: "+sqle.toString());
            throw new HandlerException(sqle);
        } catch(Exception e) {
            e.printStackTrace();
        } finally {
            close(stmt);
            close(rs);
        }
   }
      
      
      private void getProfinityBlackList(Element toHandle, Element toAppend) throws HandlerException {

        try {
            String data                     = getHttpData(new URL("http://api1.webpurify.com/services/rest/?method=webpurify.live.getblacklist&api_key=27b6dbff640d06b4beabd620aef7e565&format=json"));
            //XML xmlObject                   = new XML();
            //JSONObject xmlJson              = xmlObject.toJSONObject(data);
            JSONObject xmlJson              = new JSONObject(data);
            //logger.debug("xmlToJson:"+xmlJson.toString());
            if(xmlJson.has("rsp")){
                JSONObject res= xmlJson.getJSONObject("rsp");
                if(res.has("word")) {
                    Object words            = res.get("word");
                    if (words instanceof JSONArray) {
                        // It's an array
                        JSONArray wordArray =(JSONArray)words;
                        for(int i=0;i<wordArray.length();i++) {
                            String  word    = wordArray.getString(i);
                            System.out.println(i+":"+word);
                            Element eMsg            = toAppend.addElement("blackList");
                            eMsg.addElement("word").addText(word);
                        }
                    } else if (words instanceof JSONObject) {
                        // It's an object   				    
                        String  word        = words.toString();
                        Element eMsg            = toAppend.addElement("blackList");
                        eMsg.addElement("word").addText(word);
                       
                    }   				
                }
            }
        } catch(Exception e){
            logger.dbError("Json error: " + e.getMessage());
        } finally {
            
        }
      }
      
      
      private void addRemoveBlackList(Element toHandle, Element toAppend) throws HandlerException {

        try {
            Iterator addWord                = toHandle.elementIterator("addWord");
            while (addWord.hasNext()) {
                Element wordEl              = (Element) addWord.next();
                String word                 = HandlerUtils.getRequiredString(wordEl, "word");                
                getHttpData(new URL("http://api1.webpurify.com/services/rest/?method=webpurify.live.addtoblacklist&api_key=27b6dbff640d06b4beabd620aef7e565&word="+word.replaceAll(" ", "")));
                   
              
            }

          
            Iterator removeWord             = toHandle.elementIterator("removeWord");
            while (removeWord.hasNext()) {
                Element wordEl              = (Element) removeWord.next();
                String word                 = HandlerUtils.getRequiredString(wordEl, "word");  
                getHttpData(new URL("http://api1.webpurify.com/services/rest/?method=webpurify.live.removefromblacklist&api_key=27b6dbff640d06b4beabd620aef7e565&word="+word.replaceAll(" ", "")));
                
            }
        } catch(Exception e) {
            e.printStackTrace();
        } finally {
           
        }
   }
      
      
      public String getHttpData(URL urL) {
        String graph                        =null;
        try {
            String inputLine;
            HttpURLConnection conn         = (HttpURLConnection) urL.openConnection();
            BufferedReader in               = new BufferedReader(new InputStreamReader(conn.getInputStream()));
            StringBuffer  b                 = new StringBuffer();
            while ((inputLine = in.readLine()) != null)
             b.append(inputLine + "\n");
            in.close();
            graph                           = b.toString();
        }catch (Exception e) {
            logger.debug("Http data response"+e.getMessage());
        }
        return graph;

    }
      
      
      private void addUpdateDeleteInsiderPass(Element toHandle, Element toAppend) throws HandlerException {

        int callerId                        = getCallerId(toHandle);  
        int userId                          = HandlerUtils.getRequiredInteger(toHandle, "userId");
        int location                        = HandlerUtils.getRequiredInteger(toHandle, "locationId");
        ResultSet rs                        = null, rsDetails = null,rsFB = null;
        PreparedStatement stmt              = null;
        Calendar currentDate                = null;
        String checkReward                  = "SELECT id FROM insiderPass WHERE location = ?  AND rewardText = ? AND ? BETWEEN startTime AND endTime AND  ? BETWEEN  startTime AND endTime AND brewery = ? AND product = ?";

        try {
           

            Iterator addCustomerRewards   = toHandle.elementIterator("addPass");
            while (addCustomerRewards.hasNext()) {
                Element customerRewards     = (Element) addCustomerRewards.next();                
                String title                = HandlerUtils.getRequiredString(customerRewards, "title");
                String rewardText           = HandlerUtils.getRequiredString(customerRewards, "rewardText");
                int breweryId               = HandlerUtils.getRequiredInteger(customerRewards, "brewery");
                int productId               = HandlerUtils.getRequiredInteger(customerRewards, "product");                
                String start                = HandlerUtils.getRequiredString(customerRewards, "startTime");
                String end                  = HandlerUtils.getRequiredString(customerRewards, "endTime");
             
               
                
                String selectProduct        = "SELECT pS.name, IFNULL(pD.boardname, 'Unknown Product'),(SELECT CONCAT(boardname,', ',addrCity) from location where id=?) from bevSyncCreatives bC "
                                            + " LEFT JOIN product p ON p.id = bC.product LEFT JOIN productDescription pD ON pD.product = p.id LEFT JOIN productSet pS ON pS.id=bC.brewery WHERE pS.productSetType=7 AND bC.id=?;";
                String getLastId            = " SELECT LAST_INSERT_ID()";
                String selectToken          = "select Distinct p.pageid, u.user_id, u.access_token from usbnFacebook u LEFT JOIN usbnFacebookPage p ON u.user_id =p.fbid where p.location=?  and u.user_id!='5515563';";
                int rewardId                = 0;
                String brewery              = null,locationName = null,product= null, message = "";
                stmt                        = transconn.prepareStatement(checkReward);
                stmt.setInt(1, location);               
                stmt.setString(2, rewardText);
                stmt.setString(3, dbDateFormat.format(dbDateFormat.parse(start)));
                stmt.setString(4, dbDateFormat.format(dbDateFormat.parse(end)));
                stmt.setInt(5, breweryId);               
                stmt.setInt(6, productId);               
                rs                          = stmt.executeQuery();
                if (!rs.next()) {
                    stmt                        = transconn.prepareStatement("INSERT INTO insiderPass (rewardText, user,location,startTime,endTime,brewery,product,title) VALUES (?, ?, ?, ?, ?,?,?, ?)");                  
                    stmt.setString(1, rewardText);                    
                    stmt.setInt(2, userId);
                    stmt.setInt(3, location);
                    stmt.setString(4, dbDateFormat.format(dbDateFormat.parse(start)));
                    stmt.setString(5, dbDateFormat.format(dbDateFormat.parse(end)));
                    stmt.setInt(6, breweryId);
                    stmt.setInt(7, productId);
                    stmt.setString(8, title);
                    stmt.executeUpdate();
                    
                    stmt                        = transconn.prepareStatement(getLastId);
                    rs                          = stmt.executeQuery();
                    if(rs.next()) {
                        rewardId                = rs.getInt(1);
                    }
                    String file                 = "pass"+"-"+rewardId+"-"+userId+"-"+location;
                    generateInsiderPass(rewardId,rewardText,breweryId,file);
                    Element rewardEl          = toAppend.addElement("insiderPass");
                    rewardEl.addElement("id").addText(String.valueOf(rewardId));                                                                    
                    rewardEl.addElement("path").addText("http://social.usbeveragenet.com:8080/fileUploader/Images/"+file+".jpg");                   
                   
                    stmt                        = transconn.prepareStatement("UPDATE insiderPass SET file =? WHERE id =?");
                    stmt.setString(1,file);
                    stmt.setInt(2,rewardId);
                    stmt.executeUpdate();
                    
                    logger.debug("Reward Id: " + rewardId);
                    

                    /*stmt                        = transconn.prepareStatement("UPDATE bbtvMobileUser SET arrival = NOW() - INTERVAL 3 HOUR WHERE arrival > NOW() - INTERVAL 2 HOUR");
                    stmt.executeUpdate();
                    
                    stmt                        = transconn.prepareStatement(selectProduct);
                    stmt.setInt(1,location);                    
                    stmt.setInt(2,creatives);
                    rsDetails                   = stmt.executeQuery();
                    if(rsDetails.next()) {
                        brewery                 = rsDetails.getString(1);                        
                        product                 = rsDetails.getString(2);                        
                        locationName            = rsDetails.getString(3);
                        
                    }
                    String[] days               = {"Saturday", "Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday"};
                    Calendar c                  = Calendar.getInstance();
                    c.setTime(dbDateFormat.parse(start));
                    int day_of_week             = c.get(Calendar.DAY_OF_WEEK);
                    if(product.equals("Unknown Product") ){
                        message                 = "Get Your Reward for "+brewery +" @ "+locationName + " from " + days[day_of_week] + " " + timeFormat.format(dbDateFormat.parse(start)) + " to " + timeFormat.format(dbDateFormat.parse(end));
                    } else {
                        message                 = "Get Your Reward for "+product +" @ "+locationName + " from " + days[day_of_week] + " " + timeFormat.format(dbDateFormat.parse(start)) + " to " + timeFormat.format(dbDateFormat.parse(end));
                    }
                    currentDate                 = Calendar.getInstance();
                    stmt                        = transconn.prepareStatement("INSERT INTO pushMessage (message, location, reward, pushTime) VALUES (?, ?, ?, ?)");
                    stmt.setString(1, message);
                    stmt.setInt(2, location);
                    stmt.setInt(3, rewardId);
                    stmt.setString(4, dbDateFormat.format(currentDate.getTime()));
                    stmt.executeUpdate();
                    
                    stmt                        = transconn.prepareStatement(getLastId);
                    rsDetails                   = stmt.executeQuery();
                    if(rsDetails.next()) {
                        int messageId           = rsDetails.getInt(1);
                    }*/
                    
                    
                    /*SQLBeerBoardHandler socialMedia   = new SQLBeerBoardHandler();
                    stmt                        = transconn.prepareStatement("SELECT customer,file from bevSyncCreatives WHERE id= ?");
                    stmt.setInt(1,creatives);
                    rsDetails                   = stmt.executeQuery();
                    if(rsDetails.next()) {
                        String fileName     = "http://beerboard.tv/USBN.BeerBoard.UI/bevSyncCustomers/" + String.valueOf(rsDetails.getInt(1)) + "/" + rsDetails.getString(2).replaceAll(" ", "%20");
                        socialMedia.saveImage(fileName, "/home/midware/facebook/reward/"+rsDetails.getString(2));                    
                        File file                    = new File("/home/midware/facebook/reward/"+rsDetails.getString(2));
                        stmt                     = transconn.prepareStatement(selectToken); 
                        stmt.setInt(1,location);
                        rsFB                     = stmt.executeQuery();
                        while (rsFB.next()) {
                            String pageId        = rsFB.getString(1);
                            String fbId=rsFB.getString(2);
                            String locationToken=rsFB.getString(3);
                            socialMedia.postImage(fbId, pageId, locationToken, message, location, rewardId, file,4);
                        }
                        stmt                       = transconn.prepareStatement("Select t.consumerKey,t.consumerSecret,t.accesToken,t.tokenSecret,t.id from usbnTwitter t LEFT JOIN twitterLocationMap l ON l.twitter = t.id WHERE l.location = ?;");
                        stmt.setInt(1,location);
                        rsFB                         = stmt.executeQuery();
                        while(rsFB.next()) {
                            String consumerKey     = rsFB.getString(1);
                            String consumerSecret  = rsFB.getString(2);
                            String accesToken      = rsFB.getString(3);
                            String tokenSecret     = rsFB.getString(4);
                            int user                     = rs.getInt(5);
                            socialMedia.tweetImage(user,location,message, consumerKey, consumerSecret, accesToken, tokenSecret, file,4,rewardId);
                        }
                    }
                    */
                } else {
                    toAppend.addElement("passStatus").addText("Already Reward Assigned for this location");
                }
            }

            Iterator updateCustomerReward   = toHandle.elementIterator("updatePass");
            while (updateCustomerReward.hasNext()) {
                Element customerRewards     = (Element) updateCustomerReward.next();
                int id                      = HandlerUtils.getRequiredInteger(customerRewards, "id");   
                String title                = HandlerUtils.getRequiredString(customerRewards, "title");
                String rewardText           = HandlerUtils.getRequiredString(customerRewards, "rewardText");
                int breweryId               = HandlerUtils.getRequiredInteger(customerRewards, "brewery");
                int productId               = HandlerUtils.getRequiredInteger(customerRewards, "product");                
                String start                = HandlerUtils.getRequiredString(customerRewards, "startTime");
                String end                  = HandlerUtils.getRequiredString(customerRewards, "endTime");

                String update               = " UPDATE insiderPass SET rewardText = ?, location = ?, startTime=?, endTime = ?,user = ?, brewery =?, product = ?, title= ? WHERE id = ? ";
                
                stmt                        = transconn.prepareStatement(checkReward);
                stmt.setInt(1, location);               
                stmt.setString(2, rewardText);
                stmt.setString(3, dbDateFormat.format(dbDateFormat.parse(start)));
                stmt.setString(4, dbDateFormat.format(dbDateFormat.parse(end)));
                stmt.setInt(5, breweryId);               
                stmt.setInt(6, productId);      
                rs                          = stmt.executeQuery();
                int rid                     = 0;
                int oldCreatives            = 0;
                if (rs.next()) {
                        rid                 = rs.getInt(1);                        
                }
                
                if(rid == 0 || rid == id) {
                stmt                        = transconn.prepareStatement(update);                
                stmt.setString(1, rewardText);                
                stmt.setInt(2, location);                
                stmt.setString(3, dbDateFormat.format(dbDateFormat.parse(start)));
                stmt.setString(4, dbDateFormat.format(dbDateFormat.parse(end)));
                stmt.setInt(5, userId);                
                stmt.setInt(6, breweryId);  
                stmt.setInt(7, productId);  
                stmt.setString(8, title);                
                stmt.setInt(9, id);                
                stmt.executeUpdate();
                String file                 = "pass"+"-"+id+"-"+userId+"-"+location;
                generateInsiderPass(id,rewardText,breweryId,file);
                Element rewardEl          = toAppend.addElement("insiderPass");
                rewardEl.addElement("id").addText(String.valueOf(id));                                                                    
                rewardEl.addElement("path").addText("http://social.usbeveragenet.com:8080/fileUploader/Images/"+file+".jpg");
                
                
                stmt                        = transconn.prepareStatement("UPDATE insiderPass SET file =? WHERE id =?");
                stmt.setString(1,file);
                stmt.setInt(2,id);
                stmt.executeUpdate();                
                
                
                String logMessage           = "Updated Pass '" + id + "'";                
                logger.portalDetail(callerId, "updateInsiderPass", 0, "insiderPass", id, logMessage, transconn);
            } else {
                    toAppend.addElement("insiderPass").addText("Already Pass Assigned for this location");
                    
                }
            }

            Iterator deleteCustomerReward   = toHandle.elementIterator("deletePass");
            while (deleteCustomerReward.hasNext()) {
                Element customerReward      = (Element) deleteCustomerReward.next();
                int id                      = HandlerUtils.getRequiredInteger(customerReward, "id");                
                
                String delete               = " DELETE FROM insiderPass WHERE id = ? ";

                stmt                        = transconn.prepareStatement(delete);
                stmt.setInt(1,id);
                stmt.executeUpdate();

                String logMessage           = "Delete Passes '" + id + "'";
                logger.portalDetail(callerId, "deleteInsiderPass", 0, "insiderPass", id, logMessage, transconn);
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: "+sqle.toString());
            throw new HandlerException(sqle);
        } catch(Exception e) {
            logger.debug(e.getMessage());
            e.printStackTrace();
             } finally {
            close(rsFB);
            close(rsDetails);
            close(rs);
            close(stmt);
        
        }
      }

      private void addLocationPushNotification(Element toHandle, Element toAppend) throws HandlerException {
         PreparedStatement stmt             = null;
         ResultSet rs                       = null, rsDetails = null, rsMap = null;
         
         String selectPushNotification      = " SELECT id, location, message FROM locationPushNotification WHERE date = SUBSTRING(NOW(), 1, 10) AND active = 1;";
         String selectUserByLocation        = " SELECT u.id, u.username FROM favoriteLocation fL LEFT JOIN bbtvMobileUser u ON u.id = fL.user "
                                            + " WHERE fL.location = ? AND LENGTH(deviceToken) > 4;";
         String getLastId                   = " SELECT LAST_INSERT_ID()";
         try {
             
            stmt                            = transconn.prepareStatement(selectPushNotification);           
            rs                              = stmt.executeQuery();
            int messageId                   = 0;
            if (rs.next()) {
                String message              = "Promotions and Offers at your Favorite Bars";
                stmt                        = transconn.prepareStatement("INSERT INTO pushMessage (message, location, reward, category, pushTime, type) VALUES (?, ?, ?, 2,(SELECT   IF( TIME(now())>'14:59:00' ,  (SELECT concat (DATE(ADDDATE(NOW(), INTERVAL 1 DAY)), ' 15:00:00')),(SELECT concat (DATE(NOW()), ' 15:00:00')) ) ),2);");
                stmt.setString(1, message);
                stmt.setInt(2, 425);
                stmt.setInt(3, 0);
                stmt.executeUpdate();

                stmt                        = transconn.prepareStatement(getLastId);
                rsDetails                   = stmt.executeQuery();
                if(rsDetails.next()) {
                    messageId               = rsDetails.getInt(1);
                }

                stmt                        = transconn.prepareStatement(selectPushNotification);           
                rs                          = stmt.executeQuery();
                while(rs.next()) {
                    int notificationId      = rs.getInt(1);
                    int location            = rs.getInt(2);
                    message                 = rs.getString(3);

                    stmt                    = transconn.prepareStatement(selectUserByLocation);
                    stmt.setInt(1, location);
                    rsDetails               = stmt.executeQuery();
                    while (rsDetails.next()){
                        int userId          = rsDetails.getInt(1);
                        stmt                = transconn.prepareStatement("INSERT INTO pushMessageCenter (pushMessage, location, message, user) VALUES (?,?,?,?);");                            
                        stmt.setInt(1,messageId);
                        stmt.setInt(2,location);
                        stmt.setString(3, message);
                        stmt.setInt(4,userId);
                        stmt.executeUpdate();

                         stmt               = transconn.prepareStatement("SELECT id FROM pushMessageMap WHERE user=? AND message= ?");
                         stmt.setInt(1,userId);
                         stmt.setInt(2,messageId);
                         rsMap              = stmt.executeQuery();
                         if(!rsMap.next()){
                             stmt           = transconn.prepareStatement("INSERT INTO pushMessageMap (user, message) VALUES (?,?);");
                             stmt.setInt(1,userId);
                             stmt.setInt(2,messageId);
                             stmt.executeUpdate();
                         }
                    }

                    stmt                    = transconn.prepareStatement("UPDATE locationPushNotification SET active = 0 WHERE id = ?");
                    stmt.setInt(1, notificationId);
                    stmt.executeUpdate();
                }
            }
    	  } catch (Exception e) {
    		throw new HandlerException(e);
    	  } finally {
             close(rsMap);
            close(rsDetails);
            close(rs);
            close(stmt);
        }
     }

      private void sendBBMobileNotification(Element toHandle, Element toAppend) throws HandlerException {
         PreparedStatement stmt             = null;
         ResultSet rs                       = null, rsDetails = null;
         String logo                        = "", validity = "";
         int location                       = 0, type = 0;
         String message                     = "New Beers Now on Tap at your Favorite Bars";
         ArrayList<Integer> sentUsers       = new ArrayList<Integer>();
         String selectPushMessageMap        = "SELECT message, user, id FROM pushMessageMap WHERE sent = 0 ORDER BY user;";
         String selectPushMessage           = "SELECT p.location, IF(p.Type = 2, IFNULL((SELECT message FROM locationPushNotification WHERE location = p.location " +
                                            " AND date = DATE(p.pushTime) ORDER BY id DESC LIMIT 1), 'Check-out our beer list!'), 'New Beers Now on Tap at your Favorite Bars') " +
                                            " FROM pushMessage p WHERE p.id = ?";
         String selectUser                  = "SELECT id, platform, deviceToken, NOW() + INTERVAL 24 HOUR, username FROM bbtvMobileUser " +
                                            " WHERE id = ? AND LENGTH(deviceToken) > 63 AND platform > 0 ORDER BY id";
         try {
             stmt                           = transconn.prepareStatement(selectPushMessageMap);
             rs                             = stmt.executeQuery();
             while(rs.next()){
                int messageId               = rs.getInt(1);
                int user                    = rs.getInt(2);
                int mapId                   = rs.getInt(3);
                
                stmt                        = transconn.prepareStatement(selectPushMessage);
                stmt.setInt(1, messageId);
                rsDetails                   = stmt.executeQuery();
                if(rsDetails.next()){
                     location               = rsDetails.getInt(1);
                     message                = rsDetails.getString(2);
                }
                logo                        = "USBN.jpg";

                stmt                        = transconn.prepareStatement(selectUser);
                stmt.setInt(1,user);
                rsDetails                   = stmt.executeQuery();
                if (rsDetails.next() && !(sentUsers.contains(user))) {
                    sentUsers.add(user);
                    
                    user                    = rsDetails.getInt(1);
                    int platform            = rsDetails.getInt(2);
                    String deviceToken      = rsDetails.getString(3);
                    validity                = rsDetails.getString(4);                          
                    logger.debug("Sending message: " + message + " for UserId: " + user);
                    switch (platform) {                          
                        case 2:
                            boolean sentNotifi
                                            = false;
                           // sentNotifi      = sendAndroidNotification("AIzaSyDjrsCeKvnCWTTVr73bFwhVVdBibOaEfxs", deviceToken, 1, location, logo, messageId, message, validity);
                            if(!sentNotifi){
                                sentNotifi  = sendAndroidNotification("AIzaSyC_x06LqW2_7KzScA-8MyunIrLAUcekyz4", deviceToken, 1, location, logo, messageId, message, validity);
                            }
                            if(sentNotifi){
                                stmt        = transconn.prepareStatement("UPDATE pushMessageMap SET sent=1 WHERE id=?;");
                                stmt.setInt(1,mapId);                                
                                stmt.executeUpdate();
                            }
                            //logger.debug(result.toString());
                            break;
                        case 3:                                
                            PushNotificationPayload simplePayLoad
                                            = new PushNotificationPayload();
                            simplePayLoad.addAlert(message);
                            simplePayLoad.addBadge(1);
                            simplePayLoad.addSound("default");
                            simplePayLoad.addCustomDictionary("location", location);
                            simplePayLoad.addCustomDictionary("messageId", messageId);
                            simplePayLoad.addCustomDictionary("type", 1);
                            simplePayLoad.addCustomDictionary("logo", logo);
                            simplePayLoad.addCustomDictionary("validity", validity);
                            logger.debug(""+Push.payload(simplePayLoad, "/home/midware/Push Notification/usbnapn1.1.2.p12", "usbn", true, deviceToken));
                            //Push.payload(simplePayLoad, "/home/midware/Push Notification/usbnapndevnew.p12", "usbn", false, deviceToken); 

                            stmt       = transconn.prepareStatement("UPDATE pushMessageMap SET sent=1 WHERE id=?;");
                            stmt.setInt(1,mapId);                                
                            stmt.executeUpdate();
                            break;
                     }
                }
             }

            stmt                            = transconn.prepareStatement("UPDATE pushMessageMap SET sent = 1;");
            stmt.executeUpdate();

            stmt                            = transconn.prepareStatement("DELETE FROM pushMessageCheck;");
            stmt.executeUpdate();
    	  } catch (Exception e) {
              logger.debug(e.getMessage());
    		throw new HandlerException(e);
    	  } finally {
            close(rsDetails);
            close(rs);
            close(stmt);
        }
     }
      
      
      private void testNotification(Element toHandle, Element toAppend) throws HandlerException {
           String deviceToken                          = HandlerUtils.getRequiredString(toHandle, "deviceToken");
           try {
               sendAndroidNotification("AIzaSyC_x06LqW2_7KzScA-8MyunIrLAUcekyz4", deviceToken, 1, 425, "", 1, "TEST","2015-11-30");
           } catch(Exception e){
               logger.debug(e.getMessage());
           }
      }
      
      
      private boolean sendAndroidNotification(String senderId,String deviceToken,int type,int location,String logo,int messageId,String message,String validity) throws HandlerException {
          boolean sentNotifi                = false;
          try{
              Sender sender                 = new Sender(senderId);
              Message pushMessage           = new Message.Builder()
                                            .delayWhileIdle(false) // Wait for device to become active before sending.
                                            .addData( "message", message)   
                                            .addData( "location", String.valueOf(location) )
                                            .addData( "messageId", String.valueOf(messageId) )
                                            .addData( "logo", logo )
                                            .addData( "type", String.valueOf(type) )
                                            .addData( "validity", validity )
                                            .build();
              Result result                 = sender.send(pushMessage, deviceToken, 1);
              logger.debug(result.toString());
              if(result.toString().contains("MismatchSenderId")){
                  sentNotifi                = false;
              }else{
                  sentNotifi                = true;
              }
          } catch (Exception e) {
              logger.debug(e.getMessage());
              return false;
          }
          return sentNotifi;
      }
      
      
      private void sendBevMobileNotification(Element toHandle, Element toAppend) throws HandlerException {
         PreparedStatement stmt             = null;
         ResultSet rs                       = null, rsDetails = null;
         String logo                        = "", validity = "";
         int location                       = 0, type = 0;
         String message                     = "You have a New Notification!";
         ArrayList<Integer> sentUsers       = new ArrayList<Integer>();
         String selectPushMessageMap        = "SELECT pM.pushMessage, u.deviceToken , length(u.deviceToken), pM.id, NOW() + INTERVAL 24 HOUR FROM bevPushMessageMap pM LEFT JOIN bevPushMessage p ON p.id=pM.pushMessage LEFT JOIN bevMobileUser u ON u.id=pM.user "
                                            + " WHERE pM.sent = 0 AND length(u.deviceToken)> 60 AND DATE(p.pushTime) = DATE(now());";
         
         try {
             stmt                           = transconn.prepareStatement(selectPushMessageMap);
             rs                             = stmt.executeQuery();
             while(rs.next()){
                int messageId               = rs.getInt(1);       
                String deviceToken          = rs.getString(2);
                int length                  = rs.getInt(3);       
                int mapId                   = rs.getInt(4);    
                validity                    = rs.getString(5);
                int platform                = 0;
                if(length ==64) {
                    platform                = 3;
                } else if(length >64){
                    platform                = 2;
                }
                logger.debug("Platform:"+platform );
                
                                     
            if (platform ==2) {                          
                
                    boolean sentNotifi
                                    = false;
                    sentNotifi      = sendBevAndroidNotification("AIzaSyCmPGzLjv57ZFecOLGhL4HkSxe6u7PXp88", deviceToken, 1, location, messageId, message, validity);

                    //logger.debug(result.toString());
            } else if(platform == 3){
                    logger.debug("Platform:"+platform +" "+ deviceToken);
                    PushNotificationPayload simplePayLoad
                                    = new PushNotificationPayload();
                    simplePayLoad.addAlert(message);
                    simplePayLoad.addBadge(1);
                    simplePayLoad.addSound("default");
                    simplePayLoad.addCustomDictionary("location", location);
                    simplePayLoad.addCustomDictionary("messageId", messageId);
                    simplePayLoad.addCustomDictionary("type", 1);                    
                    simplePayLoad.addCustomDictionary("validity", validity);
                    logger.debug(""+Push.payload(simplePayLoad, "/home/midware/Push Notification/bevMan/bevProd.p12", "usbn123", true, deviceToken));
                    /*PushNotificationPayload simplePayLoad
                                    = new PushNotificationPayload();
                    simplePayLoad.addAlert(message);
                    simplePayLoad.addBadge(1);
                    simplePayLoad.addSound("default");
                    simplePayLoad.addCustomDictionary("location", location);
                    simplePayLoad.addCustomDictionary("messageId", 2);
                    simplePayLoad.addCustomDictionary("type", 1);                    
                    simplePayLoad.addCustomDictionary("validity", validity);
                    logger.debug(""+Push.payload(simplePayLoad, "/home/midware/Push Notification/bevMan/bevProd.p12", "usbn123", true, "ac75b7b83b21b1de4ea5ddfb2bb1cb8dfd0374eb6ca265fdbc16a8113cde11da"));*/
                    


                    

                    stmt       = transconn.prepareStatement("UPDATE bevPushMessageMap SET sent=1 WHERE id=?;");
                    stmt.setInt(1,mapId);                                
                    stmt.executeUpdate();
                    
             }
               
             }

            stmt                            = transconn.prepareStatement("UPDATE bevPushMessageMap SET sent = 1;");
            stmt.executeUpdate();
                    
              
             

    	  } catch (Exception e) {
    		logger.debug(e.getMessage());
    	  } finally {
            close(rsDetails);
            close(rs);
            close(stmt);
        }
     }
     
    
     
     private boolean sendBevAndroidNotification(String senderId,String deviceToken,int type,int location,int messageId,String message,String validity) throws HandlerException {
          boolean sentNotifi                = false;
          try{
              Sender sender                 = new Sender(senderId);
              Message pushMessage           = new Message.Builder()
                                            .delayWhileIdle(false) // Wait for device to become active before sending.
                                            .addData( "message", message)   
                                            .addData( "location", String.valueOf(location) )
                                            .addData( "messageId", String.valueOf(messageId) )                                            
                                            .addData( "type", String.valueOf(type) )
                                            .addData( "validity", validity )
                                            .build();
              Result result                 = sender.send(pushMessage, deviceToken, 1);
              if(result.toString().contains("MismatchSenderId")){
                  sentNotifi                = false;
              }else{
                  sentNotifi                = true;
              }
          } catch (Exception e) {
              logger.debug(e.getMessage());
              return false;
          }
          return sentNotifi;
      }

      
     
     
      
      
    
        
          
     
     
     
     



}
