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
import java.io.*;
import java.text.SimpleDateFormat;
import net.terakeet.util.TemplatedMessage;
import net.terakeet.util.ParameterFile;
import net.terakeet.util.MailException;
import net.terakeet.soapware.handlers.report.*;
import net.terakeet.usbn.WebPermission;
import net.terakeet.util.pdfCrowd;
//import com.pdfcrowd.*;
import java.net.HttpURLConnection;
import java.net.URL;
import net.terakeet.util.printableMenu;
import javapns.Push;
import javapns.communication.exceptions.CommunicationException;
import javapns.communication.exceptions.KeystoreException;
import javapns.notification.PushNotificationPayload;
import javapns.notification.Payload;
import com.google.android.gcm.server.Message;
import com.google.android.gcm.server.Result;
import com.google.android.gcm.server.Sender;

public class SQLBevManagerMobileTransHandler implements Handler {
    
    private MidwareLogger logger;
    private static final String transConnName
                                            = "auper";
    private RegisteredConnection transconn;
    private SecureSession ss;
    private DecimalFormat cf;
    private LocationMap locationMap;
    private static SimpleDateFormat dateFormat 
                                            = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
    private static SimpleDateFormat dbDateFormat
                                            = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private static SimpleDateFormat newDateFormat 
                                            = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private static SimpleDateFormat timeFormat 
                                            = new SimpleDateFormat("hh:mm aa");
    
    private LineOffsetMap lineOffsetMap;
    private ProductMap productMap;
    private ChildLevelMap childLevelMap;
    private RegionExclusionMap regionExclusionMap;
    private BarMap barMap;
    private ParentLevelMap parentLevelMap;    
    private RegionProductMap regionProductMap;
        
    /**
     * Creates a new instance of SQLBOSSHandler
     */
    public SQLBevManagerMobileTransHandler() throws HandlerException {
        HandlerUtils.initializeClientKeyManager();
        logger                              = new MidwareLogger(SQLBOSSHandler.class.getName());
        transconn                           = null;
        locationMap                         = null;
        cf                                  = (DecimalFormat) NumberFormat.getInstance(Locale.US);
    }
    
    public void handle(Element toHandle, Element toAppend) throws HandlerException {
        
        String function                     = toHandle.getName();
        String responseNamespace            = (String)SOAPMessage.getURIMap().get("tkmsg");
        
        String clientKey                    = HandlerUtils.getOptionalString(toHandle,"clientKey");
        ss                                  = ClientKeyManager.getSession(clientKey);
        
        logger                              = new MidwareLogger(SQLBOSSHandler.class.getName(), function);
        logger.debug("SQLBevManagerMobileHandler processing method: "+function);
        logger.xml("request: " + toHandle.asXML());
        
        transconn                           = DatabaseConnectionManager.getNewConnection(transConnName, function + " (SQLBevManagerMobileTransHandler)");
        
        cf.applyPattern("#.####");
        try {
            // All methods require an admin client key
            if (ss.getLocation() == 0 && ss.getClientId() == 1 && ss.getSecurityLevel().canAdmin()) {
                 
                /*if ("updateBevMobileNotification".equals(function)) {
                    updateBevMobileNotification(toHandle, responseFor(function,toAppend));
                } else if ("addUpdateDeleteBeveragePlu".equals(function)) {
                    addUpdateDeleteBeveragePlu(toHandle, responseFor(function,toAppend));
                } else if ("updateGlanolaLineCleaning".equals(function)) {
                    updateGlanolaLineCleaning(toHandle, responseFor(function,toAppend));
                } else if ("addBevPushMessage".equals(function)) {
                    addBevPushMessage(toHandle, responseFor(function,toAppend));
                } else if ("addInventoryBeveragePrice".equals(function)) {
                    addInventoryBeveragePrice(toHandle, responseFor(function,toAppend));
                } else {
                    logger.generalWarning("Unknown function '" + function + "'.");
                }*/
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
            logger.dbAction("Executed " + queryCount + " transaction quer" + (queryCount == 1 ? "y" : "ies"));
            transconn.close();
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
    
    private void addErrorDetail(Element toAppend, String message) {
        toAppend.addElement("error").addElement("detail").addText(message);
    }
    private int getCallerId(Element toHandle)  {
        try {
        return HandlerUtils.getOptionalInteger(HandlerUtils.getRequiredElement(toHandle,"caller"),"callerId");
        } catch(Exception e){
            logger.debug(e.getMessage());
            return 0;
        }
        //return 0;
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
    
    public boolean isValidAccessUser( int callerId, int location, boolean isCustomer){
        boolean isValid                  = true;
        int user[]                      ={60,60,198,166,201,212,3302,347}; //203,199         
        for(int i=0;i<user.length;i++){
            if(user[i]==callerId){
                if(!isCustomer){
                    if(location != 425) {
                        isValid                     = false;
                    }
                } else {
                    if(location != 205) {
                        isValid                     = false;
                    }
                }
            }
        }
        return isValid;
    }

    public void addBevPushMessageMap(int messageId, int location, int user, int color, int type, String message) throws HandlerException {

        transconn                           = DatabaseConnectionManager.getNewConnection(transConnName, " (SQLBevManagerMobileTransHandler)");
        PreparedStatement stmt              = null;
        ResultSet rs                        = null, rsDetails = null;

        try {
            stmt                            = transconn.prepareStatement("SELECT id FROM bevPushMessageCenter p WHERE p.user=? AND pushMessage= ? AND location = ? AND type =?");
            stmt.setInt(1,user);
            stmt.setInt(2,messageId);
            stmt.setInt(3,location);
            stmt.setInt(4,type);
            rsDetails                       = stmt.executeQuery();
            if(rsDetails.next()){
                stmt                        = transconn.prepareStatement("UPDATE  bevPushMessageCenter  SET message = ?, color =?, lastUpdate=now() WHERE  pushMessage = ? AND location = ? AND type = ? AND  user =?");
                stmt.setString(1,message);
                stmt.setInt(2,color);
                stmt.setInt(3,messageId);
                stmt.setInt(4,location);
                stmt.setInt(5,type);
                stmt.setInt(6,user);
                stmt.executeUpdate();
            } else {
                stmt                        = transconn.prepareStatement("INSERT INTO bevPushMessageCenter (pushMessage, location, message, user, color,type, lastUpdate) VALUES (?,?,?,?,?,?, now());");
                stmt.setInt(1,messageId);
                stmt.setInt(2,location);
                stmt.setString(3,message);
                stmt.setInt(4,user);
                stmt.setInt(5,color);
                stmt.setInt(6,type);
                stmt.executeUpdate();
            }

            stmt                        = transconn.prepareStatement("SELECT id FROM bevPushMessageMap WHERE user=? AND pushMessage= ?");
            stmt.setInt(1,user);
            stmt.setInt(2,messageId);
            rsDetails                   = stmt.executeQuery();
            if(!rsDetails.next()){
                stmt                    = transconn.prepareStatement("INSERT INTO bevPushMessageMap (user, pushMessage, sent) VALUES (?,?,0);");
                stmt.setInt(1,user);
                stmt.setInt(2,messageId);
                stmt.executeUpdate();
            }



        } catch (SQLException sqle) {
            logger.dbError("Database error in bevPushMessagee: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(rsDetails);
            close(rs);
            close(stmt);
        }
        transconn.close();
    }



    public void addUserHistory(int userId, String action, int locationId, String message, int mobileId) throws HandlerException {
        transconn                           = DatabaseConnectionManager.getNewConnection(transConnName, " (SQLBevManagerMobileTransHandler)");
        String checkAction                  = " SELECT id FROM task WHERE abbrev=? LIMIT 1";
        String insertFullLog                = " INSERT INTO userHistoryMobile (user,task,description,location,mobile,timestamp) "
                                            + " VALUES (?,?,?,?,?,now()) ";
        PreparedStatement stmt              = null;
        ResultSet rs                        = null;

        try {
            // check that the action exists (has a task id)
            int taskId                      = 0;
            stmt                            = transconn.prepareStatement(checkAction);
            stmt.setString(1, action);
            rs                              = stmt.executeQuery();
            if (rs.next()) {
                taskId                      = rs.getInt(1);
            } else {
                // if the task doesn't exist, we'll insert it as task 0 and
                // prepend the name of the supplied task to the message;
                message                     = "UNKNOWN ("+action+") "+message;
            }

            stmt                            = transconn.prepareStatement(insertFullLog);
            stmt.setInt(1,userId);
            stmt.setInt(2,taskId);
            stmt.setString(3, message);
            stmt.setInt(4,locationId);
            stmt.setInt(5,mobileId);
            stmt.executeUpdate();
        } catch (SQLException sqle) {
            logger.dbError("Database error in getProductSet: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(rs);
            close(stmt);
        }
        transconn.close();
    }

    public void addUserVisitHistory(int userId, String action, int locationId, String message,int minute, int mobileId) throws HandlerException {
        transconn                           = DatabaseConnectionManager.getNewConnection(transConnName, " (SQLBevManagerMobileTransHandler)");
        String checkAction                  = " SELECT id FROM task WHERE abbrev=? LIMIT 1";
        String insertFullLog                = " INSERT INTO userHistoryMobile (user,task,description,location,mobile,timestamp) "
                                            + " VALUES (?,?,?,?,?,now()) ";
        String checkLog                     = "SELECT id FROM userHistoryMobile WHERE user=? AND task=? AND location = ? AND timestamp between DATE_SUB(now(), INTERVAL ? MINUTE ) AND now();";
        PreparedStatement stmt              = null;
        ResultSet rs                        = null;

        try {
            // check that the action exists (has a task id)
            int taskId                      = 0;
            stmt                            = transconn.prepareStatement(checkAction);
            stmt.setString(1, action);
            rs                              = stmt.executeQuery();
            if (rs.next()) {
                taskId                      = rs.getInt(1);
            } else {
                // if the task doesn't exist, we'll insert it as task 0 and
                // prepend the name of the supplied task to the message;
                message                     = "UNKNOWN ("+action+") "+message;
            }
            if(taskId > 0 && locationId > 0){
            stmt = transconn.prepareStatement(checkLog);
            stmt.setInt(1, userId);
            stmt.setInt(2, taskId);
            stmt.setInt(3, locationId);
            stmt.setInt(4, minute);
            rs = stmt.executeQuery();
            if (!rs.next()) {

            stmt                            = transconn.prepareStatement(insertFullLog);
            stmt.setInt(1,userId);
            stmt.setInt(2,taskId);
            stmt.setString(3, message);
            stmt.setInt(4,locationId);
            stmt.setInt(5,mobileId);
            stmt.executeUpdate();
            }
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error in addUserVisitHistory : " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(rs);
            close(stmt);
        }
        transconn.close();
    }

    public int insertBevMobileUser(int user, String deviceToken) throws HandlerException {
        transconn                           = DatabaseConnectionManager.getNewConnection(transConnName, " (SQLBevManagerMobileTransHandler)");
        int mobileId                        = 0;
        PreparedStatement stmt              = null;
        ResultSet rs                        = null;
        try {
            stmt            = transconn.prepareStatement("INSERT INTO bevMobileUser (user, deviceToken, registeredTime) Values(?,?,now()) ");
            stmt.setInt(1,user);
            stmt.setString(2,deviceToken);
            stmt.executeUpdate();

            stmt            = transconn.prepareStatement("SELECT LAST_INSERT_ID()");
            rs              = stmt.executeQuery();
            if (rs.next()) {
                mobileId    = rs.getInt(1);
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: "+sqle.toString());
            throw new HandlerException(sqle);
        } finally {
            close(rs);
            close(stmt);
        }
        transconn.close();
        return mobileId;
    }

    public void updateBevMobileUser(int userId) throws HandlerException {
        transconn                           = DatabaseConnectionManager.getNewConnection(transConnName, " (SQLBevManagerMobileTransHandler)");
        PreparedStatement stmt              = null;
        ResultSet rs                        = null;
        String sql                          = "UPDATE bevMobileUser SET lastAccess = NOW() WHERE id= ?;";
        try {
            stmt                            = transconn.prepareStatement(sql);
            stmt.setInt(1,userId);
            stmt.executeUpdate();
        } catch (SQLException sqle) {
            logger.dbError("Database error: "+sqle.toString());
            throw new HandlerException(sqle);
        } finally {
            close(rs);
            close(stmt);
        }
        transconn.close();
    }

    public void insertBevMobileLocationMap(int mobileUserId, int locationId) throws HandlerException {
        transconn                           = DatabaseConnectionManager.getNewConnection(transConnName, " (SQLBevManagerMobileTransHandler)");
        PreparedStatement stmt              = null;
        ResultSet rs                        = null;
        String sql                          = "INSERT INTO bevMobileLocationMap (user, location, active) VALUES (?,?,?);";
        try {
            stmt                            = transconn.prepareStatement(sql);
            stmt.setInt(1, mobileUserId);
            stmt.setInt(2, locationId);
            stmt.setInt(3, 0);
            stmt.executeUpdate();
        } catch (SQLException sqle) {
            logger.dbError("Database error: "+sqle.toString());
            throw new HandlerException(sqle);
        } finally {
            close(rs);
            close(stmt);
        }
        transconn.close();
    }

    public void updateBevMobileNotification(int active, int mobileUserId, int locationId) throws HandlerException {
        transconn                           = DatabaseConnectionManager.getNewConnection(transConnName, " (SQLBevManagerMobileTransHandler)");
        PreparedStatement stmt              = null;
        String sql                          = "UPDATE bevMobileLocationMap SET active= ? WHERE user = ? AND location = ?;";
        try {
            stmt                            = transconn.prepareStatement(sql);
            stmt.setInt(1,active);
            stmt.setInt(2,mobileUserId);
            stmt.setInt(3,locationId);
            stmt.executeUpdate();
        } catch (SQLException sqle) {
            logger.dbError("Database error: "+sqle.toString());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
        }
        transconn.close();
    }

    public void insertBevMobileNotification(int active, int mobileUserId, int locationId) throws HandlerException {
        transconn                           = DatabaseConnectionManager.getNewConnection(transConnName, " (SQLBevManagerMobileTransHandler)");
        PreparedStatement stmt              = null;
        String sql                          = "INSERT INTO bevMobileLocationMap (user,location,active) VALUES (?,?,?);";
        try {
            stmt                            = transconn.prepareStatement(sql);
            stmt.setInt(1,mobileUserId);
            stmt.setInt(2,locationId);
            stmt.setInt(3,active);
            stmt.executeUpdate();
        } catch (SQLException sqle) {
            logger.dbError("Database error: "+sqle.toString());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
        }
        transconn.close();
    }

    public void insertBeverage(String productName, int location, String plu, Double quantity, Double price, int productId) throws HandlerException {
        transconn                           = DatabaseConnectionManager.getNewConnection(transConnName, " (SQLBevManagerMobileTransHandler)");
        PreparedStatement stmt              = null;
        ResultSet rs                        = null;
        try {
            stmt                            = transconn.prepareStatement("INSERT INTO beverage (name, location, plu,ounces,price, simple, pType) VALUES (?,?,?,?,?,1,1)");
            stmt.setString(1, productName);
            stmt.setInt(2, location);
            stmt.setString(3, plu);
            stmt.setDouble(4, quantity);
            stmt.setDouble(5, price);
            stmt.executeUpdate();

            stmt                            = transconn.prepareStatement("SELECT LAST_INSERT_ID()");
            rs                              = stmt.executeQuery();
            if (rs.next()) {
                int beverage                = rs.getInt(1);
                String insertIng            = "INSERT INTO ingredient (beverage, product, ounces) VALUES (?,?,?);";
                stmt                        = transconn.prepareStatement(insertIng);
                stmt.setInt(1, beverage);
                stmt.setInt(2, productId);
                stmt.setDouble(3, quantity);
                stmt.executeUpdate();
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: "+sqle.toString());
            throw new HandlerException(sqle);
        } finally {
            close(rs);
            close(stmt);
        }
        transconn.close();
    }

    public void updateBeverage(String plu, double quantity, double price, int id) throws HandlerException {
        transconn                           = DatabaseConnectionManager.getNewConnection(transConnName, " (SQLBevManagerMobileTransHandler)");
        PreparedStatement stmt              = null;
        String sql                          = "UPDATE beverage SET plu = ?, ounces = ?, price = ? WHERE id = ?;";
        try {
            stmt                            = transconn.prepareStatement(sql);
            stmt.setString(1, plu);
            stmt.setDouble(2, quantity);
            stmt.setDouble(3, price);
            stmt.setInt(4, id);
            stmt.executeUpdate();
        } catch (SQLException sqle) {
            logger.dbError("Database error: "+sqle.toString());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
        }
        transconn.close();
    }

    public void deleteBeverage(int beverage) throws HandlerException {
        transconn                           = DatabaseConnectionManager.getNewConnection(transConnName, " (SQLBevManagerMobileTransHandler)");
        PreparedStatement stmt              = null;
        String deleteBeverage               = "DELETE FROM beverage WHERE id = ?";
        String deleteIngredient             = "DELETE FROM ingredient WHERE beverage = ?";
        try {
            stmt                            = transconn.prepareStatement(deleteBeverage);
            stmt.setInt(1, beverage);
            stmt.executeUpdate();
            
            stmt                            = transconn.prepareStatement(deleteIngredient);
            stmt.setInt(1, beverage);
            stmt.executeUpdate();
        } catch (SQLException sqle) {
            logger.dbError("Database error: "+sqle.toString());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
        }
        transconn.close();
    }

    public int insertBevPushMessage(String message) throws HandlerException {
        transconn                           = DatabaseConnectionManager.getNewConnection(transConnName, " (SQLBevManagerMobileTransHandler)");
        PreparedStatement stmt              = null;
        ResultSet rs                        = null;
        int messageId                       = 0;
        String sql                          = "INSERT INTO bevPushMessage (message, pushTime) VALUES (?, (SELECT IF(TIME(NOW()) > '08:59:00' , (SELECT CONCAT(DATE(ADDDATE(NOW(), INTERVAL 1 DAY)), ' 09:00:00')), (SELECT CONCAT(DATE(NOW()), ' 09:00:00')))));";
        try {
            stmt                            = transconn.prepareStatement(sql);
            stmt.setString(1, message);
            stmt.executeUpdate();
            stmt                            = transconn.prepareStatement("SELECT LAST_INSERT_ID()");
            rs                              = stmt.executeQuery();
            if (rs.next()) {
                messageId                   = rs.getInt(1);
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: "+sqle.toString());
            throw new HandlerException(sqle);
        } finally {
            close(rs);
            close(stmt);
        }
        transconn.close();
        return messageId;
    }

    public void insertPrices(int inventory, int size, Double value) throws HandlerException {
        transconn                           = DatabaseConnectionManager.getNewConnection(transConnName, " (SQLBevManagerMobileTransHandler)");
        PreparedStatement stmt              = null;
        String sql                          = "INSERT INTO inventoryPrices (inventory, size, value) VALUES (?,?,?);";
        try {
            stmt                            = transconn.prepareStatement(sql);
            stmt.setInt(1, inventory);
            stmt.setInt(2, size);
            stmt.setDouble(3, value);
            stmt.executeUpdate();
        } catch (SQLException sqle) {
            logger.dbError("Database error: "+sqle.toString());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
        }
        transconn.close();
    }

    public void updatePrices(int inventory, Double value) throws HandlerException {
        transconn                           = DatabaseConnectionManager.getNewConnection(transConnName, " (SQLBevManagerMobileTransHandler)");
        PreparedStatement stmt              = null;
        String sql                          = "UPDATE inventoryPrices SET value = ? WHERE id = ?;";
        try {
            stmt                            = transconn.prepareStatement(sql);
            stmt.setDouble(1, value);
            stmt.setInt(2, inventory);
            stmt.executeUpdate();
        } catch (SQLException sqle) {
            logger.dbError("Database error: "+sqle.toString());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
        }
        transconn.close();
    }

    public void deletePrices(int inventory, int size, Double value) throws HandlerException {
        transconn                           = DatabaseConnectionManager.getNewConnection(transConnName, " (SQLBevManagerMobileTransHandler)");
        PreparedStatement stmt              = null;
        String sql                          = "DELETE FROM inventoryPrices WHERE inventory = ? AND size = ? AND value = ?;";
        try {
            stmt                            = transconn.prepareStatement(sql);
            stmt.setInt(1, inventory);
            stmt.setInt(2, size);
            stmt.setDouble(3, value);
            stmt.executeUpdate();
        } catch (SQLException sqle) {
            logger.dbError("Database error: "+sqle.toString());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
        }
        transconn.close();
    }

    public void updateBBTV(int locationId) throws HandlerException {
        transconn                           = DatabaseConnectionManager.getNewConnection(transConnName, " (SQLBevManagerMobileTransHandler)");
        PreparedStatement stmt              = null;
        String sql                          = "UPDATE locationBeerBoardMap SET css = 1 WHERE location = ? ;";
        try {
            stmt                            = transconn.prepareStatement(sql);
            stmt.setInt(1, locationId);
            stmt.executeUpdate();
        } catch (SQLException sqle) {
            logger.dbError("Database error: "+sqle.toString());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
        }
        transconn.close();
    }

    public void updateGlanola(int glanola) throws HandlerException {
        transconn                           = DatabaseConnectionManager.getNewConnection(transConnName, " (SQLBevManagerMobileTransHandler)");
        PreparedStatement stmt              = null;
        String sql                          = "UPDATE glanolaLineCleaning SET endTime = NOW() WHERE id = ? ;";
        try {
            stmt                            = transconn.prepareStatement(sql);
            stmt.setInt(1, glanola);
            stmt.executeUpdate();
        } catch (SQLException sqle) {
            logger.dbError("Database error: "+sqle.toString());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
        }
        transconn.close();
    }

    public void updateGlanolaEnd(int glanola) throws HandlerException {
        transconn                           = DatabaseConnectionManager.getNewConnection(transConnName, " (SQLBevManagerMobileTransHandler)");
        PreparedStatement stmt              = null;
        String sql                          = "UPDATE glanolaLineCleaning SET endTime = ADDDATE(NOW(), INTERVAL 30 MINUTE) WHERE id = ?;";
        try {
            stmt                            = transconn.prepareStatement(sql);
            stmt.setInt(1, glanola);
            stmt.executeUpdate();
        } catch (SQLException sqle) {
            logger.dbError("Database error: "+sqle.toString());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
        }
        transconn.close();
    }

    public void insertGlanola(int locationId, int lineId) throws HandlerException {
        transconn                           = DatabaseConnectionManager.getNewConnection(transConnName, " (SQLBevManagerMobileTransHandler)");
        PreparedStatement stmt              = null;
        String sql                          = "INSERT INTO glanolaLineCleaning (location, line, startTime, endTime) "
                                            + " VALUES (?, ?, NOW(), ADDDATE(NOW(), INTERVAL 30 MINUTE));";
        try {
            stmt                            = transconn.prepareStatement(sql);
            stmt.setInt(1, locationId);
            stmt.setInt(2, lineId);
            stmt.executeUpdate();
        } catch (SQLException sqle) {
            logger.dbError("Database error: "+sqle.toString());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
        }
        transconn.close();
    }
    
    
    private void mGetProductTab(Element toHandle, Element toAppend) throws HandlerException {
        int callerId                       = getCallerId(toHandle);
        int mobileUserId                   = HandlerUtils.getOptionalInteger(toHandle, "mobileUserId");
        int locationId                     = HandlerUtils.getOptionalInteger(toHandle, "locationId");
        int type                           = HandlerUtils.getOptionalInteger(toHandle, "type");
        int hide                           = HandlerUtils.getOptionalInteger(toHandle, "hide");
         

        HashMap<Integer, String> BrassTapProductName
                                            = new HashMap<Integer, String>();
        BrassTapProductName.put(0, "Unknown Product");

        PreparedStatement stmt              = null;
        ResultSet rs                        = null;
        String selectLineProduct            ="SELECT GROUP_CONCAT(l.product) FROM line l  LEFT JOIN bar b ON b.id = l.bar WHERE b.location=? AND l.product NOT IN (10661,4311,9593) AND l.status='RUNNING'";
        String selectOnDekProduct           ="SELECT GROUP_CONCAT(product) FROM comingSoonProducts WHERE location=? AND product NOT IN (10661,4311,9593) ";
        String productCondition             = "";
         int isAdmin                        = 0;   
        try {
            if(callerId > 0) {
                 stmt                       = transconn.prepareStatement("SELECT name FROM user WHERE id = ? AND customer=0 AND isManager=1;");
                 stmt.setInt(1, callerId);
                 rs                         = stmt.executeQuery();
                 if (rs.next()) {
                     toAppend.addElement("isAdmin").addText(String.valueOf(1));
                     isAdmin                = 1;
                 }else {
                     toAppend.addElement("isAdmin").addText(String.valueOf(0));
                 }
                
            }
            if (mobileUserId > 0) {
                String checkBrassTapUser    = "SELECT u.id, IFNULL(bU.id, 0) FROM bevMobileUser b LEFT JOIN user u ON u.id = b.user " +
                                           " LEFT JOIN brasstapUser bU ON bU.user = u.id WHERE b.id = ? AND u.customer IN (254, 271, 269);";
                stmt                        = transconn.prepareStatement(checkBrassTapUser);
                stmt.setInt(1, mobileUserId);
                rs                          = stmt.executeQuery();
                if (rs.next()) {
                    String selectBrassTapProductName
                                            = "SELECT usbnID, name FROM brasstapProducts WHERE usbnID > 0";
                    stmt                    = transconn.prepareStatement(selectBrassTapProductName);
                    rs                      = stmt.executeQuery();
                    while (rs.next()) {
                        BrassTapProductName.put(rs.getInt(1), rs.getString(2));
                    }
                }
            }
            if(locationId > 0){
                stmt = transconn.prepareStatement(selectLineProduct);
                stmt.setInt(1, locationId);                
                rs = stmt.executeQuery();
                if(rs.next()){
                    String products         = HandlerUtils.nullToEmpty(rs.getString(1));
                    if(products!=null && !products.equals("")){
                        productCondition    = products;
                    }
                }
                stmt = transconn.prepareStatement(selectOnDekProduct);
                stmt.setInt(1, locationId);                
                rs = stmt.executeQuery();
                if(rs.next()){
                    String products         = HandlerUtils.nullToEmpty(rs.getString(1));
                   // logger.debug("OnDek"+products);
                    if(products!=null && !products.equals("")){
                        if(!productCondition.equals("")){
                            productCondition    +=","+ products;
                        }else {
                            productCondition    = products;
                        }
                    }
                }
                if(!productCondition.equals("")){
                    productCondition            = " AND i.product IN("+productCondition+") ";
                }
                //logger.debug(productCondition);
                }
                
                
                
            
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(rs);
            close(stmt);
        }
         if(type==1){
             getCurrentLines(toHandle, toAppend, BrassTapProductName,isAdmin,hide);
         } else if(type==2){
             getTestInventory(toHandle,toAppend, BrassTapProductName, productCondition);             
         } else if(type==3){
             getBeverageSizes(locationId, toAppend);
         } else {
             getTestInventory(toHandle, toAppend, BrassTapProductName,productCondition);
             getCurrentLines(toHandle, toAppend, BrassTapProductName,isAdmin,hide);
             getBeverageSizes(locationId, toAppend);
         }
         
         if(callerId>0) {
             if(mobileUserId <1){
                 mobileUserId               = getMobileUserId(callerId);
             }
             addUserVisitHistory(callerId, "getCurrentLines", locationId, "Draft Line Access",15, mobileUserId);
          }
         //getBars(toHandle, toAppend);

       
     }
    
    private int  getMobileUserId(int userId) throws HandlerException {
         
        
         PreparedStatement stmt             = null;
         ResultSet rs                       = null;
         int mobileUserId                   = 0;
         String sql                         = "SELECT id,user FROM bevMobileUser WHERE user=?  ORDER by id DESC LIMIT 1;";
        try {
            stmt                            = transconn.prepareStatement(sql);
            stmt.setInt(1,userId);
            rs                              = stmt.executeQuery();
            if(rs.next()) {
                mobileUserId                = rs.getInt(1);
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: "+sqle.toString());
            throw new HandlerException(sqle);
        } finally {
            
            close(stmt);
            close(rs);
        }
        return mobileUserId;
    }
     
    
    
    
    private void getTestInventory(Element toHandle, Element toAppend, HashMap<Integer, String> BrassTapProductName, String productCondition) throws HandlerException {

        //NischaySharma_11-Feb-2009_Start: Added Extraction of supplierid
        int supplierId = HandlerUtils.getOptionalInteger(toHandle, "supplierId");
        //NischaySharma_11-Feb-2009_End
        //int callerId                      = getCallerId(toHandle);
          //int mobileUserId                  = HandlerUtils.getOptionalInteger(toHandle, "mobileUserId");
        int refLocationId = HandlerUtils.getOptionalInteger(toHandle, "locationId");
        int refCustomerId = HandlerUtils.getOptionalInteger(toHandle, "customerId");
        int userId      = HandlerUtils.getOptionalInteger(toHandle, "userId");
        int locationId = HandlerUtils.getOptionalInteger(toHandle, "location");
        int zoneId = HandlerUtils.getOptionalInteger(toHandle, "zoneId");
        int barId = HandlerUtils.getOptionalInteger(toHandle, "barId");
        int stationId = HandlerUtils.getOptionalInteger(toHandle, "stationId");
        int coolerId = HandlerUtils.getOptionalInteger(toHandle, "coolerId");
        int regionId = HandlerUtils.getOptionalInteger(toHandle, "regionId");
        int prodType = HandlerUtils.getRequiredInteger(toHandle, "prodID");
        boolean getActive = HandlerUtils.getOptionalBoolean(toHandle, "getActive");
        boolean getDetails = HandlerUtils.getOptionalBoolean(toHandle, "getDetails");

        String isActive = getActive ? " AND i.isActive = 1 " : " ";
        PreparedStatement stmt = null;
        ResultSet rs = null;

        try {
            
            String selectSupplier           ="SELECT s.id, s.name, a.active " +
                                            " FROM locationSupplier map LEFT JOIN supplierAddress a ON map.address = a.id" +
                                            " LEFT JOIN supplier s ON a.supplier=s.id WHERE map.location=? "
                                            + " UNION (SELECT  id, name,1 FROM supplier WHERE id=2);";
            
             stmt                           = transconn.prepareStatement(selectSupplier);
             stmt.setInt(1, refLocationId);
             rs                             = stmt.executeQuery();
              while (rs.next()) {
                  Element supplierE1 = toAppend.addElement("supplier");                  
                  supplierE1.addElement("supplierId").addText(String.valueOf(rs.getInt(1)));
                  supplierE1.addElement("supplierName").addText(HandlerUtils.nullToEmpty(rs.getString(2)));
                  supplierE1.addElement("active").addText(String.valueOf(rs.getInt(3)));
              }
              
              
            if (refCustomerId >= 0) {
                if (!checkForeignKey("customer", "id", refCustomerId)) {
                    throw new HandlerException("Foreign Key Not found : customer " + refCustomerId);
                }
                String selectByCustomerId = "SELECT i.id, i.product, p.name, " +
                        " i.location, ROUND(IF(i.qtyOnHand < 0, 0.0, i.qtyOnHand),2), i.minimumQty, i.plu, i.qtyToHave," +
                        " sup.name, sup.id, i.kegSize, i.brixWater, i.brixSyrup, p.pType," +
                        " i.bottleSize, b.name, c.id, c.name, i.kegLine, k.name, p.segment, p.category " +
                        " FROM product AS p LEFT JOIN inventory AS i ON p.id = i.product LEFT JOIN location l ON i.location = l.id " +
                        " LEFT JOIN supplier AS sup ON i.supplier = sup.id LEFT JOIN bottleSize AS b ON i.bottleSize = b.id " +
                        " LEFT JOIN kegLine AS k ON k.id = i.kegLine " +
                        " LEFT JOIN cooler AS c ON k.cooler = c.id " +
                        " WHERE l.customer = ? and p.pType = ? " + isActive;
                stmt = transconn.prepareStatement(selectByCustomerId);
                stmt.setInt(1, refCustomerId);
                stmt.setInt(2, prodType);
                rs = stmt.executeQuery();
                getTestInventoryXML(toAppend, rs, getDetails, true, BrassTapProductName);
            } else if (refLocationId >= 0) {
                if (!checkForeignKey("location", "id", refLocationId)) {
                    throw new HandlerException("Foreign Key Not found : location " + refLocationId);
                }
                String selectByLocationId = "SELECT i.id, i.product, p.name, " +
                        " i.location, ROUND(IF(i.qtyOnHand < 0, 0.0, i.qtyOnHand),2), i.minimumQty, i.plu, i.qtyToHave," +
                        " sup.name, sup.id, i.kegSize, i.brixWater, i.brixSyrup, p.pType," +
                        " i.bottleSize, b.name, c.id, c.name, i.kegLine, k.name, p.segment, p.category " +
                        " FROM product AS p LEFT JOIN inventory AS i ON p.id = i.product " +
                        " LEFT JOIN supplier AS sup ON i.supplier = sup.id LEFT JOIN bottleSize AS b ON i.bottleSize = b.id " +
                        " LEFT JOIN kegLine AS k ON k.id = i.kegLine " +
                        " LEFT JOIN cooler AS c ON k.cooler = c.id " +
                        " WHERE i.location = ? and p.pType = ? " +productCondition+ isActive;
                stmt = transconn.prepareStatement(selectByLocationId);
                stmt.setInt(1, refLocationId);
                stmt.setInt(2, prodType);
                rs = stmt.executeQuery();
                getTestInventoryXML(toAppend, rs, getDetails, false, BrassTapProductName);
            } else if (userId >= 0) {
                if (!checkForeignKey("user", "id", userId)) {
                    throw new HandlerException("Foreign Key Not found : user " + userId);
                }
                String selectByLocationId = "SELECT i.id, i.product, p.name, " +
                        " i.location, IF(i.qtyOnHand < 0, 0.0, i.qtyOnHand), i.minimumQty, i.plu, i.qtyToHave," +
                        " sup.name, sup.id, i.kegSize, i.brixWater, i.brixSyrup, p.pType," +
                        " i.bottleSize, b.name, c.id, c.name, i.kegLine, k.name, p.segment, p.category " +
                        " FROM product AS p LEFT JOIN inventory AS i ON p.id = i.product " +
                        " LEFT JOIN supplier AS sup ON i.supplier = sup.id LEFT JOIN bottleSize AS b ON i.bottleSize = b.id " +
                        " LEFT JOIN kegLine AS k ON k.id = i.kegLine " +
                        " LEFT JOIN cooler AS c ON k.cooler = c.id " +
                        " LEFT JOIN userMap uM ON uM.location = i.location WHERE uM.user = ? and p.pType = ? " + isActive;
                stmt = transconn.prepareStatement(selectByLocationId);
                stmt.setInt(1, userId);
                stmt.setInt(2, prodType);
                rs = stmt.executeQuery();
                getTestInventoryXML(toAppend, rs, getDetails, true, BrassTapProductName);
            } else if (coolerId >= 0) {
                if (!checkForeignKey("cooler", "id", coolerId)) {
                    throw new HandlerException("Foreign Key Not found : cooler " + coolerId);
                }
                String selectByCoolerId = "SELECT i.id, i.product, p.name, " +
                        " i.location, IF(i.qtyOnHand < 0, 0.0, i.qtyOnHand), i.minimumQty, i.plu, i.qtyToHave," +
                        " sup.name, sup.id, i.kegSize, i.brixWater, i.brixSyrup, p.pType, " +
                        " i.bottleSize, b.name, c.id, c.name, i.kegLine, k.name, p.segment, p.category " +
                        " FROM product AS p LEFT JOIN inventory AS i ON p.id = i.product " +
                        " LEFT JOIN supplier AS sup ON i.supplier = sup.id LEFT JOIN bottleSize AS b ON i.bottleSize = b.id " +
                        " LEFT JOIN kegLine AS k ON k.id = i.kegLine " +
                        " LEFT JOIN cooler AS c ON k.cooler = c.id " +
                        " WHERE k.cooler = ? and p.pType = ? " + isActive;
                stmt = transconn.prepareStatement(selectByCoolerId);
                stmt.setInt(1, coolerId);
                stmt.setInt(2, prodType);
                rs = stmt.executeQuery();
                getTestInventoryXML(toAppend, rs, getDetails, true, BrassTapProductName);
            } else if (zoneId >= 0) {
                if (!checkForeignKey("zone", "id", zoneId)) {
                    throw new HandlerException("Foreign Key Not found : zone " + zoneId);
                }
                String selectByZoneId = "SELECT DISTINCT i.id, i.product, p.name, " +
                        " i.location, IF(i.qtyOnHand < 0, 0.0, i.qtyOnHand), i.minimumQty, i.plu, i.qtyToHave," +
                        " sup.name, sup.id, i.kegSize, i.brixWater, i.brixSyrup, p.pType, " +
                        " i.bottleSize, b.name, c.id, c.name, i.kegLine, k.name, p.segment, p.category  " +
                        " FROM product AS p LEFT JOIN inventory AS i ON p.id = i.product " +
                        " LEFT JOIN supplier AS sup ON i.supplier = sup.id LEFT JOIN bottleSize AS b ON i.bottleSize = b.id" +
                        " LEFT JOIN kegLine AS k ON k.id = i.kegLine " +
                        " LEFT JOIN cooler AS c ON k.cooler = c.id " +
                        " WHERE i.location = ? AND c.zone=? and p.pType = ? " + isActive;
                stmt = transconn.prepareStatement(selectByZoneId);
                stmt.setInt(1, locationId);
                stmt.setInt(2, zoneId);
                stmt.setInt(3, prodType);
                rs = stmt.executeQuery();
                getTestInventoryXML(toAppend, rs, getDetails, true, BrassTapProductName);
            } else if (barId >= 0) {
                if (!checkForeignKey("bar", "id", barId)) {
                    throw new HandlerException("Foreign Key Not found : bar " + barId);
                }
                String selectByBarId = "SELECT DISTINCT i.id, i.product, p.name, " +
                        " i.location, IF(i.qtyOnHand < 0, 0.0, i.qtyOnHand), i.minimumQty, i.plu, i.qtyToHave," +
                        " sup.name, sup.id, i.kegSize, i.brixWater, i.brixSyrup, p.pType, " +
                        " i.bottleSize, b.name, c.id, c.name, i.kegLine, k.name, p.segment, p.category " +
                        " FROM product AS p LEFT JOIN inventory AS i ON p.id = i.product " +
                        " LEFT JOIN supplier AS sup ON i.supplier = sup.id LEFT JOIN bottleSize AS b ON i.bottleSize = b.id" +
                        " LEFT JOIN kegLine AS k ON k.id = i.kegLine " +
                        " LEFT JOIN cooler AS c ON k.cooler = c.id LEFT JOIN bar ba ON ba.cooler = c.id" +
                        " WHERE i.location = ? AND ba.id = ? AND p.pType = ? " + isActive;
                stmt = transconn.prepareStatement(selectByBarId);
                stmt.setInt(1, locationId);
                stmt.setInt(2, barId);
                stmt.setInt(3, prodType);
                rs = stmt.executeQuery();
                getTestInventoryXML(toAppend, rs, getDetails, true, BrassTapProductName);
            } else if (stationId >= 0) {
                if (!checkForeignKey("station", "id", stationId)) {
                    throw new HandlerException("Foreign Key Not found : station " + stationId);
                }
                String selectByStationId = "SELECT DISTINCT i.id, i.product, p.name, " +
                        " i.location, IF(i.qtyOnHand < 0, 0.0, i.qtyOnHand), i.minimumQty, i.plu, i.qtyToHave," +
                        " sup.name, sup.id, i.kegSize, i.brixWater, i.brixSyrup, p.pType, " +
                        " i.bottleSize, b.name, c.id, c.name, i.kegLine, k.name, p.segment, p.category " +
                        " FROM product AS p LEFT JOIN inventory AS i ON p.id = i.product " +
                        " LEFT JOIN supplier AS sup ON i.supplier = sup.id LEFT JOIN bottleSize AS b ON i.bottleSize = b.id" +
                        " LEFT JOIN kegLine AS k ON k.id = i.kegLine " +
                        " LEFT JOIN cooler AS c ON k.cooler = c.id  LEFT JOIN bar ba ON ba.cooler = c.id LEFT JOIN station s ON s.bar = ba.id " +
                        " WHERE i.location = ? AND s.id = ? AND p.pType = ? " + isActive;
                stmt = transconn.prepareStatement(selectByStationId);
                stmt.setInt(1, locationId);
                stmt.setInt(2, stationId);
                stmt.setInt(3, prodType);
                rs = stmt.executeQuery();
                getTestInventoryXML(toAppend, rs, getDetails, true, BrassTapProductName);
            } //NischaySharma_11-Feb-2009_Start: Added check to verify and generate response according to
            // supplierId
            else if (supplierId >= 0) {
                if (!checkForeignKey("supplier", "id", supplierId)) {
                    throw new HandlerException("Foreign Key Not found : supplier " + supplierId);
                }
                String selectBySupplierId = "SELECT i.id, i.product, p.name, " +
                        " i.location, IF(i.qtyOnHand < 0, 0.0, i.qtyOnHand), i.minimumQty, i.plu, i.qtyToHave," +
                        " sup.name, sup.id, i.kegSize, i.brixWater, i.brixSyrup, p.pType," +
                        " i.bottleSize, b.name, c.id, c.name, i.kegLine, k.name, p.segment, p.category " +
                        " FROM product AS p LEFT JOIN inventory AS i ON p.id = i.product " +
                        " LEFT JOIN supplier AS sup ON i.supplier = sup.id LEFT JOIN bottleSize AS b ON i.bottleSize = b.id " +
                        " LEFT JOIN kegLine AS k ON k.id = i.kegLine " +
                        " LEFT JOIN cooler AS c ON k.cooler = c.id " +
                        " WHERE sup.id = ? and p.pType = ? " + isActive;
                stmt = transconn.prepareStatement(selectBySupplierId);
                stmt.setInt(1, supplierId);
                stmt.setInt(2, prodType);
                rs = stmt.executeQuery();
                getTestInventoryXML(toAppend, rs, getDetails, true, BrassTapProductName);
            }
            //NischaySharma_11-Feb-2009_End
            // regionId
            else if (regionId >= 0) {
                if (!checkForeignKey("region", "id", regionId)) {
                    throw new HandlerException("Foreign Key Not found : region " + regionId);
                }

                //To fetch data for only the required locations
                int j = 0;
                String userLocationRequired = " ";
                Iterator i = toHandle.elementIterator("reqLocations");
                while (i.hasNext()) {
                    Element el = (Element) i.next();
                    if (j == 0) {
                        userLocationRequired += " AND l.id IN (" + String.valueOf(HandlerUtils.getRequiredInteger(el, "locationId"));
                        j++;
                    } else {
                        userLocationRequired += ", " + String.valueOf(HandlerUtils.getRequiredInteger(el, "locationId"));
                    }
                }
                if (j > 0) {
                    userLocationRequired += ") ";
                }

                String selectByRegionId = "SELECT i.id, i.product, p.name, " +
                        " i.location, IF(i.qtyOnHand < 0, 0.0, i.qtyOnHand), i.minimumQty, i.plu, i.qtyToHave," +
                        " sup.name, sup.id, i.kegSize, i.brixWater, i.brixSyrup, p.pType," +
                        " i.bottleSize, b.name, c.id, c.name, i.kegLine, k.name, p.segment, p.category " +
                        " FROM product AS p LEFT JOIN inventory AS i ON p.id = i.product " +
                        " LEFT JOIN supplier AS sup ON i.supplier = sup.id LEFT JOIN bottleSize AS b ON i.bottleSize = b.id " +
                        " LEFT JOIN kegLine AS k ON k.id = i.kegLine LEFT JOIN cooler AS c ON k.cooler = c.id " +
                        " LEFT JOIN location l ON l.id = i.location LEFT JOIN regionCountyMap rCM ON rCM.county = l.countyIndex " +
                        " LEFT JOIN groupRegionMap gRM ON gRM.regionMaster = rCM.region LEFT JOIN region r ON r.regionGroup = gRM.id " +
                        " WHERE r.id = ? AND p.pType = ? " + isActive + userLocationRequired;
                stmt = transconn.prepareStatement(selectByRegionId);
                stmt.setInt(1, regionId);
                stmt.setInt(2, prodType);
                rs = stmt.executeQuery();
                getTestInventoryXML(toAppend, rs, getDetails, true, BrassTapProductName);
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(rs);
            close(stmt);
        }
    }

   
    private void getTestInventoryXML(Element toAppend, ResultSet rs, boolean getDetails, boolean isCustomer, HashMap<Integer, String> BrassTapProductName) throws SQLException {
        while (rs.next()) {
            Element InventoryE1             = toAppend.addElement("inventoryItem");
            InventoryE1.addElement("inventoryId").addText(String.valueOf(rs.getInt(1)));
            InventoryE1.addElement("productId").addText(String.valueOf(rs.getInt(2)));
            String productName              = (BrassTapProductName.containsKey(rs.getInt(2)) ? BrassTapProductName.get(rs.getInt(2)) : rs.getString(3));
            InventoryE1.addElement("productName").addText(HandlerUtils.nullToEmpty(productName));
            InventoryE1.addElement("locationId").addText(String.valueOf(rs.getInt(4)));
            InventoryE1.addElement("qtyOnHand").addText(HandlerUtils.nullToEmpty(rs.getString(5)));
            InventoryE1.addElement("minimumQty").addText(String.valueOf(rs.getDouble(6)));
            InventoryE1.addElement("pluCode").addText(HandlerUtils.nullToEmpty(rs.getString(7)));
            InventoryE1.addElement("qtyToOrder").addText(String.valueOf(rs.getInt(8)));
            InventoryE1.addElement("supplierName").addText(HandlerUtils.nullToEmpty(rs.getString(9)));
            InventoryE1.addElement("supplierId").addText(String.valueOf(rs.getInt(10)));
            //InventoryE1.addElement("kegSize").addText(String.valueOf(rs.getInt(11)));
            //InventoryE1.addElement("brixWater").addText(String.valueOf(rs.getInt(12)));
            //InventoryE1.addElement("brixSyrup").addText(String.valueOf(rs.getInt(13)));
            InventoryE1.addElement("prodType").addText(String.valueOf(rs.getInt(14)));
            //InventoryE1.addElement("bottleSizeId").addText(String.valueOf(rs.getInt(15)));
            //InventoryE1.addElement("bottleSizeName").addText(HandlerUtils.nullToEmpty(rs.getString(16)));
            //InventoryE1.addElement("coolerId").addText(HandlerUtils.nullToEmpty(rs.getString(17)));
           // InventoryE1.addElement("cooler").addText(HandlerUtils.nullToEmpty(rs.getString(18)));
            //InventoryE1.addElement("kegLineId").addText(HandlerUtils.nullToEmpty(rs.getString(19)));
            //InventoryE1.addElement("kegLine").addText(HandlerUtils.nullToEmpty(rs.getString(20)));
           // InventoryE1.addElement("segment").addText(HandlerUtils.nullToEmpty(rs.getString(21)));
            //InventoryE1.addElement("category").addText(HandlerUtils.nullToEmpty(rs.getString(22)));
        }

    }
    
    
    private void getCurrentLines(Element toHandle, Element toAppend, HashMap<Integer, String> BrassTapProductName,int isAdmin, int hide) throws HandlerException {
        
        //int callerId                      = getCallerId(toHandle);
        //int mobileUserId                  = HandlerUtils.getOptionalInteger(toHandle, "mobileUserId");
        

        int lineId                          = HandlerUtils.getOptionalInteger(toHandle, "lineId");
        int systemId                        = HandlerUtils.getOptionalInteger(toHandle, "systemId");
        int zoneId                          = HandlerUtils.getOptionalInteger(toHandle, "zoneId");
        int barId                           = HandlerUtils.getOptionalInteger(toHandle, "barId");
        int stationId                       = HandlerUtils.getOptionalInteger(toHandle, "stationId");
        int locationId                      = HandlerUtils.getOptionalInteger(toHandle, "locationId");
        String getRetiredStr                = HandlerUtils.getOptionalString(toHandle, "getRetired");
        String time                         = HandlerUtils.getRequiredString(toHandle, "time");
        boolean getRetired                  = !("true".equalsIgnoreCase(getRetiredStr));
        int paramsSet                       = 0, parameter = 0;

        String tableName                    = null; 
        String selectLines                  = " SELECT l.id, l.lineIndex, l.product, p.name, l.system, l.bar, l.ouncesPoured, l.unit, l.status, " +
                                            " l.local, s.systemId, l.station, l.kegLine, lo.easternOffset, l.advertise, ROUND(IF(l.qtyOnHand < 0, 0.0, l.qtyOnHand),2),"
                                            + " (SELECT kegSize from inventory WHERE location = lo.id AND product = p.id ORDER BY id LIMIT 1),l.cask, l.lineNo," +
                                            "   IFNULL((SELECT 1 FROM glanolaLineCleaning WHERE location = b.location AND line=l.id AND ? BETWEEN startTime and endTime LIMIT 1),0) FROM line as l LEFT JOIN product AS p ON p.id = l.product " +
                                            " LEFT JOIN bar b ON b.id = l.bar LEFT JOIN location lo ON lo.id = b.location LEFT JOIN system s ON s.id = l.system ";
        String selectBar                    = "SELECT id, name,  zone, latitude, longitude, type FROM bar WHERE location = ?";
        String selectKegSize                = "Select name, size FROM kegSize order by size";
        if (lineId > 0) {
            paramsSet++;
            parameter                       = lineId;
            tableName                       = "line";
            selectLines                     += " WHERE l.id = ? ";
        }

        if (systemId > 0) {
            paramsSet++;
            parameter                       = systemId;
            tableName                       = "system";
            selectLines                     += " WHERE s.id = ? ";
        }

        if (zoneId > 0) {
            paramsSet++;
            parameter                       = zoneId;
            tableName                       = "zone";
            selectLines                     += " WHERE b.zone = ? ";
        }

        if (barId > 0) {
            paramsSet++;
            parameter                       = barId;
            tableName                       = "bar";
            selectLines                     += " WHERE b.id = ? ";
        }

        if (stationId > 0) {
            paramsSet++;
            parameter                       = stationId;
            tableName                       = "station";
            selectLines                     += " WHERE l.station = ? ";
        }

        if (locationId >= 0) {
            paramsSet++;
            parameter                       = locationId;
            tableName                       = "location";
            selectLines                     += " WHERE lo.id = ? ";
        }

        if (paramsSet > 1) {
            throw new HandlerException("Only one of the following parameters can be set for getCurrentLines: lineId systemId barId locationId");
        }

        PreparedStatement stmt              = null;
        ResultSet rs                        = null;

        try {

            if (!checkForeignKey(tableName, "id", parameter)) { throw new HandlerException("Foreign Key Not found : " + tableName + "-" + parameter); }
            selectLines                     += (getRetired ? "" : " AND l.status <> ?");
            selectLines                     += " AND l.lineNo != '0' ORDER BY l.lineNo ";
            
            stmt                            = transconn.prepareStatement(selectLines);
            stmt.setString(1, time);
            stmt.setInt(2, parameter);
            if (!getRetired) { stmt.setString(3, "RETIRED"); }
            rs                              = stmt.executeQuery();
            getLineXML(toAppend, rs, BrassTapProductName, isAdmin,hide);
            
            String selectLineCleaning           = "SELECT id,TIMESTAMPDIFF(MINUTE,startTime,endTime) FROM lineCleaning WHERE location = ? AND ? BETWEEN startTime and endTime";        
            String selectGlanolaLineCleaning     = "SELECT id,TIMESTAMPDIFF(MINUTE,startTime,endTime) FROM glanolaLineCleaning WHERE location = ? AND ? BETWEEN startTime and endTime";
            stmt                            = transconn.prepareStatement(selectLineCleaning);
            stmt.setInt(1, locationId);
            stmt.setString(2,time);
            rs                              = stmt.executeQuery();
            if (rs.next()) {
                toAppend.addElement("state").addText("true");
                 toAppend.addElement("time").addText(HandlerUtils.nullToEmpty(rs.getString(2)));
            } else {
                stmt                            = transconn.prepareStatement(selectGlanolaLineCleaning);
                stmt.setInt(1, locationId);
                stmt.setString(2, time);
                rs                              = stmt.executeQuery();
                if (rs.next()) {
                    toAppend.addElement("state").addText("true");
                    toAppend.addElement("time").addText(HandlerUtils.nullToEmpty(rs.getString(2)));
                } else {
                    toAppend.addElement("state").addText("false");
                   // toAppend.addElement("cleanTime").addText("120");
                }
            }
            
            stmt                            = transconn.prepareStatement(selectBar);
            stmt.setInt(1, locationId);            
            rs                              = stmt.executeQuery();
            while (rs.next()) {
                Element barE1                  = toAppend.addElement("bar");
                barE1.addElement("barId").addText(String.valueOf(rs.getInt(1)));
                barE1.addElement("barName").addText(HandlerUtils.nullToEmpty(rs.getString(2)));
            }
            
            stmt = transconn.prepareStatement(selectKegSize);
            rs = stmt.executeQuery();
            while (rs.next()) {
                Element sup = toAppend.addElement("kegSize");
                sup.addElement("name").addText(HandlerUtils.nullToEmpty(rs.getString(1)));
                sup.addElement("size").addText(HandlerUtils.nullToEmpty(rs.getString(2)));
            }
            
            String cleanHours[]= {"15 Minutes", "30 Minutes","45 Minutes" ,"1 Hour","1.5 Hours", "2 Hours","2.5 Hours","3 Hours","3.5 Hours","4 Hours","4.5 Hours","5 Hours"};
            int cleanMinutes[]= {15, 30,45,60,90,120,150,180,210,240,270,300};
            for(int i=0;i<cleanHours.length;i++) {
                Element sup = toAppend.addElement("cleanHours");
                sup.addElement("name").addText(cleanHours[i]);
                sup.addElement("value").addText(String.valueOf(cleanMinutes[i]));
                
            }
        
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(rs);
            close(stmt);
        }
    }

    private void getLineXML(Element toAppend, ResultSet rs, HashMap<Integer, String> BrassTapProductName,int isAdmin,int hide) throws SQLException {
        while (rs.next()) {
            String lineNo                   = HandlerUtils.nullToEmpty(rs.getString(19));
            boolean canSend                 = true;
            if(isAdmin==0 && hide >0 && lineNo.equals("0")){
                canSend                     = false;
            }
            if(canSend){
            Element lineE1                  = toAppend.addElement("line");
            lineE1.addElement("lineId").addText(String.valueOf(rs.getInt(1)));
            lineE1.addElement("lineIndex").addText(String.valueOf(rs.getInt(2)));
            lineE1.addElement("productId").addText(String.valueOf(rs.getInt(3)));
            String productName              = (BrassTapProductName.containsKey(rs.getInt(3)) ? BrassTapProductName.get(rs.getInt(3)) : rs.getString(4));
            lineE1.addElement("productName").addText(HandlerUtils.nullToEmpty(productName));
            lineE1.addElement("systemId").addText(String.valueOf(rs.getInt(5)));
            lineE1.addElement("barId").addText(String.valueOf(rs.getInt(6)));
            lineE1.addElement("ouncesPoured").addText(String.valueOf(rs.getDouble(7)));
            lineE1.addElement("unit").addText(String.valueOf(rs.getDouble(8)));
            lineE1.addElement("status").addText(HandlerUtils.nullToEmpty(rs.getString(9)));
            lineE1.addElement("local").addText(HandlerUtils.nullToEmpty(rs.getString(10)));
            lineE1.addElement("systemIndex").addText(String.valueOf(rs.getInt(11)));
            //lineE1.addElement("stationId").addText(String.valueOf(rs.getInt(12)));
            //lineE1.addElement("kegLine").addText(String.valueOf(rs.getInt(13)));
            //lineE1.addElement("easternOffset").addText(HandlerUtils.nullToEmpty(rs.getString(14)));
            lineE1.addElement("advertise").addText(HandlerUtils.nullToEmpty(rs.getString(15)));
            lineE1.addElement("resetLevel").addText(String.valueOf(rs.getFloat(16)));
            lineE1.addElement("kegSize").addText(String.valueOf(rs.getInt(17)));
            lineE1.addElement("cask").addText(String.valueOf(rs.getInt(18)));
            lineE1.addElement("lineNo").addText(HandlerUtils.nullToEmpty(rs.getString(19)));
            lineE1.addElement("clean").addText(String.valueOf(rs.getInt(20)));
            }
        }
    }
    
    
    private void mGetLocationData(Element toHandle, Element toAppend) throws HandlerException {

        PreparedStatement stmt              = null;
        ResultSet rs                        = null, rsDetail= null;
        int customerId                      = HandlerUtils.getRequiredInteger(toHandle, "customerId");
        int locationId                      = HandlerUtils.getOptionalInteger(toHandle, "locationId");
        int mobileUserId                    = HandlerUtils.getOptionalInteger(toHandle, "mobileUserId");
        int type                            = 0;
        int callerId                        = getCallerId(toHandle);

        String getLocationData              = "SELECT l.id, l.name, l.lastPoured, l.picoPowerup, l.picoVersion, l.lastSold, l.processorName, l.processorVersion, l.gatewayVersion, l.volAdjustment, l.type" +
                                            " FROM location l LEFT JOIN locationDetails lD ON lD.location = l.id   WHERE lD.active = 1 ";
        String getCustomerName              = "SELECT name from customer where id = ?";
        String getStatus                    = "SELECT c.name, l.name, l.lastPoured, l.lastSold, b.lastPing FROM location l " +
                                            " LEFT JOIN locationDetails lD ON lD.location = l.id LEFT JOIN customer c ON c.id = l.customer " +
                                            " LEFT JOIN bbtvUserMac b ON l.id = b.location WHERE lD.active = 1 ";
        try {
            if(customerId > 0) {
                 if(locationId > 0) {
                     getStatus              += "AND l.customer =? AND l.id ="+locationId+" ORDER BY l.lastSold;";
                     getLocationData        +=" AND l.customer = ? AND l.id ="+locationId+"  ORDER BY l.name ;  ";
                 } else {
                     getStatus              += "AND l.customer =? ORDER BY l.lastSold;";
                     getLocationData        +=" AND l.customer = ?  ORDER BY l.name ;  ";
                 }
          
                String logMessage           = "Getting location information for customer" + customerId;
                stmt                        = transconn.prepareStatement(getLocationData);
                stmt.setInt(1, customerId);
                rs                          = stmt.executeQuery();
                while (rs != null && rs.next()) {
                    int rsIndex             = 1;
                    Element locEl           = toAppend.addElement("location");
                    locEl.addElement("locationId").addText(String.valueOf(rs.getInt(rsIndex++)));
                    locEl.addElement("locationName").addText(HandlerUtils.nullToEmpty((rs.getString(rsIndex++))));
                    locEl.addElement("lastPoured").addText(String.valueOf(rs.getString(rsIndex++)));
                    locEl.addElement("picoPowerup").addText(HandlerUtils.nullToEmpty((rs.getString(rsIndex++))));
                    locEl.addElement("picoVersion").addText(HandlerUtils.nullToEmpty((rs.getString(rsIndex++))));
                    locEl.addElement("lastSold").addText(HandlerUtils.nullToEmpty((rs.getString(rsIndex++))));
                    locEl.addElement("processorName").addText(String.valueOf(rs.getString(rsIndex++)));
                    locEl.addElement("processorVersion").addText(String.valueOf(rs.getString(rsIndex++)));
                    locEl.addElement("gatewayVersion").addText(HandlerUtils.nullToEmpty((rs.getString(rsIndex++))));
                    locEl.addElement("volAdjustment").addText(HandlerUtils.nullToEmpty((rs.getString(rsIndex++))));
                    type                    = rs.getInt(rsIndex++);
                }

                stmt                        = transconn.prepareStatement(getStatus);
                stmt.setInt(1, customerId);
                rs                          = stmt.executeQuery();
                while (rs != null && rs.next()) {
                    int rsIndex             = 1;
                    Element locEl           = toAppend.addElement("status");
                    locEl.addElement("customerName").addText(HandlerUtils.nullToEmpty((rs.getString(rsIndex++))));
                    locEl.addElement("locationName").addText(HandlerUtils.nullToEmpty((rs.getString(rsIndex++))));
                    locEl.addElement("bevbox").addText(HandlerUtils.nullToEmpty((rs.getString(rsIndex++))));
                    locEl.addElement("gateway").addText(HandlerUtils.nullToEmpty((rs.getString(rsIndex++))));
                    locEl.addElement("bbtv").addText(HandlerUtils.nullToEmpty((rs.getString(rsIndex++))));
                }

                stmt                        = transconn.prepareStatement(getCustomerName);
                stmt.setInt(1, customerId);
                rs                          = stmt.executeQuery();
                if (rs.next()) {
                    toAppend.addElement("customerName").addText(HandlerUtils.nullToEmpty(rs.getString(1)));
                }
            } else {
                getStatus                   += " ORDER BY l.lastSold;";
                stmt                        = transconn.prepareStatement(getStatus);
                rs                          = stmt.executeQuery();
                while (rs != null && rs.next()) {
                    int rsIndex             = 1;
                    Element locEl           = toAppend.addElement("status");
                    locEl.addElement("customerName").addText(HandlerUtils.nullToEmpty((rs.getString(rsIndex++))));
                    locEl.addElement("locationName").addText(HandlerUtils.nullToEmpty((rs.getString(rsIndex++))));
                    locEl.addElement("bevbox").addText(HandlerUtils.nullToEmpty((rs.getString(rsIndex++))));
                    locEl.addElement("gateway").addText(HandlerUtils.nullToEmpty((rs.getString(rsIndex++))));
                    locEl.addElement("bbtv").addText(HandlerUtils.nullToEmpty((rs.getString(rsIndex++))));
                }
            }
            
            if(mobileUserId > 0 && locationId > 0) {
                stmt                        = transconn.prepareStatement("SELECT active FROM bevMobileLocationMap WHERE location = ? AND user =?");
                stmt.setInt(1, locationId);
                stmt.setInt(2, mobileUserId);
                rs                          = stmt.executeQuery();
                if (rs.next()) {
                    toAppend.addElement("notification").addText(String.valueOf(rs.getInt(1)));
                } else {
                    toAppend.addElement("notification").addText(String.valueOf(0));
                }
                
                updateBevMobileUser(mobileUserId);
                toHandle.addElement("platform").addText(String.valueOf(type));
                //getUnits(toHandle, toAppend);              
                addUserVisitHistory(callerId, "getSettings", locationId, "Access Settings", 15,mobileUserId);
            } 
            
            stmt                            = transconn.prepareStatement("SELECT id,name, ROUND(lastValue,2), alertPoint FROM cooler WHERE location=?;");
            stmt.setInt(1, locationId);
            rs                              = stmt.executeQuery();
            while(rs.next()) {
                Element coolEl              = toAppend.addElement("coolers");
                coolEl.addElement("coolerId").addText(String.valueOf(rs.getInt(1)));
                coolEl.addElement("coolerName").addText(HandlerUtils.nullToEmpty(rs.getString(2)));
                coolEl.addElement("value").addText(HandlerUtils.nullToEmpty(rs.getString(3)));
                coolEl.addElement("alertPoint").addText(HandlerUtils.nullToEmpty(rs.getString(4)));
            }

            stmt                            = transconn.prepareStatement("SELECT id, name, lastPoured FROM bevBox WHERE location = ?;");
            stmt.setInt(1, locationId);
            rs                              = stmt.executeQuery();
            while(rs.next()) {
                Element coolEl              = toAppend.addElement("bevBoxs");
                coolEl.addElement("bevBoxId").addText(String.valueOf(rs.getInt(1)));
                coolEl.addElement("bevBoxName").addText(HandlerUtils.nullToEmpty(rs.getString(2)));
                coolEl.addElement("lastPoured").addText(HandlerUtils.nullToEmpty(rs.getString(3)));
            }

            stmt                            = transconn.prepareStatement("SELECT id, name, lastPing FROM bbtvUserMac WHERE location = ? AND admin = 0 AND active = 1 ;");
            stmt.setInt(1, locationId);
            rs                              = stmt.executeQuery();
            while(rs.next()) {
                Element coolEl              = toAppend.addElement("bbtvs");
                coolEl.addElement("bbtvId").addText(String.valueOf(rs.getInt(1)));
                coolEl.addElement("bbtvName").addText(HandlerUtils.nullToEmpty(rs.getString(2)));
                coolEl.addElement("lastPing").addText(HandlerUtils.nullToEmpty(rs.getString(3)));
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error in getLocationData: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(rsDetail);
            close(rs);
            close(stmt);
        }
    } 
    
    private void getBeverageSizes(int location, Element toAppend) throws HandlerException {

        String select = "SELECT id,name,ounces FROM beverageSize WHERE location=?";

        PreparedStatement stmt = null;
        ResultSet rs = null;

        try {
            stmt = transconn.prepareStatement(select);
            stmt.setInt(1, location);
            rs = stmt.executeQuery();
            while (rs.next()) {
                Element size = toAppend.addElement("size");
                size.addElement("id").addText(HandlerUtils.nullToEmpty(rs.getString(1)));
                size.addElement("name").addText(HandlerUtils.nullToEmpty(rs.getString(2)));
                size.addElement("ounces").addText(HandlerUtils.nullToEmpty(rs.getString(3)));
            }

        } catch (SQLException sqle) {
            logger.dbError("Database error in getBeverageSizes: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
            close(rs);
        }

    }
    
         
    
        
      
}
