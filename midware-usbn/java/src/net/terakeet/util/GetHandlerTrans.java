/*
 */

package net.terakeet.util;

/**
 *
 * @author Sundar
 */


import java.io.File;
import java.sql.*;
import java.text.BreakIterator;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Scanner;
import net.terakeet.soapware.DatabaseConnectionManager;
import net.terakeet.soapware.HandlerUtils;
import net.terakeet.soapware.RegisteredConnection;
import net.terakeet.soapware.handlers.BeerBoardHandler;
import net.terakeet.soapware.*;
import net.terakeet.soapware.handlers.SQLGetHandler;
import org.dom4j.Element;

public class GetHandlerTrans {
    private RegisteredConnection transconn = null;
    static final String transConnName = "auper";
    private MidwareLogger logger = new MidwareLogger(GetHandlerTrans.class.getName());
    private static SimpleDateFormat newDateFormat =
            new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    
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
    
    public void addTextAlertLogs(int customerId, int alertId, int userId, String mobile, String carrier, String message) throws HandlerException {
        
        transconn                           = DatabaseConnectionManager.getNewConnection(transConnName, " (SQLGetHandler)");
        String addTextAlertLogs             = "INSERT INTO textAlertLogs (customer, location, user, mobile, carrier, messageType, dateTime) VALUES " +
                                            "(?,?,?,?,?,?,?)";
        PreparedStatement stmt              = null;
        ResultSet rs                        = null;
        
        try {
            Calendar now                    = Calendar.getInstance();
            stmt                            = transconn.prepareStatement(addTextAlertLogs);
            stmt.setInt(1, customerId);
            stmt.setInt(2, alertId);
            stmt.setInt(3, userId);
            stmt.setString(4, mobile);
            stmt.setString(5, carrier);
            stmt.setString(6, message);
            stmt.setString(7, newDateFormat.format(now.getTime()));
            stmt.executeUpdate();
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.toString());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
            close(rs);
        }
        transconn.close();
    }
    
    public void deactivateAlerts(int alertId) throws HandlerException {
        
        transconn                           = DatabaseConnectionManager.getNewConnection(transConnName, " (SQLGetHandler)");
        String deactivateAlerts             = " UPDATE alerts SET status = 0 WHERE unitId = ?;";
        PreparedStatement stmt              = null;
        ResultSet rs                        = null;
        
        try {
            stmt                            = transconn.prepareStatement(deactivateAlerts);
            stmt.setInt(1, alertId);
            stmt.executeUpdate();
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.toString());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
            close(rs);
        }
        transconn.close();
    }
    
    public void updateComponentInfo(int newLastCheckId, int componentMapId) throws HandlerException {
        
        transconn                           = DatabaseConnectionManager.getNewConnection(transConnName, " (SQLGetHandler)");
        String updateComponentInfo          = " UPDATE componentLocationMap SET lastCheckId = ? WHERE id = ? ";
        PreparedStatement stmt              = null;
        ResultSet rs                        = null;
        
        try {
            stmt                            = transconn.prepareStatement(updateComponentInfo);
            stmt.setInt(1, newLastCheckId);
            stmt.setInt(2, componentMapId);
            stmt.executeUpdate();
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.toString());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
            close(rs);
        }
        transconn.close();
    }
    
    public void updateActiveAlertTime(Timestamp time, int alertId) throws HandlerException {
        
        transconn                           = DatabaseConnectionManager.getNewConnection(transConnName, " (SQLGetHandler)");
        String updateActiveAlertTime        = "UPDATE tempActiveAlertTime SET date = ? WHERE id = ?";
        PreparedStatement stmt              = null;
        ResultSet rs                        = null;
        
        try {
            stmt                            = transconn.prepareStatement(updateActiveAlertTime);
            stmt.setTimestamp(1, time);
            stmt.setInt(2, alertId);
            stmt.executeUpdate();
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.toString());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
            close(rs);
        }
        transconn.close();
    }
    
    public void updateActiveAlertTime(int active, Timestamp time, int alertId) throws HandlerException {
        
        transconn                           = DatabaseConnectionManager.getNewConnection(transConnName, " (SQLGetHandler)");
        String updateActiveAlertTime        = "UPDATE tempActiveAlertTime SET active = ?, date = ? WHERE id = ?";
        PreparedStatement stmt              = null;
        ResultSet rs                        = null;
        
        try {
            stmt                            = transconn.prepareStatement(updateActiveAlertTime);
            stmt.setInt(1, active);
            stmt.setTimestamp(2, time);
            stmt.setInt(3, alertId);
            stmt.executeUpdate();
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.toString());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
            close(rs);
        }
        transconn.close();
    }
    
    public void updateAlertFrequency(int alertId) throws HandlerException {
        
        transconn                           = DatabaseConnectionManager.getNewConnection(transConnName, " (SQLGetHandler)");
        String updateAlertFrequency         = "UPDATE textAlert SET currentFrequency = currentFrequency + 1 WHERE id = ?";
        PreparedStatement stmt              = null;
        ResultSet rs                        = null;
        
        try {
            stmt                            = transconn.prepareStatement(updateAlertFrequency);
            stmt.setInt(1, alertId);
            stmt.executeUpdate();
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.toString());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
            close(rs);
        }
        transconn.close();
    }
    
    public void insertActiveAlertTime(Timestamp time, int alertId, int unitId) throws HandlerException {
        
        transconn                           = DatabaseConnectionManager.getNewConnection(transConnName, " (SQLGetHandler)");
        String insertActiveAlertTime        = "INSERT INTO tempActiveAlertTime (alertId, unitId, date) VALUES (?,?,?)";
        PreparedStatement stmt              = null;
        ResultSet rs                        = null;
        
        try {
            stmt                            = transconn.prepareStatement(insertActiveAlertTime);
            stmt.setInt(1, alertId);
            stmt.setInt(2, unitId);
            stmt.setTimestamp(3, time);
            stmt.executeUpdate();
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.toString());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
            close(rs);
        }
        transconn.close();
    }
    
    public void insertActiveAlertTime(Timestamp time, int alertId) throws HandlerException {
        
        transconn                           = DatabaseConnectionManager.getNewConnection(transConnName, " (SQLGetHandler)");
        String insertActiveAlertTime        = "INSERT INTO tempActiveAlertTime (alertId, date) VALUES (?,?)";
        PreparedStatement stmt              = null;
        ResultSet rs                        = null;
        
        try {
            stmt                            = transconn.prepareStatement(insertActiveAlertTime);
            stmt.setInt(1, alertId);
            stmt.setTimestamp(2, time);
            stmt.executeUpdate();
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.toString());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
            close(rs);
        }
        transconn.close();
    }
    
    public void deactivateActiveAlertTime(int alertId) throws HandlerException {
        
        transconn                           = DatabaseConnectionManager.getNewConnection(transConnName, " (SQLGetHandler)");
        String deactivateAlerts             = "DELETE FROM tempActiveAlertTime WHERE alertId = ?;";
        PreparedStatement stmt              = null;
        ResultSet rs                        = null;
        
        try {
            stmt                            = transconn.prepareStatement(deactivateAlerts);
            stmt.setInt(1, alertId);
            stmt.executeUpdate();
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.toString());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
            close(rs);
        }
        transconn.close();
    }
    
    public void resetAlertFrequency(int alertId) throws HandlerException {
        
        transconn                           = DatabaseConnectionManager.getNewConnection(transConnName, " (SQLGetHandler)");
        String deactivateAlerts             = "UPDATE textAlert SET currentFrequency = 0 WHERE id = ?;";
        PreparedStatement stmt              = null;
        ResultSet rs                        = null;
        
        try {
            stmt                            = transconn.prepareStatement(deactivateAlerts);
            stmt.setInt(1, alertId);
            stmt.executeUpdate();
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.toString());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
            close(rs);
        }
        transconn.close();
    }
    
    public void updateEmailReports() throws HandlerException {
        
        transconn                           = DatabaseConnectionManager.getNewConnection(transConnName, " (SQLGetHandler)");
        String updateEmailReports             = "UPDATE emailReports SET time = 0;";
        PreparedStatement stmt              = null;
        ResultSet rs                        = null;
        
        try {
            stmt                            = transconn.prepareStatement(updateEmailReports);
            stmt.executeUpdate();
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.toString());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
            close(rs);
        }
        transconn.close();
    }
    
    public void updateEmailReportsTime(int alertId) throws HandlerException {
        
        transconn                           = DatabaseConnectionManager.getNewConnection(transConnName, " (SQLGetHandler)");
        String updateEmailReports             = "UPDATE emailReports SET time = 1 WHERE id = ?;";
        PreparedStatement stmt              = null;
        ResultSet rs                        = null;
        
        try {
            stmt                            = transconn.prepareStatement(updateEmailReports);
            stmt.setInt(1, alertId);
            stmt.executeUpdate();
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.toString());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
            close(rs);
        }
        transconn.close();
    }
    
    public void updateEmailReportsPouredAlertCount(int pouredCount, int alertId) throws HandlerException {
        
        transconn                           = DatabaseConnectionManager.getNewConnection(transConnName, " (SQLGetHandler)");
        String updateEmailReports             = "UPDATE emailReports SET pouredAlertCount = ?, time = 1 WHERE id = ?;";
        PreparedStatement stmt              = null;
        ResultSet rs                        = null;
        
        try {
            stmt                            = transconn.prepareStatement(updateEmailReports);
            stmt.setInt(1, pouredCount);
            stmt.setInt(2, alertId);
            stmt.executeUpdate();
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.toString());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
            close(rs);
        }
        transconn.close();
    }
    
    public void updateEmailReportsSoldAlertCount(int soldCount, int alertId) throws HandlerException {
        
        transconn                           = DatabaseConnectionManager.getNewConnection(transConnName, " (SQLGetHandler)");
        String updateEmailReports             = "UPDATE emailReports SET soldAlertCount = ?, time = 1 WHERE id = ?;";
        PreparedStatement stmt              = null;
        ResultSet rs                        = null;
        
        try {
            stmt                            = transconn.prepareStatement(updateEmailReports);
            stmt.setInt(1, soldCount);
            stmt.setInt(2, alertId);
            stmt.executeUpdate();
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.toString());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
            close(rs);
        }
        transconn.close();
    }
    
    public void updateLastCustomer(int customer, int user) throws HandlerException {
        
        transconn                           = DatabaseConnectionManager.getNewConnection(transConnName, " (SQLGetHandler)");
        String updateLastCustomer           = "UPDATE user SET lastCustomer = ? WHERE id = ?;";
        PreparedStatement stmt              = null;
        ResultSet rs                        = null;
        
        try {
            stmt                            = transconn.prepareStatement(updateLastCustomer);
            stmt.setInt(1, customer);
            stmt.setInt(2, user);
            stmt.executeUpdate();
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.toString());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
            close(rs);
        }
        transconn.close();
    }

    public void increaseUserAlertCount(int userAlertId) throws HandlerException {
        transconn                           = DatabaseConnectionManager.getNewConnection(transConnName, " (SQLGetHandler)");
        String update                       = "UPDATE userAlerts SET count = count + 1, active = 1 WHERE id = ?";
        PreparedStatement stmt              = null;
        try {
            stmt                            = transconn.prepareStatement(update);
            stmt.setInt(1, userAlertId);
            stmt.executeUpdate();
        } catch (SQLException sqle) {
            logger.dbError("Database error in increaseUserAlertCount: " + sqle.getMessage());
        } finally {
            close(stmt);
        }
        transconn.close();
    }

    public void deactivateUserAlert(int userAlertId) throws HandlerException {
        transconn                           = DatabaseConnectionManager.getNewConnection(transConnName, " (SQLGetHandler)");
        String update                       = "UPDATE userAlerts SET active = 1 WHERE id = ?";
        PreparedStatement stmt              = null;
        try {
            stmt = transconn.prepareStatement(update);
            stmt.setInt(1, userAlertId);
            stmt.executeUpdate();
        } catch (SQLException sqle) {
            logger.dbError("Database error in deactivateAlert: " + sqle.getMessage());
        } finally {
            close(stmt);
        }
        transconn.close();
    }

    public void increaseSuperUserAlertCount(int userAlertId) throws HandlerException {
        transconn                           = DatabaseConnectionManager.getNewConnection(transConnName, " (SQLGetHandler)");
        String update                       = "UPDATE superUserAlerts SET count = count + 1, active = 1 WHERE id = ?";
        PreparedStatement stmt              = null;
        try {
            stmt = transconn.prepareStatement(update);
            stmt.setInt(1, userAlertId);
            stmt.executeUpdate();
        } catch (SQLException sqle) {
            logger.dbError("Database error in increaseSuperUserAlertCount: " + sqle.getMessage());
        } finally {
            close(stmt);
        }
        transconn.close();
    }

    public void deactivateSuperUserAlert(int userAlertId) throws HandlerException {
        transconn                           = DatabaseConnectionManager.getNewConnection(transConnName, " (SQLGetHandler)");
        String update                       = "UPDATE superUserAlerts SET active = 1 WHERE id = ?";
        PreparedStatement stmt              = null;
        try {
            stmt = transconn.prepareStatement(update);
            stmt.setInt(1, userAlertId);
            stmt.executeUpdate();
        } catch (SQLException sqle) {
            logger.dbError("Database error in deactivateAlert: " + sqle.getMessage());
        } finally {
            close(stmt);
        }
        transconn.close();
    }

    public void createAlertTicket(int location, int category, int problem, String notes) throws HandlerException {
        transconn                           = DatabaseConnectionManager.getNewConnection(transConnName, " (SQLGetHandler)");
        String createTroubleTicket          = " INSERT INTO ticket (owner, assignedTo, location, category, problem, open, notes) VALUES (?, ?, ?, ?, ?, ?, ?) ";
        String insertTicketUpdates          = " INSERT INTO ticketUpdates (ticket, user) VALUES (?, ?) ";
        String getLastId                    = " SELECT LAST_INSERT_ID()";
        java.util.Date timestamp            = new java.util.Date();
        PreparedStatement stmt              = null;
        ResultSet rs                        = null;
        try {
            stmt                            = transconn.prepareStatement(createTroubleTicket);
            stmt.setInt(1, 3496);
            stmt.setInt(2, 3496);
            stmt.setInt(3, location);
            stmt.setInt(4, category);
            stmt.setInt(5, problem);
            stmt.setString(6, newDateFormat.format(timestamp));
            stmt.setString(7, notes);
            stmt.executeUpdate();

            stmt                            = transconn.prepareStatement(getLastId);
            rs                              = stmt.executeQuery();
            if (rs.next()) {
                stmt                        = transconn.prepareStatement(insertTicketUpdates);
                stmt.setInt(1, rs.getInt(1));
                stmt.setInt(2, 3496);
                stmt.executeUpdate();
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error in deactivateAlert: " + sqle.getMessage());
        } finally {
            close(rs);
            close(stmt);
        }
        transconn.close();
    }
    
    public void updateBevBoxAlert(String box) throws HandlerException {
        
        transconn                           = DatabaseConnectionManager.getNewConnection(transConnName, " (SQLGetHandler)");
        String updateBevBoxAlert           = "UPDATE bevBox SET alert = 1, lastPoured = lastPoured WHERE id IN ("  + box + ");";
        PreparedStatement stmt              = null;
        ResultSet rs                        = null;
        
        try {
            stmt                            = transconn.prepareStatement(updateBevBoxAlert);
            stmt.executeUpdate();
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.toString());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
            close(rs);
        }
        transconn.close();
    }
    
    public void updateBeerBoardAlert(String box) throws HandlerException {
        
        transconn                           = DatabaseConnectionManager.getNewConnection(transConnName, " (SQLGetHandler)");
        String updateBeerBoardAlert         = "UPDATE beerboard SET alert = 1, lastPing=lastPing WHERE id IN ("  + box + ");";
        PreparedStatement stmt              = null;
        ResultSet rs                        = null;
        
        try {
            stmt                            = transconn.prepareStatement(updateBeerBoardAlert);
            stmt.executeUpdate();
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.toString());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
            close(rs);
        }
        transconn.close();
    }

    public void createNoPouredTroubleTicket(int location) throws HandlerException {
        transconn                           = DatabaseConnectionManager.getNewConnection(transConnName, " (SQLGetHandler)");
        String checkTroubleTicket           = "SELECT id FROM ticket WHERE location = ? AND category = 1 AND problem = 34 AND close IS NULL;";
        String createTroubleTicket          = "INSERT INTO ticket (location, notes, owner, assignedTo, category, problem, open, dateCreated, dateUpdated) " +
                                            " VALUES (?, 'Automated Ticket - No Poured Data', 3496, 3496, 1, 34, LEFT(NOW(),10), LEFT(NOW(),10), LEFT(NOW(),10))";
        PreparedStatement stmt              = null;
        ResultSet rs                        = null;
        try {
            stmt                            = transconn.prepareStatement(checkTroubleTicket);
            stmt.setInt(1, location);
            rs                              = stmt.executeQuery();
            if (!rs.next()) {
                stmt = transconn.prepareStatement(createTroubleTicket);
                stmt.setInt(1, location);
                stmt.executeUpdate();
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error in deactivateAlert: " + sqle.getMessage());
        } finally {
            close(stmt);
            close(rs);
        }
        transconn.close();
    }

    public void createNoSoldTroubleTicket(int location) throws HandlerException {
        transconn                           = DatabaseConnectionManager.getNewConnection(transConnName, " (SQLGetHandler)");
        String checkTroubleTicket           = "SELECT id FROM ticket WHERE location = ? AND category = 2 AND problem = 13 AND close IS NULL;";
        String createTroubleTicket          = "INSERT INTO ticket (location, notes, owner, assignedTo, category, problem, open, dateCreated, dateUpdated) " +
                                            " VALUES (?, 'Automated Ticket - No Sold Data', 3496, 3496, 2, 13, LEFT(NOW(),10), LEFT(NOW(),10), LEFT(NOW(),10))";
        PreparedStatement stmt              = null;
        ResultSet rs                        = null;
        try {
            stmt                            = transconn.prepareStatement(checkTroubleTicket);
            stmt.setInt(1, location);
            rs                              = stmt.executeQuery();
            if (!rs.next()) {
                stmt = transconn.prepareStatement(createTroubleTicket);
                stmt.setInt(1, location);
                stmt.executeUpdate();
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error in deactivateAlert: " + sqle.getMessage());
        } finally {
            close(stmt);
            close(rs);
        }
        transconn.close();
    }

    public void addLocationAlertlogs(int alertType, HashSet<Integer> locationSet, String date) throws HandlerException {
        
        transconn                           = DatabaseConnectionManager.getNewConnection(transConnName, " (SQLGetHandler)");
        String insertLocationAlertLog       = "INSERT INTO locationAlertLogs (location, alertType, date) VALUES (?,?,LEFT(?,10));";
        PreparedStatement stmt = null;
        try {
            for (Integer location : locationSet) {
                stmt = transconn.prepareStatement(insertLocationAlertLog);
                stmt.setInt(1, location);
                stmt.setInt(2, alertType);
                stmt.setString(3, date);
                stmt.executeUpdate();
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error in getAfterHoursEmail: " + sqle.getMessage());
        } finally {
            close(stmt);
        }
        transconn.close();
    }

    public void updateBevMobileUser(int user) throws HandlerException {
        transconn                           = DatabaseConnectionManager.getNewConnection(transConnName, " (SQLGetHandler)");
        String update                       = "UPDATE bevMobileUser SET lastAccess = NOW() WHERE id= ?";
        PreparedStatement stmt              = null;
        try {
            stmt                            = transconn.prepareStatement(update);
            stmt.setInt(1, user);
            stmt.executeUpdate();
        } catch (SQLException sqle) {
            logger.dbError("Database error in increaseUserAlertCount: " + sqle.getMessage());
        } finally {
            close(stmt);
        }
        transconn.close();
    }

    public void insertBrassTapUser(int user) throws HandlerException {
        transconn                           = DatabaseConnectionManager.getNewConnection(transConnName, " (SQLGetHandler)");
        String insert                       = "INSERT INTO brasstapUser (user) VALUES (?)";
        PreparedStatement stmt              = null;
        try {
            stmt                            = transconn.prepareStatement(insert);
            stmt.setInt(1, user);
            stmt.executeUpdate();
        } catch (SQLException sqle) {
            logger.dbError("Database error in increaseUserAlertCount: " + sqle.getMessage());
        } finally {
            close(stmt);
        }
        transconn.close();
    }
    
     public void addMobileUserHistory(int userId,String action,int locationId, String message, int mobileId) throws HandlerException {
        transconn                           = DatabaseConnectionManager.getNewConnection(transConnName, " (SQLGetHandler)");
        String checkAction                  = " SELECT id FROM task WHERE abbrev=? LIMIT 1";
        String insertFullLog                = " INSERT INTO userHistoryMobile (user,task,description,location,mobile,timestamp) " 
                                            + " VALUES (?,?,?,?,?,now()) ";
         PreparedStatement stmt = null;
            ResultSet rs = null;
            try {
                
                // check that the action exists (has a task id)
                int taskId = 0;
                stmt = transconn.prepareStatement(checkAction);
                stmt.setString(1, action);
                rs = stmt.executeQuery();
                if (rs.next()) {
                    taskId = rs.getInt(1);
                } else {
                    // if the task doesn't exist, we'll insert it as task 0 and
                    // prepend the name of the supplied task to the message;
                    message = "UNKNOWN ("+action+") "+message;
                }
                
                stmt = transconn.prepareStatement(insertFullLog);
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

    public void insertBBTVAutoFeed(int location) throws HandlerException {
        transconn                           = DatabaseConnectionManager.getNewConnection(transConnName, " (SQLGetHandler)");
        String insert                       = "INSERT INTO bbtvAutoFeed(location) Values( ?);";
        PreparedStatement stmt              = null;
        try {
            stmt                            = transconn.prepareStatement(insert);
            stmt.setInt(1, location);
            stmt.executeUpdate();
        } catch (SQLException sqle) {
            logger.dbError("Database error in increaseUserAlertCount: " + sqle.getMessage());
        } finally {
            close(stmt);
        }
        transconn.close();
    }

    public void insertBBTVFormat(int location) throws HandlerException {
        transconn                           = DatabaseConnectionManager.getNewConnection(transConnName, " (SQLGetHandler)");
        String insertFormat                 = "INSERT INTO bbtvMenuFormat (row, property, location) VALUES (1,1,?), (2,3,?), (3,4,?);";
        PreparedStatement stmt              = null;
        try {
            stmt                            = transconn.prepareStatement(insertFormat);
            stmt.setInt(1, location);            
            stmt.setInt(2, location);            
            stmt.setInt(3, location);            
            stmt.executeUpdate();
        } catch (SQLException sqle) {
            logger.dbError("Database error in increaseUserAlertCount: " + sqle.getMessage());
        } finally {
            close(stmt);
        }
        transconn.close();
    }

    public void updateLocationBeerBoardMap(int menu, int location) throws HandlerException {
        transconn                           = DatabaseConnectionManager.getNewConnection(transConnName, " (SQLGetHandler)");
        String update                       = "UPDATE locationBeerBoardMap SET pdfMenu= ? WHERE location =?;";
        PreparedStatement stmt              = null;
        try {
            stmt                            = transconn.prepareStatement(update);
            stmt.setInt(1, menu);            
            stmt.setInt(2, location);            
            stmt.executeUpdate();
        } catch (SQLException sqle) {
            logger.dbError("Database error in increaseUserAlertCount: " + sqle.getMessage());
        } finally {
            close(stmt);
        }
        transconn.close();
    }
    
    public void deleteProductForCorrection(int oldProduct, int newProduct) throws HandlerException {
        
        transconn                           = DatabaseConnectionManager.getNewConnection(transConnName, " (SQLGetHandler)");
        PreparedStatement stmt              = null;
        ResultSet rs                        = null, inventoryRs = null;
        
        try {
            logger.debug("To Delete: " + oldProduct + ", to replace " + newProduct);
            if(oldProduct != newProduct) {
            stmt                            = transconn.prepareStatement("SELECT id, name FROM product WHERE id= ? AND approved = 0");
            stmt.setInt(1, newProduct);
            rs                              = stmt.executeQuery();
            if (rs.next()) {
            } else {

                String deleteProduct        = "DELETE FROM product WHERE id = ? ";
                String deleteProductDescription
                                            = "DELETE FROM productDescription WHERE product = ? ";
                String deleteProductSetMap  = "DELETE FROM productSetMap WHERE product = ? ";

                String tableName[]          = { "ingredient", "line", "lineUpdates", "openHoursSummary", "openHoursSoldSummary", 
                                            "pouredSummary", "soldSummary", "afterHoursSummary", "afterHoursSoldSummary", "beveragePrices",
                                            "bevSyncCreatives", "bevSyncSummary", "concessionProductMap", "eventOpenHoursSummary", "eventOpenHoursSoldSummary",
                                            "eventPreOpenHoursSummary", "eventPreOpenHoursSoldSummary", "eventAfterHoursSummary", "eventAfterHoursSoldSummary",
                                            "inventorySummary", "purchaseDetail", "unitCount", "unitStandCount","comingSoonProducts", "favoriteBeer","bottleBeer"};

                for(String table : tableName) {
                    stmt                    = transconn.prepareStatement("UPDATE " + table + " SET product = ? WHERE product = ?");
                    stmt.setInt(1, newProduct);
                    stmt.setInt(2, oldProduct);
                    stmt.executeUpdate();
                }
                
                stmt                        = transconn.prepareStatement("SELECT Distinct location FROM inventory WHERE product= ?;");
                stmt.setInt(1, oldProduct);
                rs                          = stmt.executeQuery();
                while(rs.next()){
                    int location                = rs.getInt(1);
                    stmt                        = transconn.prepareStatement("SELECT id FROM inventory WHERE location = ? AND product= ?;");
                    stmt.setInt(1, location);
                    stmt.setInt(2, newProduct);
                    inventoryRs             = stmt.executeQuery();
                    if(inventoryRs.next()){
                        logger.debug("product present:"+location);
                        stmt                = transconn.prepareStatement("DELETE FROM inventory  WHERE location =? AND product = ?"); 
                        stmt.setInt(1, location);
                        stmt.setInt(2, oldProduct);
                        stmt.executeUpdate();
                        
                    } else {
                        logger.debug("product not present and updated:"+location);
                        stmt                = transconn.prepareStatement("UPDATE inventory SET product = ? WHERE location =? AND product = ? ");
                        stmt.setInt(1, newProduct);
                        stmt.setInt(2, location);
                        stmt.setInt(3, oldProduct);
                        stmt.executeUpdate();
                    }
                }
                
                stmt                        = transconn.prepareStatement("SELECT Distinct location FROM customBeerName WHERE product= ?;");
                stmt.setInt(1, oldProduct);
                rs                          = stmt.executeQuery();
                while(rs.next()){
                    int location                = rs.getInt(1);
                    stmt                        = transconn.prepareStatement("SELECT id FROM customBeerName WHERE location = ? AND product= ?;");
                    stmt.setInt(1, location);
                    stmt.setInt(2, newProduct);
                    inventoryRs             = stmt.executeQuery();
                    if(inventoryRs.next()){
                        logger.debug("customBeerName product present:"+location);
                        stmt                = transconn.prepareStatement("DELETE FROM customBeerName  WHERE location =? AND product = ?"); 
                        stmt.setInt(1, location);
                        stmt.setInt(2, oldProduct);
                        stmt.executeUpdate();
                    } else {
                        logger.debug("customBeerName product not present and updated:"+location);
                        stmt                = transconn.prepareStatement("UPDATE customBeerName SET product = ? WHERE location =? AND product = ? ");
                        stmt.setInt(1, newProduct);
                        stmt.setInt(2, location);
                        stmt.setInt(3, oldProduct);
                        stmt.executeUpdate();
                    }
                }
                
                stmt                        = transconn.prepareStatement("UPDATE brasstapProducts SET usbnID = ? WHERE usbnID = ?");
                stmt.setInt(1, newProduct);
                stmt.setInt(2, oldProduct);
                stmt.executeUpdate();

                stmt                        = transconn.prepareStatement(deleteProductSetMap);
                stmt.setInt(1, oldProduct);
                stmt.executeUpdate();

                stmt                        = transconn.prepareStatement(deleteProductDescription);
                stmt.setInt(1, oldProduct);
                stmt.executeUpdate();

                stmt                        = transconn.prepareStatement(deleteProduct);
                stmt.setInt(1, oldProduct);
                stmt.executeUpdate();

                String insertLog            = "INSERT INTO productChangeLog (product,type,date) VALUES (?,3,now())";
                stmt                        = transconn.prepareStatement(insertLog);
                stmt.setInt(1, oldProduct);                
                stmt.executeUpdate();

                insertLog                   = "DELETE FROM  productChangeLog WHERE product = ? AND type IN (1,2) AND productType=1;";
                stmt                        = transconn.prepareStatement(insertLog);
                stmt.setInt(1, oldProduct);
                stmt.executeUpdate();
            }
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(inventoryRs);
            close(rs);
            close(stmt);
        }
        transconn.close();
    }
    
    public void insertPOSProductCorrection(int oldProduct, int newProduct) throws HandlerException {
        
        transconn                           = DatabaseConnectionManager.getNewConnection(transConnName, " (SQLGetHandler)");
        String insert                       = "INSERT INTO posProductCorrection (newProduct, newName, oldProduct, oldName) "
                                            + " VALUES (?, (SELECT name FROM product WHERE id = ?), ?, (SELECT name FROM product WHERE id = ?));";
        PreparedStatement stmt              = null;
        ResultSet rs                        = null;
        
        try {
            stmt                            = transconn.prepareStatement("SELECT id FROM posProductCorrection WHERE newProduct = ?;");
            stmt.setInt(1, oldProduct);
            rs                              = stmt.executeQuery();
            if (!rs.next()) {
                stmt                        = transconn.prepareStatement(insert);
                stmt.setInt(1, oldProduct);
                stmt.setInt(2, oldProduct);
                stmt.setInt(3, newProduct);
                stmt.setInt(4, newProduct);
                stmt.executeUpdate();
                
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.toString());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
            close(rs);
        }
        transconn.close();
    }

    public void deletePOSProductMerge(int id) throws HandlerException {
        transconn                           = DatabaseConnectionManager.getNewConnection(transConnName, " (SQLGetHandler)");
        String delete                       = "DELETE FROM posProductCorrection WHERE id = ?;";
        PreparedStatement stmt              = null;
        try {
            stmt                            = transconn.prepareStatement(delete);
            stmt.setInt(1, id);            
            stmt.executeUpdate();
        } catch (SQLException sqle) {
            logger.dbError("Database error in deletePOSProductMerge: " + sqle.getMessage());
        } finally {
            close(stmt);
        }
        transconn.close();
    }
}
