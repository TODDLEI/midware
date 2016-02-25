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
import net.terakeet.soapware.handlers.SQLBevManagerMobileTransHandler;
import net.terakeet.util.QuadGraphics;

public class SQLBevManagerMobileHandler implements Handler {
    
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
    private SQLBevManagerMobileTransHandler g   
                                            = new SQLBevManagerMobileTransHandler();
    
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
    public SQLBevManagerMobileHandler() throws HandlerException {
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
        logger.debug("SQLBevManagerMobileHandler processing method: "+function);
        logger.xml("request: " + toHandle.asXML());
        
        transconn                           = DatabaseConnectionManager.getNewConnection(transConnName, function + " (SQLBevManagerMobileHandler)");
        
        cf.applyPattern("#.####");
        try {
            // All methods require an admin client key
            if (ss.getLocation() == 0 && ss.getClientId() == 1 && ss.getSecurityLevel().canAdmin()) {
                if ("mAuthUser".equals(function)) {
                    mAuthUser(toHandle, responseFor(function,toAppend));
                } else if ("mGetVariance".equals(function)) {
                    mGetVariance(toHandle, responseFor(function,toAppend));
                } else if ("mGetProducts1".equals(function)) {
                    mGetProducts1(toHandle, responseFor(function,toAppend));
                } else if ("mGetProductSet".equals(function)) {
                    mGetProductSet(toHandle, responseFor(function,toAppend));
                } else if ("mGetCustomer".equals(function)) {
                    mGetCustomer(toHandle, responseFor(function,toAppend));
                } else if ("getMobileDashBoardReport".equals(function)) {
                    getMobileDashBoardReport(toHandle, responseFor(function,toAppend));
                } else if ("getParValue".equals(function)) {
                    getParValue(toHandle, responseFor(function,toAppend));
                } else if ("getNewOrder".equals(function)) {
                    getNewOrder(toHandle, responseFor(function,toAppend));
                } else if ("getPastOrder".equals(function)) {
                    getPastOrder(toHandle, responseFor(function,toAppend));
                } else if ("getAllUnits".equals(function)) {
                    getAllUnits(toHandle, responseFor(function,toAppend));
                } else if ("mGetPricesTab".equals(function)) {
                    mGetPricesTab(toHandle, responseFor(function,toAppend));
                } else if ("mGetUnclaimedReoprt".equals(function)) {
                    mGetUnclaimedReoprt(toHandle, responseFor(function,toAppend));
                } else if ("mGetControlPanel".equals(function)) {
                    mGetControlPanel(toHandle, responseFor(function,toAppend));
                } else if ("getBeveragePLU".equals(function)) {
                    getBeveragePLU(toHandle, responseFor(function,toAppend));
                } else if ("mGetLineProduct".equals(function)) {
                    mGetLineProduct(toHandle, responseFor(function,toAppend));
                } else if ("sendEmailPrintDoc".equals(function)) {
                    sendEmailPrintDoc(toHandle, responseFor(function,toAppend));
                }  else if ("testPrintableMenu".equals(function)) {
                    testPrintableMenu(toHandle, responseFor(function,toAppend));
                } else if ("getNULLBeverageIngridient".equals(function)) {
                    getNULLBeverageIngridient(toHandle, responseFor(function,toAppend));
                } else if ("updateBevMobileNotification".equals(function)) {
                    updateBevMobileNotification(toHandle, responseFor(function,toAppend));
                } else if ("addUpdateDeleteBeveragePlu".equals(function)) {
                    addUpdateDeleteBeveragePlu(toHandle, responseFor(function,toAppend));
                } else if ("updateGlanolaLineCleaning".equals(function)) {
                    updateGlanolaLineCleaning(toHandle, responseFor(function,toAppend));
                } else if ("addBevPushMessage".equals(function)) {
                    addBevPushMessage(toHandle, responseFor(function,toAppend));
                }  else if ("testHtmlTemplate".equals(function)) {
                    testHtmlTemplate(toHandle, responseFor(function,toAppend));
                }else if ("mGetLocationData".equals(function)) {
                    mGetLocationData(toHandle, responseFor(function,toAppend));
                } else if ("mGetProductTab".equals(function)) {
                    mGetProductTab(toHandle, responseFor(function,toAppend));
                } else if ("testHttpClient".equals(function)) {
                    testHttpClient(toHandle, responseFor(function,toAppend));
                } else if ("sendBeerBoardIndex".equals(function)) {
                    sendBeerBoardIndex(toHandle, responseFor(function,toAppend));
                }else {
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
    
    private void mAuthUser(Element toHandle, Element toAppend) throws HandlerException {

        final int HQ_CUSTID                 = 10;
        final int ROOT_CUSTOMER             = 0;
        //final String ADMIN_SECURITY_STRING = "1";
        //final String SUPERVISOR_SECURITY_STRING = "3";

        int platform                        = HandlerUtils.getRequiredInteger(toHandle, "platform");
        String username                     = HandlerUtils.getRequiredString(toHandle, "username");
        String password                     = HandlerUtils.getRequiredString(toHandle, "password");
        String deviceToken                  = HandlerUtils.getOptionalString(toHandle, "deviceToken");

        String checkBevSyncRoot             = "SELECT u.id, uPM.easternOffset, uPM.threshold, uPM.emailType, u.customer, uGP.groups FROM user u " +
                                            " LEFT JOIN userPreferenceMap uPM ON uPM.user = u.id LEFT JOIN userGroupMap uGP on uGP.user = u.id " +
                                            " WHERE u.username = ? AND u.password = ? AND u.platform IN (?,3)";

        String checkRoot                    = "SELECT u.id, u.isManager, isITAdmin, u.lastCustomer, u.customer, u.groupId FROM user u " +
                                            " WHERE u.username = ? AND u.password = ? AND u.platform IN (?,3)";

        String checkBOSSRoot                = "SELECT u.id, uPM.easternOffset, uPM.threshold, uPM.emailType, u.customer FROM user u " +
                                            " LEFT JOIN userPreferenceMap uPM ON uPM.user = u.id  " +
                                            " WHERE u.customer = 0 AND u.username = ? AND u.password = ?";

        String selectNormal                 = "SELECT l.id, l.name, l.type, c.id, c.name, c.type, l.easternOffset, l.volAdjustment, lD.beerboard, m.securityLevel" +
                                            " FROM userMap m " +
                                            " LEFT JOIN location l ON m.location = l.id LEFT JOIN locationDetails lD ON lD.location = l.id " +
                                            " LEFT JOIN customer c ON l.customer = c.id " +
                                            " WHERE m.user=? AND lD.active = 1 " +
                                            " ORDER BY c.id,m.securityLevel ASC, l.name ASC";

        String selectRoot                   = "SELECT l.id, l.name, l.type, c.id, c.name, c.type, l.easternOffset, l.volAdjustment, lD.beerboard" +
                                            " FROM customer c " +
                                            " LEFT JOIN location l ON l.customer = c.id LEFT JOIN locationDetails lD ON lD.location = l.id " +
                                            " WHERE c.id=? AND lD.active = 1 " +
                                            " ORDER BY l.name ASC ";
        
        String checkValidCustomer           = "SELECT * FROM customer c LEFT JOIN location l ON l.customer = c.id LEFT JOIN locationDetails lD ON lD.location = l.id " +
                                            " WHERE c.isActive = 1 AND lD.active = 1 AND c.id = ?";

        int userId                          = -1;
        String platformName                 = null;

        PreparedStatement stmt              = null;
        ResultSet rs                        = null, rsDetail = null;

        try {

            switch (platform) {
                case 0:
                    platformName            = "bevBOSS";
                    // we need to know if the user has Admin access
                    stmt                    = transconn.prepareStatement(checkBOSSRoot);
                    stmt.setString(1, username);
                    stmt.setString(2, password);
                    rs                      = stmt.executeQuery();
                    if (rs != null && rs.next()) {
                        String logMessage   = "Granting Admin access to " + username + " for " + platformName;
                        logger.portalDetail(userId, "login", 0, logMessage, transconn);
                        int rsIndex         = 1;
                        userId              = rs.getInt(rsIndex++);
                        toAppend.addElement("userId").addText(String.valueOf(userId));
                        toAppend.addElement("easternOffset").addText(HandlerUtils.nullToEmpty((rs.getString(rsIndex++))));
                        toAppend.addElement("threshold").addText(HandlerUtils.nullToEmpty((rs.getString(rsIndex++))));
                        toAppend.addElement("emailType").addText(String.valueOf(rs.getInt(rsIndex++)));
                        toAppend.addElement("securityLevel").addText("1");
                        toAppend.addElement("productType").addText("1");
                        toAppend.addElement("groupId").addText("0");
                    }
                    break;
                case 1:
                    platformName            = "bevManager";
                    // we need to know if the user has Admin access
                    stmt                    = transconn.prepareStatement(checkRoot);
                    stmt.setString(1, username);
                    stmt.setString(2, password);
                    stmt.setInt(3, platform);
                    rs                      = stmt.executeQuery();
                    int isManager           = -1, isITAdmin = -1, customerToLoad = 0, associatedCustomer = -1, groupId = -1, secutityLevel= 0;
                    boolean isSuperAdmin    = false,success=false;
                   
                    if (rs != null && rs.next()) {
                        int rsIndex         = 1;
                        userId              = rs.getInt(rsIndex++);
                        isManager           = rs.getInt(rsIndex++);
                        isITAdmin           = rs.getInt(rsIndex++);
                        customerToLoad      = rs.getInt(rsIndex++);
                        associatedCustomer  = rs.getInt(rsIndex++);
                        groupId             = rs.getInt(rsIndex++);
                        
                        if (associatedCustomer == ROOT_CUSTOMER) {
                            stmt            = transconn.prepareStatement(checkValidCustomer);
                            stmt.setInt(1, customerToLoad);
                            rs              = stmt.executeQuery();
                            if (!rs.next()) {
                                customerToLoad
                                            = 205;
                            }
                            isSuperAdmin    = true;
                        } else {
                            customerToLoad  = associatedCustomer;
                            isSuperAdmin    = false;
                        }
                        success             = true;
                        toAppend.addElement("userId").addText(String.valueOf(userId));
                        toAppend.addElement("groupId").addText(String.valueOf(groupId));
                    }

                    if (isManager > 0) {                        
                        // the user is an Admin (root)
                        String logMessage   = "Granting " + (isSuperAdmin ? "Admin" : "Super-manager") + " access to " + username + " for " + platformName;
                        logger.portalDetail(userId, "login", 0, logMessage, transconn);
                        stmt                = transconn.prepareStatement(selectRoot);
                        stmt.setInt(1, customerToLoad);
                        int previousId      = 0;   
                        rs                  = stmt.executeQuery();
                        while (rs != null && rs.next()) {
                            int rsIndex     = 1;
                            Element locEl   = toAppend.addElement("location");
                            locEl.addElement("locationId").addText(String.valueOf(rs.getInt(1)));
                            locEl.addElement("locationName").addText(HandlerUtils.nullToEmpty((rs.getString(2))));
                            locEl.addElement("locationType").addText(String.valueOf(rs.getInt(3)));
                            locEl.addElement("customerId").addText(String.valueOf(rs.getInt(4)));
                            locEl.addElement("customerName").addText(HandlerUtils.nullToEmpty((rs.getString(5))));
                            locEl.addElement("customerType").addText(String.valueOf(rs.getInt(6)));
                            //locEl.addElement("easternOffset").addText(HandlerUtils.nullToEmpty((rs.getString(7))));
                            //locEl.addElement("volAdjustment").addText(HandlerUtils.nullToEmpty((rs.getString(8))));
                           // locEl.addElement("beerboard").addText(String.valueOf(rs.getInt(9)));
                            WebPermission perm
                                            = isSuperAdmin ? WebPermission.instanceOfUsbnAdmin() : WebPermission.instanceOfCustomerAdmin();
                            secutityLevel   = perm.getLevel();                            
                            locEl.addElement("securityLevel").addText(String.valueOf(secutityLevel));
                            if(previousId!=rs.getInt(4)){
                                Element customerEl                  = toAppend.addElement("customer");
                                customerEl.addElement("customerId").addText(String.valueOf(rs.getInt(4)));
                                customerEl.addElement("name").addText(HandlerUtils.nullToEmpty(rs.getString(5)));                            
                                customerEl.addElement("cType").addText(String.valueOf(rs.getInt(6)));
                                previousId  = rs.getInt(4);
                            }
                        }
                    } else if (isITAdmin > 0) {
                        //logger.debug("isItAdmin");
                        // the user is an Admin (root)
                        String logMessage   = "Granting " + (isSuperAdmin ? "Admin" : "Super-manager") + " access to " + username + " for " + platformName;
                        logger.portalDetail(userId, "login", 0, logMessage, transconn);
                        stmt                = transconn.prepareStatement(selectRoot);
                        stmt.setInt(1, customerToLoad);
                        rs                  = stmt.executeQuery();
                        int previousId      = 0;   
                        while (rs != null && rs.next()) {
                            int rsIndex     = 1;
                            Element locEl   = toAppend.addElement("location");
                            locEl.addElement("locationId").addText(String.valueOf(rs.getInt(1)));
                            locEl.addElement("locationName").addText(HandlerUtils.nullToEmpty((rs.getString(2))));
                            locEl.addElement("locationType").addText(String.valueOf(rs.getInt(3)));
                            locEl.addElement("customerId").addText(String.valueOf(rs.getInt(4)));
                            locEl.addElement("customerName").addText(HandlerUtils.nullToEmpty((rs.getString(5))));
                            locEl.addElement("customerType").addText(String.valueOf(rs.getInt(6)));
                            //locEl.addElement("easternOffset").addText(HandlerUtils.nullToEmpty((rs.getString(7))));
                            //locEl.addElement("volAdjustment").addText(HandlerUtils.nullToEmpty((rs.getString(8))));
                            //locEl.addElement("beerboard").addText(String.valueOf(rs.getInt(9)));
                            WebPermission perm
                                            = isSuperAdmin ? WebPermission.instanceOfUsbnAdmin() : WebPermission.instanceOfITAdmin();
                            secutityLevel   = perm.getLevel();                           
                            locEl.addElement("securityLevel").addText(String.valueOf(secutityLevel));
                            if(previousId!=rs.getInt(4)){
                                Element customerEl                  = toAppend.addElement("customer");
                                customerEl.addElement("customerId").addText(String.valueOf(rs.getInt(4)));
                                customerEl.addElement("name").addText(HandlerUtils.nullToEmpty(rs.getString(5)));                            
                                customerEl.addElement("cType").addText(String.valueOf(rs.getInt(6)));
                                previousId  = rs.getInt(4);
                            }

                        }
                    } else if (userId >= 0) {
                        //logger.debug("isUserID");
                        // the user is not an Admin(root)                       
                        stmt                = transconn.prepareStatement(selectNormal);
                        stmt.setInt(1, userId);
                        rs                  = stmt.executeQuery();
                        int previousId      = 0;    
                        while (rs != null && rs.next()) {
                            int rsIndex     = 1;
                            Element locEl   = toAppend.addElement("location");
                            locEl.addElement("locationId").addText(String.valueOf(rs.getInt(1)));
                            locEl.addElement("locationName").addText(HandlerUtils.nullToEmpty((rs.getString(2))));
                            locEl.addElement("locationType").addText(String.valueOf(rs.getInt(3)));
                            locEl.addElement("customerId").addText(String.valueOf(rs.getInt(4)));
                            locEl.addElement("customerName").addText(HandlerUtils.nullToEmpty((rs.getString(5))));
                            locEl.addElement("customerType").addText(String.valueOf(rs.getInt(6)));
                            //locEl.addElement("easternOffset").addText(HandlerUtils.nullToEmpty((rs.getString(7))));
                            //locEl.addElement("volAdjustment").addText(HandlerUtils.nullToEmpty((rs.getString(8))));
                            //locEl.addElement("beerboard").addText(String.valueOf(rs.getInt(9)));
                            secutityLevel   = rs.getInt(10);
                            //logger.debug("Security"+secutityLevel);
                            locEl.addElement("securityLevel").addText(String.valueOf(secutityLevel));
                            if(previousId!=rs.getInt(4)){
                                Element customerEl                  = toAppend.addElement("customer");
                                customerEl.addElement("customerId").addText(String.valueOf(rs.getInt(4)));
                                customerEl.addElement("name").addText(HandlerUtils.nullToEmpty(rs.getString(5)));                            
                                customerEl.addElement("cType").addText(String.valueOf(rs.getInt(6)));
                                previousId  = rs.getInt(4);
                            }
                        }
                    } else {
                        logger.portalAction("Authentication failed to " + username + " for " + platformName);
                        // authentication failed
                    }
                    
                    if(userId > 0 && success ) {  
                        int mobileUser      = 0;
                        if (isSuperAdmin) {
                            stmt            = transconn.prepareStatement("SELECT id, name, acctNum, type FROM customer WHERE isActive = 1 ");
                            rs              = stmt.executeQuery();
                            getCustomerXML(toAppend, rs, customerToLoad);
                        } else if (groupId > 0) {
                            stmt            = transconn.prepareStatement("SELECT c.id, c.name, c.acctNum, c.type FROM customer c LEFT JOIN customerGroupMap cGM ON cGM.customer = c.id WHERE c.isActive = 1 AND cGM.groupId = ?" +
                                            " ");
                            stmt.setInt(1, groupId);
                            rs              = stmt.executeQuery();
                            getCustomerXML(toAppend, rs, customerToLoad);
                        }
                        
                        if(deviceToken==null) {
                            deviceToken     = "";
                        }
                        
                        stmt                = transconn.prepareStatement("SELECT id FROM bevMobileUser WHERE deviceToken = ? AND user = ? ");
                        stmt.setInt(2,userId);
                        stmt.setString(1,deviceToken);
                        rs                  = stmt.executeQuery();
                        if(!rs.next()) {
                                 mobileUser = g.insertBevMobileUser(userId, deviceToken);
                                //toAppend.addElement("mobileUserId").addText(String.valueOf(rsDetail.getInt(1)));
                        } else {
                             mobileUser     = rs.getInt(1);
                             g.updateBevMobileUser(mobileUser);
                            //toAppend.addElement("mobileUserId").addText(String.valueOf(rs.getInt(1)));
                        }
                        
                        String logMessage   = "Granting access to " + username + " for " + platformName;
                        g.addUserHistory(userId, "login", 0, logMessage, mobileUser);
                        getUserDetail(userId,mobileUser,toAppend);
                    }
                    break;
                case 2:
                    platformName            = "bevSync";
                    // we need to know if the user has Admin access
                    stmt                    = transconn.prepareStatement(checkBevSyncRoot);
                    stmt.setString(1, username);
                    stmt.setString(2, password);
                    stmt.setInt(3, platform);
                    rs                      = stmt.executeQuery();
                    if (rs != null && rs.next()) {
                        int rsIndex         = 1;
                        userId              = rs.getInt(rsIndex++);
                        toAppend.addElement("userId").addText(String.valueOf(userId));
                        toAppend.addElement("easternOffset").addText(HandlerUtils.nullToEmpty((rs.getString(rsIndex++))));
                        toAppend.addElement("threshold").addText(HandlerUtils.nullToEmpty((rs.getString(rsIndex++))));
                        toAppend.addElement("emailType").addText(String.valueOf(rsIndex++));
                        if (rs.getInt(rsIndex++) == ROOT_CUSTOMER) {
                            toAppend.addElement("securityLevel").addText("1");
                            String logMessage
                                            = "Granting Admin access to " + username + " for " + platformName;
                            logger.portalDetail(userId, "login", 0, logMessage, transconn);
                        } else {
                            toAppend.addElement("securityLevel").addText("3");
                            String logMessage
                                            = "Granting map-level access to " + username + " for " + platformName;
                            logger.portalDetail(userId, "login", 0, logMessage, transconn);
                        }
                        toAppend.addElement("productType").addText("1");
                        toAppend.addElement("groupId").addText(String.valueOf(rs.getInt(rsIndex++)));
                    }
                    break;
                default:
                    break;
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(rsDetail);
            close(rs);
            close(stmt);
        }
    }
    
    
    private void mGetCustomer(Element toHandle, Element toAppend) throws HandlerException {
        int userId                          = HandlerUtils.getOptionalInteger(toHandle, "userId");
        int secutityLevel                   = HandlerUtils.getOptionalInteger(toHandle, "securityLevel");
        PreparedStatement stmt              = null;
        ResultSet rs                        = null;
        try {
            if(userId > 0 ) {
                getUserDetail(userId,0,toAppend);
                int customer            = 0;
                stmt                    = transconn.prepareStatement("SELECT customer FROM user WHERE id= ?");
                stmt.setInt(1,userId);
                rs                      = stmt.executeQuery();
                if(rs.next()) {
                    customer            = rs.getInt(1);
                }
                if(customer ==0){
                    stmt                    = transconn.prepareStatement("SELECT id, name, acctNum, type FROM customer WHERE isActive = 1 ORDER BY name");
                    rs                      = stmt.executeQuery();
                    getCustomerXML(toAppend, rs,0);
                } else {
                    stmt                    = transconn.prepareStatement("SELECT id, name, acctNum, type FROM customer WHERE isActive = 1 AND id = ? ORDER BY name");
                    stmt.setInt(1,customer);
                    rs                      = stmt.executeQuery();
                    getCustomerXML(toAppend, rs,0);
                    
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
    
    private void getUserDetail(int targetId,int mobileUser, Element toAppend) throws HandlerException {
        

        String getUser =
                " SELECT name, username, email, customer, groupId, isManager, mobile, carrier, unit" +
                " FROM user WHERE id = ?";
        String getMap =
                " SELECT l.name, l.id, m.securityLevel FROM userMap m" +
                " LEFT JOIN location l ON m.location = l.id " +
                " WHERE m.user = ?";

        PreparedStatement stmt = null;
        ResultSet rs = null;

        try {
            int callerCust = -1, groupId = -1;

            stmt = transconn.prepareStatement(getUser);
            stmt.setInt(1, targetId);
            rs = stmt.executeQuery();
            if (rs.next()) {
                callerCust = rs.getInt(4);
                groupId = rs.getInt(5);
            }


            stmt = transconn.prepareStatement(getUser);
            stmt.setInt(1, targetId);
            rs = stmt.executeQuery();
            if (rs.next()) {
                String name = rs.getString(1);
                String username = rs.getString(2);
                String email = rs.getString(3);
                int customer = rs.getInt(4);
                int group = rs.getInt(5);
                int isManager = rs.getInt(6);
                //String mobile = rs.getString(7);
                //String carrier = rs.getString(8);
                //int unit = rs.getInt(9);

                //check that the caller should be able to see this info
                if (callerCust == 0 || callerCust == customer || ((groupId > 0) && (groupId == group))) {
                    Element detailEl = toAppend.addElement("userDetail");
                    detailEl.addElement("fullName").addText(HandlerUtils.nullToEmpty(name));
                    detailEl.addElement("username").addText(HandlerUtils.nullToEmpty(username));
                    detailEl.addElement("email").addText(HandlerUtils.nullToEmpty(email));
                    //detailEl.addElement("mobile").addText(HandlerUtils.nullToEmpty(mobile));
                    //detailEl.addElement("carrier").addText(HandlerUtils.nullToEmpty(carrier));
                    detailEl.addElement("isManager").addText(String.valueOf(isManager));
                   // detailEl.addElement("unit").addText(String.valueOf(unit));
                    detailEl.addElement("mobileUserId").addText(String.valueOf(mobileUser));
                    detailEl.addElement("userId").addText(String.valueOf(targetId));
                    /*if (isManager == 0) {
                        stmt = transconn.prepareStatement(getMap);
                        stmt.setInt(1, targetId);
                        rs = stmt.executeQuery();
                        while (rs.next()) {
                            Element permEl = toAppend.addElement("location");
                            permEl.addElement("locationName").addText(HandlerUtils.nullToEmpty(rs.getString(1)));
                            permEl.addElement("locationId").addText(String.valueOf(rs.getInt(2)));
                            //permEl.addElement("permission").addText(String.valueOf(rs.getInt(3)));
                        }
                    }*/
                } else {
                    addErrorDetail(toAppend, "You don't have permission to view this user");
                    logger.portalAccessViolation("Permission problem: Tried to getUserDetail on " + targetId + " by " + targetId);
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
    
    
    private void getCustomerXML(Element toAppend, ResultSet rs,int customerToLoad) throws SQLException {
        while (rs.next()) {
            int customerId                      = rs.getInt(1);
            if(customerId!=customerToLoad) {
            Element customerEl                  = toAppend.addElement("customer");
            customerEl.addElement("customerId").addText(String.valueOf(rs.getInt(1)));
            customerEl.addElement("name").addText(HandlerUtils.nullToEmpty(rs.getString(2)));
            //customerEl.addElement("acctNum").addText(String.valueOf(rs.getInt(3)));
            customerEl.addElement("cType").addText(String.valueOf(rs.getInt(4)));
            }
        }

    }
    
    
    private void mGetVariance(Element toHandle, Element toAppend) throws HandlerException {
        
       PeriodShiftType periodShift          = PeriodShiftType.instanceOf(HandlerUtils.getRequiredString(toHandle, "periodShift"));
       String businessDateString            = HandlerUtils.getRequiredString(toHandle, "businessDate");
       int locationId                       = HandlerUtils.getRequiredInteger(toHandle, "locationId");
       int mobileUserId                     = HandlerUtils.getOptionalInteger(toHandle, "mobileUserId");
       PreparedStatement stmt               = null;
       ResultSet rs                         = null;
       try {
           if(mobileUserId > 0) {
               stmt                         = transconn.prepareStatement("SELECT id FROM bevMobileLocationMap WHERE user = ? AND location = ?");
               stmt.setInt(1,mobileUserId);
               stmt.setInt(2,locationId);
               rs                           = stmt.executeQuery();
               if (!rs.next()) {
                   g.insertBevMobileLocationMap(mobileUserId, locationId);
               }
           }
           int periodTypes                     = periodShift.toSQLQueryInt();
           getOpenHours(toHandle, toAppend, businessDateString);   
           getLocationVariance(toHandle,toAppend);
       }catch (SQLException sqle) {
            logger.dbError("Database error in mGetVariance: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
           close(rs);
           close(stmt);
        }
    }
    
    private void getLocationVariance(Element toHandle, Element toAppend) throws HandlerException {
        int location = HandlerUtils.getOptionalInteger(toHandle, "locationId");

        String select = "Select varianceAlert FROM location where id=?";

        PreparedStatement stmt = null;
        ResultSet rs = null;

        try {
            stmt = transconn.prepareStatement(select);
            stmt.setInt(1, location);
            rs = stmt.executeQuery();
            while (rs.next()) {
                toAppend.addElement("varianceValue").addText(HandlerUtils.nullToEmpty(rs.getString(1)));
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error in getLocationVariance: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(rs);
            close(stmt);
        }
    }
    
    
    private void getOpenHours(Element toHandle, Element toAppend, String businessDateString) throws HandlerException {
        
        int locationId                      = HandlerUtils.getRequiredInteger(toHandle, "locationId");
        int customer                        = HandlerUtils.getOptionalInteger(toHandle, "customerId");
              
        
        String conditionString              ="";
        if(customer > 0) {
            conditionString                 ="l.customer="+customer;
        } else if(locationId > 0) {
            conditionString                 ="l.id="+locationId;           
        }
        
        String selectOpenHours              = "SELECT " +
                                            " DATE_SUB(Concat(LEFT(?,11),IFNULL(x.open,'11:00:00')), INTERVAL eO HOUR) Open, " +
                                            " DATE_SUB(If(x.close>'12:0:0',concat(LEFT(?,11),IFNULL(x.close,'02:00:00')),concat(LEFT(ADDDATE(?,1),11),IFNULL(x.close,'02:00:00'))), INTERVAL eO HOUR) Close, eO " +
                                            " FROM (Select CASE DAYOFWEEK(?) " +
                                            " WHEN 1 THEN Right(lH.openSun,8) " +
                                            " WHEN 2 THEN Right(lH.openMon,8) " +
                                            " WHEN 3 THEN Right(lH.openTue,8) " +
                                            " WHEN 4 THEN Right(lH.openWed,8) " +
                                            " WHEN 5 THEN Right(lH.openThu,8) " +
                                            " WHEN 6 THEN Right(lH.openFri,8) " +
                                            " WHEN 7 THEN Right(lH.openSat,8) END open, " +
                                            " CASE DAYOFWEEK(?) " +
                                            " WHEN 1 THEN Right(lH.closeSun,8) " +
                                            " WHEN 2 THEN Right(lH.closeMon,8) " +
                                            " WHEN 3 THEN Right(lH.closeTue,8) " +
                                            " WHEN 4 THEN Right(lH.closeWed,8) " +
                                            " WHEN 5 THEN Right(lH.closeThu,8) " +
                                            " WHEN 6 THEN Right(lH.closeFri,8) " +
                                            " WHEN 7 THEN Right(lH.closeSat,8) END close, " +
                                            " l.easternOffset eO " +
                                            " FROM locationHours lH RIGHT JOIN location l ON lH.location=l.id " +
                                            " WHERE "+conditionString+") AS x; ";
        String selectEventHours             = "SELECT preOpen ,eventEnd FROM eventHours WHERE location =? and date = DATE(?);";

        PreparedStatement stmt              = null;
        ResultSet rs                        = null, typeRs = null,rsDetails =null;

        logger.portalAction("selectOpenHours");

        try {
            String periodType               = HandlerUtils.getRequiredString(toHandle, "periodType");
            String periodDetail             = HandlerUtils.getRequiredString(toHandle, "periodDetail");
            
            if(locationId > 0) {
                stmt                        = transconn.prepareStatement("SELECT type FROM location WHERE id = ?");
                stmt.setInt(1,locationId);
                typeRs = stmt.executeQuery();
                if(typeRs.next()) {
                    int type                = typeRs.getInt(1);
                    if(type ==1) {
                        //Check that this product doesn't already exist in inventory at this location
                        stmt = transconn.prepareStatement(selectOpenHours);
                        stmt.setString(1, businessDateString);
                        stmt.setString(2, businessDateString);
                        stmt.setString(3, businessDateString);
                        stmt.setString(4, businessDateString);
                        stmt.setString(5, businessDateString); 
                        rs                  = stmt.executeQuery();
                    //logger.debug("bS"+businessDateString);
                    if (rs.next()) {
                        String startDate    = rs.getString(1);
                        String endDate      = rs.getString(2);
                        //logger.debug("SD:"+startDate+ " ED"+endDate+"pD"+periodDetail+"PT:"+periodType);
                        mGetPoured(startDate,endDate,locationId,customer,periodType,periodDetail,toAppend);
                        
                    }
                    } else if(type == 2) {
                        stmt                = transconn.prepareStatement(selectEventHours);
                        stmt.setInt(1, locationId);
                        stmt.setString(2, businessDateString);
                        rs                  = stmt.executeQuery();
                        String startDate    = null;
                        String endDate      = null;
                        if (rs.next()) {
                            startDate       = rs.getString(1);
                            endDate         = rs.getString(2);
                        } else {
                            stmt            = transconn.prepareStatement("SELECT DATE_ADD(DATE(?),INTERVAL 7 HOUR) ,DATE_ADD(DATE(?),INTERVAL 31 HOUR);");
                            stmt.setString(1, businessDateString);
                            stmt.setString(2, businessDateString);
                            rsDetails       = stmt.executeQuery();
                            if (rsDetails.next()) {
                                startDate   = rsDetails.getString(1);
                                endDate     = rsDetails.getString(2);
                            }
                        }
                        mGetBarPoured(startDate, endDate, locationId, periodType, periodDetail, toAppend);                       
                    }
                }
            } else if(customer > 0) {
                stmt = transconn.prepareStatement(selectOpenHours);
                stmt.setString(1, businessDateString);
                stmt.setString(2, businessDateString);
                stmt.setString(3, businessDateString);
                stmt.setString(4, businessDateString);
                stmt.setString(5, businessDateString); 
                rs                  = stmt.executeQuery();
                //logger.debug("bS"+businessDateString);
                if (rs.next()) {
                    String startDate        = rs.getString(1);
                    String endDate          = rs.getString(2);
                    
                    
                    mGetPoured(startDate,endDate,locationId,customer,periodType,periodDetail,toAppend);
                    
                }
            }
            
            

        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } catch (Exception e) {
            logger.debug(e.getMessage());
        }finally {
             close(rsDetails);
              close(typeRs);
            close(stmt);
            close(rs);
        }

    }
    
    
    private void mGetPoured(String startTime,String endTime,int location,int customer,String periodStr,String periodDetail,Element toAppend) throws HandlerException {

        java.text.DecimalFormat twoDForm    = new java.text.DecimalFormat("#.##");
        java.text.DateFormat timeParse      = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        double totalPoured                  = 0.00;
        double totalSold                    = 0.00;

        logger.portalAction("getPoured");
        //int tableId = 164;
         //String startTime ="2013-06-18 14:00:00",  endTime="2013-06-19 06:00:00",periodStr = "DAILY", periodDetail ="7";
        PreparedStatement stmt              = null;
        ResultSet rs                        = null;

        try {
            java.util.Date start            = timeParse.parse(startTime);
            java.util.Date end              = timeParse.parse(endTime);

            PeriodType periodType           = PeriodType.parseString(periodStr);
            if (null == periodType) {
                throw new HandlerException("Invalid period type: " + periodStr);
            }
            //For Testing purposes change the below date.
            //start = timeParse.parse("2009-06-09 08:00:00");
            //end = timeParse.parse("2009-06-10 08:00:00");
            ReportPeriod period             = null;
            try {
                period                      = new ReportPeriod(periodType, periodDetail, start, end);
            } catch (IllegalArgumentException e) {
                throw new HandlerException(e.getMessage());
            }

            SortedSet<DatePartition> dps    = DatePartitionFactory.createPartitions(period);
            //logger.debug("Created partitions: \n"+DatePartitionFactory.partitionReport(dps));
            DatePartitionTree dpt           = new DatePartitionTree(dps);

            ReportResults rrs               = null;
            /*if(customer > 0) {
                rrs                         = ReportResults.getResultsByCustomer(period, 0, true, false, customer, "",0, transconn);
            } else if(tableId > 0) {
                rrs                         = ReportResults.getResultsByLocation(period, 0, true, false, tableId, 0, transconn);
            }*/
            StringBuilder lineString        = new StringBuilder();
            LineString ls                   = null;
            if(customer > 0) {
                ls                          = new LineString(transconn, 1, customer, 1, 0, period, "");
                lineString                  = ls.getLineString();
            } else if(location > 0) {
                ls                          = new LineString(transconn, 2, location, 1, 0, period, "");
                lineString                  = ls.getLineString();
            }
            //logger.debug("Line String: " + lineString.toString());
            rrs                             = ReportResults.getResultsByLineString(period, true, false, lineString.toString(), transconn);
           
                  
            PeriodStructure pss[]           = null;
            PeriodStructure ps              = null;
            int dpsSize                     = dps.size();
            int index;
            if (dpsSize > 0) {
                pss                         = new PeriodStructure[dpsSize];
                //this is a temporary array of the DatePartitions used to construct the PeriodStructures start dates
                Object[] dpa                = dps.toArray();
                for (int i = 0; i < dpsSize; i++) {
                    // create a new PeriodStructure and link it to the previous one (or null for the first)
                    ps                      = new PeriodStructure(ps, ((DatePartition) dpa[i]).getDate());
                    pss[i]                  = ps;
                }
                int debugCounter            = 0;
                while (rrs.next()) {
                    
                    index                   = dpt.getIndex(rrs.getDate());
                    pss[index].addReading(rrs.getLine(), rrs.getValue(), rrs.getDate(), rrs.getQuantity());
                    debugCounter++;
                    //logger.debug("#"+debugCounter+" ["+index+"]: "+"L "+rrs.getLine()+" V: "+rrs.getValue()+" D: "+rrs.getDate().toString());
                }
                rrs.close();
                //logger.debug("Processed " + debugCounter + " readings");
                
              Map<Integer, String> productMap 
                                            = new HashMap<Integer, String>();
              Map<String, Double> pouredValueMap 
                                            = new HashMap<String, Double>();
              String condition              = "";
              if(customer > 0) {
                  condition                 = " AND lo.customer="+customer;
              } else if(location >0 ) {
                  condition                 = " AND s.location="+location;
                  
              }
              
              
                String sql          = "SELECT l.id,p.name FROM line l LEFT JOIN system s ON s.id = l.system LEFT JOIN product p ON p.id = l.product"
                                    + "  LEFT JOIN bar b ON b.id = l.bar LEFT JOIN location lo ON lo.id = s.location WHERE status='RUNNING' "+condition;
                stmt = transconn.prepareStatement(sql);
                rs = stmt.executeQuery();
                while (rs.next()) {
                    
                    productMap.put(rs.getInt(1), rs.getString(2) );
                    //logger.debug("For Line : " + rs.getInt(1) + " Product = " +  rs.getString(2) );
                }
                
                for (int i = 0; i < dpsSize; i++) {
                    Map<Integer, Double> lineMap 
                                            = pss[i].getValues(transconn);
                    if (null != lineMap && lineMap.size() > 0) {
                        for (Integer key : lineMap.keySet()) {
                            totalPoured     += Double.parseDouble(twoDForm.format(lineMap.get(key)));
                            String product  = productMap.get(key);
                            if(pouredValueMap.containsKey(product)) {
                                double previousValue 
                                           = pouredValueMap.get(product);
                                pouredValueMap.remove(product);
                                pouredValueMap.put(product,lineMap.get(key)+previousValue);
                            } else {
                                pouredValueMap.put(product,lineMap.get(key));
                            }
                            //logger.debug("For Line : " + key + " poured = " + String.valueOf(lineMap.get(key)));
                        }
                    }
                }
                 Map<String, Double> soldValueMap =getSold(location,periodStr,periodDetail,startTime,endTime,customer);
                 for (String key : soldValueMap.keySet()) {
                     totalSold              += soldValueMap.get(key);
                     
                 }
                
                 for (Integer key : productMap.keySet()) {
                     String product         = productMap.get(key);
                     if(pouredValueMap.containsKey(product)||soldValueMap.containsKey(product)) {
                        double poured       = 0.0; 
                        if(pouredValueMap.containsKey(product))
                            poured          = Double.parseDouble(String.valueOf(pouredValueMap.get(product)));
                        double sold           = 0.0;
                        
                        if(soldValueMap.containsKey(product))
                            sold                  = Double.parseDouble(String.valueOf(soldValueMap.get(product)));
                        
                        double variance        = 0.0;
                        
                        if(sold==0 && poured==0) {
                            variance        = 0.00;
                        } else if (sold ==0) {
                            variance        = -100.00;
                        } else if (poured ==0){
                            variance        = 100.00;
                        } else {
                            variance        = ((sold - poured)/poured)*100; //Default Formula
                        }
                        
                        Element varianceEl = toAppend.addElement("variance");
                        varianceEl.addElement("productName").addText(product);
                        varianceEl.addElement("poured").addText(twoDForm.format(poured));
                        varianceEl.addElement("sold").addText(twoDForm.format(sold));
                        varianceEl.addElement("variance").addText(twoDForm.format(variance));
                        pouredValueMap.remove(product);
                        soldValueMap.remove(product);
                        //logger.debug( product + " poured = " + twoDForm.format(poured) +" sold = " +twoDForm.format( sold)+"Variance:"+twoDForm.format(variance));
                     }
                 }
                 
                 double variance            = 0.0;
                 if(totalSold==0 && totalPoured==0) {
                     variance               = 0.00;
                 } else if (totalSold ==0) {
                     variance               = -100.00;
                 } else if (totalPoured ==0){
                     variance               = 100.00;
                 } else {
                     variance               = ((totalSold - totalPoured)/totalPoured)*100; //Default Formula
                 }
                 //logger.debug("Total Poured = " + twoDForm.format(totalPoured) +" Total Sold = " + twoDForm.format(totalSold)+"Variance:"+twoDForm.format(variance));
                  Element varianceEl = toAppend.addElement("totalVariance");                  
                  varianceEl.addElement("poured").addText(twoDForm.format(totalPoured));
                  varianceEl.addElement("sold").addText(twoDForm.format(totalSold));
                  varianceEl.addElement("variance").addText(twoDForm.format(variance));
            }
        } catch (ParseException pe) {
            String badDate = (null == startTime) ? "start" : "end";
            throw new HandlerException("Could not parse " + badDate + " date.");
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(rs);
            close(stmt);
        }
       
    }
    
    
    private Map<String, Double> getSold(Integer tableId, String periodStr, String periodDetail, String startTime, String endTime,int customer)
            throws HandlerException {
        Map<String, Double> soldValueMap    = null;
        java.text.DecimalFormat twoDForm    = new java.text.DecimalFormat("#.##");
        java.text.DateFormat timeParse      = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        double totalSold                    = 0.00;
        int location                        = 0, bar = 0;
        String selectCostCenter             = "SELECT b.location FROM costCenter c LEFT JOIN bar b ON b.id = c.bar WHERE c.bar = ? ";

        // A cache of beverage ingredient sets (maps PLU -> RRecSet)
        Map<String, Set<ReconciliationRecord>> ingredCache 
                                            = new HashMap<String, Set<ReconciliationRecord>>();

        // Maps product ids to RRecs (oz values);
        Map<Integer, ReconciliationRecord> productSet   
                                            = new HashMap<Integer, ReconciliationRecord>();

        logger.portalAction("getSold");

        PreparedStatement stmt              = null;
        ResultSet rs                        = null;
        try {
            java.util.Date start            = timeParse.parse(startTime);
            java.util.Date end              = timeParse.parse(endTime);

            PeriodType periodType           = PeriodType.parseString(periodStr);
            if (null == periodType) {
                throw new HandlerException("Invalid period type: " + periodStr);
            }
            //For Testing purposes change the below date.
            //start = timeParse.parse("2009-06-09 08:00:00");
            //end = timeParse.parse("2009-06-10 08:00:00");
            ReportPeriod period             = null;
            try {
                period                      = new ReportPeriod(periodType, periodDetail, start, end);
            } catch (IllegalArgumentException e) {
                throw new HandlerException(e.getMessage());
            }

            SalesResults srs                = null;
            location                        = tableId;
            if(customer >0 ) {
                //srs = SalesResults.getResultsByCustomer(period, customer, transconn);
                srs                         =SalesResults.getResultsByCustomer(period, 0, customer, transconn);
            } else if(location > 0) {
                srs                         = SalesResults.getResultsByLocation(period,0, location, transconn);
            }
                  
            int totalRecords                = 0;
            while (srs.next()) {
                String product              = srs.getPlu();
                double value                = srs.getValue();
                int loc                     = srs.getLocation();
                

                Set<ReconciliationRecord> baseSet 
                                            = null;
                if (ingredCache.containsKey(product)) {
                    baseSet                 = ingredCache.get(product);
                } else { // we need to do a db lookup and add the ingredients to the cache
                    //baseSet = ReconciliationRecord.recordByPlu(product, location, bar, 1.0, transconn);
                    baseSet                 = ReconciliationRecord.recordByPlu(product, loc, bar, 1.0, transconn);
                    ingredCache.put(product, baseSet);
                }
                Set<ReconciliationRecord> rSet
                                            = ReconciliationRecord.recordByBaseSet(baseSet, value);
                totalRecords                += rSet.size();
                // loop through all RRs and add them to the product set.
                for (ReconciliationRecord rr : rSet) {
                    Integer key             = new Integer(rr.getProductId());
                    ReconciliationRecord existingRecord = productSet.get(key);
                    if (existingRecord != null) {
                        existingRecord.add(rr);
                    } else {
                        productSet.put(key, rr);
                    }
                }
            }
            //logger.debug("Processed " + totalRecords + " reconciliation record(s)");
            // use the product set to create the XML to return.
            soldValueMap                    = new HashMap<String, Double>();
            Collection<ReconciliationRecord> recs = productSet.values();           
            for (ReconciliationRecord r : recs) {
                soldValueMap.put(r.getName(), r.getValue());
                if (r.getValue() > 0) {
                    totalSold               += r.getValue();                    
                }
            }
            //logger.debug("Total Sold = " + twoDForm.format(totalSold));
        } catch (ParseException pe) {
            String badDate = (null == startTime) ? "start" : "end";
            throw new HandlerException("Could not parse " + badDate + " date.");
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
            close(rs);
        }
        return soldValueMap;
    }
    
    
    private void mGetBarPoured(String startTime,String endTime,int location,String periodStr,String periodDetail,Element toAppend) throws HandlerException {

        java.text.DecimalFormat twoDForm    = new java.text.DecimalFormat("#.##");
        java.text.DateFormat timeParse      = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        
        double totalSold                    = 0.00;
        double totalPoured                  = 0.00;
                        


        logger.portalAction("getPoured");
        //int tableId = 164;
         //String startTime ="2013-06-18 14:00:00",  endTime="2013-06-19 06:00:00",periodStr = "DAILY", periodDetail ="7";
        PreparedStatement stmt              = null;
        ResultSet rs                        = null,rsBar = null;
        Map<Integer, String> barMap 
                                            = new HashMap<Integer, String>();
        Map<Integer, Double> barPouredMap 
                                            = new HashMap<Integer, Double>();
             

        try {
            java.util.Date start            = timeParse.parse(startTime);
            java.util.Date end              = timeParse.parse(endTime);

            PeriodType periodType           = PeriodType.parseString(periodStr);
            if (null == periodType) {
                throw new HandlerException("Invalid period type: " + periodStr);
            }
            //For Testing purposes change the below date.
            //start = timeParse.parse("2009-06-09 08:00:00");
            //end = timeParse.parse("2009-06-10 08:00:00");
            ReportPeriod period             = null;
            try {
                period                      = new ReportPeriod(periodType, periodDetail, start, end);
            } catch (IllegalArgumentException e) {
                throw new HandlerException(e.getMessage());
            }

            SortedSet<DatePartition> dps    = DatePartitionFactory.createPartitions(period);
            //logger.debug("Created partitions: \n"+DatePartitionFactory.partitionReport(dps));
            DatePartitionTree dpt           = new DatePartitionTree(dps);
            
            stmt = transconn.prepareStatement("SELECT id,name FROM bar WHERE location = ?");
            stmt.setInt(1, location);
            rsBar = stmt.executeQuery();
            while (rsBar.next()) {
                int barId                   = rsBar.getInt(1);
                String barName              = rsBar.getString(2);
                barMap.put(barId, barName);
                 ReportResults rrs          = null;
                 rrs                         = ReportResults.getResultsByBar(period, 0, false, barId, 0, transconn);
                 PeriodStructure pss[]           = null;
            PeriodStructure ps              = null;
            int dpsSize                     = dps.size();
            int index;
            if (dpsSize > 0) {
                pss                         = new PeriodStructure[dpsSize];
                //this is a temporary array of the DatePartitions used to construct the PeriodStructures start dates
                Object[] dpa                = dps.toArray();
                for (int i = 0; i < dpsSize; i++) {
                    // create a new PeriodStructure and link it to the previous one (or null for the first)
                    ps                      = new PeriodStructure(ps, ((DatePartition) dpa[i]).getDate());
                    pss[i]                  = ps;
                }
                int debugCounter            = 0;
                while (rrs.next()) {
                    
                    index                   = dpt.getIndex(rrs.getDate());
                    pss[index].addReading(rrs.getLine(), rrs.getValue(), rrs.getDate(), rrs.getQuantity());
                    debugCounter++;
                    //logger.debug("#"+debugCounter+" ["+index+"]: "+"L "+rrs.getLine()+" V: "+rrs.getValue()+" D: "+rrs.getDate().toString());
                }
                rrs.close();
                //logger.debug("Processed " + debugCounter + " readings");
                
            
                double poured                  = 0.00;
                for (int i = 0; i < dpsSize; i++) {
                    Map<Integer, Double> lineMap 
                                            = pss[i].getValues(transconn);
                    if (null != lineMap && lineMap.size() > 0) {
                        for (Integer key : lineMap.keySet()) {
                            totalPoured     += Double.parseDouble(twoDForm.format(lineMap.get(key)));                            
                            poured          += Double.parseDouble(twoDForm.format(lineMap.get(key)));                            
                            
                        }
                    }
                }
                barPouredMap.put(barId, poured);
                 
            }
            }
            
            
            Map<Integer, Double> soldValueMap 
                                            =getBarSold(location,periodStr,periodDetail,startTime,endTime);
            for (Integer key : soldValueMap.keySet()) {
                totalSold              += soldValueMap.get(key);
                
            }
            
            
            for (Integer key : barMap.keySet()) {
                String product         = barMap.get(key);
                     if(barPouredMap.containsKey(key)||soldValueMap.containsKey(key)) {
                        double poured       = 0.0; 
                        if(barPouredMap.containsKey(key))
                            poured          = Double.parseDouble(String.valueOf(barPouredMap.get(key)));
                        double sold           = 0.0;
                        
                        if(soldValueMap.containsKey(key))
                            sold                  = Double.parseDouble(String.valueOf(soldValueMap.get(key)));
                        
                        double variance        = 0.0;
                        
                        if(sold==0 && poured==0) {
                            variance        = 0.00;
                        } else if (sold ==0) {
                            variance        = -100.00;
                        } else if (poured ==0){
                            variance        = 100.00;
                        } else {
                            variance        = ((sold - poured)/poured)*100; //Default Formula
                        }
                        
                        Element varianceEl = toAppend.addElement("variance");
                        varianceEl.addElement("productName").addText(product);
                        varianceEl.addElement("poured").addText(twoDForm.format(poured));
                        varianceEl.addElement("sold").addText(twoDForm.format(sold));
                        varianceEl.addElement("variance").addText(twoDForm.format(variance));
                        barPouredMap.remove(key);
                        soldValueMap.remove(key);
                        //logger.debug( product + " poured = " + twoDForm.format(poured) +" sold = " +twoDForm.format( sold)+"Variance:"+twoDForm.format(variance));
                     }
                 }
                 
                 double variance            = 0.0;
                 if(totalSold==0 && totalPoured==0) {
                     variance               = 0.00;
                 } else if (totalSold ==0) {
                     variance               = -100.00;
                 } else if (totalPoured ==0){
                     variance               = 100.00;
                 } else {
                     variance               = ((totalSold - totalPoured)/totalPoured)*100; //Default Formula
                 }
                 //logger.debug("Total Poured = " + twoDForm.format(totalPoured) +" Total Sold = " + twoDForm.format(totalSold)+"Variance:"+twoDForm.format(variance));
                  Element varianceEl = toAppend.addElement("totalVariance");                  
                  varianceEl.addElement("poured").addText(twoDForm.format(totalPoured));
                  varianceEl.addElement("sold").addText(twoDForm.format(totalSold));
                  varianceEl.addElement("variance").addText(twoDForm.format(variance));
                 
                
                
                
           
            
        } catch (ParseException pe) {
            String badDate = (null == startTime) ? "start" : "end";
            throw new HandlerException("Could not parse " + badDate + " date.");
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(rsBar);
            close(rs);
            close(stmt);
        }
       
    }
    
    
    private Map<Integer, Double> getBarSold(int location, String periodStr, String periodDetail, String startTime, String endTime)
            throws HandlerException {
        Map<Integer, Double> soldValueMap   = new HashMap<Integer, Double>();
        java.text.DecimalFormat twoDForm    = new java.text.DecimalFormat("#.##");
        java.text.DateFormat timeParse      = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        
       
        String selectCostCenter             = "SELECT b.location FROM costCenter c LEFT JOIN bar b ON b.id = c.bar WHERE c.bar = ? ";

        // A cache of beverage ingredient sets (maps PLU -> RRecSet)
        Map<String, Set<ReconciliationRecord>> ingredCache 
                                            = new HashMap<String, Set<ReconciliationRecord>>();

        // Maps product ids to RRecs (oz values);

        logger.portalAction("getSold");

        PreparedStatement stmt              = null;
        ResultSet rsBar                        = null;
        try {
            java.util.Date start            = timeParse.parse(startTime);
            java.util.Date end              = timeParse.parse(endTime);

            PeriodType periodType           = PeriodType.parseString(periodStr);
            if (null == periodType) {
                throw new HandlerException("Invalid period type: " + periodStr);
            }
            //For Testing purposes change the below date.
            //start = timeParse.parse("2009-06-09 08:00:00");
            //end = timeParse.parse("2009-06-10 08:00:00");
            ReportPeriod period             = null;
            try {
                period                      = new ReportPeriod(periodType, periodDetail, start, end);
            } catch (IllegalArgumentException e) {
                throw new HandlerException(e.getMessage());
            }
            
             stmt = transconn.prepareStatement("SELECT id,name FROM bar WHERE location = ?");
            stmt.setInt(1, location);
            rsBar = stmt.executeQuery();
            while (rsBar.next()) {
                Map<Integer, ReconciliationRecord> productSet
                                            = new HashMap<Integer, ReconciliationRecord>();
                int barId                   = rsBar.getInt(1);
                String barName              = rsBar.getString(2);
                
                SalesResults srs            = null;
                srs                         = SalesResults.getResultsByBar(period, 0, barId, transconn);
                int totalRecords            = 0;
                while (srs.next()) {
                    String product          = srs.getPlu();
                    double value            = srs.getValue();                    
                    int loc                 = srs.getLocation();
                    Set<ReconciliationRecord> baseSet 
                                            = null;
                    if (ingredCache.containsKey(product)) {
                        baseSet             = ingredCache.get(product);
                    } else { // we need to do a db lookup and add the ingredients to the cache
                        //baseSet = ReconciliationRecord.recordByPlu(product, location, bar, 1.0, transconn);
                        baseSet             = ReconciliationRecord.recordByPlu(product, loc, barId, 1.0, transconn);
                        ingredCache.put(product, baseSet);
                    }
                    Set<ReconciliationRecord> rSet
                                            = ReconciliationRecord.recordByBaseSet(baseSet, value);
                    totalRecords            += rSet.size();
                    // loop through all RRs and add them to the product set.
                    for (ReconciliationRecord rr : rSet) {
                        Integer key         = new Integer(rr.getProductId());
                        ReconciliationRecord existingRecord 
                                            = productSet.get(key);
                        if (existingRecord != null) {
                            existingRecord.add(rr);
                        } else {
                            productSet.put(key, rr);
                        }
                    }
                }
                //logger.debug("Processed " + totalRecords + " reconciliation record(s)");
                // use the product set to create the XML to return.
                Collection<ReconciliationRecord> recs 
                                            = productSet.values();    
                double totalSold            = 0.0;
                for (ReconciliationRecord r : recs) {
                    if (r.getValue() > 0) {
                        totalSold           += r.getValue();
                    }
                }
                soldValueMap.put(barId, totalSold);
                //logger.debug("Total Sold = " + twoDForm.format(totalSold) + " for " + barName );
            }
        } catch (ParseException pe) {           
            String badDate                  = (null == startTime) ? "start" : "end";
            throw new HandlerException("Could not parse " + badDate + " date.");
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
            close(rsBar);
        }
        return soldValueMap;
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
                   //logger.debug("OnDek"+products);
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
             g.addUserVisitHistory(callerId, "getCurrentLines", locationId, "Draft Line Access",15, mobileUserId);
          }
         //getBars(toHandle, toAppend);

       
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
    
    
    
    private void mGetProducts1(Element toHandle, Element toAppend) throws HandlerException {

        int id = HandlerUtils.getOptionalInteger(toHandle, "productId");
        String name = HandlerUtils.getOptionalString(toHandle, "name");
        int prodType = HandlerUtils.getOptionalInteger(toHandle, "prodType");
        int brewery = HandlerUtils.getOptionalInteger(toHandle, "breweryId");
        
        int paramsSet = 0;
        if (id >= 0) {
            paramsSet++;
        }
        if ( null != name && !name.equals("")) {
            paramsSet++;
        }
        if (brewery >= 0) {
            paramsSet++;
        }
        if (paramsSet > 1) {
            throw new HandlerException("Only one parameter can be set for getProducts.");
        }

        PreparedStatement stmt = null;
        ResultSet rs = null;
        String select = "SELECT p.id, p.name, p.category "
                + " FROM product p ";
           //     + "  LEFT JOIN productSetMap pSM ON pSM.product = pD.product LEFT JOIN productSet pS ON pS.id = pSM.productSet ";
                
        String changeLogVersion             = "SELECT max(date) from productChangeLog;";
        


        try {

            if (id > 0) {
                //NischaySharma_24-Jun-2010_Start
                //NischaySharma_02-Mar-2010_Start
                String selectById = select + " WHERE  p.id = ?";
                stmt = transconn.prepareStatement(selectById);
                stmt.setInt(1, id);
                rs = stmt.executeQuery();
                getProducts1XML(toAppend, rs);
            } //NischaySharma_08-May-2009_Start: Added check if that if name is empty string then
            //do not execue the query for name
           /* else if (brewery > 0) {
                //NischaySharma_24-Jun-2010_Start
                //NischaySharma_02-Mar-2010_Start
                String selectByBrewery = select +  " WHERE pS.productSetType = 7 AND p.isActive = 1 AND pSM.productSet = ?";
                stmt = transconn.prepareStatement(selectByBrewery);
                stmt.setInt(1, brewery);
                rs = stmt.executeQuery();
                getProducts1XML(toAppend, rs);
            } //NischaySharma_08-May-2009_Start: Added check if that if name is empty string then
            //do not execue the query for name
            else if (null != name && !name.equals("")) {
                String selectByName = select + "WHERE pS.productSetType = 7  AND p.isActive = 1 AND p.name LIKE ?";
                name = '%' + name + '%';
                stmt = transconn.prepareStatement(selectByName);
                stmt.setString(1, name);
                rs = stmt.executeQuery();
                getProducts1XML(toAppend, rs);
            } //NischaySharma_08-May-2009_End*/
            else {
                //logger.debug("SelectAll");
                String selectAll = select + " WHERE  p.id > 0 AND p.pType = ?";
                //NischaySharma_24-Jun-2010_Start
                //NischaySharma_02-Mar-2010_End
                stmt = transconn.prepareStatement(selectAll);
                stmt.setInt(1, prodType);
                rs = stmt.executeQuery();
                getProducts1XML(toAppend, rs);
            }
            
            stmt = transconn.prepareStatement(changeLogVersion);
            rs = stmt.executeQuery();
            if(rs.next()) {
                toAppend.addElement("productVersion").addText(HandlerUtils.nullToEmpty(rs.getString(1)));
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(rs);
            close(stmt);
        }

    }
   
   
    /**
     * The folowing code is to fetch the products items for each location when a single beverage is selected 
     *---- SR
     */
    private void getProducts1XML(Element toAppend, ResultSet rs) throws SQLException {
        while (rs.next()) {
            int colCount                    = 1;
            Element ProductE1               = toAppend.addElement("product");
            ProductE1.addElement("productId").addText(String.valueOf(rs.getInt(colCount++)));
            ProductE1.addElement("name").addText(HandlerUtils.nullToEmpty(rs.getString(colCount++)));
            ProductE1.addElement("category").addText(String.valueOf(rs.getInt(colCount++)));
          //  ProductE1.addElement("breweryId").addText(String.valueOf(rs.getInt(colCount++)));            
        }
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
     
     
      private void getMobileDashBoardReport(Element toHandle, Element toAppend) throws HandlerException {
          
          DateParameter validatedStartDate  = null;
          DateParameter validatedEndDate    = null;
          String startDate                  = null;
          int callerId                      = getCallerId(toHandle);
          int mobileUserId                  = HandlerUtils.getOptionalInteger(toHandle, "mobileUserId");         
          ReportType reportType             = ReportType.instanceOf("Yesterday");
          String reportTypeString           = HandlerUtils.getOptionalString(toHandle, "reportType");          
          if (null != reportTypeString &&!reportTypeString.equals("Custom")) {
              reportType                    = ReportType.instanceOf(HandlerUtils.getOptionalString(toHandle, "reportType"));
              validatedStartDate            = new DateParameter(reportType.toStartDate());
              validatedEndDate              = new DateParameter(reportType.toEndDate());
              toAppend.addElement("startDate").addText(validatedStartDate.toString());
              toAppend.addElement("endDate").addText(validatedEndDate.toString());
              startDate                     =validatedStartDate.toString();  
          } else if(reportTypeString.equals("Custom")) {
              startDate                     = HandlerUtils.getRequiredString(toHandle, "startDate");
              toAppend.addElement("startDate").addText(startDate);
              toAppend.addElement("endDate").addText(startDate);
          }
          PeriodType periodType             = PeriodType.DAILY;
          String periodStr                  = HandlerUtils.getOptionalString(toHandle, "periodType");
          if (null != periodStr) {
              periodType                    = PeriodType.parseString(HandlerUtils.getOptionalString(toHandle, "periodType"));
          }
          if (null == periodType) {
             periodType             = PeriodType.MINUTELY;
          }
          int paramType                     = HandlerUtils.getOptionalInteger(toHandle, "paramType");
          int paramId                       = HandlerUtils.getOptionalInteger(toHandle, "paramId");
          int forChart                      = HandlerUtils.getOptionalInteger(toHandle, "forChart");
          int report                        = HandlerUtils.getOptionalInteger(toHandle, "report");          
          int eventId                       = HandlerUtils.getOptionalInteger(toHandle, "eventId");
          
          if(paramType == 2 && callerId>0){
              if(mobileUserId <1){
                  mobileUserId              = getMobileUserId(callerId);
              }
              
          }
          if(report == 1|| report==2 || report == 3) {
              
              if(reportType.equals(ReportType.Yesterday)||reportType.equals(ReportType.Today)||reportTypeString.equals("Custom") || eventId > 0) {
                  getOpenHours(toHandle, toAppend, periodStr, startDate +" 07:00:00");
                  g.addUserVisitHistory(callerId, "getReport", paramId, "Access Daily Report",15, mobileUserId);
              } else {
                  getSummaryReport(validatedStartDate.toString(), validatedEndDate.toString(), periodStr,toHandle, toAppend);   
                  g.addUserVisitHistory(callerId, "getSummaryReport", paramId, "Access Summary Report",15, mobileUserId);
              }       
          } 
      }
      
      
      private void getOpenHours(Element toHandle,Element toAppend, String periodStr, String businessDateString) throws HandlerException {
          
          int paramType                     = HandlerUtils.getOptionalInteger(toHandle, "paramType");
          int paramId                       = HandlerUtils.getOptionalInteger(toHandle, "paramId");
          int forChart                      = HandlerUtils.getOptionalInteger(toHandle, "forChart");
          int report                        = HandlerUtils.getOptionalInteger(toHandle, "report");          
          int eventId                       = HandlerUtils.getOptionalInteger(toHandle, "eventId");
          String conditionString            ="";
          if(paramType == 1) {
              conditionString               ="l.customer="+paramId;
          } else if(paramType == 2) {
              conditionString               ="l.id="+paramId;           
          }
          
          String selectOpenHours            = "SELECT " +
                                            " DATE_SUB(Concat(LEFT(?,11),IFNULL(x.open,'11:00:00')), INTERVAL eO HOUR) Open, " +
                                            " DATE_SUB(If(x.close>'12:0:0',concat(LEFT(?,11),IFNULL(x.close,'02:00:00')),concat(LEFT(ADDDATE(?,1),11),IFNULL(x.close,'02:00:00'))), INTERVAL eO HOUR) Close, eO " +
                                            " FROM (Select CASE DAYOFWEEK(?) " +
                                            " WHEN 1 THEN Right(lH.openSun,8) " +
                                            " WHEN 2 THEN Right(lH.openMon,8) " +
                                            " WHEN 3 THEN Right(lH.openTue,8) " +
                                            " WHEN 4 THEN Right(lH.openWed,8) " +
                                            " WHEN 5 THEN Right(lH.openThu,8) " +
                                            " WHEN 6 THEN Right(lH.openFri,8) " +
                                            " WHEN 7 THEN Right(lH.openSat,8) END open, " +
                                            " CASE DAYOFWEEK(?) " +
                                            " WHEN 1 THEN Right(lH.closeSun,8) " +
                                            " WHEN 2 THEN Right(lH.closeMon,8) " +
                                            " WHEN 3 THEN Right(lH.closeTue,8) " +
                                            " WHEN 4 THEN Right(lH.closeWed,8) " +
                                            " WHEN 5 THEN Right(lH.closeThu,8) " +
                                            " WHEN 6 THEN Right(lH.closeFri,8) " +
                                            " WHEN 7 THEN Right(lH.closeSat,8) END close, " +
                                            " l.easternOffset eO " +
                                            " FROM locationHours lH RIGHT JOIN location l ON lH.location=l.id " +
                                            " WHERE "+conditionString+") AS x; ";
        String selectEventHours             = "SELECT preOpen, IF(eventEnd > NOW(), NOW(), eventEnd) FROM eventHours WHERE location =? and date = DATE(?);";

        PreparedStatement stmt              = null;
        ResultSet rs                        = null, typeRs = null,rsDetails =null;

        logger.portalAction("selectOpenHours");

        try {
            
            if(paramType >1) {
                int location                = paramId;
                if(paramType == 3) {
                    stmt                    = transconn.prepareStatement("SELECT location FROM bar WHERE id=?;");
                    stmt.setInt(1, paramId);
                    rs                      = stmt.executeQuery();
                    if(rs.next()) {
                        location            = rs.getInt(1);
                    }
                }
                stmt                        = transconn.prepareStatement("SELECT type,varianceAlert FROM location WHERE id = ?");
                stmt.setInt(1,location);
                typeRs = stmt.executeQuery();
                if(typeRs.next()) {
                    int type                = typeRs.getInt(1);
                    toAppend.addElement("varianceValue").addText(HandlerUtils.nullToEmpty(typeRs.getString(2)));
                    if(type == 1) {
                        //Check that this product doesn't already exist in inventory at this location
                        stmt                = transconn.prepareStatement(selectOpenHours);
                        stmt.setString(1, businessDateString);
                        stmt.setString(2, businessDateString);
                        stmt.setString(3, businessDateString);
                        stmt.setString(4, businessDateString);
                        stmt.setString(5, businessDateString); 
                        rs                  = stmt.executeQuery();
                        //logger.debug("bS" + businessDateString);
                        String periodShift  = "30";
                        if (forChart > 0) {
                            periodShift      = "10";
                        }
                        if (rs.next()) {
                            String startDate= rs.getString(1);
                            String endDate  = rs.getString(2);
                            periodStr       = "MINUTELY";
                            getYesterdayPoured(startDate, endDate, periodStr, periodShift,false,toHandle, toAppend);
                        }
                    } else if(type == 2) {
                        if(eventId > 0) {
                            stmt                = transconn.prepareStatement("SELECT preOpen, eventEnd FROM eventHours WHERE id =? ;");
                            stmt.setInt(1, eventId);
                             rs                  = stmt.executeQuery();
                             String startDate    = null;
                             String endDate      = null;
                             String periodShift  = "8";
                             if (rs.next()) {
                                 startDate       = rs.getString(1);
                                 endDate         = rs.getString(2);    
                                 periodStr       = "MINUTELY";
                                 periodShift     = "10";
                             }
                             startDate           = adjustDateForConcession(startDate);
                             endDate             = adjustDateForConcession(endDate);
                             getYesterdayPoured(startDate, endDate, periodStr, periodShift, true,toHandle,toAppend);
                        
                        } else {
                            stmt                = transconn.prepareStatement(selectEventHours);
                            stmt.setInt(1, location);
                            stmt.setString(2, businessDateString);
                            rs                  = stmt.executeQuery();
                            String startDate    = null;
                            String endDate      = null;
                            String periodShift  = "8";
                            if (rs.next()) {
                                startDate       = rs.getString(1);
                                endDate         = rs.getString(2);    
                                periodStr       = "MINUTELY";
                                periodShift     = "10";
                            } else {
                                stmt            = transconn.prepareStatement("SELECT DATE_ADD(DATE(?),INTERVAL 7 HOUR) ,DATE_ADD(DATE(?),INTERVAL 31 HOUR);");
                                stmt.setString(1, businessDateString);
                                stmt.setString(2, businessDateString);
                                rsDetails       = stmt.executeQuery();
                                if (rsDetails.next()) {
                                    startDate   = rsDetails.getString(1);
                                    endDate     = rsDetails.getString(2);
                                    periodStr       = "Hourly";
                                }
                            }

                            startDate           = adjustDateForConcession(startDate);
                            endDate             = adjustDateForConcession(endDate);
                            getYesterdayPoured(startDate, endDate, periodStr, periodShift,true,toHandle, toAppend);
                        }
                        
                        
		
                        
                    }
                }
            } else if(paramType ==1) {
                stmt                        = transconn.prepareStatement(selectOpenHours);
                stmt.setString(1, businessDateString);
                stmt.setString(2, businessDateString);
                stmt.setString(3, businessDateString);
                stmt.setString(4, businessDateString);
                stmt.setString(5, businessDateString); 
                rs                  = stmt.executeQuery();
                //logger.debug("bS"+businessDateString);
                if (rs.next()) {
                    String startDate        = rs.getString(1);
                    String endDate          = rs.getString(2);
                    startDate                       = newDateFormat.format(setStartDate(2, paramId, "", startDate));
                    endDate                         = newDateFormat.format(setEndDate(2, paramId, "", startDate));
                    periodStr               ="MINUTELY";
                    String periodShift      = "30";
                    if (forChart > 0) {
                        periodShift         = "10";
                    }
                    getYesterdayPoured(startDate, endDate, periodStr, periodShift, false,toHandle,toAppend);
                    
                }
                stmt                = transconn.prepareStatement("Select max(varianceAlert) FROM location   where customer = ?;");
                stmt.setInt(1, paramId);
                rs                  = stmt.executeQuery();
                if(rs.next()) {
                    toAppend.addElement("varianceValue").addText(HandlerUtils.nullToEmpty(rs.getString(1)));
                }
            }
            
            

        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } catch (Exception e) {
            logger.debug(e.getMessage());
        }finally {
            close(rsDetails);
            close(typeRs);
            close(stmt);
            close(rs);
        }

    }
      
      private String adjustDateForConcession(String date) {
          PreparedStatement stmt            = null;
          ResultSet rs                      = null;
          try{
          int min                           = Integer.parseInt(date.substring(14, 16));
          int hour                          = Integer.parseInt(date.substring(11, 13));
          if(min !=0 ||min !=15||min !=30||min !=45){
              if(min>0 && min<15){
                  min                       = 15;
                  date                  = date.substring(0, 11);
              } else if(min >15 && min <30){
                  min         = 30;
                  date                  = date.substring(0, 11);
              } else if(min >30 && min <45){
                  min         = 45;
                  date                  = date.substring(0, 11);
              }else if(min>45 && min<60) {
                  min                       = 0;
                  hour++;
                  if(hour == 24) {
                      hour    = 0;
                      stmt                  = transconn.prepareStatement("SELECT DATE(DATE_ADD(DATE(?) ,INTERVAL   1 DAY));");
                      stmt.setString(1, date);
                      rs                    = stmt.executeQuery();
                      date                  = rs.getString(1) + " ";
                  } else {
                      date                  = date.substring(0, 11);
                  }
              } else {
                  date                  = date.substring(0, 11);
              }
          } else {
              date                  = date.substring(0, 11);
              
              
          }
          String Minute              = "00";
          String Hour                = "00";
          if(min>0){
              Minute                = String.valueOf(min);
          }
          if(hour<10){
              Hour                  = "0"+String.valueOf(hour);
          } else  {
              Hour                  = String.valueOf(hour);
          }
          
          date                      = date + Hour + ":" + Minute + ":00" ;
          
           } catch(SQLException e) {
          } finally {
              close(rs);
              close(stmt);
              
          }
          
          return date;
          
      }
      
      private String[] getBarString(int type,int location,String StartDate,String endDate) throws HandlerException {
          
          String selectSpecialHours         = "SELECT  eH.barString,  eH.preOpen, eH.eventEnd" +
                                            " FROM eventSpecialHours eH LEFT JOIN location l ON l.id = eH.location WHERE l.id = ? AND DATEDIFF(eH.date, DATE(?)) = 0  " + 
                                            " ORDER BY eH.eventPourStart ";
          String selectCateredHours         = "SELECT eH.barString, eH.eventPourStart, eH.eventEnd " +
                                            " FROM eventCateredHours eH LEFT JOIN location l ON l.id = eH.location WHERE l.id = ? AND DATEDIFF(eH.date, DATE(?)) = 0 " + 
                                            " ORDER BY eH.eventPourStart ";
          PreparedStatement stmt            = null;
          ResultSet rs                      = null;
          String specialBarString           = null, cateredBarString = null,exclusionLines =null,inclusionLines=null;      
          String barString[]                = {specialBarString,cateredBarString,null,null,"0","0"};
          //logger.debug("location:"+location +"SD:"+StartDate );
                  
          try{
              
              stmt                  = transconn.prepareStatement(selectSpecialHours);
              stmt.setInt(1, location);
              stmt.setString(2, StartDate);
              rs                    = stmt.executeQuery();
              if(rs.next()) {
                  barString[0]      = rs.getString(1);
                  barString[2]      = rs.getString(2);
                  barString[3]      = rs.getString(3);
                  specialBarString  = rs.getString(1);
                 //logger.debug(specialBarString);
              }
              stmt                    = transconn.prepareStatement(selectCateredHours);
              stmt.setInt(1, location);
              stmt.setString(2, StartDate);
              rs                      = stmt.executeQuery();
              if(rs.next()) {
                  barString[1]      = rs.getString(1);                  
                  cateredBarString  = rs.getString(1);
              }
              
              if (null != specialBarString) {
                  if(type == 1){
                      stmt                        = transconn.prepareStatement("SELECT GROUP_CONCAT(id) FROM line WHERE bar IN (" + specialBarString + ")");
                      rs                          = stmt.executeQuery();
                      if (rs.next()) {
                          exclusionLines          = inclusionLines = rs.getString(1);
                      }
                  } else {
                      stmt                    = transconn.prepareStatement("SELECT GROUP_CONCAT(ccID) FROM costCenter WHERE bar IN (" + specialBarString + ")");
                      rs                      = stmt.executeQuery();
                      if (rs.next()) {
                          exclusionLines = inclusionLines = rs.getString(1);
                      }
                  }
            }

            if (null != cateredBarString) {
                if(type == 1){
                    stmt                        = transconn.prepareStatement("SELECT GROUP_CONCAT(id) FROM line WHERE bar IN (" + cateredBarString + ")");
                    rs                          = stmt.executeQuery();
                    if (rs.next()) {
                        if (exclusionLines != null) {
                            exclusionLines      += ", " + rs.getString(1);
                        } else {
                            exclusionLines      = rs.getString(1);
                        }
                    }
                } else {
                    stmt                    = transconn.prepareStatement("SELECT GROUP_CONCAT(ccID) FROM costCenter WHERE bar IN (" + cateredBarString + ")");
                    rs                      = stmt.executeQuery();
                    if (rs.next() && rs.getString(1) != null) {
                        if (exclusionLines != null) {
                            exclusionLines  += ", " + rs.getString(1);
                        } else{
                            exclusionLines  = rs.getString(1);
                        }
                        }
                }
            }
            barString[4]                    = inclusionLines;
            barString[5]                    = exclusionLines;
            
            
          } catch(SQLException e) {
          }
          return barString;
          
      }
      
      
      private void getYesterdayPoured(String startTime, String endTime,String periodStr,String periodDetail,boolean concession,Element toHandle,Element toAppend) throws HandlerException {
          
          
          int paramType                     = HandlerUtils.getOptionalInteger(toHandle, "paramType");
          int paramId                       = HandlerUtils.getOptionalInteger(toHandle, "paramId");
          int forChart                      = HandlerUtils.getOptionalInteger(toHandle, "forChart");
          int report                        = HandlerUtils.getOptionalInteger(toHandle, "report");          
          int eventId                       = HandlerUtils.getOptionalInteger(toHandle, "eventId");
          
          java.text.DecimalFormat twoDForm  = new java.text.DecimalFormat("#.##");
          java.text.DateFormat timeParse    = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
          double totalPoured                = 0.00;         
          logger.portalAction("getPoured");
        
          PreparedStatement stmt            = null;
          ResultSet rs                      = null;
          String specialBarString           = null, cateredBarString = null,exclusionLines =null,inclusionLines=null;  
          Map<Integer, String> barMap       = null;
          Map<Integer, String> locationBarMap  
                                            = null;
          Map<Integer, String> barLineMap   = null;
          Map<Integer, String> locationLineMap  
                                            = null;
          LineOffsetMap lineOffsetMap       = null;
           int barLocationId                   = 0;
          try {
              java.util.Date start          = timeParse.parse(startTime);
              java.util.Date end            = timeParse.parse(endTime);
              if(!concession){
                 // periodDetail              = String.valueOf(start.getHours() + 1);
              }
              PeriodType periodType         = PeriodType.parseString(periodStr);
              if (null == periodType) {
                  throw new HandlerException("Invalid period type: " + periodStr);
              }
              
              ReportPeriod period           = null,specialEventPeriod = null;
              try {
                  if(paramType ==1) {
                      lineOffsetMap         = new LineOffsetMap(transconn, 2, paramId, "", dateToString(start));
                      locationLineMap       = new HashMap<Integer, String>();
                      locationBarMap        = new HashMap<Integer, String>();
                      String conditionLocation  = "";
                      if(forChart > 0) {
                          conditionLocation = " AND loc.id = "+String.valueOf(forChart);
                      }
                      stmt                  = transconn.prepareStatement("SELECT l.id, loc.id, loc.name FROM line l LEFT JOIN  bar b ON b.id=l.bar LEFT JOIN location loc on loc.id = b.location"
                                            + " LEFT JOIN locationDetails lD ON lD.location = loc.id  WHERE lD.active = 1 AND  loc.customer= ? "+conditionLocation);
                      stmt.setInt(1,paramId);
                      String result             = "0|Unknown Location";
                      rs                        = stmt.executeQuery();
                      while(rs.next()){
                          if (rs.getString(2) != null) {
                              result            = rs.getString(2) + "|" + rs.getString(3);
                          }
                          locationLineMap.put(rs.getInt(1),result);
                          locationBarMap.put(rs.getInt(2),rs.getString(3));
                      }
                  }
                  
                  if(concession) {
                      String conditionBar  = " b.location="+ String.valueOf(paramId);                     
                      if(paramType == 3) {
                          stmt          = transconn.prepareStatement("SELECT location FROM bar WHERE id=?;");
                          stmt.setInt(1, paramId);
                          rs            = stmt.executeQuery();
                          if(rs.next()) {
                              barLocationId  = rs.getInt(1);
                          }
                      } else {
                          barLocationId          = paramId;
                      }
                      //periodDetail          = "15";
                      if(barLocationId > 0){
                          conditionBar  = " b.location="+ String.valueOf(barLocationId);
                          String SpecialsBarString[] = getBarString(1, barLocationId, startTime, endTime);
                          specialBarString  = SpecialsBarString[0];
                          cateredBarString  = SpecialsBarString[1];                          
                          inclusionLines    =SpecialsBarString[4];
                          exclusionLines    =SpecialsBarString[5];
                          String specialStart =SpecialsBarString[2];
                          String specialEnd =SpecialsBarString[3];                          
                           if(specialBarString!=null ){
                               specialEventPeriod          = new ReportPeriod(periodType, periodDetail, timeParse.parse(specialStart), timeParse.parse(specialEnd));
                           }
                          //period                      = new ReportPeriod(periodType, periodDetail, timeParse.parse(specialStart), timeParse.parse(specialEnd));
                      } 
                      
                     
                      
                      locationBarMap        = new HashMap<Integer, String>();
                      barLineMap            = new HashMap<Integer, String>();
                      String result = "0|Unknown Bar";
                      stmt                  = transconn.prepareStatement("select l.id, b.id, b.name FROM line l LEFT JOIN bar b ON b.id = l.bar WHERE " + conditionBar);
                      //stmt.setInt(1,paramId);                      
                      rs                    = stmt.executeQuery();
                      while(rs.next()){
                           if (rs.getString(2) != null) {
                            result          = rs.getString(2) + "|" + rs.getString(3);
                        }
                           barLineMap.put(rs.getInt(1),result);
                           locationBarMap.put(rs.getInt(2),rs.getString(3));
                           
                      }
                      
                  }
                  period                    = new ReportPeriod(periodType, periodDetail, start, end);
                  //logger.debug("periodType:"+periodType +" periodDetail:"+periodDetail);
                  //logger.debug("start:"+start +" end:"+end);
                  
              } catch (IllegalArgumentException e) {
                  logger.debug(e.getMessage());
                  throw new HandlerException(e.getMessage());
              }
              
              SortedSet<DatePartition> dps  = DatePartitionFactory.createPartitions(period);              
              DatePartitionTree dpt         = new DatePartitionTree(dps);              
              boolean validTimeStamp        = true;
              
              ReportResults rrs             = null,specialRSS   = null;
              StringBuilder lineString        = new StringBuilder();
              LineString ls                   = null;
              if (forChart > 0) {
                switch (paramType) {
                    case 1:
                        ls                  = new LineString(transconn, 2, forChart, 1, 0, period, "");
                        lineString          = ls.getLineString();
                        rrs                 = ReportResults.getResultsByLineString(period, true, false, lineString.toString(), transconn);
                            
                        //rrs                   = ReportResults.getResultsByLocation(period, 0, true, false, forChart, 0, transconn);
                    break;                        
                    case 2:
                        if(!concession) {
                            ls              = new LineString(transconn, 2, paramId, 1, forChart, period, "");
                            lineString      = ls.getLineString();
                            rrs             = ReportResults.getResultsByLineString(period, true, false, lineString.toString(), transconn);
                            
                           // rrs               = ReportResults.getResultsByLocation(period, 0, true, false, paramId, forChart, transconn);
                        } else {
                            rrs                         = ReportResults.getResultsByBarConcessions(period, 0, false, forChart, 0, (exclusionLines == null ? "" : exclusionLines), "", transconn);
                            if (null != inclusionLines) {
                                specialRSS             = ReportResults.getResultsByBarConcessions(specialEventPeriod, 0, false, forChart, 0, "", inclusionLines, transconn);
                            }
                           //rrs           = ReportResults.getResultsByLocationConcessions(period, 0, true, false, paramId, 0, "", inclusionLines, transconn);
                        }
                        break;
                    case 3:
                         rrs                         = ReportResults.getResultsByBarConcessions(period, 0, false, paramId, forChart, (exclusionLines == null ? "" : exclusionLines), "", transconn);
                         if (null != inclusionLines) {
                             specialRSS             = ReportResults.getResultsByBarConcessions(specialEventPeriod, 0, false, paramId,forChart, "", inclusionLines, transconn);
                         }
                
                
                       //rrs           = ReportResults.getResultsByLocationConcessions(period, 0, true, false, barLocationId, forChart, "", inclusionLines, transconn);
                        break; 
                }
              } else {
                  switch (paramType) {
                      case 1:
                           ls               = new LineString(transconn, 1, paramId, 1, 0, period, "");
                           lineString       = ls.getLineString();
                           rrs              = ReportResults.getResultsByLineString(period, true, false, lineString.toString(), transconn);
                          //rrs                   = ReportResults.getResultsByCustomer(period, 0, true, false, paramId, "",0, transconn);
                          break;
                      case 2: 
                          if(!concession) {
                              ls            = new LineString(transconn, 2, paramId, 1, 0, period, "");
                              lineString    = ls.getLineString();
                              rrs           = ReportResults.getResultsByLineString(period, true, false, lineString.toString(), transconn);
                              //rrs               = ReportResults.getResultsByLocation(period, 0, true, false, paramId, 0, transconn);
                          } else {
                              rrs                         = ReportResults.getResultsByLocationConcessions(period, 0, true, false, paramId, 0, (exclusionLines == null ? "" : exclusionLines), "", transconn);
                              if (null != inclusionLines) {
                                  specialRSS             = ReportResults.getResultsByLocationConcessions(specialEventPeriod, 0, true, false, paramId, 0, "", inclusionLines, transconn);
                              }
                              //rrs               = ReportResults.getResultsByLocationConcessions(period, 0, true, false, paramId, 0, "", inclusionLines, transconn);
                          }
                          break;
                      case 3:
                          rrs                         = ReportResults.getResultsByBarConcessions(period, 0, false, paramId, forChart, (exclusionLines == null ? "" : exclusionLines), "", transconn);
                          if (null != inclusionLines) {
                              specialRSS             = ReportResults.getResultsByBarConcessions(specialEventPeriod, 0, false, paramId,forChart, "", inclusionLines, transconn);
                          }
                          //rrs           = ReportResults.getResultsByLocationConcessions(period, 0, true, false, barLocationId, forChart, "", inclusionLines, transconn);
                          break;
                  }
              }
              
             
           /* if(customer > 0) {
                ls                          = new LineString(transconn, 1, customer, 1, 0, period, "");
                lineString                  = ls.getLineString();
            } else if(location > 0) {
                ls                          = new LineString(transconn, 2, location, 1, 0, period, "");
                lineString                  = ls.getLineString();
            }
            //logger.debug("Line String: " + lineString.toString());
            rrs                             = ReportResults.getResultsByLineString(period, true, false, lineString.toString(), transconn);
           */
                  
                        
              
              PeriodStructure pss[]         = null;
              PeriodStructure ps            = null;
              int dpsSize                   = dps.size();
              int index;
              if (dpsSize > 0) {
                  pss                       = new PeriodStructure[dpsSize];
                  //this is a temporary array of the DatePartitions used to construct the PeriodStructures start dates
                  Object[] dpa              = dps.toArray();
                  for (int i = 0; i < dpsSize; i++) {
                      // create a new PeriodStructure and link it to the previous one (or null for the first)
                      ps                    = new PeriodStructure(ps, ((DatePartition) dpa[i]).getDate());
                      pss[i]                = ps;
                  }
                  int debugCounter          = 0;
                  double testLineSum        = 0;
                  while (rrs.next()) {
                      if (paramType == 1) {
                        validTimeStamp      = isTimeValid(lineOffsetMap.getLineOffset(rrs.getLine()), rrs.getDate());
                      }
                      if (validTimeStamp) {
                          index             = dpt.getIndex(rrs.getDate());
                          pss[index].addReading(rrs.getLine(), rrs.getValue(), rrs.getDate(), rrs.getQuantity());
                          debugCounter++;
                          if(rrs.getLine()==43283){
                              testLineSum +=rrs.getQuantity();
                              //logger.debug("#"+debugCounter+" ["+index+"]: "+"L "+rrs.getLine()+" V: "+rrs.getValue()+" Q: "+rrs.getQuantity()+" D: "+rrs.getDate().toString());
                          }
                          
                          if(rrs.getValue()==-1){
                          //logger.debug("#"+debugCounter+" ["+index+"]: "+"L "+rrs.getLine()+" V: "+rrs.getValue()+" Q: "+rrs.getQuantity()+" D: "+rrs.getDate().toString());
                          }
                      }
                       
                      
                  }
                  rrs.close();
                  
                  
                  if (specialRSS != null) {
                      try {
                          while (specialRSS.next()) {
                              index               = dpt.getIndex(specialRSS.getDate());
                              pss[index].addReading(specialRSS.getLine(), specialRSS.getValue(), specialRSS.getDate(), specialRSS.getQuantity());                        
                              debugCounter++;
                              //logger.debug("#"+debugCounter+" ["+index+"]: "+"L "+specialRSS.getLine()+" V: "+specialRSS.getValue()+" D: "+specialRSS.getDate().toString());
                          }
                          specialRSS.close();
                      } catch (SQLException sqle) {
                          throw new HandlerException(sqle);
                      } finally {
                      }
                  }
                  //logger.debug("For Line : 43283 poured = " + String.valueOf(testLineSum));
                  //logger.debug("Processed " + debugCounter + " readings");
                  Map<Integer, String> productLineMap 
                                            = new HashMap<Integer, String>();
                  Map<Integer, String> productMap 
                                            = new HashMap<Integer, String>();
                  Map<Integer, String> runningProductMap 
                                            = new HashMap<Integer, String>();
                  Map<String, Double> pouredProductValueMap 
                                            = new HashMap<String, Double>();
                  Map<Integer, Double> pouredValueMap 
                                            = new HashMap<Integer, Double>();
                  String condition          = "";
                  if(paramType == 1) {
                      if(forChart > 0) {
                          condition         = " AND lo.customer="+paramId+" AND s.location = "+forChart;
                      } else {
                          condition         = " AND lo.customer="+paramId;
                      }
                  } else if(paramType ==2 ) {
                      if(concession){
                          if(forChart > 0) {
                          condition         = " AND s.location="+paramId+" AND l.bar = "+forChart;
                          } else {
                              condition     = " AND s.location="+paramId;
                          }
                      }else {
                          if(forChart > 0) {
                          condition         = " AND s.location="+paramId+" AND p.id = "+forChart;
                          } else {
                              condition     = " AND s.location="+paramId;
                          }
                      }
                  } else if(paramType == 3){
                      if(forChart > 0) {
                          condition         = " AND l.bar="+paramId+" AND p.id = "+forChart;
                      } else {
                          condition         = " AND l.bar="+paramId;
                      }
                  }
                  
                  String sql                = "SELECT DISTINCT l.id,p.id,p.name,status FROM line l LEFT JOIN system s ON s.id = l.system LEFT JOIN product p ON p.id = l.product"
                                            + "  LEFT JOIN bar b ON b.id = l.bar LEFT JOIN location lo ON lo.id = s.location WHERE  status <> 'EMPTY' "+condition;
                  stmt                      = transconn.prepareStatement(sql);
                  rs                        = stmt.executeQuery();
                  while (rs.next()) {
                      if(rs.getString(3)!=null){
                      productLineMap.put(rs.getInt(1), rs.getString(3) );
                      productMap.put(rs.getInt(2), rs.getString(3) );
                      if(rs.getString(4).equals("RUNNING")) {
                          runningProductMap.put(rs.getInt(2), rs.getString(3) );
                          
                      }
                      //logger.debug("For Line : " + rs.getInt(1) + " Product = " +  rs.getString(2) );
                      //logger.debug( rs.getInt(1)+":"+rs.getInt(2)+":" +  rs.getString(3) );
                      }
                  }               
                  Map<String, Double> pouredDateValueMap
                                            = new HashMap<String, Double>();
                  Map<Integer, Double> pouredLocationValueMap    
                                            = new HashMap<Integer, Double>();
                 
                  index                     = 0;
                  double pouredSum          = 0;
                  testLineSum               = 0;
                  HashMap<Integer, LinePeriod> linePeriods
                                            = new HashMap<Integer, LinePeriod>();
                  //logger.debug("Reach");
                  Date d                    = new Date();
                  for (DatePartition dp : dps) {
                      //logger.debug("Date:"+dateFormat.format(dp.getDate()));
                      double dateValue      =0;                    
                      Map<Integer, Double> lineMap  = pss[index].getValues(transconn);
                      //Map<Integer, Double> lineMap  = pss[index].getTotalValues(transconn,linePeriods);
                      HashMap<String, Double> valueMap
                                            = new HashMap<String, Double>();
                      if (null != lineMap && lineMap.size() > 0) {
                          for (Integer key : lineMap.keySet()) {
                              totalPoured   += Double.parseDouble(twoDForm.format(lineMap.get(key)));                            
                              dateValue     +=Double.parseDouble(twoDForm.format(lineMap.get(key)));
                              if(key==43283){
                              testLineSum +=lineMap.get(key);
                              //logger.debug("# ["+index+"]: "+"L "+key+"  Q: "+lineMap.get(key));
                                }
                              String product= productLineMap.get(key);
                             
                              pouredSum     +=lineMap.get(key);
                              if(pouredProductValueMap.containsKey(product)) {
                                  double previousValue 
                                            = pouredProductValueMap.get(product);                                  
                                  pouredProductValueMap.remove(product);
                                  pouredProductValueMap.put(product,lineMap.get(key)+previousValue);
                              } else {
                                  pouredProductValueMap.put(product,lineMap.get(key));
                              }
                              
                              int productId             = getIntegerKey(productMap, product);
                              if(!runningProductMap.containsValue(product)) {
                                  runningProductMap.put(productId,product);
                              }
                              if(pouredValueMap.containsKey(productId)) {
                                  double previousValue 
                                            = pouredValueMap.get(productId);                                  
                                  pouredValueMap.remove(productId);
                                  pouredValueMap.put(productId,lineMap.get(key)+previousValue);
                              } else {
                                  pouredValueMap.put(productId,lineMap.get(key));
                              }
                              //logger.debug( product + " :"+key+" Value = " +  lineMap.get(key) );
                              
                             
                              
                               
                               if(concession && paramType == 2) {                                   
                                   String barId    
                                            = (barLineMap.get(key).toString().split("\\|")[0]);
                                   double value      
                                            = lineMap.get(key);
                                   if (valueMap.containsKey(barId)) {
                                       value
                                            += valueMap.get(barId);
                                   } 
                                   valueMap.put(barId, value);
                               } else if(paramType ==1) { 
                                   String locationId    
                                            = (locationLineMap.get(key).toString().split("\\|")[0]);
                                   double value      
                                            = lineMap.get(key);
                                   if (valueMap.containsKey(locationId)) {
                                       value
                                            += valueMap.get(locationId);
                                   } 
                                   valueMap.put(locationId, value);
                               }
                               
                              //logger.debug("For Line : " + key + " poured = " + String.valueOf(lineMap.get(key)));
                          }
                      }
                      if(concession && paramType == 2){
                           for (String bar : valueMap.keySet()) {
                               int barId    = Integer.parseInt(bar);
                          if(pouredLocationValueMap.containsKey(barId)) {
                              double previousValue 
                                            = pouredLocationValueMap.get(barId);
                              pouredLocationValueMap.remove(barId);
                              pouredLocationValueMap.put(barId,valueMap.get(bar)+previousValue);
                          } else {
                              pouredLocationValueMap.put(barId,valueMap.get(bar));
                          }
                           }
                          
                      }else if(paramType==1) {
                           for (String location : valueMap.keySet()) {
                               int locationId
                                            = Integer.parseInt(location);
                          if(pouredLocationValueMap.containsKey(locationId)) {
                              double previousValue 
                                            = pouredLocationValueMap.get(locationId);
                              pouredLocationValueMap.remove(locationId);
                              pouredLocationValueMap.put(locationId,valueMap.get(location)+previousValue);
                          } else {
                              pouredLocationValueMap.put(locationId,valueMap.get(location));
                          }
                      }
                      
                      }
                      index++;
                      pouredDateValueMap.put(dateFormat.format(dp.getDate()),dateValue);
                      //logger.debug("Date:"+dateFormat.format(dp.getDate())+" Value:"+dateValue);
                      d                           = dp.getDate();
                  }
                  long t                          = d.getTime();
                  Date afterAddingMins            = new Date(t + (600000));
                  //logger.debug("Max Date: " + dateFormat.format(afterAddingMins));  
                  pouredDateValueMap.put(dateFormat.format(afterAddingMins), 0.001);
                  
                  //logger.debug("For Line : 43283 poured = " + String.valueOf(testLineSum));
                  
                  /* for (String product : pouredProductValueMap.keySet()) {
                       logger.debug(product+":"+pouredProductValueMap.get(product));
                       
                   }
                   logger.debug("Total:"+totalPoured);
                   if(pouredProductValueMap.isEmpty()){
                     logger.debug("prodMap:"+productMap.size()+" Running:"+runningProductMap.size());
                      productMap            = runningProductMap;
                  }*/
                 
                  getYesterdaySold(paramType, periodType.toString(), periodDetail, startTime, endTime, paramId, runningProductMap, pouredProductValueMap, locationBarMap, pouredDateValueMap, pouredValueMap,pouredLocationValueMap,toAppend,report,concession,forChart,eventId);
                  generatePerformanceData(paramType, paramId, runningProductMap, pouredProductValueMap, pouredSum, toHandle, toAppend);
            }
        } catch (ParseException pe) {
            String badDate = (null == startTime) ? "start" : "end";
            throw new HandlerException("Could not parse " + badDate + " date.");
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(rs);
            close(stmt);
        }
       
    }

      
      
      private void  getYesterdaySold(Integer paramType, String periodStr, String periodDetail, String startTime, String endTime, int paramId,
              Map<Integer, String> productMap, Map<String, Double> pouredProductValueMap, Map<Integer, String> locationBarMap, Map<String, Double> pouredDateValueMap, Map<Integer, Double> pouredValueMap,Map<Integer, Double> pouredLocationValueMap, Element toAppend,int report,boolean  concession,int forChart,int eventId) throws HandlerException {
          
          
          Map<Integer, Double> soldValueMap  = null;         
          Map<String, Double> soldDateValueMap
                                            = null;
          Map<Integer, Double> soldLocationValueMap
                                            = null;
          Map<Integer, Integer> barMap       = null;
          java.text.DecimalFormat twoDForm  = new java.text.DecimalFormat("#.##");
          java.text.DateFormat timeParse    = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
          double totalSold                  = 0.00;
          int location                      = 0, bar = 0,barLocationId = 0;
          HashMap<String,Integer> costCenterMap
                                            = new HashMap<String,Integer>();
          String selectCostCenter           = "SELECT c.ccID,b.id  FROM costCenter c LEFT JOIN bar b ON b.id=c.bar ";
          String conditionBar               = " ";
          String conditionCostCenter        = " ";
          String inclusionCCenter           = "";
          String exclusionCostCenter          = null, inclusionCostCenter = null;

          // A cache of beverage ingredient sets (maps PLU -> RRecSet)
          Map<String, Set<ReconciliationRecord>> ingredCache
                                            = new HashMap<String, Set<ReconciliationRecord>>();

          // Maps product ids to RRecs (oz values);
          Map<Integer, ReconciliationRecord> productSet
                                            = new HashMap<Integer, ReconciliationRecord>();
          HashMap<Integer, HashMap<String, ReconciliationRecord>> resultSet
                                            = new HashMap<Integer, HashMap<String, ReconciliationRecord>>();
          
          logger.portalAction("getSold");

          PreparedStatement stmt            = null;
          ResultSet rs                      = null;
          try {
              
              java.util.Date start          = timeParse.parse(startTime);
              java.util.Date end            = timeParse.parse(endTime);
              if(!concession){
                  //periodDetail                  = String.valueOf(start.getHours() + 1);
              }
              ReportPeriod period           = null,specialPeriod = null;
              PeriodShiftType periodShift   = PeriodShiftType.instanceOf("open");
              if (paramType == 1) {                
                  locationMap               = new LocationMap(transconn, periodShift.toSQLQueryInt(), paramId, timeParse.parse(startTime).toString());
              } else if (paramType == 2) {
                  locationMap              = new LocationMap(transconn, periodShift.toSQLQueryInt(), String.valueOf(paramId),timeParse.parse(startTime).toString());
              }
              PeriodType periodType        = PeriodType.parseString(periodStr);
              
              if (null == periodType) {
                  throw new HandlerException("Invalid period type: " + periodStr);
              }
              
              if(concession) {
                  //periodDetail              = "15";
                   if(paramType == 3) {
                          stmt          = transconn.prepareStatement("SELECT location FROM bar WHERE id=?;");
                          stmt.setInt(1, paramId);
                          rs            = stmt.executeQuery();
                          if(rs.next()) {
                              barLocationId  = rs.getInt(1);
                          }
                          conditionCostCenter = "WHERE b.id =?;";
                      } else {
                          barLocationId          = paramId;
                          conditionCostCenter = "WHERE b.location =?";
                      }
                      //periodDetail          = "15";
                      if(barLocationId > 0){                          
                          String SpecialsBarString[] = getBarString(2, barLocationId, startTime, endTime);                          
                          inclusionCostCenter    =SpecialsBarString[4];
                          exclusionCostCenter    =SpecialsBarString[5];
                          String specialStart =SpecialsBarString[2];
                          String specialEnd =SpecialsBarString[3];
                          //logger.debug(inclusionCostCenter + ":"+exclusionCostCenter);
                          if(SpecialsBarString[0]!=null){
                              specialPeriod                 = new ReportPeriod(periodType, periodDetail, timeParse.parse(specialStart), timeParse.parse(specialEnd));
                          }
                          
                      } 
                     if(paramType == 3 || forChart > 0){
                         if(paramType == 3){
                             conditionCostCenter = "WHERE b.id =?;";
                             if(forChart >0 ) {
                                 conditionBar    = "  b.id ="+String.valueOf(paramId);
                             } else {
                                 conditionBar    = "  b.id ="+String.valueOf(paramId);
                                 
                             }
                         } else {
                             conditionCostCenter = "WHERE b.location =?";
                             if(forChart > 0) {
                                 conditionBar    = "  b.id ="+String.valueOf(forChart);
                             } else {
                                 conditionBar    = "  b.location ="+String.valueOf(paramId);
                             }
                         }
                         String selectCCenter      = "SELECT GROUP_CONCAT(c.ccID) FROM costCenter c LEFT JOIN bar b ON b.id=c.bar WHERE  "+conditionBar;                 
                         stmt                      = transconn.prepareStatement(selectCCenter);
                         rs                 = stmt.executeQuery();
                         if(rs.next()){
                             inclusionCCenter
                                            = rs.getString(1);
                         }
                     }
                     barMap                 = new HashMap<Integer, Integer>();  
                     stmt                   = transconn.prepareStatement(selectCostCenter+conditionCostCenter);
                     stmt.setInt(1,paramId);
                     rs                     = stmt.executeQuery();
                     while(rs.next()){
                         barMap.put(rs.getInt(1), rs.getInt(2));
                        //logger.debug(rs.getInt(1)+":"+rs.getInt(2)+":"+rs.getString(3));
                     }
                     if(inclusionCCenter==null){
                         inclusionCCenter   = "";
                     }
              }
              
              period                        = new ReportPeriod(periodType, periodDetail, start, end);
              
              SortedSet<DatePartition> dps  = DatePartitionFactory.createPartitions(period);
              DatePartitionTree dpt         = new DatePartitionTree(dps);
              SalesResults srs              = null,specialEventSR = null;  
              boolean byProduct             = false;
              boolean byMinute              = false;
              
              
              if (forChart > 0) {
                switch (paramType) {
                    case 1:
                        srs                 =SalesResults.getResultsByLocation(period, 0, forChart, transconn);                        
                    break;                        
                    case 2:
                        if(!concession) {
                            srs             =SalesResults.getResultsByLocationProduct(period, 0,paramId, forChart, transconn);         
                            byMinute = true;
                        } else {
                            byProduct       = true;
                            srs                          = SalesResults.getResultsByBarCostCenter(period, 0, forChart, (exclusionCostCenter == null ? "" : exclusionCostCenter), "", transconn);
                            if (null != inclusionCostCenter) {
                                specialEventSR          = SalesResults.getResultsByBarCostCenter(specialPeriod, 0, forChart, "", inclusionCostCenter, transconn);
                            }
                            //srs              = SalesResults.getResultsByBarCostCenter(period, 0, forChart, "", inclusionCCenter, transconn);
                        }
                        break;
                    case 3:
                        srs                          = SalesResults.getResultsByBarProductCostCenter(period, 0, paramId,forChart, (exclusionCostCenter == null ? "" : exclusionCostCenter), "", transconn);
                           if (null != inclusionCostCenter) {
                               specialEventSR          = SalesResults.getResultsByBarProductCostCenter(specialPeriod, 0, paramId, forChart,"", inclusionCostCenter, transconn);
                           }
                       //srs                  = SalesResults.getResultsByBarProductCostCenter(period, 0, paramId,forChart, "", inclusionCCenter, transconn);
                        break; 
                }
              } else {
                  switch (paramType) {
                      case 1:
                          srs               =SalesResults.getResultsByCustomer(period, 0, paramId, transconn);
                          break;
                      case 2: 
                          if(!concession) {
                              srs           = SalesResults.getResultsByLocation(period,0, paramId, transconn);
                              byProduct     = true;
                          } else {
                              srs                          = SalesResults.getResultsByLocationCostCenter(period, 0, paramId, (exclusionCostCenter == null ? "" : exclusionCostCenter), "", transconn);
                              if (null != inclusionCostCenter) {
                                  specialEventSR          = SalesResults.getResultsByLocationCostCenter(specialPeriod, 0, paramId, "", inclusionCostCenter, transconn);
                              }
                              //srs           = SalesResults.getResultsByLocationCostCenter(period, 0, paramId, "", inclusionCCenter, transconn);
                          }
                          break;
                      case 3:
                           srs                          = SalesResults.getResultsByBarCostCenter(period, 0, paramId, (exclusionCostCenter == null ? "" : exclusionCostCenter), "", transconn);
                           if (null != inclusionCostCenter) {
                               specialEventSR          = SalesResults.getResultsByBarCostCenter(specialPeriod, 0, paramId, "", inclusionCostCenter, transconn);
                           }
                          //srs               = SalesResults.getResultsByBarCostCenter(period, 0, paramId, "", inclusionCCenter, transconn);
                          break;
                  }
              }
            
            int totalRecords                = 0;
            int rsCounter                   = 0;
            boolean validTimeStamp          = true;            
            
            Map<Integer, Double> indexValueMap  
                                            = new HashMap<Integer, Double>();
            soldLocationValueMap            = new HashMap<Integer, Double>(); 
            ArrayList timeList              = new ArrayList();
            while (srs.next()) {
                rsCounter++;
                String plu                  = srs.getPlu();
                double value                = srs.getValue();
                Date timestamp              = srs.getDate();
                int locationId              = srs.getLocation();
                int costCenter              = srs.getCostCenter();
                double barValue             = 0;
                Integer dateKey             = new Integer(dpt.getIndex(timestamp)); 
                //logger.debug("plu: " + plu + ", timestamp: " + timestamp.toString() + ", value: " + value);
                //logger.debug("TimeStamp:"+timestamp + ":"+timeList.contains(timestamp) );
                boolean dublicate           = false;
                /*if(byMinute && timeList.contains(timestamp)){
                    dublicate               = true;
                }*/
                if (validTimeStamp && !dublicate ) {
                    timeList.add(timestamp);
                    Set<ReconciliationRecord> baseSet
                                            = getBaseSetFromCache(plu, locationId, costCenter, ingredCache);

                    Set<ReconciliationRecord> rSet
                                            = ReconciliationRecord.recordByBaseSet(baseSet, value);
                    totalRecords            += rSet.size();
                    // loop through all the returned RRs and add them to the appropriate product set.
                    for (ReconciliationRecord rr : rSet) {
                        String productKey   = String.valueOf(rr.getProductId());
                        dateKey             = new Integer(dpt.getIndex(timestamp));                                               
                        ReconciliationRecord existingRecord 
                                            = productSet.get(Integer.parseInt(productKey));
                        // here is where we check the product filter (used to be in the query)
                        if ((((forChart > 1 && paramType == 2) || (forChart > 1 && paramType == 3))) && forChart != rr.getProductId()) {
                            continue;
                        }
                        if (existingRecord != null) {
                            //logger.debug("Adding Volume for plu: " + plu + " with product: " + productKey + " with value: " + rr.getValue() + " at time: " + timestamp);
                            existingRecord.add(rr);
                        } else {
                            productSet.put(Integer.parseInt(productKey), rr);
                        }
                        barValue            += rr.getValue();
                    }
                    //logger.debug("Bar Value"+barValue );

                }
                try {
                    if(concession && forChart < 1){
                        int barName         = barMap.get(srs.getCostCenter());                        
                       //logger.debug("Bar Name"+barName);
                        if(soldLocationValueMap.containsKey(barName)) {
                            double previousValue
                                            = soldLocationValueMap.get(barName);
                            soldLocationValueMap.remove(barName);
                            soldLocationValueMap.put(barName,barValue+previousValue);
                            
                        } else {
                            soldLocationValueMap.put(barName,barValue);
                        }
                    } else if(paramType == 1){
                        //logger.debug("Location"+locationBarMap.get(locationId));
                        if(locationBarMap.containsKey(locationId)) {
                            if(soldLocationValueMap.containsKey(locationId)) {
                                double previousValue
                                            = soldLocationValueMap.get(locationId);
                                soldLocationValueMap.remove(locationId);
                                soldLocationValueMap.put(locationId,barValue+previousValue);
                                
                            } else {
                                soldLocationValueMap.put(locationId,barValue);
                            }
                        }
                    }
                } catch(Exception e){
                    logger.debug(""+e.getMessage());
                }
                
                if(indexValueMap.containsKey(dateKey)) {
                    double previousValue    = indexValueMap.get(dateKey);
                    indexValueMap.remove(dateKey);
                   indexValueMap.put(dateKey,barValue+previousValue);
                     //indexValueMap.put(dateKey,barValue);
                } else {
                    indexValueMap.put(dateKey,barValue);
                }
            }
            
            if(specialEventSR!=null){
                while (specialEventSR.next()) {
                rsCounter++;
                String plu                  = specialEventSR.getPlu();
                double value                = specialEventSR.getValue();
                Date timestamp              = specialEventSR.getDate();
                int locationId              = specialEventSR.getLocation();
                int costCenter              = specialEventSR.getCostCenter();
                double barValue             = 0;
                Integer dateKey             = new Integer(dpt.getIndex(timestamp)); 
                
                if (validTimeStamp) {
                    Set<ReconciliationRecord> baseSet
                                            = getBaseSetFromCache(plu, locationId, costCenter, ingredCache);

                    Set<ReconciliationRecord> rSet
                                            = ReconciliationRecord.recordByBaseSet(baseSet, value);
                    totalRecords            += rSet.size();
                    // loop through all the returned RRs and add them to the appropriate product set.
                    for (ReconciliationRecord rr : rSet) {
                        String productKey   = String.valueOf(rr.getProductId());
                        dateKey             = new Integer(dpt.getIndex(timestamp));                                               
                        ReconciliationRecord existingRecord 
                                            = productSet.get(Integer.parseInt(productKey));
                    if (existingRecord != null) {
                        existingRecord.add(rr);
                    } else {
                        productSet.put(Integer.parseInt(productKey), rr);                        
                    }
                    
                    barValue                += rr.getValue();
                                       
                    }
                   //logger.debug("Bar Value"+barValue );

                }
                try {
                if(concession && forChart < 1){
                    int barName          = barMap.get(specialEventSR.getCostCenter());
                    //logger.debug("Cost Center:"+srs.getCostCenter());
                    //logger.debug("Bar Name"+barName);
                    if(soldLocationValueMap.containsKey(barName)) {
                        double previousValue 
                                            = soldLocationValueMap.get(barName);
                        soldLocationValueMap.remove(barName);
                        soldLocationValueMap.put(barName,barValue+previousValue);                        
                    } else {
                        soldLocationValueMap.put(barName,barValue);
                    }
                } else if(paramType == 1){
                    //logger.debug("Location"+locationBarMap.get(locationId));
                    if(locationBarMap.containsKey(locationId)) {
                    if(soldLocationValueMap.containsKey(locationId)) {
                        double previousValue 
                                            = soldLocationValueMap.get(locationId);
                        soldLocationValueMap.remove(locationId);
                        soldLocationValueMap.put(locationId,barValue+previousValue);                        
                       
                    } else {
                        soldLocationValueMap.put(locationId,barValue);
                    }
                    }
                    
                }
                } catch(Exception e){
                    logger.debug(""+e.getMessage());
                }
                
                
                if(indexValueMap.containsKey(dateKey)) {
                    double previousValue    = indexValueMap.get(dateKey);
                    indexValueMap.remove(dateKey);
                    indexValueMap.put(dateKey,barValue+previousValue);
                    //indexValueMap.put(dateKey,barValue);
                } else {
                    indexValueMap.put(dateKey,barValue);
                }
            }
            }
           //logger.debug("Reach");
            int index                       = 0;
            soldDateValueMap                = new HashMap<String, Double>();
            Date d                          = new Date();         
            for (DatePartition dp : dps) {
                //logger.debug("Date:"+timeFormat.format(dp.getDate())+" Index:"+index );                                
                if(indexValueMap.containsKey(index)) {                    
                        soldDateValueMap.put(dateFormat.format(dp.getDate()),indexValueMap.get(index));
                } else {
                    soldDateValueMap.put(dateFormat.format(dp.getDate()),0.0);
                }
                d                           = dp.getDate();
                index++;
            } 
            //logger.debug("Max Date: " + dateFormat.format(d));  
            long t                          = d.getTime();
            Date afterAddingMins            = new Date(t + (600000));
            //logger.debug("Max Date: " + dateFormat.format(afterAddingMins));  
            soldDateValueMap.put(dateFormat.format(new Date(t + 600000)), 0.001);
            
            soldValueMap                    = new HashMap<Integer, Double>();
            Collection<ReconciliationRecord> recs 
                                            = productSet.values();           
            for (ReconciliationRecord r : recs) {
                soldValueMap.put(r.getProductId(), r.getValue());   
                if(!productMap.containsKey(r.getProductId())) {
                    productMap.put(r.getProductId(),r.getName());
                    //logger.debug("Missing product:"+r.getName()+" : "+r.getValue());
                }
                if(!pouredProductValueMap.containsKey(r.getName())) {
                    pouredProductValueMap.put(r.getName(), 0.0);
                    //logger.debug("Missing product:"+r.getName()+" : "+r.getValue());
                }

                
                if (r.getValue() > 0) {
                    totalSold               += r.getValue();                    
                }
            }
            
            boolean byCustomer              = false;
            if(paramType == 1 && forChart<1) {
                byCustomer                  = true;
                
            }
            
            if((concession && (forChart <1 && paramType==2) )|| paramType==1 ){
                generateVarianceData(paramType, paramId, locationBarMap, pouredLocationValueMap, soldLocationValueMap, toAppend, true, byCustomer,byProduct,eventId);
            } else if((concession && (forChart >1 && paramType==2))){
                generateVarianceData(3, forChart, productMap, pouredValueMap, soldValueMap, toAppend, false, byCustomer,byProduct,eventId);
            } else {
                generateVarianceData(paramType, paramId, productMap, pouredValueMap, soldValueMap, toAppend, false, byCustomer,byProduct,eventId);
            }
          
            double  totalPoured            = 0;
            totalSold                       = 0;
           
            if(report == 1|| report == 3){
                for (String key : pouredDateValueMap.keySet()) {
                    
                    double poured           = pouredDateValueMap.get(key);                   
                    double sold             = 0;
                    if(soldDateValueMap.containsKey(key)) {
                        sold                = soldDateValueMap.get(key);
                        //logger.debug("Key:"+key +": "+sold);
                         totalSold               +=sold;
                    }
                    totalPoured             +=poured;
                   
                    
                    boolean canSend                 = true;
                    if (((forChart >= 1 && paramType == 2) || (forChart >= 1 && paramType == 3))) {
                        if(poured ==0 && sold ==0 ) {
                            canSend         = false;
                        }
                    }
                    
                    if(canSend){
                    Element chart           = toAppend.addElement("varianceChart");
                    if(concession){
                        chart.addElement("value").addText(adjustHourOffset(2, barLocationId, dateFormat.parse(key)));
                        chart.addElement("eventId").addText(String.valueOf(eventId));
                        
                    } else {
                        chart.addElement("value").addText(adjustHourOffset(paramType, paramId, dateFormat.parse(key)));
                    }
                    chart.addElement("poured").addText(String.valueOf(poured));
                    chart.addElement("sold").addText(String.valueOf(sold));
                    chart.addElement("loss").addText(String.valueOf(sold - poured));
                    chart.addElement("variance").addText(String.valueOf(getVariance(poured, sold)*-1));
                    }
                } 
            }
        } catch (ParseException pe) {
            String badDate = (null == startTime) ? "start" : "end";
            throw new HandlerException("Could not parse " + badDate + " date.");
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
            close(rs);
        }
    }

    String adjustHourOffset(int paramType, int paramId, Date timestamp){
        String date                         = "";
        PreparedStatement stmt              = null;
        ResultSet rs                        = null;

        try {
            String selectEasternOffSet      = "SELECT ADDDATE(?, INTERVAl MIN(l.easternOffset) HOUR) FROM location l ";
            switch (paramType) {
                case 1:
                    selectEasternOffSet     += " LEFT JOIN customer c ON c.id = l.customer WHERE l.customer = ? GROUP BY l.customer";
                    break;
                case 2:
                    selectEasternOffSet     += " WHERE l.id = ? GROUP BY l.customer";
                    break;
            }
            stmt                            = transconn.prepareStatement(selectEasternOffSet);
            stmt.setString(1, dbDateFormat.format(timestamp));
            stmt.setInt(2, paramId);
            rs                              = stmt.executeQuery();
            if (rs.next()) {
                date                        = rs.getString(1);
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
        } finally {
            close(rs);
            close(stmt);
        }
        return date;
    }
    
    
    double getVariance(double poured,double sold){
        double variance                     = 0.0;
        if(sold==0 && poured==0) {
            variance                        = 0.00;
        } else if (sold ==0) {
            variance                        = -100.00;
        } else if (poured ==0){
            variance                        = 100.00;
        } else {
            variance                        = ((sold - poured)/poured)*100; //Default Formula
        }
        return variance;
    }
    
    private HashMap<String,Integer> getCostCenterMap(Integer tableType, Integer tableId, String tableString) throws HandlerException {
        PreparedStatement stmt              = null;
        ResultSet rs                        = null;

        HashMap<String,Integer> costCenterMap
                                            = new HashMap<String,Integer>();

        try {
            String selectAlertDescription   = "";


            switch (tableType) {
                case 1:
                    selectAlertDescription  += " SELECT c.id, c.ccId, l.id FROM costCenter c LEFT JOIN location l ON l.id = c.location WHERE l.customer = ? ";
                    break;

                case 2:
                    selectAlertDescription  += " SELECT id, ccId, location FROM costCenter WHERE location = ? ";
                    break;

                case 3:
                    selectAlertDescription  += " SELECT id, ccId, location FROM costCenter WHERE zone = ? ";
                    break;

                case 4:
                    selectAlertDescription  += " SELECT id, ccId, location FROM costCenter WHERE bar = ?";
                    break;

                case 5:
                    selectAlertDescription  += " SELECT id, ccId, location FROM costCenter WHERE station = ? ";
                    break;

                case 6:
                    selectAlertDescription  += " SELECT id, ccId, location FROM costCenter WHERE id > ? AND location in (" + tableString + ")";
                    break;
            }
            stmt                            = transconn.prepareStatement(selectAlertDescription);
            stmt.setInt(1, tableId);
            rs                              = stmt.executeQuery();
            while (rs.next()) {
                costCenterMap.put(rs.getString(3) + "-" + rs.getString(2),rs.getInt(1));
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(rs);
            close(stmt);
        }
        return costCenterMap;
    }
      
      
       private Set<ReconciliationRecord> getBaseSetFromCache(String plu, int location, int costCenter, Map<String, Set<ReconciliationRecord>> cache) {
        Set<ReconciliationRecord> baseSet   = null;
        String cacheIdentifier              = String.valueOf(location) + "-" + String.valueOf(costCenter) + "-" + plu;
        if (cache.containsKey(cacheIdentifier)) {
            baseSet                         = cache.get(cacheIdentifier);
        } else {
            // we need to do a db lookup and add the ingredients to the cache
            baseSet                         = ReconciliationRecord.recordByPlu(plu, location, costCenter, 1.0, transconn);
            cache.put(cacheIdentifier, baseSet);
        }
        return baseSet;
    }

      
       
       private void getSummaryReport(String startTime,String endTime,String periodStr,Element toHandle, Element toAppend) throws HandlerException {
           
           int paramType                     = HandlerUtils.getOptionalInteger(toHandle, "paramType");
          int paramId                       = HandlerUtils.getOptionalInteger(toHandle, "paramId");
          int forChart                      = HandlerUtils.getOptionalInteger(toHandle, "forChart");
          int report                        = HandlerUtils.getOptionalInteger(toHandle, "report");          
          

        String startDate                    = startTime;
        String endDate                      = endTime;
        
        
        String specificLocationsString      = "";

        int station                         = -1;
        int bar                             = -1;
        int zone                            = -1;
        int location                        = -1;
        int customer                        = -1;
        int group                           = -1;
        int supplier                        = -1;
        int county                          = -1;
        int region                          = -1;
        int user                            = -1;
        int product                         = -1;        
        boolean  byDay                      = HandlerUtils.getOptionalBoolean(toHandle, "byDay");  
        String start                        = HandlerUtils.getOptionalString(toHandle, "startDate");
        String end                          = HandlerUtils.getOptionalString(toHandle, "endDate");
        if(start!=null && !start.equals("")&& end!=null && !end.equals("")) {
            startDate                       = start;
            endDate                         = end;
            periodStr                       = "daily";
        }
        boolean concession                  = false;
        PreparedStatement stmt              = null;
        ResultSet rs                        = null, gameRs = null, locationRS = null,soldRs = null, pouredRs = null,typeRs=null;
        String specificLocations            = " ", soldSpecificProduct = " ", pouredSpecificProduct = " ", selectedLevel = " ",
                                            selectedValues = " r.id, l.countyIndex, ", soldExclusion = " ", pouredExclusion = " ", specificStates = " ",
                                            bevSyncLocation = " ";
        String groupPouredLevel             = " ", groupSoldLevel = " ", groupSoldValue = " s.value, ", groupPouredValue = " p.value, ";
        String userLocationExclusions       = "0", dataLocationExclusions = "3, 4, 5, 6", userLocationRequired = " ";
        
        if(paramType ==2 || paramType == 4){
        try{
            stmt                            = transconn.prepareStatement("SELECT type,varianceAlert FROM location WHERE id = ?");
            stmt.setInt(1,paramId);
            typeRs = stmt.executeQuery();
            if(typeRs.next()) {
                int type                    = typeRs.getInt(1);
                toAppend.addElement("varianceValue").addText(HandlerUtils.nullToEmpty(typeRs.getString(2)));
                if(type ==1) {
                    concession              = false;
                } else {
                    concession              = true;
                }
            }
            close(typeRs);
        } catch(Exception e){
            
        }
        } 
        //logger.debug("concession"+concession);
        if(paramType ==1) {
             try{
            stmt                = transconn.prepareStatement("Select max(varianceAlert) FROM location   where customer = ?;");
                stmt.setInt(1, paramId);
                rs                  = stmt.executeQuery();
                if(rs.next()) {
                    toAppend.addElement("varianceValue").addText(HandlerUtils.nullToEmpty(rs.getString(1)));
                }
                 } catch(Exception e){
            
        }
            customer                        = paramId;
            if(forChart > 0){
                specificLocationsString     = String.valueOf(forChart);
            }
        } else if(paramType ==2 || paramType ==4) {
            location                        = paramId;
             if(forChart > 0){
                 if(concession)
                     bar                = forChart;
                 else
                     product            = forChart;
             }
        } else if(paramType == 3) {
            concession              = true;
            bar                             = paramId;
             if(forChart > 0){
                 product                    = forChart;
             }
        }        

        boolean includeExclusion            = false;
        boolean includePoured               = false;
        boolean includeSold                 = false;
        boolean byProductCategory           = false;
        boolean simpleData                  = false;
        boolean monthly                     = false;
        boolean showProjectionDetails       = false;
        
        includePoured = true;
        includeSold   = true;
        //forChart      = true;
        
        boolean byLocation                  = false, groupUser = false;

        

        String exclusionQuery               = "SELECT eS.location, eS.bar, eS.exclusion, eS.date FROM exclusionSummary eS ";
        int exclusionParameter              = 0;
       
        //logger.debug("P:"+periodStr);

        if (!(specificLocationsString == null || specificLocationsString.equals(""))) {
            groupUser                       = true;
        }
        PeriodType periodType               = PeriodType.parseString(periodStr);
        if (null == periodType) {
            throw new HandlerException("Invalid period type: " + periodStr);
        }

        if (periodType == PeriodType.MONTHLY ) {
            groupPouredLevel                = " GROUP BY MONTH(p.date), p.location, p.product ";
            //groupPouredLevel                = " GROUP BY p.date, p.location, p.product ";
            groupPouredValue                = " SUM(p.value), ";
            groupSoldLevel                  = " GROUP BY MONTH(s.date), s.location, s.product ";
            //groupSoldLevel                  = " GROUP BY s.date, s.location, s.product ";
            groupSoldValue                  = " SUM(s.value), ";
            monthly                         = true;
        } else {
            groupPouredLevel                = " GROUP BY p.date, p.location, p.product ";
            groupPouredValue                = " SUM(p.value), ";
            groupSoldLevel                  = " GROUP BY s.date, s.location, s.product ";
            groupSoldValue                  = " SUM(s.value), ";
            monthly                         = false;
        }

        int paramsSet = 0;
        if (station >= 0) {
            exclusionQuery                  += " LEFT JOIN station st ON st.bar = eS.bar WHERE eS.date BETWEEN ? AND ? AND st.id = ? ";
            exclusionParameter              = station;
            paramsSet++;
        }
        
        if (bar >= 0) {
            exclusionQuery                  += " WHERE eS.date BETWEEN ? AND ? AND eS.bar = ? ";
            exclusionParameter              = bar;
        }
        
        if (location >= 0 ) {
            exclusionQuery                  += " WHERE eS.date BETWEEN ? AND ? AND eS.location = ? ";
            exclusionParameter              = location;
            paramsSet++;
        }
        
        if (customer >= 0) {
            exclusionQuery                  += " LEFT JOIN location l ON l.id = eS.location WHERE eS.date BETWEEN ? AND ? AND l.customer = ? ";
            exclusionParameter              = customer;
            paramsSet++;
        }

        if (product > 0) {
            soldSpecificProduct             = " AND s.product = ? ";
            pouredSpecificProduct           = " AND p.product = ? ";
        } 
        
        if(bar > 0){
             soldSpecificProduct             = " AND s.bar = ? ";
             pouredSpecificProduct           = " AND p.bar = ? ";
        }

        if (location >= 0 ) {
            selectedLevel                   = " AND l.id = ? ";
            selectedValues                  = " l.id, p.product, ";
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
        //logger.debug("SD:"+startDate);
        if(concession) {
            Map<Integer, Double> pouredDateValueMap    
                                            = new HashMap<Integer, Double>();
            Map<Integer, String> dateMap    = new HashMap<Integer, String>();
            Map<Integer, String> allBarMap  = new HashMap<Integer, String>();
            Map<String, Double> pouredProductValueMap 
                                            = new HashMap<String, Double>();
            Map<Integer, Double> soldDateValueMap    
                                            =new HashMap<Integer, Double>();
            Map<Integer, Double> pouredValueMap 
                                            = new HashMap<Integer, Double>();
            Map<Integer, Double> soldValueMap 
                                            = new HashMap<Integer, Double>();
            Map<Integer, Double> pouredBarValueMap
                                            = new HashMap<Integer, Double>();
            Map<Integer, Double> soldBarValueMap
                                            = new HashMap<Integer, Double>();
            Map<Integer, String> allProductMap     
                                            = new HashMap<Integer, String>();
            Map<Integer, Integer> allStationMap     
                                            = new HashMap<Integer, Integer>();
            Map<Integer, String> allGameMap =new HashMap<Integer, String>();
            Map<Integer, Double> pouredGameValueMap
                                            = new HashMap<Integer, Double>();
            Map<Integer, Double> soldGameValueMap
                                            = new HashMap<Integer, Double>();
            String selectEventHours         = "SELECT id, concat(eventDesc,'-',date)  FROM eventHours WHERE location = ? AND date between ? AND ?;";
            String selectBarStation         = "SELECT GROUP_CONCAT(DISTINCT l.station) FROM line l LEFT JOIN  bar b ON b.id=l.bar WHERE b.id=? AND l.station != 'NULL';" ;
            String ConditionString          = " e.location = "+location+" AND  ";
            String selectStation            = "select DIstinct l.station,b.id,b.name FROM line l LEFT JOIN  bar b ON b.id=l.bar WHERE b.location="+location+" AND l.station != 'NULL';";
            
            try {
                if (paramType == 3){
                    stmt                    = transconn.prepareStatement(selectBarStation);
                    stmt.setInt(1, bar);
                    rs                      = stmt.executeQuery();
                    if(rs.next()){
                        //logger.debug("Station:" + rs.getString(1));
                        if(forChart > 0){
                            ConditionString = " e.station IN ("+rs.getString(1)+") AND p.id ="+forChart +" AND ";
                        } else {
                            ConditionString = " e.station IN ("+rs.getString(1)+") AND  ";
                        }
                    }

                    selectStation           = "select DIstinct l.station,b.id,b.name FROM line l LEFT JOIN  bar b ON b.id=l.bar WHERE b.id="+bar+" AND l.station != 'NULL';";
                    stmt                = transconn.prepareStatement("SELECT location FROM bar WHERE id=?;");
                    stmt.setInt(1, bar);
                    rs                  = stmt.executeQuery();
                    if(rs.next()) {
                        location        = rs.getInt(1);

                    }
                } else if(paramType ==2) {
                    if(forChart > 0) {
                        stmt = transconn.prepareStatement(selectBarStation);
                        stmt.setInt(1, bar);
                        rs = stmt.executeQuery();
                        if(rs.next()){
                            ConditionString     = " e.location = "+location+" AND e.station IN ("+rs.getString(1)+") AND  ";
                        }

                       selectStation            = "select DIstinct l.station,b.id,b.name FROM line l LEFT JOIN  bar b ON b.id=l.bar WHERE b.id="+bar+" AND l.station != 'NULL';";
                    }
                }
                String selectEventPoured        = "SELECT  eH.eventDesc,e.date, e.product, p.name, e.value,e.station "
                                                + " FROM eventOpenHoursSummary e  LEFT JOIN eventHours eH ON eH.id=e.event "
                                                + " LEFT JOIN product p ON p.id=e.product "
                                                + " WHERE "+ConditionString+" eH.id = ?" ;

                String selectEventSold          = "SELECT eH.eventDesc, e.date, e.product, p.name, e.value,e.station"
                                                + " FROM eventOpenHoursSoldSummary e LEFT JOIN eventHours eH ON eH.id=e.event "
                                                + " LEFT JOIN product p ON p.id=e.product "
                                                + " WHERE "+ConditionString+" eH.id = ?" ;
            
            
                double totalPoured          = 0;
                double totalSold            = 0;
                stmt = transconn.prepareStatement(selectStation);
                //stmt.setInt(1, location);
                rs = stmt.executeQuery();
                while(rs.next()){
                    int barId              = rs.getInt(2);
                    allStationMap.put(rs.getInt(1),barId);
                    if(!allBarMap.containsKey(barId)){
                       //logger.debug("BarId:"+barId+" BarName:"+rs.getString(3));
                       allBarMap.put(barId, rs.getString(3));
                    }
                }
                 
                stmt = transconn.prepareStatement(selectEventHours);
                stmt.setInt(1, location);
                stmt.setString(2, validatedStartDate.toString());
                stmt.setString(3, validatedEndDate.toString());
                gameRs              = stmt.executeQuery();
                while(gameRs.next()){
                    int eventId             = gameRs.getInt(1);
                    String eventDesc        = gameRs.getString(2);
                    if(!allGameMap.containsKey(eventId)){
                        //logger.debug("BarId:"+barId+" BarName:"+rs.getString(3));
                        allGameMap.put(eventId, eventDesc);
                        
                    }
                    stmt = transconn.prepareStatement(selectEventPoured);
                    stmt.setInt(1, eventId);
                    //stmt.setString(1, validatedStartDate.toString());
                    //stmt.setString(2, validatedEndDate.toString());
                    //logger.debug("EventId"+eventId);
                    rs = stmt.executeQuery();
                    while(rs.next()){
                        String date             = rs.getString(2)+" "+rs.getString(1);
                        //String date             = rs.getString(2);
                        //logger.debug("Date:"+date);
                        int productId           = rs.getInt(3);
                        String productName      = rs.getString(4);
                        double poured           = rs.getDouble(5);
                        int stationId           = rs.getInt(6);
                        //logger.debug("stage 1"+stationId+""+allStationMap.get(stationId));

                        int barId               = 0;
                        if(allStationMap.containsKey(stationId)) {
                            barId           = allStationMap.get(stationId);
                        } else {
                            if(!allBarMap.containsKey(0)){
                                allBarMap.put(0,"Unknown");
                            }
                        }

                        totalPoured             +=poured;
                        if(pouredBarValueMap.containsKey(barId)) {
                            double previousValue    = pouredBarValueMap.get(barId);
                            pouredBarValueMap.remove(barId);
                            pouredBarValueMap.put(barId,poured+previousValue);
                        } else {
                            pouredBarValueMap.put(barId,poured);
                        }

                        if(pouredProductValueMap.containsKey(productName)) {
                            double previousValue    = pouredProductValueMap.get(productName);
                            pouredProductValueMap.remove(productName);
                            pouredProductValueMap.put(productName,poured+previousValue);
                        } else {
                            pouredProductValueMap.put(productName,poured);
                        }

                        if(pouredValueMap.containsKey(productId)) {
                            double previousValue    = pouredValueMap.get(productId);
                            pouredValueMap.remove(productId);
                            pouredValueMap.put(productId,poured+previousValue);
                        } else {
                            pouredValueMap.put(productId,poured);
                        }

                        if(pouredDateValueMap.containsKey(eventId)) {
                            double previousValue    = pouredDateValueMap.get(eventId);
                            pouredDateValueMap.remove(eventId);
                            pouredDateValueMap.put(eventId,poured+previousValue);
                        } else {
                            pouredDateValueMap.put(eventId,poured);
                        }

                        if(paramType ==4) {
                            if(pouredGameValueMap.containsKey(eventId)) {
                                double previousValue    = pouredGameValueMap.get(eventId);
                                pouredGameValueMap.remove(eventId);
                                pouredGameValueMap.put(eventId,poured+previousValue);
                            } else {
                                pouredGameValueMap.put(eventId,poured);
                            }
                        }
                        
                        if(!allProductMap.containsKey(productId)){
                            allProductMap.put(productId,productName);
                        }

                        if(!dateMap.containsKey(eventId)){
                            dateMap.put(eventId,date);
                        }
                    }


                    stmt = transconn.prepareStatement(selectEventSold);
                    stmt.setInt(1, eventId);
                    //stmt.setString(1, validatedStartDate.toString());
                    //stmt.setString(2, validatedEndDate.toString());
                    rs = stmt.executeQuery();
                    while(rs.next()){
                        String date             = rs.getString(2)+" "+rs.getString(1);

                        //String date             = rs.getString(2);
                        int productId           = rs.getInt(3);
                        String productName      = rs.getString(4);
                        double sold             = rs.getDouble(5);
                        int stationId           = rs.getInt(6);
                        int barId               = 0;
                        if(allStationMap.containsKey(stationId)) {
                            barId           = allStationMap.get(stationId);
                        } else {
                            if(!allBarMap.containsKey(0)){
                                allBarMap.put(0,"Unknown");
                            }
                        }

                        totalSold               +=sold;
                        if(soldBarValueMap.containsKey(barId)) {
                            double previousValue    = soldBarValueMap.get(barId);
                            soldBarValueMap.remove(barId);
                            soldBarValueMap.put(barId,sold+previousValue);
                        } else {
                            soldBarValueMap.put(barId,sold);
                        }
                        if(soldValueMap.containsKey(productId)) {
                            double previousValue    = soldValueMap.get(productId);
                            soldValueMap.remove(productId);
                            soldValueMap.put(productId,sold+previousValue);
                        } else {
                            soldValueMap.put(productId,sold);
                        }
                        if(soldDateValueMap.containsKey(eventId)) {
                            double previousValue    = soldDateValueMap.get(eventId);
                            soldDateValueMap.remove(eventId);
                            soldDateValueMap.put(eventId,sold+previousValue);
                        } else {
                            soldDateValueMap.put(eventId,sold);
                        }

                        if(paramType ==4) {
                            if(soldGameValueMap.containsKey(eventId)) {
                                double previousValue    = soldGameValueMap.get(eventId);
                                soldGameValueMap.remove(eventId);
                                soldGameValueMap.put(eventId,sold+previousValue);
                            } else {
                                soldGameValueMap.put(eventId,sold);
                            }
                        }

                        if(!allProductMap.containsKey(productId)){
                            allProductMap.put(productId,productName);
                        }

                        if(!dateMap.containsKey(eventId)){
                            dateMap.put(eventId,date);

                        }
                    }
                }
                
                double totalShare           = 0;
                
                if (paramType == 2&&forChart <=0)
                {
                    generateVarianceData(paramType, paramId, allBarMap, pouredBarValueMap, soldBarValueMap, toAppend, false,false,false,0);
                } else {
                    if(paramType==3){
                        generateVarianceData(3, paramId, allProductMap, pouredValueMap, soldValueMap, toAppend, false,false,false,0);
                    } else if(paramType == 4){
                        generateVarianceData(4, paramId, allGameMap, pouredGameValueMap, soldGameValueMap, toAppend, false,false,false,0);
                    } else {
                        generateVarianceData(3, forChart, allProductMap, pouredValueMap, soldValueMap, toAppend, false,false,true,0);
                    }
                }

                generatePerformanceData(paramType, paramId, allProductMap, pouredProductValueMap, totalPoured,toHandle, toAppend);
                
                Map<String, Double> pouredMonthValueMap    = new HashMap<String, Double>();
                Map<String, Double> soldMonthValueMap      = new HashMap<String, Double>();                
                 for (Integer key : dateMap.keySet()) {
                     String date            = dateMap.get(key);
                     double poured          = 0;
                     if(pouredDateValueMap.containsKey(key)) {
                         poured             = pouredDateValueMap.get(key); 
                         
                     }
                     double sold            = 0;
                     if(soldDateValueMap.containsKey(key)) {
                         sold               = soldDateValueMap.get(key);  
                         
                     }
                     if(!monthly) {
                         if(report == 1){
                             Element chart  = toAppend.addElement("chart");
                             chart.addElement("value").addText(date);
                             chart.addElement("poured").addText(String.valueOf(poured));
                             chart.addElement("sold").addText(String.valueOf(sold));
                             chart.addElement("variance").addText(String.valueOf(getVariance(poured, sold)));             
                         }  else if( report == 3){
                             if(poured>0 || sold >0 ){
                             Element chart  = toAppend.addElement("varianceChart");
                             //logger.debug("Date:"+date+":"+date.substring(0,10));
                             chart.addElement("value").addText(date.substring(0,10));
                             chart.addElement("poured").addText(String.valueOf(poured));
                             chart.addElement("sold").addText(String.valueOf(sold));
                             chart.addElement("loss").addText(String.valueOf(sold - poured));
                             chart.addElement("variance").addText(String.valueOf(getVariance(poured, sold)*-1));             
                             }
                         }
                     } else {
                                               
                         date               = date.substring(0,10)+"+";
                         String dateString  = date.substring(7,11);
                         date               = date.replace(dateString, "-01");
                         //logger.debug(""+date);   
                         if(pouredMonthValueMap.containsKey(date)) {
                             double previousValue    
                                            = pouredMonthValueMap.get(date);
                             pouredMonthValueMap.remove(date);
                             pouredMonthValueMap.put(date,poured+previousValue);
                         } else {
                             pouredMonthValueMap.put(date,poured);
                         }
                         if(soldMonthValueMap.containsKey(date)) {
                             double previousValue    
                                            = soldMonthValueMap.get(date);
                             soldMonthValueMap.remove(date);
                             soldMonthValueMap.put(date,sold+previousValue);
                         } else {
                             soldMonthValueMap.put(date,sold);
                         }
                     }
                 }
                  
                 if(monthly){
                     for (String key : pouredMonthValueMap.keySet()) {
                         double poured      = pouredMonthValueMap.get(key);
                         double sold        = 0;
                         if(soldMonthValueMap.containsKey(key)) {
                             sold           = soldMonthValueMap.get(key);
                         }
                         Element chart      = toAppend.addElement("varianceChart");
                         chart.addElement("value").addText(key);
                         chart.addElement("poured").addText(String.valueOf(poured));
                         chart.addElement("sold").addText(String.valueOf(sold));
                         chart.addElement("loss").addText(String.valueOf(sold - poured));
                         chart.addElement("variance").addText(String.valueOf(getVariance(poured, sold)*-1));             
                         
                     }                
                 }                   
                                
                 
            } catch (SQLException sqle) {
                throw new HandlerException(sqle);
            }catch(Exception e){
                       logger.debug(e.getMessage());
                   } finally {
                close(gameRs);
            }
        
        } else {
        if (includeSold) {
            dataLocationExclusions          += ", 2";
            String soldTableType            = HandlerUtils.getOptionalString(toHandle, "soldSummaryType");
            if (soldTableType == null) {
                soldTableType               = "openHoursSold";
            }
            if (!("sold".equals(soldTableType) || "openHoursSold".equals(soldTableType) || "preOpenHoursSold".equals(soldTableType) || "afterHoursSold".equals(soldTableType))) {
                throw new HandlerException("Invalid Summary Type: " + soldTableType);
            }
            String soldTable = soldTableType + "Summary";
            //logger.debug(soldTable);            

            String selectLocationSold       = "SELECT s.location, s.bar, s.product, " + groupSoldValue + " s.date FROM " + soldTable + " s " +
                    " WHERE   s.location = ? AND s.product NOT IN (4311, 10661) AND s.date BETWEEN ? AND ? " + soldSpecificProduct + soldExclusion + groupSoldLevel +
                    " ORDER BY s.date, s.location, s.product ";
            
            try {
                 
                
                Element soldData = toAppend;

                 if (location >= 0) {
                    stmt = transconn.prepareStatement(selectLocationSold);
                    stmt.setInt(1, location);
                    stmt.setString(2, validatedStartDate.toString());
                    stmt.setString(3, validatedEndDate.toString());
                    if (product > 0) {
                        stmt.setInt(4, product);
                    } else if(bar >0){
                        stmt.setInt(4, bar);
                    }
                    if(report ==1 || report ==3){
                        soldRs = stmt.executeQuery();
                    }
                    //appendSummaryReportByLocationXML(soldData, rs);
                    //logger.debug("Executing getSummaryReport for Location - Sold query");
                } else if (customer >= 0) {

                    if (!(specificLocationsString == null || specificLocationsString.equals(""))) {
                        specificLocations   = " AND s.location IN (" + specificLocationsString + ")";
                    } else {

                        stmt                = transconn.prepareStatement("SELECT GROUP_CONCAT(l.id) FROM location l LEFT JOIN locationDetails lD ON lD.location = l.id " +
                                            " WHERE l.customer = ? AND lD.active = 1");
                        stmt.setInt(1, customer);
                        rs                  = stmt.executeQuery();
                        if (rs.next()) {
                            specificLocations
                                            = " AND s.location IN (" + rs.getString(1) + ")";
                        }
                    }

                    String selectCustomerSold
                                            = "SELECT s.location, s.bar, s.product, " + groupSoldValue + " s.date FROM " + soldTable + " s " +
                                            " WHERE s.product NOT IN (4311, 10661) AND  s.date BETWEEN ? AND ? "
                                            + specificLocations + soldSpecificProduct + soldExclusion + groupSoldLevel +
                                            " ORDER BY s.date, s.location, s.product ";

                    
                    stmt = transconn.prepareStatement(selectCustomerSold);
                    stmt.setString(1, validatedStartDate.toString());
                    stmt.setString(2, validatedEndDate.toString());
                    if (product > 0) {
                        stmt.setInt(3, product);
                    }
                    if(report ==1 || report ==3){
                        soldRs = stmt.executeQuery();
                    }
                    //logger.debug("Executing getSummaryReport for Customer - Sold query");
                    //appendSummaryReportByLocationXML(soldData, rs);
                   
                   
                    
                } 
                //rs.close();


            } catch (SQLException sqle) {
                throw new HandlerException(sqle);
            } finally {
            }

        }

        if (includePoured) {
            dataLocationExclusions += ", 1";
            String pouredTableType = HandlerUtils.getOptionalString(toHandle, "pouredSummaryType");
            if (pouredTableType == null) {
                pouredTableType = "openHours";
            }
            if (!("poured".equals(pouredTableType) || "preOpenHours".equals(pouredTableType) || "openHours".equals(pouredTableType) || "afterHours".equals(pouredTableType) || "lineCleaning".equals(pouredTableType) || "bevSync".equals(pouredTableType))) {
                throw new HandlerException("Invalid Summary Type: " + pouredTableType);
            }

            String pouredTable = pouredTableType + "Summary";
            //logger.debug(pouredTable);

            

            String selectLocationPoured = "SELECT p.location, p.bar, p.product," + groupPouredValue + " p.date FROM " + pouredTable + " p " +
                    " WHERE   p.location = ? AND p.date BETWEEN ? AND ? " + pouredSpecificProduct + pouredExclusion + groupPouredLevel +
                    " ORDER BY p.date, p.location, p.product ";

            try {

                int parentLevel = 0, colCount = 1, param1 = -1, param2 = -1;

                Element pouredData = toAppend;
                Element varianceData        = toAppend;


                if (location >= 0 ) {
                    stmt = transconn.prepareStatement(selectLocationPoured);
                    stmt.setInt(1, location);
                    stmt.setString(2, validatedStartDate.toString());
                    stmt.setString(3, validatedEndDate.toString());
                    if (product > 0) {
                        stmt.setInt(4, product);
                    } else if(bar >0){
                        stmt.setInt(4, bar);
                    }
                    pouredRs = stmt.executeQuery();
                    //logger.debug("Executing getSummaryReport for Location - Poured query");
                    boolean byBar           = false;
                    
                    getPouredSoldData(pouredRs,soldRs,toHandle,varianceData,false,concession,monthly,byDay);
                    //appendSummaryReportByLocationXML(pouredData, rs);
                } else if (customer >= 0) {

                    if (!(specificLocationsString == null || specificLocationsString.equals(""))) {
                        specificLocations   = " AND p.location IN (" + specificLocationsString + ")";
                    } else {

                        stmt                = transconn.prepareStatement("SELECT GROUP_CONCAT(l.id) FROM location l LEFT JOIN locationDetails lD ON lD.location = l.id " +
                                            " WHERE l.customer = ? AND lD.active = 1");
                        stmt.setInt(1, customer);
                        rs                  = stmt.executeQuery();
                        if (rs.next()) {
                            specificLocations
                                            = " AND p.location IN (" + rs.getString(1) + ")";
                        }
                    }

                    String selectCustomerPoured
                                            = "SELECT p.location, p.bar, p.product, " + groupPouredValue + " p.date FROM " + pouredTable + " p " +
                                            " WHERE p.date BETWEEN ? AND ? " + specificLocations + pouredSpecificProduct + pouredExclusion + groupPouredLevel +
                                            " ORDER BY p.date, p.location, p.product ";
                    
                    stmt = transconn.prepareStatement(selectCustomerPoured);
                    stmt.setString(1, validatedStartDate.toString());
                    stmt.setString(2, validatedEndDate.toString());
                    if (product > 0) {
                        stmt.setInt(3, product);
                    }
                    pouredRs = stmt.executeQuery();
                    //logger.debug("Executing getSummaryReport for Customer - Poured query");
                    if(forChart >0){
                        getPouredSoldData(pouredRs,soldRs,toHandle,varianceData,false,false,monthly,byDay);
                    } else {
                        getPouredSoldData(pouredRs,soldRs,toHandle,varianceData,true,false,monthly,byDay);
                    }
                   // appendSummaryReportByLocationXML(pouredData, pouredRs);
                } 
                //rs.close();
            } catch (SQLException sqle) {
                throw new HandlerException(sqle);
            } finally {
                close(rs);                
                close(pouredRs);
                close(soldRs);
            }
        }
        }
   
       
    }
       
       
        private void getPouredSoldData( ResultSet pouredRs,ResultSet soldRs,Element toHandle,Element varianceData,boolean byLocation,boolean concession,  boolean monthly,boolean byDay ) throws HandlerException, SQLException {
            
            int paramType                     = HandlerUtils.getOptionalInteger(toHandle, "paramType");
          int paramId                       = HandlerUtils.getOptionalInteger(toHandle, "paramId");
          int forChart                      = HandlerUtils.getOptionalInteger(toHandle, "forChart");
          int report                        = HandlerUtils.getOptionalInteger(toHandle, "report");          
            
        Map<Date, ArrayList> summarySet     = new HashMap<Date, ArrayList>();
        Map<Integer, Date> dateArray        = new HashMap<Integer, Date>();
        Map<String, Double> pouredDateValueMap    =null;
        Map<Integer, String> dateMap         =null;
        Map<Integer, String> monthMap         =null;
        Map<Integer, Double> pouredLocationValueMap
                                            = null;
        Map<Integer, Double> pouredValueMap = null;
        Map<Integer, Double> soldValueMap = null;
        Map<String, Double> pouredProductValueMap = null;
        Map<String, Double> soldDateValueMap    =null;
        Map<Integer, Double> soldLocationValueMap
                                            = null;
        Map<String, Double> soldProductValueMap = null;
        Map<Integer, Double> pouredBarValueMap
                                            = null;
        Map<Integer, Double> soldBarValueMap
                                            = null;
        
        Map<Integer, String> allProductMap     = new HashMap<Integer, String>();
        
        PreparedStatement stmt              = null;
        ResultSet rs                        = null;
        

        productMap = new ProductMap(transconn);
       // barMap = new BarMap(transconn);
        //locationMap = new LocationMap(transconn);
        
        
        Map<Integer, String> locationBarMap = null;
             
              
              
             
              if(!concession) {
                  String conditionLocation  = "";
                  if(!byLocation){
                      conditionLocation     = " lD.active = 1 AND loc.id = "+String.valueOf(paramId);
                  } else {
                       conditionLocation     = " lD.active = 1 AND loc.customer = "+String.valueOf(paramId);
                  }                              
                  locationBarMap            = new HashMap<Integer, String>();
                  stmt                      = transconn.prepareStatement("select l.id,loc.id,loc.name FROM line l LEFT JOIN  bar b ON b.id=l.bar LEFT JOIN location loc on loc.id = b.location"
                          + " LEFT JOIN locationDetails lD ON lD.location = loc.id  WHERE "+conditionLocation);                                
                  rs                        = stmt.executeQuery();
                  while(rs.next()){
                      locationBarMap.put(rs.getInt(2),rs.getString(3));
                  }
              } else  {
                  
                      String conditionBar   = " ";
                      if(concession && forChart > 0){
                          conditionBar      = "  b.id ="+String.valueOf(forChart);
                      } else {
                          conditionBar      = "  b.location ="+String.valueOf(paramId);
                      }
                      
                      locationBarMap        = new HashMap<Integer, String>();                      
                     
                      stmt                  = transconn.prepareStatement("select l.id,b.id,b.name FROM line l LEFT JOIN  bar b ON b.id=l.bar WHERE "+conditionBar);                     
                      rs                    = stmt.executeQuery();
                      while(rs.next()){
                           locationBarMap.put(rs.getInt(2),rs.getString(3));
                      }
                  
              } 
              String condition              = "";
              if(byLocation) {
                      condition             = " AND lo.customer="+paramId;
                  } else if(concession) {
                      condition             = " AND s.location="+paramId;
                  } else {
                      condition             = " AND s.location="+paramId;
                  }
                  String sql                = "SELECT DISTINCT l.id,p.id,p.name FROM line l LEFT JOIN system s ON s.id = l.system LEFT JOIN product p ON p.id = l.product"
                                            + "  LEFT JOIN bar b ON b.id = l.bar LEFT JOIN location lo ON lo.id = s.location WHERE status='RUNNING' "+condition;
                  
                  stmt                      = transconn.prepareStatement(sql);
                  rs                        = stmt.executeQuery();
                  while (rs.next()) {                      
                       allProductMap.put(rs.getInt(2), rs.getString(3) );
                      //logger.debug("location : " + rs.getInt(1) + " Product = " +  rs.getString(2) );
                  }
        
        Date previous = null;
        ArrayList al = new ArrayList();
        int i = 0;
        int j = 0;

        while (pouredRs.next()) {

            if (previous == null) {
                previous = new Date(pouredRs.getTimestamp(5).getTime());
            }

            if (previous.compareTo(new Date(pouredRs.getTimestamp(5).getTime())) == 0) {
                al.add(new SummaryStructure(pouredRs.getInt(1), pouredRs.getInt(2), pouredRs.getInt(3), pouredRs.getDouble(4)));
                i++;
            } else {
                summarySet.put(previous, al);
                i = 0;
                j++;
                dateArray.put(j, previous);
                al = new ArrayList();
                al.add(new SummaryStructure(pouredRs.getInt(1), pouredRs.getInt(2), pouredRs.getInt(3), pouredRs.getDouble(4)));
                i++;
                previous = new Date(pouredRs.getTimestamp(5).getTime());
            }

        }
        close(pouredRs);
        if (i > 0) {
            summarySet.put(previous, al);
            j++;
            dateArray.put(j, previous);
            i = 0;
        }
        
        pouredDateValueMap                  = new HashMap<String, Double>();
        dateMap                             = new HashMap<Integer, String>();
        monthMap                             = new HashMap<Integer, String>();
        pouredLocationValueMap              = new HashMap<Integer, Double>();
        pouredValueMap                      = new HashMap<Integer, Double>();
        pouredProductValueMap               = new HashMap<String, Double>();
        pouredBarValueMap                   = new HashMap<Integer, Double>();
        int dateIndex                       = 0;
        double pouredSum                    = 0;         

        for (i = 1; i <= j; i++) {
            ArrayList<SummaryStructure> arrayss = summarySet.get(dateArray.get(i));
            double dateValue            =0;
            for (SummaryStructure newss : arrayss) {                
                dateValue                   += newss.getValue();
                int locationId              = newss.getLocation();  
                int barId                   = newss.getBar();
                
                 if(pouredBarValueMap.containsKey(barId)) {
                    double previousValue    = pouredBarValueMap.get(barId);
                    pouredBarValueMap.remove(barId);
                    pouredBarValueMap.put(barId,newss.getValue()+previousValue);
                } else {
                    pouredBarValueMap.put(barId,newss.getValue());
                }
                 
                if(pouredLocationValueMap.containsKey(locationId)) {
                    double previousValue    = pouredLocationValueMap.get(locationId);
                    pouredLocationValueMap.remove(locationId);
                    pouredLocationValueMap.put(locationId,newss.getValue()+previousValue);
                } else {
                    pouredLocationValueMap.put(locationId,newss.getValue());
                }
                int productId               = newss.getProduct();
                String product              =productMap.getProduct(newss.getProduct());
                if(!allProductMap.containsValue(product)){
                    allProductMap.put(productId,product);
                }
                    
                pouredSum                   +=newss.getValue();
                if(pouredProductValueMap.containsKey(product)) {                    
                    double previousValue    = pouredProductValueMap.get(product);
                    pouredProductValueMap.remove(product);
                    pouredProductValueMap.put(product,newss.getValue()+previousValue);
                } else {
                    pouredProductValueMap.put(product,newss.getValue());
                }
                
                 if(pouredValueMap.containsKey(productId)) {                    
                    double previousValue    = pouredValueMap.get(productId);
                    pouredValueMap.remove(productId);
                    pouredValueMap.put(productId,newss.getValue()+previousValue);
                } else {
                    pouredValueMap.put(productId,newss.getValue());
                }

            }
            pouredDateValueMap.put(String.valueOf(dateFormat.format(dateArray.get(i))), dateValue);
            if(!dateMap.containsValue(String.valueOf(dateFormat.format(dateArray.get(i))))) {
                dateMap.put(dateIndex,String.valueOf(dateFormat.format(dateArray.get(i))));
                dateIndex++;
            }
            

        }
        
        generatePerformanceData(paramType, paramId, allProductMap, pouredProductValueMap, pouredSum,toHandle, varianceData);
        
        if(report==1 || report == 3)  {
            previous = null;
        
            al = new ArrayList();
            i = 0;
            j = 0;

            while (soldRs.next()) {

                if (previous == null) {
                    previous = new Date(soldRs.getTimestamp(5).getTime());
                }

                if (previous.compareTo(new Date(soldRs.getTimestamp(5).getTime())) == 0) {
                    al.add(new SummaryStructure(soldRs.getInt(1), soldRs.getInt(2), soldRs.getInt(3), soldRs.getDouble(4)));
                    i++;
                } else {
                    summarySet.put(previous, al);
                    i = 0;
                    j++;
                    dateArray.put(j, previous);
                    al = new ArrayList();
                    al.add(new SummaryStructure(soldRs.getInt(1), soldRs.getInt(2), soldRs.getInt(3), soldRs.getDouble(4)));
                    i++;
                    previous = new Date(soldRs.getTimestamp(5).getTime());
                }

            }
            close(soldRs);
            if (i > 0) {
                summarySet.put(previous, al);
                j++;
                dateArray.put(j, previous);
                i = 0;
            }

            soldDateValueMap                        = new HashMap<String, Double>();
            soldLocationValueMap                    = new HashMap<Integer, Double>();
            soldBarValueMap                         = new HashMap<Integer, Double>();
            soldValueMap                            = new HashMap<Integer, Double>();
            soldProductValueMap                     = new HashMap<String, Double>();

            for (i = 1; i <= j; i++) {
                ArrayList<SummaryStructure> arrayss = summarySet.get(dateArray.get(i));
                double dateValue            =0;
                for (SummaryStructure newss : arrayss) {                
                    dateValue                   += newss.getValue();
                    int locationId              =newss.getLocation();
                    int barId                   = newss.getBar();
                    
                    if(soldBarValueMap.containsKey(barId)) {
                        double previousValue    = soldBarValueMap.get(barId);
                        soldBarValueMap.remove(barId);
                        soldBarValueMap.put(barId,newss.getValue()+previousValue);
                    } else {
                        soldBarValueMap.put(barId,newss.getValue());
                    }
                    
                    if(soldLocationValueMap.containsKey(locationId)) {
                        double previousValue    = soldLocationValueMap.get(locationId);
                        soldLocationValueMap.remove(locationId);
                        soldLocationValueMap.put(locationId,newss.getValue()+previousValue);
                    } else {
                        soldLocationValueMap.put(locationId,newss.getValue());
                    }
                    int productId               = newss.getProduct();
                    String product              =productMap.getProduct(newss.getProduct());
                    if(!allProductMap.containsValue(product)){
                        allProductMap.put(productId,product);
                    }

                    if(soldProductValueMap.containsKey(product)) {
                        double previousValue    = soldProductValueMap.get(product);
                        soldProductValueMap.remove(product);
                        soldProductValueMap.put(product,newss.getValue()+previousValue);
                    } else {
                        soldProductValueMap.put(product,newss.getValue());
                    }
                    
                    if(soldValueMap.containsKey(productId)) {
                        double previousValue    = soldValueMap.get(productId);
                        soldValueMap.remove(productId);
                        soldValueMap.put(productId,newss.getValue()+previousValue);
                    } else {
                        soldValueMap.put(productId,newss.getValue());
                    }

                }
                soldDateValueMap.put(String.valueOf(dateFormat.format(dateArray.get(i))), dateValue);
                if(!dateMap.containsValue(String.valueOf(dateFormat.format(dateArray.get(i))))) {
                dateMap.put(dateIndex,String.valueOf(dateFormat.format(dateArray.get(i))));
                dateIndex++;
            }

            }
            Map<String, Double> pouredMonthValueMap    = new HashMap<String, Double>();
            Map<String, Double> soldMonthValueMap      = new HashMap<String, Double>();
            for (Integer index : dateMap.keySet()) {
                String key                  = dateMap.get(index);                
                double poured               = 0;
                if(pouredDateValueMap.containsKey(key)) {
                    poured           = pouredDateValueMap.get(key);
                }
                double sold                     = 0;
                if(soldDateValueMap.containsKey(key)) {
                    sold                       = soldDateValueMap.get(key);
                }
                if(!monthly || concession){
                    if(report == 1){
                    Element chart = varianceData.addElement("chart");

                    chart.addElement("value").addText(key);
                    chart.addElement("poured").addText(String.valueOf(poured));
                    chart.addElement("sold").addText(String.valueOf(sold));
                    chart.addElement("variance").addText(String.valueOf(getVariance(poured, sold)));             
                }  else if( report == 3){
                     try{
                         if(poured > 0 || sold >0){
                    Element chart = varianceData.addElement("varianceChart");
                    if(concession){
                       
                        stmt                      = transconn.prepareStatement("SELECT concat( eventDesc,'-',date) from eventHours  WHERE date=DATE(?);");                                
                        //logger.debug("Date"+dbDateFormat.format(dateFormat.parse(key)));
                        stmt.setString(1, dbDateFormat.format(dateFormat.parse(key)));
                        rs                        = stmt.executeQuery();
                        if(rs.next()){
                            chart.addElement("value").addText(HandlerUtils.nullToEmpty(rs.getString(1)));
                        } else {
                            chart.addElement("value").addText(dbDateFormat.format(dateFormat.parse(key)));
                        }
                        
                  
                    } else {
                        chart.addElement("value").addText(dbDateFormat.format(dateFormat.parse(key)));
                    }
                    chart.addElement("poured").addText(String.valueOf(poured));
                    chart.addElement("sold").addText(String.valueOf(sold));
                    chart.addElement("loss").addText(String.valueOf(sold-poured));
                    chart.addElement("variance").addText(String.valueOf(getVariance(poured, sold)*-1));   
                         }
                    }catch(Exception e){
                            logger.debug(e.getMessage());
                        }
                }
                     
                } else {
                    String dateString       = key.substring(2,6);
                    key                     = key.replace(dateString, "/01/");
                    if(!monthMap.containsValue(key)){
                        monthMap.put(index,key);
                        
                    }
                    if(pouredMonthValueMap.containsKey(key)) {
                        double previousValue    = pouredMonthValueMap.get(key);
                        pouredMonthValueMap.remove(key);
                        pouredMonthValueMap.put(key,poured+previousValue);
                    } else {
                        pouredMonthValueMap.put(key,poured);
                    }
                    
                    if(soldMonthValueMap.containsKey(key)) {
                        double previousValue    = soldMonthValueMap.get(key);
                        soldMonthValueMap.remove(key);
                        soldMonthValueMap.put(key,sold+previousValue);
                    } else {
                        soldMonthValueMap.put(key,sold);
                    }
                }

            }
            try {
            if(monthly && !concession){
                for (String key : pouredMonthValueMap.keySet()) {
                double poured                   = pouredMonthValueMap.get(key);
                double sold                     = 0;
                if(soldMonthValueMap.containsKey(key)) {
                    sold                       = soldMonthValueMap.get(key);
                }
                if(report == 1){
                    Element chart = varianceData.addElement("chart");

                    chart.addElement("value").addText(dbDateFormat.format(dateFormat.parse(key)));
                    chart.addElement("poured").addText(String.valueOf(poured));
                    chart.addElement("sold").addText(String.valueOf(sold));
                    chart.addElement("variance").addText(String.valueOf(getVariance(poured, sold)));             
                }  else if( report == 3){
                    Element chart = varianceData.addElement("varianceChart");

                    chart.addElement("value").addText(dbDateFormat.format(dateFormat.parse(key)));
                    chart.addElement("poured").addText(String.valueOf(poured));
                    chart.addElement("sold").addText(String.valueOf(sold));
                    chart.addElement("variance").addText(String.valueOf(getVariance(poured, sold)*-1));             
                }            
                }
                
            }
            boolean byProduct               = false;
            if((paramType == 2 && forChart ==0) ||(paramType == 1 && forChart >0)){
                byProduct                   = true;
            }
               
                if(!byLocation){
                    if(concession) {
                        generateVarianceData(paramType, paramId, locationBarMap, pouredBarValueMap, soldBarValueMap, varianceData,false,false,byProduct,0);
                       
                        
                    } else {
                        if(!byDay){
                            generateVarianceData(2, paramId, allProductMap, pouredValueMap, soldValueMap, varianceData,false,false,byProduct,0);
                        } else {
                        if(!monthly){
                            generateDateVarianceData(2, paramId, dateMap, pouredDateValueMap, soldDateValueMap, varianceData,false);
                        } else {
                            generateDateVarianceData(2, paramId, monthMap, pouredMonthValueMap, soldMonthValueMap, varianceData,true);
                        }
                        }
                    
                    }
                } else {
                    generateVarianceData(2, paramId, locationBarMap, pouredLocationValueMap, soldLocationValueMap, varianceData,true,true,byProduct,0);
                    
                }
                
            }catch(Exception e){
                logger.debug(""+e.getMessage());
            }
        }
        productMap = null;
        barMap = null;
        locationMap = null;
    }
       


    private boolean isTimeValid(ReportDateSet lineDateSet, Date readingDate) {
        Date startDate = lineDateSet.getStartDate();
        Date endDate = lineDateSet.getEndDate();
        if (readingDate.after(startDate) && readingDate.before(endDate)) {
            return true;
        } else {
            /*
            logger.debug("start time: " + startDate.toString());
            logger.debug("end time: " + endDate.toString());
            logger.debug("data time: " + readingDate.toString());
             */
            return false;
        }
    }

    private String dateToString(Date toConvert) {
        String convertedDate = newDateFormat.format(toConvert);
        return convertedDate;
    }

    private Date setStartDate(int periodShift, int customer, String specificLocation, String startDate) {        
        Date returnDate = new Date();
        PreparedStatement stmt = null;
        ResultSet rs = null;
        String sqlQuery = null;
        String selectionString = (customer > 0 ? " WHERE l.customer=? " : " WHERE l.id IN (" + specificLocation + ") ");
        try {
            switch (periodShift) {
                case 0:
                    sqlQuery = "SELECT MIN(DATE_SUB(Concat(left(?,11),IFNULL(x.preOpen,'07:00:00')), INTERVAL eO HOUR)) preOpen" +
                            " FROM (Select CASE DAYOFWEEK(?)" +
                            " WHEN 1 THEN Right(lH.preOpenSun,8)" +
                            " WHEN 2 THEN Right(lH.preOpenMon,8)" +
                            " WHEN 3 THEN Right(lH.preOpenTue,8)" +
                            " WHEN 4 THEN Right(lH.preOpenWed,8)" +
                            " WHEN 5 THEN Right(lH.preOpenThu,8)" +
                            " WHEN 6 THEN Right(lH.preOpenFri,8)" +
                            " WHEN 7 THEN Right(lH.preOpenSat,8) END preOpen," +
                            " l.easternOffset eO " +
                            " FROM locationHours lH RIGHT JOIN location l ON lH.location=l.id" + selectionString +
                            " ) AS x;";
                    stmt = transconn.prepareStatement(sqlQuery);
                    stmt.setString(1, startDate);
                    stmt.setString(2, startDate);
                    if (customer > 0) {
                        stmt.setInt(3, customer);
                    }
                    rs = stmt.executeQuery();
                    if (rs.next()) {
                        returnDate = newDateFormat.parse(rs.getString(1));
                    }
                    break;
                case 1:
                    sqlQuery = "SELECT MIN(DATE_SUB(Concat(left(?,11),IFNULL(x.preOpen,'07:00:00')), INTERVAL eO HOUR)) preOpen" +
                            " FROM (Select CASE DAYOFWEEK(?)" +
                            " WHEN 1 THEN Right(lH.preOpenSun,8)" +
                            " WHEN 2 THEN Right(lH.preOpenMon,8)" +
                            " WHEN 3 THEN Right(lH.preOpenTue,8)" +
                            " WHEN 4 THEN Right(lH.preOpenWed,8)" +
                            " WHEN 5 THEN Right(lH.preOpenThu,8)" +
                            " WHEN 6 THEN Right(lH.preOpenFri,8)" +
                            " WHEN 7 THEN Right(lH.preOpenSat,8) END preOpen," +
                            " l.easternOffset eO " +
                            " FROM locationHours lH RIGHT JOIN location l ON lH.location=l.id" + selectionString +
                            " ) AS x;";
                    stmt = transconn.prepareStatement(sqlQuery);
                    stmt.setString(1, startDate);
                    stmt.setString(2, startDate);
                    if (customer > 0) {
                        stmt.setInt(3, customer);
                    }
                    rs = stmt.executeQuery();
                    if (rs.next()) {
                        returnDate = newDateFormat.parse(rs.getString(1));
                    }
                    break;
                case 2:
                    sqlQuery = "SELECT MIN(DATE_SUB(Concat(left(?,11),IFNULL(x.open,'11:00:00')), INTERVAL eO HOUR)) Open" +
                            " FROM (Select CASE DAYOFWEEK(?)" +
                            " WHEN 1 THEN Right(lH.openSun,8)" +
                            " WHEN 2 THEN Right(lH.openMon,8)" +
                            " WHEN 3 THEN Right(lH.openTue,8)" +
                            " WHEN 4 THEN Right(lH.openWed,8)" +
                            " WHEN 5 THEN Right(lH.openThu,8)" +
                            " WHEN 6 THEN Right(lH.openFri,8)" +
                            " WHEN 7 THEN Right(lH.openSat,8) END open," +
                            " l.easternOffset eO" +
                            " FROM locationHours lH RIGHT JOIN location l ON lH.location=l.id" + selectionString +
                            " ) AS x;";
                    stmt = transconn.prepareStatement(sqlQuery);
                    stmt.setString(1, startDate);
                    stmt.setString(2, startDate);
                    if (customer > 0) {
                        stmt.setInt(3, customer);
                    }
                    rs = stmt.executeQuery();
                    if (rs.next()) {
                        returnDate = newDateFormat.parse(rs.getString(1));
                    }
                    break;
                case 3:
                    sqlQuery = "SELECT MIN(DATE_SUB(If(IFNULL(x.close,'02:00:00')>'12:0:0',concat(left(?,11),IFNULL(x.close,'02:00:00')),concat(left(adddate(?,INTERVAL 1 DAY),11),IFNULL(x.close,'02:00:00'))), INTERVAL eO HOUR)) Close" +
                            " FROM (Select CASE DAYOFWEEK(?)" +
                            " WHEN 1 THEN Right(lH.closeSun,8)" +
                            " WHEN 2 THEN Right(lH.closeMon,8)" +
                            " WHEN 3 THEN Right(lH.closeTue,8)" +
                            " WHEN 4 THEN Right(lH.closeWed,8)" +
                            " WHEN 5 THEN Right(lH.closeThu,8)" +
                            " WHEN 6 THEN Right(lH.closeFri,8)" +
                            " WHEN 7 THEN Right(lH.closeSat,8) END close," +
                            " l.easternOffset eO" +
                            " FROM locationHours lH RIGHT JOIN location l ON lH.location=l.id" + selectionString +
                            " ) AS x;";
                    stmt = transconn.prepareStatement(sqlQuery);
                    stmt.setString(1, startDate);
                    stmt.setString(2, startDate);
                    stmt.setString(3, startDate);
                    if (customer > 0) {
                        stmt.setInt(4, customer);
                    }
                    rs = stmt.executeQuery();
                    if (rs.next()) {
                        returnDate = newDateFormat.parse(rs.getString(1));
                    }
                    break;
                case 4:
                    sqlQuery = "SELECT MIN(DATE_SUB(Concat(left(?,11),IFNULL(x.preOpen,'07:00:00')), INTERVAL eO HOUR)) preOpen" +
                            " FROM (Select CASE DAYOFWEEK(?)" +
                            " WHEN 1 THEN Right(lH.preOpenSun,8)" +
                            " WHEN 2 THEN Right(lH.preOpenMon,8)" +
                            " WHEN 3 THEN Right(lH.preOpenTue,8)" +
                            " WHEN 4 THEN Right(lH.preOpenWed,8)" +
                            " WHEN 5 THEN Right(lH.preOpenThu,8)" +
                            " WHEN 6 THEN Right(lH.preOpenFri,8)" +
                            " WHEN 7 THEN Right(lH.preOpenSat,8) END preOpen," +
                            " l.easternOffset eO " +
                            " FROM locationHours lH RIGHT JOIN location l ON lH.location=l.id" + selectionString +
                            " ) AS x;";
                    stmt = transconn.prepareStatement(sqlQuery);
                    stmt.setString(1, startDate);
                    stmt.setString(2, startDate);
                    if (customer > 0) {
                        stmt.setInt(3, customer);
                    }
                    rs = stmt.executeQuery();
                    if (rs.next()) {
                        returnDate = newDateFormat.parse(rs.getString(1));
                    }
                    break;
                default:
                    break;
            }

        } catch (Exception sqle) {
            logger.dbError("Method error: " + sqle.getMessage());
        } finally {
            close(stmt);
            close(rs);
        }
        //logger.debug("New Start Date: " + returnDate.toString() + " for perdiodShift: " + String.valueOf(periodShift));
        return returnDate;
    }

    private Date setEndDate(int periodShift, int customer, String specificLocation, String endDate) {
        //String endDate = dateToString(end);
        Date returnDate = new Date();
        PreparedStatement stmt = null;
        ResultSet rs = null;
        String sqlQuery = null;
        String selectionString = (customer > 0 ? " WHERE l.customer=? " : " WHERE l.id IN (" + specificLocation + ") ");
        try {
            switch (periodShift) {
                case 0:
                    sqlQuery = "SELECT MAX(DATE_SUB(CONCAT(LEFT(ADDDATE(?,INTERVAL 1 DAY),11),IFNULL(x.preOpen,'07:00:00')), INTERVAL eO HOUR)) preOpen" +
                            " FROM (Select CASE DAYOFWEEK(ADDDATE(?,INTERVAL 1 DAY))" +
                            " WHEN 1 THEN Right(lH.preOpenSun,8)" +
                            " WHEN 2 THEN Right(lH.preOpenMon,8)" +
                            " WHEN 3 THEN Right(lH.preOpenTue,8)" +
                            " WHEN 4 THEN Right(lH.preOpenWed,8)" +
                            " WHEN 5 THEN Right(lH.preOpenThu,8)" +
                            " WHEN 6 THEN Right(lH.preOpenFri,8)" +
                            " WHEN 7 THEN Right(lH.preOpenSat,8) END preOpen," +
                            " l.easternOffset eO" +
                            " FROM locationHours lH RIGHT JOIN location l ON lH.location=l.id" + selectionString +
                            " ) AS x;";
                    stmt = transconn.prepareStatement(sqlQuery);
                    stmt.setString(1, endDate);
                    stmt.setString(2, endDate);
                    if (customer > 0) {
                        stmt.setInt(3, customer);
                    }
                    rs = stmt.executeQuery();
                    if (rs.next()) {
                        returnDate = newDateFormat.parse(rs.getString(1));
                    }
                    break;
                case 1:
                    sqlQuery = "SELECT MAX(DATE_SUB(Concat(left(?,11),IFNULL(x.open,'11:00:00')), INTERVAL eO HOUR)) Open" +
                            " FROM (Select CASE DAYOFWEEK(?)" +
                            " WHEN 1 THEN Right(lH.openSun,8)" +
                            " WHEN 2 THEN Right(lH.openMon,8)" +
                            " WHEN 3 THEN Right(lH.openTue,8)" +
                            " WHEN 4 THEN Right(lH.openWed,8)" +
                            " WHEN 5 THEN Right(lH.openThu,8)" +
                            " WHEN 6 THEN Right(lH.openFri,8)" +
                            " WHEN 7 THEN Right(lH.openSat,8) END open," +
                            " l.easternOffset eO" +
                            " FROM locationHours lH RIGHT JOIN location l ON lH.location=l.id" + selectionString +
                            " ) AS x;";
                    stmt = transconn.prepareStatement(sqlQuery);
                    stmt.setString(1, endDate);
                    stmt.setString(2, endDate);
                    if (customer > 0) {
                        stmt.setInt(3, customer);
                    }
                    rs = stmt.executeQuery();
                    if (rs.next()) {
                        returnDate = newDateFormat.parse(rs.getString(1));
                    }
                    break;
                case 2:
                    sqlQuery = "SELECT MAX(DATE_SUB(If(IFNULL(x.close,'02:00:00')>'12:0:0',concat(left(?,11),IFNULL(x.close,'02:00:00')),concat(left(ADDDATE(?,INTERVAL 1 DAY),11),IFNULL(x.close,'02:00:00'))), INTERVAL eO HOUR)) Close" +
                            " FROM (Select CASE DAYOFWEEK(?)" +
                            " WHEN 1 THEN Right(lH.closeSun,8)" +
                            " WHEN 2 THEN Right(lH.closeMon,8)" +
                            " WHEN 3 THEN Right(lH.closeTue,8)" +
                            " WHEN 4 THEN Right(lH.closeWed,8)" +
                            " WHEN 5 THEN Right(lH.closeThu,8)" +
                            " WHEN 6 THEN Right(lH.closeFri,8)" +
                            " WHEN 7 THEN Right(lH.closeSat,8) END close," +
                            " l.easternOffset eO" +
                            " FROM locationHours lH RIGHT JOIN location l ON lH.location=l.id" + selectionString +
                            " ) AS x;";
                    stmt = transconn.prepareStatement(sqlQuery);
                    stmt.setString(1, endDate);
                    stmt.setString(2, endDate);
                    stmt.setString(3, endDate);
                    if (customer > 0) {
                        stmt.setInt(4, customer);
                    }
                    rs = stmt.executeQuery();
                    if (rs.next()) {
                        returnDate = newDateFormat.parse(rs.getString(1));
                    }
                    break;
                case 3:
                    sqlQuery = "SELECT MAX(DATE_SUB(CONCAT(LEFT(ADDDATE(?,INTERVAL 1 DAY),11),IFNULL(x.preOpen,'07:00:00')), INTERVAL eO HOUR)) preOpen" +
                            " FROM (Select CASE DAYOFWEEK(ADDDATE(?,INTERVAL 1 DAY))" +
                            " WHEN 1 THEN Right(lH.preOpenSun,8)" +
                            " WHEN 2 THEN Right(lH.preOpenMon,8)" +
                            " WHEN 3 THEN Right(lH.preOpenTue,8)" +
                            " WHEN 4 THEN Right(lH.preOpenWed,8)" +
                            " WHEN 5 THEN Right(lH.preOpenThu,8)" +
                            " WHEN 6 THEN Right(lH.preOpenFri,8)" +
                            " WHEN 7 THEN Right(lH.preOpenSat,8) END preOpen," +
                            " l.easternOffset eO" +
                            " FROM locationHours lH RIGHT JOIN location l ON lH.location=l.id" + selectionString +
                            " ) AS x;";
                    stmt = transconn.prepareStatement(sqlQuery);
                    stmt.setString(1, endDate);
                    stmt.setString(2, endDate);
                    if (customer > 0) {
                        stmt.setInt(3, customer);
                    }
                    rs = stmt.executeQuery();
                    if (rs.next()) {
                        returnDate = newDateFormat.parse(rs.getString(1));
                    }
                    break;
                case 4:
                    sqlQuery = "SELECT MAX(DATE_SUB(CONCAT(LEFT(ADDDATE(?,INTERVAL 1 DAY),11),IFNULL(x.preOpen,'07:00:00')), INTERVAL eO HOUR)) preOpen" +
                            " FROM (Select CASE DAYOFWEEK(ADDDATE(?,INTERVAL 1 DAY))" +
                            " WHEN 1 THEN Right(lH.preOpenSun,8)" +
                            " WHEN 2 THEN Right(lH.preOpenMon,8)" +
                            " WHEN 3 THEN Right(lH.preOpenTue,8)" +
                            " WHEN 4 THEN Right(lH.preOpenWed,8)" +
                            " WHEN 5 THEN Right(lH.preOpenThu,8)" +
                            " WHEN 6 THEN Right(lH.preOpenFri,8)" +
                            " WHEN 7 THEN Right(lH.preOpenSat,8) END preOpen," +
                            " l.easternOffset eO" +
                            " FROM locationHours lH RIGHT JOIN location l ON lH.location=l.id" + selectionString +
                            " ) AS x;";
                    stmt = transconn.prepareStatement(sqlQuery);
                    stmt.setString(1, endDate);
                    stmt.setString(2, endDate);
                    if (customer > 0) {
                        stmt.setInt(3, customer);
                    }
                    rs = stmt.executeQuery();
                    if (rs.next()) {
                        returnDate = newDateFormat.parse(rs.getString(1));
                    }
                    break;
                default:
                    break;
            }
        } catch (Exception sqle) {
            logger.dbError("Method error: " + sqle.getMessage());
        } finally {
            close(stmt);
            close(rs);
        }
        //logger.debug("New End Date: " + returnDate.toString());
        return returnDate;
    }
    
    
    private  Map sortByComparator(Map unsortMap) {
	 
	List list = new LinkedList(unsortMap.entrySet());

	// sort list based on comparator
	Collections.sort(list, new Comparator() {
		public int compare(Object o1, Object o2) {
			return ((Comparable) ((Map.Entry) (o2)).getValue())
                                   .compareTo(((Map.Entry) (o1)).getValue());
		}
	});

	// put sorted list into map again
            //LinkedHashMap make sure order in which keys were inserted
	Map sortedMap = new LinkedHashMap();
	for (Iterator it = list.iterator(); it.hasNext();) {
		Map.Entry entry = (Map.Entry) it.next();
		sortedMap.put(entry.getKey(), entry.getValue());
	}
	return sortedMap;
}
    private Integer getIntegerKey(Map<Integer,String> map,String value){
        for(Integer s : map.keySet()){
            if(map.get(s).equals(value)) 
                return s;
        }
        return -1;
    }
    
    
    /**  Return a list of all product categories
     */
    private void mGetProductSet(Element toHandle, Element toAppend) throws HandlerException {

        String select = "SELECT p.id, p.name FROM productSet p WHERE p.productSetType = ? ORDER BY p.name ";

        int productSetType = HandlerUtils.getRequiredInteger(toHandle, "productSetType");

        PreparedStatement stmt = null;
        ResultSet rs = null;

        try {
            stmt = transconn.prepareStatement(select);
            stmt.setInt(1, productSetType);
            rs = stmt.executeQuery();
            while (rs.next()) {
                if(productSetType ==7){
                    Element pSet = toAppend.addElement("brewery");
                    pSet.addElement("id").addText(String.valueOf(rs.getInt(1)));
                    pSet.addElement("name").addText(HandlerUtils.nullToEmpty(rs.getString(2)));
                } else if(productSetType ==9){
                    Element pSet = toAppend.addElement("style");
                    pSet.addElement("id").addText(String.valueOf(rs.getInt(1)));
                    pSet.addElement("name").addText(HandlerUtils.nullToEmpty(rs.getString(2)));
                }
            }

        } catch (SQLException sqle) {
            logger.dbError("Database error in getProductSet: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(rs);
            close(stmt);
        }

    }
    
    private void generatePerformanceData(int paramType,int paramId,Map<Integer, String> productMap,Map<String, Double> pouredValueMap,double pouredSum,Element toHandle,Element toAppend){
        try {
            String total                    = HandlerUtils.nullToString(HandlerUtils.getOptionalString(toHandle, "totalPoured"),"0");
            if(total==null || total.equals("")){
                total                       = "0";
            }
            double preTrotal                       =Double.parseDouble(total);
            //logger.debug("PreTotal:"+preTrotal);
            if(preTrotal >0){
              
                pouredSum = preTrotal;
            }
            double totalShare               = 0;
            double totalPoured              = 0;
            if(!pouredValueMap.isEmpty()) {
                Map<String, Double> treeMap = sortByComparator( pouredValueMap);
                int indexCount              = 5;
                boolean other               = false;
                double otherPoured          = 0;
                if(!treeMap.isEmpty()) {
                    for (Map.Entry entry : treeMap.entrySet()) {
                        String key          = entry.getKey().toString();
                        double poured       =  Double.parseDouble(entry.getValue().toString());
                        int product         = getIntegerKey(productMap, key);
                        if (product == 4311&& product == 10661 && poured == 0) { continue; }
                        if(pouredSum > 0 && poured > 0) {
                            totalShare      +=(poured/pouredSum)*100;
                        }
                        totalPoured         +=poured;
                        if(indexCount >0){
                            Element data    = toAppend.addElement("performanceData");
                            data.addElement("paramType").addText(String.valueOf(paramType));
                            data.addElement("paramId").addText(String.valueOf(paramId));
                            data.addElement("name").addText(key);
                            data.addElement("forChart").addText(String.valueOf(product));
                            data.addElement("poured").addText(String.valueOf(poured));
                            if(pouredSum > 0 && poured > 0) {
                                data.addElement("share").addText(String.valueOf((poured/pouredSum)*100));
                            }else {
                                data.addElement("share").addText(String.valueOf(0));
                            }

                            Element chart   = toAppend.addElement("performanceChart");
                            chart.addElement("value").addText(key);
                            chart.addElement("poured").addText(String.valueOf(poured));
                            if(pouredSum > 0 && poured > 0) {
                                chart.addElement("share").addText(String.valueOf((poured/pouredSum)*100));
                            }else {
                                chart.addElement("share").addText(String.valueOf(0));
                            }
                            indexCount--;
                        } else {
                            other               = true;
                            otherPoured         +=  Double.parseDouble(entry.getValue().toString());
                            Element data        = toAppend.addElement("performanceData");
                            data.addElement("paramType").addText(String.valueOf(paramType));
                            data.addElement("paramId").addText(String.valueOf(paramId));
                            data.addElement("name").addText(key);
                            data.addElement("forChart").addText(String.valueOf(product));
                            data.addElement("poured").addText(String.valueOf(poured));
                            if(pouredSum > 0 && poured > 0) {
                                data.addElement("share").addText(String.valueOf((poured/pouredSum)*100));
                            }else {
                                data.addElement("share").addText(String.valueOf(0));
                            }
                        }
                    }
                    if(other) {
                        Element chart           = toAppend.addElement("performanceChart");
                        chart.addElement("value").addText("others");
                        chart.addElement("poured").addText(String.valueOf(otherPoured));
                        if(pouredSum > 0 && otherPoured > 0) {
                            chart.addElement("share").addText(String.valueOf((otherPoured/pouredSum)*100));
                        }else {
                            chart.addElement("share").addText(String.valueOf(0));
                        }
                    }
                    
                   if(preTrotal >0){
                   //logger.debug("other:"+(preTrotal-totalPoured));
                     Element chart           = toAppend.addElement("performanceChart");
                    chart.addElement("value").addText("Others");
                    chart.addElement("poured").addText(String.valueOf(preTrotal-totalPoured));   
                    if(pouredSum > 0 && (preTrotal-totalPoured) > 0) {
                        chart.addElement("share").addText(String.valueOf(((preTrotal-totalPoured)/pouredSum)*100));
                    }else {
                        chart.addElement("share").addText(String.valueOf(0));
                    }
                }
                } 
            } else {
                for (Integer key : productMap.keySet()) {
                    String product          = productMap.get(key);
                    double poured           =0;
                    if(pouredValueMap.containsKey(product)) {
                        poured              = Double.parseDouble(String.valueOf(pouredValueMap.get(product)));
                    }                    
                    if (key == 4311&& key== 10661 && poured == 0) { continue; }
                    Element data            = toAppend.addElement("performanceData");
                    data.addElement("paramType").addText(String.valueOf(paramType));
                    data.addElement("paramId").addText(String.valueOf(paramId));
                    data.addElement("name").addText(product);
                    data.addElement("forChart").addText(String.valueOf(key));
                    data.addElement("poured").addText(String.valueOf(poured));
                    if(pouredSum > 0 && poured > 0) {
                        data.addElement("share").addText(String.valueOf((poured/pouredSum)*100));
                    }else {
                        data.addElement("share").addText(String.valueOf(0));
                    }
                    if(pouredSum > 0 && poured > 0){
                        totalShare          +=(poured/pouredSum)*100;
                    }
                    Element chart           = toAppend.addElement("performanceChart");
                    chart.addElement("value").addText(product);
                    chart.addElement("poured").addText(String.valueOf(poured));   
                    if(pouredSum > 0 && poured > 0) {
                        chart.addElement("share").addText(String.valueOf((poured/pouredSum)*100));
                    }else {
                        chart.addElement("share").addText(String.valueOf(0));
                    }
                }
              
            }
            toAppend.addElement("totalShare").addText(String.valueOf(totalShare));
            
        }catch(Exception e) {
            logger.debug("Error:"+e.getMessage());
        }
    }
    
    
    private void generateVarianceData(int paramType, int paramId, Map<Integer, String> locationBarMap,Map<Integer, Double> pouredLocationValueMap,
                                        Map<Integer, Double> soldLocationValueMap,Element varianceData,boolean byLocation,boolean byCustomer,boolean byProduct,int eventId) {        
        double totalPoured                  = 0;
        double totalSold                    = 0;
        double totalVariancePoured          = 0;
        double totalVarianceSold            = 0;
        for (Integer key : locationBarMap.keySet()) {
            
            String locationName             = locationBarMap.get(key);

            double poured                   = 0;
            if(pouredLocationValueMap.containsKey(key)) {
                poured                      = pouredLocationValueMap.get(key);
                totalPoured                 +=poured;                
               
            }
            
            double sold                     = 0;
            if(soldLocationValueMap.containsKey(key)) {
                sold                        = soldLocationValueMap.get(key);
                totalSold                   +=sold;
                //logger.debug("Value:"+locationName+" sold:"+sold);
            }
            
            if(poured > 0 || sold >0){
                totalVariancePoured         +=poured;
                totalVarianceSold           +=sold;
            }
            boolean canSend                 = true;
            if(paramType == 3 ){
                if (poured == 0 && sold ==0) {
                    canSend                 = false;
                }
            }

            if (locationName != null) {
                //if(canSend) 
                {
                if (paramType == 2 && key ==10661 &&key == 4311 && (poured == 0 && sold ==0)) { continue; }
                Element data                = varianceData.addElement("varianceData");
                
                if(byLocation){
                    data.addElement("paramId").addText(String.valueOf(key));
                } else {
                    data.addElement("paramId").addText(String.valueOf(paramId));
                }
                data.addElement("name").addText(String.valueOf(locationName));
                data.addElement("poured").addText(String.valueOf(poured));                
                data.addElement("sold").addText(String.valueOf(sold));
                data.addElement("loss").addText(String.valueOf(sold - poured));
                data.addElement("variance").addText(String.valueOf(getVariance(poured, sold)));
                if(eventId > 0){
                    data.addElement("eventId").addText(String.valueOf(eventId));
                }
                if(paramType==4){
                    data.addElement("eventId").addText(String.valueOf(key));
                    data.addElement("eventDate").addText(String.valueOf(locationName.substring(locationName.length()-10, locationName.length())));
                    //logger.debug(locationName.substring(locationName.indexOf("-")+1, locationName.length()));
                    data.addElement("paramType").addText(String.valueOf(2));
                    data.addElement("forChart").addText(String.valueOf(0));
                } else {
                    data.addElement("paramType").addText(String.valueOf(paramType));
                    data.addElement("forChart").addText(String.valueOf(key));
                }
                }
                 
            }
            
        }
        //logger.debug("Total Sold:"+totalSold);
        if(byCustomer) {
            varianceData.addElement("totalPoured").addText(String.valueOf(totalVariancePoured));
            varianceData.addElement("totalSold").addText(String.valueOf(totalVarianceSold));
            varianceData.addElement("totalLoss").addText(String.valueOf(totalSold - totalPoured));
            varianceData.addElement("totalVariance").addText(String.valueOf(getVariance(totalVariancePoured, totalVarianceSold)));
        } else {
            varianceData.addElement("totalPoured").addText(String.valueOf(totalPoured));
            varianceData.addElement("totalSold").addText(String.valueOf(totalSold));
            varianceData.addElement("totalVariance").addText(String.valueOf(getVariance(totalPoured, totalSold)));
            varianceData.addElement("totalLoss").addText(String.valueOf(totalSold - totalPoured));
        }
    }
    
    
    private void generateDateVarianceData(int paramType, int paramId, Map<Integer, String> dateMap,Map<String, Double> pouredDateValueMap,
                                        Map<String, Double> soldDateValueMap,Element varianceData,boolean byMonth) {        
        try{
        double totalPoured                  = 0;
        double totalSold                    = 0;
        double totalVariancePoured          = 0;
        double totalVarianceSold            = 0;
        for (Integer key : dateMap.keySet()) {
            
            String date                     = dateMap.get(key);

            double poured                   = 0;
            if(pouredDateValueMap.containsKey(date)) {
                poured                      = pouredDateValueMap.get(date);
                totalPoured                 +=poured;                
               
            }
            
            double sold                     = 0;
            if(soldDateValueMap.containsKey(date)) {
                sold                        = soldDateValueMap.get(date);
                totalSold                   +=sold;
                //logger.debug("Value:"+locationName+" sold:"+sold);
            }
            
            if(poured > 0 && sold >0){
                totalVariancePoured         +=poured;
                totalVarianceSold           +=sold;
            }

            if (date != null) {
                Element data                = varianceData.addElement("varianceData");
                
                data.addElement("paramId").addText(String.valueOf(paramId));
               
                data.addElement("name").addText(String.valueOf(date.substring(0, 10)));
                data.addElement("poured").addText(String.valueOf(poured));                
                data.addElement("sold").addText(String.valueOf(sold));
                data.addElement("loss").addText(String.valueOf(sold - poured));
                data.addElement("variance").addText(String.valueOf(getVariance(poured, sold)));
               
                String startDate            =dbDateFormat.format(dateFormat.parse(date)).substring(0, 10);
                data.addElement("startDate").addText(startDate);
                if(byMonth){
                    String endDate          =getIncrementDate(startDate, 1, "MONTH");
                    data.addElement("endDate").addText(endDate);
                    //logger.debug(endDate);
                }
                data.addElement("paramType").addText(String.valueOf(2));
                data.addElement("forChart").addText(String.valueOf(0));
                 
            }
            
        }
        
        varianceData.addElement("totalPoured").addText(String.valueOf(totalPoured));
        varianceData.addElement("totalSold").addText(String.valueOf(totalSold));
        varianceData.addElement("totalLoss").addText(String.valueOf(totalSold - totalPoured));
        varianceData.addElement("totalVariance").addText(String.valueOf(getVariance(totalPoured, totalSold)));                    
        } catch (Exception e){
            logger.debug(e.getMessage());
        }
    }
    
    
     String getIncrementDate(String date,int inteval,String type)  {

        PreparedStatement stmt              = null;
        ResultSet rs                        = null;        

        String selectDate ="SELECT DATE_ADD( DATE_ADD(?, INTERVAL " +inteval+" "+type+"), INTERVAL -1 DAY);";
        try {
            stmt = transconn.prepareStatement(selectDate);     
            stmt.setString(1,date);
            rs = stmt.executeQuery();
           //logger.debug("Start:"+date);
            if (rs.next()) {
             date                           = rs.getString(1);   
             //logger.debug("End:"+date);
            }

        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.toString());
            
        } finally {
            close(stmt);
            close(rs);
        }
        return date;

    }
    
    private void getParValue(Element toHandle, Element toAppend) throws HandlerException {
        int callerId                        = getCallerId(toHandle);
        int mobileUserId                    = HandlerUtils.getOptionalInteger(toHandle, "mobileUserId");
        int locationId                      = HandlerUtils.getOptionalInteger(toHandle, "locationId");

        HashMap<Integer, String> BrassTapProductName
                                            = new HashMap<Integer, String>();
        BrassTapProductName.put(0, "Unknown Product");
         
         if(callerId>0) {
             if(mobileUserId <1){
                 mobileUserId               = getMobileUserId(callerId);
             }
             g.addUserHistory(callerId, "parValue Access", locationId, "Inventory Par valueAccess", mobileUserId);
          }
        getTestInventory(toHandle, toAppend, BrassTapProductName,"");
     }
    
    private void getNewOrder(Element toHandle, Element toAppend) throws HandlerException {
        int location = HandlerUtils.getRequiredInteger(toHandle, "location");
        int supplier = HandlerUtils.getOptionalInteger(toHandle, "supplier");

        PreparedStatement stmt = null;
        ResultSet rs = null;

        String getPurchase =
                " SELECT purchase.id, purchase.date, purchase.total, purchase.status, supplier.name, supplier.id FROM purchase " +
                " LEFT JOIN supplier ON supplier.id = purchase.supplier " +
                " WHERE purchase.location=? AND purchase.status = 'OPEN' ";

        if (supplier > 0) {
            getPurchase +=
                    " AND supplier.id = ? ";
        }

        getPurchase +=
                " ORDER BY purchase.date";
        try {
            stmt = transconn.prepareStatement(getPurchase);
            stmt.setInt(1, location);
            if (supplier > 0) {
                stmt.setInt(2, supplier);
            }

            rs = stmt.executeQuery();

            while (rs.next()) {
                Element pur = toAppend.addElement("purchase");
                pur.addElement("orderNumber").addText(String.valueOf(rs.getInt(1)));
                pur.addElement("date").addText(HandlerUtils.nullToEmpty(rs.getString(2)));
                pur.addElement("total").addText(HandlerUtils.nullToEmpty(rs.getString(3)));
                pur.addElement("status").addText(HandlerUtils.nullToEmpty(rs.getString(4)));
                pur.addElement("supplierName").addText(HandlerUtils.nullToEmpty(rs.getString(5)));
                pur.addElement("supplierId").addText(String.valueOf(rs.getInt(6)));
                Element detailEl = pur.addElement("details");
                getPurchaseDetail(rs.getInt(1),location,detailEl);
            }

        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.toString());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
            close(rs);
        }

    }
    
    
    private void getPastOrder(Element toHandle, Element toAppend) throws HandlerException {
        int location = HandlerUtils.getRequiredInteger(toHandle, "location");
        int supplier = HandlerUtils.getOptionalInteger(toHandle, "supplier");

        PreparedStatement stmt = null;
        ResultSet rs = null;

        String getPurchase =
                " SELECT purchase.id, purchase.date, purchase.total, purchase.status, supplier.name, supplier.id FROM purchase " +
                " LEFT JOIN supplier ON supplier.id = purchase.supplier " +
                " WHERE purchase.location=? ";

        if (supplier > 0) {
            getPurchase +=
                    " AND supplier.id = ? ";
        }

        getPurchase +=
                " ORDER BY purchase.date";
        try {
            stmt = transconn.prepareStatement(getPurchase);
            stmt.setInt(1, location);
            if (supplier > 0) {
                stmt.setInt(2, supplier);
            }

            rs = stmt.executeQuery();

            while (rs.next()) {
                Element pur = toAppend.addElement("purchase");
                pur.addElement("orderNumber").addText(String.valueOf(rs.getInt(1)));
                pur.addElement("date").addText(HandlerUtils.nullToEmpty(rs.getString(2)));
                pur.addElement("total").addText(HandlerUtils.nullToEmpty(rs.getString(3)));
                pur.addElement("status").addText(HandlerUtils.nullToEmpty(rs.getString(4)));
                pur.addElement("supplierName").addText(HandlerUtils.nullToEmpty(rs.getString(5)));
                pur.addElement("supplierId").addText(String.valueOf(rs.getInt(6)));
                Element detailEl = pur.addElement("details");
                getPurchaseDetail(rs.getInt(1),location,detailEl);
            }

        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.toString());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
            close(rs);
        }

    }
    
    
    private void getPurchaseDetail(int purchase, int location,Element toAppend) throws HandlerException {        

        PreparedStatement stmt = null;
        ResultSet rs = null;

        String getPurchase =
                " SELECT supplier,date,total,status FROM purchase WHERE id=? AND location=? ";
        String getDetails =
                " SELECT pr.id, pr.name, pd.quantity, pd.productPlu FROM " +
                " purchaseDetail pd LEFT JOIN product pr ON pd.product=pr.id " +
                " WHERE pd.purchase=?";
        String getDetailsMisc =
                " SELECT pr.id, pr.name, pd.quantity, pd.productPlu FROM " +
                " purchaseDetailMisc pd LEFT JOIN miscProduct pr ON pd.product=pr.id " +
                " WHERE pd.purchase=?";        

        try {
            stmt = transconn.prepareStatement(getPurchase);
            stmt.setInt(1, purchase);
            stmt.setInt(2, location);
            rs =
                    stmt.executeQuery();
            if (rs.next()) {                
                stmt = transconn.prepareStatement(getDetails);
                stmt.setInt(1, purchase);
                rs =
                        stmt.executeQuery();
                while (rs.next()) {
                    Element product = toAppend.addElement("product");
                    product.addElement("productId").addText(HandlerUtils.nullToEmpty(rs.getString(1)));
                    product.addElement("name").addText(HandlerUtils.nullToEmpty(rs.getString(2)));
                    product.addElement("quantity").addText(HandlerUtils.nullToEmpty(rs.getString(3)));
                    product.addElement("plu").addText(HandlerUtils.nullToEmpty(rs.getString(4)));
                }

                stmt = transconn.prepareStatement(getDetailsMisc);
                stmt.setInt(1, purchase);
                rs =
                        stmt.executeQuery();
                while (rs.next()) {
                    Element product = toAppend.addElement("miscProduct");
                    product.addElement("productId").addText(HandlerUtils.nullToEmpty(rs.getString(1)));
                    product.addElement("name").addText(HandlerUtils.nullToEmpty(rs.getString(2)));
                    product.addElement("quantity").addText(HandlerUtils.nullToEmpty(rs.getString(3)));
                    product.addElement("plu").addText(HandlerUtils.nullToEmpty(rs.getString(4)));
                }

            }

        } catch (SQLException sqle) {
            logger.dbError("Database error in getPurchaseDetail: " + sqle.toString());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
            close(rs);
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
                size.addElement("name").addText(HandlerUtils.nullToEmpty(rs.getString(3)+"oz"));
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
    
    
    private void getAllUnits(Element toHandle, Element toAppend) throws HandlerException {
        
        PreparedStatement stmt = null;
        ResultSet rs = null;
      
        try {
            String selectAll = "SELECT id, name, convValue, platform FROM unit WHERE platform IN (1,2) ORDER BY name";
            stmt = transconn.prepareStatement(selectAll);            
            rs = stmt.executeQuery();
            while (rs.next()) {
                Element UnitE1 = toAppend.addElement("unit");
                UnitE1.addElement("unitId").addText(String.valueOf(rs.getInt(1)));
                UnitE1.addElement("name").addText(HandlerUtils.nullToEmpty(rs.getString(2)));
                UnitE1.addElement("convValue").addText(String.valueOf(rs.getFloat(3)));
                UnitE1.addElement("platform").addText(String.valueOf(rs.getInt(4)));
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(rs);
            close(stmt);
        }

    }
    
    
    
      private void mGetPricesTab(Element toHandle, Element toAppend) throws HandlerException {
        int location = HandlerUtils.getRequiredInteger(toHandle, "locationId");          
         int mobileUserId                   = HandlerUtils.getOptionalInteger(toHandle, "mobileUserId");
         int osPlatform                     = HandlerUtils.getOptionalInteger(toHandle, "osPlatform");
         int callerId                        = getCallerId(toHandle);
         
         getBeverageSizes(location,toAppend);
         getInventoryPrices(location,toAppend);
         
         if(callerId>0) {
             if(mobileUserId <1){
                 mobileUserId               = getMobileUserId(callerId);
             }
             g.addUserVisitHistory(callerId, "getInventoryPrices", location, "Inventory Prices Access",15, mobileUserId);
          }
         
         


    }
      
      
       private void getInventoryPrices(int location, Element toAppend) throws HandlerException {
       
       
        String selectPrice                  = "Select iP.inventory,iP.value,iP.id,iP.size FROM inventoryPrices iP LEFT JOIN  beverageSize bS ON bS.id=iP.size WHERE iP.inventory =? AND bS.id <> 'null'  order by value;";
        String selectMaxColumn              = "SELECT count(iP.id)  FROM inventoryPrices iP LEFT JOIN inventory i ON i.id=iP.inventory"
                                            + " LEFT JOIN beverageSize bS ON bS.id=iP.size WHERE i.location=? AND bS.id <> 'null' GROUP BY iP.inventory ;";
         String selectSize                  = "SELECT id,name,ounces FROM beverageSize WHERE location=?";
         
         String selectLineProduct            ="SELECT GROUP_CONCAT(l.product) FROM line l  LEFT JOIN bar b ON b.id = l.bar WHERE b.location=? AND l.product NOT IN (10661,4311,9593)  AND l.status='RUNNING'";
        String selectOnDekProduct           ="SELECT GROUP_CONCAT(product) FROM comingSoonProducts WHERE location=? AND product NOT IN (10661,4311,9593) ";
        String productCondition             = "";
         
        

        PreparedStatement stmt              = null;
        ResultSet rs                        = null,rsPrice = null;
        Map<Integer, String> sizeMap       =new HashMap<Integer, String>();

        try {
            if(location > 0){
                stmt = transconn.prepareStatement(selectLineProduct);
                stmt.setInt(1, location);                
                rs = stmt.executeQuery();
                if(rs.next()){
                    String products         = HandlerUtils.nullToEmpty(rs.getString(1));
                    if(products!=null && !products.equals("")){
                        productCondition    = products;
                    }
                }
                stmt = transconn.prepareStatement(selectOnDekProduct);
                stmt.setInt(1, location);                
                rs = stmt.executeQuery();
                if(rs.next()){
                    String products         = HandlerUtils.nullToEmpty(rs.getString(1));
                   //logger.debug("OnDek"+products);
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
            
             String selectInventory              = "SELECT i.id, i.product, p.name FROM product AS p LEFT JOIN inventory AS i ON p.id = i.product"
                                            + " WHERE i.location = ? and p.pType = 1   "+productCondition+"  ORDER BY p.name;";
            stmt = transconn.prepareStatement(selectSize);
            stmt.setInt(1,location);
            rs = stmt.executeQuery();
            int size                        =0;
            while (rs.next()) {
                sizeMap.put(rs.getInt(1),rs.getString(2));
            }            
            stmt = transconn.prepareStatement("SELECT COUNT(i.id) FROM inventory i WHERE i.location=?  "+ productCondition);
            stmt.setInt(1,location);
            rs = stmt.executeQuery();
            if (rs.next()) {
                toAppend.addElement("inventoryCount").addText(String.valueOf(rs.getInt(1)));
            }
             
            stmt = transconn.prepareStatement(selectMaxColumn);
            stmt.setInt(1,location);
            rs = stmt.executeQuery();
            if (rs.next()) {
                toAppend.addElement("priceCount").addText(String.valueOf(rs.getInt(1)));
            } else {
                toAppend.addElement("priceCount").addText(String.valueOf(0));
            } 
            
            stmt = transconn.prepareStatement(selectInventory);
            stmt.setInt(1,location);
            rs = stmt.executeQuery();
            while (rs.next()) {
                Element inventory = toAppend.addElement("inventory");
                inventory.addElement("inventoryId").addText(String.valueOf(rs.getInt(1)));   
                inventory.addElement("productId").addText(String.valueOf(rs.getInt(2)));   
                inventory.addElement("productName").addText(HandlerUtils.nullToEmpty(rs.getString(3)));                   
                stmt = transconn.prepareStatement(selectPrice);
                stmt.setInt(1,rs.getInt(1));
                rsPrice                     = stmt.executeQuery();   
                boolean priceAvail          = false;
                while (rsPrice.next()) {
                    priceAvail              = true;
                    Element price = inventory.addElement("inventoryPrices");
                   // price.addElement("inventory").addText(String.valueOf(rsPrice.getInt(1)));                
                    price.addElement("value").addText(String.valueOf(rsPrice.getDouble(2)));
                    price.addElement("priceId").addText(String.valueOf(rsPrice.getInt(3)));
                    price.addElement("size").addText(String.valueOf(rsPrice.getInt(4)));                    
                }  
              
                if(!priceAvail){
                    for (Integer key : sizeMap.keySet()) {
                        String name         = sizeMap.get(key);
                        Element price = inventory.addElement("inventoryPrices");
                        // price.addElement("inventory").addText(String.valueOf(rsPrice.getInt(1)));                
                        price.addElement("value").addText(String.valueOf(0));
                        price.addElement("priceId").addText(String.valueOf(0));
                        price.addElement("size").addText(String.valueOf(key));  
                         
                      }
                      
                }
                
                
                
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error in getInventoryPrices: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(rsPrice);
            close(rs);
            close(stmt);
        }
    }
       
       
       private void mGetUnclaimedReoprt(Element toHandle, Element toAppend) throws HandlerException {
        
        int callerId                        = getCallerId(toHandle);
        int location                        = HandlerUtils.getRequiredInteger(toHandle, "locationId");
        int product                         = HandlerUtils.getOptionalInteger(toHandle, "productId");
        String startDate                    = HandlerUtils.getRequiredString(toHandle, "startDate");
        
        
        getOpenHours(toHandle, toAppend, startDate);        
        startDate                           = HandlerUtils.getOptionalString(toAppend, "startDate");
        String endDate                      =HandlerUtils.getOptionalString(toAppend, "endDate");
        
      
        String selectData                   = " SELECT type, product, productName, poured, sold, loss, date, id, color FROM unclaimedReadingData " +
                                            " WHERE location = ? AND date BETWEEN ? AND ? " + (product > 0 ? " AND product = ? " : "") + " ORDER BY productName, date; ";
        
        PreparedStatement stmt              = null;
        ResultSet rs                        = null;
        try {
            stmt                            = transconn.prepareStatement(selectData);
            stmt.setInt(1, location);
            stmt.setString(2, startDate);
            stmt.setString(3, endDate);
            if (product > 0) {
                stmt.setInt(4, product);
            }
            rs                              = stmt.executeQuery();
            while (rs.next()) {   
                Element dataEl              = toAppend.addElement("unclaimed");
                dataEl.addElement("type").addText(String.valueOf(rs.getInt(1)));                
                dataEl.addElement("productId").addText(String.valueOf(rs.getInt(2)));                
                dataEl.addElement("productName").addText(HandlerUtils.nullToEmpty(rs.getString(3)));                
                dataEl.addElement("poured").addText(HandlerUtils.nullToEmpty(rs.getString(4)));                
                dataEl.addElement("sold").addText(HandlerUtils.nullToEmpty(rs.getString(5)));
                dataEl.addElement("loss").addText(HandlerUtils.nullToEmpty(rs.getString(6)));
                dataEl.addElement("date").addText(HandlerUtils.nullToEmpty(rs.getString(7)));
                dataEl.addElement("id").addText(String.valueOf(rs.getInt(8)));
                dataEl.addElement("colorType").addText(String.valueOf(rs.getInt(9)));
                if(rs.getInt(9) == 0){
                    dataEl.addElement("color").addText("#D1FFBA");
                } else {
                    dataEl.addElement("color").addText("#FFFF99");
                }
            } 
        } catch (SQLException sqle) {
            logger.dbError("Database error in getRawData: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(rs);
            close(stmt);
        }

    }
      private void mGetControlPanel(Element toHandle, Element toAppend) throws HandlerException {
          
         int mobileUserId                   = HandlerUtils.getOptionalInteger(toHandle, "mobileUserId");
         int osPlatform                     = HandlerUtils.getOptionalInteger(toHandle, "osPlatform");
         int type                           = HandlerUtils.getOptionalInteger(toHandle, "type");
         String lastUpdate                  = HandlerUtils.getOptionalString(toHandle, "lastUpdate");
         int locationId                     = HandlerUtils.getOptionalInteger(toHandle, "locationId");
         int callerId                       = getCallerId(toHandle);
         if(type == 1) {
             if(lastUpdate==null || lastUpdate.equals("") ){
                 getNotificationCenter(mobileUserId,toAppend, 1 ,"");
             } else {
                 getNotificationCenter(mobileUserId,toAppend, 1 ,lastUpdate);
             }
         } else {
             getNotificationCenter(mobileUserId,toAppend, 0, "");
              if(callerId>0) {
             if(mobileUserId <1){
                 mobileUserId               = getMobileUserId(callerId);
             }
             g.addUserVisitHistory(callerId, "getNotification", locationId, "Get PLU Beverages",15, mobileUserId);
          }
         
         }
    }
       
       
       private void getNotificationCenter(int user, Element toAppend, int count,String lastUpdate ) throws HandlerException {

        PreparedStatement stmt              = null;
        ResultSet rs                        = null, rsDetails = null;
        boolean state                       = false;
        String selectPushLocation           = "SELECT DISTINCT  pM.id, DATE(pM.pushTime)  FROM bevPushMessageCenter pMC LEFT JOIN bevPushMessage pM ON pM.id=pMC.pushMessage "
                                            + " WHERE pMC.user = ?  AND pM.pushTime BETWEEN SUBDATE(now(), INTERVAL 3 DAY) AND NOW() ORDER BY pM.pushTime DESC;";
        String selectPushMessage            = "SELECT pMC.id,pMC.type, pMC.color,pMC.message,CONCAT(c.name, ' - ', l.name)  FROM bevPushMessageCenter pMC LEFT JOIN bevPushMessage pM ON pM.id=pMC.pushMessage "
                                            + " LEFT JOIN location l ON l.id=pMC.location LEFT JOIN customer c ON c.id =l.customer WHERE pMC.user = ?   AND pM.id =?" ;
        String getlastUpdate                = "SELECT count(id), IFNULL(MAX(lastUpdate),now())  FROM bevPushMessageCenter WHERE user = ? AND  lastUpdate  > ?";
        String getUnreadCount               = "SELECT count(id), MAX(lastUpdate)  FROM bevPushMessageCenter WHERE user = ? AND  lastUpdate  BETWEEN SUBDATE(now(), INTERVAL 3 DAY) AND now() ;";
        
        try {
            if(count >0) {
                
                if(lastUpdate.trim().equals("")){                    
                    stmt                            = transconn.prepareStatement(getUnreadCount);
                    stmt.setInt(1,user);
                    rs                              = stmt.executeQuery();
                    if(rs.next()) {   
                        toAppend.addElement("count").addText(String.valueOf(rs.getInt(1)));
                        toAppend.addElement("lastUpdate").addText(String.valueOf(HandlerUtils.nullToEmpty(rs.getString(2))));
                    }
                } else {
                    //logger.debug("2"+lastUpdate);
                    stmt                            = transconn.prepareStatement(getlastUpdate);
                    stmt.setInt(1,user);
                    stmt.setString(2,lastUpdate);
                    rs                              = stmt.executeQuery();
                    if(rs.next()) {   
                        toAppend.addElement("count").addText(String.valueOf(rs.getInt(1)));
                        toAppend.addElement("lastUpdate").addText(String.valueOf(HandlerUtils.nullToEmpty(rs.getString(2))));
                    }
                    
                }
            } else {
                int pMc                         = 0;
                stmt                            = transconn.prepareStatement(selectPushLocation);
                stmt.setInt(1,user);
                rs                              = stmt.executeQuery();
                while (rs.next()) {                       
                    int pushMessage             = rs.getInt(1);
                    String date                 = HandlerUtils.nullToEmpty(rs.getString(2));
                    pMc++;
                    Element pmsgEl              = toAppend.addElement("pushMessage");
                    pmsgEl.addElement("date").addText(date);
                    pmsgEl.addElement("location").addText(String.valueOf(0));
                    pmsgEl.addElement("ltype").addText(String.valueOf(1));
                    pmsgEl.addElement("type").addText(String.valueOf(1));
                    stmt                            = transconn.prepareStatement(selectPushMessage);
                    stmt.setInt(1,user);                    
                    stmt.setInt(2,pushMessage);
                    rsDetails                   = stmt.executeQuery();
                    int msgCount                =0;
                    while (rsDetails.next()) { 
                        msgCount++;
                        int messageId           = rsDetails.getInt(1);
                        int mType               = rsDetails.getInt(2);
                        int color               = rsDetails.getInt(3);
                        String rMessage         = HandlerUtils.nullToEmpty(rsDetails.getString(4));
                        String lMessage         = "";
                        if(mType==0) {
                            lMessage            += "Variance:";
                            rMessage            +="%";
                            
                        }
                        String rColor           = "#000000";
                        if(color >0) {
                            rColor              ="#ff0000";

                        }
                        Element msgEl           = pmsgEl.addElement("message");
                        msgEl.addElement("messageId").addText(String.valueOf(messageId));
                        msgEl.addElement("left").addText(HandlerUtils.nullToEmpty(rsDetails.getString(5)));
                        msgEl.addElement("leftColor").addText("#000000");
                        msgEl.addElement("right").addText("");
                        msgEl.addElement("rightColor").addText(rColor);                        
                        
                        msgEl           = pmsgEl.addElement("message");
                        msgEl.addElement("messageId").addText(String.valueOf(messageId));
                        msgEl.addElement("left").addText(lMessage);
                        msgEl.addElement("leftColor").addText("#000000");
                        msgEl.addElement("right").addText(rMessage);
                        msgEl.addElement("rightColor").addText(rColor);

                    }
                    
                    if(msgCount==1) {
                        Element msgEl           = pmsgEl.addElement("message");
                        msgEl.addElement("messageId").addText(String.valueOf(0));
                        msgEl.addElement("left").addText("");
                        msgEl.addElement("leftColor").addText("#000000");
                        msgEl.addElement("right").addText("");
                        msgEl.addElement("rightColor").addText("#000000");
                        
                    }
                    
                    
                    

              
                
                }
                //logger.debug("Count"+pMc);
                if(user!=1012){
                if(pMc ==0){
                        Element pmsgEl              = toAppend.addElement("pushMessage");
                        pmsgEl.addElement("date").addText("");
                        pmsgEl.addElement("location").addText(String.valueOf(0));
                        pmsgEl.addElement("ltype").addText(String.valueOf(1));
                        pmsgEl.addElement("type").addText(String.valueOf(1));
                        Element msgEl           = pmsgEl.addElement("message");
                        msgEl.addElement("messageId").addText(String.valueOf(0));
                        msgEl.addElement("left").addText("");
                        msgEl.addElement("leftColor").addText("#000000");
                        msgEl.addElement("right").addText("");
                        msgEl.addElement("rightColor").addText("#000000");
                        
                        msgEl           = pmsgEl.addElement("message");
                        msgEl.addElement("messageId").addText(String.valueOf(0));
                        msgEl.addElement("left").addText("");
                        msgEl.addElement("leftColor").addText("#000000");
                        msgEl.addElement("right").addText("");
                        msgEl.addElement("rightColor").addText("#000000");
                        msgEl           = pmsgEl.addElement("message");
                        msgEl.addElement("messageId").addText(String.valueOf(0));
                        msgEl.addElement("left").addText("");
                        msgEl.addElement("leftColor").addText("#000000");
                        msgEl.addElement("right").addText("");
                        msgEl.addElement("rightColor").addText("#000000");
                        pMc=1;
                    }
                    if(pMc ==1){
                        Element pmsgEl              = toAppend.addElement("pushMessage");
                        pmsgEl.addElement("date").addText("");
                        pmsgEl.addElement("location").addText(String.valueOf(0));
                        pmsgEl.addElement("ltype").addText(String.valueOf(1));
                        pmsgEl.addElement("type").addText(String.valueOf(1));
                        Element msgEl           = pmsgEl.addElement("message");
                        msgEl.addElement("messageId").addText(String.valueOf(0));
                        msgEl.addElement("left").addText("");
                        msgEl.addElement("leftColor").addText("#000000");
                        msgEl.addElement("right").addText("");
                        msgEl.addElement("rightColor").addText("#000000");
                        
                        msgEl           = pmsgEl.addElement("message");
                        msgEl.addElement("messageId").addText(String.valueOf(0));
                        msgEl.addElement("left").addText("");
                        msgEl.addElement("leftColor").addText("#000000");
                        msgEl.addElement("right").addText("");
                        msgEl.addElement("rightColor").addText("#000000");
                        msgEl           = pmsgEl.addElement("message");
                        msgEl.addElement("messageId").addText(String.valueOf(0));
                        msgEl.addElement("left").addText("");
                        msgEl.addElement("leftColor").addText("#000000");
                        msgEl.addElement("right").addText("");
                        msgEl.addElement("rightColor").addText("#000000");
                        
                    }
                }
                stmt                            = transconn.prepareStatement("SELECT  MAX(lastUpdate)  FROM bevPushMessageCenter WHERE user = ?;");
                stmt.setInt(1,user);
                rs                              = stmt.executeQuery();
                if(rs.next()) {                           
                    toAppend.addElement("lastUpdate").addText(String.valueOf(HandlerUtils.nullToEmpty(rs.getString(1))));
                }
            }
           /* if(user==1012){
            String msg[]                    = {"Variance Alert :","Performance :  ","Temparature Alert :","Variance Alert :","Top 3 Brands :  ","After Hours :"};
            String msg1[]                   = {"12.5% ","1.BudLight<br/>2.yungling<br/>3.Guinnes<br/>  ","cooler1: 45<br/>Cooler2:36<br/>  ","12.5% ","1.BudLight<br/>2.yungling<br/>3.Guinnes<br/>  ",""};
            int loc[]                       = {425, 976, 970,425, 976};
            String msgDate[]                = {"2015-05-05","2015-05-04","2015-05-03","2015-05-02","2015-05-01"};
            for(int i = 0; i < 5; i++) {
                Element pmsgEl              = toAppend.addElement("pushMessage");
                pmsgEl.addElement("date").addText(msgDate[i]);
                pmsgEl.addElement("location").addText(String.valueOf(loc[i]));
                pmsgEl.addElement("ltype").addText(String.valueOf(1));
                pmsgEl.addElement("type").addText(String.valueOf(1));
                for(int j = 0; j < 6; j++) {
                    Element msgEl           = pmsgEl.addElement("message");
                    msgEl.addElement("messageId").addText(String.valueOf(j));
                    msgEl.addElement("left").addText(msg[j]);
                    msgEl.addElement("leftColor").addText("#000000");
                    msgEl.addElement("right").addText(msg1[j]);
                    if(i >3){
                        msgEl.addElement("rightColor").addText("#ff0000");
                    } else {
                        msgEl.addElement("rightColor").addText("#000000");
                    }
                }
            }
            }*/
            
            
        } catch (SQLException sqle) {
            logger.dbError("Database error: "+sqle.toString());
            throw new HandlerException(sqle);
        } catch(Exception e) {
            
        } finally {
             close(rsDetails);
            close(rs);
            close(stmt);
        }
    }
       
    void getBeveragePLU(Element toHandle, Element toAppend) throws HandlerException {
         int callerId                        = getCallerId(toHandle);  
         int mobileUserId                   = HandlerUtils.getOptionalInteger(toHandle, "mobileUserId");
         int osPlatform                     = HandlerUtils.getOptionalInteger(toHandle, "osPlatform");    
        int location                        = HandlerUtils.getRequiredInteger(toHandle, "locationId");
        int productId                       = HandlerUtils.getOptionalInteger(toHandle, "productId");

        String selectBeverages = "SELECT name,plu,id,simple FROM beverage WHERE location=?";
        String selectIngredients = "SELECT product.name, product.id, ingredient.ounces " +
                "FROM ingredient LEFT JOIN product on ingredient.product = product.id " +
                "WHERE ingredient.beverage=?";
        String selectPluPrices              = "SELECT b.id,b.name,b.plu,b.ounces,b.price  FROM beverage b "
                                            + " LEFT JOIN ingredient i ON i.beverage=b.id  WHERE simple=1 AND b.location=? AND i.product=?";


        PreparedStatement stmt = null;
        ResultSet bev = null;
        
        try {            
            getBeverageSizes(location, toAppend);
            stmt = transconn.prepareStatement(selectPluPrices);
            stmt.setInt(1, location);
            stmt.setInt(2, productId);
            bev = stmt.executeQuery();
            while (bev.next()) {
                Element beverage = toAppend.addElement("beverage");
                beverage.addElement("id").addText(String.valueOf(bev.getInt(1)));
                beverage.addElement("name").addText(HandlerUtils.nullToEmpty(bev.getString(2)));
                beverage.addElement("plu").addText(HandlerUtils.nullToEmpty(bev.getString(3)));
                beverage.addElement("size").addText(HandlerUtils.nullToEmpty(bev.getString(4)));
                beverage.addElement("price").addText(HandlerUtils.nullToEmpty(bev.getString(5)));
            }
            
             if(callerId>0) {
             if(mobileUserId <1){
                 mobileUserId               = getMobileUserId(callerId);
             }
             g.addUserVisitHistory(callerId, "getBeverages", location, "Get PLU Beverages",15, mobileUserId);
          }
         
         

        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {           
            close(bev);
            close(stmt);
        }

    }
       
       
       private void mGetLineProduct(Element toHandle, Element toAppend)
            throws HandlerException {
        int location                        = HandlerUtils.getRequiredInteger(toHandle, "locationId");
        int version                         = HandlerUtils.getOptionalInteger(toHandle, "version");
        

        String selectLine                   = "SELECT l.id,concat(s.systemId,'/', (l.lineIndex+1), ' ', p.name) FROM line as l LEFT JOIN product AS p ON p.id = l.product"
                                            + " LEFT JOIN bar b ON b.id = l.bar  LEFT JOIN system s ON s.id = l.system WHERE b.location = ? AND l.status <>  'RETIRED' ORDER by s.systemId,l.lineIndex ;";
        
        if(version > 0) {
            selectLine                      = "SELECT l.id,IFNULL(concat(l.lineNo, ' ', p.name),'-'),IF( ( l.lineNo REGEXP '^(-|\\\\+){0,1}([0-9]+\\\\.[0-9]*|[0-9]*\\\\.[0-9]+|[0-9]+)$') = 0 ,2 , 1) AS num FROM line as l LEFT JOIN product AS p ON p.id = l.product"
                                            + " LEFT JOIN bar b ON b.id = l.bar  LEFT JOIN system s ON s.id = l.system WHERE b.location = ? AND l.status <>  'RETIRED' ORDER by num,l.lineNo*1 ;";
        
        }

        PreparedStatement stmt = null;
        ResultSet rs = null;
        
        try {
            stmt = transconn.prepareStatement(selectLine);
            stmt.setInt(1, location);            
            rs = stmt.executeQuery();
            while (rs.next()) {
                Element line = toAppend.addElement("line");
                line.addElement("id").addText(String.valueOf(rs.getInt(1)));
                line.addElement("name").addText(HandlerUtils.nullToEmpty(rs.getString(2)));                
            }

        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {           
            close(rs);
            close(stmt);
        }

    }
       
       
       private void testPrintableMenu(Element toHandle, Element toAppend) throws HandlerException {

         
        int template                        = HandlerUtils.getRequiredInteger(toHandle, "template");
        String email                        = "suba@beerboard.com";

        ArrayList<String> menus             = null;
        PreparedStatement stmt              = null;
        ResultSet rs                        = null, lRS= null;
        int locationId[]                    = {425,1011,758,990};
        ArrayList<String> files             = new ArrayList<String>();
        int locCount                        = 0;
        String selectLoc                    = "SELECT COUNT(l.id) lcount, s.location FROM line l LEFT JOIN system s ON s.id = l.system LEFT JOIN locationDetails lD ON lD.location = s.location"
                                            + " WHERE l.product != 4311 AND l.status = 'RUNNING' AND l.lastPoured > '2015-03-01 10:15:33' AND lD.active = 1 AND lD.pouredUp = 1"
                                            + " GROUP BY s.location HAVING lcount = ? ORDER BY lcount, s.location LIMIT 1;";
        String selectTemplate               = "SELECT mTop, mRight, mBottom, mLeft, height, width, vMargin, zoom FROM pdfMenuTemplate WHERE id = ?;";
        try {
            // Username: sravindran   API key: 3f324f38ed732151a5a4d120b29f980e
            pdfCrowd client                 = new pdfCrowd("sravindran", "3f324f38ed732151a5a4d120b29f980e");
            stmt                            = transconn.prepareStatement(selectTemplate); 
            stmt.setInt(1, template);
            rs                              = stmt.executeQuery();
            if(rs.next()){
                String mTop                 = rs.getString(1);
                String mRight               = rs.getString(2);
                String mBottom              = rs.getString(3);
                String mLeft                = rs.getString(4);
                double height               = rs.getDouble(5);
                double width                = rs.getDouble(6);
                String vMargin              = rs.getString(7);
                int zoom                    = rs.getInt(8);
                client.setPageMargins (mTop,mRight, mBottom, mLeft);
                client.setPageHeight(height);
                client.setPageWidth(width);
                client.setInitialPdfExactZoom(zoom);
                if(vMargin!=null&&!vMargin.equals("0.0in")){
                    client.setVerticalMargin(vMargin);
                }
            }
            for(int l=8;l<=100;l++){
                stmt                        = transconn.prepareStatement(selectLoc); 
                stmt.setInt(1, l);
                lRS                         = stmt.executeQuery();
                while(lRS.next()){
                    int location            = lRS.getInt(2);
                    stmt                    = transconn.prepareStatement("SELECT l.id, CONCAT(l.boardname, ' : ', l.name), l.customer, " +
                                            " IFNULL(REPLACE(lG.logo, ' ', '%20'), 'USBNLogo.png') FROM location l " +
                                            " LEFT JOIN locationGraphics lG ON lG.location = l.id LEFT JOIN locationDetails lD ON lD.location = l.id WHERE l.id=? ");
                    stmt.setInt(1, location);
                    rs                      = stmt.executeQuery();
                    if (rs.next()) {
                        String locationName = "", logo = "USBNLogo.png";
                        int customerId       = rs.getInt(3);
                        locationName         = rs.getString(2)+ "-"+String.valueOf(l);
                        logo                        = "http://bevmanager.net/Images/Location_logo/" + HandlerUtils.nullToString( rs.getString(4),logo).replaceAll(" ", "%20");                        
                        
                        FileOutputStream fileStream     = new FileOutputStream("/home/midware/pdf/test/"+locationName+".pdf");
                        boolean generate            = generatePrintableMenu(location, customerId, logo, toAppend, template, client, fileStream);
                        if(generate) {
                            files.add(locationName);
                        }
                        /*boolean enableTextFill= false;
                        String style3              = "";
                        String bblogo                  = "";
                        String footerHtml           = "";
                        switch(template){
                            case 3: 
                                menus       = getBottledTemplate3(2, location, customerId, 36, "#000000", logo, toAppend);
                                client.setPageMargins ("4.5cm","3.0cm", "3.5cm", "1.2cm");
                                client.setPageHeight(900.00);
                                client.setPageWidth(1000.00);
                                client.setInitialPdfExactZoom(75);
                            break;
                            case 4: 
                                menus       = getBottledTemplate4(2, location, customerId, 36, "#000000", logo, toAppend);
                                client.setPageMargins ("4.5cm","3.0cm", "3.5cm", "1.2cm");
                                client.setPageHeight(900.00);
                                client.setPageWidth(1000.00);
                                client.setInitialPdfExactZoom(75);
                            break;
                            case 6: 
                                menus       = getBottledTemplate6(2, location, customerId, 36, "#000000", logo, toAppend);
                                client.setPageMargins ("4.5cm","3.0cm", "3.5cm", "1.2cm");
                                client.setPageHeight(900.00);
                                client.setPageWidth(1000.00);
                                client.setInitialPdfExactZoom(75);
                            break;
                            case 7:
                                enableTextFill   = true;
                                menus       = getDescriptionTemplate(2, location, customerId, 36, "#000000", logo, toAppend);
                                client.setPageMargins("0.0cm","0.0cm", "0.0cm", "0.0cm");
                                client.setPageHeight(830.00);
                                client.setPageWidth(550.00);
                                client.setInitialPdfExactZoom(100);
                            break;
                            case 8: 
                                enableTextFill              = true;
                                menus                       = getDescriptionFeatureTemplate(2, location, customerId, 36, "#000000", logo, toAppend);
                                client.setPageMargins("0.0cm","0.0cm", "0.0cm", "0.0cm");                                
                                client.setPageWidth(550.00);
                                client.setInitialPdfExactZoom(100);
                                client.setPageHeight(880.00);
                                style3              = "style=\" color: #000000; font-family:'Avantgarde'; font-size:10pt;  word-wrap:break-word; text-align:right;\"";
                                bblogo                  = "http://beerboard.tv/USBN.BeerBoard.UI/Images/Assets/BeerBoardLogo70.jpg";
                                //client.setHeaderHtml("<div> ... </div>");
                                footerHtml           = "<html><head><style> @font-face {font-family: Avantgarde;  src: url(http://social.usbeveragenet.com:8080/fileUploader/Images/Avantgarde.ttf) format(\"truetype\"); }  "
                                            + " </style></head><body><table align=\"right\"> <tr><td  align=\"right\" "+style3+">Our draft list is mobile <img style=\"max-height:20px;\" src=\"" + bblogo + "\"></td></tr> </table></body></html>";
                                client.setFooterHtml(footerHtml);
                                client.setVerticalMargin("0.3in");
                                
                            break;
                            case 9:
                                enableTextFill              = true;
                                menus                       = getPrintableMenuTemplate9(2, location, customerId, 36, "#000000", logo, toAppend);
                                client.setPageMargins("0.0cm","0.0cm", "0.0cm", "0.0cm");
                                client.setPageWidth(550.00);
                                client.setInitialPdfExactZoom(100);
                                client.setPageHeight(880.00);
                                style3              = "style=\" color: #000000; font-family:'Avantgarde'; font-size:10pt;  word-wrap:break-word; text-align:right;\"";
                                bblogo                  = "http://beerboard.tv/USBN.BeerBoard.UI/Images/Assets/BeerBoardLogo70.jpg";
                                //client.setHeaderHtml("<div> ... </div>");
                                footerHtml           = "<html><head><style> @font-face {font-family: Avantgarde;  src: url(http://social.usbeveragenet.com:8080/fileUploader/Images/Avantgarde.ttf) format(\"truetype\"); }  "
                                            + " </style></head><body><table align=\"right\"> <tr><td  align=\"right\" "+style3+">Our draft list is mobile <img style=\"max-height:20px;\" src=\"" + bblogo + "\"></td></tr> </table></body></html>";
                                client.setFooterHtml(footerHtml);
                                client.setVerticalMargin("0.3in");

                            break;
                        }
                        String html                     = "<head></head><body>";
                        if(enableTextFill){
                            html                     = "<head><script src=\"https://ajax.googleapis.com/ajax/libs/jquery/1/jquery.min.js\"></script>"
                                            + "<script src=\"http://beerboard.tv/USBN.BeerBoard.UI/Images/Assets/js/jquery.textfill.min.js\"></script>"
                                            + " <script type=\"text/javascript\">"
                                            + "$(document).ready(function() {"
                                            + "    $('.dtextfill').textfill({ maxFontPixels: 13 }); "                                            
                                            + "});</script>  <script type=\"text/javascript\">"
                                             +"$(document).ready(function() { "
                                            + " $('.ptextfill').textfill({ maxFontPixels: 18 }); "
                                            + "});</script> "
                                            + " <style> @font-face {font-family: Avantgarde;  src: url(http://social.usbeveragenet.com:8080/fileUploader/Images/Avantgarde.ttf) format(\"truetype\"); }  "
                                            + " </style></head><body>";
                        }if(menus.size() > 0) {
                        for (int i = 0; i < menus.size(); i++){
                            if (i > 0) {
                                html                    += "<div style='page-break-before:always' align='center'></div>";
                            }
                            html                        +=  menus.get(i);
                        }
                        html                            += "</body>";


                        File file                       = new File("/home/midware/pdf/test/HTML.htm");
                        BufferedWriter bw               = new BufferedWriter(new FileWriter(file));
                        bw.write(html);
                        bw.close();
            
            
            
                    //client.useSSL(true);
                    FileOutputStream fileStream     = new FileOutputStream("/home/midware/pdf/test/"+locationName+".pdf");
                    client.convertFile("/home/midware/pdf/test/HTML.htm", fileStream);
                    fileStream.close();

                    files.add(locationName);*/
                    locCount++;
                        }
                    }
                }
            //}
            /*
            logger.debug("Token Count:" + ntokens); */
            //logger.debug("Token Count:" + ntokens); */
            StringBuilder message            = new StringBuilder();
            message.append("<tr><td>Find the attached menu</td></tr>");
            if(email!=null && !email.equals("")) {
                sendMailWithMultiAttachment("", "tech@beerboard.com", email , "", " Beer Menu", "sendMail", message, false, files);
            }  
        } catch(Exception e) {
            logger.debug(e.getMessage());
        } finally {
             close(lRS);
            close(rs);
            close(stmt);
        }
         
    }


       private void sendEmailPrintDoc(Element toHandle, Element toAppend) throws HandlerException {

        int callerId                        = getCallerId(toHandle);
        int location                        = HandlerUtils.getRequiredInteger(toHandle, "locationId");        
        String email                        = HandlerUtils.getOptionalString(toHandle, "email");
        int mobileUserId                   = HandlerUtils.getOptionalInteger(toHandle, "mobileUserId");
        ArrayList<String> menus             = null;
        PreparedStatement stmt              = null;
        ResultSet rs                        = null;        
        
        try {
           
            String locationName             = "", logo = "USBNLogo.png";
            String selectTemplate           = "SELECT l.pdfMenu, CONCAT(lo.boardname, ' : ', lo.name), IFNULL(REPLACE(lG.logo, ' ', '%20'), 'USBNLogo.png'), " +
                                            " t.mTop, t.mRight, t.mBottom, t.mLeft, t.height, t.width, t.vMargin, t.zoom, lo.customer FROM locationBeerBoardMap l"
                                            + " LEFT JOIN location lo ON lo.id = l.location LEFT JOIN locationGraphics lG ON lG.location = lo.id " +
                                            " LEFT JOIN pdfMenuTemplate t ON t.id = l.pdfMenu WHERE l.location=?;";
            int customerId                  = 0;
            pdfCrowd client                 = new pdfCrowd("sravindran", "3f324f38ed732151a5a4d120b29f980e"); 
            printableMenu menuTemplate      = new printableMenu();
           
            int template                    = 0;
            if(location==360){
                stmt                        = transconn.prepareStatement("UPDATE locationBeerBoardMap set pdfMenu=20 WHERe location=?;");
                stmt.setInt(1, location);
                stmt.executeUpdate();
            }
            stmt                            = transconn.prepareStatement(selectTemplate);
            stmt.setInt(1, location);
            rs                              = stmt.executeQuery();
            if (rs.next()) {
                template                    = rs.getInt(1);
                locationName                = rs.getString(2);                    
                logo                        = "http://bevmanager.net/Images/Location_logo/" + HandlerUtils.nullToString( rs.getString(3),logo).replaceAll(" ", "%20");
                String mTop                 = rs.getString(4);
                String mRight               = rs.getString(5);
                String mBottom              = rs.getString(6);
                String mLeft                = rs.getString(7);
                double height               = rs.getDouble(8);
                double width                = rs.getDouble(9);
                String vMargin              = rs.getString(10);
                int zoom                    = rs.getInt(11);
                customerId                  = rs.getInt(12);
                client.setPageMargins (mTop,mRight, mBottom, mLeft);
                client.setPageHeight(height);
                client.setPageWidth(width);
                client.setInitialPdfExactZoom(zoom);
                if(vMargin!=null&&!vMargin.equals("0.0in")){
                    client.setVerticalMargin(vMargin);
                }
                String style3               = "";
                String bblogo               = "";
                String footerHtml           = "";
                FileOutputStream fileStream = new FileOutputStream("/home/midware/pdf/file.pdf");
                String fileName             = "/home/midware/pdf/file.pdf";
                
                
                boolean generate            = generatePrintableMenu(location, customerId, logo, toAppend, template, client, fileStream);               
                if(generate){
                StringBuilder message            = new StringBuilder();
                message.append("<tr><td>Find the attached menu</td></tr>");
                if(location==758){
                    fileName                 = "/home/midware/pdf/OC Print Menu - Apple Valley.pdf";
                }
                if(email!=null && !email.equals("")) {
                    sendMailWithAttachment("", "tech@beerboard.com", email , "", locationName + " Beer Menu", "sendMail", message, false, fileName);
                } else {
                    stmt                            = transconn.prepareStatement("SELECT email FROM user WHERE id = ?");
                    stmt.setInt(1, callerId);
                    rs                              = stmt.executeQuery();
                    if (rs.next()) {
                        sendMailWithAttachment("", " tech@beerboard.com", rs.getString(1) , "", locationName + " Beer Menu", "sendMail", message, false,fileName);
                        email               = rs.getString(1) ;
                    }
                }
                }
                if(mobileUserId <1){
                    logger.portalDetail(callerId, "printableMenu", location, "printableMenu", template, "Menu Sent to:"+email, transconn);
                }
                g.addUserHistory(callerId, "printableMenu", location, "Menu Sent to:"+email, mobileUserId);
               toAppend.addElement("status").addText("success") ;
            }  
        } catch(Exception e) {
            logger.debug(e.getMessage());
        } finally {
            close(rs);
            close(stmt);
        }
         
    }
       
       
       boolean generatePrintableMenu(int location,int customerId,String logo, Element toAppend,int template, pdfCrowd client,FileOutputStream fileStream){
           ArrayList<String> menus          = null;
           PreparedStatement stmt           = null;
           ResultSet rs                     = null;
           String style3                    ="",bblogo ="",footerHtml="";
           printableMenu menuTemplate       = new printableMenu();
           int maxFontName                  = 18;
           int maxFontDesc                  = 13;
           /*if(location==2221){
               template                     =21;
           }*/
           try {
               String bgColor               =  " ";
               String bgImage               = "";
               switch(template) {
                        case 3:
                            menus           = menuTemplate.getBottledTemplate3(2, location, customerId, 36, "#000000", logo, toAppend,logger,transconn);
                            /*client.setPageMargins ("4.5cm","3.0cm", "3.5cm", "1.2cm");
                            client.setPageHeight(900.00);
                            client.setPageWidth(1000.00);
                            client.setInitialPdfExactZoom(75);*/
                        break;
                        case 4:
                            menus       = menuTemplate.getBottledTemplate4(2, location, customerId, 36, "#000000", logo, toAppend,logger,transconn);
                           /* client.setPageMargins ("4.5cm","3.0cm", "3.5cm", "1.2cm");
                            client.setPageHeight(900.00);
                            client.setPageWidth(1000.00);
                            client.setInitialPdfExactZoom(75);*/
                        break;
                        case 6:
                            menus           =menuTemplate.getBottledTemplate6(2, location, customerId, 36, "#000000", logo, toAppend,logger,transconn);
                            /*client.setPageMargins ("4.5cm","3.0cm", "3.5cm", "1.2cm");
                            client.setPageHeight(900.00);
                            client.setPageWidth(1000.00);
                            client.setInitialPdfExactZoom(75);*/
                        break;
                        case 7:

                            //menus       = getDescriptionTemplate(2, location, customerId, 36, "#000000", logo, toAppend);
                             menus          = menuTemplate.getDescriptionTemplate(2, location, customerId, 36, "#000000", logo, toAppend,logger,transconn);
                            /*client.setPageMargins("0.0cm","0.0cm", "0.0cm", "0.0cm");
                            client.setPageHeight(830.00);
                            client.setPageWidth(550.00);
                            client.setInitialPdfExactZoom(100);*/
                            style3          = "style=\" color: #000000; font-family:'Avantgarde'; font-size:10pt;  word-wrap:break-word; text-align:right;\"";
                            bblogo          = "http://beerboard.tv/USBN.BeerBoard.UI/Images/Assets/BeerBoardLogo70.jpg";
                            //client.setHeaderHtml("<div> ... </div>");
                            footerHtml      = "<html><head><style> @font-face {font-family: Avantgarde;  src: url('http://social.usbeveragenet.com:8080/fileUploader/Images/Avantgarde.ttf') " +
                                            " format(\"truetype\"); } </style></head><body><table align=\"right\"> <tr><td  align=\"right\" "+style3+">Our draft list is mobile " +
                                            " <img style=\"max-height:20px;\" src=\"" + bblogo + "\"></td></tr> </table></body></html>";
                            client.setFooterHtml(footerHtml);
                        break;
                        case 8:
                            menus       = menuTemplate.getDescriptionFeatureTemplate(2, location, customerId, 36, "#000000", logo, toAppend,logger,transconn);
                           /* client.setPageMargins("0.0cm","0.0cm", "0.0cm", "0.0cm");
                            client.setPageWidth(550.00);
                            client.setInitialPdfExactZoom(100);
                            client.setPageHeight(880.00);
                            client.setVerticalMargin("0.3in");*/
                            style3          = "style=\" color: #000000; font-family:'Avantgarde'; font-size:10pt;  word-wrap:break-word; text-align:right;\"";
                            bblogo          = "http://beerboard.tv/USBN.BeerBoard.UI/Images/Assets/BeerBoardLogo70.jpg";
                            //client.setHeaderHtml("<div> ... </div>");
                            footerHtml      = "<html><head><style> @font-face {font-family: Avantgarde;  src: url('http://social.usbeveragenet.com:8080/fileUploader/Images/Avantgarde.ttf') " +
                                            " format(\"truetype\"); } </style></head><body><table align=\"right\"> <tr><td  align=\"right\" "+style3+">Our draft list is mobile " +
                                            " <img style=\"max-height:20px;\" src=\"" + bblogo + "\"></td></tr> </table></body></html>";
                            client.setFooterHtml(footerHtml);
                        break;
                        case 9:
                            menus           = menuTemplate.getPrintableMenuTemplate9(2, location, customerId, 36, "#000000", logo, toAppend,logger,transconn);
                           /* client.setPageMargins("0.0cm","0.0cm", "0.0cm", "0.0cm");
                            client.setPageWidth(550.00);
                            client.setInitialPdfExactZoom(100);
                            client.setPageHeight(880.00);
                            client.setVerticalMargin("0.3in");*/
                            style3          = "style=\" color: #000000; font-family:'Avantgarde'; font-size:10pt;  word-wrap:break-word; text-align:right;\"";
                            bblogo          = "http://beerboard.tv/USBN.BeerBoard.UI/Images/Assets/BeerBoardLogo70.jpg";
                            //client.setHeaderHtml("<div> ... </div>");
                            footerHtml      = "<html><head><style> @font-face {font-family: Avantgarde;  src: url('http://social.usbeveragenet.com:8080/fileUploader/Images/Avantgarde.ttf') " +
                                            " format(\"truetype\"); } </style></head><body><table align=\"right\"> <tr><td  align=\"right\" "+style3+">Our draft list is mobile " +
                                            " <img style=\"max-height:20px;\" src=\"" + bblogo + "\"></td></tr> </table></body></html>";
                            client.setFooterHtml(footerHtml);
                        break;
                        case 10:
                             bgColor                =  "#336699";
                            menus           = menuTemplate.getCWOCTemplate(2, location, customerId, 36, "#000000", logo, toAppend,logger,transconn);
                           /* client.setPageMargins("0.0cm","0.0cm", "0.0cm", "0.0cm");
                            client.setPageWidth(550.00);
                            client.setInitialPdfExactZoom(100);
                            client.setPageHeight(880.00);
                            client.setVerticalMargin("0.3in");*/ 
                            style3              = "style=\" color: #000000; font-family:'Avantgarde'; font-size:10pt;  word-wrap:break-word; text-align:right;\"";
                            bblogo                  = "http://beerboard.tv/USBN.BeerBoard.UI/Images/Assets/BeerBoardLogo70.jpg";
                            //client.setHeaderHtml("<div> ... </div>");
                             String headerHtml        = "<html><body><table  height=\"100%\" width=\"100%\"><tr><td align=\"center\" height=\"150px\"><img style=\" max-height:200px; max-width:300px;\" src=\""+logo+"\"/></td></tr><tr><td align=\"center\"></body></html>";
                            footerHtml           = "<html><head><style> @font-face {font-family: Avantgarde;  src: url('http://social.usbeveragenet.com:8080/fileUploader/Images/Avantgarde.ttf') format(\"truetype\"); }  "
                                        + " </style></head><body><table align=\"right\"> <tr><td  align=\"right\" "+style3+">Our draft list is mobile <img style=\"max-height:20px;\" src=\"" + bblogo + "\"></td></tr> </table></body></html>";
                          //  client.setHeaderHtml(headerHtml);
                           // client.setFooterHtml(footerHtml);
                        break;
                        case 11:
                            menus       = menuTemplate.getBWWTemplate(2, location, customerId, 36, "#000000", logo, toAppend,logger,transconn);
                           
                        break;
                        case 12:
                            style3          = "style=\" color: #000000; font-family:'Avantgarde'; font-size:10pt;  word-wrap:break-word; text-align:right;\"";
                            bblogo          = "http://beerboard.tv/USBN.BeerBoard.UI/Images/Assets/BeerBoardLogo70.jpg";
                            menus           = menuTemplate.getTemplate12(2, location, customerId, 36, "#000000", logo, toAppend,logger,transconn);                           
                            footerHtml      = "<html><head><style> @font-face {font-family: Avantgarde;  src: url('http://social.usbeveragenet.com:8080/fileUploader/Images/Avantgarde.ttf') " +
                                            " format(\"truetype\"); } </style></head><body><table align=\"right\"> <tr><td  align=\"right\" "+style3+">Our draft list is mobile " +
                                            " <img style=\"max-height:20px;\" src=\"" + bblogo + "\"></td></tr> </table></body></html>";
                            client.setFooterHtml(footerHtml);
                        break;
                        case 13:
                            menus       = menuTemplate.getSupplyHouseTemplate(2, location, customerId, 36, "#000000", logo, toAppend,logger,transconn);                           
                        break;
                        case 14:
                            menus       = menuTemplate.getBWWMenuTemplate(2, location, customerId, 36, "#000000", logo, toAppend,logger,transconn);                           
                        break;
                        case 15:
                            menus       = menuTemplate.getExtendedFeaturedTemplate(2, location, customerId, 36, "#000000", logo, toAppend,logger,transconn);
                           /* client.setPageMargins("0.0cm","0.0cm", "0.0cm", "0.0cm");
                            client.setPageWidth(550.00);
                            client.setInitialPdfExactZoom(100);
                            client.setPageHeight(880.00);
                            client.setVerticalMargin("0.3in");*/
                            style3          = "style=\" color: #000000; font-family:'Avantgarde'; font-size:10pt;  word-wrap:break-word; text-align:right;\"";
                            bblogo          = "http://beerboard.tv/USBN.BeerBoard.UI/Images/Assets/BeerBoardLogo70.jpg";
                            //client.setHeaderHtml("<div> ... </div>");
                            footerHtml      = "<html><head><style> @font-face {font-family: Avantgarde;  src: url('http://social.usbeveragenet.com:8080/fileUploader/Images/Avantgarde.ttf') " +
                                            " format(\"truetype\"); } </style></head><body><table align=\"right\"> <tr><td  align=\"right\" "+style3+">Our draft list is mobile " +
                                            " <img style=\"max-height:20px;\" src=\"" + bblogo + "\"></td></tr> </table></body></html>";
                            client.setFooterHtml(footerHtml);
                        break;   
                       case 16:
                            menus       = menuTemplate.getFatHeadTemplate(2, location, customerId, 36, "#000000", logo, toAppend,logger,transconn);                           
                        break;
                           
                        case 17:
                             bgColor                =  "#336699";
                            menus           = menuTemplate.getCWOCTemplate2(2, location, customerId, 36, "#000000", logo, toAppend,logger,transconn);
                           /* client.setPageMargins("0.0cm","0.0cm", "0.0cm", "0.0cm");
                            client.setPageWidth(550.00);
                            client.setInitialPdfExactZoom(100);
                            client.setPageHeight(880.00);
                            client.setVerticalMargin("0.3in");*/ 
                            style3              = "style=\" color: #000000; font-family:'Avantgarde'; font-size:10pt;  word-wrap:break-word; text-align:right;\"";
                            bblogo                  = "http://beerboard.tv/USBN.BeerBoard.UI/Images/Assets/BeerBoardLogo70.jpg";
                            //client.setHeaderHtml("<div> ... </div>");
                             headerHtml        = "<html><body><table  height=\"100%\" width=\"100%\"><tr><td align=\"center\" height=\"150px\"><img style=\" max-height:200px; max-width:300px;\" src=\""+logo+"\"/></td></tr><tr><td align=\"center\"></body></html>";
                            footerHtml           = "<html><head><style> @font-face {font-family: Avantgarde;  src: url('http://social.usbeveragenet.com:8080/fileUploader/Images/Avantgarde.ttf') format(\"truetype\"); }  "
                                        + " </style></head><body><table align=\"right\"> <tr><td  align=\"right\" "+style3+">Our draft list is mobile <img style=\"max-height:20px;\" src=\"" + bblogo + "\"></td></tr> </table></body></html>";
                          //  client.setHeaderHtml(headerHtml);
                           // client.setFooterHtml(footerHtml);
                        break;
                        case 18:
                            menus           = menuTemplate.getBWW60MenuTemplate(2, location, customerId, 36, "#000000", logo, toAppend,logger,transconn);    
                            maxFontName     =35;
                            bgColor               =  " bgcolor='#ffd200'";
                            client.setPageBackgroundColor("ffd200");                            
                              client.setPageHeight("15.25in");
                              client.setPageWidth("7.0in");
                              //client.setPageBackgroundColor("http://beerboard.tv/USBN.BeerBoard.UI/Images/Assets/pdf/bwwbgyellow.png");
                              //bgImage       ="background=\"http://beerboard.tv/USBN.BeerBoard.UI/Images/Assets/pdf/bwwbgyellow.jpg\"  background:no-repeat ";
                              //bgImage       =" bgcolor='#ffd200'"; 
                              client.setWatermark("http://beerboard.tv/USBN.BeerBoard.UI/Images/Assets/pdf/bwwbgyellow.png", 0, 0);
                              client.setWatermarkInBackground(true);
                        break;     
                        case 19:
                            menus           = menuTemplate.getHootersCTemplate(2, location, customerId, 36, "#000000", logo, toAppend,logger,transconn);    
                            maxFontName     =25;
                            bgColor         =  " bgcolor='#ffd200'";
                            //client.setPageBackgroundColor("ffd200");                            
                              client.setPageHeight("7.0in");
                              client.setPageWidth("5.0in");
                              bgImage       ="  background=\"http://beerboard.tv/USBN.BeerBoard.UI/Images/Assets/pdf/Hootersbg.png\"  background:no-repeat  ";
                        break;
                        case 20:
                            menus           = menuTemplate.getBWW4ColTemplate(2, location, customerId, 36, "#000000", logo, toAppend,logger,transconn);    
                            maxFontName     =35;
                            bgColor               =  " bgcolor='#ffd200'";
                            client.setPageBackgroundColor("ffd200");                            
                              client.setPageHeight("15.0in");
                              client.setPageWidth("7.0in");
                              //client.setPageBackgroundColor("http://beerboard.tv/USBN.BeerBoard.UI/Images/Assets/pdf/bwwbgyellow.png");
                              bgImage       =" bgcolor='#ffd200'";
                        break;  
                        case 21:
                            logo        = "http://beerboard.tv/USBN.BeerBoard.UI/Images/Assets/pdf/tratalg.png";
                            menus       = menuTemplate.getMultiSizeTemplate(2, location, customerId, 36, "#000000", logo, toAppend,logger,transconn);
                            client.setPageMargins("0.0cm","0.0cm", "0.0cm", "0.0cm");
                            client.setPageWidth("8.5in");
                            client.setInitialPdfExactZoom(100);
                            client.setPageHeight("14.0in");
                            //client.setVerticalMargin("0.3in");
                            style3          = "style=\" color: #000000; font-family:'Avantgarde'; font-size:10pt;  word-wrap:break-word; text-align:right;\"";
                            bblogo          = "http://beerboard.tv/USBN.BeerBoard.UI/Images/Assets/BeerBoardLogo70.jpg";
                            //client.setHeaderHtml("<div> ... </div>");
                            footerHtml      = "<html><head><style> @font-face {font-family: Avantgarde;  src: url('http://social.usbeveragenet.com:8080/fileUploader/Images/Avantgarde.ttf') " +
                                            " format(\"truetype\"); } </style></head><body><table align=\"right\"> <tr><td  align=\"right\" "+style3+">Our draft list is mobile " +
                                            " <img style=\"max-height:20px;\" src=\"" + bblogo + "\"></td></tr> </table></body></html>";
                            //client.setFooterHtml(footerHtml);
                            maxFontDesc     =13;
                        break;        
                    }
               
               String cssBG                     ="body { background-image: url(\"http://beerboard.tv/USBN.BeerBoard.UI/Images/Assets/pdf/bwwbgyellow.jpg\");}";
               if(template!=18){
                   cssBG                        = "";
               }
            
                String fileName                 = "/home/midware/pdf/file.pdf";

                String html                     = "<head><script src=\"https://ajax.googleapis.com/ajax/libs/jquery/1/jquery.min.js\"></script>"
                                                + "<script src=\"http://social.usbeveragenet.com:8080/fileUploader/Images/jquery.textfill.min.js\"></script>"
                                                + " <script type=\"text/javascript\">"
                                                + "$(document).ready(function() {"
                                                + "    $('.dtextfill').textfill({ maxFontPixels: "+maxFontDesc+" }); "
                                                + "});</script>  <script type=\"text/javascript\">"
                                                 +"$(document).ready(function() { "
                                                + " $('.ptextfill').textfill({ maxFontPixels: "+maxFontName+" }); "
                                                + "});</script> "
                                                + " <style> @font-face {font-family: Avantgarde;  src: url('http://social.usbeveragenet.com:8080/fileUploader/Images/Avantgarde.ttf') format(\"truetype\"); }"
                                                + " @font-face {font-family: AvantgardeBold;  src: url('http://social.usbeveragenet.com:8080/fileUploader/Images/avantgarde-bold.ttf') format(\"truetype\"); }  " +cssBG
                                                + " </style>";
                       if(template==13) {
                           html                 +="<link rel=\"stylesheet\" href=\"http://beerboard.tv/USBN.BeerBoard.UI/Images/Assets/pdf/supply.css\">";
                       } else if(template ==14){
                           html                 +="<link rel=\"stylesheet\" href=\"http://beerboard.tv/USBN.BeerBoard.UI/Images/Assets/pdf/BWW/bww.css\">";
                       }
                       html                     += "</head><body "+bgImage+">";
                       
                for (int i = 0; i < menus.size(); i++){
                    if (i > 0) {
                        html                    += "<div style='page-break-before:always' align='center'></div>";
                    }
                    html                        +=  menus.get(i);
                }
                html                            += "</body>";

                // Username: sravindran   API key: 3f324f38ed732151a5a4d120b29f980e
                File file                       = new File("/home/midware/pdf/HTML.htm");
                BufferedWriter bw               = new BufferedWriter(new FileWriter(file));
                bw.write(html);
                bw.close();
                
                client.convertFile("/home/midware/pdf/HTML.htm", fileStream);
                fileStream.close();
                return true;
           }catch(Exception e) {
            return false;
        } finally {
            close(rs);
            close(stmt);
        }
       }
        



     public void sendMailWithAttachment(String title, String senderAddr, String emailAddr, String supportEmailAddr, String templateMessageTitle,
            String templateMessage, StringBuilder emailBody, boolean sendBCC, String filePath) {
        String emailTemplatePath            = HandlerUtils.getSetting("email.templatePath");
        if ((emailTemplatePath == null) || "".equals(emailTemplatePath)) {
            emailTemplatePath               = ".";
        }

        try {
            if ((emailBody != null) && (emailBody.length() > 0)) {
                logger.debug("Loading Template");
                TemplatedMessage poEmail   = new TemplatedMessage(templateMessageTitle, emailTemplatePath, templateMessage);

                poEmail.setSender(senderAddr);
                poEmail.setRecipient(emailAddr);
                if (sendBCC) {
                    poEmail.setRecipientBCC(supportEmailAddr);
                }
                poEmail.setField("TITLE", title);
                poEmail.setField("BODY", emailBody.toString());
                if(filePath!=null&&!filePath.equals("")){
                    poEmail.sendWithAttachment(filePath, templateMessageTitle + ".pdf");
                } else {
                    poEmail.send();
                }
            }
        } catch (MailException me) {
            logger.dbError("Error sending message to " + emailAddr + ": " + me.toString());
        }
    }
     
     public void sendMailWithMultiAttachment(String title, String senderAddr, String emailAddr, String supportEmailAddr, String templateMessageTitle,
            String templateMessage, StringBuilder emailBody, boolean sendBCC, ArrayList<String> files ) {
        String emailTemplatePath            = HandlerUtils.getSetting("email.templatePath");
        if ((emailTemplatePath == null) || "".equals(emailTemplatePath)) {
            emailTemplatePath               = ".";
        }

        try {
            if ((emailBody != null) && (emailBody.length() > 0)) {
                logger.debug("Loading Template");
                TemplatedMessage poEmail   = new TemplatedMessage(templateMessageTitle, emailTemplatePath, templateMessage);

                poEmail.setSender(senderAddr);
                poEmail.setRecipient(emailAddr);
                if (sendBCC) {
                    poEmail.setRecipientBCC(supportEmailAddr);
                }
                poEmail.setField("TITLE", title);
                poEmail.setField("BODY", emailBody.toString());
                poEmail.sendWithMultiAttachment(files);
               
            }
        } catch (MailException me) {
            logger.dbError("Error sending message to " + emailAddr + ": " + me.toString());
        }
    }
     
     
     
      private void checkPdfCrowdConnection(Element toHandle, Element toAppend) throws HandlerException {

        try
    	{
    	URL url;
    	HttpURLConnection transconn = (HttpURLConnection)new URL("http://pdfcrowd.com/pdf/convert/html/").openConnection();    	
    	transconn.setRequestMethod("POST");
    	transconn.setDoOutput(true);
    	transconn.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
    	transconn.setConnectTimeout(120000);
    	logger.debug("PDFCrowd Response Code"+transconn.getResponseCode());
    	}
    	catch(Exception e)
    	{
    		logger.debug("Pdef Crowd Connection Error:"+e.getMessage());
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
                
                g.updateBevMobileUser(mobileUserId);
                toHandle.addElement("platform").addText(String.valueOf(type));
                //getUnits(toHandle, toAppend);              
                g.addUserVisitHistory(callerId, "getSettings", locationId, "Access Settings", 15,mobileUserId);
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
         
    private void getNULLBeverageIngridient(Element toHandle, Element toAppend) throws HandlerException {
        String selectNullIng                 = "SELECT l.id, l.name,b.id, b.name FROM beverage b LEFT JOIN ingredient i ON i.beverage = b.id LEFT JOIN location l ON l.id = b.location"
                                            + " WHERE i.id IS NULL ORDER BY l.name, b.name;"; 
        PreparedStatement stmt              = null;
        ResultSet rs                        = null;
        try {
            boolean canSend                 = false;
            stmt                            = transconn.prepareStatement(selectNullIng);             
            rs                           =  stmt.executeQuery();
            StringBuilder productList       = new StringBuilder();
            productList.append("<tr><td><table border=\"1\"><tr><th   scope=\"col\">NULL Beverage Ingridient</th> </tr> <td valign=\"top\"><table border=1 style='border: thin solid Black;'>");
            productList.append("  <tr><td width=\"10%\"> location Id </td><td width=\"45%\">Location Name</td><td width=\"10%\"> Beverage Id </td><td width=\"45%\">Beverage Name</td> </tr>");
            while(rs.next()) {
                productList.append("<tr> <td>"+String.valueOf(rs.getInt(1))+"</td><td>"+HandlerUtils.nullToEmpty(rs.getString(2))+"</td><td>"+String.valueOf(rs.getInt(3))+"</td><td>"+HandlerUtils.nullToEmpty(rs.getString(4))+"</td>  </tr>");
                canSend                     = true;
            }
            productList.append("</table></td></tr>");
            if(canSend){                
                sendMailWithAttachment("", " tech@beerboard.com", "suba@beerboard.com" , "","NULL Beverage Ingridient", "sendMail", productList, false,"");
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error in moveBeverage: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(rs);
            close(stmt);
        }
    }
    
    private void generateVariancePerformanceMessage(int locationId) throws HandlerException {
        
        String selectLocation               = "Select l.id, MAX(l.varianceAlert) FROM location l WHERE l.id = ? ";
        String selectPoured                 = "SELECT p.name, SUM(oHS.value) FROM openHoursSummary oHS LEFT JOIN product p on oHS.product = p.id "
                                            + " WHERE p.pType=1 and oHS.date BETWEEN SUBDATE(DATE(now()), INTERVAl 51 DAY) AND DATE(now()) " +
                                            " AND oHS.location = ? GROUP BY  oHS.product ORDER BY p.name  ";
        String selectSold                   = "SELECT p.name, SUM(oHSS.value) FROM openHoursSoldSummary oHSS LEFT JOIN product p on oHSS.product = p.id " +
                                            "   WHERE p.pType=1 and oHSS.date BETWEEN SUBDATE(DATE(now()), INTERVAl 51 DAY) AND DATE(now()) " +
                                            " AND oHSS.location = ? GROUP BY oHSS.product";
         

        PreparedStatement stmt              = null;
        ResultSet rs                        = null, rsLocation = null;

        try {
            double threshold                = -5.00;
            stmt                            = transconn.prepareStatement(selectLocation);
            stmt.setInt(1, locationId);
            rsLocation                      = stmt.executeQuery();
            if (rsLocation.next()) {
                threshold                   = rsLocation.getDouble(2);
                
            }
            
            HashMap<String,Double> pouredMap
                                            = new HashMap<String,Double>();
            HashMap<String,Double> soldMap
                                            = new HashMap<String,Double>();
            stmt                            = transconn.prepareStatement(selectPoured);                
            stmt.setInt(1, locationId);
            rs                              =    stmt.executeQuery();
            while (rs.next()) {
                pouredMap.put(rs.getString(1), rs.getDouble(2));
            }

            stmt                            = transconn.prepareStatement(selectSold);                
            stmt.setInt(1, locationId);
            rs                              = stmt.executeQuery();
            while (rs.next()) {
                soldMap.put(rs.getString(1), rs.getDouble(2));
            }
            
            double sold                     = 0;
            double poured                   = 0;
            String varianceMessage          = "";
            String performanceMessage       = "";
            int prodCount                   = 0;
            for (String i : pouredMap.keySet()) {
                poured                      = pouredMap.get(i);
                if(soldMap.containsKey(i)){
                    sold                    = soldMap.get(i);
                } else {
                    sold                    = 0;
                }
                prodCount++;
                
                if(!i.equals("") && prodCount <4) {                   
                    if(!performanceMessage.equals("") ){
                        performanceMessage     +="</br>"+i +" : "+poured +"";
                    } else {
                        performanceMessage     +=i +" : "+poured +"";
                    }
                }
                double variance              = getVariance(poured, sold);
                //if(variance > threshold)
                {
                    if(!varianceMessage.equals("")){
                        varianceMessage     +="</br>"+i +" : "+variance +"%";
                    } else {
                        varianceMessage     +=i +" : "+variance +"%";
                    }

                }
                soldMap.remove(i);
            }
            for (String j : soldMap.keySet()) {
                poured                  = 0;
                if(soldMap.containsKey(j)){
                    sold                    = soldMap.get(j);
                } else {
                    sold                    = 0;
                }
                double variance              = getVariance(poured, sold);
               // if(variance > threshold)
                {
                    if(!varianceMessage.equals("")){
                        varianceMessage     +="</br>"+j +" : "+variance +"%";
                    } else {
                        varianceMessage     +=j +" : "+variance +"%";
                    }
                }
            }
            //logger.debug(performanceMessage);
            //logger.debug(varianceMessage);
           
        } catch (SQLException sqle) {
            logger.dbError("Database error in getDashBoardVariance: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(rsLocation);
            close(rs);
            close(stmt);
        }
    }
    
    
     
    
    
     private void testHtmlTemplate(Element toHandle, Element toAppend) throws HandlerException {
        printableMenu menuTemplate      = new printableMenu();
        menuTemplate.getSupplyHouseTemplate(2, 425, 205, 10, "#000000", "", toAppend, logger, transconn);
                
       

    }


     private void updateBevMobileNotification(Element toHandle, Element toAppend) throws HandlerException {

         int locationId                     = HandlerUtils.getOptionalInteger(toHandle, "locationId");
         int mobileUserId                   = HandlerUtils.getOptionalInteger(toHandle, "mobileUserId");
         int active                         = HandlerUtils.getOptionalInteger(toHandle, "active");

         PreparedStatement stmt             = null;
         ResultSet rs                       = null;
         String sql                         = "";
        try {
            if(mobileUserId >0) {
                stmt                        = transconn.prepareStatement("SELECT id FROM bevMobileLocationMap WHERE user = ? AND location = ?");
                stmt.setInt(1,mobileUserId);
                stmt.setInt(2,locationId);
                rs                          = stmt.executeQuery();
                if (rs.next()) {
                    g.updateBevMobileNotification(active, mobileUserId, locationId);
                } else {
                    g.insertBevMobileNotification(active, mobileUserId, locationId);
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

    private void addUpdateDeleteBeveragePlu(Element toHandle, Element toAppend) throws HandlerException {
        int mobileUserId                    = HandlerUtils.getOptionalInteger(toHandle, "mobileUserId");
        int callerId                        = getCallerId(toHandle);
        int location                        = HandlerUtils.getRequiredInteger(toHandle, "locationId");

        String checkPlu                     = "SELECT id,bev.name, price FROM beverage bev  WHERE bev.location=? AND bev.plu=? LIMIT 1";
        
        PreparedStatement stmt              = null;
        ResultSet rs                        = null, rsDetail = null;
        int type                            = 0;
        String  error                       = "";
        try {
            if(callerId > 0 && location > 0 && isValidAccessUser(callerId, location,false)) {
             Iterator beverage   = toHandle.elementIterator("beverage");
             while (beverage.hasNext()) {
                Element bevPlu              = (Element) beverage.next();
                int id                      = HandlerUtils.getOptionalInteger(bevPlu, "id");
                String size                 = HandlerUtils.nullToString(HandlerUtils.getRequiredString(bevPlu, "size"),"0");
                String plu                  = HandlerUtils.nullToEmpty(HandlerUtils.getRequiredString(bevPlu, "plu"));
                int productId               = HandlerUtils.getRequiredInteger(bevPlu, "productId");
                String productName          = HandlerUtils.nullToEmpty(HandlerUtils.getOptionalString(bevPlu, "productName"));
                String priceValue           = HandlerUtils.nullToString(HandlerUtils.getOptionalString(bevPlu, "price"),"0.0");
                if(priceValue==null || priceValue.trim().equals("")) {
                    priceValue              = "0.0";
                }
                if(plu!=null && !plu.equals("")){
                if(!size.contains("Select")){
                if(size==null || size.trim().equals("")||size.contains("Size")) {
                    size                    = "0.0";
                }
                
                double quantity             = 0.0;
                double price                = 0.0;

                 quantity                   = Double.parseDouble(size);
                 price                      = Double.parseDouble(priceValue);                 
                 addUpdateDeleteInventoryPrices(location, productId, Float.parseFloat(size), 0, price, 1);
                //logger.debug("id: "+id +"plu "+ plu +" " );
                stmt                        = transconn.prepareStatement(checkPlu);
                stmt.setInt(1, location);
                stmt.setString(2, plu);
                rs                          = stmt.executeQuery();
                if (rs.next())  {
                    double prePrice         = rs.getDouble(3);                    
                    if(id==rs.getInt(1)) {
                        g.updateBeverage(plu, quantity, price, id);
                        String logMessage = "Updated plu#" + plu +" "+quantity +" "+price;
                        //g.addUserHistory(callerId, "updateBeverage", location, logMessage, mobileUserId);
                    } else {
                         //addErrorDetail(toAppend, "The plu '" + plu + "' already exists");
                        type                = 1;
                        error               = "The plu '" + plu + "' already exists";
                    }
                } else {
                    if(id > 0) {
                        g.updateBeverage(plu, quantity, price, id);
                        String logMessage   = "Updated plu#" + plu +" "+quantity +" "+price;
                        g.addUserHistory(callerId, "updateBeverage", location, logMessage, mobileUserId);
                    } else {
                        String logMessage   = "Added plu#" + plu;
                        g.insertBeverage(productName, location, plu, quantity, price, productId);
                        g.addUserHistory(callerId, "addBeverage", location, logMessage, mobileUserId);
                    }
                }
                } else {
                   //addErrorDetail(toAppend, "Please Select the Glass Size");
                   type                =2;
                   error               = "Please Select the Glass Size";
                }
                } else {
                   // addErrorDetail(toAppend, "Beverage PLU cannot be Empty");
                    type                = 3;
                    error               = "Beverage PLU cannot be Empty";
                }


            }

             Iterator delBev                = toHandle.elementIterator("deleteBeverage");
             while (delBev.hasNext()) {
                Element bevPlu              = (Element) delBev.next();
                int id                      = HandlerUtils.getRequiredInteger(bevPlu, "id");
                String plu                  = HandlerUtils.nullToEmpty(HandlerUtils.getRequiredString(bevPlu, "plu"));
                
                stmt                        = transconn.prepareStatement(checkPlu);
                stmt.setInt(1, location);
                stmt.setString(2, plu);
                rs                          = stmt.executeQuery();
                if(rs.next()) {
                    double prePrice         = rs.getDouble(3);
                    if (id == rs.getInt(1)) {
                        g.deleteBeverage(rs.getInt(1));
                        //logger.portalDetail(callerId, "deleteBeverage", location, "beverage", rs.getInt(1), "Deleted PLU " + plu, transconn);
                        g.addUserHistory(callerId, "deleteBeverage", location, "Deleted PLU " + plu, mobileUserId);
                    } else {
                        //addErrorDetail(toAppend, "Unable to Delete PLU: " +plu );
                        error               = "Unable to Delete PLU: " +plu;
                        type            = 4;
                    }
                }
            }
            } else {
                addErrorDetail(toAppend, "Invalid Access"  );
            }
            if(type >0){
                addErrorDetail(toAppend, error);
            }

        } catch (SQLException sqle) {
            logger.dbError("Database error in addUpdateDeleteBeveragePlu: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(rsDetail);
            close(rs);
            close(stmt);
        }

    }
    
    
    private void addUpdateDeleteInventoryPrices(int locationId, int product,  float ounces,double preValue,double value, int type) throws HandlerException {
         
         PreparedStatement stmt             = null;
         ResultSet rs                       = null, rsInventory = null;
         String selectPrices                = " SELECT id FROM inventoryPrices WHERE inventory = ? AND size = ? AND value = ? ";         
         String selectInventory             = " SELECT id FROM inventory WHERE location = ? AND product = ?";
         String checkSize                   = " SELECT id FROM beverageSize WHERE location=? AND ounces= ?";
         
         try {
             int size                       = 0;
             stmt                           = transconn.prepareStatement(checkSize);
             stmt.setInt(1, locationId);
             stmt.setFloat(2, ounces);
             rs                             = stmt.executeQuery();
             if (rs.next()) {
                 size                       = rs.getInt(1);
             }
           
                 
             switch(type)    {
                 case 1:
                     if(locationId > 0 && product > 0 && size >0 && value > 0) {
                         stmt                   = transconn.prepareStatement(selectInventory);
                         stmt.setInt(1,locationId);
                         stmt.setInt(2,product);
                         rsInventory            = stmt.executeQuery();
                         if(rsInventory.next()){
                             stmt           = transconn.prepareStatement(selectPrices);
                             stmt.setInt(1,rsInventory.getInt(1));
                             stmt.setInt(2,size);  
                             stmt.setDouble(3, value);
                             rs = stmt.executeQuery();
                             if (!rs.next()) {
                                 g.insertPrices(rsInventory.getInt(1), size, value);
                             } 
                        }
                     }
                    break;
                 case 2:
                     if(locationId > 0 && product > 0 && size >0 && preValue >0 && value > 0) {
                         stmt               = transconn.prepareStatement(selectInventory);
                         stmt.setInt(1,locationId);
                         stmt.setInt(2,product);
                         rsInventory        = stmt.executeQuery();
                         if(rsInventory.next()){
                             int prevId     = 0;
                             stmt           = transconn.prepareStatement(selectPrices);
                             stmt.setInt(1,rsInventory.getInt(1));
                             stmt.setInt(2,size);  
                             stmt.setDouble(3, preValue);
                             rs             = stmt.executeQuery();
                             if (rs.next()) {   
                                 prevId     = rs.getInt(1);
                             }
                             
                             stmt           = transconn.prepareStatement(selectPrices);
                             stmt.setInt(1,rsInventory.getInt(1));
                             stmt.setInt(2,size);  
                             stmt.setDouble(3, value);
                             rs             = stmt.executeQuery();
                             if (!rs.next()) {   
                                 if(prevId > 0) {
                                     g.updatePrices(prevId, value);
                                 } else {
                                     g.insertPrices(rsInventory.getInt(1), size, value);
                                 }
                             } 
                         }
                     }
                    break;
                 case 3:
                     if(locationId > 0 && product > 0 && size >0 ) {
                          stmt              = transconn.prepareStatement(selectInventory);
                          stmt.setInt(1,locationId);
                          stmt.setInt(2,product);
                          rsInventory       = stmt.executeQuery();
                          if(rsInventory.next()){
                              g.deletePrices(rsInventory.getInt(1), size, value);
                          }
                      }
                break;
            }
             
            g.updateBBTV(locationId);
         } catch (SQLException sqle) {
             logger.dbError("Database error: " + sqle.getMessage());
             throw new HandlerException(sqle);
         } finally {
             close(rsInventory);
             close(rs);
             close(stmt);
         }
    }

    private void updateGlanolaLineCleaning(Element toHandle, Element toAppend) throws HandlerException {

        String barname                      = HandlerUtils.getOptionalString(toHandle, "barname");
        String cycle                        = HandlerUtils.getOptionalString(toHandle, "cycle");
        String time                         = HandlerUtils.getOptionalString(toHandle, "time");
        String lines                        = HandlerUtils.getOptionalString(toHandle, "lines");
        int locationId                      = HandlerUtils.getOptionalInteger(toHandle, "locationId");
        int abort                           = HandlerUtils.getOptionalInteger(toHandle, "abort");

        logger.debug("Bar Name:"+barname);
        logger.debug("cycle:"+cycle);
        logger.debug("time:"+time);
        logger.debug("lines:"+lines);
        logger.debug("locationId:"+locationId);
        logger.debug("abort:"+abort);

        String selectLineId                 = "SELECT l.id FROM line l LEFT JOIN bar b ON b.id = l.bar "
                                            + " WHERE l.status = 'RUNNING' AND b.location = ? AND l.lineNo = TRIM(REPLACE(?, 'Line', ''))";
        String selectLineCleaning           = "SELECT id FROM glanolaLineCleaning WHERE location = ? AND line = ? "
                                            + " AND NOW() BETWEEN startTime and endTime";
        PreparedStatement stmt              = null;
        ResultSet rs                        = null;

        try {

            if (abort == 1) {
                stmt                        = transconn.prepareStatement("SELECT id FROM glanolaLineCleaning WHERE location = ? AND NOW() BETWEEN startTime and endTime");
                stmt.setInt(1, locationId);
                rs                          = stmt.executeQuery();
                while (rs.next()) {
                    g.updateGlanola(rs.getInt(1));
                }
            } else {
                String[] linesArray         = lines.split(",");
                for (int i = 0; i < linesArray.length; i++) {
                    String line             = linesArray[i].trim();
                    logger.debug("Cleaning Line: " + line);
                    stmt                    = transconn.prepareStatement(selectLineId);
                    stmt.setInt(1, locationId);
                    stmt.setString(2, line);
                    rs                      = stmt.executeQuery();
                    if (rs.next()) {
                        int lineId          = rs.getInt(1);

                        stmt                = transconn.prepareStatement(selectLineCleaning);
                        stmt.setInt(1, locationId);
                        stmt.setInt(2, lineId);
                        rs                  = stmt.executeQuery();
                        if (rs.next()) {
                            g.updateGlanolaEnd(rs.getInt(1));
                        } else {
                            g.insertGlanola(locationId, lineId);
                        }
                    }
                }
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(rs);
            close(stmt);
        }/**/
    }

    private void addBevPushMessage(Element toHandle, Element toAppend) throws HandlerException {

        // int locationId                   = HandlerUtils.getOptionalInteger(toHandle, "locationId");
        // generateVariancePerformanceMessage(locationId);
        String selectVarianceAlert          = "SELECT DISTINCT bMU.deviceToken, bMLM.location, l.name, l.varianceAlert, tS.var"
                                            + " FROM bevMobileLocationMap bMLM LEFT JOIN bevMobileUser bMU ON bMU.id = bMLM.user"
                                            + " LEFT JOIN location l ON l.id = bMLM.location LEFT JOIN tierSummary tS ON tS.location = bMLM.location "
                                            + " WHERE bMLM.active = 1 AND LENGTH(bMU.deviceToken) > 0 AND tS.date = DATE(SUBDATE(NOW(), INTERVAL 1 DAY)) "
                                            + " ORDER BY bMU.deviceToken, bMLM.location;";
        String selectLocationVariance       = "SELECT DISTINCT bMU.id,  bMLM.location, l.name, l.varianceAlert, tS.var "
                                            + " FROM bevMobileLocationMap bMLM LEFT JOIN bevMobileUser bMU ON bMU.id = bMLM.user "
                                            + " LEFT JOIN location l ON l.id = bMLM.location LEFT JOIN tierSummary tS ON tS.location = bMLM.location "
                                            + " WHERE bMLM.active = 1 AND LENGTH(bMU.deviceToken) >60 AND tS.date = DATE(SUBDATE(NOW(), INTERVAL 1 DAY)) "
                                            + " ORDER BY  bMLM.location;";
        String selectMessage                = "SELECT id FROM bevPushMessage WHERE  pushTime BETWEEN (SELECT   IF( TIME(now())>'08:59:00' ,  (SELECT concat (DATE(ADDDATE(NOW(), INTERVAL 1 DAY)), ' 09:00:00')),(SELECT concat (DATE(NOW()), ' 08:59:00')) ) ) AND "
                                            + " (SELECT IF( TIME(now())>'8:59:00', (SELECT concat (DATE(ADDDATE(NOW(), INTERVAL 2 DAY)), ' 08:59:00')),(SELECT concat (DATE(ADDDATE(NOW(), INTERVAL 1 DAY)), ' 09:00:00'))));";
        PreparedStatement stmt              = null;
        ResultSet rs                        = null, rsDetails = null;

        try {
            String message                  = "", productName = "";
            int messageId                   = 0;
            stmt                            = transconn.prepareStatement(selectMessage);
            rs                              = stmt.executeQuery();
            if (rs.next()) {
                 messageId                  = rs.getInt(1);
            }
            
            if(messageId < 1) {
                message                     = "You have a New Notification!";
                messageId                   = g.insertBevPushMessage(message);
            }

            stmt                            = transconn.prepareStatement(selectLocationVariance);
            rs                              = stmt.executeQuery();
            while(rs.next()) {
                 int userId                 = rs.getInt(1);
                 int locationId             = rs.getInt(2);
                 double locationAlert       = rs.getDouble(4);
                 double variance            = rs.getDouble(5);
                 int color                  = 0;
                 if(variance<locationAlert){
                     color                  = 1;
                 }
                 message                    = String.valueOf(variance);
                 g.addBevPushMessageMap(messageId, locationId, userId, color, 0, message);
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error in getDashBoardVariance: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(rsDetails);
            close(rs);
            close(stmt);
        }
    }
    
    
    private void testHttpClient(Element toHandle, Element toAppend) throws HandlerException {
        try{
        QuadGraphics Q= new QuadGraphics();
         
        //Q.postMenu();
        } catch(Exception e){
            logger.debug(e.getMessage());
        }
    }
    
    
    private void sendBeerBoardIndex(Element toHandle, Element toAppend) throws HandlerException {

        //int callerId                        = getCallerId(toHandle);
        int location                        = HandlerUtils.getRequiredInteger(toHandle, "locationId");  
        int days                            = HandlerUtils.getRequiredInteger(toHandle, "days");           
        String email                        = HandlerUtils.getOptionalString(toHandle, "email");
        
        ArrayList<String> menus             = null;
        PreparedStatement stmt              = null;
        ResultSet rs                        = null;     
        
        Map<Integer, String> lineMap        = new HashMap<Integer, String>();
        Map<Integer, String> handleMap      = new HashMap<Integer, String>();
        Map<Integer, Double> handleValueMap = new HashMap<Integer, Double>();
        
        try {
            String selectDate               = "SELECT CONCAT(date_format(SUBDATE(NOW(), INTERVAL ? + 1 DAY),'%m/%d/%Y'),' - ', date_format(SUBDATE(NOW(), INTERVAL 1 DAY), '%m/%d/%Y' ))";
            String selectDayCount           = "SELECT COUNT(id) FROM tierSummary WHERE date > SUBDATE(NOW(), INTERVAL ? + 1 DAY) AND location = ? "
                                            + " AND tier < 4;";
            String selectDaysToExclude      = "SELECT IFNULL(REPLACE(CONCAT('\\'', GROUP_CONCAT(DISTINCT date), '\\''), ',', '\\',\\''), '') FROM tierSummary "
                                            + " WHERE date > SUBDATE(NOW(), INTERVAL ? + 1 DAY) AND location = ? AND tier = 4;";
            pdfCrowd client                 = new pdfCrowd("sravindran", "3f324f38ed732151a5a4d120b29f980e");             
            double tapSum                   = 0; int tapCount = 0;
            String customerName             = "", locationName = "";
            String period                   = "", daysToExculde = "";
            int dayCount                    = days;
            
            stmt                            = transconn.prepareStatement(selectDaysToExclude);
            stmt.setInt(1, days);
            stmt.setInt(2, location);
            rs                              = stmt.executeQuery();
            if (rs.next()) {
                if (rs.getString(1) != null) { daysToExculde = rs.getString(1); }
            }
            
            stmt                            = transconn.prepareStatement(selectDayCount);
            stmt.setInt(1, days);
            stmt.setInt(2, location);
            rs                              = stmt.executeQuery();
            if (rs.next()) {
                dayCount                    = rs.getInt(1);
            }
            
            stmt                            = transconn.prepareStatement(selectDate);
            stmt.setInt(1, days);
            rs                              = stmt.executeQuery();
            if (rs.next()) {
                period                      = rs.getString(1);
            }
            
            String selectSummary            = "SELECT c.name Customer, l.name Location, li.lineNo Handle, GROUP_CONCAT(DISTINCT p.name) Products, SUM(lS.value) Volume "
                                            + " FROM location l LEFT JOIN customer c ON c.id = l.customer LEFT JOIN locationDetails lD ON lD.location = l.id "
                                            + " LEFT JOIN system s ON s.location = l.id LEFT JOIN line li ON li.system = s.id "
                                            + " LEFT JOIN lineSummary lS ON lS.line = li.id LEFT JOIN product p ON p.id = li.product "
                                            + " WHERE lD.active = 1 AND l.id = ? AND lD.pouredUp = 1 AND lS.date > SUBDATE(NOW(), INTERVAL ? + 2 DAY) AND lS.date < DATE(NOW()) "
                                            + (daysToExculde.length() > 0 ? " AND lS.date NOT IN (" + daysToExculde + ")" : "")
                                            + " GROUP BY s.id, li.lineIndex ORDER BY volume DESC, CAST(li.lineNo AS UNSIGNED); " ;
            logger.debug(selectSummary);
            
            stmt                            = transconn.prepareStatement(selectSummary);
            stmt.setInt(1, location);
            stmt.setInt(2, days);
            rs                              = stmt.executeQuery();
            while (rs.next()) {
                customerName                = rs.getString(1);
                locationName                = rs.getString(2);
                String handle               = rs.getString(3);
                String productName          = rs.getString(4);
                double volume               = rs.getDouble(5);
                if (volume > 10.0) {
                    double ozPerDay         = (volume / dayCount);
                    double kegTurnOver      = (ozPerDay * 30) / 1984;

                    tapSum                  += kegTurnOver;
                    tapCount++;
                    handle                  = String.valueOf(tapCount);
                    lineMap.put(tapCount,handle);
                    handleMap.put(tapCount, productName);
                    handleValueMap.put(tapCount, kegTurnOver);
                }
            }  
            double average                  = tapSum/tapCount;
            printableMenu menuTemplate      = new printableMenu();
            menus                           = menuTemplate.getBeerBoardIndexTemplate(customerName, locationName, period, tapSum, average, tapCount, dayCount, lineMap, handleMap, handleValueMap, toAppend, logger);

                String mTop                 = "0.cm";
                String mRight               = "0.1cm";
                String mBottom              = "0.0cm";
                String mLeft                = "0.1cm";
                String height               = "11.0in";
                String width                = "8.5in";
                String vMargin              = ".5in";
                int zoom                    = 100;
                
                client.setPageMargins (mTop,mRight, mBottom, mLeft);
                client.setPageHeight(height);
                client.setPageWidth(width);
                client.setInitialPdfExactZoom(zoom);
                if(vMargin!=null&&!vMargin.equals("0.0in")){
                    client.setVerticalMargin(vMargin);
                }
                String headerHtml="<html><body><div align='left'><img style=\" max-height:40px; max-width:200px;\" src=\"http://bevmanager.net/Images/Assets/Generic/beerboardlogo.png\"/></div></body></html>";
                client.setHeaderHtml(headerHtml);
                
            String footerHtml           = "<html><head><style> .ft8{text-align: center; font: 7px 'Arial';color: #737373;line-height: 13px;}  </style></head>"
                                        + "<body><div align='center'><P class=\"ft8\">225 W. Jefferson Street, Syracuse, NY 13202 <SPAN style=\"font: 11px 'Arial';color: #737373;line-height: 14px;\">| </SPAN>tf 888 298 3641 <SPAN style=\"font: 11px 'Arial';color: #737373;line-height: 14px;\">| </SPAN>ph 315 579 2025 <SPAN style=\"font: 11px 'Arial';color: #737373;line-height: 14px;\">| </SPAN>fx 315 579 4337 <SPAN style=\"font: 11px 'Arial';color: #737373;line-height: 14px;\">| </SPAN>beerboardapp.com</P></div></body></html>";
            //client.setFooterHtml(footerHtml);
            
             String fileName                 = "/home/midware/pdf/indexfile.pdf";

                String html                     = "<head><style> </style></head><body>";
                       
                for (int i = 0; i < menus.size(); i++){
                    if (i > 0) {
                        html                    += "<div style='page-break-before:always' align='center'></div>";
                    }
                    html                        +=  menus.get(i);
                }
                html                            += "</body>";

                // Username: sravindran   API key: 3f324f38ed732151a5a4d120b29f980e
                File file                       = new File("/home/midware/pdf/indexHTML.htm");
                BufferedWriter bw               = new BufferedWriter(new FileWriter(file));
                bw.write(html);
                bw.close();
                FileOutputStream fileStream = new FileOutputStream("/home/midware/pdf/indexfile.pdf");
                client.convertFile("/home/midware/pdf/indexHTML.htm", fileStream);
                fileStream.close();
                
                StringBuilder message            = new StringBuilder();
                message.append("<tr><td>Find the attached BeerBoard Index</td></tr>");
                 
                if(email!=null && !email.equals("")) {
                    sendMailWithAttachment("", "tech@beerboard.com", email , "", customerName + " - " + locationName + " BeerBoard Index Report ", "sendMail", message, false, fileName);
                } else {
                    sendMailWithAttachment("", "tech@beerboard.com", "suba@beerboard.com" , "", customerName + " - " + locationName + " BeerBoard Index", "sendMail", message, false, fileName);
                }
        } catch(Exception e) {
            logger.debug(e.getMessage());
        } finally {
            close(rs);
            close(stmt);
        }
         
    }

}
