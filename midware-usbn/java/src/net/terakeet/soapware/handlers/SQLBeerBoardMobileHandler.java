    /*
     * SQLBeerBoardMobileHandler.java
     *
     * Created on February 23, 2011, 15:00
     *
     */

package net.terakeet.soapware.handlers;

import java.io.*;
import java.net.*;
import java.security.Permission;
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
import java.sql.Timestamp;
import java.sql.Time;
import java.util.*;
import java.text.*;
import java.text.SimpleDateFormat;
import java.util.Map.Entry;
import javax.imageio.ImageIO;
import javax.net.ssl.HttpsURLConnection;
import net.terakeet.util.TemplatedMessage;
import net.terakeet.util.ParameterFile;
import net.terakeet.util.MailException;
import net.terakeet.soapware.handlers.report.*;
import net.terakeet.util.*;
import java.awt.image.BufferedImage;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.multipart.FilePart;
import org.apache.commons.httpclient.methods.multipart.MultipartRequestEntity;
import org.apache.commons.httpclient.methods.multipart.Part;
import org.apache.commons.httpclient.methods.multipart.StringPart;
import org.apache.commons.httpclient.params.HttpMethodParams;
import org.apache.commons.httpclient.util.URIUtil;
import twitter4j.IDs;
import twitter4j.Status;
import twitter4j.StatusUpdate;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.User;
import twitter4j.auth.AccessToken;
import twitter4j.auth.BasicAuthorization;
import twitter4j.auth.RequestToken;
import twitter4j.conf.Configuration;
import twitter4j.conf.ConfigurationBuilder;
import twitter4j.media.ImageUpload;
import twitter4j.media.ImageUploadFactory;
import twitter4j.media.MediaProvider;
import org.json.JSONObject;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.XML;
import org.apache.log4j.BasicConfigurator;

import javapns.Push;
import javapns.communication.exceptions.CommunicationException;
import javapns.communication.exceptions.KeystoreException;
import javapns.notification.PushNotificationPayload;
import javapns.notification.Payload;

import com.google.android.gcm.server.Message;
import com.google.android.gcm.server.Result;
import com.google.android.gcm.server.Sender;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;

import java.security.cert.CertificateException;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.KeyManager;



public class SQLBeerBoardMobileHandler  implements Handler{

    private MidwareLogger logger;
    private static final String transConnName
                                            = "auper";
    private RegisteredConnection transconn;
    private SecureSession ss;
    private DecimalFormat cf;
    private LocationMap locationMap;
    private static SimpleDateFormat dateFormat 
                                            =   new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
    private static SimpleDateFormat dbDateFormat
                                            = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private static SimpleDateFormat timeFormat
                                            = new SimpleDateFormat("h a");
    

    /**
     * Creates a new instance of SQLBeerBoardHandler
     */
    public SQLBeerBoardMobileHandler() throws HandlerException {
        HandlerUtils.initializeClientKeyManager();
        logger                              = new MidwareLogger(SQLBeerBoardMobileHandler.class.getName());
        transconn                           = null;
        locationMap                         = null;
        cf                                  = (DecimalFormat) NumberFormat.getInstance(Locale.US);
    }

    public void handle(Element toHandle, Element toAppend) throws HandlerException{

        String function                     = toHandle.getName();
        String responseNamespace            = (String)SOAPMessage.getURIMap().get("tkmsg");

        String clientKey                    = HandlerUtils.getOptionalString(toHandle,"clientKey");
        ss                                  = ClientKeyManager.getSession(clientKey);

        logger                              = new MidwareLogger(SQLBeerBoardMobileHandler.class.getName(), function);
        logger.debug("SQLBeerBoardMobileHandler processing method: "+function);
        logger.xml("request: " + toHandle.asXML());

        transconn                           = DatabaseConnectionManager.getNewConnection(transConnName, function + " (SQLBeerBoardMobileHandler)");

        cf.applyPattern("#.####");
        try {
            // All methods require an admin client key
            if (ss.getLocation() == 0 && ss.getClientId() == 1 && ss.getSecurityLevel().canAdmin()) {
                if ("authBeerBoardUser".equals(function)) {
                    authBeerBoardUser(toHandle, responseFor(function,toAppend) );
                } else if ("authBeerBoardUserLogout".equals(function)) {
                    authBeerBoardUserLogout(toHandle, responseFor(function,toAppend));
                } else if ("getBeerBoardList".equals(function)) {
                    getBeerBoardList(toHandle, responseFor(function,toAppend));
                }  else if ("getBeerBoardMenu".equals(function)) {
                    getBeerBoardMenu(toHandle, responseFor(function,toAppend));
                } else if ("getLocationPromotions".equals(function)) {
                    getLocationPromotions(toHandle, responseFor(function,toAppend));
                } else if ("getLocationSponsors".equals(function)) {
                    getLocationSponsors(toHandle, responseFor(function,toAppend));
                } else if ("getBeerBoardUpdates".equals(function)) {
                    getBeerBoardUpdates(toHandle, responseFor(function,toAppend));
                } else if ("addUpdateDeleteLocationSpecials".equals(function)) {
                    addUpdateDeleteLocationSpecials(toHandle, responseFor(function,toAppend));
                } else if ("addUpdateDeleteLocationPromotions".equals(function)) {
                    addUpdateDeleteLocationPromotions(toHandle, responseFor(function,toAppend));
                } else if ("addUpdateDeleteLocationSponsors".equals(function)) {
                    addUpdateDeleteLocationSponsors(toHandle, responseFor(function,toAppend));
                } else if ("getLocationSpecials".equals(function)) {
                    getLocationSpecials(toHandle, responseFor(function,toAppend));
                } else if ("postSocialMediaUpdates".equals(function)) {
                    postSocialMediaUpdates(toHandle, responseFor(function,toAppend));
                } else if ("postRewardOnSocialMedia".equals(function)) {
                    postRewardOnSocialMedia(toHandle, responseFor(function,toAppend));
                } else if ("postCreativeOnSocialMedia".equals(function)) {
                    postCreativeOnSocialMedia(toHandle, responseFor(function,toAppend));
                } else if ("postCustomSocialMediaUpdates".equals(function)) {
                    postCustomSocialMediaUpdates(toHandle, responseFor(function,toAppend));
                } else if ("deleteFBPosts".equals(function)) {
                    deleteFBPosts(toHandle, responseFor(function,toAppend));
                } else if ("getLocationFeed".equals(function)) {
                    getLocationFeed(toHandle, responseFor(function,toAppend));
                }  else if ("getBeerBoardStates".equals(function)) {
                    getBeerBoardStates(toHandle, responseFor(function,toAppend));
                }else if ("pushNotificationApple".equals(function)) {
                    pushNotificationApple(toHandle, responseFor(function,toAppend));
                }else if ("pushNotificationAndroid".equals(function)) {
                    pushNotificationAndroid(toHandle, responseFor(function,toAppend));
                } else if ("sendManualPushMessage".equals(function)) {
                    sendManualPushMessage(toHandle, responseFor(function,toAppend));
                } else if ("promoNightPushMessage".equals(function)) {
                    promoNightPushMessage(toHandle, responseFor(function,toAppend));
                } else if ("debugFacebookToken".equals(function)) {
                    debugFacebookToken(toHandle, responseFor(function,toAppend));
                } else if ("sendRewardRemainder".equals(function)) {
                    sendRewardRemainder(toHandle, responseFor(function,toAppend));
                } else if ("getPushNotification".equals(function)) {
                    getPushNotification(toHandle, responseFor(function,toAppend));
                } else if ("addUpdateDeletePushNotification".equals(function)) {
                    addUpdateDeletePushNotification(toHandle, responseFor(function,toAppend));
                }  else if ("getLocationMenu".equals(function)) {
                    getLocationMenu(toHandle, responseFor(function,toAppend));
                } else if ("checkFullProductVersion".equals(function)) {
                    checkFullProductVersion(toHandle, responseFor(function,toAppend));
                } else if ("updateJSMenuDb".equals(function)) {
                    updateJSMenuDb(toHandle, responseFor(function,toAppend));
                } else {
                    logger.generalWarning("Unknown function '" + function + "'.");
                }
            } else if ("getBeerBoardLocations".equals(function)) {
                    getBeerBoardLocations(toHandle, responseFor(function,toAppend));
            } else if ("getConsumerBeerBoardMenu".equals(function)) {
                    getConsumerBeerBoardMenu(toHandle, responseFor(function,toAppend));
            } else if ("getConsumerBeerBoardMobileMenu".equals(function)) {
                    getBeerBoardMenu(toHandle, responseFor(function,toAppend));
            } else if ("getConsumerBeerBoardList".equals(function)) {
                    getConsumerBeerBoardList(toHandle, responseFor(function,toAppend));
            } else if ("getConsumerLocationSpecials".equals(function)) {
                    getLocationSpecials(toHandle, responseFor(function,toAppend));
            } else if ("getConsumerLocationPromotions".equals(function)) {
                    getLocationPromotions(toHandle, responseFor(function,toAppend));
            } else if ("getConsumerLocationSponsors".equals(function)) {
                    getLocationSponsors(toHandle, responseFor(function,toAppend));
            } else if ("checkDbVersion".equals(function)) {
                    checkDbVersion(toHandle, responseFor(function,toAppend));
            } else if ("addUserRating".equals(function)) {
                    addUserRating(toHandle, responseFor(function,toAppend));
            } else if ("addUserProfile".equals(function)) {
                    addUserProfile(toHandle, responseFor(function,toAppend));
            } else if ("addUserCheckin".equals(function)) {
                    addUserCheckin(toHandle, responseFor(function,toAppend));
            } else if ("getSpotLight".equals(function)) {
                    getSpotLight(toHandle, responseFor(function,toAppend));
            } else if ("getBeerBoardStates".equals(function)) {
                    getBeerBoardStates(toHandle, responseFor(function,toAppend));
            } else if ("getSponsorAd".equals(function)) {
                    getSponsorAd(toHandle, responseFor(function,toAppend));
            } else if ("getSponsorPromotion".equals(function)) {
                    getSponsorPromotion(toHandle, responseFor(function,toAppend));
            } else if ("redeemSponsorPromotion".equals(function)) {
                    redeemSponsorPromotion(toHandle, responseFor(function,toAppend));
            } else if ("addHtml5VisitorsLog".equals(function)) {
                    addHtml5VisitorsLog(toHandle, responseFor(function,toAppend));
            } else if ("getStyles".equals(function)) {
                    getStyles(toHandle, responseFor(function,toAppend));
            } else if ("getNearByProducts".equals(function)) {
                    getNearByProducts(toHandle, responseFor(function,toAppend));
            } else if ("getMobileUserId".equals(function)) {
                    getMobileUserId(toHandle, responseFor(function,toAppend));
            } else if ("getInsiderPasses".equals(function)) {
                    getInsiderPasses(toHandle, responseFor(function,toAppend));
            } else if ("testSocialMediaUpdates".equals(function)) {
                    testSocialMediaUpdates(toHandle, responseFor(function,toAppend));
            } else if ("getJSBeerBoardMenu".equals(function)) {
                    getJSBeerBoardMenu(toHandle, responseFor(function,toAppend));
            } else if ("getFavouriteBeer".equals(function)) {
                    getFavouriteBeer(toHandle, responseFor(function,toAppend));
            }  else if ("getFavouriteLocation".equals(function)) {
                    getFavouriteLocation(toHandle, responseFor(function,toAppend));
            }  else if ("removeFavouriteBeer".equals(function)) {
                    removeFavouriteBeer(toHandle, responseFor(function,toAppend));
            }  else if ("removeFavouriteLocation".equals(function)) {
                    removeFavouriteLocation(toHandle, responseFor(function,toAppend));
            }  else {
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
        try {
        return HandlerUtils.getOptionalInteger(HandlerUtils.getRequiredElement(toHandle,"caller"),"callerId");
        } catch(Exception e){
            logger.debug(e.getMessage());
            return 0;
        }
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

    /**     Beer Board security levels:
     *  All users are associated with a Beer Board Location
     *
     *  Each user has a Manager flag.
     *  A manager can access admin functions for the location.  They can also set
     *  up subordinate accounts for their location, and set other permissions.
     *
     * If a user is not a manager, the user is considered to be the beer board page
     *
     * See also: net.terakeet.usbn.WebPermssion
     */
    private void authBeerBoardUser(Element toHandle, Element toAppend) throws HandlerException {
           
        final int ROOT_CUSTOMER             = 0;

        int platform                        = HandlerUtils.getRequiredInteger(toHandle, "platform");
        String username                     = HandlerUtils.getOptionalString(toHandle, "username");
        String password                     = HandlerUtils.getOptionalString(toHandle, "password");
        String wmac                         = HandlerUtils.getOptionalString(toHandle, "wmac");
        String emac                         = HandlerUtils.getOptionalString(toHandle, "emac");
        
        String selectWMACBBTV               = "SELECT id,location FROM bbtvUserMac WHERE wmac=?";
        String selectEMACBBTV               = "SELECT id,location FROM bbtvUserMac WHERE emac=?";
        

        String checkRoot                    = "SELECT u.id, u.isBeerBoardManager, u.customer FROM user u " +
                                            " WHERE u.username = ? AND u.password = ? AND u.platform IN (?)";

        String selectBBTVLocation           = "SELECT l.id, l.name,  l.type,  c.id, c.name, c.type, l.easternOffset, lG.logo " +
                                            " FROM customer c LEFT JOIN location l ON l.customer = c.id "+
                                            " LEFT JOIN locationGraphics lG ON lG.location = l.id "+
                                            " LEFT JOIN locationDetails lD ON lD.location = l.id WHERE lD.active = 1 " +
                                            " ORDER BY l.name ASC ";


        String selectRoot                   = "SELECT l.id, l.name,  l.type, c.id, c.name, c.type, l.easternOffset, lG.logo " +
                                            " FROM customer c LEFT JOIN location l ON l.customer = c.id LEFT JOIN locationGraphics lG ON lG.location = l.id " +
                                            " WHERE c.id = ? ORDER BY l.name ASC ";

        String selectNormal                 = "SELECT l.id, l.name,  l.type, c.id, c.name, c.type, l.easternOffset, lG.logo " +
                                            " FROM userBeerBoardMap uBBM LEFT JOIN location l ON uBBM.location = l.id LEFT JOIN locationGraphics lG ON lG.location = l.id " +
                                            " LEFT JOIN customer c ON l.customer = c.id WHERE uBBM.user = ? ORDER BY l.name ASC";
        String selectNormalLocation         = "SELECT l.id, l.name,  l.type,  c.id, c.name, c.type, l.easternOffset, lG.logo " +
                                            " FROM customer c LEFT JOIN location l ON l.customer = c.id "+
                                            " LEFT JOIN locationGraphics lG ON lG.location = l.id "+
                                            " LEFT JOIN locationDetails lD ON lD.location = l.id WHERE lD.active = 1 " +
                                            " AND l.id= ? ";
        String insertUserAccess             = "INSERT INTO bbtvUsage (location, user, lastAccess) VALUES (?, ?, ?);";
        String updateUserLogin              = "UPDATE bbtvUserMac SET lastLogin = ? WHERE id = ?;";
        
        Calendar currentDate                = Calendar.getInstance();
        String lastAccess                   = dbDateFormat.format(currentDate.getTime());

        int userId                          = -1,user=-1;
        int location                        = -1;
        String platformName                 = "BeerBoard";

        PreparedStatement stmt              = null;
        ResultSet rs                        = null;

        try {
            if(wmac != null || emac != null) {
                if(emac != null && emac.length() > 5) {
                    stmt                    = transconn.prepareStatement(selectEMACBBTV);
                    stmt.setString(1, emac);
                    rs                      = stmt.executeQuery();
                    if (rs != null && rs.next()) {
                        user                = rs.getInt(1);
                        location            = rs.getInt(2);
                    }
                }
                
                if(wmac!=null && wmac.length() > 5) {
                    stmt                    = transconn.prepareStatement(selectWMACBBTV);
                    stmt.setString(1, wmac);
                    rs                      = stmt.executeQuery();
                    if (rs != null && rs.next()) {
                        user                = rs.getInt(1);
                        location            = rs.getInt(2);
                        
                    }
                }

                if (location > 0) {
                    stmt                    = transconn.prepareStatement(selectNormalLocation);
                    stmt.setInt(1, location);
                    rs                      = stmt.executeQuery();
                    if (rs != null && rs.next()) {
                        if(user > 0) {
                            stmt            = transconn.prepareStatement(updateUserLogin);
                            stmt.setString(1, lastAccess);
                            stmt.setInt(2, user);
                            stmt.executeUpdate();

                            stmt            = transconn.prepareStatement(insertUserAccess);
                            stmt.setInt(1, location);
                            stmt.setInt(2, user);
                            stmt.setString(3, lastAccess);
                            stmt.executeUpdate();
                        }
                    }

                    stmt                    = transconn.prepareStatement(selectNormalLocation);
                    stmt.setInt(1, location);
                    rs                      = stmt.executeQuery();
                    if (rs != null && rs.next()) {
                        int rsIndex         = 1;
                        Element locEl       = toAppend.addElement("location");
                        locEl.addElement("userId").addText(String.valueOf(user));
                        locEl.addElement("locationId").addText(String.valueOf(rs.getInt(rsIndex++)));
                        locEl.addElement("locationName").addText(HandlerUtils.nullToEmpty((rs.getString(rsIndex++))));
                        locEl.addElement("locationType").addText(String.valueOf(rs.getInt(rsIndex++)));
                        locEl.addElement("customerId").addText(String.valueOf(rs.getInt(rsIndex++)));
                        locEl.addElement("customerName").addText(HandlerUtils.nullToEmpty((rs.getString(rsIndex++))));
                        locEl.addElement("customerType").addText(String.valueOf(rs.getInt(rsIndex++)));
                        locEl.addElement("easternOffset").addText(HandlerUtils.nullToEmpty((rs.getString(rsIndex++))));
                        locEl.addElement("logo").addText(HandlerUtils.nullToEmpty(("http://bevmanager.net/Images/Location_logo/" + rs.getString(rsIndex++).trim().replaceAll("\'", "%27").replaceAll(" ", "%20"))));
                    } else {
                        logger.portalAction("Authentication failed to WMAC:" + wmac + " and EMAC: " + emac);
                        // authentication failed

                    }
                } else {
                    logger.ip("BB");
                    stmt                    = transconn.prepareStatement("SELECT SUBSTRING(REPLACE(ipAddress, '/',''), 1, LOCATE(':',ipAddress) - 2), id FROM ipLogs WHERE modified = 0 ORDER BY id DESC LIMIT 1;");
                    rs                      = stmt.executeQuery();
                    if (rs.next()) {
                        String ipAddress    = rs.getString(1);
                        logger.debug("IP:" + ipAddress);
                        stmt                = transconn.prepareStatement("UPDATE ipLogs SET modified = 1 WHERE modified = 0");
                        stmt.executeUpdate();

                        stmt                = transconn.prepareStatement("SELECT id FROM bbtvUserMac WHERE ipAddress LIKE '%" + ipAddress + "%'");
                        rs                  = stmt.executeQuery();
                        if (rs.next()) {
                            stmt            = transconn.prepareStatement("UPDATE bbtvUserMac SET wmac = ?, emac = ? WHERE id = ?;");
                            stmt.setString(1, wmac);
                            stmt.setString(2, emac);
                            stmt.setInt(3, rs.getInt(1));
                            stmt.executeUpdate();
                        } else {
                            stmt            = transconn.prepareStatement("SELECT location FROM ipLocationMap WHERE ipAddress  LIKE '%" + ipAddress + "%'");
                            rs              = stmt.executeQuery();
                            if (rs.next()) {
                                stmt        = transconn.prepareStatement("INSERT INTO bbtvUserMac (name, location, wmac, emac, ipAddress) VALUES ((SELECT name FROM location WHERE id = ?), ?, ?, ?, ?)");
                                stmt.setInt(1, rs.getInt(1));
                                stmt.setInt(2, rs.getInt(1));
                                stmt.setString(3, wmac);
                                stmt.setString(4, emac);
                                stmt.setString(5, ipAddress);
                                stmt.executeUpdate();
                            }
                        }
                    }
                }
            } else {
                stmt                        = transconn.prepareStatement(checkRoot);
                stmt.setString(1, username);
                stmt.setString(2, password);
                stmt.setInt(3, platform);
                rs                          = stmt.executeQuery();
                int isBeerBoardManager      = -1, associatedCustomer = -1, superadmin=-1;
                boolean isSuperAdmin        = false;
                if (rs != null && rs.next()) {
                    int rsIndex             = 1;
                    userId                  = rs.getInt(rsIndex++);
                    isBeerBoardManager      = rs.getInt(rsIndex++);
                    associatedCustomer      = rs.getInt(rsIndex++);

                    if (associatedCustomer == ROOT_CUSTOMER) {
                        isSuperAdmin        = true;
                        superadmin          = 1;
                    } else {
                        isSuperAdmin        = false;
                        superadmin          = 0;
                    }
                    toAppend.addElement("userId").addText(String.valueOf(userId));
                    toAppend.addElement("isAdmin").addText(String.valueOf(superadmin));
                    toAppend.addElement("hasUpdate").addText("1");
                }
                if (isSuperAdmin) {
                    // the user is an Admin (root)
                    String logMessage       = "Granting " + (isSuperAdmin ? "Admin" : "Super-manager") + " access to " + username + " for " + platformName;
                    logger.portalDetail(userId, "login", 0, logMessage, transconn);

                    stmt                    = transconn.prepareStatement(selectBBTVLocation);
                    rs                      = stmt.executeQuery();
                    while (rs != null && rs.next()) {
                        int rsIndex         = 1;
                        Element locEl       = toAppend.addElement("location");
                        locEl.addElement("locationId").addText(String.valueOf(rs.getInt(rsIndex++)));
                        locEl.addElement("locationName").addText(HandlerUtils.nullToEmpty((rs.getString(rsIndex++))));
                        locEl.addElement("locationType").addText(String.valueOf(rs.getInt(rsIndex++)));
                        locEl.addElement("customerId").addText(String.valueOf(rs.getInt(rsIndex++)));
                        locEl.addElement("customerName").addText(HandlerUtils.nullToEmpty((rs.getString(rsIndex++))));
                        locEl.addElement("customerType").addText(String.valueOf(rs.getInt(rsIndex++)));
                        locEl.addElement("easternOffset").addText(HandlerUtils.nullToEmpty((rs.getString(rsIndex++))));
                        locEl.addElement("logo").addText(HandlerUtils.nullToEmpty(("http://bevmanager.net/Images/Location_logo/" + rs.getString(rsIndex++).trim().replaceAll("\'", "%27").replaceAll(" ", "%20"))));
                    }
                } else if (isBeerBoardManager > 0) {
                    // the user is an Admin (root)
                    String logMessage       = "Granting " + (isSuperAdmin ? "Admin" : "Super-manager") + " access to " + username + " for " + platformName;
                    logger.portalDetail(userId, "login", 0, logMessage, transconn);
                    stmt                    = transconn.prepareStatement(selectRoot);
                    stmt.setInt(1, associatedCustomer);
                    rs                      = stmt.executeQuery();
                    while (rs != null && rs.next()) {
                        int rsIndex         = 1;
                        Element locEl       = toAppend.addElement("location");
                        locEl.addElement("locationId").addText(String.valueOf(rs.getInt(rsIndex++)));
                        locEl.addElement("locationName").addText(HandlerUtils.nullToEmpty((rs.getString(rsIndex++))));
                        locEl.addElement("locationType").addText(String.valueOf(rs.getInt(rsIndex++)));
                        locEl.addElement("customerId").addText(String.valueOf(rs.getInt(rsIndex++)));
                        locEl.addElement("customerName").addText(HandlerUtils.nullToEmpty((rs.getString(rsIndex++))));
                        locEl.addElement("customerType").addText(String.valueOf(rs.getInt(rsIndex++)));
                        locEl.addElement("easternOffset").addText(HandlerUtils.nullToEmpty((rs.getString(rsIndex++))));
                        locEl.addElement("logo").addText(HandlerUtils.nullToEmpty(("http://bevmanager.net/Images/Location_logo/" + rs.getString(rsIndex++).trim().replaceAll("\'", "%27").replaceAll(" ", "%20"))));
                    }
                } else if (userId >= 0) {
                    // the user is not an Admin(root)
                    String logMessage       = "Granting map-level access to " + username + " for " + platformName;
                    logger.portalDetail(userId, "login", 0, logMessage, transconn);
                    stmt                    = transconn.prepareStatement(selectNormal);
                    stmt.setInt(1, userId);
                    rs                      = stmt.executeQuery();
                    if (rs != null && rs.next()) {
                        int rsIndex         = 1;
                        int locationId      = rs.getInt(rsIndex++);
                        Element locEl       = toAppend.addElement("location");
                        locEl.addElement("locationId").addText(String.valueOf(locationId));
                        locEl.addElement("locationName").addText(HandlerUtils.nullToEmpty((rs.getString(rsIndex++))));
                        locEl.addElement("locationType").addText(String.valueOf(rs.getInt(rsIndex++)));
                        locEl.addElement("customerId").addText(String.valueOf(rs.getInt(rsIndex++)));
                        locEl.addElement("customerName").addText(HandlerUtils.nullToEmpty((rs.getString(rsIndex++))));
                        locEl.addElement("customerType").addText(String.valueOf(rs.getInt(rsIndex++)));
                        locEl.addElement("easternOffset").addText(HandlerUtils.nullToEmpty((rs.getString(rsIndex++))));
                        locEl.addElement("logo").addText(HandlerUtils.nullToEmpty(("http://bevmanager.net/Images/Location_logo/" + rs.getString(rsIndex++).trim().replaceAll("\'", "%27").replaceAll(" ", "%20"))));

                        String selectLocationGraphics
                                            = "SELECT logo FROM locationGraphics WHERE location = ? ";
                        stmt                = transconn.prepareStatement(selectLocationGraphics);
                        stmt.setInt(1, locationId);
                        rs                  = stmt.executeQuery();
                        if (rs != null && rs.next()) {
                            locEl.addElement("logo").addText(HandlerUtils.nullToEmpty(("http://bevmanager.net/Images/Location_logo/" + rs.getString(1).trim().replaceAll("\'", "%27").replaceAll(" ", "%20"))));
                        }
                    }
                } else {
                    logger.portalAction("Authentication failed to " + username + " for " + platformName);
                    // authentication failed
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
    
    
    private void authBeerBoardUserLogout(Element toHandle, Element toAppend) throws HandlerException {
        int userId                          = HandlerUtils.getRequiredInteger(toHandle, "userId");
        String logMessage                   = "Logging out user from Beer Board";
        logger.portalDetail(userId, "logout", 0, logMessage, transconn);
    }
    
    
     private void getBeerBoardStates(Element toHandle, Element toAppend) throws HandlerException {
         String country                         = HandlerUtils.getOptionalString(toHandle, "country");
         
        //String selectStateOLD                  = "SELECT DISTINCT(l.addrState), s.STNAME, s.country FROM location l LEFT JOIN state s ON s.USPSST=l.addrState WHERE s.country !='Brazil';";
        String selectState                  = "SELECT DISTINCT(l.addrState), s.STNAME, s.country FROM location l LEFT JOIN state s ON s.USPSST = l.addrState " +
                                            " AND s.country = l.addrCountry LEFT JOIN locationDetails lD ON lD.location = l.id WHERE lD.active = 1 " +
                                            " AND s.id IS NOT NULL ORDER BY s.country DESC, s.STNAME;";
        String selectIrelandState           = "SELECT STNAME,STNAME AS state, country FROM state where country='ireland'  ORDER BY  state;";
        PreparedStatement stmt              = null;
        ResultSet rs                        = null;
        try {
            if(country!=null && country.equals("ireland")){
                stmt                        = transconn.prepareStatement(selectIrelandState);
                rs                          = stmt.executeQuery();
                while (rs != null && rs.next()) {
                    int rsIndex             = 1;
                    Element locEl           = toAppend.addElement("state");
                    locEl.addElement("code").addText("IR-"+rs.getString(2));
                    locEl.addElement("name").addText(HandlerUtils.nullToEmpty((rs.getString(2))));
                    locEl.addElement("country").addText(HandlerUtils.nullToEmpty((rs.getString(3))));
                }
            } else {
                stmt                        = transconn.prepareStatement(selectState);
                rs                          = stmt.executeQuery();
                while (rs != null && rs.next()) {
                    int rsIndex             = 1;
                    String stateCode        = HandlerUtils.nullToEmpty((rs.getString(rsIndex++)));
                    String stateName        = HandlerUtils.nullToEmpty((rs.getString(rsIndex++)));
                    String countryName      = HandlerUtils.nullToEmpty((rs.getString(rsIndex++)));
                    if(countryName.equalsIgnoreCase("ireland")) {
                        stateCode           ="IR-"+stateName;
                    }
                    Element locEl           = toAppend.addElement("state");
                    locEl.addElement("code").addText(stateCode);
                    locEl.addElement("name").addText(stateName);
                    locEl.addElement("country").addText(countryName);
                }
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


    private void getBeerBoardLocations(Element toHandle, Element toAppend) throws HandlerException {

        String city                         = HandlerUtils.getOptionalString(toHandle, "city");
        String state                        = HandlerUtils.getOptionalString(toHandle, "state");
        int location                        = HandlerUtils.getOptionalInteger(toHandle, "locationId");
        int forMobile                       = HandlerUtils.getOptionalInteger(toHandle, "forMobile");
        int version                         = HandlerUtils.getOptionalInteger(toHandle, "version");
        String latitude                     = HandlerUtils.getOptionalString(toHandle, "latitude");
        String longitude                    = HandlerUtils.getOptionalString(toHandle, "longitude");
        int user                            = HandlerUtils.getOptionalInteger(toHandle, "user");
        Calendar currentDate                = Calendar.getInstance();
        Calendar calendar                   = Calendar.getInstance();
        int day                             = calendar.get(Calendar.DAY_OF_WEEK);
        boolean isProduction                = isProduction();
        String openHours                    = "";
        String oldState                     = state;
        if(state!=null && !state.equals("")) {
            if(state.contains("IR-")){
                state                       = "IR";
            }
        }
        switch(day) {
            
            case 1:
                openHours                   = "DATE_FORMAT(IFNULL(lH.openSun, '2000-00-00 11:00:00'),'%l:%i %p'), DATE_FORMAT(IFNULL(lH.closeSun, '2000-00-00 02:00:00'),' %l:%i %p')";
                break;
            case 2:
                openHours                   = "DATE_FORMAT(IFNULL(lH.openMon, '2000-00-00 11:00:00'),'%l:%i %p'), DATE_FORMAT(IFNULL(lH.closeMon, '2000-00-00 02:00:00'),' %l:%i %p')";
                break;
            case 3:
                openHours                   = "DATE_FORMAT(IFNULL(lH.openTue, '2000-00-00 11:00:00'),'%l:%i %p'), DATE_FORMAT(IFNULL(lH.closeTue, '2000-00-00 02:00:00'),' %l:%i %p')";
                break;
            case 4:
                openHours                   = "DATE_FORMAT(IFNULL(lH.openWed, '2000-00-00 11:00:00'),'%l:%i %p'), DATE_FORMAT(IFNULL(lH.closeWed, '2000-00-00 02:00:00'),' %l:%i %p')";
                break;    
            case 5:
                openHours                   = "DATE_FORMAT(IFNULL(lH.openThu, '2000-00-00 11:00:00'),'%l:%i %p'), DATE_FORMAT(IFNULL(lH.closeThu, '2000-00-00 02:00:00'),' %l:%i %p')";
                break;
            case 6:
                openHours                   = "DATE_FORMAT(IFNULL(lH.openFri, '2000-00-00 11:00:00'),'%l:%i %p'), DATE_FORMAT(IFNULL(lH.closeFri, '2000-00-00 02:00:00'),' %l:%i %p')";
                break;    
            case 7:
                openHours                   = "DATE_FORMAT(IFNULL(lH.openSat, '2000-00-00 11:00:00'),'%l:%i %p'), DATE_FORMAT(IFNULL(lH.closeSat, '2000-00-00 02:00:00'),' %l:%i %p')";
                break;
        }
        
        String selectCity                   = "SELECT l.id, CONCAT(c.name, ' - ', l.name), l.latitude, l.longitude, l.easternOffset, IFNULL(REPLACE(lG.logo, ' ', '%20'), 'USBNLogo.png'), l.addrStreet, l.addrCity, l.addrState, l.addrZip, l.boardname, " +openHours +
                                            " FROM location l LEFT JOIN customer c ON c.id = l.customer LEFT JOIN locationGraphics lG ON lG.location = l.id "+
                                            " LEFT JOIN locationDetails lD ON lD.location = l.id LEFT JOIN locationHours lH ON lH.location =l.id WHERE lD.active = 1 AND l.addrCountry = 'USA' AND l.addrCity = ? " +(isProduction ? " AND l.customer<> 205 " : "  ") +
                                            " ORDER BY l.addrState, l.boardname, l.name ASC ";
       String selectLocationId              = "SELECT l.id, CONCAT(c.name, ' - ', l.name), l.latitude, l.longitude, l.easternOffset, IFNULL(REPLACE(lG.logo, ' ', '%20'), 'USBNLogo.png'), l.addrStreet, l.addrCity, l.addrState, l.addrZip, l.boardname, " +openHours +
                                            " FROM location l LEFT JOIN customer c ON c.id = l.customer LEFT JOIN locationGraphics lG ON lG.location = l.id "+
                                            " LEFT JOIN locationDetails lD ON lD.location = l.id LEFT JOIN locationHours lH ON lH.location =l.id WHERE lD.active = 1  AND l.id = ? " +
                                            " ORDER BY l.addrState, l.boardname, l.name ASC ";
       String selectLocationState           = "SELECT l.id, l.name, l.latitude, l.longitude, l.easternOffset, IFNULL(REPLACE(lG.logo, ' ', '%20'), 'USBNLogo.png'), l.addrStreet, l.addrCity, l.addrState, l.addrZip, l.boardname, " +openHours +
                                            " FROM location l LEFT JOIN customer c ON c.id = l.customer LEFT JOIN locationGraphics lG ON lG.location = l.id "+
                                            " LEFT JOIN locationDetails lD ON lD.location = l.id LEFT JOIN locationHours lH ON lH.location = l.id " +
                                            " WHERE lD.active = 1 AND l.addrState = ? AND l.type = 1 " + (isProduction ? " AND l.customer<> 205 " : "  ") +
                                            " UNION " +
                                            " SELECT l.id, l.name, l.latitude, l.longitude, l.easternOffset, IFNULL(REPLACE(lG.logo, ' ', '%20'), 'USBNLogo.png'), l.addrStreet, l.addrCity, l.addrState, l.addrZip, b.boardname, " +openHours +
                                            " FROM bar b LEFT JOIN location l ON l.id = b.location LEFT JOIN customer c ON c.id = l.customer LEFT JOIN locationGraphics lG ON lG.location = l.id "+
                                            " LEFT JOIN locationDetails lD ON lD.location = l.id LEFT JOIN locationHours lH ON lH.location = l.id " +
                                            " WHERE lD.active = 1 AND l.addrState = ? AND l.type = 2 " + (isProduction ? " AND l.customer<> 205 " : "  ") +
                                            " ORDER BY addrState, boardname, name ASC ";
       String selectLocationLatLong         = "SELECT l.id, CONCAT(c.name, ' - ', l.name), l.latitude, l.longitude, l.easternOffset, IFNULL(REPLACE(lG.logo, ' ', '%20'), 'USBNLogo.png'), l.addrStreet, l.addrCity, l.addrState, l.addrZip, l.boardname, " +openHours+
                                            " FROM location l LEFT JOIN customer c ON c.id = l.customer LEFT JOIN locationGraphics lG ON lG.location = l.id "+
                                            " LEFT JOIN locationDetails lD ON lD.location = l.id LEFT JOIN locationHours lH ON lH.location =l.id WHERE lD.active = 1 AND l.addrCity = 'Syracuse' " +
                                            " ORDER BY l.addrState, l.boardname, l.name ASC ";
       String selectSponsor                 = "SELECT id, count FROM bevSyncCampaignReward WHERE CURDATE() = DATE(startTime) AND location = ?; ";
       String selectSponsorMap              = "SELECT reward from promotionUserMap WHERE promotion = ? AND user =?; ";
       String updateLatLong                 = "UPDATE bbtvMobileUser SET latitude=?, longitude = ?, arrival= ? WHERE id =?;";
       String selectUserLoc                 = "SELECT id, arrival FROM bbtvMobileUser WHERE id = ? ;";
       String selectLatLongProduction       = "SELECT l.id, l.name, l.latitude, l.longitude, l.easternOffset, IFNULL(REPLACE(lG.logo, ' ', '%20'), 'USBNLogo.png'), " +
                                            " l.addrStreet, l.addrCity, l.addrState, l.addrZip, l.boardname, " + openHours + ", " +
                                            " (6371 * acos( cos( radians(?) ) * cos( radians( l.latitude ) ) * cos( radians( ? ) - radians(l.longitude) ) + sin( radians(?) ) * sin( radians(l.latitude) ) )) AS distanta " +
                                            " FROM location l LEFT JOIN customer c ON c.id = l.customer LEFT JOIN locationGraphics lG ON lG.location = l.id "+
                                            " LEFT JOIN locationDetails lD ON lD.location = l.id LEFT JOIN locationHours lH ON lH.location =l.id " +
                                            " WHERE lD.active = 1 AND l.type = 1 "+(isProduction ? " AND l.customer<> 205 " : "  ") + " HAVING distanta < 33 " +
                                            " UNION " +
                                            " SELECT l.id, l.name, l.latitude, l.longitude, l.easternOffset, IFNULL(REPLACE(lG.logo, ' ', '%20'), 'USBNLogo.png'), " +
                                            " l.addrStreet, l.addrCity, l.addrState, l.addrZip, b.boardname, " + openHours + ", " +
                                            " (6371 * acos( cos( radians(?)) * cos( radians( l.latitude ) ) * cos( radians( ? ) - radians(l.longitude) ) + sin( radians(?) ) * sin( radians(l.latitude) ) )) AS distanta " +
                                            " FROM bar b LEFT JOIN location l ON l.id = b.location LEFT JOIN customer c ON c.id = l.customer LEFT JOIN locationGraphics lG ON lG.location = l.id "+
                                            " LEFT JOIN locationDetails lD ON lD.location = l.id LEFT JOIN locationHours lH ON lH.location =l.id " +
                                            " WHERE lD.active = 1 AND l.type = 2 "+ (isProduction ? " AND l.customer<> 205 " : "   ") +" HAVING distanta < 33 " ;
       if(!isProduction) {
           selectLatLongProduction          += " UNION " +
                                            " SELECT l.id, l.name, l.latitude, l.longitude, l.easternOffset, IFNULL(REPLACE(lG.logo, ' ', '%20'), 'USBNLogo.png'), " +
                                            " l.addrStreet, l.addrCity, l.addrState, l.addrZip, b.boardname, " + openHours + ",0 " +
                                            " FROM bar b LEFT JOIN location l ON l.id = b.location LEFT JOIN customer c ON c.id = l.customer LEFT JOIN locationGraphics lG ON lG.location = l.id "+
                                            " LEFT JOIN locationDetails lD ON lD.location = l.id LEFT JOIN locationHours lH ON lH.location =l.id " +
                                            " WHERE lD.active = 1 AND l.id IN ( 425) " ;
       }
       
       selectLatLongProduction              +=" ORDER BY distanta, boardname, name;";
       
       PreparedStatement stmt               = null;
       ResultSet rs                         = null, rsSponsor = null, rsSponsorMap = null, rsLat = null;
       if(latitude!=null) {
           if (latitude.equalsIgnoreCase("0.0")) {
               latitude                     = "43.0478720";
               longitude                    = "-76.1867790";
           }
       }
       int loc[]                            = {872,3,852,853,81,851,853};
       String indianLoc[]                   = {"Anjappar","Kabul","Saravana bavan","Wangs","pizza hut","panjabi tadka","Aahar"};
       String indianLat[]                   = {"13.044463","13.044693","13.044045","13.043187","13.044525","13.044463","13.041891"};
       String indianLon[]                   = {"80.266448","80.264474","80.263981","80.268294","80.265676","80.266148","80.268036"};

       try {
            boolean hqLocations             = false;
            if(city !=null && !city.equals("")) {
                stmt                        = transconn.prepareStatement(selectCity);
                stmt.setString(1, city);
                rs                          = stmt.executeQuery();
                while (rs != null && rs.next()) {
                    int rsIndex             = 1;
                    hqLocations             = false;
                    int locationId          = rs.getInt(rsIndex++);
                    if (hasNoActiveLines(locationId)) {
                        continue;
                    }
                    Element locEl           = toAppend.addElement("location");
                    locEl.addElement("locationId").addText(String.valueOf(locationId));
                    locEl.addElement("locationName").addText(HandlerUtils.nullToEmpty((rs.getString(rsIndex++))));
                    locEl.addElement("latitude").addText(String.valueOf(rs.getString(rsIndex++)));
                    locEl.addElement("longitude").addText(String.valueOf(rs.getString(rsIndex++)));
                    locEl.addElement("easternOffset").addText(HandlerUtils.nullToEmpty((rs.getString(rsIndex++))));
                    locEl.addElement("logo").addText(HandlerUtils.nullToEmpty(("http://bevmanager.net/Images/Location_logo/" + rs.getString(rsIndex++).trim().replaceAll("\'", "%27").replaceAll(" ", "%20"))));
                    locEl.addElement("addrStreet").addText(HandlerUtils.nullToEmpty((rs.getString(rsIndex++))));
                    locEl.addElement("addrCity").addText(HandlerUtils.nullToEmpty((rs.getString(rsIndex++))));
                    locEl.addElement("addrState").addText(HandlerUtils.nullToEmpty((rs.getString(rsIndex++))));
                    locEl.addElement("addrZip").addText(HandlerUtils.nullToEmpty((rs.getString(rsIndex++))));
                    locEl.addElement("boardName").addText(HandlerUtils.nullToEmpty((rs.getString(rsIndex++))));
                    String hours        = "11:00 AM - 2:00 AM";
                    String open         = rs.getString(rsIndex++);
                    String close        = rs.getString(rsIndex++);
                    if(open!=null & close!=null) {
                        hours           = open + " - " + close;
                    }
                    locEl.addElement("hours").addText(HandlerUtils.nullToEmpty(hours));
                    if(user > 0) {
                        stmt            = transconn.prepareStatement(selectSponsor);
                        stmt.setInt(1, locationId);
                        rsSponsor       = stmt.executeQuery();
                        if(rsSponsor.next()) {
                            int sponsor     = rsSponsor.getInt(1);
                            int count       = rsSponsor.getInt(2);
                            logger.debug("Sponsor:Present");
                            if(count > 0) {
                                stmt            = transconn.prepareStatement(selectSponsorMap);
                                stmt.setInt(1, sponsor);
                                stmt.setInt(2, user);
                                rsSponsorMap= stmt.executeQuery();
                                if(rsSponsorMap.next()) {
                                    int reward
                                            = rsSponsorMap.getInt(1);
                                        locEl.addElement("sponsor").addText("2");
                                        logger.debug("Sponsor:0");
                                    } else {
                                        locEl.addElement("sponsor").addText("2");
                                        logger.debug("Sponsor:2");
                                    }
                                }else {
                                    locEl.addElement("sponsor").addText("2");
                                }
                        }else {
                                locEl.addElement("sponsor").addText("0");
                        }
                    }
                }
            }

            if (location > 0) {
                hqLocations                 = false;
                stmt                        = transconn.prepareStatement(selectLocationId);
                stmt.setInt(1, location);
                rs                          = stmt.executeQuery();
                if (rs != null && rs.next()) {
                    int rsIndex             = 1;
                    Element locEl           = toAppend.addElement("location");
                    locEl.addElement("locationId").addText(String.valueOf(rs.getInt(rsIndex++)));
                    locEl.addElement("locationName").addText(HandlerUtils.nullToEmpty((rs.getString(rsIndex++))));
                    locEl.addElement("latitude").addText(String.valueOf(rs.getString(rsIndex++)));
                    locEl.addElement("longitude").addText(String.valueOf(rs.getString(rsIndex++)));
                    locEl.addElement("easternOffset").addText(HandlerUtils.nullToEmpty((rs.getString(rsIndex++))));
                    locEl.addElement("logo").addText(HandlerUtils.nullToEmpty(("http://bevmanager.net/Images/Location_logo/" + rs.getString(rsIndex++).trim().replaceAll("\'", "%27").replaceAll(" ", "%20"))));
                    locEl.addElement("addrStreet").addText(HandlerUtils.nullToEmpty((rs.getString(rsIndex++))));
                    locEl.addElement("addrCity").addText(HandlerUtils.nullToEmpty((rs.getString(rsIndex++))));
                    locEl.addElement("addrState").addText(HandlerUtils.nullToEmpty((rs.getString(rsIndex++))));
                    locEl.addElement("addrZip").addText(HandlerUtils.nullToEmpty((rs.getString(rsIndex++))));
                    locEl.addElement("boardName").addText(HandlerUtils.nullToEmpty((rs.getString(rsIndex++))));
                    String hours            = "11:00 AM - 2:00 AM";
                    String open             = rs.getString(rsIndex++);
                    String close            = rs.getString(rsIndex++);
                    if(open!=null & close!=null) {
                        hours               = open + " - " + close;
                    }
                    locEl.addElement("hours").addText(HandlerUtils.nullToEmpty(hours));
                    if(user > 0) {
                    stmt                    = transconn.prepareStatement(selectSponsor);
                    stmt.setInt(1, location);

                    rsSponsor               = stmt.executeQuery();
                    if(rsSponsor.next()) {
                        int sponsor         = rsSponsor.getInt(1);
                        int count           = rsSponsor.getInt(2);
                        locEl.addElement("passId").addText(String.valueOf(sponsor));
                        locEl.addElement("sponsor").addText("2");
                        } else {
                            locEl.addElement("passId").addText(String.valueOf(0));
                            locEl.addElement("sponsor").addText("0");
                        }
                    }
                }
            } else  if(state !=null && !state.equals("")) {
                stmt                        = transconn.prepareStatement(selectLocationState);
                stmt.setString(1, state);
                stmt.setString(2, state);
                rs                          = stmt.executeQuery();
                while (rs.next()) {
                    int rsIndex             = 1;
                    hqLocations             = false;
                    int locationId          = rs.getInt(rsIndex++);
                    if (hasNoActiveLines(locationId)) {
                        continue;
                    }
                    Element locEl           = toAppend.addElement("location");
                    locEl.addElement("locationId").addText(String.valueOf(locationId));
                    locEl.addElement("locationName").addText(HandlerUtils.nullToEmpty((rs.getString(rsIndex++))));
                    locEl.addElement("latitude").addText(String.valueOf(rs.getString(rsIndex++)));
                    locEl.addElement("longitude").addText(String.valueOf(rs.getString(rsIndex++)));
                    locEl.addElement("easternOffset").addText(HandlerUtils.nullToEmpty((rs.getString(rsIndex++))));
                    locEl.addElement("logo").addText(HandlerUtils.nullToEmpty(("http://bevmanager.net/Images/Location_logo/" + HandlerUtils.nullToEmpty(rs.getString(rsIndex++)).trim().replaceAll("\'", "%27").replaceAll(" ", "%20"))));
                    locEl.addElement("addrStreet").addText(HandlerUtils.nullToEmpty((rs.getString(rsIndex++))));
                    locEl.addElement("addrCity").addText(HandlerUtils.nullToEmpty((rs.getString(rsIndex++))));
                    String addrState            =HandlerUtils.nullToEmpty((rs.getString(rsIndex++)));
                    if(state.contains("IR")){
                        addrState               =oldState;
                    }
                    locEl.addElement("addrState").addText(addrState);
                    locEl.addElement("addrZip").addText(HandlerUtils.nullToEmpty((rs.getString(rsIndex++))));
                    locEl.addElement("boardName").addText(HandlerUtils.nullToEmpty((rs.getString(rsIndex++))));
                    String hours            = "11:00 AM - 2:00 AM";
                    String open             = rs.getString(rsIndex++);
                    String close            = rs.getString(rsIndex++);

                    if(open!=null & close!=null) {
                        hours               = open + " - " + close;
                    }
                    locEl.addElement("hours").addText(HandlerUtils.nullToEmpty(hours));
                    if(user > 0) {
                        stmt                = transconn.prepareStatement(selectSponsor);
                        stmt.setInt(1, locationId);
                        rsSponsor           = stmt.executeQuery();
                        if(rsSponsor.next()) {
                            int sponsor     = rsSponsor.getInt(1);
                            int count       = rsSponsor.getInt(2);
                            locEl.addElement("passId").addText(String.valueOf(sponsor));
                            logger.debug("Sponsor:Present");
                            if(count > 0) {
                                stmt        = transconn.prepareStatement(selectSponsorMap);
                                stmt.setInt(1, sponsor);
                                stmt.setInt(2, user);
                                rsSponsorMap= stmt.executeQuery();
                                if(rsSponsorMap.next()) {
                                    int reward
                                            = rsSponsorMap.getInt(1);
                                    locEl.addElement("sponsor").addText("2");
                                } else {
                                    locEl.addElement("sponsor").addText("2");
                                }
                            }else {
                                locEl.addElement("sponsor").addText("2");
                            }
                        }else {
                            locEl.addElement("passId").addText(String.valueOf(0));
                            locEl.addElement("sponsor").addText("0");
                        }
                    }
                }
            } else if(latitude !=null && !latitude.equals("") && longitude !=null && !longitude.equals("")) {
                boolean updateLocation  = true;
                if(user >0) {
                    String today        = dbDateFormat.format(currentDate.getTime());
                    //logger.debug("today: "+today);
                    stmt                = transconn.prepareStatement(selectUserLoc);
                    stmt.setInt(1, user);
                    rsLat               = stmt.executeQuery();
                    if(rsLat.next()) {
                        String arrival  = rsLat.getString(2);
                        //logger.debug("arrival: "+arrival);
                        if(arrival!=null &&! arrival.equals("")){
                            long[] diff = getTimeDifference(dbDateFormat.parse(arrival), dbDateFormat.parse(today));
                            //logger.debug("diff: "+(diff[0]*24+diff[1]));
                            if(diff[0]*24+diff[1]>=2) {
                                updateLocation
                                        = true;
                            }
                        } else {
                            updateLocation
                                        = true;
                        }
                        if(user == 464||user == 698||user== 718|| user== 719 || user==1722)
                        {
                            latitude    = "43.0499035";
                            longitude   = "-76.1514456";
                            updateLocation
                                        = true;
                        }
                        if(updateLocation) {
                            stmt        = transconn.prepareStatement(updateLatLong);
                            stmt.setString(1, latitude);
                            stmt.setString(2, longitude);
                            stmt.setString(3, today);
                            stmt.setInt(4, user);
                            stmt.executeUpdate();
                        }
                    }
                }
                
                
                if(user == 380||user==1594||user==1554||user==2064){
                    
                    for(int i=0;i<indianLoc.length;i++){
                        Element locEl       = toAppend.addElement("location");
                        int locationId      =loc[i];
                        String locationName = indianLoc[i];
                        locEl.addElement("locationId").addText(String.valueOf(locationId));
                        locEl.addElement("locationName").addText(HandlerUtils.nullToEmpty(locationName));
                        locEl.addElement("latitude").addText(indianLat[i]);
                        locEl.addElement("longitude").addText(indianLon[i]);
                        locEl.addElement("easternOffset").addText(HandlerUtils.nullToEmpty(("")));
                        locEl.addElement("logo").addText(HandlerUtils.nullToEmpty(("http://bevmanager.net/Images/Location_logo/BlueTusk.jpg")));
                        locEl.addElement("addrStreet").addText(HandlerUtils.nullToEmpty("RK.Salai"));
                        locEl.addElement("addrCity").addText(HandlerUtils.nullToEmpty("Chennai"));
                        locEl.addElement("addrState").addText(HandlerUtils.nullToEmpty("Tamil Nadu"));
                        locEl.addElement("addrZip").addText(HandlerUtils.nullToEmpty("600004"));
                        locEl.addElement("boardName").addText(HandlerUtils.nullToEmpty("Netgrow"));
                        stmt            = transconn.prepareStatement("SELECT m.id,m.message,p.openLocation FROM wakeupMessage m LEFT JOIN wakeupMessageMap p ON p.message=m.id  WHERE p.location = ?;");
                        stmt.setInt(1, locationId);
                        rsSponsor       = stmt.executeQuery();
                        if(rsSponsor.next()) {
                            locEl.addElement("messageId").addText(String.valueOf(rsSponsor.getInt(1)));
                            locEl.addElement("message").addText(HandlerUtils.nullToEmpty(rsSponsor.getString(2)));
                            if(locationName.equals("pizza hut")){
                                locEl.addElement("openLocation").addText(String.valueOf(0));
                            } else {
                                locEl.addElement("openLocation").addText(String.valueOf(rsSponsor.getInt(3)));
                            }
                        } else {
                            locEl.addElement("messageId").addText(String.valueOf(0));
                            locEl.addElement("message").addText(HandlerUtils.nullToEmpty(""));
                            locEl.addElement("openLocation").addText(String.valueOf(0));
                        }
                        
                        locEl.addElement("hours").addText(HandlerUtils.nullToEmpty("11:00 AM - 2:00 AM"));
                        locEl.addElement("passId").addText(String.valueOf(0));
                        locEl.addElement("sponsor").addText("0");
                    }
                }

                if(forMobile == 0 ||(forMobile == 1 && updateLocation == true)) {
                    //stmt                        = transconn.prepareStatement(selectLocationLatLong);
                    stmt                    = transconn.prepareStatement(selectLatLongProduction);
                    stmt.setDouble(1, Double.parseDouble(latitude));
                    stmt.setDouble(2, Double.parseDouble(longitude));
                    stmt.setDouble(3, Double.parseDouble(latitude));
                    stmt.setDouble(4, Double.parseDouble(latitude));
                    stmt.setDouble(5, Double.parseDouble(longitude));
                    stmt.setDouble(6, Double.parseDouble(latitude));
                    rs                      = stmt.executeQuery();
                    while (rs != null && rs.next()) {
                        int rsIndex         = 1;
                        hqLocations         = false;
                        int locationId          = rs.getInt(rsIndex++);
                        if (hasNoActiveLines(locationId)) {
                            continue;
                        }
                        Element locEl       = toAppend.addElement("location");
                        String locationName = rs.getString(rsIndex++);
                        locEl.addElement("locationId").addText(String.valueOf(locationId));
                        locEl.addElement("locationName").addText(HandlerUtils.nullToEmpty(locationName));
                        locEl.addElement("latitude").addText(String.valueOf(rs.getString(rsIndex++)));
                        locEl.addElement("longitude").addText(String.valueOf(rs.getString(rsIndex++)));
                        locEl.addElement("easternOffset").addText(HandlerUtils.nullToEmpty((rs.getString(rsIndex++))));
                        locEl.addElement("logo").addText(HandlerUtils.nullToEmpty(("http://bevmanager.net/Images/Location_logo/" + rs.getString(rsIndex++).trim().replaceAll("\'", "%27").replaceAll(" ", "%20"))));
                        locEl.addElement("addrStreet").addText(HandlerUtils.nullToEmpty((rs.getString(rsIndex++))));
                        locEl.addElement("addrCity").addText(HandlerUtils.nullToEmpty((rs.getString(rsIndex++))));
                        locEl.addElement("addrState").addText(HandlerUtils.nullToEmpty((rs.getString(rsIndex++))));
                        locEl.addElement("addrZip").addText(HandlerUtils.nullToEmpty((rs.getString(rsIndex++))));
                        locEl.addElement("boardName").addText(HandlerUtils.nullToEmpty((rs.getString(rsIndex++))));
                        
                        stmt            = transconn.prepareStatement("SELECT m.id,m.message,p.openLocation FROM wakeupMessage m LEFT JOIN wakeupMessageMap p ON p.message=m.id  WHERE p.location = ?;");
                        stmt.setInt(1, locationId);
                        rsSponsor       = stmt.executeQuery();
                        if(rsSponsor.next()) {
                            locEl.addElement("messageId").addText(String.valueOf(rsSponsor.getInt(1)));
                            locEl.addElement("message").addText(HandlerUtils.nullToEmpty(rsSponsor.getString(2)));
                            locEl.addElement("openLocation").addText(String.valueOf(rsSponsor.getInt(3)));
                        } else {
                            locEl.addElement("messageId").addText(String.valueOf(0));
                            locEl.addElement("message").addText(HandlerUtils.nullToEmpty(""));
                            locEl.addElement("openLocation").addText(String.valueOf(0));
                        }
                        
                        String hours        = "11:00 AM - 2:00 AM";
                        String open         = rs.getString(rsIndex++);
                        String close        = rs.getString(rsIndex++);
                        if(open!=null & close!=null) {
                            hours           = open + " - " + close;
                        }
                        locEl.addElement("hours").addText(HandlerUtils.nullToEmpty(hours));
                        if(user > 0) {
                            stmt            = transconn.prepareStatement(selectSponsor);
                            stmt.setInt(1, locationId);
                            rsSponsor       = stmt.executeQuery();
                            if(rsSponsor.next()) {
                                int sponsor = rsSponsor.getInt(1);
                                int count   = rsSponsor.getInt(2);
                                locEl.addElement("passId").addText(String.valueOf(sponsor));
                                logger.debug("Sponsor:Present");
                                if(count > 0) {
                                    stmt= transconn.prepareStatement(selectSponsorMap);
                                    stmt.setInt(1, sponsor);
                                    stmt.setInt(2, user);
                                    rsSponsorMap
                                            = stmt.executeQuery();
                                    if(rsSponsorMap.next()) {
                                        int reward
                                            = rsSponsorMap.getInt(1);
                                        locEl.addElement("sponsor").addText("2");
                                    } else {
                                        locEl.addElement("sponsor").addText("2");
                                    }
                                }else {
                                    locEl.addElement("sponsor").addText("2");
                                }
                            }else {
                                locEl.addElement("passId").addText(String.valueOf(0));
                                locEl.addElement("sponsor").addText("0");
                            }
                        }
                    }
                }
            } else if(hqLocations) {
                String selectLocation   = "SELECT l.id, l.name, l.latitude, l.longitude, l.easternOffset, IFNULL(REPLACE(lG.logo, ' ', '%20'), 'USBNLogo.png'), " +
                                        " l.addrStreet, l.addrCity, l.addrState, l.addrZip, l.boardname,"+openHours +" FROM location l LEFT JOIN customer c ON c.id = l.customer " +
                                        " LEFT JOIN locationGraphics lG ON lG.location = l.id LEFT JOIN locationDetails lD ON lD.location = l.id LEFT JOIN locationHours lH ON lH.location =l.id" +
                                        " WHERE lD.active = 1 AND l.type = 1 AND l.addrCountry = 'USA' AND l.addrState != 'DC' " +
                                        " ORDER BY l.addrState, c.name, l.name ASC ";
                stmt                    = transconn.prepareStatement(selectLocation);
                rs                      = stmt.executeQuery();
                while (rs != null && rs.next()) {
                    int rsIndex         = 1;
                    int locationId      = rs.getInt(rsIndex++);
                    if (hasNoActiveLines(locationId)) {
                        continue;
                    }
                    Element locEl       = toAppend.addElement("location");
                    locEl.addElement("locationId").addText(String.valueOf(locationId));
                    locEl.addElement("locationName").addText(HandlerUtils.nullToEmpty((rs.getString(rsIndex++))));
                    locEl.addElement("latitude").addText(String.valueOf(rs.getString(rsIndex++)));
                    locEl.addElement("longitude").addText(String.valueOf(rs.getString(rsIndex++)));
                    locEl.addElement("easternOffset").addText(HandlerUtils.nullToEmpty((rs.getString(rsIndex++))));
                    locEl.addElement("logo").addText(HandlerUtils.nullToEmpty(("http://bevmanager.net/Images/Location_logo/" + rs.getString(rsIndex++).trim().replaceAll("\'", "%27").replaceAll(" ", "%20"))));
                    locEl.addElement("addrStreet").addText(HandlerUtils.nullToEmpty((rs.getString(rsIndex++))));
                    locEl.addElement("addrCity").addText(HandlerUtils.nullToEmpty((rs.getString(rsIndex++))));
                    locEl.addElement("addrState").addText(HandlerUtils.nullToEmpty((rs.getString(rsIndex++))));
                    locEl.addElement("addrZip").addText(HandlerUtils.nullToEmpty((rs.getString(rsIndex++))));
                    locEl.addElement("boardName").addText(HandlerUtils.nullToEmpty((rs.getString(rsIndex++))));
                    String hours        = "11:00 AM - 2:00 AM";
                    String open         = rs.getString(rsIndex++);
                    String close        = rs.getString(rsIndex++);
                    if(open!=null & close!=null) {
                            hours       = open + " - " + close;
                    }
                    locEl.addElement("hours").addText(HandlerUtils.nullToEmpty(hours));
                    if(user > 0) {
                        stmt            = transconn.prepareStatement(selectSponsor);
                        stmt.setInt(1, locationId);
                        rsSponsor           =stmt.executeQuery();
                        if(rsSponsor.next()) {
                            int sponsor = rsSponsor.getInt(1);
                            int count   = rsSponsor.getInt(2);
                            locEl.addElement("passId").addText(String.valueOf(sponsor));
                            logger.debug("Sponsor:Present");
                            if(count > 0) {
                                stmt    = transconn.prepareStatement(selectSponsorMap);
                                stmt.setInt(1, sponsor);
                                stmt.setInt(2, user);
                                rsSponsorMap
                                        = stmt.executeQuery();
                                if(rsSponsorMap.next()) {
                                    int reward
                                        = rsSponsorMap.getInt(1);
                                    locEl.addElement("sponsor").addText("2");
                                } else {
                                    locEl.addElement("sponsor").addText("2");
                                }
                            }else {
                                locEl.addElement("sponsor").addText("1");
                            }
                        }else {
                            locEl.addElement("passId").addText(String.valueOf(0));
                            locEl.addElement("sponsor").addText("0");
                        }
                    }
                }
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } catch (Exception ex) {
            System.out.println("ERROR: " + ex.getClass().getName() + " " + ex.getMessage());
            ex.printStackTrace();
        } finally {
             close(rsSponsorMap);
             close(rsSponsor);
            close(rs);
            close(rsLat);
            close(stmt);
        }
    }

    private boolean hasNoActiveLines (int locationId) throws HandlerException {
        boolean hasNoLines                  = true;

        PreparedStatement stmt              = null;
        ResultSet rs                        = null;

        try {
            stmt                            = transconn.prepareStatement("SELECT l.id FROM line l LEFT JOIN bar b ON b.id = l.bar " +
                                            " WHERE l.status = 'RUNNING' AND l.product NOT IN(4311,10661) AND b.location = ?");
            stmt.setInt(1, locationId);
            rs                              = stmt.executeQuery();
            if (rs.next()) {
                hasNoLines                  = false;
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: "+sqle.toString());
            throw new HandlerException(sqle);
        } finally {
            close(rs);
            close(stmt);
        }
        return hasNoLines;
    }


     private void getConsumerBeerBoardMenu(Element toHandle, Element toAppend) throws HandlerException {

        int locationId                      = HandlerUtils.getRequiredInteger(toHandle, "locationId");
        
        PreparedStatement stmt              = null;
        ResultSet rs                        = null;
        
        String selectBeverageDescription    = "SELECT DISTINCT pD.category, pD.boardName, IF(pD.abv = 0.0, '', CONCAT(pD.abv,'%')), sPS.name FROM line l LEFT JOIN bar b ON b.id = l.bar " +
                                            " LEFT JOIN productSetMap sPSM ON sPSM.product = l.product LEFT JOIN productSet sPS ON sPS.id = sPSM.productSet " +
                                            " LEFT JOIN productDescription pD ON pD.product = l.product " +
                                            " WHERE l.status = 'RUNNING' AND b.location = ? AND sPS.productSetType = 9 ORDER BY pD.category, pD.boardName";
        
        try {

            stmt                            = transconn.prepareStatement(selectBeverageDescription);
            stmt.setInt(1, locationId);
            rs                              = stmt.executeQuery();
            int totalBeerMenus              = 0;
            while (rs.next()) {
                int rsIndex                 = 1;
                Element beerMenusEl         = toAppend.addElement("beerMenus");
                beerMenusEl.addElement("category").addText(String.valueOf(rs.getInt(rsIndex++)));
                beerMenusEl.addElement("boardName").addText(String.valueOf(rs.getString(rsIndex++)));
                beerMenusEl.addElement("abv").addText(String.valueOf(rs.getString(rsIndex++)));
                beerMenusEl.addElement("description").addText(String.valueOf(rs.getString(rsIndex++)));
                beerMenusEl.addElement("latitude").addText(String.valueOf(39.9044734));
                beerMenusEl.addElement("longitude").addText(String.valueOf(-75.1712574));
                totalBeerMenus++;
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: "+sqle.toString());
            throw new HandlerException(sqle);
        } finally {
            close(rs);
            close(stmt);
        }
    }


       private void getConsumerBeerBoardList(Element toHandle, Element toAppend) throws HandlerException {

        int locationId                      = HandlerUtils.getRequiredInteger(toHandle, "locationId");
        int forMobile                       = HandlerUtils.getOptionalInteger(toHandle, "forMobile");
        PreparedStatement stmt              = null;
        ResultSet rs                        = null, innerRS = null;

        String selectUserTVFeatures         = "SELECT id FROM locationBeerBoardMap WHERE location = ? ";
        String selectBeverageList           = "SELECT DISTINCT pD.category, IF(pD.category = 1, 'Domestic', IF(pD.category = 2, 'Craft', 'Import')), pD.boardName, IF(pD.abv = 0.0, '', CONCAT(pD.abv,'%')), sPS.name " +
                                            " FROM line l LEFT JOIN bar b ON b.id = l.bar LEFT JOIN productDescription pD ON pD.product = l.product " +
                                            " LEFT JOIN productSetMap sPSM ON sPSM.product = l.product LEFT JOIN productSet sPS ON sPS.id = sPSM.productSet" +
                                            " WHERE l.status = 'RUNNING' AND b.location = ? AND sPS.productSetType = 9 ORDER BY pD.category, pD.boardName; ";

        try {
            stmt                            = transconn.prepareStatement(selectUserTVFeatures);
            stmt.setInt(1, locationId);
            rs                              = stmt.executeQuery();
            if (rs.next()) {
                stmt                        = transconn.prepareStatement(selectBeverageList);
                stmt.setInt(1, locationId);
                innerRS                     = stmt.executeQuery();
                int totalBeerMenus          = 0;

                while (innerRS.next()) {
                    Element beerMenusEl     = toAppend.addElement("beerList");
                    beerMenusEl.addElement("category").addText(String.valueOf(innerRS.getInt(2)));
                    beerMenusEl.addElement("boardName").addText(String.valueOf(innerRS.getString(3)));
                    beerMenusEl.addElement("abv").addText(String.valueOf(innerRS.getString(4)));
                    beerMenusEl.addElement("description").addText(String.valueOf(innerRS.getString(5)));
                    totalBeerMenus++;
                }
            } else {
                logger.debug("Not a valid BeerBoard Location");
                addErrorDetail(toAppend, "Not a valid BeerBoard Location");
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: "+sqle.toString());
            throw new HandlerException(sqle);
        } finally {
            close(innerRS);
            close(rs);
            close(stmt);
        }
    }


    private void getBeerBoardMenu(Element toHandle, Element toAppend) throws HandlerException {

        int userId                          = HandlerUtils.getOptionalInteger(toHandle, "userId");
        int locationId                      = HandlerUtils.getOptionalInteger(toHandle, "locationId");
        int forMobile                       = HandlerUtils.getOptionalInteger(toHandle, "forMobile");
        int platform                        = HandlerUtils.getOptionalInteger(toHandle, "platform");
        int url                             = HandlerUtils.getOptionalInteger(toHandle, "url");
        int mobileCustomer                  = HandlerUtils.getOptionalInteger(toHandle, "customerId");
        int version                         = HandlerUtils.getOptionalInteger(toHandle, "version");
        
        PreparedStatement stmt              = null;
        ResultSet rs                        = null, innerRS = null;
        Calendar currentDate                = Calendar.getInstance();

        String selectBeerBoardMenuTemplate  = "SELECT template FROM locationBeerBoardMap WHERE location = ?; ";
        String selectLocationSpecials       = "SELECT id, specials, sequence FROM locationSpecials WHERE location = ?  ";
        String selectBeverageSize           = "SELECT name FROM beverageSize WHERE location = ? AND showOnTV = 1 ORDER BY ounces LIMIT 4; ";
        String selectFont                   = "SELECT DISTINCT(f.filename) FROM locationFontMap lf LEFT JOIN fonts f ON lf.font = f.id WHERE location = ?  ;";
        String selectCount                  = "SELECT COUNT(DISTINCT l.product) FROM line l LEFT JOIN bar b ON b.id = l.bar WHERE b.location = ? AND l.status = 'RUNNING'; ";
        String insertAccess                 = "INSERT INTO bbtvMobileUsage (location, user, lastAccess) VALUES (?, ?, ?);";
        String lastAccess                   = dbDateFormat.format(currentDate.getTime());

        try {
            if (userId > 0) {
                stmt                        = transconn.prepareStatement("SELECT location FROM userBeerBoardMap WHERE user = ?");
                stmt.setInt(1, userId);
                rs                          = stmt.executeQuery();
                if (rs.next()) {
                    locationId              = rs.getInt(1);
                }
            }

            int template                    = 2;
            stmt                            = transconn.prepareStatement(selectBeerBoardMenuTemplate);
            stmt.setInt(1, locationId);
            rs                              = stmt.executeQuery();
            if (rs.next()) {
                template                    = rs.getInt(1);
            }

            int itemCount                   = 0;
            stmt                            = transconn.prepareStatement(selectCount);
            stmt.setInt(1, locationId);
            rs                              = stmt.executeQuery();
            if (rs.next()) {
                itemCount                   = rs.getInt(1);
            }

            StringBuilder beerSize          = new StringBuilder("<table width='100%' height='100%' border='0' cellspacing='0' cellpadding='0'>");
            beerSize.append("<tr><td align='left' valign='bottom'><span class='beer_size'>Sizes: </span></td><td align='left' valign='bottom'>");

            StringBuilder beerSizesInternal = new StringBuilder("");
            stmt                            = transconn.prepareStatement(selectBeverageSize);
            stmt.setInt(1, locationId);
            rs                              = stmt.executeQuery();
            while (rs.next()) {
                beerSizesInternal.append(rs.getString(1));
                if (!rs.isLast()) {
                    beerSizesInternal.append(" | ");
                }
            }

            if ((forMobile == 1) && (beerSizesInternal.length() > 32)) {
               beerSize.append("<div id='fader'");
            }

            beerSize.append("<span class='beer_size'>");
            beerSize.append(beerSizesInternal);
            beerSize.append("</span>");

            if ((forMobile == 1) && (beerSizesInternal.length() > 32)) {
                beerSize.append("</div>");
                beerSize.append("<script type='text/javascript'> var fade_in_from = 0; var fade_out_from = 10; function fadeIn(element){	var target = document.getElementById(element); target.style.display = 'block';	var newSetting = fade_in_from / 10; target.style.opacity = newSetting;	fade_in_from++;");
                beerSize.append("if(fade_in_from == 10){	target.style.opacity = 1; clearTimeout(loopTimer);	fade_in_from = 0; fadeOut('fader');	return false; } var loopTimer = setTimeout('fadeIn(\\''+element+'\\')',500); }");
                beerSize.append("function fadeOut(element){	var target = document.getElementById(element);	var newSetting = fade_out_from / 10; target.style.opacity = newSetting;	fade_out_from--; if(fade_out_from == 0){");
                beerSize.append("target.style.opacity = 0; target.style.display = 'none'; clearTimeout(loopTimer); fade_out_from = 10; fadeIn('fader'); return false; } var loopTimer = setTimeout('fadeOut(\\''+element+'\\')',500); }");
                beerSize.append("</script><script type='text/javascript'> fadeOut('fader'); </script>");
            }

            beerSize.append("</td></tr></table>");

            ArrayList<String> beerBoardMenus= new ArrayList<String>();
            if (forMobile == 1|| platform == 7) {
                 if(platform == 2 || platform == 3|| platform == 7) {
                     if (itemCount >= 20) {
                         if(platform == 3){
                             beerBoardMenus         = getBeerBoardMobileMenuStyleTemplate(locationId, platform, url, toAppend);
                         } else {
                             if(version > 30){
                                 beerBoardMenus     = getBeerBoardMobileMenuStyleTemplate(locationId, platform, url, toAppend);
                             } else {
                                 beerBoardMenus     = getBeerBoardMobileMenuStyleTemplate(locationId, platform, url, toAppend);
                             }
                         }
                     } else {
                         beerBoardMenus             = getBeerBoardMobileMenuStyleTemplate(locationId, platform, url, toAppend);
                     }
                 } else {
                     beerBoardMenus         = getBeerBoardMenuMobileTemplate(locationId);
                 }
            } else {                
                switch (template) {
                    case 1:
                            beerBoardMenus  = getBeerBoardMenuTemplate(locationId, 3);
                        break;
                    case 2:
                            beerBoardMenus  = getBeerBoardMenuTwoColTemplate(locationId, 3);                            
                        break;
                    case 3:
                            beerBoardMenus  = getBeerBoardMenuTwoColTemplate(locationId, 2);
                        break;
                    case 4:
                            beerBoardMenus  = getBeerBoardMenuStyleTemplate(locationId, 3);
                        break;
                    default:
                        break;
                }
            }

            int totalBeerMenus              = beerBoardMenus.size();
            for (int i = 0; i < totalBeerMenus; i++) {
                String beerListString       = beerBoardMenus.get(i);
                if (forMobile == 1) {
                    beerListString.replaceAll("'45", "'12");
                    beerListString.replaceAll("'30", "'8");
                    beerListString.replaceAll("'20", "'7");
                    beerListString.replaceAll("'10", "'3");
                    beerListString.replaceAll("'10px", "'5px");
                }
                
                Element beerMenusEl         = toAppend.addElement("beerMenus");
                beerMenusEl.addElement("visibleTime").addText("10000");
                beerMenusEl.addElement("sequence").addText(HandlerUtils.nullToEmpty(String.valueOf(i + 1)));
                beerMenusEl.addElement("beerSize").addText(HandlerUtils.nullToEmpty(beerSize.toString()));
                beerMenusEl.addElement("beerMenu").addText(HandlerUtils.nullToEmpty(beerListString));
            }

            toAppend.addElement("pageCount").addText(HandlerUtils.nullToEmpty(String.valueOf(totalBeerMenus)));
            if(platform == 3) {
                //toAppend.addElement("css").addText(getBeerBoardMenuFont(3, locationId));
            } else {
                String fontString           = null;

                if (forMobile == 1)  {
                    fontString              = getBeerBoardMenuFont(2, locationId);
                } else {
                    switch (template) {
                        case 1:
                                fontString  = getBeerBoardMenuFont(1, locationId);
                            break;
                        case 2:
                                fontString  = getBeerBoardMenuFont(4, locationId);
                            break;
                        case 3:
                                fontString  = getBeerBoardMenuFont(5, locationId);
                            break;
                        case 4:
                                fontString  = getBeerBoardMenuFont(1, locationId);
                            break;
                        default:
                                fontString  = getBeerBoardMenuFont(1, locationId);
                            break;
                    }
                    toAppend.addElement("css").addText(HandlerUtils.nullToEmpty(fontString));
                }
                
            }

            if (forMobile == 1) {
                toAppend.addElement("downloadLogo").addText("http://beerboard.tv/USBN.BeerBoard.UI/Images/FBglass/");
            } else {

                toAppend.addElement("font").addText("1");
                String furl[]               = {"chalkdust.ttf","lcchalk_.ttf","patrick_hand.otf","PPETRIAL.otf"};
                for(int i=0;i<furl.length;i++) {
                    Element font            = toAppend.addElement("downloadFont");
                    font.addElement("url").addText("http://beerboard.tv/USBN.BeerBoard.UI/Fonts/"+furl[i]);
                }
                stmt                        = transconn.prepareStatement(selectFont);
                stmt.setInt(1, locationId);
                rs                          = stmt.executeQuery();
                while (rs.next()) {
                        Element font        = toAppend.addElement("downloadFont");
                        font.addElement("url").addText("http://beerboard.tv/USBN.BeerBoard.UI/Fonts/"+rs.getString(1).trim().replaceAll("\'", "%27").replaceAll(" ", "%20"));
                }
            }

            boolean hasNoSpecials           = true;
            String specialsFont             = "patrick_hand.otf";
            String specialsSize             = "45";

            stmt                            = transconn.prepareStatement("SELECT f.filename, lf.size FROM locationFontMap lf LEFT JOIN fonts f ON lf.font = f.id WHERE location = ? AND lf.type = 1  ;");
            stmt.setInt(1,locationId);
            rs                              = stmt.executeQuery();
            if (rs.next()) {
                specialsFont                = rs.getString(1);
                specialsSize                = rs.getString(2);
            }

            stmt                            = transconn.prepareStatement(selectLocationSpecials);
            stmt.setInt(1,locationId);
            rs                              = stmt.executeQuery();
            while (rs.next()) {
                hasNoSpecials               = false;
                Element locationSpecialsEl  = toAppend.addElement("locationSpecials");
                locationSpecialsEl.addElement("visibleTime").addText("10000");
                locationSpecialsEl.addElement("id").addText(HandlerUtils.nullToEmpty(rs.getString(1)));
                locationSpecialsEl.addElement("specials").addText(HandlerUtils.nullToEmpty(rs.getString(2)));
                locationSpecialsEl.addElement("sequence").addText(HandlerUtils.nullToEmpty(rs.getString(3)));
                locationSpecialsEl.addElement("font").addText(specialsFont);
                locationSpecialsEl.addElement("size").addText(specialsSize);
            }
            if(hasNoSpecials && platform == 3) {
                 Element locationSpecialsEl  = toAppend.addElement("locationSpecials");
                locationSpecialsEl.addElement("visibleTime").addText("10000");
                locationSpecialsEl.addElement("id").addText(String.valueOf(1));
                locationSpecialsEl.addElement("specials").addText("    .");
                locationSpecialsEl.addElement("sequence").addText(String.valueOf(1));
                locationSpecialsEl.addElement("font").addText(specialsFont);
                locationSpecialsEl.addElement("size").addText(String.valueOf(0));
                
            }

            if(mobileCustomer > 0) {
                String selectVisitCount     = "SELECT count FROM menuVisitCount WHERE user= ? AND location = ?;";
                
                stmt                        = transconn.prepareStatement(selectVisitCount);
                stmt.setInt(1, mobileCustomer);
                stmt.setInt(2, locationId);
                rs                          = stmt.executeQuery();
                if(rs.next()){
                    int count               = rs.getInt(1);
                    count++;
                    if((platform == 2 && count > 2) || (platform == 3 && count > 2)){
                        stmt                = transconn.prepareStatement("SELECT id FROM favoriteLocation WHERE user =? AND location =?;");
                        stmt.setInt(1, mobileCustomer);
                        stmt.setInt(2, locationId);
                        rs                  = stmt.executeQuery();
                        if(!rs.next()){
                             stmt           = transconn.prepareStatement("INSERT INTO favoriteLocation (user,location) VALUES(?, ?);");
                             stmt.setInt(1, mobileCustomer);
                             stmt.setInt(2, locationId);
                             stmt.executeUpdate();
                             count          = 0;
                        }
                    }
                    
                    stmt                    = transconn.prepareStatement("UPDATE menuVisitCount SET count= ? WHERE user = ? AND location = ?;");
                    stmt.setInt(1, count);
                    stmt.setInt(2, mobileCustomer);
                    stmt.setInt(3, locationId);
                    stmt.executeUpdate();
                    
                } else {
                    stmt                    = transconn.prepareStatement("INSERT INTO menuVisitCount (user,location,count) VALUES(?, ?, 1);");
                    stmt.setInt(1, mobileCustomer);
                    stmt.setInt(2, locationId);
                    stmt.executeUpdate();
                }
              
                stmt                        = transconn.prepareStatement(insertAccess);
                stmt.setInt(1, locationId);
                stmt.setInt(2, mobileCustomer);
                stmt.setString(3, lastAccess);
                stmt.executeUpdate();
            }
                
            getAdBanners(locationId, toAppend);
            
        } catch (SQLException sqle) {
            logger.dbError("Database error: "+sqle.toString());
            throw new HandlerException(sqle);
        } finally {
            close(innerRS);
            close(rs);
            close(stmt);
        }
    }

    private String getBeerBoardMenuFont(int type, int location) throws HandlerException {
        String css                          = "";

        switch (type) {
            case 1:
                css                         = "@charset 'utf-8'; " +
                                            " @font-face { font-family: 'ChalkDust'; src: url('file:///android_asset/fonts/chalkdust.ttf'); } " +
                                            " @font-face { font-family: 'PPETRIAL'; src: url('file:///android_asset/fonts/PPETRIAL.otf'); } " +
                                            " @font-face { font-family: 'patrick'; src: url('file:///android_asset/fonts/patrick_hand.otf'); } " +
                                            " * { color:#FFFFFF; } " +
                                            " body { background-image: url('../Images/USBN_back.gif'); background-position: left top; background-repeat: repeat; fieldset border-style:none; } " +
                                            " .beer_size { font-family: 'PPETRIAL'; font-size:30px; font-weight:bold; overflow:hidden;  white-space:nowrap; } " +
                                            " .beer_price { font-family: 'patrick'; font-size:20px; font-weight:normal; overflow:hidden;  white-space:nowrap; } " +
                                            " .beer_category { font-family: 'ChalkDust'; font-size:26px; font-weight:bold; color: #ff9900; overflow:hidden;  white-space:nowrap; } " +
                                            " .beer_name { font-family: 'PPETRIAL'; font-size:50px; overflow:hidden;  white-space:nowrap; } " +
                                            " .beer_big_name { font-family: 'patrick'; font-size:76px; overflow:hidden;  white-space:nowrap; } " +
                                            " .beer_description { font-family: 'PPETRIAL'; font-size:26px;  font-style:italic;  font-weight:normal; overflow:hidden;  white-space:nowrap; } " +
                                            " .beer_abv { font-family: 'patrick'; font-size:20px; margin-left:5px; font-weight:normal; } " +
                                            " .vendor_logo { float:left;  width:150px; margin-top:15px; margin-left:15px; } " +
                                            " .other_data_feed { float:left;  font-size:26px;  margin-top:25px; margin-left:25px; text-align: center; } " +
                                            " .sponsor_header { font-size:16px; }";
                break;
            case 2:
                css                         = "@charset 'utf-8'; " +
                                            " @font-face { font-family: 'ChalkDust'; src: url('file:///android_asset/fonts/chalkdust.ttf'); } " +
                                            " @font-face { font-family: 'PPETRIAL'; src: url('file:///android_asset/fonts/PPETRIAL.otf'); } " +
                                            " @font-face { font-family: 'patrick'; src: url('file:///android_asset/fonts/patrick_hand.otf'); } " +
                                            " * { color:#FFFFFF; } a {text-decoration: none}" +
                                            " body { background-image: url('../Images/USBN_back.gif'); background-position: left top; background-repeat: repeat; fieldset border-style:none; } " +
                                            " .beer_size { font-family: 'PPETRIAL'; font-size:18px; font-weight:bold; overflow:hidden;  white-space:nowrap; } " +
                                            " .beer_price { font-family: 'patrick'; font-size:18px; font-weight:normal; overflow:hidden;  white-space:nowrap; } " +
                                            " .beer_category { font-family: 'ChalkDust'; font-size:25px; font-weight:bold; color: #ff9900; overflow:hidden;  white-space:nowrap; } " +
                                            " .beer_name { font-family: 'PPETRIAL'; font-size:30px; overflow:hidden;  white-space:nowrap; } " +
                                            " .beer_big_name { font-family: 'PPETRIAL'; font-size:37px; overflow:hidden;  white-space:nowrap; } " +
                                            " .beer_description { font-family: 'PPETRIAL'; font-size:20px;  font-style:italic;  font-weight:normal; overflow:hidden;  white-space:nowrap; } " +
                                            " .beer_abv { font-family: 'patrick'; font-size:26px; margin-left:5px; font-weight:normal; } " +
                                            " .vendor_logo { float:left;  width:150px; margin-top:15px; margin-left:15px; } " +
                                            " .other_data_feed { float:left;  font-size:20px;  margin-top:25px; margin-left:25px; text-align: center; } " +
                                            " .sponsor_header { font-size:8px; }";
                break;
            case 3:
                css                         = "@charset 'utf-8'; " +
                                            " @font-face { font-family: 'ChalkDust'; src: url('chalkdust.ttf'); } " +
                                            " @font-face { font-family: 'PPETRIAL'; src: url('PPETRIAL.otf'); } " +
                                            " @font-face { font-family: 'patrick'; src: url('patrick_hand.otf'); } " +
                                            " * { color:#FFFFFF;} a {text-decoration: none} " +
                                            " body { background-image: url('USBN_back.gif'); background-position: left top; background-repeat: repeat; fieldset border-style:none; } " +
                                            " .beer_size { font-family: 'PPETRIAL'; font-size:18px; font-weight:bold; overflow:hidden;  white-space:nowrap; } " +
                                            " .beer_price { font-family: 'patrick'; font-size:18px; font-weight:normal; overflow:hidden;  white-space:nowrap; } " +
                                            " .beer_category { font-family: 'ChalkDust'; font-size:30px; font-weight:bold; color: #ff9900; overflow:hidden;  white-space:nowrap; } " +
                                            " .beer_name { font-family: 'PPETRIAL'; font-size:36px; overflow:hidden;  white-space:nowrap; } " +
                                            " .beer_big_name { font-family: 'PPETRIAL'; font-size:37px; overflow:hidden;  white-space:nowrap; } " +
                                            " .beer_description { font-family: 'PPETRIAL'; font-size:20px;  font-style:italic;  font-weight:normal; overflow:hidden;  white-space:nowrap; } " +
                                            " .beer_abv { font-family: 'patrick'; font-size:26px; margin-left:5px; font-weight:normal; } " +
                                            " .vendor_logo { float:left;  width:150px; margin-top:15px; margin-left:15px; } " +
                                            " .other_data_feed { float:left;  font-size:20px;  margin-top:25px; margin-left:25px; text-align: center; } " +
                                            " .sponsor_header { font-size:8px; }";
                break;
            case 4:
                css                         = "@charset 'utf-8'; " +
                                            " @font-face { font-family: 'ChalkDust'; src: url('file:///android_asset/fonts/chalkdust.ttf'); } " +
                                            " @font-face { font-family: 'PPETRIAL'; src: url('file:///android_asset/fonts/PPETRIAL.otf'); } " +
                                            " @font-face { font-family: 'patrick'; src: url('file:///android_asset/fonts/patrick_hand.otf'); } " +
                                            " * { color:#FFFFFF; } " +
                                            " body { background-image: url('../Images/USBN_back.gif'); background-position: left top; background-repeat: repeat; fieldset border-style:none; } " +
                                            " .beer_size { font-family: 'PPETRIAL'; font-size:30px; font-weight:bold; overflow:hidden;  white-space:nowrap; } " +
                                            " .beer_price { font-family: 'patrick'; font-size:20px; font-weight:normal; overflow:hidden;  white-space:nowrap; } " +
                                            " .beer_category { font-family: 'ChalkDust'; font-size:26px; font-weight:bold; color: #ff9900; overflow:hidden;  white-space:nowrap; } " +
                                            " .beer_name { font-family: 'PPETRIAL'; font-size:50px; overflow:hidden;  white-space:nowrap; } " +
                                            " .beer_big_name { font-family: 'patrick'; font-size:80px; overflow:hidden;  white-space:nowrap; } " +
                                            " .beer_description { font-family: 'PPETRIAL'; font-size:28px;  font-style:italic;  font-weight:normal; overflow:hidden;  white-space:nowrap; } " +
                                            " .beer_abv { font-family: 'patrick'; font-size:20px; margin-left:5px; font-weight:normal; } " +
                                            " .vendor_logo { float:left;  width:150px; margin-top:15px; margin-left:15px; } " +
                                            " .other_data_feed { float:left;  font-size:26px;  margin-top:25px; margin-left:25px; text-align: center; } " +
                                            " .sponsor_header { font-size:16px; }";
                break;
            case 5:
                css                         = "@charset 'utf-8'; " +
                                            " @font-face { font-family: 'categoryFont'; src: url('/mnt/sdcard/fonts/chalkdust.ttf'); } " +
                                            " @font-face { font-family: 'nameFont'; src: url('/mnt/sdcard/fonts/patrick_hand.otf'); } " +
                                            " @font-face { font-family: 'styleFont'; src: url('/mnt/sdcard/fonts/PPETRIAL.otf'); } " +
                                            " @font-face { font-family: 'abvFont'; src: url('/mnt/sdcard/fonts/patrick_hand.otf'); } " +
                                            " @font-face { font-family: 'sizeFont'; src: url('/mnt/sdcard/fonts/PPETRIAL.otf'); } " +
                                            " * { color:#FFFFFF; } " +
                                            " body { background-image: url('../Images/USBN_back.gif'); background-position: left top; background-repeat: repeat; fieldset border-style:none; } " +
                                            " .beer_size { font-family: 'sizeFont'; font-size:30px; font-weight:bold; overflow:hidden;  white-space:nowrap; } " +
                                            " .beer_price { font-family: 'sizeFont'; font-size:20px; font-weight:normal; overflow:hidden;  white-space:nowrap; } " +
                                            " .beer_category { font-family: 'categoryFont'; font-size:36px; font-weight:bold; color: #ff9900; overflow:hidden;  white-space:nowrap; } " +
                                            " .beer_name { font-family: 'PPETRIAL'; font-size:50px; overflow:hidden;  white-space:nowrap; } " +
                                            " .beer_big_name { font-family: 'nameFont'; font-size:110px; overflow:hidden;  white-space:nowrap; } " +
                                            " .beer_description { font-family: 'styleFont'; font-size:40px;  font-style:italic;  font-weight:normal; overflow:hidden;  white-space:nowrap; } " +
                                            " .beer_abv { font-family: 'patrick'; font-size:35px; margin-left:5px; font-weight:normal; } " +
                                            " .vendor_logo { float:left;  width:150px; margin-top:15px; margin-left:15px; } " +
                                            " .other_data_feed { float:left;  font-size:26px;  margin-top:25px; margin-left:25px; text-align: center; } " +
                                            " .sponsor_header { font-size:16px; }";
                break;
            case 6:

                    String selectType       = "SELECT  lf.type, f.filename, lf.size FROM locationFontMap lf LEFT JOIN fonts f ON lf.font = f.id WHERE location = ?  ; ";
                    PreparedStatement stmt  = null;
                    ResultSet rs            = null;
                    String categoryFont     = "chalkdust.ttf";
                    String nameFont         = "patrick_hand.otf";
                    String styleFont        = "PPETRIAL.otf";
                    String abvFont          = "patrick_hand.otf";
                    String sizeFont         = "PPETRIAL.otf";
                    int categorySize        = 36;
                    int nameSize            = 110;
                    int styleSize           = 40;
                    int abvSize             = 35;
                    int draftSize           = 30;
                /*try {

                    stmt                            = transconn.prepareStatement(selectType);
                    stmt.setInt(1,location);
                    rs                              = stmt.executeQuery();
                    while (rs.next()) {
                        int style                  = rs.getInt(1);
                        if(style == 2) {
                            categoryFont    = rs.getString(2);
                            categorySize    = rs.getInt(3);
                        }  else if(style == 3) {
                            nameFont    = rs.getString(2);
                            nameSize    = rs.getInt(3);
                        } else if(style == 4) {
                            styleFont    = rs.getString(2);
                            styleSize    = rs.getInt(3);
                        } else if(style == 5) {
                            abvFont    = rs.getString(2);
                            abvSize    = rs.getInt(3);
                        } else if(style == 6) {
                            sizeFont    = rs.getString(2);
                            draftSize    = rs.getInt(3);
                        }
                
                } catch (SQLException sqle) {
                   logger.dbError("Database error in getFonts: " + sqle.getMessage());
                   throw new HandlerException(sqle);
                } finally {
                   close(rs);
                   close(stmt);
                }*/
                css                         = "@charset 'utf-8'; " +
                                            " @font-face { font-family: 'categoryFont'; src: url('/mnt/sdcard/fonts/"+categoryFont+"'); } " +
                                            " @font-face { font-family: 'nameFont'; src: url('/mnt/sdcard/fonts/"+nameFont+"'); } " +
                                            " @font-face { font-family: 'styleFont'; src: url('/mnt/sdcard/fonts/"+styleFont+"'); } " +
                                            " @font-face { font-family: 'abvFont'; src: url('/mnt/sdcard/fonts/"+abvFont+"'); } " +
                                            " @font-face { font-family: 'sizeFont'; src: url('/mnt/sdcard/fonts/"+sizeFont+"'); } " +
                                            " * { color:#FFFFFF; } " +
                                            " body { background-image: url('../Images/USBN_back.gif'); background-position: left top; background-repeat: repeat; fieldset border-style:none; } " +
                                            " .beer_size { font-family: 'sizeFont'; font-size:"+draftSize+"px; font-weight:bold; overflow:hidden;  white-space:nowrap; } " +
                                            " .beer_price { font-family: 'sizeFont'; font-size:20px; font-weight:normal; overflow:hidden;  white-space:nowrap; } " +
                                            " .beer_category { font-family: 'categoryFont'; font-size:"+categorySize+"px; font-weight:bold; color: #ff9900; overflow:hidden;  white-space:nowrap; } " +
                                            " .beer_name { font-family: 'PPETRIAL'; font-size:50px; overflow:hidden;  white-space:nowrap; } " +
                                            " .beer_big_name { font-family: 'nameFont'; font-size:"+nameSize+"px; overflow:hidden;  white-space:nowrap; } " +
                                            " .beer_description { font-family: 'styleFont'; font-size:"+styleSize+"px;  font-style:italic;  font-weight:normal; overflow:hidden;  white-space:nowrap; } " +
                                            " .beer_abv { font-family: 'patrick'; font-size:"+abvSize+"px; margin-left:5px; font-weight:normal; } " +
                                            " .vendor_logo { float:left;  width:150px; margin-top:15px; margin-left:15px; } " +
                                            " .other_data_feed { float:left;  font-size:26px;  margin-top:25px; margin-left:25px; text-align: center; } " +
                                            " .sponsor_header { font-size:16px; }";
                break;
        }
        return css;
    }

    private ArrayList<String> getBeerBoardMenuTemplate(int location, int maxRow) throws HandlerException {


        ArrayList<String> beerBoardMenus    = new ArrayList<String>();
        PreparedStatement stmt              = null;
        ResultSet rs                        = null, innerRS = null;

        try {
            logger.debug("Loading Auto-Adjustment Template");
            StringBuilder beerMenuSize      = new StringBuilder();

            String selectBeverageSize       = "SELECT name FROM beverageSize WHERE location = ? AND showOnTV = 1 ORDER BY ounces; ";
            String selectBeverageDescription= "SELECT DISTINCT pD.category, pD.boardName, IF(pD.abv = 0.0, '', CONCAT(pD.abv,'%')), sPS.name FROM line l LEFT JOIN bar b ON b.id = l.bar " +
                                            " LEFT JOIN product p ON p.id = l.product LEFT JOIN productDescription pD ON pD.product = p.id " +
                                            " LEFT JOIN productSetMap sPSM ON sPSM.product = l.product LEFT JOIN productSet sPS ON sPS.id = sPSM.productSet " +
                                            " WHERE l.status = 'RUNNING' AND p.id NOT IN(4311,10661) AND b.location = ? AND sPS.productSetType = 9 ORDER BY pD.category, pD.boardName";
            beerMenuSize.append("<table width='100%' height='500' border='0' cellspacing='0' cellpadding='0'>");

            StringBuilder beerSize          = new StringBuilder("<tr><td colspan='6' align='left' valign='top'><span class='beer_size'>Draft Sizes:</span> ");
            stmt                            = transconn.prepareStatement(selectBeverageSize);
            stmt.setInt(1, location);
            rs                              = stmt.executeQuery();
            while (rs.next()) {
                beerSize.append("<span class='beer_size'>");
                beerSize.append(rs.getString(1));
                if (!rs.isLast()) {
                    beerSize.append(" | ");
                }
                beerSize.append("</span> ");
            }
            beerSize.append("</td></tr>");
            beerMenuSize.append(beerSize);

            StringBuilder prodDesc          = new StringBuilder();
            StringBuilder beerMenu          = new StringBuilder("<table height='100%' width='100%' valign='top'>");
            StringBuilder productCraft      = new StringBuilder("<tr valign='top'><td colspan='6' align='center'><span class='beer_category'>Craft</span></td></tr>");
            StringBuilder productImport     = new StringBuilder("<tr valign='top'><td colspan='6' align='center'><span class='beer_category'>Import</span></td></tr>");
            StringBuilder productDomest     = new StringBuilder("<tr valign='top'><td colspan='6' align='center'><span class='beer_category'>Domestic</span></td></tr>");

            int colCount                    = 0, rowCount = 0, domesticCount = 0, importCount = 0, craftCount = 0;
            stmt                            = transconn.prepareStatement(selectBeverageDescription);
            stmt.setInt(1, location);
            rs                              = stmt.executeQuery();
            while (rs.next()) {
                int prodCategory            = rs.getInt(1);
                String productName          = rs.getString(2);

                switch(prodCategory) {
                    case 1:

                        if (domesticCount == 0) {
                            beerMenu.append(productDomest);
                        }
                        if(colCount == 0) {
                            beerMenu.append("<tr><td height='45' width='32%' align='left' valign='bottom'><span class='beer_big_name'>");
                            beerMenu.append(productName);
                            beerMenu.append("</span></td>");

                            prodDesc.append("<tr><td height='30' align='left' valign='bottom'><span class='beer_description'>");
                            prodDesc.append(rs.getString(4));
                            prodDesc.append("</span> <span class='beer_abv'>");
                            prodDesc.append(rs.getString(3));
                            prodDesc.append("</span></td>");
                            colCount++;
                        } else if(colCount == 1) {
                            beerMenu.append("<td width='2%' align='left' valign='bottom'></td><td height='45' width='32%' align='left' valign='bottom'><span class='beer_big_name'>");
                            beerMenu.append(productName);
                            beerMenu.append("</span></td>");

                            prodDesc.append("<td align='left' valign='bottom'></td><td height='30' align='left' valign='bottom'><span class='beer_description'>");
                            prodDesc.append(rs.getString(4));
                            prodDesc.append("</span> <span class='beer_abv'>");
                            prodDesc.append(rs.getString(3));
                            prodDesc.append("</span></td>");
                            colCount++;
                        } else {
                            beerMenu.append("<td width='2%' align='left' valign='bottom'></td><td height='45' width='32%' align='left' valign='bottom'><span class='beer_big_name'>");
                            beerMenu.append(productName);
                            beerMenu.append("</span></td></tr>");

                            beerMenu.append(prodDesc);
                            beerMenu.append("<td align='left' valign='bottom'></td><td height='30' align='left' valign='bottom'><span class='beer_description'>");
                            beerMenu.append(rs.getString(4));
                            beerMenu.append("</span> <span class='beer_abv'>");
                            beerMenu.append(rs.getString(3));
                            beerMenu.append("</span></td></tr>");

                            prodDesc        = new StringBuilder();
                            beerMenu.append("<tr><td height='20px' colspan='6'></td></tr>");

                            rowCount++;
                            colCount        = 0;
                        }
                        domesticCount++;

                        if (rowCount == maxRow) {
                            for (int i = 0; i < (3 - maxRow); i++) {
                                beerMenu.append("<tr><td height='20px' colspan='6'></td></tr>");
                            }
                            beerMenu.append("</table>");
                            beerBoardMenus  = loadBeerBoardMenu(beerBoardMenus, beerMenuSize, beerMenu);
                            beerMenu        = new StringBuilder();
                            rowCount        = 0;
                            domesticCount   = 0;
                        }
                        break;
                    case 2:

                        if (craftCount == 0) {
                            if ((domesticCount % 3) > 0) {
                                beerMenu    = fillOverflowMenu(domesticCount, beerMenu, prodDesc);
                                rowCount++;

                                if (rowCount > (maxRow - 1)) {
                                    beerMenu.append("<tr><td height='45' width='32%' align='left' valign='bottom'><span class='beer_big_name'></span></td></tr>");
                                    beerMenu.append("<tr><td height='30' align='left' valign='bottom'><span class='beer_description'></span></td></tr>");
                                    beerMenu.append("</table>");
                                    beerBoardMenus
                                            = loadBeerBoardMenu(beerBoardMenus, beerMenuSize, beerMenu);
                                    beerMenu= new StringBuilder();
                                    rowCount= 0;
                                }
                            }

                            prodDesc        = new StringBuilder();
                            colCount        = 0;
                            domesticCount   = 0;
                            beerMenu.append(productCraft);
                        }

                        if(colCount == 0) {
                            beerMenu.append("<tr><td height='45' width='32%' align='left' valign='bottom'><span class='beer_big_name'>");
                            beerMenu.append(productName);
                            beerMenu.append("</span></td>");

                            prodDesc.append("<tr><td height='30' align='left' valign='bottom'><span class='beer_description'>");
                            prodDesc.append(rs.getString(4));
                            prodDesc.append("</span> <span class='beer_abv'>");
                            prodDesc.append(rs.getString(3));
                            prodDesc.append("</span></td>");
                            colCount++;
                        } else if(colCount == 1) {
                            beerMenu.append("<td width='2%' align='left' valign='bottom'></td><td height='45' width='32%' align='left' valign='bottom'><span class='beer_big_name'>");
                            beerMenu.append(productName);
                            beerMenu.append("</span></td>");

                            prodDesc.append("<td align='left' valign='bottom'></td><td height='30' align='left' valign='bottom'><span class='beer_description'>");
                            prodDesc.append(rs.getString(4));
                            prodDesc.append("</span> <span class='beer_abv'>");
                            prodDesc.append(rs.getString(3));
                            prodDesc.append("</span></td>");
                            colCount++;
                        } else {
                            beerMenu.append("<td width='2%' align='left' valign='bottom'></td><td height='45' width='32%' align='left' valign='bottom'><span class='beer_big_name'>");
                            beerMenu.append(productName);
                            beerMenu.append("</span></td></tr>");

                            beerMenu.append(prodDesc);
                            beerMenu.append("<td align='left' valign='bottom'></td><td height='30' align='left' valign='bottom'><span class='beer_description'>");
                            beerMenu.append(rs.getString(4));
                            beerMenu.append("</span> <span class='beer_abv'>");
                            beerMenu.append(rs.getString(3));
                            beerMenu.append("</span></td></tr>");

                            prodDesc        = new StringBuilder();
                            beerMenu.append("<tr><td height='20px' colspan='6'></td></tr>");

                            rowCount++;
                            colCount        = 0;
                        }
                        craftCount++;

                        if (rowCount == maxRow) {
                            for (int i = 0; i < (3 - maxRow); i++) {
                                beerMenu.append("<tr><td height='20px' colspan='6'></td></tr>");
                            }
                            beerMenu.append("</table>");
                            beerBoardMenus  = loadBeerBoardMenu(beerBoardMenus, beerMenuSize, beerMenu);
                            beerMenu        = new StringBuilder();
                            rowCount        = 0;
                            craftCount      = 0;
                        }
                        break;
                    case 3:

                        if (importCount == 0) {
                            if ((craftCount % 3) > 0) {
                                beerMenu    = fillOverflowMenu(craftCount, beerMenu, prodDesc);
                                rowCount++;

                                if (rowCount > (maxRow - 1)) {
                                    beerMenu.append("<tr><td height='45' width='32%' align='left' valign='bottom'><span class='beer_big_name'></span></td></tr>");
                                    beerMenu.append("<tr><td height='30' align='left' valign='bottom'><span class='beer_description'></span></td></tr>");
                                    beerMenu.append("</table>");
                                    beerBoardMenus
                                            = loadBeerBoardMenu(beerBoardMenus, beerMenuSize, beerMenu);
                                    beerMenu= new StringBuilder();
                                    rowCount= 0;
                                }
                            }
                            
                            prodDesc        = new StringBuilder();
                            colCount        = 0;
                            craftCount      = 0;
                            beerMenu.append(productImport);
                        }

                        if(colCount == 0) {
                            beerMenu.append("<tr><td height='45' width='32%' align='left' valign='bottom'><span class='beer_big_name'>");
                            beerMenu.append(productName);
                            beerMenu.append("</span></td>");

                            prodDesc.append("<tr><td height='30' align='left' valign='bottom'><span class='beer_description'>");
                            prodDesc.append(rs.getString(4));
                            prodDesc.append("</span> <span class='beer_abv'>");
                            prodDesc.append(rs.getString(3));
                            prodDesc.append("</span></td>");
                            colCount++;
                        } else if(colCount == 1) {
                            beerMenu.append("<td width='2%' align='left' valign='bottom'></td><td height='45' width='32%' align='left' valign='bottom'><span class='beer_big_name'>");
                            beerMenu.append(productName);
                            beerMenu.append("</span></td>");

                            prodDesc.append("<td align='left' valign='bottom'></td><td height='30' align='left' valign='bottom'><span class='beer_description'>");
                            prodDesc.append(rs.getString(4));
                            prodDesc.append("</span> <span class='beer_abv'>");
                            prodDesc.append(rs.getString(3));
                            prodDesc.append("</span></td>");
                            colCount++;
                        } else {
                            beerMenu.append("<td width='2%' align='left' valign='bottom'></td><td height='45' width='32%' align='left' valign='bottom'><span class='beer_big_name'>");
                            beerMenu.append(productName);
                            beerMenu.append("</span></td></tr>");

                            beerMenu.append(prodDesc);
                            beerMenu.append("<td align='left' valign='bottom'></td><td height='30' align='left' valign='bottom'><span class='beer_description'>");
                            beerMenu.append(rs.getString(4));
                            beerMenu.append("</span> <span class='beer_abv'>");
                            beerMenu.append(rs.getString(3));
                            beerMenu.append("</span></td></tr>");

                            prodDesc        = new StringBuilder();
                            beerMenu.append("<tr><td height='20px' colspan='6'></td></tr>");

                            rowCount++;
                            colCount        = 0;
                        }
                        importCount++;

                        if (rowCount == maxRow) {
                            for (int i = 0; i < (3 - maxRow); i++) {
                                beerMenu.append("<tr><td height='20px' colspan='6'></td></tr>");
                            }
                            beerMenu.append("</table>");
                            beerBoardMenus  = loadBeerBoardMenu(beerBoardMenus, beerMenuSize, beerMenu);
                            beerMenu        = new StringBuilder();
                            rowCount        = 0;
                            importCount     = 0;
                        }
                        break;
                }
            }

            if ((importCount % 3) > 0) {
                beerMenu    = fillOverflowMenu(importCount, beerMenu, prodDesc);
                rowCount++;

                if (rowCount > (maxRow - 1)) {
                    beerMenu.append("<tr><td height='45' width='32%' align='left' valign='bottom'><span class='beer_big_name'></span></td></tr>");
                    beerMenu.append("<tr><td height='30' align='left' valign='bottom'><span class='beer_description'></span></td></tr>");
                    beerMenu.append("</table>");
                    beerBoardMenus
                            = loadBeerBoardMenu(beerBoardMenus, beerMenuSize, beerMenu);
                    beerMenu= new StringBuilder();
                    rowCount= 0;
                }
            }

            if ((craftCount % 3) > 0) {
                beerMenu    = fillOverflowMenu(craftCount, beerMenu, prodDesc);
                rowCount++;

                if (rowCount > (maxRow - 1)) {
                    beerMenu.append("<tr><td height='45' width='32%' align='left' valign='bottom'><span class='beer_big_name'></span></td></tr>");
                    beerMenu.append("<tr><td height='30' align='left' valign='bottom'><span class='beer_description'></span></td></tr>");
                    beerMenu.append("</table>");
                    beerBoardMenus
                            = loadBeerBoardMenu(beerBoardMenus, beerMenuSize, beerMenu);
                    beerMenu= new StringBuilder();
                    rowCount= 0;
                }
            }

            if ((domesticCount % 3) > 0) {
                beerMenu    = fillOverflowMenu(importCount, beerMenu, prodDesc);
                rowCount++;

                if (rowCount > (maxRow - 1)) {
                    beerMenu.append("<tr><td height='45' width='32%' align='left' valign='bottom'><span class='beer_big_name'></span></td></tr>");
                    beerMenu.append("<tr><td height='30' align='left' valign='bottom'><span class='beer_description'></span></td></tr>");
                    beerMenu.append("</table>");
                    beerBoardMenus
                            = loadBeerBoardMenu(beerBoardMenus, beerMenuSize, beerMenu);
                    beerMenu= new StringBuilder();
                    rowCount= 0;
                }
            }

            if (rowCount > 0) {
                beerMenu.append("<tr><td height='45' width='32%' align='left' valign='bottom'><span class='beer_big_name'>&nbsp;</span></td></tr>");
                beerMenu.append("<tr><td height='30' align='left' valign='bottom'><span class='beer_description'>&nbsp;</span></td></tr>");
                beerMenu.append("</table>");
                beerBoardMenus              = loadBeerBoardMenu(beerBoardMenus, beerMenuSize, beerMenu);
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: "+sqle.toString());
            throw new HandlerException(sqle);
        } finally {
            close(innerRS);
            close(rs);
            close(stmt);
        }
        return beerBoardMenus;
    }

    private ArrayList<String> getBeerBoardMenuTwoColTemplate(int location, int maxRow) throws HandlerException {

        ArrayList<String> beerBoardMenus    = new ArrayList<String>();
        PreparedStatement stmt              = null;
        ResultSet rs                        = null, innerRS = null;

        try {
            logger.debug("Loading Auto-Adjustment Template");
            

            String selectBeverageSize       = "SELECT name FROM beverageSize WHERE location = ? AND showOnTV = 1 ORDER BY ounces; ";
            String selectBeverageDescription= "SELECT DISTINCT pD.category, pD.boardName, IF(pD.abv = 0.0, '', CONCAT(pD.abv,'%')), sPS.name FROM line l LEFT JOIN bar b ON b.id = l.bar " +
                                            " LEFT JOIN product p ON p.id = l.product LEFT JOIN productDescription pD ON pD.product = p.id " +
                                            " LEFT JOIN productSetMap sPSM ON sPSM.product = l.product LEFT JOIN productSet sPS ON sPS.id = sPSM.productSet " +
                                            " WHERE l.status = 'RUNNING' AND p.id NOT IN(4311,10661) AND b.location = ? AND sPS.productSetType = 9 ORDER BY pD.category, pD.boardName";

            StringBuilder beerMenuSize      = new StringBuilder("<table width='100%' height='500' border='0' cellspacing='0' cellpadding='0'>");
            StringBuilder beerSize          = new StringBuilder("<tr><td colspan='3' align='left' valign='top'><span class='beer_size'>Draft Sizes:</span> ");
            stmt                            = transconn.prepareStatement(selectBeverageSize);
            stmt.setInt(1, location);
            rs                              = stmt.executeQuery();
            while (rs.next()) {
                beerSize.append("<span class='beer_size'>");
                beerSize.append(rs.getString(1));
                if (!rs.isLast()) {
                    beerSize.append(" | ");
                }
                beerSize.append("</span> ");
            }
            beerSize.append("</td></tr>");
            beerMenuSize.append(beerSize);

            StringBuilder prodDesc          = new StringBuilder();
            StringBuilder beerMenu          = new StringBuilder("<table height='100%' width='100%' valign='top'>");
            StringBuilder productCraft      = new StringBuilder();
            StringBuilder productImport     = new StringBuilder();
            StringBuilder productDomest     = new StringBuilder();

            if (maxRow == 3) {
                productCraft.append("<tr valign='top'><td colspan='3' align='center'><span class='beer_category'>Craft</span></td></tr>");
                productImport.append("<tr valign='top'><td colspan='3' align='center'><span class='beer_category'>Import</span></td></tr>");
                productDomest.append("<tr valign='top'><td colspan='3' align='center'><span class='beer_category'>Domestic</span></td></tr>");
            }

            int colCount                    = 0, rowCount = 0, domesticCount = 0, importCount = 0, craftCount = 0;
            stmt                            = transconn.prepareStatement(selectBeverageDescription);
            stmt.setInt(1, location);
            rs                              = stmt.executeQuery();
            while (rs.next()) {
                int prodCategory            = rs.getInt(1);
                String productName          = rs.getString(2);

                switch(prodCategory) {
                    case 1:

                        if (domesticCount == 0) {
                            beerMenu.append(productDomest);
                        }
                        if(colCount == 0) {
                            beerMenu.append("<tr><td height='45' width='32%' align='left' valign='bottom'><span class='beer_big_name'>");
                            beerMenu.append(productName);
                            beerMenu.append("</span></td>");

                            prodDesc.append("<tr><td height='30' align='left' valign='bottom'><span class='beer_description'>");
                            prodDesc.append(rs.getString(4));
                            prodDesc.append("</span> <span class='beer_abv'>");
                            prodDesc.append(rs.getString(3));
                            prodDesc.append("</span></td>");
                            colCount++;
                        } else {
                            beerMenu.append("<td width='2%' align='left' valign='bottom'></td><td height='45' width='32%' align='left' valign='bottom'><span class='beer_big_name'>");
                            beerMenu.append(productName);
                            beerMenu.append("</span></td></tr>");

                            beerMenu.append(prodDesc);
                            beerMenu.append("<td align='left' valign='bottom'></td><td height='30' align='left' valign='bottom'><span class='beer_description'>");
                            beerMenu.append(rs.getString(4));
                            beerMenu.append("</span> <span class='beer_abv'>");
                            beerMenu.append(rs.getString(3));
                            beerMenu.append("</span></td></tr>");

                            prodDesc        = new StringBuilder();
                            beerMenu.append("<tr><td height='20px' colspan='6'></td></tr>");

                            rowCount++;
                            colCount        = 0;
                        }
                        domesticCount++;

                        if (rowCount == maxRow) {
                            for (int i = 0; i < (3 - maxRow); i++) {
                                beerMenu.append("<tr><td height='20px' colspan='6'></td></tr>");
                            }
                            beerMenu.append("</table>");
                            beerBoardMenus  = loadBeerBoardMenu(beerBoardMenus, beerMenuSize, beerMenu);
                            beerMenu        = new StringBuilder();
                            rowCount        = 0;
                            domesticCount   = 0;
                        }
                        break;
                    case 2:

                        if (craftCount == 0) {
                            if ((domesticCount % 2) > 0) {
                                beerMenu    = fillOverflowMenu(2, beerMenu, prodDesc);
                                rowCount    = (rowCount == 0 ? 2 : rowCount + 1);
                            }

                            if (rowCount > (maxRow - 2)) {
                                beerMenu.append("<tr><td height='45' width='32%' align='left' valign='bottom'><span class='beer_big_name'></span></td></tr>");
                                beerMenu.append("<tr><td height='30' align='left' valign='bottom'><span class='beer_description'></span></td></tr>");
                                beerMenu.append("</table>");
                                beerBoardMenus
                                            = loadBeerBoardMenu(beerBoardMenus, beerMenuSize, beerMenu);
                                beerMenu    = new StringBuilder();
                                rowCount    = 0;
                            } else if (rowCount == 1) {
                                rowCount++;
                            }

                            prodDesc        = new StringBuilder();
                            colCount        = 0;
                            domesticCount   = 0;
                            beerMenu.append(productCraft);
                        }

                        if(colCount == 0) {
                            beerMenu.append("<tr><td height='45' width='32%' align='left' valign='bottom'><span class='beer_big_name'>");
                            beerMenu.append(productName);
                            beerMenu.append("</span></td>");

                            prodDesc.append("<tr><td height='30' align='left' valign='bottom'><span class='beer_description'>");
                            prodDesc.append(rs.getString(4));
                            prodDesc.append("</span> <span class='beer_abv'>");
                            prodDesc.append(rs.getString(3));
                            prodDesc.append("</span></td>");
                            colCount++;
                        } else {
                            beerMenu.append("<td width='2%' align='left' valign='bottom'></td><td height='45' width='32%' align='left' valign='bottom'><span class='beer_big_name'>");
                            beerMenu.append(productName);
                            beerMenu.append("</span></td></tr>");

                            beerMenu.append(prodDesc);
                            beerMenu.append("<td align='left' valign='bottom'></td><td height='30' align='left' valign='bottom'><span class='beer_description'>");
                            beerMenu.append(rs.getString(4));
                            beerMenu.append("</span> <span class='beer_abv'>");
                            beerMenu.append(rs.getString(3));
                            beerMenu.append("</span></td></tr>");

                            prodDesc        = new StringBuilder();
                            beerMenu.append("<tr><td height='20px' colspan='6'></td></tr>");


                            rowCount++;
                            colCount        = 0;
                        }
                        craftCount++;

                        if (rowCount == maxRow) {
                            for (int i = 0; i < (3 - maxRow); i++) {
                                beerMenu.append("<tr><td height='20px' colspan='6'></td></tr>");
                            }
                            beerMenu.append("</table>");
                            beerBoardMenus  = loadBeerBoardMenu(beerBoardMenus, beerMenuSize, beerMenu);
                            beerMenu        = new StringBuilder();
                            rowCount        = 0;
                            craftCount      = 0;
                        }
                        break;
                    case 3:

                        if (importCount == 0) {
                            if ((craftCount % 2) > 0) {
                                beerMenu    = fillOverflowMenu(2, beerMenu, prodDesc);
                                rowCount    = (rowCount == 0 ? 2 : rowCount + 1);
                            }

                            if (rowCount > (maxRow - 2)) {
                                beerMenu.append("<tr><td height='45' width='32%' align='left' valign='bottom'><span class='beer_big_name'></span></td></tr>");
                                beerMenu.append("<tr><td height='30' align='left' valign='bottom'><span class='beer_description'></span></td></tr>");
                                beerMenu.append("</table>");
                                beerBoardMenus
                                            = loadBeerBoardMenu(beerBoardMenus, beerMenuSize, beerMenu);
                                beerMenu    = new StringBuilder();
                                rowCount    = 0;
                            } else if (rowCount == 1) {
                                rowCount++;
                            }

                            prodDesc        = new StringBuilder();
                            colCount        = 0;
                            craftCount      = 0;
                            beerMenu.append(productImport);
                        }

                        if(colCount == 0) {
                            beerMenu.append("<tr><td height='45' width='32%' align='left' valign='bottom'><span class='beer_big_name'>");
                            beerMenu.append(productName);
                            beerMenu.append("</span></td>");

                            prodDesc.append("<tr><td height='30' align='left' valign='bottom'><span class='beer_description'>");
                            prodDesc.append(rs.getString(4));
                            prodDesc.append("</span> <span class='beer_abv'>");
                            prodDesc.append(rs.getString(3));
                            prodDesc.append("</span></td>");
                            colCount++;
                        } else {
                            beerMenu.append("<td width='2%' align='left' valign='bottom'></td><td height='45' width='32%' align='left' valign='bottom'><span class='beer_big_name'>");
                            beerMenu.append(productName);
                            beerMenu.append("</span></td></tr>");

                            beerMenu.append(prodDesc);
                            beerMenu.append("<td align='left' valign='bottom'></td><td height='30' align='left' valign='bottom'><span class='beer_description'>");
                            beerMenu.append(rs.getString(4));
                            beerMenu.append("</span> <span class='beer_abv'>");
                            beerMenu.append(rs.getString(3));
                            beerMenu.append("</span></td></tr>");

                            prodDesc        = new StringBuilder();
                            beerMenu.append("<tr><td height='20px' colspan='6'></td></tr>");

                            rowCount++;
                            colCount        = 0;
                        }
                        importCount++;

                        if (rowCount == maxRow) {
                            for (int i = 0; i < (3 - maxRow); i++) {
                                beerMenu.append("<tr><td height='20px' colspan='6'></td></tr>");
                            }
                            beerMenu.append("</table>");
                            beerBoardMenus  = loadBeerBoardMenu(beerBoardMenus, beerMenuSize, beerMenu);
                            beerMenu        = new StringBuilder();
                            rowCount        = 0;
                            importCount     = 0;
                        }
                        break;
                }
            }

            if ((importCount % 2) > 0) {
                beerMenu    = fillOverflowMenu(2, beerMenu, prodDesc);
                rowCount++;
            }

            if ((craftCount % 2) > 0) {
                beerMenu    = fillOverflowMenu(2, beerMenu, prodDesc);
                rowCount++;
            }

            if ((domesticCount % 2) > 0) {
                beerMenu    = fillOverflowMenu(2, beerMenu, prodDesc);
                rowCount++;
            }

            if (rowCount > 0) {
                beerMenu.append("<tr><td height='45' width='32%' align='left' valign='bottom'><span class='beer_big_name'>&nbsp;</span></td></tr>");
                beerMenu.append("<tr><td height='30' align='left' valign='bottom'><span class='beer_description'>&nbsp;</span></td></tr>");
                beerMenu.append("</table>");
                beerBoardMenus              = loadBeerBoardMenu(beerBoardMenus, beerMenuSize, beerMenu);
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: "+sqle.toString());
            throw new HandlerException(sqle);
        } finally {
            close(innerRS);
            close(rs);
            close(stmt);
        }
        return beerBoardMenus;
    }

    private StringBuilder fillOverflowMenu(int beerCount, StringBuilder beerMenu, StringBuilder prodDesc) {

        if (beerCount % 3 == 1) {
            beerMenu.append("<td /><td /><td /><td /></tr>");
            beerMenu.append(prodDesc);
            beerMenu.append("<td /><td /><td /><td /></tr>");
        } else if (beerCount % 3 == 2) {
            beerMenu.append("<td /><td /></tr>");
            beerMenu.append(prodDesc);
            beerMenu.append("<td /><td /></tr>");
        }
        return beerMenu;
    }
    
    
    private ArrayList<String> getBeerBoardMenuStyleTemplate(int location, int maxRow) throws HandlerException {

        ArrayList<String> beerBoardMenus    = new ArrayList<String>();
        PreparedStatement stmt              = null;
        ResultSet rs                        = null, innerRS = null;

        try {
            logger.debug("Loading Auto-Adjustment Style Template");

            String selectBeverageSize       = "SELECT name FROM beverageSize WHERE location = ? AND showOnTV = 1 ORDER BY ounces; ";
            String selectBeverageDescription= "SELECT DISTINCT pD.category, pD.boardName, IF(pD.abv = 0.0, '', CONCAT(pD.abv,'%')), sPS.name FROM line l LEFT JOIN bar b ON b.id = l.bar " +
                                            " LEFT JOIN product p ON p.id = l.product LEFT JOIN productDescription pD ON pD.product = p.id " +
                                            " LEFT JOIN productSetMap sPSM ON sPSM.product = l.product LEFT JOIN productSet sPS ON sPS.id = sPSM.productSet " +
                                            " WHERE l.status = 'RUNNING' AND p.id NOT IN(4311,10661) AND b.location = ? AND sPS.productSetType = 9 ORDER BY sPS.name, pD.boardName";

            StringBuilder beerMenuSize      = new StringBuilder("<table width='100%' height='500' border='0' cellspacing='0' cellpadding='0'>");
            StringBuilder beerSize          = new StringBuilder("<tr><td colspan='3' align='left' valign='top'><span class='beer_size'>Draft Sizes:</span> ");
            stmt                            = transconn.prepareStatement(selectBeverageSize);
            stmt.setInt(1, location);
            rs                              = stmt.executeQuery();
            while (rs.next()) {
                beerSize.append("<span class='beer_size'>");
                beerSize.append(rs.getString(1));
                if (!rs.isLast()) {
                    beerSize.append(" | ");
                }
                beerSize.append("</span> ");
            }
            beerSize.append("</td></tr>");
            beerMenuSize.append(beerSize);

            StringBuilder prodDesc          = new StringBuilder();
            StringBuilder beerMenu          = new StringBuilder("<table height='100%' width='100%' valign='top'>");          
            int colCount                    = 0, rowCount = 0;
            String preStyle                 = null, currStyle = null;
            stmt                            = transconn.prepareStatement(selectBeverageDescription);
            stmt.setInt(1, location);
            rs                              = stmt.executeQuery();
            while (rs.next()) {
                String productName          = rs.getString(2);
                String abv                  = rs.getString(3);
                currStyle                   = rs.getString(4);
                if(preStyle==null) {
                    preStyle                = currStyle;
                    beerMenu.append("<tr valign='top'><td colspan='3' align='center'><span class='beer_category'>"+ currStyle +"</span></td></tr>");
                }
                if(!currStyle.equals(preStyle) || rowCount == maxRow) {
                    if(colCount==1) {
                        beerMenu.append("<td width='2%' align='left' valign='bottom'></td><td height='45' width='32%' align='left' valign='bottom'></td></tr>");
                        beerMenu.append(prodDesc);
                        prodDesc            = new StringBuilder();
                        beerMenu.append("<tr><td height='20px' colspan='6'></td></tr>");
                        rowCount++;
                        colCount            = 0;
                    }
                    if (rowCount == (maxRow - 2)) {
                        preStyle            = currStyle;
                        beerMenu.append("<tr valign='top'><td colspan='3' align='center'><span class='beer_category'>"+ currStyle +"</span></td></tr>");
                        colCount            = 0;
                        rowCount++;
                    } else {
                        if (rowCount == (maxRow - 1)) {
                            beerMenu.append("<tr valign='top'><td height='80' colspan='3' align='center'>&nbsp;</td></tr>");
                        }
                        beerMenu.append("</table>");
                        StringBuilder beerMenuList
                                            = new StringBuilder();
                        beerMenuList.append(beerMenuSize);
                        beerMenuList.append("<tr><td colspan='2' height='5px'></td></tr>");
                        beerMenuList.append("<tr><td colspan='2'>");
                        beerMenuList.append(beerMenu);
                        beerMenuList.append("</td></tr></table>");
                        beerBoardMenus      = loadBeerBoardMenu(beerBoardMenus, beerMenuSize, beerMenu);
                        beerMenu            = new StringBuilder("<table height='100%' width='100%' valign='top'>");
                        beerMenu.append("<tr valign='top'><td colspan='3' align='center'><span class='beer_category'>"+ currStyle +"</span></td></tr>");
                        rowCount            = 0;
                    }
                }
                if(colCount == 0) {
                    beerMenu.append("<tr><td height='45' width='32%' align='left' valign='bottom'><span class='beer_big_name'>");
                    beerMenu.append(productName);
                    beerMenu.append("</span></td>");
                    prodDesc.append("<tr><td height='30' align='left' valign='bottom'><span class='beer_description'>");
                    prodDesc.append("ABV: "+ abv);                 
                    prodDesc.append("</span></td>");
                    colCount++;
                } else {
                    beerMenu.append("<td width='2%' align='left' valign='bottom'></td><td height='45' width='32%' align='left' valign='bottom'><span class='beer_big_name'>");
                    beerMenu.append(productName);
                    beerMenu.append("</span></td></tr>");
                    beerMenu.append(prodDesc);
                    beerMenu.append("<td align='left' valign='bottom'></td><td height='30' align='left' valign='bottom'><span class='beer_description'>");
                    beerMenu.append("ABV: "+ abv);                
                    beerMenu.append("</span></td></tr>");
                    prodDesc        = new StringBuilder();
                    beerMenu.append("<tr><td height='20px' colspan='6'></td></tr>");
                    rowCount++;
                    colCount                = 0;
                }
                preStyle                    = currStyle;
            }

            int size                        = beerBoardMenus.size();
            for (int i = 0; i < size; i++) {
                beerBoardMenus.add(beerBoardMenus.get(i));
            }
            
            size                            = beerBoardMenus.size();
            for (int i = 0; i < size; i++) {
                beerBoardMenus.add(beerBoardMenus.get(i));
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: "+sqle.toString());
            throw new HandlerException(sqle);
        } finally {
            close(innerRS);
            close(rs);
            close(stmt);
        }
        return beerBoardMenus;
    }


     private ArrayList<String> getBeerBoardListTemplate(int location) throws HandlerException {

        ArrayList<String> beerBoardList     = new ArrayList<String>();
        StringBuilder beerList              = new StringBuilder();
        PreparedStatement stmt              = null;
        ResultSet rs                        = null;

        String selectBeverageList           = "SELECT DISTINCT pD.category, IF(pD.category = 1, 'Domestic', IF(pD.category = 2, 'Craft', 'Import')), pD.boardName, IF(pD.abv = 0.0, '', CONCAT('(', pD.abv,'%)')), sPS.name " +
                                            " FROM line l LEFT JOIN bar b ON b.id = l.bar LEFT JOIN productDescription pD ON pD.product = l.product " +
                                            " LEFT JOIN productSetMap sPSM ON sPSM.product = l.product LEFT JOIN productSet sPS ON sPS.id = sPSM.productSet " +
                                            " WHERE l.status = 'RUNNING' AND b.location = ? AND sPS.productSetType = 9 ORDER BY pD.category, pD.boardName; ";

        try {

            int productSet                  = -1;
            int count                       = 0;
            stmt                            = transconn.prepareStatement(selectBeverageList);
            stmt.setInt(1,location);
            rs                              = stmt.executeQuery();
            while (rs.next()) {
                String productStyle         = rs.getString(2);
                if (count == 0) {
                    beerList                = new StringBuilder();
                    beerList.append("<table width='100%' border='0'  style='padding-left: 15px;'>");
                    beerList.append("<tr><td align='left' valign='bottom'></td></tr>");
                }
                if (productSet != rs.getInt(1)) {
                    productSet              = rs.getInt(1);
                    beerList.append("<tr><td height='10px' align='left' valign='bottom'></td></tr>");
                    beerList.append("<tr><td align='center' valign='bottom'><span class='beer_style'>");
                    beerList.append(productStyle);
                    beerList.append("</span></td></tr>");
                    beerList.append("<tr><td height='5px' align='left' valign='bottom'></td></tr>");
                    count++;

                }
                beerList.append("<tr><td align='left' valign='middle' height='30px'><span class='beer_list_name'>");
                beerList.append(rs.getString(3));
                beerList.append("</span><br /><span class='beer_list_abv'>");
                beerList.append(rs.getString(4));
                beerList.append("</span></td></tr>");
                count++;

                if(count == 8) {
                    beerList.append("</table>");
                    beerBoardList.add(beerList.toString());
                    count                   = 0;
                    productSet              = -1;
                }
            }
            if(count > 0) {
                beerList.append("</table>");
                beerBoardList.add(beerList.toString());
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: "+sqle.toString());
            throw new HandlerException(sqle);
        } finally {
            close(rs);
            close(stmt);
        }
        return beerBoardList;
    }


    private ArrayList<String> loadBeerBoardMenu(ArrayList<String> beerBoardMenus, StringBuilder beerMenuSize, StringBuilder beerMenu) {
        StringBuilder beerMenuList          = new StringBuilder();

        beerMenuList.append(beerMenuSize);
        beerMenuList.append("<tr><td colspan='2' height='5px'></td></tr>");
        beerMenuList.append("<tr><td colspan='2'>");
        beerMenuList.append(beerMenu);
        beerMenuList.append("</td></tr></table>");
        beerBoardMenus.add(beerMenuList.toString());

        //logger.debug("Beer Menu: " + beerMenuList.toString());
        return beerBoardMenus;
    }


    private ArrayList<String> getBeerBoardMenuMobileTemplate(int location) throws HandlerException {

        ArrayList<String> beerBoardMenus    = new ArrayList<String>();
        PreparedStatement stmt              = null;
        ResultSet rs                        = null, innerRS = null;

        try {
            logger.debug("Loading Mobile Template");
            String selectBeverageDescription= "SELECT DISTINCT pD.category, p.name, IF(pD.abv = 0.0, '', CONCAT(pD.abv,'%')), sPS.name FROM line l LEFT JOIN bar b ON b.id = l.bar " +
                                            " LEFT JOIN product p ON p.id = l.product LEFT JOIN productDescription pD ON pD.product = p.id " +
                                            " LEFT JOIN productSetMap sPSM ON sPSM.product = l.product LEFT JOIN productSet sPS ON sPS.id = sPSM.productSet " +
                                            " WHERE l.status = 'RUNNING' AND p.id NOT IN(4311,10661)AND b.location = ? AND sPS.productSetType = 9 " +
                                            " GROUP BY p.id ORDER BY pD.category, p.name";

            StringBuilder beerMenu          = new StringBuilder("<table width='100%' height='100%' border='0' cellspacing='0' cellpadding='0'>");
            StringBuilder productCraft      = new StringBuilder("<table width='100%'><tr><td colspan='3' align='center'><span class='beer_category'>Craft</span></td></tr>");
            StringBuilder productImport     = new StringBuilder("<table width='100%'><tr><td colspan='3' align='center'><span class='beer_category'>Import</span></td></tr>");
            StringBuilder productDomest     = new StringBuilder("<table width='100%'><tr><td colspan='3' align='center'><span class='beer_category'>Domestic</span></td></tr>");
            StringBuilder productHomeBrew   = new StringBuilder("<table width='100%'><tr><td colspan='3' align='center'><span class='beer_category'>Home Brew</span></td></tr>");

            int domesticCount               = 0, importCount = 0, craftCount = 0, homeBrewCount = 0;
            stmt                            = transconn.prepareStatement(selectBeverageDescription);
            stmt.setInt(1, location);
            rs                              = stmt.executeQuery();
            while (rs.next()) {
                String productName          = rs.getString(2);
                switch(rs.getInt(1)) {
                    case 1:
                        productDomest.append("<tr><td height='45' align='center' valign='bottom'><span class='beer_name'>");
                        productDomest.append(productName);
                        productDomest.append("</span></td></tr>");
                        productDomest.append("<tr><td height='30' align='center' valign='bottom'><span class='beer_description'>");
                        productDomest.append(rs.getString(4));
                        productDomest.append("</span> <span class='beer_abv'>");
                        productDomest.append(rs.getString(3));
                        productDomest.append("</span></td></tr>");
                        if (!rs.isLast()) {
                                productDomest.append("<tr><td height='20px' colspan='2'></td></tr>");
                        }
                        domesticCount++;
                        break;
                    case 2:
                        productCraft.append("<tr><td  height='45' align='center' valign='bottom'><span class='beer_name'>");
                        productCraft.append(productName);
                        productCraft.append("</span></td></tr>");
                        productCraft.append("<tr><td height='30' align='center' valign='bottom'><span class='beer_description'>");
                        productCraft.append(rs.getString(4));
                        productCraft.append("</span> <span class='beer_abv'>");
                        productCraft.append(rs.getString(3));
                        productCraft.append("</span></td></tr>");
                        if (!rs.isLast()) {
                            productCraft.append("<tr><td height='20px' colspan='2'></td></tr>");
                        }
                        craftCount++;
                        break;
                    case 3:
                        productImport.append("<tr><td  height='45' align='center' valign='bottom'><span class='beer_name'>");
                        productImport.append(productName);
                        productImport.append("</span></td></tr>");
                        productImport.append("<tr><td height='30' align='center' valign='bottom'><span class='beer_description'>");
                        productImport.append(rs.getString(4));
                        productImport.append("</span> <span class='beer_abv'>");
                        productImport.append(rs.getString(3));
                        productImport.append("</span></td></tr>");
                        if (!rs.isLast()) {
                            productImport.append("<tr><td height='20px' colspan='2'></td></tr>");
                        }
                        importCount++;
                        break;
                    case 4:
                        productHomeBrew.append("<tr><td  height='45' align='center' valign='bottom'><span class='beer_name'>");
                        productHomeBrew.append(productName);
                        productHomeBrew.append("</span></td></tr>");
                        productHomeBrew.append("<tr><td height='30' align='center' valign='bottom'><span class='beer_description'>");
                        productHomeBrew.append(rs.getString(4));
                        productHomeBrew.append("</span> <span class='beer_abv'>");
                        productHomeBrew.append(rs.getString(3));
                        productHomeBrew.append("</span></td></tr>");
                        if (!rs.isLast()) {
                            productHomeBrew.append("<tr><td height='20px' colspan='2'></td></tr>");
                        }
                        homeBrewCount++;
                        break;
                }
            }

            productDomest.append("</table>");
            productImport.append("</table>");
            productCraft.append("</table>");
            productHomeBrew.append("</table>");

            beerMenu.append("<tr><td height='5px'></td></tr>");
            if (craftCount > 0) {
                beerMenu.append("<tr><td>");
                beerMenu.append(productCraft);
                beerMenu.append("</td></tr>");
            }
            if (homeBrewCount > 0) {
                beerMenu.append("<tr><td>");
                beerMenu.append(productHomeBrew);
                beerMenu.append("</td></tr>");
            }
            if (importCount > 0) {
                beerMenu.append("<tr><td>");
                beerMenu.append(productImport);
                beerMenu.append("</td></tr>");
            }
            if (domesticCount > 0) {
                beerMenu.append("<tr><td>");
                beerMenu.append(productDomest);
                beerMenu.append("</td></tr>");
            }
            beerMenu.append("</table>");
            
            beerBoardMenus.add(beerMenu.toString());
        } catch (SQLException sqle) {
            logger.dbError("Database error: "+sqle.toString());
            throw new HandlerException(sqle);
        } finally {
            close(innerRS);
            close(rs);
            close(stmt);
        }
        return beerBoardMenus;
    }


     private ArrayList<String> getBeerBoardMobileMenuCategoryTemplate(int location, int platform, int url, Element toAppend) throws HandlerException {

        ArrayList<String> beerBoardMenus    = new ArrayList<String>();
        ArrayList<String> product           = new ArrayList<String>();
        ArrayList<String> kegIcon           = new ArrayList<String>();
        ArrayList<String> type              = new ArrayList<String>();

        PreparedStatement stmt              = null;
        ResultSet rs                        = null, innerRS = null;

        Calendar calendar                   = Calendar.getInstance();
        int day                             = calendar.get(Calendar.DAY_OF_WEEK);
        String openHours                    = "";
        switch(day) {

            case 1:
                openHours                   ="lH.openSun, lH.closeSun";
                break;
            case 2:
                openHours                   ="lH.openMon, lH.closeMon";
                break;
            case 3:
                openHours                   ="lH.openTue, lH.closeTue";
                break;
            case 4:
                openHours                   ="lH.openWed, lH.closeWed";
                break;
            case 5:
                openHours                   ="lH.openThu, lH.closeThu";
                break;
            case 6:
                openHours                   ="lH.openFri, lH.closeFri";
                break;
            case 7:
                openHours                   ="lH.openSat, lH.closeSat";
                break;
        }

        try {
            logger.debug("Loading Mobile Template");
            String selectBeverageDescription= "SELECT DISTINCT pD.category, p.name, IF(pD.abv = 0.0, '', CONCAT(pD.abv,'%')), sPS.name, p.id, l.advertise, l.qtyOnHand * 100, " +
                                            " (SELECT k. file FROM kegIcon k WHERE (l.qtyOnHand*100.0 ) >= k.start AND  (l.qtyOnHand*100.0 ) <=k.end) FROM line l " +
                                            " LEFT JOIN bar b ON b.id = l.bar LEFT JOIN product p ON p.id = l.product LEFT JOIN productDescription pD ON pD.product = p.id " +
                                            " LEFT JOIN productSetMap sPSM ON sPSM.product = l.product LEFT JOIN productSet sPS ON sPS.id = sPSM.productSet " +
                                            " WHERE l.status = 'RUNNING' AND p.id NOT IN(4311,10661)  AND l.system != 2251 AND b.location = ? AND sPS.productSetType = 9 " +
                                            " GROUP BY p.id ORDER BY pD.category, p.name";
            String selectLogo               = "SELECT l.id, CONCAT(c.name, ' - ', l.name), l.latitude, l.longitude, l.easternOffset, IFNULL(REPLACE(lG.logo, ' ', '%20'), 'USBNLogo.png'), l.addrStreet, l.addrCity, l.addrState, l.addrZip, l.boardname, " +openHours +
                                            " FROM location l LEFT JOIN customer c ON c.id = l.customer LEFT JOIN locationGraphics lG ON lG.location = l.id "+
                                            " LEFT JOIN locationDetails lD ON lD.location = l.id LEFT JOIN locationHours lH ON lH.location =l.id WHERE lD.active = 1 AND l.type = 1  AND l.id = ? " +
                                            " ORDER BY l.addrState, c.name, l.name ASC ";

            StringBuilder beerMenu          = new StringBuilder("<table width='100%' height='100%' border='0' cellspacing='0' cellpadding='0'>");
            StringBuilder productCraft      = new StringBuilder("<table width='100%'><tr><td colspan='3' align='center'><span class='beer_category'>Craft</span></td></tr>");
            StringBuilder productImport     = new StringBuilder("<table width='100%'><tr><td colspan='3' align='center'><span class='beer_category'>Import</span></td></tr>");
            StringBuilder productDomest     = new StringBuilder("<table width='100%'><tr><td colspan='3' align='center'><span class='beer_category'>Domestic</span></td></tr>");
            StringBuilder productHomeBrew   = new StringBuilder("<table width='100%'><tr><td colspan='3' align='center'><span class='beer_category'>Home Brew</span></td></tr>");

            boolean craftVal                = false, domesticVal = false, importVal = false, homeBrewVal = false;
            stmt                            = transconn.prepareStatement(selectBeverageDescription);
            stmt.setInt(1, location);
            rs                              = stmt.executeQuery();
            while (rs.next()) {
                String productName          = rs.getString(2);
                String productId            = rs.getString(5);
                int advertise               = rs.getInt(6);
                String icon                 = "";
                if(advertise > 0) {
                    double qtyOnHnd         = rs.getDouble(7);
                    if(qtyOnHnd > 0){
                        icon                = HandlerUtils.nullToEmpty(rs.getString(8));
                    }
                }
                kegIcon.add(icon);
                
                switch(rs.getInt(1)) {
                    case 1:
                        domesticVal         = true;
                        productDomest.append("<tr><td height='45' align='center' valign='bottom'><a href=\""+productId+"\"><span class='beer_name'>");
                        productDomest.append(productName);
                        productDomest.append("</span></a></td></tr>");
                        productDomest.append("<tr><td height='30' align='center' valign='bottom'><span class='beer_description'>");
                        productDomest.append(rs.getString(4));
                        productDomest.append("</span> <span class='beer_abv'>");
                        productDomest.append(rs.getString(3));
                        productDomest.append("</span></td></tr>");
                        if (!rs.isLast()) {
                                productDomest.append("<tr><td height='20px' colspan='2'></td></tr>");
                        }
                        product.add(productId);
                        type.add("3");

                        break;
                    case 2:
                        craftVal            = true;
                        productCraft.append("<tr><td height='45' align='center' valign='bottom'><a href=\""+productId+"\"><span class='beer_name'>");
                        productCraft.append(productName);
                        productCraft.append("</span></a></td></tr>");
                        productCraft.append("<tr><td height='30' align='center' valign='bottom'><span class='beer_description'>");
                        productCraft.append(rs.getString(4));
                        productCraft.append("</span> <span class='beer_abv'>");
                        productCraft.append(rs.getString(3));
                        productCraft.append("</span></td></tr>");
                        if (!rs.isLast()) {
                            productCraft.append("<tr><td height='20px' colspan='2'></td></tr>");
                        }
                        product.add(productId);
                        type.add("1");
                        break;
                    case 3:
                        importVal           = true;
                        productImport.append("<tr><td height='45' align='center' valign='bottom'><a href=\""+productId+"\"><span class='beer_name'>");
                        productImport.append(productName);
                        productImport.append("</span></a></td></tr>");
                        productImport.append("<tr><td height='30' align='center' valign='bottom'><span class='beer_description'>");
                        productImport.append(rs.getString(4));
                        productImport.append("</span> <span class='beer_abv'>");
                        productImport.append(rs.getString(3));
                        productImport.append("</span></td></tr>");
                        if (!rs.isLast()) {
                            productImport.append("<tr><td height='20px' colspan='2'></td></tr>");
                        }
                        product.add(productId);
                        type.add("2");
                        break;
                    case 4:
                        homeBrewVal         = true;
                        productHomeBrew.append("<tr><td height='45' align='center' valign='bottom'><a href=\""+productId+"\"><span class='beer_name'>");
                        productHomeBrew.append(productName);
                        productHomeBrew.append("</span></a></td></tr>");
                        productHomeBrew.append("<tr><td height='30' align='center' valign='bottom'><span class='beer_description'>");
                        productHomeBrew.append(rs.getString(4));
                        productHomeBrew.append("</span> <span class='beer_abv'>");
                        productHomeBrew.append(rs.getString(3));
                        productHomeBrew.append("</span></td></tr>");
                        if (!rs.isLast()) {
                            productHomeBrew.append("<tr><td height='20px' colspan='2'></td></tr>");
                        }
                        product.add(productId);
                        type.add("4");
                        break;
                }
            }

            productDomest.append("</table>");
            productImport.append("</table>");
            productCraft.append("</table>");
            productHomeBrew.append("</table>");

            beerMenu.append("<tr><td height='5px'></td></tr>");
            if(craftVal) {
                beerMenu.append("<tr><td>");
                beerMenu.append(productCraft);
                beerMenu.append("</td></tr>");
                beerMenu.append("<tr><td height='20px' colspan='2'></td></tr>");
            }
            if(homeBrewVal) {
                beerMenu.append("<tr><td>");
                beerMenu.append(productHomeBrew);
                beerMenu.append("</td></tr>");
                beerMenu.append("<tr><td height='20px' colspan='2'></td></tr>");
            }
            if(importVal) {
                beerMenu.append("<tr><td>");
                beerMenu.append(productImport);
                beerMenu.append("</td></tr>");
                beerMenu.append("<tr><td height='20px' colspan='2'></td></tr>");
            }
            if(domesticVal) {
                beerMenu.append("<tr><td>");
                beerMenu.append(productDomest);
                beerMenu.append("</td></tr>");
            }
            beerMenu.append("</td></tr></table>");

            beerBoardMenus.add(beerMenu.toString());
            toAppend.addElement("orderCount").addText("4");
            Element orderEl                 = toAppend.addElement("order");
            orderEl.addElement("O1").addText("Craft");
            orderEl.addElement("O2").addText("Import");
            orderEl.addElement("O3").addText("Domestic");
            orderEl.addElement("O4").addText("Home");
            if(product.size() >0 && type.size()>0) {
                getNewMobileMenu(location, toAppend, product, type, kegIcon);
            }
            if(platform == 7) {
                stmt                        = transconn.prepareStatement(selectLogo);
                stmt.setInt(1, location);
                rs                          = stmt.executeQuery();
                if (rs != null && rs.next()) {
                    int rsIndex             = 1;
                    Element locEl           = toAppend.addElement("location");
                    locEl.addElement("locationId").addText(String.valueOf(rs.getInt(rsIndex++)));
                    locEl.addElement("locationName").addText(HandlerUtils.nullToEmpty((rs.getString(rsIndex++))));
                    locEl.addElement("latitude").addText(String.valueOf(rs.getString(rsIndex++)));
                    locEl.addElement("longitude").addText(String.valueOf(rs.getString(rsIndex++)));
                    locEl.addElement("easternOffset").addText(HandlerUtils.nullToEmpty((rs.getString(rsIndex++))));
                    locEl.addElement("logo").addText(HandlerUtils.nullToEmpty(("http://bevmanager.net/Images/Location_logo/" + rs.getString(rsIndex++).trim().replaceAll("\'", "%27").replaceAll(" ", "%20"))));
                    locEl.addElement("addrStreet").addText(HandlerUtils.nullToEmpty((rs.getString(rsIndex++))));
                    locEl.addElement("addrCity").addText(HandlerUtils.nullToEmpty((rs.getString(rsIndex++))));
                    locEl.addElement("addrState").addText(HandlerUtils.nullToEmpty((rs.getString(rsIndex++))));
                    locEl.addElement("addrZip").addText(HandlerUtils.nullToEmpty((rs.getString(rsIndex++))));
                    locEl.addElement("boardName").addText(HandlerUtils.nullToEmpty((rs.getString(rsIndex++))));
                    String hours            = "11:00 AM - 02:00 AM";
                    String open             = rs.getString(rsIndex++);
                    String close            = rs.getString(rsIndex++);
                    if(open!=null & close!=null) {
                        if( open.length()>16 && close.length()>16 ) {
                            close           = close.replace("00:","12:");
                            hours           = open.substring(11,16) +" AM - "+ close.substring(11,16)+" AM";
                        }
                    }
                    locEl.addElement("hours").addText(HandlerUtils.nullToEmpty(hours));
                }
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: "+sqle.toString());
            throw new HandlerException(sqle);
        } finally {
            close(innerRS);
            close(rs);
            close(stmt);
        }
        beerBoardMenus    = new ArrayList<String>();
        return beerBoardMenus;
    }
       
    private ArrayList<String> getBeerBoardMobileMenuStyleTemplate(int location, int platform, int url, Element toAppend) throws HandlerException {

        ArrayList<String> beerBoardMenus    = new ArrayList<String>();
        ArrayList<String> product           = new ArrayList<String>();
        ArrayList<String> kegIcon           = new ArrayList<String>();
        ArrayList<String> type              = new ArrayList<String>();
        ArrayList<String> style            = new ArrayList<String>();
        
        PreparedStatement stmt              = null;
        ResultSet rs                        = null;

        Calendar calendar                   = Calendar.getInstance();
        int day                             = calendar.get(Calendar.DAY_OF_WEEK);
        String openHours                    = "";
        switch(day) {

            case 1:
                openHours                   ="lH.openSun, lH.closeSun";
                break;
            case 2:
                openHours                   ="lH.openMon, lH.closeMon";
                break;
            case 3:
                openHours                   ="lH.openTue, lH.closeTue";
                break;
            case 4:
                openHours                   ="lH.openWed, lH.closeWed";
                break;
            case 5:
                openHours                   ="lH.openThu, lH.closeThu";
                break;
            case 6:
                openHours                   ="lH.openFri, lH.closeFri";
                break;
            case 7:
                openHours                   ="lH.openSat, lH.closeSat";
                break;
        }

        try {
            logger.debug("Loading Mobile Style Template");

            String selectBeverageDescription= "SELECT DISTINCT bS.id, p.id, bS.style, l.advertise,l.qtyOnHand*100,(SELECT k.file FROM kegIcon k " +
                                            " WHERE (l.qtyOnHand*100.0 ) >= k.start AND  (l.qtyOnHand*100.0 ) <=k.end )  FROM line l LEFT JOIN bar b ON b.id = l.bar " +
                                            " LEFT JOIN product p ON p.id = l.product LEFT JOIN productSetMap pSM ON pSM.product = l.product " +
                                            " LEFT JOIN productSet pS ON pS.id = pSM.productSet " +
                                            " LEFT JOIN beerStylesMap bSM ON bSM.productSet = pS.id LEFT JOIN beerStyles bS ON bS.id = bSM.style " +
                                            " WHERE l.status = 'RUNNING' AND p.id NOT IN(4311,10661) AND l.system != 2251 AND b.location = ? AND pS.productSetType = 9 AND bS.id<> 'null' " +
                                            " GROUP BY p.id ORDER BY bS.style, p.name;";
            String selectLogo               = "SELECT l.id, CONCAT(c.name, ' - ', l.name), l.latitude, l.longitude, l.easternOffset, IFNULL(REPLACE(lG.logo, ' ', '%20'), 'USBNLogo.png'), "
                                            + " l.addrStreet, l.addrCity, l.addrState, l.addrZip, l.boardname, " +openHours +
                                            " FROM location l LEFT JOIN customer c ON c.id = l.customer LEFT JOIN locationGraphics lG ON lG.location = l.id "+
                                            " LEFT JOIN locationDetails lD ON lD.location = l.id LEFT JOIN locationHours lH ON lH.location =l.id WHERE lD.active = 1 AND l.type = 1  AND l.id = ? " +
                                            " ORDER BY l.addrState, c.name, l.name ASC ";

            int orderCount                  = 0;           
            Element orderEl                 = toAppend.addElement("order");
            int productStyle                = -1;
            stmt                            = transconn.prepareStatement(selectBeverageDescription);
            stmt.setInt(1, location);
            rs                              = stmt.executeQuery();
            while (rs.next()) {
                String productId            = rs.getString(2);
                if (productStyle != rs.getInt(1)) {
                    productStyle            = rs.getInt(1);
                    if(platform == 2 || platform == 3 || platform == 7){  
                        if(!style.contains(rs.getString(3))) {
                            style.add(rs.getString(3));
                        }                       
                    } else {
                        orderEl.addElement("O" + String.format("%02d", rs.getInt(1))).addText(rs.getString(3));
                    }
                    orderCount++;
                }
                int advertise               = rs.getInt(4);
                String icon                 = "";
                if(advertise>0) {
                    double qtyOnHnd         = rs.getDouble(5);
                    if(qtyOnHnd > 0){
                        icon                = HandlerUtils.nullToEmpty(rs.getString(6));
                    }
                }
                kegIcon.add(icon);
                product.add(productId);
                type.add(String.valueOf(style.size()));
            }           
            orderCount                      = 0;
            for(int i=0; i<style.size(); i++){
                orderEl.addElement("O" +String.valueOf(i+1)).addText(style.get(i));
                orderCount++;                     
            }
            toAppend.addElement("orderCount").addText(String.valueOf(String.valueOf(orderCount)));
            if(product.size() >0 && type.size()>0) {
                getNewMobileMenu(location, toAppend, product, type, kegIcon);
            }
            if(platform == 7) {
                stmt                        = transconn.prepareStatement(selectLogo);
                stmt.setInt(1, location);
                rs                          = stmt.executeQuery();
                if (rs != null && rs.next()) {
                    int rsIndex             = 1;
                    Element locEl           = toAppend.addElement("location");
                    locEl.addElement("locationId").addText(String.valueOf(rs.getInt(rsIndex++)));
                    locEl.addElement("locationName").addText(HandlerUtils.nullToEmpty((rs.getString(rsIndex++))));
                    locEl.addElement("latitude").addText(String.valueOf(rs.getString(rsIndex++)));
                    locEl.addElement("longitude").addText(String.valueOf(rs.getString(rsIndex++)));
                    locEl.addElement("easternOffset").addText(HandlerUtils.nullToEmpty((rs.getString(rsIndex++))));
                    locEl.addElement("logo").addText(HandlerUtils.nullToEmpty(("http://bevmanager.net/Images/Location_logo/" + rs.getString(rsIndex++).trim().replaceAll("\'", "%27").replaceAll(" ", "%20"))));
                    locEl.addElement("addrStreet").addText(HandlerUtils.nullToEmpty((rs.getString(rsIndex++))));
                    locEl.addElement("addrCity").addText(HandlerUtils.nullToEmpty((rs.getString(rsIndex++))));
                    locEl.addElement("addrState").addText(HandlerUtils.nullToEmpty((rs.getString(rsIndex++))));
                    locEl.addElement("addrZip").addText(HandlerUtils.nullToEmpty((rs.getString(rsIndex++))));
                    locEl.addElement("boardName").addText(HandlerUtils.nullToEmpty((rs.getString(rsIndex++))));
                    String hours            = "11:00 AM - 02:00 AM";
                    String open             = rs.getString(rsIndex++);
                    String close            = rs.getString(rsIndex++);
                    if(open!=null & close!=null) {
                        if( open.length()>16 && close.length()>16 ) {
                            close           = close.replace("00:","12:");
                            hours           = open.substring(11,16) +" AM - "+ close.substring(11,16)+" AM";
                        }
                    }
                    locEl.addElement("hours").addText(HandlerUtils.nullToEmpty(hours));
                }
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: "+sqle.toString());
            throw new HandlerException(sqle);
        } finally {
            close(rs);
            close(stmt);
        }
        return beerBoardMenus;
    }

    private void getBeerBoardList(Element toHandle, Element toAppend) throws HandlerException {

        int userId                          = HandlerUtils.getOptionalInteger(toHandle, "userId");
        int locationId                      = HandlerUtils.getOptionalInteger(toHandle, "locationId");
        int forMobile                       = HandlerUtils.getOptionalInteger(toHandle, "forMobile");
        PreparedStatement stmt              = null;
        ResultSet rs                        = null;

        String selectUserTVFeatures         = "SELECT id FROM locationBeerBoardMap WHERE location = ? ";

        try {
            if (userId > 0) {
                stmt                        = transconn.prepareStatement("SELECT location FROM userBeerBoardMap WHERE user = ?");
                stmt.setInt(1, userId);
                rs                          = stmt.executeQuery();
                if (rs.next()) {
                    locationId              = rs.getInt(1);
                }
            }
            
            ArrayList<String> beerBoardLists= new ArrayList<String>();
            stmt                            = transconn.prepareStatement(selectUserTVFeatures);
            stmt.setInt(1, locationId);
            rs                              = stmt.executeQuery();
            if (rs.next()) {
                beerBoardLists              = getBeerBoardListTemplate(locationId);

                for (int i = 0; i < beerBoardLists.size(); i++) {
                    String beerListString   = beerBoardLists.get(i);
                    if (forMobile == 1) {
                        beerListString.replaceAll("15px", "0px");
                        beerListString.replaceAll("10px", "0px");
                        beerListString.replaceAll("5px", "0px");
                        beerListString.replaceAll("30px", "0px");
                    }
                    Element beerMenusEl     = toAppend.addElement("beerLists");
                    beerMenusEl.addElement("beerList").addText(HandlerUtils.nullToEmpty(beerListString));
                }
                toAppend.addElement("css").addText((forMobile == 1 ? getBeerBoardListFont(2) : getBeerBoardListFont(1)));
                toAppend.addElement("visibleTime").addText("10000");
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: "+sqle.toString());
            throw new HandlerException(sqle);
        } finally {
            close(rs);
            close(stmt);
        }
    }

    private String getBeerBoardListFont(int type) throws HandlerException {
        String css = "";
        switch (type) {
            case 1:
                css                         = "@charset 'utf-8'; " +
                                            " @font-face { font-family: 'ChalkDust'; src: url('file:///android_asset/fonts/chalkdust.ttf'); } " +
                                            " @font-face { font-family: 'PPETRIAL'; src: url('file:///android_asset/fonts/PPETRIAL.otf'); } " +
                                            " @font-face { font-family: 'patrick'; src: url('file:///android_asset/fonts/patrick_hand.otf'); } " +
                                            " .beer_list_name { font-family: 'patrick'; font-size:42px; overflow:hidden;  white-space:nowrap; color:white; } " +
                                            " .beer_list_abv { font-family: 'patrick'; font-size:28px; margin-left:5px; font-weight:normal; color:white; } " +
                                            " .beer_style { font-family: 'ChalkDust'; font-size:20px; font-weight:bold; color: #ff9900; overflow:hidden;  white-space:nowrap; color:white; }";
                break;
            case 2:
                css                         = "@charset 'utf-8'; " +
                                            " @font-face { font-family: 'ChalkDust'; src: url('file:///android_asset/fonts/chalkdust.ttf'); } " +
                                            " @font-face { font-family: 'PPETRIAL'; src: url('file:///android_asset/fonts/PPETRIAL.otf'); } " +
                                            " @font-face { font-family: 'patrick'; src: url('file:///android_asset/fonts/patrick_hand.otf'); } " +
                                            " .beer_list_name { font-family: 'PPETRIAL'; font-size:15px; overflow:hidden;  white-space:nowrap; color:white; } " +
                                            " .beer_list_abv { font-family: 'patrick'; font-size:11px; margin-left:5px; font-weight:normal; color:white; } " +
                                            " .beer_style { font-family: 'ChalkDust'; font-size:13px; font-weight:bold; color: #ff9900; overflow:hidden;  white-space:nowrap; color:white; }";
                break;
        }
        return css;
    }


     private void getLocationPromotions(Element toHandle, Element toAppend) throws HandlerException {

         int callerId                        = getCallerId(toHandle);
        int location                        = HandlerUtils.getRequiredInteger(toHandle, "locationId");
        PreparedStatement stmt              = null;
        ResultSet rs                        = null;

        String selectUserTVAccess           = "SELECT id FROM locationBeerBoardMap WHERE location = ? ";
        String selectLocationPromotions     = "SELECT id, type, visibleTime, file, sequence FROM locationPromotions WHERE location = ? ORDER BY sequence";

        try {
            stmt                            = transconn.prepareStatement(selectUserTVAccess);
            stmt.setInt(1, location);
            rs                              = stmt.executeQuery();
            if (rs.next()) {
                int count                   = 0;
                stmt                        = transconn.prepareStatement(selectLocationPromotions);
                stmt.setInt(1,location);
                rs                          = stmt.executeQuery();
                while (rs.next()) {

                    count++;
                    Element locationPromotionsEl
                                            = toAppend.addElement("locationPromotions");
                    locationPromotionsEl.addElement("id").addText(String.valueOf(rs.getInt(1)));
                    locationPromotionsEl.addElement("page").addText("2");
                    locationPromotionsEl.addElement("type").addText(String.valueOf(rs.getInt(2)));
                    locationPromotionsEl.addElement("visibleTime").addText(String.valueOf(rs.getInt(3)));
                    locationPromotionsEl.addElement("file").addText(HandlerUtils.nullToEmpty(rs.getString(4)));
                    locationPromotionsEl.addElement("filePath").addText(HandlerUtils.nullToEmpty("http://beerboard.tv/USBN.BeerBoard.UI/Images/Customers/"+ String.valueOf(location) + "/" + rs.getString(4).trim().replaceAll(" ", "%20")));
                    locationPromotionsEl.addElement("sequence").addText(String.valueOf(rs.getInt(5)));
                }
                toAppend.addElement("fileCount").addText(String.valueOf(count++));
                if(callerId >0 ){
                    logger.portalVisitDetail(callerId, "getLocationPromotions", location, "getLocationPromotions", 0,10, "", transconn);
                }
            } else {
                addErrorDetail(toAppend, "Beer Board does not have access for this location.");
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: "+sqle.toString());
            throw new HandlerException(sqle);
        } finally {
            close(rs);
            close(stmt);
        }
    }

    private void getLocationSponsors(Element toHandle, Element toAppend) throws HandlerException {

        int locationId                      = HandlerUtils.getRequiredInteger(toHandle, "locationId");
        PreparedStatement stmt              = null;
        ResultSet rs                        = null, innerRS = null;

        String selectUserTVAccess           = "SELECT lBM.externalMedia, l.latitude, l.longitude FROM locationBeerBoardMap lBM " +
                                            " LEFT JOIN location l ON l.id = lBM.location WHERE lBM.location = ?;";

        try {
            int count                       = 1;
            stmt                            = transconn.prepareStatement(selectUserTVAccess);
            stmt.setInt(1, locationId);
            rs                              = stmt.executeQuery();
            if (rs.next()) {
                if (rs.getInt(1) > 0) {
                    /*JSONArray jsonArray     = downloadFromVistar(location, 3, 872, 480, rs.getDouble(2), rs.getDouble(3));
                    for (int i = 0; i < jsonArray.length(); i++) {
                        count++;
                        JSONObject adv      = jsonArray.getJSONObject(i);

                        String asset_id     = adv.getString("asset_id") + ".mp4";
                        String asset_url    = adv.getString("asset_url");
                        //logger.debug("asset_id: " + asset_id);
                        //logger.debug("asset_url: " + asset_url);

                        String selectLocationSponsors
                                            = "SELECT id FROM locationSponsors WHERE location = ? AND file = ? ";
                        stmt                = transconn.prepareStatement(selectLocationSponsors);
                        stmt.setInt(1, location);
                        stmt.setString(2, asset_id);
                        rs                  = stmt.executeQuery();
                        if (!rs.next()) {
                            stmt            = transconn.prepareStatement("INSERT INTO locationSponsors (location, type, visibleTime, file, sequence) VALUES (?, ?, ?, ?, ?)");
                            stmt.setInt(1, location);
                            stmt.setInt(2, 1);
                            stmt.setInt(3, 30000);
                            stmt.setString(4, asset_id);
                            stmt.setInt(5, i);
                            stmt.executeUpdate();
                        }

                        if (!mediaName.contains(asset_id)) {
                            mediaName.add(asset_id);
                            Element locationSponsorsEl
                                            = toAppend.addElement("locationSponsors");
                            locationSponsorsEl.addElement("id").addText(adv.getString("asset_id"));


                            locationSponsorsEl.addElement("page").addText("2");
                            locationSponsorsEl.addElement("type").addText(String.valueOf("1"));
                            locationSponsorsEl.addElement("visibleTime").addText("30000");
                            locationSponsorsEl.addElement("file").addText(adv.getString("asset_id") + ".mp4");
                            locationSponsorsEl.addElement("filePath").addText(asset_url);
                            locationSponsorsEl.addElement("sequence").addText(String.valueOf(i));
                        }
                    }*/
                } else {

                    String selectLocationSponsors
                                            = "SELECT bSCC.id, bSCr.brewery, bSCr.product, bSC.customer, bSCr.file, bSCr.type FROM bevSyncCampaign bSC " +
                                            " LEFT JOIN bevSyncCampaignLocations bSCL ON bSCL.campaign = bSC.id " +
                                            " LEFT JOIN bevSyncCampaignCreatives bSCC ON bSCC.campaign = bSC.id LEFT JOIN bevSyncCreatives bSCr ON bSCr.id = bSCC.creatives " +
                                            " WHERE bSCL.location = ? AND NOW() BETWEEN bSC.start AND bSC.end AND NOW() < bSCr.validity AND bSCr.type IN (1, 2, 5);";

                    stmt                    = transconn.prepareStatement(selectLocationSponsors);
                    stmt.setInt(1,locationId);
                    rs                      = stmt.executeQuery();
                    while (rs.next()) {
                        int creativeId      = rs.getInt(1);
                        int brewery         = rs.getInt(2);
                        int product         = rs.getInt(3);
                        String customer     = rs.getString(4);
                        String creative     = rs.getString(5);
                        int creativeType    = rs.getInt(6);

                        if (product > 0) {
                            String selectLocationProducts
                                                = "SELECT l.id FROM line l LEFT JOIN bar b ON b.id = l.bar WHERE b.location = ? AND l.status = 'RUNNING' AND l.product = ?;";
                            stmt            = transconn.prepareStatement(selectLocationProducts);
                            stmt.setInt(1,locationId);
                            stmt.setInt(2,product);
                            innerRS         = stmt.executeQuery();
                            if (innerRS.next()) {
                                Element locationSponsorEl
                                            = toAppend.addElement("locationSponsors");
                                locationSponsorEl.addElement("id").addText(String.valueOf(creativeId));
                                locationSponsorEl.addElement("page").addText("2");
                                locationSponsorEl.addElement("type").addText(String.valueOf(creativeType - 1));
                                locationSponsorEl.addElement("visibleTime").addText(String.valueOf("30000"));
                                locationSponsorEl.addElement("file").addText(HandlerUtils.nullToEmpty(creative));
                                locationSponsorEl.addElement("filePath").addText(HandlerUtils.nullToEmpty("http://beerboard.tv/USBN.BeerBoard.UI/bevSyncCustomers/" + customer + "/" + creative.trim().replaceAll("\'", "%27").replaceAll(" ", "%20")));
                                locationSponsorEl.addElement("sequence").addText(String.valueOf(count++));
                            }
                        } else if (brewery > 0) {
                                String selectBreweryProducts
                                            = "SELECT l.id FROM line l LEFT JOIN bar b ON b.id = l.bar WHERE b.location = ? AND l.status = 'RUNNING' " +
                                            " AND l.product IN (SELECT p.id FROM product p LEFT JOIN productSetMap pSM ON pSM.product = p.id " +
                                            " LEFT JOIN productSet pS ON pS.id = pSM.productSet WHERE pS.productSetType = 7 AND p.id NOT IN(4311,10661) AND pSM.productSet = ?);";
                                stmt        = transconn.prepareStatement(selectBreweryProducts);
                                stmt.setInt(1,locationId);
                                stmt.setInt(2,brewery);
                                innerRS     = stmt.executeQuery();
                                if (innerRS.next()) {
                                    Element locationSponsorEl
                                            = toAppend.addElement("locationSponsors");
                                    locationSponsorEl.addElement("id").addText(String.valueOf(creativeId));
                                    locationSponsorEl.addElement("page").addText("2");
                                    locationSponsorEl.addElement("type").addText(String.valueOf(creativeType - 1));
                                    locationSponsorEl.addElement("visibleTime").addText(String.valueOf("30000"));
                                    locationSponsorEl.addElement("file").addText(HandlerUtils.nullToEmpty(creative));
                                    locationSponsorEl.addElement("filePath").addText(HandlerUtils.nullToEmpty("http://beerboard.tv/USBN.BeerBoard.UI/bevSyncCustomers/" + customer + "/" + creative.trim().replaceAll("\'", "%27").replaceAll(" ", "%20")));
                                    locationSponsorEl.addElement("sequence").addText(String.valueOf(count++));
                                }
                            }
                    }
                }
                toAppend.addElement("fileCount").addText(String.valueOf(count++));
            } else {
                addErrorDetail(toAppend, "Beer Board does not have access for this location.");
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: "+sqle.toString());
            throw new HandlerException(sqle);
        } catch (Exception e) {
           logger.dbError("getJson error: "+e.toString());
            throw new HandlerException(e);
       } finally {
            close(innerRS);
            close(rs);
            close(stmt);
        }
    }

   /*private JSONArray downloadFromVistar(int location, int totalVideos, int width, int height, double latitude, double longitude) throws HandlerException {

       long epoch                           = System.currentTimeMillis() / 1000;
       JSONArray jsonArray                  = new JSONArray();
       try {
           JSONObject obj                   = new JSONObject();
           obj.put("network_id", "63zkbes2T_Kaw-1n3GzcXg");
           obj.put("api_key","75ffc9fe-579b-412c-9a11-8d0d4611d993");
           obj.put("device_id","VistarDisplay0");
           obj.put("number_of_screens",new Integer(1));
           obj.put("venue_id", String.valueOf(location));
           obj.put("duration",new Integer(totalVideos * 30));
           obj.put("interval",new Integer(30));

           JSONArray dalist                 = new JSONArray();
           JSONObject da                    = new JSONObject();
           da.put("id", "display-area-1");
           da.put("width", new Integer(width));
           da.put("height", new Integer(height));

           JSONArray list                   = new JSONArray();
           list.put("application/x-shockwave-flash");
           list.put("video/mp4");

           da.put("supported_media", list);
           da.put("min_duration", 0);
           da.put("max_duration", new Integer(60));
           da.put("min_bitrate", 0);
           da.put("max_bitrate", 0);
           da.put("cpm_floor_cents", 0);
           da.put("allow_audio", true);
           dalist.put(da);
           obj.put("display_area",dalist);
           obj.put("latitude", latitude);
           obj.put("longitude", longitude);
           obj.put("display_time", epoch);
           obj.put("direct_connection",false);

           JSONStringer json                = new JSONStringer();

           if (obj!=null) {
               Iterator<String> itKeys      = obj.keys();
               if(itKeys.hasNext())
                   json.object();
               while (itKeys.hasNext()) {
                   String k                 = itKeys.next();
                   json.key(k).value(obj.get(k));
               }
           }
           json.endObject();
           String jsonValue                 = json.toString();
           //logger.debug("Request Json: " + jsonValue);
           URL url                          = new URL("http://staging.api.vistarmedia.com/api/v1/get_ad/json");
           HttpURLConnection conn           = (HttpURLConnection) url.openConnection();
           conn.setRequestMethod("POST");
           conn.setDoOutput(true);
           conn.setUseCaches(false);
           conn.setRequestProperty("Content-Type", "application/json");
           conn.setRequestProperty("Accept", "application/json");
           conn.setRequestProperty("Content-Length", Integer.toString(jsonValue.length()));
           conn.getOutputStream().write(jsonValue.getBytes());
           conn.getOutputStream().flush();
           conn.connect();

           //testHttpURLConnection(conn);

           if (conn.getResponseCode() != HttpURLConnection.HTTP_OK) {
               logger.debug("POST method failed: " + conn.getResponseCode() + "\t" + conn.getResponseMessage());
               logger.debug("getJson Fail");
           } else {
               InputStream responseContent  = (InputStream) conn.getContent();
               ByteArrayOutputStream buffer = new ByteArrayOutputStream();
               int nRead;
               byte[] data                  = new byte[16384];
               while ((nRead = responseContent.read(data, 0, data.length)) != -1) {
                   buffer.write(data, 0, nRead);
               }
               buffer.flush();
               byte responses[]             = buffer.toByteArray();
               String response              = new String(responses);
               //logger.debug(response);

               JSONObject resp              = new JSONObject(response);
               //logger.debug("resp length is: " + resp.length());
               if (resp.length() > 0) {
                   jsonArray                = new JSONArray(String.valueOf(resp.get("advertisement")));
                   //logger.debug("jsonArray length is: " + jsonArray.length());
               }
           }
       } catch (Exception e) {
           logger.dbError("getJson error: "+e.toString());
            throw new HandlerException(e);
       }
       return jsonArray;
   }

   private void dailyDownloadsFromVistar(Element toHandle, Element toAppend) throws HandlerException {

       int width                            = HandlerUtils.getRequiredInteger(toHandle, "width");
       int height                           = HandlerUtils.getRequiredInteger(toHandle, "height");
       int duration                         = HandlerUtils.getRequiredInteger(toHandle, "duration");
       int interval                         = HandlerUtils.getRequiredInteger(toHandle, "interval");
       long epoch                           = System.currentTimeMillis() / 1000;
       String selectBBTVLocation            = "SELECT DISTINCT l.id, ROUND(l.latitude, 4), ROUND(l.longitude, 4) FROM location l LEFT JOIN locationBeerBoardMap lBM ON lBM.location = l.id " +
                                            " WHERE lBM.externalMedia = 1; ";
       JSONArray jsonArray                  = new JSONArray();
       PreparedStatement stmt               = null;
       ResultSet rs                         = null;
       try {
           stmt                             = transconn.prepareStatement(selectBBTVLocation);
           rs                               = stmt.executeQuery();
           while (rs.next()) {
               int locationId               = rs.getInt(1);
               JSONObject obj               = new JSONObject();
               obj.put("network_id", "63zkbes2T_Kaw-1n3GzcXg");
               obj.put("api_key","75ffc9fe-579b-412c-9a11-8d0d4611d993");
               obj.put("device_id","VistarDisplay0");
               obj.put("number_of_screens",new Integer(1));
               obj.put("venue_id", String.valueOf(locationId));
               obj.put("duration",new Integer(duration));
               obj.put("interval",new Integer(interval));

               JSONArray dalist             = new JSONArray();
               JSONObject da                = new JSONObject();
               da.put("id", "display-area-1");
               da.put("width", new Integer(width));
               da.put("height", new Integer(height));

               JSONArray list               = new JSONArray();
               list.put("application/x-shockwave-flash");
               list.put("video/mp4");

               da.put("supported_media", list);
               da.put("min_duration", 0);
               da.put("max_duration", new Integer(60));
               da.put("min_bitrate", 0);
               da.put("max_bitrate", 0);
               da.put("cpm_floor_cents", 0);
               da.put("allow_audio", true);
               dalist.put(da);
               obj.put("display_area",dalist);
               obj.put("latitude", rs.getDouble(2));
               obj.put("longitude", rs.getDouble(3));
               obj.put("display_time", epoch);
               obj.put("direct_connection",false);

               JSONStringer json            = new JSONStringer();

               if (obj!=null) {
                   Iterator<String> itKeys  = obj.keys();
                   if(itKeys.hasNext())
                       json.object();
                   while (itKeys.hasNext()) {
                       String k             = itKeys.next();
                       json.key(k).value(obj.get(k));
                   }
               }
               json.endObject();
               String jsonValue             = json.toString();
               //logger.debug("Request Json: " + jsonValue);
               URL url                      = new URL("http://staging.api.vistarmedia.com/api/v1/get_ad/json");
               HttpURLConnection conn       = (HttpURLConnection) url.openConnection();
               conn.setRequestMethod("POST");
               conn.setDoOutput(true);
               conn.setUseCaches(false);
               conn.setRequestProperty("Content-Type", "application/json");
               conn.setRequestProperty("Accept", "application/json");
               conn.setRequestProperty("Content-Length", Integer.toString(jsonValue.length()));
               conn.getOutputStream().write(jsonValue.getBytes());
               conn.getOutputStream().flush();
               conn.connect();

               //testHttpURLConnection(conn);

               if (conn.getResponseCode() != HttpURLConnection.HTTP_OK) {
                   logger.debug("POST method failed: " + conn.getResponseCode() + "\t" + conn.getResponseMessage());
                   logger.debug("getJson Fail");
               } else {
                   InputStream responseContent
                                            = (InputStream) conn.getContent();
                   ByteArrayOutputStream buffer
                                            = new ByteArrayOutputStream();
                   int nRead;
                   byte[] data              = new byte[16384];
                   while ((nRead = responseContent.read(data, 0, data.length)) != -1) {
                       buffer.write(data, 0, nRead);
                   }
                   buffer.flush();
                   byte responses[]         = buffer.toByteArray();
                   String response          = new String(responses);
                   //logger.debug(response);

                   JSONObject resp          = new JSONObject(response);
                   //logger.debug("resp length is: " + resp.length());

                   if (resp.length() > 0) {
                       jsonArray            = new JSONArray(String.valueOf(resp.get("advertisement")));
                       for (int i = 0; i < jsonArray.length(); i++) {
                           JSONObject adv   = jsonArray.getJSONObject(i);
                           stmt             = transconn.prepareStatement("INSERT INTO vistarCreatives (location, file, pop) VALUES (?, ?, ?)");
                           stmt.setInt(1, locationId);
                           stmt.setString(2, adv.getString("asset_id") + ".mp4");
                           stmt.setString(3, adv.getString("proof_of_play_url"));
                           stmt.executeUpdate();
                       }
                   }
               }
           }
       } catch (SQLException sqle) {
           logger.dbError("Database error: "+sqle.toString());
           throw new HandlerException(sqle);
       } catch (Exception e) {
           logger.dbError("getJson error: "+e.toString());
           throw new HandlerException(e);
       } finally {
           close(rs);
           close(stmt);
       }
   }*/

    private void getBeerBoardUpdates(Element toHandle, Element toAppend) throws HandlerException {

        int location                        = HandlerUtils.getRequiredInteger(toHandle, "locationId");
        int bbtvUser                        = HandlerUtils.getOptionalInteger(toHandle, "userId");        
        String version                      = HandlerUtils.getOptionalString(toHandle, "version");
        String wmac                         = HandlerUtils.getOptionalString(toHandle, "wmac");
        String emac                         = HandlerUtils.getOptionalString(toHandle, "emac");
        PreparedStatement stmt              = null;
        ResultSet rs                        = null;
        Calendar currentDate                = Calendar.getInstance();

        String selectMACUserAccess          = "SELECT COUNT(id) FROM bbtvUserMac WHERE location = ? ";
        String selectUserTVAccess           = "SELECT logo, css, specials, promotions, sponsors, count, tempCount, marketing, feed FROM locationBeerBoardMap WHERE location = ? ";
        String updateUserTVAccess           = "UPDATE locationBeerBoardMap SET logo=?, css=?, specials=?, promotions=?, sponsors=?, tempCount = ?, marketing = ?, feed = ? WHERE location = ? ";
        String selectBBTVUserPing           = "SELECT lastPing FROM bbtvUserMac WHERE id = ? ;";        
        String updateBBTVUserPing           = "UPDATE bbtvUserMac SET lastPing = ?, version=? WHERE id = ?;";
        String updateBBTVPing               = "UPDATE beerboard SET lastPing = ?, version=? WHERE location = ?;";
        String insertInterruptionLogs       = "INSERT INTO bbtvInterruptionLogs (location, startTime, endTime, totalMinutes,user) VALUES (?,?,?,?,?)";
        String selectBBTV                   = "SELECT id, lastPing FROM beerboard WHERE location = ? and (wmac=? OR emac=?);";
        String insertBBTV                   = "INSERT INTO beerboard (name, location, wmac, emac, version) (SELECT name, id, ?, ?, ? FROM location WHERE id = ?)";
        String updateBBTV                   = "UPDATE beerboard SET lastPing = ?, wmac = ?, emac =?, version = ?  WHERE id = ?";        
        String updateBBTVUserPing1          = "UPDATE bbtvUserMac SET lastPing = ? WHERE location = ?;";
        String insertInterruptionLogs1      = "INSERT INTO bbtvInterruptionLogs (location, startTime, endTime, totalMinutes,emac,wmac) VALUES (?,?,?,?,?,?)";

        try {
            stmt                            = transconn.prepareStatement(selectMACUserAccess);
            stmt.setInt(1, location);
            rs                              = stmt.executeQuery();
            if (rs.next() && (rs.getInt(1) > 0)) {
                stmt                        = transconn.prepareStatement("UPDATE locationBeerBoardMap SET count = ? WHERE location = ?");
                stmt.setInt(1, rs.getInt(1));
                stmt.setInt(2, location);
                stmt.executeUpdate();
            }

            stmt                            = transconn.prepareStatement(selectUserTVAccess);
            stmt.setInt(1, location);
            rs                              = stmt.executeQuery();
            if (rs.next()) {
                int updateLogo              = rs.getInt(1);
                int updateCSS               = rs.getInt(2);
                int updateMenu              = rs.getInt(3);
                int updatePromotions        = rs.getInt(4);
                int updateSponsors          = rs.getInt(5);
                int count                   = rs.getInt(6);
                int tempCount               = rs.getInt(7);
                int updateMarketing         = rs.getInt(8);
                int updateFeed              = rs.getInt(9);

                toAppend.addElement("updateLogo").addText(String.valueOf(updateLogo));
                toAppend.addElement("updateCSS").addText(String.valueOf(updateCSS));
                toAppend.addElement("updateMenu").addText(String.valueOf(updateMenu));
                toAppend.addElement("updateList").addText(String.valueOf(updateMenu));
                toAppend.addElement("updatePromotions").addText(String.valueOf(updatePromotions));
                toAppend.addElement("updateSponsors").addText(String.valueOf(updateSponsors));
                toAppend.addElement("updateMarketing").addText(String.valueOf(updateMarketing));
                toAppend.addElement("updateFeed").addText(String.valueOf(updateFeed));

                if (updateLogo > 0 || updateCSS > 0 || updateMenu > 0 || updatePromotions > 0 || updateSponsors > 0 || updateMarketing > 0 || updateFeed > 0) {
                    if ((count - tempCount) == 1) {
                        stmt                = transconn.prepareStatement(updateUserTVAccess);
                        stmt.setInt(1, 0);
                        stmt.setInt(2, 0);
                        stmt.setInt(3, 0);
                        stmt.setInt(4, 0);
                        stmt.setInt(5, 0);
                        stmt.setInt(6, 0);
                        stmt.setInt(7, 0);
                        stmt.setInt(8, 0);
                        stmt.setInt(9, location);
                        stmt.executeUpdate();
                    } else {
                        stmt                = transconn.prepareStatement(updateUserTVAccess);
                        stmt.setInt(1, updateLogo);
                        stmt.setInt(2, updateCSS);
                        stmt.setInt(3, updateMenu);
                        stmt.setInt(4, updatePromotions);
                        stmt.setInt(5, updateSponsors);
                        stmt.setInt(6, tempCount + 1);
                        stmt.setInt(7, updateMarketing);
                        stmt.setInt(8, updateFeed);
                        stmt.setInt(9, location);
                        stmt.executeUpdate();
                    }
                }
            } else {
                addErrorDetail(toAppend, "Beer Board does not have access for this location.");
            }
            
            if (bbtvUser > 0) {
                String lastPing             = dbDateFormat.format(currentDate.getTime());
                stmt                        = transconn.prepareStatement(selectBBTVUserPing);
                stmt.setInt(1, bbtvUser);                
                rs                          = stmt.executeQuery();
                if (rs.next()) {              
                    long timeDiff           = (Math.abs(currentDate.getTime().getTime() - rs.getTimestamp(1).getTime()) / 1000) / 60;
                     if (timeDiff > 15 && timeDiff < 36000)
                     {
                        //logger.debug("Data Interruption found for system Location " + String.valueOf(location) + " : Interrupted by " + String.valueOf(timeDiff) + " minutes");
                         stmt               = transconn.prepareStatement(insertInterruptionLogs);
                         stmt.setInt(1, location);               
                         stmt.setTimestamp(2, rs.getTimestamp(1));
                         stmt.setTimestamp(3, new java.sql.Timestamp(currentDate.getTime().getTime()));
                         stmt.setLong(4, timeDiff);
                         stmt.setInt(5, bbtvUser);                         
                         stmt.executeUpdate();              
                     }

                    stmt                    = transconn.prepareStatement(updateBBTVUserPing);
                    stmt.setString(1, lastPing);
                    stmt.setString(2, version);
                    stmt.setInt(3, bbtvUser);
                    stmt.executeUpdate();
                }

                stmt                        = transconn.prepareStatement(updateBBTVPing);
                stmt.setString(1, lastPing);
                stmt.setString(2, version);
                stmt.setInt(3, location);
                stmt.executeUpdate();
            } else{
                if (wmac == null) {
                    wmac                        = "NA";
                }          
            if(emac == null) {
                emac                       = "NA";
            }

            if (!wmac.equals("00:0a:eb:c0:9e:bf") && !wmac.equals("48:02:2a:c7:c1:96") && !wmac.equals("48:02:2a:c7:c1:80") && !wmac.equals("48:02:2a:c7:c1:7e") && !wmac.equals("48:02:2a:c7:be:5e")) {
                stmt                        = transconn.prepareStatement(selectBBTV);
                stmt.setInt(1, location);
                stmt.setString(2, wmac);
                stmt.setString(3, emac);
                rs                          = stmt.executeQuery();
                if (rs.next()) {              
                    String lastPing         = dbDateFormat.format(currentDate.getTime());
                    long timeDiff                   = (Math.abs(currentDate.getTime().getTime() - rs.getTimestamp(2).getTime()) / 1000) / 60;
                     if (timeDiff > 15 && timeDiff < 36000)
                     {
                        //logger.debug("Data Interruption found for system Location " + String.valueOf(location) + " : Interrupted by " + String.valueOf(timeDiff) + " minutes");
                         stmt                        = transconn.prepareStatement(insertInterruptionLogs1);
                         stmt.setInt(1, location);               
                         stmt.setTimestamp(2, rs.getTimestamp(2));
                         stmt.setTimestamp(3, new java.sql.Timestamp(currentDate.getTime().getTime()));
                         stmt.setLong(4, timeDiff);
                         stmt.setString(5, emac);
                         stmt.setString(6, wmac);
                         stmt.executeUpdate();              
                     }
                    
                     stmt                    = transconn.prepareStatement(updateBBTV);  
                     stmt.setString(1, lastPing);
                     stmt.setString(2, wmac);
                     stmt.setString(3, emac);
                     stmt.setString(4, version);
                     stmt.setInt(5, rs.getInt(1));
                     stmt.executeUpdate();
                     
                     stmt                    = transconn.prepareStatement(updateBBTVUserPing1);
                     stmt.setString(1, lastPing);
                     stmt.setInt(2, location);
                     stmt.executeUpdate();
                } else {
                    stmt                    = transconn.prepareStatement(insertBBTV);
                    stmt.setString(1, wmac);
                    stmt.setString(2, emac);
                    stmt.setString(3, version);
                    stmt.setInt(4, location);
                    stmt.executeUpdate();
                }
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

    private void addUpdateDeleteLocationSpecials(Element toHandle, Element toAppend) throws HandlerException {

        int callerId                        = getCallerId(toHandle);
        int locationId                      = HandlerUtils.getRequiredInteger(toHandle, "locationId");
        ResultSet rs                        = null;
        PreparedStatement stmt              = null;

        try {

            stmt                            = transconn.prepareStatement(" UPDATE locationBeerBoardMap SET specials=? WHERE location = ? ");
            stmt.setInt(1, 1);
            stmt.setInt(2, locationId);
            stmt.executeUpdate();

            Iterator addLocationSpecials    = toHandle.elementIterator("addLocationSpecials");
            while (addLocationSpecials.hasNext()) {
                Element locationSpecials    = (Element) addLocationSpecials.next();
                String specials             = HandlerUtils.getRequiredString(locationSpecials, "specials");
                int sequence                = HandlerUtils.getRequiredInteger(locationSpecials, "sequence");

                String getLastId            = " SELECT LAST_INSERT_ID()";

                stmt                        = transconn.prepareStatement("INSERT INTO locationSpecials (location, specials, sequence) VALUES (?, ?, ?)");
                stmt.setInt(1, locationId);
                stmt.setString(2, specials);
                stmt.setInt(3, sequence);
                stmt.executeUpdate();

                stmt                        = transconn.prepareStatement(getLastId);
                rs                          = stmt.executeQuery();
                if (rs.next()) {
                    int id                  = rs.getInt(1);
                    String logMessage       = "Added Location Specials '" + specials + "'";
                    logger.portalDetail(callerId, "addLocationSpecials", 0, "locationSpecials", id, logMessage, transconn);
                } else {
                    logger.dbError("first call to LAST_INSERT_ID in addLocationSpecials failed to return a result");
                    throw new HandlerException("Database Error");
                }
            }

            Iterator updateLocationSpecials = toHandle.elementIterator("updateLocationSpecials");
            while (updateLocationSpecials.hasNext()) {
                Element locationSpecials    = (Element) updateLocationSpecials.next();
                int id                      = HandlerUtils.getRequiredInteger(locationSpecials, "id");
                String specials             = HandlerUtils.getRequiredString(locationSpecials, "specials");
                int sequence                = HandlerUtils.getRequiredInteger(locationSpecials, "sequence");

                String updateAccountEmail   = " UPDATE locationSpecials SET specials=?, sequence=? WHERE id = ? ";

                stmt                        = transconn.prepareStatement(updateAccountEmail);
                stmt.setString(1, specials);
                stmt.setInt(2, sequence);
                stmt.setInt(3, id);
                stmt.executeUpdate();
                String logMessage           = "Updated Location Specials '" + specials + "'";
                logger.portalDetail(callerId, "updateLocationSpecials", 0, "accountEmailMap", id, logMessage, transconn);
            }

            Iterator deleteLocationSpecials = toHandle.elementIterator("deleteLocationSpecials");
            while (deleteLocationSpecials.hasNext()) {
                Element locationSpecials    = (Element) deleteLocationSpecials.next();
                int id                      = HandlerUtils.getRequiredInteger(locationSpecials, "id");

                String delete               = " DELETE FROM locationSpecials WHERE id = ? ";

                stmt                        = transconn.prepareStatement(delete);
                stmt.setInt(1,id);
                stmt.executeUpdate();

                String logMessage           = "Delete Location Specials '" + id + "'";
                logger.portalDetail(callerId, "deleteLocationSpecials", 0, "accountEmailMap", id, logMessage, transconn);
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: "+sqle.toString());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
            close(rs);
        }
    }

    private void addUpdateDeleteLocationPromotions(Element toHandle, Element toAppend) throws HandlerException {

        int callerId                        = getCallerId(toHandle);
        int locationId                      = HandlerUtils.getRequiredInteger(toHandle, "locationId");
        ResultSet rs                        = null;
        PreparedStatement stmt              = null;
        Calendar currentDate                = Calendar.getInstance();

        try {

            stmt                            = transconn.prepareStatement(" UPDATE locationBeerBoardMap SET promotions=? WHERE location = ? ");
            stmt.setInt(1, 1);
            stmt.setInt(2, locationId);
            stmt.executeUpdate();

            Iterator addLocationPromotions  = toHandle.elementIterator("addLocationPromotions");
            while (addLocationPromotions.hasNext()) {
                Element locationPromotions  = (Element) addLocationPromotions.next();
                int type                    = HandlerUtils.getRequiredInteger(locationPromotions, "type");
                int visibility              = HandlerUtils.getRequiredInteger(locationPromotions, "visibility");
                String file                 = HandlerUtils.getRequiredString(locationPromotions, "file");
                int sequence                = HandlerUtils.getRequiredInteger(locationPromotions, "sequence");

                String getLastId            = " SELECT LAST_INSERT_ID()";
                stmt                        = transconn.prepareStatement("SELECT l.customer,count(lP.id) FROM locationPromotions lP left Join location l ON l.id= lP.location WHERE lP.type=? AND lP.location=?;");
                stmt.setInt(1, type);
                stmt.setInt(2, locationId);
                boolean canInsert           = true;
                rs                          = stmt.executeQuery();
                if(rs.next()) {
                    int customer            = rs.getInt(1);
                    int count               = rs.getInt(2);
                    if(customer!=4 &&  customer!=205){
                    switch(type) {
                        case 1:
                            if(count>=5) {
                                canInsert   = false;
                            }
                            break;
                        case 2:
                            if(count>=2) {
                                canInsert   = false;
                            }
                            break;
                    }
                    }
                }
                
                if(canInsert) {
                stmt                        = transconn.prepareStatement("INSERT INTO locationPromotions (location, type, visibleTime, file, sequence,date) VALUES (?, ?, ?, ?, ?,?)");
                stmt.setInt(1, locationId);
                stmt.setInt(2, type);
                stmt.setInt(3, visibility);
                stmt.setString(4, file);
                stmt.setInt(5, sequence);
                stmt.setString(6, dbDateFormat.format(currentDate.getTime()));
                stmt.executeUpdate();

                stmt                        = transconn.prepareStatement(getLastId);
                rs                          = stmt.executeQuery();
                if (rs.next()) {
                    int id                  = rs.getInt(1);
                    String logMessage       = "Added Location Promotions '" + file + "'";
                    logger.portalDetail(callerId, "addLocationPromotions", 0, "accountEmailMap", id, logMessage, transconn);
                } else {
                    logger.dbError("first call to LAST_INSERT_ID in addLocationPromotions failed to return a result");
                    throw new HandlerException("Database Error");
                }
                } else {
                    if(type==1) {
                        addErrorDetail(toAppend, "We allow max 5 Images only"  );
                    } else if(type==2){
                        addErrorDetail(toAppend, "We allow max 2 Videos only"  );
                    }
                    
                }
            }

            Iterator updateLocationPromotions
                                            = toHandle.elementIterator("updateLocationPromotions");
            while (updateLocationPromotions.hasNext()) {
                Element locationPromotions  = (Element) updateLocationPromotions.next();
                int id                      = HandlerUtils.getRequiredInteger(locationPromotions, "id");
                int type                    = HandlerUtils.getRequiredInteger(locationPromotions, "type");
                int visibility              = HandlerUtils.getRequiredInteger(locationPromotions, "visibility");
                String file                 = HandlerUtils.getRequiredString(locationPromotions, "file");
                int sequence                = HandlerUtils.getRequiredInteger(locationPromotions, "sequence");

                String updateAccountEmail   = " UPDATE locationPromotions SET type = ?, visibleTime = ?, file = ?, sequence = ? WHERE id = ? ";

                stmt                        = transconn.prepareStatement(updateAccountEmail);
                stmt.setInt(1, type);
                stmt.setInt(2, visibility);
                stmt.setString(3, file);
                stmt.setInt(4, sequence);
                stmt.setInt(5, id);
                stmt.executeUpdate();
                String logMessage           = "Updated Location Promotions '" + id + "'";
                logger.portalDetail(callerId, "updateLocationPromotions", 0, "accountEmailMap", id, logMessage, transconn);
            }

            Iterator deleteLocationPromotions
                                            = toHandle.elementIterator("deleteLocationPromotions");
            while (deleteLocationPromotions.hasNext()) {
                Element locationPromotions  = (Element) deleteLocationPromotions.next();
                int id                      = HandlerUtils.getRequiredInteger(locationPromotions, "id");

                String delete               = " DELETE FROM locationPromotions WHERE id = ? ";

                stmt                        = transconn.prepareStatement(delete);
                stmt.setInt(1,id);
                stmt.executeUpdate();

                String logMessage           = "Delete Location Promotions '" + id + "'";
                logger.portalDetail(callerId, "deleteLocationPromotions", 0, "accountEmailMap", id, logMessage, transconn);
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: "+sqle.toString());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
            close(rs);
        }
    }

   private void addUpdateDeleteLocationSponsors(Element toHandle, Element toAppend) throws HandlerException {

        int callerId                        = getCallerId(toHandle);
        int locationId                      = HandlerUtils.getRequiredInteger(toHandle, "locationId");
        ResultSet rs                        = null;
        PreparedStatement stmt              = null;

        try {

            stmt                            = transconn.prepareStatement(" UPDATE locationBeerBoardMap SET sponsors=? WHERE location = ? ");
            stmt.setInt(1, 1);
            stmt.setInt(2, locationId);
            stmt.executeUpdate();

            Iterator addLocationSponsors  = toHandle.elementIterator("addLocationSponsors");
            while (addLocationSponsors.hasNext()) {
                Element locationSponsors  = (Element) addLocationSponsors.next();
                int type                    = HandlerUtils.getRequiredInteger(locationSponsors, "type");
                int visibility              = HandlerUtils.getRequiredInteger(locationSponsors, "visibility");
                String file                 = HandlerUtils.getRequiredString(locationSponsors, "file");
                int sequence                = HandlerUtils.getRequiredInteger(locationSponsors, "sequence");

                String getLastId            = " SELECT LAST_INSERT_ID()";

                stmt                        = transconn.prepareStatement("INSERT INTO locationSponsors (location, type, visibleTime, file, sequence) VALUES (?, ?, ?, ?, ?)");
                stmt.setInt(1, locationId);
                stmt.setInt(2, type);
                stmt.setInt(3, visibility);
                stmt.setString(4, file);
                stmt.setInt(5, sequence);
                stmt.executeUpdate();

                stmt                        = transconn.prepareStatement(getLastId);
                rs                          = stmt.executeQuery();
                if (rs.next()) {
                    int id                  = rs.getInt(1);
                    String logMessage       = "Added Location Sponsors '" + file + "'";
                    logger.portalDetail(callerId, "addLocationSponsors", 0, "accountEmailMap", id, logMessage, transconn);
                } else {
                    logger.dbError("first call to LAST_INSERT_ID in addLocationSponsors failed to return a result");
                    throw new HandlerException("Database Error");
                }
            }

            Iterator updateLocationSponsors
                                            = toHandle.elementIterator("updateLocationSponsors");
            while (updateLocationSponsors.hasNext()) {
                Element locationSponsors  = (Element) updateLocationSponsors.next();
                int id                      = HandlerUtils.getRequiredInteger(locationSponsors, "id");
                int type                    = HandlerUtils.getRequiredInteger(locationSponsors, "type");
                int visibility              = HandlerUtils.getRequiredInteger(locationSponsors, "visibility");
                String file                 = HandlerUtils.getRequiredString(locationSponsors, "file");
                int sequence                = HandlerUtils.getRequiredInteger(locationSponsors, "sequence");

                String updateAccountEmail   = " UPDATE locationSponsors SET type = ?, visibleTime = ?, file = ?, sequence = ? WHERE id = ? ";

                stmt                        = transconn.prepareStatement(updateAccountEmail);
                stmt.setInt(1, type);
                stmt.setInt(2, visibility);
                stmt.setString(3, file);
                stmt.setInt(4, sequence);
                stmt.setInt(5, id);
                stmt.executeUpdate();
                String logMessage           = "Updated Location Sponsors '" + id + "'";
                logger.portalDetail(callerId, "updateLocationSponsors", 0, "accountEmailMap", id, logMessage, transconn);
            }

            Iterator deleteLocationSponsors
                                            = toHandle.elementIterator("deleteLocationSponsors");
            while (deleteLocationSponsors.hasNext()) {
                Element locationSponsors  = (Element) deleteLocationSponsors.next();
                int id                      = HandlerUtils.getRequiredInteger(locationSponsors, "id");

                String delete               = " DELETE FROM locationSponsors WHERE id = ? ";

                stmt                        = transconn.prepareStatement(delete);
                stmt.setInt(1,id);
                stmt.executeUpdate();

                String logMessage           = "Delete Location Sponsors '" + id + "'";
                logger.portalDetail(callerId, "deleteLocationSponsors", 0, "accountEmailMap", id, logMessage, transconn);
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: "+sqle.toString());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
            close(rs);
        }
   }


   private void getLocationSpecials(Element toHandle, Element toAppend) throws HandlerException {

        int locationId                      = HandlerUtils.getRequiredInteger(toHandle, "locationId");
        PreparedStatement stmt              = null;
        ResultSet rs                        = null;
        String selectLocationSpecials       = "SELECT id, specials, sequence FROM locationSpecials WHERE location = ?  ";

        try {
            stmt                            = transconn.prepareStatement(selectLocationSpecials);
            stmt.setInt(1,locationId);
            rs                              = stmt.executeQuery();
            while (rs.next()) {
                Element locationSpecialsEl  = toAppend.addElement("locationSpecials");
                locationSpecialsEl.addElement("visibleTime").addText("10000");
                locationSpecialsEl.addElement("id").addText(HandlerUtils.nullToEmpty(rs.getString(1)));
                locationSpecialsEl.addElement("specials").addText(HandlerUtils.nullToEmpty(rs.getString(2)));
                locationSpecialsEl.addElement("sequence").addText(HandlerUtils.nullToEmpty(rs.getString(3)));
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: "+sqle.toString());
            throw new HandlerException(sqle);
        } finally {
            close(rs);
            close(stmt);
        }
    }

    public boolean checkOnTap (int locationId, int productId) throws HandlerException {
        ResultSet rs                        = null;
        PreparedStatement stmt              = null;
        boolean isOnTap                     = false;
        String isBrewOnTap                  = "SELECT p.id FROM bar b LEFT JOIN line l on l.bar = b.id LEFT JOIN product p on l.product = p.id " +
                                            " AND l.status = 'RUNNING' AND b.location = ? AND l.product = ?";

        try {
            stmt                            = transconn.prepareStatement(isBrewOnTap);
            stmt.setInt(1, locationId);
            stmt.setInt(2, productId);
            rs                              = stmt.executeQuery();
            if (rs.next()) {
                isOnTap                     = true;
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: "+sqle.toString());
            throw new HandlerException(sqle);
        } finally {
            close(rs);
            close(stmt);
        }
        return isOnTap;
    }

   private StringBuilder getMarketingTemplate1(String name, String abv, String origin, String seasonal, String style) throws HandlerException {

        StringBuilder beerInfo               = new StringBuilder();
        beerInfo.append("<table width='80%' border='0' cellspacing='0' cellpadding='0' style='padding-left: 15px;'><tr><td align='left' valign='bottom'></td></tr><tr><td align='left' valign='middle' height='30px'>");
        beerInfo.append("<p><span class='beer_list_title'>");
        beerInfo.append(name);
        beerInfo.append("</span></p><p><span class='beer_list_abv'>Style: "+style+"</span></p>");
        beerInfo.append("<P> <span class='beer_list_abv'>ABV : "+abv+"</span></p><p><span class='beer_list_abv'>Origin: "+origin+"</span></p><p>&nbsp;</p></td></tr> <tr> </tr>");
        beerInfo.append(" <tr> <td align='left' valign='bottom' height='10px'><p class=\"beer_list_Season\"> </p></td></tr>");
        beerInfo.append("<tr><td align='left' valign='bottom' height='30px'><p class=\"beer_list_Season\"> Availablity: "+seasonal+"</p></td> </tr></table>");

        return beerInfo;
   }

   private void getMarketingTemplate2(String name,String abv,String origin,String seasonal,String style,String logo,String glass,int location ) throws HandlerException {

       PreparedStatement stmt              = null;
       ResultSet rs                        = null;
       String subjectLine                  = "Marketing Page Approval from US Beverage NET, Inc.";
       String getmail                      = " SELECT u.email FROM user u WHERE u.location= ? and u.isManager = 1";
       StringBuilder beerInfo               = new StringBuilder();
       beerInfo.append("<table bgcolor=\"#000000\" width='100%' border='0' cellspacing='0' cellpadding='0' style='padding-left: 15px;'><tr>");
       beerInfo.append("<td width=\"25%\" rowspan=\"4\" align='left' valign='center'><img src=\""+logo+"\" /></td><td width=\"50%\" align='left' valign='center'></td>");
       beerInfo.append("<td width=\"25%\" rowspan=\"4\" align='left' valign='center'><img src=\""+glass+"\" /></td></tr><tr> <td align='left' valign='middle' height='30'>");
       beerInfo.append("<p><font color=\"orange\" size=\"6\" face=\"ChalkDust\">");
       beerInfo.append(name);
       beerInfo.append("</font></p><font color=\"orange\" size=\"5\" face=\"ChalkDust\"><p>Style: "+style+"</span></p></font>");
       beerInfo.append("<P><font color=\"orange\" size=\"5\" face=\"ChalkDust\">ABV : "+abv+"</font></p><font color=\"orange\" size=\"5\" face=\"ChalkDust\"><p>Origin: "+origin+"</span></p></font><p>&nbsp;</p></td></tr> <tr> </tr>");
       beerInfo.append(" <tr> <td align='left' valign='bottom' height='10px'><p> </p></td></tr>");
       beerInfo.append("<tr><td align='left' valign='bottom' height='30px'><font color=\"orange\" size=\"6\" face=\"ChalkDust\"><p > </p></font></td> </tr></table>");
       try {

           stmt                            = transconn.prepareStatement(getmail);
           stmt.setInt(1, location);
           rs                              = stmt.executeQuery();
           while(rs.next()) {
               sendMail("", "tech@beerboard.com", rs.getString(1), "tech@beerboard.com", subjectLine, "sendMail", beerInfo, true);
           }
       } catch (SQLException sqle) {
            logger.dbError("Database error: "+sqle.toString());
            throw new HandlerException(sqle);
        } finally {
            close(rs);
            close(stmt);

        }

   }



   private String getMarketingCSS() throws HandlerException {

         String  css                        = "@charset 'utf-8'; " +
                                            " @font-face { font-family: 'ChalkDust'; src: url('file:///android_asset/fonts/chalkdust.ttf'); } " +
                                            " @font-face { font-family: 'PPETRIAL'; src: url('file:///android_asset/fonts/PPETRIAL.otf'); } " +
                                            " body { background-image: url('../Images/USBN_back.gif'); background-position: left top; background-repeat: repeat; fieldset border-style:none; }" +
                                            " .beer_list_title{ font-family: 'ChalkDust'; font-size:30px; font-weight:bold; color: #ff9900; overflow:hidden;  white-space:nowrap; } " +
                                            " .beer_list_abv { font-family: 'ChalkDust'; font-size:20px; font-weight:bold; color: #ff9900; overflow:hidden;  white-space:nowrap; } "+
                                            "  .percentage { font-family: 'PPETRIAL'; font-size:26px; font-weight:bold; color: #ff9900; overflow:hidden;  white-space:nowrap; } " +
                                            " .beer_list_desc { font-family: 'ChalkDust'; font-size:20px; font-weight:bold; color: #ff9900; overflow:hidden;  white-space:nowrap; } " +
                                            " .beer_list_Season { font-family: 'ChalkDust'; font-size:24px; font-weight:bold; color: #ff9900; overflow:hidden;  white-space:nowrap; } ";
        return css;
   }


   public void sendMail(String title, String senderAddr, String emailAddr, String supportEmailAddr, String templateMessageTitle,
            String templateMessage, StringBuilder emailBody, boolean sendBCC) {
        String emailTemplatePath            = HandlerUtils.getSetting("email.templatePath");
        if ((emailTemplatePath == null) || "".equals(emailTemplatePath)) {
            emailTemplatePath               = ".";
        }
        logger.debug("Marketing Approval Email");
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
                poEmail.send();
            }
        } catch (MailException me) {
            logger.dbError("Error sending message to " + emailAddr + ": " + me.toString());
        }
   }


    private void testHttpURLConnection(HttpURLConnection connection) throws Exception {

        boolean connAllowUserInteraction    = connection.getAllowUserInteraction();
        String connContentType              = connection.getContentType();
        String connContentEncoding          = connection.getContentEncoding();
        String connRequestMethod            = connection.getRequestMethod();
        boolean connDoInput                 = connection.getDoInput();
        boolean connDoOutput                = connection.getDoOutput();
        Permission connPermission           = connection.getPermission();
        URL connURL                         = connection.getURL();
        Map<String, List<String>> connHeaderFields
                                            = connection.getHeaderFields();

        logger.debug("connAllowUserInteraction: " + connAllowUserInteraction);
        logger.debug("connContentType: " + connContentType);
        logger.debug("connContentEncoding: " + connContentEncoding);
        logger.debug("connRequestMethod: " + connRequestMethod);
        logger.debug("connDoInput: " + connDoInput);
        logger.debug("connDoOutput: " + connDoOutput);
        logger.debug("connPermission: " + connPermission);
        logger.debug("connURL: " + connURL);

        if (connHeaderFields != null) {
            Set<Entry<String, List<String>>> connHeaderFieldsEntries
                                            = connHeaderFields.entrySet();
            for (Entry<String, List<String>> entry : connHeaderFieldsEntries) {
                logger.debug("connHeaderField: " + entry);
            }
        }
    }

     
    private void checkDbVersion(Element toHandle, Element toAppend) throws HandlerException {

        int version                         = HandlerUtils.getOptionalInteger(toHandle, "version");
        int platform                        = HandlerUtils.getOptionalInteger(toHandle, "platform");
        int user                            = HandlerUtils.getOptionalInteger(toHandle, "user");
 
        PreparedStatement stmt              = null;
        ResultSet rs                        = null;
        
        String selectCity                   = "SELECT COUNT(l.id)" +
                                            " FROM location l LEFT JOIN customer c ON c.id = l.customer LEFT JOIN locationGraphics lG ON lG.location = l.id "+
                                            " LEFT JOIN locationDetails lD ON lD.location = l.id WHERE lD.active = 1 AND l.addrCountry = 'USA' AND l.addrState != 'DC' " +
                                            " ORDER BY c.name, l.name ASC ";
        String selectLocations              = "SELECT l.id, CONCAT(c.name, ' - ', l.name), l.latitude, l.longitude, l.easternOffset, IFNULL(REPLACE(lG.logo, ' ', '%20'), 'USBNLogo.png'), l.addrStreet, l.addrCity, l.addrState, l.addrZip, l.boardname " +
                                            " FROM location l LEFT JOIN customer c ON c.id = l.customer LEFT JOIN locationGraphics lG ON lG.location = l.id "+
                                            " LEFT JOIN locationDetails lD ON lD.location = l.id WHERE lD.active = 1 AND l.addrCountry = 'USA' AND l.addrState != 'DC' " +
                                            " ORDER BY c.name, l.name ASC ";
        String selectState                  = "SELECT COUNT(DISTINCT(l.addrState)) FROM location l LEFT JOIN state s ON s.USPSST=l.addrState LEFT JOIN locationDetails lD ON lD.location=l.id WHERE  lD.active=1;";
        String selectProfile                = "SELECT id,deviceToken FROM bbtvMobileUser WHERE email = ? and platform = ?; ";
        String selectUser                   = "SELECT id FROM bbtvMobileUser WHERE id = ?; ";
        String updateProfile                = "UPDATE bbtvMobileUser SET arrival=now() WHERE id = ?;";
        String selectVersion                = "SELECT version FROM bbtvMobileVersion WHERE platform = ?;";


        try {
            stmt                            = transconn.prepareStatement(selectVersion);
            stmt.setInt(1, platform);
            rs                              = stmt.executeQuery();
            int count                       = 0;
            if(rs.next()) { 
                count                       = rs.getInt(1);
                if (user == 0 && version == 43) {
                    if (version == count) {
                        count               = count - 1;
                    } else if (version < count) {
                        count               = count + 1;
                    }
                }
                if (version == 42) {
                    count                   = 42;
                }
                toAppend.addElement("version").addText(String.valueOf(count));
                if(user > 0) {
                    stmt                    = transconn.prepareStatement(updateProfile);
                    stmt.setInt(1, user);
                    stmt.executeUpdate();
                    getNotificationCenter(platform, user, toAppend);
                }
            }
            
             stmt                = transconn.prepareStatement("SELECT id, username,fbID,token FROM bbtvMobileUser WHERE id = ?");
             stmt.setInt(1, user);
             rs            = stmt.executeQuery();
             if(rs.next()) { 
                 if(rs.getString(3)!=null && !rs.getString(3).equals("")) { 
                     if(rs.getString(3).length() >10) {
                         int id=rs.getInt(1);
                         toAppend.addElement("facebook").addText(String.valueOf(checkFacebookToken(rs.getString(4))));
                     }
                 }
             }
             toAppend.addElement("meters").addText(String.valueOf(100));
            
          /*  if(version != count) {
                stmt                        = transconn.prepareStatement(selectLocations);
                rs                          = stmt.executeQuery();
                while (rs != null && rs.next()) {
                    int rsIndex             = 1;
                    Element locEl           = toAppend.addElement("checkLocation");
                    locEl.addElement("check").addText(String.valueOf("2"));
                    locEl.addElement("locationId").addText(String.valueOf(rs.getInt(rsIndex++)));
                    locEl.addElement("locationName").addText(HandlerUtils.nullToEmpty((rs.getString(rsIndex++))));
                    locEl.addElement("latitude").addText(String.valueOf(rs.getString(rsIndex++)));
                    locEl.addElement("longitude").addText(String.valueOf(rs.getString(rsIndex++)));
                    locEl.addElement("easternOffset").addText(HandlerUtils.nullToEmpty((rs.getString(rsIndex++))));
                    locEl.addElement("logo").addText(HandlerUtils.nullToEmpty(("http://bevmanager.net/Images/Location_logo/" + rs.getString(rsIndex++))));
                    locEl.addElement("addrStreet").addText(HandlerUtils.nullToEmpty((rs.getString(rsIndex++))));
                    locEl.addElement("addrCity").addText(HandlerUtils.nullToEmpty((rs.getString(rsIndex++))));
                    locEl.addElement("addrState").addText(HandlerUtils.nullToEmpty((rs.getString(rsIndex++))));
                    locEl.addElement("addrZip").addText(HandlerUtils.nullToEmpty((rs.getString(rsIndex++))));
                    locEl.addElement("boardName").addText(HandlerUtils.nullToEmpty((rs.getString(rsIndex++))));
                }
            }*/
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        }catch(Exception e) {
        }finally {
            close(stmt);
            close(rs);
        }
    }
    
    
    private void getNewMobileMenu(int locationId, Element toAppend, ArrayList<String> products, ArrayList<String> type) throws HandlerException {
        
        PreparedStatement stmt              = null;        
        ResultSet rs                        = null,rsDetails = null;        
        String selectDetails                = "SELECT pD.boardName, IF(pD.abv = 0.0, '', CONCAT(pD.abv,'%')), sPS.name, pD.origin, IFNULL(sL.logo, 'Seasonal'), sPS.id " +
                                            " FROM productDescription pD LEFT JOIN productSetMap sPSM ON sPSM.product = pD.product LEFT JOIN productSet sPS ON sPS.id = sPSM.productSet " +
                                            " LEFT JOIN styleLogo sL ON sL.style = sPS.name " +
                                            " LEFT JOIN productSetMap pSM ON pSM.product = pD.product LEFT JOIN productSet pS ON pS.id = pSM.productSet " +
                                            " LEFT JOIN breweryLogo bL ON bL.brewery = pS.name " +
                                            " WHERE pS.productSetType = 7 AND sPS.productSetType = 9 AND pD.product = ?;";
        String selectCustomName             = "SELECT IFNULL((SELECT name FROM customBeerName WHERE location =? AND product = ?),?),name,logo FROM customStyleName WHERE location =? AND productSet =? ;";
        
       try {
           
           for(int i=0;i< products.size(); i++) {   
               int productId                = Integer.parseInt(products.get(i));
               String typeId                = type.get(i);
               
               stmt                         = transconn.prepareStatement(selectDetails);
               stmt.setInt(1, productId);
               rs                           = stmt.executeQuery();
               if(rs.next()) {
                   String productName       = rs.getString(1);
                   int styleId              = rs.getInt(6);
                   String styleName         = rs.getString(3);
                   String styleLogo         = "http://beerboard.tv/USBN.BeerBoard.UI/Images/glass/" +rs.getString(5)+".png";
                    stmt                     = transconn.prepareStatement(selectCustomName);
                   stmt.setInt(1, locationId);
                   stmt.setInt(2, productId);
                   stmt.setString(3, productName);
                   stmt.setInt(4, locationId);
                   stmt.setInt(5, styleId);
                   rsDetails                = stmt.executeQuery();
                   if(rsDetails.next()){
                       productName          = rsDetails.getString(1);
                       styleName            =HandlerUtils.nullToString(rsDetails.getString(2), styleName);
                       String customLogo    =rsDetails.getString(3);
                       if(customLogo!=null&& !customLogo.equals("") ){
                           styleLogo        = "http://beerboard.tv/USBN.BeerBoard.UI/Images/CustomStyle/"+String.valueOf(locationId)+"/"+customLogo;    
                       }
                   }
                   int colCount                 = 1;
                   Element recommendEl = toAppend.addElement("Menu");
                   recommendEl.addElement("type").addText(typeId); 
                   recommendEl.addElement("productId").addText(Integer.toString(productId)); 
                   recommendEl.addElement("product").addText(productName); 
                   recommendEl.addElement("abv").addText(HandlerUtils.nullToEmpty(rs.getString(2))); 
                   recommendEl.addElement("style").addText(styleName); 
                   recommendEl.addElement("origin").addText(HandlerUtils.nullToEmpty(rs.getString(4))); 
                   recommendEl.addElement("logo").addText(styleLogo);
               }
           }
            
           
        } catch ( Exception sqle) {
            logger.dbError("Database error: "+sqle.toString());
            throw new HandlerException(sqle);
        } finally {
           close(rsDetails);
            close(rs);            
            close(stmt);
        }
   }
   
    
    private void getNewMobileMenu(int locationId, Element toAppend, ArrayList<String> products, ArrayList<String> type, ArrayList<String> kegIcon) throws HandlerException {
        
        PreparedStatement stmt              = null;        
        ResultSet rs                        = null,rsDetails = null;        
        String selectDetails                = "SELECT p.name, IF(pD.abv = 0.0, '', CONCAT(pD.abv,'%')), sPS.name, pD.origin, IFNULL(sL.logo, 'Seasonal'),sPS.id  " +
                                            " FROM product p LEFT JOIN productDescription pD ON pD.product=p.id  LEFT JOIN productSetMap sPSM ON sPSM.product = pD.product LEFT JOIN productSet sPS ON sPS.id = sPSM.productSet " +
                                            " LEFT JOIN styleLogo sL ON sL.style = sPS.name " +
                                            //" LEFT JOIN productSetMap pSM ON pSM.product = p.id LEFT JOIN productSet pS ON pS.id = pSM.productSet " +
                                           // " LEFT JOIN breweryLogo bL ON bL.brewery = pS.name " +
                                            " WHERE  sPS.productSetType = 9 AND p.id = ?;";
        String selectCustomName             = "SELECT IFNULL((SELECT name FROM customBeerName WHERE location =? AND product = ?),?),name,logo FROM customStyleName WHERE location =? AND productSet =? ;";
        
        
       try {
           
           for(int i=0; i< products.size(); i++) {
               int productId                = Integer.parseInt(products.get(i));
               String typeId                = type.get(i);
               String keg                   = kegIcon.get(i);
               
               stmt                         = transconn.prepareStatement(selectDetails);
               stmt.setInt(1, productId);
               rs                           = stmt.executeQuery();
               if(rs.next()) {
                   //logger.debug("New Menu");
                   String productName       = rs.getString(1);
                   int styleId              = rs.getInt(6);
                   String styleName         = rs.getString(3);
                   String styleLogo         = "http://beerboard.tv/USBN.BeerBoard.UI/Images/glass/" +rs.getString(5)+".png";
                   stmt                     = transconn.prepareStatement(selectCustomName);
                   stmt.setInt(1, locationId);
                   stmt.setInt(2, productId);
                   stmt.setString(3, productName);
                   stmt.setInt(4, locationId);
                   stmt.setInt(5, styleId);
                   rsDetails                = stmt.executeQuery();
                   if(rsDetails.next()){
                       productName          = rsDetails.getString(1);
                       styleName            = HandlerUtils.nullToString(rsDetails.getString(2), styleName);
                       String customLogo    = rsDetails.getString(3);
                       if(customLogo!=null&& !customLogo.equals("") ){
                           styleLogo        = "http://beerboard.tv/USBN.BeerBoard.UI/Images/CustomStyle/"+String.valueOf(locationId)+"/"+customLogo;    
                       }
                   }
                   //logger.debug(styleLogo);
                   Element recommendEl      = toAppend.addElement("Menu");
                   recommendEl.addElement("type").addText(typeId); 
                   recommendEl.addElement("productId").addText(Integer.toString(productId)); 
                   recommendEl.addElement("product").addText(productName); 
                   recommendEl.addElement("abv").addText(HandlerUtils.nullToEmpty(rs.getString(2)));                    
                   recommendEl.addElement("newstyle").addText(styleName); 
                   recommendEl.addElement("origin").addText(HandlerUtils.nullToEmpty(rs.getString(4))); 
                   if(keg != null && !keg.equals("")) {
                       recommendEl.addElement("style").addText(keg);
                       recommendEl.addElement("logo").addText("http://beerboard.tv/USBN.BeerBoard.UI/Images/Assets/Generic/"+keg);                       
                   } else {
                       recommendEl.addElement("style").addText(styleName);
                       recommendEl.addElement("logo").addText(styleLogo);
                   }
               }
           }
        } catch ( Exception sqle) {
            logger.dbError("Database error: "+sqle.toString());
            throw new HandlerException(sqle);
        } finally {
           close(rsDetails);
            close(rs);            
            close(stmt);
        }
   }
   
    
    
      
   private void getMobileMarketing1(int locationId, Element toAppend, ArrayList<String> products, int platform) throws HandlerException {
        
        PreparedStatement stmt              = null;
        String location                     = null;
        ResultSet rs                        = null, rsDetails = null;
        String selectLocation               = "SELECT name FROM location WHERE id = ? ; ";
        String selectDetails                = "SELECT pD.boardName, IF(pD.abv = 0.0, '', CONCAT(pD.abv,'%')), pD.origin, sPS.name, IFNULL(sL.logo, 'NA.png'), CONCAT(IFNULL(bL.logo, 'Custom'), IF(bL.type = 0, '.png', IF(bL.type = 1, '.jpg', '.gif'))) " +
                                            " FROM productDescription pD LEFT JOIN productSetMap sPSM ON sPSM.product = pD.product LEFT JOIN productSet sPS ON sPS.id = sPSM.productSet " +
                                            " LEFT JOIN styleLogo sL ON sL.style = sPS.name " +
                                            " LEFT JOIN productSetMap pSM ON pSM.product = pD.product LEFT JOIN productSet pS ON pS.id = pSM.productSet " +
                                            " LEFT JOIN breweryLogo bL ON bL.brewery = pS.name " +
                                            " WHERE pS.productSetType = 7 AND sPS.productSetType = 9 AND pD.product = ?;";
        
       try {
           stmt                         = transconn.prepareStatement(selectLocation);
            stmt.setInt(1, locationId);
            rs                          = stmt.executeQuery();
            if(rs.next()) {
                location                = rs.getString(1);
            }
           for(int i=0;i< products.size(); i++) {   
               int productId                = Integer.parseInt(products.get(i));
               stmt                         = transconn.prepareStatement(selectDetails);
               stmt.setInt(1, productId);
               rsDetails                    = stmt.executeQuery();
               if(rsDetails.next()) {
                   String product           = rsDetails.getString(1);
                   String abv               = rsDetails.getString(2);
                   String origin             = rsDetails.getString(3);
                   String style             = rsDetails.getString(4);
                   String styleLogo         = rsDetails.getString(5);
                   String brewery           = rsDetails.getString(6);
                   
                   Element mobileproduct    = toAppend.addElement("menuLink");
                   mobileproduct.addElement("product").addText(Integer.toString(productId));
                   mobileproduct.addElement("name").addText(HandlerUtils.nullToEmpty(product));
                   mobileproduct.addElement("abv").addText(HandlerUtils.nullToEmpty(abv));
                   mobileproduct.addElement("origin").addText(HandlerUtils.nullToEmpty(origin));
                   mobileproduct.addElement("style").addText(HandlerUtils.nullToEmpty(style));
                   mobileproduct.addElement("stylelogo").addText(styleLogo);
                   brewery                  = brewery.substring(0, brewery.length()-4);
                   mobileproduct.addElement("brewery").addText(brewery.replaceAll("%20", " "));
                    
                   }
           }
            
           
        } catch ( Exception sqle) {
            logger.dbError("Database error: "+sqle.toString());
            throw new HandlerException(sqle);
        } finally {
            close(rs);
            close(rsDetails);
            close(stmt);
        }
   }
   
    
    private void getMobileMarketing(int locationId, Element toAppend, ArrayList<String> products, int platform) throws HandlerException {
        
        PreparedStatement stmt              = null;
        String location                     = null;
        ResultSet rs                        = null, rsDetails = null;
        String selectLocation               = "SELECT name FROM location WHERE id = ? ; ";
        String selectDetails                = "SELECT pD.boardName, IF(pD.abv = 0.0, '', CONCAT(pD.abv,'%')), pD.origin, sPS.name, IFNULL(sL.logo, 'NA.png'), CONCAT(IFNULL(bL.logo, 'Custom'), IF(bL.type = 0, '.png', IF(bL.type = 1, '.jpg', '.gif'))) " +
                                            " FROM productDescription pD " +
                                            " LEFT JOIN productSetMap sPSM ON sPSM.product = pD.product LEFT JOIN productSet sPS ON sPS.id = sPSM.productSet " +
                                            " LEFT JOIN styleLogo sL ON sL.style = sPS.name " +
                                            " LEFT JOIN productSetMap pSM ON pSM.product = pD.product LEFT JOIN productSet pS ON pS.id = pSM.productSet " +
                                            " LEFT JOIN breweryLogo bL ON bL.brewery = pS.name " +
                                            " WHERE pS.productSetType = 7 AND sPS.productSetType = 9 AND pD.product = ?;";
        
       try {
            stmt                            = transconn.prepareStatement(selectLocation);
            stmt.setInt(1, locationId);
            rs                              = stmt.executeQuery();
            if(rs.next()) {
                location                    = rs.getString(1);
            }
            for(int i=0;i< products.size(); i++) {
               int productId                = Integer.parseInt(products.get(i));
               stmt                         = transconn.prepareStatement(selectDetails);
               stmt.setInt(1, productId);
               rsDetails                    = stmt.executeQuery();
               if(rsDetails.next()) {
                   String product           = rsDetails.getString(1);
                   String abv               = rsDetails.getString(2);
                   String orgin             = rsDetails.getString(3);
                   String style             = rsDetails.getString(4);
                   String styleLogo         = rsDetails.getString(5);
                   String brewery           = rsDetails.getString(6);
                   String html              = writeHtml(location, product, abv, orgin, style, styleLogo+".png", brewery);
                   Element mobileproduct    = toAppend.addElement("menuLink");
                   mobileproduct.addElement("product").addText(Integer.toString(productId));
                   mobileproduct.addElement("html").addText(HandlerUtils.nullToEmpty(html));
               }
           }
            toAppend.addElement("menuLinkCss").addText(getMobileMarketingCSS(platform));
           
        } catch ( Exception sqle) {
            logger.dbError("Database error: "+sqle.toString());
            throw new HandlerException(sqle);
        } finally {
            close(rs);
            close(rsDetails);
            close(stmt);
        }
   }
    
        String  writeHtml(String location, String product, String abv, String orgin, String style, String styleLogo, String breweryLogo) {
        StringBuffer sb                     = new StringBuffer();
            try {
        
        sb.append("<table width=\"100%\" height=\"20\" border='0' cellspacing='0' cellpadding='0' style='padding-left: 5px;'>");
        sb.append("<tr><td valign=\"top\" align=\"center\"><span class='beer_loc_title'>Now on Tap at ");
        sb.append(location);
        sb.append("</span><p><span class='beer_list_title'>");
        sb.append(product);
        sb.append("</span></p></td></tr></table>");
        sb.append("<table width=\"100%\" height=\"200\" border='0' cellspacing='0' cellpadding='0' style='padding-left: 15px;'>");
        sb.append("<tr><td valign=\"top\" align=\"center\"><img src=\"http://beerboard.tv/USBN.BeerBoard.UI/Images/logo/"+breweryLogo.trim().replaceAll("\'", "%27").replaceAll(" ", "%20"));
        sb.append("\" width=\"155\" height=\"200\" alt=\"Computer Hope\"/></td></tr></table>");
        sb.append("<table width=\"100%\" height=\"180\" border='0' cellspacing='0' cellpadding='0' style='padding-left: 15px;'>  ");
        sb.append("<tr><td align=\"left\" valign=\"middle\"><span class='beer_list_abv'>"
                + "Style: "+ style + "</span><p><span class='beer_list_abv'>ABV: "+abv+"</span><span class='percentage'></span></p>"
                + "<p><span class=\"beer_list_abv\">Origin: " + orgin + "</span></p></td>");
        sb.append("<td align=\"left\" valign=\"top\"><img src=\"http://beerboard.tv/USBN.BeerBoard.UI/Images/glass/" + styleLogo.trim().replaceAll("\'", "%27").replaceAll(" ", "%20"));
        sb.append("\" width=\"80\" height=\"180\" alt=\"Computer Hope\"/></td>");
        sb.append("</tr></table>");
        sb.append("<table width=\"100%\" height=\"100\" border='0' cellspacing='0' cellpadding='0' style='padding-top: 15px;'>");
        sb.append("<tr><td valign=\"top\" align=\"right\" valign=\"bottom\"><img src=\"http://beerboard.tv/USBN.BeerBoard.UI/Images/beerboardlogo.png");
        sb.append("\" width=\"140\" height=\"40\" alt=\"Computer Hope\"/></td></tr></table>");
         //logger.debug(sb.toString());
        }catch (Exception e) {
                logger.dbError("Html: "+e.toString());
                e.printStackTrace();
        }
            return sb.toString();
        
    }
        
        
       private String getMobileMarketingCSS(int type) throws HandlerException {
           
            String  css                     = "";
           switch(type) {
               default:
                   css                      = "@charset 'utf-8'; " +
                                            " @font-face { font-family: 'ChalkDust'; src: url('file:///android_asset/fonts/chalkdust.ttf'); } " +
                                            " @font-face { font-family: 'PPETRIAL'; src: url('file:///android_asset/fonts/PPETRIAL.otf'); } " +
                                            " body {  }" +
                                            " .beer_loc_title{ font-family: 'PPETRIAL'; font-size:20px; font-weight:bold; color: #ff9900; overflow:hidden;  white-space:nowrap; } " +
                                            " .beer_list_title{ font-family: 'ChalkDust'; font-size:28px; font-weight:bold; color: #ffffff; overflow:hidden;  white-space:nowrap; } " +
                                            " .beer_list_abv { font-family: 'ChalkDust'; font-size:15px; font-weight:bold; color: #ff9900; overflow:hidden;  white-space:nowrap; } "+
                                            "  .percentage { font-family: 'PPETRIAL'; font-size:15px; font-weight:bold; color: #ff9900; overflow:hidden;  white-space:nowrap; } " +
                                            " .beer_list_desc { font-family: 'ChalkDust'; font-size:15px; font-weight:bold; color: #ff9900; overflow:hidden;  white-space:nowrap; } " +
                                            " .beer_list_Season { font-family: 'ChalkDust'; font-size:15px; font-weight:bold; color: #ff9900; overflow:hidden;  white-space:nowrap; } ";
                   break;
                   
                   case 3:
                   css                      = "@charset 'utf-8'; " +
                                            " @font-face { font-family: 'ChalkDust'; src: url('chalkdust.ttf'); } " +
                                            " @font-face { font-family: 'PPETRIAL'; src: url('PPETRIAL.otf'); } " +
                                            " body {  }" +
                                            " .beer_loc_title{ font-family: 'PPETRIAL'; font-size:20px; font-weight:bold; color: #ff9900; overflow:hidden;  white-space:nowrap; } " +
                                            " .beer_list_title{ font-family: 'ChalkDust'; font-size:28px; font-weight:bold; color: #ffffff; overflow:hidden;  white-space:nowrap; } " +
                                            " .beer_list_abv { font-family: 'ChalkDust'; font-size:15px; font-weight:bold; color: #ff9900; overflow:hidden;  white-space:nowrap; } "+
                                            "  .percentage { font-family: 'PPETRIAL'; font-size:15px; font-weight:bold; color: #ff9900; overflow:hidden;  white-space:nowrap; } " +
                                            " .beer_list_desc { font-family: 'ChalkDust'; font-size:15px; font-weight:bold; color: #ff9900; overflow:hidden;  white-space:nowrap; } " +
                                            " .beer_list_Season { font-family: 'ChalkDust'; font-size:15px; font-weight:bold; color: #ff9900; overflow:hidden;  white-space:nowrap; } ";
                   break;
           }
        return css;
   }

   private void postSocialMediaUpdates(Element toHandle, Element toAppend) throws HandlerException {

        PreparedStatement stmt              = null;
        ResultSet rs                        = null, rsDetails = null,rsFB=null;
        //SQLSocialMediaHandler socialMedia   = new SQLSocialMediaHandler();
        String selectLineUpdates            = "SELECT l.id, lU.id, l.boardname, l.name, l.addrCity, l.addrState, l.addrZip, sU.url, lU.product, l.twitterHandle,l.customer FROM lineUpdates lU " +
                                            " LEFT JOIN location l ON l.id = lU.location LEFT JOIN shortURL sU ON l.id = sU.location WHERE lU.active = 1 " +
                                            " ORDER BY l.id LIMIT 10";
        String updateLineUpdates            = "UPDATE lineUpdates SET active = 0 WHERE active = 1;";
        String selectDetails                = "SELECT p.name, IF(pD.abv = 0.0, '', CONCAT(pD.abv,'%')), pD.origin, sL.logo, CONCAT(bL.logo, IF(bL.type = 0, '.png', IF(bL.type = 1, '.jpg', '.gif'))), bL.twitterHandle,pS.name  " +
                                            " FROM product p LEFT JOIN productDescription pD ON pD.product=p.id  LEFT JOIN productSetMap sPSM ON sPSM.product = pD.product LEFT JOIN productSet sPS ON sPS.id = sPSM.productSet " +
                                            " LEFT JOIN styleLogo sL ON sL.productSet = sPS.id " +
                                            " LEFT JOIN productSetMap pSM ON pSM.product = pD.product LEFT JOIN productSet pS ON pS.id = pSM.productSet " +
                                            " LEFT JOIN breweryLogo bL ON bL.productSet = pS.id  WHERE pS.productSetType = 7 AND sPS.productSetType = 9 AND pD.product = ?"  ;
        String selectToken                  = "SELECT DISTINCT p.pageid, u.user_id, u.access_token, p.type, " +
                                            " IFNULL((SELECT lineUpdate FROM socialMediaPost WHERE location = ? AND type=1),0) FROM usbnFacebook u " +
                                            " LEFT JOIN usbnFacebookPage p ON u.user_id =p.fbid LEFT JOIN socialMediaPost sMP ON sMP.location = p.location " +
                                            " WHERE p.location = ? AND sMP.lineUpdate = 1 AND sMP.type = 1;";
        String updateKegLevels              = "UPDATE locationBeerBoardMap SET css = 1 WHERE location IN (970)";

       try {
           stmt                             = transconn.prepareStatement(updateKegLevels);
           stmt.executeUpdate();
           
           stmt                             = transconn.prepareStatement(selectLineUpdates);
           rs                               = stmt.executeQuery();
           while (rs.next()) {
               int colCount                 = 1;
               int locationId               = rs.getInt(colCount++);
               int postId                   = rs.getInt(colCount++);
               String boardName             = rs.getString(colCount++);
               String location              = rs.getString(colCount++);
               String city                  = rs.getString(colCount++);
               String state                 = rs.getString(colCount++);
               String zip                   = rs.getString(colCount++);
               String shortURL              = rs.getString(colCount++);
               int productId                = rs.getInt(colCount++);
               String twitterHandle         = rs.getString(colCount++);               
               int customerId               = rs.getInt(colCount++);
               if(twitterHandle == null) {
                   twitterHandle            = "";
               }

               stmt                         = transconn.prepareStatement(selectDetails);
               stmt.setInt(1, productId);
               rsDetails                    = stmt.executeQuery();
               if(rsDetails.next()) {
                   String product           = rsDetails.getString(1);
                   String abv               = rsDetails.getString(2);
                   String orgin             = rsDetails.getString(3);
                   String style             = HandlerUtils.nullToEmpty(rsDetails.getString(4));
                   String brewery           =  HandlerUtils.nullToEmpty(rsDetails.getString(5));
                   String twitter           = rsDetails.getString(6);
                   String brewName          = rsDetails.getString(7);
                   if(brewery==null && brewery.equals("") ){
                       brewery              = "NoBrewery.png";

                   } 
                   
                   if(twitter == null || twitter.equals("")) {
                       twitter              = " @ "+brewName;
                   }
                   if(twitterHandle == null|| twitterHandle.equals("")) {
                       twitterHandle        = "@ "+boardName;
                   }
                   File file                = new File("/home/midware/facebook/image.jpg");
                   file.delete();
                   file                     = new File("/home/midware/facebook/productHTML.htm");
                   file.delete();
                   file                     = new File("/home/midware/facebook/brewlogo.png");
                   file.delete();
                   file                     = new File("/home/midware/facebook/glasslogo.png");
                   file.delete();
                   writeHtml(boardName, location, city, zip,  product, abv, orgin, style, brewery,"Now on Tap");
                   //Now on Tap 'Full Beer Menu' @Location. Full Beer List: 'GoogleShortURL' @Brewery @BeerBoardTV
                   String facebookMessage   = "Now on Tap  \'" + product + "\'  @ " + boardName ;
                   String twitterMessage    = "Now on Tap \'" + product + "\' " +  twitterHandle ;
                   if(shortURL!=null) {
                       facebookMessage      = facebookMessage + " \n "  ;
                       twitterMessage       = twitterMessage + ". Full Beer List: " + shortURL ;
                   }
                   if((twitterMessage.length()+( twitter +" @BeerBoardTV").length() +2) < 110 ) {
                       twitterMessage           += twitter +" @BeerBoardTV";                   
                   } else if((twitterMessage.length()+(" @BeerBoardTV").length()+2) < 110) {
                       twitterMessage           += " @BeerBoardTV";                   
                   }
                   facebookMessage          += " @"+brewName +" @BeerBoardTV";
                   
                   
                   logger.debug("FB: " + facebookMessage);
                   logger.debug("TW: " + twitterMessage + "length:"+twitterMessage.length());
                   stmt                     = transconn.prepareStatement(selectToken);
                   stmt.setInt(1,locationId);
                   stmt.setInt(2,locationId);
                   rsFB                     = stmt.executeQuery();
                   while (rsFB.next()) {
                       String pageId        = rsFB.getString(1);
                       String fbId          = rsFB.getString(2);
                       String locationToken = rsFB.getString(3);
                       int type             = rsFB.getInt(4);
                       int lineUpdate       = rsFB.getInt(5);
                       if(type == 0) {
                           postOnFacebook(fbId, locationToken, facebookMessage,pageId,1,locationId,productId);
                       }
                   }
                   if(customerId !=205){
                    //   postOnFacebook("100000256298215", "access_token=CAACNB03nnL0BAHN0HizaDRjyGRLOsRoxI0L4yfCiylfkSGRL7YZCHIZBdBBTvXmu94E7ndJP3cen5rPHYW5e66xPT1ZBBezLchWWtZBHjq4GpvjXXST6TPpzZBtmZCxj4OOZBj3dC4v4ZBPR74Wfs2Lb9TaBX4tgqtgHEwr8JmErLbHNzA7KIlJdxog6NxBxlygZD&expires=5183639", facebookMessage,"670510736317171",1,locationId,productId);
                   //postOnFacebook("5515563", "access_token=CAACNB03nnL0BAIV98zMeXy2UR7TOMOMuSzqVpog8oz5WN24jF8xr7yq3gWVIudF8Jcx9sKEKFA2HXKnqbGpCl1MxIy4w9NnCKNIUGhW1O5OwxrmJ3JfBIvUI0G14EGyZBiBf8ShlkSMuOZCs0NDx743dZCXZBlEqlfDe7aMOlT8SSbS16gIQ3tIDZCrd4XWHLaW0fEezSAZCviM6hqHXmCoZCy6ajMlFZAAZD&expires=5123519", facebookMessage, locationId, productId);
                       
                   }
                   tweetStatus(locationId, twitterMessage, 1, productId,customerId);
               }
               
           }
           stmt                             = transconn.prepareStatement(updateLineUpdates);
           stmt.executeUpdate();
           
           updateTodayHours();
        } catch ( Exception sqle) {
            logger.dbError("Database error: "+sqle.toString());
            throw new HandlerException(sqle);
        } finally {
            close(rs);
            close(rsDetails);
            close(stmt);
        }
   }
   
   private void updateTodayHours() throws HandlerException {
       PreparedStatement stmt               = null;
       String clearHours                    = "DELETE FROM todayHours;";
       String insertHours                   = "INSERT INTO todayHours " +
                                            "(SELECT ID, eO, " +
                                            "SUBDATE(IF(IFNULL(x.close, '02:00:00')>'12:0:0',CONCAT(left(subdate(now(),1),11),IFNULL(x.close, '02:00:00')), " +
                                            "CONCAT(left(now(),11),IFNULL(x.close, '02:00:00'))), INTERVAL eO HOUR) Close, " +
                                            "SUBDATE(CONCAT(left(now(),11),IFNULL(x.preOpen, '07:00:00')), INTERVAL eO HOUR) preOpen, " +
                                            "SUBDATE(CONCAT(left(now(),11),IFNULL(x.open, '07:00:00')), INTERVAL eO HOUR) Open, " +
                                            "DATE_SUB(If(x.close2>'12:0:0',CONCAT(LEFT(NOW(),11),IFNULL(x.close2,'02:00:00')), " +
                                            "CONCAT(LEFT(ADDDATE(NOW(),1),11),IFNULL(x.close2,'02:00:00'))), INTERVAL eO HOUR) Close2  FROM (SELECT " +
                                            "CASE DAYOFWEEK(NOW()-1000000) " +
                                            "WHEN 1 THEN Right(lH.closeSun,8)  " +
                                            "WHEN 2 THEN Right(lH.closeMon,8) " +
                                            "WHEN 3 THEN Right(lH.closeTue,8)  " +
                                            "WHEN 4 THEN Right(lH.closeWed,8)  " +
                                            "WHEN 5 THEN Right(lH.closeThu,8)  " +
                                            "WHEN 6 THEN Right(lH.closeFri,8)  " +
                                            "WHEN 7 THEN Right(lH.closeSat,8) END close, " +
                                            "CASE DAYOFWEEK(NOW()) " +
                                            "WHEN 1 THEN Right(lH.preOpenSun,8) " +
                                            "WHEN 2 THEN Right(lH.preOpenMon,8) " +
                                            "WHEN 3 THEN Right(lH.preOpenTue,8) " +
                                            "WHEN 4 THEN Right(lH.preOpenWed,8) " +
                                            "WHEN 5 THEN Right(lH.preOpenThu,8) " +
                                            "WHEN 6 THEN Right(lH.preOpenFri,8) " +
                                            "WHEN 7 THEN Right(lH.preOpenSat,8) END preOpen, " +
                                            "CASE DAYOFWEEK(NOW()) " +
                                            "WHEN 1 THEN Right(lH.openSun,8) " +
                                            "WHEN 2 THEN Right(lH.openMon,8) " +
                                            "WHEN 3 THEN Right(lH.openTue,8) " +
                                            "WHEN 4 THEN Right(lH.openWed,8) " +
                                            "WHEN 5 THEN Right(lH.openThu,8) " +
                                            "WHEN 6 THEN Right(lH.openFri,8) " +
                                            "WHEN 7 THEN Right(lH.openSat,8) END open, " +
                                            "CASE DAYOFWEEK(NOW()) " +
                                            "WHEN 1 THEN Right(lH.closeSun,8)  " +
                                            "WHEN 2 THEN Right(lH.closeMon,8) " +
                                            "WHEN 3 THEN Right(lH.closeTue,8)  " +
                                            "WHEN 4 THEN Right(lH.closeWed,8)  " +
                                            "WHEN 5 THEN Right(lH.closeThu,8)  " +
                                            "WHEN 6 THEN Right(lH.closeFri,8) " +
                                            "WHEN 7 THEN Right(lH.closeSat,8) END close2, " +
                                            "l.easternOffset eO, " +
                                            "l.id ID " +
                                            "FROM location l LEFT JOIN locationHours lH ON lH.location = l.id) AS x);";
       try {
           stmt                             = transconn.prepareStatement(clearHours);
           stmt.executeUpdate();
           
           stmt                             = transconn.prepareStatement(insertHours);
           stmt.executeUpdate();           
       } catch ( Exception sqle) {
            logger.dbError("Database error: "+sqle.toString());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
        }
   }
   
   
    public static boolean httpFileExists(String URLName){
            try {
              HttpURLConnection.setFollowRedirects(false);
              HttpURLConnection con         = (HttpURLConnection) new URL(URLName).openConnection();
              con.setRequestMethod("HEAD");
              return (con.getResponseCode() == HttpURLConnection.HTTP_OK);
            }
            catch (Exception e) {
               e.printStackTrace();
               return false;
            }
        }


   private void postRewardOnSocialMedia(Element toHandle, Element toAppend) throws HandlerException {

        int rewardId                        = HandlerUtils.getRequiredInteger(toHandle, "rewardId");

        String selectReward                 = "SELECT bCR.rewardText, bCr.file, bCR.location FROM bevSyncCampaignReward bCR " +
                                            " LEFT JOIN bevSyncCampaign bC ON bC.id = bCR.campaign  LEFT JOIN bevSyncCreatives bCr ON bCr.id = bCR.creatives " +
                                            " WHERE bCR.id = ? AND bC.customer > 0;";
        String selectToken                  = "SELECT DISTINCT p.pageid, u.user_id, u.access_token ,p.type FROM usbnFacebook u LEFT JOIN usbnFacebookPage p ON u.user_id = p.fbid " +
                                            " WHERE p.location=? AND u.user_id!='100000256298215';";

        PreparedStatement stmt              = null;
        ResultSet rs                        = null;

        try {
            stmt                            = transconn.prepareStatement(selectReward);
            stmt.setInt(1,rewardId);
            rs                              = stmt.executeQuery();
            if(rs.next()) {
                String message              = rs.getString(1);
                File file                   = new File("/home/midware/facebook/reward/"+rs.getString(2));
                int location                = rs.getInt(3);

                stmt                        = transconn.prepareStatement(selectToken);
                stmt.setInt(1, location);
                rs                          = stmt.executeQuery();
                while (rs.next()) {
                    String pageId           = rs.getString(1);
                    String fbId             = rs.getString(2);
                    String locationToken    = rs.getString(3);
                    postImage(fbId, pageId, locationToken, "Get Your Reward for Saranac Brewing @ Tully's from 6:00 PM to 9:00 PM", location, rewardId, file, 4);
                }
                //postImage("5515563", "670510736317171","access_token=CAACNB03nnL0BAIV98zMeXy2UR7TOMOMuSzqVpog8oz5WN24jF8xr7yq3gWVIudF8Jcx9sKEKFA2HXKnqbGpCl1MxIy4w9NnCKNIUGhW1O5OwxrmJ3JfBIvUI0G14EGyZBiBf8ShlkSMuOZCs0NDx743dZCXZBlEqlfDe7aMOlT8SSbS16gIQ3tIDZCrd4XWHLaW0fEezSAZCviM6hqHXmCoZCy6ajMlFZAAZD&expires=5123519", message, location, rewardId, file, 4);
            }
        } catch ( Exception sqle) {
            logger.dbError("Database error: "+sqle.toString());
            throw new HandlerException(sqle);
        } finally {
            close(rs);
            close(stmt);
        }
   }

   private void postCreativeOnSocialMedia(Element toHandle, Element toAppend) throws HandlerException {

        int creativeId                      = HandlerUtils.getRequiredInteger(toHandle, "creativeId");
        int locationId                      = HandlerUtils.getRequiredInteger(toHandle, "locationId");
        String message                      = HandlerUtils.getRequiredString(toHandle, "message");

        String selectCreative               = "SELECT bCr.customer, bCr.file FROM bevSyncCreatives bCr WHERE bCr.id = ?;";
        String selectToken                  = "SELECT DISTINCT p.pageid, u.user_id, u.access_token ,p.type FROM usbnFacebook u LEFT JOIN usbnFacebookPage p ON u.user_id = p.fbid " +
                                            " WHERE p.location=? AND u.user_id != '100000256298215';";

        PreparedStatement stmt              = null;
        ResultSet rs                        = null;

        try {
            stmt                            = transconn.prepareStatement(selectCreative);
            stmt.setInt(1, creativeId);
            rs                              = stmt.executeQuery();
            if(rs.next()) {
                String fileName             = "http://beerboard.tv/USBN.BeerBoard.UI/bevSyncCustomers/" + String.valueOf(rs.getInt(1)) + "/" + rs.getString(2).replaceAll(" ", "%20");
                saveImage(fileName, "/home/midware/facebook/reward/" + rs.getString(2));
                File file                   = new File("/home/midware/facebook/reward/"+rs.getString(2));
                
                stmt                        = transconn.prepareStatement(selectToken);
                stmt.setInt(1, locationId);
                rs                          = stmt.executeQuery();
                while (rs.next()) {
                    String pageId           = rs.getString(1);
                    String fbId             = rs.getString(2);
                    String locationToken    = rs.getString(3);
                    postImage(fbId, pageId, locationToken, message, locationId, creativeId, file, 4);
                }
                postImage("5515563", "670510736317171","access_token=CAACNB03nnL0BAIV98zMeXy2UR7TOMOMuSzqVpog8oz5WN24jF8xr7yq3gWVIudF8Jcx9sKEKFA2HXKnqbGpCl1MxIy4w9NnCKNIUGhW1O5OwxrmJ3JfBIvUI0G14EGyZBiBf8ShlkSMuOZCs0NDx743dZCXZBlEqlfDe7aMOlT8SSbS16gIQ3tIDZCrd4XWHLaW0fEezSAZCviM6hqHXmCoZCy6ajMlFZAAZD&expires=5123519", message, locationId, creativeId, file, 4);
            }
        } catch ( Exception sqle) {
            logger.dbError("Database error: "+sqle.toString());
            throw new HandlerException(sqle);
        } finally {
            close(rs);
            close(stmt);
        }
   }

   private void postCustomSocialMediaUpdates(Element toHandle, Element toAppend) throws HandlerException {

       PreparedStatement stmt               = null;
       ResultSet rs                         = null;
       int locationId                       = HandlerUtils.getRequiredInteger(toHandle, "locationId");
       String message                       = HandlerUtils.getRequiredString(toHandle, "message");
       String selectToken                   = "SELECT DISTINCT p.pageid, u.user_id, u.access_token ,p.type FROM usbnFacebook u LEFT JOIN usbnFacebookPage p ON u.user_id =p.fbid " +
                                            " WHERE p.location=? AND u.user_id!='100000256298215';";

       try {
           stmt                             = transconn.prepareStatement(selectToken);
           stmt.setInt(1,locationId);
           rs                               = stmt.executeQuery();
           if (rs.next()) {
               String pageId                = rs.getString(1);
               String fbId                  = rs.getString(2);
               String locationToken         = rs.getString(3);
               int type                     = rs.getInt(4);
               if(type == 0) {
                   postOnFacebook(fbId, locationToken, message, pageId, "");
               }
           }

        } catch ( Exception sqle) {
            logger.dbError("Database error: "+sqle.toString());
            throw new HandlerException(sqle);
        } finally {
            close(rs);
            close(stmt);
        }
   }
   
   
   
   private void getLocationFeed(Element toHandle, Element toAppend) throws HandlerException {
       
       int locationId                       = HandlerUtils.getRequiredInteger(toHandle, "locationId");
       PreparedStatement stmt               = null;
       ResultSet rs                         = null, innerRS = null;
       HashMap<Integer, Integer> productList= new HashMap<Integer, Integer>();
       ArrayList<String> url                = new ArrayList<String>();
       url.add("http://beerboard.tv/USBN.BeerBoard.UI/Fonts/arial.ttf");
       url.add("http://lakoo.com/wp-content/uploads/2010/07/app_store_badge.png");
       url.add("http://clinked.com/wp-content/uploads/2012/10/google-play-logo.jpg");
       url.add("http://z3.ifrm.com/22/130/0/f663592/med_mac_App_Store_Badge_EN.png");
       url.add("http://androiddevelopmentexperts.files.wordpress.com/2013/01/android-logo.png");
       url.add("http://files.softicons.com/download/system-icons/apple-logo-icons-by-thvg/png/512/Apple%20logo%20icon%20-%20Aluminum.png");
        
       String selectLocationProducts        = "SELECT l.id FROM line l LEFT JOIN bar b ON b.id = l.bar WHERE b.location = ? AND l.status = 'RUNNING' AND l.product = ?;";
       String selectLocationSponsorBanners  = "SELECT bSCr.brewery, bSCr.product, bSC.customer, bSCr.file, bSCr.type  FROM bevSyncCampaign bSC " +
                                            " LEFT JOIN bevSyncCampaignLocations bSCL ON bSCL.campaign = bSC.id " +
                                            " LEFT JOIN bevSyncCampaignCreatives bSCC ON bSCC.campaign = bSC.id LEFT JOIN bevSyncCreatives bSCr ON bSCr.id = bSCC.creatives " +
                                            " WHERE bSCL.location = ? AND NOW() BETWEEN bSC.start AND bSC.end AND NOW() < bSCr.validity AND bSCr.type = 3;";
       String selectLocationProductLikes    = "SELECT bMU.username, p.name, bMU.avatar, p.id FROM bbtvMobileUserRating bMUR LEFT JOIN bbtvMobileUser bMU ON bMU.id = bMUR.user " +
                                            " LEFT JOIN product p ON p.id = bMUR.product WHERE bMUR.location = ? AND bMUR.feed = 0; ";
       String updateLocationProductLikes    = "UPDATE bbtvMobileUserRating SET feed = 1 WHERE location = ? AND product = ? AND feed = 0 ";
       String selectLocationCheckins        = "SELECT bMU.username, bMU.avatar FROM bbtvMobileUserCheckin bMUC LEFT JOIN bbtvMobileUser bMU ON bMU.id = bMUC.user " +
                                            "  WHERE bMUC.location = ? AND bMUC.feed = 0; ";
       String updateLocationCheckins        = "UPDATE bbtvMobileUserCheckin SET feed = 1 WHERE location = ? AND feed = 0 ";

       for(int i=1;i<=3;i++) {
               Element mobileproduct        = toAppend.addElement("locationFeed");
               mobileproduct.addElement("html").addText(getFeedTemplate1(i, locationId).toString());
               mobileproduct.addElement("css").addText(HandlerUtils.nullToEmpty(getFeedCSS()));
               mobileproduct.addElement("visibility").addText(HandlerUtils.nullToEmpty("5000"));
       }

       try {
           logger.debug("Location Sponsor Banners");
           stmt                             = transconn.prepareStatement(selectLocationSponsorBanners);
           stmt.setInt(1,locationId);
           rs                               = stmt.executeQuery();
           while (rs.next()) {
               int brewery                  = rs.getInt(1);
               int product                  = rs.getInt(2);


               stmt                         = transconn.prepareStatement(selectLocationProducts);
               stmt.setInt(1,locationId);
               stmt.setInt(2,product);
               innerRS                      = stmt.executeQuery();
               if (innerRS.next()) {
                   String customer          = rs.getString(3);
                   String banner            = rs.getString(4);
                   int creativeType         = rs.getInt(5);
                   
                   url.add("http://beerboard.tv/USBN.BeerBoard.UI/bevSyncCustomers/" + customer + "/" + banner.trim().replaceAll("\'", "%27").replaceAll(" ", "%20"));
                   String message           = getFeedSponsorBanner(banner).toString();

                   Element mobileproduct    = toAppend.addElement("locationFeed");
                   mobileproduct.addElement("html").addText(message);
                   mobileproduct.addElement("css").addText(HandlerUtils.nullToEmpty(getFeedCSS()));
                   mobileproduct.addElement("visibility").addText(HandlerUtils.nullToEmpty("15000"));
               }
           }

           logger.debug("Location Product Likes");
           stmt                            = transconn.prepareStatement(selectLocationProductLikes);
           stmt.setInt(1,locationId);
           rs                              = stmt.executeQuery();
           while (rs.next()) {
               String logo                 = "";
               String[] avatarString       = rs.getString(3).split("/");
               if (avatarString != null) {
                   logo                    = avatarString[avatarString.length - 1];
               }
               logger.debug("Getting info for: " + rs.getString(2));
               String message              = getFeedProductLikes(logo, rs.getString(1), rs.getString(2)).toString();
               logger.debug("message: " + message);
               Element mobileproduct       = toAppend.addElement("locationFeed");
               mobileproduct.addElement("html").addText(message);
               mobileproduct.addElement("css").addText(HandlerUtils.nullToEmpty(getFeedCSS()));
               mobileproduct.addElement("visibility").addText(HandlerUtils.nullToEmpty("10000"));
               url.add(rs.getString(3));
               
               stmt                        = transconn.prepareStatement(updateLocationProductLikes);
               stmt.setInt(1,locationId);
               stmt.setInt(2,rs.getInt(4));
               stmt.executeUpdate();
           }

           logger.debug("Location Checkins");
           stmt                            = transconn.prepareStatement(selectLocationCheckins);
           stmt.setInt(1,locationId);
           rs                              = stmt.executeQuery();
           while (rs.next()) {
               String logo                 = "";
               String[] avatarString       = rs.getString(2).split("/");
               if (avatarString != null) {
                   logo                    = avatarString[avatarString.length - 1];
               }
               String message              = getFeedBarCheckin(logo, rs.getString(1)).toString();
               
               Element mobileCheckin       = toAppend.addElement("locationFeed");
               mobileCheckin.addElement("html").addText(message);
               mobileCheckin.addElement("css").addText(HandlerUtils.nullToEmpty(getFeedCSS()));
               mobileCheckin.addElement("visibility").addText(HandlerUtils.nullToEmpty("10000"));
               url.add(rs.getString(2));
           }
            
           for(int i=0;i<url.size();i++) {
               Element image                   = toAppend.addElement("imageURL");
               image.addElement("url").addText(url.get(i));
           }

           stmt                            = transconn.prepareStatement(updateLocationCheckins);
           stmt.setInt(1,locationId);
           stmt.executeUpdate();

       } catch (SQLException sqle) {
           logger.dbError("Database error in getLocationFeed: "+sqle.toString());
           throw new HandlerException(sqle);
       } finally {
           close(stmt);
       }
   }
   
    private StringBuilder getFeedTemplate1(int template, int locationId) throws HandlerException {
        
        StringBuilder beerInfo               = new StringBuilder();
        switch(template) {
            case 1:
                beerInfo.append("<table width='50%' height='100%' border='0' cellspacing='2' cellpadding='0' style='padding-left: 15px;'><tr>");
                beerInfo.append("<td align='left'><div align=\"left\"><img src =\"/mnt/sdcard/feedLogo/app_store_badge.png\" width=\"150\" height=\"50\"/></div></td>");
                beerInfo.append("<td><div align=\"left\"><img src=\"/mnt/sdcard/feedLogo/google-play-logo.jpg\" width=\"150\" height=\"50\"/></div></td> </tr>");
                beerInfo.append( "</table>");
                break;
            case 2:    
                beerInfo.append("<table width='100%' height='100%' border='0' cellspacing='2' cellpadding='0' style='padding-left: 15px;'><tr>");
                beerInfo.append("<td><div align=\"left\" class=\"feed\">Download BeerBoard Mobile</div></td></tr>");
                beerInfo.append( "</table>");
                break;
            case 3:
                beerInfo.append("<table width='100%' height='100%' border='0' cellspacing='2' cellpadding='0' style='padding-left: 15px;'><tr>");
                beerInfo.append("<td><div align=\"left\" class=\"feed\">BeerBoard.TV Mobile ID: " + locationId  + "</div></td></tr>");
                beerInfo.append("</table>");
                break;
        }
            
        return beerInfo;
   }

    private StringBuilder getFeedProductLikes(String logo, String user, String product) throws HandlerException {

        StringBuilder beerInfo               = new StringBuilder();
        beerInfo.append("<table width='100%' height='100%' border='0' cellspacing='2' cellpadding='0' style='padding-left: 15px;'><tr>");
        beerInfo.append("<td><div align=\"left\" class=\"feed\">" + (logo.length() > 0 ? "<img src =\"/mnt/sdcard/feedLogo/" +  logo + "\" width=\"50\" height=\"50\"/> " : " ") + user + " likes " + product + "</div></td></tr>");
        beerInfo.append( "</table>");
        return beerInfo;
   }
    
    
    private StringBuilder getFeedBarCheckin(String logo, String user) throws HandlerException {

        StringBuilder beerInfo               = new StringBuilder();
        beerInfo.append("<table width='100%' height='100%' border='0' cellspacing='2' cellpadding='0' style='padding-left: 15px;'><tr>");
        beerInfo.append("<td><div align=\"left\" class=\"feed\">" + (logo.length() > 0 ? "<img src =\"/mnt/sdcard/feedLogo/" +  logo + "\" width=\"50\" height=\"50\"/> " : " ") + user + " checked-in </div></td></tr>");
        beerInfo.append( "</table>");
        return beerInfo;
    }


    private StringBuilder getFeedSponsorBanner(String logo) throws HandlerException {

        StringBuilder beerInfo               = new StringBuilder();
        beerInfo.append("<table width='100%' height='100%' border='0' cellspacing='2' cellpadding='0' style='padding-left: 15px;'><tr>");
        beerInfo.append("<td><div align=\"left\" class=\"feed\">" + (logo.length() > 0 ? "<img src =\"/mnt/sdcard/feedLogo/" +  logo + "\"/> " : " ") + "</div></td></tr>");
        beerInfo.append( "</table>");
        return beerInfo;
    }
        
        
     private String getFeedCSS() throws HandlerException {

         String  css                        = "@charset 'utf-8'; " +
                                            " @font-face { font-family: 'ChalkDust'; src: url('file:///android_asset/fonts/chalkdust.ttf'); } " +
                                            " @font-face { font-family: 'PPETRIAL'; src: url('file:///android_asset/fonts/PPETRIAL.otf'); } " +
                                            " @font-face { font-family: 'Arial'; src: url('file:///sdcard/fonts/arial.otf'); } " +
                                            " .feed{ font-family: 'Arial'; font-size:25px; font-weight:bold; color: #ff9900; overflow:hidden;  white-space:nowrap; } " ;
                                            
        return css;
   }
     
    private void addUserRating(Element toHandle, Element toAppend) throws HandlerException {
        
        int platform                        = HandlerUtils.getOptionalInteger(toHandle, "platform");
        int location                        = HandlerUtils.getRequiredInteger(toHandle, "location");
        int product                         = HandlerUtils.getRequiredInteger(toHandle, "product");        
        int user                            = HandlerUtils.getOptionalInteger(toHandle, "user");
        int rate                            = HandlerUtils.getOptionalInteger(toHandle, "rate");
        String customerMessage              = HandlerUtils.getOptionalString(toHandle, "fbMessage");
        
        String checkUserRating              = "SELECT IF(COUNT(id) > 3, 1, 0) FROM bbtvMobileUserRating WHERE user = ? AND date > CONCAT(LEFT(NOW(), 11), '07:00:00');";
        String checkFavouriteBeer           = "SELECT id FROM favoriteBeer WHERE user=? AND product = ?;";
        String insertFavouriteBeer           = "INSERT INTO favoriteBeer (user,product) VALUES (?,?);";

        PreparedStatement stmt              = null;
        ResultSet rs                        = null;
            
        try {
            
            if(platform == 7) {
                 String userName            = HandlerUtils.getRequiredString(toHandle, "userName");
                 String userId              = HandlerUtils.getOptionalString(toHandle, "userId");
                 String email               = HandlerUtils.getOptionalString(toHandle, "email");
                 String token               = HandlerUtils.getOptionalString(toHandle, "token");
                 user                       = addUpdateMobileUser(userName, userId, token, email, "", "", "", platform,toAppend);
            }
            if(user > 0){
                stmt                        = transconn.prepareStatement(checkFavouriteBeer);
                stmt.setInt(1,user);
                stmt.setInt(2,product);
                rs                          = stmt.executeQuery();
                if (!rs.next()) {
                     stmt                   = transconn.prepareStatement(insertFavouriteBeer);
                     stmt.setInt(1,user);
                     stmt.setInt(2,product);
                     stmt.executeUpdate();
                }
            }
            
            stmt                            = transconn.prepareStatement(checkUserRating);
            stmt.setInt(1,user);
            rs                              = stmt.executeQuery();
            if (rs.next() && (rs.getInt(1) < 1)) {

                String insertUserRating     = "INSERT INTO bbtvMobileUserRating (location, user, product, rating, date) VALUES (?, ?, ?, ?,?); ";
                String updateUserTVAccess   = "UPDATE locationBeerBoardMap SET feed = 1 WHERE location = ? ";
                String selectLocationFeed   = "SELECT bMU.fbID, bMU.token, bMU.username, p.name, l.boardname FROM bbtvMobileUserRating bMUR LEFT JOIN bbtvMobileUser bMU ON bMU.id = bMUR.user " +
                                            " LEFT JOIN product p ON p.id = bMUR.product LEFT JOIN location l ON l.id = bMUR.location  WHERE bMUR.location = ? AND bMU.id = ? AND bMUR.product =?; ";
                String selectToken          = "SELECT DISTINCT p.pageid, u.user_id,  u.access_token, IFNULL((SELECT likes FROM socialMediaPost WHERE location=? AND type=1),0) FROM usbnFacebook u LEFT JOIN usbnFacebookPage p ON u.user_id =p.fbid where p.location=? AND p.location != 856 and u.user_id!='5515563';";

                Calendar currentDate        = Calendar.getInstance();
                String checkin              = dbDateFormat.format(currentDate.getTime());
                ArrayList<String> pageId    = new ArrayList<String>();
                ArrayList<String> fbId      = new ArrayList<String>();
                ArrayList<String> locationToken
                                            = new ArrayList<String>();

                stmt                        = transconn.prepareStatement(selectToken);
                stmt.setInt(1,location);
                stmt.setInt(2,location);
                rs                          = stmt.executeQuery();
                while (rs.next()) {
                    if(rs.getInt(4)>0){
                        pageId.add(rs.getString(1));
                        fbId.add(rs.getString(2));
                        locationToken.add(rs.getString(3));
                    }
                }

                stmt                        = transconn.prepareStatement(insertUserRating);
                stmt.setInt(1,location);
                stmt.setInt(2,user);
                stmt.setInt(3,product);
                stmt.setInt(4,rate);
                stmt.setString(5, checkin);
                stmt.executeUpdate();

                stmt                        = transconn.prepareStatement(updateUserTVAccess);
                stmt.setInt(1,location);
                stmt.executeUpdate();

                stmt                        = transconn.prepareStatement(selectLocationFeed);
                stmt.setInt(1,location);
                stmt.setInt(2,user);
                stmt.setInt(3,product);
                rs = stmt.executeQuery();
                if (rs.next()) {
                    int colCount            = 1;
                    String fbid             = rs.getString(colCount++);
                    String token            = rs.getString(colCount++);
                    String msg              = rs.getString(colCount++) + "  likes  " + rs.getString(colCount++) + " @ " + rs.getString(colCount++);
                    if(customerMessage!=null && !customerMessage.equals("")){
                        msg                 = customerMessage;
                    }
                     logger.debug(fbid + " : " + token + " : " + customerMessage);
                    logger.debug(fbid + " : " + token + " : " + msg);
                    if(token.length() > 10) {
                        postFacebookStatus(fbid,token,msg);
                    }
                    if(fbId.size()>0) {
                        postOnPage(fbId, pageId, locationToken, msg, fbid);
                    }
                    tweetStatus(location,msg,5,product,0);
                }
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error in addUserRating: "+sqle.toString());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
        }
    }
    
    public void postOnPage(ArrayList<String> fbId,ArrayList<String> pageId, ArrayList<String> accessToken, String message,String fbid) {
          for(int i=0;i<fbId.size();i++) {
              String user_id                = fbId.get(i);
              String access_token           = accessToken.get(i);
              String page_id                = pageId.get(i);
              if(access_token!=null) {
                  try {
                      postOnFacebook(user_id, access_token, message, page_id,fbid);
                  } catch (Exception e) {
                      logger.dbError("Post On FB Page "+e.toString());
                      e.printStackTrace();
                  }
              }
          }
      }
   
   
    public void postOnFacebook(String user_id, String access_token, String message, String pageId,String tagId) throws Exception {
        if(access_token!=null) {
            try {
                String g                    = "https://graph.facebook.com/fql?q=SELECT+page_id%2c+name%2C+pic%2C+access_token%2c+fan_count+FROM+page+WHERE+page_id+IN+(select+page_id+from+page_admin+where+uid="+user_id+")+&"+access_token;
                URL u = new URL(g);
                signHttpsCertificate();
                URLConnection c             = u.openConnection();
                HttpsURLConnection conn     = (HttpsURLConnection) u.openConnection();
                BufferedReader in           = new BufferedReader(new InputStreamReader(conn.getInputStream()));
                String inputLine;
                StringBuffer b              = new StringBuffer();
                while ((inputLine = in.readLine()) != null)
                    b.append(inputLine + "\n");
                in.close();
                String graph                = b.toString();
                JSONObject json             = new JSONObject(graph);
                JSONArray fa1               = new JSONArray(String.valueOf(json.get("data")));
                for(int i=0;i<fa1.length();i++) {
                    String accesstoken      = String.valueOf(fa1.getJSONObject(i).get("access_token"));
                    if(!accesstoken.equals("null")) {
                        String pageid       = fa1.getJSONObject(i).getString("page_id");
                        if(pageid.equals(pageId)) {
                            postOnFeed(accesstoken, pageid, message,tagId);
                        }
                    }                    
                }
            } catch (Exception e) {
                logger.dbError("post On FB Wall "+e.toString());
                
            }
        }
    }
      
    public void postOnFeed(String access_token, String page_id, String message,String tag )throws HandlerException  {
        PreparedStatement stmt              = null;
        if(access_token!=null) {
            try {
                String tar                  ="{'countries':['US']}";
                String url                  = "https://graph.facebook.com/"+page_id+"/feed?access_token="+access_token.replace("&access_token", "")+"&method=post&message="+message+"&feed_targeting="+tar;
                logger.debug(url);
                URL urL                     = new URL(URIUtil.encodeQuery(url));
                getData(urL);
                
            } catch (Exception sqle) {
                logger.debug(sqle.getMessage());
            throw new HandlerException(sqle);
        }  finally {
            close(stmt);
        }
        }
    }
    
    
     
    private void getMobileUserId(Element toHandle, Element toAppend) throws HandlerException {
        
        int platform                        = HandlerUtils.getOptionalInteger(toHandle, "platform");
        String deviceToken                  = HandlerUtils.getOptionalString(toHandle, "deviceToken");
        String deviceId                     = HandlerUtils.getOptionalString(toHandle, "deviceId");
        String osVersion                    = HandlerUtils.getOptionalString(toHandle, "osVersion");
        osVersion                           = HandlerUtils.nullToString(osVersion, "0");
        logger.debug("OsVersion:"+osVersion);
        String selectProfile                = "SELECT m.id,m.deviceToken,m.username,m.fbID,m.email,m.token,m.avatar,styles "
                                            + " FROM bbtvMobileUser m WHERE m.deviceToken = ? and m.platform = ?; ";
        String selectAndroidProfile         = "SELECT m.id,m.deviceToken,m.username,m.fbID,m.email,m.token,m.avatar,styles "
                                            + " FROM bbtvMobileUser m WHERE m.deviceId = ? and m.platform = ?; ";
         String selectLastId                = "SELECT m.id FROM bbtvMobileUser m order by id DESC LIMIT 1; ";
        PreparedStatement stmt              = null;
        ResultSet rs                        = null;
        if(deviceToken==null){
            deviceToken                     = "";
        }
        String userName                 = "Guest";
        try {      
            if(deviceToken.equals("")) {
                stmt                       = transconn.prepareStatement(selectLastId);
                rs = stmt.executeQuery();
                if (rs.next()) {
                    userName                    ="Guest-"+String.valueOf((rs.getInt(1))+1);
                }
                //userName                    = RandomAlphaNumericString(8);
                int user                    = 0;
                if(platform == 3){
                    user                    = addUpdateMobileUser(userName,"","","","","",deviceToken,platform,toAppend);
                } else if(platform ==2 && deviceId!=null && !deviceId.equals("")){
                    user                    = addUpdateAndroidMobileUser(userName,"","","","","",deviceToken,platform,toAppend,deviceId);
                } else {
                    user                    = addUpdateMobileUser(userName,"","","","","",deviceToken,platform,toAppend);
                }
                toAppend.addElement("customerId").addText(String.valueOf(user));
                toAppend.addElement("userName").addText(HandlerUtils.nullToEmpty(userName));
                toAppend.addElement("userId").addText(HandlerUtils.nullToEmpty(""));
                toAppend.addElement("email").addText(HandlerUtils.nullToEmpty(""));
                toAppend.addElement("token").addText(HandlerUtils.nullToEmpty(""));
                toAppend.addElement("avatar").addText(HandlerUtils.nullToEmpty(""));
                toAppend.addElement("styles").addText(HandlerUtils.nullToEmpty(""));
            } else {
                if(platform ==2 && deviceId!=null && !deviceId.equals("") ){
                    stmt                    = transconn.prepareStatement(selectAndroidProfile);
                    stmt.setString(1,deviceId);
                    stmt.setInt(2,platform);
                    rs                      = stmt.executeQuery();
                    if (rs.next()) {
                        int customerId      = rs.getInt(1);
                        toAppend.addElement("customerId").addText(String.valueOf(rs.getInt(1)));
                        toAppend.addElement("email").addText(HandlerUtils.nullToEmpty(rs.getString(5)));
                        String token        = HandlerUtils.nullToEmpty(rs.getString(6));
                        if(checkFacebookToken(token)>0){
                            toAppend.addElement("token").addText(HandlerUtils.nullToEmpty(rs.getString(6)));
                            toAppend.addElement("userId").addText(HandlerUtils.nullToEmpty(rs.getString(4)));
                        } else {
                            stmt              = transconn.prepareStatement("UPDATE bbtvMobileUser set fbID='', token='' WHERE id=?");
                            stmt.setInt(1,customerId);
                            stmt.executeUpdate();
                            toAppend.addElement("userId").addText(HandlerUtils.nullToEmpty(""));
                            toAppend.addElement("token").addText(HandlerUtils.nullToEmpty(""));
                        }
                        String avatar       = rs.getString(7);
                        if(avatar== null || avatar.equals("")) {
                            avatar          = "http://beerboard.tv/USBN.BeerBoard.UI/Images/Assets/Generic/profile-pic.png";
                        }
                        toAppend.addElement("avatar").addText(avatar);
                        String styles       = HandlerUtils.nullToEmpty(rs.getString(8));
                        String oldUser      = rs.getString(3);
                        if(oldUser!=null && !oldUser.equals("")){
                            toAppend.addElement("userName").addText(HandlerUtils.nullToEmpty(rs.getString(3)));
                        } else {
                            if(deviceToken.length() > 5){
                                userName    += deviceToken.substring(deviceToken.length()-6,deviceToken.length()-1);
                            }
                            stmt            = transconn.prepareStatement("UPDATE bbtvMobileUser SET username = ? WHERE id= ?;");
                            stmt.setString(1, userName);
                            stmt.setInt(2, rs.getInt(1));
                            stmt.executeUpdate();
                            toAppend.addElement("userName").addText(HandlerUtils.nullToEmpty(userName));
                        }
                        stmt                = transconn.prepareStatement("UPDATE bbtvMobileUser SET deviceToken = ? WHERE id= ?;");
                        stmt.setString(1, deviceToken);
                        stmt.setInt(2, rs.getInt(1));
                        stmt.executeUpdate();
                        
                        if(!styles.equals("")){
                            stmt            = transconn.prepareStatement("SELECT GROUP_CONCAT(style)  FROm beerStyles WHERE id IN("+styles+")");
                            rs = stmt.executeQuery();
                            if (rs.next()) {
                                toAppend.addElement("styles").addText(HandlerUtils.nullToEmpty(rs.getString(1)));
                            }
                        }else{
                            toAppend.addElement("styles").addText(HandlerUtils.nullToEmpty(rs.getString(8)));
                        }
                    } else {
                        if(deviceToken!=null&&deviceToken.length() > 5){
                            userName        += deviceToken.substring(deviceToken.length()-6,deviceToken.length()-1);
                            int user        = addUpdateAndroidMobileUser(userName,"","","","","",deviceToken,platform,toAppend,deviceId);
                            toAppend.addElement("customerId").addText(String.valueOf(user));
                        } else {
                            stmt            = transconn.prepareStatement(selectLastId);
                            rs              = stmt.executeQuery();
                            if (rs.next()) {
                                userName    ="Guest-"+String.valueOf((rs.getInt(1))+1);
                            }
                            int user        = addUpdateAndroidMobileUser(userName,"","","","","",deviceToken,platform,toAppend,deviceId);
                            toAppend.addElement("customerId").addText(String.valueOf(user));
                        }
                        toAppend.addElement("userName").addText(HandlerUtils.nullToEmpty(userName));
                        toAppend.addElement("userId").addText(HandlerUtils.nullToEmpty(""));
                        toAppend.addElement("email").addText(HandlerUtils.nullToEmpty(""));
                        toAppend.addElement("token").addText(HandlerUtils.nullToEmpty(""));
                        toAppend.addElement("avatar").addText(HandlerUtils.nullToEmpty("http://beerboard.tv/USBN.BeerBoard.UI/Images/Assets/Generic/profile-pic.png"));
                        toAppend.addElement("styles").addText(HandlerUtils.nullToEmpty(""));
                    }
                } else{
                    stmt                    = transconn.prepareStatement(selectProfile);
                    stmt.setString(1,deviceToken);
                    stmt.setInt(2,platform);
                    rs                      = stmt.executeQuery();
                    if (rs.next()) {
                        int customerId      = rs.getInt(1);
                        toAppend.addElement("customerId").addText(String.valueOf(rs.getInt(1)));
                        toAppend.addElement("email").addText(HandlerUtils.nullToEmpty(rs.getString(5)));
                        String token        = HandlerUtils.nullToEmpty(rs.getString(6));
                        if(checkFacebookToken(token)>0){
                            toAppend.addElement("token").addText(HandlerUtils.nullToEmpty(rs.getString(6)));
                            toAppend.addElement("userId").addText(HandlerUtils.nullToEmpty(rs.getString(4)));
                        } else {
                            stmt            = transconn.prepareStatement("UPDATE bbtvMobileUser set fbID='', token='' WHERE id=?");
                            stmt.setInt(1,customerId);
                            stmt.executeUpdate();
                            toAppend.addElement("userId").addText(HandlerUtils.nullToEmpty(""));
                            toAppend.addElement("token").addText(HandlerUtils.nullToEmpty(""));
                        }
                        String avatar       = rs.getString(7);
                        if(avatar== null || avatar.equals("")) {
                            avatar          = "http://beerboard.tv/USBN.BeerBoard.UI/Images/Assets/Generic/profile-pic.png";
                        }
                        toAppend.addElement("avatar").addText(avatar);
                        String styles       = HandlerUtils.nullToEmpty(rs.getString(8));
                        String oldUser      = rs.getString(3);
                        if(oldUser!=null && !oldUser.equals("")){
                            toAppend.addElement("userName").addText(HandlerUtils.nullToEmpty(rs.getString(3)));
                        } else {
                            if(deviceToken.length() > 5){
                                userName    += deviceToken.substring(deviceToken.length()-6,deviceToken.length()-1);
                            }
                            stmt            = transconn.prepareStatement("UPDATE bbtvMobileUser SET username = ? WHERE id= ?;");
                            stmt.setString(1, userName);
                            stmt.setInt(2, rs.getInt(1));
                            stmt.executeUpdate();
                            toAppend.addElement("userName").addText(HandlerUtils.nullToEmpty(userName));
                        }
                        if(!styles.equals("")){
                            stmt            = transconn.prepareStatement("SELECT GROUP_CONCAT(style)  FROm beerStyles WHERE id IN("+styles+")");
                            rs              = stmt.executeQuery();
                            if (rs.next()) {
                                toAppend.addElement("styles").addText(HandlerUtils.nullToEmpty(rs.getString(1)));
                            }
                        } else {
                            toAppend.addElement("styles").addText(HandlerUtils.nullToEmpty(rs.getString(8)));
                        }
                    } else {
                        if(deviceToken!=null&&deviceToken.length() > 5){
                            userName        += deviceToken.substring(deviceToken.length()-6,deviceToken.length()-1);
                            int user        = addUpdateMobileUser(userName,"","","","","",deviceToken,platform,toAppend);
                            toAppend.addElement("customerId").addText(String.valueOf(user));
                        } else {
                            stmt            = transconn.prepareStatement(selectLastId);
                            rs              = stmt.executeQuery();
                            if (rs.next()) {
                                userName    = "Guest-"+String.valueOf((rs.getInt(1))+1);
                            }
                            int user        = addUpdateMobileUser(userName,"","","","","",deviceToken,platform,toAppend);
                            toAppend.addElement("customerId").addText(String.valueOf(user));
                        }
                        toAppend.addElement("userName").addText(HandlerUtils.nullToEmpty(userName));
                        toAppend.addElement("userId").addText(HandlerUtils.nullToEmpty(""));
                        toAppend.addElement("email").addText(HandlerUtils.nullToEmpty(""));
                        toAppend.addElement("token").addText(HandlerUtils.nullToEmpty(""));
                        toAppend.addElement("avatar").addText(HandlerUtils.nullToEmpty("http://beerboard.tv/USBN.BeerBoard.UI/Images/Assets/Generic/profile-pic.png"));
                        toAppend.addElement("styles").addText(HandlerUtils.nullToEmpty(""));
                    }
                } 
            }
        } catch (Exception sqle) {
            logger.dbError("Database error in addUserRating: "+sqle.toString());
            throw new HandlerException(sqle);
        }  finally {
            close(stmt);
            close(rs);
        }
    } 
    
    
    
    
    private void addUserProfile(Element toHandle, Element toAppend) throws HandlerException {
        
        int customer                        = HandlerUtils.getOptionalInteger(toHandle, "user");
        String token                        = HandlerUtils.getOptionalString(toHandle, "token");
        String styles                       = HandlerUtils.getOptionalString(toHandle, "styles");
        String userName                     = HandlerUtils.getOptionalString(toHandle, "userName");

        logger.debug("usern"+userName);

        if(userName!=null&&!userName.equals("") ){
            userName                        = checkValidName(customer,userName) ;
            toAppend.addElement("userName").addText(userName);
        }
        
        if(customer > 0) {
            if(token!=null || userName!=null){
                updateFacebookToken(customer,token,userName);
            }
            if(styles!=null) {
                updateMobileUserStyles(customer,styles);
            }
            toAppend.addElement("customerId").addText(String.valueOf(customer));
        } else {            
            String userId                       = HandlerUtils.getOptionalString(toHandle, "userId");
            String email                        = HandlerUtils.getOptionalString(toHandle, "email");
            String style                        = HandlerUtils.getOptionalString(toHandle, "style");            
            String deviceToken                  = HandlerUtils.getOptionalString(toHandle, "deviceToken");
            int platform                        = HandlerUtils.getOptionalInteger(toHandle, "platform");
            int user                            = addUpdateMobileUser(userName, userId, token, email, style, styles, deviceToken, platform,toAppend);
            toAppend.addElement("customerId").addText(String.valueOf(user));
        }
        
    } 
    
    private void updateFacebookToken(int user,String token,String userName) throws HandlerException {
        PreparedStatement stmt              = null;
        ResultSet rs                        = null;
        try {
            if(token!= null&&!token.equals("")) {
            stmt                = transconn.prepareStatement("UPDATE bbtvMobileUser SET token= ? WHERE id =? ");
            stmt.setString(1, token);
            stmt.setInt(2, user);
            stmt.executeUpdate();
            }
            
            if(userName!= null&&!userName.equals("")) {                
                stmt                = transconn.prepareStatement("UPDATE bbtvMobileUser SET username= ? WHERE id =? ");
                stmt.setString(1, userName);
                stmt.setInt(2, user);
                stmt.executeUpdate();
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error in addUserRating: "+sqle.toString());
            throw new HandlerException(sqle);
        }  finally {
            close(stmt);
            close(rs);
        }
        
    }
    
     private void updateMobileUserStyles(int user,String styles) throws HandlerException {
        PreparedStatement stmt              = null;
        ResultSet rs                        = null;
        String selectStyles                 = "SELECT GROUP_CONCAT(DISTINCT bS.id ORDER BY bS.id SEPARATOR ',') FROM beerStyles bS  WHERE bS.style IN ("+styles+");";
        String stylesId                     = "";
        try {
            if(styles!= null&&!styles.equals("")) {
                stmt                        = transconn.prepareStatement(selectStyles);           
                rs                          = stmt.executeQuery();
                if (rs.next()) {
                    stylesId                = rs.getString(1);
                    logger.debug(stylesId);
                    stmt                    = transconn.prepareStatement("UPDATE bbtvMobileUser SET styles= ? WHERE id =? ");
                    stmt.setString(1, stylesId);
                    stmt.setInt(2, user);
                    stmt.executeUpdate();
                }
             }
        } catch (SQLException sqle) {
            logger.dbError("Database error in addUserRating: "+sqle.toString());
            throw new HandlerException(sqle);
        }  finally {
            close(stmt);
            close(rs);
        }
        
    }
     
    
    private int addUpdateMobileUser(String userName, String userId, String token, String email, String style, String styles, String deviceToken,
            int platform,Element toAppend) throws HandlerException {
        
        int user                            = 0;
        if(userName == null) {
            userName                        = "";
        }
        if(userId == null || userId.equals("0")) {
            userId                          = "";
        }
        if(token == null) {
            token                           = "";
        }
        if(email == null) {
            email                           = "";
        }
        if(style == null) {
            style                           = "";
        }
        if(styles == null) {
            styles                          = "";
        }
        if(deviceToken == null) {
            deviceToken                     = "";
        }
        
        String avatar                       = null;
        String insertProfile                = "INSERT INTO bbtvMobileUser (username, fbID, token, email, registration, style, avatar, deviceToken, platform, styles) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?); ";
        String updateProfile                = "UPDATE bbtvMobileUser SET fbID = ?, token = ?, style = ?, username = ?, avatar = ?, arrival=null, deviceToken = ?,styles = ?, email =? WHERE id = ? and platform = ?; ";
        String selectProfile                = "SELECT id,deviceToken FROM bbtvMobileUser WHERE deviceToken = ? and platform = ?; ";
        String selectFBProfile              = "SELECT id,fbID FROM bbtvMobileUser WHERE email = ? and platform = ?;";
        String selectStyle                  = "SELECT sL.id FROM  styleLogo sL WHERe sL.style = ?;";
        String selectStyles                 = "SELECT GROUP_CONCAT(DISTINCT bS.id ORDER BY bS.id SEPARATOR ',') FROM beerStyles bS  WHERE bS.style IN ("+styles+");";
        String selectFBUser                 = "SELECT id, styles,deviceToken FROM bbtvMobileUser WHERE fbID = ? and platform = ? ORDER by arrival DESC,id DESC LIMIT 1;";
        int styleId                         = 0;
        String stylesId                     = "";
       
        PreparedStatement stmt              = null;
        ResultSet rs                        = null;
        try {
            if(userId.length() > 0) {
                avatar                      = getAvatar(userId);
            }
        } catch(Exception e) {
            logger.dbError("Error in get Avatar: "+e.toString());
        }
        
        if(avatar == null) {
            avatar                          = "";
        }
        
        try {
            if(style!= null && !style.equals("")) {
                stmt                        = transconn.prepareStatement(selectStyle);
                stmt.setString(1,style);
                rs                          = stmt.executeQuery();
                if (rs.next()) {
                    styleId                 = rs.getInt(1);
                }
            }
            
            if(styles!= null && !styles.equals("")) {
                stmt                        = transconn.prepareStatement(selectStyles);
                rs                          = stmt.executeQuery();
                if (rs.next()) {
                    stylesId                = rs.getString(1);
                }
            }
            Calendar currentDate            = Calendar.getInstance();
            if ((deviceToken.length() > 0)) {
                if (userName==null || userName.equals("")){
                        if(deviceToken.length() > 5) {
                            userName            = "Guest" + deviceToken.substring(deviceToken.length()-6,deviceToken.length()-1);
                        } 
                }
                stmt                            = transconn.prepareStatement(selectProfile);
                stmt.setString(1, deviceToken);
                stmt.setInt(2, platform);
                rs                              = stmt.executeQuery();
                if(rs.next()){
                    user                        = rs.getInt(1);
                    stmt                    = transconn.prepareStatement(updateProfile);
                    stmt.setString(1,userId);
                    stmt.setString(2,token);
                    stmt.setInt(3,styleId);
                    stmt.setString(4,userName);
                    stmt.setString(5,avatar);
                    stmt.setString(6,deviceToken);
                    stmt.setString(7, stylesId);
                    stmt.setString(8, email);
                    stmt.setInt(9,user);
                    stmt.setInt(10,platform);
                    stmt.executeUpdate();
                
                    
                } else {
                    currentDate             = Calendar.getInstance();
                    String today            = dbDateFormat.format(currentDate.getTime());
                    if(userName!=null && !userName.equals("")&&!userName.equals("Guest")  ) {
                        stmt                = transconn.prepareStatement(insertProfile);
                        stmt.setString(1,userName);
                        stmt.setString(2,userId);
                        stmt.setString(3,token);
                        stmt.setString(4,email);
                        stmt.setString(5,today);
                        stmt.setInt(6,styleId);
                        stmt.setString(7,avatar);
                        stmt.setString(8,deviceToken);
                        stmt.setInt(9,platform);
                        stmt.setString(10, stylesId);
                        logger.debug("UserName:"+userName+"  lastName:"+userId+"  email:"+email+" token:"+token+" Style"+style);
                        //toAppend.addElement("customerId").addText(String.valueOf("5"));
                        stmt.executeUpdate();
                        stmt                = transconn.prepareStatement("SELECT LAST_INSERT_ID()");
                        rs                  = stmt.executeQuery();
                        if (rs.next()) {
                            user            = rs.getInt(1);
                        }
                    } else {
                        user                = 0;
                    }
                }
            } else if(userId !=null && userId.length() > 2) {
                if (userName==null || userName.equals("")){
                    if(deviceToken.length() > 5) {
                        userName            = "Guest" + deviceToken.substring(deviceToken.length()-6,deviceToken.length()-1);
                    } 
                }
                stmt                        = transconn.prepareStatement(selectFBUser);
                stmt.setString(1, userId);
                stmt.setInt(2, platform);
                rs                          = stmt.executeQuery();
                if(rs.next()){
                    user                    = rs.getInt(1);
                    stylesId                = HandlerUtils.nullToEmpty(rs.getString(2));
                    deviceToken             = HandlerUtils.nullToEmpty(rs.getString(3));
                    toAppend.addElement("styles").addText(stylesId);
                    stmt                    = transconn.prepareStatement(updateProfile);
                    stmt.setString(1,userId);
                    stmt.setString(2,token);
                    stmt.setInt(3,styleId);
                    stmt.setString(4,userName);
                    stmt.setString(5,avatar);
                    stmt.setString(6,deviceToken);
                    stmt.setString(7, stylesId);
                    stmt.setString(8, email);
                    stmt.setInt(9,user);
                    stmt.setInt(10,platform);
                    stmt.executeUpdate();
                } else {
                    currentDate             = Calendar.getInstance();
                    String today            = dbDateFormat.format(currentDate.getTime());               
                    if(userName!=null && !userName.equals("")&&!userName.equals("Guest")  ) {
                        stmt                = transconn.prepareStatement(insertProfile);
                        stmt.setString(1,userName);
                        stmt.setString(2,userId);
                        stmt.setString(3,token);
                        stmt.setString(4,email);
                        stmt.setString(5,today);
                        stmt.setInt(6,styleId);
                        stmt.setString(7,avatar);
                        stmt.setString(8,deviceToken);
                        stmt.setInt(9,platform);
                        stmt.setString(10, stylesId);
                        logger.debug("UserName:"+userName+"  lastName:"+userId+"  email:"+email+" token:"+token+" Style"+style);
                        //toAppend.addElement("customerId").addText(String.valueOf("5"));
                        stmt.executeUpdate();
                        stmt                = transconn.prepareStatement("SELECT LAST_INSERT_ID()");
                        rs                  = stmt.executeQuery();
                        if (rs.next()) {
                            user            = rs.getInt(1);
                        }
                    } else {
                        user                = 0;
                    }
                }
            } else if(email !=null && email.length() > 2) {
                if (userName==null || userName.equals("")){
                    if(deviceToken.length() > 5) {
                        userName            = "Guest" + deviceToken.substring(deviceToken.length()-6,deviceToken.length()-1);
                    } 
                }
                stmt                        = transconn.prepareStatement(selectFBProfile);
                stmt.setString(1, email);
                stmt.setInt(2, platform);
                rs                          = stmt.executeQuery();
                if(rs.next()){
                    user                    = rs.getInt(1);
                    stmt                    = transconn.prepareStatement(updateProfile);
                    stmt.setString(1,userId);
                    stmt.setString(2,token);
                    stmt.setInt(3,styleId);
                    stmt.setString(4,userName);
                    stmt.setString(5,avatar);
                    stmt.setString(6,deviceToken);
                    stmt.setString(7, stylesId);
                    stmt.setString(8, email);
                    stmt.setInt(9,user);
                    stmt.setInt(10,platform);
                    stmt.executeUpdate();
                } else {
                    currentDate             = Calendar.getInstance();
                    String today            = dbDateFormat.format(currentDate.getTime());               
                    if(userName!=null && !userName.equals("")&&!userName.equals("Guest")  ) {
                        stmt                = transconn.prepareStatement(insertProfile);
                        stmt.setString(1,userName);
                        stmt.setString(2,userId);
                        stmt.setString(3,token);
                        stmt.setString(4,email);
                        stmt.setString(5,today);
                        stmt.setInt(6,styleId);
                        stmt.setString(7,avatar);
                        stmt.setString(8,deviceToken);
                        stmt.setInt(9,platform);
                        stmt.setString(10, stylesId);
                        logger.debug("UserName:"+userName+"  lastName:"+userId+"  email:"+email+" token:"+token+" Style"+style);
                        //toAppend.addElement("customerId").addText(String.valueOf("5"));
                        stmt.executeUpdate();
                        stmt                = transconn.prepareStatement("SELECT LAST_INSERT_ID()");
                        rs                  = stmt.executeQuery();
                        if (rs.next()) {
                            user            = rs.getInt(1);
                        }
                    } else {
                        user                = 0;
                    }
                }
            } else {
                currentDate                 = Calendar.getInstance();
                String today                = dbDateFormat.format(currentDate.getTime());
                stmt                        = transconn.prepareStatement(insertProfile);
                stmt.setString(1,userName);
                stmt.setString(2,userId);
                stmt.setString(3,token);
                stmt.setString(4,email);
                stmt.setString(5,today);
                stmt.setInt(6,styleId);
                stmt.setString(7,avatar);
                stmt.setString(8,deviceToken);
                stmt.setInt(9,platform);
                stmt.setString(10, stylesId);
                logger.debug("UserName:"+userName+"  lastName:"+userId+"  email:"+email+" token:"+token+" Style"+style);
                //toAppend.addElement("customerId").addText(String.valueOf("5"));
                stmt.executeUpdate();
                stmt                        = transconn.prepareStatement("SELECT LAST_INSERT_ID()");
                rs                          = stmt.executeQuery();
                if (rs.next()) {
                    user                    = rs.getInt(1); 
                }
                
            }
            
           /* stmt                            = transconn.prepareStatement(selectProfile);
            stmt.setString(1, deviceToken);
            stmt.setInt(2, platform);
            rs                              = stmt.executeQuery();
            if ((deviceToken.length() > 0) && rs.next()) {
                user                        = rs.getInt(1);  
                if (!userName.equals("")){
                    stmt                    = transconn.prepareStatement(updateProfile);
                    stmt.setString(1,userId);
                    stmt.setString(2,token);
                    stmt.setInt(3,styleId);
                    stmt.setString(4,userName);
                    stmt.setString(5,avatar);
                    stmt.setString(6,deviceToken);
                    stmt.setString(7, stylesId);
                    stmt.setString(8, email);
                    stmt.setInt(9,user);
                    stmt.setInt(10,platform);
                    stmt.executeUpdate();
                }
            } else {
                Calendar currentDate        = Calendar.getInstance();
                String today                = dbDateFormat.format(currentDate.getTime());
                if(userName == null || userName.equals("")){
                    if(deviceToken.length() > 5) {
                        userName            = "Guest" + deviceToken.substring(deviceToken.length()-6,deviceToken.length()-1);
                   } else {
                        userName            = "Guest";
                   }
                }
                if(userName!=null && !userName.equals("")&&!userName.equals("Guest")  ) {
                stmt                        = transconn.prepareStatement(insertProfile);
                stmt.setString(1,userName);
                stmt.setString(2,userId);
                stmt.setString(3,token);
                stmt.setString(4,email);
                stmt.setString(5,today);
                stmt.setInt(6,styleId);
                stmt.setString(7,avatar);
                stmt.setString(8,deviceToken);
                stmt.setInt(9,platform);
                stmt.setString(10, stylesId);
                logger.debug("UserName:"+userName+"  lastName:"+userId+"  email:"+email+" token:"+token+" Style"+style);
                //toAppend.addElement("customerId").addText(String.valueOf("5"));
                stmt.executeUpdate();
                stmt                        = transconn.prepareStatement("SELECT LAST_INSERT_ID()");
                rs                          = stmt.executeQuery();
                if (rs.next()) {
                    user                    = rs.getInt(1);
                }
                } else {
                    user                    = 0;
                }
                
            }*/
        } catch (SQLException sqle) {
            logger.dbError("Database error in addUserRating: "+sqle.toString());
            throw new HandlerException(sqle);
        } catch (Exception e) {
           logger.dbError("addUpdateMobileUser "+e.toString());
            throw new HandlerException(e);
       }  finally {
            close(stmt);
            close(rs);
       }
       return user;
    }
    
    
    private int addUpdateAndroidMobileUser(String userName, String userId, String token, String email, String style, String styles, String deviceToken,
            int platform,Element toAppend, String deviceId) throws HandlerException {
        
        int user                            = 0;
        if(userName == null) {
            userName                        = "";
        }
        if(userId == null || userId.equals("0")) {
            userId                          = "";
        }
        if(token == null) {
            token                           = "";
        }
        if(email == null) {
            email                           = "";
        }
        if(style == null) {
            style                           = "";
        }
        if(styles == null) {
            styles                          = "";
        }
        if(deviceToken == null) {
            deviceToken                     = "";
        }
        
        String avatar                       = null;
        String insertProfile                = "INSERT INTO bbtvMobileUser (username, fbID, token, email, registration, style, avatar, deviceToken, platform, styles,deviceId) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?); ";
        String updateProfile                = "UPDATE bbtvMobileUser SET fbID = ?, token = ?, style = ?, username = ?, avatar = ?, arrival=null, deviceToken = ?,styles = ?, email =? WHERE id = ? and platform = ?; ";
        String selectProfile                = "SELECT id,deviceToken FROM bbtvMobileUser WHERE deviceId = ? and platform = ?; ";
        String selectFBProfile              = "SELECT id,fbID FROM bbtvMobileUser WHERE email = ? and platform = ?;";
        String selectStyle                  = "SELECT sL.id FROM  styleLogo sL WHERe sL.style = ?;";
        String selectStyles                 = "SELECT GROUP_CONCAT(DISTINCT bS.id ORDER BY bS.id SEPARATOR ',') FROM beerStyles bS  WHERE bS.style IN ("+styles+");";
        String selectFBUser                 = "SELECT id, styles,deviceToken FROM bbtvMobileUser WHERE fbID = ? and platform = ? ORDER by arrival DESC,id DESC LIMIT 1;";
        int styleId                         = 0;
        String stylesId                     = "";
       
        PreparedStatement stmt              = null;
        ResultSet rs                        = null;
        try {
            if(userId.length() > 0) {
                avatar                      = getAvatar(userId);
            }
        } catch(Exception e) {
            logger.dbError("Error in get Avatar: "+e.toString());
        }
        
        if(avatar == null) {
            avatar                          = "";
        }
        
        try {
            if(style!= null && !style.equals("")) {
                stmt                        = transconn.prepareStatement(selectStyle);
                stmt.setString(1,style);
                rs                          = stmt.executeQuery();
                if (rs.next()) {
                    styleId                 = rs.getInt(1);
                }
            }
            
            if(styles!= null && !styles.equals("")) {
                stmt                        = transconn.prepareStatement(selectStyles);
                rs                          = stmt.executeQuery();
                if (rs.next()) {
                    stylesId                = rs.getString(1);
                }
            }
            Calendar currentDate            = Calendar.getInstance();
            if ((deviceId.length() > 5) && deviceToken.length() > 5) {
                if (userName==null || userName.equals("")){
                        if(deviceToken.length() > 5) {
                            userName            = "Guest" + deviceToken.substring(deviceToken.length()-6,deviceToken.length()-1);
                        } 
                }
                stmt                            = transconn.prepareStatement(selectProfile);
                stmt.setString(1, deviceId);
                stmt.setInt(2, platform);
                rs                              = stmt.executeQuery();
                if(rs.next()){
                    user                        = rs.getInt(1);
                    stmt                    = transconn.prepareStatement(updateProfile);
                    stmt.setString(1,userId);
                    stmt.setString(2,token);
                    stmt.setInt(3,styleId);
                    stmt.setString(4,userName);
                    stmt.setString(5,avatar);
                    stmt.setString(6,deviceToken);
                    stmt.setString(7, stylesId);
                    stmt.setString(8, email);
                    stmt.setInt(9,user);
                    stmt.setInt(10,platform);
                    stmt.executeUpdate();
                
                    
                } else {
                    currentDate             = Calendar.getInstance();
                    String today            = dbDateFormat.format(currentDate.getTime());
                    if(userName!=null && !userName.equals("")&&!userName.equals("Guest")  ) {
                        stmt                = transconn.prepareStatement(insertProfile);
                        stmt.setString(1,userName);
                        stmt.setString(2,userId);
                        stmt.setString(3,token);
                        stmt.setString(4,email);
                        stmt.setString(5,today);
                        stmt.setInt(6,styleId);
                        stmt.setString(7,avatar);
                        stmt.setString(8,deviceToken);
                        stmt.setInt(9,platform);
                        stmt.setString(10, stylesId);
                        stmt.setString(11, deviceId);
                        logger.debug("UserName:"+userName+"  lastName:"+userId+"  email:"+email+" token:"+token+" Style"+style);
                        //toAppend.addElement("customerId").addText(String.valueOf("5"));
                        stmt.executeUpdate();
                        stmt                = transconn.prepareStatement("SELECT LAST_INSERT_ID()");
                        rs                  = stmt.executeQuery();
                        if (rs.next()) {
                            user            = rs.getInt(1);
                        }
                    } else {
                        user                = 0;
                    }
                }
            } else if ((deviceId.length() > 5) && deviceToken.equals("")) {
                if (userName==null || userName.equals("")){
                        if(deviceToken.length() > 5) {
                            userName            = "Guest" + deviceToken.substring(deviceToken.length()-6,deviceToken.length()-1);
                        } 
                }
                stmt                            = transconn.prepareStatement(selectProfile);
                stmt.setString(1, deviceId);
                stmt.setInt(2, platform);
                rs                              = stmt.executeQuery();
                if(rs.next()){
                    user                        = rs.getInt(1);
                    deviceToken                 = rs.getString(2);
                    stmt                    = transconn.prepareStatement(updateProfile);
                    stmt.setString(1,userId);
                    stmt.setString(2,token);
                    stmt.setInt(3,styleId);
                    stmt.setString(4,userName);
                    stmt.setString(5,avatar);
                    stmt.setString(6,deviceToken);
                    stmt.setString(7, stylesId);
                    stmt.setString(8, email);
                    stmt.setInt(9,user);
                    stmt.setInt(10,platform);
                    stmt.executeUpdate();
                
                    
                } else {
                    currentDate             = Calendar.getInstance();
                    String today            = dbDateFormat.format(currentDate.getTime());
                    if(userName!=null && !userName.equals("")&&!userName.equals("Guest")  ) {
                        stmt                = transconn.prepareStatement(insertProfile);
                        stmt.setString(1,userName);
                        stmt.setString(2,userId);
                        stmt.setString(3,token);
                        stmt.setString(4,email);
                        stmt.setString(5,today);
                        stmt.setInt(6,styleId);
                        stmt.setString(7,avatar);
                        stmt.setString(8,deviceToken);
                        stmt.setInt(9,platform);
                        stmt.setString(10, stylesId);
                        stmt.setString(11, deviceId);
                        logger.debug("UserName:"+userName+"  lastName:"+userId+"  email:"+email+" token:"+token+" Style"+style);
                        //toAppend.addElement("customerId").addText(String.valueOf("5"));
                        stmt.executeUpdate();
                        stmt                = transconn.prepareStatement("SELECT LAST_INSERT_ID()");
                        rs                  = stmt.executeQuery();
                        if (rs.next()) {
                            user            = rs.getInt(1);
                        }
                    } else {
                        user                = 0;
                    }
                }
            } else {
                currentDate                 = Calendar.getInstance();
                String today                = dbDateFormat.format(currentDate.getTime());
                stmt                        = transconn.prepareStatement(insertProfile);
                stmt.setString(1,userName);
                stmt.setString(2,userId);
                stmt.setString(3,token);
                stmt.setString(4,email);
                stmt.setString(5,today);
                stmt.setInt(6,styleId);
                stmt.setString(7,avatar);
                stmt.setString(8,deviceToken);
                stmt.setInt(9,platform);
                stmt.setString(10, stylesId);
                stmt.setString(11, deviceId);
                logger.debug("UserName:"+userName+"  lastName:"+userId+"  deviceId:"+deviceId+" token:"+token+" Style"+style);
                //toAppend.addElement("customerId").addText(String.valueOf("5"));
                stmt.executeUpdate();
                stmt                        = transconn.prepareStatement("SELECT LAST_INSERT_ID()");
                rs                          = stmt.executeQuery();
                if (rs.next()) {
                    user                    = rs.getInt(1); 
                }
                
            }
            
           
        } catch (SQLException sqle) {
            logger.dbError("Database error in addUserRating: "+sqle.toString());
            throw new HandlerException(sqle);
        } catch (Exception e) {
           logger.dbError("addUpdateMobileUser "+e.toString());
            throw new HandlerException(e);
       }  finally {
            close(stmt);
            close(rs);
       }
       return user;
    }
    
    private void addUserCheckin(Element toHandle, Element toAppend) throws HandlerException {

        int location                        = HandlerUtils.getRequiredInteger(toHandle, "locationId");
        int user                            = HandlerUtils.getOptionalInteger(toHandle, "user");
        int platform                        = HandlerUtils.getOptionalInteger(toHandle, "platform");
        String customerMessage              = HandlerUtils.getOptionalString(toHandle, "fbMessage");
        if(platform == 7) {
            String userName                 = HandlerUtils.getRequiredString(toHandle, "userName");
            String userId                   = HandlerUtils.getOptionalString(toHandle, "userId");
            String email                    = HandlerUtils.getOptionalString(toHandle, "email");
            String token                    = HandlerUtils.getOptionalString(toHandle, "token");
            
            user                            = addUpdateMobileUser(userName,userId,token,email,"","","",platform,toAppend);
        }
        String checkFavouriteLoc            = "SELECT id FROM favoriteLocation WHERE user=? AND location = ?;";
        String insertFavouriteLoc           = "INSERT INTO favoriteLocation (user,location) VALUES (?,?);";
        
        PreparedStatement stmt              = null;       
        ResultSet rs                        = null;
        ArrayList<String> pageId            = new ArrayList<String>();  
        ArrayList<String> fbId              = new ArrayList<String>();
        ArrayList<String> locationToken     = new ArrayList<String>();
        String promoMessage                 = "";
        
        Calendar currentDate                = Calendar.getInstance();
        
        String insertCheckin                = "INSERT INTO bbtvMobileUserCheckin(user, location, date) VALUES(?, ?, ?);";
        String checkPromoNight              = "SELECT pS.name, IFNULL(p.name, 'Unknown Product'), CONCAT(l.boardname,', ',l.addrCity) FROM bevSyncCampaignReward bCR " +
                                            " LEFT JOIN bevSyncCreatives bC ON bC.id = bCR.creatives LEFT JOIN product p ON p.id = bC.product LEFT JOIN productDescription pD ON pD.product = bC.product " +
                                            " LEFT JOIN productSet pS ON pS.id = bC.brewery LEFT JOIN location l ON l.id = bCR.location " +
                                            " WHERE pS.productSetType=7 AND bCR.location = ? AND CURDATE() = DATE(bCR.startTime) ";
        String selectInfo                   = "SELECT u.fbID, u.token, l.boardname, l.name, l.addrCity, l.addrZip, u.username FROM bbtvMobileUser u LEFT JOIN bbtvMobileUserCheckin c ON c.user = u.id LEFT JOIN location l ON l.id = c.location WHERE u.id = ? AND c.location = ?;";
        String selectToken                  = "select Distinct p.pageid, u.user_id,  u.access_token,IFNULL((SELECT checkin FROM socialMediaPost WHERE location=? AND type=1),0) from usbnFacebook u LEFT JOIN usbnFacebookPage p ON u.user_id =p.fbid where p.location=? AND p.location != 856 and u.user_id!='5515563';";
        String updateUserTVAccess           = "UPDATE locationBeerBoardMap SET feed = 1 WHERE location = ? ";
        try { 
            
            if(user> 0){
                stmt                        = transconn.prepareStatement(checkFavouriteLoc);
                stmt.setInt(1,user);
                stmt.setInt(2,location);
                rs                          = stmt.executeQuery();
                if (!rs.next()) {
                     stmt                   = transconn.prepareStatement(insertFavouriteLoc);
                     stmt.setInt(1,user);
                     stmt.setInt(2,location);
                     stmt.executeUpdate();
                }
            }
            
            stmt                            = transconn.prepareStatement(selectToken);            
            stmt.setInt(1,location);
            stmt.setInt(2,location);
            rs                              = stmt.executeQuery();
            while (rs.next()) {
                if(rs.getInt(4)>0){
                    pageId.add(rs.getString(1));
                    fbId.add(rs.getString(2));
                    locationToken.add(rs.getString(3));
                }
            }
            
            String checkin                  = dbDateFormat.format(currentDate.getTime());
            stmt                            = transconn.prepareStatement(insertCheckin);
            stmt.setInt(1,user);
            stmt.setInt(2,location);
            stmt.setString(3, checkin);
            stmt.executeUpdate();

            stmt                            = transconn.prepareStatement(updateUserTVAccess);
            stmt.setInt(1,location);
            stmt.executeUpdate();
            
            stmt                            = transconn.prepareStatement(selectInfo);
            stmt.setInt(1,user);
            stmt.setInt(2,location);
            rs                              = stmt.executeQuery();
            if (rs.next()) {
                int colCount                = 1;
                String fbid                 = rs.getString(colCount++);
                String token                = rs.getString(colCount++);
                String msg                  = "Checked-in @ "+rs.getString(colCount++)+" - "+rs.getString(colCount++)+" , "+rs.getString(colCount++)+" - "+rs.getString(colCount++);
                String username             = rs.getString(colCount++);
                if(customerMessage!=null && !customerMessage.equals("")){
                        msg                 = customerMessage;
                    }
                
                if(token.length() > 10) {
                    postFacebookStatus(fbid, token, msg);
                }
                if(fbId.size()>0) {
                    postOnPage(fbId, pageId, locationToken, username + " " + msg, fbid);
                }
                tweetStatus(location,msg,6,0,0);

                msg                         = "";
                
                stmt                        = transconn.prepareStatement(checkPromoNight);
                stmt.setInt(1,location);
                rs                          = stmt.executeQuery();
                if (rs.next()) {
                    String brewery          = rs.getString(1);
                    String product          = rs.getString(2);
                    String locationName     = rs.getString(3);
                    if(product.equals("Unknown Product") ){
                        msg                 = username + "  likes  " + brewery + " @ " + locationName;
                    } else {
                        msg                 = username + "  likes  " + product + " @ " + locationName;
                    }
                    if(token.length() > 10) {
                        postFacebookStatus(fbid, token, msg);
                    }
                    if(fbId.size()>0) {
                        postOnPage(fbId, pageId, locationToken, username + " " + msg, fbid);
                    }
                    tweetStatus(location,msg,6,0,0);
                }
            }
        } catch ( Exception sqle) {
            logger.dbError("Database error: "+sqle.toString());
            throw new HandlerException(sqle);
        } finally {
           close(stmt);
        }
    }
    
    private void postFacebookStatus(String fbid, String token, String  msg) throws HandlerException {
        String post                         = "https://graph.facebook.com/"+fbid+"/feed/?message="+msg+"&method=post&access_token="+token;
         try {
             URL urL                        = new URL(URIUtil.encodeQuery(post));              
             getData(urL);
        }catch (Exception e) {
            logger.dbError("Error Post on Facebook: "+e.toString());
        }   
    }
    
    
    private String getAvatar(String fbid){
        String avatar                       = "http://beerboard.tv/USBN.BeerBoard.UI/Images/Assets/Generic/profile-pic.png";
        try {
            if(fbid!= null && fbid.length() > 4) {
                String data                 = getData(new URL("http://graph.facebook.com/"+fbid+"/picture?type=normal&redirect=false"));      
                //logger.debug("data:"+data);
               if(data!= null && data.length() >6) {
                    JSONObject jdata        = new JSONObject(data);
                    if(jdata.has("data")){
                        jdata               = (JSONObject)jdata.get("data");
                        avatar              = jdata.get("url").toString();	
	                }
               /* data                        = data.replaceAll("/", "");
                avatar                      = data.substring(data.indexOf("url")+6,data.indexOf(",")-1);
                avatar                      = avatar.replaceAll("\\\\","/");
                logger.debug("Avatar:"+avatar);*/
                    
               }
            }
        } catch(Exception e) {
            return avatar;            
        }
        return avatar;
    }

    public String getData(URL urL) {
        TrustManager[] trustAllCerts        = new TrustManager[]{new X509TrustManager(){
            public X509Certificate[] getAcceptedIssuers(){return null;}
            public void checkClientTrusted(X509Certificate[] certs, String authType){}
            public void checkServerTrusted(X509Certificate[] certs, String authType){}
        }};
        
        // Install the all-trusting trust manager
        try {
            SSLContext sc                   = SSLContext.getInstance("TLS");
            sc.init(null, trustAllCerts, new SecureRandom());
            HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());
        } catch (Exception e) {
           logger.debug(e.getMessage());
        }
        String graph                        = null;
        try {
            String inputLine;            
            HttpsURLConnection conn         = (HttpsURLConnection) urL.openConnection();            
            BufferedReader in               = new BufferedReader(new InputStreamReader(conn.getInputStream()));           
            StringBuffer  b                 = new StringBuffer();           
            while ((inputLine = in.readLine()) != null){          
                b.append(inputLine + "\n");
            }            
            in.close();           
            graph                           = b.toString();
            //logger.debug(graph);
        }catch (Exception e) {
            e.printStackTrace();
        }
        return graph;

    }
    
    public String getHttpData(URL urL) {
        String graph                        = null;
        try {
            String inputLine;            
            HttpURLConnection conn         = (HttpURLConnection) urL.openConnection();            
            BufferedReader in               = new BufferedReader(new InputStreamReader(conn.getInputStream()));           
            StringBuffer  b                 = new StringBuffer();           
            while ((inputLine = in.readLine()) != null){          
                b.append(inputLine + "\n");
            }            
            in.close();           
            graph                           = b.toString();
            //logger.debug(graph);
        }catch (Exception e) {
            e.printStackTrace();
        }
        return graph;

    }

    public void deleteFBPosts(Element toHandle, Element toAppend) throws HandlerException {

        String selectPostsToDelete          = "SELECT fP.postId, uF.access_token FROM facebookPost fP LEFT JOIN usbnFacebook uF ON fP.user = uF.user_id " +
                                            " WHERE fP.location IN (367) AND fP.postTime > '2013-10-03 15:11:27' AND uF.access_token IS NOT NULL;";
        //String selectPostsToDelete          = "SELECT postId, 'access_token=CAACNB03nnL0BAHN0HizaDRjyGRLOsRoxI0L4yfCiylfkSGRL7YZCHIZBdBBTvXmu94E7ndJP3cen5rPHYW5e66xPT1ZBBezLchWWtZBHjq4GpvjXXST6TPpzZBtmZCxj4OOZBj3dC4v4ZBPR74Wfs2Lb9TaBX4tgqtgHEwr8JmErLbHNzA7KIlJdxog6NxBxlygZD&expires=5183639' FROM facebookPost WHERE id IN (1157, 1188, 1222);";
        PreparedStatement stmt              = null;
        ResultSet rs                        = null;
        try {

            stmt                            = transconn.prepareStatement(selectPostsToDelete);
            rs                              = stmt.executeQuery();
            while (rs.next()) {
                String url                  = "https://graph.facebook.com/"+rs.getString(1)+"?method=delete&"+rs.getString(2);
                URL urL                     = new URL(URIUtil.encodeQuery(url));
                signHttpsCertificate();
                String inputLine;
                HttpsURLConnection conn     = (HttpsURLConnection) urL.openConnection();
                BufferedReader in           = new BufferedReader(new InputStreamReader(conn.getInputStream()));
                StringBuffer  b             = new StringBuffer();
                while ((inputLine = in.readLine()) != null){
                    b.append(inputLine + "\n");
                }
                in.close();
                logger.debug( b.toString());
            }
        } catch ( Exception sqle) {
            logger.dbError("Database error: "+sqle.toString());
            throw new HandlerException(sqle);
        } finally {
           close(stmt);
           close(rs);
        } 
    }
    
    
    private void getSpotLight(Element toHandle, Element toAppend) throws HandlerException {

        int platform                        = HandlerUtils.getOptionalInteger(toHandle, "platform");        
        int location                        = HandlerUtils.getRequiredInteger(toHandle, "locationId");
        int user                            = HandlerUtils.getOptionalInteger(toHandle, "user");
        
        String selectNewBeer                = "SELECT DISTINCT lU.product FROM lineUpdates lU LEFT JOIN line l ON l.id = lU.line WHERE lU.location = ? "
                                            + " AND lU.date > SUBDATE(NOW(), INTERVAL 7 DAY) AND l.status = 'RUNNING' ORDER BY lU.date DESC, lU.id DESC LIMIT 2;";

        String selectLocalBeer              = "SELECT DISTINCT l.product FROM line l LEFT JOIN bar b ON b.id = l.bar WHERE b.location = ? " +
                                            " AND l.status = 'RUNNING' AND l.local = 1;";
        
        String getProductDescription        = "SELECT p.name, IF(pD.abv = 0.0, '', CONCAT(pD.abv,'%')), sPS.name, pD.origin, IFNULL(sL.logo, 'Seasonal'), sPS.id FROM product p"
                                            + " LEFT JOIN  productDescription pD ON pD.product=p.id" +
                                            " LEFT JOIN productSetMap sPSM ON sPSM.product = pD.product LEFT JOIN productSet sPS ON sPS.id = sPSM.productSet " +
                                            " LEFT JOIN styleLogo sL ON sL.style = sPS.name LEFT JOIN productSetMap pSM ON pSM.product = pD.product " +
                                            " LEFT JOIN productSet pS ON pS.id = pSM.productSet WHERE pS.productSetType = 7 AND sPS.productSetType = 9 AND pD.product = ? "  ;

        String selectLocationProductLikes   = "SELECT  bMU.username, p.name, bMU.avatar, bMU.fbID  FROM bbtvMobileUserRating bMUR LEFT JOIN bbtvMobileUser bMU ON bMU.id = bMUR.user " +
                                            " LEFT JOIN product p ON p.id = bMUR.product WHERE bMUR.location = ? " +
                                            " AND bMUR.date > IF(HOUR (NOW()) < 7 , CONCAT(DATE_SUB(CURDATE(), INTERVAL 1 DAY), ' 07:00:00'), CONCAT(CURDATE(), ' 07:00:00')) " +
                                            " ORDER BY bMUR.date DESC LIMIT 5; ";
        
        String selectLocationCheckin        = "SELECT bMUC.date, bMU.username, bMU.avatar, bMU.fbID  FROM bbtvMobileUserCheckin bMUC LEFT JOIN bbtvMobileUser bMU ON bMU.id = bMUC.user " +
                                            " WHERE bMUC.location = ? AND bMUC.date > IF(HOUR (NOW()) < 7 , CONCAT(DATE_SUB(CURDATE(), INTERVAL 1 DAY), ' 07:00:00'), CONCAT(CURDATE(), ' 07:00:00')) " +
                                            " ORDER BY bMUC.date DESC LIMIT 5; ";
        String selectStyles                 = "SELECT IF(style = 0, styles, style) FROM bbtvMobileUser WHERE id = ? ; ";
        String selectCustomName             = "SELECT IFNULL((SELECT name FROM customBeerName WHERE location =? AND product = ?),?),name,logo FROM customStyleName WHERE location =? AND productSet =? ;";

        
        PreparedStatement stmt              = null;       
        ResultSet rs                        = null,rsDetail = null,rsCustom = null;
        Calendar currentDate                = Calendar.getInstance();
        try {
            Element orderEl = toAppend.addElement("order");
            orderEl.addElement("O1").addText("Recommendation"); 
            orderEl.addElement("O2").addText("Check-ins"); 
            orderEl.addElement("O3").addText("Local Beer"); 
            orderEl.addElement("O4").addText("Likes"); 
            orderEl.addElement("O5").addText("New Beer"); 
            if(platform!=7) {
                stmt                        = transconn.prepareStatement(selectStyles);
                stmt.setInt(1,user);
                rs                          = stmt.executeQuery();
                if (rs.next()) {
                    String style            = rs.getString(1);
                    if(style == null || style.equals("")){
                        style="0";
                    }
                    int styleId             = -1;
                    int rowCount            = 1;
                    String selectRecommendProducts
                                            = "SELECT sL.id, p.id, pD.boardName, IF(pD.abv = 0.0, '', CONCAT(pD.abv,'%')), sL.style, pD.origin, IFNULL(sL.style, 'Seasonal') " +
                                            " FROM openHoursSummary oHS LEFT JOIN product p on oHS.product = p.id LEFT JOIN productDescription pD ON pD.product = p.id " +
                                            " LEFT JOIN productSetMap pSM ON pSM.product = p.id LEFT JOIN productSet pS ON pS.id = pSM.productSet " +
                                            " LEFT JOIN beerStylesMap bSM ON bSM.productSet = pS.id LEFT JOIN beerStyles sL ON sL.id = bSM.style " +
                                            " WHERE oHS.date > LEFT(ADDDATE(NOW(), INTERVAL -7 DAY), 10) AND oHS.location = ? AND pS.productSetType = 9 " +
                                            " AND sL.id IN (" + style +") GROUP BY oHS.product ORDER BY sL.id, SUM(oHS.value) DESC;";
                    
                    stmt                    = transconn.prepareStatement(selectRecommendProducts);
                    stmt.setInt(1, location);                   
                    rsDetail                = stmt.executeQuery();
                    while (rsDetail.next() && rowCount < 4) {
                        int colCount        = 1;
                        if (styleId != rsDetail.getInt(colCount++)) {
                            styleId         = rsDetail.getInt(1);
                            rowCount++;
                            Element recommendEl
                                            = toAppend.addElement("spotLight");
                            recommendEl.addElement("type").addText("1");
                            recommendEl.addElement("productId").addText(HandlerUtils.nullToEmpty(rsDetail.getString(colCount++)));
                            recommendEl.addElement("product").addText(HandlerUtils.nullToEmpty(rsDetail.getString(colCount++)));
                            recommendEl.addElement("abv").addText(HandlerUtils.nullToEmpty(rsDetail.getString(colCount++)));
                            recommendEl.addElement("newstyle").addText(HandlerUtils.nullToEmpty(rsDetail.getString(4)));
                            recommendEl.addElement("style").addText(HandlerUtils.nullToEmpty(rsDetail.getString(colCount++)));
                            recommendEl.addElement("origin").addText(HandlerUtils.nullToEmpty(rsDetail.getString(colCount++)));
                            String logo     = rsDetail.getString(colCount++);
                            if(logo!=null && !logo.equals("")) {
                                recommendEl.addElement("logo").addText(HandlerUtils.nullToEmpty("http://beerboard.tv/USBN.BeerBoard.UI/Images/glass/" +logo+".png"));
                            }
                        }
                    } 
                }
            }

            stmt                            = transconn.prepareStatement(selectNewBeer);
            stmt.setInt(1, location);
            rs                              = stmt.executeQuery();
            while (rs.next()) {
                int productId               = rs.getInt(1);
                stmt                        = transconn.prepareStatement(getProductDescription);
                stmt.setInt(1, productId);
                rsDetail                    = stmt.executeQuery();
                while(rsDetail.next()) {
                    String productName      = rsDetail.getString(1);
                    int styleId             = rsDetail.getInt(6);
                    String styleName        = rsDetail.getString(3);
                    String styleLogo        = "http://beerboard.tv/USBN.BeerBoard.UI/Images/glass/" +rsDetail.getString(5)+".png";
                    stmt                    = transconn.prepareStatement(selectCustomName);
                    stmt.setInt(1, location);
                    stmt.setInt(2, productId);
                    stmt.setString(3, productName);
                    stmt.setInt(4, location);
                    stmt.setInt(5, styleId);
                    rsCustom                = stmt.executeQuery();
                    if(rsCustom.next()){
                        productName         = rsCustom.getString(1);
                        styleName           = HandlerUtils.nullToString(rsCustom.getString(2), styleName);
                        String customLogo=rsCustom.getString(3);
                        if(customLogo!=null&& !customLogo.equals("") ){
                            styleLogo       = "http://beerboard.tv/USBN.BeerBoard.UI/Images/CustomStyle/"+String.valueOf(location)+"/"+customLogo;
                        }
                    }
                    int colCount            = 1;
                    Element localEl         = toAppend.addElement("spotLight");
                    localEl.addElement("type").addText("5");
                    localEl.addElement("productId").addText(String.valueOf(productId));
                    localEl.addElement("product").addText(productName);
                    localEl.addElement("abv").addText(HandlerUtils.nullToEmpty(rsDetail.getString(2)));
                    localEl.addElement("newstyle").addText(styleName);
                    localEl.addElement("style").addText( rsDetail.getString(3));
                    localEl.addElement("origin").addText(HandlerUtils.nullToEmpty(rsDetail.getString(4)));
                    localEl.addElement("logo").addText(styleLogo);
                }
            }
            
            stmt                            = transconn.prepareStatement(selectLocalBeer);
            stmt.setInt(1, location);
            rs                              = stmt.executeQuery();
            while (rs.next()) {
                int productId               = rs.getInt(1);
                stmt                        = transconn.prepareStatement(getProductDescription);
                stmt.setInt(1, productId);
                rsDetail                    = stmt.executeQuery();
                while(rsDetail.next()) {
                    String productName      = rsDetail.getString(1);
                    int styleId             = rsDetail.getInt(6);
                    String styleName        = rsDetail.getString(3);
                    String styleLogo        = "http://beerboard.tv/USBN.BeerBoard.UI/Images/glass/" +rsDetail.getString(5)+".png";
                    stmt                    = transconn.prepareStatement(selectCustomName);
                    stmt.setInt(1, location);
                    stmt.setInt(2, productId);
                    stmt.setString(3, productName);
                    stmt.setInt(4, location);
                    stmt.setInt(5, styleId);
                    rsCustom                = stmt.executeQuery();
                    if(rsCustom.next()){
                        productName         = rsCustom.getString(1);
                        styleName           = HandlerUtils.nullToString(rsCustom.getString(2), styleName);
                        String customLogo=rsCustom.getString(3);
                        if(customLogo!=null&& !customLogo.equals("") ){
                            styleLogo       = "http://beerboard.tv/USBN.BeerBoard.UI/Images/CustomStyle/"+String.valueOf(location)+"/"+customLogo;
                        }
                    }
                    int colCount            = 1;
                    Element localEl         = toAppend.addElement("spotLight");
                    localEl.addElement("type").addText("3");
                    localEl.addElement("productId").addText(String.valueOf(productId));
                    localEl.addElement("product").addText(productName);
                    localEl.addElement("abv").addText(HandlerUtils.nullToEmpty(rsDetail.getString(2)));
                    localEl.addElement("newstyle").addText(styleName);
                    localEl.addElement("style").addText( rsDetail.getString(3));
                    localEl.addElement("origin").addText(HandlerUtils.nullToEmpty(rsDetail.getString(4)));
                    localEl.addElement("logo").addText(styleLogo);
                }
            }
            
            stmt                            = transconn.prepareStatement(selectLocationProductLikes);
            stmt.setInt(1, location);
            rs                              = stmt.executeQuery();
            while (rs.next()) {
                int colCount                = 1;
                Element likeEl              = toAppend.addElement("spotLight");
                String msg                  = rs.getString(colCount++) + " likes " + rs.getString(colCount++) ;
                likeEl.addElement("type").addText("4");
                String avatar               = rs.getString(colCount++);
                String fbId                 = rs.getString(colCount++);
                if(fbId!=null) {
                    if(fbId.length() > 4) {
                        String avatar1      = getAvatar(fbId);
                        likeEl.addElement("user").addText(HandlerUtils.nullToEmpty(avatar1));
                    } else {
                        if(avatar==null|| avatar.equals("")) {
                            avatar          = "http://beerboard.tv/USBN.BeerBoard.UI/Images/Assets/Generic/profile-pic.png";
                        }
                        likeEl.addElement("user").addText(HandlerUtils.nullToEmpty(avatar));
                    }
                } else {
                    if(avatar==null|| avatar.equals("")) {
                        avatar              = "http://beerboard.tv/USBN.BeerBoard.UI/Images/Assets/Generic/profile-pic.png";
                    }
                    likeEl.addElement("user").addText(HandlerUtils.nullToEmpty(avatar));
                }
                likeEl.addElement("msg").addText(msg);
            }
                
            stmt                            = transconn.prepareStatement(selectLocationCheckin);
            stmt.setInt(1, location);
            rs                              = stmt.executeQuery();
            while (rs.next()) {
                int colCount                = 1;
                String arrival              =  rs.getString(colCount++);
                String today                = dbDateFormat.format(currentDate.getTime());
                if(arrival!=null&&!arrival.equals("")){
                    long[] diff             = getTimeDifference(dbDateFormat.parse(arrival), dbDateFormat.parse(today));
                    //logger.debug("diff:"+(diff[0]*24+diff[1]));
                    if(diff[0]*24+diff[1]<=12) {
                        Element checkinEl   = toAppend.addElement("spotLight");
                        String msg          = rs.getString(colCount++) +  " checked-in " ;
                        checkinEl.addElement("type").addText("2");
                        String avatar       = rs.getString(colCount++);
                        String fbId         = rs.getString(colCount++);
                        if(fbId!=null) {
                            if(fbId.length() > 4) {
                                String avatar1
                                            = getAvatar(fbId);
                                checkinEl.addElement("user").addText(HandlerUtils.nullToEmpty(avatar1));
                            } else {
                                if(avatar==null|| avatar.equals("")) {
                                    avatar  = "http://beerboard.tv/USBN.BeerBoard.UI/Images/Assets/Generic/profile-pic.png";
                                }
                                checkinEl.addElement("user").addText(HandlerUtils.nullToEmpty(avatar));
                            }
                        }else {
                            if(avatar==null|| avatar.equals("")) {
                            avatar          = "http://beerboard.tv/USBN.BeerBoard.UI/Images/Assets/Generic/profile-pic.png";
                            }
                            checkinEl.addElement("user").addText(HandlerUtils.nullToEmpty(avatar));
                        }
                        checkinEl.addElement("msg").addText(msg);
                    }
                }
            }
        } catch ( Exception sqle) {
            logger.dbError("Database error: "+sqle.toString());
            throw new HandlerException(sqle);
        } finally {
            close(rsCustom);
            close(rsDetail);
            close(rs);          
            close(stmt);
        }
    }
    
    private void getSponsorAd(Element toHandle, Element toAppend) throws HandlerException {

        int location                        = HandlerUtils.getRequiredInteger(toHandle, "locationId");
        int sponsor                         = HandlerUtils.getRequiredInteger(toHandle, "sponsor");
        int platform                        = HandlerUtils.getRequiredInteger(toHandle, "platform");
        String selectPromotion              = "SELECT bCR.id, bC.customer, bCr.file, bCR.startTime, bCR.endTime, (NOW() > bCR.startTime AND NOW() < bCR.endTime) FROM bevSyncCampaignReward bCR " +
                                            " LEFT JOIN bevSyncCampaign bC ON bC.id = bCR.campaign LEFT JOIN bevSyncCreatives bCr ON bCr.id = bCR.creatives " +
                                            " WHERE CURDATE() = DATE(bCR.startTime) AND bCr.type IN (4,5) AND bCR.location = ?;";
        String  css                         = "@charset 'utf-8'; " +
                                            " @font-face { font-family: 'Franchise'; src: url('Franchise-Bold.ttf'); } " +
                                            " @font-face { font-family: 'PPETRIAL'; src: url('PPETRIAL.otf'); } " +
                                            " @font-face { font-family: 'Arial'; src: url('arial.otf'); } " +
                                            " .feediOS { font-family: 'Franchise'; font-size:43px; font-weight:bold; color: #FFFFFF; overflow:hidden;  white-space:nowrap; text-align: left; } " +
                                            " .feedAndroid{ font-family: 'Franchise'; font-size:30px; font-weight:bold; color: #FFFFFF; overflow:hidden;  white-space:nowrap; text-align: left; } " ;

        PreparedStatement stmt              = null;
        ResultSet rs                        = null;
        try {

            Element sponsorEl               = toAppend.addElement("sponsor");
            Element image                   = sponsorEl.addElement("imageURL");
            
            stmt                            = transconn.prepareStatement(selectPromotion);
            stmt.setInt(1, location);
            rs                              = stmt.executeQuery();
            if (rs.next()) {
                String rewards              = rs.getString(3);
                StringBuilder beerInfo      = new StringBuilder();
                String availability         = timeFormat.format(dbDateFormat.parse(rs.getString(4))) + " to " + timeFormat.format(dbDateFormat.parse(rs.getString(5)));
                beerInfo.append("<table width='100%' height='100%' border='0' cellspacing='0' cellpadding='0'>");
                if(platform==3) {
                    beerInfo.append("<tr><td><div class=\"feediOS\">Available ");
                    beerInfo.append(availability);
                    beerInfo.append("</div></td></tr><tr><td align='left'><div align=\"left\"><img src =\"");
                    beerInfo.append(rewards);
                    beerInfo.append("\"/></div></td></tr>");
                image.addElement("url").addText("http://beerboard.tv/USBN.BeerBoard.UI/bevSyncCustomers/" + rs.getString(2) + "/" + rewards);
                } else if(platform ==2) {
                    beerInfo.append("<tr><td><div class=\"feedAndroid\">Available ");
                    beerInfo.append(availability);
                    beerInfo.append("</div></td></tr><tr><td align='left'><div align=\"left\"><img src =\"/mnt/sdcard/sponsorAd/");
                    beerInfo.append(rewards.trim().replaceAll("\'", "%27").replaceAll(" ", "%20"));
                    beerInfo.append("\"/></div></td></tr>");
                image.addElement("url").addText("http://beerboard.tv/USBN.BeerBoard.UI/bevSyncCustomers/" + rs.getString(2) + "/" + rewards.trim().replaceAll("\'", "%27").replaceAll(" ", "%20"));
                }
                beerInfo.append( "</table>");
                sponsorEl.addElement("html").addText(beerInfo.toString());
                sponsorEl.addElement("css").addText(HandlerUtils.nullToEmpty(css));
                sponsorEl.addElement("promotionId").addText(String.valueOf(rs.getInt(1)));
            }

            if(rs.getInt(6) == 1 ) {
                sponsorEl.addElement("hasReward").addText("1") ;
                sponsorEl.addElement("buttonText").addText("Accept");
            } else {
                sponsorEl.addElement("hasReward").addText("0");
                sponsorEl.addElement("buttonText").addText("Close");
            }
        } catch ( Exception sqle) {
            logger.dbError("Database error: "+sqle.toString());
            throw new HandlerException(sqle);
        } finally {
           close(stmt);
           close(rs);

        }
        
    }

    
    
    private void getSponsorPromotion(Element toHandle, Element toAppend) throws HandlerException {

        int location                        = HandlerUtils.getRequiredInteger(toHandle, "locationId");
        int platform                        = HandlerUtils.getRequiredInteger(toHandle, "platform");
        String selectpromotion              = "SELECT bCR.id, bC.customer, bCr.file,bCR.endTime FROM bevSyncCampaignReward bCR LEFT JOIN bevSyncCampaign bC ON bC.id = bCR.campaign " +
                                            " LEFT JOIN bevSyncCreatives bCr ON bCr.id = bCR.creatives WHERE CONCAT(CURDATE(),' ',CURTIME()) " +
                                            " BETWEEN bCR.startTime AND bCR.endTime AND  bCr.type = 5 AND bCR.location = ?;";
        String  css                         = "@charset 'utf-8'; " +
                                            " @font-face { font-family: 'ChalkDust'; src: url('chalkdust.ttf'); } " +
                                            " @font-face { font-family: 'PPETRIAL'; src: url('PPETRIAL.otf'); } " +
                                            " @font-face { font-family: 'Arial'; src: url('arial.otf'); } " +
                                            " .feed{ font-family: 'Arial'; font-size:25px; font-weight:bold; color: #ff9900; overflow:hidden;  white-space:nowrap; } " ;
        
        PreparedStatement stmt              = null;       
        ResultSet rs                        = null;
        String rewards                      = "", customer = "";
        
        int rewardId                        = 0;
        ArrayList<String> url               = new ArrayList<String>();
        
        try {
            Element sponsorEl               = toAppend.addElement("sponsor");
            stmt                            = transconn.prepareStatement(selectpromotion);
            stmt.setInt(1, location);
            rs                              = stmt.executeQuery();
            if (rs.next()) {
                rewardId                    = rs.getInt(1);
                customer                    = rs.getString(2);
                rewards                     = rs.getString(3);               

                sponsorEl.addElement("validity").addText(HandlerUtils.nullToEmpty(rs.getString(4)));
                url.add("http://beerboard.tv/USBN.BeerBoard.UI/bevSyncCustomers/" + customer + "/" + rewards);
            }
            //url.add("http://www.usbeveragenet.com/Images/USBN-AboutUs.png");
            
            for(int i=0;i<url.size();i++) {
                    Element image           = sponsorEl.addElement("imageURL");
                    image.addElement("url").addText((platform == 3 ? url.get(i): url.get(i).trim().replaceAll("\'", "%27").replaceAll(" ", "%20")));
            }

            StringBuilder beerInfo          = new StringBuilder();
            beerInfo.append("<table width='100%' height='100%' border='0' cellspacing='2' cellpadding='0' style='padding-left: 15px;'><tr>");
            if(platform == 3){
                beerInfo.append("<td align='left'><div align=\"left\"><img src =\"" + rewards + "\"/></div></td></tr>");
                //beerInfo.append("<td align='left'><div align=\"left\"><img src =\"USBN-AboutUs.png\"/></div></td></tr>");
            }else if(platform == 2) {
                beerInfo.append("<td align='left'><div align=\"left\"><img src =\"/mnt/sdcard/sponsorAd/" + rewards.trim().replaceAll("\'", "%27").replaceAll(" ", "%20") + "\"/></div></td></tr>");
            }
            beerInfo.append( "</table>");

            sponsorEl.addElement("html").addText(beerInfo.toString());
            sponsorEl.addElement("css").addText(HandlerUtils.nullToEmpty(css));
            sponsorEl.addElement("promotionId").addText(String.valueOf(rewardId));
        } catch ( Exception sqle) {
            logger.dbError("Database error: "+sqle.toString());
            throw new HandlerException(sqle);
        } finally {
           close(stmt);
           close(rs);
        }
    }
    
    
    private void redeemSponsorPromotion(Element toHandle, Element toAppend) throws HandlerException {

        int user                            = HandlerUtils.getRequiredInteger(toHandle, "user");
        int promotion                       = HandlerUtils.getRequiredInteger(toHandle, "promotionId");
        String code                         = HandlerUtils.getRequiredString(toHandle, "code");
        String selectpromotion              = "SELECT id, count, code FROM bevSyncCampaignReward WHERE id = ? ; ";
        String updateUserArrival            = "UPDATE bbtvMobileUser SET arrival=null WHERE id =?;";
        PreparedStatement stmt              = null;
        ResultSet rs                        = null;
        Calendar currentDate                = Calendar.getInstance();

        try {
            stmt                            = transconn.prepareStatement(selectpromotion);
            stmt.setInt(1, promotion);
            rs                              = stmt.executeQuery();
            if (rs.next()) {
                int count                   = rs.getInt(2);
                if(count > 0) {
                    if(code.equals(rs.getString(3))) {
                        String today        = dbDateFormat.format(currentDate.getTime());
                        stmt                = transconn.prepareStatement("INSERT INTO promotionUserMap (user,promotion,reward,rewardDate) VALUES(?,?,?,?);");
                        stmt.setInt(1, user);
                        stmt.setInt(2, promotion);
                        stmt.setInt(3, 1);
                        stmt.setString(4, today);
                        stmt.executeUpdate();
                        toAppend.addElement("message").addText(String.valueOf("success"));

                        stmt               = transconn.prepareStatement("UPDATE bevSyncCampaignReward SET count = ? WHERE id=?;");
                        stmt.setInt(1, count-1);
                        stmt.setInt(2, promotion);
                        stmt.executeUpdate();

                        stmt               = transconn.prepareStatement(updateUserArrival);
                        stmt.setInt(1, user);
                        stmt.executeUpdate();
                    } else {
                        toAppend.addElement("message").addText(String.valueOf("Wrong Code"));
                        stmt                = transconn.prepareStatement(updateUserArrival);
                        stmt.setInt(1, user);
                        stmt.executeUpdate();
                    }
                } else {
                    toAppend.addElement("message").addText(String.valueOf("Sorry - Offer Max Reached"));
                }
            } else {
                toAppend.addElement("message").addText(String.valueOf("Sorry - Offer Expired"));
            }
        } catch ( Exception sqle) {
            logger.dbError("Database error: "+sqle.toString());
            throw new HandlerException(sqle);
        } finally {
           close(stmt);
           close(rs);
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
    
    
    public void writeHtml(String boardName,String location, String city, String zip,  String product, String abv, String orgin, String style, String brewery,String message) {
        try {
        StringBuffer sb                     = new StringBuffer();
        String brewVisibility               = "none";
        String glassVisibility              = "none";
        String glassUrl                     = "http://beerboard.tv/USBN.BeerBoard.UI/Images/FBglass/" + style + ".png";
        String brewUrl                      = "http://beerboard.tv/USBN.BeerBoard.UI/Images/FBlogo/"+brewery.trim().replaceAll("\'", "%27").replaceAll(" ", "%20");
         if(style==null && style.equals("") ){
             glassUrl                     = "http://beerboard.tv/USBN.BeerBoard.UI/Images/FBglass/Altbier.png";
         } else {
             if(!httpFileExists("http://beerboard.tv/USBN.BeerBoard.UI/Images/FBGlass/"+style +".png")){
                 glassUrl                     = "http://beerboard.tv/USBN.BeerBoard.UI/Images/FBglass/Altbier.png";
             }
         }
         
         if(!httpFileExists(brewUrl) || !brewUrl.contains(".png")){
             brewUrl                     = "http://beerboard.tv/USBN.BeerBoard.UI/Images/FBlogo/NoBrewery.png";
         }
         logger.debug("BrewLogo:"+brewUrl);
        
        if(!saveImage(brewUrl, "/home/midware/facebook/brewlogo.png")) {
            brewVisibility                  = "hidden";
        }
        if(!saveImage(glassUrl, "/home/midware/facebook/glasslogo.png")) {
            glassVisibility                 = "hidden";
        }
        
        sb.append("<html><head><style type=\"text/css\">");
        sb.append(getMarketingFBCSS());
        sb.append("body { }</style></head>");
        sb.append("<body>");
        sb.append("<table width=\"500\" height=\"20\" border='0' cellspacing='0' cellpadding='0' style='padding-left: 5px;'>");
        sb.append("<tr width=\"500\" height=\"20\"><td valign=\"top\" align=\"center\"><span class='beer_top_title'>"+message);
        sb.append("</span><p><span class='beer_loc_title'>");
        sb.append(boardName);
        sb.append("</span></p><p><span class='beer_list_title'></span></p><p><span class='beer_loc_title'>");
        sb.append(product);
        sb.append("</span></p></td></tr></table>");
        sb.append("<table width=\"500\" height=\"200\" border='0' cellspacing='0' cellpadding='0' style='padding-left: 15px;'>");
        sb.append("<tr><td valign=\"top\" align=\"center\"><img src=\"brewlogo.png");
        sb.append("\" width=\"500\" height=\"200\" alt=\"" + brewery.replaceAll("%20", " ") + "\" style=\"visibility:"+brewVisibility+"\"/></td></tr></table>");
        sb.append("<table width=\"500\" height=\"220\" border='0' cellspacing='0' cellpadding='0' style='padding-left: 15px;'>  ");
        sb.append("<tr><td width=\"250\" align=\"left\" valign=\"middle\"><span class='beer_abv'>"
                + "Style: " + style.replaceAll("%20", " ") + "</span><p><span class='beer_abv'>ABV: "+abv+"</span></p>"
                + "<p><span class=\"beer_abv\">Origin: "+orgin + "</span></p></td>");
        sb.append("<td width=\"250\" align=\"center\" valign=\"middle\"><img src=\"glasslogo.png");
        sb.append("\" width=\"100\" height=\"200\" alt=\"" + style.replaceAll("%20", " ") + "\" style=\"visibility:"+glassVisibility+"\"/><p><img src=\"beerboard.png\" /></p></td>");
        sb.append("</tr></table>");
        sb.append("</body></html>");
        
        
         File file                          = new File("/home/midware/facebook/productHTML.htm");
         BufferedWriter bw                  = new BufferedWriter(new FileWriter(file));
         bw.write(sb.toString());
         bw.close();
         //logger.debug(sb.toString());
        }catch (Exception e) {
                logger.dbError("Html: "+e.toString());
                e.printStackTrace();
        }

    }
    
    
    private String getMarketingFBCSS() throws HandlerException {

         String  css                        = "@charset 'utf-8';"
                                            + " body { background-image: url('background.jpg'); background-position: left top;"
                                            + "background-repeat: repeat; fieldset border-style:none; } .beer_top_title{ font-family: 'Franchise';"
                                            + "font-size:55px; font-weight:bold; color: #000000;} .beer_loc_title{ font-family: 'Franchise'; font-size:35px; font-weight:bold; color: #000000; overflow:hidden;  word-wrap:break-word; } "
                                            + ".beer_abv{font-family: 'Franchise'; font-size:25px; font-weight:bold; color: #000000; overflow:hidden;  white-space:nowrap; } ";
        return css;
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

   public void postOnFacebook(String user_id, String access_token, String message, String pageid, int location,int product) throws Exception {
        if(access_token!=null) {
            try {
                String g                    = "https://graph.facebook.com/fql?q=SELECT+page_id%2c+name%2C+pic%2C+access_token%2c+fan_count+FROM+page+WHERE+page_id+IN+(select+page_id+from+page_admin+where+uid="+user_id+")+&"+access_token;
                URL u = new URL(g);
                URLConnection c             = u.openConnection();
                signHttpsCertificate();
                HttpsURLConnection conn     = (HttpsURLConnection) u.openConnection();
                BufferedReader in           = new BufferedReader(new InputStreamReader(conn.getInputStream()));
                String inputLine;
                StringBuffer b              = new StringBuffer();
                while ((inputLine = in.readLine()) != null)
                    b.append(inputLine + "\n");
                in.close();
                String graph                = b.toString();
                JSONObject json             = new JSONObject(graph);
                JSONArray fa1               = new JSONArray(String.valueOf(json.get("data")));
                for(int i=0;i<fa1.length();i++) {
                    String accesstoken      = String.valueOf(fa1.getJSONObject(i).get("access_token"));
                    if(!accesstoken.equals("null")) {
                        String pagename     = fa1.getJSONObject(i).getString("name");
                        String fancount     = fa1.getJSONObject(i).getString("fan_count");
                        String pic          = fa1.getJSONObject(i).getString("pic");
                        String msg          ="testmessageSuba";
                        String tar          ="{'countries':['US']}";
                        String url          = "https://graph.facebook.com/"+pageid+"/feed?access_token="+accesstoken+"&method=post&message="+msg+"&feed_targeting="+tar;
                        url                 = "https://graph.facebook.com/"+pageid+"/tabs?access_token="+accesstoken;

                        u                   = new URL(url);
                        c                   = u.openConnection();
                        conn                = (HttpsURLConnection) u.openConnection();
                        in                  = new BufferedReader(new InputStreamReader(conn.getInputStream()));
                        b                   = new StringBuffer();

                        while ((inputLine = in.readLine()) != null)
                            b.append(inputLine + "\n");

                        in.close();
                        graph               = b.toString();
                        json                = new JSONObject(graph);
                        JSONArray fa2       = json.getJSONArray("data");
                        for(int n = 0; n < fa2.length(); n++) {
                            JSONObject object = fa2.getJSONObject(n);
                            if(object.toString().contains("application")) {
                                JSONObject fa3 = fa2.getJSONObject(n).getJSONObject("application");
                                //object = fa3.getJSONObject(n);
                                System.out.println("Application"+fa3.toString());
                                if(fa3.toString().contains("USBeverageNet")) {
                                    url = "https://graph.facebook.com/"+pageid+"/feed?access_token="+accesstoken+"&method=post&message="+msg+"&feed_targeting="+tar;
                                    String albumurl="https://graph.facebook.com/"+pageid+"/albums?access_token="+accesstoken+"&name=bbtv&message=BeerBoardTv&method=post";
                                    //  json = new JSONObject(albumid);
                                    String imageurl="https://graph.facebook.com/520776424610813/photos?access_token="+accesstoken+"&method=post&url=http://beerboard.tv/USBN.BeerBoard.UI/Images/logo/Arbor%20Brewing%20Company.png&message=Arbor%20Brewing%20Company&feed_targeting="+tar;
                                    //logger.debug("Post Photos");
                                    postHTML(user_id,pageid, accesstoken, message,location,product,1);
                                }
                            }
                        }
                    }
                }
            } catch (Exception e) {
                logger.dbError("Fb Post Error error: "+e.toString());
                e.printStackTrace();
            }
        }
    }
   
   
    public void postOnFacebook(String user_id, String access_token, String message,String pageId,int type,int location,int product) throws Exception {
        if(access_token!=null) {
            try {
                String g                    = "https://graph.facebook.com/fql?q=SELECT+page_id%2c+name%2C+pic%2C+access_token%2c+fan_count+FROM+page+WHERE+page_id+IN+(select+page_id+from+page_admin+where+uid="+user_id+")+&"+access_token;
                URL u = new URL(g);
                URLConnection c             = u.openConnection();
                signHttpsCertificate();
                HttpsURLConnection conn     = (HttpsURLConnection) u.openConnection();
                BufferedReader in           = new BufferedReader(new InputStreamReader(conn.getInputStream()));
                String inputLine;
                StringBuffer b              = new StringBuffer();
                while ((inputLine = in.readLine()) != null)
                    b.append(inputLine + "\n");
                in.close();
                String graph                = b.toString();
                JSONObject json             = new JSONObject(graph);
                JSONArray fa1               = new JSONArray(String.valueOf(json.get("data")));
                for(int i=0;i<fa1.length();i++) {
                    String accesstoken      = String.valueOf(fa1.getJSONObject(i).get("access_token"));
                    if(!accesstoken.equals("null")) {
                        String pageid       = fa1.getJSONObject(i).getString("page_id");
                        if(pageid.equals(pageId)) {
                            if(type==1 || type==3) {
                                postHTML(user_id, pageid, accesstoken, message, location, product, type);
                            } else {
                               // postOnFeed(accesstoken, pageid, message);
                            }
                        }
                    }                    
                }
            } catch (Exception e) {                
                logger.debug("Facebook error: "+e.getMessage());
            }
        }
    }
    
    
     private static class DefaultTrustManager implements X509TrustManager {

        @Override
        public void checkClientTrusted(X509Certificate[] arg0, String arg1) throws CertificateException {}

        @Override
        public void checkServerTrusted(X509Certificate[] arg0, String arg1) throws CertificateException {}

        @Override
        public X509Certificate[] getAcceptedIssuers() {
            return null;
        }
    }
    
    private void postHTML(String userId,String pageid, String accesstoken, String message,int location,int product,int type) {
       
       
        
         
             
        signHttpsCertificate();
        PostMethod filePost                 = new PostMethod("https://graph.facebook.com/"+pageid+"/photos");
        File file = null;
        Calendar currentDate                = Calendar.getInstance();
         String lastAccess                   = dbDateFormat.format(currentDate.getTime());
        
        PreparedStatement stmt              = null;
        //ResultSet rs                        = null;
        try {
            
           // stmt                       = transconn.prepareStatement("SELECT * FROM facebookPost WHERE typeId= ? AND location = ? AND timeDIFF(NOW(),postTime) < TIME('24:00:00');");
            // stmt.setInt(1,product);
           // stmt.setInt(2,location);
           // rs                         = stmt.executeQuery();
            //if(!rs.next()) {
            logger.debug("postHtml");
            filePost.getParams().setBooleanParameter(HttpMethodParams.USE_EXPECT_CONTINUE, false);
            BufferedImage  ire = HtmlToImage.create("file:////home/midware/facebook/productHTML.htm", 500, 650);
            ImageIO.write(ire, "jpg", new File("/home/midware/facebook/image.jpg"));
            file = new File("/home/midware/facebook/image.jpg");
            FilePart fp=new FilePart("source", file.getName(), file);
            Part[] parts ={fp, new StringPart("access_token", accesstoken), new StringPart("message", message)};
            filePost.setRequestEntity(new MultipartRequestEntity(parts, filePost.getParams()));
            
            signHttpsCertificate();
            HttpClient client = new HttpClient();
            signHttpsCertificate();
            client.getHttpConnectionManager().getParams().setConnectionTimeout(5000);    
            logger.debug("Reach http client");            
             signHttpsCertificate();
             // configure the SSLContext with a TrustManager
             SSLContext ctx = SSLContext.getInstance("TLS");
             ctx.init(new KeyManager[0], new TrustManager[] {new DefaultTrustManager()}, new SecureRandom());
             SSLContext.setDefault(ctx);
             
            int status = client.executeMethod(filePost);
            logger.debug("Reach status: " + status);
             signHttpsCertificate();
            if (status == HttpStatus.SC_OK) {
                logger.debug("Upload complete, response=" + filePost.getResponseBodyAsString());  
                JSONObject json = new JSONObject(filePost.getResponseBodyAsString());
	        logger.debug("id:"+json.getString("id"));
	        logger.debug("post_id:"+json.getString("post_id"));
                stmt                        = transconn.prepareStatement("INSERT INTO facebookPost (user,postId,albumId,location,type,typeId,postTime) VALUES (?, ?, ?, ?, ?,?,?)");
                stmt.setString(1, userId);
                stmt.setString(2, json.getString("post_id"));
                stmt.setString(3, json.getString("id"));
                stmt.setInt(4, location);
                stmt.setInt(5, type);
                stmt.setInt(6, product);
                stmt.setString(7, lastAccess);
                stmt.executeUpdate();
                //logger.debug("Post Completed");
                //tweetStatus();
            } else {
                logger.debug("Upload failed, response=" + HttpStatus.getStatusText(status));
                // Create response
                StringBuilder notificationsSendResponse = new StringBuilder();
                byte[] byteArrayNotifications = new byte[4096];
                for (int n; (n = filePost.getResponseBodyAsStream().read(byteArrayNotifications)) != -1;) {
                    notificationsSendResponse.append(new String(byteArrayNotifications, 0, n));
                }
                String notificationInfo = notificationsSendResponse.toString();
            }
            //}
        } catch (Exception ex) {
             logger.debug("facebook Post error: "+ex.getMessage());
        } finally {
            //close(rs);
              close(stmt);
            filePost.releaseConnection();
            
        }        
             
            
    }
    
    public void tweetStatus(int user,String message,String consumerKey,String consumerSecret,String oauthToken,String oauthSecret,int type,int location,int product) {
        
        try {
            signHttpsCertificate();
            PreparedStatement stmt              = null;
            Calendar currentDate                = Calendar.getInstance();
            String lastAccess                   = dbDateFormat.format(currentDate.getTime());
            BasicConfigurator.configure();
            ConfigurationBuilder cb             = new ConfigurationBuilder();
            cb.setDebugEnabled(true)
                    .setOAuthConsumerKey(consumerKey)
                    .setOAuthConsumerSecret(consumerSecret)
                    .setOAuthAccessToken(oauthToken)
                    .setOAuthAccessTokenSecret(oauthSecret).setHttpConnectionTimeout(100000);
            TwitterFactory tf                   = new TwitterFactory(cb.build());
            Twitter twitter                     = tf.getInstance();
            try {
                RequestToken requestToken       = twitter.getOAuthRequestToken();
                AccessToken accessToken         = null;
                while (null == accessToken) {
                    try {
                        accessToken             = twitter.getOAuthAccessToken(requestToken);
                        logger.debug("Step 2");
                    } catch (TwitterException te) {
                        logger.debug("In the Access exception:");
                        if (401 == te.getStatusCode()) {
                            logger.debug("Unable to get the access token.");
                        } else {
                            logger.debug("");
                            te.printStackTrace();
                        }
                    }
                }
                logger.debug("Got access token.");
                logger.debug("Access token: " + accessToken.getToken());
                logger.debug("Access token secret: " + accessToken.getTokenSecret());
            } catch (IllegalStateException ie) {
                // access token is already available, or consumer key/secret is not set.
                if (!twitter.getAuthorization().isEnabled()) {
                    logger.debug("OAuth consumer key/secret is not set.");
                    return;
                }
            } catch (Exception te) {
                logger.debug("In the request exception:");
                te.printStackTrace();
            }
            
            StatusUpdate status             = new StatusUpdate(message);
            //BufferedImage  ire = HtmlToImage.create("file:////home/midware/facebook/productHTML.htm", 500, 650);
            //ImageIO.write(ire, "jpg", new File("/home/midware/facebook/image.jpg"));
            logger.debug("Type: " + type);
            if(type < 5) {
                File im                     = new File("/home/midware/facebook/image.jpg");
                if(!im.exists()) {
                    logger.debug("File does not exist");
                    signHttpsCertificate();
                    BufferedImage  ire      = HtmlToImage.create("file:////home/midware/facebook/productHTML.htm", 500, 650);
                    ImageIO.write(ire, "jpg", new File("/home/midware/facebook/image.jpg"));
                }
                status.setMedia(new File("/home/midware/facebook/image.jpg"));
                Status status2              = twitter.updateStatus(status);
                stmt                        = transconn.prepareStatement("INSERT INTO twitterPost (user,postId,location,type,typeId,postTime) VALUES (?, ?, ?, ?, ?,?)");
                stmt.setInt(1, user);
                stmt.setString(2, String.valueOf(status2.getId()));
                stmt.setInt(3, location);
                stmt.setInt(4, type);
                stmt.setInt(5, product);
                stmt.setString(6, lastAccess);
                stmt.executeUpdate();
                logger.debug("Successfully updated the status to [" + status2.getText() + "].");
            } else {
                Status status2              = twitter.updateStatus(status);
                stmt                        = transconn.prepareStatement("INSERT INTO twitterPost (user,postId,location,type,typeId,postTime) VALUES (?, ?, ?, ?, ?,?)");
                stmt.setInt(1, user);
                stmt.setString(2, String.valueOf(status2.getId()));
                stmt.setInt(3, location);
                stmt.setInt(4, type);
                stmt.setInt(5, product);
                stmt.setString(6, lastAccess);
                stmt.executeUpdate();
                logger.debug("Successfully updated the status to [" + status2.getText() + "].");
            }
        } catch (TwitterException te) {
            te.printStackTrace();
            logger.debug("Failed to get timeline: " + te.getMessage());
        } catch(Exception ie){
            ie.printStackTrace();;
            logger.debug("Exception: " + ie.getMessage());
        }
    }
    
    
    public void tweetStatus(int location,String message,int type,int product, int customerId) {
        PreparedStatement stmt              = null;
        ResultSet rs                        = null,rsDetail= null;
         try {
             if(location>0) {
                 stmt                       = transconn.prepareStatement("SELECT DISTINCT  t.consumerKey, t.consumerSecret, t.accesToken, t.tokenSecret, t.id "
                                            + " FROM usbnTwitter t LEFT JOIN twitterLocationMap l ON l.twitter = t.id LEFT JOIN socialMediaPost sMP ON sMP.location = l.location " +
                                            " WHERE l.location = ? AND sMP.lineUpdate = 1 AND sMP.type=2;");
                 stmt.setInt(1,location);                
                 rs                         = stmt.executeQuery();
                 while(rs.next()) {
                     String consumerKey     = rs.getString(1);
                     String consumerSecret  = rs.getString(2);
                     String accesToken      = rs.getString(3);
                     String tokenSecret     = rs.getString(4);
                     int userId             = rs.getInt(5);
                     
                     int lineUpdates        =0,likes = 0,checkin=0;
                     stmt                   = transconn.prepareStatement("SELECT lineUpdate, likes, checkin FROM socialMediaPost WHERE type=2 AND location=?;");
                     stmt.setInt(1, location);
                     rsDetail                = stmt.executeQuery();
                     if(rsDetail.next()) {
                         lineUpdates        = rsDetail.getInt(1);
                         likes              = rsDetail.getInt(2);
                         checkin            = rsDetail.getInt(3);
                     }

                     logger.debug("Tweeting for location: " + location + ", type: " + type + ", type: " + lineUpdates);                     
                     if(type < 5 && lineUpdates >  0){
                         tweetStatus(userId,message, consumerKey, consumerSecret, accesToken, tokenSecret, type, location, product);
                     } else if(type == 5 && likes > 0){
                         tweetStatus(userId,message, consumerKey, consumerSecret, accesToken, tokenSecret,type,location,product);
                     } else if(type==6 && checkin >0){
                         tweetStatus(userId,message, consumerKey, consumerSecret, accesToken, tokenSecret,type,location,product);
                     }
                 }
             }
             if(type ==1) {
                 if(customerId != 205){
                     //tweetStatus(3, message, "Hs4u3FYn1KbeqA9ztuFyDQ", "rMQcqe6TKuemxP7MZoduJVGsx9SwdEAeInTufnb6U", "169164243-8SM0eCmER85akyk9J8TOau5gKcFDoHQN5Vk8QIqo","zVED8YrRyKwAZQEyXuV0sehWXxXRkaNkJEh7DEoMjIhzh",1,location,product);
                 }
             }
         } catch(Exception ie){
            ie.printStackTrace();
        } finally {           
             close(rsDetail);
             close(rs);
             close(stmt);
            }
    }
    
     private void pushNotificationApple(Element toHandle, Element toAppend) throws HandlerException {

        try {
            /*PushNotificationPayload simplePayLoad
                                            = new PushNotificationPayload();
            simplePayLoad.addAlert("High Variance Alert @ Cure Club : -13.90%");
            simplePayLoad.addBadge(1);
            simplePayLoad.addSound("default");
            simplePayLoad.addCustomDictionary("location", 773);
            simplePayLoad.addCustomDictionary("customer", 79);

            ArrayList<String> mobileTokens  = new ArrayList<String>();
            //mobileTokens.add("c480018df21ba93492d444078e5edc08fa6d5624e5f78834f5f0b6f8e246de6a");
            mobileTokens.add("c83a01dce08cae1f4a9faa8dc10b5aea738109d829037dce84204205ef7d5688");
            //mobileTokens.add("752b2d2d5f1f3f36f5c9ffbae428db27992dd3a3363a664fbd2c4b9b46dd3281");
            mobileTokens.add("62c1f005e20849f4660867eea59e4e1f95c1195ef6633b9fce8173e778726f25");
            //mobileTokens.add("1fc9324060af392e12c9c1779c062d0bdffe3c343a521ba3c5d766f49f63ac55");

            for (int i = 0; i < mobileTokens.size(); i++) {
                //Push.alert("Magner's Apple Cider - New Beer on Tap @ Kitty Hoynes", "/home/midware/Push Notification/usbnapn.p12", "usbn", false, mobileTokens.get(i));
                //Push.payload(simplePayLoad, "/home/midware/Push Notification/usbnapnDev.p12", "usbn", false, "806fab532a8aca1acc784ff65e996753b5493594253e5f100a9ff6b7f4a21a57");
                Push.payload(simplePayLoad, "/home/midware/Push Notification/bevmanapn.p12", "usbn", false, mobileTokens.get(i));
            }*/
            PushNotificationPayload simplePayLoad=new PushNotificationPayload();
          simplePayLoad.addAlert("Get Your Reward for Shock Top Seasonal @Blue Tusk ");
          simplePayLoad.addBadge(1);
         // simplePayLoad.addSound("default");
          simplePayLoad.addCustomDictionary("location", 853);
          
            logger.debug(""+Push.payload(simplePayLoad, "/home/midware/Push Notification/usbnapn1.1.2.p12", "usbn", true, "6b6329e59b525ab901118dba0efd73f1932c4654c18023baab958917d42d1a8b"));
    	  }catch (Exception e) {
    		  e.printStackTrace();
    	  }
     }
        
     private void pushNotificationAndroid(Element toHandle, Element toAppend) throws HandlerException {             
         PreparedStatement stmt             = null;
         ResultSet rs                       = null, rsDetails = null;
         try {
            
             //String deviceToken             = "APA91bEBPtfsnq1Vf1QCy-sjmRwSvdjAyaa03_2DyHZdYkAobSSCty1VkEZCUwWgFNUNn8-3_Eacy9jLQzB5mFIowQhv8mqAqeOC3ecTcTLpsrW5Kb_kmzmmpA3yA6VlO9DkSmgZ3sykvHOzLj6PXz98tw2hgKnu1A";
             //suba String deviceToken = "APA91bEktv-LKoYxjZyIwsMq74dJA1JArmOJ4Sash_DAX3OslBoidK6iLgOBkLFG7r3uYy8n1Hm-4wIUSix4u8Dl0csHXB72nmJjjsKiFTpzQ6rCWOrrIypVqDlgGxUFIMUiUX5MHxh0mGm-n_B4wAzidplae3_q-g";
             
             //System.out.println(Thread.activeCount());
             //String name                    = Thread.currentThread().getName();
             //AIzaSyDjrsCeKvnCWTTVr73bFwhVVdBibOaEfxs  old
			//AIzaSyC_x06LqW2_7KzScA-8MyunIrLAUcekyz4
             Sender sender                  = new Sender("AIzaSyC_x06LqW2_7KzScA-8MyunIrLAUcekyz4");
             
             //Sender sender = new Sender("AIzaSyB9h-gJ2k4vQ9iv9fFJ0VbS0w7luCJV0HY");
             //Message message                = new Message.Builder().addData("message", "Test Suba For Usbn").build();
              Message pushMessage = new Message.Builder()
                            .delayWhileIdle(true) // Wait for device to become active before sending.
                            .addData( "message", "Get Your Reward for Shock Top Seasonal @Blue Tusk ")
                            .addData( "location", String.valueOf(425) )
                            .addData( "messageId", String.valueOf(1) )
                            .build();
            /// System.out.println("deviceToken " + deviceToken);
             //Result result                  = sender.send(pushMessage, deviceToken, 1);
             //logger.debug(result.toString());
               
         String selectUser                  = "SELECT u.id,platform,u.deviceToken FROM bbtvMobileUser u WHERE LENGTH(u.deviceToken) > 4 AND u.id IN (4746) ;";

     
            stmt                            = transconn.prepareStatement(selectUser);        
            rs = stmt.executeQuery();
            while(rs.next()){
                int user                    = rs.getInt(1);
                int platform                = rs.getInt(2);
                String deviceToken          = rs.getString(3);
                logger.debug("User:"+user+" platform:"+platform); 
                 switch (platform) {
                case 2:
                    logger.debug("Enter ");
                     sender = new Sender("AIzaSyC_x06LqW2_7KzScA-8MyunIrLAUcekyz4");
                     //Sender sender = new Sender("AIzaSyB9h-gJ2k4vQ9iv9fFJ0VbS0w7luCJV0HY");
                    //Message message = new Message.Builder().addData("message", "Test Suba For Usbn").build();
                     pushMessage = new Message.Builder()
                            .delayWhileIdle(false) // Wait for device to become active before sending.
                            .addData( "message", "Get Your Reward for Shock Top Seasonal @Clarenton ")
                            .addData( "location", String.valueOf(425) )
                            .addData( "messageId", String.valueOf(2) )
                            .build();
                    Result result = sender.send(pushMessage, deviceToken, 1);
                    logger.debug(result.toString());
                    break;
            }
               
               
            }
            
            
    	 
             
          }catch (Exception e) {
    		 logger.debug(e.getMessage());
    	  }  finally {
            close(rsDetails);
            close(rs);
            close(stmt);
        }
    }
     
     
    private void promoNightPushMessage(Element toHandle, Element toAppend) throws HandlerException {
        
        PreparedStatement stmt              = null;
        ResultSet rs                        = null, rsDetails = null;
        String selectLocationPromo          = "SELECT id, location, startTime, endTime, code, count FROM bevSyncCampaignReward WHERE count > 0 AND NOW() BETWEEN startTime AND endTime;";
        String selectLastUserPush           = "SELECT GROUP_CONCAT(DISTINCT user) FROM bbtvMobileUserRewardPush WHERE promo = ? GROUP BY promo";
        String getLastId                    = "SELECT LAST_INSERT_ID()";

        try {
                String userIdString         = "0";
                int promoId                 = 1;
                int locationId              = 853;
                String startTime            = "2014-02-08 13:00:00";
                int winnerCount             = 2;

                stmt                        = transconn.prepareStatement(selectLastUserPush);
                stmt.setInt(1, promoId);
                rs                          = stmt.executeQuery();
                if(rs.next()){
                    userIdString            = rs.getString(1);
                    logger.debug(userIdString);
                }

                String selectPromoNightUsers= "SELECT bMU.id, bMU.username FROM bbtvMobileUser bMU " +
                                            " WHERE bMU.registration > ? AND bMU.id NOT IN (" + userIdString + ")";

                ArrayList<Integer> userArray= new ArrayList<Integer>();
                ArrayList<String> usernameArray
                                            = new ArrayList<String>();
                ArrayList<Integer> sentUserArray
                                            = new ArrayList<Integer>();

                stmt                        = transconn.prepareStatement(selectPromoNightUsers);
                stmt.setString(1, startTime);
                rs                          = stmt.executeQuery();
                while (rs.next()){
                    userArray.add(rs.getInt(1));
                    usernameArray.add(rs.getString(2));
                }

                logger.debug("Size of array: " + userArray.size());

                for (int i = 0; i < winnerCount; i++) {
                    int userIdIndex         = new Random().nextInt(userArray.size());
                    int userId              = userArray.get(userIdIndex);
                    if (!sentUserArray.contains(userId)) {
                        sentUserArray.add(userId);
                        logger.debug("Random user: " + usernameArray.get(userIdIndex));

                        stmt                = transconn.prepareStatement("INSERT INTO bbtvMobileUserRewardPush (user, promo) VALUES (?, ?)");
                        stmt.setInt(1, userId);
                        stmt.setInt(2, promoId);
                        stmt.executeUpdate();

                        String message      = "Congratulations... You've won a BeerBoard Gift. Please show this message at Booth 51 to receive the gift.";

                        Calendar currentDate= Calendar.getInstance();
                        stmt                = transconn.prepareStatement("INSERT INTO pushMessage (message, location, reward, pushTime) VALUES (?, ?, ?, ?)");
                        stmt.setString(1, message);
                        stmt.setInt(2, locationId);
                        stmt.setInt(3, promoId);
                        stmt.setString(4, dbDateFormat.format(currentDate.getTime()));
                        stmt.executeUpdate();

                        stmt                = transconn.prepareStatement(getLastId);
                        rs                  = stmt.executeQuery();
                        if(rs.next()) {
                            int messageId   = rs.getInt(1);
                            stmt            = transconn.prepareStatement("SELECT bMU.platform, bMU.deviceToken, bMU.username FROM bbtvMobileUser bMU WHERE bMU.id = ?");
                            stmt.setInt(1, userId);
                            rs              = stmt.executeQuery();
                            if(rs.next()) {
                                int platform= rs.getInt(1);
                                String deviceToken
                                            = rs.getString(2);
                                switch (platform) {
                                case 2:
                                    Sender sender
                                            = new Sender("AIzaSyDjrsCeKvnCWTTVr73bFwhVVdBibOaEfxs");
                                     //Sender sender = new Sender("AIzaSyB9h-gJ2k4vQ9iv9fFJ0VbS0w7luCJV0HY");
                                    //Message message = new Message.Builder().addData("message", "Test Suba For Usbn").build();
                                    Message pushMessage = new Message.Builder()
                                            .delayWhileIdle(false) // Wait for device to become active before sending.
                                            .addData( "message", message)
                                            .addData( "location", String.valueOf(locationId) )
                                            .addData( "messageId", String.valueOf(messageId) )
                                            .build();
                                    Result result
                                            = sender.send(pushMessage, deviceToken, 1);
                                    break;
                                case 3:
                                    PushNotificationPayload simplePayLoad
                                            = new PushNotificationPayload();
                                    simplePayLoad.addAlert(message);
                                    simplePayLoad.addBadge(1);
                                    simplePayLoad.addSound("default");
                                    simplePayLoad.addCustomDictionary("location", locationId);
                                    simplePayLoad.addCustomDictionary("messageId", messageId);
                                    Push.payload(simplePayLoad, "/home/midware/Push Notification/usbnapn.p12", "usbn", true, deviceToken);
                                    break;
                                }
                                stmt        = transconn.prepareStatement("UPDATE bevSyncCampaignReward SET count = count - 1 WHERE id = ?)");
                                stmt.setInt(1, promoId);
                                stmt.executeUpdate();
                            }
                        }
                    }
                }
            
    	  }catch (Exception e) {
    		  e.printStackTrace();
    	  }  finally {
            close(rsDetails);
            close(rs);
            close(stmt);
        }
     }

    private void sendManualPushMessage(Element toHandle, Element toAppend) throws HandlerException {

        String message                      = HandlerUtils.getRequiredString(toHandle, "message");
        String userString                   = HandlerUtils.getRequiredString(toHandle, "userString");
        int location                        = HandlerUtils.getRequiredInteger(toHandle, "location");
        int messageId                       = HandlerUtils.getRequiredInteger(toHandle, "messageId");

        PreparedStatement stmt              = null;
        ResultSet rs                        = null, rsDetails = null;
        String selectUserByLoc              = "SELECT id,platform, deviceToken, username FROM bbtvMobileUser WHERE LENGTH(deviceToken) > 4 AND id IN (" + userString + ")";
        try {

            stmt                            = transconn.prepareStatement(selectUserByLoc);
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
                    Message pushMessage = new Message.Builder()
                            .delayWhileIdle(false) // Wait for device to become active before sending.
                            .addData( "message", message)
                            .addData( "location", String.valueOf(location) )
                            .addData( "messageId", String.valueOf(messageId) )
                            .build();
                    Result result = sender.send(pushMessage, deviceToken, 1);
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
                    simplePayLoad.addCustomDictionary("logo", "http://bevmanager.net/Images/Location_logo/Tullys.jpg");
                    //Push.payload(simplePayLoad, "/home/midware/Push Notification/usbnapnDev.p12", "usbn", false, deviceToken);
                    //Push.payload(simplePayLoad, "/home/midware/Push Notification/usbnapn.p12", "usbn", true, deviceToken);
                    Push.payload(simplePayLoad, "/home/midware/Push Notification/usbnapnNew.p12", "usbn", true, deviceToken);
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
     
    private void debugFacebookToken(Element toHandle, Element toAppend) throws HandlerException {
         PreparedStatement stmt              = null;
        ResultSet rs                        = null;
         try {
             stmt                = transconn.prepareStatement("SELECT id, username,fbID,token FROM bbtvMobileUser");
             rs            = stmt.executeQuery();
             while(rs.next()) { 
                 
                 if(rs.getString(3)!=null && !rs.getString(3).equals("")) { 
                     if(rs.getString(3).length() >10) {
                         logger.debug("userName:"+rs.getString(2));
                         int id=rs.getInt(1);
                         String token = HandlerUtils.nullToEmpty(rs.getString(4));
                         if(token==null || token.equals("")){
                             stmt                    =transconn.prepareStatement("UPDATE bbtvMobileUser set fbID='', token='' WHERE platform != 7 AND id=?");
                             stmt.setInt(1,id);
                             stmt.executeUpdate();
                         } else {
                             debugFacebookToken(id,rs.getString(4));
                         }
                     }
                 }
             }
             
             } catch (Exception e) {
             e.printStackTrace();
         } finally {           
             close(rs);
             close(stmt);
            }
	}
    
    public static void signHttpsCertificate(){
        TrustManager[] trustAllCerts        = new TrustManager[]{new X509TrustManager(){
            public X509Certificate[] getAcceptedIssuers(){return null;}
            public void checkClientTrusted(X509Certificate[] certs, String authType){}
            public void checkServerTrusted(X509Certificate[] certs, String authType){}
        }};
        
        // Install the all-trusting trust manager
        try {
            SSLContext sc                   = SSLContext.getInstance("TLS");
            sc.init(null, trustAllCerts, new SecureRandom());
            HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());
        } catch (Exception e) {
               e.printStackTrace();
        }
    }
     
     
     public void debugFacebookToken( int id,String access_token) throws Exception {
         Calendar currentDate                = null;
         PreparedStatement stmt              = null;
        if(access_token!=null) {
            try {
                //access_token                = access_token.replace("&access_token", "");
                String g                    = "https://graph.facebook.com/oauth/access_token_info?client_id=536074729771401&access_token="+access_token.replace("&access_token", "");
               URL u = new URL(URIUtil.encodeQuery(g));
               signHttpsCertificate();
        
        HttpsURLConnection httpConn = (HttpsURLConnection)u.openConnection();
        InputStream is;
        if (httpConn.getResponseCode() >= 400) {
            is = httpConn.getErrorStream();
            String data=getInputStreamData(is);
            JSONObject json = new JSONObject(data);
            logger.debug((json.getJSONObject("error")).getString("message"));
            stmt                    =transconn.prepareStatement("UPDATE bbtvMobileUser set fbID='', token='' WHERE id=?");
            stmt.setInt(1,id);
            stmt.executeUpdate();
        } else {
            is = httpConn.getInputStream();
            String data=getInputStreamData(is);
            JSONObject json = new JSONObject(data);
            int expire=Integer.parseInt(json.getString("expires_in"));
         	System.out.println(json.getString("expires_in"));
         	currentDate                 = Calendar.getInstance();
                
                Date today                  = currentDate.getTime();
         	currentDate.add(Calendar.SECOND,expire);  
                long[] diff = getTimeDifference(dbDateFormat.parse(dbDateFormat.format(currentDate.getTime())), dbDateFormat.parse(dbDateFormat.format(today)));
                //logger.debug("Token Expired in "+diff[0]+"  days");
                if(diff[0]<10) {
                    String debug="https://graph.facebook.com/oauth/access_token?grant_type=fb_exchange_token&client_id=536074729771401&client_secret=06d20a4e888b46d54434fde0adf5369c&fb_exchange_token="+access_token;
                    u = new URL(URIUtil.encodeQuery(debug));
                    httpConn = (HttpsURLConnection)u.openConnection();
                    if (httpConn.getResponseCode() >= 400) {
                        is = httpConn.getErrorStream();
                        data=getInputStreamData(is);
                        logger.debug("error: "+data);
                        stmt                    =transconn.prepareStatement("UPDATE bbtvMobileUser set fbID='', token='' WHERE id=?");
                        stmt.setInt(1,id);
                        stmt.executeUpdate();
                    } else {
                   // String newToken = getData(new URL(debug));
                        is = httpConn.getInputStream();
                        String newToken =getInputStreamData(is);
                        stmt                = transconn.prepareStatement("UPDATE bbtvMobileUser SET token= ? WHERE id =? ");
                        stmt.setString(1, newToken);
                        stmt.setInt(2, id);
                        stmt.executeUpdate();
                        logger.debug("Token Expired in "+diff[0]+"  days");
                        logger.debug("New Token"+newToken);
                    }
                    
                }
         	//logger.debug(dbDateFormat.format(currentDate.getTime()));
        }
         
                
            } catch (Exception e) {
                logger.dbError("Debug Facebook error: "+e.getMessage());
                e.printStackTrace();
            }
        }
    }
     
     public int checkFacebookToken( String access_token) throws Exception {
         int status                     = 0;
        if(access_token!=null) {
            try {
                //access_token                = access_token.replace("&access_token", "");
                String g                    = "https://graph.facebook.com/oauth/access_token_info?client_id=536074729771401&access_token="+access_token.replace("&access_token", "");
               URL u = new URL(URIUtil.encodeQuery(g));
        signHttpsCertificate();
        HttpsURLConnection httpConn = (HttpsURLConnection)u.openConnection();
        InputStream is;
        if (httpConn.getResponseCode() >= 400) {
            is = httpConn.getErrorStream();
            String data=getInputStreamData(is);
            JSONObject json = new JSONObject(data);
            logger.debug((json.getJSONObject("error")).getString("message"));
            status                          = 0;
        } else {
            status                          = 1;
            
        }
        } catch (Exception e) {
                logger.dbError("CheckFBToken error: "+e.getMessage());
                e.printStackTrace();
            }
            
        }
        return status;
    }

    private String getInputStreamData(InputStream is) {
        StringBuilder results = new StringBuilder();
		 try {
		
	        BufferedReader reader = new BufferedReader(new InputStreamReader(is));
	       
	        String line;
	        while ((line = reader.readLine()) != null) {
	            results.append(line);
	        }	       
		 }catch (Exception e) {
				e.printStackTrace();
			}
	        return results.toString();
    }
    
    
    private void getNotificationCenter (int platform, int user, Element toAppend) throws HandlerException {

        PreparedStatement stmt              = null;
        ResultSet rs                        = null, rsDetails = null;
        boolean state                       = false;
        String selectPushMessage            = "SELECT pM.message, m.message, m.location, IFNULL(REPLACE(g.logo, ' ', '%20'), 'USBNLogo.png'), ADDDATE(m.pushTime, INTERVAL 3 DAY) "
                                            + " FROM pushMessageMap pM LEFT JOIN pushMessage m ON m.id = pM.message LEFT JOIN locationGraphics g ON g.location=m.location  "
                                            + " WHERE pM.user = ? AND m.pushTime BETWEEN SUBDATE(now(), INTERVAL 3 DAY) AND NOW() ORDER BY m.pushTime DESC;" ;        
        String selectPushMessage1           = "SELECT DISTINCT pM.message, m.message,  pM.count, DATE(m.pushTime),ADDDATE(m.pushTime, INTERVAL 3 DAY), m.type "
                                            + " FROM pushMessageMap pM LEFT JOIN pushMessage m ON m.id = pM.message   WHERE pM.user = ? AND m.pushTime BETWEEN SUBDATE(now(), INTERVAL 3 DAY) AND NOW() ORDER BY m.pushTime DESC, type ASC;" ;        
        String selectMessageCenter          = "SELECT m.id, m.message, m.location, (SELECT CONCAT(boardname, ' - ', name) FROM location WHERE id=m.location) FROM pushMessageCenter m "
                                            + "  WHERE m.user=? and m.pushMessage=?;";
        String selectMessageCount           = "SELECT DATE(m.pushTime),(SELECT count(pMC.id) FROM pushMessageCenter pMC LEFT JOIN pushMessage pM ON pM.id = pMC.pushMessage " +
                                            " WHERE pMC.user = ? and  DATE(pM.pushTime) = DATE(m.pushTime)) FROM pushMessageMap pM LEFT JOIN pushMessage m ON m.id = pM.message " +
                                            " WHERE pM.user = ? AND m.pushTime BETWEEN SUBDATE(now(), INTERVAL 3 DAY) AND NOW() ORDER BY m.pushTime DESC LIMIT 1;";
        
        logger.debug("getting notifications");
        try {
           /* if(user !=3599){
            stmt                            = transconn.prepareStatement(selectPushMessage);
            stmt.setInt(1,user);
            rs                              = stmt.executeQuery();
            int count                       = 0;
            while (rs.next()) {       
                int pushMessage             = rs.getInt(1);
                String validity             = HandlerUtils.nullToEmpty(rs.getString(5));
                Element msgEl               = toAppend.addElement("pushMessage");
                
                msgEl.addElement("message").addText(rs.getString(2));
                msgEl.addElement("messageId").addText(String.valueOf(rs.getInt(1)));
                msgEl.addElement("location").addText(String.valueOf(rs.getInt(3)));                                    
                msgEl.addElement("count").addText(String.valueOf(0));
                msgEl.addElement("type").addText(String.valueOf(1));
                msgEl.addElement("validity").addText(validity);
                msgEl.addElement("logo").addText(HandlerUtils.nullToEmpty(rs.getString(4).trim().replaceAll("\'", "%27").replaceAll(" ", "%20")));
                state                       = true;
            }
            }
            if(user ==3599)
            */{
            if(!isProduction()){
                user                        = 4509;
            }
                
                stmt                            = transconn.prepareStatement(selectPushMessage1);
                stmt.setInt(1,user);
                rs                              = stmt.executeQuery();                                                  
                while (rs.next()) {       
                    int pushMessage             = rs.getInt(1);
                    String messageHead          = HandlerUtils.nullToEmpty(rs.getString(2));
                    String validity             = HandlerUtils.nullToEmpty(rs.getString(5));
                    int type                    = rs.getInt(6);                    
                    if(type == 1){
                        messageHead             ="Just Tapped";
                    } else if(type==2) {
                        messageHead             ="Promotions & Offers";
                    }
                    String PushTime             = HandlerUtils.nullToEmpty(rs.getString(4));
                    
                    Element msgHEl               = toAppend.addElement("pushMessage");                                        
                    msgHEl.addElement("message").addText(messageHead);
                    msgHEl.addElement("messageId").addText(String.valueOf(pushMessage));
                    msgHEl.addElement("location").addText(String.valueOf(0));                    
                    msgHEl.addElement("type").addText(String.valueOf(0));
                    msgHEl.addElement("mType").addText(String.valueOf(type));
                    msgHEl.addElement("validity").addText(validity);
                    msgHEl.addElement("locationName").addText(HandlerUtils.nullToEmpty(rs.getString(4)));
                    msgHEl.addElement("pushTime").addText(HandlerUtils.nullToEmpty(rs.getString(4)));
                    stmt                        = transconn.prepareStatement(selectMessageCenter);
                    stmt.setInt(1,user);
                    stmt.setInt(2,pushMessage);
                    rsDetails                   = stmt.executeQuery();
                    while (rsDetails.next()) { 
                    Element msgEl               = toAppend.addElement("pushMessage");

                    msgEl.addElement("message").addText(rsDetails.getString(2));
                    msgEl.addElement("messageId").addText(String.valueOf(pushMessage));
                    msgEl.addElement("location").addText(String.valueOf(rsDetails.getInt(3)));                    
                    msgEl.addElement("type").addText(String.valueOf(1));
                    msgEl.addElement("mType").addText(String.valueOf(type));
                    msgEl.addElement("validity").addText(validity);
                    msgEl.addElement("locationName").addText(HandlerUtils.nullToEmpty(rsDetails.getString(4)));
                    msgEl.addElement("pushTime").addText(PushTime);                    
                   
                    }
                }

                stmt                        = transconn.prepareStatement("SELECT id FROM pushMessageCheck WHERE user = ?");
                stmt.setInt(1,user);
                rs                          = stmt.executeQuery();
                if (!rs.next() || platform == 2) {
                    stmt                    = transconn.prepareStatement(selectMessageCount);
                    stmt.setInt(1,user);
                    stmt.setInt(2,user);
                    rs                      = stmt.executeQuery();
                    if (rs.next()) {
                        toAppend.addElement("messageCount").addText(String.valueOf(rs.getInt(2)));
                    } else {
                        toAppend.addElement("messageCount").addText(String.valueOf(0));
                    }
                    
                    stmt                    = transconn.prepareStatement("INSERT INTO pushMessageCheck (user) VALUES (?) ");
                    stmt.setInt(1, user);
                    stmt.executeUpdate();
                }
            }
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
    
    private void sendRewardRemainder(Element toHandle, Element toAppend) throws HandlerException {
        String checkReward          = "SELECT location,creatives,id FROM bevSyncCampaignReward WHERE DATEDIFF( now(),startTime) =7";
        String selectProduct        = "SELECT pS.name,p.name,(SELECT CONCAT(boardname,'-',name,',',addrCity) from location where id=?) from bevSyncCreatives bC "
                                    + " LEFT JOIN product p ON p.id=bC.product LEFT JOIN productSet pS ON pS.id=bC.brewery WHERE pS.productSetType=7 AND bC.id=?;";
        String selectToken          = "select Distinct p.pageid, u.user_id, u.access_token from usbnFacebook u LEFT JOIN usbnFacebookPage p ON u.user_id =p.fbid where p.location=?  and u.user_id!='5515563';";
        
        String brewery              = null,locationName = null,product= null, postMessage = "";
        PreparedStatement stmt              = null;
        ResultSet rs                        = null,rsDetails= null,rsFB = null;
         try {
             stmt                           = transconn.prepareStatement("SELECT mP.id,mP.user,u.platform,pM.id,pM.message,pM.location,mP.count,u.deviceToken FROM bevSyncCampaignReward bC LEFT JOIN pushMessage pM ON pM.reward=bC.id "
                                            + " LEFT JOIN pushMessageMap mP ON mP.message=pM.id LEFT JOIN bbtvMobileUser u ON u.id=mP.user WHERE now() BETWEEN bC.startTime AND bC.endTime;");
             rs                             = stmt.executeQuery();
             while(rs.next()) { 
                int mapId                   = rs.getInt(1) ;
                int user                    = rs.getInt(2) ;
                int platform                = rs.getInt(3) ;
                int messageId               = rs.getInt(4) ;
                String message              = rs.getString(5);
                int location                = rs.getInt(6) ;
                int count                   = rs.getInt(7) ;              
                String deviceToken          = rs.getString(8);
                switch (platform) {
                    case 2:
                        Sender sender       = new Sender("AIzaSyCf6IJFaHoVV732Bb1_EVnVFbUs-ESF84s");
                        //Message message = new Message.Builder().addData("message", "Test Suba For Usbn").build();
                        Message pushMessage = new Message.Builder()
                                .delayWhileIdle(true) // Wait for device to become active before sending.
                                .addData( "message", message)
                                .addData( "location", String.valueOf(location) )
                                .addData( "messageId", String.valueOf(messageId) )
                                .build();
                        Result result       = sender.send(pushMessage, deviceToken, 1);    
                        break;
                    case 3:
                        PushNotificationPayload simplePayLoad
                                            = new PushNotificationPayload();
                        simplePayLoad.addAlert(message);
                        simplePayLoad.addBadge(1);
                        simplePayLoad.addSound("default");
                        simplePayLoad.addCustomDictionary("location", location);
                        simplePayLoad.addCustomDictionary("messageId", messageId);
                        Push.payload(simplePayLoad, "/home/midware/Push Notification/usbnapn.p12", "usbn", true, deviceToken);
            
                    break;
                }
               stmt                         = transconn.prepareStatement("UPDATE pushMessageMap SET count = ? WHERE id =?;");    
               stmt.setInt(1,count+1);
               stmt.setInt(2,mapId);
               stmt.executeUpdate();
             }
               
                stmt                        = transconn.prepareStatement(checkReward);
                rs                             = stmt.executeQuery();
             while(rs.next()) { 
                 int location       =rs.getInt(1);
                 int creatives      = rs.getInt(2);
                 int rewardId      = rs.getInt(2);
                stmt                        = transconn.prepareStatement(selectProduct);
                    stmt.setInt(1,location);
                    stmt.setInt(2,creatives);
                    rsDetails                   = stmt.executeQuery();
                    if(rsDetails.next()) {
                        brewery                 = rsDetails.getString(1);                        
                        product                 = rsDetails.getString(2);                        
                        locationName            = rsDetails.getString(3);                        
                    }
                    if(product.equals("Unknown Product") ){
                        postMessage                 = "Get Your Reward for "+brewery +" @ "+locationName;
                    } else {
                        postMessage                 = "Get Your Reward for "+product +" @ "+locationName;
                    }
                    stmt                        = transconn.prepareStatement("SELECT customer,file from bevSyncCreatives WHERE id= ?");
                    stmt.setInt(1,rs.getInt(2));
                    rsDetails                   = stmt.executeQuery();
                    if(rsDetails.next()) {
                        String fileName     = "http://beerboard.tv/USBN.BeerBoard.UI/bevSyncCustomers/" + String.valueOf(rsDetails.getInt(1)) + "/" + rsDetails.getString(2).replaceAll(" ", "%20");
                        saveImage(fileName, "/home/midware/facebook/reward/"+rsDetails.getString(2));                    
                        File file                    = new File("/home/midware/facebook/reward/"+rsDetails.getString(2));
                        stmt                     = transconn.prepareStatement(selectToken); 
                        stmt.setInt(1,location);
                        rsFB                     = stmt.executeQuery();
                        while (rsFB.next()) {
                            String pageId        = rsFB.getString(1);
                            String fbId=rsFB.getString(2);
                            String locationToken=rsFB.getString(3);
                            postImage(fbId, pageId, locationToken, postMessage, location, rewardId, file,4);
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
                            tweetImage(user,location,postMessage, consumerKey, consumerSecret, accesToken, tokenSecret, file,4,rewardId);
                        }
                    }
                  
                    
                
             }
             
             } catch (Exception e) {
             e.printStackTrace();
         } finally {           
             close(rs);
             close(stmt);
            }
	}


    private void postLocationImages()throws HandlerException {
       PreparedStatement stmt               = null;
       ResultSet rs                         = null, innerRS = null;
       String selectPromotion               = "SELECT p.id, p.location, p.file ,fP.fbid, fP.pageid, u.access_token FROM locationPromotions p LEFT JOIN usbnFacebookPage fP ON fP.location =p.location " +
                                            " LEFT JOIN usbnFacebook u ON fP.fbid=u.user_id WHERE p.location = 3 AND p.facebook = 0 AND p.type=1 AND length(u.access_token) > 4 " +
                                            " AND u.user_id!='100000256298215' AND DATEDIFF( now(), p.date) <10;";
       try {
           stmt                             = transconn.prepareStatement(selectPromotion);
           rs                               = stmt.executeQuery();
           while (rs.next()) {
               int promotionId              = rs.getInt(1);
               int location                 = rs.getInt(2);
               String userId                = rs.getString(4);
               String pageId                = rs.getString(5);
               String accessToken           = rs.getString(6);
               String fileName              = "http://beerboard.tv/USBN.BeerBoard.UI/Images/Customers/"+ String.valueOf(location) + "/" +rs.getString(3).trim().replaceAll("\'", "%27").replaceAll(" ", "%20");
               saveImage(fileName, "/home/midware/facebook/promotion/"+rs.getString(3));
               File file                    = new File("/home/midware/facebook/promotion/"+rs.getString(3));
               postImage(userId, pageId, accessToken, "Promotion", location, promotionId, file,2);
               stmt                             = transconn.prepareStatement("UPDATE locationPromotions SET facebook =1 WHERE id = ?");
               stmt.setInt(1,promotionId);
               stmt.executeUpdate();
               stmt                       = transconn.prepareStatement("Select t.consumerKey,t.consumerSecret,t.accesToken,t.tokenSecret,t.id from usbnTwitter t LEFT JOIN twitterLocationMap l ON l.twitter = t.id WHERE l.location = ?;");
               stmt.setInt(1,location);
               innerRS                         = stmt.executeQuery();
               if(innerRS.next()) {
                   String consumerKey     = innerRS.getString(1);
                   String consumerSecret  = innerRS.getString(2);
                   String accesToken      = innerRS.getString(3);
                   String tokenSecret     = innerRS.getString(4);
                   int user               = rs.getInt(5);
                   logger.debug("Tweeting for location: " + location);
                   tweetImage(user,location,"Promotion", consumerKey, consumerSecret, accesToken, tokenSecret,file,2,promotionId);
               }
               file.delete();
           }
       } catch ( Exception sqle) {
            logger.dbError("Database error: "+sqle.toString());
            throw new HandlerException(sqle);
        } finally {
           close(innerRS);
            close(rs);
            close(stmt);
        }
   }
    
    
    private void postLocationPromotion()throws HandlerException {
       PreparedStatement stmt               = null;
       ResultSet rs                         = null, innerRS = null;
       String selectPromotion               = "SELECT p.id, p.location, p.file ,fP.fbid, fP.pageid, u.access_token FROM locationPromotions p LEFT JOIN usbnFacebookPage fP ON fP.location =p.location " +
                                            " LEFT JOIN usbnFacebook u ON fP.fbid=u.user_id WHERE p.location = 3 AND p.facebook = 0 AND p.type=1 AND length(u.access_token) > 4 " +
                                            " AND u.user_id!='100000256298215' AND DATEDIFF( now(), p.date) <10;";
       String selectPromotionTwitter        = "SELECT p.id,p.location,p.file ,t.consumerKey,t.consumerSecret,t.accesToken,t.tokenSecret,t.id FROM locationPromotions p"
                                            + " LEFT JOIN twitterLocationMap l ON l.location = p.location LEFT JOIN usbnTwitter t ON t.id=l.twitter WHERE p.twitter =0 AND p.type=1  AND DATEDIFF( now(), p.date) <10 ;";
       try {
           stmt                             = transconn.prepareStatement(selectPromotion);
           rs                               = stmt.executeQuery();
           while (rs.next()) {
               int promotionId              = rs.getInt(1);
               int location                 = rs.getInt(2);
               String userId                = rs.getString(4);
               String pageId                = rs.getString(5);
               String accessToken           = rs.getString(6);
               String fileName              = "http://beerboard.tv/USBN.BeerBoard.UI/Images/Customers/"+ String.valueOf(location) + "/" +rs.getString(3).trim().replaceAll("\'", "%27").replaceAll(" ", "%20");
               saveImage(fileName, "/home/midware/facebook/promotion/"+rs.getString(3));
               File file                    = new File("/home/midware/facebook/promotion/"+rs.getString(3));
               postImage(userId, pageId, accessToken, "Promotion", location, promotionId, file,2);
               stmt                             = transconn.prepareStatement("UPDATE locationPromotions SET facebook =1 WHERE id = ?");
               stmt.setInt(1,promotionId);
               stmt.executeUpdate();
               stmt                       = transconn.prepareStatement("Select t.consumerKey,t.consumerSecret,t.accesToken,t.tokenSecret,t.id from usbnTwitter t LEFT JOIN twitterLocationMap l ON l.twitter = t.id WHERE l.location = ?;");
               stmt.setInt(1,location);
               innerRS                         = stmt.executeQuery();
               if(innerRS.next()) {
                   String consumerKey     = innerRS.getString(1);
                   String consumerSecret  = innerRS.getString(2);
                   String accesToken      = innerRS.getString(3);
                   String tokenSecret     = innerRS.getString(4);
                   int user               = rs.getInt(5);
                   logger.debug("Tweeting for location: " + location);
                   tweetImage(user,location,"Promotion", consumerKey, consumerSecret, accesToken, tokenSecret,file,2,promotionId);
               }
               file.delete();
               
               
           }
           
           /*stmt                             = transconn.prepareStatement(selectPromotionTwitter);
           rs                               = stmt.executeQuery();
           while (rs.next()) {
               int promotionId              = rs.getInt(1);
               int location                 = rs.getInt(2);
               String fileName              = "http://beerboard.tv/USBN.BeerBoard.UI/Images/Customers/"+ String.valueOf(location) + "/" +rs.getString(3);
               saveImage(fileName, "/home/midware/facebook/promotion/"+rs.getString(3));
               File file                    = new File("/home/midware/facebook/promotion/"+rs.getString(3));
               String consumerKey           = rs.getString(4);
               String consumerSecret        = rs.getString(5);
               String accesToken            = rs.getString(6);
               String tokenSecret           = rs.getString(7);
               int user                     = rs.getInt(8);
               logger.debug("Tweeting for location: " + location);
               tweetImage(user,location,"Promotion", consumerKey, consumerSecret, accesToken, tokenSecret,file,2,promotionId);
               file.delete();
           }*/
       } catch ( Exception sqle) {
            logger.dbError("Database error: "+sqle.toString());
            throw new HandlerException(sqle);
        } finally {
           close(innerRS);
            close(rs);           
            close(stmt);
        }
   }
    
    public void postImage(String userId, String pageId, String access_token, String message, int location, int promotion, File file, int type) {


        if(access_token!=null) {
            try {
                String g                    = "https://graph.facebook.com/fql?q=SELECT+page_id%2c+name%2C+pic%2C+access_token%2c+fan_count+FROM+page+WHERE+page_id+IN+(select+page_id+from+page_admin+where+uid="+userId+")+&"+access_token;
                URL u                       = new URL(g);
                URLConnection c             = u.openConnection();
                signHttpsCertificate();
                HttpsURLConnection conn     = (HttpsURLConnection) u.openConnection();
                BufferedReader in           = new BufferedReader(new InputStreamReader(conn.getInputStream()));
                String inputLine;
                StringBuffer b              = new StringBuffer();
                while ((inputLine = in.readLine()) != null)
                    b.append(inputLine + "\n");
                in.close();
                String graph                = b.toString();
                JSONObject json             = new JSONObject(graph);
                JSONArray fa1               = new JSONArray(String.valueOf(json.get("data")));
                for(int i=0;i<fa1.length();i++) {
                    String accesstoken      = String.valueOf(fa1.getJSONObject(i).get("access_token"));
                    if(!accesstoken.equals("null")) {
                        String pageid       = fa1.getJSONObject(i).getString("page_id");
                        if(pageid.equals(pageId)) {
                            signHttpsCertificate();
                            PostMethod filePost                 = new PostMethod("https://graph.facebook.com/"+pageid+"/photos");
                            Calendar currentDate                = Calendar.getInstance();
                            String lastAccess                   = dbDateFormat.format(currentDate.getTime());

                            PreparedStatement stmt              = null;
                            try {

                                logger.debug("post file" + file);
                                filePost.getParams().setBooleanParameter(HttpMethodParams.USE_EXPECT_CONTINUE, false);
                                FilePart fp                     = new FilePart("source", file.getName(), file);
                                Part[] parts                    = {fp, new StringPart("access_token", accesstoken), new StringPart("message", message)};
                                filePost.setRequestEntity(new MultipartRequestEntity(parts, filePost.getParams()));

                                signHttpsCertificate();
                                HttpClient client               = new HttpClient();
                                signHttpsCertificate();
                                client.getHttpConnectionManager().getParams().setConnectionTimeout(5000);
                                logger.debug("Reach http client");
                                 signHttpsCertificate();
                                 // configure the SSLContext with a TrustManager
                                 SSLContext ctx                 = SSLContext.getInstance("TLS");
                                 ctx.init(new KeyManager[0], new TrustManager[] {new DefaultTrustManager()}, new SecureRandom());
                                 SSLContext.setDefault(ctx);

                                int status                      = client.executeMethod(filePost);
                                logger.debug("Reach status");
                                signHttpsCertificate();
                                if (status == HttpStatus.SC_OK) {
                                    logger.debug("Upload complete, response=" + filePost.getResponseBodyAsString());
                                    json = new JSONObject(filePost.getResponseBodyAsString());
                                    logger.debug("id:"+json.getString("id"));
                                    logger.debug("post_id:"+json.getString("post_id"));
                                    stmt                        = transconn.prepareStatement("INSERT INTO facebookPost (user,postId,albumId,location,type,typeId,postTime) VALUES (?, ?, ?, ?, ?,?,?)");
                                    stmt.setString(1, userId);
                                    stmt.setString(2, json.getString("post_id"));
                                    stmt.setString(3, json.getString("id"));
                                    stmt.setInt(4, location);
                                    stmt.setInt(5, type);
                                    stmt.setInt(6, 0);
                                    stmt.setString(7, lastAccess);
                                    stmt.executeUpdate();
                                    logger.debug("Post Completed");
                                } else {
                                    System.out.println("Upload failed, response=" + HttpStatus.getStatusText(status));
                                    // Create response
                                    StringBuilder notificationsSendResponse = new StringBuilder();
                                    byte[] byteArrayNotifications = new byte[4096];
                                    for (int n; (n = filePost.getResponseBodyAsStream().read(byteArrayNotifications)) != -1;) {
                                        notificationsSendResponse.append(new String(byteArrayNotifications, 0, n));
                                    }
                                    String notificationInfo = notificationsSendResponse.toString();
                                }
                            } catch (Exception ex) {
                                 logger.dbError(" error: "+ex.getMessage());
                            } finally {
                                filePost.releaseConnection();
                            }
                        }
                    }
                }
            } catch (Exception e) {
                logger.debug("Post On Page error: "+e.getMessage());                
            }
        }
    }

    
    
    public void tweetImage(int user,int location,String message,String consumerKey,String consumerSecret,String oauthToken,String oauthSecret,File file,int type,int typeId) {
        
        try {
            PreparedStatement stmt              = null;
            Calendar currentDate                = Calendar.getInstance();
            String lastAccess                   = dbDateFormat.format(currentDate.getTime());
            BasicConfigurator.configure();
            ConfigurationBuilder cb         = new ConfigurationBuilder();
            cb.setDebugEnabled(true)
                    .setOAuthConsumerKey(consumerKey)
                    .setOAuthConsumerSecret(consumerSecret)
                    .setOAuthAccessToken(oauthToken)
                    .setOAuthAccessTokenSecret(oauthSecret).setHttpConnectionTimeout(100000);
            
            TwitterFactory tf               = new TwitterFactory(cb.build());
            Twitter twitter                 = tf.getInstance();
            try {
                RequestToken requestToken   = twitter.getOAuthRequestToken();
                AccessToken accessToken     = null;
                while (null == accessToken) {
                    try {
                        accessToken         = twitter.getOAuthAccessToken(requestToken);
                    } catch (TwitterException te) {
                        if (401 == te.getStatusCode()) {
                        logger.debug("Unable to get the access token.");
                        } else {
                            te.printStackTrace();
                        }
                    }
                }
                logger.debug("Got access token.");
                logger.debug("Access token: " + accessToken.getToken());
                logger.debug("Access token secret: " + accessToken.getTokenSecret());
            } catch (IllegalStateException ie) {
                // access token is already available, or consumer key/secret is not set.
                if (!twitter.getAuthorization().isEnabled()) {
                    logger.debug("OAuth consumer key/secret is not set.");
                    return;
                }
            }
            StatusUpdate status             = new StatusUpdate(message);
            //BufferedImage  ire = HtmlToImage.create("file:////home/midware/facebook/productHTML.htm", 500, 650);
            //ImageIO.write(ire, "jpg", new File("/home/midware/facebook/image.jpg"));
           
            if(file.exists()) {
                status.setMedia(file);
                Status status2              = twitter.updateStatus(status);
                stmt                        = transconn.prepareStatement("INSERT INTO twitterPost (user,postId,location,type,typeId,postTime) VALUES (?, ?, ?, ?, ?,?)");
                stmt.setInt(1, user);
                stmt.setString(2, String.valueOf(status2.getId()));
                stmt.setInt(3, location);
                 stmt.setInt(4, type);
                stmt.setInt(5, typeId);
                stmt.setString(6, lastAccess);
                stmt.executeUpdate();
                logger.debug("Successfully updated the status to [" + status2.getText() + "].");
            }
        } catch (TwitterException te) {            
            logger.debug("Twitter Image Post Error: " + te.getMessage());
        }catch (Exception de) {            
            logger.debug("Twitter Image Post Error: " + de.getMessage());
        } 
       
    }
    
    
    private void addHtml5VisitorsLog(Element toHandle, Element toAppend) throws HandlerException {
        
        PreparedStatement stmt              = null;
        String latitude                     = HandlerUtils.getOptionalString(toHandle, "latitude");
        String longitude                    = HandlerUtils.getOptionalString(toHandle, "longitude");
        String time                         = HandlerUtils.getOptionalString(toHandle, "lastAccess");
        try {
            stmt                            = transconn.prepareStatement("INSERT INTO html5VisitorsLog (latitude,longitude,lastAccess) VALUES (?,?,?);");
            stmt.setString(1, latitude);
            stmt.setString(2, longitude);
            stmt.setString(3, time);
            stmt.executeUpdate();
        } catch (SQLException sqle) {
            logger.dbError("Database error: "+sqle.toString());
            throw new HandlerException(sqle);
        } finally {
           
            close(stmt);
        }
    
    }
    
    
     private void getStyles(Element toHandle, Element toAppend) throws HandlerException {
        
         String selectStyle                 = "SELECT id, style FROM beerStyles ORDER BY style;";
        PreparedStatement stmt              = null;
        ResultSet rs                        = null;
        try {
            stmt                        = transconn.prepareStatement(selectStyle);
            rs                          = stmt.executeQuery();
            while (rs != null && rs.next()) {
                int rsIndex             = 1;
                Element locEl           = toAppend.addElement("style");
                locEl.addElement("id").addText(String.valueOf(rs.getInt(rsIndex++)));
                locEl.addElement("name").addText(HandlerUtils.nullToEmpty((rs.getString(rsIndex++))));                
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
     
     
     private void getNearByProducts(Element toHandle, Element toAppend) throws HandlerException {

         String latitude                    = HandlerUtils.getOptionalString(toHandle, "latitude");
         String longitude                   = HandlerUtils.getOptionalString(toHandle, "longitude");
         int product                        = HandlerUtils.getOptionalInteger(toHandle, "product");
         boolean isProduction               = isProduction();
        
        if(!isProduction){
            latitude                        = "43.0478720";
            longitude                       = "-76.1867790";
        }
         Calendar calendar                  = Calendar.getInstance();
         int day                            = calendar.get(Calendar.DAY_OF_WEEK);
         String openHours                   = "";
         switch(day) {
             case 1:
                 openHours                  = "DATE_FORMAT(IFNULL(lH.openSun, '2000-00-00 11:00:00'),'%l:%i %p'), DATE_FORMAT(IFNULL(lH.closeSun, '2000-00-00 02:00:00'),' %l:%i %p')";
                 break;
             case 2:
                 openHours                  = "DATE_FORMAT(IFNULL(lH.openMon, '2000-00-00 11:00:00'),'%l:%i %p'), DATE_FORMAT(IFNULL(lH.closeMon, '2000-00-00 02:00:00'),' %l:%i %p')";
                 break;
             case 3:
                 openHours                  = "DATE_FORMAT(IFNULL(lH.openTue, '2000-00-00 11:00:00'),'%l:%i %p'), DATE_FORMAT(IFNULL(lH.closeTue, '2000-00-00 02:00:00'),' %l:%i %p')";
                 break;
             case 4:
                 openHours                  = "DATE_FORMAT(IFNULL(lH.openWed, '2000-00-00 11:00:00'),'%l:%i %p'), DATE_FORMAT(IFNULL(lH.closeWed, '2000-00-00 02:00:00'),' %l:%i %p')";
                 break;
             case 5:
                 openHours                  = "DATE_FORMAT(IFNULL(lH.openThu, '2000-00-00 11:00:00'),'%l:%i %p'), DATE_FORMAT(IFNULL(lH.closeThu, '2000-00-00 02:00:00'),' %l:%i %p')";
                 break;
             case 6:
                 openHours                  = "DATE_FORMAT(IFNULL(lH.openFri, '2000-00-00 11:00:00'),'%l:%i %p'), DATE_FORMAT(IFNULL(lH.closeFri, '2000-00-00 02:00:00'),' %l:%i %p')";
                 break;
             case 7:
                 openHours                  = "DATE_FORMAT(IFNULL(lH.openSat, '2000-00-00 11:00:00'),'%l:%i %p'), DATE_FORMAT(IFNULL(lH.closeSat, '2000-00-00 02:00:00'),' %l:%i %p')";
                 break;
        }
         
        // latitude    = "43.0499035";
        //longitude   = "-76.1514456";
                            
        
        
        String selectLatLongProducts        = "SELECT DISTINCT p.id, p.name, bS.style FROM line l LEFT JOIN bar b ON b.id = l.bar LEFT JOIN product p ON p.id = l.product " +
                                            " LEFT JOIN productDescription pD ON pD.product = p.id LEFT JOIN productSetMap sPSM ON sPSM.product = l.product " +
                                            " LEFT JOIN productSet sPS ON sPS.id = sPSM.productSet LEFT JOIN beerStylesMap bSM ON bSM.productSet = sPS.id LEFT JOIN beerStyles bS ON bS.id =  bSM.style " +
                                            " WHERE l.status = 'RUNNING' AND p.id NOT IN(4311,10661) AND b.location IN (SELECT id FROM (SELECT l.id, (6371 * acos(cos(radians(?)) * cos(radians( l.latitude )) * " +
                                            " cos(radians(?) - radians(l.longitude)) + sin(radians(?)) * sin(radians(l.latitude)))) AS distanta " +
                                            " FROM location l LEFT JOIN customer c ON c.id = l.customer LEFT JOIN locationGraphics lG ON lG.location = l.id " +
                                            " LEFT JOIN locationDetails lD ON lD.location = l.id LEFT JOIN locationHours lH ON lH.location = l.id " +
                                            " WHERE lD.active = 1 "+ (isProduction ? " AND l.customer<> 205 " : " AND l.id = 425   ") +" HAVING distanta < 33 ORDER BY l.addrState, l.boardname, l.name) as l) " +
                                            " AND sPS.productSetType = 9 ORDER BY pD.boardName,sPS.name, pD.boardName;";
        String selectProductLocation        = "SELECT DISTINCT l.id, l.name, l.latitude, l.longitude, l.easternOffset, IFNULL(REPLACE(lG.logo, ' ', '%20'), 'USBNLogo.png'), l.addrStreet, l.addrCity, l.addrState, l.addrZip, l.boardname,"+openHours+","
                                            + "(6371 * acos( cos( radians(?) ) * cos( radians( l.latitude ) ) * cos( radians( ? ) - radians(l.longitude) ) + sin( radians(?) ) * sin( radians(l.latitude) ) )) AS distanta FROM location l LEFT JOIN customer c ON c.id = l.customer LEFT JOIN locationGraphics lG ON lG.location = l.id LEFT JOIN locationDetails lD ON lD.location = l.id "
                                            + " LEFT JOIN bar b ON b.location = l.id LEFT JOIN line li ON li.bar=b.id LEFT JOIN locationHours lH ON lH.location =l.id"
                                            + " WHERE li.status = 'RUNNING' AND li.product = ? AND lD.active = 1 "+ (isProduction ? " AND l.customer<> 205 " : " AND l.id = 425  ") +" HAVING distanta < 33 ORDER BY l.addrState, l.boardname;";
        String selectSponsor                = "SELECT id, count FROM bevSyncCampaignReward WHERE CURDATE() = DATE(startTime) AND location = ?; ";
       
       
        PreparedStatement stmt              = null;
        ResultSet rs                        = null, rsSponsor = null;
        try {
            
            if(product > 0) {
                stmt                        = transconn.prepareStatement(selectProductLocation);
                stmt.setDouble(1, Double.parseDouble(latitude));
                stmt.setDouble(2, Double.parseDouble(longitude));
                stmt.setDouble(3, Double.parseDouble(latitude));
                stmt.setInt(4, product);
                rs                          = stmt.executeQuery();
                while (rs != null && rs.next()) {
                     int rsIndex            = 1;
                     Element locEl          = toAppend.addElement("location");
                     int location           = rs.getInt(rsIndex++);
                     String locationName    = rs.getString(rsIndex++);
                     locEl.addElement("locationId").addText(String.valueOf(location));
                     locEl.addElement("locationName").addText(HandlerUtils.nullToEmpty((locationName)));
                     locEl.addElement("latitude").addText(String.valueOf(rs.getString(rsIndex++)));
                     locEl.addElement("longitude").addText(String.valueOf(rs.getString(rsIndex++)));
                     locEl.addElement("easternOffset").addText(HandlerUtils.nullToEmpty((rs.getString(rsIndex++))));
                     locEl.addElement("logo").addText(HandlerUtils.nullToEmpty(("http://bevmanager.net/Images/Location_logo/" + rs.getString(rsIndex++).trim().replaceAll("\'", "%27").replaceAll(" ", "%20"))));
                     locEl.addElement("addrStreet").addText(HandlerUtils.nullToEmpty((rs.getString(rsIndex++))));
                     locEl.addElement("addrCity").addText(HandlerUtils.nullToEmpty((rs.getString(rsIndex++))));
                     locEl.addElement("addrState").addText(HandlerUtils.nullToEmpty((rs.getString(rsIndex++))));
                     locEl.addElement("addrZip").addText(HandlerUtils.nullToEmpty((rs.getString(rsIndex++))));
                     locEl.addElement("boardName").addText(HandlerUtils.nullToEmpty((rs.getString(rsIndex++))));
                     String hours           = "11:00 AM - 2:00 AM";
                     String open            = rs.getString(rsIndex++);
                     String close           = rs.getString(rsIndex++);
                     if(open!=null & close!=null) {
                         hours              = open + " - " + close;
                     }
                     locEl.addElement("hours").addText(HandlerUtils.nullToEmpty(hours));
                      stmt            = transconn.prepareStatement("SELECT m.id,m.message,p.openLocation FROM wakeupMessage m LEFT JOIN wakeupMessageMap p ON p.message=m.id  WHERE p.location = ?;");
                        stmt.setInt(1, location);
                        rsSponsor       = stmt.executeQuery();
                        if(rsSponsor.next()) {
                            locEl.addElement("messageId").addText(String.valueOf(rsSponsor.getInt(1)));
                            locEl.addElement("openLocation").addText(String.valueOf(rsSponsor.getInt(3)));
                            locEl.addElement("message").addText(HandlerUtils.nullToEmpty(rsSponsor.getString(2)));
                        } else {
                            locEl.addElement("messageId").addText(String.valueOf(0));
                            locEl.addElement("message").addText(HandlerUtils.nullToEmpty(""));
                            locEl.addElement("openLocation").addText(String.valueOf(0));
                        }
                        
                     stmt                   = transconn.prepareStatement(selectSponsor);
                     stmt.setInt(1, location);
                     rsSponsor              = stmt.executeQuery();
                     if(rsSponsor.next()) {
                         int sponsor        = rsSponsor.getInt(1);
                         int count          = rsSponsor.getInt(2);
                         locEl.addElement("passId").addText(String.valueOf(sponsor));
                         logger.debug("Sponsor:Present");
                         if(count > 0) {
                             locEl.addElement("sponsor").addText("2");
                             logger.debug("Sponsor:2");
                         } else {
                             locEl.addElement("sponsor").addText("2");
                        }
                     } else {
                         locEl.addElement("sponsor").addText("0");
                         locEl.addElement("passId").addText(String.valueOf(0));
                     }
                }
            } else {
                stmt                        = transconn.prepareStatement(selectLatLongProducts);
                stmt.setDouble(1, Double.parseDouble(latitude));
                stmt.setDouble(2, Double.parseDouble(longitude));
                stmt.setDouble(3, Double.parseDouble(latitude));
                rs                          = stmt.executeQuery();
                while (rs != null && rs.next()) {
                    Element proEl           = toAppend.addElement("Products");
                    proEl.addElement("productId").addText(String.valueOf(rs.getInt(1)));
                    proEl.addElement("product").addText(HandlerUtils.nullToEmpty(rs.getString(2)));
                    proEl.addElement("style").addText(HandlerUtils.nullToEmpty(rs.getString(3)));
                }
            }
        } catch (Exception ex) {
            System.out.println("ERROR: " + ex.getClass().getName() + " " + ex.getMessage());
            ex.printStackTrace();
        } finally {         
            close(rsSponsor);
            close(rs);
            close(stmt);
        }
     }
     
     
     private void getInsiderPasses(Element toHandle, Element toAppend) throws HandlerException {
         
        String selectPromotion              = "SELECT bCR.id, bC.customer, bCr.id, bCr.file, bCR.location, bCR.startTime, bCR.endTime, bCR.rewardText FROM bevSyncCampaignReward bCR " +
                                            " LEFT JOIN bevSyncCampaign bC ON bC.id = bCR.campaign  LEFT JOIN bevSyncCreatives bCr ON bCr.id = bCR.creatives " +
                                            " WHERE bCr.validity > NOW() AND  bCr.type = 5 AND bC.customer > 0;";
        String selectProduct                = "SELECT pS.name, IFNULL(p.name, 'Unknown Product'), (SELECT CONCAT(boardname) from location where id=?) from bevSyncCreatives bC "
                                            + " LEFT JOIN product p ON p.id = bC.product LEFT JOIN productDescription pD ON pD.product = p.id LEFT JOIN productSet pS ON pS.id=bC.brewery WHERE pS.productSetType=7 AND bC.id=?;";
        
        PreparedStatement stmt              = null;       
        ResultSet rs                        = null ,rsDetails = null;  
        String title[]                        = {"Blue Moon $3 Pint @ Nibsy's On Wednesday ,Nov-13 6-9  PM","Empire Amber $2 Pint @ KittyHoyans On Friday ,Jan-15 6-11  PM","Bells Hopslam $2 Pint @ Clarandon On Tuesday ,Feb-25 6-10  PM","Brooklyn $1 Pint @ Clarandon On Wednesday ,Mar-19 5-7  PM"};
        
        int mobileCustomer                  = HandlerUtils.getOptionalInteger(toHandle, "customerId");
        String oldProduct                   = "", oldBrewery = "", oldLocationName = "";
        
        try {
            stmt                            = transconn.prepareStatement(selectPromotion);
            rs                              = stmt.executeQuery();
            int index                       = 0;
            while (rs != null && rs.next()) {
                String[] days               = {"Saturday", "Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday"};
                String start                = rs.getString(6);
                String end                  = rs.getString(7);
                String rewardText           = rs.getString(8);
                int creatives               = rs.getInt(3);
                int location                = rs.getInt(5);
                String product              = "", brewery = "", locationName = "",message = "";
                
                stmt                        = transconn.prepareStatement(selectProduct); 
                stmt.setInt(1,location);
                stmt.setInt(2,creatives);
                rsDetails                   = stmt.executeQuery();
                if(rsDetails.next()) {
                    if ((oldBrewery.equalsIgnoreCase(rsDetails.getString(1)))  && (oldProduct.equalsIgnoreCase(rsDetails.getString(2)))
                            && (oldLocationName.equalsIgnoreCase(rsDetails.getString(3)))) {
                    } else {
                        brewery             = oldBrewery = rsDetails.getString(1);
                        product             = oldProduct = rsDetails.getString(2);
                        locationName        = oldLocationName = rsDetails.getString(3);

                        Calendar c          = Calendar.getInstance();
                        c.setTime(dbDateFormat.parse(start));
                        int day_of_week     = c.get(Calendar.DAY_OF_WEEK);
                        if(product.equals("Unknown Product") ){
                            message         = locationName + " " + brewery + " " + rewardText + " from " + days[day_of_week] + " " + timeFormat.format(dbDateFormat.parse(start)) + " to " + timeFormat.format(dbDateFormat.parse(end));
                        } else {
                            message         = "Get Your Reward for " + product + " @ " + locationName + " from " + days[day_of_week] + " " + timeFormat.format(dbDateFormat.parse(start)) + " to " + timeFormat.format(dbDateFormat.parse(end));
                        }

                        Element proEl       = toAppend.addElement("passes");
                        proEl.addElement("passId").addText(String.valueOf(rs.getInt(1)));
                        proEl.addElement("logo").addText(HandlerUtils.nullToEmpty("http://social.usbeveragenet.com:8080/fileUploader/Images/" +  rs.getString(4).trim().replaceAll("\'", "%27").replaceAll(" ", "%20")));
                        proEl.addElement("title").addText(HandlerUtils.nullToEmpty(message));
                        proEl.addElement("update").addText(String.valueOf(0));
                        proEl.addElement("location").addText(String.valueOf(rs.getInt(5)));
                        proEl.addElement("validity").addText(HandlerUtils.nullToEmpty(end));
                    }
                }
            }
        } catch ( Exception sqle) {
            logger.dbError("Database error: "+sqle.toString());
            throw new HandlerException(sqle);
        } finally {
           close(stmt);
           close(rs);
        }
    }
     
     
     private void getAdBanners(int location, Element toAppend) throws HandlerException {
         PreparedStatement stmt             = null;
         ResultSet rs                       = null;
         
         String selectBanners               = "SELECT bCr.file FROM bevSyncCampaignBanner bCB LEFT JOIN bevSyncCreatives bCr ON bCr.id = bCB.creatives "
                                            + " WHERE bCB.location = ? AND NOW() BETWEEN bCB.startTime AND bCB.endTime AND NOW() < bCr.validity;";
         try {
             int sequence                   = 1;
             stmt                           = transconn.prepareStatement(selectBanners);
             stmt.setInt(1, location);
             rs                             = stmt.executeQuery();
             while (rs.next()) {
                Element proEl               = toAppend.addElement("adBanner");
                proEl.addElement("file").addText(HandlerUtils.nullToEmpty("http://beerboard.tv/USBN.BeerBoard.UI/Images/Customers/"+ String.valueOf(location) + "/" + rs.getString(1).trim().replaceAll(" ", "%20")));                
                proEl.addElement("sequence").addText(String.valueOf(sequence++));
                proEl.addElement("visible").addText(String.valueOf(10000));
                proEl.addElement("type").addText(String.valueOf(1));
                proEl.addElement("link").addText(HandlerUtils.nullToEmpty("http://www.beerboard.tv"));
                proEl.addElement("update").addText(String.valueOf(0));
             }
         } catch(Exception e) {
              logger.dbError("Exception While checking name is Valid");
         } finally {
             close(rs);
             close(stmt);
         }
     }
      
      private String checkValidName(int user,String text){
        String userName                     =null;
        
        PreparedStatement stmt              = null;       
        ResultSet rs                        = null ;
        try {
             text                           = text.replaceAll(" ", "")   ;
             String data                    = getHttpData(new URL("http://api1.webpurify.com/services/rest/?method=webpurify.live.check&api_key=27b6dbff640d06b4beabd620aef7e565&text="+text));
             XML xmlObject                  = new XML();
             JSONObject xmlJson             = xmlObject.toJSONObject(data);
             logger.debug("xmlToJson:"+xmlJson.toString());
             if(xmlJson.has("rsp")){
                 JSONObject res= xmlJson.getJSONObject("rsp");
                 if(res.has("found")){
                     int found              = res.getInt("found");
                     if(found == 1) {
                         if(user > 0){
                          stmt                        = transconn.prepareStatement("SELECT username FROM bbtvMobileUser WHERE id = ?; "); 
                          stmt.setInt(1, user);
                          rs = stmt.executeQuery();
                          if (rs.next()) {
                              userName      = HandlerUtils.nullToEmpty(rs.getString(1));
                              
                          }
                         } else {
                             userName           = "Guest-BBTV";
                         }
                     } else if(found == 0) {
                         userName           = text;
                     }
                 } else if(res.has("err")){
                     userName               = text;

                 }
             }
             logger.debug(userName);
        } catch(Exception e) {
             logger.dbError("Exception While checking name is Valid");
        } finally {
            close(rs);
            close(stmt);
        }
        return userName;
    }
      private void testSocialMediaUpdates(Element toHandle, Element toAppend) throws HandlerException {
          try {
              signHttpsCertificate();
              postOnFacebook("100000256298215", "access_token=CAAE2ulGzdqcBAKvjCb4n31xabyThyitQtYBL40cl55a59JP0cQf4QbBC55dkzrUIos17MOOkhglxvHbY2Wf1MbTc3l7KoCZAaWHLYgxsQrYxWFNzFQv2ZCdZB48oRZBQsHBy82DRYyVaGXBh1qaOorSMZAf6PbWH7iMZCKlWtMlqKDv4bEEBVZBBajhHKC3uIyBJ3BUx8mzz4n6WGAwLii5&expires=5183998", "Testing Post","166410700160923",1,0,0);
              tweetStatus(3,"Test","Hs4u3FYn1KbeqA9ztuFyDQ","rMQcqe6TKuemxP7MZoduJVGsx9SwdEAeInTufnb6U","333884997-KVMmR0XpNQAO9pdcjm1AfeEdrhDQpZjp0C5MSaUN","jT35kVg2t3B4aMbU6FAdmFb1kvXIfRBP1PzYSFjq2k",1,425,0);
              //postOnFacebook("100000256298215", "access_token=CAACNB03nnL0BAHN0HizaDRjyGRLOsRoxI0L4yfCiylfkSGRL7YZCHIZBdBBTvXmu94E7ndJP3cen5rPHYW5e66xPT1ZBBezLchWWtZBHjq4GpvjXXST6TPpzZBtmZCxj4OOZBj3dC4v4ZBPR74Wfs2Lb9TaBX4tgqtgHEwr8JmErLbHNzA7KIlJdxog6NxBxlygZD&expires=5183639", "Testing Post","670510736317171",1,0,0);
              //166410700160923 JavaSparks
              //670510736317171 Beerboard
              //postOnFacebook("100000256298215", "access_token=CAACNB03nnL0BAHN0HizaDRjyGRLOsRoxI0L4yfCiylfkSGRL7YZCHIZBdBBTvXmu94E7ndJP3cen5rPHYW5e66xPT1ZBBezLchWWtZBHjq4GpvjXXST6TPpzZBtmZCxj4OOZBj3dC4v4ZBPR74Wfs2Lb9TaBX4tgqtgHEwr8JmErLbHNzA7KIlJdxog6NxBxlygZD&expires=5183639", facebookMessage,"670510736317171",1,locationId,productId);
          } catch(Exception e){
              logger.debug(e.getMessage());
          }
          
      }
      
      
      private void getJSBeerBoardMenu(Element toHandle, Element toAppend) throws HandlerException {

        int locationId                      = HandlerUtils.getRequiredInteger(toHandle, "locationId");
        int type                            = HandlerUtils.getOptionalInteger(toHandle, "type");
        String products                     = " 0";
        PreparedStatement stmt              = null;
        ResultSet rs                        = null, outerRS = null;
        ArrayList<String> pricesMenus       = new ArrayList<String>();
        ArrayList<String> sizeMenus         = new ArrayList<String>();
        HashMap<Integer, ArrayList<String>> productPrices
                                            = new HashMap<Integer, ArrayList<String>>();
         HashMap<Integer, ArrayList<String>> productSizes
                                            = new HashMap<Integer, ArrayList<String>>();
        //HashMap<Integer, String> productStyle
          //                                  = new HashMap<Integer, String>();
        
        String selectBeverageDescription    = "SELECT DISTINCT p.id,pD.category, p.name, IF(pD.abv = 0.0, '', CONCAT(pD.abv,'%')), pS.name,pS2.name, " +
                                            " pD.origin, pD.seasonality,pD.ibu FROM line l LEFT JOIN bar b ON b.id = l.bar"
                                            + " LEFT JOIN product p ON p.id = l.product LEFT JOIN productDescription pD ON pD.product = p.id"
                                            + " LEFT JOIN brewStyleMap pSM ON pSM.product = l.product LEFT JOIN productSet pS ON pS.id = pSM.brewery LEFT JOIN productSet pS2 ON pS2.id = pSM.style "
                                            + " WHERE l.status = 'RUNNING' AND p.id NOT IN(4311,10661) AND b.location = ?  ORDER BY pS.name, pD.boardName;";
        
        String selectNewBeer                = "SELECT DISTINCT lU.product FROM lineUpdates lU LEFT JOIN line l ON l.id = lU.line WHERE lU.location = ? "
                                            + " AND lU.date > SUBDATE(NOW(), INTERVAL 7 DAY) AND l.status = 'RUNNING' ORDER BY lU.date DESC, lU.id DESC LIMIT 2;";

        String selectLocalBeer              = "SELECT DISTINCT l.product FROM line l LEFT JOIN bar b ON b.id = l.bar WHERE l.product NOT IN(4311,10661) AND b.location = ? " +
                                            " AND l.status = 'RUNNING' AND l.local = 1;";

        String selectProducts               = " SELECT DISTINCT p.id,pD.category, pD.boardName, IF(pD.abv = 0.0, '', CONCAT(pD.abv,'%')), pS.name,sPS.name, pD.origin,pD.seasonality FROM"
                                            + " product p LEFT JOIN productDescription pD ON pD.product = p.id"
                                            + " LEFT JOIN productSetMap pSM ON pSM.product = p.id LEFT JOIN productSet pS ON pS.id = pSM.productSet"
                                            + " LEFT JOIN productSetMap sPSM ON sPSM.product = p.id LEFT JOIN productSet sPS ON sPS.id = sPSM.productSet"
                                            + " WHERE p.isActive = 1 AND p.id IN ("+ products+") AND sPS.productSetType = 9 AND pS.productSetType = 7 ORDER BY pS.name, pD.boardName;";
        
        String selectBottleBeer             = "SELECT DISTINCT p.id,pD.category,p.name, IF(pD.abv = 0.0, '', CONCAT(pD.abv,'%')), pS.name,pS2.name,pD.origin,pD.seasonality,pD.ibu,CONCAT('$', b.price) FROM bottleBeer b LEFT JOIN product p ON p.id=b.product"
                                            + " LEFT JOIN productDescription pD ON pD.product=p.id  LEFT JOIN brewStyleMap pSM ON pSM.product=p.id LEFT JOIN productSet pS ON pS.id=pSM.brewery LEFT JOIN productSet pS2 ON pS2.id = pSM.style "
                                            + "  LEFT JOIN productSetMap sPSM ON sPSM.product = b.product LEFT JOIN productSet sPS ON sPS.id = sPSM.productSet  WHERE location=?    ORDER by pS.name,pD.boardName;";
                
        
        try {

             /*stmt                           = transconn.prepareStatement("SELECT sPSM.product, sPS.name FROM productSetMap sPSM LEFT JOIN productSet sPS ON sPS.id = sPSM.productSet WHERE sPS.productSetType = 9");
             rs                             = stmt.executeQuery();
             while (rs.next()) {
                productStyle.put(rs.getInt(1), rs.getString(2));
             }*/
            String selectBeveragePrices    = "SELECT i.product, s.name,CONCAT('$', iP.value) FROM inventoryPrices iP LEFT JOIN inventory i ON i.id = iP.inventory"
                                            + " LEFT JOIN beverageSize s ON s.id = iP.size WHERE i.location = ?  AND s.id <> 'null' " +
                                            " AND s.showOnTV = 1 GROUP BY i.product ORDER BY i.product, iP.value";
            String selectPrices            = "SELECT i.product, s.name, CONCAT('$', iP.value) FROM inventoryPrices iP LEFT JOIN inventory i " +
                                            " ON i.id = iP.inventory LEFT JOIN beverageSize s ON s.id = iP.size WHERE i.location = ? AND s.id <> 'null' " +
                                            " AND s.showOnTV = 1 GROUP BY i.product ORDER BY i.product, iP.value;";
             boolean menuAvail              = false;
             int product                    = -1;
             stmt                           = transconn.prepareStatement(selectBeveragePrices);
             stmt.setInt(1, locationId);
             rs                             = stmt.executeQuery();
             while (rs.next()) {
                 if (product == rs.getInt(1)) {
                        pricesMenus.add(rs.getString(3));
                        sizeMenus.add(rs.getString(2));
                 } else {
                     if (product < 0) {
                         product            = rs.getInt(1);
                         pricesMenus        = new ArrayList<String>();
                         sizeMenus          = new ArrayList<String>();
                         pricesMenus.add(rs.getString(3));
                         sizeMenus.add(rs.getString(2));
                     } else {
                         productPrices.put(product, pricesMenus);
                         productSizes.put(product, sizeMenus);
                         product            = rs.getInt(1);
                         pricesMenus        = new ArrayList<String>();
                         sizeMenus          = new ArrayList<String>();
                         pricesMenus.add(rs.getString(3));
                         sizeMenus.add(rs.getString(2));
                     }
                 }
                 if (rs.isLast()) {
                     productPrices.put(product, pricesMenus);
                     productSizes.put(product, sizeMenus);
                 }
             }
             
             if (type == 2) {
                 stmt                       = transconn.prepareStatement(selectNewBeer);
                 stmt.setInt(1, locationId);
                 outerRS                    = stmt.executeQuery();
                 while (outerRS.next()) {
                     selectProducts         = " SELECT DISTINCT p.id,pD.category, pD.boardName, IF(pD.abv = 0.0, '', CONCAT(pD.abv,'%')), pS.name, pS2.name, "
                                            + " pD.origin, pD.seasonality,pD.ibu FROM product p LEFT JOIN productDescription pD ON pD.product = p.id"
                                            + " LEFT JOIN brewStyleMap pSM ON pSM.product = pD.product LEFT JOIN productSet pS ON pS.id = pSM.brewery LEFT JOIN productSet pS2 ON pS2.id = pSM.style "
                                            + " WHERE p.id = ? AND p.id NOT IN(4311,10661)  " +
                                            " ORDER BY pS.name, pD.boardName;";

                    stmt                    = transconn.prepareStatement(selectProducts);
                    stmt.setInt(1, outerRS.getInt(1));
                    rs                      = stmt.executeQuery();            
                    if (rs.next()) {
                        menuAvail           = true;
                        int rsIndex         = 1;
                        Element beerMenusEl = toAppend.addElement("beerMenus");
                        int productId       = rs.getInt(rsIndex++);
                        beerMenusEl.addElement("productId").addText(String.valueOf(productId));
                        beerMenusEl.addElement("category").addText(String.valueOf(rs.getInt(rsIndex++)));
                        beerMenusEl.addElement("boardName").addText(String.valueOf(rs.getString(rsIndex++)));
                        beerMenusEl.addElement("abv").addText(String.valueOf(rs.getString(rsIndex++)));
                        beerMenusEl.addElement("brewery").addText(String.valueOf(rs.getString(rsIndex++)));
                        beerMenusEl.addElement("style").addText(String.valueOf(rs.getString(rsIndex++)));
                        beerMenusEl.addElement("origin").addText(String.valueOf(rs.getString(rsIndex++)));
                        beerMenusEl.addElement("seasonality").addText(String.valueOf(rs.getString(rsIndex++)));
                        beerMenusEl.addElement("ibu").addText(String.valueOf(rs.getInt(rsIndex++)));
                        String prices       = "";
                        if (productPrices.containsKey(productId)) {                                           
                            ArrayList<String> invoiceArray
                                            = productPrices.get(productId);
                            for (String price : invoiceArray) {
                                prices      +=" " + price ;
                            }
                        }
                        String sizes         = "";
                        if (productSizes.containsKey(productId)) {
                                               
                            ArrayList<String> sizeArray
                                            = productSizes.get(productId);
                            for (String size : sizeArray) {
                                sizes      +=" " + size ;
                            }
                        }
                        beerMenusEl.addElement("size").addText(sizes);
                        beerMenusEl.addElement("price").addText(prices);
                    }                
                }
            } else if (type == 3) {
                 stmt                       = transconn.prepareStatement(selectLocalBeer);
                 stmt.setInt(1, locationId);
                 outerRS                    = stmt.executeQuery();
                 while (outerRS.next()) {
                     selectProducts         = " SELECT DISTINCT p.id,pD.category, pD.boardName, IF(pD.abv = 0.0, '', CONCAT(pD.abv,'%')), pS.name, pS2.name, "
                                            + " pD.origin, pD.seasonality,pD.ibu FROM product p LEFT JOIN productDescription pD ON pD.product = p.id"
                                            + " LEFT JOIN brewStyleMap pSM ON pSM.product = pD.product LEFT JOIN productSet pS ON pS.id = pSM.brewery LEFT JOIN productSet pS2 ON pS2.id = pSM.style "
                                            + " WHERE p.id = ? AND p.id NOT IN(4311,10661)  " +
                                            " ORDER BY pS.name, pD.boardName;";

                    stmt                    = transconn.prepareStatement(selectProducts);
                    stmt.setInt(1, outerRS.getInt(1));
                    rs                      = stmt.executeQuery();
                    if (rs.next()) {
                        menuAvail           = true;
                        int rsIndex         = 1;
                        Element beerMenusEl = toAppend.addElement("beerMenus");
                        int productId       = rs.getInt(rsIndex++);
                        beerMenusEl.addElement("productId").addText(String.valueOf(productId));
                        beerMenusEl.addElement("category").addText(String.valueOf(rs.getInt(rsIndex++)));
                        beerMenusEl.addElement("boardName").addText(String.valueOf(rs.getString(rsIndex++)));
                        beerMenusEl.addElement("abv").addText(String.valueOf(rs.getString(rsIndex++)));
                        beerMenusEl.addElement("brewery").addText(HandlerUtils.nullToEmpty(rs.getString(rsIndex++)));
                        beerMenusEl.addElement("style").addText(HandlerUtils.nullToEmpty(rs.getString(rsIndex++)));
                        beerMenusEl.addElement("origin").addText(String.valueOf(rs.getString(rsIndex++)));
                        beerMenusEl.addElement("seasonality").addText(String.valueOf(rs.getString(rsIndex++)));
                        beerMenusEl.addElement("ibu").addText(String.valueOf(rs.getInt(rsIndex++)));
                        String prices       = "";
                        if (productPrices.containsKey(productId)) {                           
                            ArrayList<String> invoiceArray
                                            = productPrices.get(productId);
                            for (String price : invoiceArray) {
                                prices      +=" " + price ;
                            }
                        }
                        String sizes         = "";
                        if (productSizes.containsKey(productId)) {
                                               
                            ArrayList<String> sizeArray
                                            = productSizes.get(productId);
                            for (String size : sizeArray) {
                                sizes      +=" " + size ;
                            }
                        }
                        beerMenusEl.addElement("size").addText(sizes);
                        beerMenusEl.addElement("price").addText(prices);
                    }
                }
            } else if(type == 4) {
                stmt                            = transconn.prepareStatement("SELECT product FROM comingSoonProducts WHERE location=?");
                stmt.setInt(1, locationId);                
                outerRS                         = stmt.executeQuery();
                while (outerRS.next()) {
                    selectProducts         = " SELECT DISTINCT p.id,pD.category, pD.boardName, IF(pD.abv = 0.0, '', CONCAT(pD.abv,'%')), pS.name, pS2.name, "
                                            + " pD.origin, pD.seasonality,pD.ibu FROM product p LEFT JOIN productDescription pD ON pD.product = p.id"
                                            + " LEFT JOIN brewStyleMap pSM ON pSM.product = pD.product LEFT JOIN productSet pS ON pS.id = pSM.brewery LEFT JOIN productSet pS2 ON pS2.id = pSM.style "
                                            + " WHERE p.id = ? AND p.id NOT IN(4311,10661)  " +
                                            " ORDER BY pS.name, pD.boardName;";
                    stmt                        = transconn.prepareStatement(selectProducts);
                    stmt.setInt(1, outerRS.getInt(1));
                    rs                          = stmt.executeQuery();            
                    if (rs.next()) {
                        menuAvail               = true;
                        int rsIndex             = 1;
                        Element beerMenusEl     = toAppend.addElement("beerMenus");
                        int productId           = rs.getInt(rsIndex++);
                        beerMenusEl.addElement("productId").addText(String.valueOf(productId));
                        beerMenusEl.addElement("category").addText(String.valueOf(rs.getInt(rsIndex++)));
                        beerMenusEl.addElement("boardName").addText(String.valueOf(rs.getString(rsIndex++)));
                        beerMenusEl.addElement("abv").addText(String.valueOf(rs.getString(rsIndex++)));
                        beerMenusEl.addElement("brewery").addText(HandlerUtils.nullToEmpty(rs.getString(rsIndex++)));
                        beerMenusEl.addElement("style").addText(HandlerUtils.nullToEmpty(rs.getString(rsIndex++)));
                        beerMenusEl.addElement("origin").addText(String.valueOf(rs.getString(rsIndex++)));
                        beerMenusEl.addElement("seasonality").addText(String.valueOf(rs.getString(rsIndex++)));
                        beerMenusEl.addElement("ibu").addText(String.valueOf(rs.getInt(rsIndex++)));
                        String prices               = "";
                        if (productPrices.containsKey(productId)) {                            
                            ArrayList<String> invoiceArray
                                                = productPrices.get(productId);
                            for (String price : invoiceArray) {
                                prices             +=" " + price ;
                            }
                        }
                        String sizes         = "";
                        if (productSizes.containsKey(productId)) {
                                               
                            ArrayList<String> sizeArray
                                            = productSizes.get(productId);
                            for (String size : sizeArray) {
                                sizes      +=" " + size ;
                            }
                        }
                        beerMenusEl.addElement("size").addText(sizes);
                        beerMenusEl.addElement("price").addText(prices);
                    }                
                }
            } else if( type == 5) {
                stmt                        = transconn.prepareStatement(selectBottleBeer);
                stmt.setInt(1, locationId);
                rs                          = stmt.executeQuery();                
                while (rs.next()) {
                    menuAvail               = true;
                    int rsIndex             = 1;
                    Element beerMenusEl     = toAppend.addElement("beerMenus");
                    int productId           = rs.getInt(rsIndex++);
                    beerMenusEl.addElement("productId").addText(String.valueOf(productId));
                    beerMenusEl.addElement("category").addText(String.valueOf(rs.getInt(rsIndex++)));
                    beerMenusEl.addElement("boardName").addText(String.valueOf(rs.getString(rsIndex++)));
                    beerMenusEl.addElement("abv").addText(String.valueOf(rs.getString(rsIndex++)));
                    beerMenusEl.addElement("brewery").addText(HandlerUtils.nullToEmpty(rs.getString(rsIndex++)));
                    beerMenusEl.addElement("style").addText(HandlerUtils.nullToEmpty(rs.getString(rsIndex++)));
                    beerMenusEl.addElement("origin").addText(String.valueOf(rs.getString(rsIndex++)));
                    beerMenusEl.addElement("seasonality").addText(String.valueOf(rs.getString(rsIndex++)));
                    beerMenusEl.addElement("ibu").addText(String.valueOf(rs.getInt(rsIndex++)));
                    beerMenusEl.addElement("price").addText(String.valueOf(rs.getString(rsIndex++)));
                    beerMenusEl.addElement("price").addText("");
                }
                if(!menuAvail){
                    Element beerMenusEl     = toAppend.addElement("beerMenus");
                    beerMenusEl.addElement("productId").addText(String.valueOf(0));
                    beerMenusEl.addElement("category").addText("0");
                    beerMenusEl.addElement("boardName").addText("-");
                    beerMenusEl.addElement("abv").addText("-");
                    beerMenusEl.addElement("brewery").addText("-");
                    beerMenusEl.addElement("style").addText("-");
                    beerMenusEl.addElement("origin").addText("-");
                    beerMenusEl.addElement("seasonality").addText("-");
                    beerMenusEl.addElement("ibu").addText("0");
                    beerMenusEl.addElement("size").addText("-");
                    beerMenusEl.addElement("price").addText("-");
                }
            } else {
                stmt                        = transconn.prepareStatement(selectBeverageDescription);
                stmt.setInt(1, locationId);
                rs                          = stmt.executeQuery();
                int totalBeerMenus          = 0;
                while (rs.next()) {
                    menuAvail               = true;
                    int rsIndex             = 1;
                    Element beerMenusEl     = toAppend.addElement("beerMenus");
                    int productId           = rs.getInt(rsIndex++);
                    beerMenusEl.addElement("productId").addText(String.valueOf(productId));
                    beerMenusEl.addElement("category").addText(String.valueOf(rs.getInt(rsIndex++)));
                    beerMenusEl.addElement("boardName").addText(String.valueOf(rs.getString(rsIndex++)));
                    beerMenusEl.addElement("abv").addText(String.valueOf(rs.getString(rsIndex++)));
                    beerMenusEl.addElement("brewery").addText(HandlerUtils.nullToEmpty(rs.getString(rsIndex++)));
                    beerMenusEl.addElement("style").addText(HandlerUtils.nullToEmpty(rs.getString(rsIndex++)));
                    beerMenusEl.addElement("origin").addText(String.valueOf(rs.getString(rsIndex++)));
                    beerMenusEl.addElement("seasonality").addText(String.valueOf(rs.getString(rsIndex++)));
                    beerMenusEl.addElement("ibu").addText(String.valueOf(rs.getInt(rsIndex++)));
                    String prices               = "";
                    if (productPrices.containsKey(productId)) {                                       
                        ArrayList<String> invoiceArray
                                                = productPrices.get(productId);
                        for (String price : invoiceArray) {
                            prices          +=" " + price ;
                        }
                    }
                    String sizes         = "";
                        if (productSizes.containsKey(productId)) {
                                               
                            ArrayList<String> sizeArray
                                            = productSizes.get(productId);
                            for (String size : sizeArray) {
                                sizes      +=" " + size ;
                            }
                        }
                     beerMenusEl.addElement("size").addText(sizes);
                    beerMenusEl.addElement("price").addText(prices);
                    totalBeerMenus++;
                }
             }


             if(!menuAvail){
                 Element beerMenusEl     = toAppend.addElement("beerMenus");  
                 beerMenusEl.addElement("productId").addText(String.valueOf(0));
                 beerMenusEl.addElement("category").addText("0");
                 beerMenusEl.addElement("boardName").addText("-");
                 beerMenusEl.addElement("abv").addText("-");
                 beerMenusEl.addElement("brewery").addText("-");
                 beerMenusEl.addElement("style").addText("-");
                 beerMenusEl.addElement("origin").addText("-");
                 beerMenusEl.addElement("seasonality").addText("-");
                 beerMenusEl.addElement("ibu").addText("0");
                 beerMenusEl.addElement("size").addText("-");
                 beerMenusEl.addElement("price").addText("-");
             }
            
        } catch (SQLException sqle) {
            logger.dbError("Database error: "+sqle.toString());
            throw new HandlerException(sqle);
        } finally {
            close(rs);
            close(stmt);
        }
    }

      
       private void getFavouriteBeer(Element toHandle, Element toAppend) throws HandlerException {
        
        
        int user                            = HandlerUtils.getRequiredInteger(toHandle, "userId");
        
      
        
        String selectProduct                = "Select  f.product, p.name FROM favoriteBeer  f LEFT JOIN product p ON p.id=f.product WHERE f.user = ? ORDER BY p.name;";
        
        PreparedStatement stmt              = null;
        ResultSet rs                        = null;
        try {
            stmt                            = transconn.prepareStatement(selectProduct);
            stmt.setInt(1, user);
            rs                              = stmt.executeQuery();   
            while (rs.next()) {   
                Element dataEl              = toAppend.addElement("favouriteBeer");                
                dataEl.addElement("id").addText(String.valueOf(rs.getInt(1)));                
                dataEl.addElement("name").addText(HandlerUtils.nullToEmpty(rs.getString(2)));                
            }             
        
        } catch (SQLException sqle) {
            logger.dbError("Database error in getFavouriteBeer: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(rs);
            close(stmt);
        }
       }
       
       
        private void getFavouriteLocation(Element toHandle, Element toAppend) throws HandlerException {
        
      
        int user                            = HandlerUtils.getRequiredInteger(toHandle, "userId");
        
      
        
        String selectLocation               = "SELECT DISTINCT f.location, CONCAT(l.boardName, ' - ', l.name, ', ', l.addrState) Name FROM favoriteLocation f " +
                                            " LEFT JOIN location l ON l.id=f.location WHERE f.user = ? ORDER BY l.addrState, Name;";
        
        PreparedStatement stmt              = null;
        ResultSet rs                        = null;
        try {
            stmt                            = transconn.prepareStatement(selectLocation);
            stmt.setInt(1, user);
            rs                              = stmt.executeQuery();   
            while (rs.next()) {   
                Element dataEl              = toAppend.addElement("favouriteLocation");                
                dataEl.addElement("id").addText(String.valueOf(rs.getInt(1)));                
                dataEl.addElement("name").addText(HandlerUtils.nullToEmpty(rs.getString(2)));                
            }             
        
        } catch (SQLException sqle) {
            logger.dbError("Database error in getFavouriteLocation: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(rs);
            close(stmt);
        }
       }
        
        
         private void removeFavouriteBeer(Element toHandle, Element toAppend) throws HandlerException {
        
    
        int user                            = HandlerUtils.getRequiredInteger(toHandle, "userId");
        String products                     = HandlerUtils.getRequiredString(toHandle, "products");
        
      
        
        String deleteBeer               = "DELETE FROM favoriteBeer   WHERE product IN ("+products+") AND user = ?;";
        
        PreparedStatement stmt              = null;
        ResultSet rs                        = null;
        try {
            if(products!=null && !products.equals("")){
                stmt                            = transconn.prepareStatement(deleteBeer);
                stmt.setInt(1,user);
                stmt.executeUpdate();
            }
           
        } catch (SQLException sqle) {
            logger.dbError("Database error in removeFavouriteLocation: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(rs);
            close(stmt);
        }
       }
        
        
         private void removeFavouriteLocation(Element toHandle, Element toAppend) throws HandlerException {
        
        
        int user                            = HandlerUtils.getRequiredInteger(toHandle, "userId");
        String locations                    = HandlerUtils.getRequiredString(toHandle, "locations");
        
      
        
        String deleteLocation               = "DELETE FROM favoriteLocation   WHERE location IN ("+locations+") AND user = ?;";
        
        PreparedStatement stmt              = null;
        ResultSet rs                        = null;
        try {
            if(locations!=null && !locations.equals("")){
                stmt                            = transconn.prepareStatement(deleteLocation);
                stmt.setInt(1,user);
                stmt.executeUpdate();
            }
           
        } catch (SQLException sqle) {
            logger.dbError("Database error in removeFavouriteLocation: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(rs);
            close(stmt);
        }
       }
         
         
         private void getPushNotification(Element toHandle, Element toAppend) throws HandlerException {

        int locationId                      = HandlerUtils.getRequiredInteger(toHandle, "locationId");
        int isActive                        = HandlerUtils.getRequiredInteger(toHandle, "isActive");
        PreparedStatement stmt              = null;
        ResultSet rs                        = null;
        String selectLocationNotification   = "SELECT id, message, date FROM locationPushNotification WHERE location = ?  ";
        if(isActive > 0) {
            selectLocationNotification      += " AND date >= DATE(now());";
        } else {
            selectLocationNotification      += " AND date < DATE(now());";
        }

        try {
            stmt                            = transconn.prepareStatement(selectLocationNotification);
            stmt.setInt(1,locationId);
            rs                              = stmt.executeQuery();
            while (rs.next()) {
                Element locationNotifiEl  = toAppend.addElement("pushNotifications");               
                locationNotifiEl.addElement("id").addText(HandlerUtils.nullToEmpty(rs.getString(1)));
                locationNotifiEl.addElement("message").addText(HandlerUtils.nullToEmpty(rs.getString(2)));
                locationNotifiEl.addElement("date").addText(HandlerUtils.nullToEmpty(rs.getString(3))); 
                        
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: "+sqle.toString());
            throw new HandlerException(sqle);
        } finally {
            close(rs);
            close(stmt);
        }
    }
         
         
         private void addUpdateDeletePushNotification(Element toHandle, Element toAppend) throws HandlerException {

        int callerId                        = getCallerId(toHandle);
        int locationId                      = HandlerUtils.getRequiredInteger(toHandle, "locationId");
        ResultSet rs                        = null;
        PreparedStatement stmt              = null;

        try {

         
            Iterator addNotification        = toHandle.elementIterator("addPushNotifications");
            while (addNotification.hasNext()) {
                Element locationNotification= (Element) addNotification.next();
                String message              = HandlerUtils.getRequiredString(locationNotification, "message");
                String date                 = HandlerUtils.getRequiredString(locationNotification, "date");
                if(date!=null){
                    date                    = dbDateFormat.format(dateFormat.parse(date)).substring(0, 10);
                }

                String getLastId            = " SELECT LAST_INSERT_ID()";

                stmt                        = transconn.prepareStatement("INSERT INTO locationPushNotification (location, message, date) VALUES (?, ?, ?)");
                stmt.setInt(1, locationId);
                stmt.setString(2, message);
                stmt.setString(3, date);
                stmt.executeUpdate();

                stmt                        = transconn.prepareStatement(getLastId);
                rs                          = stmt.executeQuery();
                if (rs.next()) {
                    int id                  = rs.getInt(1);
                    String logMessage       = "Added Location PushNotification '" + message + "'";
                    logger.portalDetail(callerId, "addLocationPushNotification", 0, "locationPushNotification", id, logMessage, transconn);
                } else {
                    logger.dbError("first call to LAST_INSERT_ID in addLocationPushNotification failed to return a result");
                    throw new HandlerException("Database Error");
                }
            }

            Iterator updateNotification = toHandle.elementIterator("updatePushNotifications");
            while (updateNotification.hasNext()) {
                Element locationNotification= (Element) updateNotification.next();
                int id                      = HandlerUtils.getRequiredInteger(locationNotification, "id");
                String message              = HandlerUtils.getRequiredString(locationNotification, "message");
                String date                 = HandlerUtils.getRequiredString(locationNotification, "date");
                 if(date!=null){
                    date                    = dbDateFormat.format(dateFormat.parse(date)).substring(0, 10);
                }

                String updateAccountEmail   = " UPDATE locationPushNotification SET message=?, date=? WHERE id = ? ";

                stmt                        = transconn.prepareStatement(updateAccountEmail);
                stmt.setString(1, message);
                stmt.setString(2, date);
                stmt.setInt(3, id);
                stmt.executeUpdate();
                String logMessage           = "Updated Location PushNotification '" + message + "'";
                logger.portalDetail(callerId, "updateLocationPushNotification", 0, "locationPushNotification", id, logMessage, transconn);
            }

            Iterator deleteNotification = toHandle.elementIterator("deletePushNotifications");
            while (deleteNotification.hasNext()) {
                Element locationNotification= (Element) deleteNotification.next();
                int id                      = HandlerUtils.getRequiredInteger(locationNotification, "id");

                String delete               = " DELETE FROM locationPushNotification WHERE id = ? ";

                stmt                        = transconn.prepareStatement(delete);
                stmt.setInt(1,id);
                stmt.executeUpdate();

                String logMessage           = "Delete Location PushNotification '" + id + "'";
                logger.portalDetail(callerId, "deleteLocationPushNotification", 0, "locationPushNotification", id, logMessage, transconn);
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: "+sqle.toString());
            throw new HandlerException(sqle);
        } catch(Exception e) {
             logger.dbError("Database error: "+e.toString());
        }finally {
            close(stmt);
            close(rs);
        }
    }
         
         
         private void checkFullProductVersion(Element toHandle, Element toAppend) throws HandlerException {

        String version                      = HandlerUtils.getRequiredString(toHandle, "productVersion");        
        PreparedStatement stmt              = null;
        ResultSet rs                        = null,rsDetail = null;
        logger.debug("Version:"+version);
        String changeLogVersion             = "SELECT DISTINCT l.product,   IFNULL(p.name, ''), pD.category, pD.boardName, IF(pD.abv = 0.0, '', CONCAT(pD.abv,'%')),pD.origin, pD.seasonality ,  IFNULL((SELECT pSM.productSet FROM productSetMap pSM LEFT JOIN productSet pS ON pS.id= pSM.productSet"
                                            + " WHERE product=l.product AND pS.productSetType=7 ORDER BY pS.id DESC LIMIT 1), 0),IFNULL((SELECT pSM.productSet FROM productSetMap pSM LEFT JOIN productSet pS ON pS.id= pSM.productSet"
                                            + " WHERE product=l.product AND pS.productSetType=9 ORDER BY pS.id DESC LIMIT 1), 0),pD.ibu FROM productChangeLog l LEFT JOIN product p ON p.id = l.product LEFT JOIN productDescription pD ON pD.product=p.id"
                                            + " WHERE date > ? AND productType=1 AND l.type IN (1,2) ORDER BY l.id, date;";
        String selectBrewStyle              = "SELECT pS.id, pS.name, pS.productSetType FROM productSetMap pSM LEFT JOIN productSet pS ON pS.id = pSM.productSet " +
                                            " WHERE pSM.product=? AND pS.productSetType IN (7,9);";
        String breweryLog                   = "SELECT DISTINCT l.product, l.type, p.name ,l.productType, p.approved from productChangeLog l LEFT JOIN productSet p ON p.id=l.product " +
                                            " WHERE  date > ? AND p.name <> 'null' AND l.productType > 1 ORDER BY l.id, date;";
        
        try {

            

            stmt                            = transconn.prepareStatement(changeLogVersion);
            stmt.setString(1, version);
            rs                              = stmt.executeQuery();
            while(rs.next()) {
                Element productEl           = toAppend.addElement("changeProduct");
                productEl.addElement("category").addText(String.valueOf(rs.getInt(3)));
                productEl.addElement("id").addText(String.valueOf(rs.getInt(1)));
                productEl.addElement("name").addText(HandlerUtils.nullToEmpty(rs.getString(2)));
                productEl.addElement("boardName").addText(HandlerUtils.nullToEmpty(rs.getString(4)));
                productEl.addElement("abv").addText(HandlerUtils.nullToEmpty(rs.getString(5)));
                productEl.addElement("origin").addText(HandlerUtils.nullToEmpty(rs.getString(6)));
                productEl.addElement("seasonality").addText(HandlerUtils.nullToEmpty(rs.getString(7)));                
                productEl.addElement("breweryId").addText(String.valueOf(rs.getInt(8)));
                productEl.addElement("styleId").addText(String.valueOf(rs.getInt(9)));
                productEl.addElement("ibu").addText(String.valueOf(rs.getInt(10)));
            }
            stmt                            = transconn.prepareStatement(breweryLog);
            stmt.setString(1, version);
            rs                              = stmt.executeQuery();
            while(rs.next()) {
               int prodtype                 = rs.getInt(4);
               if(prodtype==7){
                   Element productEl        = toAppend.addElement("changeBrewery");
                   productEl.addElement("type").addText(String.valueOf(rs.getInt(2)));
                   productEl.addElement("breweryId").addText(String.valueOf(rs.getInt(1)));
                   productEl.addElement("breweryName").addText(HandlerUtils.nullToEmpty(rs.getString(3)));                   
                   productEl.addElement("approved").addText(String.valueOf(rs.getInt(5)));
               } else if (prodtype==9){
                   Element productEl        = toAppend.addElement("changeStyle");
                   productEl.addElement("type").addText(String.valueOf(rs.getInt(2)));
                   productEl.addElement("styleId").addText(String.valueOf(rs.getInt(1)));
                   productEl.addElement("styleName").addText(HandlerUtils.nullToEmpty(rs.getString(3)));
                   productEl.addElement("approved").addText(String.valueOf(rs.getInt(5)));
                }
            }
            stmt                            = transconn.prepareStatement("SELECT max(date) from productChangeLog");
            rs                              = stmt.executeQuery();
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

         
         
         private void getLocationMenu(Element toHandle, Element toAppend) throws HandlerException {
             
        String products                     = " 0";
        PreparedStatement stmt              = null;
        ResultSet rs                        = null, rsDetail = null;
        ArrayList<Double> pricesMenus       = new ArrayList<Double>();
        HashMap<Integer, ArrayList<Double>> productPrices
                                            = new HashMap<Integer, ArrayList<Double>>();
        String selectLocations              = "SELECT IFNULL(GROUP_CONCAT(location),0) FROM locationBeerBoardMap WHERE api=1 LIMIT 25;";
        String selectLocation               = "SELECT location FROM locationBeerBoardMap WHERE api=1;";
        
        
        try {
            
             boolean menuAvail              = false;
             int product                    = -1;            
             String selectProducts      = "SELECT 1,l.product,l.local FROM line l LEFT JOIN bar b ON b.id = l.bar WHERE l.status = 'RUNNING' AND l.product NOT IN(4311,10661) AND b.location = ?"
                                        + " UNION ALL (SELECT DISTINCT 2, lU.product, l.local FROM lineUpdates lU LEFT JOIN line l ON l.id = lU.line WHERE lU.location  = ?  AND lU.date > SUBDATE(NOW(), INTERVAL 7 DAY) AND l.status = 'RUNNING' order by lU.id DESC LIMIT 2 )"
                                        + " UNION SELECT DISTINCT 3, b.product ,0 FROM bottleBeer b  WHERE b.location  = ? "
                                        + " UNION SELECT DISTINCT 4,c.product, 0 FROM comingSoonProducts c WHERE c.location=?";
             
             stmt                           = transconn.prepareStatement(selectLocation);             
             rs                             = stmt.executeQuery();
             while(rs.next()) {
                 int locationId             = rs.getInt(1);
                 Element locationEl     = toAppend.addElement("location");                     
                 locationEl.addElement("locationId").addText(String.valueOf(locationId));
                 stmt                        = transconn.prepareStatement(selectProducts);
                 stmt.setInt(1,locationId);
                 stmt.setInt(2,locationId);
                 stmt.setInt(3,locationId);
                 stmt.setInt(4,locationId);
                 rsDetail                          = stmt.executeQuery();
                 while (rsDetail.next()) {
                     Element beerMenusEl     = locationEl.addElement("product");                                          
                     beerMenusEl.addElement("type").addText(String.valueOf(rsDetail.getInt(1)));
                     beerMenusEl.addElement("productId").addText(String.valueOf(rsDetail.getInt(2)));
                     beerMenusEl.addElement("local").addText(String.valueOf(rsDetail.getInt(3)));
                 }
                 stmt            = transconn.prepareStatement("UPDATE locationBeerBoardMap SET  api=0 WHERE location = ?");
                 stmt.setInt(1, locationId);
                 stmt.executeUpdate();
                 
                 
                 
             }
             
             
            
        } catch (SQLException sqle) {
            logger.dbError("Database error: "+sqle.toString());
            throw new HandlerException(sqle);
        } finally {
            close(rsDetail);
            close(rs);
            close(stmt);
        }
    }
         
    
     private void updateJSMenuDb(Element toHandle, Element toAppend) {
          try {
            
            URL url = new URL("http://social.usbeveragenet.com:8080/usbnapi/updateBeerMenu");
            URLConnection conn = url.openConnection();
            conn.setRequestProperty("User-Agent","Mozilla/5.0 (Windows NT 5.1; rv:19.0) Gecko/20100101 Firefox/19.0");           
            conn.connect();
            BufferedReader serverResponse = new BufferedReader(
                    new InputStreamReader(conn.getInputStream()));
            logger.debug("JSMenu Update Response:"+serverResponse.readLine());
            serverResponse.close();            
            
         
        } catch (IOException e) {
            logger.debug("JSMenu IO:"+e.getMessage());
        }
        
     }
         
    public boolean isProduction(){
        boolean production                  = true;
        try{
            NetworkInterface ni             = NetworkInterface.getByName("eth0");
            Enumeration<InetAddress> ias    = ni.getInetAddresses();
            for (; ias.hasMoreElements();){
                InetAddress addr            = ias.nextElement();
                //logger.debug(addr.getHostAddress());
                if(addr.getHostAddress().contains("10.10.30.210")) {
                    production              = false;
                }
            }      
        } catch(Exception e ){}
        return production;
    }
}
