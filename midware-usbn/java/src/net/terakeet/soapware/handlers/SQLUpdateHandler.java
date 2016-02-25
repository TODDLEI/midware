/*
 * SQLUpdateHandler.java
 *
 * Created on August 23, 2005, 4:29 PM
 *
 */
package net.terakeet.soapware.handlers;

import java.io.Externalizable;
import net.terakeet.soapware.*;
import net.terakeet.soapware.security.*;
import net.terakeet.soapware.handlers.auper.*;
import net.terakeet.util.MidwareLogger;
import net.terakeet.usbn.WebPermission;
import net.terakeet.util.TemplatedMessage;
import javax.net.ssl.HttpsURLConnection;
import net.terakeet.util.MailException;
import java.text.ParseException;
import org.apache.log4j.Logger;
import java.sql.Date;
import java.sql.Time;
import java.text.SimpleDateFormat;
import net.terakeet.soapware.handlers.report.*;
import net.terakeet.util.QuadGraphics;
import org.dom4j.Element;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.CallableStatement;
import java.lang.String;
import java.security.MessageDigest;
import java.util.Iterator;
import java.util.Set;
import java.util.HashSet;
import java.util.Map;
import java.util.HashMap;
import java.text.DecimalFormat;
import java.util.*;
import org.apache.commons.lang3.text.WordUtils;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;

import java.security.cert.CertificateException;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.KeyManager;

import org.json.JSONObject;
import org.json.JSONArray;
import org.json.JSONException;
import net.terakeet.util.QuadGraphics;

public class SQLUpdateHandler implements Handler {

    private LocationMap locationMap;
    private MidwareLogger logger;
    private static final String transConnName = "auper";
    private RegisteredConnection transconn ;
    private SecureSession ss;
    private static SimpleDateFormat dbDateFormat
                                            = new SimpleDateFormat("yyyy-MM-dd");
    private static SimpleDateFormat newDateFormat =
            new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private static SimpleDateFormat dateFormat
                                            = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");

    /**
     * Creates a new instance of SQLUpdateHandler
     */
    public SQLUpdateHandler() {
        HandlerUtils.initializeClientKeyManager();
        logger = new MidwareLogger(SQLUpdateHandler.class.getName());
        transconn = null;
    }

    public void handle(Element toHandle, Element toAppend) throws HandlerException {

        String clientKey = HandlerUtils.getOptionalString(toHandle, "clientKey");
        ss = ClientKeyManager.getSession(clientKey);

        String function = toHandle.getName();

        logger = new MidwareLogger(SQLUpdateHandler.class.getName(), function);
        logger.debug("SQLUpdateHandler processing method: " + function);
        logger.xml("request: " + toHandle.asXML());

        transconn = DatabaseConnectionManager.getNewConnection(transConnName,
                function + " (SQLUpdateHandler)");

        try {
            // All methods in this handler require a "L0-C1 Admin" key
            if (ss.getLocation() == 0 && ss.getClientId() == 1 && ss.getSecurityLevel().canAdmin()) {
                if ("updateBeveragePlu".equals(function)) {
                    updateBeveragePlu(toHandle, responseFor(function, toAppend));
                } else if ("deleteBeveragePlu".equals(function)) {
                    deleteBeveragePlu(toHandle, responseFor(function, toAppend));
                } else if ("deleteBeverageSize".equals(function)) {
                    deleteBeverageSize(toHandle, responseFor(function, toAppend));
                } else if ("updateInventory".equals(function)) {
                    updateInventory(toHandle, responseFor(function, toAppend));
                } else if ("updateLineProduct".equals(function)) {
                    updateLineProduct(toHandle, responseFor(function, toAppend));
                } else if ("resetLineCounters".equals(function)) {
                    resetLineCounters(toHandle, responseFor(function, toAppend));
                } else if ("updateLineUnit".equals(function)) {
                    updateLineUnit(toHandle, responseFor(function, toAppend));
                } else if ("updateEmailReports".equals(function)) {
                    updateEmailReports(toHandle, responseFor(function, toAppend));
                } else if ("updateTextAlerts".equals(function)) {
                    updateTextAlerts(toHandle, responseFor(function, toAppend));
                } else if ("deleteInventory".equals(function)) {
                    deleteInventory(toHandle, responseFor(function, toAppend));
                } else if ("updateProduct".equals(function)) {
                    updateProduct(toHandle, responseFor(function, toAppend));
                } else if ("deleteProductRequest".equals(function)) {
                    deleteRequest(toHandle, responseFor(function, toAppend), "product");
                } else if ("deleteSupplierRequest".equals(function)) {
                    deleteRequest(toHandle, responseFor(function, toAppend), "supplier");
                } else if ("updateLowStockAlerts".equals(function)) {
                    updateLowStockAlerts(toHandle, responseFor(function, toAppend));
                } else if ("updateProductRequestAlert".equals(function)) {
                    updateProductRequestAlert(toHandle, responseFor(function, toAppend));
                } else if ("updateLocationStatusAlert".equals(function)) {
                    updateLocationStatusAlert(toHandle, responseFor(function, toAppend));
                } else if ("changeUnitPreference".equals(function)) {
                    changeUnitPreference(toHandle, responseFor(function, toAppend));
                } else if ("changeHomePreference".equals(function)) {
                    changeHomePreference(toHandle, responseFor(function, toAppend));
                } else if ("changePassword".equals(function)) {
                    changePassword(toHandle, responseFor(function, toAppend));
                } else if ("deleteGroupProductSet".equals(function)) {
                    deleteGroupProductSet(toHandle, responseFor(function, toAppend));
                } else if ("deleteRegionProductSet".equals(function)) {
                    deleteRegionProductSet(toHandle, responseFor(function, toAppend));
                } else if ("deleteUserRegion".equals(function)) {
                    deleteUserRegion(toHandle, responseFor(function, toAppend));
                } else if ("deleteGroups".equals(function)) {
                    deleteGroups(toHandle, responseFor(function, toAppend));
                } else if ("deleteGroupExclusion".equals(function)) {
                    deleteGroupExclusion(toHandle, responseFor(function, toAppend));
                } else if ("deleteGroupRegion".equals(function)) {
                    deleteGroupRegion(toHandle, responseFor(function, toAppend));
                } else if ("updateGroups".equals(function)) {
                    updateGroups(toHandle, responseFor(function, toAppend));
                } else if ("updateRegion".equals(function)) {
                    updateRegion(toHandle, responseFor(function, toAppend));
                } else if ("updateProductSet".equals(function)) {
                    updateProductSet(toHandle, responseFor(function, toAppend));
                } else if ("updateCounty".equals(function)) {
                    updateCounty(toHandle, responseFor(function, toAppend));
                } else if ("updateLocation".equals(function)) {
                    updateLocation(toHandle, responseFor(function, toAppend));
                } else if ("updateLocationDetails".equals(function)) {
                    updateLocationDetails(toHandle, responseFor(function, toAppend));
                } else if ("updateCustomer".equals(function)) {
                    updateCustomer(toHandle, responseFor(function, toAppend));
                } else if ("deleteUser".equals(function)) {
                    deleteUser(toHandle, responseFor(function, toAppend));
                } else if ("moveUser".equals(function)) {
                    moveUser(toHandle, responseFor(function, toAppend));
                } else if ("moveAdmin".equals(function)) {
                    moveAdmin(toHandle, responseFor(function, toAppend));
                } else if ("demoteAdmin".equals(function)) {
                    demoteAdmin(toHandle, responseFor(function, toAppend));
                } else if ("promoteUser".equals(function)) {
                    promoteUser(toHandle, responseFor(function, toAppend));
                } else if ("updateUserPermissions".equals(function)) {
                    updateUserPermissions(toHandle, responseFor(function, toAppend));
                } else if ("updateUserExclusion".equals(function)) {
                    updateUserExclusion(toHandle, responseFor(function, toAppend));
                } else if ("updateGroupRegion".equals(function)) {
                    updateGroupRegion(toHandle, responseFor(function, toAppend));
                } else if ("updateUser".equals(function)) {
                    updateUser(toHandle, responseFor(function, toAppend));
                } else if ("receiveOrder".equals(function)) {
                    receiveOrder(toHandle, responseFor(function, toAppend));
                } else if ("deactivateActiveUserAlerts".equals(function)) {
                    deactivateActiveUserAlerts(toHandle, responseFor(function, toAppend));
                } else if ("deactivateSuspensions".equals(function)) {
                    deactivateSuspensions(toHandle, responseFor(function, toAppend));
                } else if ("deactivateActiveAlerts".equals(function)) {
                    deactivateActiveAlerts(toHandle, responseFor(function, toAppend));
                } else if ("updateUserDashboardExclusion".equals(function)) {
                    updateUserDashboardExclusion(toHandle, responseFor(function, toAppend));
                } else if ("deleteGroups".equals(function)) {
                    deleteGroups(toHandle, responseFor(function, toAppend));
                } else if ("deleteOrder".equals(function)) {
                    deleteOrder(toHandle, responseFor(function, toAppend));
                } else if ("deleteEmailReports".equals(function)) {
                    deleteEmailReports(toHandle, responseFor(function, toAppend));
                } else if ("deleteTextAlerts".equals(function)) {
                    deleteTextAlerts(toHandle, responseFor(function, toAppend));
                } else if ("deleteCostCenter".equals(function)) {
                    deleteCostCenter(toHandle, responseFor(function, toAppend));
                } else if ("updateOrder".equals(function)) {
                    updateOrder(toHandle, responseFor(function, toAppend));
                } else if ("placeOrder".equals(function)) {
                    placeOrder(toHandle, responseFor(function, toAppend));
                } else if ("lineCleaning".equals(function)) {
                    lineCleaning(toHandle, responseFor(function, toAppend));
                } else if ("deactivateInventory".equals(function)) {
                    deactivateInventory(toHandle, responseFor(function, toAppend));
                } else if ("deactivateSupplierAddress".equals(function)) {
                    deactivateSupplierAddress(toHandle, responseFor(function, toAppend));
                } else if ("activateSupplierAddress".equals(function)) {
                    activateSupplierAddress(toHandle, responseFor(function, toAppend));
                } else if ("updateSupplier".equals(function)) {
                    updateSupplier(toHandle, responseFor(function, toAppend));
                } else if ("updateSupplierAddress".equals(function)) {
                    updateSupplierAddress(toHandle, responseFor(function, toAppend));
                } else if ("updateSupplierEmail".equals(function)) {
                    updateSupplierEmail(toHandle, responseFor(function, toAppend));
                } else if ("updateLocationSupplier".equals(function)) {
                    updateLocationSupplier(toHandle, responseFor(function, toAppend));
                } else if ("deleteLocationSupplier".equals(function)) {
                    deleteLocationSupplier(toHandle, responseFor(function, toAppend));
                } else if ("updatePluCodes".equals(function)) {
                    updatePluCodes(toHandle, responseFor(function, toAppend));
                } else if ("activateMiscProduct".equals(function)) {
                    activateMiscProduct(toHandle, responseFor(function, toAppend));
                } else if ("deactivateMiscProduct".equals(function)) {
                    deactivateMiscProduct(toHandle, responseFor(function, toAppend));
                } else if ("updateMiscProduct".equals(function)) {
                    updateMiscProduct(toHandle, responseFor(function, toAppend));
                } else if ("importMiscProducts".equals(function)) {
                    importMiscProducts(toHandle, responseFor(function, toAppend));
                } else if ("updateBar".equals(function)) {
                    updateBar(toHandle, responseFor(function, toAppend));
                } else if ("deleteBar".equals(function)) {
                    deleteBar(toHandle, responseFor(function, toAppend));
                } //NischaySharma_14-May-2009_Start: Added call to updateEventDates
                else if ("updateEventDates".equals(function)) {
                    updateEventDates(toHandle, responseFor(function, toAppend));
                } //NischaySharma_14-May-2009_End
                else if ("updateZones".equals(function)) {
                    updateZones(toHandle, responseFor(function, toAppend));
                } else if ("updateStations".equals(function)) {
                    updateStations(toHandle, responseFor(function, toAppend));
                } else if ("updateCostCenters".equals(function)) {
                    updateCostCenters(toHandle, responseFor(function, toAppend));
                } else if ("resetPasswordByAdmin".equals(function)) {
                    resetPasswordByAdmin(toHandle, responseFor(function, toAppend));
                } else if ("updateUserAlerts".equals(function)) {
                    updateUserAlerts(toHandle, responseFor(function, toAppend));
                } else if ("updateBevBox".equals(function)) {
                    updateBevBox(toHandle, responseFor(function, toAppend));
                } else if ("tempCommand".equals(function)) {
                    tempCommand(toHandle, responseFor(function, toAppend));
                } else if ("updateLineCleaning".equals(function)) {
                    updateLineCleaning(toHandle, responseFor(function, toAppend));
                } else if ("updateBeverageSize".equals(function)) {
                    updateBeverageSize(toHandle, responseFor(function, toAppend));
                } else if ("updateCustomerPeriods".equals(function)) {
                    updateCustomerPeriods(toHandle, responseFor(function, toAppend));
                } else if ("transferTableData".equals(function)) {
                    transferTableData(toHandle, responseFor(function, toAppend));
                } else if ("updateBeerBoardSettings".equals(function)) {
                    updateBeerBoardSettings(toHandle, responseFor(function, toAppend));
                } else if ("updateSpecialsEventDates".equals(function)) {
                    updateSpecialsEventDates(toHandle, responseFor(function, toAppend));
                } else if ("updateCateredEventDates".equals(function)) {
                    updateCateredEventDates(toHandle, responseFor(function, toAppend));
                } else if ("updateTestInventory".equals(function)) {
                    updateTestInventory(toHandle, responseFor(function, toAppend));
                } else if ("updateBottleInventory".equals(function)) {
                    updateBottleInventory(toHandle, responseFor(function, toAppend));
                } else if ("updateTestUser".equals(function)) {
                    updateTestUser(toHandle, responseFor(function, toAppend));
                } else if ("setBottleInv".equals(function)) {
                    setBottleInv(toHandle, responseFor(function, toAppend));
                } else if ("updateUnitSettings".equals(function)) {
                    updateUnitSettings(toHandle, responseFor(function, toAppend));
                } else if ("updateSMSAlerts".equals(function)) {
                    updateSMSAlerts(toHandle, responseFor(function, toAppend));
                } else if ("deleteBeverageSize1".equals(function)) {
                    deleteBeverageSize1(toHandle, responseFor(function, toAppend));
                } else if ("setBrixRatio".equals(function)) {
                    setBrixRatio(toHandle, responseFor(function, toAppend));
                } else if ("updateProduct1".equals(function)) {
                    updateProduct1(toHandle, responseFor(function, toAppend));
                } else if ("adminChangeProductType".equals(function)) {
                    adminChangeProductType(toHandle, responseFor(function, toAppend));
                } else if ("disableRewards".equals(function)) {
                    disableRewards(toHandle, responseFor(function, toAppend));
                } else if ("updateTestBeveragePlu".equals(function)) {
                    updateTestBeveragePlu(toHandle, responseFor(function, toAppend));
                } else if ("deleteTestBeveragePlu".equals(function)) {
                    deleteTestBeveragePlu(toHandle, responseFor(function, toAppend));
                } else if ("disableReportRequest".equals(function)) {
                    disableReportRequest(toHandle, responseFor(function, toAppend));
                } else if ("manualInventoryUpload".equals(function)) {
                    manualInventoryUpload(toHandle, responseFor(function, toAppend));
                } else if ("updateUnitCount".equals(function)) {
                    updateUnitCount(toHandle, responseFor(function, toAppend));
                } else if ("readingDateFixer".equals(function)) {
                    readingDateFixer(toHandle, responseFor(function, toAppend));
                } else if ("updateLocationLogs".equals(function)) {
                    updateLocationLogs(toHandle, responseFor(function, toAppend));
                } else if ("deleteLocationLogs".equals(function)) {
                    deleteLocationLogs(toHandle, responseFor(function, toAppend));
                } else if ("promoteITUser".equals(function)) {
                    promoteITUser(toHandle, responseFor(function, toAppend));
                } else if ("demoteITAdmin".equals(function)) {
                    demoteITAdmin(toHandle, responseFor(function, toAppend));
                } else if ("updateCoolers".equals(function)) {
                    updateCoolers(toHandle, responseFor(function, toAppend));
                } else if ("deleteKegLines".equals(function)) {
                    deleteKegLines(toHandle, responseFor(function, toAppend));
                } else if ("updateConcessionProductSupplier".equals(function)) {
                    updateConcessionProductSupplier(toHandle, responseFor(function, toAppend));
                } else if ("deleteConcessionProductSupplier".equals(function)) {
                    deleteConcessionProductSupplier(toHandle, responseFor(function, toAppend));
                } else if ("queryRunner".equals(function)) {
                    queryRunner(toHandle, responseFor(function, toAppend));
                } else if ("deleteUnApprovedProduct".equals(function)) {
                    deleteUnApprovedProduct(toHandle, responseFor(function, toAppend));
                } else if ("updateDeleteUnApprovedProductSet".equals(function)) {
                    updateDeleteUnApprovedProductSet(toHandle, responseFor(function, toAppend));
                } else if ("resetForgotPassword".equals(function)) {
                    resetForgotPassword(toHandle, responseFor(function, toAppend));
                } else if ("updateOrderSetting".equals(function)) {
                    updateOrderSetting(toHandle, responseFor(function, toAppend));
                } else if ("updateBBTVAutoFeed".equals(function)) {
                    updateBBTVAutoFeed(toHandle, responseFor(function, toAppend));
                } else if ("updateBBTVMenuFormat".equals(function)) {
                    updateBBTVMenuFormat(toHandle, responseFor(function, toAppend));
                } else if ("updateCustomBeerName".equals(function)) {
                    updateCustomBeerName(toHandle, responseFor(function, toAppend));
                } else if ("updateCustomBeerDesc".equals(function)) {
                    updateCustomBeerDesc(toHandle, responseFor(function, toAppend));
                } else if ("updateCustomStyleName".equals(function)) {
                    updateCustomStyleName(toHandle, responseFor(function, toAppend));
                } else if ("createOrder".equals(function)) {
                    createOrder(toHandle, responseFor(function, toAppend));
                }else if ("generateNewPurchase".equals(function)) {
                    generateNewPurchase(toHandle, responseFor(function, toAppend));
                } else if ("updateFacebookTwitterSetting".equals(function)) {
                    updateFacebookTwitterSetting(toHandle, responseFor(function, toAppend));
                }  else if ("linkBrasstapItems".equals(function)) {
                    linkBrasstapItems(toHandle, responseFor(function, toAppend));
                } else if ("resetKegLevel".equals(function)) {
                    resetKegLevel(toHandle, responseFor(function, toAppend));
                } else if ("updateComingSoonProductLine".equals(function)) {
                    updateComingSoonProductLine(toHandle, responseFor(function, toAppend));
                } else if ("updateBottleBeer".equals(function)) {
                    updateBottleBeer(toHandle, responseFor(function, toAppend));
                }  else if ("deleteBottleBeer".equals(function)) {
                    deleteBottleBeer(toHandle, responseFor(function, toAppend));
                } else if ("testNewBeerNotification".equals(function)) {
                    testNewBeerNotification(toHandle, responseFor(function, toAppend));
                } else if ("getCurrentLines".equals(function)) {
                    getCurrentLines(toHandle, responseFor(function, toAppend));
                } else if ("postQuadMenu".equals(function)) {
                    postQuadMenu(toHandle, responseFor(function, toAppend));
                } else if ("onDeckGoLive".equals(function)) {
                    onDeckGoLive(toHandle, responseFor(function, toAppend));
                } else if ("sendToPrinter".equals(function)) {
                    sendToPrinter(toHandle, responseFor(function, toAppend));
                } else if ("pluCopyOver".equals(function)) {
                    pluCopyOver(toHandle, responseFor(function, toAppend));
                } else if ("clearPLUBeverages".equals(function)) {
                    clearPLUBeverages(toHandle, responseFor(function, toAppend));
                } else {
                    logger.generalWarning("Unknown function '" + function + "'.");
                }
            } else {
                // access violation
                addErrorDetail(toAppend, "Access violation: This method is not available with your client key");
                logger.portalAccessViolation("Tried to call '" + function + "' with key " + ss.toString());
            }

        } catch (Exception e) {
            if (e instanceof HandlerException) {
                throw (HandlerException) e;
            } else {
                logger.midwareError("Non-handler exception thrown in SQLUpdateHandler: " + e.toString());
                logger.midwareError("XML: " + toHandle.asXML());
                throw new HandlerException(e);
            }
        } finally {
            // Log database use
            int queryCount = transconn.getQueryCount();
            logger.dbAction("Executed " + queryCount + " transactional quer" + (queryCount == 1 ? "y" : "ies"));
            transconn.close();
        }
        logger.xml("response: " + toAppend.asXML());
    }

    private Element responseFor(String s, Element e) {
        String responseNamespace = (String) SOAPMessage.getURIMap().get("tkmsg");
        return e.addElement("m:" + s + "Response", responseNamespace);
    }

    private String nullToEmpty(String s) {
        return (null == s) ? "" : s;
    }

    private void close(Statement s) {
        if (s != null) {
            try {
                s.close();
            } catch (SQLException sqle) {
            }
        }
    }

    private void close(ResultSet rs) {
        if (rs != null) {
            try {
                rs.close();
            } catch (SQLException sqle) {
            }
        }
    }

    private void close(Connection c) {
        if (c != null) {
            try {
                c.close();
            } catch (SQLException sqle) {
            }
        }
    }

    private void close(RegisteredConnection c) {
        if (c != null) {
            c.close();
        }
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

    private boolean checkForeignKey(String table, int value, RegisteredConnection transconn) throws SQLException {
        return checkForeignKey(table, "id", value, transconn);
    }

    /**  Check that a "field" within "table" exists with specified "value"
     */
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


    private void assertForeignKey(String table, int value, RegisteredConnection transconn) throws SQLException, HandlerException {
        if (!checkForeignKey(table, value, transconn)) {
            throw new HandlerException("Unknown " + table + ": " + value);
        }
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
    
    public boolean isReadOnlyUser( int callerId, int location){
        boolean readOnly                    = false;
        PreparedStatement stmt              = null;
        ResultSet rs                        =  null;
        try {
            stmt                      = transconn.prepareStatement("SELECT id FROM userMap  WHERE user=? AND location=? AND securityLevel=7;");                    
            stmt.setInt(1,callerId);
            stmt.setInt(2,location);
            rs                        = stmt.executeQuery();
            if (rs.next()) {
                readOnly              = true;
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error in deleteInventory: " + sqle.getMessage());            
        } finally {
            close(rs);
            close(stmt);
        }
        return readOnly;
            
    }

    /**  Check that a record in the datacase has a specified associated field
     *  For example, this method could be used that Bar#5745 was associated with Location #12
     *  The method call for this example would be ('bar',5754,'location',12,transconn)
     *  @param table the table that record to check exists in
     *  @param id the primary key of the record to check
     *  @param field the name of the field in the record to examine
     *  @param fieldValue the value of the specified field to match
     *  @param transconn a connected connection
     *
     *  @returns true if the row exists and the field matches, false otherwise
     */
    private boolean checkDatabaseAssociation(String table, int id, String field, int fieldValue, RegisteredConnection transconn) throws SQLException {
        PreparedStatement stmt = null;
        ResultSet rs = null;
        boolean result = false;

        String select = "SELECT " + field + " FROM " + table + " WHERE id=? LIMIT 1";

        stmt = transconn.prepareStatement(select);
        stmt.setInt(1, id);
        rs = stmt.executeQuery();
        if (rs.next()) {
            if (fieldValue == rs.getInt(1)) {
                result = true;
            }
        }
        close(rs);
        close(stmt);

        return result;
    }

    private void deleteInventory(Element toHandle, Element toAppend) throws HandlerException {
        PreparedStatement stmt = null;
        ResultSet rs = null;
         int callerId = getCallerId(toHandle);

        String deleteInventory = "DELETE FROM inventory WHERE id=? AND location=? LIMIT 1";
        String findProduct = "SELECT pr.id, pr.name FROM inventory i " +
                " LEFT JOIN product pr ON i.product = pr.id " +
                " WHERE i.id=? AND i.location=? LIMIT 1";
        String checkLine                    = "SELECT l.id,concat(s.systemId,' - ',l.lineIndex)  FROM line l  LEFT JOIN bar b ON b.id = l.bar "
                                            + " LEFT JOIN system s ON s.id = l.system WHERE b.location=? AND l.product=?;";
        try {
            Iterator prods = toHandle.elementIterator("inventory");
            while (prods.hasNext()) {
                Element prod = (Element) prods.next();
                int invId = HandlerUtils.getRequiredInteger(prod, "invId");
                int location = HandlerUtils.getRequiredInteger(prod, "locationId");
                if(callerId > 0 && location > 0 && isValidAccessUser(callerId, location,false)){
                // Logging
                stmt = transconn.prepareStatement(findProduct);
                stmt.setInt(1, invId);
                stmt.setInt(2, location);
                rs = stmt.executeQuery();
                int productId               = 0;
                String productName          = "";
                if (rs.next()) {
                    productId               = rs.getInt(1);
                    productName             = HandlerUtils.nullToEmpty(rs.getString(2));
                    String loggerMessage = "Deleted " + rs.getString(2) + " (" + rs.getInt(1) + ") from Inv";
                    logger.portalDetail(getCallerId(toHandle), "deleteInventory", location, "inventory", invId, loggerMessage, transconn);
                }
                
                stmt = transconn.prepareStatement(checkLine);                
                stmt.setInt(1, location);
                stmt.setInt(2, productId);
                rs = stmt.executeQuery();                
                if (!rs.next()) {
                    stmt = transconn.prepareStatement(deleteInventory);
                    stmt.setInt(1, invId);
                    stmt.setInt(2, location);
                    stmt.executeUpdate();
                } else {
                    String loggerMessage =  productName + " is active on the line "+rs.getString(2);                   
                    addErrorDetail(toAppend,loggerMessage);
                    logger.portalDetail(getCallerId(toHandle), "deleteInventory", location, "inventory", invId, loggerMessage, transconn);
                    
                }
                } else {
                    addErrorDetail(toAppend, "Invalid Access"  );
                }
                
                

            }
        } catch (SQLException sqle) {
            logger.dbError("Database error in deleteInventory: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
        }

    }

    private void deleteEmailReports(Element toHandle, Element toAppend) throws HandlerException {
        PreparedStatement stmt = null, innerstmt = null;
        ResultSet rs = null, innerrs = null;

        String select = " SELECT eR.id FROM emailReport eR WHERE eR.id=? AND eR.user=? ";
        String delete = " DELETE FROM emailReport WHERE id=? ";

        String selectTimeTable = " SELECT id FROM emailTimeTable WHERE report=? and user=?";
        String deleteTimeTable = "DELETE FROM emailTimeTable WHERE id=? ";

        int userId = HandlerUtils.getRequiredInteger(toHandle, "userId");
        Iterator i = toHandle.elementIterator("reports");

        try {
            while (i.hasNext()) {
                Element prod = (Element) i.next();
                int reportId = HandlerUtils.getRequiredInteger(prod, "reportId");
                stmt = transconn.prepareStatement(select);
                stmt.setInt(1, reportId);
                stmt.setInt(2, userId);
                rs = stmt.executeQuery();
                if (rs.next()) {
                    stmt = transconn.prepareStatement(delete);
                    stmt.setInt(1, reportId);
                    stmt.executeUpdate();
                    
                    innerstmt = transconn.prepareStatement(selectTimeTable);
                    innerstmt.setInt(1, reportId);
                    innerstmt.setInt(2, userId);
                    innerrs = innerstmt.executeQuery();
                    while (innerrs.next()) {
                        innerstmt = transconn.prepareStatement(deleteTimeTable);
                        innerstmt.setInt(1, innerrs.getInt(1));
                        innerstmt.executeUpdate();
                    }
                }
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error in deleteTextAlerts: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
            close(rs);
            close(innerstmt);
            close(innerrs);
        }

    }

    private void deleteTextAlerts(Element toHandle, Element toAppend) throws HandlerException {
        PreparedStatement stmt = null;
        ResultSet rs = null;

        String select = " SELECT id FROM textAlert WHERE id=? and user=?";
        String delete = "DELETE FROM textAlert WHERE id=? ";

        int userId = HandlerUtils.getRequiredInteger(toHandle, "userId");
        Iterator i = toHandle.elementIterator("alerts");

        try {
            while (i.hasNext()) {
                Element prod = (Element) i.next();
                int alertId = HandlerUtils.getRequiredInteger(prod, "alertId");

                //Check that this alert doesn't already exist in textAlerts at this location
                stmt = transconn.prepareStatement(select);
                stmt.setInt(1, alertId);
                stmt.setInt(2, userId);
                rs = stmt.executeQuery();
                if (rs.next()) {
                    stmt = transconn.prepareStatement(delete);
                    stmt.setInt(1, alertId);
                    stmt.executeUpdate();
                }
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error in deleteTextAlerts: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
        }

    }

    /** Delete a user and its permission map.
     * The client must pass a permission level as well to make sure the caller
     * can legally delete this user.
     *  args:
     *   <userId>  The user id to delete
     *   <customer> The customer id of the CALLER
     *   <access> The access level of the CALLER
     *
     *  The permission requirement is as follows:
     *     Deletee is a 0-manager : requires P:1
     *     Deletee is an N-manager : requires P:3 and a customerId that matches N
     *     Deletee is not a manager : requires P:3 and a customerId that matches
     */
    private void deleteUser(Element toHandle, Element toAppend) throws HandlerException {

        int userId = HandlerUtils.getRequiredInteger(toHandle, "userId");
        int customerId = HandlerUtils.getRequiredInteger(toHandle, "customer");
        WebPermission access = new WebPermission(HandlerUtils.getRequiredInteger(toHandle, "access"));
        int callerId = getCallerId(toHandle);

        String select = "SELECT customer,name,username " +
                " FROM user WHERE id =? LIMIT 1";
        String deleteUser = "DELETE FROM user WHERE id=?";
        String deleteMap = "DELETE FROM userMap WHERE user=?";
        String deleteNewEmailReports = "DELETE FROM emailReport WHERE user=?";
        String deleteTextAlerts = "DELETE FROM textAlert WHERE user=?";
        String deleteEmailTimeTable = "DELETE FROM emailTimeTable WHERE user=?";

        PreparedStatement stmt = null;
        ResultSet rs = null;

        try {
            stmt = transconn.prepareStatement(select);
            stmt.setInt(1, userId);
            rs = stmt.executeQuery();
            if (rs.next()) {
                int userCustomer = rs.getInt(1);
                String fullname = rs.getString(2);
                String username = rs.getString(3);

                String logMessage = "Deleting user " + userId + " (" + fullname + ", '" + username + "')";
                //check permission to delete.
                if ((access.canSuperAdmin()) || ((userCustomer == customerId) && (access.canCustomerAdmin()))) {
                    logger.portalDetail(callerId, "deleteUser", 0, "user", userId, logMessage, transconn);

                    stmt = transconn.prepareStatement(deleteUser);
                    stmt.setInt(1, userId);
                    stmt.executeUpdate();

                    stmt = transconn.prepareStatement(deleteMap);
                    stmt.setInt(1, userId);
                    stmt.executeUpdate();

                    stmt = transconn.prepareStatement(deleteTextAlerts);
                    stmt.setInt(1, userId);
                    stmt.executeUpdate();

                    stmt = transconn.prepareStatement(deleteNewEmailReports);
                    stmt.setInt(1, userId);
                    stmt.executeUpdate();

                    stmt = transconn.prepareStatement(deleteEmailTimeTable);
                    stmt.setInt(1, userId);
                    stmt.executeUpdate();

                    int group = HandlerUtils.getOptionalInteger(toHandle, "group");
                    if (group > 0)
                    {
                        deleteBevSyncUser(userId);
                    }

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

    private void deleteBevSyncUser(int userId) throws HandlerException 
    {
        String select = "SELECT id, region FROM userRegionMap WHERE user=?";

        String deleteRegion = "DELETE FROM region WHERE id=?";
        String deleteRegionProductSet = "DELETE FROM regionProductSet WHERE region=?";
        String deleteRegionExclusionMap = "DELETE FROM regionExclusionMap WHERE region=?";
        String deleteUserRegionMap = "DELETE FROM userRegionMap WHERE id=?";

        PreparedStatement stmt = null;
        ResultSet rs = null;
        
        try {
            stmt = transconn.prepareStatement(select);
            stmt.setInt(1, userId);
            rs = stmt.executeQuery();
            while (rs.next()) {
                int region  = rs.getInt(2);

                stmt = transconn.prepareStatement(deleteRegion);
                stmt.setInt(1, region);
                stmt.executeUpdate();

                stmt = transconn.prepareStatement(deleteRegionProductSet);
                stmt.setInt(1, region);
                stmt.executeUpdate();

                stmt = transconn.prepareStatement(deleteRegionExclusionMap);
                stmt.setInt(1, region);
                stmt.executeUpdate();

                stmt = transconn.prepareStatement(deleteUserRegionMap);
                stmt.setInt(1, rs.getInt(1));
                stmt.executeUpdate();
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error in deleteBevSyncUser: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
            close(rs);
        }

    }

    /**  Demote a customer-admin to a map-level user with manager access at all locations
     *  The caller must pass its customer id and permission level.  CustomerIds
     *  must the the caller must have P:3 or better permissions.
     */
    private void demoteAdmin(Element toHandle, Element toAppend) throws HandlerException {
        int targetUser = HandlerUtils.getRequiredInteger(toHandle, "userId");
        int customerId = HandlerUtils.getRequiredInteger(toHandle, "customer");
        WebPermission access = new WebPermission(HandlerUtils.getRequiredInteger(toHandle, "access"));
        int callerId = getCallerId(toHandle);

        String select = "SELECT customer,name,isManager FROM user WHERE id=?";
        String updateUser = "UPDATE user SET isManager=0 WHERE id=?";
        String insertMap = "INSERT INTO userMap (user,securityLevel,location) " +
                " SELECT ?,?,id FROM location WHERE customer=?";

        PreparedStatement stmt = null;
        ResultSet rs = null;

        try {
            stmt = transconn.prepareStatement(select);
            stmt.setInt(1, targetUser);
            rs = stmt.executeQuery();
            if (rs.next()) {
                int userCustomer = rs.getInt(1);
                String fullname = rs.getString(2);
                int isManager = rs.getInt(3);

                String logMessage = "Demoting admin user " + targetUser + " (" + fullname + ")";
                //check permission to delete.
                if ((access.canSuperAdmin()) || ((userCustomer == customerId) && (access.canCustomerAdmin()))) {
                    logger.portalDetail(callerId, "demoteAdmin", 0, "user", targetUser, logMessage, transconn);

                    //change to map-level access
                    stmt = transconn.prepareStatement(updateUser);
                    stmt.setInt(1, targetUser);
                    stmt.executeUpdate();

                    //add manager access for all locations
                    stmt = transconn.prepareStatement(insertMap);
                    stmt.setInt(1, targetUser);
                    stmt.setInt(2, WebPermission.instanceOfManager().getLevel());
                    stmt.setInt(3, userCustomer);
                    stmt.executeUpdate();
                } else {
                    logger.portalAccessViolation("Demote admin user failed by U#" + callerId + " C#" + customerId + " uC#" + userCustomer + " P#" + access + ": " + logMessage);
                    addErrorDetail(toAppend, "Unable to demote that user.");
                }
            } else {
                logger.debug("Unknown user: " + targetUser);
                addErrorDetail(toAppend, "Unable to demote that user.");
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error in demoteAdmin " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(rs);
            close(stmt);
        }
    }

    /** Promote a user to Customer-Administrator.  This will wipe out his userMap
     *
     *  The caller must pass its customer id and permission level.  CustomerIds
     *  must the the caller must have P:3 or better permissions.
     */
    private void promoteUser(Element toHandle, Element toAppend) throws HandlerException {

        int targetUser = HandlerUtils.getRequiredInteger(toHandle, "userId");
        int customerId = HandlerUtils.getRequiredInteger(toHandle, "customer");
        WebPermission access = new WebPermission(HandlerUtils.getRequiredInteger(toHandle, "access"));
        int callerId = getCallerId(toHandle);

        String select = "SELECT customer,name FROM user WHERE id =? ";
        String updateUser = "UPDATE user SET isManager=1 WHERE id=?";
        String deleteMap = "DELETE FROM userMap WHERE user=?";

        PreparedStatement stmt = null;
        ResultSet rs = null;

        try {
            stmt = transconn.prepareStatement(select);
            stmt.setInt(1, targetUser);
            rs = stmt.executeQuery();
            if (rs.next()) {
                int userCustomer = rs.getInt(1);
                String fullname = rs.getString(2);

                String logMessage = "Promoting user " + targetUser + " (" + fullname + ")";
                //check permission to delete.
                if ((access.canSuperAdmin()) || ((userCustomer == customerId) && (access.canCustomerAdmin()))) {
                    logger.portalDetail(callerId, "promoteUser", 0, "user", targetUser, logMessage, transconn);

                    stmt = transconn.prepareStatement(updateUser);
                    stmt.setInt(1, targetUser);
                    stmt.executeUpdate();

                    stmt = transconn.prepareStatement(deleteMap);
                    stmt.setInt(1, targetUser);
                    stmt.executeUpdate();
                } else {
                    logger.portalAccessViolation("Promote user failed by U#" + callerId + " C#" + customerId + " uC#" + userCustomer + " P#" + access + ": " + logMessage);
                    addErrorDetail(toAppend, "Unable to promote that user.");
                }
            } else {
                logger.debug("Unknown user: " + targetUser);
                addErrorDetail(toAppend, "Unable to promote that user.");
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error in promoteUser " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(rs);
            close(stmt);
        }
    }

    /**  Move a customer-admin from one customer to another.
     *
     *  This method is intended to be called only by super-admins, but this
     *  requirement isn't explictly checked here.
     *
     */
    private void moveAdmin(Element toHandle, Element toAppend) throws HandlerException {
        int targetUser = HandlerUtils.getRequiredInteger(toHandle, "userId");
        int customer = HandlerUtils.getRequiredInteger(toHandle, "customerId");
        int callerId = getCallerId(toHandle);

        String moveAdmin = "UPDATE user SET customer=? WHERE id=? AND isManager=1";

        PreparedStatement stmt = null;

        try {
            stmt = transconn.prepareStatement(moveAdmin);
            stmt.setInt(1, customer);
            stmt.setInt(2, targetUser);
            stmt.executeUpdate();

            logger.portalDetail(callerId, "moveUser", 0, "user", targetUser, "Moving user " + targetUser + " to Customer " + customer, transconn);
        } catch (SQLException sqle) {
            logger.dbError("Database error in moveUser " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
        }

    }

    /** Move a user from one customer to a different customer.
     *  This will wipe out their userMap and give them read-only access at
     *  one of the new customer's location.  (Chosen by lowest LocId).
     *
     *  This method is intended to be called only by super-admins (P:1), but
     *  this requirement isn't explicitly checked here.
     */
    private void moveUser(Element toHandle, Element toAppend) throws HandlerException {

        int targetUser = HandlerUtils.getRequiredInteger(toHandle, "userId");
        int customer = HandlerUtils.getRequiredInteger(toHandle, "customer");
        int callerId = getCallerId(toHandle);

        String updateUser = "UPDATE user SET customer=? WHERE id=?";
        String deleteMap = "DELETE FROM userMap WHERE user=?";
        String selectLocation = "SELECT id FROM location WHERE customer=? " +
                " ORDER BY id ASC LIMIT 1 ";
        String insertMap = "INSERT INTO userMap (user,location,securityLevel)" +
                " VALUES (?,?,?) ";

        PreparedStatement stmt = null;
        ResultSet rs = null;

        try {
            //make sure the user and the new customer exist
            assertForeignKey("user", targetUser, transconn);
            assertForeignKey("customer", customer, transconn);

            logger.portalDetail(callerId, "moveUser", 0, "user", targetUser, "Moving user " + targetUser + " to Customer " + customer, transconn);

            //change the user's customer
            stmt = transconn.prepareStatement(updateUser);
            stmt.setInt(1, customer);
            stmt.setInt(2, targetUser);
            stmt.executeUpdate();

            //delete the old userMap
            stmt = transconn.prepareStatement(deleteMap);
            stmt.setInt(1, targetUser);
            stmt.executeUpdate();

            //attempt to give the user read-only access at a location
            stmt = transconn.prepareStatement(selectLocation);
            stmt.setInt(1, customer);
            rs = stmt.executeQuery();
            if (rs.next()) {
                int location = rs.getInt(1);
                logger.portalAction("Adding read-only access at Loc " + location);
                stmt = transconn.prepareStatement(insertMap);
                stmt.setInt(1, targetUser);
                stmt.setInt(2, location);
                stmt.setInt(3, WebPermission.instanceOfReadOnly().getLevel());
                stmt.executeUpdate();

            } else {
                addErrorDetail(toAppend, "Moved OK, but no new permissions set.");
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error in moveUser " + sqle.getMessage());
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
    private void updateUserPermissions(Element toHandle, Element toAppend) throws HandlerException {

        int location                        = HandlerUtils.getOptionalInteger(toHandle, "locationId");
        int user                            = HandlerUtils.getOptionalInteger(toHandle, "userId");
        int callerId                        = getCallerId(toHandle);

        if (location < 0 && user < 0) {
            throw new HandlerException("Either locationId or userId must be set.");
        }

        if (location > 0) {
            Iterator users                  = toHandle.elementIterator("user");
            while (users.hasNext()) {
                Element userEl              = (Element) users.next();
                int userId                  = HandlerUtils.getRequiredInteger(userEl, "userId");
                int beerboard               = HandlerUtils.getRequiredInteger(userEl, "beerboard");
                WebPermission permission    = new WebPermission(HandlerUtils.getRequiredInteger(userEl, "permission"));
                updateUserPermissionsInner(userId, location, callerId, beerboard, permission, toAppend);
            }
        }
        if (user > 0) {
            Iterator locations = toHandle.elementIterator("location");
            while (locations.hasNext()) {
                Element locationEl = (Element) locations.next();
                int locationId = HandlerUtils.getRequiredInteger(locationEl, "locationId");
                WebPermission permission = new WebPermission(HandlerUtils.getRequiredInteger(locationEl, "permission"));
                updateUserPermissionsInner(user, locationId, callerId, 0, permission, toAppend);
            }
        }

    }

    private void createBeerBoardAccess(int location) throws HandlerException {

        String select                       = "SELECT id FROM locationBeerBoardMap WHERE location=? ";
        String selectLocationDetails        = "SELECT id FROM locationDetails WHERE beerboard=1 AND location=? ";
        String insertMap                    = "INSERT INTO locationBeerBoardMap (location) VALUES (?) ";
        String insertGraphics               = "INSERT INTO locationGraphics (location) VALUES (?) ";
        String update                       = "UPDATE locationDetails SET beerboard=1 WHERE location=?";
        String insertUserAccess             = "INSERT INTO userBeerBoardMap (user, location) (SELECT u.id, uM.location FROM `user` u LEFT JOIN userBeerBoardMap uBM ON uBM.user = u.id LEFT JOIN userMap uM ON uM.user = u.id WHERE u.platform = 5 AND uBM.id IS NULL AND uM.id IS NOT NULL);";
        PreparedStatement stmt              = null;
        ResultSet rs                        = null;

        try {
            stmt                            = transconn.prepareStatement(select);
            stmt.setInt(1, location);
            rs                              = stmt.executeQuery();
            if (!rs.next()) {
                stmt                        = transconn.prepareStatement(insertMap);
                stmt.setInt(1, location);
                stmt.executeUpdate();
            }
            
            stmt                            = transconn.prepareStatement(selectLocationDetails);
            stmt.setInt(1, location);
            rs                              = stmt.executeQuery();
            if (!rs.next()) {
                stmt                        = transconn.prepareStatement(insertGraphics);
                stmt.setInt(1, location);
                stmt.executeUpdate();
                
                stmt                        = transconn.prepareStatement(update);
                stmt.setInt(1, location);
                stmt.executeUpdate();
            }
                
            stmt                            = transconn.prepareStatement(insertUserAccess);
            stmt.executeUpdate();
            
        } catch (SQLException sqle) {
            logger.dbError("Database error in createBeerBoardAccess: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
        }
    }

    /**  Helper method for updateUserPermission.  Takes one user, location, and permission and performs the update
     */
    private void updateUserPermissionsInner(int user, int location, int callerId, int beerboard, WebPermission permission, Element toAppend) throws HandlerException {
        
        String checkUserLocationByCustomer  = "SELECT c.id FROM customer c LEFT JOIN location l ON l.customer = c.id " +
                                            " LEFT JOIN user u ON u.customer = c.id WHERE u.id = ? AND l.id = ? LIMIT 1";
        String checkUserLocationByGroup     = "SELECT c.groupId FROM customer c LEFT JOIN location l ON l.customer = c.id " +
                                            " LEFT JOIN user u ON u.groupId = c.groupId WHERE u.id = ? AND l.id = ? LIMIT 1";
        
        String selectMapRecords             = "SELECT id, securityLevel FROM userMap WHERE user=? AND location=? ";
        String updateBBTVPermissions        = "UPDATE user set platform=? WHERE id=? ";
        String updateMap                    = "UPDATE userMap set securityLevel=? WHERE id=? ";
        String deleteMap                    = "DELETE FROM userMap WHERE id=?";
        String selectemailReports           = "SELECT id FROM emailReport WHERE user = ? AND tableType = 2 AND tableId = ?";
        String deleteNewEmailReports        = "DELETE FROM emailReport WHERE id=?";
        String deleteEmailTimeTable         = "DELETE FROM emailTimeTable WHERE report=?";
        String deleteTextAlerts             = "DELETE FROM textAlert WHERE user=? AND tableType = 2 AND tableId = ?";

        String insertMap                    = " INSERT INTO userMap (user,location,securityLevel) VALUES (?,?,?) ";

        PreparedStatement stmt              = null;
        ResultSet rs                        = null, mapId = null;

        try {
            
            stmt                            = transconn.prepareStatement(updateBBTVPermissions);
            stmt.setInt(1, (beerboard == 1 ? 5 : 1));
            stmt.setInt(2, user);
            stmt.executeUpdate();
            
            if (beerboard == 1) { createBeerBoardAccess(location); }

            //check that this user belongs to the customer of the location
            boolean userAccess              = false;
            stmt                            = transconn.prepareStatement(checkUserLocationByCustomer);
            stmt.setInt(1, user);
            stmt.setInt(2, location);
            rs                              = stmt.executeQuery();
            if (rs.next()) {
                userAccess                  = true;
            } else {
                stmt                        = transconn.prepareStatement(checkUserLocationByGroup);
                stmt.setInt(1, user);
                stmt.setInt(2, location);
                rs                          = stmt.executeQuery();
                if (rs.next() && (rs.getInt(1) > 0)) {
                    userAccess              = true;
                }
            }
            
            if (userAccess) {
                //see how many userMap records exist for this user/location.
                //Hoping for 0 or 1.  >1 would be a problem, but we will fix it if so
                stmt                        = transconn.prepareStatement(selectMapRecords);
                stmt.setInt(1, user);
                stmt.setInt(2, location);
                mapId                       = stmt.executeQuery();

                if (!permission.canRead()) {
                    //we are just deleting any mappings
                    int delCount            = 0;
                    while (mapId.next()) {
                        stmt                = transconn.prepareStatement(deleteMap);
                        stmt.setInt(1, mapId.getInt(1));
                        stmt.executeUpdate();
                        delCount++;
                    }
                    if (delCount > 0) {
                        stmt                = transconn.prepareStatement(selectemailReports);
                        stmt.setInt(1, user);
                        stmt.setInt(2, location);
                        rs                  = stmt.executeQuery();
                        while (rs.next()) {
                            stmt            = transconn.prepareStatement(deleteNewEmailReports);
                            stmt.setInt(1, rs.getInt(1));
                            stmt.executeUpdate();

                            stmt            = transconn.prepareStatement(deleteEmailTimeTable);
                            stmt.setInt(1, rs.getInt(1));
                            stmt.executeUpdate();
                        }
                        stmt                = transconn.prepareStatement(deleteTextAlerts);
                        stmt.setInt(1, user);
                        stmt.setInt(2, location);
                        stmt.executeUpdate();
                    }
                    String logMessage       = "Removed access for U" + user + " at L" + location + " (" + delCount + ") mappings removed";

                    logger.portalDetail(callerId, "updateUserPermission", location, "user", user, logMessage, transconn);
                } else {
                    //we need to add or update the mapping
                    boolean updated         = false;
                    while (mapId.next()) {
                        if (!updated) {
                            //update the first (and hopefully only) mapping
                            if (mapId.getInt(2) == permission.getLevel()) {
                                logger.debug("No change needed for U" + user + " L" + location);
                            } else {
                                String logMessage
                                            = "Updated access for U#" + user + " to " + permission;
                                logger.debug(logMessage);
                                logger.portalDetail(callerId, "updateUserPermission", location, "user", user, logMessage, transconn);
                                stmt        = transconn.prepareStatement(updateMap);
                                stmt.setInt(1, permission.getLevel());
                                stmt.setInt(2, mapId.getInt(1));
                                stmt.executeUpdate();
                            }
                            updated         = true;
                        } else {
                            //delete any other mappings, these shouldn't be there
                            logger.generalWarning("removing EXTRA MAPPING for U" + user + " L" + location);
                            stmt            = transconn.prepareStatement(deleteMap);
                            stmt.setInt(1, mapId.getInt(1));
                            stmt.executeUpdate();
                        }
                    }
                    if (!updated) {
                        // No map record, so we need to add one
                        String logMessage   = "Adding permission for U#" + user + ": " + permission;
                        logger.debug("Adding new mapping for U" + user + " L" + location + " for P" + permission);
                        logger.portalDetail(callerId, "updateUserPermission", location, "user", user, logMessage, transconn);
                        stmt                = transconn.prepareStatement(insertMap);
                        stmt.setInt(1, user);
                        stmt.setInt(2, location);
                        stmt.setInt(3, permission.getLevel());
                        stmt.executeUpdate();
                    }
                }
            } else {
                //Location Cust and User Cust don't match
                logger.portalAccessViolation("Customer mismatch in updateUserPermissions for U" + user + " L" + location);
                Element er                  = toAppend.addElement("error");
                er.addElement("detail").addText("Couldn't update permissions for this user/location");
                er.addElement("userId").addText(String.valueOf(user));
                er.addElement("locationId").addText(String.valueOf(location));
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error in updateUserPermissions " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(rs);
            close(mapId);
            close(stmt);
        }
    }

    /**  Updates the fullname and/or username of a user.
     *  The caller id and caller customer must be passed, and is subject to the following permission rules
     *  1. A user may update his own information
     *  2. A customer-admin may update any of his users
     *  3. A super-admin may update anyone
     */
    private void updateUser(Element toHandle, Element toAppend) throws HandlerException {
        int targetUser = HandlerUtils.getRequiredInteger(toHandle, "userId");
        int platform = HandlerUtils.getRequiredInteger(toHandle, "platform");
        int callerId = getCallerId(toHandle);
        String fullnameToTry = HandlerUtils.getOptionalString(toHandle, "fullname");
        String usernameToTry = HandlerUtils.getOptionalString(toHandle, "username");
        String emailToTry = HandlerUtils.getOptionalString(toHandle, "email");
        String mobileToTry = HandlerUtils.getOptionalString(toHandle, "mobile");
        String carrierToTry = HandlerUtils.getOptionalString(toHandle, "carrier");
        String select = "SELECT customer,name,username,email,mobile,carrier,platform FROM user WHERE id=?";
        String updateUser = "UPDATE user SET username=?,name=?,email=?,mobile=?,carrier=? WHERE id=?";
        String checkUsername = "SELECT id FROM user WHERE username=? LIMIT 1";
        String checkTextAlerts = "SELECT id from textAlerts where user=? LIMIT 1";
        String updateTextAlerts = " UPDATE textAlerts SET mobile = ?,carrier = ? WHERE user = ?";

        PreparedStatement stmt = null;
        ResultSet rs = null;

        try {
            stmt = transconn.prepareStatement(select);
            stmt.setInt(1, targetUser);
            rs = stmt.executeQuery();
            if (rs.next()) {
                int userCustomer = rs.getInt(1);
                String fullname = rs.getString(2);
                String username = rs.getString(3);
                String email = rs.getString(4);
                String mobile = rs.getString(5);
                String carrier = rs.getString(6);
                String fullnameToUse = fullname;
                String usernameToUse = username;
                String emailToUse = email;
                String mobileToUse = mobile;
                String carrierToUse = carrierToTry;
                String logMessage;

                switch (platform) {
                    case 1:
                        int customerId = HandlerUtils.getRequiredInteger(toHandle, "customer");
                        int locationId = HandlerUtils.getRequiredInteger(toHandle, "locationId");
                        WebPermission access = new WebPermission(HandlerUtils.getRequiredInteger(toHandle, "access"));

                        if (usernameToTry != null && usernameToTry.length() > 3 && !usernameToTry.equals(username)) {
                            stmt = transconn.prepareStatement(checkUsername);
                            stmt.setString(1, usernameToTry);
                            rs = stmt.executeQuery();
                            if (!rs.next()) {
                                usernameToUse = usernameToTry;
                            } else {
                                addErrorDetail(toAppend, "The username '" + usernameToTry + "' already exists");
                            }
                        }
                        if (fullnameToTry != null && fullnameToTry.length() > 3) {
                            fullnameToUse = fullnameToTry;
                        }
                        if (emailToTry != null && emailToTry.length() > 3) {
                            emailToUse = emailToTry;
                        }
                        if (mobileToTry != null) {
                            mobileToUse = mobileToTry;
                        }


                        logMessage = "Updating " + username + " (" + fullname + ") to " + usernameToUse + " (" + fullnameToUse + ")";
                        //check permission to delete.
                        if ((access.canSuperAdmin()) || ((userCustomer == customerId) && (access.canWrite()))) {
                            logger.portalDetail(callerId, "updateTestUser", 0, "user", targetUser, logMessage, transconn);
                            stmt = transconn.prepareStatement(updateUser);
                            stmt.setString(1, usernameToUse);
                            stmt.setString(2, fullnameToUse);
                            stmt.setString(3, emailToUse);
                            stmt.setString(4, mobileToUse);
                            stmt.setString(5, carrierToUse);
                            stmt.setInt(6, targetUser);
                            stmt.executeUpdate();

                            stmt = transconn.prepareStatement(checkTextAlerts);
                            stmt.setInt(1, targetUser);
                            rs = stmt.executeQuery();
                            if (rs.last()) {
                                //user = ?,location = ?,customer = ?,mobile = ?,carrier = ? WHERE user = ? and location = ?";
                                logger.portalDetail(callerId, "updateTestUser", 0, "user", targetUser, logMessage, transconn);
                                stmt = transconn.prepareStatement(updateTextAlerts);
                                stmt.setString(1, mobileToUse);
                                stmt.setString(2, carrierToUse);
                                stmt.setInt(3, targetUser);
                                stmt.executeUpdate();
                            }
                        } else {
                            //permission problem
                            logger.portalAccessViolation("Update user failed by U#" + callerId + " C#" + customerId + " uC#" + userCustomer + " P#" + access + ": " + logMessage);
                            addErrorDetail(toAppend, "Unable to update that user.");
                        }
                    case 2:
                        if (usernameToTry != null && usernameToTry.length() > 3 && !usernameToTry.equals(username)) {
                            stmt = transconn.prepareStatement(checkUsername);
                            stmt.setString(1, usernameToTry);
                            rs = stmt.executeQuery();
                            if (!rs.next()) {
                                usernameToUse = usernameToTry;
                            } else {
                                addErrorDetail(toAppend, "The username '" + usernameToTry + "' already exists");
                            }
                        }
                        if (fullnameToTry != null && fullnameToTry.length() > 3) {
                            fullnameToUse = fullnameToTry;
                        }
                        if (emailToTry != null && emailToTry.length() > 3) {
                            emailToUse = emailToTry;
                        }
                        if (mobileToTry != null) {
                            mobileToUse = mobileToTry;
                        }

                        logMessage = "Updating " + username + " (" + fullname + ") to " + usernameToUse + " (" + fullnameToUse + ")";

                        //check permission to delete.
                        logger.portalDetail(callerId, "updateTestUser", 0, "user", targetUser, logMessage, transconn);
                        stmt = transconn.prepareStatement(updateUser);
                        stmt.setString(1, usernameToUse);
                        stmt.setString(2, fullnameToUse);
                        stmt.setString(3, emailToUse);
                        stmt.setString(4, mobileToUse);
                        stmt.setString(5, carrierToUse);
                        stmt.setInt(6, targetUser);
                        stmt.executeUpdate();

                        stmt = transconn.prepareStatement(checkTextAlerts);
                        stmt.setInt(1, targetUser);
                        rs = stmt.executeQuery();
                        if (rs.last()) {
                            //user = ?,location = ?,customer = ?,mobile = ?,carrier = ? WHERE user = ? and location = ?";
                            logger.portalDetail(callerId, "updateUser", 0, "user", targetUser, logMessage, transconn);
                            stmt = transconn.prepareStatement(updateTextAlerts);
                            stmt.setString(1, mobileToUse);
                            stmt.setString(2, carrierToUse);
                            stmt.setInt(3, targetUser);
                            stmt.executeUpdate();
                        }
                    default:
                        break;
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

     private void updateUserDashboardExclusion(Element toHandle, Element toAppend) throws HandlerException {

        int user = HandlerUtils.getRequiredInteger(toHandle, "userId");

        String deleteUserDashboardExclusionMap = " DELETE FROM userDashboardExclusionMap WHERE user = ? ";
        String insertUserDashboardExclusionMap = " INSERT INTO userDashboardExclusionMap (dashboardComponent, user) VALUES (?,?) ";

        PreparedStatement stmt = null;
        try {
            stmt = transconn.prepareStatement(deleteUserDashboardExclusionMap);
            stmt.setInt(1, user);
            stmt.executeUpdate();
            Iterator j = toHandle.elementIterator("dcId");
            while (j.hasNext()) {
                Element dashboardComponent = (Element) j.next();
                
                logger.debug("Adding dashboard componenet: " + dashboardComponent.getText());
                stmt = transconn.prepareStatement(insertUserDashboardExclusionMap);
                stmt.setInt(1, Integer.valueOf(dashboardComponent.getText()));
                stmt.setInt(2, user);
                stmt.executeUpdate();
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error in deleteUserDashboardExclusion: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
        }
    }

    private void changeHomePreference(Element toHandle, Element toAppend) throws HandlerException {

        int userId = HandlerUtils.getRequiredInteger(toHandle, "userId");
        int home = HandlerUtils.getRequiredInteger(toHandle, "home");
        String insert = "INSERT INTO userPreferenceMap (home, user) VALUES (?,?) ";
        String update = "UPDATE userPreferenceMap SET home=? WHERE id=?";
        String select = "SELECT id FROM userPreferenceMap WHERE user=? ";
        PreparedStatement stmt = null;
        ResultSet rs = null;

        try {
            stmt = transconn.prepareStatement(select);
            stmt.setInt(1, userId);
            rs = stmt.executeQuery();
            if (rs.next()) {
                stmt = transconn.prepareStatement(update);
                stmt.setInt(1, home);
                stmt.setInt(2, rs.getInt(1));
                stmt.executeUpdate();
                logger.portalDetail(userId, "changeHomePreference", 0, "user", userId, "Updated Home Preference", transconn);
            } else {
                stmt = transconn.prepareStatement(insert);
                stmt.setInt(1, home);
                stmt.setInt(2, userId);
                stmt.executeUpdate();
                logger.portalDetail(userId, "changeHomePreference", 0, "user", userId, "Added Home Preference", transconn);
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error in changeHomePreference: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
        }
    }

    private void changeUnitPreference(Element toHandle, Element toAppend) throws HandlerException {

        int userId = HandlerUtils.getRequiredInteger(toHandle, "userId");
        int unitId = HandlerUtils.getRequiredInteger(toHandle, "unitId");
        String update = "UPDATE user SET unit=? WHERE id=?";
        String select = "SELECT id FROM user WHERE id=? ";
        PreparedStatement stmt = null;
        ResultSet rs = null;

        try {
            stmt = transconn.prepareStatement(select);
            stmt.setInt(1, userId);
            rs = stmt.executeQuery();
            if (rs.next()) {
                stmt = transconn.prepareStatement(update);
                stmt.setInt(1, unitId);
                stmt.setInt(2, userId);
                stmt.executeUpdate();
                logger.portalDetail(userId, "changeUnitPreference", 0, "user", userId, "Changed Unit Preference", transconn);
            } else {
                logger.portalAction("changeUnitPreference failed for U#" + userId + ", user not found");
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error in changeUnitPreference: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
        }
    }

    private void changePassword(Element toHandle, Element toAppend) throws HandlerException {

        String oldPass = HandlerUtils.getRequiredString(toHandle, "oldPassword");
        String newPass = HandlerUtils.getRequiredString(toHandle, "newPassword");
        int userId = HandlerUtils.getRequiredInteger(toHandle, "userId");

        String update = "UPDATE user SET password=? WHERE id=? AND password=?";
        String select = "SELECT id FROM user WHERE id=? AND password=? LIMIT 1";

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

    /** Update a locations customer, name, and address
     *  <locationId> 9999       # The location to update
     *  <customerId> 333        # The new customer id to associate with
     *  <locationName> String
     *  <addrStreet>
     *  <addrCity>
     *  <addrState>
     *  <addrZip>
     */
    private void updateLocation(Element toHandle, Element toAppend) throws HandlerException {

        int locationId;
        int customerId = HandlerUtils.getRequiredInteger(toHandle, "customerId");
        String locationName;
        String addrCity;
        String addrState;
        String addrZip;
        String addrStreet;
        String easternOffset;
        double latitude;
        double longitude;
        //NischaySharma_02-Oct-2009_Start
        boolean isMultipleUpdate = HandlerUtils.getOptionalBoolean(toHandle, "isMultipleUpdate");
        //NischaySharma_02-Oct-2009_End

        //NischaySharma_09-Oct-2009_Start
        boolean isPlotCenter = HandlerUtils.getOptionalBoolean(toHandle, "isPlotCenter");
        int zoomLevel;
        //NischaySharma_09-Oct-2009_End

        int callerId = getCallerId(toHandle);

        String update = "UPDATE location SET name=?,customer=?,addrStreet=?,addrCity=?,addrState=?,addrZip=?,easternOffset=?,latitude=?,longitude=? WHERE id=?";
        String updateCoordIndex = "UPDATE location l LEFT JOIN state s ON s.USPSST = l.addrState LEFT JOIN zipList z ON z.zip = l.addrZip " +
                " LEFT JOIN county c ON c.state = s.FIPSST AND z.county = c.county " +
                " SET l.zipIndex = z.id, l.countyIndex = c.id, l.stateIndex = s.id WHERE l.id = ? ";
        String updatePlotCenter = " UPDATE location SET zoomLevel = ?, latitude = ?, longitude = ? WHERE id = ? ";
        PreparedStatement stmt = null;

        try {

            if (checkForeignKey("customer", customerId, transconn)) {
                int colCount = 1;
                //NischaySharma_09-Oct-2009_Start
                if (isPlotCenter) {
                    stmt = transconn.prepareStatement(updatePlotCenter);
                    locationId = HandlerUtils.getRequiredInteger(toHandle, "locationId");
                    latitude = HandlerUtils.getRequiredDouble(toHandle, "locationLat");
                    longitude = HandlerUtils.getRequiredDouble(toHandle, "locationLong");
                    zoomLevel = HandlerUtils.getRequiredInteger(toHandle, "zoomLevel");
                    stmt.setInt(colCount++, zoomLevel);
                    stmt.setDouble(colCount++, latitude);
                    stmt.setDouble(colCount++, longitude);
                    stmt.setInt(colCount++, locationId);
                    stmt.executeUpdate();
                    String logMessage = "Updated location to '" + latitude + "' C#" + longitude + ", " +
                            zoomLevel;
                    logger.portalDetail(callerId, "updateLocation", locationId, "location", locationId, logMessage, transconn);
                    return;
                }
                //NischaySharma_09-Oct-2009_End
                //NischaySharma_02-Oct-2009_Start
                if (!isMultipleUpdate) {
                    stmt = transconn.prepareStatement(update);
                    locationId = HandlerUtils.getRequiredInteger(toHandle, "locationId");
                    locationName = HandlerUtils.getRequiredString(toHandle, "locationName");
                    addrCity = HandlerUtils.getRequiredString(toHandle, "addrCity");
                    addrState = HandlerUtils.getRequiredString(toHandle, "addrState");
                    addrZip = HandlerUtils.getRequiredString(toHandle, "addrZip");
                    addrStreet = HandlerUtils.getRequiredString(toHandle, "addrStreet");
                    easternOffset = HandlerUtils.getRequiredString(toHandle, "easternOffset");
                    latitude = HandlerUtils.getRequiredDouble(toHandle, "locationLat");
                    longitude = HandlerUtils.getRequiredDouble(toHandle, "locationLong");                    
                    stmt.setString(colCount++, locationName);
                    stmt.setInt(colCount++, customerId);
                    stmt.setString(colCount++, addrStreet);
                    stmt.setString(colCount++, addrCity);
                    stmt.setString(colCount++, addrState);
                    stmt.setString(colCount++, addrZip);
                    stmt.setString(colCount++, easternOffset);
                    stmt.setDouble(colCount++, latitude);
                    stmt.setDouble(colCount++, longitude);                    
                    stmt.setInt(colCount++, locationId);
                    stmt.executeUpdate();

                    stmt = transconn.prepareStatement(updateCoordIndex);
                    stmt.setInt(1, locationId);
                    stmt.executeUpdate();

                    String logMessage = "Updated location to '" + locationName + "' C#" + customerId + ", " +
                            addrStreet + "; " + addrCity + ", " + addrState + " " + addrZip;
                    logger.portalDetail(callerId, "updateLocation", locationId, "location", locationId, logMessage, transconn);
                } else {
                    stmt = transconn.prepareStatement(update);
                    Iterator locations = toHandle.elementIterator("location");
                    while (locations.hasNext()) {
                        Element location = (Element) locations.next();
                        colCount = 1;
                        locationId = HandlerUtils.getRequiredInteger(location, "locationId");
                        locationName = HandlerUtils.getRequiredString(location, "locationName");
                        addrCity = HandlerUtils.getRequiredString(location, "addrCity");
                        addrState = HandlerUtils.getRequiredString(location, "addrState");
                        addrZip = HandlerUtils.getRequiredString(location, "addrZip");
                        addrStreet = HandlerUtils.getRequiredString(location, "addrStreet");
                        easternOffset = HandlerUtils.getRequiredString(location, "easternOffset");
                        latitude = HandlerUtils.getRequiredDouble(location, "locationLat");
                        longitude = HandlerUtils.getRequiredDouble(location, "locationLong");                        
                        stmt = transconn.prepareStatement(update);
                        stmt.setString(colCount++, locationName);
                        stmt.setInt(colCount++, customerId);
                        stmt.setString(colCount++, addrStreet);
                        stmt.setString(colCount++, addrCity);
                        stmt.setString(colCount++, addrState);
                        stmt.setString(colCount++, addrZip);
                        stmt.setString(colCount++, easternOffset);
                        stmt.setDouble(colCount++, latitude);
                        stmt.setDouble(colCount++, longitude);                        
                        stmt.setInt(colCount++, locationId);
                        stmt.executeUpdate();

                        stmt = transconn.prepareStatement(updateCoordIndex);
                        stmt.setInt(1, locationId);
                        stmt.executeUpdate();

                        String logMessage = "Updated location to '" + locationName + "' C#" + customerId + ", " +
                                addrStreet + "; " + addrCity + ", " + addrState + " " + addrZip;
                        logger.portalDetail(callerId, "updateLocation", locationId, "location", locationId, logMessage, transconn);
                    }
                }
                //NischaySharma_02-Oct-2009_End
            } else {
                addErrorDetail(toAppend, "Invalid customer ID: " + customerId);
            }

        } catch (SQLException sqle) {
            logger.dbError("Database error in updateLocation: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
        }
    }

    
    private void updateLocationDetails(Element toHandle, Element toAppend) throws HandlerException {

        int callerId                        = getCallerId(toHandle);
        String update                       = "UPDATE locationDetails SET active=?,pouredUp=?,soldUp=?, beerboard=? WHERE location = ?";
        PreparedStatement stmt              = null;

        try {
            Iterator locationIterator       = toHandle.elementIterator("locations");
            while (locationIterator.hasNext()) {
                Element location            = (Element) locationIterator.next();
                int locationId              = HandlerUtils.getRequiredInteger(location, "locationId");
                if (checkForeignKey("location", locationId, transconn)) {
                    int colCount            = 1;
                    int active              = HandlerUtils.getRequiredInteger(location, "active");
                    int pouredUp            = HandlerUtils.getRequiredInteger(location, "pouredUp");
                    int soldUp              = HandlerUtils.getRequiredInteger(location, "soldUp");
                    int bbtv                = HandlerUtils.getRequiredInteger(location, "suspended");
                    //int data                = HandlerUtils.getRequiredInteger(location, "data");
                    //int suspended           = HandlerUtils.getRequiredInteger(location, "suspended");
                    stmt = transconn.prepareStatement(update);
                    stmt.setInt(colCount++, active);
                    stmt.setInt(colCount++, pouredUp);
                    stmt.setInt(colCount++, soldUp);
                    stmt.setInt(colCount++, bbtv);
                   // stmt.setInt(colCount++, data);
                   // stmt.setInt(colCount++, suspended);
                    stmt.setInt(colCount++, locationId);
                    stmt.executeUpdate();
                } else {
                    addErrorDetail(toAppend, "Not a valid Location #" + locationId);
                }
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error in updateLocationDetails: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
        }
    }

    /** Update the name of an existing customer.  If there were more editable customer fields,
     *  this method would support them.
     *
     *  <customerId> 999        # id to change
     *  <customerName> String   # The new name
     */
    private void updateEventDates(Element toHandle, Element toAppend) throws HandlerException {

        int locationId = HandlerUtils.getRequiredInteger(toHandle, "locationId");
        int eventId = HandlerUtils.getOptionalInteger(toHandle, "eventId");
        String eventDate = HandlerUtils.getRequiredString(toHandle, "eventDate");
        String eventPreOpenTime = HandlerUtils.getRequiredString(toHandle, "eventPreOpenTime");
        String eventStartTime = HandlerUtils.getRequiredString(toHandle, "eventStartTime");
        String eventEndTime = HandlerUtils.getRequiredString(toHandle, "eventEndTime");
        String eventDesc = HandlerUtils.getRequiredString(toHandle, "eventDesc");
        String eventPourStartTime = eventDate;
        String eventAfterHoursEndTime = eventDate;
        int callerId = getCallerId(toHandle);
        
        int startMinutes = 7 * 60;
        int endMinutes = 31 * 60;

        String selectEvent = "SELECT id, preOpen, eventEnd FROM eventHours WHERE location = ? AND date = ?";

        String updateExistingEventStart = "UPDATE eventHours SET eventPourStart=(? - INTERVAL 5 MINUTE) WHERE id=?";

        String updateExistingEventEnd = "UPDATE eventHours SET eventAfterHoursEnd=(? - INTERVAL 10 MINUTE) WHERE id=?";

        String updateEvent = "UPDATE eventHours SET date=?, eventPourStart=(? + INTERVAL ? MINUTE), preOpen=?, eventStart=?, eventEnd=?, eventAfterHoursEnd=(? + INTERVAL ? MINUTE), eventDesc=? WHERE id=?";

        String insertEvent = "INSERT INTO eventHours (location, date, eventPourStart, preOpen, eventStart, eventEnd, eventAfterHoursEnd, eventDesc) VALUES " +
                " (?,?,(? + INTERVAL ? MINUTE),?,?,?,(? + INTERVAL ? MINUTE),?);";

        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            int colCount = 1;

            stmt = transconn.prepareStatement(selectEvent);
            stmt.setInt(1, locationId);
            stmt.setString(2, eventDate);
            rs = stmt.executeQuery();
            if (rs.next()) {
                int existEventId = rs.getInt(1);
                java.util.Date existStart = rs.getTimestamp(2);
                java.util.Date existEnd = rs.getTimestamp(3);
                int param = 0;

                SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                
            try {
                    java.util.Date validatedStartDate = dateFormat.parse(eventPreOpenTime);
                    java.util.Date validatedEndDate = dateFormat.parse(eventEndTime);
                    String tempTime = "";

                    if (validatedStartDate.after(existEnd)) {
                        param++;
                        tempTime = eventPreOpenTime;
                        stmt = transconn.prepareStatement(updateExistingEventEnd);
                        eventPourStartTime = eventPreOpenTime;
                        startMinutes = -5;
                    }
                    if (validatedEndDate.before(existStart)) {
                        param++;
                        tempTime = rs.getString(2);
                        endMinutes = -10;
                        eventAfterHoursEndTime = rs.getString(2);
                        stmt = transconn.prepareStatement(updateExistingEventStart);
                    }

                    if (param == 1) {
                        stmt.setString(1, tempTime);
                        stmt.setInt(2, existEventId);
                        stmt.executeUpdate();
                    } else {
                        throw new HandlerException("Another game already exists in the same time frame ");
                    }
                } catch (Exception e) { }

            }

            if (eventId > 0) {

                stmt = transconn.prepareStatement(updateEvent);
                stmt.setString(colCount++, eventDate);
                stmt.setString(colCount++, eventPourStartTime);
                stmt.setInt(colCount++, startMinutes);
                stmt.setString(colCount++, eventPreOpenTime);
                stmt.setString(colCount++, eventStartTime);
                stmt.setString(colCount++, eventEndTime);
                stmt.setString(colCount++, eventAfterHoursEndTime);
                stmt.setInt(colCount++, endMinutes);
                stmt.setString(colCount++, eventDesc);
                stmt.setInt(colCount++, eventId);
                stmt.executeUpdate();

                String logMessage = "Updated Event for '" + eventDate + "'";
                logger.portalDetail(callerId, "updateEventDates", 0, "location", locationId, logMessage, transconn);

            } else {

                stmt = transconn.prepareStatement(insertEvent);
                stmt.setInt(colCount++, locationId);
                stmt.setString(colCount++, eventDate);
                stmt.setString(colCount++, eventPourStartTime);
                stmt.setInt(colCount++, startMinutes);
                stmt.setString(colCount++, eventPreOpenTime);
                stmt.setString(colCount++, eventStartTime);
                stmt.setString(colCount++, eventEndTime);
                stmt.setString(colCount++, eventAfterHoursEndTime);
                stmt.setInt(colCount++, endMinutes);
                stmt.setString(colCount++, eventDesc);
                stmt.executeUpdate();

                String logMessage = "Inserted Event for '" + eventDate + "'";
                logger.portalDetail(callerId, "updateEventDates", 0, "location", locationId, logMessage, transconn);

            }


        } catch (SQLException sqle) {
            logger.dbError("Database error in updateEventDates: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
            close(rs);
        }
    }
    
     private void updateSpecialsEventDates(Element toHandle, Element toAppend) throws HandlerException {

        int locationId = HandlerUtils.getRequiredInteger(toHandle, "locationId");
        String barString = HandlerUtils.getRequiredString(toHandle, "barString");
        String eventDate = HandlerUtils.getRequiredString(toHandle, "eventDate");
        String eventPreOpenTime = HandlerUtils.getRequiredString(toHandle, "eventPreOpenTime");
        String eventStartTime = HandlerUtils.getRequiredString(toHandle, "eventStartTime");
        String eventEndTime = HandlerUtils.getRequiredString(toHandle, "eventEndTime");
        String eventDesc = HandlerUtils.getRequiredString(toHandle, "eventDesc");
        String eventPourStartTime = eventDate;
        String eventAfterHoursEndTime = eventDate;
        int callerId = getCallerId(toHandle);
        
        int startMinutes = 7 * 60;
        int endMinutes = 31 * 60;

        String selectEvent = "SELECT id, preOpen, eventEnd FROM eventSpecialHours WHERE location = ? AND date = ?";

        String updateEvent = "UPDATE eventSpecialHours SET barString = ?, date=?, eventPourStart=(? + INTERVAL ? MINUTE), preOpen=?, eventStart=?, eventEnd=?, eventAfterHoursEnd=(? + INTERVAL ? MINUTE), eventDesc=? WHERE id=?";

        String insertEvent = "INSERT INTO eventSpecialHours (location, barString, date, eventPourStart, preOpen, eventStart, eventEnd, eventAfterHoursEnd, eventDesc) VALUES " +
                " (?,?,?,(? + INTERVAL ? MINUTE),?,?,?,(? + INTERVAL ? MINUTE),?);";

        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            int colCount = 1;

            stmt = transconn.prepareStatement(selectEvent);
            stmt.setInt(1, locationId);
            stmt.setString(2, eventDate);
            rs = stmt.executeQuery();
            if (rs.next()) {
                int existEventId = rs.getInt(1);
                
            
            if (existEventId > 0) {

                stmt = transconn.prepareStatement(updateEvent);
                stmt.setString(colCount++, barString);
                stmt.setString(colCount++, eventDate);
                stmt.setString(colCount++, eventPourStartTime);
                stmt.setInt(colCount++, startMinutes);
                stmt.setString(colCount++, eventPreOpenTime);
                stmt.setString(colCount++, eventStartTime);
                stmt.setString(colCount++, eventEndTime);
                stmt.setString(colCount++, eventAfterHoursEndTime);
                stmt.setInt(colCount++, endMinutes);
                stmt.setString(colCount++, eventDesc);
                stmt.setInt(colCount++, existEventId);
                stmt.executeUpdate();

                String logMessage = "Updated Event for '" + eventDate + "'";
                logger.portalDetail(callerId, "updateSpecialsEventDates", locationId, "location", locationId, logMessage, transconn);
            }

            } else {

                stmt = transconn.prepareStatement(insertEvent);
                stmt.setInt(colCount++, locationId);
                stmt.setString(colCount++, barString);
                stmt.setString(colCount++, eventDate);
                stmt.setString(colCount++, eventPourStartTime);
                stmt.setInt(colCount++, startMinutes);
                stmt.setString(colCount++, eventPreOpenTime);
                stmt.setString(colCount++, eventStartTime);
                stmt.setString(colCount++, eventEndTime);
                stmt.setString(colCount++, eventAfterHoursEndTime);
                stmt.setInt(colCount++, endMinutes);
                stmt.setString(colCount++, eventDesc);
                stmt.executeUpdate();

                String logMessage = "Inserted Event for '" + eventDate + "'";
                logger.portalDetail(callerId, "updateSpecialsEventDates", locationId, "location", locationId, logMessage, transconn);

            }


        } catch (SQLException sqle) {
            logger.dbError("Database error in updateEventDates: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
            close(rs);
        }
    }

     
     private void updateCateredEventDates(Element toHandle, Element toAppend) throws HandlerException {

        int locationId = HandlerUtils.getRequiredInteger(toHandle, "locationId");        
        String barString = HandlerUtils.getRequiredString(toHandle, "barString");
        String eventDate = HandlerUtils.getRequiredString(toHandle, "eventDate");
        String eventPreOpenTime = HandlerUtils.getRequiredString(toHandle, "eventPreOpenTime");
        String eventStartTime = HandlerUtils.getRequiredString(toHandle, "eventStartTime");
        String eventEndTime = HandlerUtils.getRequiredString(toHandle, "eventEndTime");
        String eventDesc = HandlerUtils.getRequiredString(toHandle, "eventDesc");
        String eventPourStartTime = eventDate;
        String eventAfterHoursEndTime = eventDate;
        int callerId = getCallerId(toHandle);
        
        int startMinutes = 7 * 60;
        int endMinutes = 31 * 60;

        String selectEvent = "SELECT id, preOpen, eventEnd FROM eventCateredHours WHERE location = ? AND date = ?";

        String updateEvent = "UPDATE eventCateredHours SET barString = ?, date=?, eventPourStart=(? + INTERVAL ? MINUTE), preOpen=?, eventStart=?, eventEnd=?, eventAfterHoursEnd=(? + INTERVAL ? MINUTE), eventDesc=? WHERE id=?";

        String insertEvent = "INSERT INTO eventCateredHours (location, barString, date, eventPourStart, preOpen, eventStart, eventEnd, eventAfterHoursEnd, eventDesc) VALUES " +
                " (?,?,?,(? + INTERVAL ? MINUTE),?,?,?,(? + INTERVAL ? MINUTE),?);";

        PreparedStatement stmt = null;
        ResultSet rs = null;
         try {
            int colCount = 1;

            stmt = transconn.prepareStatement(selectEvent);
            stmt.setInt(1, locationId);
            stmt.setString(2, eventDate);
            rs = stmt.executeQuery();
            if (rs.next()) {
                int existEventId = rs.getInt(1);
                
            
            if (existEventId > 0) {

                stmt = transconn.prepareStatement(updateEvent);
                stmt.setString(colCount++, barString);
                stmt.setString(colCount++, eventDate);
                stmt.setString(colCount++, eventPourStartTime);
                stmt.setInt(colCount++, startMinutes);
                stmt.setString(colCount++, eventPreOpenTime);
                stmt.setString(colCount++, eventStartTime);
                stmt.setString(colCount++, eventEndTime);
                stmt.setString(colCount++, eventAfterHoursEndTime);
                stmt.setInt(colCount++, endMinutes);
                stmt.setString(colCount++, eventDesc);
                stmt.setInt(colCount++, existEventId);
                stmt.executeUpdate();

                String logMessage = "Updated Event for '" + eventDate + "'";
                logger.portalDetail(callerId, "updateEventDates", 0, "location", locationId, logMessage, transconn);
            }

            } else {

                stmt = transconn.prepareStatement(insertEvent);
                stmt.setInt(colCount++, locationId);
                stmt.setString(colCount++, barString);
                stmt.setString(colCount++, eventDate);
                stmt.setString(colCount++, eventPourStartTime);
                stmt.setInt(colCount++, startMinutes);
                stmt.setString(colCount++, eventPreOpenTime);
                stmt.setString(colCount++, eventStartTime);
                stmt.setString(colCount++, eventEndTime);
                stmt.setString(colCount++, eventAfterHoursEndTime);
                stmt.setInt(colCount++, endMinutes);
                stmt.setString(colCount++, eventDesc);
                stmt.executeUpdate();

                String logMessage = "Inserted Event for '" + eventDate + "'";
                logger.portalDetail(callerId, "updateEventDates", 0, "location", locationId, logMessage, transconn);

            }


        } catch (SQLException sqle) {
            logger.dbError("Database error in updateEventDates: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
            close(rs);
        }
    }


    /** Update the name of an existing customer.  If there were more editable customer fields,
     *  this method would support them.
     *
     *  <customerId> 999        # id to change
     *  <customerName> String   # The new name
     */
    private void updateCustomer(Element toHandle, Element toAppend) throws HandlerException {

        String custName = HandlerUtils.getRequiredString(toHandle, "customerName");
        int custId = HandlerUtils.getRequiredInteger(toHandle, "customerId");
        int callerId = getCallerId(toHandle);

        String update = "UPDATE customer SET name=? WHERE id=?";

        PreparedStatement stmt = null;

        try {
            stmt = transconn.prepareStatement(update);
            int colCount = 1;

            stmt.setString(colCount++, custName);
            stmt.setInt(colCount++, custId);

            stmt.executeUpdate();

            String logMessage = "Updated customer to '" + custName + "'";
            logger.portalDetail(callerId, "updateCustomer", 0, "customer", custId, logMessage, transconn);

        } catch (SQLException sqle) {
            logger.dbError("Database error in updateCustomer: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
        }
    }

    /**  Change the status of a supplier address
     *  <addressId>000
     *
     */
    private void deactivateSupplierAddress(Element toHandle, Element toAppend) throws HandlerException {
        int addrId = HandlerUtils.getRequiredInteger(toHandle, "addressId");
        int callerId = getCallerId(toHandle);

        try {
            changeSupplierAddressStatus(addrId, 0, transconn);
            String logMessage = "Deactivated Supplier Address #" + addrId;
            logger.portalDetail(callerId, "updateSupplier", 0, "supplierAddress", addrId, logMessage, transconn);
        } catch (SQLException sqle) {
            logger.dbError("Database error in deactivateSupplierAddress: " + sqle.getMessage());
            throw new HandlerException(sqle);
        }
    }

    /**  Change the status of a supplier address
     *  <addressId>000
     *
     */
    private void activateSupplierAddress(Element toHandle, Element toAppend) throws HandlerException {
        int addrId = HandlerUtils.getRequiredInteger(toHandle, "addressId");
        int callerId = getCallerId(toHandle);

        try {
            changeSupplierAddressStatus(addrId, 1, transconn);
            String logMessage = "Activated Supplier Address #" + addrId;
            logger.portalDetail(callerId, "updateSupplier", 0, "supplierAddress", addrId, logMessage, transconn);
        } catch (SQLException sqle) {
            logger.dbError("Database error in activateSupplierAddress: " + sqle.getMessage());
            throw new HandlerException(sqle);
        }
    }

    /** Companion method to activate and deactivate supplier address
     *  @param addressId the address to change
     *  @param newStatus 1 to activate, 0 to deactivate
     *  @registeredConnection transconn the connected database transconn
     */
    private void changeSupplierAddressStatus(int addressId, int newStatus, RegisteredConnection transconn) throws SQLException {
        PreparedStatement stmt = transconn.prepareStatement("UPDATE supplierAddress SET active=? WHERE id=?");
        stmt.setInt(1, newStatus);
        stmt.setInt(2, addressId);
        stmt.executeUpdate();
    }

    /** Update an existing supplier.  Right now, all you can change is the name.
     *  <supplierId> 0000
     *  <name> String
     */
    private void updateSupplier(Element toHandle, Element toAppend) throws HandlerException {
        int supplierId = HandlerUtils.getRequiredInteger(toHandle, "supplierId");
        String name = HandlerUtils.getRequiredString(toHandle, "name");
        int callerId = getCallerId(toHandle);

        String update = " UPDATE supplier SET name=? WHERE id=? ";

        PreparedStatement stmt = null;
        try {
            stmt = transconn.prepareStatement(update);
            stmt.setString(1, name);
            stmt.setInt(2, supplierId);
            stmt.executeUpdate();

            String logMessage = "Changed Supplier #" + supplierId + " to '" + name + "'";
            logger.portalDetail(callerId, "updateSupplier", 0, "supplier", supplierId, logMessage, transconn);

        } catch (SQLException sqle) {
            logger.dbError("Database error in updateSupplier: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
        }
    }

    /** Update an existing supplier.  Right now, all you can change is the name.
     *  <supplierId> 0000
     *  <name> String
     */
    private void updateCounty(Element toHandle, Element toAppend) throws HandlerException {

        Iterator i = toHandle.elementIterator("county");
        int callerId = getCallerId(toHandle);


        String updateCounty = " UPDATE county SET points = GEOMFROMTEXT(?) WHERE id = ? ";

        PreparedStatement stmt = null;
        try {
            while (i.hasNext()) {
                Element cou = (Element) i.next();
                int countyId = HandlerUtils.getRequiredInteger(cou, "id");
                String points = HandlerUtils.getOptionalString(cou, "points");
                stmt = transconn.prepareStatement(updateCounty);
                stmt.setString(1, points);
                stmt.setInt(2, countyId);
                stmt.executeUpdate();

            }

        } catch (SQLException sqle) {
            logger.dbError("Database error in updateCounty: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
        }
    }

    /** Update an existing supplier.  Right now, all you can change is the name.
     *  <supplierId> 0000
     *  <name> String
     */
    private void updateUserExclusion(Element toHandle, Element toAppend) throws HandlerException {

        int user = HandlerUtils.getRequiredInteger(toHandle, "user");
        Iterator add = toHandle.elementIterator("addExclusion");
        Iterator delete = toHandle.elementIterator("deleteExclusion");
        int callerId = getCallerId(toHandle);

        String getLastId = " SELECT LAST_INSERT_ID()";
        String selectExclusion = " SELECT e.id FROM exclusion e WHERE e.type = 2 AND e.tables = ? AND e.field = ? AND e.value = ? ";
        String selectUserExclusion = " SELECT id FROM userExclusionMap WHERE user = ? AND exclusion = ? ";
        String deleteUserExclusion = " DELETE FROM userExclusionMap WHERE id = ? ";
        String createExclusion = " INSERT INTO exclusion (type, name, tables, field, value) VALUES (2,?,?,?,?)";
        String addUserExclusion = " INSERT INTO userExclusionMap (user, exclusion) VALUES (?,?) ";

        PreparedStatement stmt = null;
        ResultSet rs = null;
        String tableName = "", tableField = "", exclusionName = "";
        int exclusion = -1;
        try {
            while (delete.hasNext()) {
                Element rem = (Element) delete.next();
                int userExclusionId = HandlerUtils.getRequiredInteger(rem, "userExclusionId");
                stmt = transconn.prepareStatement(deleteUserExclusion);
                stmt.setInt(1, userExclusionId);
                stmt.executeUpdate();
            }

            while (add.hasNext()) {
                Element reg = (Element) add.next();
                int exclusionType = HandlerUtils.getRequiredInteger(reg, "exclusionType");
                int exclusionValue = HandlerUtils.getRequiredInteger(reg, "exclusionValue");

                switch (exclusionType) {
                    case 1:
                        tableName = "county";
                        tableField = "id";
                        break;
                    case 2:
                        tableName = "location";
                        tableField = "id";
                        break;
                    default:
                        break;
                }
                stmt = transconn.prepareStatement(selectExclusion);
                stmt.setString(1, tableName);
                stmt.setString(2, tableField);
                stmt.setString(3, String.valueOf(exclusionValue));
                rs = stmt.executeQuery();
                if (rs.next()) {
                    exclusion = rs.getInt(1);
                } else {
                    exclusionName = tableName + ":" + tableField + ":" + String.valueOf(exclusionValue);
                    stmt = transconn.prepareStatement(createExclusion);
                    stmt.setString(1, exclusionName);
                    stmt.setString(2, tableName);
                    stmt.setString(3, tableField);
                    stmt.setString(4, String.valueOf(exclusionValue));
                    stmt.executeUpdate();

                    // get the id of the exclusion we added
                    stmt = transconn.prepareStatement(getLastId);
                    rs = stmt.executeQuery();
                    if (rs.next()) {
                        exclusion = rs.getInt(1);
                    } else {
                        logger.dbError("SQL Last_Insert_Id FAILED in addTestUser ");
                        throw new HandlerException("database error");
                    }
                }
                stmt = transconn.prepareStatement(selectUserExclusion);
                stmt.setInt(1, user);
                stmt.setInt(2, exclusion);
                rs = stmt.executeQuery();
                if (!rs.next()) {
                    stmt = transconn.prepareStatement(addUserExclusion);
                    stmt.setInt(1, user);
                    stmt.setInt(2, exclusion);
                    stmt.executeUpdate();
                }

            }
        } catch (SQLException sqle) {
            logger.dbError("Database error in updateUserExclusion: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
        }
    }

    /** Update an existing supplier.  Right now, all you can change is the name.
     *  <supplierId> 0000
     *  <name> String
     */
    private void deleteGroupExclusion(Element toHandle, Element toAppend) throws HandlerException {

        Iterator i = toHandle.elementIterator("groupExclusions");
        int callerId = getCallerId(toHandle);

        String selectGroupExclusion = " SELECT gEM.id FROM groupExclusionMap gEM WHERE gEM.id = ? ";
        String deleteGroupExclusion = " DELETE FROM groupExclusionMap WHERE id = ? ";

        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            while (i.hasNext()) {
                Element grou = (Element) i.next();
                int groupExclusionId = HandlerUtils.getRequiredInteger(grou, "groupExclusionId");
                stmt = transconn.prepareStatement(selectGroupExclusion);
                stmt.setInt(1, groupExclusionId);
                rs = stmt.executeQuery();
                if (rs.next()) {
                    stmt = transconn.prepareStatement(deleteGroupExclusion);
                    stmt.setInt(1, groupExclusionId);
                    stmt.executeUpdate();
                } else {
                    logger.debug("Group Exclusion does not exists");
                }
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error in deleteGroupExclusion: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
        }
    }

    /** Update an existing supplier.  Right now, all you can change is the name.
     *  <supplierId> 0000
     *  <name> String
     */
    private void deleteGroupProductSet(Element toHandle, Element toAppend) throws HandlerException {

        Iterator i = toHandle.elementIterator("groupProductSet");
        int callerId = getCallerId(toHandle);

        String selectGroupProductSet = " SELECT gPS.id FROM groupProductSet gPS WHERE gPS.groups = ? AND gPS.productSet = ? ";
        String deleteGroupProductSet = " DELETE FROM groupProductSet WHERE id = ? ";

        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            while (i.hasNext()) {
                Element grou = (Element) i.next();
                int productSet = HandlerUtils.getRequiredInteger(grou, "productSet");
                int groupId = HandlerUtils.getRequiredInteger(grou, "groupId");
                stmt = transconn.prepareStatement(selectGroupProductSet);
                stmt.setInt(1, groupId);
                stmt.setInt(2, productSet);
                rs = stmt.executeQuery();
                if (rs.next()) {
                    stmt = transconn.prepareStatement(deleteGroupProductSet);
                    stmt.setInt(1, rs.getInt(1));
                    stmt.executeUpdate();
                } else {
                    logger.debug("Group Product Set does not exists");
                }
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error in deleteGroupProductSet: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
        }
    }

    /** Update an existing supplier.  Right now, all you can change is the name.
     *  <supplierId> 0000
     *  <name> String
     */
    private void deleteRegionProductSet(Element toHandle, Element toAppend) throws HandlerException {

        Iterator i = toHandle.elementIterator("regionProductSet");
        int callerId = getCallerId(toHandle);

        String selectRegionProductSet = " SELECT rPS.id FROM regionProductSet rPS WHERE rPS.region = ? AND rPS.productSet = ? ";
        String deleteRegionProductSet = " DELETE FROM regionProductSet WHERE id = ? ";

        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            while (i.hasNext()) {
                Element reg = (Element) i.next();
                int productSet = HandlerUtils.getRequiredInteger(reg, "productSet");
                int regionId = HandlerUtils.getRequiredInteger(reg, "regionId");
                stmt = transconn.prepareStatement(selectRegionProductSet);
                stmt.setInt(1, regionId);
                stmt.setInt(2, productSet);
                rs = stmt.executeQuery();
                if (rs.next()) {
                    stmt = transconn.prepareStatement(deleteRegionProductSet);
                    stmt.setInt(1, rs.getInt(1));
                    stmt.executeUpdate();
                } else {
                    logger.debug("Region Product Set does not exists");
                }
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error in deleteUserRegion: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
        }
    }

    /** Update an existing supplier.  Right now, all you can change is the name.
     *  <supplierId> 0000
     *  <name> String
     */
    private void deleteUserRegion(Element toHandle, Element toAppend) throws HandlerException {

        Iterator i = toHandle.elementIterator("userRegions");
        int callerId = getCallerId(toHandle);

        String selectRegion = " SELECT uRM.id FROM region r LEFT JOIN userRegionMap uRM on uRM.region = r.id WHERE r.id = ? AND uRM.user = ? ";

        String deleteUserRegionAccess = " DELETE FROM userRegionMap WHERE id = ? ";
        String deleteRegion = " DELETE FROM region WHERE id = ? ";
        String deleteRegionProductSet = " DELETE FROM regionProductSet WHERE region = ? ";
        String deleteRegionExclusionMap = " DELETE FROM regionExclusionMap WHERE region = ? ";

        String selectUserRegionEmails = " SELECT id FROM emailReport WHERE tableType = 13 AND tableId = ? AND user = ? ";
        String deleteUserRegionEmails = " DELETE FROM emailReport WHERE id = ? ";
        String deleteCustomEmailTime = " DELETE FROM emailTimeTable WHERE report = ? ";

        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            while (i.hasNext()) {
                Element reg = (Element) i.next();
                int user = HandlerUtils.getRequiredInteger(reg, "user");
                int regionId = HandlerUtils.getRequiredInteger(reg, "regionId");
                stmt = transconn.prepareStatement(selectRegion);
                stmt.setInt(1, regionId);
                stmt.setInt(2, user);
                rs = stmt.executeQuery();
                if (rs.next()) {
                    // delete the region access for the user
                    stmt = transconn.prepareStatement(deleteUserRegionAccess);
                    stmt.setInt(1, rs.getInt(1));
                    stmt.executeUpdate();

                    // delete the region
                    stmt = transconn.prepareStatement(deleteRegion);
                    stmt.setInt(1, regionId);
                    stmt.executeUpdate();

                    // delete the region product set
                    stmt = transconn.prepareStatement(deleteRegionProductSet);
                    stmt.setInt(1, regionId);
                    stmt.executeUpdate();

                    // delete the region exclusion
                    stmt = transconn.prepareStatement(deleteRegionExclusionMap);
                    stmt.setInt(1, regionId);
                    stmt.executeUpdate();

                    stmt = transconn.prepareStatement(selectUserRegionEmails);
                    stmt.setInt(1, regionId);
                    stmt.setInt(2, user);
                    rs = stmt.executeQuery();
                    while (rs.next()) {
                        // delete the email access for the user for the deleted region
                        stmt = transconn.prepareStatement(deleteUserRegionEmails);
                        stmt.setInt(1, rs.getInt(1));
                        stmt.executeUpdate();

                        // delete the custom email time for the deleted user - region email
                        stmt = transconn.prepareStatement(deleteCustomEmailTime);
                        stmt.setInt(1, rs.getInt(1));
                        stmt.executeUpdate();
                    }
                }
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error in deleteUserRegion: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
        }
    }

    private void deleteGroupRegion(Element toHandle, Element toAppend) throws HandlerException {

        PreparedStatement stmt = null;
        ResultSet rs = null;

        String selectGroupRegionMap = "SELECT id FROM groupRegionMap WHERE groups = ? AND regionMaster = ? ";
        String selectGroupRegions = "SELECT id FROM region WHERE regionGroup = ? ";
        String deleteGroupRegionMap = "DELETE FROM groupRegionMap WHERE groups = ? AND regionMaster = ? ";

        Iterator i = toHandle.elementIterator("groupsList");

        try {
            while (i.hasNext()) {
                Element grou = (Element) i.next();
                int groupId = HandlerUtils.getRequiredInteger(grou, "groupId");
                int masterRegionId = HandlerUtils.getRequiredInteger(grou, "masterRegionId");

                stmt = transconn.prepareStatement(selectGroupRegionMap);
                stmt.setInt(1, groupId);
                stmt.setInt(2, masterRegionId);
                rs = stmt.executeQuery();
                if (rs.next()) {
                    stmt = transconn.prepareStatement(selectGroupRegions);
                    stmt.setInt(1, rs.getInt(1));
                    rs = stmt.executeQuery();
                    if (!rs.next()) {
                        stmt = transconn.prepareStatement(deleteGroupRegionMap);
                        stmt.setInt(1, groupId);
                        stmt.setInt(2, masterRegionId);
                        stmt.executeUpdate();
                    } else {
                        addErrorDetail(toAppend, "Cannot delete group region. Users Region associated to Group Region were found");
                    }
                }
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error in deleteTextAlerts: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
        }

    }

    private void deleteGroups(Element toHandle, Element toAppend) throws HandlerException {
        PreparedStatement stmt = null;
        ResultSet rs = null;

        String selectGroupsUsers = " SELECT uGM.id FROM userGroupMap uGM WHERE uGM.groups=? ";
        String deleteGroupProductSet = "DELETE FROM groupProductSet WHERE groups=? ";
        String deleteGroupExclusion = "DELETE FROM groupExclusionMap WHERE groups=? ";
        String selectGroupRegionMap = "SELECT id FROM groupRegionMap WHERE groups = ? ";
        String deleteGroupRegionMap = "DELETE FROM groupRegionMap WHERE id = ? ";
        String deleteGroupRegions = "DELETE FROM region WHERE regionGroup = ? ";
        String deleteGroup = "DELETE FROM groups WHERE id = ? ";

        Iterator i = toHandle.elementIterator("groupsList");

        try {
            while (i.hasNext()) {
                Element grou = (Element) i.next();
                int groupId = HandlerUtils.getRequiredInteger(grou, "groupId");
                String groupName = HandlerUtils.getRequiredString(grou, "groupName");

                //Check that this group already has users
                stmt = transconn.prepareStatement(selectGroupsUsers);
                stmt.setInt(1, groupId);
                rs = stmt.executeQuery();
                if (!rs.next()) {
                    stmt = transconn.prepareStatement(deleteGroupProductSet);
                    stmt.setInt(1, groupId);
                    stmt.executeUpdate();

                    stmt = transconn.prepareStatement(deleteGroupExclusion);
                    stmt.setInt(1, groupId);
                    stmt.executeUpdate();

                    stmt = transconn.prepareStatement(selectGroupRegionMap);
                    stmt.setInt(1, groupId);
                    rs = stmt.executeQuery();
                    if (rs.next()) {
                        stmt = transconn.prepareStatement(deleteGroupRegionMap);
                        stmt.setInt(1, rs.getInt(1));
                        stmt.executeUpdate();

                        stmt = transconn.prepareStatement(deleteGroupRegions);
                        stmt.setInt(1, rs.getInt(1));
                        stmt.executeUpdate();
                    }

                    stmt = transconn.prepareStatement(deleteGroup);
                    stmt.setInt(1, groupId);
                    stmt.executeUpdate();

                } else {
                    addErrorDetail(toAppend, "Cannot delete group. Users associated to Group: " + groupName + " found");
                }
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error in deleteTextAlerts: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
        }

    }

    /** Update an existing supplier.  Right now, all you can change is the name.
     *  <supplierId> 0000
     *  <name> String
     */
    private void updateGroupRegion(Element toHandle, Element toAppend) throws HandlerException {

        Iterator i = toHandle.elementIterator("groupRegion");
        int callerId = getCallerId(toHandle);

        String updateGroupRegion = " UPDATE groupRegionMap SET threshold = ? WHERE id = ? ";

        PreparedStatement stmt = null;
        try {
            while (i.hasNext()) {
                Element grou = (Element) i.next();
                int regionGroup = HandlerUtils.getRequiredInteger(grou, "regionGroup");
                double threshold = HandlerUtils.getRequiredDouble(grou, "threshold");
                stmt = transconn.prepareStatement(updateGroupRegion);
                stmt.setDouble(1, threshold);
                stmt.setInt(2, regionGroup);
                stmt.executeUpdate();
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error in updateGroupRegion: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
        }
    }

    /** Update an existing supplier.  Right now, all you can change is the name.
     *  <supplierId> 0000
     *  <name> String
     */
    private void updateGroups(Element toHandle, Element toAppend) throws HandlerException {

        int groupId = HandlerUtils.getRequiredInteger(toHandle, "groupId");
        int groupType = HandlerUtils.getRequiredInteger(toHandle, "groupType");
        String groupName = HandlerUtils.getRequiredString(toHandle, "groupName");
        int callerId = getCallerId(toHandle);

        String updateGroups = " UPDATE groups SET name = ?, type = ? WHERE id = ? ";

        PreparedStatement stmt = null;
        try {
            stmt = transconn.prepareStatement(updateGroups);
            stmt.setString(1, groupName);
            stmt.setInt(2, groupType);
            stmt.setInt(3, groupId);
            stmt.executeUpdate();
        } catch (SQLException sqle) {
            logger.dbError("Database error in updateGroups: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
        }
    }

    /** Update an existing supplier.  Right now, all you can change is the name.
     *  <supplierId> 0000
     *  <name> String
     */
    private void updateProductSet(Element toHandle, Element toAppend) throws HandlerException {

        int productSetId = HandlerUtils.getRequiredInteger(toHandle, "productSetId");
        String productSetName = HandlerUtils.getRequiredString(toHandle, "productSetName");

        Iterator i = toHandle.elementIterator("add");
        Iterator j = toHandle.elementIterator("delete");
        Iterator k = toHandle.elementIterator("update");
        int callerId = getCallerId(toHandle);

        String selectProductSet = " SELECT id FROM productSet WHERE id = ? ";
        String updateProductSet = " UPDATE productSet SET name = ? WHERE id = ? ";

        String selectProductSetMap = " SELECT id FROM productSetMap WHERE productSet = ? AND product = ? AND plu = ?";
        String selectProductSetMapId = " SELECT id FROM productSetMap WHERE id = ?";
        String insertProductSetProductMap = " INSERT INTO productSetMap (productSet, product, plu) VALUES (?,?,?) ";
        String updateProductSetProductMap = " UPDATE productSetMap SET plu = ? WHERE id = ? ";
        String deleteProductSetProductMap = " DELETE FROM productSetMap WHERE id = ?";

        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            stmt = transconn.prepareStatement(selectProductSet);
            stmt.setInt(1, productSetId);
            rs = stmt.executeQuery();
            if (rs.next()) {
                stmt = transconn.prepareStatement(updateProductSet);
                stmt.setString(1, productSetName);
                stmt.setInt(2, productSetId);
                stmt.executeUpdate();

                // Deleting product set map
                while (j.hasNext()) {
                    Element pro = (Element) j.next();
                    int productMapId = HandlerUtils.getRequiredInteger(pro, "productMapId");

                    stmt = transconn.prepareStatement(selectProductSetMapId);
                    stmt.setInt(1, productMapId);
                    rs = stmt.executeQuery();
                    if (rs.next()) {
                        stmt = transconn.prepareStatement(deleteProductSetProductMap);
                        stmt.setInt(1, productMapId);
                        stmt.executeUpdate();
                    } else {
                        addErrorDetail(toAppend, "Product Set does not have the specified product set map ID: " + productMapId);
                    }
                }

                // Updating product set map
                while (k.hasNext()) {
                    Element pro = (Element) k.next();
                    int productMapId = HandlerUtils.getRequiredInteger(pro, "productMapId");
                    String productPlu = HandlerUtils.getRequiredString(pro, "productPlu");

                    stmt = transconn.prepareStatement(selectProductSetMapId);
                    stmt.setInt(1, productMapId);
                    rs = stmt.executeQuery();
                    if (rs.next()) {
                        stmt = transconn.prepareStatement(updateProductSetProductMap);
                        stmt.setString(1, productPlu);
                        stmt.setInt(2, productMapId);
                        stmt.executeUpdate();
                    } else {
                        addErrorDetail(toAppend, "Product Set does not have the specified product set map ID: " + productMapId + " and plu: " + productPlu);
                    }
                }

                // Adding product set map
                while (i.hasNext()) {
                    Element pro = (Element) i.next();
                    int productId = HandlerUtils.getRequiredInteger(pro, "productId");
                    String productPlu = HandlerUtils.getRequiredString(pro, "productPlu");

                    stmt = transconn.prepareStatement(selectProductSetMap);
                    stmt.setInt(1, productSetId);
                    stmt.setInt(2, productId);
                    stmt.setString(3, productPlu);
                    rs = stmt.executeQuery();
                    if (!rs.next()) {
                        stmt = transconn.prepareStatement(insertProductSetProductMap);
                        stmt.setInt(1, productSetId);
                        stmt.setInt(2, productId);
                        stmt.setString(3, productPlu);
                        stmt.executeUpdate();
                    } else {
                        addErrorDetail(toAppend, "Product Set with specified product: " + productId + " and plu: " + productPlu + " already exists");
                    }
                }
            } else {
                addErrorDetail(toAppend, "Product Set does not exists");
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error in updateProductSet: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
            close(rs);
        }
    }

    /** Update an existing supplier.  Right now, all you can change is the name.
     *  <supplierId> 0000
     *  <name> String
     */
    private void updateRegion(Element toHandle, Element toAppend) throws HandlerException {

        int regionId = HandlerUtils.getRequiredInteger(toHandle, "regionId");
        String regionName = HandlerUtils.getRequiredString(toHandle, "regionName");
        double threshold = HandlerUtils.getRequiredDouble(toHandle, "threshold");
        String points = "POINT(0 0)";
        points = HandlerUtils.getOptionalString(toHandle, "points");

        Iterator i = toHandle.elementIterator("add");
        Iterator j = toHandle.elementIterator("delete");
        int callerId = getCallerId(toHandle);

        String selectRegionMaster = " SELECT id FROM regionMaster WHERE id = ? ";
        String updateRegionMaster = " UPDATE regionMaster SET name = ?, threshold = ?, points = GEOMFROMTEXT(?) WHERE id = ? ";
        String insertRegionCountyMap = " INSERT INTO regionCountyMap (region, county, description) VALUES (?,?,?) ";
        String deleteRegionCountyMap = " DELETE FROM regionCountyMap WHERE region = ? AND county = ? ";

        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            stmt = transconn.prepareStatement(selectRegionMaster);
            stmt.setInt(1, regionId);
            rs = stmt.executeQuery();
            if (rs.next()) {
                stmt = transconn.prepareStatement(updateRegionMaster);
                stmt.setString(1, regionName);
                stmt.setDouble(2, threshold);
                stmt.setString(3, points);
                stmt.setInt(4, regionId);
                stmt.executeUpdate();

                while (i.hasNext()) {
                    Element coun = (Element) i.next();
                    int countyId = HandlerUtils.getRequiredInteger(coun, "countyId");
                    String countyName = HandlerUtils.getRequiredString(coun, "countyName");
                    stmt = transconn.prepareStatement(insertRegionCountyMap);
                    stmt.setInt(1, regionId);
                    stmt.setInt(2, countyId);
                    stmt.setString(3, countyName);
                    stmt.executeUpdate();
                }

                while (j.hasNext()) {
                    Element coun = (Element) j.next();
                    int countyId = HandlerUtils.getRequiredInteger(coun, "countyId");
                    stmt = transconn.prepareStatement(deleteRegionCountyMap);
                    stmt.setInt(1, regionId);
                    stmt.setInt(2, countyId);
                    stmt.executeUpdate();
                }
            } else {
                addErrorDetail(toAppend, "Region does not exists");
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error in updateRegion: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
            close(rs);
        }
    }

    /** Update an existing supplier address.
     *  <addressId>0000
     *  <addrStreet>
     *  <addrCity>
     *  <addrState>
     *  <addrZip>
     */
    private void updateSupplierAddress(Element toHandle, Element toAppend) throws HandlerException {
        int addrId = HandlerUtils.getRequiredInteger(toHandle, "addressId");
        String addrStreet = HandlerUtils.getRequiredString(toHandle, "addrStreet");
        String addrCity = HandlerUtils.getRequiredString(toHandle, "addrCity");
        String addrState = HandlerUtils.getRequiredString(toHandle, "addrState");
        String addrZip = HandlerUtils.getRequiredString(toHandle, "addrZip");
        int callerId = getCallerId(toHandle);

        String update = " UPDATE supplierAddress SET addrStreet=?,addrCity=?,addrState=?,addrZip=? WHERE id=? ";

        PreparedStatement stmt = null;
        try {
            stmt = transconn.prepareStatement(update);
            stmt.setString(1, addrStreet);
            stmt.setString(2, addrCity);
            stmt.setString(3, addrState);
            stmt.setString(4, addrZip);
            stmt.setInt(5, addrId);
            stmt.executeUpdate();

            String logMessage = "Changed Supplier Address #" + addrId;
            logger.portalDetail(callerId, "updateSupplier", 0, "supplierAddress", addrId, logMessage, transconn);

        } catch (SQLException sqle) {
            logger.dbError("Database error in updateSupplierAddress: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
        }
    }

    /** Update the sales rep email address for one location's suppliers
     *  <locationId>0000
     *    <supplier>
     *      <supplierId>0000
     *      <email>address@domain.com
     *    </supplier>
     *    <supplier ... />
     */
    private void updateSupplierEmail(Element toHandle, Element toAppend) throws HandlerException {
        int locationId = HandlerUtils.getRequiredInteger(toHandle, "locationId");
        int callerId = getCallerId(toHandle);

        String update = " UPDATE locationSupplier map LEFT JOIN supplierAddress addr ON map.address = addr.id " +
                " SET map.repEmail=? WHERE map.location=? AND addr.supplier=? ";

        PreparedStatement stmt = null;
        try {
            stmt = transconn.prepareStatement(update);
            Iterator sups = toHandle.elementIterator("supplier");
            while (sups.hasNext()) {
                Element supEl = (Element) sups.next();
                int supplierId = HandlerUtils.getRequiredInteger(supEl, "supplierId");
                String email = HandlerUtils.getOptionalString(supEl, "email");
                if (email == null) {
                    email = "";
                }
                stmt.setString(1, email);
                stmt.setInt(2, locationId);
                stmt.setInt(3, supplierId);
                stmt.executeUpdate();

                String logMessage = "Changed Supplier Rep Email for S#" + supplierId + " L#" + locationId + " to " + email;
                logger.portalDetail(callerId, "updateSupplierEmail", locationId, "supplier", supplierId, logMessage, transconn);
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error in updateSupplierEmail: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
        }
    }

    /*  Change the address of an existing supplier.
     *  All products in inventory that belong to the supplier will change to the new address
     *  since the rule is one address per supplier.
     *  Two options:  either specify <oldAddressId> -OR- <supplierId>
     *
     *  EITHER  <oldAddressId>
     *  OR      <supplierId>
     *  <newAddressId>
     *  <locationId>
     *
     */
    private void updateLocationSupplier(Element toHandle, Element toAppend) throws HandlerException {

        int callerId = getCallerId(toHandle);
        int location = HandlerUtils.getRequiredInteger(toHandle, "locationId");

        String logMessage = null;
        String updateFromAddress = "UPDATE locationSupplier SET address=? WHERE location=? AND address=?";
        String updateAccount = "UPDATE locationSupplier SET account=? WHERE location=? AND address=?";

        Iterator i = toHandle.elementIterator("supplierUpdate");
        PreparedStatement stmt = null;

        try {
            while (i.hasNext()) {
                Element update = (Element) i.next();
                int oldAddress = HandlerUtils.getOptionalInteger(update, "oldAddressId");
                int addressId = HandlerUtils.getOptionalInteger(update, "addressId");
                String accountIdString = HandlerUtils.getOptionalString(update, "accountIdString");
                if (accountIdString == null) {
                    accountIdString = "0";
                }

                if (oldAddress <= 0 && addressId <= 0) {
                    throw new HandlerException("Must supply either oldAddress or addressId");
                }

                if (oldAddress > 0) {
                    stmt = transconn.prepareStatement(updateFromAddress);
                    stmt.setInt(1, addressId);
                    stmt.setInt(2, location);
                    stmt.setInt(3, oldAddress);
                    stmt.executeUpdate();
                    logMessage = "Changed Supplier Address for addr:" + oldAddress + "/loc:" + location;
                    logger.portalDetail(callerId, "updateSupplier", location, "location", location, logMessage, transconn);
                }
                if (addressId > 0) {
                    stmt = transconn.prepareStatement(updateAccount);
                    stmt.setString(1, accountIdString);
                    stmt.setInt(2, location);
                    stmt.setInt(3, addressId);
                    stmt.executeUpdate();
                    logMessage = "Updated Supplier Location Account ID for loc:" + location;
                    logger.portalDetail(callerId, "updateSupplier", location, "location", location, logMessage, transconn);
                }
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error in updateSupplierAddress: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
        }

    }

    /** Remove a location-supplier association
     *  <addressId>
     *  <locationId>
     */
    private void deleteLocationSupplier(Element toHandle, Element toAppend) throws HandlerException {
        int address = HandlerUtils.getRequiredInteger(toHandle, "addressId");
        int location = HandlerUtils.getRequiredInteger(toHandle, "locationId");
        int callerId = getCallerId(toHandle);

        String selectSupplier = "SELECT supplier FROM supplierAddress WHERE id=? ";
        String delete = "DELETE FROM locationSupplier WHERE location=? AND address=?";
        String updateInv = "UPDATE inventory SET plu=0,supplier=2 WHERE location=? AND supplier = ? ";
        PreparedStatement stmt = null;
        ResultSet rs = null;

        try {

            stmt = transconn.prepareStatement(selectSupplier);
            stmt.setInt(1, address);
            rs = stmt.executeQuery();
            if (rs.next()) {
                stmt = transconn.prepareStatement(delete);
                stmt.setInt(1, location);
                stmt.setInt(2, address);
                stmt.executeUpdate();
                String logMessage = "Deleted address #" + address + " from location #" + location;
                logger.portalDetail(callerId, "updateSupplier", location, "location", location, logMessage, transconn);

                stmt = transconn.prepareStatement(updateInv);
                stmt.setInt(1, location);
                stmt.setInt(2, rs.getInt(1));
                stmt.executeUpdate();
                logMessage = "Reset inventory-supplier details from location #" + location;
                logger.portalDetail(callerId, "updateSupplier", location, "location", location, logMessage, transconn);
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error in deleteSupplierAddress: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
            close(rs);
        }

    }

    private void deleteBeverageSize(Element toHandle, Element toAppend) throws HandlerException {
        PreparedStatement stmt = null;
        ResultSet rs = null;

        int location = HandlerUtils.getRequiredInteger(toHandle, "locationId");
        String name = HandlerUtils.getRequiredString(toHandle, "name");
        int callerId = getCallerId(toHandle);

        String getSize = "SELECT id FROM beverageSize WHERE location=? AND name=?";
        String deleteSize = "DELETE FROM beverageSize WHERE id=?";

        try {
            stmt = transconn.prepareStatement(getSize);
            stmt.setInt(1, location);
            stmt.setString(2, name);

            rs = stmt.executeQuery();
            if (rs.next()) {
                int toKill = rs.getInt(1);
                stmt = transconn.prepareStatement(deleteSize);
                stmt.setInt(1, toKill);
                stmt.executeUpdate();

                String logMessage = "Deleted beverage size '" + name + "'";
                logger.portalDetail(callerId, "deleteBeverageSize", location, "beverageSize", toKill, logMessage, transconn);
            } else {
                addErrorDetail(toAppend, "The beverage size '" + name + "' does not exist");
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error in deleteBeverageSize: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
            close(rs);
        }
    }

    private void deleteBeveragePlu(Element toHandle, Element toAppend) throws HandlerException {

        PreparedStatement stmt = null;
        ResultSet rs = null;

        boolean forCustomer = HandlerUtils.getOptionalBoolean(toHandle, "forCustomer");
        int location = 0;
        int callerId = getCallerId(toHandle);

        String selectLocations = " SELECT id FROM location WHERE customer = ? ";

        try {
            if (forCustomer) {
                int customerId = HandlerUtils.getRequiredInteger(toHandle, "customerId");
                if(callerId > 0 && customerId > 0 && isValidAccessUser(callerId, customerId,true)){  
                    stmt                    = transconn.prepareStatement("SELECT id FROM customer WHERE id = ? AND groupId = 2");
                    stmt.setInt(1, customerId);
                    rs                      = stmt.executeQuery();
                    if (rs.next()) {
                        selectLocations     = "SELECT id FROM location WHERE customer IN (SELECT id FROM customer WHERE groupId = ?)";
                        customerId          = 2;
                    }   
                stmt = transconn.prepareStatement(selectLocations);
                stmt.setInt(1, customerId);
                rs = stmt.executeQuery();
                while (rs.next()) {
                    location = rs.getInt(1);
                    deleteBeveragePluDetail(callerId, location, toHandle, toAppend);
                }
                } else {
                    addErrorDetail(toAppend, "Invalid Access"  );
                }
            } else {
                location = HandlerUtils.getRequiredInteger(toHandle, "locationId");
                if(callerId > 0 && location > 0 && isValidAccessUser(callerId, location,false)){
                    deleteBeveragePluDetail(callerId, location, toHandle, toAppend);
                } else {
                    addErrorDetail(toAppend, "Invalid Access"  );
                }
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error in deleteBeveragePlu: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
        }

    }

    private void deleteBeveragePluDetail(int callerId, int location, Element toHandle, Element toAppend) throws HandlerException {

        PreparedStatement stmt = null;
        ResultSet rs = null, rsIng = null, ing = null;;

        String selectBeverage = "SELECT id, name,ounces FROM beverage WHERE plu=? AND location=?";
        String deleteBeverage = "DELETE FROM beverage WHERE id=?";
        String deleteIngredient = "DELETE FROM ingredient WHERE beverage=?";
        String selectIngredient = "SELECT product FROM ingredient WHERE beverage = ? ";
        String selectIngredients = "SELECT  i.product, b.name, i.ounces FROM ingredient i LEFT JOIN beverage b ON b.id = i.beverage WHERE i.beverage=?";

        // A cache of product data delay sets (maps product -> datadelay hour)
        HashMap<Integer, Timestamp> dataDelayCache = new HashMap<Integer, Timestamp>();

        java.util.Date now = new java.util.Date();


        Iterator bevs = toHandle.elementIterator("beverage");

        try {
            while (bevs.hasNext()) {
                Element bev = (Element) bevs.next();
                String plu = HandlerUtils.getRequiredString(bev, "plu");
                boolean toReAdd = HandlerUtils.getOptionalBoolean(bev, "toReAdd");
                
                stmt = transconn.prepareStatement(selectBeverage);
                stmt.setString(1, plu);
                stmt.setInt(2, location);
                rs = stmt.executeQuery();
                String loggerMesssage       = "Deleted PLU #"+ plu;
                if (rs.next()) {
                     loggerMesssage      +=" from beverage "+ HandlerUtils.nullToEmpty(rs.getString(2))+" "+String.valueOf(rs.getDouble(3))+"oz" ;

                    if (toReAdd) {
                        stmt                = transconn.prepareStatement(selectIngredients);
                        stmt.setInt(1, rs.getInt(1));
                         Element beverage = toAppend.addElement("beverages");
                        beverage.addElement("name").addText(HandlerUtils.nullToEmpty(rs.getString(2)));
                        beverage.addElement("plu").addText(plu);                       
                        ing                 = stmt.executeQuery();
                        while (ing.next()) {
                            Element ingredient
                                            = beverage.addElement("ingredients");
                            ingredient.addElement("product").addText(HandlerUtils.nullToEmpty(ing.getString(1)));
                            ingredient.addElement("quantity").addText(HandlerUtils.nullToEmpty(ing.getString(3)));
                            
                        }
                    }

                    stmt = transconn.prepareStatement(deleteBeverage);
                    stmt.setInt(1, rs.getInt(1));
                    stmt.executeUpdate();
                    logger.portalDetail(callerId, "deleteBeverage", location, "beverage", rs.getInt(1), loggerMesssage, transconn);

                    stmt = transconn.prepareStatement(selectIngredient);
                    stmt.setInt(1, rs.getInt(1));
                    rsIng = stmt.executeQuery();
                    while (rsIng.next()) {
                        dataDelayCache.put(rsIng.getInt(1), toSqlTimestamp(now));
                    }
                    stmt = transconn.prepareStatement(deleteIngredient);
                    stmt.setInt(1, rs.getInt(1));
                    stmt.executeUpdate();
                }
            }
            if (dataDelayCache.size() > 0) {
                setDataMod(location, dataDelayCache);
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
            close(rs);
            close(rsIng);
        }
    }

    private void setDataMod(int location, HashMap<Integer, Timestamp> dataDelayCache) {

        String sql = "SELECT l.easternOffset, l.lastSold FROM location l WHERE l.id = ? ";
        String insertDataDelay = "INSERT INTO dataModNew (location, modType, modId, start, end, date) VALUES (?,?,?,?,?,?);";
        long oneHour = 60 * 60 * 1000;
        long oneDay = 24 * 60 * 60 * 1000;
        Double offset = 0.0;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            java.util.Date startTime = new java.util.Date();
            java.util.Date endTime = new java.util.Date();
            stmt = transconn.prepareStatement(sql);
            stmt.setInt(1, location);
            rs = stmt.executeQuery();
            if (rs.next()) {
                offset = rs.getDouble(1);
            }
            for (Integer product : dataDelayCache.keySet()) {
                startTime = new java.util.Date(dataDelayCache.get(product).getTime());
                startTime.setHours(7 - offset.intValue());
                startTime.setMinutes(0);
                startTime.setSeconds(0);
                endTime = new java.util.Date(dataDelayCache.get(product).getTime() + oneHour);
                endTime.setMinutes(0);
                endTime.setSeconds(0);
                if (endTime.before(startTime)) {
                    startTime = new java.util.Date(startTime.getTime() - oneDay);
                }
                int i = 1;
                stmt = transconn.prepareStatement(insertDataDelay);
                stmt.setInt(i++, location);
                stmt.setInt(i++, 3);
                stmt.setInt(i++, product);
                stmt.setTimestamp(i++, toSqlTimestamp(startTime));
                stmt.setTimestamp(i++, toSqlTimestamp(endTime));
                stmt.setTimestamp(i++, toSqlTimestamp(startTime));
                stmt.executeUpdate();
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error in setDataMod: " + sqle.getMessage());
        } finally {
            close(rs);
            close(stmt);
        }
    }

    /** Converts a java.util.Date to a java.sql.Date
     */
    private java.sql.Timestamp toSqlTimestamp(java.util.Date d) {
        return new java.sql.Timestamp(d.getTime());
    }

    /** Converts a java.util.Date to a java.sql.Date
     */
    private DateTimeParameter toDateTimeParameter(java.util.Date d) {
        return new DateTimeParameter(d);
    }

    private void updateBeveragePlu(Element toHandle, Element toAppend) throws HandlerException {

        PreparedStatement stmt = null;
        ResultSet rs = null;

        boolean forCustomer = HandlerUtils.getOptionalBoolean(toHandle, "forCustomer");
        int location = 0;
        int callerId = getCallerId(toHandle);
        String selectLocations = " SELECT id FROM location WHERE customer = ? ";

        try {
            locationMap = new LocationMap(transconn);
            if (forCustomer) {
                int customerId = HandlerUtils.getRequiredInteger(toHandle, "customerId");
                if(callerId > 0 && customerId > 0 && isValidAccessUser(callerId, customerId,true)){  
                    stmt                    = transconn.prepareStatement("SELECT id FROM customer WHERE id = ? AND groupId = 2");
                    stmt.setInt(1, customerId);
                    rs                      = stmt.executeQuery();
                    if (rs.next()) {
                        selectLocations     = "SELECT id FROM location WHERE customer IN (SELECT id FROM customer WHERE groupId = ?)";
                        customerId          = 2;
                    }   
                stmt = transconn.prepareStatement(selectLocations);
                stmt.setInt(1, customerId);
                rs = stmt.executeQuery();
                while (rs.next()) {
                    location = rs.getInt(1);
                    updateBeveragePluDetail(callerId, location, toHandle, toAppend);
                }
                } else {
                addErrorDetail(toAppend, "Invalid Access"  );
            }
            } else {
                location = HandlerUtils.getRequiredInteger(toHandle, "locationId");
                if(callerId > 0 && location > 0 && isValidAccessUser(callerId, location,false)){
                    updateBeveragePluDetail(callerId, location, toHandle, toAppend);
                } else {
                addErrorDetail(toAppend, "Invalid Access"  );
            }
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
            close(rs);
        }
        locationMap = null;
    }

    private void updateBeveragePluDetail(int callerId, int location, Element toHandle, Element toAppend) throws HandlerException {

        PreparedStatement getStmt = null;
        PreparedStatement getIngredients = null;
        PreparedStatement updateStmt = null;
        ResultSet rs = null;

        String getBeverage =
                " SELECT bev.id, bar.name, IF(COUNT(i.id) > 1, bev.name, p.name) FROM beverage bev LEFT JOIN bar ON bev.bar = bar.id " +
                " LEFT JOIN ingredient i ON i.beverage = bev.id LEFT JOIN product p ON p.id = i.product " +
                " WHERE bev.location = ? AND bev.plu = ? GROUP BY bev.id";

        String updateBeverage = "UPDATE beverage SET plu=? WHERE id=?";
        String selectIngredients = "SELECT product FROM ingredient WHERE beverage = ? ";

        // A cache of product data delay sets (maps product -> datadelay hour)
        HashMap<Integer, Timestamp> dataDelayCache = new HashMap<Integer, Timestamp>();
        java.util.Date now = new java.util.Date();

        try {
            getStmt = transconn.prepareStatement(getBeverage);
            updateStmt = transconn.prepareStatement(updateBeverage);
            getIngredients = transconn.prepareStatement(selectIngredients);
            // we need to check for update conflicts
            // 1. first, ensure that there are no duplicates in the "new" update set
            // 2. second, any "new" plu must exist in the "old" update set or NOT exist in the database
            Iterator i = toHandle.elementIterator("beverage");
            Set<String> oldSet = new HashSet<String>();
            Set<String> newSet = new HashSet<String>();
            Set<String> invalidSet = new HashSet<String>();
            Set<String> offendingNew = new HashSet<String>();
            Map<String, Integer> pluToId = new HashMap<String, Integer>();
            Map<String, String> oldNewSet = new HashMap<String, String>();
            Map<String, String> pluToName = new HashMap<String, String>();
            Map<String, Integer> validPlu = new HashMap<String, Integer>();

            while (i.hasNext()) {
                Element el = (Element) i.next();
                String newPlu = HandlerUtils.getRequiredString(el, "newPlu");
                String oldPlu = HandlerUtils.getRequiredString(el, "oldPlu");
                if (newPlu.length() < 1 || oldPlu.length() < 1) {
                    //throw new HandlerException("Found an empty PLU code");
                    addErrorDetail(toAppend, "Found an empty PLU code ");
                    continue;
                }
                // check update conflict case #1:
                if (newSet.contains(newPlu)) {
                    offendingNew.add(newPlu);
                    addErrorDetail(toAppend, "Duplicate new PLU '" + newPlu + "' was not added for old PLU '" + oldPlu + "' at Location: " + locationMap.getLocation(location));
                } else {
                    oldNewSet.put(newPlu, oldPlu);
                    newSet.add(newPlu);
                    oldSet.add(oldPlu);
                    getStmt.setInt(1, location);
                    getStmt.setString(2, oldPlu);
                    rs = getStmt.executeQuery();
                    if (rs.next()) {
                        pluToId.put(oldPlu, new Integer(rs.getInt(1)));
                        pluToName.put(oldPlu, rs.getString(3));
                    } else {
                        //throw new HandlerException("Could not find PLU: " + oldPlu);
                        addErrorDetail(toAppend, "Could not find PLU: " + oldPlu + " at Location: " + locationMap.getLocation(location));
                    }
                }
            }
            /* *
            if (offendingNew.size() > 0) {
            for (String s : offendingNew) {
            addErrorDetail(toAppend, "Found duplicate new PLUs" + s);
            }
            } else {
             * */
            //logger.debug("Checkset sizes: old, " + oldSet.size() + "; new, " + newSet.size() + "; ids, " + pluToId.size() + "; names, " + pluToName.size());
            i = toHandle.elementIterator("beverage");
            while (i.hasNext()) {
                Element el = (Element) i.next();
                String oldPlu = HandlerUtils.getRequiredString(el, "oldPlu");
                String newPlu = HandlerUtils.getRequiredString(el, "newPlu");
                Integer updateInt = pluToId.get(oldPlu);
                if (!oldPlu.equals(newPlu) && updateInt != null) {

                    // check update conflict case #2
                    boolean okToAdd = false;
                    if (oldSet.contains(newPlu)) {
                        okToAdd = true;
                        //logger.debug("New PLU " + newPlu + " exists in old set, OK to update");
                    } else {
                        getStmt.setInt(1, location);
                        getStmt.setString(2, newPlu);
                        rs = getStmt.executeQuery();
                        //This is a duplicate check, so we want no result
                        if (rs.next()) {
                            String barName = rs.getString(2);
                            String beverageName = rs.getString(3);
                            String detailMessage = "The PLU '" + newPlu + "' (for: " + pluToName.get(oldPlu) + ") already exists at Location: " + locationMap.getLocation(location);
                            if (barName != null && barName.length() > 0) {
                                detailMessage += " (" + barName + ")";
                            }
                            if (beverageName != null && beverageName.length() > 0) {
                                detailMessage += " for " + beverageName;
                            }
                            invalidSet.add(oldPlu);
                            invalidSet.add(oldNewSet.get(oldPlu));
                            addErrorDetail(toAppend, detailMessage);
                            //logger.debug("New PLU " + newPlu + " exists in the db, NOT updating. old PLU: " + oldPlu + " New PLU: " + oldNewSet.get(oldPlu));
                        } else {
                            okToAdd = true;
                            //logger.debug("New PLU " + newPlu + " doesnt exist in db, OK to update");
                        }
                    }
                    if (okToAdd) {
                        int bevId = updateInt.intValue();
                        validPlu.put(newPlu, bevId);
                    } else {
                        logger.debug("Skipping " + newPlu);
                    }
                }
            }
            //logger.debug("Size of validPlu is: " + validPlu.size() + " and size of invalidSet is " + invalidSet.size());
            Iterator iterator = validPlu.keySet().iterator();
            while (iterator.hasNext()) {
                String newPlu = (String) iterator.next();
                if (!invalidSet.contains(newPlu)) {
                    updateStmt.setString(1, newPlu);
                    updateStmt.setInt(2, validPlu.get(newPlu));
                    updateStmt.executeUpdate();

                    getIngredients.setInt(1, validPlu.get(newPlu));
                    rs = getIngredients.executeQuery();
                    while (rs.next()) {
                        dataDelayCache.put(rs.getInt(1), toSqlTimestamp(now));
                    }
                } else {
                    addErrorDetail(toAppend, "Unable to update PLU: " + newPlu + " at Location: " + locationMap.getLocation(location));
                }
                //String logMessage = "Changed PLU from '" + oldPlu + "' to '" + newPlu + " at Location " + location;
                //logger.portalDetail(callerId, "updateBeverage", location, "beverage", bevId, logMessage, transconn);
            }
            if (dataDelayCache.size() > 0) {
                setDataMod(location, dataDelayCache);
            }
            //}
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(getIngredients);
            close(updateStmt);
            close(getStmt);
            close(rs);
        }
    }

    /**  NOT LONGER USED Aug 1 2006
     *  @deprecated use updateBeveragePlu instead
     */
    private void updatePluCodes(Element toHandle, Element toAppend) throws HandlerException {

        PreparedStatement stmt = null;

        String updateByRowId = "UPDATE inventory SET plu=? WHERE id=?";
        String updateByProdAndLocation = "UPDATE inventory SET plu=? " +
                " WHERE product=? AND location=?";
        logger.generalWarning("Called deprecated method updatePluCodes");
        logger.portalAccessViolation("User " + getCallerId(toHandle) + " called updatePluCodes");
        throw new HandlerException("Call to Deprecated updatePluCodes");

//        try {
//            Iterator i = toHandle.elementIterator("product");
//            while (i.hasNext()) {
//                Element el = (Element) i.next();
//                String plu = HandlerUtils.getRequiredString(el,"pluCode");
//                int id = HandlerUtils.getOptionalInteger(el, "invId");
//                if (id == HandlerUtils.NULL_INTEGER) {
//                    int location = HandlerUtils.getRequiredInteger(el,"locationId");
//                    int product = HandlerUtils.getRequiredInteger(el,"productId");
//                    stmt = transconn.prepareStatement(updateByProdAndLocation);
//                    stmt.setString(1,plu);
//                    stmt.setInt(2, product);
//                    stmt.setInt(3,location);
//                    logger.portalAction("Updating PLU for product "+product+" at L"+location+" to "+plu);
//                    stmt.executeUpdate();
//                } else {
//                    stmt = transconn.prepareStatement(updateByRowId);
//                    stmt.setString(1,plu);
//                    stmt.setInt(2, id);
//                    logger.portalAction("Updating PLU for inventory id "+id+" to "+plu);
//                    stmt.executeUpdate();
//                }
//            }
//        } catch (SQLException sqle) {
//            logger.dbError("Database error: " + sqle.getMessage());
//            throw new HandlerException(sqle);
//        } finally {
//            close(stmt);
//        }
    }

    /**  Mark an order received, and update the location's inventory to reflect
     * the contents of the order.
     */
    private void receiveOrder(Element toHandle, Element toAppend) throws HandlerException {

        int orderNumber = HandlerUtils.getRequiredInteger(toHandle, "orderNumber");
        int locationId = HandlerUtils.getRequiredInteger(toHandle, "locationId");
        int callerId = getCallerId(toHandle);
        int osPlatform                      = HandlerUtils.getOptionalInteger(toHandle, "osPlatform");
        int mobileUserId                    = HandlerUtils.getOptionalInteger(toHandle, "mobileUserId"); 

        PreparedStatement stmt = null;
        ResultSet rs = null, rs2 = null;

        DecimalFormat twoPlaces = new DecimalFormat("0.00");

        String select = "SELECT status FROM purchase WHERE location=? AND id=? LIMIT 1";
        String receive = "UPDATE purchase SET status='RECEIVED', receivedBy=?, receivedDate=NOW() WHERE id=? LIMIT 1";
        String selectDetail = "SELECT quantity,product FROM purchaseDetail " +
                " WHERE purchase=? ";
        String getInventory = "SELECT id FROM inventory WHERE product=? AND location=?";
        String updateInventory = "UPDATE inventory SET qtyOnHand=qtyOnHand+?, isActive = 1 WHERE id=?";

        try {
            // check the order exists at this location
            stmt = transconn.prepareStatement(select);
            stmt.setInt(1, locationId);
            stmt.setInt(2, orderNumber);
            rs = stmt.executeQuery();
            if (rs.next() && "OPEN".equalsIgnoreCase(rs.getString(1))) {
                //change the status
                stmt = transconn.prepareStatement(receive);
                stmt.setInt(1, callerId);
                stmt.setInt(2, orderNumber);
                stmt.executeUpdate();
                if(osPlatform >1 && callerId>0){
                    if(mobileUserId <1){
                        mobileUserId              = getMobileUserId(callerId);
                    }
                    addUserHistory(callerId, "receiveOrder", locationId, "Received Order ", mobileUserId,orderNumber);
                } else {
                    logger.portalDetail(callerId, "receiveOrder", locationId, "order", orderNumber, "Received order #" + orderNumber, transconn);
                }
                

                //update qty on hand
                stmt = transconn.prepareStatement(selectDetail);
                stmt.setInt(1, orderNumber);
                rs = stmt.executeQuery();

                while (rs.next()) {
                    float quantity = rs.getFloat(1);
                    int productId = rs.getInt(2);

                    stmt = transconn.prepareStatement(getInventory);
                    stmt.setInt(1, productId);
                    stmt.setInt(2, locationId);
                    rs2 = stmt.executeQuery();
                    if (rs2.next()) {
                        int invId = rs2.getInt(1);

                        stmt = transconn.prepareStatement(updateInventory);
                        stmt.setFloat(1, quantity);
                        stmt.setInt(2, invId);
                        stmt.executeUpdate();
                        String logMessage = "Added " + twoPlaces.format(quantity) + " kegs to product " + productId;
                         if(osPlatform >1 && callerId>0){
                             if(mobileUserId <1){
                                 mobileUserId              = getMobileUserId(callerId);
                             }
                             addUserHistory(callerId, "updateInventory", locationId, "Updated Inventory Kegs "+ twoPlaces.format(quantity) , mobileUserId,invId);
                         } else {
                             logger.portalDetail(callerId, "updateInventory", locationId, "inventory", invId, logMessage, transconn);
                         }
                    } else {
                        addErrorDetail(toAppend, "Inventory not found for product " +
                                net.terakeet.soapware.handlers.report.ProductMap.staticLookup(productId, transconn));
                    }
                }
            } else {
                addErrorDetail(toAppend, "Open order number not found");
            }

        } catch (SQLException sqle) {
            logger.dbError("Database error in receiveOrder: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
            close(rs);
            close(rs2);
        }

    }

    /**  Attempts to clean up the readings database.
     */
    private void cleanUpReadings(Element toHandle, Element toAppend, SecureSession ss) throws HandlerException {
        if (!ss.getSecurityLevel().canAdmin()) {
            throw new HandlerException("Insufficent access level to run this method");
        }
        /*
         * Parameter we need:
         *  Scope:
         *    All locations
         *    Single
         *
         */

    }

    /**  Deletes a purchase by changing its status to 'canceled'
     */
    private void deleteOrder(Element toHandle, Element toAppend) throws HandlerException {
        int orderNumber = HandlerUtils.getRequiredInteger(toHandle, "orderNumber");
        int locationId = HandlerUtils.getRequiredInteger(toHandle, "locationId");
        int callerId = getCallerId(toHandle);

        PreparedStatement stmt = null;

        String update = "UPDATE purchase SET status='CANCELED' WHERE id=? AND location=? LIMIT 1";

        try {

            stmt = transconn.prepareStatement(update);
            stmt.setInt(1, orderNumber);
            stmt.setInt(2, locationId);
            stmt.executeUpdate();
            logger.portalDetail(callerId, "deleteOrder", locationId, "order", orderNumber, "Canceling order #" + orderNumber, transconn);

        } catch (SQLException sqle) {
            logger.dbError("Database error in deleteOrder: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
        }
    }

    private void placeOrder(Element toHandle, Element toAppend) throws HandlerException {

        int location = HandlerUtils.getRequiredInteger(toHandle, "locationId");
        int supplier = HandlerUtils.getRequiredInteger(toHandle, "supplierId");
        String totalString = HandlerUtils.getRequiredString(toHandle, "totalPrice");
        int callerId = getCallerId(toHandle);
        double total = 0.0;
        int purchaseId = 0;

        PreparedStatement stmt = null;
        ResultSet rs = null;

        String insertPurchase = "INSERT INTO purchase (location,supplier,total) " +
                "VALUES (?,?,?)";
        String getLastId = "SELECT LAST_INSERT_ID()";
        String insertPurchaseDetail = "INSERT INTO purchaseDetail " +
                " (purchase, product, productPlu, quantity, price) " +
                " VALUES (?,?,?,?,?) ";
        String insertPurchaseDetailMisc = "INSERT INTO purchaseDetailMisc " +
                " (purchase, product, productPlu, quantity, price) " +
                " VALUES (?,?,?,?,?) ";
        try {
            //check params
            assertForeignKey("location", location, transconn);
            assertForeignKey("supplier", supplier, transconn);
            try {
                total = Double.parseDouble(totalString);
            } catch (NumberFormatException nfe) {
                throw new HandlerException("Not a parseable decimal: " + totalString);
            }
            Iterator i = toHandle.elementIterator("product");
            while (i.hasNext()) {
                Element el = (Element) i.next();
                int productId = HandlerUtils.getRequiredInteger(el, "id");
                assertForeignKey("product", productId, transconn);
                try {
                    Double.parseDouble(HandlerUtils.getRequiredString(el, "price"));
                } catch (NumberFormatException nfe) {
                    throw new HandlerException("Not a parseable decimal: " + HandlerUtils.getRequiredString(el, "price"));
                }
            }
            i = toHandle.elementIterator("miscProduct");
            while (i.hasNext()) {
                Element el = (Element) i.next();
                int productId = HandlerUtils.getRequiredInteger(el, "id");
                assertForeignKey("miscProduct", productId, transconn);
                try {
                    Double.parseDouble(HandlerUtils.getRequiredString(el, "price"));
                } catch (NumberFormatException nfe) {
                    throw new HandlerException("Not a parseable decimal: " + HandlerUtils.getRequiredString(el, "price"));
                }
            }
            //finished checking parameters;

            //add the purchase record
            stmt = transconn.prepareStatement(insertPurchase);
            stmt.setInt(1, location);
            stmt.setInt(2, supplier);
            stmt.setDouble(3, total);
            stmt.executeUpdate();

            stmt = transconn.prepareStatement(getLastId);
            rs = stmt.executeQuery();
            if (rs.next()) {
                purchaseId = rs.getInt(1);
            } else {
                throw new HandlerException("Error: SELECT_LAST_ID didn't return a result");
            }
            toAppend.addElement("orderNumber").addText(String.valueOf(purchaseId));
            String logMessage = "Placed order # " + purchaseId;
            logger.portalDetail(callerId, "placeOrder", location, "purchase", purchaseId, logMessage, transconn);
            //add a purchaseDetail record for each product in the order
            i = toHandle.elementIterator("product");
            while (i.hasNext()) {
                Element el = (Element) i.next();
                String plu = HandlerUtils.getRequiredString(el, "plu");
                int quantity = HandlerUtils.getRequiredInteger(el, "quantity");
                int productId = HandlerUtils.getRequiredInteger(el, "id");
                String priceString = HandlerUtils.getRequiredString(el, "price");
                double price;
                try {
                    price = Double.parseDouble(priceString);
                } catch (NumberFormatException nfe) {
                    throw new HandlerException("Not a parseable decimal: " + priceString);
                }
                if(quantity >0){
                stmt = transconn.prepareStatement(insertPurchaseDetail);
                stmt.setInt(1, purchaseId);
                stmt.setInt(2, productId);
                stmt.setString(3, plu);
                stmt.setInt(4, quantity);
                stmt.setDouble(5, price);
                stmt.executeUpdate();
                }

            }
            //add a purchaseDetailMisc record for each misc product in the order
            i = toHandle.elementIterator("miscProduct");
            while (i.hasNext()) {
                Element el = (Element) i.next();
                String plu = HandlerUtils.getRequiredString(el, "plu");
                int quantity = HandlerUtils.getRequiredInteger(el, "quantity");
                int productId = HandlerUtils.getRequiredInteger(el, "id");
                String priceString = HandlerUtils.getRequiredString(el, "price");
                double price;
                try {
                    price = Double.parseDouble(priceString);
                } catch (NumberFormatException nfe) {
                    throw new HandlerException("Not a parseable decimal: " + priceString);
                }
                if(quantity > 0){
                stmt = transconn.prepareStatement(insertPurchaseDetailMisc);
                stmt.setInt(1, purchaseId);
                stmt.setInt(2, productId);
                stmt.setString(3, plu);
                stmt.setInt(4, quantity);
                stmt.setDouble(5, price);
                stmt.executeUpdate();
                }

            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
            close(rs);
        }
    }

    private void updateOrder(Element toHandle, Element toAppend) throws HandlerException {
        int location = HandlerUtils.getRequiredInteger(toHandle, "locationId");
        int purchase = HandlerUtils.getRequiredInteger(toHandle, "orderNumber");
        int callerId = getCallerId(toHandle);        
        int osPlatform                      = HandlerUtils.getOptionalInteger(toHandle, "osPlatform");
        int mobileUserId                    = HandlerUtils.getOptionalInteger(toHandle, "mobileUserId");  

        String updateProduct = "UPDATE purchaseDetail SET quantity=? WHERE purchase=? AND product=?";
        String updateMisc = "UPDATE purchaseDetailMisc SET quantity=? WHERE purchase=? AND product=?";
        String delProduct = "DELETE FROM purchaseDetail WHERE purchase=? AND product=?";
        String delMisc = "DELETE FROM purchaseDetailMisc WHERE purchase=? AND product=?";

        PreparedStatement stmt = null;
        try {
            Iterator i = toHandle.elementIterator("product");
            while (i.hasNext()) {
                Element prEl = (Element) i.next();
                int prId = HandlerUtils.getRequiredInteger(prEl, "id");
                int quan = HandlerUtils.getRequiredInteger(prEl, "quantity");
                if (quan > 0) {
                    stmt = transconn.prepareStatement(updateProduct);
                    stmt.setInt(1, quan);
                    stmt.setInt(2, purchase);
                    stmt.setInt(3, prId);
                } else {
                    stmt = transconn.prepareStatement(delProduct);
                    stmt.setInt(1, purchase);
                    stmt.setInt(2, prId);
                }
                stmt.executeUpdate();               
                
                logger.portalDetail(callerId, "updateOrder", location, "purchase", purchase, "Updated Pr#" + prId + " to " + quan, transconn);
                
            }
            
            if(osPlatform >1 && callerId>0){
                if(mobileUserId <1){
                    mobileUserId              = getMobileUserId(callerId);
                }                    
                addUserHistory(callerId, "updateOrder", location, "update Order ", mobileUserId,purchase);
            }
            
            i = toHandle.elementIterator("miscProduct");
            while (i.hasNext()) {
                Element prEl = (Element) i.next();
                int prId = HandlerUtils.getRequiredInteger(prEl, "id");
                int quan = HandlerUtils.getRequiredInteger(prEl, "quantity");
                if (quan > 0) {
                    stmt = transconn.prepareStatement(updateMisc);
                    stmt.setInt(1, quan);
                    stmt.setInt(2, purchase);
                    stmt.setInt(3, prId);
                } else {
                    stmt = transconn.prepareStatement(delMisc);
                    stmt.setInt(1, purchase);
                    stmt.setInt(2, prId);
                }
                stmt.executeUpdate();
                logger.portalDetail(callerId, "updateOrder", location, "purchase", purchase, "Updated Misc Pr#" + prId + " to " + quan, transconn);
                
            }
            
            
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
        }

    }

    /** Clear a line over a date range.  The date range must span less than 36 hours,
     *  and the lines must belond to the location submitted.
     */
    private void lineCleaning(Element toHandle, Element toAppend) throws HandlerException {

        DateTimeParameter startDate = HandlerUtils.getRequiredTimestamp(toHandle, "startDate");
        DateTimeParameter endDate = HandlerUtils.getRequiredTimestamp(toHandle, "endDate");
        int locationId = HandlerUtils.getRequiredInteger(toHandle, "locationId");
        int callerId = getCallerId(toHandle);

        // A cache of product data delay sets (maps product -> datadelay hour)
        PreparedStatement lineStmt, retiredLinesStmt = null;
        ResultSet rs = null;

        String checkLine =
                " SELECT system.location " +
                " FROM system LEFT JOIN line ON line.system=system.id " +
                " WHERE line.id=? ";
        
        String selectRetiredLines = "SELECT l.id FROM line AS l LEFT JOIN system s ON s.id = l.system " +
                " WHERE s.location = ? AND l.statusChange > ? AND l.status = 'RETIRED' ";

        // check to make sure the date range is in sequence, and spans no more than 36 hours
        long diff = endDate.toDate().getTime() - startDate.toDate().getTime();
        final long thirtySixHours = 1000 * 60 * 60 * 36;
        if (diff < 0) {
            addErrorDetail(toAppend, "The start date must be earlier than the end date.");
        } else if (diff > thirtySixHours) {
            addErrorDetail(toAppend, "The maximum supported line cleaning interval is 36 hours.");
        } else {
            // Proceed with the line reset
            try {

                lineStmt = transconn.prepareStatement(checkLine);
                retiredLinesStmt = transconn.prepareStatement(selectRetiredLines);

                Iterator i = toHandle.elementIterator("line");
                int cleanCount = 0;

                String startString = startDate.toString();
                String endString = endDate.toString();

                //Cleaning active lines
                // customers of type=2 use the KegLine lookup
                while (i.hasNext()) {
                    // Parse the Line ID
                    Element lineEl = (Element) i.next();
                    int lineId = 0;
                    String lineString = lineEl.getTextTrim();

                    try {
                        lineId = Integer.valueOf(lineString).intValue();
                    } catch (NumberFormatException nfe) {
                        logger.debug("Unabled to parse line: " + lineString);
                        addErrorDetail(toAppend, "There was a problem clearing line #" + lineString);
                    }

                    if (lineId > 0) {
                        lineStmt.setInt(1, lineId);
                        rs = lineStmt.executeQuery();
                        if (rs.next()) {
                            int lineLocation = rs.getInt(1);
                            if (lineLocation == locationId) {
                                cleanCount = cleanLine(toHandle, callerId, locationId, lineId, cleanCount);
                            } else {
                                logger.portalAccessViolation(
                                    "Tried to line-clean " + lineId + " (Loc " + lineLocation + ") " +
                                    "from location " + locationId + " by user " + callerId);
                            }
                        }
                    }
                }
                logger.debug("Cleaned " + cleanCount + " active lines");

                //Cleaning retired lines
                cleanCount = 0;
                retiredLinesStmt.setInt(1, locationId);
                retiredLinesStmt.setString(2, startString);
                rs = retiredLinesStmt.executeQuery();
                while (rs.next()) {
                    cleanCount = cleanLine(toHandle, callerId, locationId, rs.getInt(1), cleanCount);
                }
                logger.debug("Cleaned " + cleanCount + " retired lines");
            } catch (SQLException sqle) {
                logger.dbError("Database error in lineCleaning: " + sqle.getMessage());
                throw new HandlerException(sqle);
            }
        }
    }

    private int cleanLine(Element toHandle, int callerId, int locationId, int lineId, int cleanCount) throws HandlerException {
        
        DateTimeParameter startDate         = HandlerUtils.getRequiredTimestamp(toHandle, "startDate");
        DateTimeParameter endDate           = HandlerUtils.getRequiredTimestamp(toHandle, "endDate");
        int pourOff                         = HandlerUtils.getRequiredInteger(toHandle, "pourOff");
        int volInclusion                    = HandlerUtils.getRequiredInteger(toHandle, "volInclusion");

        int invId                           = 0, product = 0;
        double kegSize                      = 1920.0;
        double kegDifference                = 0.0;
        double ozDifference                 = 0.0;
        Double offset                       = 0.0;
        SimpleDateFormat newDateFormat      = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String selectMaxReading             = " SELECT MAX(value) FROM reading WHERE line = ? AND date BETWEEN ? AND ? AND type = 0";
        String updateReading                = " UPDATE reading SET type = 1, date = date WHERE line = ? AND date BETWEEN ? AND ?";
        String insertSpike                  = " INSERT INTO reading (line,date,value) VALUES (?,?,-1)";
        String insertMaxValue               = " INSERT INTO reading (line,date,value) VALUES (?,(? + INTERVAL 10 SECOND),?)";
        String insertLCSpike                = " INSERT INTO reading (line,date,value,type) VALUES (?,(? + INTERVAL 30 SECOND),-1,1)";
        String insertLCSpikeCover           = " INSERT INTO reading (line,date,value,type) VALUES (?,(? - INTERVAL 30 SECOND),-1,1)";
        String insertDataMod                = " INSERT INTO dataModNew (location, modType, modId, start, end, date) VALUES (?,?,?,?,(? + INTERVAL 1 HOUR),?);";
        String selectInventory              = " SELECT inv.id, inv.kegSize, inv.product, lo.easternOffset FROM inventory AS inv LEFT JOIN line ON inv.product = line.product " +
                                            " LEFT JOIN location lo ON lo.id = inv.location WHERE line.id=? AND inv.location=?";
        String updateInventory              = " UPDATE inventory SET qtyOnHand=qtyOnHand+? WHERE id=?";

        PreparedStatement selectMaxStmt, insertDatMod, updateStmt, insertStmt, insertMaxStmt, insertLCStmt, inventoryStmt, updateInvStmt, insertLCCoverStmt
                                            = null;
        ResultSet rs                        = null;

        String startString                  = newDateFormat.format(startDate.toDate());
        String endString                    = newDateFormat.format(endDate.toDate());

        try {
            selectMaxStmt                   = transconn.prepareStatement(selectMaxReading);
            updateStmt                      = transconn.prepareStatement(updateReading);
            insertStmt                      = transconn.prepareStatement(insertSpike);
            insertMaxStmt                   = transconn.prepareStatement(insertMaxValue);
            insertLCStmt                    = transconn.prepareStatement(insertLCSpike);
            insertLCCoverStmt               = transconn.prepareStatement(insertLCSpikeCover);
            inventoryStmt                   = transconn.prepareStatement(selectInventory);
            updateInvStmt                   = transconn.prepareStatement(updateInventory);
            insertDatMod                    = transconn.prepareStatement(insertDataMod);

            //get total ounces poured during line cleaning
            ozDifference                    = getPoured(lineId, "Daily", "0", startString, endString);
            //logger.debug("Total Line Cleaning Poured: " + String.valueOf(ozDifference));

            //get the inventory ID and the keg-size for the inventory
            inventoryStmt.setInt(1, lineId);
            inventoryStmt.setInt(2, locationId);
            rs                              = inventoryStmt.executeQuery();
            if (rs.next()) {
                invId                       = rs.getInt(1);
                kegSize                     = rs.getDouble(2);
                product                     = rs.getInt(3);
                offset                      = rs.getDouble(4);
                if (kegSize > 1.0) {
                    kegDifference           = ozDifference / kegSize;
                }
                //logger.debug("Keg Equalant of line cleaning poured: " + String.valueOf(kegDifference));
            }

            //Update the quantity on hand for the inventory item
            if ((invId > 0) && (kegDifference > 0.0) && (pourOff == 0)) {
                updateInvStmt.setDouble(1, kegDifference);
                updateInvStmt.setInt(2, invId);
                updateInvStmt.executeUpdate();
                //logger.debug("Updating Inventory for item : " + String.valueOf(invId));
            }

            if (volInclusion == 0) {
                //logger.debug("Line Cleaing volume adjustment for item : " + String.valueOf(invId));
                cleanCount++;                
                
                selectMaxStmt.setInt(1, lineId);
                selectMaxStmt.setString(2, startString);
                selectMaxStmt.setString(3, endString);
                rs                          = selectMaxStmt.executeQuery();
                
                //update the line cleaning reading from the readings table
                updateStmt.setInt(1, lineId);
                updateStmt.setString(2, startString);
                updateStmt.setString(3, endString);
                updateStmt.executeUpdate();


                //Inserting the forced spike '-1.00' to identify line cleaning
                insertStmt.setInt(1, lineId);
                insertStmt.setString(2, startString);
                insertStmt.executeUpdate();

                insertLCStmt.setInt(1, lineId);
                insertLCStmt.setString(2, startString);
                insertLCStmt.executeUpdate();

                //Inserting the spike cover 'max value' to adjust the next pour
                insertStmt.setInt(1, lineId);
                insertStmt.setString(2, endString);
                insertStmt.executeUpdate();

                insertLCCoverStmt.setInt(1, lineId);
                insertLCCoverStmt.setString(2, endString);
                insertLCCoverStmt.executeUpdate();
                
                if (rs.next()) {
                    if(rs.getDouble(1) > 0) {
                        insertMaxStmt.setInt(1, lineId);
                        insertMaxStmt.setString(2, endString);
                        insertMaxStmt.setDouble(3, rs.getDouble(1));
                        insertMaxStmt.executeUpdate();
                    }
                }
                
                logger.portalDetail(callerId, "lineClean", locationId, "line", lineId, "Cleaned line " + lineId + " from " + startString + " to " + endString, transconn);

                java.util.Date busStart = new java.util.Date();

                busStart                    = startDate.toDate();
                busStart.setHours(7 - offset.intValue());
                busStart.setMinutes(0);
                busStart.setSeconds(0);

                DateTimeParameter startLineCleaningDate
                                            = startDate;
                DateTimeParameter endLineCleaningDate
                                            = endDate;

                startLineCleaningDate.setMinute(0);
                startLineCleaningDate.setSeconds(0);

                if ((startDate.toDate().before(busStart)) && (endDate.toDate().after(busStart))) {

                    endLineCleaningDate     = toDateTimeParameter(busStart);
                    endLineCleaningDate.addHour(-1);

                    insertDatMod.setInt(1, locationId);
                    insertDatMod.setInt(2, 1);
                    insertDatMod.setInt(3, product);
                    insertDatMod.setString(4, startLineCleaningDate.toString());
                    insertDatMod.setString(5, endLineCleaningDate.toString());
                    insertDatMod.setString(6, startLineCleaningDate.toString());
                    insertDatMod.executeUpdate();

                    startLineCleaningDate   = toDateTimeParameter(busStart);
                    endLineCleaningDate     = endDate;
                }

                endLineCleaningDate.setMinute(0);
                endLineCleaningDate.setSeconds(0);


                insertDatMod.setInt(1, locationId);
                insertDatMod.setInt(2, 1);
                insertDatMod.setInt(3, product);
                insertDatMod.setString(4, startLineCleaningDate.toString());
                insertDatMod.setString(5, endLineCleaningDate.toString());
                insertDatMod.setString(6, startLineCleaningDate.toString());
                insertDatMod.executeUpdate();
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error in lineCleaning: " + sqle.getMessage());
            throw new HandlerException(sqle);
        }
        return cleanCount;
    }

    private double getPoured(Integer lineId, String periodStr, String periodDetail, String startTime, String endTime) throws HandlerException {

        java.text.DecimalFormat twoDForm    = new java.text.DecimalFormat("#.##");
        java.text.DateFormat timeParse      = new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
        double totalPoured                  = 0.00;

        logger.portalAction("getPoured");

        try {
            java.util.Date start            = timeParse.parse(startTime);
            java.util.Date end              = timeParse.parse(endTime);

            PeriodType periodType = PeriodType.parseString(periodStr);
            if (null == periodType) {
                throw new HandlerException("Invalid period type: " + periodStr);
            }
            //For Testing purposes change the below date.
            //start = timeParse.parse("2009-06-09 08:00:00");
            //end = timeParse.parse("2009-06-10 08:00:00");
            ReportPeriod period = null;
            try {
                period = new ReportPeriod(periodType, periodDetail, start, end);
            } catch (IllegalArgumentException e) {
                throw new HandlerException(e.getMessage());
            }

            SortedSet<DatePartition> dps = DatePartitionFactory.createPartitions(period);
            //logger.debug("Created partitions: \n"+DatePartitionFactory.partitionReport(dps));
            DatePartitionTree dpt = new DatePartitionTree(dps);

            ReportResults rrs = null;
            if (lineId >= 0) {
                rrs = ReportResults.getResultsByLine(period, 0, false, lineId, transconn);
            }
            PeriodStructure pss[] = null;
            PeriodStructure ps = null;
            int dpsSize = dps.size();
            int index;
            if (dpsSize > 0) {
                pss = new PeriodStructure[dpsSize];
                //this is a temporary array of the DatePartitions used to construct the PeriodStructures start dates
                Object[] dpa = dps.toArray();
                for (int i = 0; i < dpsSize; i++) {
                    // create a new PeriodStructure and link it to the previous one (or null for the first)
                    ps = new PeriodStructure(ps, ((DatePartition) dpa[i]).getDate());
                    pss[i] = ps;
                }
                int debugCounter = 0;
                while (rrs.next()) {
                    index = dpt.getIndex(rrs.getDate());
                    pss[index].addReading(rrs.getLine(), rrs.getValue(), rrs.getDate(), rrs.getQuantity());
                    debugCounter++;
                    //logger.debug("#"+debugCounter+" ["+index+"]: "+"L "+rrs.getLine()+" V: "+rrs.getValue()+" D: "+rrs.getDate().toString());
                }
                rrs.close();
                logger.debug("Processed " + debugCounter + " readings");
                for (int i = 0; i < dpsSize; i++) {
                    Map<Integer, Double> lineMap = pss[i].getValues(transconn);
                    if (null != lineMap && lineMap.size() > 0) {
                        for (Integer key : lineMap.keySet()) {
                            totalPoured += Double.parseDouble(twoDForm.format(lineMap.get(key)));
                            //logger.debug("For Line : " + key + " poured = " + String.valueOf(lineMap.get(key)));
                        }
                    }
                }
                logger.debug("Total Poured = " + twoDForm.format(totalPoured));
                ReportResults.clearLineCache();
            }
        } catch (ParseException pe) {
            String badDate = (null == startTime) ? "start" : "end";
            throw new HandlerException("Could not parse " + badDate + " date.");
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
        }
        return totalPoured;
    }

    /**  Draft lines keep a total poured counter.
     *  This function will reset one or more counters.
     *  To reset individual lines, pass ids <line><id>100</id></line><line><id>200</id></line>
     *  To reset all lines this location pass <resetAllLines>1</resetAllLines> (must be 1)
     */
    private void resetLineCounters(Element toHandle, Element toAppend) throws HandlerException {

        int location = HandlerUtils.getRequiredInteger(toHandle, "locationId");
        boolean resetAll = (HandlerUtils.getOptionalInteger(toHandle, "resetAllLines") == 1 ? true : false);
        int callerId = getCallerId(toHandle);

        String singleReset = "UPDATE line LEFT JOIN bar ON line.bar = bar.id " +
                "  SET line.ouncesPoured=0.0 " +
                "  WHERE bar.location=? AND line.id=?";
        String multipleReset = "UPDATE line LEFT JOIN bar ON line.bar = bar.id " +
                "  SET line.ouncesPoured=0.0 " +
                "  WHERE bar.location=?";

        PreparedStatement stmt = null;

        if (resetAll) {
            try {
                stmt = transconn.prepareStatement(multipleReset);
                stmt.setInt(1, location);
                stmt.executeUpdate();
                logger.portalDetail(callerId, "resetLineCounter", location, "Reset all draft lines", transconn);
            } catch (SQLException sqle) {
                logger.dbError("Database error: " + sqle.getMessage());
                throw new HandlerException(sqle);
            } finally {
                close(stmt);
            }
        } else {
            Iterator i = toHandle.elementIterator("line");
            while (i.hasNext()) {
                try {
                    Element lineEl = (Element) i.next();
                    int lineId = HandlerUtils.getRequiredInteger(lineEl, "id");
                    stmt = transconn.prepareStatement(singleReset);
                    stmt.setInt(1, location);
                    stmt.setInt(2, lineId);
                    stmt.executeUpdate();
                    logger.portalDetail(callerId, "resetLineCounter", location, "line", lineId, "Reset draft line #" + lineId, transconn);
                } catch (Exception e) {
                    addErrorDetail(toAppend, "Unable to reset a counter");
                }
            }
            close(stmt);
        }

    }

    /** Update the draft line unit(oz) for every line at a location.
     *  The line unit must be in the range [1.0,2000.0]
     */
    private void updateLineUnit(Element toHandle, Element toAppend) throws HandlerException {
        int location = HandlerUtils.getRequiredInteger(toHandle, "locationId");
        int callerId = getCallerId(toHandle);
        float unit = 0;
        try {
            unit = Float.parseFloat(HandlerUtils.getRequiredString(toHandle, "unitOunces"));
        } catch (NumberFormatException nfe) {
        }

        DecimalFormat twoPlaces = new DecimalFormat("0.00");

        String update = "UPDATE line LEFT JOIN bar ON line.bar = bar.id " +
                "  SET line.unit=? " +
                "  WHERE bar.location=? ";

        if (unit < 1 || unit > 2000) {
            addErrorDetail(toAppend, "Unit Ounces must be between 1.0 and 2000");
        } else {
            PreparedStatement stmt = null;
            try {
                stmt = transconn.prepareStatement(update);
                stmt.setFloat(1, unit);
                stmt.setInt(2, location);
                stmt.executeUpdate();
                String logMessage = "Updated all line units to " + twoPlaces.format(unit);
                logger.portalDetail(callerId, "updateLineUnit", location, logMessage, transconn);
            } catch (SQLException sqle) {
                logger.dbError("Database error: " + sqle.getMessage());
                throw new HandlerException(sqle);
            } finally {
                close(stmt);
            }
        }
    }

    /**  This will change the product that is being poured on a line.  If readings
     *  exist for the old product, the old line will be retired and a new line created.
     *
     *  This method can now also update the bar to which this line is pouring.
     *
     *  To clear a line, send product id 0.
     */
    private void updateLineProduct(Element toHandle, Element toAppend) throws HandlerException {
        int location                        = HandlerUtils.getRequiredInteger(toHandle, "locationId");
        int callerId                        = getCallerId(toHandle);
        int osPlatform                      = HandlerUtils.getOptionalInteger(toHandle, "osPlatform");
        int mobileUserId                    = HandlerUtils.getOptionalInteger(toHandle, "mobileUserId");  

        String selectPLURetirement          = "SELECT id FROM locationDetails WHERE location = ? AND pluRetirement = 1;";
        String selectExisting               = "SELECT l.lineIndex, l.product, l.system, system.systemId, l.bar, l.station, " +
                                            " l.status, l.unit, l.local, l.advertise, l.qtyOnHand, l.cask, l.lineNo, " +
                                            " (SELECT kegSize from inventory WHERE location= system.location AND product=l.product ORDER BY id LIMIT 1), l.onDeck " +
                                            " FROM line AS l LEFT JOIN system ON l.system = system.id " +
                                            " WHERE l.id=? AND system.location=? ";
        String selectReadings               = "SELECT id FROM reading WHERE line = ? AND value > 1 AND type = 0 LIMIT 1";
        String selectSocialMediaID          = "SELECT id FROM usbnFacebook WHERE location = ?;";
        String selectLineUpdates            = "SELECT id FROM lineUpdates WHERE location = ? AND product = ? AND date = ?;";
        String selectInventory              = "SELECT id FROM inventory WHERE location=? AND product = ? ";
        String selectBeerBoardAccess        = "SELECT id FROM locationBeerBoardMap WHERE location=? ";
        String getLastId                    = "SELECT LAST_INSERT_ID()";

        String insertInventory              = "INSERT INTO inventory (location, product) VALUES (?, ?);";
        String insertDuplicate              = "INSERT INTO line (lineIndex, product, onDeck, ouncesPoured, system, bar, station, status, unit, lineNo, statusChange) " +
                                            " VALUES (?, ?, ?, -1.00, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP) ";
        String insertLineUpdates            = "INSERT INTO lineUpdates (location, line, product, date) VALUES (?, ?, ?, ?);";
        String update                       = "UPDATE line SET product=?, bar=?, station=?, local=?, advertise = ?, "
                                            + " cask= ?, qtyOnHand = ?, ouncesPoured=-1.00, statusChange = CURRENT_TIMESTAMP "
                                            + " WHERE id = ?";
        String updateStatus                 = "UPDATE line SET status=?, local=? WHERE id= ?";
        String updateOnDeck                 = "UPDATE line SET onDeck=? WHERE id = ?";
        String updateLocal                  = "UPDATE line SET local=? WHERE id= ?";
        String updateAdvertise              = "UPDATE line SET advertise=? WHERE id= ?";
        String updateCask                   = "UPDATE line SET cask=? WHERE id= ?";
        String updateResetLevel             = "UPDATE line SET qtyOnHand=? WHERE id= ?";
        String retire                       = "UPDATE line SET status='RETIRED', statusChange = CURRENT_TIMESTAMP WHERE id=?";
        String updateCooler                 = "UPDATE kegLine SET cooler=? WHERE id=?";
        String updateKegLine                = "UPDATE line SET kegLine=?, local=? WHERE id=?";
        String updateLineEmpty              = "UPDATE line SET product=null WHERE id=?";
        String updateKegSize                = "UPDATE inventory SET kegSize=?  WHERE location=? AND product = ?";
        String insertNewBeer                = "INSERT INTO newBeerList(location,product,assignTime) VALUES (?, ?, now());";
        
        //To update all active and inactive products when lines are updated
        ArrayList<Integer> newActiveProducts= new ArrayList();
        ArrayList<Integer> inactiveProducts = new ArrayList();

        PreparedStatement stmt              = null;
        ResultSet rs                        = null;

        try {         
            boolean readOnly                = false, pluRetirement = false;
            if(mobileUserId >0 || osPlatform >0){
                  stmt                      = transconn.prepareStatement("SELECT id FROM userMap  WHERE user=? AND location=? AND securityLevel=7;");                    
                  stmt.setInt(1,callerId);
                  stmt.setInt(2,location);
                  rs                        = stmt.executeQuery();
                  if (rs.next()) {
                      readOnly              = true;
                  }
            }
            Map<Integer, String> lineNoMap = new HashMap<Integer, String>();
            if(callerId > 0 && location > 0 && isValidAccessUser(callerId, location,false) && !readOnly){
                
            //PLU Retirement    
            stmt                            = transconn.prepareStatement(selectPLURetirement);                    
            stmt.setInt(1,location);
            rs                              = stmt.executeQuery();
            if (rs.next()) {
                pluRetirement               = true;
            }
            
            //Line Updates
            Iterator lineIterator           = toHandle.elementIterator("line");
            while (lineIterator.hasNext()) {
                Element line                = (Element) lineIterator.next();
                int lineId                  = HandlerUtils.getRequiredInteger(line, "id");
                int product                 = HandlerUtils.getRequiredInteger(line, "product");
                int onDeck                  = HandlerUtils.getOptionalInteger(line, "onDeck");
                int local                   = HandlerUtils.getOptionalInteger(line, "local");
                int newBar                  = HandlerUtils.getOptionalInteger(line, "barId");
                int newStation              = HandlerUtils.getOptionalInteger(line, "stationId");
                int newCoolerId             = HandlerUtils.getOptionalInteger(line, "coolerId");
                int newKegLineId            = HandlerUtils.getOptionalInteger(line, "kegLineId");                
                int advertise               = HandlerUtils.getOptionalInteger(line, "advertise");
                String strResetLevel        = HandlerUtils.getOptionalString(line, "resetLevel");
                int kegSize                 = HandlerUtils.getOptionalInteger(line, "kegSize");
                int cask                    = HandlerUtils.getOptionalInteger(line, "cask");
                String lineNo               = HandlerUtils.getOptionalString(line, "lineNo");
                String productName          = HandlerUtils.getOptionalString(line, "productName");
                if(lineNo!=null && !lineNo.equals("0") && lineNo.length() > 0 && lineNoMap.containsValue(lineNo)) {
                    lineNo                  = lineNo.trim();
                    logger.debug(lineNo+" Line No Already assigned ");
                    addErrorDetail(toAppend, "Duplicate Line NO '" + lineNo + "' was not Updated '");
                } else {
                if (onDeck < 0) {
                    onDeck                  = product;
                }
                
                if(product == 0 && productName!=null && !productName.equals("")) {
                    product                 = addProduct(productName.trim(), callerId, location);
                }
                
                if(strResetLevel == null || strResetLevel.equals("")) {
                    strResetLevel           = "0";
                }
                double resetLevel           = Double.parseDouble(strResetLevel);
                
                if(product == 4864 || product == 0) {
                    product                 = 4311;
                }
                if(onDeck == 4864 || onDeck == 0) {
                    onDeck                  = 4311;
                }
                
                if (checkForeignKey("product", product, transconn)) {

                    stmt                    = transconn.prepareStatement(selectInventory);
                    stmt.setInt(1, location);
                    stmt.setInt(2, product);
                    rs                      = stmt.executeQuery();
                    if ((product > 0) && !rs.next()) {
                        //Adding into inventory
                        stmt                = transconn.prepareStatement(insertInventory);
                        stmt.setInt(1, location);
                        stmt.setInt(2, product);
                        stmt.executeUpdate();
                    }

                    stmt                    = transconn.prepareStatement(selectInventory);
                    stmt.setInt(1, location);
                    stmt.setInt(2, onDeck);
                    rs                      = stmt.executeQuery();
                    if ((product > 0) && !rs.next()) {
                        //Adding into inventory
                        stmt                = transconn.prepareStatement(insertInventory);
                        stmt.setInt(1, location);
                        stmt.setInt(2, onDeck);
                        stmt.executeUpdate();
                    }

                    stmt                    = transconn.prepareStatement("UPDATE brasstapLocations SET lineUpdate = 1 WHERE usbnID = ?");
                    stmt.setInt(1, location);
                    stmt.executeUpdate();

                    stmt                    = transconn.prepareStatement(selectExisting);
                    stmt.setInt(1, lineId);
                    stmt.setInt(2, location);
                    rs                      = stmt.executeQuery();
                    if (rs.next()) {
                        int lineIndex       = rs.getInt(1);
                        int existingProduct = rs.getInt(2);
                        int system          = rs.getInt(3);
                        int systemID        = rs.getInt(4);
                        int existingBar     = rs.getInt(5);
                        int existingStation = rs.getInt(6);
                        String status       = rs.getString(7);
                        double unit         = rs.getDouble(8);
                        int existingLocal   = rs.getInt(9);
                        int existingAd      = rs.getInt(10);
                        double existingRLevel
                                            = rs.getDouble(11);
                        int existingCask    = rs.getInt(12);
                        String oldLineNo    = rs.getString(13);
                        int existingKegSize = rs.getInt(14);
                        int existingOnDeck  = rs.getInt(15);
                        int barToUse        = newBar > 0 ? newBar : existingBar;
                        int stationToUse    = newStation > 0 ? newStation : existingStation;
                        
                        
                        if(local == -1){
                            local           = existingLocal;
                        }
                        if(resetLevel == -1) {
                            resetLevel  = existingRLevel;
                        }
                        if(advertise == -1){
                            advertise       = existingAd;
                        }
                        if(kegSize==-1){                            
                            kegSize         = existingKegSize;    
                            
                        }
                        if(cask == -1){
                            cask            = existingCask;
                        }
                        
                        //logger.debug("Existing Product : " + existingProduct + ", New Product" + product);
                        //logger.debug("Existing On-Deck : " + existingOnDeck + ", New On-Deck" + onDeck);
                        if (existingProduct != product) {
                           // logger.debug("Changed Product:"+existingProduct + ":"+product);
                            resetLevel      = 1.0;
                            cask            = 0;
                            local           = 0;
                            advertise       = 0;
                            existingKegSize = 0;
                            kegSize         = 1984;
                            if ("RETIRED".equals(status)) {
                                // not allowed to change the product of a retired line
                                //logger.debug("System " + systemID + " Line " + (lineIndex + 1) + " is retired");
                                addErrorDetail(toAppend, "Can't change a retired line: " + "System " + systemID + " Line " + (lineIndex + 1));
                            } else {
                                String logMessage
                                            = "Changing Sys/Line " + systemID + "/" + (lineIndex + 1) + " from " + net.terakeet.soapware.handlers.report.ProductMap.staticLookup(existingProduct, transconn) + " to " + net.terakeet.soapware.handlers.report.ProductMap.staticLookup(product, transconn);
                                logger.portalDetail(callerId, "updateLine", location, "line", lineId, logMessage, transconn);
                                // we need to check if there are existing readings
                                stmt        = transconn.prepareStatement(selectReadings);
                                stmt.setInt(1, lineId);
                                rs          = stmt.executeQuery();
                                if (rs.next()) {
                                    //This means readings exist, so we must retire the old line
                                    //logger.debug("Retiring line " + lineId);
                                    stmt    = transconn.prepareStatement(retire);
                                    stmt.setInt(1, lineId);
                                    stmt.executeUpdate();


                                    // Product #0 means clear the line
                                    if (product == 0) {
                                        status
                                            = "EMPTY";
                                        //logger.debug("Clearing line #" + lineId);
                                    }

                                    // If the line was empty, set it to running
                                    if ("EMPTY".equals(status) && product > 0) {
                                        //logger.debug("Turning on line #" + lineId);
                                        status
                                            = "RUNNING";
                                        newActiveProducts.add(product);
                                        inactiveProducts.add(existingProduct);
                                    }

                                    //Create a new line
                                    stmt    = transconn.prepareStatement(insertDuplicate);
                                    stmt.setInt(1, lineIndex);
                                    stmt.setInt(2, product);
                                    stmt.setInt(3, onDeck);
                                    stmt.setInt(4, system);
                                    stmt.setInt(5, barToUse);
                                    stmt.setInt(6, stationToUse);
                                    stmt.setString(7, status);
                                    stmt.setDouble(8, unit);
                                    stmt.setString(9, oldLineNo);
                                    stmt.executeUpdate();

                                    // get the id of the line we added
                                    stmt    = transconn.prepareStatement(getLastId);
                                    rs      = stmt.executeQuery();
                                    if (rs.next()) {
                                        lineId  = rs.getInt(1);                                        
                                    } else {
                                        logger.dbError("SQL Last_Insert_Id FAILED in updateLineProduct");
                                        throw new HandlerException("database error");
                                    }
                                    //logger.debug("Turning on line #" + lineId);
                                } else {
                                    // OK to do a straight update
                                    if (checkDatabaseAssociation("bar", barToUse, "location", location, transconn)) {
                                        //logger.debug("Updating System " + systemID + " Line " + (lineIndex + 1) + " with product " + net.terakeet.soapware.handlers.report.ProductMap.staticLookup(product, transconn));
                                        stmt= transconn.prepareStatement(update);
                                        stmt.setInt(1, product);
                                        stmt.setInt(2, barToUse);
                                        stmt.setInt(3, stationToUse);
                                        stmt.setInt(4, 0);
                                        stmt.setInt(5, 0);
                                        stmt.setInt(6, 0);
                                        stmt.setDouble(7, 1);
                                        stmt.setInt(8, lineId);
                                        stmt.executeUpdate();

                                        String newStatus
                                            = null;
                                        //If product is 0, we need to clear the line
                                        if (product == 0) {
                                            //logger.debug("Clearing line #" + lineId);
                                            newStatus
                                            = "EMPTY";
                                            //If the line was empty, turn it on
                                        } else if ("EMPTY".equals(status)) {
                                            //logger.debug("Turning on line #" + lineId);
                                            newStatus
                                            = "RUNNING";
                                            newActiveProducts.add(product);
                                        }
                                        if (newStatus != null) {
                                            //logger.debug("Updating status to " + newStatus);
                                            stmt
                                            = transconn.prepareStatement(updateStatus);
                                            stmt.setString(1, newStatus);
                                            stmt.setInt(2, local);
                                            stmt.setInt(3, lineId);
                                            stmt.executeUpdate();
                                        }
                                    } else {
                                        logger.portalAccessViolation("User " + callerId + " tried to change System " + systemID + " Line " + (lineIndex + 1));
                                        addErrorDetail(toAppend, "Invalid bar");
                                    }
                                }
                            }

                            int updateCount = 0;
                            stmt            = transconn.prepareStatement("SELECT SUM(value) FROM openHoursSummary "
                                            + " WHERE location = ? AND product = ? AND date > (CURDATE() - INTERVAL 7 DAY) "
                                            + " GROUP BY product");
                            stmt.setInt(1, location);
                            stmt.setInt(2, product);
                            rs              = stmt.executeQuery();
                            if (product != 4311 && (!rs.next()) && updateCount < 3 && product > 0) {
                                updateCount++;
                                SimpleDateFormat dateFormat
                                            = new SimpleDateFormat("yyyy-MM-dd");
                                java.util.Date date
                                            = new java.util.Date();
                                stmt        = transconn.prepareStatement(selectLineUpdates);
                                stmt.setInt(1, location);
                                stmt.setInt(2, product);
                                stmt.setString(3, dateFormat.format(date));
                                rs          = stmt.executeQuery();
                                if (!rs.next()) {
                                    stmt     = transconn.prepareStatement(insertLineUpdates);
                                    stmt.setInt(1, location);
                                    stmt.setInt(2, lineId);
                                    stmt.setInt(3, product);
                                    stmt.setString(4, dateFormat.format(date));
                                    stmt.executeUpdate();
                                    if(product != 9593){
                                        generateNewBeerPushNotification(location, product);
                                    }                                    
                                }
                            }
                            
                            if (pluRetirement) {
                                Calendar now 
                                            = Calendar.getInstance();
                                DecimalFormat decFormat 
                                            = new DecimalFormat("00");
                                String today 
                                            = decFormat.format(now.get(Calendar.DAY_OF_MONTH)) + decFormat.format((now.get(Calendar.MONTH) + 1)) + String.valueOf(now.get(Calendar.YEAR)).substring(2, 4);
                                stmt        = transconn.prepareStatement("SELECT b.id, b.plu, b.dateAdded FROM beverage b LEFT JOIN ingredient i ON i.beverage = b.id "
                                            + " WHERE b.location = ? AND i.product = ? AND b.plu NOT LIKE '%-%'");
                                stmt.setInt(1, location);
                                stmt.setInt(2, existingProduct);
                                rs          = stmt.executeQuery();
                                while (rs.next()) {
                                    stmt    = transconn.prepareStatement("UPDATE beverage SET plu = ? WHERE id = ?;");
                                    stmt.setString(1, rs.getString(2) + "-" + today);
                                    stmt.setInt(2, rs.getInt(1));
                                    stmt.executeUpdate();
                                    
                                    stmt    = transconn.prepareStatement("UPDATE sales SET pluNumber = ? WHERE date >= ? AND location = ? AND pluNumber = ?;");
                                    stmt.setString(1, rs.getString(2) + "-" + today);
                                    stmt.setString(2, rs.getString(3));
                                    stmt.setInt(3, location);
                                    stmt.setString(4, rs.getString(2));
                                    stmt.executeUpdate();
                                }
                            }
                        } else if (barToUse != existingBar) { // only the bar has changed
                            if (checkDatabaseAssociation("bar", barToUse, "location", location, transconn)) {
                                String logMessage
                                            = "Changing bar for System " + systemID + " Line " + (lineIndex + 1);
                                logger.portalDetail(callerId, "updateLine", location, "line", lineId, logMessage, transconn);

                                //logger.debug("Changing Bar on line #" + lineId + " to bar " + newBar);
                                stmt        = transconn.prepareStatement(update);
                                stmt.setInt(1, existingProduct);
                                stmt.setInt(2, newBar);
                                stmt.setInt(3, newStation);
                                stmt.setInt(4, local);
                                stmt.setInt(5, advertise);
                                stmt.setInt(6, cask);
                                stmt.setDouble(7, resetLevel);
                                stmt.setInt(8, lineId);
                                stmt.executeUpdate();
                            } else {
                                logger.portalAccessViolation("User " + callerId + " tried to change line " + lineId + " to bar " + barToUse);
                                addErrorDetail(toAppend, "Invalid bar");
                            }
                        } else if (stationToUse != existingStation) { // only the station has changed
                            if (checkDatabaseAssociation("station", stationToUse, "bar", barToUse, transconn)) {
                                String logMessage
                                            = "Changing system for System " + systemID + " Line " + (lineIndex + 1);
                                logger.portalDetail(callerId, "updateLine", location, "line", lineId, logMessage, transconn);

                                logger.debug("Changing Station on line #" + lineId + " to Station " + newStation);
                                stmt        = transconn.prepareStatement(update);
                                stmt.setInt(1, existingProduct);
                                stmt.setInt(2, existingBar);
                                stmt.setInt(3, newStation);
                                stmt.setInt(4, local);
                                stmt.setInt(5, advertise);
                                stmt.setInt(6, cask);
                                stmt.setDouble(7, resetLevel);
                                stmt.setInt(8, lineId);
                                stmt.executeUpdate();
                            } else {
                                logger.portalAccessViolation("User " + callerId + " tried to change line " + lineId + " to station " + stationToUse);
                                addErrorDetail(toAppend, "Invalid Station");
                            }
                        }  
                        if (existingOnDeck != onDeck) {
                            stmt            = transconn.prepareStatement(updateOnDeck);
                            stmt.setInt(1, onDeck);
                            stmt.setInt(2, lineId);
                            stmt.executeUpdate();
                        }
                        if ((local > -1) && existingLocal != local) {
                            stmt            = transconn.prepareStatement(updateLocal);
                            stmt.setInt(1, local);
                            stmt.setInt(2, lineId);
                            stmt.executeUpdate();
                        } 
                        if ((advertise > -1) && existingAd != advertise) {
                            stmt            = transconn.prepareStatement(updateAdvertise);
                            stmt.setInt(1, advertise);
                            stmt.setInt(2, lineId);
                            stmt.executeUpdate();
                        } 
                        if ((resetLevel > -1) && existingRLevel != resetLevel) {
                            stmt            = transconn.prepareStatement(updateResetLevel);
                            stmt.setDouble(1, resetLevel);
                            stmt.setInt(2, lineId);
                            stmt.executeUpdate();
                        } 
                        
                        if ((kegSize > -1) && existingKegSize != kegSize) {
                            //logger.debug(product+":"+ kegSize);
                            stmt            = transconn.prepareStatement(updateKegSize);
                            stmt.setInt(1, kegSize);
                            stmt.setInt(2, location);
                            stmt.setInt(3, product);
                            stmt.executeUpdate();
                        } 
                        if ((cask > -1) && existingCask != cask) {
                            //logger.debug("Product:"+product +"cask:"+cask +"Ex:"+existingCask +" Line:"+lineId );
                            stmt            = transconn.prepareStatement(updateCask);
                            stmt.setInt(1, cask);
                            stmt.setInt(2, lineId);
                            stmt.executeUpdate();
                        } 
                        
                        // Updating BeerBoard TV menu
                        stmt                = transconn.prepareStatement(selectBeerBoardAccess);
                        stmt.setInt(1, location);
                        rs                  = stmt.executeQuery();
                        if (rs.next()) {
                            stmt            = transconn.prepareStatement("UPDATE locationBeerBoardMap SET css=1, api=1 WHERE location = ?");
                            stmt.setInt(1, location);
                            stmt.executeUpdate();
                        }
                    } else {
                        addErrorDetail(toAppend, "Can't update line # " + lineId);
                    }
                    if (newKegLineId > 0) {
                        stmt                = transconn.prepareStatement(updateKegLine);
                        stmt.setInt(1, newKegLineId);
                        stmt.setInt(2, local);
                        stmt.setInt(3, lineId);
                        stmt.executeUpdate();
                    }
                    if (newCoolerId > 0) {
                        stmt                = transconn.prepareStatement(updateCooler);
                        stmt.setInt(1, newCoolerId);
                        stmt.setInt(2, newKegLineId);
                        stmt.executeUpdate();
                    }
                    
                    if(lineNo != null && !lineNo.equals("")) {
                        stmt        = transconn.prepareStatement("UPDATE line SET lineNo = ? WHERE id = ?");
                        stmt.setString(1, lineNo);
                        stmt.setInt(2, lineId);
                        stmt.executeUpdate();
                        /*if(lineNo.equals("0")){
                            stmt            = transconn.prepareStatement("UPDATE line SET lineNo='0' WHERE id = ?");
                            stmt.setInt(1, lineId);
                            stmt.executeUpdate();
                        } else {
                            stmt            = transconn.prepareStatement("SELECT l.id FROM line l LEFT JOIN bar b ON b.id = l.bar WHERE b.location = ? AND l.lineNo = ? AND l.status = 'RUNNING'");
                            stmt.setInt(1, location);
                            stmt.setString(2, lineNo);
                            rs              = stmt.executeQuery();
                            if (!rs.next()) {
                                stmt        = transconn.prepareStatement("UPDATE line SET lineNo = ? WHERE id = ?");
                                stmt.setString(1, lineNo);
                                stmt.setInt(2, lineId);
                                stmt.executeUpdate();
                            }
                        }*/
                    }
                    
                    if(osPlatform >1 && callerId>0){
                        if(mobileUserId <1){
                            mobileUserId    = getMobileUserId(callerId);
                        }
                        addUserHistory(callerId, "updateLine", location, "update Line ", mobileUserId,product);
                    }
                    
                    stmt                    = transconn.prepareStatement("DELETE FROM comingSoonProducts WHERE location = ? AND product = ?");
                    stmt.setInt(1, location);
                    stmt.setInt(2, product);                        
                    stmt.executeUpdate();
                    
                    if(product == 0){
                        stmt                = transconn.prepareStatement(updateLineEmpty);
                        stmt.setInt(1, lineId);
                        stmt.executeUpdate();
                    }
                   
                } else {
                    addErrorDetail(toAppend, "Can't find product #" + product);
                }
                 }
                lineNoMap.put(lineId, lineNo);
            }
            int arrayLength                 = newActiveProducts.size();
            if (arrayLength > 0) {
                for (int i = 0; i < arrayLength; i++) {
                    String sql              = " UPDATE inventory SET isActive = 1 WHERE location = ? AND product = ? ";
                    stmt = transconn.prepareStatement(sql);
                    stmt.setInt(1, location);
                    stmt.setInt(2, newActiveProducts.get(i));
                    stmt.executeUpdate();
                    inactiveProducts.remove(newActiveProducts.get(i));
                }
            }

            arrayLength                     = inactiveProducts.size();
            if (arrayLength > 0) {
                for (int i = 0; i < arrayLength; i++) {
                    String sql              = " UPDATE inventory SET isActive = 0 WHERE location = ? AND product = ? ";
                    stmt = transconn.prepareStatement(sql);
                    stmt.setInt(1, location);
                    stmt.setInt(2, inactiveProducts.get(i));
                    stmt.executeUpdate();
                }
            }
            //postBWWQuadMenu(location, toAppend);
            } else {
                addErrorDetail(toAppend, "Invalid Access"  );
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(rs);
            close(stmt);
        }
    }

    /**  Update all fields for one or more inventory records
     *
     */
    private void updateInventory(Element toHandle, Element toAppend) throws HandlerException {

        int callerId = getCallerId(toHandle);
        int osPlatform                      = HandlerUtils.getOptionalInteger(toHandle, "osPlatform");
        int mobileUserId                    = HandlerUtils.getOptionalInteger(toHandle, "mobileUserId");
        HashMap<Integer, HashMap> SupplierProductMap = new HashMap<Integer, HashMap>();
        HashMap<Integer, String> ProductPLUMap = new HashMap<Integer, String>();

        String update = "UPDATE inventory SET minimumQty=?,qtyToHave=?,plu=?,supplier=?,kegSize=?,qtyOnHand=?,bottleSize=?,kegLine = ? " +
                " WHERE id=? AND location = ?";

        String select = "SELECT qtyOnHand, product, kegSize FROM inventory WHERE id=?";

        String selectSupplierProducts = "SELECT pSM.product, pSM.plu FROM productSetMap pSM LEFT JOIN supplier s ON s.productSet = pSM.productSet WHERE s.id = ? ";

        String selectCustomerInventory = " SELECT i.location, i.id, i.qtyOnHand, i.supplier, i.plu FROM inventory i LEFT JOIN location l ON l.id = i.location WHERE l.customer = ? AND i.product = ?";

        String selectkegLineInventory = "SELECT i.qtyOnHand, pr.name FROM inventory i " +
                " LEFT JOIN product pr ON i.product=pr.id WHERE i.location=? AND i.kegLine = ?";
        String updateIBU                    = "UPDATE productDescription SET ibu =  ? WHERE product =?";

        PreparedStatement stmt = null;
        ResultSet rs = null, rsLocation = null;

        boolean forCustomer = HandlerUtils.getOptionalBoolean(toHandle, "forCustomer");
        Iterator i = toHandle.elementIterator("inventory");

        try {
            while (i.hasNext()) {
                int locationId = 0, productId = 0;
                Element inv = (Element) i.next();
                int invId = HandlerUtils.getRequiredInteger(inv, "invId");
                int coolerId = HandlerUtils.getOptionalInteger(inv, "coolerId");
                int lineId = 0;
                lineId = HandlerUtils.getOptionalInteger(inv, "lineId");
                float reorderPoint = HandlerUtils.getRequiredFloat(inv, "reorderPoint");
                float reorderQty = HandlerUtils.getRequiredFloat(inv, "reorderQty");
                int supplierId = HandlerUtils.getRequiredInteger(inv, "supplierId");
                int kegSize = 0;
                kegSize = HandlerUtils.getOptionalInteger(inv, "kegSize");
                String plu = HandlerUtils.getOptionalString(inv, "plu");
                float qtyOnHand = HandlerUtils.getRequiredFloat(inv, "qtyOnHand");
                int bottleSize = 0;
                bottleSize = HandlerUtils.getOptionalInteger(inv, "bottleSize");
                int ibu = HandlerUtils.getOptionalInteger(inv, "ibu");

                if (SupplierProductMap.containsKey(supplierId)) {
                    ProductPLUMap = SupplierProductMap.get(supplierId);
                } else {
                    stmt = transconn.prepareStatement(selectSupplierProducts);
                    stmt.setInt(1, supplierId);
                    rs = stmt.executeQuery();
                    while (rs.next()) {
                        ProductPLUMap.put(new Integer(rs.getInt(1)), rs.getString(2));
                    }
                    SupplierProductMap.put(supplierId, ProductPLUMap);
                }

                stmt = transconn.prepareStatement(select);
                stmt.setInt(1, invId);
                rs = stmt.executeQuery();
                if (rs.next()) {
                    float oldQty = rs.getFloat(1);
                    productId = rs.getInt(2);
                    if (kegSize <= 0) {
                        kegSize = rs.getInt(3);
                    }

                    if (ProductPLUMap.containsKey(productId)) {
                        plu = ProductPLUMap.get(productId);
                    } else if (plu == null) {
                        plu = "";
                    }


                    // We only update the quantity if the new value if there is a 0.1 keg difference or more
                    // -or- if the new value is a whole number
                    if (Math.abs(oldQty - qtyOnHand) <= 0.1 && (Math.abs(Math.round(qtyOnHand) - qtyOnHand) > 0.02)) {
                        qtyOnHand = oldQty;
                        addErrorDetail(toAppend, "Didn't update qty on hand for ID "+invId+": new qty was within 0.1 kegs of the old value");
                    }

                    if (forCustomer) {
                        int customerId = HandlerUtils.getRequiredInteger(toHandle, "customerId");
                        if(callerId > 0 && customerId > 0 && isValidAccessUser(callerId, customerId,true)){
                        if (checkForeignKey("inventory", invId, transconn) && checkForeignKey("customer", customerId, transconn) && checkForeignKey("supplier", supplierId, transconn)) {

                            stmt = transconn.prepareStatement(selectCustomerInventory);
                            stmt.setInt(1, customerId);
                            stmt.setInt(2, productId);
                            rsLocation = stmt.executeQuery();
                            while (rsLocation.next()) {
                                locationId = rsLocation.getInt(1);
                                int newInvId = rsLocation.getInt(2);
                                float newQtyOnHand = rsLocation.getFloat(3);
                                int newSupplierId = rsLocation.getInt(4);
                                String newPlu = rsLocation.getString(5);

                                if (invId == newInvId) {
                                    newQtyOnHand = qtyOnHand;
                                    newSupplierId = supplierId;
                                    newPlu = plu;
                                }
                                stmt = transconn.prepareStatement(update);
                                stmt.setFloat(1, reorderPoint);
                                stmt.setFloat(2, reorderQty);
                                stmt.setString(3, newPlu);
                                stmt.setInt(4, newSupplierId);
                                stmt.setInt(5, kegSize);
                                stmt.setFloat(6, newQtyOnHand);
                                stmt.setInt(7, bottleSize);
                                stmt.setInt(8, lineId);
                                stmt.setInt(9, newInvId);
                                stmt.setInt(10, locationId);
                                stmt.executeUpdate();

                                String logMessage = "Updating inventory fields for " + productId + " (" + invId + ")";
                                logger.portalDetail(callerId, "updateInventory", locationId, "inventory", invId, logMessage, transconn);
                            }
                        }
                        } else {
                            addErrorDetail(toAppend, "Invalid Access"  );
                        }

                    } else {
                        locationId = HandlerUtils.getRequiredInteger(inv, "locationId");
                        if(callerId > 0 && locationId > 0 && isValidAccessUser(callerId, locationId,false)){
                        if (checkForeignKey("inventory", invId, transconn) && checkForeignKey("location", locationId, transconn) && checkForeignKey("supplier", supplierId, transconn)) {


                            stmt = transconn.prepareStatement(update);
                            stmt.setFloat(1, reorderPoint);
                            stmt.setFloat(2, reorderQty);
                            stmt.setString(3, plu);
                            stmt.setInt(4, supplierId);
                            stmt.setInt(5, kegSize);
                            stmt.setFloat(6, qtyOnHand);
                            stmt.setInt(7, bottleSize);
                            stmt.setInt(8, lineId);
                            stmt.setInt(9, invId);
                            stmt.setInt(10, locationId);
                            stmt.executeUpdate();

                            String logMessage = "Updating inventory fields for " + productId + " (" + invId + ")";
                            logger.portalDetail(callerId, "updateInventory", locationId, "inventory", invId, logMessage, transconn);
                            
                            if(osPlatform > 1 && callerId>0){
                                if(mobileUserId <1){
                                    mobileUserId              = getMobileUserId(callerId);
                                }
                                addUserHistory(callerId, "updateInventory", locationId, "Update Inventory Product", mobileUserId,invId);
                            }

                        } else {
                            logger.dbError("Foreign key check failed for Inv# " + invId + " or Loc# " + locationId + " or Sup# " + supplierId);
                            addErrorDetail(toAppend, "Unable to update inv ID " + invId + "; a database problem occurred");
                        }
                        
                    } else {
                            addErrorDetail(toAppend, "Invalid Access"  );
                        }

                    }
                    
                    
                }
                if(ibu >=0 && ibu<=100){
                    stmt = transconn.prepareStatement(updateIBU);
                    stmt.setInt(1, ibu);
                    stmt.setInt(2, productId);
                    stmt.executeUpdate();
                }
            }
            
            
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
            close(rs);
        }

    }

    /** Updates the low-stock alert preferences for a user.  Also will update the users email address
     *  The locations should be of the form <location><id>1234</id><value>X</value></location> where
     *  X should be 1 or 0. A value of 1 will enable the alert, 0 will disable it.
     */
    private void updateLowStockAlerts(Element toHandle, Element toAppend) throws HandlerException {
        int user = HandlerUtils.getRequiredInteger(toHandle, "userId");
        String email = HandlerUtils.getOptionalString(toHandle, "email");

        String selectAlerts = "SELECT location FROM lowStockNotification WHERE user=?";
        String updateEmail = "UPDATE user SET email=? WHERE id=?";
        String deleteAlert = "DELETE FROM lowStockNotification WHERE user=? AND location=? LIMIT 1";
        String addAlert = "INSERT INTO lowStockNotification (user,location) VALUES (?,?)";

        Set<Integer> existingLocations = new HashSet<Integer>();

        PreparedStatement stmt = null;
        ResultSet rs = null;

        try {
            //get a list of the existing location alerts
            stmt = transconn.prepareStatement(selectAlerts);
            stmt.setInt(1, user);
            rs = stmt.executeQuery();
            while (rs.next()) {
                existingLocations.add(new Integer(rs.getInt(1)));
            }

            Iterator i = toHandle.elementIterator("location");
            while (i.hasNext()) {
                Element locEl = (Element) i.next();
                int location = HandlerUtils.getRequiredInteger(locEl, "id");
                int value = HandlerUtils.getRequiredInteger(locEl, "value");
                //check if we need to delete
                if (value == 0 && existingLocations.contains(new Integer(location))) {
                    String logMessage = "Deleting low stock alert for L#" + location + " U#" + user;
                    logger.debug(logMessage);
                    stmt = transconn.prepareStatement(deleteAlert);
                    stmt.setInt(1, user);
                    stmt.setInt(2, location);
                    stmt.executeUpdate();
                    logger.portalDetail(user, "updateUser", 0, "user", user, logMessage, transconn);
                    //check if we need to add
                } else if (value == 1 && !existingLocations.contains(new Integer(location))) {
                    String logMessage = "Adding low stock alert for L#" + location + " U#" + user;
                    logger.debug(logMessage);
                    stmt = transconn.prepareStatement(addAlert);
                    stmt.setInt(1, user);
                    stmt.setInt(2, location);
                    stmt.executeUpdate();
                    logger.portalDetail(user, "updateUser", 0, "user", user, logMessage, transconn);
                }
            }

            //update email address, if supplied
            if (email != null) {
                String logMessage = "Updating email for U#" + user + ": '" + email + "'";
                logger.debug(logMessage);
                stmt = transconn.prepareStatement(updateEmail);
                stmt.setString(1, email);
                stmt.setInt(2, user);
                stmt.executeUpdate();

                logger.portalDetail(user, "updateUser", 0, "user", user, logMessage, transconn);
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error in updateLowStockAlerts: " + sqle.toString());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
            close(rs);
        }
    }

    /** Updates the Product Request notification for a user.  Also will update the users email address.
     *  the args are
     *   <userId> user id
     *   <notify> 1 or 0
     *   <email> the email address
     *   Note:  ONLY admin users can receive product request notifications
     */
    private void updateProductRequestAlert(Element toHandle, Element toAppend) throws HandlerException {
        int userId = HandlerUtils.getRequiredInteger(toHandle, "userId");
        int notify = HandlerUtils.getRequiredInteger(toHandle, "notify");
        String email = HandlerUtils.getRequiredString(toHandle, "email");

        String sql = "UPDATE user SET email=?, notifyOnProductRequest=? WHERE id=?";

        PreparedStatement stmt = null;

        try {
            stmt = transconn.prepareStatement(sql);
            stmt.setString(1, email);
            stmt.setInt(2, notify);
            stmt.setInt(3, userId);
            stmt.executeUpdate();

            String logMessage = "updateProductRequestAlert for U#" + userId + " to " + notify;
            logger.portalDetail(userId, "updateUser", 0, "user", userId, logMessage, transconn);
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
        }
    }

    /** Updates the Location Status notification for a user.
     *  the args are
     *   <userId> user id
     *   <notify> 1 or 0
     *   Note:  ONLY admin users can receive product request notifications
     */
    private void updateLocationStatusAlert(Element toHandle, Element toAppend) throws HandlerException {
        int userId = HandlerUtils.getRequiredInteger(toHandle, "userId");
        int notify = HandlerUtils.getRequiredInteger(toHandle, "notify");

        String sql = "UPDATE user SET notifyOnLocationStatus=? WHERE id=?";

        PreparedStatement stmt = null;

        try {
            stmt = transconn.prepareStatement(sql);
            stmt.setInt(1, notify);
            stmt.setInt(2, userId);
            stmt.executeUpdate();

            String logMessage = "updateLocationStatusAlert for U#" + userId + " to " + notify;
            logger.portalDetail(userId, "updateUser", 0, "user", userId, logMessage, transconn);
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
        }
    }

    /**
     * Change a misc product's status to activate.
     * The specific misc product is located by its unique id.
     */
    private void activateMiscProduct(Element toHandle, Element toAppend) throws HandlerException {
        int callerId = getCallerId(toHandle);
        int location = HandlerUtils.getRequiredInteger(toHandle, "locationId");

        try {
            Iterator i = toHandle.elementIterator("product");
            while (i.hasNext()) {
                Element el = (Element) i.next();
                int mpId = HandlerUtils.getRequiredInteger(el, "id");
                changeMiscProductStatus(mpId, true, transconn);

                String logMessage = "Activate miscProduct id = " + String.valueOf(mpId);
                logger.portalDetail(callerId, "activateMiscProduct", location, "miscProduct", mpId, logMessage, transconn);
                logger.debug(logMessage);
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        }
    }

    /**
     * Deativating all inventory that has not been assigned to a line for a week
     */
    private void deactivateInventory(Element toHandle, Element toAppend) throws HandlerException {

        int activate = HandlerUtils.getRequiredInteger(toHandle, "activate");
        String sql = " UPDATE inventory SET isActive = ? ";

        PreparedStatement stmt = null;
        int paramsSet = 0;
        try {
            Iterator inventoryIterator = toHandle.elementIterator("inventory");
            while (inventoryIterator.hasNext()) {
                Element inventory = (Element) inventoryIterator.next();
                int inventoryId = HandlerUtils.getOptionalInteger(inventory, "id");
                String inventoryIdString = HandlerUtils.getOptionalString(inventory, "inventoryIds");
                if (inventoryId >= 0) {
                    sql += " WHERE id = ? ";
                    paramsSet++;
                }
                if (null != inventoryIdString) {
                    sql += " WHERE id IN (" + inventoryIdString + ") ";
                    paramsSet++;
                }
                if (paramsSet != 1) {
                    throw new HandlerException("Exactly one of the following must be set: inventoryId or inventoryIdString");
                }

                stmt = transconn.prepareStatement(sql);
                stmt.setInt(1, activate);
                if (inventoryId >= 0) {
                    stmt.setInt(2, inventoryId);
                }
                stmt.executeUpdate();
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
        }
    }

    /**
     * Deativating all alerts from the night before
     */
    private void deactivateActiveAlerts(Element toHandle, Element toAppend) throws HandlerException {

        int status = HandlerUtils.getRequiredInteger(toHandle, "status");
        int alertType = 0;

        String sql = " UPDATE alerts SET status = ? ";
        String resetAlertFrequency = "UPDATE textAlert SET currentFrequency = 0 ";
        String resetLocationPromotions = "UPDATE locationBeerBoardMap SET promotions = 1 ";

        if (status > 0) {
            alertType = HandlerUtils.getRequiredInteger(toHandle, "alertType");
            sql += " WHERE alertType = ? ";
        }
        PreparedStatement stmt = null;

        try {
            stmt = transconn.prepareStatement(sql);
            stmt.setInt(1, status);
            if (status > 0) {
                stmt.setInt(2, alertType);
            }
            stmt.executeUpdate();

            stmt = transconn.prepareStatement(resetAlertFrequency);
            stmt.executeUpdate();

            stmt = transconn.prepareStatement(resetLocationPromotions);
            stmt.executeUpdate();
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
        }
    }

    /**
     * Deativating all alerts from the night before
     */
    private void deactivateActiveUserAlerts(Element toHandle, Element toAppend) throws HandlerException {

        boolean resetCount = HandlerUtils.getOptionalBoolean(toHandle, "resetCount");

        String resetUserAlertFrequency = "UPDATE userAlerts SET active = 0 ";
        String resetSuperUserAlertFrequency = "UPDATE superUserAlerts SET active = 0 ";

        if (resetCount) {
            resetUserAlertFrequency += " AND count = 0; ";
        }

        PreparedStatement stmt = null;

        try {
            stmt = transconn.prepareStatement(resetUserAlertFrequency);
            stmt.executeUpdate();

            stmt = transconn.prepareStatement(resetSuperUserAlertFrequency);
            stmt.executeUpdate();
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
        }
    }

    /**
     * Deativating all alerts from the night before
     */
    private void deactivateSuspensions(Element toHandle, Element toAppend) throws HandlerException {

        String selectSuspensions            = "SELECT location FROM suspensionLogs WHERE active = 1  ";
        String resetSuspension              = "UPDATE suspensionLogs SET active = 0 WHERE location = ? ";
        String resetLocationSuspension      = "UPDATE locationDetails SET suspended = 0 WHERE location = ? ";
        int locationId                      = HandlerUtils.getOptionalInteger(toHandle, "locationId");

        if (locationId > 0) {
            selectSuspensions               += " AND location = ? ";
        } else {
            selectSuspensions               += " AND endDate < NOW() ";
        }
        
        PreparedStatement stmt              = null;
        ResultSet rs                        = null;

        try {
            stmt                            = transconn.prepareStatement(selectSuspensions);
            if (locationId > 0) {
                stmt.setInt(1, locationId);
            }
            rs                              = stmt.executeQuery();
            while (rs.next()) {
                stmt                        = transconn.prepareStatement(resetSuspension);
                stmt.setInt(1, rs.getInt(1));
                stmt.executeUpdate();

                stmt                        = transconn.prepareStatement(resetLocationSuspension);
                stmt.setInt(1, rs.getInt(1));
                stmt.executeUpdate();
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
            close(rs);
        }
    }

    /**
     * Change a mice product's status to inactive.
     * The specific misc product is located by its unique id.
     */
    private void deactivateMiscProduct(Element toHandle, Element toAppend) throws HandlerException {
        int callerId = getCallerId(toHandle);
        int location = HandlerUtils.getRequiredInteger(toHandle, "locationId");

        try {
            Iterator i = toHandle.elementIterator("product");
            while (i.hasNext()) {
                Element el = (Element) i.next();
                int mpId = HandlerUtils.getRequiredInteger(el, "id");
                changeMiscProductStatus(mpId, false, transconn);

                String logMessage = "Deactivate miscProduct id = " + String.valueOf(mpId);
                logger.portalDetail(callerId, "deactivateMiscProduct", location, "miscProduct", mpId, logMessage, transconn);
                logger.debug(logMessage);
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        }
    }

    /**
     * helper function to change the product status
     */
    private void changeMiscProductStatus(int mpId, boolean active, RegisteredConnection transconn) throws SQLException {
        String sql = "UPDATE miscProduct SET active=? WHERE id=?";
        PreparedStatement stmt = transconn.prepareStatement(sql);
        stmt.setInt(1, active ? 1 : 0);
        stmt.setInt(2, mpId);
        stmt.executeUpdate();
    }

    /**
     * Update a misc product according to given value.
     * The specific misc product is located by its unique id.
     */
    private void updateMiscProduct(Element toHandle, Element toAppend) throws HandlerException {
        int callerId = getCallerId(toHandle);
        int location = HandlerUtils.getRequiredInteger(toHandle, "locationId");

        String sql = "UPDATE miscProduct SET name=?, plu=?, supplier=? WHERE id=?";
        PreparedStatement stmt = null;

        try {
            Iterator i = toHandle.elementIterator("product");
            while (i.hasNext()) {
                Element inv = (Element) i.next();
                int mpId = HandlerUtils.getRequiredInteger(inv, "id");
                String newName = HandlerUtils.getRequiredString(inv, "name");
                String newPlu = HandlerUtils.getRequiredString(inv, "plu");
                int newSupplier = HandlerUtils.getRequiredInteger(inv, "supplier");

                int paramIndex = 1;
                stmt = transconn.prepareStatement(sql);
                stmt.setString(paramIndex++, newName);
                stmt.setString(paramIndex++, newPlu);
                stmt.setInt(paramIndex++, newSupplier);
                stmt.setInt(paramIndex++, mpId);
                stmt.executeUpdate();

                String logMessage = "Update miscProduct id = " + String.valueOf(mpId);
                logger.portalDetail(callerId, "updateMiscProduct", location, "miscProduct", mpId, logMessage, transconn);
                logger.debug(logMessage);
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
        }
    }

    /**
     * copy all products from one location to another location.
     * The original location is written as "location" and the new location is written as "newLocation".
     */
    private void importMiscProducts(Element toHandle, Element toAppend) throws HandlerException {
        int fromLocation = HandlerUtils.getRequiredInteger(toHandle, "fromLocationId");
        int newLocation = HandlerUtils.getRequiredInteger(toHandle, "newLocationId");
        int callerId = getCallerId(toHandle);

        String insert =
                " INSERT INTO miscProduct (location, name, plu, supplier, active) " +
                " SELECT ?,name,plu,supplier,active FROM miscProduct WHERE location=? ";

        PreparedStatement stmt = null;
        int colCount = 1;

        try {
            stmt = transconn.prepareStatement(insert);

            stmt.setInt(colCount++, newLocation);
            stmt.setInt(colCount++, fromLocation);
            stmt.executeUpdate();
            String logMessage = "import all product from loc " + String.valueOf(fromLocation) + " to loc " + String.valueOf(newLocation);
            logger.portalDetail(callerId, "importMiscProducts", 0, "miscProduct", 0, logMessage, transconn);
            logger.debug(logMessage);
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
        }
    }

    private void updateProduct(Element toHandle, Element toAppend) throws HandlerException {

        String name = HandlerUtils.getRequiredString(toHandle, "productName");
        int qid = HandlerUtils.getOptionalInteger(toHandle, "quicksell");
        int id = HandlerUtils.getRequiredInteger(toHandle, "productId");
        int callerId = getCallerId(toHandle);

        PreparedStatement stmt = null;
        ResultSet rs = null;

        if (qid < 0) {
            qid = 0;
        }

        String update = "UPDATE product SET  name=?, approved = approved WHERE id=?";
        String fullUpdate = "UPDATE product SET name=?, qid=?, approved = approved WHERE id=?";

        try {
            String logMessage = "Changing product name to " + name + " for id" + id;
            logger.portalDetail(callerId, "updateProduct", 0, "product", id, logMessage, transconn);
            //String select = "SELECT name FROM Location WHERE id=?";
            if (qid == 0) {
                stmt = transconn.prepareStatement(update);
                stmt.setString(1, name);
                stmt.setInt(2, id);
                stmt.executeUpdate();
            } else {
                stmt = transconn.prepareStatement(fullUpdate);
                stmt.setString(1, name);
                stmt.setInt(2, qid);
                stmt.setInt(3, id);
                stmt.executeUpdate();
            }


        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
            close(rs);
        }

    }

    /**  Delete or close a product OR supplier request
     *     Two options for permission:
     *     Either provide a userId -AND- locationId, and the record must match either
     *     Provide none, which is admin-mode and allows any delete
     *     You can optionally provide the status to change the request to, the default is "deleted"
     *     "Deleted" requests will never appear on the manager, but other status codes may appear as
     *     a subcategory of "closed" to users.
     *
     *     @param type must be supplier or product
     *
     */
    private void deleteRequest(Element toHandle, Element toAppend, String type) throws HandlerException {

        String table                        = "",action = "";
        if ("supplier".equals(type)) {
            table                           = "supplierRequest";
            action                          = "deleteSupplierRequest";
        } else if ("product".equals(type)) {
            table                           = "productRequest";
            action                          = "deleteProductRequest";
        } else {
            throw new HandlerException("Invalid internal type: " + type);
        }

        int requestId                       = HandlerUtils.getRequiredInteger(toHandle, "requestId");
        String newName                      = HandlerUtils.getOptionalString(toHandle, "newName");
        int userId                          = HandlerUtils.getOptionalInteger(toHandle, "userId");
        int locationId                      = HandlerUtils.getOptionalInteger(toHandle, "locationId");
        String status                       = HandlerUtils.getOptionalString(toHandle, "status");
        if (status == null || "".equals(status)) {
            status                          = "deleted";
        }
        int callerId                        = getCallerId(toHandle);

        PreparedStatement stmt              = null;
        ResultSet rs                        = null;

        String selectReqUser                = "SELECT " + type + "Name, u.name, u.email FROM " + table + " t LEFT JOIN user u ON u.id = t.user WHERE t.id=? ";
        String updateAsUser                 = "UPDATE " + table + " SET status=? WHERE id=? AND (user=? OR location=?)";
        String updateAsAdmin                = "UPDATE " + table + " SET status=? WHERE id=? ";

        String logMessage                   = "Changing " + type + " request #" + requestId + " to '" + status + "'";
        logger.portalDetail(callerId, action, locationId, table, requestId, logMessage, transconn);
        try {
            if (locationId > 0 && userId > 0) {
                logger.portalAction(action + " by U#" + userId + " L#" + locationId +
                        ": changing record id " + requestId + " to status '" + status + "'");
                stmt                        = transconn.prepareStatement(updateAsUser);
                stmt.setString(1, status);
                stmt.setInt(2, requestId);
                stmt.setInt(3, userId);
                stmt.setInt(4, locationId);
                stmt.executeUpdate();
            } else {
                logger.portalAction(action + " Admin mode: " +
                        "changing record id " + requestId + " to status '" + status + "'");
                stmt                        = transconn.prepareStatement(updateAsAdmin);
                stmt.setString(1, status);
                stmt.setInt(2, requestId);
                stmt.executeUpdate();

                stmt                        = transconn.prepareStatement(selectReqUser);
                stmt.setInt(1, requestId);
                rs                          = stmt.executeQuery();
                if (rs.next()) {
                    StringBuilder requestNotification
                                                = new StringBuilder();
                    requestNotification.append("<tr align=justify><td colspan=4>The " + type + " request for '");
                    requestNotification.append(HandlerUtils.nullToEmpty(rs.getString(1)));
                    requestNotification.append("' has been approved and was added to the " + type + " master list as  '" + newName + "'.</td></tr>");
                    requestNotification.append("<tr align=center valign=middle><td height=20 colspan=4>&nbsp;</td></tr>");
                    requestNotification.append("<tr align=justify><td colspan=4>Thank You,</td></tr>");
                    requestNotification.append("<tr align=justify><td colspan=4>US Beverage Net Support</td></tr>");
                    requestNotification.append("<tr align=center valign=middle><td height=35 colspan=4>&nbsp;</td></tr>");
                    requestNotification.append("<tr align=justify><td colspan=4><strong>This email was automatically generated; please do not reply.</strong></td></tr><tr><td colspan=4>&nbsp;</td></tr>");
                    sendMail("USBN " + type.substring(0, 1).toUpperCase() + type.substring(1) + " Approval Notification", rs.getString(2), rs.getString(3), "support@beerboard.com",
                                                                    type.substring(0, 1).toUpperCase() + type.substring(1) + " Approval Notification", "sendMail", requestNotification, false);
                }
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
        }

    }

    private long getAlertOffsetInMillis(Integer tableId, Integer tableType) throws HandlerException {
        PreparedStatement stmt = null;
        ResultSet rs = null;

        long alertOffsetInMillis = 0;

        try {
            String selectAlertDescription = "";

            switch (tableType) {
                case 0:
                    selectAlertDescription += " SELECT 0.00 FROM customer WHERE id = ? ";
                    break;

                case 1:
                    selectAlertDescription += " SELECT 0.00 FROM customer WHERE id = ? ";
                    break;

                case 2:
                    selectAlertDescription += " SELECT l.easternOffset FROM location l WHERE l.id = ? ";
                    break;

                case 3:
                    selectAlertDescription += " SELECT l.easternOffset FROM bar b LEFT JOIN location l ON l.id = b.location WHERE b.id = ? ";
                    break;

                case 4:
                    selectAlertDescription += " SELECT l.easternOffset FROM cooler c LEFT JOIN location l ON l.id = c.location WHERE c.id = ? ";
                    break;

                case 5:
                    selectAlertDescription += " SELECT l.easternOffset FROM zone z LEFT JOIN locatoion l ON l.id = z.location WHERE z.id = ? ";
                    break;

                case 6:
                    selectAlertDescription += " SELECT l.easternOffset FROM bar b LEFT JOIN zone z ON z.id = b.zone " +
                            " LEFT JOIN location l ON l.id = z.location WHERE b.id = ? ";
                    break;

                case 7:
                    selectAlertDescription += " SELECT l.easternOffset FROM station st LEFT JOIN bar b ON b.id = st.bar " +
                            " LEFT JOIN zone z ON z.id = b.zone LEFT JOIN location l ON l.id = z.location WHERE st.id = ? ";
                    break;

                case 8:
                    selectAlertDescription += " SELECT l.easternOffset FROM system s LEFT JOIN location l ON l.id = s.location WHERE s.id = ? ";
                    break;

                case 9:
                    selectAlertDescription += " SELECT l.easternOffset FROM inventory i LEFT JOIN location l ON l.id = i.location " +
                            " LEFT JOIN product p ON p.id = i.product WHERE i.kegLine = ? ";
                    break;

                case 10:
                    selectAlertDescription += " SELECT l.easternOffset FROM inventory i LEFT JOIN location l ON l.id = i.location " +
                            " LEFT JOIN product p ON p.id = i.product WHERE i.id = ? ";
                    break;

                case 11:
                    selectAlertDescription += " SELECT l.easternOffset FROM line li LEFT JOIN system s ON s.id = li.system " +
                            " LEFT JOIN location l ON l.id = s.location WHERE li.id = ? ";
                    break;

                case 12:
                    selectAlertDescription += " SELECT 0.00 FROM groups g WHERE g.id = ? ";
                    break;

                case 13:
                    selectAlertDescription += " SELECT 0.00 FROM region r WHERE r.id = ? ";
                    break;

                case 14:
                    selectAlertDescription += " SELECT 0.00 FROM county c LEFT JOIN state s ON s.FIPSST = c.state WHERE c.id = ? ";
                    break;

                default:
                    selectAlertDescription += " SELECT 0.00 FROM customer WHERE id = ?";
                    break;
            }


            stmt = transconn.prepareStatement(selectAlertDescription);
            stmt.setInt(1, tableId);
            rs = stmt.executeQuery();
            if (rs.next()) {
                alertOffsetInMillis = 1000 * 60 * 60 * rs.getLong(1);
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(rs);
            close(stmt);
        }
        //logger.debug(String.valueOf(alertOffsetInMillis));
        return alertOffsetInMillis;
    }

    private String dateToString(java.util.Date toConvert) {
        SimpleDateFormat newDateFormat = new SimpleDateFormat("HH:mm:ss");
        String convertedDate = newDateFormat.format(toConvert);
        return convertedDate;
    }

    /**
     * The folowing code is to update or insert the report requirements for each user for the email address
     * that they provide - SR
     */
    private void updateEmailReports(Element toHandle, Element toAppend) throws HandlerException {

        int callerId = getCallerId(toHandle);
        String select = " SELECT platform FROM emailReport WHERE id=?";

        String update = " UPDATE emailReport SET user=?, report=?, tableId=?, tableType=?, tableValues=?, minAlertThreshold=?, maxAlertThreshold=?, reportFormat=?, time=? WHERE id=? ";

        SimpleDateFormat f = new SimpleDateFormat("HH:mm:ss");
        PreparedStatement stmt = null, innerstmt = null;
        ResultSet rs = null, innerrs = null;

        int userId = HandlerUtils.getRequiredInteger(toHandle, "userId");
        Iterator i = toHandle.elementIterator("reports");

        try {
            while (i.hasNext()) {
                Element repo = (Element) i.next();
                int reportId = HandlerUtils.getRequiredInteger(repo, "reportId");
                int report = HandlerUtils.getRequiredInteger(repo, "report");
                int tableId = HandlerUtils.getRequiredInteger(repo, "tableId");
                int tableType = HandlerUtils.getRequiredInteger(repo, "tableType");
                String tableValues = HandlerUtils.getOptionalString(repo, "tableValues");
                double minAlertThreshold = HandlerUtils.getRequiredDouble(repo, "minAlertThreshold");
                double maxAlertThreshold = HandlerUtils.getRequiredDouble(repo, "maxAlertThreshold");
                int reportFormat = HandlerUtils.getRequiredInteger(repo, "reportFormat");
                int platform = -1;
                //Check that this alert doesn't already exist in inventory at this location
                stmt = transconn.prepareStatement(select);
                stmt.setInt(1, reportId);
                rs = stmt.executeQuery();
                if (rs.next()) {
                    platform = rs.getInt(1);
                    stmt = transconn.prepareStatement(update);
                    stmt.setInt(1, userId);
                    stmt.setInt(2, report);
                    stmt.setInt(3, tableId);
                    stmt.setInt(4, tableType);
                    stmt.setString(5, HandlerUtils.nullToString(tableValues, "0"));
                    stmt.setDouble(6, minAlertThreshold);
                    stmt.setDouble(7, maxAlertThreshold);
                    stmt.setInt(8, reportFormat);
                    Iterator j = repo.elementIterator("time");
                    if (j.hasNext()) {
                        stmt.setInt(9, 1);
                        String selectTimeTable = " SELECT id FROM emailTimeTable WHERE id=? ";
                        String updateTimeTable = " UPDATE emailTimeTable SET user=?, report=?, time=?, day=? WHERE id = ?";
                        String insertTimeTable = " INSERT INTO emailTimeTable (user, report, time, day) VALUES (?, ?, ?, ?)";
                        while (j.hasNext()) {
                            Element time = (Element) j.next();
                            int timeId = HandlerUtils.getRequiredInteger(time, "timeId");
                            String alertTime = HandlerUtils.getRequiredString(time, "time");
                            int alertDay = HandlerUtils.getOptionalInteger(time, "reportDay");
                            java.util.Date d = f.parse(alertTime);
                            java.util.Date date = new java.util.Date(d.getTime() - getAlertOffsetInMillis(tableId, tableType));
                            
                            if (platform == 2 && alertDay < 0) {
                                alertDay = 1;
                            }
                            //Check that this report doesn't already exist in textAlerts at this location
                            innerstmt = transconn.prepareStatement(selectTimeTable);
                            innerstmt.setInt(1, timeId);
                            innerrs = innerstmt.executeQuery();
                            if (innerrs.next()) {
                                innerstmt = transconn.prepareStatement(updateTimeTable);
                                innerstmt.setInt(1, userId);
                                innerstmt.setInt(2, reportId);
                                innerstmt.setString(3, dateToString(date));
                                innerstmt.setInt(4, alertDay);
                                innerstmt.setInt(5, timeId);
                                innerstmt.executeUpdate();
                            } else {
                                innerstmt = transconn.prepareStatement(insertTimeTable);
                                innerstmt.setInt(1, userId);
                                innerstmt.setInt(2, reportId);
                                innerstmt.setString(3, dateToString(date));
                                innerstmt.setInt(4, alertDay);
                                innerstmt.executeUpdate();
                            }
                        }
                    } else {
                        stmt.setInt(9, 0);
                        String deleteTimeTable = "DELETE FROM emailTimeTable WHERE user=? AND report=? ";
                        innerstmt = transconn.prepareStatement(deleteTimeTable);
                        innerstmt.setInt(1, userId);
                        innerstmt.setInt(2, report);
                        innerstmt.executeUpdate();
                    }
                    stmt.setInt(10, reportId);
                    stmt.executeUpdate();
                }
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } catch (Exception e) {
            logger.dbError("Parse error: " + e.getMessage());
            throw new HandlerException(e);
        } finally {
            close(innerstmt);
            close(innerrs);
            close(stmt);
            close(rs);
        }
    }

    /**
     * The folowing code is to update or insert the report requirements for each user for the email address
     * that they provide - SR
     */
    private void updateTextAlerts(Element toHandle, Element toAppend) throws HandlerException {

        int callerId = getCallerId(toHandle);
        String select = " SELECT id FROM textAlert WHERE id=?";

        String update = " UPDATE textAlert SET user=?, alert=?, tableId=?, tableType=?, minAlertThreshold=?, maxAlertThreshold=?, alertTime=?, alertFrequency=? WHERE id=? ";

        SimpleDateFormat f = new SimpleDateFormat("HH:mm:ss");
        PreparedStatement stmt = null;
        ResultSet rs = null;

        int userId = HandlerUtils.getRequiredInteger(toHandle, "userId");
        Iterator i = toHandle.elementIterator("alerts");

        try {
            while (i.hasNext()) {
                Element prod = (Element) i.next();
                int alertId = HandlerUtils.getRequiredInteger(prod, "alertId");
                int alert = HandlerUtils.getRequiredInteger(prod, "alert");
                int tableId = HandlerUtils.getRequiredInteger(prod, "tableId");
                int tableType = HandlerUtils.getRequiredInteger(prod, "tableType");
                double minAlertThreshold = HandlerUtils.getRequiredDouble(prod, "minAlertThreshold");
                double maxAlertThreshold = HandlerUtils.getRequiredDouble(prod, "maxAlertThreshold");
                String alertTimestamp = HandlerUtils.getRequiredString(prod, "alertTimestamp");
                int alertFrequency = HandlerUtils.getRequiredInteger(prod, "alertFrequency");

                //Check that this alert doesn't already exist in inventory at this location
                stmt = transconn.prepareStatement(select);
                stmt.setInt(1, alertId);
                rs = stmt.executeQuery();

                if (rs.next()) {
                    stmt = transconn.prepareStatement(update);
                    stmt.setInt(1, userId);
                    stmt.setInt(2, alert);
                    stmt.setInt(3, tableId);
                    stmt.setInt(4, tableType);
                    stmt.setDouble(5, minAlertThreshold);
                    stmt.setDouble(6, maxAlertThreshold);
                    String alertTime = alertTimestamp;
                    java.util.Date d = f.parse(alertTime);
                    java.util.Date date = new java.util.Date(d.getTime() - getAlertOffsetInMillis(tableId, tableType));
                    stmt.setString(7, dateToString(date));
                    stmt.setInt(8, alertFrequency);
                    stmt.setInt(9, alertId);
                    stmt.executeUpdate();
                }
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } catch (Exception e) {
            logger.dbError("Parse error: " + e.getMessage());
            throw new HandlerException(e);
        } finally {
            close(stmt);
            close(rs);
        }
    }

    /**  Update a bar by renaming it
     *  <locationId>
     *  <barId>
     *  <name>
     */
    private void updateBar(Element toHandle, Element toAppend) throws HandlerException {

        logger.debug("updateBar starting");

        int locationId = HandlerUtils.getRequiredInteger(toHandle, "locationId");

        //NischaySharma_02-Oct-2009_Start
        boolean isConcessions = HandlerUtils.getOptionalBoolean(toHandle, "isConcessions");


        int callerId = getCallerId(toHandle);

        PreparedStatement stmt = null;
        ResultSet rs = null;
        String update = " UPDATE bar SET name=? WHERE id=? AND location=? ";
        String updateWZone = " UPDATE bar SET name=? WHERE id=? AND location=? AND zone = ?";
        String updatePosition = " UPDATE bar SET latitude=?, longitude=? WHERE id=?";
        String checkName = " SELECT id FROM bar WHERE location=? and lcase(name)=lcase(?) AND id != ? ";
        String checkNameWZone = " SELECT id FROM bar WHERE location=? AND zone = ? and lcase(name)=lcase(?) AND id != ? ";
        try {

            Iterator els = toHandle.elementIterator("bar");
            while (els.hasNext()) {
                Element bar = (Element) els.next();
                int barId = HandlerUtils.getRequiredInteger(bar, "barId");
                String name = HandlerUtils.getOptionalString(bar, "name");
                int zoneId = HandlerUtils.getOptionalInteger(bar, "zoneId");

                if (null != name && name.length() > 0) {

                    if (isConcessions) {
                        stmt = transconn.prepareStatement(checkNameWZone);
                        stmt.setInt(1, locationId);
                        stmt.setInt(2, zoneId);
                        stmt.setString(3, name);
                        stmt.setInt(4, barId);
                    } else {
                        stmt = transconn.prepareStatement(checkName);
                        stmt.setInt(1, locationId);
                        stmt.setString(2, name);
                        stmt.setInt(3, barId);
                    }
                    rs = stmt.executeQuery();
                    if (!rs.next()) {

                        String logMessage = "Renaming bar to " + name;

                        logger.portalDetail(callerId, "updateBar", locationId, "bar", barId, logMessage, transconn);
                        if (isConcessions) {
                            stmt = transconn.prepareStatement(updateWZone);
                            stmt.setString(1, name);
                            stmt.setInt(2, barId);
                            stmt.setInt(3, locationId);
                            stmt.setInt(4, zoneId);
                            stmt.executeUpdate();
                        } else {
                            stmt = transconn.prepareStatement(update);
                            stmt.setString(1, name);
                            stmt.setInt(2, barId);
                            stmt.setInt(3, locationId);
                            stmt.executeUpdate();
                        }
                        //NischaySharma_02-Oct-2009_End
                    } else {
                        addErrorDetail(toAppend, "A bar named " + name + " already exists.");
                    }
                } else {
                    double latitude = HandlerUtils.getRequiredDouble(bar, "latitude");
                    double longitude = HandlerUtils.getRequiredDouble(bar, "longitude");
                    stmt = transconn.prepareStatement(updatePosition);
                    stmt.setDouble(1, latitude);
                    stmt.setDouble(2, longitude);
                    stmt.setInt(3, barId);
                    stmt.executeUpdate();
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

    /**  Attempt to permanently delete a bar
     *  <locationId>
     *  <barId>
     *
     *   You cannot delete the only bar at a location, so if another bar doesn't exist, this method
     *   will return an error detail.  If the bar is available to delete, then any draft lines
     *   pouring to this bar will be redirected to another valid bar (if multiple bars exist, the
     *   oldest bar will receive the lines).
     */
    private void deleteBar(Element toHandle, Element toAppend) throws HandlerException {

        logger.debug("deleteBar starting");

        int locationId = HandlerUtils.getRequiredInteger(toHandle, "locationId");
        int barId = HandlerUtils.getRequiredInteger(toHandle, "barId");
        int callerId = getCallerId(toHandle);

        PreparedStatement stmt = null;
        ResultSet rs = null;
        String getOther = "SELECT id,name FROM bar where location=? AND id !=? ORDER BY id ASC LIMIT 1";
        String updateLine = "UPDATE line SET bar=? WHERE bar=?";
        String updateBev = "UPDATE beverage SET bar=? WHERE location=? AND bar=?";
        String delete = "DELETE FROM bar WHERE id=?";
        try {
            stmt = transconn.prepareStatement(getOther);
            stmt.setInt(1, locationId);
            stmt.setInt(2, barId);
            rs = stmt.executeQuery();
            if (rs.next()) {
                int otherId = rs.getInt(1);
                String otherName = rs.getString(2);

                stmt = transconn.prepareStatement(updateLine);
                stmt.setInt(1, otherId);
                stmt.setInt(2, barId);
                stmt.executeUpdate();

                stmt = transconn.prepareStatement(updateBev);
                stmt.setInt(1, otherId);
                stmt.setInt(2, locationId);
                stmt.setInt(3, barId);
                stmt.executeUpdate();

                stmt = transconn.prepareStatement(delete);
                stmt.setInt(1, barId);
                stmt.executeUpdate();

                String logMessage = "Deleted bar #" + barId + ", moving lines and bevs to " + otherName + " (#" + otherId + ")";
                logger.portalDetail(callerId, "deleteBar", locationId, "bar", barId, logMessage, transconn);

            } else {
                addErrorDetail(toAppend, "Unable to delete this bar since it is the only one at this location.");
            }

        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(rs);
            close(stmt);
        }
    }

    /**  Attempt to permanently delete a bar
     *  <locationId>
     *  <barId>
     *  
     *   You cannot delete the only bar at a location, so if another bar doesn't exist, this method
     *   will return an error detail.  If the bar is available to delete, then any draft lines
     *   pouring to this bar will be redirected to another valid bar (if multiple bars exist, the
     *   oldest bar will receive the lines).  
     */
    private void deleteCostCenter(Element toHandle, Element toAppend) throws HandlerException {

        int costCenterId = HandlerUtils.getRequiredInteger(toHandle, "costCenterId");
        int locationId = HandlerUtils.getRequiredInteger(toHandle, "locationId");
        int barId = HandlerUtils.getOptionalInteger(toHandle, "barId");
        int stationId = HandlerUtils.getOptionalInteger(toHandle, "stationId");
        int callerId = getCallerId(toHandle);

        PreparedStatement stmt = null;
        ResultSet rs = null;

        String getOther = "SELECT id FROM costCenter WHERE bar=? AND ccId!=? ORDER BY id ASC LIMIT 1";
        String getOtherByStation = "SELECT id FROM costCenter WHERE station=? AND ccId!=? ORDER BY id ASC LIMIT 1";
        String getCostCenter = " SELECT id FROM costCenter WHERE bar=? AND ccId=?";
        String getCostCenterByStation = " SELECT id FROM costCenter WHERE station=? AND ccId=?";
        String delete = "DELETE FROM costCenter WHERE id=?";

        try {
            if (barId > 0) {
                stmt = transconn.prepareStatement(getOther);
                stmt.setInt(1, barId);
            } else {
                stmt = transconn.prepareStatement(getOtherByStation);
                stmt.setInt(1, stationId);
            }
            stmt.setInt(2, costCenterId);
            rs = stmt.executeQuery();
            if (!rs.next()) {
                if (barId > 0) {
                    stmt = transconn.prepareStatement(getCostCenter);
                    stmt.setInt(1, barId);
                } else {
                    stmt = transconn.prepareStatement(getCostCenterByStation);
                    stmt.setInt(1, stationId);
                }
                stmt.setInt(2, costCenterId);
                rs = stmt.executeQuery();
                if (rs.next()) {
                    stmt = transconn.prepareStatement(delete);
                    stmt.setInt(1, rs.getInt(1));
                    stmt.executeUpdate();
                    String logMessage = "Deleted costCenter #" + costCenterId;
                    logger.portalDetail(callerId, "deleteCostCenter", locationId, logMessage, transconn);
                }
            } else {
                addErrorDetail(toAppend, "Unable to delete this cost center since it is the only one for the specified bar or station.");
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(rs);
            close(stmt);
        }
    }

    //NischaySharma_02-Oct-2009_Start
    //NischaySharma_02-Oct-2009_Start
    private void updateZones(Element toHandle, Element toAppend) throws HandlerException {

        String update = " UPDATE zone SET name=? WHERE id=? AND location = ? ";
        String checkName = " SELECT id FROM zone WHERE location=? AND LCASE(name)=LCASE(?) AND id != ? ";

        String updateZonePoints = " UPDATE zone_point SET new = 1, points = GEOMFROMTEXT(?) WHERE zone = ? ";


        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            Iterator zones = toHandle.elementIterator("zone");
            int colCount = 1;
            while (zones.hasNext()) {
                Element zone = (Element) zones.next();
                int zoneId = HandlerUtils.getRequiredInteger(zone, "zoneId");
                int locationId = HandlerUtils.getRequiredInteger(zone, "locationId");
                String points = HandlerUtils.getOptionalString(zone, "points");
                String zoneName = HandlerUtils.getOptionalString(zone, "zoneName");
                if (checkForeignKey("zone", zoneId, transconn) && checkForeignKey("location", locationId, transconn)) {
                    colCount = 1;
                    if (null != points && points.length() > 0) {
                        stmt = transconn.prepareStatement(updateZonePoints);
                        stmt.setString(colCount++, points);
                        stmt.setInt(colCount++, zoneId);
                        stmt.executeUpdate();
                    } else {
                        stmt = transconn.prepareStatement(checkName);
                        stmt.setInt(1, locationId);
                        stmt.setString(2, zoneName);
                        stmt.setInt(3, zoneId);
                        rs = stmt.executeQuery();
                        if (!rs.next()) {
                            stmt = transconn.prepareStatement(update);
                            stmt.setString(colCount++, zoneName);
                            stmt.setInt(colCount++, zoneId);
                            stmt.setInt(colCount++, locationId);
                            stmt.executeUpdate();
                        } else {
                            addErrorDetail(toAppend, "A zone with that name already exists");
                        }
                    }
                } else {
                    addErrorDetail(toAppend, "Invalid location ID: " + locationId + " or zone ID: " + zoneId);
                }
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error in updateZones: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
            close(rs);
        }

    }

    private void updateStations(Element toHandle, Element toAppend) throws HandlerException {

        String update = " UPDATE station SET name=? WHERE id=? AND bar = ? ";
        String checkName = " SELECT id FROM station WHERE bar=? AND LCASE(name)=LCASE(?) ";

        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            Iterator stations = toHandle.elementIterator("station");
            int colCount = 1;
            while (stations.hasNext()) {
                Element station = (Element) stations.next();
                int stationId = HandlerUtils.getRequiredInteger(station, "stationId");
                int barId = HandlerUtils.getRequiredInteger(station, "barId");

                if (checkForeignKey("station", stationId, transconn) && checkForeignKey("bar", barId, transconn)) {
                    String stationName = HandlerUtils.getRequiredString(station, "stationName");

                    stmt = transconn.prepareStatement(checkName);
                    stmt.setInt(1, barId);
                    stmt.setString(2, stationName);
                    rs = stmt.executeQuery();
                    if (!rs.next()) {
                        stmt = transconn.prepareStatement(update);
                        colCount = 1;
                        stmt.setString(colCount++, stationName);
                        stmt.setInt(colCount++, stationId);
                        stmt.setInt(colCount++, barId);
                        stmt.executeUpdate();
                    } else {
                        addErrorDetail(toAppend, "A station with that name already exists for the specified stand");
                    }
                } else {
                    addErrorDetail(toAppend, "Invalid bar ID: " + barId + " or station ID: " + stationId);
                }
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error in updateStations: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
            close(rs);
        }
    }
    //NischaySharma_02-Oct-2009_End

    //NischaySharma_14-Oct-2009_Start
    private void updateCostCenters(Element toHandle, Element toAppend) throws HandlerException {
        int locationId                      = HandlerUtils.getRequiredInteger(toHandle, "locationId");
        boolean isConcessions               = HandlerUtils.getRequiredBoolean(toHandle, "isConcessions");
        int callerId                        = getCallerId(toHandle);

        PreparedStatement stmt              = null;
        ResultSet rs                        = null;
        String checkNameCons                = " SELECT id FROM costCenter WHERE location = ? AND ccID = ? AND bar = ? "+ (isConcessions ? "AND zone = ? AND station = ? " : "");
        String updateCons                   = " UPDATE costCenter SET name = ?, ccID = ?, bar = ? " + (isConcessions ? ", zone = ?, station = ? " : "")  + " WHERE id = ? ";
        try {
            Iterator costCenters            = toHandle.elementIterator("costCenter");
            int colCount;
            while (costCenters.hasNext()) {
                Element costCenter          = (Element) costCenters.next();
                int ccId                    = HandlerUtils.getRequiredInteger(costCenter, "ccID");
                String name                 = HandlerUtils.getRequiredString(costCenter, "name");
                int id                      = HandlerUtils.getRequiredInteger(costCenter, "id");
                int bar                     = HandlerUtils.getRequiredInteger(costCenter, "barId");
                int zone                    = HandlerUtils.getOptionalInteger(costCenter, "zoneId");
                int station                 = HandlerUtils.getOptionalInteger(costCenter, "stationId");
                
                colCount                    = 1;
                stmt                        = transconn.prepareStatement(checkNameCons);
                stmt.setInt(colCount++, locationId);
                stmt.setInt(colCount++, ccId);
                stmt.setInt(colCount++, bar);
                if (isConcessions) {
                    stmt.setInt(colCount++, zone);
                    stmt.setInt(colCount++, station);
                }
                rs                          = stmt.executeQuery();
                if (!rs.next()) {
                    colCount                = 1;
                    stmt                    = transconn.prepareStatement(updateCons);
                    stmt.setString(colCount++, name);
                    stmt.setInt(colCount++, ccId);
                    stmt.setInt(colCount++, bar);
                    if (isConcessions) {
                        stmt.setInt(colCount++, zone);
                        stmt.setInt(colCount++, station);
                    }
                    stmt.setInt(colCount++, id);
                    stmt.executeUpdate();
                } else {
                    addErrorDetail(toAppend, "A duplicate cost center was found for: " + ccId);
                }
            }
            logger.portalDetail(callerId, "updateCostCenters", locationId, "updateCostCenters", 0, "", transconn);
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(rs);
            close(stmt);
        }
    }
    //NischaySharma_14-Oct-2009_End

    //NischaySharma_18-Jun-2010_Start

    private void resetPasswordByAdmin(Element toHandle, Element toAppend) throws HandlerException {
        int suAdminId = HandlerUtils.getRequiredInteger(toHandle, "suAdminId");
        String suAdminPasswd = HandlerUtils.getRequiredString(toHandle, "suAdminPasswd");
        int usrId = HandlerUtils.getRequiredInteger(toHandle, "usrId");
        String resetPasswd = HandlerUtils.getRequiredString(toHandle, "resetPasswd");;

        PreparedStatement stmt = null;
        ResultSet rs = null;

        String checkSuAdmin = " SELECT id FROM user WHERE id = ? AND password = ? AND customer = 0; ";
        String checkUser = " SELECT id FROM user WHERE id = ?; ";
        String updatePasswd = " UPDATE user SET password = ? WHERE id = ? ";


        try {
            stmt = transconn.prepareStatement(checkSuAdmin);
            stmt.setInt(1, suAdminId);
            stmt.setString(2, suAdminPasswd);
            rs = stmt.executeQuery();
            if(rs.next())
            {
                stmt = transconn.prepareStatement(checkUser);
                stmt.setInt(1, usrId);
                rs = stmt.executeQuery();
                if(rs.next())
                {
                    stmt = transconn.prepareStatement(updatePasswd);
                    stmt.setString(1, resetPasswd);
                    stmt.setInt(2, usrId);
                    stmt.executeUpdate();
                    toAppend.addElement("success").addText("Password reset was successfull");
                } else {
                    toAppend.addElement("error").addText("User does not exists");
                }
            } else {
                toAppend.addElement("error").addText("You are not an authorized user");
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(rs);
            close(stmt);
        }
    }

    //NischaySharma_18-Jun-2010_End

    /**
     * The folowing code is to update or insert the report requirements for each user for the email address
     * that they provide - SR
     */
    private void updateUserAlerts(Element toHandle, Element toAppend) throws HandlerException {

        int callerId                        = getCallerId(toHandle);
        SimpleDateFormat f                  = new SimpleDateFormat("HH:mm:ss");
        PreparedStatement stmt              = null, innerstmt = null;
        ResultSet rs                        = null, innerrs = null;
        
        
        int userId                          = HandlerUtils.getRequiredInteger(toHandle, "userId");

        Iterator i                          = toHandle.elementIterator("alerts");

        try {while (i.hasNext()) {
                Element alerts              = (Element) i.next();
                int alertId                 = HandlerUtils.getRequiredInteger(alerts, "alertId");
                int alertType               = HandlerUtils.getRequiredInteger(alerts, "alertType");

                //Check that this alert doesn't already exist in inventory at this location

                String alertTable           = "userAlerts";
                if (alertType == 1) {
                    alertTable              = "superUserAlerts";
                }
                String select               = " SELECT time FROM " + alertTable + " WHERE id=? AND user=?";

                stmt                        = transconn.prepareStatement(select);
                stmt.setInt(1, alertId);
                stmt.setInt(2, userId);
                rs                          = stmt.executeQuery();
                if (rs.next()) {
                    int timeId              = rs.getInt(1);

                    Iterator u              = alerts.elementIterator("update");
                    if (u.hasNext()) {
                        Element time        = (Element) u.next();
                        String alertTime    = HandlerUtils.getRequiredString(time, "alertTime");
                        int alertDay        = HandlerUtils.getOptionalInteger(time, "alertDay");
                        
                        java.util.Date d    = f.parse(alertTime);
                        java.util.Date date = new java.util.Date();
                        
                        if (alertType == 1) {
                            date            = new java.util.Date(d.getTime() - getAlertOffsetInMillis(0, 0));
                        } else {
                            String selectLoctionOffset
                                            = "SELECT tableType, tableId FROM userAlerts WHERE id = ?";
                            stmt            = transconn.prepareStatement(selectLoctionOffset);
                            stmt.setInt(1, alertId);
                            rs              = stmt.executeQuery();
                            if (rs.next()) {
                                int tableType
                                            = rs.getInt(1);
                                int tableId = rs.getInt(2);
                                date        = new java.util.Date(d.getTime() - getAlertOffsetInMillis(tableType, tableId));
                            } else {
                                date        = new java.util.Date(d.getTime());
                            }
                        }
                        
                        if (timeId > 0) {
                            String selectTime
                                            = " SELECT id FROM userAlertsTimeTable WHERE id=? AND time=? AND day = ?; ";
                            stmt            = transconn.prepareStatement(selectTime);
                            stmt.setInt(1, timeId);
                            stmt.setString(2, dateToString(date));
                            stmt.setInt(3, alertDay);
                            rs              = stmt.executeQuery();
                            if (!rs.next()) {
                                String updateTimeTable = " UPDATE userAlertsTimeTable SET time=?, day=? WHERE id=? ";
                                innerstmt = transconn.prepareStatement(updateTimeTable);
                                innerstmt.setString(1, dateToString(date));
                                innerstmt.setInt(2, alertDay);
                                innerstmt.setInt(3, timeId);
                                innerstmt.executeUpdate();
                            } else {
                                logger.generalWarning("Alert already exists for the specified time");
                                addErrorDetail(toAppend, "Alert already exists for the specified time");
                            }
                        } else {
                            String insertTimeTable = " INSERT INTO userAlertsTimeTable (time, day) VALUES (?, ?)";
                            innerstmt = transconn.prepareStatement(insertTimeTable);
                            innerstmt.setString(1, dateToString(date));
                            innerstmt.setInt(2, alertDay);
                            innerstmt.executeUpdate();

                            innerstmt       = transconn.prepareStatement("SELECT LAST_INSERT_ID()");
                            rs              = innerstmt.executeQuery();
                            if (rs.next()) {
                                timeId      = rs.getInt(1);
                            }

                            String updateAlert
                                            = " UPDATE " + alertTable + " SET time=? WHERE id=? ";
                            innerstmt       = transconn.prepareStatement(updateAlert);
                            innerstmt.setInt(1, timeId);
                            innerstmt.setInt(2, alertId);
                            innerstmt.executeUpdate();
                        }
                    }

                    Iterator d              = alerts.elementIterator("delete");
                    if (d.hasNext()) {
                        Element time        = (Element) d.next();
                        int confirmation    = HandlerUtils.getRequiredInteger(time, "confirmation");
                        if (confirmation > 0) {
                            String delete   = " DELETE FROM " + alertTable + " WHERE id=? ";
                            innerstmt       = transconn.prepareStatement(delete);
                            innerstmt.setInt(1, alertId);
                            innerstmt.executeUpdate();

                            if (timeId > 0) {
                                String deleteTimeTable
                                            = " DELETE FROM userAlertsTimeTable WHERE id=? ";
                                innerstmt   = transconn.prepareStatement(deleteTimeTable);
                                innerstmt.setInt(1, timeId);
                                innerstmt.executeUpdate();
                            }
                        }
                    }
                }
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } catch (Exception e) {
            logger.dbError("Parse error: " + e.getMessage());
            throw new HandlerException(e);
        } finally {
            close(innerstmt);
            close(innerrs);
            close(stmt);
            close(rs);
        }
    }
    private void updateBevBox(Element toHandle, Element toAppend) throws HandlerException {
        int locationId                      = HandlerUtils.getOptionalInteger(toHandle, "locationId");
        int bossId                          = HandlerUtils.getOptionalInteger(toHandle, "bossId");
        int callerId                        = getCallerId(toHandle);

        int paramsSet                       = 0;
        if (locationId >= 0) {
            paramsSet++;
        }
        if (bossId >= 0) {
            paramsSet++;
        }
        if (paramsSet > 1) {
            throw new HandlerException("Only one parameter can be set for updateBevBox.");
        }
        
        PreparedStatement stmt              = null;
        ResultSet rs                        = null;

        try {
            Iterator bevBoxs                = toHandle.elementIterator("bevBox");
            while (bevBoxs.hasNext()) {
                String checkNameCons        = " SELECT b.startSystem, b.totalSystems FROM bevBox b ";
                ArrayList<Integer> systemsArray
                                            = new ArrayList<Integer>();
                Element bevBox              = (Element) bevBoxs.next();
                int id                      = HandlerUtils.getRequiredInteger(bevBox, "id");

                if (locationId >= 0) {
                    checkNameCons               += " WHERE b.id NOT IN (?) AND b.location = ?";
                    stmt                        = transconn.prepareStatement(checkNameCons);
                    stmt.setInt(1, id);
                    stmt.setInt(2, locationId);
                    rs                          = stmt.executeQuery();
                } else if (bossId >= 0) {
                    checkNameCons               += " LEFT JOIN BOSS_Location BL ON BL.usbn_location = b.location WHERE b.id NOT IN (?) AND BL.id = ? ";
                    stmt                        = transconn.prepareStatement(checkNameCons);
                    stmt.setInt(1, id);
                    stmt.setInt(2, bossId);
                    rs                          = stmt.executeQuery();
                }
                while (rs.next()) {
                    for (int i = rs.getInt(1); i < rs.getInt(2); i++) {
                        systemsArray.add(i);
                    }
                }
                
                String name                 = HandlerUtils.getRequiredString(bevBox, "name");
                int totalSystems            = HandlerUtils.getRequiredInteger(bevBox, "totalSystems");
                int startSystem             = HandlerUtils.getRequiredInteger(bevBox, "startSystem");
                boolean details             = HandlerUtils.getOptionalBoolean(bevBox, "details");
                boolean coords              = HandlerUtils.getOptionalBoolean(bevBox, "coords");

                for (int i = startSystem; i < (startSystem + totalSystems); i++) {
                    if (systemsArray.contains(i)) { throw new HandlerException("One or more of the System numbers for " + name + " already exists for another bevBox");}
                    else {systemsArray.add(i);}
                }

                int colCount                = 1;
                String updateCons           = " UPDATE bevBox SET name=?, version=?, totalSystems=?, startSystem=?, systemInterval=?, active=?, alert=?, mac=?, dhcp=?, lastPoured=lastPoured " +
                                            (details ? ", ip=INET_ATON(?), gateway=INET_ATON(?), netmask=INET_ATON(?), dns1=INET_ATON(?), dns2=INET_ATON(?) " : "") +
                                            (coords ? ", latitude=?, longitude=? " : "") +
                                            " WHERE id = ? ";
                colCount                    = 1;
                stmt                        = transconn.prepareStatement(updateCons);
                stmt.setString(colCount++, name);
                stmt.setString(colCount++, HandlerUtils.getRequiredString(bevBox, "version"));
                stmt.setInt(colCount++, totalSystems);
                stmt.setInt(colCount++, startSystem);
                stmt.setInt(colCount++, HandlerUtils.getRequiredInteger(bevBox, "systemInterval"));
                stmt.setInt(colCount++, HandlerUtils.getRequiredInteger(bevBox, "active"));
                stmt.setInt(colCount++, HandlerUtils.getRequiredInteger(bevBox, "alert"));
                stmt.setString(colCount++, HandlerUtils.getRequiredString(bevBox, "mac"));
                stmt.setInt(colCount++, HandlerUtils.getRequiredInteger(bevBox, "dhcp"));
                if (details) {
                    stmt.setString(colCount++, HandlerUtils.getRequiredString(bevBox, "ip"));
                    stmt.setString(colCount++, HandlerUtils.getRequiredString(bevBox, "gateway"));
                    stmt.setString(colCount++, HandlerUtils.getRequiredString(bevBox, "netmask"));
                    stmt.setString(colCount++, HandlerUtils.getRequiredString(bevBox, "dns1"));
                    stmt.setString(colCount++, HandlerUtils.getRequiredString(bevBox, "dns2"));
                }
                if (coords) {
                    stmt.setDouble(colCount++, HandlerUtils.getRequiredDouble(bevBox, "latitude"));
                    stmt.setDouble(colCount++, HandlerUtils.getRequiredDouble(bevBox, "longitude"));
                }
                stmt.setInt(colCount++, id);
                stmt.executeUpdate();

                String logMessage           = "Updating bevBox: '" + name + "'";
                logger.portalDetail(callerId, "updateBevBox", locationId, "bevBox", id, logMessage, transconn);
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(rs);
            close(stmt);
        }
    }

    private void tempCommand(Element toHandle, Element toAppend) throws HandlerException {
        PreparedStatement stmt = null;
        String tempCommand = " DELETE FROM reading WHERE date BETWEEN '2011-05-05 10:20:47' AND '2011-05-05 17:46:02' AND value <= 0 AND line IN (15551, 15552, 15553, 15554, 15555, 15556, 15557, 15558, 15559, 15560); ";
        try {
            stmt = transconn.prepareStatement(tempCommand);
            stmt.executeUpdate();
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
        }
    }

    private void updateLineCleaning(Element toHandle, Element toAppend) throws HandlerException {
        int locationId                      = HandlerUtils.getRequiredInteger(toHandle, "locationId");
        int callerId                        = getCallerId(toHandle);
        int osPlatform                      = HandlerUtils.getOptionalInteger(toHandle, "osPlatform");
        int mobileUserId                    = HandlerUtils.getOptionalInteger(toHandle, "mobileUserId");   

        String selectLineCleaning           = "SELECT id FROM lineCleaning WHERE location = ? AND NOW() BETWEEN startTime and endTime";
        String updateLineCleaning           = "UPDATE lineCleaning SET endTime = NOW() WHERE id = ?";
        String selectGlanolaLineCleaning    = "SELECT id FROM glanolaLineCleaning WHERE location = ? AND NOW() BETWEEN startTime and endTime";        
        String updateGlanolaLineCleaning    = "UPDATE glanolaLineCleaning SET endTime = NOW() WHERE id = ?";
        PreparedStatement stmt              = null;
        ResultSet rs = null;

        try {
            if(callerId > 0 && locationId > 0 && isValidAccessUser(callerId, locationId,false)){
            stmt                            = transconn.prepareStatement(selectLineCleaning);
            stmt.setInt(1, locationId);
            rs                              = stmt.executeQuery();
            if (rs.next()) {
                stmt                        = transconn.prepareStatement(updateLineCleaning);
                stmt.setInt(1, rs.getInt(1));
                stmt.executeUpdate();
            }
            
            stmt                            = transconn.prepareStatement(selectGlanolaLineCleaning);
            stmt.setInt(1, locationId);
            rs                              = stmt.executeQuery();
            while(rs.next()) {
                stmt                        = transconn.prepareStatement(updateGlanolaLineCleaning);
                stmt.setInt(1, rs.getInt(1));
                stmt.executeUpdate();
            }
            
             if(osPlatform > 1 && callerId>0){
              if(mobileUserId <1){
                  mobileUserId              = getMobileUserId(callerId);
              }
              addUserHistory(callerId, "addLineCleaning", locationId, "Close Line Cleaning", mobileUserId,0);
          } else {
                 logger.portalDetail(callerId, "addLineCleaning", locationId, "addLineCleaning", 0, "", transconn);
             }
            }else {
                addErrorDetail(toAppend, "Invalid Access"  );
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(rs);
            close(stmt);
        }
    }

    /**  Add a user-defined beverage size
     *
     *  <locationId>000
     *  <size>
     *    <name>"String" : "Pint" max length 30
     *    <ounces>0.00 : size in ounces
     *  </size>
     *  <size>...</size>
     *
     *  will return an error if:
     *  the case-insensitive name already exists at this location
     *  the name is too long
     *  the size is non-positive
     *
     */
    private void updateBeverageSize(Element toHandle, Element toAppend) throws HandlerException {

        boolean forCustomer                 = HandlerUtils.getOptionalBoolean(toHandle, "forCustomer");
        int location                        = 0;
        int callerId                        = getCallerId(toHandle);

        PreparedStatement stmt              = null;
        ResultSet rs                        = null;

        logger.portalAction("updateBeverageSize");
        String selectLocations              = " SELECT id FROM location WHERE customer = ? ";
        try {
            locationMap                     = new LocationMap(transconn);
            if (forCustomer) {
                int customerId              = HandlerUtils.getRequiredInteger(toHandle, "customerId");  
                stmt                        = transconn.prepareStatement("SELECT id FROM customer WHERE id = ? AND groupId = 2");
                stmt.setInt(1, customerId);
                rs                          = stmt.executeQuery();
                if (rs.next()) {
                    selectLocations         = "SELECT id FROM location WHERE customer IN (SELECT id FROM customer WHERE groupId = ?)";
                    customerId              = 2;
                }   
                stmt                        = transconn.prepareStatement(selectLocations);
                stmt.setInt(1, customerId);
                rs                          = stmt.executeQuery();
                while (rs.next()) {
                    location                = rs.getInt(1);
                    updateBeverageSizeDetail(callerId, location, toHandle, toAppend);
                }
            } else {
                location                    = HandlerUtils.getRequiredInteger(toHandle, "locationId");
                updateBeverageSizeDetail(callerId, location, toHandle, toAppend);
            }
        } catch (SQLException sqle) {
            logger.dbError("SQL Exception in updateBeverageSize: " + sqle.toString());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
            close(rs);
        }
    }


    private void updateBeverageSizeDetail(int callerId, int location, Element toHandle, Element toAppend) throws HandlerException {

        String getSizeId                    = "SELECT id FROM beverageSize WHERE location=? AND ounces BETWEEN (?-0.01) AND (?+0.01)";
        String delete                       = "DELETE FROM beverageSize WHERE location=? AND ounces BETWEEN (?-0.01) AND (?+0.01) ";

        PreparedStatement stmt              = null;
        ResultSet rs                        = null, rs2 = null;

        DecimalFormat twoPlaces             = new DecimalFormat("0.00");
        logger.portalAction("updateBeverageSize");
        try {
            Iterator updateSizes            = toHandle.elementIterator("update");
            while (updateSizes.hasNext()) {
                Element sizeEl              = (Element) updateSizes.next();
                String name                 = HandlerUtils.getRequiredString(sizeEl, "name");
                float ounces                = HandlerUtils.getRequiredFloat(sizeEl, "ounces");
                float originalOunces        = HandlerUtils.getRequiredFloat(sizeEl, "originalOunces");

                //check params
                stmt                        = transconn.prepareStatement(getSizeId);
                stmt.setInt(1, location);
                stmt.setFloat(2, originalOunces);
                stmt.setFloat(3, originalOunces);
                rs                          = stmt.executeQuery();
                if (!rs.next()) {
                    addErrorDetail(toAppend, "The size '" + ounces + "' does not exists for " + locationMap.getLocation(location));
                } else if (ounces < 0.1) {
                    addErrorDetail(toAppend, "The size '" + ounces + "' is not allowed for " + locationMap.getLocation(location));
                } else {
                    int sizeId              = rs.getInt(1);

                    String updateSize       = "UPDATE beverageSize SET name = ?,ounces = ? WHERE id = ?";
                    String selectBeverage   = "SELECT id FROM beverage WHERE location = ? AND ounces = ?";
                    String updateBeverage   = "UPDATE beverage SET ounces = ? WHERE id = ?";
                    String selectIngredientCount
                                            = "SELECT COUNT(id) FROM ingredient WHERE beverage = ? GROUP BY beverage";
                    String selectIngredients= "SELECT id FROM ingredient WHERE beverage = ?";
                    String updateIngredients= "UPDATE ingredient SET ounces = ? WHERE id = ?";

                    // OK to add
                    stmt                    = transconn.prepareStatement(updateSize);
                    stmt.setString(1, name);
                    stmt.setFloat(2, ounces);
                    stmt.setInt(3, sizeId);
                    stmt.executeUpdate();

                    String logMessage       = "Updating beverage size named '" + name + "' " + "(" + twoPlaces.format(ounces) + " oz)";
                    logger.portalDetail(callerId, "updateBeverageSize", location, "beverageSize", sizeId, logMessage, transconn);

                    stmt                    = transconn.prepareStatement(selectBeverage);
                    stmt.setInt(1, location);
                    stmt.setFloat(2, originalOunces);
                    rs                      = stmt.executeQuery();
                    while (rs.next()) {
                        int beverageId      = rs.getInt(1);
                        stmt                = transconn.prepareStatement(updateBeverage);
                        stmt.setFloat(1, ounces);
                        stmt.setInt(2, beverageId);
                        stmt.executeUpdate();

                        int ingredientCount = 1;
                        stmt                = transconn.prepareStatement(selectIngredientCount);
                        stmt.setInt(1, beverageId);
                        rs2                 = stmt.executeQuery();
                        if (rs2.next()) {
                            ingredientCount = rs2.getInt(1);
                        }

                        stmt                = transconn.prepareStatement(selectIngredients);
                        stmt.setInt(1, beverageId);
                        rs2                 = stmt.executeQuery();
                        while (rs2.next()) {
                            stmt            = transconn.prepareStatement(updateIngredients);
                            stmt.setFloat(1, ounces/ingredientCount);
                            stmt.setInt(2, rs2.getInt(1));
                            stmt.executeUpdate();
                        }
                    }
                }
            }

            Iterator deleteSizes            = toHandle.elementIterator("delete");
            while (deleteSizes.hasNext()) {
                Element sizeEl              = (Element) deleteSizes.next();
                float originalOunces        = HandlerUtils.getRequiredFloat(sizeEl, "originalOunces");

                stmt                        = transconn.prepareStatement(delete);
                stmt.setInt(1, location);
                stmt.setFloat(2, originalOunces);
                stmt.setFloat(3, originalOunces);
                stmt.executeUpdate();

                String logMessage           = "Delete Beverage Size '" + originalOunces + "'";
                logger.portalDetail(callerId, "deleteBeverageSizes", 0, "location", location, logMessage, transconn);
            }
        } catch (SQLException sqle) {
            logger.dbError("SQL Exception in updateBeverageSize: " + sqle.toString());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
            close(rs);
            close(rs2);
        }
    }

    public void sendMail(String title, String userName, String emailAddr, String supportEmailAddr, String templateMessageTitle,
            String templateMessage, StringBuilder emailBody, boolean sendBCC) {
        String emailTemplatePath            = HandlerUtils.getSetting("email.templatePath");
        if ((emailTemplatePath == null) || "".equals(emailTemplatePath)) {
            emailTemplatePath               = ".";
        }
        logger.debug("Packaging Email");
        try {
            if ((emailBody != null) && (emailBody.length() > 0)) {
                logger.debug("Loading Template");
                TemplatedMessage poEmail   = new TemplatedMessage(templateMessageTitle, emailTemplatePath, templateMessage);

                poEmail.setSender("tech@beerboard.com");
                poEmail.setRecipient(emailAddr);
                if (sendBCC) {
                    poEmail.setRecipientBCC(supportEmailAddr);
                }
                poEmail.setField("TITLE", title);
                poEmail.setField("BODY", emailBody.toString());
                poEmail.send();
                logger.debug("Email sent successfully for " + userName);
            }
        } catch (MailException me) {
            logger.dbError("Error sending message to " + emailAddr + ": " + me.toString());
        }
    }

    private void updateCustomerPeriods(Element toHandle, Element toAppend) throws HandlerException {

        int callerId                        = getCallerId(toHandle);
        int id                              = HandlerUtils.getRequiredInteger(toHandle, "id");
        String date                         = HandlerUtils.getRequiredString(toHandle, "date");
        String start                        = HandlerUtils.getRequiredString(toHandle, "start");
        String end                          = HandlerUtils.getRequiredString(toHandle, "end");
        String details                      = HandlerUtils.getRequiredString(toHandle, "details");

        String insertCustomerPeriods        = " UPDATE customerPeriods SET date = ?, start = ?, end = ?, description = ? WHERE id = ?; ";

        PreparedStatement stmt              = null;
        try {
            stmt                            = transconn.prepareStatement(insertCustomerPeriods);
            stmt.setString(1,date);
            stmt.setString(2,start);
            stmt.setString(3,end);
            stmt.setString(4,details);
            stmt.setInt(5,id);
            stmt.executeUpdate();
        } catch (SQLException sqle) {
            logger.dbError("Database error in updateCustomerPeriods: "+sqle.toString());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
        }
    }

    private void transferTableData(Element toHandle, Element toAppend) throws HandlerException {

        boolean reading                     = HandlerUtils.getOptionalBoolean(toHandle, "reading");
        boolean sales                       = HandlerUtils.getOptionalBoolean(toHandle, "sales");
        boolean cooler                      = HandlerUtils.getOptionalBoolean(toHandle, "cooler");
        boolean dataMod                     = HandlerUtils.getOptionalBoolean(toHandle, "dataMod");
        boolean loadingData                 = false;

        String selectLastTransferId         = "SELECT lastRecord FROM tableTransferId WHERE tableType = ? AND active = 0";
        String stopDataTransfer             = "UPDATE tableTransferId SET active = 1 WHERE id = ?;";
        String updateLastTransferId         = "UPDATE tableTransferId SET active = 0, lastRecord = ? WHERE id = ?;";

        String selectReadingData            = "SELECT date, line, value, quantity, type, id FROM reading WHERE id > ?;";
        String insertReadingData            = "INSERT INTO reading (date, line, value, quantity, type) VALUES (?,?,?,?,?);";

        String selectSalesData              = "SELECT location, pluNumber, costCenter, quantity, date, checkId, reportRecordId, sid FROM sales WHERE sid > ?;";
        String insertSalesData              = "INSERT INTO sales (location, pluNumber, costCenter, quantity, date, checkId, reportRecordId) VALUES (?,?,?,?,?,?,?);";

        String selectCoolerTData            = "SELECT cooler, value, date, id FROM coolerTemperature WHERE id > ?;";
        String insertCoolerTData            = "INSERT INTO coolerTemperature (cooler, value, date) VALUES (?,?,?);";

        String selectDataModData            = "SELECT location, modType, modId, start, end, date, id FROM dataModNew WHERE id > ?;";
        String insertDataModData            = "INSERT INTO dataModNew (location, modType, modId, start, end, date) VALUES (?,?,?,?,?,?);";

        PreparedStatement stmt              = null;
        ResultSet rs                        = null;
        try {

            if (reading) {

                stmt                        = transconn.prepareStatement(selectLastTransferId);
                stmt.setInt(1, 1);
                rs                          = stmt.executeQuery();
                if (rs.next()) {
                    logger.debug("Adding Reading Data: ");
                    stmt                    = transconn.prepareStatement(stopDataTransfer);
                    stmt.setInt(1, 1);
                    stmt.executeUpdate();
                    
                    int lastRecordId        = rs.getInt(1);
                    stmt                    = transconn.prepareStatement(selectReadingData);
                    stmt.setInt(1, lastRecordId);
                    rs                      = stmt.executeQuery();
                    while (rs.next()) {
                        stmt                = transconn.prepareStatement(insertReadingData);
                        stmt.setTimestamp(1, rs.getTimestamp(1));
                        stmt.setInt(2, rs.getInt(2));
                        stmt.setDouble(3, rs.getDouble(3));
                        stmt.setDouble(4, rs.getDouble(4));
                        stmt.setInt(5, rs.getInt(5));
                        stmt.executeUpdate();
                        lastRecordId        = rs.getInt(6);
                    }

                    logger.debug("Updating Reading ID");
                    stmt                    = transconn.prepareStatement(updateLastTransferId);
                    stmt.setInt(1, lastRecordId);
                    stmt.setInt(2, 1);
                    stmt.executeUpdate();
                }
            }
            
            if (sales) {

                stmt                        = transconn.prepareStatement(selectLastTransferId);
                stmt.setInt(1, 2);
                rs                          = stmt.executeQuery();
                if (rs.next()) {
                    logger.debug("Adding Sales Data: ");
                    stmt                    = transconn.prepareStatement(stopDataTransfer);
                    stmt.setInt(1, 2);
                    stmt.executeUpdate();
                    
                    int lastRecordId        = rs.getInt(1);
                    stmt                    = transconn.prepareStatement(selectSalesData);
                    stmt.setInt(1, lastRecordId);
                    rs                      = stmt.executeQuery();
                    while (rs.next()) {
                        stmt                = transconn.prepareStatement(insertSalesData);
                        stmt.setInt(1, rs.getInt(1));
                        stmt.setString(2, rs.getString(2));
                        stmt.setInt(3, rs.getInt(3));
                        stmt.setDouble(4, rs.getDouble(4));
                        stmt.setTimestamp(5, rs.getTimestamp(5));
                        stmt.setString(6, rs.getString(6));
                        stmt.setInt(7, rs.getInt(7));
                        stmt.executeUpdate();
                        lastRecordId        = rs.getInt(8);
                    }

                    logger.debug("Updating Sales ID");
                    stmt                    = transconn.prepareStatement(updateLastTransferId);
                    stmt.setInt(1, lastRecordId);
                    stmt.setInt(2, 2);
                    stmt.executeUpdate();
                }
            }

            if (cooler) {

                stmt                        = transconn.prepareStatement(selectLastTransferId);
                stmt.setInt(1, 3);
                rs                          = stmt.executeQuery();
                if (rs.next()) {
                    logger.debug("Adding Cooler Temp Data: ");
                    stmt                    = transconn.prepareStatement(stopDataTransfer);
                    stmt.setInt(1, 3);
                    stmt.executeUpdate();
                    
                    int lastRecordId        = rs.getInt(1);
                    stmt                    = transconn.prepareStatement(selectCoolerTData);
                    stmt.setInt(1, lastRecordId);
                    rs                      = stmt.executeQuery();
                    while (rs.next()) {
                        stmt                = transconn.prepareStatement(insertCoolerTData);
                        stmt.setInt(1, rs.getInt(2));
                        stmt.setDouble(2, rs.getDouble(2));
                        stmt.setTimestamp(3, rs.getTimestamp(3));
                        stmt.executeUpdate();
                        lastRecordId        = rs.getInt(4);
                    }

                    logger.debug("Updating Cooler Temp ID");
                    stmt                    = transconn.prepareStatement(updateLastTransferId);
                    stmt.setInt(1, lastRecordId);
                    stmt.setInt(2, 3);
                    stmt.executeUpdate();
                }
            }

            if (dataMod) {

                stmt                        = transconn.prepareStatement(selectLastTransferId);
                stmt.setInt(1, 4);
                rs                          = stmt.executeQuery();
                if (rs.next()) {
                    logger.debug("Adding Data Mod Data: ");
                    stmt                    = transconn.prepareStatement(stopDataTransfer);
                    stmt.setInt(1, 4);
                    stmt.executeUpdate();

                    int lastRecordId        = rs.getInt(1);
                    stmt                    = transconn.prepareStatement(selectDataModData);
                    stmt.setInt(1, lastRecordId);
                    rs                      = stmt.executeQuery();
                    while (rs.next()) {
                        stmt                = transconn.prepareStatement(insertDataModData);
                        stmt.setInt(1, rs.getInt(1));
                        stmt.setInt(2, rs.getInt(2));
                        stmt.setInt(3, rs.getInt(3));
                        stmt.setTimestamp(4, rs.getTimestamp(4));
                        stmt.setTimestamp(5, rs.getTimestamp(5));
                        stmt.setString(6, rs.getString(6));
                        stmt.executeUpdate();
                        lastRecordId        = rs.getInt(7);
                    }

                    logger.debug("Updating Data Mod ID");
                    stmt                    = transconn.prepareStatement(updateLastTransferId);
                    stmt.setInt(1, lastRecordId);
                    stmt.setInt(2, 4);
                    stmt.executeUpdate();
                }
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
            close(rs);
        }
    }

    public void updateComponentStatus(int newLastCheckId, int componentMapId) throws HandlerException  {
        PreparedStatement stmt              = null;
        try {
            stmt                            = transconn.prepareStatement("UPDATE componentLocationMap SET lastCheckId = ? WHERE id = ?");
            stmt.setInt(1, newLastCheckId);
            stmt.setInt(2, componentMapId);
            stmt.executeUpdate();
        }   catch(SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
        }
    }
    
    
    private void updateBeerBoardSettings(Element toHandle, Element toAppend) throws HandlerException {

        int callerId                        = getCallerId(toHandle);
        int location                        = HandlerUtils.getRequiredInteger(toHandle, "locationId");
        int template                        = HandlerUtils.getRequiredInteger(toHandle, "template");
        int tile                            = 0;
        String updateTileOption             = "UPDATE bbtvUserMac SET tile = ? WHERE location = ?; ";
        String updateLocationFeed           = "UPDATE locationBeerBoardMap SET restart = 1 WHERE location = ?; ";
        String updateTemplate               = "UPDATE locationBeerBoardMap SET template = ? WHERE location = ?; ";
        String updateTileTemplate           = "UPDATE bbtvUserMac SET template = ? WHERE location = ?; ";
        String selectTemplate               = "SELECT tile,template FROM bbtvMenuTemplate WHERE id= ?";

        PreparedStatement stmt              = null;
        ResultSet rs                        = null;
        try {
            stmt                            = transconn.prepareStatement(selectTemplate);
            stmt.setInt(1,template);
            rs                              = stmt.executeQuery();
            if (rs.next()) {  
                
                tile                        = rs.getInt(1);
                template                    = rs.getInt(2);
                if(tile > 0) {                    
                    stmt                    = transconn.prepareStatement(updateTileTemplate);
                    stmt.setInt(1,template);
                    stmt.setInt(2,location);
                    stmt.executeUpdate();
                } else {
                    
                    stmt                    = transconn.prepareStatement(updateTemplate);
                    stmt.setInt(1,template);
                    stmt.setInt(2,location);
                    stmt.executeUpdate();
                }
            } 
         
                
                

            stmt                            = transconn.prepareStatement(updateTileOption);
            stmt.setInt(1,tile);
            stmt.setInt(2,location);
            stmt.executeUpdate();


            stmt                            = transconn.prepareStatement(updateLocationFeed);
            stmt.setInt(1,location);
            stmt.executeUpdate();
            logger.portalDetail(callerId, "updateBeerBoardSettings", location, "updateBeerBoardSettings", template, "", transconn);
        } catch (SQLException sqle) {
            logger.dbError("Database error in updateTemplate: "+sqle.toString());
            throw new HandlerException(sqle);
        } finally {
            close(rs);
            close(stmt);
        }
    }

    public void addNewBeerPushNotification(int location, int productId) throws SQLException, HandlerException {
        
        String limitedUsers                 = "438,1798";
        PreparedStatement stmt              = null;     
        ResultSet rs                        = null,rsDetails = null;
        String selectLocation               = "SELECT boardname, latitude, longitude, (SELECT logo from locationGraphics WHERE location = l.id) FROM location l WHERE l.id = ?";
        String selectStyleDetail            = "SELECT (SELECT style FROM beerStylesMap WHERE productSet=pS.id),p.name,pS.id,p.id,pS.name FROM product p LEFT JOIN productSetMap pSM ON pSM.product=p.id LEFT JOIN productSet pS ON pS.id=pSM.productSet"
                                            + "  WHERE p.id  = ?  AND pS.productSetType=9  ORDER by p.name;";
        String getLastId                    = " SELECT LAST_INSERT_ID()";
        try {
            String locationName             ="", message ="", productName="";
            double latitude                 = 0,longitude = 0;
            int messageId                   = 0;
            stmt                            = transconn.prepareStatement(selectLocation);
            stmt.setInt(1, location);             
            rs                              = stmt.executeQuery();
            if (rs.next()) {
                locationName                = rs.getString(1);
                latitude                    = rs.getDouble(2);
                longitude                   = rs.getDouble(3);
            }
            stmt                            = transconn.prepareStatement(selectStyleDetail);
            stmt.setInt(1, productId);             
            rs                              = stmt.executeQuery();
            if (rs.next()) {
                 int style                  = rs.getInt(1);
                 productName                = rs.getString(2);
                 
                 message                    = productName + " Now On Tap @ " + locationName;
                 
                 stmt                       = transconn.prepareStatement("INSERT INTO pushMessage (message, location, reward, category, pushTime) VALUES (?, ?, ?, 2,now());");
                 stmt.setString(1, message);
                 stmt.setInt(2, location);
                 stmt.setInt(3, productId);
                 stmt.executeUpdate();

                 stmt                       = transconn.prepareStatement(getLastId);
                 rsDetails                  = stmt.executeQuery();
                 if(rsDetails.next()) {
                        messageId           = rsDetails.getInt(1);
                 }
                
                 String selectUser          = "SELECT u.id,u.username,platform,u.deviceToken,(6371 * acos( cos( radians(?) ) * cos( radians( latitude ) ) * cos( radians( ?) - radians(longitude) ) + sin( radians(?) ) * sin( radians(latitude) ) )) AS distant"
                                            + " FROM bbtvMobileUser u WHERE styles LIKE ('"+style+"') OR styles LIKE ('%,"+style+",%') OR styles LIKE ('"+style+",%') OR  styles LIKE ('%,"+style+"')"
                                            + " AND  LENGTH(deviceToken) > 4 AND arrival > SUBDATE(NOW(), INTERVAL 20 DAY) HAVING distant < 33"
                                            + " UNION"
                                            + " SELECT u.id,u.username,styles,deviceToken,(6371 * acos( cos( radians(?) ) * cos( radians( latitude ) ) * cos( radians( ?) - radians(longitude) ) + sin( radians(?) ) * sin( radians(latitude) ) )) AS distant"
                                            + " FROM favouriteLocation fL LEFT JOIN bbtvMobileUser u ON u.id=fL.user WHERE fL.location=? AND   LENGTH(deviceToken) > 4 AND arrival > SUBDATE(NOW(), INTERVAL 20 DAY) HAVING distant < 33 "
                                            + " UNION "
                                            + " SELECT u.id,u.username,platform,deviceToken,(6371 * acos( cos( radians(?) ) * cos( radians( latitude ) ) * cos( radians( ?) - radians(longitude) ) + sin( radians(?) ) * sin( radians(latitude) ) )) AS distant"
                                            + " FROM  bbtvMobileUser u WHERE u.id IN (2894) AND LENGTH(deviceToken) > 4 AND arrival > SUBDATE(NOW(), INTERVAL 20 DAY) ORDER by username ;";
                 
                 String selectLimitedUser   = "SELECT u.id,u.username,platform,deviceToken,(6371 * acos( cos( radians(?) ) * cos( radians( latitude ) ) * cos( radians( ?) - radians(longitude) ) + sin( radians(?) ) * sin( radians(latitude) ) )) AS distant"
                                            + " FROM  bbtvMobileUser u WHERE u.id IN ("+limitedUsers+") AND LENGTH(deviceToken) > 4 AND arrival > SUBDATE(NOW(), INTERVAL 20 DAY) ORDER by username ;";
                 
                 String selectUserByLocation= "SELECT u.id, u.username, (6371 * acos( cos( radians(?) ) * cos( radians( latitude ) ) * cos( radians( ?) - radians(longitude) ) + sin( radians(?) ) * sin( radians(latitude) ) )) AS distant "
                                            + " FROM favoriteLocation fL LEFT JOIN bbtvMobileUser u ON u.id=fL.user WHERE fL.location=? AND   LENGTH(deviceToken) > 4 AND arrival > SUBDATE(NOW(), INTERVAL 20 DAY);";
                 String checkUserByProduct  = "SELECT id FROM favoriteBeer WHERE user=? AND product = ?;";     
                 String checkUserByStyle    = "SELECT u.id,u.username FROM bbtvMobileUser u WHERE u.id =? AND ( u.styles LIKE ('"+style+"') OR u.styles LIKE ('%,"+style+",%') OR u.styles LIKE ('"+style+",%') OR  u.styles LIKE ('%,"+style+"')) "
                                            + " AND  LENGTH(deviceToken) > 4 AND arrival > SUBDATE(NOW(), INTERVAL 20 DAY);";
                
                  logger.debug("Style:"+style);
                   
                  
                  stmt                     = transconn.prepareStatement(selectUserByLocation); 
                  stmt.setDouble(1, latitude);
                  stmt.setDouble(2, longitude);
                  stmt.setDouble(3, latitude);
                  stmt.setInt(4,location);
                  rs                        = stmt.executeQuery();
                  while (rs.next()) {
                      int userId            = rs.getInt(1);
                       stmt                 = transconn.prepareStatement(checkUserByStyle);  
                       stmt.setInt(1,userId);
                       // logger.debug("Style:"+checkUserByStyle+""+userId);
                       rsDetails            = stmt.executeQuery();
                       boolean canSend      = false;
                       if(rsDetails.next()){
                            canSend          = true;
                            //logger.debug("By Style:"+canSend);
                       } else {
                            stmt             = transconn.prepareStatement(checkUserByProduct);  
                            stmt.setInt(1,userId);
                            stmt.setInt(2,productId);
                            rsDetails            = stmt.executeQuery();
                            if(rsDetails.next()){
                                canSend        = true;
                                //logger.debug("By product:"+canSend);
                            }
                       }
                       if(canSend){
                           stmt                  = transconn.prepareStatement("INSERT INTO pushMessageMap (user, message) VALUES (?,?);");
                           stmt.setInt(1,userId);
                           stmt.setInt(2,messageId);
                           stmt.executeUpdate();
                           logger.debug("user:"+rs.getInt(1)+" - "+rs.getString(2));
                           logger.debug("Distance:"+rs.getDouble(3));
                       }
                  }
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: "+sqle.toString());
            throw new HandlerException(sqle);
        } finally {
             close(rsDetails);
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
    
    
    //GenericHandler
    /**  Update all fields for one or more inventory records
     *
     */
    private void updateTestInventory(Element toHandle, Element toAppend) throws HandlerException {

        int callerId = getCallerId(toHandle);
        HashMap<Integer, HashMap> SupplierProductMap = new HashMap<Integer, HashMap>();
        HashMap<Integer, String> ProductPLUMap = new HashMap<Integer, String>();

        String update = "UPDATE inventory SET minimumQty=?,qtyToHave=?,plu=?,supplier=?,kegSize=?,qtyOnHand=?,bottleSize=?,kegLine = ? " +
                " WHERE id=? AND location = ?";

        String selectSupplierProducts = "SELECT pSM.product, pSM.plu FROM productSetMap pSM LEFT JOIN supplier s ON s.productSet = pSM.productSet WHERE s.id = ? ";

        String select = "SELECT i.qtyOnHand, pr.name, i.product FROM inventory i " +
                " LEFT JOIN product pr ON i.product=pr.id WHERE i.id=? AND i.location=?";

        String selectkegLineInventory = "SELECT i.qtyOnHand, pr.name FROM inventory i " +
                " LEFT JOIN product pr ON i.product=pr.id WHERE i.location=? AND i.kegLine = ?";

        PreparedStatement stmt = null;
        ResultSet rs = null;

        Iterator i = toHandle.elementIterator("inventory");
        int productId = 0;
        try {
            while (i.hasNext()) {
                Element inv = (Element) i.next();

                int invId = HandlerUtils.getRequiredInteger(inv, "invId");
                int locationId = HandlerUtils.getRequiredInteger(inv, "locationId");
                int coolerId = HandlerUtils.getOptionalInteger(inv, "coolerId");
                int lineId = 0;
                lineId = HandlerUtils.getOptionalInteger(inv, "lineId");
                float reorderPoint = HandlerUtils.getRequiredFloat(inv, "reorderPoint");
                float reorderQty = HandlerUtils.getRequiredFloat(inv, "reorderQty");
                int supplierId = HandlerUtils.getRequiredInteger(inv, "supplierId");
                int kegSize = 0;
                kegSize = HandlerUtils.getOptionalInteger(inv, "kegSize");
                String plu = HandlerUtils.getOptionalString(inv, "plu");
                float qtyOnHand = HandlerUtils.getRequiredFloat(inv, "qtyOnHand");
                int bottleSize = 0;
                bottleSize = HandlerUtils.getOptionalInteger(inv, "bottleSize");


                if (SupplierProductMap.containsKey(supplierId)) {
                    ProductPLUMap = SupplierProductMap.get(supplierId);
                } else {
                    stmt = transconn.prepareStatement(selectSupplierProducts);
                    stmt.setInt(1, supplierId);
                    rs = stmt.executeQuery();
                    while (rs.next()) {
                        ProductPLUMap.put(new Integer(rs.getInt(1)), rs.getString(2));
                    }
                    SupplierProductMap.put(supplierId, ProductPLUMap);
                }

                if (checkForeignKey("inventory", invId, transconn) && checkForeignKey("location", locationId, transconn) && checkForeignKey("supplier", supplierId, transconn)) {


                    stmt = transconn.prepareStatement(select);
                    stmt.setInt(1, invId);
                    stmt.setInt(2, locationId);
                    rs = stmt.executeQuery();
                    if (rs.next()) {
                        float oldQty = rs.getFloat(1);
                        productId = rs.getInt(3);
                        if (ProductPLUMap.containsKey(productId)) {
                            plu = ProductPLUMap.get(productId);
                        } else if (plu == null) {
                            plu = "";
                        }
                        // We only update the quantity if the new value if there is a 0.1 keg difference or more 
                        // -or- if the new value is a whole number
                        if (Math.abs(oldQty - qtyOnHand) <= 0.1 && (Math.abs(Math.round(qtyOnHand) - qtyOnHand) > 0.02)) {
                            qtyOnHand = oldQty;
                            // addErrorDetail(toAppend, "Didn't update qty on hand for ID "+invId+": new qty was within 0.1 kegs of the old value");
                        }

                        stmt = transconn.prepareStatement(update);
                        stmt.setFloat(1, reorderPoint);
                        stmt.setFloat(2, reorderQty);
                        stmt.setString(3, plu);
                        stmt.setInt(4, supplierId);
                        stmt.setInt(5, kegSize);
                        stmt.setFloat(6, qtyOnHand);
                        stmt.setInt(7, bottleSize);
                        stmt.setInt(8, lineId);
                        stmt.setInt(9, invId);
                        stmt.setInt(10, locationId);
                        stmt.executeUpdate();

                        String logMessage = "Updating inventory fields for " + rs.getString(2) + " (" + invId + ")";
                        logger.portalDetail(callerId, "updateInventory", locationId, "inventory", invId, logMessage, transconn);

                    }


                } else {
                    logger.dbError("Foreign key check failed for Inv# " + invId + " or Loc# " + locationId + " or Sup# " + supplierId);
                    addErrorDetail(toAppend, "Unable to update inv ID " + invId + "; a database problem occurred");
                }





            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
            close(rs);
        }

    }

    /**  Update all fields for one or more bottle inventory records 
     *
     */
    private void updateBottleInventory(Element toHandle, Element toAppend) throws HandlerException {

        int callerId = getCallerId(toHandle);

        String select = "SELECT * FROM bottleInv WHERE id = ? AND location = ? AND product = ? AND lastActualInv = ?";

        String insert = "INSERT INTO bottleInv (product, location, lastActualInv, received, sold, calcOnHand, actualInv, var, date) VALUES (?,?,?,?,?,?,?,?,?)";

        String update = "UPDATE bottleInv SET actualInv=? WHERE id = ? AND location = ? AND product = ? AND lastActualInv = ?";

        String updateInventory = "UPDATE inventory SET qtyOnHand=? WHERE product=? AND location = ?";

        PreparedStatement stmt = null;
        ResultSet rs = null;

        PreparedStatement stmt1 = null;
        ResultSet rs1 = null;

        Iterator i = toHandle.elementIterator("bottleInventory");

        try {
            while (i.hasNext()) {
                Element inv = (Element) i.next();

                int invId = HandlerUtils.getRequiredInteger(inv, "invId");
                int product = HandlerUtils.getRequiredInteger(inv, "product");
                int location = HandlerUtils.getRequiredInteger(inv, "location");
                float lastActualInv = HandlerUtils.getRequiredFloat(inv, "lastActualInv");
                int received = HandlerUtils.getOptionalInteger(inv, "received");
                float sold = HandlerUtils.getRequiredFloat(inv, "sold");
                float calcOnHand = HandlerUtils.getRequiredFloat(inv, "calcOnHand");
                float actual = HandlerUtils.getRequiredFloat(inv, "actual");
                String variance = HandlerUtils.getRequiredString(inv, "variance");
                String date1 = HandlerUtils.getRequiredString(inv, "date1");

                if (received < 1) {
                    received = 0;
                }

                stmt = transconn.prepareStatement(select);
                stmt.setInt(1, invId);
                stmt.setInt(2, location);
                stmt.setInt(3, product);
                stmt.setFloat(4, lastActualInv);
                rs = stmt.executeQuery();
                if (rs.next()) {
                    stmt = transconn.prepareStatement(update);
                    stmt.setFloat(1, actual);
                    stmt.setInt(2, invId);
                    stmt.setInt(3, location);
                    stmt.setInt(4, product);
                    stmt.setFloat(5, lastActualInv);
                    stmt.executeUpdate();
                    logger.debug("Overwriting existing Bottle Inventory");
                } else {
                    stmt = transconn.prepareStatement(insert);
                    stmt.setInt(1, product);
                    stmt.setInt(2, location);
                    stmt.setFloat(3, lastActualInv);
                    stmt.setFloat(4, received);
                    stmt.setFloat(5, sold);
                    stmt.setFloat(6, calcOnHand);
                    stmt.setFloat(7, actual);
                    stmt.setString(8, variance);
                    stmt.setString(9, date1);
                    stmt.executeUpdate();
                    logger.debug("Inserting Bottle Inventory");
                }

                stmt1 = transconn.prepareStatement(updateInventory);
                stmt1.setFloat(1, actual);
                stmt1.setInt(2, product);
                stmt1.setInt(3, location);
                stmt1.executeUpdate();

                String logMessage = "Updating inventory fields for bottle products" + location + " (" + invId + ")";
                logger.portalDetail(callerId, "updateBottleInventory", location, "inventory", invId, logMessage, transconn);

            }

        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
            close(rs);
        }

    }
    
    
    /**  Updates the fullname and/or username and/or email of a user.
     *  The caller id and caller customer must be passed, and is subject to the following permission rules
     *  1. A user may update his own information
     *  2. A customer-admin may update any of his users
     *  3. A super-admin may update anyone
     */
    private void updateTestUser(Element toHandle, Element toAppend) throws HandlerException {
        int targetUser = HandlerUtils.getRequiredInteger(toHandle, "userId");
        int customerId = HandlerUtils.getRequiredInteger(toHandle, "customer");
        int locationId = HandlerUtils.getRequiredInteger(toHandle, "locationId");
        WebPermission access = new WebPermission(HandlerUtils.getRequiredInteger(toHandle, "access"));
        int callerId = getCallerId(toHandle);
        String fullnameToTry = HandlerUtils.getOptionalString(toHandle, "fullname");
        String usernameToTry = HandlerUtils.getOptionalString(toHandle, "username");
        String emailToTry = HandlerUtils.getOptionalString(toHandle, "email");
        String mobileToTry = HandlerUtils.getOptionalString(toHandle, "mobile");
        String carrierToTry = HandlerUtils.getOptionalString(toHandle, "carrier");
        String select = "SELECT customer,name,username,email,mobile,carrier FROM user WHERE id=?";
        String updateUser = "UPDATE user SET username=?,name=?,email=?,mobile=?,carrier=? WHERE id=?";
        String checkUsername = "SELECT id FROM user WHERE username=? LIMIT 1";
        String checkTextAlerts = "SELECT id from textAlerts where user=? LIMIT 1";
        String updateTextAlerts = " UPDATE textAlerts SET mobile = ?,carrier = ? WHERE user = ?";

        PreparedStatement stmt = null;
        ResultSet rs = null;

        try {
            stmt = transconn.prepareStatement(select);
            stmt.setInt(1, targetUser);
            rs = stmt.executeQuery();
            if (rs.next()) {
                int userCustomer = rs.getInt(1);
                String fullname = rs.getString(2);
                String username = rs.getString(3);
                String email = rs.getString(4);
                String mobile = rs.getString(5);
                String carrier = rs.getString(6);
                String fullnameToUse = fullname;
                String usernameToUse = username;
                String emailToUse = email;

                String mobileToUse = mobile;

                if (usernameToTry != null && usernameToTry.length() > 3 && !usernameToTry.equals(username)) {
                    stmt = transconn.prepareStatement(checkUsername);
                    stmt.setString(1, usernameToTry);
                    rs = stmt.executeQuery();
                    if (!rs.next()) {
                        usernameToUse = usernameToTry;
                    } else {
                        addErrorDetail(toAppend, "The username '" + usernameToTry + "' already exists");
                    }
                }
                if (fullnameToTry != null && fullnameToTry.length() > 3) {
                    fullnameToUse = fullnameToTry;
                }
                if (emailToTry != null && emailToTry.length() > 3) {
                    emailToUse = emailToTry;
                }
                if (mobileToTry != null) {
                    mobileToUse = mobileToTry;
                }

                String carrierToUse = carrierToTry;
                String logMessage = "Updating " + username + " (" + fullname + ") to " + usernameToUse + " (" + fullnameToUse + ")";
                //check permission to delete.
                if ((access.canSuperAdmin()) || ((userCustomer == customerId) && (access.canWrite()))) {
                    logger.portalDetail(callerId, "updateTestUser", 0, "user", targetUser, logMessage, transconn);
                    stmt = transconn.prepareStatement(updateUser);
                    stmt.setString(1, usernameToUse);
                    stmt.setString(2, fullnameToUse);
                    stmt.setString(3, emailToUse);
                    stmt.setString(4, mobileToUse);
                    stmt.setString(5, carrierToUse);
                    stmt.setInt(6, targetUser);
                    stmt.executeUpdate();

                    stmt = transconn.prepareStatement(checkTextAlerts);
                    stmt.setInt(1, targetUser);
                    rs = stmt.executeQuery();
                    if (rs.last()) {
                        //user = ?,location = ?,customer = ?,mobile = ?,carrier = ? WHERE user = ? and location = ?";
                        logger.portalDetail(callerId, "updateTestUser", 0, "user", targetUser, logMessage, transconn);
                        stmt = transconn.prepareStatement(updateTextAlerts);
                        stmt.setString(1, mobileToUse);
                        stmt.setString(2, carrierToUse);
                        stmt.setInt(3, targetUser);
                        stmt.executeUpdate();
                    }
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



     /**
     * Handler that sets the brix ratio for all soda line thought out the day
     */
    public void setBottleInv(Element toHandle, Element toAppend) throws HandlerException {

        String sqlBottlePlu = "SELECT plu FROM beverage WHERE location = 5 and pType =3";

        String selectSoldQuantity = "SELECT COUNT(*) FROM sales s WHERE s.pluNumber=? and s.date>? and s.date<? and s.location=5";

        String selectInvQuantity = "SELECT iv.id, IF(iv.qtyOnHand < 0, 0.0, iv.qtyOnHand) FROM ingredient i left join inventory iv on iv.product = i.product left join beverage b on b.id = i.beverage where iv.location = 5 and b.pType = 3 and b.location =5 and b.plu=? order by b.name";

        String sqlUpdateInv = "UPDATE inventory SET qtyOnHand=? WHERE id=?";

        String selectLastSalesTime = " SELECT date FROM sales WHERE location = 5 ORDER BY sid DESC LIMIT 1";

        PreparedStatement stmt = null;
        PreparedStatement stmt1 = null;
        ResultSet rs = null;
        ResultSet rs1 = null;
        ResultSet rs2 = null;
        String lastDate;
        int plu, inventory, timeDiff;
        double quantity, currInv, newInv, packSize;
        packSize = 24.00;
        Calendar lastSales = Calendar.getInstance();
        DecimalFormat decFormat = new DecimalFormat("00");
        String month, day, year, hour, minute, second, time1, time2, buff;
        buff = "";
        try {
            stmt = transconn.prepareStatement(selectLastSalesTime);
            rs = stmt.executeQuery();
            if (rs.next()) {
                buff = rs.getString(1);
            }
            lastSales.set(Calendar.YEAR, Integer.parseInt(buff.substring(0, 4)));
            lastSales.set(Calendar.MONTH, Integer.parseInt(buff.substring(5, 7)) - 1);
            lastSales.set(Calendar.DAY_OF_MONTH, Integer.parseInt(buff.substring(8, 10)));
            lastSales.set(Calendar.HOUR_OF_DAY, Integer.parseInt(buff.substring(11, 13)));
            lastSales.set(Calendar.MINUTE, Integer.parseInt(buff.substring(14, 16)));
            lastSales.set(Calendar.SECOND, Integer.parseInt(buff.substring(17)));

            month = decFormat.format((lastSales.get(Calendar.MONTH) + 1));
            day = decFormat.format(lastSales.get(Calendar.DAY_OF_MONTH));
            year = String.valueOf(lastSales.get(Calendar.YEAR));
            hour = decFormat.format(lastSales.get(Calendar.HOUR_OF_DAY));
            minute = decFormat.format(lastSales.get(Calendar.MINUTE));
            second = decFormat.format(lastSales.get(Calendar.SECOND));

            time1 = year + "-" + month + "-" + day + " " + hour + ":" + minute + ":" + second;

            lastSales.add(Calendar.MINUTE, -1);

            month = decFormat.format((lastSales.get(Calendar.MONTH) + 1));
            day = decFormat.format(lastSales.get(Calendar.DAY_OF_MONTH));
            year = String.valueOf(lastSales.get(Calendar.YEAR));
            hour = decFormat.format(lastSales.get(Calendar.HOUR_OF_DAY));
            minute = decFormat.format(lastSales.get(Calendar.MINUTE));
            second = decFormat.format(lastSales.get(Calendar.SECOND));

            time2 = year + "-" + month + "-" + day + " " + hour + ":" + minute + ":" + second;


            stmt = transconn.prepareStatement(sqlBottlePlu);
            rs = stmt.executeQuery();
            while (rs.next()) {
                stmt1 = transconn.prepareStatement(selectSoldQuantity);
                stmt1.setInt(1, rs.getInt(1));
                stmt1.setString(2, time2);
                stmt1.setString(3, time1);
                rs1 = stmt1.executeQuery();
                if (rs1.next()) {
                    quantity = rs1.getInt(1);
                    quantity = quantity / 24.00;
                    stmt1 = transconn.prepareStatement(selectInvQuantity);
                    stmt1.setInt(1, rs.getInt(1));
                    rs2 = stmt1.executeQuery();

                    if (rs2.next()) {
                        inventory = rs2.getInt(1);
                        currInv = rs2.getDouble(2);
                        newInv = currInv - quantity;

                        stmt1 = transconn.prepareStatement(sqlUpdateInv);
                        stmt1.setDouble(1, newInv);
                        stmt1.setInt(2, inventory);
                        stmt1.executeUpdate();

                    }

                }

            }


        } catch (SQLException sqle) {
            logger.dbError("Error in setBrixRatio " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
            close(rs);
            close(stmt1);
            close(rs1);
            close(rs2);
        }

    }

    /**
     * Handler that sets the brix ratio for all soda line thought out the day
     */
    public void setBrixRatio(Element toHandle, Element toAppend) throws HandlerException {

        String sqlLastPouredDate =
                " SELECT r.date" +
                " FROM location lo LEFT JOIN bar b ON lo.id=b.location " +
                " LEFT JOIN line ln ON b.id=ln.bar " +
                " LEFT JOIN reading r ON r.line=ln.id " +
                " LEFT JOIN product p ON ln.product=p.id " +
                " WHERE lo.id=? AND p.pType=2 AND r.type = 0 ORDER BY r.date desc limit 1";
        String sqlLatestValue =
                " SELECT r.line,r.value,ln.product " +
                " FROM location lo LEFT JOIN bar b ON lo.id=b.location " +
                " LEFT JOIN line ln ON b.id=ln.bar " +
                " LEFT JOIN reading r ON r.line=ln.id " +
                " LEFT JOIN product p ON ln.product=p.id " +
                " WHERE lo.id=? AND ln.id in (2067, 2069, 2070, 2138, 2068, 2076) AND p.pType=2 AND r.date=? AND r.type = 0";
        String sqlLastValue =
                " SELECT r.line,r.value " +
                " FROM location lo LEFT JOIN bar b ON lo.id=b.location " +
                " LEFT JOIN line ln ON b.id=ln.bar " +
                " LEFT JOIN reading r ON r.line=ln.id " +
                " LEFT JOIN product p ON ln.product=p.id " +
                " WHERE lo.id=? AND p.pType=2 AND r.line=? AND r.date<? AND r.type = 0 order by r.date desc limit 1";

        String sqlCheckIfWater =
                " SELECT brixWater, brixSyrup FROM inventory where location = ? AND product = ?";

        String sqlCheckDate =
                " SELECT * FROM brixRatio where date = ?";

        String sqlStoreRatio =
                " INSERT INTO brixRatio (location,line,product,ratio,date) VALUES (?,?,?,?,?)";

        PreparedStatement stmt = null;
        PreparedStatement stmt1 = null;
        ResultSet rs = null;
        ResultSet rs1 = null;
        ResultSet rs2 = null;
        String lastDate;
        int line1, line2, product1, product2, location;
        boolean isWater, isSyrup;
        double ounces1, ounces2, ouncesDiff, totalWater, totalSyrup, ratio;
        totalWater = 0;
        totalSyrup = 0;
        lastDate = " ";
        ouncesDiff = 0;
        location = 5;
        try {
            stmt = transconn.prepareStatement(sqlLastPouredDate);
            stmt.setInt(1, location);
            rs = stmt.executeQuery();
            if (rs.next()) {
                lastDate = rs.getString(1);
            }

            stmt = transconn.prepareStatement(sqlLatestValue);
            stmt.setInt(1, location);
            stmt.setString(2, lastDate);
            rs = stmt.executeQuery();

            while (rs.next()) {

                line1 = rs.getInt(1);
                ounces1 = rs.getDouble(2);
                product1 = rs.getInt(3);

                stmt1 = transconn.prepareStatement(sqlLastValue);
                stmt1.setInt(1, location);
                stmt1.setInt(2, line1);
                stmt1.setString(3, lastDate);
                rs1 = stmt1.executeQuery();
                if (rs1.next()) {
                    line2 = rs1.getInt(1);
                    ounces2 = rs1.getDouble(2);
                    ouncesDiff = ounces1 - ounces2;
                }
                if (ouncesDiff > 0) {
                    stmt = transconn.prepareStatement(sqlCheckIfWater);
                    stmt.setInt(1, location);
                    stmt.setInt(2, product1);
                    rs2 = stmt.executeQuery();

                    if (rs2.next()) {

                        isWater = rs2.getBoolean(1);
                        isSyrup = rs2.getBoolean(2);
                        if (isSyrup) {
                            totalSyrup = totalSyrup + ouncesDiff;
                        }
                        if (isWater) {
                            totalWater = totalWater + ouncesDiff;
                        }

                    }

                }

            }
            ratio = totalWater / totalSyrup;

            if (ratio < 8.50 && ratio > 3.50) {

                if (totalSyrup != 0.0 && totalWater != 0.0) {

                    stmt = transconn.prepareStatement(sqlLatestValue);
                    stmt.setInt(1, location);
                    stmt.setString(2, lastDate);
                    rs = stmt.executeQuery();
                    while (rs.next()) {
                        line1 = rs.getInt(1);
                        product1 = rs.getInt(3);
                        if (product1 != 492) {

                            stmt1 = transconn.prepareStatement(sqlCheckDate);
                            stmt1.setString(1, lastDate);
                            rs2 = stmt1.executeQuery();
                            if (!rs2.next()) {
                                stmt = transconn.prepareStatement(sqlStoreRatio);
                                stmt.setInt(1, location);
                                stmt.setInt(2, line1);
                                stmt.setInt(3, product1);
                                stmt.setDouble(4, ratio);
                                stmt.setString(5, lastDate);
                                rs1 = stmt.executeQuery();

                            }
                        }
                    }

                }

            }

        } catch (SQLException sqle) {
            logger.dbError("Error in setBrixRatio " + sqle.getMessage());
            throw new HandlerException(sqle);
        }

    }
    
    
    private void updateUnitSettings(Element toHandle, Element toAppend) throws HandlerException {

        int userId = HandlerUtils.getRequiredInteger(toHandle, "userId");
        int unitId = HandlerUtils.getRequiredInteger(toHandle, "unitId");

        String update = "UPDATE user SET unit=? WHERE id=?";

        PreparedStatement stmt = null;

        try {
            stmt = transconn.prepareStatement(update);
            stmt.setInt(1, unitId);
            stmt.setInt(2, userId);
            stmt.executeUpdate();

        } catch (SQLException sqle) {
            logger.dbError("Database error in updateCustomer: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
        }
    }
    
    
     /**
     * The folowing code is to update or insert the SMS alert requirements for each user for the location
     * that they provide - AD
     */
    private void updateSMSAlerts(Element toHandle, Element toAppend) throws HandlerException {

        int callerId = getCallerId(toHandle);

        String update = " UPDATE textAlerts SET alert2=?" +
                " WHERE user = ? and location = ? and type = ?";


        PreparedStatement stmt = null;
        ResultSet rs = null;

        logger.portalAction("updateSMSAlerts");

        try {

            int locationId = HandlerUtils.getRequiredInteger(toHandle, "locationId");
            int userId = HandlerUtils.getRequiredInteger(toHandle, "userId");
            Boolean smsalert2 = HandlerUtils.getRequiredBoolean(toHandle, "smsalert2");
            int alertType = HandlerUtils.getRequiredInteger(toHandle, "alertType");

            stmt = transconn.prepareStatement(update);
            stmt.setBoolean(1, smsalert2);
            stmt.setInt(2, userId);
            stmt.setInt(3, locationId);
            stmt.setInt(4, alertType);
            stmt.executeUpdate();

            // Log the action test
            stmt = transconn.prepareStatement("SELECT LAST_INSERT_ID()");
            rs = stmt.executeQuery();
            if (rs.next()) {
                int newId = rs.getInt(1);
                String logMessage = "Updated SMS Alert Requirements";
                logger.portalDetail(callerId, "addInventory", locationId, "SMS", newId, logMessage, transconn);

            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
            close(rs);
        }


    }
    
    
    private void deleteBeverageSize1(Element toHandle, Element toAppend) throws HandlerException {
        PreparedStatement stmt = null;
        ResultSet rs = null;

        int location = HandlerUtils.getRequiredInteger(toHandle, "locationId");
        String name = HandlerUtils.getRequiredString(toHandle, "name");
        int prodType = HandlerUtils.getRequiredInteger(toHandle, "prodID");
        int callerId = getCallerId(toHandle);

        String getSize = "SELECT id FROM beverageSize WHERE location=? AND name=? AND pType=?";
        String deleteSize = "DELETE FROM beverageSize WHERE id=?";

        try {
            stmt = transconn.prepareStatement(getSize);
            stmt.setInt(1, location);
            stmt.setString(2, name);
            stmt.setInt(3, prodType);
            rs = stmt.executeQuery();
            if (rs.next()) {
                int toKill = rs.getInt(1);
                stmt = transconn.prepareStatement(deleteSize);
                stmt.setInt(1, toKill);
                stmt.executeUpdate();

                String logMessage = "Deleted beverage size '" + name + "'";
                logger.portalDetail(callerId, "deleteBeverageSize", location, "beverageSize", toKill, logMessage, transconn);                
            } else {
                addErrorDetail(toAppend, "The beverage size '" + name + "' does not exist");
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error in deleteBeverageSize1: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
            close(rs);
        }
    }
    
    
     /**  Update a product in the master product list along with the product type value.
     *  Will also mark as "added" any pending requests to add a product with this name.
     *
     *  <productName>"String"
     *  opt:<quicksell>000
     *---------SR
     */
    private void updateProduct1(Element toHandle, Element toAppend) throws HandlerException {

        String name                         = HandlerUtils.getRequiredString(toHandle, "productName");
        int qid                             = HandlerUtils.getOptionalInteger(toHandle, "quicksell");
        int id                              = HandlerUtils.getRequiredInteger(toHandle, "productId");
        int prodType                        = HandlerUtils.getRequiredInteger(toHandle, "prodType");
        int category                        = HandlerUtils.getRequiredInteger(toHandle, "category");
        //NischaySharma_02-Mar-2010_Start
        int segment                         = HandlerUtils.getOptionalInteger(toHandle, "segment");
        String boardName                    = HandlerUtils.nullToEmpty(HandlerUtils.getOptionalString(toHandle, "boardName"));
        String abv                          = HandlerUtils.getRequiredString(toHandle, "abv");
        String origin                       = HandlerUtils.getOptionalString(toHandle, "origin");
        String seasonality                  = HandlerUtils.getOptionalString(toHandle, "seasonality");
        int bbtvCategory                    = HandlerUtils.getRequiredInteger(toHandle, "bbtvCategory");
        String approvedString               = HandlerUtils.getOptionalString(toHandle, "isApproved");
        String description                  = HandlerUtils.getOptionalString(toHandle, "description");
        String bA                           = HandlerUtils.getOptionalString(toHandle, "beerAdvocate");
        String bDB                          = HandlerUtils.getOptionalString(toHandle, "breweryDB");
        String rateBeer                     = HandlerUtils.getOptionalString(toHandle, "rateBeer");
        int ibu                             = HandlerUtils.getOptionalInteger(toHandle, "ibu");
        int approved                        = HandlerUtils.getOptionalBoolean(toHandle, "isApproved") ?  1 : 0;
        int calorie                         = HandlerUtils.getOptionalInteger(toHandle, "calories");
        
        int oldApprovedStatus               = 0;
        if (approvedString.length() < 1) {
            approved                        = 1;
        }
        
        int isActive                        = HandlerUtils.getOptionalBoolean(toHandle, "isInactive") ? 0 : 1;

        int callerId                        = getCallerId(toHandle);

        PreparedStatement stmt              = null;
        ResultSet rs = null;

        if (qid < 0) {
            qid = 0;
        }
        
        String selectApprovalStatus         = "SELECT p.approved, p.name, pD.origin, pD.abv, pD.seasonality FROM product p " +
                                            " LEFT JOIN productDescription pD ON pD.product = p.id WHERE p.id = ?;";
        String selectOldInformation         = "SELECT pS.name FROM product p LEFT JOIN productSetMap pSM ON pSM.product = p.id " +
                                            " LEFT JOIN productSet pS ON pS.id = pSM.productSet WHERE pS.productSetType = ? AND p.id = ?;";
        String insertProductApproval        = "INSERT INTO productApproval (id, name, brewery, style, abv, origin, seasonality, dateApproved, status) " +
                                            " VALUES(?, ?, ?, ?, ?, ?, ?, NOW(), 0) ON DUPLICATE KEY UPDATE name = ?, brewery = ?, style = ?, abv = ?, origin = ?, seasonality = ?;";
        String update                       = "UPDATE product SET  name=?, pType=?, category=?, segment=?, isActive=?, approved = ?, cleanup =? WHERE id=?";
        String fullUpdate                   = "UPDATE product SET name=?, qid=?, pType=?, category=?, segment=?, isActive=?, approved = ?,cleanup =?  WHERE id=?";
        String updateDescription            = "UPDATE productDescription SET  boardName=?, abv=?, category=?, origin=?, seasonality=?, beerAdvocate = ?, breweryDB = ?, rateBeer=? WHERE product=?";
        String selectProductSet             = "SELECT pS.productSetType,pS.name,pSM.productSet from productSetMap pSM LEFT JOIN productSet pS ON pS.id=pSM.productSet Where pSM.product=? AND pS.productSetType IN (7, 9)";
        String updateIBU                    = "UPDATE productDescription SET  ibu=?  WHERE product=?";
        String updateCalorie                = "UPDATE productDescription SET  calorie=?  WHERE product=?";
        String selectProductDesc            = "SELECT id FROM productDesc WHERE product= ?";
        String insertProductDesc            = "INSERT INTO productDesc(product,description) VALUES(?, ?)";
        String updateProductDesc            = "UPDATE productDesc SET description = ? WHERE product= ?";
        
        try {
            String oldBreweryName           = "", oldStyleName = "", oldName = "", oldOrigin = "", oldABV = "", oldSeasonality = "";
            
            stmt                            = transconn.prepareStatement("SELECT id FROM productDescription WHERE product=?");
            stmt.setInt(1, id);
            rs                              = stmt.executeQuery();
            if (!rs.next()) {
                stmt                    = transconn.prepareStatement("INSERT INTO  productDescription(product,boardName,abv) VALUES(?,'',0)");
                stmt.setInt(1, id);                    
                stmt.executeUpdate();
                
            }
            stmt                            = transconn.prepareStatement(selectApprovalStatus);
            stmt.setInt(1, id);
            rs                              = stmt.executeQuery();
            if (rs.next()) {
                oldApprovedStatus           = rs.getInt(1);
                oldName                     = rs.getString(2);
                oldOrigin                   = rs.getString(3);
                oldABV                      = rs.getString(3);
                oldSeasonality              = rs.getString(4);
            }
            
            stmt                            = transconn.prepareStatement(selectOldInformation);
            stmt.setInt(1, 7);
            stmt.setInt(2, id);
            rs                              = stmt.executeQuery();
            if (rs.next()) {
                oldBreweryName              = rs.getString(1);
            }

            stmt                            = transconn.prepareStatement(selectOldInformation);
            stmt.setInt(1, 9);
            stmt.setInt(2, id);
            rs                              = stmt.executeQuery();
            if (rs.next()) {
                oldStyleName                = rs.getString(1);
            }

            
            String logMessage = "Changing product name to " + name + " for id" + id;
            logger.portalDetail(callerId, "updateProduct", 0, "product", id, logMessage, transconn);
            //String select = "SELECT name FROM Location WHERE id=?";

            if (prodType < 0) {
                prodType                    = 1;
            }
            int cleanup                     =0;
            if(approved == 1){
                cleanup                     = 3;              
            }
            
            if (qid == 0) {
                stmt = transconn.prepareStatement(update);
                stmt.setString(1, name);
                stmt.setInt(2, prodType);
                stmt.setInt(3, category);
                stmt.setInt(4, segment);
                stmt.setInt(5, isActive);
                stmt.setInt(6, approved);
                stmt.setInt(7, cleanup);
                stmt.setInt(8, id);
                stmt.executeUpdate();
            } else {
                stmt = transconn.prepareStatement(fullUpdate);
                stmt.setString(1, name);
                stmt.setInt(2, qid);
                stmt.setInt(3, prodType);
                stmt.setInt(4, category);
                stmt.setInt(5, segment);
                stmt.setInt(6, isActive);
                stmt.setInt(7, approved);                
                stmt.setInt(8, cleanup);                
                stmt.setInt(9, id);
                stmt.executeUpdate();
            }
            
            
            stmt = transconn.prepareStatement(updateDescription);
            stmt.setString(1, boardName);
            stmt.setString(2, abv);
            stmt.setInt(3, bbtvCategory);
            stmt.setString(4, origin);
            stmt.setString(5, seasonality);
            stmt.setString(6, HandlerUtils.nullToEmpty(bA));
            stmt.setString(7, HandlerUtils.nullToEmpty(bDB));
            stmt.setString(8, HandlerUtils.nullToEmpty(rateBeer));
            stmt.setInt(9, id);
            stmt.executeUpdate();
            
            if(ibu >0 && ibu<=100){
                stmt                        = transconn.prepareStatement(updateIBU);
                stmt.setInt(1, ibu);
                stmt.setInt(2, id);
                stmt.executeUpdate();
            }
            
            if(calorie > 0) {
                stmt                        = transconn.prepareStatement(updateCalorie);
                stmt.setInt(1, calorie);
                stmt.setInt(2, id);
                stmt.executeUpdate();
                
            }
            if(description!=null && !description.equals("")){
                stmt = transconn.prepareStatement(selectProductDesc);                
                stmt.setInt(1, id);
                rs = stmt.executeQuery();
                if(rs.next()) { 
                    stmt                        = transconn.prepareStatement(updateProductDesc);
                    stmt.setString(1, description);
                    stmt.setInt(2, id);
                    stmt.executeUpdate();
                } else {
                    stmt                        = transconn.prepareStatement(insertProductDesc);
                    stmt.setString(2, description);
                    stmt.setInt(1, id);
                    stmt.executeUpdate();

                }    
            }
            
            
            

            //Adding productSet information
            Iterator i = toHandle.elementIterator("productSetMap");
            addProductSetMap(id,i,toAppend,callerId);
            
            if(id>0) {
                stmt = transconn.prepareStatement(selectProductSet);                
                stmt.setInt(1, id);
                rs = stmt.executeQuery();
                while(rs.next()) {                   
                    if(rs.getInt(1) ==7){
                        toAppend.addElement("breweryId").addText(String.valueOf(rs.getInt(3)));
                        toAppend.addElement("brewery").addText(HandlerUtils.nullToEmpty(rs.getString(2)));
                    } if(rs.getInt(1) ==9){
                        toAppend.addElement("styleId").addText(String.valueOf(rs.getInt(3)));
                        toAppend.addElement("style").addText(HandlerUtils.nullToEmpty(rs.getString(2)));
                    }
                   
                    
                }
                
                if (oldApprovedStatus < 1 && approved == 1) {
                    stmt                    = transconn.prepareStatement(insertProductApproval);
                    stmt.setInt(1, id);
                    stmt.setString(2, oldName);
                    stmt.setString(3, oldBreweryName);
                    stmt.setString(4, oldStyleName);
                    stmt.setString(5, oldABV);
                    stmt.setString(6, oldOrigin);
                    stmt.setString(7, oldSeasonality);
                    stmt.setString(8, oldName);
                    stmt.setString(9, oldBreweryName);
                    stmt.setString(10, oldStyleName);
                    stmt.setString(11, oldABV);
                    stmt.setString(12, oldOrigin);
                    stmt.setString(13, oldSeasonality);
                    stmt.executeUpdate();
                    
                    sendProductApprovalEmail(id);
                }
                
                String insertLog        = "INSERT INTO productChangeLog (product,type,date,user) VALUES (?,2,now(),?)";
                stmt = transconn.prepareStatement(insertLog);
                stmt.setInt(1, id);                    
                stmt.setInt(2, callerId);   
                stmt.executeUpdate();
            }

        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
            close(rs);
        }

    }
    
    
     // Adding in productSet information for the product
    private void addProductSetMap(int product, Iterator i, Element toAppend, int callerId) throws HandlerException {
        String selectProductSet = " SELECT productSetType FROM productSet WHERE id = ? ";
        String selectProductSetMap = " SELECT pSM.id FROM productSetMap pSM LEFT JOIN productSet pS ON pS.id = pSM.productSet "
                                    + " WHERE pS.productSetType = ? AND pSM.product = ? ";
        String insertProductSetProductMap = " INSERT INTO productSetMap (productSet, product) VALUES (?,?) ";
        String updateProductSetProductMap = " UPDATE productSetMap SET productSet = ? WHERE id = ? ";
        
         String updateBrewMap               = " UPDATE brewStyleMap SET brewery = ? WHERE product = ? ";
        String updateStyleMap               = " UPDATE brewStyleMap SET style = ? WHERE product = ? ";

        PreparedStatement stmt = null;
        ResultSet rs = null;        

        try {
            while (i.hasNext()) {
                Element prod = (Element) i.next();
                int productSet = HandlerUtils.getRequiredInteger(prod, "productSet");
                int productSetType = HandlerUtils.getRequiredInteger(prod, "productSetType");

                if (productSet == 0) {
                    String productSetName = HandlerUtils.getRequiredString(prod, "productSetName");                    

                    String selectProductSetName = " SELECT id FROM productSet WHERE name = ? AND productSetType = ? ";
                    stmt = transconn.prepareStatement(selectProductSetName);
                    stmt.setString(1, productSetName);
                    stmt.setInt(2, productSetType);
                    rs = stmt.executeQuery();
                    if (!rs.next()) {
                        String insertNewProductSet = " INSERT INTO productSet (name, productSetType) VALUES (?,?) ";
                        stmt = transconn.prepareStatement(insertNewProductSet);
                        stmt.setString(1, productSetName);
                        stmt.setInt(2, productSetType);
                        stmt.executeUpdate();

                        stmt = transconn.prepareStatement("SELECT LAST_INSERT_ID()");
                        rs = stmt.executeQuery();
                        if (rs.next()) {
                            productSet = rs.getInt(1);
                        }
                        String insertLog        = "INSERT INTO productChangeLog (product,type,productType,date,user) VALUES (?,1,?,now(),?)";
                        stmt                    = transconn.prepareStatement(insertLog);
                        stmt.setInt(1, productSet);  
                        stmt.setInt(2, productSetType);  
                        stmt.setInt(3, callerId);  
                        stmt.executeUpdate();
                       
                    } else {
                        productSet = rs.getInt(1);
                    }
                    
                }
                
                stmt = transconn.prepareStatement("SELECT id FROM brewStyleMap WHERE product = ?; ");
                stmt.setInt(1, product);
                rs = stmt.executeQuery();
                if (!rs.next()) {
                   stmt                     = transconn.prepareStatement(" INSERT INTO brewStyleMap (product, brewery, style) VALUES (?,0,0)");
                   stmt.setInt(1, product);
                   stmt.executeUpdate(); 
                }
                
                 if(productSetType == 7) {
                    stmt                    = transconn.prepareStatement(updateBrewMap);
                    stmt.setInt(1, productSet);
                    stmt.setInt(2, product);
                    stmt.executeUpdate();
                } else   if(productSetType == 9) {
                    stmt                    = transconn.prepareStatement(updateStyleMap);
                    stmt.setInt(1, productSet);
                    stmt.setInt(2, product);
                    stmt.executeUpdate();
                }


                stmt = transconn.prepareStatement(selectProductSet);
                stmt.setInt(1, productSet);
                rs = stmt.executeQuery();
                if (rs.next()) {
                    stmt = transconn.prepareStatement(selectProductSetMap);
                    stmt.setInt(1, rs.getInt(1));
                    stmt.setInt(2, product);
                    rs = stmt.executeQuery();
                    if (rs.next()) {
                        stmt = transconn.prepareStatement(updateProductSetProductMap);
                        stmt.setInt(1, productSet);
                        stmt.setInt(2, rs.getInt(1));
                        stmt.executeUpdate();
                    } else {
                        stmt = transconn.prepareStatement(insertProductSetProductMap);
                        stmt.setInt(1, productSet);
                        stmt.setInt(2, product);
                        stmt.executeUpdate();
                    }
                }
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
            close(rs);
        }
    }
    
    
    private int  addProduct(String productName, int callerId, int location) throws HandlerException {
         
        
         PreparedStatement stmt             = null;
         ResultSet rs                       = null;
         int productId                      = 0;
         String sql                         = "SELECT id  FROM product WHERE ptype=1 AND isActive=1 AND name=?  LIMIT 1;";
         String insertProduct                = "INSERT INTO product (name, qId, pType, category, segment, isActive, approved) VALUES (?,0,1,2,10,1,0)";
        String insertLog                    = "INSERT INTO productChangeLog (product,type,date,user) VALUES (?,1,now(),?)";
        String insertProductDescription     = " INSERT INTO productDescription (product, boardName, abv, category, origin, seasonality,ibu,breweryDB) VALUES" +
                                            " (?,?,0.0,2,'','',0,'')";
        String insertBrewStyleMap           = " INSERT INTO brewStyleMap (product, brewery, style) VALUES (?,0,0)";
        String getLastId                    =" SELECT LAST_INSERT_ID()";
        try {
            
            
            stmt                            = transconn.prepareStatement(sql);
            stmt.setString(1,productName);
            rs                              = stmt.executeQuery();
            if(rs.next()) {
                productId                   = rs.getInt(1);
            }
            if(productName.contains("Un-Assigned")){
                productId                   = 4311;
            }
            if(productId ==0) {
                WordUtils w                =new WordUtils();
                productName                = w.capitalize(productName);
                stmt                       = transconn.prepareStatement(insertProduct);                    
                    stmt.setString(1,productName);
                    stmt.executeUpdate();
                    stmt                       = transconn.prepareStatement(getLastId);
                    rs                  = stmt.executeQuery();                   
                    if (rs.next()) { 
                        productId              = rs.getInt(1);

                    }
                    if(productId > 0){
                        String logMessage = "Added product " + productName;
                        logger.portalDetail(callerId, "addProduct", location, "product", productId, logMessage, transconn);
                        stmt                       = transconn.prepareStatement(insertProductDescription);      
                        stmt.setInt(1, productId);
                        stmt.setString(2,"");
                        stmt.executeUpdate();
                        
                        stmt                       = transconn.prepareStatement(insertBrewStyleMap);      
                        stmt.setInt(1, productId);                        
                        stmt.executeUpdate();
                        
                        stmt                       = transconn.prepareStatement(insertLog);      
                        stmt.setInt(1, productId);                        
                        stmt.setInt(2, callerId); 
                        stmt.executeUpdate();
                        
                        
                    }
            }
            logger.debug(productName+" : "+ productId + " Added");
        } catch (SQLException sqle) {
            logger.dbError("Database error: "+sqle.toString());
            throw new HandlerException(sqle);
        } finally {
            
            close(stmt);
            close(rs);
        }
        return productId;
    }
    
    
    /**
     * Inorder to set the beverage type category in the manager, this below handler is used to get all the possible
     * beverage type that we currently service.  SR
     */
    private void adminChangeProductType(Element toHandle, Element toAppend)
            throws HandlerException {

        String selectBeverage = "SELECT id, name FROM productType order by name";


        PreparedStatement stmt = null;
        ResultSet rs = null;

        try {
            stmt = transconn.prepareStatement(selectBeverage);
            rs = stmt.executeQuery();
            while (rs != null && rs.next()) {
                int rsIndex = 1;
                Element locEl = toAppend.addElement("prod");
                locEl.addElement("prodID").addText(HandlerUtils.nullToEmpty((rs.getString(rsIndex++))));
                locEl.addElement("prodName").addText(HandlerUtils.nullToEmpty((rs.getString(rsIndex++))));
            }

        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(rs);
            close(stmt);
        }

    }
    
    
    /** Update the name of an existing customer.  If there were more editable customer fields,
     *  this method would support them.
     *
     *  <customerId> 999        # id to change
     *  <customerName> String   # The new name
     */
    private void disableRewards(Element toHandle, Element toAppend) throws HandlerException {

        int userId = HandlerUtils.getRequiredInteger(toHandle, "userId");

        String update = "UPDATE user SET showRewards=1 WHERE id=?";

        PreparedStatement stmt = null;

        try {
            stmt = transconn.prepareStatement(update);
            stmt.setInt(1, userId);
            stmt.executeUpdate();


        } catch (SQLException sqle) {
            logger.dbError("Database error in updateCustomer: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
        }
    }


        private void updateTestBeveragePlu(Element toHandle, Element toAppend) throws HandlerException {

        PreparedStatement getStmt = null;
        PreparedStatement updateStmt = null;
        ResultSet rs = null;

        int location = HandlerUtils.getRequiredInteger(toHandle, "locationId");
        int callerId = getCallerId(toHandle);

        String getBeverage =
                " SELECT bev.id, bar.name FROM beverage bev LEFT JOIN bar ON bev.bar=bar.id " +
                " WHERE bev.location=? AND bev.plu=? LIMIT 1";
        String updateBeverage = "UPDATE beverage SET plu=? WHERE id=?";


        try {
            getStmt = transconn.prepareStatement(getBeverage);
            updateStmt = transconn.prepareStatement(updateBeverage);

            // we need to check for update conflicts
            // 1. first, ensure that there are no duplicates in the "new" update set
            // 2. second, any "new" plu must exist in the "old" update set or NOT exist in the database

            Iterator i = toHandle.elementIterator("beverage");
            Set<String> oldSet = new HashSet<String>();
            Set<String> newSet = new HashSet<String>();
            Set<String> offendingNew = new HashSet<String>();
            Map<String, Integer> pluToId = new HashMap<String, Integer>();

            while (i.hasNext()) {
                Element el = (Element) i.next();
                String newPlu = HandlerUtils.getRequiredString(el, "newPlu");
                String oldPlu = HandlerUtils.getRequiredString(el, "oldPlu");
                if (newPlu.length() < 1 || oldPlu.length() < 1) {
                    throw new HandlerException("Found an empty PLU code");
                }
                // check update conflict case #1:
                if (newSet.contains(newPlu)) {
                    offendingNew.add(newPlu);
                } else {
                    newSet.add(newPlu);
                    oldSet.add(oldPlu);
                    getStmt.setInt(1, location);
                    getStmt.setString(2, oldPlu);
                    rs = getStmt.executeQuery();
                    if (rs.next()) {
                        pluToId.put(oldPlu, new Integer(rs.getInt(1)));
                    } else {
                        throw new HandlerException("Could not find PLU: " + oldPlu);
                    }
                }
            }
            if (offendingNew.size() > 0) {
                Element er = toAppend.addElement("error");
                er.addElement("detail").addText("Found duplicate new PLUs");
                for (String s : offendingNew) {
                    er.addElement("plu").addText(s);
                }
            } else {
                logger.debug("Checkset sizes: old, " + oldSet.size() + "; new, " + newSet.size() + "; ids, " + pluToId.size());

                i = toHandle.elementIterator("beverage");
                while (i.hasNext()) {
                    Element el = (Element) i.next();
                    String oldPlu = HandlerUtils.getRequiredString(el, "oldPlu");
                    String newPlu = HandlerUtils.getRequiredString(el, "newPlu");
                    if (!oldPlu.equals(newPlu)) {

                        // check update conflict case #2
                        boolean okToAdd = false;
                        if (oldSet.contains(newPlu)) {
                            okToAdd = true;
                            logger.debug("New PLU " + newPlu + " exists in old set, OK to update");
                        } else {
                            getStmt.setInt(1, location);
                            getStmt.setString(2, newPlu);
                            rs = getStmt.executeQuery();
                            //This is a duplicate check, so we want no result
                            if (rs.next()) {
                                String barName = rs.getString(2);
                                String detailMessage = "The PLU '" + newPlu + "' already exists";
                                if (barName != null && barName.length() > 0) {
                                    detailMessage += " (" + barName + ")";
                                }
                                addErrorDetail(toAppend, detailMessage);
                                logger.debug("New PLU " + newPlu + " exists in the db, NOT updating.");
                            } else {
                                okToAdd = true;
                                logger.debug("New PLU " + newPlu + " doesnt exist in db, OK to update");
                            }
                        }
                        Integer updateInt = pluToId.get(oldPlu);
                        if (okToAdd && updateInt != null) {
                            int bevId = updateInt.intValue();
                            updateStmt.setString(1, newPlu);
                            updateStmt.setInt(2, bevId);
                            updateStmt.executeUpdate();
                            String logMessage = "Changed PLU from '" + oldPlu + "' to '" + newPlu + " at Location " + location;
                            logger.portalDetail(callerId, "updateBeverage", location, "beverage", bevId, logMessage, transconn);
                        } else {
                            logger.debug("Skipping " + newPlu);
                        }
                    }
                }
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(updateStmt);
            close(getStmt);
            close(rs);
        }

    }
        
        
        private void deleteTestBeveragePlu(Element toHandle, Element toAppend) throws HandlerException {

        PreparedStatement stmt = null;
        ResultSet rs = null;

        int location = HandlerUtils.getRequiredInteger(toHandle, "locationId");
        int callerId = getCallerId(toHandle);

        String selectBeverage = "SELECT id FROM beverage WHERE plu=? AND location=?";
        String deleteBeverage = "DELETE FROM beverage WHERE id=?";
        String deleteIngredient = "DELETE FROM ingredient WHERE beverage=?";
        try {
            Iterator bevs = toHandle.elementIterator("beverage");
            while (bevs.hasNext()) {
                Element bev = (Element) bevs.next();
                String plu = HandlerUtils.getRequiredString(bev, "plu");

                stmt = transconn.prepareStatement(selectBeverage);
                stmt.setString(1, plu);
                stmt.setInt(2, location);
                rs = stmt.executeQuery();

                if (rs.next()) {
                    stmt = transconn.prepareStatement(deleteBeverage);
                    stmt.setInt(1, rs.getInt(1));
                    stmt.executeUpdate();
                    logger.portalDetail(callerId, "deleteBeverage", location, "beverage", rs.getInt(1), "Deleted PLU " + plu, transconn);

                    stmt = transconn.prepareStatement(deleteIngredient);
                    stmt.setInt(1, rs.getInt(1));
                    stmt.executeUpdate();
                }
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error in deleteBeveragePlu: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
        }

    }

    /**
     * Store sales data directly for a specified location.
     * Data is passed as item elements, each containing
     *  epoch - the date epoch of the sale
     *  checkId - the data id
     *  id - the pos id
     *  qty - the quantity sold
     *
     *  <item>
     *      <epoch>
     *      <checkId>
     *      <id>
     *      <qty>
     *  </item>
     *  <item>...</item>
     *
     */
    private boolean manualInventoryUpload(Element toHandle, Element toAppend) throws HandlerException {
        int location = HandlerUtils.getRequiredInteger(toHandle, "locationId");
        boolean storeAll = HandlerUtils.getOptionalBoolean(toHandle, "storeAll");
        int stmtIndex = 1;
        boolean success = false;
        List itemList = toHandle.elements("item");
        if (null == itemList || itemList.size() == 0) {
            return true;
        }
        double qty;

        PreparedStatement stmt = null;
        ResultSet rs1 = null;
        ResultSet rs = null;
        int queryCount = 0;
        String id;
        String sqlIns = null;
        String sqlSel = null;
        boolean oldAutoCommit = true;
        boolean changedAutoCommit = false;

        Hashtable<String, Boolean> idHash = new Hashtable<String, Boolean>();

        try {
            Calendar calEpoch = Calendar.getInstance();
            oldAutoCommit = transconn.getAutoCommit();
            for (Object o : itemList) {
                Element item = (Element) o;
                id = HandlerUtils.getRequiredString(item, "id");

                if (storeAll) {
                    sqlSel = "SELECT name FROM beverage where pluNumber = ?";
                    stmt = transconn.prepareStatement(sqlSel);
                    stmt.setString(1, id);
                    rs1 = stmt.executeQuery();
                    queryCount++;
                    while (null != rs1 && rs1.next()) {
                        String prodName = HandlerUtils.nullToEmpty(rs1.getString(1));

                        // Check for the current value
                        sqlSel =
                                " SELECT id FROM product WHERE name = ? ";
                        // Insert the current value
                        sqlIns =
                                " UPDATE inventory set qtyOnHand=?, isActive = 1 WHERE location=? AND product=? and bottleSize=1";
                        changedAutoCommit = true;

                        String qtyStr;
                        boolean duplicate = false;
                        int duplicateCount = 0;
                        int insertedCount = 0;
                        if (storeAll || idHash.containsKey(id)) {
                            stmt = transconn.prepareStatement(sqlSel);
                            stmt.setString(1, prodName);
                            rs = stmt.executeQuery();
                            int prodID = new Integer(rs.getInt(1));
                            duplicate = (null != rs && rs.next());
                            close(rs);
                            if (duplicate) {
                                qtyStr = HandlerUtils.getRequiredString(item, "qty");
                                qty = Double.parseDouble(qtyStr);
                                stmtIndex = 1;
                                stmt = transconn.prepareStatement(sqlIns);
                                stmt.setDouble(stmtIndex++, qty);
                                stmt.setInt(stmtIndex++, location);
                                stmt.setInt(stmtIndex++, prodID);
                                stmt.executeUpdate();
                                queryCount++;
                                insertedCount++;

                            }
                        }
                    }

                    transconn.commit();
                }
            }

            transconn.commit();
            success = true;

        } catch (SQLException sqle) {
            if (null != transconn) {
                try {
                    logger.dbAction("manualInventoryUpload: database rollback");
                    transconn.rollback();
                } catch (SQLException ignore) {
                }
            }
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            if (null != transconn && changedAutoCommit) {
                try {
                    transconn.setAutoCommit(oldAutoCommit);
                } catch (SQLException ignore) {
                }
            }

            close(rs);
            close(stmt);
            close(transconn);
        }
        return success;
    }
    
    
    /** Returns a list of the locations for which a user has requested low-stock notifications
     *  Will also return that users email address, and if he has location-status updates enabled
     */
    private void disableReportRequest(Element toHandle, Element toAppend) throws HandlerException {
        int user = HandlerUtils.getOptionalInteger(toHandle, "userId");
        PreparedStatement stmt = null;

        try {
            String clearall = "UPDATE user SET emailReports=false WHERE id=?";
            stmt = transconn.prepareStatement(clearall);
            stmt.setInt(1, user);
            stmt.executeUpdate();
        } catch (SQLException sqle) {
            logger.dbError("Database error in authUser: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
        }

    }

    
    /**  Update all fields for one or more inventory records
     *
     */
    private void updateUnitCount(Element toHandle, Element toAppend) throws HandlerException {

        int callerId = getCallerId(toHandle);
        int location = HandlerUtils.getRequiredInteger(toHandle, "locationId");
        int zone = HandlerUtils.getOptionalInteger(toHandle, "zoneId");
        int bar = HandlerUtils.getOptionalInteger(toHandle, "barId");
        String eventTime = HandlerUtils.getOptionalString(toHandle, "eventTime");

        String selectStandUnitCount = "SELECT id FROM unitStandCount " +
                "WHERE location = ? and zone=? and bar=? and product=? " +
                "and date between (? - INTERVAL 1 HOUR) and (? + INTERVAL 1 HOUR)";

        String selectUnitCount = "SELECT id FROM unitCount " +
                "WHERE location = ? and zone=? and bar=? and station = ? and product=? " +
                "and date between (? - INTERVAL 1 HOUR) and (? + INTERVAL 1 HOUR)";

        String insertStandUnitCount = "INSERT INTO unitStandCount (location, zone, bar, product, count, date) " +
                "VALUES (?,?,?,?,?,?)";

        String updateStandUnitCount = "UPDATE unitStandCount SET count=? " +
                "WHERE location = ? and zone=? and bar=? and product=? " +
                "and date between (? - INTERVAL 1 HOUR) and (? + INTERVAL 1 HOUR)";

        String insertUnitCount = "INSERT INTO unitCount (location, zone, bar, station, product, count, date) " +
                "VALUES (?,?,?,?,?,?,?)";

        String updateUnitCount = "UPDATE unitCount SET count=? " +
                "WHERE location = ? and zone=? and bar=? and station = ? and product=? " +
                "and date between (? - INTERVAL 1 HOUR) and (? + INTERVAL 1 HOUR)";


        PreparedStatement stmt = null;
        ResultSet rs = null;

        Iterator i = toHandle.elementIterator("standUnitCount");
        Iterator j = toHandle.elementIterator("unitCount");

        try {
            while (i.hasNext()) {
                Element scount = (Element) i.next();

                int productId = HandlerUtils.getRequiredInteger(scount, "productID");
                float standUnitCount = HandlerUtils.getRequiredFloat(scount, "count");

                stmt = transconn.prepareStatement(selectStandUnitCount);
                stmt.setInt(1, location);
                stmt.setInt(2, zone);
                stmt.setInt(3, bar);
                stmt.setInt(4, productId);
                stmt.setString(5, eventTime);
                stmt.setString(6, eventTime);
                rs = stmt.executeQuery();
                if (rs.next()) {

                    stmt = transconn.prepareStatement(updateStandUnitCount);
                    stmt.setFloat(1, standUnitCount);
                    stmt.setInt(2, location);
                    stmt.setInt(3, zone);
                    stmt.setInt(4, bar);
                    stmt.setInt(5, productId);
                    stmt.setString(6, eventTime);
                    stmt.setString(7, eventTime);
                    stmt.executeUpdate();

                    String logMessage = "Updating stand unit counts for location: " + location + " zone: " + zone + " stand:" + bar + " on " + eventTime;
                    logger.portalDetail(callerId, "updateUnitCount", location, "zone", zone, logMessage, transconn);


                } else {

                    stmt = transconn.prepareStatement(insertStandUnitCount);
                    stmt.setInt(1, location);
                    stmt.setInt(2, zone);
                    stmt.setInt(3, bar);
                    stmt.setInt(4, productId);
                    stmt.setFloat(5, standUnitCount);
                    stmt.setString(6, eventTime);
                    stmt.executeUpdate();

                    String logMessage = "Inserting stand unit counts for location: " + location + " zone: " + zone + " stand:" + bar + " on " + eventTime;
                    logger.portalDetail(callerId, "updateUnitCount", location, "zone", zone, logMessage, transconn);

                }

            }

            while (j.hasNext()) {
                Element count = (Element) j.next();

                int station = HandlerUtils.getRequiredInteger(count, "stationID");
                int productId = HandlerUtils.getRequiredInteger(count, "productID");
                float unitCount = HandlerUtils.getRequiredFloat(count, "count");

                stmt = transconn.prepareStatement(selectUnitCount);
                stmt.setInt(1, location);
                stmt.setInt(2, zone);
                stmt.setInt(3, bar);
                stmt.setInt(4, station);
                stmt.setInt(5, productId);
                stmt.setString(6, eventTime);
                stmt.setString(7, eventTime);
                rs = stmt.executeQuery();
                if (rs.next()) {

                    stmt = transconn.prepareStatement(updateUnitCount);
                    stmt.setFloat(1, unitCount);
                    stmt.setInt(2, location);
                    stmt.setInt(3, zone);
                    stmt.setInt(4, bar);
                    stmt.setInt(5, station);
                    stmt.setInt(6, productId);
                    stmt.setString(7, eventTime);
                    stmt.setString(8, eventTime);
                    stmt.executeUpdate();

                    String logMessage = "Updating unit counts for location: " + location + " zone: " + zone + " stand: " + bar + " station: " + station + " on " + eventTime;
                    logger.portalDetail(callerId, "updateUnitCount", location, "zone", zone, logMessage, transconn);


                } else {

                    stmt = transconn.prepareStatement(insertUnitCount);
                    stmt.setInt(1, location);
                    stmt.setInt(2, zone);
                    stmt.setInt(3, bar);
                    stmt.setInt(4, station);
                    stmt.setInt(5, productId);
                    stmt.setFloat(6, unitCount);
                    stmt.setString(7, eventTime);
                    stmt.executeUpdate();

                    String logMessage = "Inserting unit counts for location: " + location + " zone: " + zone + " stand: " + bar + " station: " + station + " on " + eventTime;
                    logger.portalDetail(callerId, "updateUnitCount", location, "zone", zone, logMessage, transconn);

                }

            }

        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
            close(rs);
        }

    }
    
    
    private void readingDateFixer(Element toHandle, Element toAppend) throws HandlerException {

        String selectLineId                 = " SELECT id FROM reading WHERE line = ? AND id BETWEEN ? AND ?";
        String selectDate                   = " SELECT date FROM reading WHERE id = ? ";
        String updateNext                   = " UPDATE reading SET date = ? WHERE id = ?";


        int lineId                          = HandlerUtils.getRequiredInteger(toHandle, "lineId");
        int startId                         = HandlerUtils.getRequiredInteger(toHandle, "startId");
        int endId                           = HandlerUtils.getRequiredInteger(toHandle, "endId");

        PreparedStatement stmt              = null;

        ResultSet rs                        = null, innerRS = null;

        try {
            stmt                            = transconn.prepareStatement(selectLineId);
            stmt.setInt(1, lineId);
            stmt.setInt(2, startId);
            stmt.setInt(3, endId);
            rs                              = stmt.executeQuery();
            while (rs.next()) {
                int readingId               = rs.getInt(1);
                stmt                        = transconn.prepareStatement(selectDate);
                stmt.setInt(1, readingId + 10);
                innerRS                     = stmt.executeQuery();
                if (innerRS.next()) {
                    stmt                    = transconn.prepareStatement(updateNext);
                    stmt.setTimestamp(1, innerRS.getTimestamp(1));
                    stmt.setInt(2, readingId);
                    stmt.executeUpdate();
                }
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error in readingDateFixer: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(rs);
            close(stmt);
        }

    }

    private void queryRunner(Element toHandle, Element toAppend) throws HandlerException {

        String selectBeverage               = " SELECT id FROM beverage WHERE ounces = 0";
        String selectSize                   = " SELECT SUM(ounces) FROM ingredient WHERE beverage = ? GROUP BY beverage";
        String updateBeverage               = " UPDATE beverage SET ounces = ? WHERE id = ?";

        PreparedStatement stmt              = null;
        ResultSet rs                        = null, innerRS = null;

        try {
            stmt                            = transconn.prepareStatement(selectBeverage);
            rs                              = stmt.executeQuery();
            while (rs.next()) {
                stmt                        = transconn.prepareStatement(selectSize);
                stmt.setInt(1, rs.getInt(1));
                innerRS                     = stmt.executeQuery();
                if (innerRS.next()) {
                    stmt                    = transconn.prepareStatement(updateBeverage);
                    stmt.setDouble(1, innerRS.getDouble(1));
                    stmt.setInt(2, rs.getInt(1));
                    stmt.executeUpdate();
                }
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error in queryRunner: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(innerRS);
            close(rs);
            close(stmt);
        }
    }
    
    
    /**
     * The folowing code is to update or insert the shift hour information for each location - SR
     */
    private void deleteLocationLogs(Element toHandle, Element toAppend) throws HandlerException {

        String deleteLogs = "DELETE FROM techLogs WHERE id=?";


        PreparedStatement stmt = null;
        ResultSet rs = null;

        try {
            Iterator logs = toHandle.elementIterator("deleteList");
            while (logs.hasNext()) {
                Element log = (Element) logs.next();
                int logId = HandlerUtils.getRequiredInteger(log, "logId");

                stmt = transconn.prepareStatement(deleteLogs);
                stmt.setInt(1, logId);
                stmt.executeUpdate();
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
            close(rs);
        }


    }
    
    
    /**
     * The folowing code is to update or insert the shift hour information for each location - SR
     */
    private void updateLocationLogs(Element toHandle, Element toAppend) throws HandlerException {

        int logId = HandlerUtils.getRequiredInteger(toHandle, "logId");
        int customerId = HandlerUtils.getRequiredInteger(toHandle, "customerId");
        int locationId = HandlerUtils.getRequiredInteger(toHandle, "locationId");
        int userId = HandlerUtils.getRequiredInteger(toHandle, "userId");
        String category = HandlerUtils.getRequiredString(toHandle, "category");
        String issue = HandlerUtils.getOptionalString(toHandle, "issue");
        String resolution = HandlerUtils.getOptionalString(toHandle, "resolution");
        String status = HandlerUtils.getRequiredString(toHandle, "status");
        String resolutionDate = HandlerUtils.getOptionalString(toHandle, "resolutionDate");
        int callerId = getCallerId(toHandle);

        String updateLogs = "UPDATE techLogs SET customer=?, location=?, user=?, category=?, issue=?, status=?, resolution=?,resolutionDate=? WHERE id=?";


        PreparedStatement stmt = null;
        ResultSet rs = null;

        try {
            if (issue.length() < 1) {
                addErrorDetail(toAppend, "Cannot log problem without describing the issue");
            } else {


                stmt = transconn.prepareStatement(updateLogs);
                stmt.setInt(1, customerId);
                stmt.setInt(2, locationId);
                stmt.setInt(3, userId);
                stmt.setString(4, category);
                stmt.setString(5, issue);
                stmt.setString(6, status);
                if (resolution.length() > 0 && resolutionDate.length() > 0) {

                    stmt.setString(7, resolution);
                    stmt.setString(8, resolutionDate);

                } else if (resolution.length() > 0) {
                    stmt.setString(7, resolution);
                    stmt.setString(8, "");

                } else if (resolutionDate.length() > 0) {
                    stmt.setString(7, "");
                    stmt.setString(8, resolutionDate);
                } else {
                    stmt.setString(7, "");
                    stmt.setString(8, "");

                }


                stmt.setInt(9, logId);
                stmt.executeUpdate();

            }


        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
            close(rs);
        }


    }
    
    
    /** Promote a user to Customer-Administrator.  This will wipe out his userMap
     *
     *  The caller must pass its customer id and permission level.  CustomerIds
     *  must the the caller must have P:3 or better permissions.
     */
    private void promoteITUser(Element toHandle, Element toAppend) throws HandlerException {

        int targetUser = HandlerUtils.getRequiredInteger(toHandle, "userId");
        int customerId = HandlerUtils.getRequiredInteger(toHandle, "customer");
        WebPermission access = new WebPermission(HandlerUtils.getRequiredInteger(toHandle, "access"));
        int callerId = getCallerId(toHandle);

        String select = "SELECT customer,name FROM user WHERE id =? ";
        String updateUser = "UPDATE user SET isITAdmin=1 WHERE id=?";
        String deleteMap = "DELETE FROM userMap WHERE user=?";

        PreparedStatement stmt = null;
        ResultSet rs = null;

        try {
            stmt = transconn.prepareStatement(select);
            stmt.setInt(1, targetUser);
            rs = stmt.executeQuery();
            if (rs.next()) {
                int userCustomer = rs.getInt(1);
                String fullname = rs.getString(2);

                String logMessage = "Promoting IT user " + targetUser + " (" + fullname + ")";
                //check permission to delete.
                if ((access.canSuperAdmin()) || ((userCustomer == customerId) && (access.canCustomerAdmin()))) {
                    logger.portalDetail(callerId, "promoteUser", 0, "IT user", targetUser, logMessage, transconn);

                    stmt = transconn.prepareStatement(updateUser);
                    stmt.setInt(1, targetUser);
                    stmt.executeUpdate();

                    stmt = transconn.prepareStatement(deleteMap);
                    stmt.setInt(1, targetUser);
                    stmt.executeUpdate();
                } else {
                    logger.portalAccessViolation("Promote IT user failed by U#" + callerId + " C#" + customerId + " uC#" + userCustomer + " P#" + access + ": " + logMessage);
                    addErrorDetail(toAppend, "Unable to promote that IT user.");
                }
            } else {
                logger.debug("Unknown IT user: " + targetUser);
                addErrorDetail(toAppend, "Unable to promote that IT user.");
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error in promoteUser " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(rs);
            close(stmt);
        }
    }

    /**  Demote a customer-admin to a map-level user with manager access at all locations
     *  The caller must pass its customer id and permission level.  CustomerIds
     *  must the the caller must have P:3 or better permissions.
     */
    private void demoteITAdmin(Element toHandle, Element toAppend) throws HandlerException {
        int targetUser = HandlerUtils.getRequiredInteger(toHandle, "userId");
        int customerId = HandlerUtils.getRequiredInteger(toHandle, "customer");
        WebPermission access = new WebPermission(HandlerUtils.getRequiredInteger(toHandle, "access"));
        int callerId = getCallerId(toHandle);

        String select = "SELECT customer,name,isITAdmin FROM user WHERE id=?";
        String updateUser = "UPDATE user SET isITAdmin=0 WHERE id=?";
        String insertMap = "INSERT INTO userMap (user,securityLevel,location) " +
                " SELECT ?,?,id FROM location WHERE customer=?";

        PreparedStatement stmt = null;
        ResultSet rs = null;

        try {
            stmt = transconn.prepareStatement(select);
            stmt.setInt(1, targetUser);
            rs = stmt.executeQuery();
            if (rs.next()) {
                int userCustomer = rs.getInt(1);
                String fullname = rs.getString(2);
                int isManager = rs.getInt(3);

                String logMessage = "Demoting IT Admin user " + targetUser + " (" + fullname + ")";
                //check permission to delete.
                if ((access.canSuperAdmin()) || ((userCustomer == customerId) && (access.canCustomerAdmin()))) {
                    logger.portalDetail(callerId, "demoteITAdmin", 0, "user", targetUser, logMessage, transconn);

                    //change to map-level access
                    stmt = transconn.prepareStatement(updateUser);
                    stmt.setInt(1, targetUser);
                    stmt.executeUpdate();

                    //add manager access for all locations
                    stmt = transconn.prepareStatement(insertMap);
                    stmt.setInt(1, targetUser);
                    stmt.setInt(2, WebPermission.instanceOfManager().getLevel());
                    stmt.setInt(3, userCustomer);
                    stmt.executeUpdate();
                } else {
                    logger.portalAccessViolation("Demote IT Adminn user failed by U#" + callerId + " C#" + customerId + " uC#" + userCustomer + " P#" + access + ": " + logMessage);
                    addErrorDetail(toAppend, "Unable to demote that IT user.");
                }
            } else {
                logger.debug("Unknown user: " + targetUser);
                addErrorDetail(toAppend, "Unable to demote that IT user.");
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error in demoteITAdmin " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(rs);
            close(stmt);
        }
    }
    
    
    private void updateCoolers(Element toHandle, Element toAppend) throws HandlerException {

        Iterator coolers                    = toHandle.elementIterator("cooler");
        PreparedStatement stmt              = null;
        ResultSet rs                        = null;
        String insertStmt                   = "INSERT INTO cooler (name, location, zone, system, line, alertPoint, offset) VALUES (?,?,?,?,?,?,?)";
        String updateStmt                   = "UPDATE cooler SET name = ?, location = ?, zone = ?, system = ?, line = ?, alertPoint = ?, offset = ? WHERE id = ?";

        try {
            while (coolers.hasNext()) {
                Element cooler              = (Element) coolers.next();
                int coolerId                = HandlerUtils.getOptionalInteger(cooler, "coolerId");
                String name                 = HandlerUtils.getRequiredString(cooler, "name");
                int locationId              = HandlerUtils.getRequiredInteger(cooler, "locationId");
                int zoneId                  = HandlerUtils.getOptionalInteger(cooler, "zoneId");
                int system                  = HandlerUtils.getRequiredInteger(cooler, "system");
                int line                    = HandlerUtils.getOptionalInteger(cooler, "line");
                int alertPoint              = HandlerUtils.getOptionalInteger(cooler, "alertPoint");
                double offset               = HandlerUtils.getOptionalDouble(cooler, "offset");

                if (coolerId > 0) {
                    if (!checkForeignKey("cooler", "id", coolerId)) {
                        throw new HandlerException("Foreign Key Not found : cooler " + coolerId);
                    }
                    stmt                    = transconn.prepareStatement(updateStmt);
                    stmt.setString(1, name);
                    stmt.setInt(2, locationId);
                    stmt.setInt(3, zoneId);
                    stmt.setInt(4, system);
                    stmt.setInt(5, line);
                    stmt.setInt(6, alertPoint);
                    stmt.setDouble(7, offset);
                    stmt.setInt(8, coolerId);
                    stmt.executeUpdate();
                } else {
                    stmt                    = transconn.prepareStatement(insertStmt);
                    stmt.setString(1, name);
                    stmt.setInt(2, locationId);
                    stmt.setInt(3, zoneId);
                    stmt.setInt(4, system);
                    stmt.setInt(5, line);
                    stmt.setInt(6, alertPoint);
                    stmt.setDouble(7, offset);
                    stmt.executeUpdate();
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
    //NischaySharma_16-Jun-2009_End

    //NischaySharma_17-Jun-2009_Start: Method to delete keg lines
    private void deleteKegLines(Element toHandle, Element toAppend) throws HandlerException {

        Iterator kegs = toHandle.elementIterator("keg");

        PreparedStatement stmt = null;
        ResultSet rs = null;
        String deleteStmt = "DELETE FROM kegLine WHERE id = ?";

        try {
            while (kegs.hasNext()) {
                Element keg = (Element) kegs.next();
                int kegId = HandlerUtils.getOptionalInteger(keg, "kegId");
                stmt = transconn.prepareStatement(deleteStmt);
                stmt.setInt(1, kegId);
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
    
    
     private void updateConcessionProductSupplier(Element toHandle, Element toAppend) throws HandlerException {
        Iterator products = toHandle.elementIterator("product");
        PreparedStatement stmt = null;
        ResultSet rs = null;
        String updateStmt = "UPDATE concessionProductMap SET supplier = ? WHERE id = ?";
        try {
            while (products.hasNext()) {
                Element product = (Element) products.next();
                int productId = HandlerUtils.getRequiredInteger(product, "productId");
                int supplierId = HandlerUtils.getRequiredInteger(product, "supplierId");
                stmt = transconn.prepareStatement(updateStmt);
                stmt.setInt(1, productId);
                stmt.setInt(2, supplierId);
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

    private void deleteConcessionProductSupplier(Element toHandle, Element toAppend) throws HandlerException {
        Iterator products = toHandle.elementIterator("product");
        PreparedStatement stmt = null;
        ResultSet rs = null;
        String updateStmt = "DELETE FROM concessionProductMap WHERE id = ?";
        try {
            while (products.hasNext()) {
                Element product = (Element) products.next();
                int productId = HandlerUtils.getRequiredInteger(product, "productId");
                stmt = transconn.prepareStatement(updateStmt);
                stmt.setInt(1, productId);
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
    
    
    private void deleteUnApprovedProduct(Element toHandle, Element toAppend) throws HandlerException {
        int callerId                        = getCallerId(toHandle);
        int oldProduct                      = HandlerUtils.getRequiredInteger(toHandle, "oldProductId");
        int newProduct                      = HandlerUtils.getRequiredInteger(toHandle, "newProductId");
        int update                          = HandlerUtils.getOptionalInteger(toHandle, "update");
        double ounces                       = HandlerUtils.getOptionalDouble(toHandle, "ounces");
        int beverage                        = HandlerUtils.getOptionalInteger(toHandle, "beverage");
        
        
        PreparedStatement stmt              = null;
        ResultSet rs                        = null, inventoryRs = null;
        
        try {
            if(oldProduct != newProduct){
            stmt                            = transconn.prepareStatement("SELECT id, name FROM product WHERE id= ? AND approved = 1");
            stmt.setInt(1, newProduct);
            rs                              = stmt.executeQuery();
            if (rs.next()){
                String productName          = rs.getString(2);

                String selectApprovalStatus = "SELECT p.approved, p.name, pD.origin, pD.abv, pD.seasonality, p.pos FROM product p " +
                                            " LEFT JOIN productDescription pD ON pD.product = p.id WHERE p.id = ?;";
                String selectOldInformation = "SELECT pS.name FROM product p LEFT JOIN productSetMap pSM ON pSM.product = p.id " +
                                            " LEFT JOIN productSet pS ON pS.id = pSM.productSet WHERE pS.productSetType = ? AND p.id = ?;";
                String insertProductApproval= "INSERT INTO productApproval (id, name, brewery, style, abv, origin, seasonality, dateApproved, status) " +
                                            " VALUES(?, ?, ?, ?, ?, ?, ?, NOW(), 1) ON DUPLICATE KEY UPDATE name = ?, brewery = ?, style = ?, abv = ?, origin = ?, seasonality = ?;";
                int oldApprovedStatus       = 0, pos = -1;
                String oldBreweryName       = "", oldStyleName = "", oldName = "", oldOrigin = "", oldABV = "", oldSeasonality = "";
                stmt                        = transconn.prepareStatement(selectApprovalStatus);
                stmt.setInt(1, oldProduct);
                rs                          = stmt.executeQuery();
                if(rs.next()) {
                    oldApprovedStatus       = rs.getInt(1);
                    oldName                 = HandlerUtils.nullToEmpty(rs.getString(2));
                    oldOrigin               = HandlerUtils.nullToEmpty(rs.getString(3));
                    oldABV                  = HandlerUtils.nullToEmpty(rs.getString(4));
                    oldSeasonality          = HandlerUtils.nullToEmpty(rs.getString(5));
                    pos                     = rs.getInt(6);
                }

                stmt                        = transconn.prepareStatement(selectOldInformation);
                stmt.setInt(1, 7);
                stmt.setInt(2, oldProduct);
                rs                          = stmt.executeQuery();
                if (rs.next()) {
                    oldBreweryName          = HandlerUtils.nullToEmpty(rs.getString(1));
                }

                stmt                        = transconn.prepareStatement(selectOldInformation);
                stmt.setInt(1, 9);
                stmt.setInt(2, oldProduct);
                rs                          = stmt.executeQuery();
                if (rs.next()) {
                    oldStyleName            = HandlerUtils.nullToEmpty(rs.getString(1));
                }
                sendProductApprovalEmail(oldProduct,newProduct);
                
                if (pos == 1 && newProduct == 9593) {
                    addToBeveragesToIgnore(oldProduct);
                }

                if (oldApprovedStatus < 1) {
                    stmt                    = transconn.prepareStatement(insertProductApproval);
                    stmt.setInt(1, oldProduct);
                    stmt.setString(2, oldName);
                    stmt.setString(3, oldBreweryName);
                    stmt.setString(4, oldStyleName);
                    stmt.setString(5, oldABV);
                    stmt.setString(6, oldOrigin);
                    stmt.setString(7, oldSeasonality);
                    stmt.setString(8, oldName);
                    stmt.setString(9, oldBreweryName);
                    stmt.setString(10, oldStyleName);
                    stmt.setString(11, oldABV);
                    stmt.setString(12, oldOrigin);
                    stmt.setString(13, oldSeasonality);
                    stmt.executeUpdate();
                }

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
                    int location            = rs.getInt(1);
                    stmt                    = transconn.prepareStatement("SELECT id FROM customBeerName WHERE location = ? AND product= ?;");
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
                        stmt                = transconn.prepareStatement("UPDATE customBeerName SET product = ?, name=? WHERE location =? AND product = ? ");
                        stmt.setInt(1, newProduct);
                        stmt.setString(2, productName);
                        stmt.setInt(3, location);
                        stmt.setInt(4, oldProduct);
                        stmt.executeUpdate();
                    }
                }
                
                stmt                        = transconn.prepareStatement("UPDATE line SET ondeck = ? WHERE ondeck = ?; ");
                stmt.setInt(1, newProduct);
                stmt.setInt(2, oldProduct);
                stmt.executeUpdate();
                
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

                String insertLog            = "INSERT INTO productChangeLog (product,type,date,user) VALUES (?,3,now(),?)";
                stmt                        = transconn.prepareStatement(insertLog);
                stmt.setInt(1, oldProduct);                
                stmt.setInt(2, callerId);       
                stmt.executeUpdate();

                insertLog                   = "DELETE FROM  productChangeLog WHERE product = ? AND type IN (1,2) AND productType=1;";
                stmt                        = transconn.prepareStatement(insertLog);
                stmt.setInt(1, oldProduct);
                //stmt.executeUpdate();

                toAppend.addElement("oldProductId").addText(String.valueOf(oldProduct));
                toAppend.addElement("newProductId").addText(String.valueOf(newProduct));
                String logMessage = "Changing product  to " + newProduct + " for id" + oldProduct;
                logger.portalDetail(callerId, "updateProduct", 0, "product", oldProduct, logMessage, transconn);
            } else {
                addErrorDetail(toAppend, "Product  " + HandlerUtils.nullToEmpty(rs.getString(2)) + " Still Not Approved");
            }  
            
            if(update>0 && beverage>0){
                String updateBev            = "UPDATE beverage set ounces = ? WHERE id=?";
                String updateIng            = "UPDATE ingredient set ounces = ? WHERE beverage=?";
                stmt                        = transconn.prepareStatement(updateBev);
                stmt.setDouble(1, ounces);                
                stmt.setInt(2, beverage);       
                stmt.executeUpdate();
                stmt                        = transconn.prepareStatement(updateIng);
                stmt.setDouble(1, ounces);                
                stmt.setInt(2, beverage);       
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
    }
    
    private void addToBeveragesToIgnore(int productId) throws HandlerException {
        
        PreparedStatement stmt              = null;
        String sqlInsert                    = " INSERT INTO beverageToIgnore (name, location, plu) (SELECT IFNULL(name, 'NA'), location, plu FROM beverage "
                                            + " WHERE id IN (SELECT beverage FROM ingredient WHERE product = ?));";
        String deleteBeverage               = " DELETE FROM beverage WHERE id IN (SELECT beverage FROM ingredient WHERE product = ?);";
        String deleteIngredient             = " DELETE FROM ingredient WHERE product = ?;";
        
        try {
            stmt                            = transconn.prepareStatement(sqlInsert);
            stmt.setInt(1, productId);
            stmt.executeUpdate();
            
            stmt                            = transconn.prepareStatement(deleteBeverage);
            stmt.setInt(1, productId);
            stmt.executeUpdate();
            
            stmt                            = transconn.prepareStatement(deleteIngredient);
            stmt.setInt(1, productId);
            stmt.executeUpdate();
        } catch (SQLException sqle) {
            logger.dbError("Database error: "+sqle.toString());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
        }
    }
    
    
     private void updateDeleteUnApprovedProductSet(Element toHandle, Element toAppend) throws HandlerException {
         int callerId                       = getCallerId(toHandle);
         int type                           =  HandlerUtils.getRequiredInteger(toHandle, "type");
         PreparedStatement stmt             = null;
         ResultSet rs                       = null;
         String updateProductSetMap         = "UPDATE productSetMap SET productSet=? WHERE productSet= ?"; 
         String updateBrewStyleMap          = "UPDATE brewStyleMap SET brewery=? WHERE brewery= ?"; 
         String deleteProductSet            = "DELETE FROM productSet WHERE id = ? ";
         String updateProductSet            = "UPDATE productSet SET name=?, approved = ? WHERE id= ?"; 
         String updateBrewLogo              = "UPDATE breweryLogo SET brewery=? WHERE productSet= ?"; 
         String selectBrewLogo              = "SELECT id FROM  breweryLogo WHERE productSet= ?"; 
         int productSetType                 = 7;  
         SQLGetHandler get                  = new SQLGetHandler();
         String insertLog                   = "";
         
         try {
             switch(type){
                 case 1:
                     int id                 = HandlerUtils.getRequiredInteger(toHandle, "id");
                     String name            = HandlerUtils.getRequiredString(toHandle, "name");
                      productSetType        = HandlerUtils.getRequiredInteger(toHandle, "productSetType");
                     int approved           = HandlerUtils.getRequiredBoolean(toHandle, "isApproved") ?  1 : 0;
                     
                     stmt = transconn.prepareStatement(updateProductSet);
                     stmt.setString(1, name);
                     stmt.setInt(2, approved);
                     stmt.setInt(3, id);
                     stmt.executeUpdate();
                     
                     stmt = transconn.prepareStatement(updateBrewLogo);
                     stmt.setString(1, name);                     
                     stmt.setInt(2, id);
                     stmt.executeUpdate();
                     
                     
                     insertLog              = "INSERT INTO productChangeLog (product,type,productType,date,user) VALUES (?,2,?,now(),?)";
                     stmt                   = transconn.prepareStatement(insertLog);
                     stmt.setInt(1, id);  
                     stmt.setInt(2, productSetType);  
                     stmt.setInt(3, callerId);  
                     stmt.executeUpdate();
                     
                     if(approved>0){                          
                          get.sendBrewLogoRequestEmail(name,id, "Inserted" );
                     }
                     break;
                 case 2:
                     int oldProductSet      = HandlerUtils.getRequiredInteger(toHandle, "oldProductSetId");
                     int newProductSet      = HandlerUtils.getRequiredInteger(toHandle, "newProductSetId");
                     productSetType         = HandlerUtils.getRequiredInteger(toHandle, "productSetType");

                     stmt                   = transconn.prepareStatement(updateProductSetMap);
                     stmt.setInt(1, newProductSet);
                     stmt.setInt(2, oldProductSet);
                     stmt.executeUpdate();
                     
                     stmt                   = transconn.prepareStatement(updateBrewStyleMap);
                     stmt.setInt(1, newProductSet);
                     stmt.setInt(2, oldProductSet);
                     stmt.executeUpdate();
                     
                     
                     stmt                   = transconn.prepareStatement(deleteProductSet);            
                     stmt.setInt(1, oldProductSet);
                     stmt.executeUpdate();
                     
                     stmt                   = transconn.prepareStatement(selectBrewLogo);  
                     stmt.setInt(1, newProductSet);
                     rs                     = stmt.executeQuery();   
                     if (rs.next()) {
                         stmt               = transconn.prepareStatement("DELETE FROM breweryLogo WHERE productSet=?");            
                         stmt.setInt(1, oldProductSet);
                         stmt.executeUpdate();
                         
                     } else {
                         stmt               = transconn.prepareStatement("UPDATE breweryLogo SET productSet = ? WHERE productSet=?");            
                         stmt.setInt(1, newProductSet);
                         stmt.setInt(2, oldProductSet);
                         stmt.executeUpdate();
                     }
                     
                     
                     insertLog              = "INSERT INTO productChangeLog (product,type,productType,date,user) VALUES (?,3,?,now(),?)";
                     stmt                   = transconn.prepareStatement(insertLog);
                     stmt.setInt(1, oldProductSet);  
                     stmt.setInt(2, productSetType);  
                     stmt.setInt(3, callerId);  
                     stmt.executeUpdate();
                     
                     insertLog              = "DELETE FROM  productChangeLog WHERE product = ? AND type IN (1,2) AND productType= ?;";
                     stmt                   = transconn.prepareStatement(insertLog);
                     stmt.setInt(1, oldProductSet);     
                     stmt.setInt(2, productSetType);  
                     //stmt.executeUpdate();
                     
                     String logMessage = "Changing productSet  to " + newProductSet + " for id" + oldProductSet;
                     logger.portalDetail(callerId, "updateProduct", 0, "product", oldProductSet, logMessage, transconn);
             
                     break;
             }
         
         } catch (SQLException sqle) {
             logger.dbError("Database error: " + sqle.getMessage());
             throw new HandlerException(sqle);
         } finally {
             close(stmt);
         }
          
    }
     
     private void resetForgotPassword(Element toHandle, Element toAppend) throws HandlerException {
        
        
        String username                     = HandlerUtils.getRequiredString(toHandle, "username");
        String email                        = HandlerUtils.getRequiredString(toHandle, "email");
      
        
        String selectUsername               = "SELECT id,name FROM user  WHERE email = ? AND username= ?;";
        String selectUser                   = "SELECT IF(u.isManager = 0, (SELECT COUNT(uM.id) FROM userMap uM WHERE uM.user = u.id), isManager) AS active , u.id, u.username, u.name FROM `user` u "
                                            + " WHERE u.email = ? AND u.username= ? ORDER BY id DESC, active DESC;";
        
        PreparedStatement stmt              = null;
        ResultSet rs                        = null;
        try {
            stmt                            = transconn.prepareStatement(selectUser);  
            stmt.setString(1, email);
            stmt.setString(2, username);
            
            rs                              = stmt.executeQuery();   
            if (rs.next()) {
                int active                  = rs.getInt(1);
                int userId                  = rs.getInt(2);                
                String name                 = rs.getString(4);
                if(active > 0) {
                    String password         = RandomAlphaNumericString(8);
                    logger.debug("Password:"+password);
              
                    MessageDigest sha1      = MessageDigest.getInstance("SHA1");             
                    byte[] bytesOfPassword  = password.getBytes("UTF-8");
                    sha1.reset();
                    sha1.update(bytesOfPassword);
                    Formatter formatter = new Formatter();
                    for (byte b : sha1.digest()) {
                        formatter.format("%02x", b);
                    }
                    String shaPassword      = formatter.toString();
                    logger.debug("SHA1 Password:"+shaPassword);
                    stmt                     = transconn.prepareStatement("UPDATE user SET password = ? WHERE id= ?");  
                    stmt.setString(1, shaPassword);
                    stmt.setInt(2, userId);
                    stmt.executeUpdate();




                    StringBuilder forgetUser= new StringBuilder();
                    forgetUser.append("<tr align=justify><td colspan=4>Your BevManager Username: "+username+"</td></tr>");
                    forgetUser.append("<tr align=justify><td colspan=4>Your BevManager Password: "+password+"</td></tr>");
                    forgetUser.append("<tr align=justify><td colspan=4>Account:"+email+"</td></tr>");
                    forgetUser.append("<tr align=center valign=middle><td height=20 colspan=4>&nbsp;</td></tr>");
                    forgetUser.append("<tr align=justify><td colspan=4>Thank You,</td></tr>");
                    forgetUser.append("<tr align=justify><td colspan=4>US Beverage Net Support</td></tr>");
                    forgetUser.append("<tr align=center valign=middle><td height=35 colspan=4>&nbsp;</td></tr>");
                    forgetUser.append("<tr align=justify><td colspan=4><strong>This email was automatically generated; please do not reply.</strong></td></tr><tr><td colspan=4>&nbsp;</td></tr>");

                    sendMail("Your BevManager Password ", name,email, "support@beerboard.com", "BevManager Reset Password", "sendMail",forgetUser, false);
                } else {
                     toAppend.addElement("message").addText("Your userId is Inactive please contact Customer Care!");
                }

                
            } else {
                toAppend.addElement("message").addText("Invalid emailId or username!");
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error in getErrorLogs: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } catch(Exception se){
        }finally {
            close(rs);
            close(stmt);
        }

    }
     
    private String RandomAlphaNumericString(int size){
        String chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        String ret = "";
        int length = chars.length();
        for (int i = 0; i < size; i ++){
            ret += chars.split("")[ (int) (Math.random() * (length - 1)) ];
        }
        return ret;
}
    
    
     private void updateOrderSetting(Element toHandle, Element toAppend) throws HandlerException {

        int location                        = HandlerUtils.getRequiredInteger(toHandle, "locationId");
        int supplier                        = HandlerUtils.getRequiredInteger(toHandle, "supplierId");        
        int day                             = HandlerUtils.getRequiredInteger(toHandle, "day");        
        int callerId                        = getCallerId(toHandle);
        
        PreparedStatement stmt              = null;
        ResultSet rs                        = null;

        String insertPurchase               = "INSERT INTO purchaseSetting (location,supplier,day,date) " +
                                            "VALUES (?,?,?,now())";
        String getLastId                    = "SELECT LAST_INSERT_ID()";
        String insertPurchaseDetail         = "INSERT INTO purchaseSettingDetail " +
                                            " (purchaseSetting, product)  VALUES (?,?) ";
        String deleteSetting                = "DELETE FROM purchaseSettingDetail WHERE purchaseSetting=?";
        String selectSetting                = "SELECT pS.id FROM purchaseSetting pS WHERE pS.location=? AND pS.supplier= ? AND pS.day = ?;";
        int purchaseSetting                 = 0;
        try {
            //check params
            assertForeignKey("location", location, transconn);
            assertForeignKey("supplier", supplier, transconn);
            stmt = transconn.prepareStatement(selectSetting);
            stmt.setInt(1, location);
            stmt.setInt(2, supplier);
            stmt.setInt(3, day);
            rs = stmt.executeQuery();
            if (rs.next()) {
                purchaseSetting             = rs.getInt(1);
            } else {
                 stmt = transconn.prepareStatement(insertPurchase);
                 stmt.setInt(1, location);
                 stmt.setInt(2, supplier);
                 stmt.setInt(3, day);
                 stmt.executeUpdate();
                 
                 stmt = transconn.prepareStatement(getLastId);
                 rs = stmt.executeQuery();
                 if (rs.next()) {
                     purchaseSetting        = rs.getInt(1);
                 }
            }
            if(purchaseSetting > 0) {
                stmt                        = transconn.prepareStatement(deleteSetting);
                stmt.setInt(1, purchaseSetting);
                stmt.executeUpdate();
                
                Iterator i = toHandle.elementIterator("product");
                while (i.hasNext()) {
                    Element el = (Element) i.next();
                    int productId = HandlerUtils.getRequiredInteger(el, "id");
                    assertForeignKey("product", productId, transconn);
                    stmt = transconn.prepareStatement(insertPurchaseDetail);
                    stmt.setInt(1, purchaseSetting);
                    stmt.setInt(2, productId);               
                    stmt.executeUpdate();
                }
            }
           logger.portalDetail(callerId, "updateOrderSetting", location, "updateOrderSetting", supplier, "", transconn);
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
            close(rs);
        }
    }
     
     
     private void updateBBTVAutoFeed(Element toHandle, Element toAppend) throws HandlerException {
        int callerId                        = getCallerId(toHandle);
        int location                        = HandlerUtils.getRequiredInteger(toHandle, "locationId");
        int newBeer                         = HandlerUtils.getRequiredInteger(toHandle, "newBeer");
        int localBeer                       = HandlerUtils.getRequiredInteger(toHandle, "localBeer");
        int comingSoon                      = HandlerUtils.getRequiredInteger(toHandle, "comingSoon");
        PreparedStatement stmt              = null;

        try {
            String updateFeed = "UPDATE bbtvAutoFeed SET newBeer=?, localBeer=?, comingSoon = ? WHERE location=?";
            stmt = transconn.prepareStatement(updateFeed);
            stmt.setInt(1, newBeer);
            stmt.setInt(2, localBeer);
            stmt.setInt(3, comingSoon);
            stmt.setInt(4, location);
            stmt.executeUpdate();
            
            stmt = transconn.prepareStatement("UPDATE locationBeerBoardMap SET css=1 WHERE location=?");            
            stmt.setInt(1, location);
            stmt.executeUpdate();
            logger.portalDetail(callerId, "updateBBTVMenuSetting", location, "updateBBTVAutoFeed", 0, "", transconn);
        } catch (SQLException sqle) {
            logger.dbError("Database error in updateBBTVAutoFeed: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
        }

    }
     
     
       private void updateBBTVMenuFormat(Element toHandle, Element toAppend) throws HandlerException {
           int callerId                     = getCallerId(toHandle);
        int location                        = HandlerUtils.getRequiredInteger(toHandle, "locationId");
        
        PreparedStatement stmt              = null;

        try {
            String updateFeed = "UPDATE bbtvMenuFormat SET  property=? WHERE location=? AND row =?";
             Iterator i = toHandle.elementIterator("menuFormat");
             while (i.hasNext()) {
                Element menu = (Element) i.next();
                int row                     = HandlerUtils.getRequiredInteger(menu, "row");
                int property                = HandlerUtils.getRequiredInteger(menu, "property");
                stmt                        = transconn.prepareStatement(updateFeed);           
                stmt.setInt(1, property);
                stmt.setInt(2, location);
                stmt.setInt(3, row);
                stmt.executeUpdate();
             }
             logger.portalDetail(callerId, "updateBBTVMenuSetting", location, "updateBBTVMenuFormat", 0, "", transconn);
            
        } catch (SQLException sqle) {
            logger.dbError("Database error in updateBBTVAutoFeed: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
        }

    }
       
       private void updateCustomBeerName(Element toHandle, Element toAppend) throws HandlerException {
           int callerId                     = getCallerId(toHandle);
        int location                        = HandlerUtils.getRequiredInteger(toHandle, "locationId");
        int product                         = HandlerUtils.getRequiredInteger(toHandle, "productId");
        String name                         = HandlerUtils.getRequiredString(toHandle, "productName");
       
        PreparedStatement stmt              = null;
        ResultSet rs                        = null;

        try {
            String updateName               = "UPDATE customBeerName SET name=? WHERE location=? AND product = ?;";
            String insertName               = "INSERT INTO customBeerName (location,product,name) Values(?,?,?);";
            String selectName               = "SELECT id FROM customBeerName WHERE location=? AND product = ?;";
            stmt                            = transconn.prepareStatement(selectName);
            stmt.setInt(1, location);            
            stmt.setInt(2, product);            
            rs                              = stmt.executeQuery();   
            if (rs.next()) {  
                stmt                        = transconn.prepareStatement(updateName);
                stmt.setString(1, name);
                stmt.setInt(2, location);
                stmt.setInt(3, product);
                
                stmt.executeUpdate();
            } else {
                stmt                        = transconn.prepareStatement(insertName);
                stmt.setString(3, name);
                stmt.setInt(1, location);
                stmt.setInt(2, product);                
                stmt.executeUpdate();
            }
            stmt            = transconn.prepareStatement("UPDATE locationBeerBoardMap SET css=1,marketing = 1 WHERE location = ?");
            stmt.setInt(1, location);
            stmt.executeUpdate();
            
            logger.portalDetail(callerId, "updateCustomBeerName", location, "updateCustomBeerName", product, "", transconn);
            
        } catch (SQLException sqle) {
            logger.dbError("Database error in updateCustomBeerName: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(rs);
            close(stmt);
        }

    }
       
       
       private void updateCustomBeerDesc(Element toHandle, Element toAppend) throws HandlerException {
           int callerId                     = getCallerId(toHandle);
        int location                        = HandlerUtils.getRequiredInteger(toHandle, "locationId");
        int product                         = HandlerUtils.getRequiredInteger(toHandle, "productId");        
        String description                  = HandlerUtils.getOptionalString(toHandle, "description");
        PreparedStatement stmt              = null;
        ResultSet rs                        = null;

        try {
            String updateName               = "UPDATE customBeerName SET   description=? WHERE location=? AND product = ?;";
            String insertName               = "INSERT INTO customBeerName (location,product,description) Values(?,?,?);";
            String selectName               = "SELECT id FROM customBeerName WHERE location=? AND product = ?;";
            stmt                            = transconn.prepareStatement(selectName);
            stmt.setInt(1, location);            
            stmt.setInt(2, product);            
            rs                              = stmt.executeQuery();   
            if (rs.next()) {  
                stmt                        = transconn.prepareStatement(updateName);
                stmt.setString(1, HandlerUtils.nullToEmpty(description));
                stmt.setInt(2, location);
                stmt.setInt(3, product);
                
                stmt.executeUpdate();
            } else {
                stmt                        = transconn.prepareStatement(insertName);
               
                stmt.setInt(1, location);
                stmt.setInt(2, product);
                stmt.setString(3, HandlerUtils.nullToEmpty(description));
                stmt.executeUpdate();
            }
            stmt            = transconn.prepareStatement("UPDATE locationBeerBoardMap SET css=1,marketing = 1 WHERE location = ?");
            stmt.setInt(1, location);
            stmt.executeUpdate();
            logger.portalDetail(callerId, "updateCustomBeerDesc", location, "updateCustomBeerDesc", product, "", transconn);
            
        } catch (SQLException sqle) {
            logger.dbError("Database error in updateCustomBeerName: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(rs);
            close(stmt);
        }

    }
       
       
       private void updateCustomStyleName(Element toHandle, Element toAppend) throws HandlerException {
           int callerId                     = getCallerId(toHandle);
        int location                        = HandlerUtils.getRequiredInteger(toHandle, "locationId");
        int productSet                      = HandlerUtils.getRequiredInteger(toHandle, "productSet");
        String name                         = HandlerUtils.getRequiredString(toHandle, "styleName");
        String logo                         = HandlerUtils.getOptionalString(toHandle, "logo");
        if(logo==null){
            logo                            = HandlerUtils.nullToEmpty(logo);
        } else {
            logo                            = logo.trim().replaceAll("\'", "%27").replaceAll(" ", "%20");
        }
        PreparedStatement stmt              = null;
        ResultSet rs                        = null;

        try {
            String updateName               = "UPDATE customStyleName SET name=?,logo =? WHERE location=? AND productSet = ?;";
            String insertName               = "INSERT INTO customStyleName (location,productSet,name,logo) Values(?,?,?,?);";
            String selectName               = "SELECT id FROM customStyleName WHERE location=? AND productSet = ?;";
            stmt                            = transconn.prepareStatement(selectName);
            stmt.setInt(1, location);            
            stmt.setInt(2, productSet);            
            rs                              = stmt.executeQuery();   
            if (rs.next()) {  
                stmt                        = transconn.prepareStatement(updateName);
                stmt.setString(1, name);
                stmt.setString(2, logo);
                stmt.setInt(3, location);
                stmt.setInt(4, productSet);
                stmt.executeUpdate();
            } else {
                stmt                        = transconn.prepareStatement(insertName);
                stmt.setString(3, name);
                stmt.setString(4, logo);
                stmt.setInt(1, location);
                stmt.setInt(2, productSet);
                stmt.executeUpdate();
            }
            stmt            = transconn.prepareStatement("UPDATE locationBeerBoardMap SET css=1,marketing = 1 WHERE location = ?");
            stmt.setInt(1, location);
            stmt.executeUpdate();
            logger.portalDetail(callerId, "updateCustomStyleName", location, "updateCustomStyleName", productSet, "", transconn);
            
        } catch (SQLException sqle) {
            logger.dbError("Database error in updateCustomStyleName: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(rs);
            close(stmt);
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
    
    
    public void addUserHistory(int userId,String action,int locationId, String message,int mobileId,int product)throws HandlerException {
        String checkAction                  = " SELECT id FROM task WHERE abbrev=? LIMIT 1";
        String insertFullLog                = " INSERT INTO userHistoryMobile (user,task,description,location,mobile,timestamp,product) " 
                                            + " VALUES (?,?,?,?,?,now(),?) ";
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
                logger.debug(message);
                stmt = transconn.prepareStatement(insertFullLog);
                stmt.setInt(1,userId);
                stmt.setInt(2,taskId);
                stmt.setString(3, message);
                stmt.setInt(4,locationId);                
                stmt.setInt(5,mobileId);
                stmt.setInt(6,product);
                stmt.executeUpdate();
            
            } catch (SQLException sqle) {
            logger.dbError("Database error in addUserHistory: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(rs);
            close(stmt);
        }
        
    }
    
    
    private void generateNewPurchase(Element toHandle, Element toAppend) throws HandlerException {
        
        
      
        
        String selectLocationSupplier       = "SELECT location,supplier from purchaseSetting WHERE day=dayofweek(now())  AND supplier <> 2; ";        
        
        PreparedStatement stmt              = null;
        ResultSet rs                        = null;
        try {
            stmt                            = transconn.prepareStatement(selectLocationSupplier);
            rs                              = stmt.executeQuery();   
            while (rs.next()) {   
                int locationId              = rs.getInt(1);
                int supplier                = rs.getInt(2);
                toHandle.addElement("locationId").addText(String.valueOf(locationId));
                toHandle.addElement("supplierId").addText(String.valueOf(supplier));
                createOrder(toHandle,toAppend);
                
            } 
        
        } catch (SQLException sqle) {
            logger.dbError("Database error in getRawData: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(rs);
            close(stmt);
        }
    }
    
    
    private void createOrder(Element toHandle, Element toAppend) throws HandlerException {

        int location                        = HandlerUtils.getRequiredInteger(toHandle, "locationId");
        int supplier                        = HandlerUtils.getRequiredInteger(toHandle, "supplierId");
        String totalString                  = "0.0";
        int callerId                        = getCallerId(toHandle);
        double total                        = 0.0;
        int purchaseId                      = 0;

        PreparedStatement stmt              = null;
        ResultSet rs                        = null;

        String insertPurchase               = "INSERT INTO purchase (location,supplier,total) " +
                                            " VALUES (?,?,?)";
        String getLastId                    = "SELECT LAST_INSERT_ID()";
        String insertPurchaseDetail         = "INSERT INTO purchaseDetail " +
                                            " (purchase, product, productPlu, quantity, price) " +
                                            " VALUES (?,?,?,?,?) ";       
        String checkOpenPurchase            = "SELECT id FROM purchase WHERE location= ? and supplier = ? AND status <> 'OPEN';";
        String selectInvProducts            = "SELECT product,plu,minimumQty, qtyOnHand,qtyToHave from inventory WHERE location=? AND supplier = ? AND minimumQty > 0  AND qtyOnHand < qtyToHave AND supplier <> 2 ;";
        try {
            //check params
            assertForeignKey("location", location, transconn);
            assertForeignKey("supplier", supplier, transconn);
            stmt = transconn.prepareStatement(checkOpenPurchase);
            stmt.setInt(1, location);
            stmt.setInt(2, supplier);
            rs = stmt.executeQuery();
            if (!rs.next()) {

            //add the purchase record
            stmt = transconn.prepareStatement(insertPurchase);
            stmt.setInt(1, location);
            stmt.setInt(2, supplier);
            stmt.setDouble(3, total);
            stmt.executeUpdate();

            stmt = transconn.prepareStatement(getLastId);
            rs = stmt.executeQuery();
            if (rs.next()) {
                purchaseId                  = rs.getInt(1);
            } else {
                throw new HandlerException("Error: SELECT_LAST_ID didn't return a result");
            }
            toAppend.addElement("orderNumber").addText(String.valueOf(purchaseId));
            String logMessage               = "Placed order # " + purchaseId;
            logger.portalDetail(callerId, "placeOrder", location, "purchase", purchaseId, logMessage, transconn);
            //add a purchaseDetail record for each product in the order
            
            stmt = transconn.prepareStatement(selectInvProducts);
            stmt.setInt(1, location);
            stmt.setInt(2, supplier);
            rs = stmt.executeQuery();           
            while (rs.next()) {
                String plu                  =rs.getString(2);
                int quantity                = rs.getInt(3);
                int productId               = rs.getInt(1);
                int qtyOnHand               = rs.getInt(4);
                int qtyToHave               = rs.getInt(5);
                if(qtyToHave > quantity) {
                    quantity                = qtyToHave;
                } 
                String priceString          = "0.0";
                double price;
                try {
                    price                   = Double.parseDouble(priceString);
                } catch (NumberFormatException nfe) {
                    throw new HandlerException("Not a parseable decimal: " + priceString);
                }
                stmt = transconn.prepareStatement(insertPurchaseDetail);
                stmt.setInt(1, purchaseId);
                stmt.setInt(2, productId);
                stmt.setString(3, plu);
                stmt.setInt(4, quantity);
                stmt.setDouble(5, price);
                stmt.executeUpdate();

            }
           
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
            close(rs);
        }
    }

    
    private void updateFacebookTwitterSetting(Element toHandle, Element toAppend) throws HandlerException {
        int callerId                        = getCallerId(toHandle);
        int location                        = HandlerUtils.getRequiredInteger(toHandle, "locationId");
        int lineUpdate                      = 0,like =0, ckeckin =0, promotion =0;
        int type                            = HandlerUtils.getRequiredInteger(toHandle, "type");
        PreparedStatement stmt              = null;
        ResultSet rs                        = null;

        try {            
             Iterator i = toHandle.elementIterator("setting");
             while (i.hasNext()) {
                Element setting = (Element) i.next();
                int value                     = HandlerUtils.getRequiredInteger(setting, "value");
                String name                = HandlerUtils.getRequiredString(setting, "name");
                if(name.equals("lineUpdate")) {
                    lineUpdate              = value;
                } else if(name.equals("likes")){
                    like                    = value;
                } else if(name.equals("checkin")){
                    ckeckin                 = value;
                } else if(name.equals("promotion")){
                    promotion               = value;
                }
             }
            String updateSetting            = "UPDATE socialMediaPost SET lineUpdate=?,likes = ?,checkin = ?,promotion = ? WHERE location=? AND type = ?;";
            String insertSetting            = "INSERT INTO socialMediaPost (lineUpdate,likes,checkin,promotion,location,type) Values(?,?,?,?,?,?);";
            String selectSetting            = "SELECT id FROM socialMediaPost WHERE location=? AND type = ?;";
            stmt                            = transconn.prepareStatement(selectSetting);
            stmt.setInt(1, location);            
            stmt.setInt(2, type);            
            rs                              = stmt.executeQuery();   
            if (rs.next()) {  
                stmt                        = transconn.prepareStatement(updateSetting);                
                stmt.setInt(1, lineUpdate);
                stmt.setInt(2, like);
                stmt.setInt(3, ckeckin);
                stmt.setInt(4, promotion);
                stmt.setInt(5, location);
                stmt.setInt(6, type);
                stmt.executeUpdate();
            } else {
                stmt                        = transconn.prepareStatement(insertSetting);                
                stmt.setInt(1, lineUpdate);
                stmt.setInt(2, like);
                stmt.setInt(3, ckeckin);
                stmt.setInt(4, promotion);
                stmt.setInt(5, location);
                stmt.setInt(6, type);
                stmt.executeUpdate();
            }
            logger.portalDetail(callerId, "updateFacebookTwitterSetting", location, "updateFacebookTwitterSetting", type, "", transconn);
        } catch (SQLException sqle) {
            logger.dbError("Database error in updateFacebookTwitterSetting: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(rs);
            close(stmt);
        }

    }
    
    
    private void linkBrasstapItems(Element toHandle, Element toAppend) throws HandlerException {
          
         int locationId                     = HandlerUtils.getRequiredInteger(toHandle, "locationId");
         String itemIdString                = HandlerUtils.getOptionalString(toHandle, "itemId");
         int beerID                         = HandlerUtils.getOptionalInteger(toHandle, "beerID");         
         PreparedStatement stmt             = null;
         String updateItem                  = " UPDATE brasstapItems SET beerID=? WHERE id in ("+itemIdString+")";
         
         try {
             if(itemIdString != null && !itemIdString.equals("")){
                 stmt                       = transconn.prepareStatement(updateItem);
                 stmt.setInt(1,beerID);
                 stmt.executeUpdate();
             }
         } catch (SQLException sqle) {
             logger.dbError("Database error: " + sqle.getMessage());
             throw new HandlerException(sqle);
         } finally {           
             close(stmt);
         }
    }
    
     private void resetKegLevel(Element toHandle, Element toAppend) throws HandlerException {
          
         int locationId                     = HandlerUtils.getRequiredInteger(toHandle, "locationId");
         int lineId                         = HandlerUtils.getRequiredInteger(toHandle, "lineId");
         
         PreparedStatement stmt             = null;         
         
         try {
             stmt                = transconn.prepareStatement("UPDATE line SET level = resetLevel WHERE id = ? ;");
             stmt.setInt(1, lineId);                  
             stmt.executeUpdate();
                        
              
         } catch (SQLException sqle) {
             logger.dbError("Database error: " + sqle.getMessage());
             throw new HandlerException(sqle);
         } finally {           
             close(stmt);
         }
    }
     
     
       private void updateComingSoonProductLine(Element toHandle, Element toAppend) throws HandlerException {
          
           PreparedStatement stmt             = null;     
           ResultSet rs                        = null;
           String selectProduct                = "SELECT location,line,product FROM comingSoonProducts WHERE assignDate=DATE(now());";        
        
        
        try {
            stmt                        = transconn.prepareStatement(selectProduct);           
            rs                          = stmt.executeQuery();   
            while (rs.next()) {   
                int location                = rs.getInt(1);
                int line                    = rs.getInt(2);
                int product                 = rs.getInt(3);
                toHandle.addElement("locationId").addText(String.valueOf(location));  
                Element callerEl    = toHandle.addElement("caller");                
                callerEl.addElement("callerId").addText(String.valueOf(0));   
                Element dataEl    = toHandle.addElement("line");                
                dataEl.addElement("id").addText(String.valueOf(line));                
                dataEl.addElement("product").addText(String.valueOf(product));  
                updateLineProduct(toHandle, toAppend);
            }             
        
        } catch (SQLException sqle) {
            logger.dbError("Database error in getComingSoonProducts: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(rs);
            close(stmt);
        }
    }
    
    
    
   private void testNewBeerNotification(Element toHandle, Element toAppend) throws HandlerException {
          
         int locationId                     = HandlerUtils.getRequiredInteger(toHandle, "locationId");
          int productId                     = HandlerUtils.getRequiredInteger(toHandle, "productId");
          try{
              generateNewBeerPushNotification(locationId, productId);
          } catch(Exception e){
              
          }
   }
   
   public void generateNewBeerPushNotification(int location, int productId) throws SQLException, HandlerException {
        
        PreparedStatement stmt              = null;     
        ResultSet rs                        = null,rsDetails = null;        
        String selectLocationProduct        = "SELECT DISTINCT p.id, IFNULL((SELECT style FROM beerStylesMap WHERE productSet=pS.id),0) AS style,p.name,pS.id,pS.name FROM  product p "
                                            + " LEFT JOIN productSetMap pSM ON pSM.product=p.id LEFT JOIN productSet pS ON pS.id=pSM.productSet"
                                            + "  WHERE p.id = ? AND pS.productSetType=9 AND p.id NOT IN (4311,9593,10661) HAVING style >0;";        
        String getLastId                    = " SELECT LAST_INSERT_ID()";        
        
        String selectMessage                = "SELECT id FROM pushMessage WHERE message = 'New Beers Now on Tap at your Favorite Bars' AND pushTime BETWEEN " +
                                            " (SELECT IF( TIME(NOW()) > '16:29:00' , (SELECT CONCAT(DATE(ADDDATE(NOW(), INTERVAL 1 DAY)), ' 16:30:00')), " +
                                            " (SELECT CONCAT(DATE(NOW()), ' 16:29:00')) ) ) AND (SELECT IF(TIME(NOW()) > '16:29:00', " +
                                            " (SELECT CONCAT(DATE(ADDDATE(NOW(), INTERVAL 2 DAY)), ' 16:29:00')), " +
                                            " (SELECT CONCAT(DATE(ADDDATE(NOW(), INTERVAL 1 DAY)), ' 16:30:00'))));";
        
        Map<Integer, String> mobUserMap     = new HashMap<Integer, String>();                   
        Map<Integer, String> userMessageMap = new HashMap<Integer, String>();
        try {
            String  message                 = "", productName = "";
            int messageId                   = 0;
            
            stmt                            = transconn.prepareStatement(selectMessage);
            rs                              = stmt.executeQuery();
            if(rs.next()) {
                 messageId                  = rs.getInt(1);
            } 
            if(messageId < 1) {
                message                     = "New Beers Now on Tap at your Favorite Bars";
                stmt                        = transconn.prepareStatement("INSERT INTO pushMessage (message, location, reward, category, pushTime) VALUES (?, ?, ?, 2, " +
                                            " (SELECT IF(TIME(NOW()) > '16:29:00' ,  (SELECT CONCAT(DATE(ADDDATE(NOW(), INTERVAL 1 DAY)), ' 16:30:00')), " +
                                            " (SELECT CONCAT(DATE(NOW()), ' 16:30:00')))));");
                stmt.setString(1, message);
                stmt.setInt(2, 425);
                stmt.setInt(3, 0);
                stmt.executeUpdate();

                stmt                        = transconn.prepareStatement(getLastId);
                rsDetails                   = stmt.executeQuery();
                if(rsDetails.next()) {
                    messageId               = rsDetails.getInt(1);
                }
            }
            productName                     = "";
            stmt                            = transconn.prepareStatement(selectLocationProduct);
            stmt.setInt(1, productId);      
            rs                              = stmt.executeQuery();
            if (rs.next()) {
                int style                  = rs.getInt(2);
                productName                 += HandlerUtils.nullToEmpty(rs.getString(3));
                String selectUserByLocation = "SELECT 1, u.id AS userId, u.username, IFNULL ((SELECT message FROM pushMessageCenter p WHERE p.user=u.id AND pushMessage= ? " +
                                            " AND location = fL.location),\"\") FROM favoriteLocation fL LEFT JOIN bbtvMobileUser u ON u.id=fL.user "
                                            + " WHERE LENGTH(deviceToken) > 4 AND arrival > SUBDATE(NOW(), INTERVAL 20 DAY) AND ((fL.location= ? AND u.styles='') OR " +
                                            " (fL.location = ? AND (u.styles LIKE ('"+style+"') OR u.styles LIKE ('%,"+style+",%') OR u.styles LIKE ('"+style+",%') OR " +
                                            " u.styles LIKE ('%,"+style+"')))) "
                                            + " UNION "
                                            + " SELECT 1,u.id AS userId, u.username, IFNULL ((SELECT message FROM pushMessageCenter p WHERE p.user=u.id AND pushMessage= ? " +
                                            " AND location = ?),\"\") FROM favoriteBeer fB LEFT JOIN bbtvMobileUser u ON u.id=fB.user " +
                                            " LEFT JOIN favoriteLocation fL ON fL.user = u.id WHERE  fB.product = ? AND fL.location = ? group BY userId order by userId ;";
                
                userMessageMap              = new HashMap<Integer, String>();  
                stmt                        = transconn.prepareStatement(selectUserByLocation);                
                stmt.setInt(1,messageId);
                stmt.setInt(2,location);
                stmt.setInt(3,location);
                stmt.setInt(4,messageId);
                stmt.setInt(5,location);
                stmt.setInt(6,productId);
                stmt.setInt(7,location);
                rsDetails                   = stmt.executeQuery();
                mobUserMap                  = new HashMap<Integer, String>();                
                while (rsDetails.next()) {                    
                    int userId              = rsDetails.getInt(2);
                    String userName         = HandlerUtils.nullToEmpty(rsDetails.getString(3));
                    String productMessage   = HandlerUtils.nullToEmpty(rsDetails.getString(4));
                   // logger.debug("User:"+userId +":"+userName +" Msg:"+productMessage);
                    if(userId > 0) {
                        mobUserMap.put(userId,userName);
                        userMessageMap.put(userId,  productMessage);
                    }  
                }
                
                for (Integer key : userMessageMap.keySet()) {
                    String productMessage   = userMessageMap.get(key);
                    productMessage          = productMessage.trim();
                    if(!productMessage.equals("") && !productMessage.contains(productName)){
                        productMessage      =productMessage +", "+productName;
                    } else if(productMessage.equals("")){
                        productMessage      = productName;
                    }
                    if(!productMessage.equals("")) {
                        stmt                    = transconn.prepareStatement("SELECT id FROM pushMessageCenter p WHERE p.user=? AND pushMessage= ? AND location = ?");
                     stmt.setInt(1,key);
                     stmt.setInt(2,messageId);
                     stmt.setInt(3,location);
                     rsDetails              = stmt.executeQuery();
                     if(rsDetails.next()){
                         stmt               = transconn.prepareStatement("UPDATE  pushMessageCenter  SET message = ? WHERE  pushMessage = ? AND location = ? AND user =?");                            
                         stmt.setString(1,productMessage);
                         stmt.setInt(2,messageId);
                         stmt.setInt(3,location);
                         stmt.setInt(4,key);
                         stmt.executeUpdate();
                     } else {
                         stmt                = transconn.prepareStatement("INSERT INTO pushMessageCenter (pushMessage, location, message, user) VALUES (?,?,?,?);");                            
                         stmt.setInt(1,messageId);
                         stmt.setInt(2,location);
                         stmt.setString(3,productMessage);
                         stmt.setInt(4,key);
                         stmt.executeUpdate();
                     }
                    }
                }
            }
            for (Integer key : userMessageMap.keySet()) {
                //logger.debug("user:"+key+" - "+mobUserMap.get(key) );
                stmt                        = transconn.prepareStatement("SELECT id FROM pushMessageMap WHERE user=? AND message= ?");
                stmt.setInt(1,key);
                stmt.setInt(2,messageId);
                rsDetails                   = stmt.executeQuery();
                if(!rsDetails.next()){
                    stmt                    = transconn.prepareStatement("INSERT INTO pushMessageMap (user, message) VALUES (?,?);");
                    stmt.setInt(1,key);
                    stmt.setInt(2,messageId);
                    stmt.executeUpdate();
                }
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: "+sqle.toString());
            throw new HandlerException(sqle);
        } finally {            
             close(rsDetails);
             close(rs);
            close(stmt);
        }
    }
      
   
    private void updateBottleBeer(Element toHandle, Element toAppend) throws HandlerException {
        int callerId                        = getCallerId(toHandle);
        int location                        = HandlerUtils.getRequiredInteger(toHandle, "locationId");       
        PreparedStatement stmt              = null;
        ResultSet rs                        = null;
        String checkBeer                    = "SELECT id FROM bottleBeer WHERE product = ? AND location=? AND type = ?;";
        String updateBeer                   = "UPDATE bottleBeer SET  type = ?,price =? WHERE id =?; ";
        try {
            if(callerId > 0 && location > 0 && isValidAccessUser(callerId, location,false)){
           Iterator upBeer   = toHandle.elementIterator("updateBottle");
             while (upBeer.hasNext()) {
                Element updateProduct       = (Element) upBeer.next();
                int id                      = HandlerUtils.getRequiredInteger(updateProduct, "id");
                int type                    = HandlerUtils.getRequiredInteger(updateProduct, "type");                
                String price                = HandlerUtils.getRequiredString(updateProduct, "price");
                if(price==null || price.equals("")){
                    price                   = "0";
                }                
                             
                if (id > 0) { 
                    stmt                    = transconn.prepareStatement(updateBeer);                                        
                    stmt.setInt(1, type); 
                    stmt.setDouble(2, Double.parseDouble(price));
                    stmt.setInt(3, id); 
                    stmt.executeUpdate();               
                }
            }
             logger.portalDetail(callerId, "updateBottleBeer", location, "updateBottleBeer", 0, "updateBottleBeer", transconn);
            } else {
                addErrorDetail(toAppend, "Invalid Access"  );
            }

        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
        }
    }
    
    private void deleteBottleBeer(Element toHandle, Element toAppend) throws HandlerException {
        int callerId                        = getCallerId(toHandle);
        int location                        = HandlerUtils.getRequiredInteger(toHandle, "locationId");
       
        PreparedStatement stmt              = null;
        ResultSet rs                        = null;
        String checkBeer                    = "SELECT id FROM bottleBeer WHERE product = ? AND location=? AND type = ?;";
        String deleteBeer                   = "DELETE FROM bottleBeer  WHERE id =?; ";
        try {
            if(callerId > 0 && location > 0 && isValidAccessUser(callerId, location,false) && !isReadOnlyUser(callerId, location)){
           Iterator delBeer   = toHandle.elementIterator("deleteBottle");
             while (delBeer.hasNext()) {
                Element delProduct          = (Element) delBeer.next();
                int id                      = HandlerUtils.getRequiredInteger(delProduct, "id");               
                
                             
                if (id>0) { 
                    stmt                    = transconn.prepareStatement(deleteBeer);                                       
                    stmt.setInt(1, id); 
                    stmt.executeUpdate();       
                    
                }
            }
             logger.portalDetail(callerId, "deleteBottleBeer", location, "deleteBottleBeer", 0, "deleteBottleBeer", transconn);
            } else {
                addErrorDetail(toAppend, "Invalid Access"  );
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
        }
     
    }

    private void getCurrentLines(Element toHandle, Element toAppend) throws HandlerException {
        int callerId                       = getCallerId(toHandle);
        int lineId                          = HandlerUtils.getOptionalInteger(toHandle, "lineId");
        int systemId                        = HandlerUtils.getOptionalInteger(toHandle, "systemId");
        int zoneId                          = HandlerUtils.getOptionalInteger(toHandle, "zoneId");
        int barId                           = HandlerUtils.getOptionalInteger(toHandle, "barId");
        int stationId                       = HandlerUtils.getOptionalInteger(toHandle, "stationId");
        int locationId                      = HandlerUtils.getOptionalInteger(toHandle, "locationId");
        String getRetiredStr                = HandlerUtils.getOptionalString(toHandle, "getRetired");
        boolean getRetired                  = !("true".equalsIgnoreCase(getRetiredStr));

        String startDate                    = HandlerUtils.getOptionalString(toHandle, "startDate");
        String endDate                      = HandlerUtils.getOptionalString(toHandle, "endDate");
        int calibrate                       = HandlerUtils.getOptionalBoolean(toHandle, "calibrate") ?  1 : 0;
        int customerId                      = HandlerUtils.getOptionalInteger(toHandle, "customerId");
        int paramsSet                       = 0, parameter = 0;
        String tableName                    = null;
        boolean isBrasstap                  = false;
        String selectLines                  = " SELECT l.id, l.lineIndex, l.product, "
                                            + " IFNULL((SELECT name FROM customBeerName WHERE location = lo.id AND product = l.product), (SELECT name FROM product WHERE id = l.product)), "
                                            + " l.system, l.bar, l.ouncesPoured, l.unit, l.status, l.statusChange, s.systemId, "
                                            + " l.station, l.kegLine, lo.easternOffset, l.local, l.advertise, ROUND(IF(l.qtyOnHand < 0, 0.0, l.qtyOnHand),2), l.cask, "
                                            + " IFNULL((SELECT kegSize from inventory WHERE location = lo.id AND product = l.product ORDER BY id LIMIT 1), 1984), l.lineNo, l.onDeck, "
                                            + " IFNULL((SELECT name FROM customBeerName WHERE location = lo.id AND product = l.onDeck), (SELECT name FROM product WHERE id = l.onDeck)) "
                                            + " FROM line as l LEFT JOIN bar b ON b.id = l.bar LEFT JOIN location lo ON lo.id = b.location "
                                            + " LEFT JOIN system s ON s.id = l.system ";

         if(locationId >0 || customerId > 0) {
            if(locationId > 0){
                isBrasstap = ReportHandler.checkBrasstapLocation(1, locationId, transconn);
            } else if(customerId > 0) {
                isBrasstap = ReportHandler.checkBrasstapLocation(2, customerId, transconn);
            }

        }

        if(isBrasstap){
            selectLines                     = " SELECT l.id, l.lineIndex, l.product, IFNULL((SELECT name FROM brasstapProducts WHERE usbnId=l.product LIMIT 1),p.name), l.system, l.bar, l.ouncesPoured, l.unit, l.status, " +
                                            " l.statusChange, s.systemId, l.station, l.kegLine, lo.easternOffset, l.local,l.advertise, l.qtyOnHand,l.cask,"
                                            + " (SELECT kegSize from inventory WHERE location= lo.id AND product=l.product ORDER BY id LIMIT 1), l.lineNo, l.onDeck, "
                                            + " IFNULL((SELECT name FROM customBeerName WHERE location = lo.id AND product = l.onDeck), (SELECT name FROM product WHERE id = l.onDeck)) "
                                            + " FROM line as l LEFT JOIN product AS p ON p.id = l.product LEFT JOIN bar b ON b.id = l.bar LEFT JOIN location lo ON lo.id = b.location "
                                            + " LEFT JOIN system s ON s.id = l.system ";
        }

        String volumeCondition             = "";

        if (lineId >= 0) {
            paramsSet++;
            parameter                       = lineId;
            tableName                       = "line";
            selectLines                     += " WHERE l.id = ? ";
            volumeCondition                 = " WHERE l.id = ? AND l.lastPoured > ? ";
        }

        if (systemId >= 0) {
            paramsSet++;
            parameter                       = systemId;
            tableName                       = "system";
            selectLines                     += " WHERE s.id = ? ";
            volumeCondition                 = " WHERE l.system = ? AND l.lastPoured > ? ";
        }

        if (zoneId >= 0) {
            paramsSet++;
            parameter                       = zoneId;
            tableName                       = "zone";
            selectLines                     += " WHERE b.zone = ? ";
            volumeCondition                 = " WHERE b.zone = ? AND l.lastPoured > ? ";
        }

        if (barId >= 0) {
            paramsSet++;
            parameter                       = barId;
            tableName                       = "bar";
            selectLines                     += " WHERE b.id = ? ";
            volumeCondition                 = " WHERE b.id = ? AND l.lastPoured > ? ";
        }

        if (stationId >= 0) {
            paramsSet++;
            parameter                       = stationId;
            tableName                       = "station";
            selectLines                     += " WHERE l.station = ? ";
            volumeCondition                 = " WHERE l.station = ? AND l.lastPoured > ? ";
        }

        if (locationId >= 0) {
            paramsSet++;
            parameter                       = locationId;
            tableName                       = "location";
            selectLines                     += " WHERE lo.id = ? ";
            volumeCondition                 = " WHERE b.location = ? AND l.lastPoured > ? ";
        }

        if (paramsSet > 1) {
            throw new HandlerException("Only one of the following parameters can be set for getCurrentLines: lineId systemId barId locationId");
        }

        PreparedStatement stmt              = null;
        ResultSet rs                        = null,rsDetails = null;

        try {
            if (!checkForeignKey(tableName, "id", parameter)) { throw new HandlerException("Foreign Key Not found : " + tableName + "-" + parameter); }
            HashMap<String, Double> lineVol= new HashMap<String, Double>();
            if(calibrate > 0 && startDate != null && endDate != null) {
                String selectLinesAssign    = "SELECT GROUP_CONCAT(l.id) FROM line l LEFT JOIN bar b ON b.id = l.bar " + volumeCondition;
                stmt                        = transconn.prepareStatement(selectLinesAssign);
                stmt.setInt(1, parameter);
                stmt.setString(2, newDateFormat.format(dateFormat.parse(startDate)));
                rs                          = stmt.executeQuery();
                if (rs.next()) {
                    String selectLineOunces = "SELECT CONCAT(l.system, ' : ', l.lineIndex), SUM(r.quantity) FROM reading r LEFT JOIN line l ON l.id = r.line " +
                                            " WHERE r.line IN (" + rs.getString(1) + ") AND r.date BETWEEN ? AND ? AND r.type = 0 GROUP BY r.line;";
                    stmt                    = transconn.prepareStatement(selectLineOunces);
                    stmt.setString(1, newDateFormat.format(dateFormat.parse(startDate)));
                    stmt.setString(2, newDateFormat.format(dateFormat.parse(endDate)));
                    rsDetails               = stmt.executeQuery();
                    while (rsDetails.next()) {
                        lineVol.put(rsDetails.getString(1), rsDetails.getDouble(2));
                    }
                }
            }

            selectLines                     += (getRetired ? "" : " AND l.status <> ?");
            stmt                            = transconn.prepareStatement(selectLines);
            stmt.setInt(1, parameter);
            if (!getRetired) { stmt.setString(2, "RETIRED"); }
            rs                              = stmt.executeQuery();
            while (rs.next()) {
                lineId                      = rs.getInt(1);
                Element lineE1              = toAppend.addElement("line");
                lineE1.addElement("lineId").addText(String.valueOf(lineId));
                lineE1.addElement("lineIndex").addText(String.valueOf(rs.getInt(2)));
                lineE1.addElement("productId").addText(String.valueOf(rs.getInt(3)));
                lineE1.addElement("productName").addText(HandlerUtils.nullToEmpty(rs.getString(4)));
                lineE1.addElement("systemId").addText(String.valueOf(rs.getInt(5)));
                lineE1.addElement("barId").addText(String.valueOf(rs.getInt(6)));
                lineE1.addElement("ouncesPoured").addText(String.valueOf(rs.getDouble(7)));
                lineE1.addElement("unit").addText(String.valueOf(rs.getDouble(8)));
                lineE1.addElement("status").addText(HandlerUtils.nullToEmpty(rs.getString(9)));
                lineE1.addElement("lastStatusChange").addText(HandlerUtils.nullToEmpty(rs.getString(10)));
                lineE1.addElement("systemIndex").addText(String.valueOf(rs.getInt(11)));
                lineE1.addElement("stationId").addText(String.valueOf(rs.getInt(12)));
                lineE1.addElement("kegLine").addText(String.valueOf(rs.getInt(13)));
                lineE1.addElement("easternOffset").addText(HandlerUtils.nullToEmpty(rs.getString(14)));
                lineE1.addElement("local").addText(String.valueOf(rs.getInt(15)));
                lineE1.addElement("advertise").addText(String.valueOf(rs.getInt(16)));
                lineE1.addElement("resetLevel").addText(String.valueOf(rs.getDouble(17)));
                lineE1.addElement("cask").addText(String.valueOf(rs.getInt(18)));
                lineE1.addElement("kegSize").addText(String.valueOf(rs.getInt(19)));
                lineE1.addElement("lineNo").addText(HandlerUtils.nullToEmpty(rs.getString(20)));
                lineE1.addElement("onDeck").addText(String.valueOf(rs.getInt(21)));
                lineE1.addElement("onDeckName").addText(HandlerUtils.nullToEmpty(rs.getString(22)));
                if(calibrate > 0) {
                    String systemIndex      = rs.getString(5) + " : " + rs.getString(2);
                    lineE1.addElement("volume").addText(String.valueOf(lineVol.containsKey(systemIndex) ? lineVol.get(systemIndex) : 0.0));
                }
            }
            logger.portalVisitDetail(callerId, "getCurrentLines", locationId, "DraftLine", 0,30, "getCurrentLines", transconn);
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        }catch(Exception e) {
            logger.debug(e.getMessage());
        }
        finally {
            close(rsDetails);
            close(rs);
            close(stmt);
        }
    }


       private void postQuadMenu(Element toHandle, Element toAppend) throws HandlerException {
           
           try {
               net.terakeet.soapware.handlers.SQLUpdateHandler.signHttpsCertificate();
               QuadGraphics qg = new QuadGraphics();
               //qg.postMenu();
           } catch (Exception e) {
               logger.dbError("JSON/URI error: " + e.getMessage());
           } finally {
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

       private void postBWWQuadMenu(int locationId, Element toAppend) throws HandlerException {

         //int locationId                      = HandlerUtils.getOptionalInteger(toHandle, "locationId");
         String select                      = "SELECT l.lineIndex, lo.id, lo.name, p.id, IF(cBN.name IS NULL, p.name, cBN.name), pD.abv ,pS.id,pS.name, pS1.id,pS1.name,"
                                            + "  IFNULL(IF(LENGTH(cBN.description) < 4 OR cBN.description IS NULL, pDE.description, cBN.description), pSD.description), pD.origin, pD.calorie,"
                                            + "  lo.store, pD.ibu,now() FROM line l LEFT JOIN bar b ON b.id = l.bar LEFT JOIN product p ON p.id = l.onDeck"
                                            + "  LEFT JOIN productDescription pD ON pD.product = p.id LEFT JOIN productDesc pDE ON pDE.product = p.id"
                                            + "  LEFT JOIN brewStyleMap bM ON bM.product = l.onDeck LEFT JOIN productSet pS ON pS.id = bM.style LEFT JOIN productSet pS1 ON pS1.id=bM.brewery"
                                            + "  LEFT JOIN beerStylesMap bSM ON bSM.productSet = bM.style LEFT JOIN beerStyles bS ON bS.id =  bSM.style"
                                            + "  LEFT JOIN styleLogo sL ON sL.style = pS.name LEFT JOIN productSetDescription pSD ON pSD.productSet = pS.id"
                                            + "  LEFT JOIN customBeerName cBN ON cBN.product = p.id AND cBN.location = b.location LEFT JOIN location lo ON lo.id=b.location  WHERE l.status = 'RUNNING'"
                                            + "  AND  l.cask = 0 AND p.id NOT IN(4311,10661) AND b.location = ? AND lo.store > 0 GROUP BY l.onDeck "
                                            + " ORDER BY l.lineIndex;";
         String regionalABV                 = "SELECT abv FROM regionalABV WHERE addrState = ? AND product = ?";
         PreparedStatement stmt             = null;
         ResultSet rs                       = null, customRS = null;
         try {
             QuadGraphics q                 = new QuadGraphics();
             JSONArray menuArray            = new JSONArray();
             int storeId                    = 0;
             String storeName               = "";             
             stmt                           = transconn.prepareStatement("SELECT addrState FROM location WHERE id=? AND store>0");
             stmt.setInt(1, locationId);
             rs                             = stmt.executeQuery();
             if (rs.next()) {
                 String addrState           = rs.getString(1);
                 stmt                       = transconn.prepareStatement(select);
                 stmt.setInt(1, locationId);
                 rs                         = stmt.executeQuery();
                 while (rs.next()) {
                     int lineId             = rs.getInt(1);
                     //int locationId             = rs.getInt(2);
                     String locationName    = HandlerUtils.nullToEmpty(rs.getString(3));
                     int productId          = rs.getInt(4);
                     String productName     = HandlerUtils.nullToEmpty(rs.getString(5));
                     double abv             = rs.getDouble(6);
                     
                     stmt                   = transconn.prepareStatement(regionalABV);
                     stmt.setString(1, addrState);
                     stmt.setInt(2, productId);
                     customRS               = stmt.executeQuery();
                     if (customRS.next()) {
                         abv                = customRS.getDouble(1);
                     }
                     if (addrState.equalsIgnoreCase("ut")) {
                         abv                = 4.0;
                     }
                     
                     int style              = rs.getInt(7);
                     String styleName       = HandlerUtils.nullToEmpty(rs.getString(8));
                     int brewId             = rs.getInt(9);
                     String brewName        = HandlerUtils.nullToEmpty(rs.getString(10));
                     String description     = HandlerUtils.nullToEmpty(rs.getString(11));
                     String origin          = HandlerUtils.nullToEmpty(rs.getString(12));
                     int calorie            = rs.getInt(13);                 
                     storeId                = rs.getInt(14);
                     int ibu                = rs.getInt(15);
                     String date            =  HandlerUtils.nullToEmpty(rs.getString(16));
                     storeName              = locationName;
                     if(locationName.length() >60){
                         locationName       = locationName.substring(0, 59);
                     }
                     if(brewName.length() >60){
                         brewName           = brewName.substring(0, 59);
                     }
                     if(styleName.length() >60){
                         styleName          = styleName.substring(0, 59);
                     }
                     if(description.length() >1000){
                         description        = description.substring(0, 999);
                     }
                     JSONObject menu        = new JSONObject();
                     menu.put("locationId", storeId);
                     menu.put("locationName", locationName);
                     menu.put("beerlineid", lineId+1);
                     menu.put("beername", productName.toUpperCase());
                     menu.put("beerId", productId);
                     menu.put("ABV", abv);
                     menu.put("IBU", ibu);
                     menu.put("calories", calorie);
                     menu.put("origin", origin);
                     menu.put("segment", "Craft");
                     menu.put("brewery", brewName);
                     menu.put("breweryId", brewId);
                     menu.put("styleName", styleName);
                     menu.put("styleId", style);
                     menu.put("description", description);
                    //menu.put("CreatedDateTime", date);
                    //menu.put("UpdatedDateTime", date);
                     
                     menuArray.put(menu);
                     //logger.debug(""+menu.toString());
                     JSONArray menuArrayOne           = new JSONArray();
                     menuArrayOne.put(menu);
                     /*if(productId == 2235){
                    logger.debug("Product:"+ productId +" beerName:"+ productName);
                    logger.debug(menuArrayOne.toString());
                    q.postMenu(menuArrayOne.toString(),logger);

                }*/
                 }
                 JSONObject lineObj         = new JSONObject();
                 lineObj.put("line", menuArray);
                 JSONObject mainObj = new JSONObject();
                 mainObj.put("DraftLineChanges", lineObj);
                 //logger.debug("Menu:"+menuArray.toString());
                 if(storeId > 0){
                     logger.debug("StoreId:"+ storeId +" storeName:"+ storeName);
                     q.postMenu(menuArray.toString(),logger);
                 }
             }
         } catch (SQLException sqle) {
             logger.dbError("Database error in getCountryState: " + sqle.getMessage());
             throw new HandlerException(sqle);
         } catch(Exception je){
             logger.debug(je.getMessage());
             
         }finally {
             close(customRS);
             close(rs);
             close(stmt);
         }
     }


    private void onDeckGoLive(Element toHandle, Element toAppend) throws HandlerException {

        String selectExisting               = "SELECT l.lineIndex, l.product, l.system, system.systemId, l.bar, l.station, " +
                                            " l.status, l.unit, l.local, l.advertise, l.qtyOnHand, l.cask, l.lineNo, " +
                                            " (SELECT kegSize from inventory WHERE location= system.location AND product=l.product ORDER BY id LIMIT 1), l.onDeck " +
                                            " FROM line AS l LEFT JOIN system ON l.system = system.id " +
                                            " WHERE l.id=? AND system.location=? ";
        String updateStatus                 = "UPDATE line SET status = 'RETIRED' WHERE id= ?";
        String insertDuplicate              = "INSERT INTO line (lineIndex, product, onDeck, ouncesPoured, system, bar, station, status, unit, lineNo, statusChange) " +
                                            " VALUES (?, ?, ?, -1.00, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP) ";

        int locationId                      = HandlerUtils.getRequiredInteger(toHandle, "locationId");
        int lineId                          = HandlerUtils.getRequiredInteger(toHandle, "lineId");

        PreparedStatement stmt              = null;
        ResultSet rs                        = null;

        try {
            stmt                            = transconn.prepareStatement(selectExisting);
            stmt.setInt(1, lineId);
            stmt.setInt(2, locationId);
            rs                              = stmt.executeQuery();
            if (rs.next()) {
                int lineIndex               = rs.getInt(1);
                int product                 = rs.getInt(2);
                int system                  = rs.getInt(3);
                int systemID                = rs.getInt(4);
                int barToUse                = rs.getInt(5);
                int stationToUse            = rs.getInt(6);
                String status               = rs.getString(7);
                double unit                 = rs.getDouble(8);
                int existingLocal           = rs.getInt(9);
                int existingAd              = rs.getInt(10);
                double existingRLevel       = rs.getDouble(11);
                int existingCask            = rs.getInt(12);
                String oldLineNo            = rs.getString(13);
                int existingKegSize         = rs.getInt(14);
                int onDeck                  = rs.getInt(15);
                
                //logger.debug("Updating status to " + newStatus);
                stmt                        = transconn.prepareStatement(updateStatus);
                stmt.setInt(1, lineId);
                stmt.executeUpdate();

                //Create a new line
                stmt                        = transconn.prepareStatement(insertDuplicate);
                stmt.setInt(1, lineIndex);
                stmt.setInt(2, onDeck);
                stmt.setInt(3, onDeck);
                stmt.setInt(4, system);
                stmt.setInt(5, barToUse);
                stmt.setInt(6, stationToUse);
                stmt.setString(7, status);
                stmt.setDouble(8, unit);
                stmt.setString(9, oldLineNo);
                stmt.executeUpdate();
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error in readingDateFixer: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(rs);
            close(stmt);
        }
    }

    private void sendToPrinter(Element toHandle, Element toAppend) throws HandlerException {
        int locationId                      = HandlerUtils.getRequiredInteger(toHandle, "locationId");
        postBWWQuadMenu(locationId, toAppend);
    }


    private void pluCopyOver(Element toHandle, Element toAppend) throws HandlerException {

        int customerId                      = HandlerUtils.getOptionalInteger(toHandle, "customerId");
        int groupId                         = HandlerUtils.getOptionalInteger(toHandle, "groupId");
        int parameter                       = -1, paramsSet = 0;
        
        String conditionString              = " WHERE c.id = ? ";
        if (customerId >= 0) {
            parameter                       = customerId;
            conditionString                 = " WHERE c.id = ? ";
            paramsSet++;
        }
        if (groupId >= 0) {
            parameter                       = groupId;
            conditionString                 = " WHERE c.groupId = ? ";
            paramsSet++;
        }
        if (paramsSet != 1) {
            throw new HandlerException("Exactly only one of the following must be set: customerId groupId");
        }
        String selectMissingPLU             = "SELECT lo.id, p.id FROM line l LEFT JOIN system s ON s.id = l.system LEFT JOIN location lo ON lo.id = s.location " 
                                            + " LEFT JOIN product p ON p.id = l.product LEFT JOIN (SELECT DISTINCT lo.id, b.id bev, i.product FROM beverage b "
                                            + " LEFT JOIN ingredient i ON i.beverage = b.id LEFT JOIN location lo ON lo.id = b.location LEFT JOIN customer c ON c.id = lo.customer "
                                            + conditionString + " ) AS b ON b.id = lo.id AND b.product = l.product LEFT JOIN customer c ON c.id = lo.customer " + conditionString
                                            + " AND l.status = 'RUNNING' AND l.product NOT IN (4311, 4864, 10661) AND b.bev IS NULL ORDER BY p.name, c.name, lo.name; ";
        String selectExistingPLU            = "SELECT DISTINCT b.name, b.plu, i.ounces FROM beverage b LEFT JOIN ingredient i ON i.beverage = b.id LEFT JOIN location l ON l.id = b.location "
                                            + " LEFT JOIN customer c ON c.id = l.customer " + conditionString + " AND i.product = ? ORDER BY l.name, b.name, b.ounces ";
        
        PreparedStatement stmt              = null;
        ResultSet rs                        = null, innerRS = null, beverageRS = null;

        try {
            stmt                            = transconn.prepareStatement(selectMissingPLU);
            stmt.setInt(1, parameter);
            stmt.setInt(2, parameter);
            rs                              = stmt.executeQuery();
            while (rs.next()) {
                int locationId              = rs.getInt(1);
                int product                 = rs.getInt(2);
                stmt                        = transconn.prepareStatement(selectExistingPLU);
                stmt.setInt(1, parameter);
                stmt.setInt(2, product);
                innerRS                     = stmt.executeQuery();
                while (innerRS.next()) {
                    stmt                    = transconn.prepareStatement("INSERT INTO beverage (name, location, plu, ounces, simple, pType) VALUES (?,?,?,?,1,1)");
                    stmt.setString(1, innerRS.getString(1));
                    stmt.setInt(2, locationId);
                    stmt.setString(3, innerRS.getString(2));
                    stmt.setDouble(4, innerRS.getDouble(3));
                    stmt.executeUpdate();

                    stmt                    = transconn.prepareStatement("SELECT LAST_INSERT_ID()");
                    beverageRS              = stmt.executeQuery();
                    if (beverageRS.next()) {
                        stmt                = transconn.prepareStatement("INSERT INTO ingredient (beverage, product, ounces) VALUES (?,?,?)");
                        stmt.setInt(1, beverageRS.getInt(1));
                        stmt.setInt(2, product);
                        stmt.setDouble(3, innerRS.getDouble(3));
                        stmt.executeUpdate();
                    }
                }
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error in pluCopyOver: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(beverageRS);
            close(innerRS);
            close(rs);
            close(stmt);
        }
    }
    
    
    private void clearPLUBeverages(Element toHandle, Element toAppend) throws HandlerException {
        PreparedStatement stmt              = null;
        ResultSet rs                        = null;
        int location                        = HandlerUtils.getRequiredInteger(toHandle, "locationId");        
        String selectBev                    = "SELECT id FROM beverage WHERE location = ?";
        String deleteIng                    = "DELETE FROM ingredient WHERE beverage=?";
        String deleteBev                    = "DELETE FROM beverage WHERE id=?";

        try {
            logger.debug("Clearing PLU Bevrages for Location#"+location);
            stmt = transconn.prepareStatement(selectBev);
            stmt.setInt(1, location);
            rs = stmt.executeQuery();
            while (rs.next()) {
                int beverage = rs.getInt(1);
                stmt = transconn.prepareStatement(deleteIng);
                stmt.setInt(1, beverage);
                stmt.executeUpdate();
                
                stmt = transconn.prepareStatement(deleteBev);
                stmt.setInt(1, beverage);
                stmt.executeUpdate();
            } 
        } catch (SQLException sqle) {
            logger.dbError("Database error in clearPLUBeverages: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(rs);
            close(stmt);            
        }
    }
    
    private void sendProductApprovalEmail(int productId) throws HandlerException {

        PreparedStatement stmt              = null;
        ResultSet rs                        = null;
        String selectData                   = "SELECT l.product, u.name,u.email, p.name,b.name,s.name,pD.origin,pD.abv,pD.ibu FROM productChangeLog l  LEFT JOIN user u ON u.id=l.user LEFT JOIN userMap uM ON uM.user=u.id "
                                            + " LEFT JOIN location lo ON lo.id=uM.location LEFT JOIN product p ON p.id=l.product LEFT JOIN productDescription pD on pD.product=p.id LEFT JOIN  brewStyleMap bSM ON bSM.product=p.id "
                                            + "  LEFT JOIN productSet b ON b.id=bSM.brewery LEFT JOIN productSet s ON s.id=bSM.style  where l.product=? AND lo.store >0 AND l.user>0 AND productType=1 AND l.type=1 ;";


        try {
            
                stmt                        = transconn.prepareStatement(selectData);
                stmt.setInt(1, productId);
                rs                          = stmt.executeQuery();
                if (rs.next()) {
                    StringBuilder requestNotification
                                                = new StringBuilder();
                    requestNotification.append("<tr align=left><td colspan=4>Hello " + rs.getString(2) + ",");                    
                    requestNotification.append("</td></tr>");
                    requestNotification.append("<tr align=center valign=middle><td height=20 colspan=4>&nbsp;</td></tr>");
                    requestNotification.append("<tr align=left valign=middle><td  colspan=4>Your new beer request has been approved. Below are the details.</td></tr>");
                    requestNotification.append("<tr align=center valign=middle><td height=20 colspan=4>&nbsp;</td></tr>");
                    requestNotification.append("<tr align=left valign=middle><td  colspan=2>Beer Name</td><td  colspan=2>: "+HandlerUtils.nullToEmpty(rs.getString(4))+"</td></tr>");
                    requestNotification.append("<tr align=left valign=middle><td  colspan=2>Brewery</td><td  colspan=2>: "+HandlerUtils.nullToEmpty(rs.getString(5))+"</td></tr>");
                    requestNotification.append("<tr align=left valign=middle><td  colspan=2>Style</td><td  colspan=2>: "+HandlerUtils.nullToEmpty(rs.getString(6))+"</td></tr>");
                    requestNotification.append("<tr align=left valign=middle><td  colspan=2>Origin</td><td  colspan=2>: "+HandlerUtils.nullToEmpty(rs.getString(7))+"</td></tr>");
                    requestNotification.append("<tr align=left valign=middle><td  colspan=2>ABV</td><td  colspan=2>: "+HandlerUtils.nullToEmpty(rs.getString(8))+"</td></tr>");
                    requestNotification.append("<tr align=left valign=middle><td  colspan=2>IBU</td><td  colspan=2>: "+HandlerUtils.nullToEmpty(rs.getString(9))+"</td></tr>");
                    requestNotification.append("<tr align=center valign=middle><td height=35 colspan=4>&nbsp;</td></tr>");
                    requestNotification.append("<tr align=justify><td colspan=4>Thank You,</td></tr>");
                    requestNotification.append("<tr align=justify><td colspan=4>US Beverage Net Support</td></tr>");
                    requestNotification.append("<tr align=center valign=middle><td height=35 colspan=4>&nbsp;</td></tr>");
                    requestNotification.append("<tr align=justify><td colspan=4><strong>This email was automatically generated; please do not reply.</strong></td></tr><tr><td colspan=4>&nbsp;</td></tr>");
                    String userEmail = rs.getString(3);
                    if(userEmail!=null && !userEmail.equals("")){
                        sendMail("", rs.getString(2), userEmail, "support@beerboard.com", "USBN New Beer Approval Notification", "sendMail", requestNotification, false);
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
    
    private void sendProductApprovalEmail(int oldProduct, int productId) throws HandlerException {

        PreparedStatement stmt              = null;
        ResultSet rs                        = null, rsDetail= null;
        String selectLineCount              ="SELECT count(id)+(SELECT count(id) FROM comingSoonProducts WHERE product=?)  FROM line WHERE product=?;";
        String selectOldData                = "SELECT l.product, u.name,u.email, p.name,b.name,s.name,pD.origin,pD.abv,pD.ibu FROM productChangeLog l  LEFT JOIN user u ON u.id=l.user LEFT JOIN userMap uM ON uM.user=u.id "
                                            + " LEFT JOIN location lo ON lo.id=uM.location LEFT JOIN product p ON p.id=l.product LEFT JOIN productDescription pD on pD.product=p.id LEFT JOIN  brewStyleMap bSM ON bSM.product=p.id "
                                            + "  LEFT JOIN productSet b ON b.id=bSM.brewery LEFT JOIN productSet s ON s.id=bSM.style  where l.product=? AND lo.store >0 AND l.user>0 AND productType=1 AND l.type=1 ;";
        String selectData                   = "SELECT  p.name,b.name,s.name,pD.origin,pD.abv,pD.ibu FROM product p   LEFT JOIN productDescription pD on pD.product=p.id LEFT JOIN  brewStyleMap bSM ON bSM.product=p.id "
                                            + "  LEFT JOIN productSet b ON b.id=bSM.brewery LEFT JOIN productSet s ON s.id=bSM.style  where p.id=? ;";


        try {
                stmt                        = transconn.prepareStatement(selectLineCount);
                stmt.setInt(1, oldProduct);
                stmt.setInt(2, oldProduct);
                rs                          = stmt.executeQuery();
                if (rs.next()) {
                    if(rs.getInt(1)>0){
                        stmt                = transconn.prepareStatement(selectOldData);
                        stmt.setInt(1, oldProduct);
                        rs                  = stmt.executeQuery();
                        if (rs.next()) {
                             stmt           = transconn.prepareStatement(selectData);
                             stmt.setInt(1, productId);
                             rsDetail       = stmt.executeQuery();
                              StringBuilder requestNotification
                                            = new StringBuilder();
                             if (rsDetail.next()) {
                                 requestNotification.append("<tr align=left><td colspan=4>Hello " + rs.getString(2) + ",");                    
                                requestNotification.append("</td></tr>");
                                requestNotification.append("<tr align=center valign=middle><td height=20 colspan=4>&nbsp;</td></tr>");
                                requestNotification.append("<tr align=left valign=middle><td  colspan=4>Your new beer request has been replaced with one of our exiting beers. Below are the details.</td></tr>");
                                requestNotification.append("<tr align=center valign=middle><td height=20 colspan=4>&nbsp;</td></tr>");
                                requestNotification.append("<tr align=left valign=middle><td  colspan=2></td><td> Your Beer Request   </td><td> Replaced with  </td></tr>");
                                requestNotification.append("<tr align=left valign=middle><td  colspan=2>Beer Name</td><td  >: "+HandlerUtils.nullToEmpty(rsDetail.getString(1))+"</td><td  >: "+HandlerUtils.nullToEmpty(rs.getString(4))+"</td></tr>");
                                requestNotification.append("<tr align=left valign=middle><td  colspan=2>Brewery</td><td  >: "+HandlerUtils.nullToEmpty(rsDetail.getString(2))+"</td><td  >: "+HandlerUtils.nullToEmpty(rs.getString(5))+"</td></tr>");
                                requestNotification.append("<tr align=left valign=middle><td  colspan=2>Style</td><td  >: "+HandlerUtils.nullToEmpty(rsDetail.getString(3))+"</td><td  >: "+HandlerUtils.nullToEmpty(rs.getString(6))+"</td></tr>");
                                requestNotification.append("<tr align=left valign=middle><td  colspan=2>Origin</td><td  >: "+HandlerUtils.nullToEmpty(rsDetail.getString(4))+"</td><td  >: "+HandlerUtils.nullToEmpty(rs.getString(7))+"</td></tr>");
                                requestNotification.append("<tr align=left valign=middle><td  colspan=2>ABV</td><td  >: "+HandlerUtils.nullToEmpty(rsDetail.getString(5))+"</td><td  >: "+HandlerUtils.nullToEmpty(rs.getString(8))+"</td></tr>");
                                requestNotification.append("<tr align=left valign=middle><td  colspan=2>IBU</td><td  >: "+HandlerUtils.nullToEmpty(rsDetail.getString(6))+"</td><td  >: "+HandlerUtils.nullToEmpty(rs.getString(9))+"</td></tr>");
                                requestNotification.append("<tr align=center valign=middle><td height=35 colspan=4>&nbsp;</td></tr>");
                                requestNotification.append("<tr align=justify><td colspan=4>Thank You,</td></tr>");
                                requestNotification.append("<tr align=justify><td colspan=4>US Beverage Net Support</td></tr>");
                                requestNotification.append("<tr align=center valign=middle><td height=35 colspan=4>&nbsp;</td></tr>");
                                requestNotification.append("<tr align=justify><td colspan=4><strong>This email was automatically generated; please do not reply.</strong></td></tr><tr><td colspan=4>&nbsp;</td></tr>");
                             }
                             String userEmail = rs.getString(3);
                             if(userEmail!=null && !userEmail.equals("")){
                                 sendMail("", rs.getString(2), userEmail, "support@beerboard.com", "USBN New Beer Approval Notification", "sendMail", requestNotification, false);
                             }
                        }
                    }
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
}




