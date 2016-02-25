    /*
 * SQLGenericHandler.java
 *
 * Created on July 17, 2007, 10:33 AM
 *
 */
package net.terakeet.soapware.handlers;

import net.terakeet.soapware.*;
import net.terakeet.soapware.handlers.auper.*;
import net.terakeet.soapware.handlers.report.ProductMap;
import net.terakeet.soapware.security.*;
import net.terakeet.usbn.WebPermission;
import net.terakeet.util.MidwareLogger;
import net.terakeet.util.TemplatedMessage;
import net.terakeet.util.MailException;
import org.apache.log4j.Logger;
import java.sql.Date;
import java.sql.Time;
import org.dom4j.Element;
import java.sql.*;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.CallableStatement;
import java.lang.String;
import java.util.Iterator;
import java.util.Set;
import java.util.HashSet;
import java.util.Map;
import java.util.HashMap;
import java.util.Properties;
import java.util.*;
import java.text.*;
//import javax.mail.*;
//import javax.mail.internet.MimeMessage;
//import javax.mail.internet.InternetAddress;

public class SQLGenericHandler implements Handler {

    private MidwareLogger logger;
    private static final String transConnName = "auper";
    private RegisteredConnection transconn;
    private SecureSession ss;

    /**
     * Creates a new instance of SQLGenericHandler
     */
    public SQLGenericHandler() throws HandlerException {
        HandlerUtils.initializeClientKeyManager();
        logger = new MidwareLogger(SQLGenericHandler.class.getName());
        transconn = null;
    }

    public void handle(Element toHandle, Element toAppend) throws HandlerException {

        String function = toHandle.getName();
        String responseNamespace = (String) SOAPMessage.getURIMap().get("tkmsg");

        String clientKey = HandlerUtils.getOptionalString(toHandle, "clientKey");
        ss = ClientKeyManager.getSession(clientKey);

        logger = new MidwareLogger(SQLGenericHandler.class.getName(), function);
        logger.debug("SQLGenericHandler processing method: " + function);
        logger.xml("request: " + toHandle.asXML());

        transconn = DatabaseConnectionManager.getNewConnection(transConnName, function + " (SQLGenericHandler)");

        try {
            // All methods require an admin client key
            if (ss.getLocation() == 0 && ss.getClientId() == 1 && ss.getSecurityLevel().canAdmin()) {
                if ("getGenericResult".equals(function)) {
                    getGenericResult(toHandle, responseFor(function, toAppend));
                } else if ("authTestUser".equals(function)) {
                    authTestUser(toHandle, responseFor(function, toAppend));
                } else if ("getProducts1".equals(function)) {
                    getProducts1(toHandle, responseFor(function, toAppend));
                } else if ("getDraftInventory".equals(function)) {
                    getDraftInventory(toHandle, responseFor(function, toAppend));
                } else if ("updateTestInventory".equals(function)) {
                    updateTestInventory(toHandle, responseFor(function, toAppend));
                } else if ("updateBottleInventory".equals(function)) {
                    updateBottleInventory(toHandle, responseFor(function, toAppend));
                } else if ("getTestInventory".equals(function)) {
                    getTestInventory(toHandle, responseFor(function, toAppend));
                } else if ("getTestInventory1".equals(function)) {
                    getTestInventory1(toHandle, responseFor(function, toAppend));
                } else if ("getGenericResult1".equals(function)) {
                    getGenericResult1(toHandle, responseFor(function, toAppend));
                } else if ("addTestUser".equals(function)) {
                    addTestUser(toHandle, responseFor(function, toAppend));
                } else if ("updateTestUser".equals(function)) {
                    updateTestUser(toHandle, responseFor(function, toAppend));
                } else if ("getLocationErrorDetail".equals(function)) {
                    getLocationErrorDetail(toHandle, responseFor(function, toAppend));
                } else if ("getGenericResult2".equals(function)) {
                    getGenericResult2(toHandle, responseFor(function, toAppend));
                } else if ("setBottleInv".equals(function)) {
                    setBottleInv(toHandle, responseFor(function, toAppend));
                } else if ("emailUser".equals(function)) {
                    emailUser(toHandle, responseFor(function, toAppend));
                } else if ("getGenericResult3".equals(function)) {
                    getGenericResult3(toHandle, responseFor(function, toAppend));
                } else if ("getTestPurchaseDetail".equals(function)) {
                    getTestPurchaseDetail(toHandle, responseFor(function, toAppend));
                } else if ("getGenericResult4".equals(function)) {
                    getGenericResult4(toHandle, responseFor(function, toAppend));
                } else if ("getGenericResult5".equals(function)) {
                    getGenericResult5(toHandle, responseFor(function, toAppend));
                } else if ("addReports".equals(function)) {
                    addReports(toHandle, responseFor(function, toAppend));
                } else if ("addAlerts".equals(function)) {
                    addAlerts(toHandle, responseFor(function, toAppend));
                } else if ("getVarianceTextAlerts".equals(function)) {
                    getVarianceTextAlerts(toHandle, responseFor(function, toAppend));
                } else if ("getLowStockTextAlerts".equals(function)) {
                    getLowStockTextAlerts(toHandle, responseFor(function, toAppend));
                } else if ("getConcessionVarianceTextAlerts".equals(function)) {
                    getConcessionVarianceTextAlerts(toHandle, responseFor(function, toAppend));
                } else if ("getConcessionLowStockTextAlerts".equals(function)) {
                    getConcessionLowStockTextAlerts(toHandle, responseFor(function, toAppend));
                } else if ("getReports".equals(function)) {
                    getReports(toHandle, responseFor(function, toAppend));
                } else if ("getZones".equals(function)) {
                    getZones(toHandle, responseFor(function, toAppend));
                } else if ("getStations".equals(function)) {
                    getStations(toHandle, responseFor(function, toAppend));
                } else if ("getCoolers".equals(function)) {
                    getCoolers(toHandle, responseFor(function, toAppend));
                } else if ("getKegLines".equals(function)) {
                    getKegLines(toHandle, responseFor(function, toAppend));
                } else if ("getUnits".equals(function)) {
                    getUnits(toHandle, responseFor(function, toAppend));
                } else if ("updateUnitSettings".equals(function)) {
                    updateUnitSettings(toHandle, responseFor(function, toAppend));
                } else if ("addLocationReports".equals(function)) {
                    addLocationReports(toHandle, responseFor(function, toAppend));
                } else if ("getLocationReports".equals(function)) {
                    getLocationReports(toHandle, responseFor(function, toAppend));
                } else if ("addSMSAlerts".equals(function)) {
                    addSMSAlerts(toHandle, responseFor(function, toAppend));
                } else if ("updateSMSAlerts".equals(function)) {
                    updateSMSAlerts(toHandle, responseFor(function, toAppend));
                } else if ("getLocationSMSAlerts".equals(function)) {
                    getLocationSMSAlerts(toHandle, responseFor(function, toAppend));
                } else if ("getLocationTextAlerts".equals(function)) {
                    getLocationTextAlerts(toHandle, responseFor(function, toAppend));
                } else if ("checkRewards".equals(function)) {
                    checkRewards(toHandle, responseFor(function, toAppend));
                } else if ("getBeverages1".equals(function)) {
                    getBeverages1(toHandle, responseFor(function, toAppend));
                } else if ("getComplexPlu".equals(function)) {
                    getComplexPlu(toHandle, responseFor(function, toAppend));
                } else if ("deleteBeverageSize1".equals(function)) {
                    deleteBeverageSize1(toHandle, responseFor(function, toAppend));
                } else if ("copyTestBeverage".equals(function)) {
                    copyTestBeverage(toHandle, responseFor(function, toAppend));
                } else if ("getTestNormalUsers".equals(function)) {
                    getTestNormalUsers(toHandle, responseFor(function, toAppend));
                } else if ("addTestInventory".equals(function)) {
                    addTestInventory(toHandle, responseFor(function, toAppend));
                } else if ("addBeverage1".equals(function)) {
                    addBeverage1(toHandle, responseFor(function, toAppend));
                } else if ("addShifts".equals(function)) {
                    addShifts(toHandle, responseFor(function, toAppend));
                } else if ("setBrixRatio".equals(function)) {
                    setBrixRatio(toHandle, responseFor(function, toAppend));
                } else if ("getLocationVariance".equals(function)) {
                    getLocationVariance(toHandle, responseFor(function, toAppend));
                } else if ("addProduct1".equals(function)) {
                    addProduct1(toHandle, responseFor(function, toAppend));
                } else if ("addBeverageSize1".equals(function)) {
                    addBeverageSize1(toHandle, responseFor(function, toAppend));
                } else if ("importBeverages1".equals(function)) {
                    importBeverages1(toHandle, responseFor(function, toAppend));
                } else if ("getRecentlyReceivedProducts".equals(function)) {
                    getRecentlyReceivedProducts(toHandle, responseFor(function, toAppend));
                } else if ("getLocationData".equals(function)) {
                    getLocationData(toHandle, responseFor(function, toAppend));
                } else if ("sendAlerts".equals(function)) {
                    sendAlerts(toHandle, responseFor(function, toAppend));
                } else if ("sendLocationStatusAlerts".equals(function)) {
                    sendLocationStatusAlerts(toHandle, responseFor(function, toAppend));
                } else if ("updateProduct1".equals(function)) {
                    updateProduct1(toHandle, responseFor(function, toAppend));
                } else if ("getBeverageSizes1".equals(function)) {
                    getBeverageSizes1(toHandle, responseFor(function, toAppend));
                } else if ("addVarianceValue".equals(function)) {
                    addVarianceValue(toHandle, responseFor(function, toAppend));
                } else if ("adminChangeProductType".equals(function)) {
                    adminChangeProductType(toHandle, responseFor(function, toAppend));
                } else if ("getBottleSize".equals(function)) {
                    getBottleSize(toHandle, responseFor(function, toAppend));
                } else if ("getKegSize".equals(function)) {
                    getKegSize(toHandle, responseFor(function, toAppend));
                } else if ("getShifts".equals(function)) {
                    getShifts(toHandle, responseFor(function, toAppend));
                } else if ("disableRewards".equals(function)) {
                    disableRewards(toHandle, responseFor(function, toAppend));
                } else if ("updateTestBeveragePlu".equals(function)) {
                    updateTestBeveragePlu(toHandle, responseFor(function, toAppend));
                } else if ("getLocationId".equals(function)) {
                    getLocationId(toHandle, responseFor(function, toAppend));
                } else if ("deleteTestBeveragePlu".equals(function)) {
                    deleteTestBeveragePlu(toHandle, responseFor(function, toAppend));
                } else if ("getReportRequest".equals(function)) {
                    getReportRequest(toHandle, responseFor(function, toAppend));
                } else if ("requestReports".equals(function)) {
                    requestReports(toHandle, responseFor(function, toAppend));
                } else if ("authReportRequest".equals(function)) {
                    authReportRequest(toHandle, responseFor(function, toAppend));
                } else if ("disableReportRequest".equals(function)) {
                    disableReportRequest(toHandle, responseFor(function, toAppend));
                } else if ("manualInventoryUpload".equals(function)) {
                    manualInventoryUpload(toHandle, responseFor(function, toAppend));
                } else if ("getTestAdminUsers".equals(function)) {
                    getTestAdminUsers(toHandle, responseFor(function, toAppend));
                } else if ("getBottleInv".equals(function)) {
                    getBottleInv(toHandle, responseFor(function, toAppend));
                } else if ("getInventoryDates".equals(function)) {
                    getInventoryDates(toHandle, responseFor(function, toAppend));
                } else if ("getEventDates".equals(function)) {
                    getEventDates(toHandle, responseFor(function, toAppend));
                } else if ("getSpecialCateredEvent".equals(function)) {
                    getSpecialCateredEvent(toHandle, responseFor(function, toAppend));
                } else if ("getStandUnitCount".equals(function)) {
                    getStandUnitCount(toHandle, responseFor(function, toAppend));
                } else if ("getUnitCount".equals(function)) {
                    getUnitCount(toHandle, responseFor(function, toAppend));
                } else if ("updateUnitCount".equals(function)) {
                    updateUnitCount(toHandle, responseFor(function, toAppend));
                } else if ("getBottleInventoryData".equals(function)) {
                    getBottleInventoryData(toHandle, responseFor(function, toAppend));
                } else if ("getYesterdayVariance".equals(function)) {
                    getYesterdayVariance(toHandle, responseFor(function, toAppend));
                } else if ("readingDateFixer".equals(function)) {
                    readingDateFixer(toHandle, responseFor(function, toAppend));
                } else if ("getLocationLogs".equals(function)) {
                    getLocationLogs(toHandle, responseFor(function, toAppend));
                } else if ("datedLocationLogs".equals(function)) {
                    datedLocationLogs(toHandle, responseFor(function, toAppend));
                } else if ("addLocationLogs".equals(function)) {
                    addLocationLogs(toHandle, responseFor(function, toAppend));
                } else if ("updateLocationLogs".equals(function)) {
                    updateLocationLogs(toHandle, responseFor(function, toAppend));
                } else if ("addTextAlertLogs".equals(function)) {
                    addTextAlertLogs(toHandle, responseFor(function, toAppend));
                } else if ("deleteLocationLogs".equals(function)) {
                    deleteLocationLogs(toHandle, responseFor(function, toAppend));
                } else if ("getLogCategory".equals(function)) {
                    getLogCategory(toHandle, responseFor(function, toAppend));
                } else if ("promoteITUser".equals(function)) {
                    promoteITUser(toHandle, responseFor(function, toAppend));
                } else if ("demoteITAdmin".equals(function)) {
                    demoteITAdmin(toHandle, responseFor(function, toAppend));
                } else if ("getCustomerPoints".equals(function)) {
                    getCustomerPoints(toHandle, responseFor(function, toAppend));
                } else if ("getCustomerVariance".equals(function)) {
                    getCustomerVariance(toHandle, responseFor(function, toAppend));
                } else if ("getUserLocations".equals(function)) {
                    getUserLocations(toHandle, responseFor(function, toAppend));
                } else if ("getUserInfo".equals(function)) {
                    getUserInfo(toHandle, responseFor(function, toAppend));
                } else if ("updateCoolers".equals(function)) {
                    updateCoolers(toHandle, responseFor(function, toAppend));
                } else if ("deleteKegLines".equals(function)) {
                    deleteKegLines(toHandle, responseFor(function, toAppend));
                } else if ("addKegLines".equals(function)) {
                    addKegLines(toHandle, responseFor(function, toAppend));
                } else if ("getConcessionProductMap".equals(function)) {
                    getConcessionProductMap(toHandle, responseFor(function, toAppend));
                } else if ("updateConcessionProductSupplier".equals(function)) {
                    updateConcessionProductSupplier(toHandle, responseFor(function, toAppend));
                } else if ("addConcessionProductSupplier".equals(function)) {
                    addConcessionProductSupplier(toHandle, responseFor(function, toAppend));
                } else if ("deleteConcessionProductSupplier".equals(function)) {
                    deleteConcessionProductSupplier(toHandle, responseFor(function, toAppend));
                } else if ("getEmailReportMaster".equals(function)) {
                    getEmailReportMaster(toHandle, responseFor(function, toAppend));
                } else if ("getEmailReportComponents".equals(function)) {
                    getEmailReportComponents(toHandle, responseFor(function, toAppend));
                } else if ("getEmailReportComponentsMap".equals(function)) {
                    getEmailReportComponentsMap(toHandle, responseFor(function, toAppend));
                } else if ("getEmailTimeTable".equals(function)) {
                    getEmailTimeTable(toHandle, responseFor(function, toAppend));
                } else if ("getEmailReportDurations".equals(function)) {
                    getEmailReportDurations(toHandle, responseFor(function, toAppend));
                } else if ("addEmailReportLog".equals(function)) {
                    addEmailReportLog(toHandle, responseFor(function, toAppend));
                } else if ("addEmailReportArchive".equals(function)) {
                    addEmailReportArchive(toHandle, responseFor(function, toAppend));
                } else if ("getEmailReportLogtype".equals(function)) {
                    getEmailReportLogtype(toHandle, responseFor(function, toAppend));
                } else if ("addUpdateDeleteProductGrouping".equals(function)) {
                    addUpdateDeleteProductGrouping(toHandle, responseFor(function, toAppend));
                } else if ("getGrouping".equals(function)) {
                    getGrouping(toHandle, responseFor(function, toAppend));
                } else if ("getProductGrouping".equals(function)) {
                    getProductGrouping(toHandle, responseFor(function, toAppend));
                } else if ("getGroupingForGroup".equals(function)) {
                    getGroupingForGroup(toHandle, responseFor(function, toAppend));
                }  else if ("addUpdateDeleteGroupingForGroup".equals(function)) {
                    addUpdateDeleteGroupingForGroup(toHandle, responseFor(function, toAppend));
                } else if ("getEmailReportLogs".equals(function)) {
                    getEmailReportLogs(toHandle, responseFor(function, toAppend));
                } else if ("queryRunner".equals(function)) {
                    queryRunner(toHandle, responseFor(function, toAppend));
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
                logger.midwareError("Non-handler exception thrown in ReportHandler: " + e.toString());
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
        c.close();
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

    private void addErrorDetail(Element toAppend, String message) {
        toAppend.addElement("error").addElement("detail").addText(message);
    }

    private int getCallerId(Element toHandle) throws HandlerException {
        return HandlerUtils.getRequiredInteger(HandlerUtils.getRequiredElement(toHandle, "caller"), "callerId");
    }

    private boolean checkForeignKey(String table, int value) throws SQLException, HandlerException {
        return checkForeignKey(table, "id", value);
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

    private void getLocationErrorDetail(Element toHandle, Element toAppend) throws HandlerException {

        int location = HandlerUtils.getRequiredInteger(toHandle, "locationId");

        PreparedStatement stmt = null;
        ResultSet rs = null;

        String selectLocation = "SELECT name FROM location WHERE id=?";
        String selectLocationError = "SELECT message, level, datetime FROM clientError WHERE location=? ORDER BY datetime DESC LIMIT 5";

        try {
            stmt = transconn.prepareStatement(selectLocation);
            stmt.setInt(1, location);
            rs = stmt.executeQuery();
            if (rs.next()) {
                toAppend.addElement("name").addText(HandlerUtils.nullToEmpty(rs.getString(1)));
            }

            stmt = transconn.prepareStatement(selectLocationError);
            stmt.setInt(1, location);
            rs = stmt.executeQuery();

            while (rs.next()) {
                Element line = toAppend.addElement("error");
                line.addElement("message").addText(rs.getString(1));
                line.addElement("level").addText(rs.getString(2));
                line.addElement("datetime").addText(rs.getString(3));
            }

        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.toString());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
            close(rs);
        }

    }

    /**     Web manager security levels:
     *  All users are associated with a customer
     *
     *  Each user has a Manager flag.
     *  A manager can access every location for that customer.  They can also set
     *  up subordinate accounts for their customer, and set other permissions.
     *
     * If a user is not a manager, his permissions are defined individually for
     * each location.  The possible location-level permissions are:
     *      No-access
     *      Read-only
     *      Supervisor
     *
     *  No-access:  This location isn't available at all to this user
     *  Read-only:  This user can view reports and inventory information, but can't make any changes
     *  Supervisor: This user can make changes, including inventory and draft lines.  He can
     *      also manage read-only accounts at the location.  He can see a list of all non-
     *      manager accounts across ALL locations for this customer, and give or remove
     *      read-only access to his location.
     *
     * As far as the web manager is concerned, it loads one location at a time, and needs to know
     * what permissions a user has at that location.  The web manager actually has a fourth permission,
     * called admin.  Admin is a special permission level for super users that are assoicated with
     * customer 0, the super-customer.    Admins can access all customers, and can set up new
     * customers and new locations.  So, this method needs to return a list of locations and permissions
     * that this user has.
     *
     * By convention, the usbn database uses the followng numbers for permissions:
     *    1 : admin
     *    3 : super-manager (customer level)
     *    5 : manager
     *    7 : read-only
     * See also: net.terakeet.usbn.WebPermssion
     */
    private void authTestUser(Element toHandle, Element toAppend)
            throws HandlerException {

        final int HQ_CUSTID = 10;
        final int ROOT_CUSTOMER = 0;
        //final String ADMIN_SECURITY_STRING = "1";
        //final String SUPERVISOR_SECURITY_STRING = "3";

        String username = HandlerUtils.getRequiredString(toHandle, "username");
        String password = HandlerUtils.getRequiredString(toHandle, "password");

        String checkRoot = "SELECT u.id, u.isManager, isITAdmin, u.lastCustomer, u.customer FROM user u " +
                " WHERE u.username = ? AND u.password = ? ";

        String selectNormal = "SELECT l.id, l.name, l.type, c.id, c.name, c.type, l.easternOffset, l.volAdjustment, m.securityLevel" +
                " FROM userMap m " +
                " LEFT JOIN location l ON m.location = l.id " +
                " LEFT JOIN customer c ON l.customer = c.id " +
                " WHERE m.user=? " +
                " ORDER BY m.securityLevel ASC, l.name ASC";

        String selectRoot = "SELECT l.id, l.name, l.type, c.id, c.name, c.type, l.easternOffset, l.volAdjustment" +
                " FROM customer c " +
                " LEFT JOIN location l ON l.customer = c.id " +
                " WHERE c.id=? " +
                " ORDER BY l.name ASC ";


        PreparedStatement stmt = null;
        ResultSet rs = null;

        try {
            // we need to know if the user has Admin access
            stmt = transconn.prepareStatement(checkRoot);
            stmt.setString(1, username);
            stmt.setString(2, password);
            rs = stmt.executeQuery();

            int isManager = -1;
            int isITAdmin = -1;
            int userId = -1;
            int customerToLoad = 0;
            int associatedCustomer = -1;
            boolean isSuperAdmin = false;
            if (rs != null && rs.next()) {
                int rsIndex = 1;
                userId = rs.getInt(rsIndex++);
                isManager = rs.getInt(rsIndex++);
                isITAdmin = rs.getInt(rsIndex++);
                customerToLoad = rs.getInt(rsIndex++);
                associatedCustomer = rs.getInt(rsIndex++);
                if (associatedCustomer == ROOT_CUSTOMER) {
                    if (customerToLoad <= 0) {
                        customerToLoad = HQ_CUSTID;
                    }
                    isSuperAdmin = true;
                } else {
                    customerToLoad = associatedCustomer;
                    isSuperAdmin = false;
                }
                toAppend.addElement("userId").addText(String.valueOf(userId));
            }
            if (isManager > 0) {
                // the user is an Admin (root)
                String logMessage = "Granting " + (isSuperAdmin ? "Admin" : "Super-manager") + " access for " + username;
                logger.portalDetail(userId, "login", 0, logMessage, transconn);
                stmt = transconn.prepareStatement(selectRoot);
                stmt.setInt(1, customerToLoad);
                rs = stmt.executeQuery();
                while (rs != null && rs.next()) {
                    int rsIndex = 1;
                    Element locEl = toAppend.addElement("location");
                    locEl.addElement("locationId").addText(String.valueOf(rs.getInt(rsIndex++)));
                    locEl.addElement("locationName").addText(HandlerUtils.nullToEmpty((rs.getString(rsIndex++))));
                    locEl.addElement("locationType").addText(String.valueOf(rs.getInt(rsIndex++)));
                    locEl.addElement("customerId").addText(String.valueOf(rs.getInt(rsIndex++)));
                    locEl.addElement("customerName").addText(HandlerUtils.nullToEmpty((rs.getString(rsIndex++))));
                    locEl.addElement("customerType").addText(String.valueOf(rs.getInt(rsIndex++)));
                    locEl.addElement("easternOffset").addText(HandlerUtils.nullToEmpty((rs.getString(rsIndex++))));
                    locEl.addElement("volAdjustment").addText(HandlerUtils.nullToEmpty((rs.getString(rsIndex++))));
                    WebPermission perm = isSuperAdmin ? WebPermission.instanceOfUsbnAdmin() : WebPermission.instanceOfCustomerAdmin();
                    locEl.addElement("securityLevel").addText(String.valueOf(perm.getLevel()));

                }
            } else if (isITAdmin > 0) {
                // the user is an Admin (root)
                String logMessage = "Granting " + (isSuperAdmin ? "Admin" : "Super-manager") + " access for " + username;
                logger.portalDetail(userId, "login", 0, logMessage, transconn);
                stmt = transconn.prepareStatement(selectRoot);
                stmt.setInt(1, customerToLoad);
                rs = stmt.executeQuery();
                while (rs != null && rs.next()) {
                    int rsIndex = 1;
                    Element locEl = toAppend.addElement("location");
                    locEl.addElement("locationId").addText(String.valueOf(rs.getInt(rsIndex++)));
                    locEl.addElement("locationName").addText(HandlerUtils.nullToEmpty((rs.getString(rsIndex++))));
                    locEl.addElement("locationType").addText(String.valueOf(rs.getInt(rsIndex++)));
                    locEl.addElement("customerId").addText(String.valueOf(rs.getInt(rsIndex++)));
                    locEl.addElement("customerName").addText(HandlerUtils.nullToEmpty((rs.getString(rsIndex++))));
                    locEl.addElement("customerType").addText(String.valueOf(rs.getInt(rsIndex++)));
                    locEl.addElement("easternOffset").addText(HandlerUtils.nullToEmpty((rs.getString(rsIndex++))));
                    locEl.addElement("volAdjustment").addText(HandlerUtils.nullToEmpty((rs.getString(rsIndex++))));
                    WebPermission perm = isSuperAdmin ? WebPermission.instanceOfUsbnAdmin() : WebPermission.instanceOfITAdmin();
                    locEl.addElement("securityLevel").addText(String.valueOf(perm.getLevel()));

                }
            } else if (userId >= 0) {
                // the user is not an Admin(root)
                String logMessage = "Granting map-level access for " + username;
                logger.portalDetail(userId, "login", 0, logMessage, transconn);
                stmt = transconn.prepareStatement(selectNormal);
                stmt.setInt(1, userId);
                rs = stmt.executeQuery();

                while (rs != null && rs.next()) {
                    int rsIndex = 1;
                    Element locEl = toAppend.addElement("location");
                    locEl.addElement("locationId").addText(String.valueOf(rs.getInt(rsIndex++)));
                    locEl.addElement("locationName").addText(HandlerUtils.nullToEmpty((rs.getString(rsIndex++))));
                    locEl.addElement("locationType").addText(String.valueOf(rs.getInt(rsIndex++)));
                    locEl.addElement("customerId").addText(String.valueOf(rs.getInt(rsIndex++)));
                    locEl.addElement("customerName").addText(HandlerUtils.nullToEmpty((rs.getString(rsIndex++))));
                    locEl.addElement("customerType").addText(String.valueOf(rs.getInt(rsIndex++)));
                    locEl.addElement("easternOffset").addText(HandlerUtils.nullToEmpty((rs.getString(rsIndex++))));
                    locEl.addElement("volAdjustment").addText(HandlerUtils.nullToEmpty((rs.getString(rsIndex++))));
                    locEl.addElement("securityLevel").addText(String.valueOf(rs.getInt(rsIndex++)));

                }
            } else {
                logger.portalAction("Authentication failed for " + username);
                // authentication failed
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(rs);
            close(stmt);
        }

    }

    private void getLocationData(Element toHandle, Element toAppend) throws HandlerException {

        PreparedStatement stmt = null;
        ResultSet rs = null;

        int customerId = HandlerUtils.getRequiredInteger(toHandle, "customerId");
        int callerId = getCallerId(toHandle);

        String getLocationData = "SELECT l.id, l.name, l.lastPoured, l.picoPowerup, l.picoVersion, l.lastSold, l.processorName, l.processorVersion, l.gatewayVersion, l.volAdjustment" +
                                " FROM location l LEFT JOIN locationDetails lD ON lD.location = l.id   WHERE lD.active = 1 AND l.customer = ? ORDER BY l.name ;  ";
        String getCustomerName = "SELECT name from customer where id = ?";
        String getStatus                    = "SELECT c.name, l.name, l.lastPoured, l.lastSold, b.lastPing FROM location l LEFT JOIN locationDetails lD ON lD.location = l.id LEFT JOIN customer c ON c.id = l.customer LEFT JOIN beerboard b ON l.id = b.location WHERE lD.active = 1 ";
        
            
        try {
            if(customerId > 0) {
               
                getStatus                   += "AND l.customer =? ORDER BY l.lastSold;";
          
            String logMessage = "Getting location information for customer" + customerId;
            stmt = transconn.prepareStatement(getLocationData);            
            stmt.setInt(1, customerId);            
            rs = stmt.executeQuery();
            while (rs != null && rs.next()) {
                int rsIndex = 1;
                Element locEl = toAppend.addElement("location");
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
                
            }
            
            
            stmt = transconn.prepareStatement(getStatus);            
            stmt.setInt(1, customerId);            
            rs = stmt.executeQuery();
            while (rs != null && rs.next()) {
                int rsIndex = 1;
                Element locEl = toAppend.addElement("status");
                locEl.addElement("customerName").addText(HandlerUtils.nullToEmpty((rs.getString(rsIndex++))));
                locEl.addElement("locationName").addText(HandlerUtils.nullToEmpty((rs.getString(rsIndex++))));
                locEl.addElement("bevbox").addText(HandlerUtils.nullToEmpty((rs.getString(rsIndex++))));
                locEl.addElement("gateway").addText(HandlerUtils.nullToEmpty((rs.getString(rsIndex++))));
                locEl.addElement("bbtv").addText(HandlerUtils.nullToEmpty((rs.getString(rsIndex++))));
                
            }
            
            stmt = transconn.prepareStatement(getCustomerName);
            stmt.setInt(1, customerId);
            rs = stmt.executeQuery();
            if (rs.next()) {
                toAppend.addElement("customerName").addText(HandlerUtils.nullToEmpty(rs.getString(1)));
            }
            } else {
                
                getStatus                   += " ORDER BY l.lastSold;";
                stmt = transconn.prepareStatement(getStatus);            
                     
            rs = stmt.executeQuery();
            while (rs != null && rs.next()) {
                int rsIndex = 1;
                Element locEl = toAppend.addElement("status");
               locEl.addElement("customerName").addText(HandlerUtils.nullToEmpty((rs.getString(rsIndex++))));
                locEl.addElement("locationName").addText(HandlerUtils.nullToEmpty((rs.getString(rsIndex++))));
                locEl.addElement("bevbox").addText(HandlerUtils.nullToEmpty((rs.getString(rsIndex++))));
                locEl.addElement("gateway").addText(HandlerUtils.nullToEmpty((rs.getString(rsIndex++))));
                locEl.addElement("bbtv").addText(HandlerUtils.nullToEmpty((rs.getString(rsIndex++))));
                
            }
                
                
                
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error in getLocationData: " + sqle.getMessage());
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

    private void getLocationId(Element toHandle, Element toAppend) throws HandlerException {

        PreparedStatement stmt = null;
        ResultSet rs = null;

        int store = HandlerUtils.getRequiredInteger(toHandle, "storeId");

        String getLocationId = "SELECT location FROM customerStoreId WHERE store=?";
        try {
            stmt = transconn.prepareStatement(getLocationId);
            stmt.setInt(1, store);
            rs = stmt.executeQuery();
            if (rs.next()) {
                toAppend.addElement("locationId").addText(String.valueOf(rs.getInt(1)));
            }

        } catch (SQLException sqle) {
            logger.dbError("Database error in getLocationId: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {

            close(stmt);
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

    /**  Create a new beverage by copying the recipe from a plu at the same location,
     *  but scaling it to a new size. This method now includes
     *  an optional "barId"  If this is set, the beverage will be associated with this
     *  bar.  If not, the beverage will have a 'null' bar field. 
     *  
     *
     *  <locationId>
     *  <beverage>
     *    <plu>"String"
     *    <copyFrom>"String"
     *    <size>0.00
     *  </beverage>  
     *
     */
    private void copyTestBeverage(Element toHandle, Element toAppend) throws HandlerException {

        boolean forCustomer = HandlerUtils.getOptionalBoolean(toHandle, "forCustomer");
        
        int location = 0;

        location = HandlerUtils.getRequiredInteger(toHandle, "location");
        int callerId = getCallerId(toHandle);

        String selectLocations = " SELECT id FROM location WHERE customer = ? ";

        PreparedStatement stmt = null;
        ResultSet rs = null;

        try {
            if (forCustomer) {
                int customerId = HandlerUtils.getRequiredInteger(toHandle, "customerId");  
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
                    copyBeverageDetail(callerId, location, toHandle, toAppend);
                }
            } else {
                location = HandlerUtils.getRequiredInteger(toHandle, "location");
                copyBeverageDetail(callerId, location, toHandle, toAppend);
            }
        } catch (SQLException sqle) {
            logger.dbError("SQL Exception: " + sqle.toString());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
            close(rs);
        }
    }

    private void copyBeverageDetail(int callerId, int location, Element toHandle, Element toAppend) throws HandlerException {

        PreparedStatement stmt = null;
        ResultSet rs = null;
        ResultSet ings = null;

        
        int bar = HandlerUtils.getOptionalInteger(toHandle, "barId");
        int prodType = HandlerUtils.getRequiredInteger(toHandle, "prodID");

        String checkPlu =
                " SELECT id FROM beverage WHERE plu=? AND location=?";
        String getLastId =
                " SELECT LAST_INSERT_ID()";
        String getReference =
                " SELECT b.name, b.simple, SUM(i.ounces) FROM beverage b LEFT JOIN ingredient i ON " +
                " i.beverage=b.id WHERE b.id=? GROUP BY b.id ";
        String getIngs =
                " SELECT product,ounces FROM ingredient WHERE beverage=?";
        String insertBev = (bar > 0 ? " INSERT INTO beverage (name, location, plu, ounces, simple, pType, bar) VALUES (?,?,?,?,?,?,?) "
                : " INSERT INTO beverage (name, location, plu, ounces, simple, pType) VALUES (?,?,?,?,?,?)");
        String insertIng =
                " INSERT INTO ingredient (beverage, product, ounces) " +
                " VALUES (?,?,?)";

        boolean oldAutoCommit = true;
        boolean changedAutoCommit = false;

        try {

            oldAutoCommit = transconn.getAutoCommit();
            changedAutoCommit = true;
            transconn.setAutoCommit(false);

            Iterator bevs = toHandle.elementIterator("beverage");
            while (bevs.hasNext()) {
                Element beverage = (Element) bevs.next();

                String plu = HandlerUtils.getRequiredString(beverage, "plu");
                String copyFrom = HandlerUtils.getRequiredString(beverage, "copyFrom");
                double size = HandlerUtils.getRequiredDouble(beverage, "size");
                String name = HandlerUtils.getOptionalString(beverage, "name");
                int simple = HandlerUtils.getOptionalInteger(beverage, "simple");
                boolean overWrite = HandlerUtils.getOptionalBoolean(beverage, "overwrite");
                if (simple < 0) {
                    simple = 1;
                }

                // check that the reference plu exists
                stmt = transconn.prepareStatement(checkPlu);
                stmt.setString(1, copyFrom);
                stmt.setInt(2, location);
                rs = stmt.executeQuery();
                int fromId=0;
                if (rs.next()) {
                    fromId = rs.getInt(1);
                } else {
                    addErrorDetail(toAppend, "Reference plu doesn't exist: " + copyFrom);
                    continue;
                     
                    
                }
                //check that the new plu doesn't already exist
                stmt = transconn.prepareStatement(checkPlu);
                stmt.setString(1, plu);
                stmt.setInt(2, location);
                rs = stmt.executeQuery();
                if (rs.next()) {
                    addErrorDetail(toAppend, "The plu '" + plu + "' already exists");
                    continue;
                     
                }

                //get the details of the reference beverage and the list of ingredients
                stmt = transconn.prepareStatement(getReference);
                stmt.setInt(1, fromId);
                rs = stmt.executeQuery();
                double scaleFactor = 1;
                simple = 0;
                name = "";
                if (rs.next()) {
                    name = rs.getString(1);
                    simple = rs.getInt(2);
                    scaleFactor = size / rs.getDouble(3);
                } else {
                    addErrorDetail(toAppend, "Reference plu doesn't exist: " + copyFrom);
                    continue;
                     
                }
                stmt = transconn.prepareStatement(getIngs);
                stmt.setInt(1, fromId);
                ings = stmt.executeQuery();

                // insert the bev (name,location,plu,simple)
                stmt = transconn.prepareStatement(insertBev);
                stmt.setString(1, name);
                stmt.setInt(2, location);
                stmt.setString(3, plu);
                stmt.setDouble(4, size);
                stmt.setInt(5, simple);
                stmt.setInt(6, prodType);
                if (bar > 0) {
                    stmt.setInt(7, bar);
                }
                stmt.executeUpdate();
                stmt = transconn.prepareStatement(getLastId);
                rs = stmt.executeQuery();
                int newId = 0;
                if (rs.next()) {
                    newId = rs.getInt(1);
                } else {
                    addErrorDetail(toAppend, "Database Error");
                    transconn.rollback();
                    continue;
                }

                //add the ingredients
                double sizeLeft = size;
                while (ings.next()) {
                    double sizeToInsert = 0.0;
                    int product = ings.getInt(1);
                    if (ings.isLast()) {
                        sizeToInsert = sizeLeft;
                    } else {
                        sizeToInsert = twoPlaces(scaleFactor * ings.getDouble(2));
                        sizeLeft -= sizeToInsert;
                    }
                    logger.debug("Adding ingredient " + product + ": " + sizeToInsert + " oz");
                    stmt = transconn.prepareStatement(insertIng); // (bev,product,ounces)
                    stmt.setInt(1, newId);
                    stmt.setInt(2, product);
                    stmt.setDouble(3, sizeToInsert);
                    stmt.executeUpdate();
                }
                String logMessage = "Added plu#" + plu + " (copied from " + copyFrom + " to " + size + " oz)";
                logger.portalDetail(callerId, "addBeverage", location, "beverage", newId, logMessage, transconn);
                transconn.commit();
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            if (null != transconn && changedAutoCommit) {
                try {
                    transconn.setAutoCommit(oldAutoCommit);
                } catch (SQLException ignore) {
                }
            }
            close(ings);
            close(stmt);
            close(rs);
        }
    }

    private double twoPlaces(double d) {
        return Math.floor(d * 100 + 0.5) / 100;
    }
    
    
     private void addReferencePlu(String name, String plu, int simple, int location, Element toHandle, Element toAppend) throws HandlerException {
         
         PreparedStatement stmt             = null;
         ResultSet rs                       = null;
         int bar                            = HandlerUtils.getOptionalInteger(toHandle, "barId");
         int prodType                       = HandlerUtils.getRequiredInteger(toHandle, "prodID");
          logger.debug("bar"+bar);
          logger.debug("prodType"+prodType);
         
         String getLastId                   = " SELECT LAST_INSERT_ID()";
         String insertBev                   = (bar > 0 ? " INSERT INTO beverage (name, location, plu, simple, pType, bar) VALUES (?,?,?,?,?,?) "
                                            : " INSERT INTO beverage (name, location, plu, simple, pType) VALUES (?,?,?,?,?)");
         String insertIng                   = " INSERT INTO ingredient (beverage, product, ounces) " +
                                            " VALUES (?,?,?)";
         String updateBeverageSize          = "UPDATE beverage SET ounces = ? WHERE id = ? ";
         
         
         try {
             
              
             Iterator ings = toHandle.elementIterator("ingredients");
             while (ings.hasNext()) {
                 Element ingredient             = (Element) ings.next();
                 logger.debug("Ingredients"+HandlerUtils.getRequiredInteger(ingredient, "product"));
             }
             
             /*stmt                           = transconn.prepareStatement(insertBev);
             stmt.setString(1, name);
             stmt.setInt(2, location);
             stmt.setString(3, plu);
             stmt.setInt(4, simple);
             stmt.setInt(5, prodType);
             if (bar > 0) {
                 stmt.setInt(6, bar);
             }
             stmt.executeUpdate();
             stmt                           = transconn.prepareStatement(getLastId);
             rs                             = stmt.executeQuery();
             int newId                      = 0;
             if (rs.next()) {
                 newId                      = rs.getInt(1);
             } else {
                 addErrorDetail(toAppend, "Database Error");
                 
                 
             }
             double totalQuantity = 0.0;
             Iterator ings = toHandle.elementIterator("ingredients");
             while (ings.hasNext()) {
                 logger.debug("Ingredients");
                 Element ingredient             = (Element) ings.next();
                 String quantityString          = HandlerUtils.getRequiredString(ingredient, "quantity");
                 double quantity                = 0.0;
                 try {
                     quantity                   = Double.parseDouble(quantityString);
                 } catch (NumberFormatException nfe) {
                     throw new HandlerException("Quantity (" + quantityString + ") must be a double");
                 }
                 totalQuantity                  += quantity;
                 int product                    = HandlerUtils.getRequiredInteger(ingredient, "product");
                 stmt = transconn.prepareStatement(insertIng);
                 stmt.setInt(1, newId);
                 stmt.setInt(2, product);
                 stmt.setDouble(3, quantity);
                 stmt.executeUpdate();
             }
             stmt                           = transconn.prepareStatement(updateBeverageSize);
             stmt.setDouble(1, totalQuantity);
             stmt.setInt(2, newId);
             stmt.executeUpdate();*/
            
         } catch (Exception sqle) {
             logger.dbError("Database error: " + sqle.getMessage());
             throw new HandlerException(sqle);
         } finally {
             
             
             close(stmt);
             close(rs);
         }
            
             
     }

    

    /**
     * The folowing code is to run Generic Querys and return the value to the report 
     * - This particular hander gets 1 input and returns 1 output ---- SR
     * - Messed with --- JL1
     */
    private String getGenericResult(Element toHandle, Element toAppend) throws HandlerException {


        String query = HandlerUtils.getRequiredString(toHandle, "query");
        PreparedStatement stmt = null;
        ResultSet rs = null;
        String queryResult = null;
        String sql = query;

        try {
            stmt = transconn.prepareStatement(sql);
            rs = stmt.executeQuery();

            int i = 1;
            String totalrow = "";
            while (rs.next()) {
                try {
                    while (rs.getObject(i) != null) {
                        totalrow = totalrow + String.valueOf(rs.getObject(i) + ", ");
                        i = i + 1;
                    }
                } catch (SQLException sqle1) {
                }
                toAppend.addElement("queryResult").addText(HandlerUtils.nullToEmpty(totalrow));
                i = 1;
                totalrow = "";
            }

        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.toString());
            throw new HandlerException(sqle);
        } finally {
            close(rs);
            close(stmt);
            return queryResult;
        }
    }

    /**  Add one or more products to a locations inventory
     *  <product>
     *      <productId>000
     *      <locationId>000
     *      <reorderPoint>000 // TODO:  This should be a float
     *      <reorderQty>000
     *      <supplierId>000
     *      <kegSize>000
     *      <plu>"String"
     *  </product>
     *  <product>...</product>
     */
    private void addTestInventory(Element toHandle, Element toAppend) throws HandlerException {
        int callerId = getCallerId(toHandle);
        int location = HandlerUtils.getRequiredInteger(toHandle, "locationId");

        HashMap<Integer, HashMap> SupplierProductMap = new HashMap<Integer, HashMap>();
        HashMap<Integer, String> ProductPLUMap = new HashMap<Integer, String>();
        HashMap<Integer, Integer> ProductSupplierMap = new HashMap<Integer, Integer>();


        String selectSupplier = "SELECT sA.supplier FROM locationSupplier lS LEFT JOIN supplierAddress sA ON sA.id = lS.address WHERE lS.location=? ";
        String select =
                " SELECT id FROM inventory WHERE product=? AND location=?";
        String selectSupplierProducts = "SELECT pSM.product, pSM.plu FROM productSetMap pSM LEFT JOIN supplier s ON s.productSet = pSM.productSet WHERE s.id = ? ";
        String insert =
                " INSERT INTO inventory (product,location,qtyOnHand,minimumQty,qtyToHave,plu,supplier,kegSize,bottleSize) " +
                " VALUES (?,?,?,?,?,?,?,?,?) ";
        String selectCoolerInventory =
                " SELECT id FROM inventory WHERE product=? AND location=? AND bottleSize=? AND cooler=? AND kegLine=? ";
        String insertCoolerInventory =
                " INSERT INTO inventory (product,location,qtyOnHand,minimumQty,qtyToHave,plu,supplier,kegSize,bottleSize,cooler,kegLine) " +
                " VALUES (?,?,?,?,?,?,?,?,?,?,?) ";


        PreparedStatement stmt = null;
        ResultSet rs = null, rsSupplier = null;

        logger.portalAction("addInventory");

        Iterator i = toHandle.elementIterator("product");

        try {

            stmt = transconn.prepareStatement(selectSupplier);
            stmt.setInt(1, location);
            rsSupplier = stmt.executeQuery();
            while (rsSupplier.next()) {
                stmt = transconn.prepareStatement(selectSupplierProducts);
                stmt.setInt(1, rsSupplier.getInt(1));
                rs = stmt.executeQuery();
                while (rs.next()) {
                    ProductPLUMap.put(new Integer(rs.getInt(1)), rs.getString(2));
                    ProductSupplierMap.put(new Integer(rs.getInt(1)), new Integer(rsSupplier.getInt(1)));
                }
                SupplierProductMap.put(rsSupplier.getInt(1), ProductPLUMap);
            }


            while (i.hasNext()) {
                Element prod = (Element) i.next();


                int productId = HandlerUtils.getRequiredInteger(prod, "productId");
                int locationId = HandlerUtils.getRequiredInteger(prod, "locationId");
                float qtyOnHand = HandlerUtils.getRequiredFloat(prod, "qtyOnHand");
                float reorderPoint = HandlerUtils.getRequiredFloat(prod, "reorderPoint");
                float reorderQty = HandlerUtils.getRequiredFloat(prod, "reorderQty");
                int supplierId = HandlerUtils.getRequiredInteger(prod, "supplierId");
                int kegSize = 0;
                kegSize = HandlerUtils.getOptionalInteger(prod, "kegSize");
                String plu = HandlerUtils.getRequiredString(prod, "plu");
                int bottleSize = 0;
                bottleSize = HandlerUtils.getOptionalInteger(prod, "bottleSizeId");
                int coolerId = HandlerUtils.getOptionalInteger(prod, "coolerId");
                int lineId = HandlerUtils.getOptionalInteger(prod, "lineId");
                
                if ((supplierId == 2) && (ProductSupplierMap.containsKey(productId))) {
                        supplierId = ProductSupplierMap.get(productId);
                }

                if (checkForeignKey("product", productId, transconn) && checkForeignKey("location", locationId, transconn) && checkForeignKey("supplier", supplierId, transconn)) {
    
                    if (SupplierProductMap.containsKey(supplierId)) {
                        ProductPLUMap = SupplierProductMap.get(supplierId);
                    } 

                    if (ProductPLUMap.containsKey(productId)) {
                        plu = ProductPLUMap.get(productId);
                    }

                    //Check that this product doesn't already exist in inventory at this location
                    if (coolerId > 0) {
                        stmt = transconn.prepareStatement(selectCoolerInventory);
                        stmt.setInt(4, coolerId);
                        stmt.setInt(5, lineId);
                    } else {
                        stmt = transconn.prepareStatement(select);
                    }
                    stmt.setInt(1, productId);
                    stmt.setInt(2, locationId);
                    stmt.setInt(3, bottleSize);
                    rs = stmt.executeQuery();
                    if (rs.next()) {
                        logger.generalWarning("Inventory exists at Loc#" + locationId + " for Prod# " + productId);
                        addErrorDetail(toAppend, "Product ID " + productId + " already exists");
                    } else {
                        // Insert a new inventory record
                        if (coolerId > 0) {
                            stmt = transconn.prepareStatement(insertCoolerInventory);
                            stmt.setInt(10, coolerId);
                            stmt.setInt(11, lineId);
                        } else {
                            stmt = transconn.prepareStatement(insert);
                        }
                        stmt.setInt(1, productId);
                        stmt.setInt(2, locationId);
                        stmt.setFloat(3, qtyOnHand);
                        stmt.setFloat(4, reorderPoint);
                        stmt.setFloat(5, reorderQty);
                        stmt.setString(6, plu);
                        stmt.setInt(7, supplierId);
                        stmt.setInt(8, kegSize);
                        stmt.setInt(9, bottleSize);
                        stmt.executeUpdate();

                        // Log the action
                        stmt = transconn.prepareStatement("SELECT LAST_INSERT_ID()");
                        rs = stmt.executeQuery();
                        if (rs.next()) {
                            int newId = rs.getInt(1);
                            String logMessage = "Added product " + productId + " to inventory";
                            logger.portalDetail(callerId, "addTestInventory", locationId, "inventory", newId, logMessage, transconn);
                        }
                    }
                } else {
                    logger.generalWarning("Foreign key check failed for Prod# " + productId + " or Loc# " + locationId + " or Sup# " + supplierId);
                    addErrorDetail(toAppend, "Unable to add product ID " + productId + "; a database problem occurred");
                }
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
            close(rs);
            close(rsSupplier);
        }

    }

    /**  Return Yesterdays's Value for a location
     */
    private void getYesterdayVariance(Element toHandle, Element toAppend) throws HandlerException {
        int user    = HandlerUtils.getOptionalInteger(toHandle, "userId");
        int location = HandlerUtils.getOptionalInteger(toHandle, "locationId");
        int customer = HandlerUtils.getOptionalInteger(toHandle, "customerId");
        int sqlParameter = 0;

        String selectPoured = "Select SUM(ps.value)*l.volAdjustment FROM openHoursSummary ps LEFT JOIN product p on ps.product = p.id " +
                " LEFT JOIN location l ON l.id = ps.location WHERE p.pType=1 and ps.date=? AND l.id = ? GROUP BY ps.location ORDER BY ps.location";
        String selectSold = "Select SUM(ss.value) FROM openHoursSoldSummary ss LEFT JOIN product p on ss.product = p.id " +
                " LEFT JOIN location l ON l.id = ss.location WHERE p.pType=1 and ss.date=? AND l.id = ? group by ss.location ORDER BY ss.location ";
        String selectLocation = "Select l.id FROM location l ";
        PreparedStatement stmt = null;
        ResultSet rs = null, rsLocation = null;
        Calendar date1 = Calendar.getInstance();
        date1.add(Calendar.DATE, -1);

        int paramsSet = 0, locationId = 0;

        if (user >= 0) {
            paramsSet++;
            selectLocation += " LEFT JOIN userMap uM ON uM.location = l.id WHERE uM.user = ? ";
            sqlParameter = user;
        }

        if (location >= 0) {
            paramsSet++;
            selectLocation += " WHERE l.id = ? ";
            sqlParameter = location;
        }

        if (customer >= 0) {
            paramsSet++;
            selectLocation += " WHERE l.customer = ? ORDER BY l.name";
            sqlParameter = customer;
        }

        if (paramsSet != 1) {
            throw new HandlerException("Exactly one of the following must be set: location customer");
        }

        String Yesterday = String.valueOf(date1.get(Calendar.YEAR)) + "-0" + String.valueOf(date1.get(Calendar.MONTH) + 1) + "-" + String.valueOf(date1.get(Calendar.DAY_OF_MONTH));

        try {
            stmt = transconn.prepareStatement(selectLocation);
            stmt.setInt(1, sqlParameter);
            rsLocation = stmt.executeQuery();
            while (rsLocation.next()) {
                double pouredTotal = 0.00, soldTotal = 0.00, varTotal = 0.00;

                locationId = rsLocation.getInt(1);

                stmt = transconn.prepareStatement(selectPoured);
                stmt.setString(1, Yesterday);
                stmt.setInt(2, locationId);
                rs = stmt.executeQuery();
                if (rs.next()) {
                    pouredTotal = rs.getDouble(1);
                }

                stmt = transconn.prepareStatement(selectSold);
                stmt.setString(1, Yesterday);
                stmt.setInt(2, locationId);
                rs = stmt.executeQuery();
                if (rs.next()) {
                    soldTotal = rs.getDouble(1);
                }

                varTotal = ((soldTotal - pouredTotal) / pouredTotal) * 100;

                Element locationEl = toAppend.addElement("location");
                locationEl.addElement("locationId").addText(String.valueOf(locationId));
                locationEl.addElement("pouredTotal").addText(String.valueOf(pouredTotal));
                locationEl.addElement("soldTotal").addText(String.valueOf(soldTotal));
                locationEl.addElement("ydayVariance").addText(String.valueOf(varTotal));
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error in getYesterdayVariance: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(rs);
            close(stmt);
        }
    }

    /**  Return the Variance Value for a location
     */
    private void getLocationVariance(Element toHandle, Element toAppend) throws HandlerException {
        int location = HandlerUtils.getOptionalInteger(toHandle, "location");

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

    /**  Return a list of all associated suppliers from this location
     */
    private void getBottleSize(Element toHandle, Element toAppend) throws HandlerException {
        int location = HandlerUtils.getOptionalInteger(toHandle, "locationId");
        int prodType = HandlerUtils.getOptionalInteger(toHandle, "prodID");

        String select = "Select id, name FROM bottleSize where pType=? order by name";

        PreparedStatement stmt = null;
        ResultSet rs = null;

        try {
            stmt = transconn.prepareStatement(select);
            stmt.setInt(1, prodType);
            rs = stmt.executeQuery();
            while (rs.next()) {
                Element sup = toAppend.addElement("bottleSize");
                sup.addElement("id").addText(HandlerUtils.nullToEmpty(rs.getString(1)));
                sup.addElement("sizeName").addText(HandlerUtils.nullToEmpty(rs.getString(2)));
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error in getBottleSize: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(rs);
            close(stmt);
        }
    }

    /**  Return a list of all associated suppliers from this location
     */
    private void getEventDates(Element toHandle, Element toAppend) throws HandlerException {

        int location                        = HandlerUtils.getOptionalInteger(toHandle, "locationId");
        int customer                        = HandlerUtils.getOptionalInteger(toHandle, "customerId");
        String eventDate                    = HandlerUtils.getOptionalString(toHandle, "eventDate");

        int eventId                         = HandlerUtils.getOptionalInteger(toHandle, "eventId");
        boolean getNoDetails                = HandlerUtils.getOptionalBoolean(toHandle, "getNoDetails");

        boolean getPastGames                = HandlerUtils.getOptionalBoolean(toHandle, "getPastGames");
        String conditionString              = "";
        int parameter                       = -1;

        if (customer > 0) {
            conditionString                 = " WHERE l.customer = ? ";
            parameter                       = customer;
        } else {
            conditionString                 = " WHERE l.id = ? ";
            parameter                       = location;
        }

        if (eventDate != null && !eventDate.equals("")) {
            conditionString                 += " AND DATEDIFF(eH.date, '" + eventDate + "') = 0 ";
        }

        if(!getPastGames)
        {
            conditionString                 += " AND eH.date > LEFT(SUBDATE(NOW(),1),10) ";
        }

        String select                       = "SELECT eH.id, eH.location, l.name, eH.date, eH.eventDesc" +
                                            (getNoDetails ? "" : ", eH.eventPourStart, eH.preOpen, eH.eventStart, eH.eventEnd, eH.eventAfterHoursEnd ") +
                                            " FROM eventHours eH LEFT JOIN location l ON l.id = eH.location " + conditionString + (eventId > 0 ? " AND eH.id = ? " : "") +
                                            " ORDER BY eH.eventPourStart ";

        String selectSpecialHours           = "SELECT eH.id, eH.barString, eH.eventPourStart, eH.preOpen, eH.eventStart, eH.eventEnd, eH.eventAfterHoursEnd " +
                                            " FROM eventSpecialHours eH LEFT JOIN location l ON l.id = eH.location " + conditionString +
                                            " ORDER BY eH.eventPourStart ";

        String selectCateredHours           = "SELECT eH.id, eH.barString, eH.eventPourStart, eH.preOpen, eH.eventStart, eH.eventEnd, eH.eventAfterHoursEnd " +
                                            " FROM eventCateredHours eH LEFT JOIN location l ON l.id = eH.location " + conditionString +
                                            " ORDER BY eH.eventPourStart ";

        PreparedStatement stmt              = null;
        ResultSet rs                        = null, innerRS = null;

        try {
            stmt                            = transconn.prepareStatement(select);
            stmt.setInt(1, parameter);
            if(eventId > 0){ stmt.setInt(2, eventId); }
            rs                              = stmt.executeQuery();
            while (rs.next()) {
                int i                       = 1;
                
                Element event               = toAppend.addElement("eventDates");
                event.addElement("eventId").addText(HandlerUtils.nullToEmpty(rs.getString(i++)));
                event.addElement("locationId").addText(HandlerUtils.nullToEmpty(rs.getString(i++)));
                event.addElement("locationName").addText(HandlerUtils.nullToEmpty(rs.getString(i++)));
                event.addElement("eventDate").addText(HandlerUtils.nullToEmpty(rs.getString(i++)));
                event.addElement("eventDesc").addText(HandlerUtils.nullToEmpty(rs.getString(i++)));
                if(!getNoDetails)
                {
                    
                    event.addElement("eventPourStart").addText(HandlerUtils.nullToEmpty(rs.getString(i++)));
                    event.addElement("eventPreOpenTime").addText(HandlerUtils.nullToEmpty(rs.getString(i++)));
                    event.addElement("eventStartTime").addText(HandlerUtils.nullToEmpty(rs.getString(i++)));
                    event.addElement("eventEndTime").addText(HandlerUtils.nullToEmpty(rs.getString(i++)));
                    event.addElement("eventAfterHoursEnd").addText(HandlerUtils.nullToEmpty(rs.getString(i++)));
                    
                    // Getting special events hours
                    stmt                    = transconn.prepareStatement(selectSpecialHours);
                    stmt.setInt(1, parameter);
                    innerRS                 = stmt.executeQuery();
                    if (innerRS.next()) {
                        Element specialEvent= event.addElement("specialEventDates");
                        specialEvent.addElement("eventId").addText(HandlerUtils.nullToEmpty(innerRS.getString(1)));
                        specialEvent.addElement("barString").addText(HandlerUtils.nullToEmpty(innerRS.getString(2)));
                        specialEvent.addElement("eventPourStart").addText(HandlerUtils.nullToEmpty(innerRS.getString(3)));
                        specialEvent.addElement("eventPreOpenTime").addText(HandlerUtils.nullToEmpty(innerRS.getString(4)));
                        specialEvent.addElement("eventStartTime").addText(HandlerUtils.nullToEmpty(innerRS.getString(5)));
                        specialEvent.addElement("eventEndTime").addText(HandlerUtils.nullToEmpty(innerRS.getString(6)));
                        specialEvent.addElement("eventAfterHoursEnd").addText(HandlerUtils.nullToEmpty(innerRS.getString(7)));
                        
                    }

                    // Getting catered events hours
                    stmt                    = transconn.prepareStatement(selectCateredHours);
                    stmt.setInt(1, parameter);
                    innerRS                 = stmt.executeQuery();
                    if (innerRS.next()) {
                        Element cateredEvent= event.addElement("cateredEventDates");
                        cateredEvent.addElement("eventId").addText(HandlerUtils.nullToEmpty(innerRS.getString(1)));
                        cateredEvent.addElement("barString").addText(HandlerUtils.nullToEmpty(innerRS.getString(2)));
                        cateredEvent.addElement("eventPourStart").addText(HandlerUtils.nullToEmpty(innerRS.getString(3)));
                        cateredEvent.addElement("eventPreOpenTime").addText(HandlerUtils.nullToEmpty(innerRS.getString(4)));
                        cateredEvent.addElement("eventStartTime").addText(HandlerUtils.nullToEmpty(innerRS.getString(5)));
                        cateredEvent.addElement("eventEndTime").addText(HandlerUtils.nullToEmpty(innerRS.getString(6)));
                        cateredEvent.addElement("eventAfterHoursEnd").addText(HandlerUtils.nullToEmpty(innerRS.getString(7)));
                        
                    }
                }
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error in getEventDates: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(innerRS);
            close(rs);
            close(stmt);
        }
    }
    
    
    private void getSpecialCateredEvent(Element toHandle, Element toAppend) throws HandlerException {

        int location                        = HandlerUtils.getOptionalInteger(toHandle, "locationId");        
        String eventDate                    = HandlerUtils.getOptionalString(toHandle, "eventDate");
        int type                            = HandlerUtils.getOptionalInteger(toHandle, "type");
        
        String conditionString              = "";
        int parameter                       = -1;
        
        
        if (location > 0) {
            conditionString                 = " WHERE l.id = ? ";
            parameter                       = location;
        }

        if (eventDate != null && !eventDate.equals("")) {
            conditionString                 += " AND DATEDIFF(eH.date, '" + eventDate + "') = 0 ";
        }

        String selectSpecialHours           = "SELECT eH.id, eH.barString, eH.eventPourStart, eH.preOpen, eH.eventStart, eH.eventEnd, eH.eventAfterHoursEnd " +
                                            " FROM eventSpecialHours eH LEFT JOIN location l ON l.id = eH.location " + conditionString +
                                            " ORDER BY eH.eventPourStart ";

        String selectCateredHours           = "SELECT eH.id, eH.barString, eH.eventPourStart, eH.preOpen, eH.eventStart, eH.eventEnd, eH.eventAfterHoursEnd, eH.eventDesc " +
                                            " FROM eventCateredHours eH LEFT JOIN location l ON l.id = eH.location " + conditionString +
                                            " ORDER BY eH.eventPourStart ";

        PreparedStatement stmt              = null;
        ResultSet rs                        = null, innerRS = null;

        try {
            switch(type) {
                case 1: 
            stmt                            = transconn.prepareStatement(selectSpecialHours);
            stmt.setInt(1, parameter);            
            rs                              = stmt.executeQuery();
            if (rs.next()) {
                Element specialEvent        = toAppend.addElement("specialEvent");
                specialEvent.addElement("eventId").addText(HandlerUtils.nullToEmpty(rs.getString(1)));
                specialEvent.addElement("barString").addText(HandlerUtils.nullToEmpty(rs.getString(2)));
                specialEvent.addElement("eventPourStart").addText(HandlerUtils.nullToEmpty(rs.getString(3)));
                specialEvent.addElement("eventPreOpenTime").addText(HandlerUtils.nullToEmpty(rs.getString(4)));
                specialEvent.addElement("eventStartTime").addText(HandlerUtils.nullToEmpty(rs.getString(5)));
                specialEvent.addElement("eventEndTime").addText(HandlerUtils.nullToEmpty(rs.getString(6)));
                specialEvent.addElement("eventAfterHoursEnd").addText(HandlerUtils.nullToEmpty(rs.getString(7)));                 
                    
                }
            break;
                case 2: 
                    // Getting catered events hours
                    stmt                    = transconn.prepareStatement(selectCateredHours);
                    stmt.setInt(1, parameter);
                    rs                 = stmt.executeQuery();
                    if (rs.next()) {                       
                        Element cateredEvent= toAppend.addElement("cateredEvent");
                        cateredEvent.addElement("eventId").addText(HandlerUtils.nullToEmpty(rs.getString(1)));
                        cateredEvent.addElement("barString").addText(HandlerUtils.nullToEmpty(rs.getString(2)));
                        cateredEvent.addElement("eventPourStart").addText(HandlerUtils.nullToEmpty(rs.getString(3)));
                        cateredEvent.addElement("eventPreOpenTime").addText(HandlerUtils.nullToEmpty(rs.getString(4)));
                        cateredEvent.addElement("eventStartTime").addText(HandlerUtils.nullToEmpty(rs.getString(5)));
                        cateredEvent.addElement("eventEndTime").addText(HandlerUtils.nullToEmpty(rs.getString(6)));
                        cateredEvent.addElement("eventAfterHoursEnd").addText(HandlerUtils.nullToEmpty(rs.getString(7)));
                        cateredEvent.addElement("eventDesc").addText(HandlerUtils.nullToEmpty(rs.getString(8)));
                        
                    }
                    break;
            }
    
        } catch (SQLException sqle) {
            logger.dbError("Database error in getEventDates: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(innerRS);
            close(rs);
            close(stmt);
        }
    }

    /**  Return a list of all associated suppliers from this location
     */
    private void getStandUnitCount(Element toHandle, Element toAppend) throws HandlerException {

        int location = HandlerUtils.getRequiredInteger(toHandle, "locationId");
        int zone = HandlerUtils.getOptionalInteger(toHandle, "zoneId");
        int bar = HandlerUtils.getOptionalInteger(toHandle, "barId");
        String eventStartTime = HandlerUtils.getOptionalString(toHandle, "eventStartTime");

        //NischaySharma_12-Feb-2009_Start: Changed the xml to fetch unitStandCountId, barid and zoneid
        String select = "SELECT p.id, p.name, ifnull(uSC.count,0), ifnull(uSC.id,0), " +
                " ifnull(uSC.bar,0), ifnull(uSC.zone,0) FROM unitStandCount uSC " +
                " LEFT JOIN product p ON p.id = uSC.product " +
                " WHERE uSC.location = ? ";
        if (zone > 0) {
            select += " AND uSC.zone = ? ";
        }
        if (bar > 0) {
            select += " AND uSC.bar = ? ";
        }
        select += " AND uSC.date BETWEEN ? and (? + INTERVAL 1 DAY) GROUP BY uSC.bar, p.id";

        //NischaySharma_12-Feb-2009_End
        PreparedStatement stmt = null;
        ResultSet rs = null;

        try {
            int fieldCount = 0;

            stmt = transconn.prepareStatement(select);
            stmt.setInt(++fieldCount, location);
            if (zone > 0) {
                stmt.setInt(++fieldCount, zone);
            }
            if (bar > 0) {
                stmt.setInt(++fieldCount, bar);
            }
            stmt.setString(++fieldCount, eventStartTime);
            stmt.setString(++fieldCount, eventStartTime);
            rs = stmt.executeQuery();
            while (rs.next()) {
                Element eventStandCount = toAppend.addElement("eventStandCount");
                eventStandCount.addElement("productID").addText(HandlerUtils.nullToEmpty(rs.getString(1)));
                eventStandCount.addElement("product").addText(HandlerUtils.nullToEmpty(rs.getString(2)));
                eventStandCount.addElement("count").addText(HandlerUtils.nullToEmpty(rs.getString(3)));
                //NischaySharma_12-Feb-2009_Start: Added new tags to the response XML: unitStandCountId,
                //barId & zoneId
                eventStandCount.addElement("unitStandCountId").addText(HandlerUtils.nullToEmpty(rs.getString(4)));
                eventStandCount.addElement("barId").addText(HandlerUtils.nullToEmpty(rs.getString(5)));
                eventStandCount.addElement("zoneId").addText(HandlerUtils.nullToEmpty(rs.getString(6)));
                //NischaySharma_12-Feb-2009_End
                }
        } catch (SQLException sqle) {
            logger.dbError("Database error in getStandUnitCount: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(rs);
            close(stmt);
        }
    }

    /**  Return a list of all associated suppliers from this location
     */
    private void getUnitCount(Element toHandle, Element toAppend) throws HandlerException {

        int location = HandlerUtils.getRequiredInteger(toHandle, "locationId");
        int zone = HandlerUtils.getOptionalInteger(toHandle, "zoneId");
        int bar = HandlerUtils.getOptionalInteger(toHandle, "barId");
        String eventStartTime = HandlerUtils.getOptionalString(toHandle, "eventStartTime");
        //NischaySharma_12-Feb-2009_Start: chnaged the query to return UnitCountId also
        String select = "SELECT p.id, p.name, st.id, st.name, ifnull(uC.count,0), ifnull(uC.id,0) FROM unitCount uC " +
                " LEFT JOIN product p ON p.id = uC.product " +
                " LEFT JOIN station st ON st.id = uC.station " +
                " WHERE uC.location = ?";
        if (zone > 0) {
            select += " and uC.zone = ? ";
        }
        if (bar > 0) {
            select += " and uC.bar = ? ";
        }
        select += " AND uC.date BETWEEN ? and (? + INTERVAL 1 DAY)" +
                " GROUP BY uC.station, p.id;";

        PreparedStatement stmt = null;
        ResultSet rs = null;

        try {
            int fieldCount = 0;

            stmt = transconn.prepareStatement(select);
            stmt.setInt(++fieldCount, location);
            if (zone > 0) {
                stmt.setInt(++fieldCount, zone);
            }
            if (bar > 0) {
                stmt.setInt(++fieldCount, bar);
            }
            stmt.setString(++fieldCount, eventStartTime);
            stmt.setString(++fieldCount, eventStartTime);
            rs = stmt.executeQuery();
            while (rs.next()) {
                Element eventStandCount = toAppend.addElement("eventCount");
                eventStandCount.addElement("productID").addText(HandlerUtils.nullToEmpty(rs.getString(1)));
                eventStandCount.addElement("product").addText(HandlerUtils.nullToEmpty(rs.getString(2)));
                eventStandCount.addElement("stationID").addText(HandlerUtils.nullToEmpty(rs.getString(3)));
                eventStandCount.addElement("station").addText(HandlerUtils.nullToEmpty(rs.getString(4)));
                eventStandCount.addElement("count").addText(HandlerUtils.nullToEmpty(rs.getString(5)));
                //NischaySharma_12-Feb-2009_Start: Added tag unitCountId to the response XML
                eventStandCount.addElement("unitCountId").addText(HandlerUtils.nullToEmpty(rs.getString(6)));
                //NischaySharma_12-Feb-2009_End
                }
        } catch (SQLException sqle) {
            logger.dbError("Database error in getStandUnitCount: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(rs);
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

    /**  Return a list of all associated suppliers from this location
     */
    private void getInventoryDates(Element toHandle, Element toAppend) throws HandlerException {

        int location = HandlerUtils.getOptionalInteger(toHandle, "locationId");

        String select = "Select DISTINCT date FROM bottleInv where location=? order by date desc";

        PreparedStatement stmt = null;
        ResultSet rs = null;

        try {
            stmt = transconn.prepareStatement(select);
            stmt.setInt(1, location);
            rs = stmt.executeQuery();
            while (rs.next()) {
                Element sup = toAppend.addElement("invDates");
                sup.addElement("date").addText(HandlerUtils.nullToEmpty(rs.getString(1)));
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error in getInventoryDates: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(rs);
            close(stmt);
        }
    }

    /**  Return a list of all associated suppliers from this location
     */
    private void getBottleInventoryData(Element toHandle, Element toAppend) throws HandlerException {

        int location = HandlerUtils.getOptionalInteger(toHandle, "locationId");
        String invDate = HandlerUtils.getOptionalString(toHandle, "invDate");

        String select = "Select b.id, b.product, p.name, b.location, b.bar, b.lastActualInv, b.received, b.sold, b.calcOnhand, b.actualInv, b.var FROM bottleInv b left join product p on p.id = b.product where b.location=? and b.date =?";

        String selectStartDate = "Select date FROM bottleInv where location=? and date <? order by date desc limit 1";

        PreparedStatement stmt = null;
        ResultSet rs = null;

        try {
            stmt = transconn.prepareStatement(select);
            stmt.setInt(1, location);
            stmt.setString(2, invDate);
            rs = stmt.executeQuery();
            while (rs.next()) {
                int colCount = 1;
                Element sup = toAppend.addElement("invData");
                sup.addElement("inventoryID").addText(HandlerUtils.nullToEmpty(rs.getString(colCount++)));
                sup.addElement("product").addText(HandlerUtils.nullToEmpty(rs.getString(colCount++)));
                sup.addElement("productName").addText(HandlerUtils.nullToEmpty(rs.getString(colCount++)));
                sup.addElement("locationId").addText(HandlerUtils.nullToEmpty(rs.getString(colCount++)));
                sup.addElement("bar").addText(HandlerUtils.nullToEmpty(rs.getString(colCount++)));
                sup.addElement("lastActualInv").addText(HandlerUtils.nullToEmpty(rs.getString(colCount++)));
                sup.addElement("ordered").addText(HandlerUtils.nullToEmpty(rs.getString(colCount++)));
                sup.addElement("sold").addText(HandlerUtils.nullToEmpty(rs.getString(colCount++)));
                sup.addElement("calcOnHand").addText(HandlerUtils.nullToEmpty(rs.getString(colCount++)));
                sup.addElement("actInventory").addText(HandlerUtils.nullToEmpty(rs.getString(colCount++)));
                sup.addElement("variance").addText(HandlerUtils.nullToEmpty(rs.getString(colCount++)));
            }
            stmt = transconn.prepareStatement(selectStartDate);
            stmt.setInt(1, location);
            stmt.setString(2, invDate);
            rs = stmt.executeQuery();
            if (rs.next()) {
                toAppend.addElement("startDate").addText(HandlerUtils.nullToEmpty(rs.getString(1)));
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error in getBottleInventoryData: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(rs);
            close(stmt);
        }
    }

    /**  Return a list of all Keg sizes
     */
    private void getKegSize(Element toHandle, Element toAppend) throws HandlerException {
        int location = HandlerUtils.getOptionalInteger(toHandle, "locationId");
        int prodType = HandlerUtils.getOptionalInteger(toHandle, "prodID");

        String select = "Select name, size FROM kegSize order by size";

        PreparedStatement stmt = null;
        ResultSet rs = null;

        try {
            stmt = transconn.prepareStatement(select);
            rs = stmt.executeQuery();
            while (rs.next()) {
                Element sup = toAppend.addElement("kegSize");
                sup.addElement("name").addText(HandlerUtils.nullToEmpty(rs.getString(1)));
                sup.addElement("size").addText(HandlerUtils.nullToEmpty(rs.getString(2)));
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error in getKegSize: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(rs);
            close(stmt);
        }
    }


    // Adding in productSet information for the product
    private void addProductSetMap(int product, Iterator i, Element toAppend) throws HandlerException {
        String selectProductSet = " SELECT productSetType FROM productSet WHERE id = ? ";
        String selectProductSetMap = " SELECT pSM.id FROM productSetMap pSM LEFT JOIN productSet pS ON pS.id = pSM.productSet "
                                    + " WHERE pS.productSetType = ? AND pSM.product = ? ";
        String insertProductSetProductMap = " INSERT INTO productSetMap (productSet, product) VALUES (?,?) ";
        String updateProductSetProductMap = " UPDATE productSetMap SET productSet = ? WHERE id = ? ";

        PreparedStatement stmt = null;
        ResultSet rs = null;

        try {
            while (i.hasNext()) {
                Element prod = (Element) i.next();
                int productSet = HandlerUtils.getRequiredInteger(prod, "productSet");

                if (productSet == 0) {
                    String productSetName = HandlerUtils.getRequiredString(prod, "productSetName");
                    int productSetType = HandlerUtils.getRequiredInteger(prod, "productSetType");

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
                    } else {
                        productSet = rs.getInt(1);
                    }
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

    /**  Add a new product to the master product list along with the product type value.
     *  Will also mark as "added" any pending requests to add a product with this name.
     *
     *  <productName>"String"
     *  opt:<quicksell>000
     *---------SR
     */
    private void addProduct1(Element toHandle, Element toAppend) throws HandlerException {
        //  (int id, String name, int qId)
        String name                         = HandlerUtils.getRequiredString(toHandle, "productName");
        int callerId                        = getCallerId(toHandle);
        int qId                             = HandlerUtils.getOptionalInteger(toHandle, "quicksell");
        //NischaySharma_02-Mar-2010_Start
        int prodType                        = HandlerUtils.getRequiredInteger(toHandle, "prodType");
        int category                        = HandlerUtils.getRequiredInteger(toHandle, "category");
        int segment                         = HandlerUtils.getOptionalInteger(toHandle, "segment");
        String boardName                    = HandlerUtils.getRequiredString(toHandle, "boardName");
        String abv                          = HandlerUtils.getRequiredString(toHandle, "abv");
        String origin                       = HandlerUtils.getOptionalString(toHandle, "origin");
        String seasonality                  = HandlerUtils.getOptionalString(toHandle, "seasonality");
        int bbtvCategory                    = HandlerUtils.getRequiredInteger(toHandle, "bbtvCategory");
        int newProductId                    = 0;
        if (qId < 0) {
            qId = 0;
        }
        
        String insert                       = "INSERT INTO product (name, qId, pType, category, segment) VALUES (?,?,?,?,?)";
        String lookup                       = "SELECT id FROM product WHERE name=? AND pType=? LIMIT 1";
        String deleteRequest                = "UPDATE productRequest SET status='added' WHERE productName=? AND status='open'";
        
        PreparedStatement stmt              = null;
        ResultSet rs                        = null;

        logger.portalAction("addProduct1");

        try {

            if (prodType < 0) {
                prodType                    = 1;
            }
            
            if (callerId != 477) {
                throw new HandlerException("You don't have permission to add products");
            }

            stmt = transconn.prepareStatement(lookup);
            stmt.setString(1, name);
            stmt.setInt(2, prodType);
            rs = stmt.executeQuery();
            if (rs.next()) {
                addErrorDetail(toAppend, "A product named " + name + " already exists");
            } else {
                stmt = transconn.prepareStatement(insert);
                stmt.setString(1, name);
                stmt.setInt(2, qId);
                stmt.setInt(3, prodType);
                stmt.setInt(4, category);
                stmt.setInt(5, segment);
                stmt.executeUpdate();

                // Delete any pending ProductRequests
                stmt = transconn.prepareStatement(deleteRequest);
                stmt.setString(1, name);
                stmt.executeUpdate();

                // Log the action
                stmt = transconn.prepareStatement("SELECT LAST_INSERT_ID()");
                rs = stmt.executeQuery();
                if (rs.next()) {
                    newProductId = rs.getInt(1);
                    String logMessage = "Added product " + name;
                    logger.portalDetail(callerId, "addProduct", 0, "product", newProductId, logMessage, transconn);

                    String insertProductDescription 
                                            =" INSERT INTO productDescription (product, boardName, abv, category, origin, seasonality) VALUES" +
                            " (?,?,?,?,?,?)";
                    stmt = transconn.prepareStatement(insertProductDescription);
                    stmt.setInt(1, newProductId);
                    stmt.setString(2, boardName);
                    stmt.setString(3, abv);
                    stmt.setInt(4, bbtvCategory);
                    stmt.setString(5, origin);
                    stmt.setString(6, seasonality);
                    stmt.executeUpdate();


                    //Adding productSet information
                    Iterator i = toHandle.elementIterator("productSetMap");
                    addProductSetMap(newProductId,i,toAppend);
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
        String boardName                    = HandlerUtils.getRequiredString(toHandle, "boardName");
        String abv                          = HandlerUtils.getRequiredString(toHandle, "abv");
        String origin                       = HandlerUtils.getOptionalString(toHandle, "origin");
        String seasonality                  = HandlerUtils.getOptionalString(toHandle, "seasonality");
        int bbtvCategory                    = HandlerUtils.getRequiredInteger(toHandle, "bbtvCategory");
        
        int isActive                        = HandlerUtils.getOptionalBoolean(toHandle, "isInactive") ? 0 : 1;

        int callerId                        = getCallerId(toHandle);

        PreparedStatement stmt              = null;
        ResultSet rs = null;

        if (qid < 0) {
            qid = 0;
        }

        String update                       = "UPDATE product SET  name=?, pType=?, category=?, segment=?, isActive=? WHERE id=?";
        String fullUpdate                   = "UPDATE product SET name=?, qid=?, pType=?, category=?, segment=?, isActive=? WHERE id=?";
        String updateDescription            = "UPDATE productDescription SET  boardName=?, abv=?, category=?, origin=?, seasonality=? WHERE product=?";

        try {

            if (callerId != 477) {
                throw new HandlerException("You don't have permission to update products");
            }

            if (prodType < 0) {
                prodType                    = 1;
            }

            String logMessage = "Changing product name to " + name + " for id" + id;
            logger.portalDetail(callerId, "updateProduct", 0, "product", id, logMessage, transconn);
            //String select = "SELECT name FROM Location WHERE id=?";
            if (qid == 0) {
                stmt = transconn.prepareStatement(update);
                stmt.setString(1, name);
                stmt.setInt(2, prodType);
                stmt.setInt(3, category);
                stmt.setInt(4, segment);
                stmt.setInt(5, isActive);
                stmt.setInt(6, id);
                stmt.executeUpdate();
            } else {
                stmt = transconn.prepareStatement(fullUpdate);
                stmt.setString(1, name);
                stmt.setInt(2, qid);
                stmt.setInt(3, prodType);
                stmt.setInt(4, category);
                stmt.setInt(5, segment);
                stmt.setInt(6, isActive);
                stmt.setInt(7, id);
                stmt.executeUpdate();
            }
            
            stmt = transconn.prepareStatement(updateDescription);
            stmt.setString(1, boardName);
            stmt.setString(2, abv);
            stmt.setInt(3, bbtvCategory);
            stmt.setString(4, origin);
            stmt.setString(5, seasonality);
            stmt.setInt(6, id);
            stmt.executeUpdate();

            //Adding productSet information
            Iterator i = toHandle.elementIterator("productSetMap");
            addProductSetMap(id,i,toAppend);

        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
            close(rs);
        }

    }

    private void getCoolersXML(Element toAppend, ResultSet rs) throws SQLException {
        while (rs.next()) {
            Element CoolerE1 = toAppend.addElement("cooler");
            CoolerE1.addElement("coolerId").addText(String.valueOf(rs.getInt(1)));
            CoolerE1.addElement("name").addText(HandlerUtils.nullToEmpty(rs.getString(2)));
            CoolerE1.addElement("locationId").addText(String.valueOf(rs.getInt(3)));
            CoolerE1.addElement("zoneId").addText(String.valueOf(rs.getInt(4)));
            //NischaySharma_16-Jun-2009_Start: Changed the structure of response xml to send back
            //system and line also
            CoolerE1.addElement("system").addText(String.valueOf(rs.getInt(5)));
            CoolerE1.addElement("line").addText(String.valueOf(rs.getInt(6)));
            CoolerE1.addElement("alertPoint").addText(String.valueOf(rs.getInt(7)));
            //NischaySharma_16-Jun-2009_End
        }

    }

    private void getUnits(Element toHandle, Element toAppend) throws HandlerException {

        int platform = HandlerUtils.getOptionalInteger(toHandle, "platform");
        PreparedStatement stmt = null;
        ResultSet rs = null;
        if (platform < 1) {
            platform = 1;
        }
        try {
            String selectAll = "SELECT id, name, convValue FROM unit WHERE platform = ? ORDER BY name";
            stmt = transconn.prepareStatement(selectAll);
            stmt.setInt(1, platform);
            rs = stmt.executeQuery();
            while (rs.next()) {
                Element UnitE1 = toAppend.addElement("unit");
                UnitE1.addElement("unitId").addText(String.valueOf(rs.getInt(1)));
                UnitE1.addElement("name").addText(HandlerUtils.nullToEmpty(rs.getString(2)));
                UnitE1.addElement("convValue").addText(String.valueOf(rs.getFloat(3)));
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(rs);
            close(stmt);
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

    private void getKegLines(Element toHandle, Element toAppend) throws HandlerException {

        int id = HandlerUtils.getOptionalInteger(toHandle, "stationId");
        int coolerId = HandlerUtils.getOptionalInteger(toHandle, "coolerId");
        //NischaySharma_10-Jul-2009_Start: Added new request parameter locationId
        int locationId = HandlerUtils.getOptionalInteger(toHandle, "locationId");
        int zoneId = HandlerUtils.getOptionalInteger(toHandle, "zoneId");
        //NischaySharma_10-Jul-2009_End

        PreparedStatement stmt = null;
        ResultSet rs = null;

        try {
            if (coolerId >= 0) {
                String selectByCoolerId = "SELECT id, name, cooler " +
                        "FROM kegLine WHERE cooler=?";
                stmt = transconn.prepareStatement(selectByCoolerId);
                stmt.setInt(1, coolerId);
                rs = stmt.executeQuery();
                while (rs.next()) {
                    Element KegLineE1 = toAppend.addElement("kegLine");
                    KegLineE1.addElement("id").addText(String.valueOf(rs.getInt(1)));
                    KegLineE1.addElement("name").addText(HandlerUtils.nullToEmpty(rs.getString(2)));
                    KegLineE1.addElement("cooler").addText(String.valueOf(rs.getInt(3)));
                }

            } else if (id >= 0) {
                String selectByKegLineId = "SELECT id, name, cooler " +
                        "FROM kegLine WHERE id=?";
                stmt = transconn.prepareStatement(selectByKegLineId);
                stmt.setInt(1, id);
                rs = stmt.executeQuery();
                while (rs.next()) {
                    Element KegLineE1 = toAppend.addElement("kegLine");
                    KegLineE1.addElement("id").addText(String.valueOf(rs.getInt(1)));
                    KegLineE1.addElement("name").addText(HandlerUtils.nullToEmpty(rs.getString(2)));
                    KegLineE1.addElement("cooler").addText(String.valueOf(rs.getInt(3)));
                }

            } //NischaySharma_10-Jul-2009_Start: querying based on location id
            else if (locationId >= 0) {
                String selectByLocationId = " SELECT k.id, k.name, k.cooler FROM kegLine k " +
                        " LEFT JOIN cooler c on k.cooler = c.id " +
                        " WHERE c.location = ? ";
                stmt = transconn.prepareStatement(selectByLocationId);
                stmt.setInt(1, locationId);
                rs = stmt.executeQuery();
                while (rs.next()) {
                    Element KegLineE1 = toAppend.addElement("kegLine");
                    KegLineE1.addElement("id").addText(String.valueOf(rs.getInt(1)));
                    KegLineE1.addElement("name").addText(HandlerUtils.nullToEmpty(rs.getString(2)));
                    KegLineE1.addElement("cooler").addText(String.valueOf(rs.getInt(3)));
                }
            } else if (zoneId >= 0) {
                String selectByZoneId = " SELECT k.id, k.name, k.cooler FROM kegLine k " +
                        " LEFT JOIN cooler c on k.cooler = c.id " +
                        " WHERE c.zone = ? ";
                stmt = transconn.prepareStatement(selectByZoneId);
                stmt.setInt(1, zoneId);
                rs = stmt.executeQuery();
                while (rs.next()) {
                    Element KegLineE1 = toAppend.addElement("kegLine");
                    KegLineE1.addElement("id").addText(String.valueOf(rs.getInt(1)));
                    KegLineE1.addElement("name").addText(HandlerUtils.nullToEmpty(rs.getString(2)));
                    KegLineE1.addElement("cooler").addText(String.valueOf(rs.getInt(3)));
                }
            } //NischaySharma_10-Jul-2009_End
            else {
                String selectAll = "SELECT id, name, cooler " +
                        "FROM kegLine";
                stmt = transconn.prepareStatement(selectAll);
                rs = stmt.executeQuery();
                while (rs.next()) {
                    Element KegLineE1 = toAppend.addElement("kegLine");
                    KegLineE1.addElement("id").addText(String.valueOf(rs.getInt(1)));
                    KegLineE1.addElement("name").addText(HandlerUtils.nullToEmpty(rs.getString(2)));
                    KegLineE1.addElement("cooler").addText(String.valueOf(rs.getInt(3)));
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

    private void getCoolers(Element toHandle, Element toAppend) throws HandlerException {

        int id = HandlerUtils.getOptionalInteger(toHandle, "coolerId");
        int locationId = HandlerUtils.getOptionalInteger(toHandle, "locationId");
        int zoneId = HandlerUtils.getOptionalInteger(toHandle, "zoneId");

        PreparedStatement stmt = null;
        ResultSet rs = null;
        //NischaySharma_16-Jun-2009_Start: Changed the query to return system and line also
        try {
            if (locationId >= 0) {
                String selectByLocationId = "SELECT id, name, location, zone, system, line, alertPoint " +
                        "FROM cooler WHERE location=?";
                stmt = transconn.prepareStatement(selectByLocationId);
                stmt.setInt(1, locationId);
                rs = stmt.executeQuery();

                getCoolersXML(toAppend, rs);
            } else if (zoneId >= 0) {
                String selectByZoneId = "SELECT id, name, location, zone, system, line, alertPoint " +
                        "FROM cooler WHERE zone=?";
                stmt = transconn.prepareStatement(selectByZoneId);
                stmt.setInt(1, zoneId);
                rs = stmt.executeQuery();

                getCoolersXML(toAppend, rs);
            } else if (id >= 0) {
                String selectByCoolerId = "SELECT id, name, location, zone, system, line, alertPoint " +
                        "FROM cooler WHERE id=?";
                stmt = transconn.prepareStatement(selectByCoolerId);
                stmt.setInt(1, id);
                rs = stmt.executeQuery();

                getCoolersXML(toAppend, rs);
            } else {
                String selectAll = "SELECT id, name, location, zone, system, line, alertPoint " +
                        "FROM cooler";
                stmt = transconn.prepareStatement(selectAll);
                rs = stmt.executeQuery();

                getCoolersXML(toAppend, rs);
            }
            //NischaySharma_16-Jun-2009_End

        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(rs);
            close(stmt);
        }

    }

    private void getStationsXML(Element toAppend, ResultSet rs) throws SQLException {
        while (rs.next()) {
            Element BarE1                   = toAppend.addElement("station");
            BarE1.addElement("stationId").addText(String.valueOf(rs.getInt(1)));
            BarE1.addElement("name").addText(HandlerUtils.nullToEmpty(rs.getString(2)));
            BarE1.addElement("barId").addText(String.valueOf(rs.getInt(3)));
            BarE1.addElement("barName").addText(HandlerUtils.nullToEmpty(rs.getString(4)));
            BarE1.addElement("zoneId").addText(String.valueOf(rs.getInt(5)));
            BarE1.addElement("zoneName").addText(HandlerUtils.nullToEmpty(rs.getString(6)));
            BarE1.addElement("locationId").addText(String.valueOf(rs.getInt(7)));
            BarE1.addElement("locationName").addText(HandlerUtils.nullToEmpty(rs.getString(8)));
        }
    }

    private void getStations(Element toHandle, Element toAppend) throws HandlerException {

        int id                              = HandlerUtils.getOptionalInteger(toHandle, "stationId");
        int barId                           = HandlerUtils.getOptionalInteger(toHandle, "barId");
        int customerId                      = HandlerUtils.getOptionalInteger(toHandle, "customerId");
        int locationId                      = HandlerUtils.getOptionalInteger(toHandle, "locationId");
        int zoneId                          = HandlerUtils.getOptionalInteger(toHandle, "zoneId");
        int parameter                       = 0;
        PreparedStatement stmt              = null;
        ResultSet rs                        = null;
        String selectStations               = "SELECT st.id, st.name, b.id, b.name, z.id, z.name, lo.id, lo.name FROM station st LEFT JOIN bar b on st.bar = b.id " +
                                            " LEFT JOIN location lo on b.location = lo.id LEFT JOIN zone z ON z.id = b.zone ";
        try {
            if (id >= 0) {
                selectStations              += " WHERE st.id=? ";
                parameter                   = id;
            } else if (barId >= 0) {
                selectStations              += " WHERE b.id=? ";
                parameter                   = barId;
            } else if (zoneId > 0) {
                selectStations              += " WHERE z.id=? ";
                parameter                   = zoneId;
            } else if (locationId > 0) {
                selectStations              += " WHERE lo.id=? ";
                parameter                   = locationId;
            } else if (customerId > 0) {
                selectStations              += " WHERE lo.customer=? ";
                parameter                   = customerId;
            }
            stmt                            = transconn.prepareStatement(selectStations);
            stmt.setInt(1, parameter);
            rs                              = stmt.executeQuery();
            getStationsXML(toAppend, rs);
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(rs);
            close(stmt);
        }
    }

    private void getZonesXML(Element toAppend, ResultSet rs) throws SQLException {
        while (rs.next()) {
            Element BarE1 = toAppend.addElement("zone");

            BarE1.addElement("zoneId").addText(String.valueOf(rs.getInt(1)));
            BarE1.addElement("name").addText(HandlerUtils.nullToEmpty(rs.getString(2)));
            BarE1.addElement("locationId").addText(String.valueOf(rs.getInt(3)));
            BarE1.addElement("latitude").addText(String.valueOf(rs.getDouble(4)));
            BarE1.addElement("longitude").addText(String.valueOf(rs.getDouble(5)));

            //NischaySharma_09-Feb-2009_Start: Added tags to hold Zone Poly Point
            String selectZonePolyPoints = "SELECT Replace(AsText(zp.points), 'POLYGON', '') FROM zone_point zp WHERE zp.zone = ? and zp.new = 1;";
            PreparedStatement stmt = transconn.prepareStatement(selectZonePolyPoints);
            stmt.setInt(1, rs.getInt(1));
            ResultSet tempRS = stmt.executeQuery();
            Element zonePolyPoints = BarE1.addElement("zonePolyPoints");
            if (tempRS.next()) {
                zonePolyPoints.addElement("zonePoints").addText(HandlerUtils.nullToEmpty(tempRS.getString(1)));
            }

            //NischaySharma_09-Feb-2009_End
        }

    }

    private void getZones(Element toHandle, Element toAppend) throws HandlerException {

        int id = HandlerUtils.getOptionalInteger(toHandle, "zoneId");
        int refLocationId = HandlerUtils.getOptionalInteger(toHandle, "locationId");
        int refCustomerId = HandlerUtils.getOptionalInteger(toHandle, "customerId");

        int paramsSet = 0;
        if (id >= 0) {
            paramsSet++;
        }
        if (refLocationId >= 0) {
            paramsSet++;
        }
        if (refCustomerId >= 0) {
            paramsSet++;
        }
        if (paramsSet > 1) {
            throw new HandlerException("Only one parameter can be set for getZones.");
        }

        PreparedStatement stmt = null;
        ResultSet rs = null;

        try {
            if (refLocationId >= 0) {
                if (!checkForeignKey("location", "id", refLocationId)) {
                    throw new HandlerException("Foreign Key Not found : location " +
                            refLocationId);
                }
                String selectByLocationId = "SELECT id, name, location, latitude, longitude " +
                        "FROM zone WHERE location = ?";
                stmt = transconn.prepareStatement(selectByLocationId);
                stmt.setInt(1, refLocationId);
                rs = stmt.executeQuery();
                getZonesXML(toAppend, rs);
            } else if (refCustomerId >= 0) {
                if (!checkForeignKey("customer", "id", refCustomerId)) {
                    throw new HandlerException("Foreign Key Not found : customer " +
                            refCustomerId);
                }
                String selectByCustomerId = "SELECT zone.id, zone.name, zone.location, zone.latitude, zone.longitude " +
                        "FROM zone LEFT JOIN location ON location.id=zone.location " +
                        "LEFT JOIN customer ON location.customer = customer.id " +
                        "WHERE customer.id=?";
                stmt = transconn.prepareStatement(selectByCustomerId);
                stmt.setInt(1, refCustomerId);
                rs = stmt.executeQuery();
                getZonesXML(toAppend, rs);

            } else if (id >= 0) {
                String selectByZoneId = "SELECT id, name, location, latitude, longitude " +
                        "FROM zone WHERE id=?";
                stmt = transconn.prepareStatement(selectByZoneId);
                stmt.setInt(1, id);
                rs = stmt.executeQuery();

                getZonesXML(toAppend, rs);
            } else {
                String selectByLocationId = "SELECT id, name, location, latitude, longitude " +
                        "FROM zone";
                stmt = transconn.prepareStatement(selectByLocationId);
                rs = stmt.executeQuery();

                getZonesXML(toAppend, rs);
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
            ProductE1.addElement("segment").addText(String.valueOf(rs.getInt(colCount++)));
            ProductE1.addElement("isActive").addText(String.valueOf(rs.getBoolean(colCount++)));
            ProductE1.addElement("boardName").addText(HandlerUtils.nullToEmpty(rs.getString(colCount++)));
            ProductE1.addElement("abv").addText(HandlerUtils.nullToEmpty(rs.getString(colCount++)));
            ProductE1.addElement("bbtvCategory").addText(String.valueOf(rs.getInt(colCount++)));
            ProductE1.addElement("origin").addText(HandlerUtils.nullToEmpty(rs.getString(colCount++)));
            ProductE1.addElement("styleLogo").addText(HandlerUtils.nullToEmpty("http://beerboard.tv/USBN.BeerBoard.UI/Images/glass/"+rs.getString(colCount++)+".png"));
            ProductE1.addElement("brewLogo").addText(HandlerUtils.nullToEmpty("http://beerboard.tv/USBN.BeerBoard.UI/Images/logo/"+rs.getString(colCount++)));
            ProductE1.addElement("seasonality").addText(HandlerUtils.nullToEmpty(rs.getString(colCount++)));
        }
    }

    private void getProducts1(Element toHandle, Element toAppend) throws HandlerException {

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
        String select = "SELECT p.id, p.name, p.category, p.segment, p.isActive, " +
                        " pD.boardName, pD.abv, pD.category, pD.origin, sL.logo, CONCAT(bL.logo, IF(bL.type = 0, '.png', IF(bL.type = 1, '.jpg', '.gif'))),  pD.seasonality " +
                        " FROM product p LEFT JOIN productDescription pD ON pD.product = p.id " +
                        " LEFT JOIN productSetMap sPSM ON sPSM.product = pD.product LEFT JOIN productSet sPS ON sPS.id = sPSM.productSet " +
                        " LEFT JOIN styleLogo sL ON sL.style = sPS.name  " +
                        " LEFT JOIN productSetMap pSM ON pSM.product = pD.product LEFT JOIN productSet pS ON pS.id = pSM.productSet " +
                        "LEFT JOIN breweryLogo bL ON bL.brewery = pS.name ";
        


        try {

            if (id > 0) {
                //NischaySharma_24-Jun-2010_Start
                //NischaySharma_02-Mar-2010_Start
                String selectById = select + " WHERE pS.productSetType = 7 AND sPS.productSetType = 9 AND p.isActive = 1 AND p.id = ?";
                stmt = transconn.prepareStatement(selectById);
                stmt.setInt(1, id);
                rs = stmt.executeQuery();
                getProducts1XML(toAppend, rs);
            } //NischaySharma_08-May-2009_Start: Added check if that if name is empty string then
            //do not execue the query for name
            else if (brewery > 0) {
                //NischaySharma_24-Jun-2010_Start
                //NischaySharma_02-Mar-2010_Start
                String selectByBrewery = select +  " WHERE pS.productSetType = 7 AND sPS.productSetType = 9 AND p.isActive = 1 AND pSM.productSet = ?";
                stmt = transconn.prepareStatement(selectByBrewery);
                stmt.setInt(1, brewery);
                rs = stmt.executeQuery();
                getProducts1XML(toAppend, rs);
            } //NischaySharma_08-May-2009_Start: Added check if that if name is empty string then
            //do not execue the query for name
            else if (null != name && !name.equals("")) {
                String selectByName = select + "WHERE pS.productSetType = 7 AND sPS.productSetType = 9 AND p.isActive = 1 AND p.name LIKE ?";
                name = '%' + name + '%';
                stmt = transconn.prepareStatement(selectByName);
                stmt.setString(1, name);
                rs = stmt.executeQuery();
                getProducts1XML(toAppend, rs);
            } //NischaySharma_08-May-2009_End
            else {
                //logger.debug("SelectAll");
                String selectAll = select + " WHERE pS.productSetType = 7 AND sPS.productSetType = 9 AND p.isActive = 1 AND p.id > 0 AND p.pType = ?";
                //NischaySharma_24-Jun-2010_Start
                //NischaySharma_02-Mar-2010_End
                stmt = transconn.prepareStatement(selectAll);
                stmt.setInt(1, prodType);
                rs = stmt.executeQuery();
                getProducts1XML(toAppend, rs);
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
    private void addBeverageSize1(Element toHandle, Element toAppend) throws HandlerException {

        boolean forCustomer                 = HandlerUtils.getOptionalBoolean(toHandle, "forCustomer");
        int location                        = 0;
        int callerId                        = getCallerId(toHandle);

        PreparedStatement stmt              = null;
        ResultSet rs                        = null;

        logger.portalAction("addBeverageSize");
        String selectLocations = " SELECT id FROM location WHERE customer = ? ";
        try {
            if (forCustomer) {
                int customerId              = HandlerUtils.getRequiredInteger(toHandle, "customerId");  
                stmt                    = transconn.prepareStatement("SELECT id FROM customer WHERE id = ? AND groupId = 2");
                stmt.setInt(1, customerId);
                rs                      = stmt.executeQuery();
                if (rs.next()) {
                    selectLocations     = "SELECT id FROM location WHERE customer IN (SELECT id FROM customer WHERE groupId = ?)";
                    customerId          = 2;
                }
                stmt                        = transconn.prepareStatement(selectLocations);
                stmt.setInt(1, customerId);
                rs                          = stmt.executeQuery();
                while (rs.next()) {
                    location                = rs.getInt(1);
                    addBeverageSizeDetail(callerId, location, toHandle, toAppend);
                }
            } else {
                location                    = HandlerUtils.getRequiredInteger(toHandle, "locationId");
                addBeverageSizeDetail(callerId, location, toHandle, toAppend);
            }
        } catch (SQLException sqle) {
            logger.dbError("SQL Exception in updateBeverageSize: " + sqle.toString());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
            close(rs);
        }
    }


    private void addBeverageSizeDetail(int callerId, int location, Element toHandle, Element toAppend) throws HandlerException {

        String checkName =
                "SELECT id FROM beverageSize WHERE location=? AND LCASE(name)=? AND pType=?";
        String checkSize =
                "SELECT id FROM beverageSize WHERE location=? AND ounces BETWEEN (?-0.05) AND (?+0.05) AND pType=?";
        String insert =
                "INSERT INTO beverageSize (location,name,ounces,pType) VALUES (?,?,?,?)";

        PreparedStatement stmt = null;
        ResultSet rs = null, rs2 = null;

        DecimalFormat twoPlaces = new DecimalFormat("0.00");

        logger.portalAction("addBeverageSize1");

        try {
            Iterator sizes = toHandle.elementIterator("size");
            while (sizes.hasNext()) {
                Element sizeEl = (Element) sizes.next();

                String name = HandlerUtils.getRequiredString(sizeEl, "name");
                float ounces = HandlerUtils.getRequiredFloat(sizeEl, "ounces");
                int prodType = HandlerUtils.getRequiredInteger(toHandle, "prodID");
                //check params
                stmt = transconn.prepareStatement(checkName);
                stmt.setInt(1, location);
                stmt.setString(2, name.toLowerCase());
                stmt.setInt(3, prodType);
                rs = stmt.executeQuery();
                stmt = transconn.prepareStatement(checkSize);
                stmt.setInt(1, location);
                stmt.setFloat(2, ounces);
                stmt.setFloat(3, ounces);
                stmt.setInt(4, prodType);
                rs2 = stmt.executeQuery();
                if (rs.next()) {
                    addErrorDetail(toAppend, "The name '" + name + "' already exists at this location.");
                } else if (rs2.next()) {
                    addErrorDetail(toAppend, "The size '" + ounces + "' already exists at this location.");
                } else if (ounces < 0.1) {
                    addErrorDetail(toAppend, "The size '" + ounces + "' is not allowed.");
                } else {
                    // OK to add
                    stmt = transconn.prepareStatement(insert);
                    stmt.setInt(1, location);
                    stmt.setString(2, name);
                    stmt.setFloat(3, ounces);
                    stmt.setInt(4, prodType);
                    stmt.executeUpdate();

                    // Log the action
                    stmt = transconn.prepareStatement("SELECT LAST_INSERT_ID()");
                    rs = stmt.executeQuery();
                    if (rs.next()) {
                        int newId = rs.getInt(1);
                        String logMessage = "Adding a beverage size named '" + name + "' " +
                                "(" + twoPlaces.format(ounces) + " oz)";
                        logger.portalDetail(callerId, "addBeverageSize", location, "beverageSize", newId, logMessage, transconn);
                    }

                }
            }
        } catch (SQLException sqle) {
            logger.dbError("SQL Exception in addBeverageSize: " + sqle.toString());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
            close(rs);
        }
    }

    /**  Gets all beverages at a location
     *  This method now has an optional 'barId' param 
     *  If this is set, then only beverage matching this bar will be returned.  If this
     *  field is NOT set, then all beverages will be returned, regardless if they
     *  are associated with a specific bar or null
     *
     *  BarId 'b' supplied   =  Only beverages of bar 'b'
     *  BarId 'b' null       =  All beverages, including bar=null and bar='x'
     */
    private void getBeverages1(Element toHandle, Element toAppend)
            throws HandlerException {
        int location = HandlerUtils.getRequiredInteger(toHandle, "locationId");
        int barId = HandlerUtils.getOptionalInteger(toHandle, "barId");
        int prodType = HandlerUtils.getRequiredInteger(toHandle, "prodID");
        String selectBeverages = "SELECT id, name,plu,id,simple FROM beverage WHERE location=? AND pType=? " + (barId > 0 ? " AND bar=?" : "");
        String selectIngredients = "SELECT product.name, product.id, ingredient.ounces " +
                "FROM ingredient LEFT JOIN product on ingredient.product = product.id " +
                "WHERE ingredient.beverage=?";


        PreparedStatement stmt = null;
        ResultSet bev = null;
        ResultSet ing = null;
        ResultSet comp = null;
        try {
            stmt = transconn.prepareStatement(selectBeverages);
            stmt.setInt(1, location);
            stmt.setInt(2, prodType);
            if (barId > 0) {
                stmt.setInt(3, barId);
            }
            bev = stmt.executeQuery();
            while (bev.next()) {
                Element beverage = toAppend.addElement("beverage");
                beverage.addElement("id").addText(HandlerUtils.nullToEmpty(bev.getString(1)));
                beverage.addElement("name").addText(HandlerUtils.nullToEmpty(bev.getString(2)));
                beverage.addElement("plu").addText(HandlerUtils.nullToEmpty(bev.getString(3)));
                beverage.addElement("simple").addText(HandlerUtils.nullToEmpty(bev.getString(5)));
                stmt = transconn.prepareStatement(selectIngredients);
                stmt.setInt(1, bev.getInt(4));
                ing = stmt.executeQuery();
                while (ing.next()) {
                    Element ingredient = beverage.addElement("ingredient");
                    ingredient.addElement("name").addText(HandlerUtils.nullToEmpty(ing.getString(1)));
                    ingredient.addElement("id").addText(HandlerUtils.nullToEmpty(ing.getString(2)));
                    ingredient.addElement("ounces").addText(HandlerUtils.nullToEmpty(ing.getString(3)));

                }

            }


        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(ing);
            close(bev);
            close(stmt);
        }


    }

    private void getComplexPlu(Element toHandle, Element toAppend)
            throws HandlerException {
        int location = HandlerUtils.getRequiredInteger(toHandle, "locationId");
        String name = HandlerUtils.getRequiredString(toHandle, "name");
        int prodType = HandlerUtils.getRequiredInteger(toHandle, "prodID");
        String selectComplexPlu = "SELECT id, plu FROM beverage WHERE name = ? and location=? and pType=?";


        PreparedStatement stmt = null;
        ResultSet comp = null;
        try {
            stmt = transconn.prepareStatement(selectComplexPlu);
            stmt.setString(1, name);
            stmt.setInt(2, location);
            stmt.setInt(3, prodType);
            comp = stmt.executeQuery();
            while (comp.next()) {
                Element compPlu = toAppend.addElement("complexPlu");
                compPlu.addElement("id").addText(HandlerUtils.nullToEmpty(comp.getString(1)));
                compPlu.addElement("plu").addText(HandlerUtils.nullToEmpty(comp.getString(2)));


            }

        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(comp);
            close(stmt);
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
                logger.portalDetail(callerId, "deleteBeverageSize1", location, "beverageSize", toKill, logMessage, transconn);
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

    /**  Add a new "Beverage" or a recipe with a plu.  This method now includes
     *  an optional "barId"  If this is set, the beverage will be associated with this
     *  bar.  If not, the beverage will have a 'null' bar field.  
     *
     *  <beverage>
     *      <plu>"String"
     *      <name>"String"
     *      <simple>*OPT* 1|0
     *      <ingredient>
     *          <product>000
     *          <quantity>0.00
     *      </ingredient>
     *      <ingredent>...</ingredient>
     *  </beverage>
     *  <beverage>...</beverage>
     *
     */
    private void addBeverage1(Element toHandle, Element toAppend) throws HandlerException {

        int location = HandlerUtils.getRequiredInteger(toHandle, "location");
        int bar = HandlerUtils.getOptionalInteger(toHandle, "barId");
        int prodType = HandlerUtils.getRequiredInteger(toHandle, "prodID");
        int callerId = getCallerId(toHandle);

        String checkPlu =
                " SELECT bev.id, bar.name FROM beverage bev LEFT JOIN bar ON bev.bar=bar.id " +
                " WHERE bev.location=? AND bev.plu=? LIMIT 1";
        String getLastId =
                " SELECT LAST_INSERT_ID()";
        String insertBev = (bar > 0 ? " INSERT INTO beverage (name, location, plu, simple, bar, pType) VALUES (?,?,?,?,?,?) "
                : " INSERT INTO beverage (name, location, plu, simple, pType) VALUES (?,?,?,?,?)");
        String updateBeverageSize = "UPDATE beverage SET ounces = ? WHERE id = ? ";
        String insertIng =
                " INSERT INTO ingredient (beverage, product, ounces) " +
                " VALUES (?,?,?)";

        PreparedStatement stmt = null;
        ResultSet rs = null;

        logger.portalAction("addBeverage1");

        try {
            Iterator bevs = toHandle.elementIterator("beverage");
            while (bevs.hasNext()) {
                Element beverage = (Element) bevs.next();
                String plu = HandlerUtils.getRequiredString(beverage, "plu");
                String name = HandlerUtils.getRequiredString(beverage, "name");
                int simple = HandlerUtils.getOptionalInteger(beverage, "simple");
                if (simple < 0) {
                    simple = 1;
                }

                //check PLU
                stmt = transconn.prepareStatement(checkPlu);
                stmt.setInt(1, location);
                stmt.setString(2, plu);
                rs = stmt.executeQuery();
                if (rs.next()) {
                    String barName = rs.getString(2);
                    String detailMessage = "The PLU '" + plu + "' already exists";
                    if (barName != null && barName.length() > 0) {
                        detailMessage += " (" + barName + ")";
                    }
                    addErrorDetail(toAppend, detailMessage);
                    logger.debug("New PLU " + plu + " exists in the db, NOT adding.");
                } else {

                    stmt = transconn.prepareStatement(insertBev);
                    stmt.setString(1, name);
                    stmt.setInt(2, location);
                    stmt.setString(3, plu);
                    stmt.setInt(4, simple);
                    if (bar > 0) {
                        stmt.setInt(5, bar);
                        stmt.setInt(6, prodType);
                    } else {
                        stmt.setInt(5, prodType);
                    }

                    stmt.executeUpdate();

                    int beverageId = -1;
                    stmt = transconn.prepareStatement(getLastId);
                    rs = stmt.executeQuery();
                    if (rs.next()) {
                        beverageId = rs.getInt(1);
                    } else {
                        logger.dbError("DB Error, mysql function last_insert_id failed");
                        throw new HandlerException("Database Error");
                    }

                    String logMessage = "Added plu#" + plu;
                    logger.portalDetail(callerId, "addBeverage", location, "beverage", beverageId, logMessage, transconn);

                    double totalQuantity = 0.0;
                    Iterator ings = beverage.elementIterator("ingredient");
                    while (ings.hasNext()) {
                        Element ingredient = (Element) ings.next();

                        String quantityString = HandlerUtils.getRequiredString(ingredient, "quantity");
                        double quantity = 0.0;
                        try {
                            quantity = Double.parseDouble(quantityString);
                        } catch (NumberFormatException nfe) {
                            throw new HandlerException("Quantity (" + quantityString + ") must be a double");
                        }
                        int product = HandlerUtils.getRequiredInteger(ingredient, "product");

                        stmt = transconn.prepareStatement(insertIng);

                        stmt.setInt(1, beverageId);
                        stmt.setInt(2, product);
                        stmt.setDouble(3, quantity);
                        stmt.executeUpdate();
                        totalQuantity += quantity;
                    }

                    stmt = transconn.prepareStatement(updateBeverageSize);
                    stmt.setDouble(1, totalQuantity);
                    stmt.setInt(2, beverageId);
                    stmt.executeUpdate();
                }
            }
        } catch (SQLException sqle) {
            logger.dbError("SQL Exception: " + sqle.toString());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
            close(rs);
        }
    }

    /**  Copy every PLU recipe and beverage size from one location to another
     *  The caller ID will be checked for permissions: needs read access for the source
     *  and write access to the destination. This method now includes
     *  an optional "barId"  If this is set, the beverage will be associated with this
     *  bar.  If not, the beverage will have a 'null' bar field. 
     *
     *  Duplicate sizes or plus will NOT overwrite existing records
     */
    private void importBeverages1(Element toHandle, Element toAppend) throws HandlerException {
        int fromLoc                         = HandlerUtils.getRequiredInteger(toHandle, "importFrom");
        int toLoc                           = HandlerUtils.getRequiredInteger(toHandle, "importTo");
        int bar                             = HandlerUtils.getOptionalInteger(toHandle, "barId");
        int prodType1                       = HandlerUtils.getRequiredInteger(toHandle, "prodID");
        int callerId                        = getCallerId(toHandle);

        DecimalFormat twoPlaces             = new DecimalFormat("0.00");

        String getInventory                 = " SELECT product, plu, minimumQty, qtyToHave, kegSize, brixWater, brixSyrup FROM inventory WHERE location = ? ";
        String getSizes                     = " SELECT name, ounces, pType FROM beverageSize WHERE location=? and pType=?";
        String getBevs                      = " SELECT id, name, plu, ounces, simple, pType FROM beverage WHERE location=? and pType=?";
        String getExistingPlus              = " SELECT DISTINCT plu FROM beverage WHERE location=?";

        String getLastId                    = " SELECT LAST_INSERT_ID()";

        String insertInventory              = " INSERT INTO inventory (location, product, minimumQty, plu, qtyToHave, kegSize, brixWater, brixSyrup) " +
                                            " VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
        String insertSize                   = " INSERT INTO beverageSize (name, ounces, location, pType) VALUES (?, ?, ?, ?)";
        String insertBev                    = (bar > 0 ? " INSERT INTO beverage (name, plu, ounces, simple, location, pType, bar) VALUES (?, ?, ?, ?, ?, ?, ?) "
                                            : " INSERT INTO beverage (name, plu, ounces, simple, location, pType) VALUES (?, ?, ?, ?, ?, ?) ");
        String insertIng                    = " INSERT INTO ingredient (beverage,product,ounces) " +
                                            " SELECT ?, product, ounces FROM ingredient WHERE beverage=?";

        PreparedStatement stmt              = null, insertInvStmt = null, insertIngStmt = null, insertBevStmt = null, lastIdStmt = null, insertSizeStmt = null;
        ResultSet rs                        = null, inventory = null, sizes = null, bevs = null;

        Set<Integer> existingInventory      = new HashSet<Integer>();
        Set<String> existingSizeNames       = new HashSet<String>();
        Set<String> existingSizeOunces      = new HashSet<String>();
        Set<String> existingPlus            = new HashSet<String>();

        try {
            if (WebPermission.permissionAt(callerId, fromLoc, transconn).canRead() && WebPermission.permissionAt(callerId, toLoc, transconn).canWrite()) {

                logger.debug("Importing Beverages from L#" + fromLoc + " to L#" + toLoc);
                insertInvStmt               = transconn.prepareStatement(insertInventory);
                insertSizeStmt              = transconn.prepareStatement(insertSize);
                insertBevStmt               = transconn.prepareStatement(insertBev);
                insertIngStmt               = transconn.prepareStatement(insertIng);
                lastIdStmt                  = transconn.prepareStatement(getLastId);

                // Get Existing Inventory
                stmt                        = transconn.prepareStatement(getInventory);
                stmt.setInt(1, toLoc);
                rs                          = stmt.executeQuery();
                while (rs.next()) {
                    existingInventory.add(rs.getInt(1));
                }
                logger.debug("Checking " + existingInventory.size() + " existing inventory");

                //re-using stmt for getInventory
                stmt.setInt(1, fromLoc);
                inventory                   = stmt.executeQuery();
                int insertCount             = 0, failCount = 0, newId = 0;

                // Copy Inventory
                while (inventory.next()) {
                    int productId           = inventory.getInt(1);
                    if (!existingInventory.contains(productId)) {
                        //insertInventory (name,ounces,location)
                        insertInvStmt.setInt(1, toLoc);
                        insertInvStmt.setInt(2, productId);
                        insertInvStmt.setString(3, inventory.getString(2));
                        insertInvStmt.setDouble(4, inventory.getDouble(3));
                        insertInvStmt.setDouble(5, inventory.getDouble(4));
                        insertInvStmt.setInt(6, inventory.getInt(5));
                        insertInvStmt.setInt(7, inventory.getInt(6));
                        insertInvStmt.setInt(8, inventory.getInt(7));
                        insertInvStmt.executeUpdate();
                        insertCount++;

                        rs                  = lastIdStmt.executeQuery();
                        if (rs.next()) {
                            newId           = rs.getInt(1);
                        }
                        String logMessage   = "Added product " + productId + " to inventory";
                        logger.portalDetail(callerId, "addTestInventory", toLoc, "inventory", newId, logMessage, transconn);
                    }
                }
                logger.debug("Added " + insertCount + " inventory OK");

                // Get Existing Sizes
                stmt                        = transconn.prepareStatement(getSizes);
                stmt.setInt(1, toLoc);
                stmt.setInt(2, prodType1);
                rs                          = stmt.executeQuery();
                while (rs.next()) {
                    existingSizeNames.add(rs.getString(1).toLowerCase());
                    existingSizeOunces.add(twoPlaces.format(rs.getDouble(2)));
                }
                logger.debug("Checking " + existingSizeNames.size() + " existing sizes");
                //re-using stmt for getSizes
                stmt.setInt(1, fromLoc);
                sizes                   = stmt.executeQuery();
                insertCount             = 0;
                failCount               = 0;
                newId                   = 0;

                // Copy Beverage Sizes
                while (sizes.next()) {
                    String sizeName     = sizes.getString(1);
                    double ounces       = sizes.getDouble(2);
                    int pType           = sizes.getInt(3);
                    if (existingSizeNames.contains(sizeName.toLowerCase())) {
                        addErrorDetail(toAppend, "The size '" + sizeName + "' already exists");
                        failCount++;
                    } else if (existingSizeOunces.contains(twoPlaces.format(ounces))) {
                        addErrorDetail(toAppend, "The size '" + twoPlaces.format(ounces) + " oz' already exists");
                        failCount++;
                    } else {
                        //insertSize (name,ounces,location)
                        insertSizeStmt.setString(1, sizeName);
                        insertSizeStmt.setDouble(2, ounces);
                        insertSizeStmt.setInt(3, toLoc);
                        insertSizeStmt.setInt(4, pType);
                        insertSizeStmt.executeUpdate();
                        insertCount++;

                        rs                  = lastIdStmt.executeQuery();
                        if (rs.next()) {
                            newId           = rs.getInt(1);
                        }
                        String logMessage   = "Adding a beverage size named '" + sizeName + "' " + "(" + twoPlaces.format(ounces) + " oz)";
                        logger.portalDetail(callerId, "addBeverageSize", toLoc, "beverageSize", newId, logMessage, transconn);
                    }
                }
                logger.debug("Added " + insertCount + " sizes OK" + (failCount > 0 ? ", failed: " + failCount : ""));

                // Get Existing PLUS
                stmt                        = transconn.prepareStatement(getExistingPlus);
                stmt.setInt(1, toLoc);
                rs                          = stmt.executeQuery();
                while (rs.next()) {
                    existingPlus.add(rs.getString(1));
                }
                logger.debug("Checking " + existingPlus.size() + " existing plus");

                stmt                        = transconn.prepareStatement(getBevs);
                stmt.setInt(1, fromLoc);
                stmt.setInt(2, prodType1);
                bevs                        = stmt.executeQuery();

                insertCount                 = 0;
                failCount                   = 0;
                newId                       = 0;
                // Copy all the beverages
                while (bevs.next()) {
                    //id,name,plu,simple
                    int bevId               = bevs.getInt(1);
                    String bevName          = bevs.getString(2);
                    String plu              = bevs.getString(3);
                    Double size             = bevs.getDouble(4);
                    int simple              = bevs.getInt(5);
                    int prodType            = bevs.getInt(6);
                    if (!existingPlus.contains(plu)) {
                        insertBevStmt.setString(1, bevName);
                        insertBevStmt.setString(2, plu);
                        insertBevStmt.setDouble(3, size);
                        insertBevStmt.setInt(4, simple);
                        insertBevStmt.setInt(5, toLoc);
                        insertBevStmt.setInt(6, prodType);
                        if (bar > 0) {
                            insertBevStmt.setInt(7, bar);
                        }
                        insertBevStmt.executeUpdate();

                        rs                  = lastIdStmt.executeQuery();
                        if (rs.next()) {
                            newId           = rs.getInt(1);
                        }
                        insertIngStmt.setInt(1, newId);
                        insertIngStmt.setInt(2, bevId);
                        insertIngStmt.executeUpdate();
                        insertCount++;

                        String logMessage   = "Added plu#" + plu;
                        logger.portalDetail(callerId, "addBeverage", toLoc, "beverage", newId, logMessage, transconn);
                    } else {
                        addErrorDetail(toAppend, "The plu '" + plu + "' already exists");
                        failCount++;
                    }
                }
                logger.debug("Added " + insertCount + " beverages OK" + (failCount > 0 ? ", failed: " + failCount : ""));
            } else {
                addErrorDetail(toAppend, "You don't have permission to perform this import");
            }
        } catch (SQLException sqle) {
            logger.dbError("SQL Exception in importBeverages1: " + sqle.toString());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
            close(insertInvStmt);
            close(insertBevStmt);
            close(insertIngStmt);
            close(lastIdStmt);
            close(rs);
            close(bevs);
            close(sizes);
            close(inventory);
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

    /**  Add a new normal user with Read Only access to one location.
     *   <fullName>"String"
     *   <username>"String"
     *   <password>"String" SHA-1 encoded
     *   <email>"String"
     *   <locationId>000
     */
    private void addTestUser(Element toHandle, Element toAppend) throws HandlerException {
        final int READ_ONLY_ACCESS = 7;

        String fullName = HandlerUtils.getRequiredString(toHandle, "fullName");
        String username = HandlerUtils.getRequiredString(toHandle, "username");
        String password = HandlerUtils.getRequiredString(toHandle, "password");
        String email = HandlerUtils.getRequiredString(toHandle, "email");
        String mobile = HandlerUtils.getRequiredString(toHandle, "mobile");
        String carrier = HandlerUtils.getRequiredString(toHandle, "carrier");
        int location = HandlerUtils.getRequiredInteger(toHandle, "locationId");
        int callerId = getCallerId(toHandle);


        String checkUser =
                " SELECT id FROM user WHERE username=? LIMIT 1";
        String getCustomer =
                " SELECT l.customer, c.groupId FROM location l LEFT JOIN customer c ON c.id = l.customer WHERE l.id=?";
        String insertUser =
                " INSERT INTO user (name,username,password,customer,groupId,email,mobile,carrier) " +
                " VALUES (?,?,?,?,?,?,?,?) ";
        String getLastId =
                " SELECT LAST_INSERT_ID()";
        String insertMap =
                " INSERT INTO userMap (user,location,securityLevel) " +
                " VALUES (?,?,?) ";

        PreparedStatement stmt = null;
        ResultSet rs = null;

        logger.portalAction("addTestUser");

        try {
            //Check that the username doesn't already exist
            stmt = transconn.prepareStatement(checkUser);
            stmt.setString(1, username);
            rs = stmt.executeQuery();
            if (rs.next()) {
                logger.debug("Username already exists");
                addErrorDetail(toAppend, "Username already exists");
            } else {
                int customer = -1, groupId = -1;
                stmt = transconn.prepareStatement(getCustomer);
                stmt.setInt(1, location);
                rs = stmt.executeQuery();

                if (rs.next()) {
                    customer = rs.getInt(1);
                    groupId = rs.getInt(2);
                } else {
                    throw new HandlerException("Location has no associated customer");
                }

                // add the user record
                stmt = transconn.prepareStatement(insertUser);
                stmt.setString(1, fullName);
                stmt.setString(2, username);
                stmt.setString(3, password);
                stmt.setInt(4, customer);
                stmt.setInt(5, groupId);
                stmt.setString(6, email);
                stmt.setString(7, mobile);
                stmt.setString(8, carrier);
                stmt.executeUpdate();

                // get the id of the user we added
                stmt = transconn.prepareStatement(getLastId);
                rs = stmt.executeQuery();
                int userDbId;
                if (rs.next()) {
                    userDbId = rs.getInt(1);
                } else {
                    logger.dbError("SQL Last_Insert_Id FAILED in addTestUser ");
                    throw new HandlerException("database error");
                }

                String logMessage = "Added user " + fullName + " as '" + username + "'";
                logger.portalDetail(callerId, "addTestUser", 0, "user", userDbId, logMessage, transconn);

                // add read-only access for this location into the user map
                stmt = transconn.prepareStatement(insertMap);
                stmt.setInt(1, userDbId);
                stmt.setInt(2, location);
                stmt.setInt(3, READ_ONLY_ACCESS);
                stmt.executeUpdate();

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

    /** Return all customer-managers for a specific customer
     */
    private void getTestAdminUsers(Element toHandle, Element toAppend) throws HandlerException {

        int customerId                      = HandlerUtils.getRequiredInteger(toHandle, "customerId");

        String getGroupId                   = " SELECT groupId FROM customer WHERE id=? ";
        PreparedStatement stmt              = null;
        ResultSet rs                        = null;

        try {
            int parameter                   = -1;
            ArrayList<Integer> userIds      = new ArrayList<Integer>();
            String listSelection            = "customer";
            
            stmt                            = transconn.prepareStatement(getGroupId);
            stmt.setInt(1, customerId);
            rs                              = stmt.executeQuery();
            if (rs.next() && (rs.getInt(1) > 0)) {
                parameter                   = rs.getInt(1);
                listSelection               = "groupId";
            } else {
                parameter                   = customerId;
            }

            String getAdmins                = " SELECT id,name,email FROM user WHERE " + listSelection + "=? AND isManager=1 ORDER BY name, username ";
            stmt                            = transconn.prepareStatement(getAdmins);
            stmt.setInt(1, parameter);
            rs                              = stmt.executeQuery();
            while (rs.next()) {
                int userId                  = rs.getInt(1);
                if (userIds.contains(userId)) {
                    continue;
                } else {
                    userIds.add(userId);
                }
                Element userEl              = toAppend.addElement("user");
                userEl.addElement("userId").addText(String.valueOf(userId));
                userEl.addElement("fullName").addText(HandlerUtils.nullToEmpty(rs.getString(2)));
                userEl.addElement("email").addText(HandlerUtils.nullToEmpty(rs.getString(3)));
            }

            String getITAdmins              = " SELECT id,name,email FROM user WHERE " + listSelection + "=? AND isITAdmin=1 ORDER BY name, username ";
            stmt                            = transconn.prepareStatement(getITAdmins);
            stmt.setInt(1, parameter);
            rs                              = stmt.executeQuery();
            while (rs.next()) {
                int userId                  = rs.getInt(1);
                if (userIds.contains(userId)) {
                    continue;
                } else {
                    userIds.add(userId);
                }
                Element userEl              = toAppend.addElement("ITuser");
                userEl.addElement("userId").addText(String.valueOf(userId));
                userEl.addElement("fullName").addText(HandlerUtils.nullToEmpty(rs.getString(2)));
                userEl.addElement("email").addText(HandlerUtils.nullToEmpty(rs.getString(3)));
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(rs);
            close(stmt);
        }
    }

    private void getShifts(Element toHandle, Element toAppend)
            throws HandlerException {
        int location = HandlerUtils.getRequiredInteger(toHandle, "location");
        String select = "SELECT preOpenSun, openSun, closeSun, preOpenMon, openMon, closeMon, preOpenTue, openTue, closeTue, preOpenWed, openWed, closeWed, preOpenThu, openThu, closeThu, preOpenFri, openFri, closeFri, preOpenSat, openSat, closeSat FROM locationHours WHERE location=?";

        PreparedStatement stmt = null;
        ResultSet rs = null;

        try {
            stmt = transconn.prepareStatement(select);
            stmt.setInt(1, location);
            rs = stmt.executeQuery();
            while (rs.next()) {

                toAppend.addElement("preOpenSun").addText(HandlerUtils.nullToEmpty(rs.getString(1)));
                toAppend.addElement("openSun").addText(HandlerUtils.nullToEmpty(rs.getString(2)));
                toAppend.addElement("closeSun").addText(HandlerUtils.nullToEmpty(rs.getString(3)));
                toAppend.addElement("preOpenMon").addText(HandlerUtils.nullToEmpty(rs.getString(4)));
                toAppend.addElement("openMon").addText(HandlerUtils.nullToEmpty(rs.getString(5)));
                toAppend.addElement("closeMon").addText(HandlerUtils.nullToEmpty(rs.getString(6)));
                toAppend.addElement("preOpenTue").addText(HandlerUtils.nullToEmpty(rs.getString(7)));
                toAppend.addElement("openTue").addText(HandlerUtils.nullToEmpty(rs.getString(8)));
                toAppend.addElement("closeTue").addText(HandlerUtils.nullToEmpty(rs.getString(9)));
                toAppend.addElement("preOpenWed").addText(HandlerUtils.nullToEmpty(rs.getString(10)));
                toAppend.addElement("openWed").addText(HandlerUtils.nullToEmpty(rs.getString(11)));
                toAppend.addElement("closeWed").addText(HandlerUtils.nullToEmpty(rs.getString(12)));
                toAppend.addElement("preOpenThu").addText(HandlerUtils.nullToEmpty(rs.getString(13)));
                toAppend.addElement("openThu").addText(HandlerUtils.nullToEmpty(rs.getString(14)));
                toAppend.addElement("closeThu").addText(HandlerUtils.nullToEmpty(rs.getString(15)));
                toAppend.addElement("preOpenFri").addText(HandlerUtils.nullToEmpty(rs.getString(16)));
                toAppend.addElement("openFri").addText(HandlerUtils.nullToEmpty(rs.getString(17)));
                toAppend.addElement("closeFri").addText(HandlerUtils.nullToEmpty(rs.getString(18)));
                toAppend.addElement("preOpenSat").addText(HandlerUtils.nullToEmpty(rs.getString(19)));
                toAppend.addElement("openSat").addText(HandlerUtils.nullToEmpty(rs.getString(20)));
                toAppend.addElement("closeSat").addText(HandlerUtils.nullToEmpty(rs.getString(21)));

            }

        } catch (SQLException sqle) {
            logger.dbError("Database error in getBeverageSizes: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
            close(rs);
        }

    }

    /**  Obtain a list of the user-defined beverage sizes for a location
     *
     *  required arguments:
     *  <locationId>
     *
     *  returns:
     *  <size>
     *    <id>int</id>
     *    <name>String</name>
     *    <ounces>00.0</ounces>
     *  </size>
     *  <size>...</size>
     *
     */
    private void getBeverageSizes1(Element toHandle, Element toAppend)
            throws HandlerException {
        int location = HandlerUtils.getRequiredInteger(toHandle, "locationId");
        int prodType = HandlerUtils.getRequiredInteger(toHandle, "prodID");
        String select = "SELECT id,name,ounces FROM beverageSize WHERE location=? and pType=?";

        PreparedStatement stmt = null;
        ResultSet rs = null;
        boolean hasSize = false;
        try {
            stmt = transconn.prepareStatement(select);
            stmt.setInt(1, location);
            stmt.setInt(2, prodType);
            rs = stmt.executeQuery();
            while (rs.next()) {
                Element size = toAppend.addElement("size");
                size.addElement("id").addText(HandlerUtils.nullToEmpty(rs.getString(1)));
                size.addElement("name").addText(HandlerUtils.nullToEmpty(rs.getString(2)));
                size.addElement("ounces").addText(HandlerUtils.nullToEmpty(rs.getString(3)));
                hasSize = true;
            }
            if (!hasSize) {
                Element size = toAppend.addElement("size");
                size.addElement("id").addText(HandlerUtils.nullToEmpty("0"));
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error in getBeverageSizes: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
            close(rs);
        }

    }

    /**
     * The folowing code is to fetch the inventory items for each location when a single beverage is selected 
     *---- SR
     */
    private void getDraftInventoryXML(Element toAppend, ResultSet rs) throws SQLException {
        while (rs.next()) {
            Element InventoryE1 = toAppend.addElement("inventoryItem");

            InventoryE1.addElement("inventoryId").addText(String.valueOf(rs.getInt(1)));
            InventoryE1.addElement("productId").addText(String.valueOf(rs.getInt(2)));
            InventoryE1.addElement("productName").addText(HandlerUtils.nullToEmpty(rs.getString(3)));
            InventoryE1.addElement("locationId").addText(String.valueOf(rs.getInt(4)));
            InventoryE1.addElement("qtyOnHand").addText(String.valueOf(rs.getDouble(5)));
            InventoryE1.addElement("minimumQty").addText(String.valueOf(rs.getDouble(6)));
            InventoryE1.addElement("pluCode").addText(HandlerUtils.nullToEmpty(rs.getString(7)));
            InventoryE1.addElement("qtyToOrder").addText(String.valueOf(rs.getInt(8)));
            InventoryE1.addElement("supplierName").addText(HandlerUtils.nullToEmpty(rs.getString(9)));
            InventoryE1.addElement("supplierId").addText(String.valueOf(rs.getInt(10)));
            InventoryE1.addElement("kegSize").addText(String.valueOf(rs.getInt(11)));
            InventoryE1.addElement("brixWater").addText(String.valueOf(rs.getInt(12)));
            InventoryE1.addElement("brixSyrup").addText(String.valueOf(rs.getInt(13)));
            InventoryE1.addElement("prodType").addText(String.valueOf(rs.getInt(14)));
        }

    }

    private void getDraftInventory(Element toHandle, Element toAppend) throws HandlerException {

        int refLocationId = HandlerUtils.getRequiredInteger(toHandle, "locationId");

        PreparedStatement stmt = null;
        ResultSet rs = null;


        try {
            if (refLocationId >= 0) {
                if (!checkForeignKey("location", "id", refLocationId)) {
                    throw new HandlerException("Foreign Key Not found : location " + refLocationId);
                }
                String selectByLocationId = "SELECT i.id, i.product, p.name, " +
                        " i.location, IF(i.qtyOnHand < 0, 0.0, i.qtyOnHand), i.minimumQty, i.plu, i.qtyToHave," +
                        " sup.name, sup.id, i.kegSize, i.brixWater, i.brixSyrup, p.pType " +
                        " FROM product AS p LEFT JOIN inventory AS i ON p.id = i.product " +
                        " LEFT JOIN supplier AS sup ON i.supplier = sup.id " +
                        " WHERE i.location = ? and p.pType in (1,2)";
                stmt = transconn.prepareStatement(selectByLocationId);
                stmt.setInt(1, refLocationId);
                rs = stmt.executeQuery();
                getDraftInventoryXML(toAppend, rs);
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

    /**  Get all products ordered after a cutoff date or within X days (date takes priority
     */
    private void getRecentlyReceivedProducts(Element toHandle, Element toAppend) throws HandlerException {
        String cutoffDate = HandlerUtils.getOptionalString(toHandle, "date");
        String startDate = HandlerUtils.getOptionalString(toHandle, "startDate");
        String endDate = HandlerUtils.getOptionalString(toHandle, "endDate");
        int cutoffDays = HandlerUtils.getOptionalInteger(toHandle, "daysOld");
        int location = HandlerUtils.getRequiredInteger(toHandle, "locationId");

        PreparedStatement stmt = null;
        ResultSet rs = null;

        String dateCondition = "";
        if (cutoffDate != null) {
            dateCondition = " AND p.date>?";
        } else if (cutoffDays > 0) {
            dateCondition = " AND TO_DAYS(NOW())-TO_DAYS(p.date) <= ? ";
        } else if (startDate != null && endDate != null) {
            dateCondition = " AND p.date BETWEEN ? AND ? ";
        }
        String getRecent =
                " SELECT DISTINCT pr.id, pr.name " +
                " FROM purchaseDetail d LEFT JOIN purchase p ON d.purchase=p.id " +
                "   LEFT JOIN product pr ON d.product = pr.id " +
                " WHERE p.location=? " + dateCondition;
        String getReceived =
                " SELECT d.product,pr.name,SUM(d.quantity) " +
                " FROM purchase p LEFT JOIN purchaseDetail d ON d.purchase=p.id " +
                " LEFT JOIN product pr ON d.product=pr.id " +
                " WHERE p.location=? AND p.status='RECEIVED' AND d.product IS NOT NULL" + dateCondition +
                " GROUP BY pr.id ";

        HashMap<Integer, String> productNames = new HashMap<Integer, String>();
        //Set<Integer> recentProducts = new HashSet<Integer>();
        HashMap<Integer, Integer> onOrder = new HashMap<Integer, Integer>();

        try {
            // get recently received products
            stmt = transconn.prepareStatement(getRecent);
            stmt.setInt(1, location);
            if (cutoffDate != null) {
                stmt.setString(2, cutoffDate);
            } else if (cutoffDays > 0) {
                stmt.setInt(2, cutoffDays);
            } else if (startDate != null && endDate != null) {
                stmt.setString(2, startDate);
                stmt.setString(3, endDate);
            }
            rs = stmt.executeQuery();
            while (rs.next()) {
                Integer productId = new Integer(rs.getInt(1));
                String productName = rs.getString(2);
                productNames.put(productId, productName);
                //recentProducts.add(productId);
            }

            // get received orders
            stmt = transconn.prepareStatement(getReceived);
            stmt.setInt(1, location);
            if (cutoffDate != null) {
                stmt.setString(2, cutoffDate);
            } else if (startDate != null && endDate != null) {
                stmt.setString(2, startDate);
                stmt.setString(3, endDate);
            }
            rs = stmt.executeQuery();
            while (rs.next()) {
                Integer productId = new Integer(rs.getInt(1));
                String productName = rs.getString(2);
                Integer quantity = new Integer(rs.getInt(3));
                productNames.put(productId, productName);
                onOrder.put(productId, quantity);
            }

            //build the output XML
            for (Integer pkey : productNames.keySet()) {
                Element prEl = toAppend.addElement("product");
                prEl.addElement("productId").addText(String.valueOf(pkey));
                prEl.addElement("name").addText(String.valueOf(productNames.get(pkey)));
                int orderQty = 0;
                Integer checkQty = onOrder.get(pkey);
                if (checkQty != null) {
                    orderQty = checkQty.intValue();
                }
                prEl.addElement("receivedQuantity").addText(String.valueOf(orderQty));
            }

        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.toString());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
            close(rs);
        }

    }

    private void getTestInventory(Element toHandle, Element toAppend) throws HandlerException {

        //NischaySharma_11-Feb-2009_Start: Added Extraction of supplierid
        int supplierId = HandlerUtils.getOptionalInteger(toHandle, "supplierId");
        //NischaySharma_11-Feb-2009_End

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
            if (refCustomerId >= 0) {
                if (!checkForeignKey("customer", "id", refCustomerId)) {
                    throw new HandlerException("Foreign Key Not found : customer " + refCustomerId);
                }
                String selectByCustomerId = "SELECT i.id, i.product, p.name, " +
                        " i.location, IF(i.qtyOnHand < 0, 0.0, i.qtyOnHand), i.minimumQty, i.plu, i.qtyToHave," +
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
                getTestInventoryXML(toAppend, rs, getDetails, true);
            } else if (refLocationId >= 0) {
                if (!checkForeignKey("location", "id", refLocationId)) {
                    throw new HandlerException("Foreign Key Not found : location " + refLocationId);
                }
                String selectByLocationId = "SELECT i.id, i.product, p.name, " +
                        " i.location, IF(i.qtyOnHand < 0, 0.0, i.qtyOnHand), i.minimumQty, i.plu, i.qtyToHave," +
                        " sup.name, sup.id, i.kegSize, i.brixWater, i.brixSyrup, p.pType," +
                        " i.bottleSize, b.name, c.id, c.name, i.kegLine, k.name, p.segment, p.category " +
                        " FROM product AS p LEFT JOIN inventory AS i ON p.id = i.product " +
                        " LEFT JOIN supplier AS sup ON i.supplier = sup.id LEFT JOIN bottleSize AS b ON i.bottleSize = b.id " +
                        " LEFT JOIN kegLine AS k ON k.id = i.kegLine " +
                        " LEFT JOIN cooler AS c ON k.cooler = c.id " +
                        " WHERE i.location = ? and p.pType = ? " + isActive;
                stmt = transconn.prepareStatement(selectByLocationId);
                stmt.setInt(1, refLocationId);
                stmt.setInt(2, prodType);
                rs = stmt.executeQuery();
                getTestInventoryXML(toAppend, rs, getDetails, false);
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
                getTestInventoryXML(toAppend, rs, getDetails, true);
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
                getTestInventoryXML(toAppend, rs, getDetails, true);
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
                getTestInventoryXML(toAppend, rs, getDetails, true);
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
                getTestInventoryXML(toAppend, rs, getDetails, true);
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
                getTestInventoryXML(toAppend, rs, getDetails, true);
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
                getTestInventoryXML(toAppend, rs, getDetails, true);
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
                getTestInventoryXML(toAppend, rs, getDetails, true);
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
     * The folowing code is to fetch the inventory items for each location when a single beverage is selected 
     *---- SR
     */
    private void getTestInventoryXML(Element toAppend, ResultSet rs, boolean getDetails, boolean isCustomer) throws SQLException {
        while (rs.next()) {
            Element InventoryE1 = toAppend.addElement("inventoryItem");
            InventoryE1.addElement("inventoryId").addText(String.valueOf(rs.getInt(1)));
            InventoryE1.addElement("productId").addText(String.valueOf(rs.getInt(2)));
            InventoryE1.addElement("productName").addText(HandlerUtils.nullToEmpty(rs.getString(3)));
            InventoryE1.addElement("locationId").addText(String.valueOf(rs.getInt(4)));
            InventoryE1.addElement("qtyOnHand").addText(String.valueOf(rs.getDouble(5)));
            InventoryE1.addElement("minimumQty").addText(String.valueOf(rs.getDouble(6)));
            InventoryE1.addElement("pluCode").addText(HandlerUtils.nullToEmpty(rs.getString(7)));
            InventoryE1.addElement("qtyToOrder").addText(String.valueOf(rs.getInt(8)));
            InventoryE1.addElement("supplierName").addText(HandlerUtils.nullToEmpty(rs.getString(9)));
            InventoryE1.addElement("supplierId").addText(String.valueOf(rs.getInt(10)));
            InventoryE1.addElement("kegSize").addText(String.valueOf(rs.getInt(11)));
            InventoryE1.addElement("brixWater").addText(String.valueOf(rs.getInt(12)));
            InventoryE1.addElement("brixSyrup").addText(String.valueOf(rs.getInt(13)));
            InventoryE1.addElement("prodType").addText(String.valueOf(rs.getInt(14)));
            InventoryE1.addElement("bottleSizeId").addText(String.valueOf(rs.getInt(15)));
            InventoryE1.addElement("bottleSizeName").addText(HandlerUtils.nullToEmpty(rs.getString(16)));
            InventoryE1.addElement("coolerId").addText(HandlerUtils.nullToEmpty(rs.getString(17)));
            InventoryE1.addElement("cooler").addText(HandlerUtils.nullToEmpty(rs.getString(18)));
            InventoryE1.addElement("kegLineId").addText(HandlerUtils.nullToEmpty(rs.getString(19)));
            InventoryE1.addElement("kegLine").addText(HandlerUtils.nullToEmpty(rs.getString(20)));
            InventoryE1.addElement("segment").addText(HandlerUtils.nullToEmpty(rs.getString(21)));
            InventoryE1.addElement("category").addText(HandlerUtils.nullToEmpty(rs.getString(22)));
        }

    }

    /**
     * The folowing code is to fetch the inventory items for each location when all beverages is selected 
     *---- SR
     */
    private void getTestInventoryXML1(Element toAppend, ResultSet rs) throws SQLException {
        while (rs.next()) {
            Element InventoryE1 = toAppend.addElement("inventoryItem");

            InventoryE1.addElement("inventoryId").addText(String.valueOf(rs.getInt(1)));
            InventoryE1.addElement("productId").addText(String.valueOf(rs.getInt(2)));
            InventoryE1.addElement("productName").addText(HandlerUtils.nullToEmpty(rs.getString(3)));
            InventoryE1.addElement("locationId").addText(String.valueOf(rs.getInt(4)));
            InventoryE1.addElement("qtyOnHand").addText(String.valueOf(rs.getDouble(5)));
            InventoryE1.addElement("minimumQty").addText(String.valueOf(rs.getDouble(6)));
            InventoryE1.addElement("pluCode").addText(HandlerUtils.nullToEmpty(rs.getString(7)));
            InventoryE1.addElement("qtyToOrder").addText(String.valueOf(rs.getInt(8)));
            InventoryE1.addElement("supplierName").addText(HandlerUtils.nullToEmpty(rs.getString(9)));
            InventoryE1.addElement("supplierId").addText(String.valueOf(rs.getInt(10)));
            InventoryE1.addElement("kegSize").addText(String.valueOf(rs.getInt(11)));
            InventoryE1.addElement("brixWater").addText(String.valueOf(rs.getInt(12)));
            InventoryE1.addElement("brixSyrup").addText(String.valueOf(rs.getInt(13)));
            InventoryE1.addElement("prodType").addText(String.valueOf(rs.getInt(14)));
        }

    }

    private void getTestInventory1(Element toHandle, Element toAppend) throws HandlerException {

        int refLocationId = HandlerUtils.getRequiredInteger(toHandle, "locationId");
        int prodType = HandlerUtils.getRequiredInteger(toHandle, "prodID");
        PreparedStatement stmt = null;
        ResultSet rs = null;


        try {
            if (refLocationId >= 0) {
                if (!checkForeignKey("location", "id", refLocationId)) {
                    throw new HandlerException("Foreign Key Not found : location " + refLocationId);
                }
                String selectByLocationId = "SELECT i.id, i.product, p.name, " +
                        " i.location, IF(i.qtyOnHand < 0, 0.0, i.qtyOnHand), i.minimumQty, i.plu, i.qtyToHave," +
                        " sup.name, sup.id, i.kegSize, i.brixWater, i.brixSyrup, p.pType " +
                        " FROM product AS p LEFT JOIN inventory AS i ON p.id = i.product " +
                        " LEFT JOIN supplier AS sup ON i.supplier = sup.id " +
                        " WHERE i.location = ?";
                stmt = transconn.prepareStatement(selectByLocationId);
                stmt.setInt(1, refLocationId);
                rs = stmt.executeQuery();
                getTestInventoryXML1(toAppend, rs);
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
     * The folowing code is to run Generic Querys and return the value to the report 
     * - This particular hander gets 2 input and returns 1 output ---- SR
     */
    private void getGenericResult1(Element toHandle, Element toAppend) throws HandlerException {

        int queryEntry1 = HandlerUtils.getRequiredInteger(toHandle, "queryEntry1");
        int queryEntry2 = HandlerUtils.getRequiredInteger(toHandle, "queryEntry2");
        String query = HandlerUtils.getRequiredString(toHandle, "query");
        PreparedStatement stmt = null;
        ResultSet rs = null;

        String sql = query;

        try {
            stmt = transconn.prepareStatement(sql);
            stmt.setInt(1, queryEntry1);
            stmt.setInt(1, queryEntry2);
            rs = stmt.executeQuery();
            if (rs.next()) {
                toAppend.addElement("queryResult").addText(HandlerUtils.nullToEmpty(rs.getString(1)));

            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.toString());
            throw new HandlerException(sqle);
        } finally {
            close(rs);
            close(stmt);
        }
    }

    /**
     * The folowing code is to run Generic Querys and return the value to the report 
     * - This particular hander gets 1 input and returns 2 output ---- SR
     */
    private void getGenericResult2(Element toHandle, Element toAppend) throws HandlerException {

        int queryEntry1 = HandlerUtils.getRequiredInteger(toHandle, "queryEntry1");

        String query = HandlerUtils.getRequiredString(toHandle, "query");
        PreparedStatement stmt = null;
        ResultSet rs = null;

        String sql = query;

        try {
            stmt = transconn.prepareStatement(sql);
            stmt.setInt(1, queryEntry1);

            rs = stmt.executeQuery();
            if (rs.next()) {
                toAppend.addElement("queryResult").addText(HandlerUtils.nullToEmpty(rs.getString(1)));
                toAppend.addElement("queryResult1").addText(HandlerUtils.nullToEmpty(rs.getString(2)));
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.toString());
            throw new HandlerException(sqle);
        } finally {
            close(rs);
            close(stmt);
        }
    }

    /**
     * The folowing code is to run Generic Querys and return the value to the report 
     * - This particular hander gets 2 input and returns 2 output ---- SR
     */
    private void getGenericResult3(Element toHandle, Element toAppend) throws HandlerException {

        int queryEntry1 = HandlerUtils.getRequiredInteger(toHandle, "queryEntry1");
        int queryEntry2 = HandlerUtils.getRequiredInteger(toHandle, "queryEntry2");
        String query = HandlerUtils.getRequiredString(toHandle, "query");
        PreparedStatement stmt = null;
        ResultSet rs = null;

        String sql = query;

        try {
            stmt = transconn.prepareStatement(sql);
            stmt.setInt(1, queryEntry1);
            stmt.setInt(2, queryEntry2);
            rs = stmt.executeQuery();
            if (rs.next()) {
                toAppend.addElement("queryResult").addText(HandlerUtils.nullToEmpty(rs.getString(1)));
                toAppend.addElement("queryResult1").addText(HandlerUtils.nullToEmpty(rs.getString(2)));
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.toString());
            throw new HandlerException(sqle);
        } finally {
            close(rs);
            close(stmt);
        }
    }

    /**
     * The folowing code is to run Generic Querys and return the value to the report 
     * - This particular hander gets 1 input and returns 3 output ---- SR
     */
    private void getGenericResult4(Element toHandle, Element toAppend) throws HandlerException {

        int queryEntry1 = HandlerUtils.getRequiredInteger(toHandle, "queryEntry1");

        String query = HandlerUtils.getRequiredString(toHandle, "query");
        PreparedStatement stmt = null;
        ResultSet rs = null;

        String sql = query;

        try {
            stmt = transconn.prepareStatement(sql);
            stmt.setInt(1, queryEntry1);

            rs = stmt.executeQuery();
            if (rs.next()) {
                toAppend.addElement("queryResult").addText(HandlerUtils.nullToEmpty(rs.getString(1)));
                toAppend.addElement("queryResult1").addText(HandlerUtils.nullToEmpty(rs.getString(2)));
                toAppend.addElement("queryResult2").addText(HandlerUtils.nullToEmpty(rs.getString(3)));
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.toString());
            throw new HandlerException(sqle);
        } finally {
            close(rs);
            close(stmt);
        }
    }

    /**
     * The folowing code is to run Generic Querys and return the value to the report 
     * - This particular hander gets 2 input and returns 3 output ---- SR
     */
    private void getGenericResult5(Element toHandle, Element toAppend) throws HandlerException {

        int queryEntry1 = HandlerUtils.getRequiredInteger(toHandle, "queryEntry1");
        int queryEntry2 = HandlerUtils.getRequiredInteger(toHandle, "queryEntry2");
        String query = HandlerUtils.getRequiredString(toHandle, "query");
        PreparedStatement stmt = null;
        ResultSet rs = null;

        String sql = query;

        try {
            stmt = transconn.prepareStatement(sql);
            stmt.setInt(1, queryEntry1);
            stmt.setInt(2, queryEntry2);

            rs = stmt.executeQuery();
            if (rs.next()) {
                toAppend.addElement("queryResult").addText(HandlerUtils.nullToEmpty(rs.getString(1)));
                toAppend.addElement("queryResult1").addText(HandlerUtils.nullToEmpty(rs.getString(2)));
                toAppend.addElement("queryResult2").addText(HandlerUtils.nullToEmpty(rs.getString(3)));
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.toString());
            throw new HandlerException(sqle);
        } finally {
            close(rs);
            close(stmt);
        }
    }

    private void getDailyLineCleaningReports(Element toHandle, Element toAppend) throws HandlerException {

        int callerId = getCallerId(toHandle);
        String select = "SELECT Location, Bar, SUM(Poured) Poured, IF(SUM(Poured)>0,'***',' ') Xcpt" +
                "FROM (SELECT c.id Customer, l.name Location, b.name Bar, s.systemId Sys," +
                "line.lineIndex Line, p.name Product, ROUND(MAX(r.value)-MIN(r.value),1) Poured" +
                "FROM customer c JOIN location l ON c.id=l.customer JOIN bar b ON l.id=b.location" +
                "JOIN system s ON l.id=s.location JOIN line ON s.id=line.system" +
                "JOIN product p ON p.id=line.product JOIN reading r ON line.id=r.line" +
                "JOIN locationHours lH ON lH.location=l.id" +
                "JOIN (select Cust, Loc, addtime(Close2,concat(eO,':0:0')) Closed," +
                "addtime(Open2,concat(eO,':0:0')) Opened, eO from" +
                "(select Cust, Loc, If(Close1>'12:0:0',concat(left(now()-1000000,11),Close1),concat(left(now(),11),Close1)) Close2," +
                "Concat(left(now(),11),Open1) Open2, eO from" +
                "(Select l.customer Cust, l.id Loc," +
                "CASE DAYOFWEEK(NOW()-1000000)" +
                "WHEN 1 THEN Right(lH.preOpenSun,8)" +
                "WHEN 2 THEN Right(lH.preOpenMon,8)" +
                "WHEN 3 THEN Right(lH.preOpenTue,8)" +
                "WHEN 4 THEN Right(lH.preOpenWed,8)" +
                "WHEN 5 THEN Right(lH.preOpenThu,8)" +
                "WHEN 6 THEN Right(lH.preOpenFri,8)" +
                "WHEN 7 THEN Right(lH.preOpenSat,8) END Close1," +
                "CASE DAYOFWEEK(NOW())" +
                "WHEN 1 THEN Right(lH.OpenSun,8)" +
                "WHEN 2 THEN Right(lH.OpenMon,8)" +
                "WHEN 3 THEN Right(lH.OpenTue,8)" +
                "WHEN 4 THEN Right(lH.OpenWed,8)" +
                "WHEN 5 THEN Right(lH.OpenThu,8)" +
                "WHEN 6 THEN Right(lH.OpenFri,8)" +
                "WHEN 7 THEN Right(lH.OpenSat,8) END Open1, MID(easternOffset,2,1) eO" +
                "FROM locationHours lH JOIN location l ON lH.location=l.id" +
                "WHERE l.customer=?) AS x) AS y) As z ON z.Loc=l.id" +
                "WHERE r.date BETWEEN Closed AND Opened AND r.type = 0" +
                "AND line.status='RUNNING' AND pType=1" +
                "GROUP BY l.id, b.name, s.systemId, line.lineIndex) AS x GROUP BY Location,Bar;";

        PreparedStatement stmt = null;
        ResultSet rs = null;

        logger.portalAction("addTest");

        try {

            int locationId = HandlerUtils.getRequiredInteger(toHandle, "locationId");
            int userId = HandlerUtils.getRequiredInteger(toHandle, "userId");
            int customerId = HandlerUtils.getRequiredInteger(toHandle, "customerId");
            String emailAddr = HandlerUtils.getRequiredString(toHandle, "emailAddr");


            //Check that this product doesn't already exist in inventory at this location
            stmt = transconn.prepareStatement(select);
            stmt.setString(1, emailAddr);
            stmt.setInt(2, locationId);
            rs = stmt.executeQuery();

            if (rs.next()) {
                toAppend.addElement("report1").addText(HandlerUtils.nullToEmpty(rs.getString(1)));
                toAppend.addElement("report2").addText(HandlerUtils.nullToEmpty(rs.getString(2)));
                toAppend.addElement("report3").addText(HandlerUtils.nullToEmpty(rs.getString(3)));
                toAppend.addElement("report4").addText(HandlerUtils.nullToEmpty(rs.getString(4)));
                toAppend.addElement("report5").addText(HandlerUtils.nullToEmpty(rs.getString(5)));
                toAppend.addElement("report6").addText(HandlerUtils.nullToEmpty(rs.getString(6)));
                toAppend.addElement("exception").addText(HandlerUtils.nullToEmpty(rs.getString(7)));
            }


        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
            close(rs);
        }


    }

    /** RETIRED
     * The folowing code is to update or insert the report requirements for each user for the email address
     * that they provide - SR
     */
    private void getReports(Element toHandle, Element toAppend) throws HandlerException {

        int callerId = getCallerId(toHandle);
        String select = " SELECT report1, report2, report3, report4, report5, report6, exceptionsOnly  FROM emailReports WHERE emailAddr=? and location=?";
        String selectAlert = " SELECT noSoldAlert, noPouredAlert  FROM emailReports WHERE emailAddr=? and customer=?";

        PreparedStatement stmt = null;
        ResultSet rs = null;

        logger.portalAction("getReports");

        try {
            int locationId = -1;
            locationId = HandlerUtils.getOptionalInteger(toHandle, "locationId");
            int userId = HandlerUtils.getRequiredInteger(toHandle, "userId");
            int customerId = HandlerUtils.getRequiredInteger(toHandle, "customerId");
            String emailAddr = HandlerUtils.getRequiredString(toHandle, "emailAddr");

            if (locationId > 0) {

                stmt = transconn.prepareStatement(select);
                stmt.setString(1, emailAddr);
                stmt.setInt(2, locationId);
                rs = stmt.executeQuery();
                if (rs.next()) {
                    toAppend.addElement("report1").addText(HandlerUtils.nullToEmpty(rs.getString(1)));
                    toAppend.addElement("report2").addText(HandlerUtils.nullToEmpty(rs.getString(2)));
                    toAppend.addElement("report3").addText(HandlerUtils.nullToEmpty(rs.getString(3)));
                    toAppend.addElement("report4").addText(HandlerUtils.nullToEmpty(rs.getString(4)));
                    toAppend.addElement("report5").addText(HandlerUtils.nullToEmpty(rs.getString(5)));
                    toAppend.addElement("report6").addText(HandlerUtils.nullToEmpty(rs.getString(6)));
                    toAppend.addElement("exception").addText(HandlerUtils.nullToEmpty(rs.getString(7)));
                }

            }

            stmt = transconn.prepareStatement(selectAlert);
            stmt.setString(1, emailAddr);
            stmt.setInt(2, customerId);
            rs = stmt.executeQuery();
            if (rs.next()) {
                toAppend.addElement("noSold").addText(HandlerUtils.nullToEmpty(rs.getString(1)));
                toAppend.addElement("noPoured").addText(HandlerUtils.nullToEmpty(rs.getString(2)));
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

    private void getBottleInv(Element toHandle, Element toAppend)
            throws HandlerException {

        int location = HandlerUtils.getRequiredInteger(toHandle, "locationId");
        String date;
        String getRecentInvDate = "SELECT date FROM bottleInv WHERE location = ? ORDER BY date DESC LIMIT 1";
        String getLastInvDate = "SELECT date FROM bottleInv WHERE location = ? and date < ? ORDER BY date DESC LIMIT 1";
        String getLastInv = "SELECT product, actualInv FROM bottleInv WHERE location = ? AND date=?";
        String getLastBeforeInv = "SELECT product, calcOnHand, actualInv, ROUND(actualInv-calcOnHand,2) FROM bottleInv WHERE location = ? AND date=?";

        PreparedStatement stmt = null;
        ResultSet rs = null;

        try {
            // we need to know if the user is logging in for the First Time
            stmt = transconn.prepareStatement(getRecentInvDate);
            stmt.setInt(1, location);
            rs = stmt.executeQuery();
            if (rs.next()) {
                date = rs.getString(1);
                toAppend.addElement("lastDate").addText(String.valueOf(rs.getString(1)));
            } else {
                date = "2007-01-01 08:00:00";
                toAppend.addElement("lastDate").addText("2007-01-01 08:00:00");
            }

            stmt = transconn.prepareStatement(getLastInv);
            stmt.setInt(1, location);
            stmt.setString(2, date);
            rs = stmt.executeQuery();
            while (rs.next()) {
                Element p = toAppend.addElement("productInv");
                p.addElement("product").addText(String.valueOf(rs.getString(1)));
                p.addElement("lastActualInv").addText(String.valueOf(rs.getString(2)));
            }

            stmt = transconn.prepareStatement(getLastInvDate);
            stmt.setInt(1, location);
            stmt.setString(2, date);
            rs = stmt.executeQuery();
            if (rs.next()) {

                toAppend.addElement("lastBeforeDate").addText(String.valueOf(rs.getString(1)));
            } else {

                toAppend.addElement("lastBeforeDate").addText("2007-01-01 08:00:00");
            }
            stmt = transconn.prepareStatement(getLastBeforeInv);
            stmt.setInt(1, location);
            stmt.setString(2, date);
            rs = stmt.executeQuery();
            while (rs.next()) {
                Element p = toAppend.addElement("productBeforeInv");
                p.addElement("product").addText(String.valueOf(rs.getString(1)));
                p.addElement("lastBeforeOnHand").addText(String.valueOf(rs.getString(2)));
                p.addElement("lastBeforeActInv").addText(String.valueOf(rs.getString(3)));
                p.addElement("lastBeforeVar").addText(String.valueOf(rs.getString(4)));
            }

        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(rs);
            close(stmt);
        }

    }

    /** Get the details of a purchase number, and lookup the supplier address that this location uses
     *  <orderNumber> 0000
     *  <locationId> 000
     *
     *  returns:
     *
     *  <date>
     *  <totalPrice>
     *  <status>
     *  <supplierId>
     *  <supplierName>
     *  <supplierAddress> 1|0   //if the suppler address is set up
     *  OPT<supplierStreet>
     *  OPT<supplierCity>
     *  OPT<supplierState>
     *  OPT<supplierZip>
     *  <product >
     *      <name>
     *      <productId>
     *      <quantity>
     *      <plu>
     *  </product>
     *  <product>...</>
     *  <product>...</>
     */
    private void getTestPurchaseDetail(Element toHandle, Element toAppend) throws HandlerException {
        int purchase = HandlerUtils.getRequiredInteger(toHandle, "orderNumber");
        int location = HandlerUtils.getRequiredInteger(toHandle, "locationId");

        PreparedStatement stmt = null;
        ResultSet rs = null;

        String getPurchase =
                " SELECT supplier,date,total,status FROM purchase WHERE id=? AND location=? ";
        String getDetails =
                " SELECT pr.id, pr.name, pd.quantity, pd.productPlu, pr.pType FROM " +
                " purchaseDetail pd LEFT JOIN product pr ON pd.product=pr.id " +
                " WHERE pd.purchase=?";
        String getDetailsMisc =
                " SELECT pr.id, pr.name, pd.quantity, pd.productPlu, pr.pType FROM " +
                " purchaseDetailMisc pd LEFT JOIN miscProduct pr ON pd.product=pr.id " +
                " WHERE pd.purchase=?";
        String getAddress =
                " SELECT s.name, sa.addrStreet, sa.addrCity, sa.addrState, sa.addrZip " +
                " FROM location l LEFT JOIN locationSupplier map ON l.id=map.location " +
                " LEFT JOIN supplierAddress sa ON map.address=sa.id " +
                " LEFT JOIN supplier s ON sa.supplier = s.id " +
                " WHERE l.id=? AND s.id=?";
        String getSupplier =
                " SELECT name FROM supplier WHERE id=? ";

        try {
            stmt = transconn.prepareStatement(getPurchase);
            stmt.setInt(1, purchase);
            stmt.setInt(2, location);
            rs = stmt.executeQuery();
            if (rs.next()) {
                int supplier = rs.getInt(1);
                toAppend.addElement("date").addText(HandlerUtils.nullToEmpty(rs.getString(2)));
                toAppend.addElement("totalPrice").addText(HandlerUtils.nullToEmpty(rs.getString(3)));
                toAppend.addElement("status").addText(HandlerUtils.nullToEmpty(rs.getString(4)));
                toAppend.addElement("supplierId").addText(String.valueOf(supplier));
                // get the supplier address IF the location has this supplier in its current list, otherwise just grab its name
                stmt = transconn.prepareStatement(getAddress);
                stmt.setInt(1, location);
                stmt.setInt(2, supplier);
                rs = stmt.executeQuery();
                if (rs.next()) {
                    toAppend.addElement("supplierAddress").addText(String.valueOf(1));
                    toAppend.addElement("supplierName").addText(HandlerUtils.nullToEmpty(rs.getString(1)));
                    toAppend.addElement("supplierStreet").addText(HandlerUtils.nullToEmpty(rs.getString(2)));
                    toAppend.addElement("supplierCity").addText(HandlerUtils.nullToEmpty(rs.getString(3)));
                    toAppend.addElement("supplierState").addText(HandlerUtils.nullToEmpty(rs.getString(4)));
                    toAppend.addElement("supplierZip").addText(HandlerUtils.nullToEmpty(rs.getString(5)));
                } else {
                    // No supplier address at this location, just grab the name
                    toAppend.addElement("supplierAddress").addText(String.valueOf(0));
                    stmt = transconn.prepareStatement(getSupplier);
                    stmt.setInt(1, supplier);
                    rs = stmt.executeQuery();
                    if (rs.next()) {
                        toAppend.addElement("supplierName").addText(HandlerUtils.nullToEmpty(rs.getString(1)));
                    }
                }
                stmt = transconn.prepareStatement(getDetails);
                stmt.setInt(1, purchase);
                rs = stmt.executeQuery();
                while (rs.next()) {
                    Element product = toAppend.addElement("product");
                    product.addElement("productId").addText(HandlerUtils.nullToEmpty(rs.getString(1)));
                    product.addElement("name").addText(HandlerUtils.nullToEmpty(rs.getString(2)));
                    product.addElement("quantity").addText(HandlerUtils.nullToEmpty(rs.getString(3)));
                    product.addElement("plu").addText(HandlerUtils.nullToEmpty(rs.getString(4)));
                    product.addElement("pType").addText(HandlerUtils.nullToEmpty(rs.getString(5)));
                }
                stmt = transconn.prepareStatement(getDetailsMisc);
                stmt.setInt(1, purchase);
                rs = stmt.executeQuery();
                while (rs.next()) {
                    Element product = toAppend.addElement("miscProduct");
                    product.addElement("productId").addText(HandlerUtils.nullToEmpty(rs.getString(1)));
                    product.addElement("name").addText(HandlerUtils.nullToEmpty(rs.getString(2)));
                    product.addElement("quantity").addText(HandlerUtils.nullToEmpty(rs.getString(3)));
                    product.addElement("plu").addText(HandlerUtils.nullToEmpty(rs.getString(4)));
                    product.addElement("pType").addText(HandlerUtils.nullToEmpty(rs.getString(5)));
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

    private void isNewUser(Element toHandle, Element toAppend)
            throws HandlerException {

        String username = HandlerUtils.getRequiredString(toHandle, "username");
        String password = HandlerUtils.getRequiredString(toHandle, "password");

        String getNewUser = "SELECT u.newUser FROM user u " +
                " WHERE u.username = ? AND u.password = ? ";

        PreparedStatement stmt = null;
        ResultSet rs = null;

        try {
            // we need to know if the user is logging in for the First Time
            stmt = transconn.prepareStatement(getNewUser);
            stmt.setString(1, username);
            stmt.setString(2, password);
            rs = stmt.executeQuery();
            int newUser = 0;
            if (rs != null && rs.next()) {
                int rsIndex = 1;
                newUser = rs.getInt(rsIndex++);
                toAppend.addElement("newUser").addText(String.valueOf(newUser));
            }

        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(rs);
            close(stmt);
        }

    }

    /** Returns a list of the locations for which a user has requested low-stock notifications
     *  Will also return that users email address, and if he has location-status updates enabled
     */
    private void authReportRequest(Element toHandle, Element toAppend) throws HandlerException {
        int customer = HandlerUtils.getOptionalInteger(toHandle, "customerId");
        int location = HandlerUtils.getOptionalInteger(toHandle, "locationId");
        int user = HandlerUtils.getOptionalInteger(toHandle, "userId");
        int callerId = getCallerId(toHandle);

        if (location < 0) {
            throw new HandlerException("Either locationId or userId must be set.");
        }

        PreparedStatement stmt = null;

        if (location > 0) {
            try {
                String clearall = "UPDATE user SET emailReports=false WHERE customer=?";
                stmt = transconn.prepareStatement(clearall);
                stmt.setInt(1, customer);
                stmt.executeUpdate();
            } catch (SQLException sqle) {
                logger.dbError("Database error in authUser: " + sqle.getMessage());
                throw new HandlerException(sqle);
            } finally {
                close(stmt);
            }

            Iterator users = toHandle.elementIterator("userAuths");
            while (users.hasNext()) {
                Element userEl = (Element) users.next();
                int userId = HandlerUtils.getRequiredInteger(userEl, "userId");
                int notify = HandlerUtils.getOptionalInteger(userEl, "userAuth");
                Boolean notify1 = false;
                if (notify > 0) {
                    notify1 = true;
                }

                String update = "UPDATE user SET emailReports=? WHERE id=?";



                try {
                    stmt = transconn.prepareStatement(update);
                    stmt.setBoolean(1, notify1);
                    stmt.setInt(2, userId);
                    stmt.executeUpdate();

                } catch (SQLException sqle) {
                    logger.dbError("Database error in authUser: " + sqle.getMessage());
                    throw new HandlerException(sqle);
                } finally {
                    close(stmt);
                }
            }
        }

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

    /** Returns a list of the locations for which a user has requested low-stock notifications
     *  Will also return that users email address, and if he has location-status updates enabled
     */
    private void getReportRequest(Element toHandle, Element toAppend) throws HandlerException {
        int user = HandlerUtils.getRequiredInteger(toHandle, "userId");
        String selectReportRequest = "SELECT emailReports FROM user WHERE id=?";
        PreparedStatement stmt = null;
        ResultSet rs = null;

        try {
            stmt = transconn.prepareStatement(selectReportRequest);
            stmt.setInt(1, user);
            rs = stmt.executeQuery();
            rs = stmt.executeQuery();
            String request = "";
            if (rs.next()) {
                request = rs.getString(1);
            }
            toAppend.addElement("request").addText(HandlerUtils.nullToEmpty(request));


        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.toString());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
            close(rs);
        }
    }

    /** Return all the normal users and their permission level for a specific location's customer.
     *    Only users who are NOT admins or supermanagers will be returned.
     *    For example, if Location 3 belongs to Customer 1, and Loc 3 is passed,
     *    the all the normal users for Customer 1 will be returned, along with their
     *    permission level for Loc 3, including users that have "no access" to Loc 3.
     */
    private void getTestNormalUsers(Element toHandle, Element toAppend) throws HandlerException {

        int locationId                      = HandlerUtils.getRequiredInteger(toHandle, "locationId");

        String getCustomer                  = "SELECT c.id, c.groupId FROM location l LEFT JOIN customer c ON c.id = l.customer WHERE l.id=?";
        String getUserAuth                  = "SELECT emailReports FROM user WHERE id=?";
        String listSelection                = "customer";
        PreparedStatement stmt              = null;
        ResultSet rs                        = null, rs1 = null;

        try {
            int customerId                  = -1, groupId = -1, parameter = -1;
            ArrayList<Integer> userIds      = new ArrayList<Integer>();
            
            stmt                            = transconn.prepareStatement(getCustomer);
            stmt.setInt(1, locationId);
            rs                              = stmt.executeQuery();
            if (rs.next()) {
                customerId                  = rs.getInt(1);
                groupId                     = rs.getInt(2);
                if (groupId > 0) {
                    listSelection           = "groupId";
                    parameter               = groupId;
                } else {
                    parameter               = customerId;
                }

                String getUsers             = "(SELECT u.id, u.name, u.email, m.securityLevel, IF (u.platform = 5, 1, 0) FROM user u LEFT JOIN userMap m ON m.user=u.id " +
                                            " WHERE u.isManager=0 AND u.isITAdmin=0 AND u." + listSelection + " = ? AND m.location=?)" +
                                            " UNION " +
                                            " (SELECT u2.id, u2.name, u2.email, 10, IF (u2.platform = 5, 1, 0) FROM user u2 WHERE u2.isManager=0 AND u2.isITAdmin=0 AND u2." + listSelection + "=? " +
                                            " AND u2.id NOT IN (SELECT user FROM userMap WHERE location=?)) ORDER BY name, id";
                stmt                        = transconn.prepareStatement(getUsers);
                int index                   = 0;
                stmt.setInt(++index, parameter);
                stmt.setInt(++index, locationId);
                stmt.setInt(++index, parameter);
                stmt.setInt(++index, locationId);
                rs                          = stmt.executeQuery();
                while (rs.next()) {
                    index                   = 0;
                    int userId              = rs.getInt(++index);
                    if (userIds.contains(userId)) {
                        continue;
                    } else {
                        userIds.add(userId);
                    }
                    String fullName         = rs.getString(++index);
                    String email            = rs.getString(++index);
                    int securityLevel       = rs.getInt(++index);
                    if (securityLevel < 1) {
                        securityLevel       = 10;
                    }
                    stmt                    = transconn.prepareStatement(getUserAuth);
                    stmt.setInt(1, userId);
                    String userAuth         = "";
                    rs1                     = stmt.executeQuery();
                    if (rs1.next()) {
                        userAuth            = rs1.getString(1);
                    }
                    Element userEl          = toAppend.addElement("user");
                    userEl.addElement("fullName").addText(HandlerUtils.nullToEmpty(fullName));
                    userEl.addElement("userId").addText(String.valueOf(userId));
                    userEl.addElement("email").addText(String.valueOf(email));
                    userEl.addElement("permission").addText(String.valueOf(securityLevel));
                    userEl.addElement("userAuth").addText(String.valueOf(userAuth));
                    userEl.addElement("beerboard").addText(String.valueOf(rs.getInt(++index)));
                }
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(rs);
            close(rs1);
            close(stmt);
        }
    }

    /** Send an email to the Corporate Admin to authorize Automated Reports for an user
     */
    private void requestReports(Element toHandle, Element toAppend) throws HandlerException {

        int locationId = HandlerUtils.getRequiredInteger(toHandle, "locationId");
        int customerId = HandlerUtils.getRequiredInteger(toHandle, "customerId");
        int userId = HandlerUtils.getRequiredInteger(toHandle, "userId");
        int notify = HandlerUtils.getRequiredInteger(toHandle, "notify");
        PreparedStatement stmt1 = null;
        ResultSet rs1 = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;

        String getLocationInformation =
                " SELECT name FROM location WHERE id=? ";
        String getCustomerInformation =
                " SELECT name FROM customer WHERE id=? ";
        String getUserInformation =
                " SELECT name, email FROM user WHERE id=? ";
        String getAdminInformation =
                " SELECT name, email FROM user WHERE customer = ? and isManager = 1 ";

        String emailTemplatePath = HandlerUtils.getSetting("email.templatePath");
        if ((emailTemplatePath == null) || "".equals(emailTemplatePath)) {
            emailTemplatePath = ".";
        }
        logger.debug("Packaging Email");

        try {

            stmt = transconn.prepareStatement(getAdminInformation);
            stmt.setInt(1, customerId);
            rs = stmt.executeQuery();
            String adminEmail = "";
            String adminName = "";
            while (rs.next()) {

                adminName = rs.getString(1);
                adminEmail = rs.getString(2);
                logger.debug("Loading Template");
                TemplatedMessage poEmail =
                        new TemplatedMessage("User Authorization",
                        emailTemplatePath,
                        "authUser");

                //logger.debug("Setting Template Fields to "+email+" #"+String.valueOf(purchase));
                poEmail.setSender("Tech@beerboard.com");
                poEmail.setRecipient(adminEmail);
                poEmail.setField("ADMINNAME", adminName.toString());

                stmt1 = transconn.prepareStatement(getCustomerInformation);
                stmt1.setInt(1, customerId);
                rs1 = stmt1.executeQuery();
                if (rs1.next()) {
                    String customer = rs1.getString(1);
                    poEmail.setField("CUSTOMER", customer.toString());
                }
                stmt1 = transconn.prepareStatement(getLocationInformation);
                stmt1.setInt(1, locationId);
                rs1 = stmt1.executeQuery();
                if (rs1.next()) {
                    String location = rs1.getString(1);
                    poEmail.setField("LOCATION", location.toString());
                }
                stmt1 = transconn.prepareStatement(getUserInformation);
                stmt1.setInt(1, userId);
                rs1 = stmt1.executeQuery();
                if (rs1.next()) {
                    String user = rs1.getString(1);
                    String userEmail = rs1.getString(2);
                    poEmail.setField("USER", user.toString());
                    poEmail.setField("USEREMAIL", userEmail.toString());
                }
                poEmail.setField("USERID", String.valueOf(userId));
                poEmail.setField("NOTIFY", String.valueOf(notify));
                logger.debug("Sending...");
                poEmail.send();
                logger.debug("Email sent successfully");
            }


        } catch (SQLException sqle) {
            logger.dbError("Database error in emailPurchase: " + sqle.toString());
            throw new HandlerException(sqle);
        } catch (MailException me) {
            logger.debug("Error sending purchase message to ");
            addErrorDetail(toAppend, "Error sending mail: " + me.toString());
        } finally {
            close(stmt);
            close(rs);
            close(stmt1);
            close(rs1);
        }
    }

    /** Send an email to the user to confirm their email address
     */
    private void emailUser(Element toHandle, Element toAppend) throws HandlerException {
        String email = HandlerUtils.getRequiredString(toHandle, "email");
        String name = HandlerUtils.getRequiredString(toHandle, "name");
        String uname = HandlerUtils.getRequiredString(toHandle, "uname");
        String pwd = HandlerUtils.getRequiredString(toHandle, "pwd");
        int locationId = HandlerUtils.getRequiredInteger(toHandle, "locationId");
        int customerId = HandlerUtils.getRequiredInteger(toHandle, "customerId");

        PreparedStatement stmt = null;
        ResultSet rs = null;

        String getLocationInformation =
                " SELECT name FROM location WHERE id=? ";
        String getCustomerInformation =
                " SELECT name FROM customer WHERE id=? ";

        String emailTemplatePath = HandlerUtils.getSetting("email.templatePath");
        if ((emailTemplatePath == null) || "".equals(emailTemplatePath)) {
            emailTemplatePath = ".";
        }
        logger.debug("Packaging Email");

        try {
            logger.debug("Loading Template");
            TemplatedMessage poEmail =
                    new TemplatedMessage("User Confirmation",
                    emailTemplatePath,
                    "emailUser");

            logger.debug("Setting To email");
            //logger.debug("Setting Template Fields to "+email+" #"+String.valueOf(purchase));
            poEmail.setSender("Tech@beerboard.com");
            logger.debug("Setting To Sender");
            poEmail.setRecipient(email);
            logger.debug("Setting To Recipient");
            poEmail.setField("NAME", name.toString());
            logger.debug("Setting Name");
            poEmail.setField("UNAME", uname.toString());
            logger.debug("Setting USERNAME");
            poEmail.setField("PWD", pwd.toString());
            logger.debug("Setting PASSWORD");

            stmt = transconn.prepareStatement(getLocationInformation);
            stmt.setInt(1, locationId);
            rs = stmt.executeQuery();
            if (rs.next()) {
                String location = rs.getString(1);
                poEmail.setField("LOCATION", location.toString());
            }

            stmt = transconn.prepareStatement(getCustomerInformation);
            stmt.setInt(1, customerId);
            rs = stmt.executeQuery();
            if (rs.next()) {
                String customer = rs.getString(1);
                poEmail.setField("CUSTOMER", customer.toString());
            }
            logger.debug("Sending...");
            poEmail.send();
            logger.debug("Email sent successfully");

        } catch (SQLException sqle) {
            logger.dbError("Database error in emailPurchase: " + sqle.toString());
            throw new HandlerException(sqle);
        } catch (MailException me) {
            logger.debug("Error sending purchase message to " + email + ": " + me.toString());
            addErrorDetail(toAppend, "Error sending mail: " + me.toString());
        } finally {
            close(stmt);
            close(rs);
        }
    }

    private StringBuilder getAllSoldList(int locationId, String locationName, String customerName, String buff, int offSet) throws HandlerException {

        PreparedStatement stmt = null;
        ResultSet rs = null;
        //String selectSalesTime = " SELECT date FROM sales WHERE location = ? ORDER BY sid DESC LIMIT 1";

        Calendar lastSalesPing = Calendar.getInstance();
        StringBuilder noSoldList = new StringBuilder();

        long diff = 1000 * 60 * 60 * 3;
        Calendar buff1 = Calendar.getInstance();
        diff = buff1.getTimeInMillis() - diff;

        try {
            lastSalesPing.set(Calendar.YEAR, Integer.parseInt(buff.substring(0, 4)));
            lastSalesPing.set(Calendar.MONTH, Integer.parseInt(buff.substring(5, 7)) - 1);
            lastSalesPing.set(Calendar.DAY_OF_MONTH, Integer.parseInt(buff.substring(8, 10)));
            lastSalesPing.set(Calendar.HOUR_OF_DAY, Integer.parseInt(buff.substring(11, 13)));
            lastSalesPing.set(Calendar.MINUTE, Integer.parseInt(buff.substring(14, 16)));
            lastSalesPing.set(Calendar.SECOND, Integer.parseInt(buff.substring(17)));
            if (diff > lastSalesPing.getTimeInMillis()) {
                lastSalesPing.add(Calendar.HOUR_OF_DAY, offSet);
                noSoldList.append("<tr><td style=font-size:x-small height=30>");
                noSoldList.append(HandlerUtils.nullToEmpty(customerName)); // Customer Name
                noSoldList.append("</td><td style=font-size:x-small>");
                noSoldList.append(HandlerUtils.nullToEmpty(locationName)); // Location Name
                noSoldList.append("</td><td align=center style=font-size:x-small>");
                noSoldList.append(HandlerUtils.nullToEmpty((lastSalesPing.getTime().toString()).substring(0, 20))); // Last Sales Reading
                noSoldList.append("</td><td align=center style=font-size:x-small></td style=font-size:x-small><td /></tr>");
            }
        } catch (Exception e) {
            logger.dbError("Error: " + e.getMessage());
            throw new HandlerException(e);
        } finally {
            close(stmt);
            close(rs);
        }
        return noSoldList;
    }

    private StringBuilder getAllPouredList(int locationId, String locationName, String customerName, String buff, int offSet) throws HandlerException {

        PreparedStatement stmt = null;
        ResultSet rs = null;

        Calendar lastPouredTime = Calendar.getInstance();
        StringBuilder noPouredList = new StringBuilder();

        long diff = 1000 * 60 * 60 * 3;
        Calendar buff1 = Calendar.getInstance();
        diff = buff1.getTimeInMillis() - diff;

        try {
            lastPouredTime.set(Calendar.YEAR, Integer.parseInt(buff.substring(0, 4)));
            lastPouredTime.set(Calendar.MONTH, Integer.parseInt(buff.substring(5, 7)) - 1);
            lastPouredTime.set(Calendar.DAY_OF_MONTH, Integer.parseInt(buff.substring(8, 10)));
            lastPouredTime.set(Calendar.HOUR_OF_DAY, Integer.parseInt(buff.substring(11, 13)));
            lastPouredTime.set(Calendar.MINUTE, Integer.parseInt(buff.substring(14, 16)));
            lastPouredTime.set(Calendar.SECOND, Integer.parseInt(buff.substring(17)));
            if (diff > lastPouredTime.getTimeInMillis()) {
                lastPouredTime.add(Calendar.HOUR_OF_DAY, offSet);
                noPouredList.append("<tr><td style=font-size:x-small height=30>");
                noPouredList.append(HandlerUtils.nullToEmpty(customerName)); // Customer Name
                noPouredList.append("</td><td style=font-size:x-small>");
                noPouredList.append(HandlerUtils.nullToEmpty(locationName)); // Location Name
                noPouredList.append("</td><td align=center style=font-size:x-small>");
                noPouredList.append(HandlerUtils.nullToEmpty((lastPouredTime.getTime().toString()).substring(0, 20))); // Last Poured Reading
                noPouredList.append("</td><td align=center style=font-size:x-small></td style=font-size:x-small><td /></tr>");
                noPouredList.append("");
            }
        } catch (Exception e) {
            logger.dbError("Error: " + e.getMessage());
            throw new HandlerException(e);
        } finally {
            close(stmt);
            close(rs);
        }
        return noPouredList;
    }

    private StringBuilder getPouredList(int locationId, String locationName, String customerName, HashMap<Integer, String> pouredPingList, int offSet) throws HandlerException {

        PreparedStatement stmt = null;
        ResultSet rs = null;

        Calendar lastPouredTime = Calendar.getInstance();

        StringBuilder noPouredList = new StringBuilder();

        String buff;
        long diff = 1000 * 60 * 60 * 1;
        Calendar buff1 = Calendar.getInstance();
        diff = buff1.getTimeInMillis() - diff;

        try {
            buff = pouredPingList.get(locationId);
            lastPouredTime.set(Calendar.YEAR, Integer.parseInt(buff.substring(0, 4)));
            lastPouredTime.set(Calendar.MONTH, Integer.parseInt(buff.substring(5, 7)) - 1);
            lastPouredTime.set(Calendar.DAY_OF_MONTH, Integer.parseInt(buff.substring(8, 10)));
            lastPouredTime.set(Calendar.HOUR_OF_DAY, Integer.parseInt(buff.substring(11, 13)));
            lastPouredTime.set(Calendar.MINUTE, Integer.parseInt(buff.substring(14, 16)));
            lastPouredTime.set(Calendar.SECOND, Integer.parseInt(buff.substring(17)));
            if (diff > lastPouredTime.getTimeInMillis()) {
                lastPouredTime.add(Calendar.HOUR_OF_DAY, offSet);
                noPouredList.append("<tr align=justify><td colspan=4>Your bevBox at ");
                noPouredList.append(HandlerUtils.nullToEmpty(locationName)); // Last Poured Reading
                noPouredList.append(" has <strong>not communicated draft beer pour data</strong> to our systems for over an hour.</td></tr>");
                noPouredList.append("<tr align=justify><td colspan=4>Last check-in at <strong>");
                noPouredList.append(HandlerUtils.nullToEmpty((lastPouredTime.getTime().toString()).substring(0, 20))); // Last Poured Reading
                noPouredList.append("</strong></td></tr>");
                noPouredList.append("<tr align=center valign=middle><td height=20 colspan=4>&nbsp;</td></tr>");
                noPouredList.append("<tr align=justify><td colspan=4>Please try unplugging the power from the bevBox and plug it back in. This will reset the bevBox. You should also ensure the Ethernet cable is plugged in properly (Green light will be on where the Ethernet cable plugs into bevBox)</td></tr>");
                noPouredList.append("<tr align=center valign=middle><td height=20 colspan=4>&nbsp;</td></tr>");
                noPouredList.append("<tr align=justify><td colspan=4>Thank You,</td></tr>");
                noPouredList.append("<tr align=justify><td colspan=4>US Beverage Net Support</td></tr>");
                noPouredList.append("<tr align=center valign=middle><td height=35 colspan=4>&nbsp;</td></tr>");
                noPouredList.append("<tr align=justify><td colspan=4><strong>This email was automatically generated; please do not reply.</strong></td></tr><tr><td colspan=4>&nbsp;</td></tr>");
            }
        } catch (Exception e) {
            logger.dbError("Error: " + e.getMessage());
            throw new HandlerException(e);
        } finally {
            close(stmt);
            close(rs);
        }
        return noPouredList;
    }

    private StringBuilder getSoldList(int locationId, String locationName, String customerName, HashMap<Integer, String> soldPingList, int offSet) throws HandlerException {

        PreparedStatement stmt = null;
        ResultSet rs = null;

        Calendar lastSalesPing = Calendar.getInstance();

        StringBuilder noSoldList = new StringBuilder();

        String buff;
        long diff = 1000 * 60 * 60 * 1;
        Calendar buff1 = Calendar.getInstance();
        diff = buff1.getTimeInMillis() - diff;

        try {
            buff = soldPingList.get(locationId);
            lastSalesPing.set(Calendar.YEAR, Integer.parseInt(buff.substring(0, 4)));
            lastSalesPing.set(Calendar.MONTH, Integer.parseInt(buff.substring(5, 7)) - 1);
            lastSalesPing.set(Calendar.DAY_OF_MONTH, Integer.parseInt(buff.substring(8, 10)));
            lastSalesPing.set(Calendar.HOUR_OF_DAY, Integer.parseInt(buff.substring(11, 13)));
            lastSalesPing.set(Calendar.MINUTE, Integer.parseInt(buff.substring(14, 16)));
            lastSalesPing.set(Calendar.SECOND, Integer.parseInt(buff.substring(17)));

            if (diff > lastSalesPing.getTimeInMillis()) {
                lastSalesPing.add(Calendar.HOUR_OF_DAY, offSet);
                noSoldList.append("<tr align=justify><td colspan=4>Your USBN Gateway at ");
                noSoldList.append(HandlerUtils.nullToEmpty(locationName)); // Last Poured Reading
                noSoldList.append(" has <strong>not communicated draft beer sales data</strong> to our systems for over an hour.</td></tr>");
                noSoldList.append("<tr align=justify><td colspan=4>Last check-in at <strong>");
                noSoldList.append(HandlerUtils.nullToEmpty((lastSalesPing.getTime().toString()).substring(0, 20))); // Last Poured Reading
                noSoldList.append("</strong></td></tr>");
                noSoldList.append("<tr align=center valign=middle><td height=20 colspan=4>&nbsp;</td></tr>");
                noSoldList.append("<tr align=justify><td colspan=4>Please try restarting the USBN Gateway software. <a href=http://www.usbeveragenet.com/Docs/WebSite/Gateway%20Restart%20Instructions.pdf>Click here</a> for restart instructions.</td></tr>");
                noSoldList.append("<tr align=center valign=middle><td height=20 colspan=4>&nbsp;</td></tr>");
                noSoldList.append("<tr align=justify><td colspan=4>Thank You,</td></tr>");
                noSoldList.append("<tr align=justify><td colspan=4>US Beverage Net Support</td></tr>");
                noSoldList.append("<tr align=center valign=middle><td height=35 colspan=4>&nbsp;</td></tr>");
                noSoldList.append("<tr align=justify><td colspan=4><strong>This email was automatically generated; please do not reply.</strong></td></tr><tr><td colspan=4>&nbsp;</td></tr>");

            }
        } catch (Exception sqle) {
            logger.dbError("Error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
            close(rs);
        }
        return noSoldList;
    }

    /**
     * The folowing code is to update or insert the report requirements for each user for the email address
     * that they provide - SR
     */
    private void sendLocationStatusAlerts(Element toHandle, Element toAppend) throws HandlerException {


        String select = "SELECT name, email FROM user WHERE customer = 0 and emailReports = 1";

        String selectLocations = "SELECT l.id, l.name, l.customer, c.name, l.easternOffset, lD.soldUp, lD.pouredUp, " +
                " IFNULL(lastSold,'2005-01-01 00:00:00'), IFNULL(lastPoured,'2005-01-01 00:00:00') " +
                " FROM location l left join customer c on c.id = l.customer left join locationDetails lD ON lD.location = l.id " +
                " where lD.active=1 AND lD.suspended = 0 order by c.name, l.name";

        PreparedStatement stmt = null;
        ResultSet rs = null;

        try {
            int customerId = -1;
            int locationId = -1;
            int offSet = 0;
            int hasSold = -1;
            int hasPoured = -1;

            String emailAddr, userName, locationName, customerName;
            StringBuilder noPouredHeader = new StringBuilder();
            StringBuilder noSoldHeader = new StringBuilder();

            emailAddr = "";
            noSoldHeader.append("<tr align=center valign=middle><td colspan=4 ><strong><span style=text-decoration: underline>No Sales Reading Alert</span> </strong></td></tr><tr><td colspan=4>&nbsp;</td></tr>");
            noSoldHeader.append("<tr><td width=180 align=left><strong>Customer Name</strong></td><td width=180 align=left><strong>Location Name</strong></td><td width=200 align=center><strong>Last Reading</strong></td><td width=200 align=center></td></tr>");
            noPouredHeader.append("<tr align=center><td colspan=4 ><strong><span style=text-decoration: underline>No Pour Reading Alert</span> </strong></td></tr><tr><td colspan=4>&nbsp;</td></tr>");
            noPouredHeader.append("<tr><td width=180 align=left><strong>Customer Name</strong></td><td width=180 align=left><strong>Location Name</strong></td><td width=200 align=center><strong>Last Reading</strong></td><td width=200 align=center></td></tr>");
            customerName = "";

            long diff = 1000 * 60 * 60 * 3;
            long businessStart = 1000 * 60 * 60 * 13;
            long businessEnd = 1000 * 60 * 60 * 24;

            Calendar buff1 = Calendar.getInstance();
            diff = buff1.getTimeInMillis() - diff;


            StringBuilder noPouredList = new StringBuilder();
            StringBuilder noSoldList = new StringBuilder();
            long startMillis = System.currentTimeMillis();

            stmt = transconn.prepareStatement(selectLocations);
            rs = stmt.executeQuery();
            while (rs.next()) {
                logger.debug("Adding info for " + rs.getString(2));
                int soldUp = -1;
                int pouredUp = -1;
                locationId = rs.getInt(1);
                locationName = rs.getString(2);
                customerId = rs.getInt(3);
                customerName = rs.getString(4);
                offSet = rs.getInt(5);
                soldUp = rs.getInt(6);
                pouredUp = rs.getInt(7);

                if (soldUp > 0) {
                    noSoldList.append(getAllSoldList(locationId, locationName, customerName, rs.getString(8), offSet));
                }

                if (pouredUp > 0) {
                    noPouredList.append(getAllPouredList(locationId, locationName, customerName, rs.getString(9), offSet));
                }

            }

            long elapsedTime = System.currentTimeMillis() - startMillis;
            logger.debug("Alerts took " + elapsedTime + " ms to generate");

            /* Retreiving one super user information at a time*/
            stmt = transconn.prepareStatement(select);
            rs = stmt.executeQuery();

            /* This is the master loop for every super user*/

            while (rs.next()) {

                userName = rs.getString(1);
                emailAddr = rs.getString(2);
                Calendar businessTime = Calendar.getInstance();

                String h = businessTime.getTime().toString();

                int bushour = Integer.parseInt(h.substring(11, 13));
                long businessHour = (bushour) * 1000 * 60 * 60;

                if (noSoldList.length() != 0 || noPouredList.length() != 0) {
                    String emailTemplatePath = HandlerUtils.getSetting("email.templatePath");
                    if ((emailTemplatePath == null) || "".equals(emailTemplatePath)) {
                        emailTemplatePath = ".";
                    }
                    logger.debug("Packaging Email");
                    try {
                        logger.debug("Loading Template");
                        TemplatedMessage poEmail =
                                new TemplatedMessage("Location Status Alert",
                                emailTemplatePath, "locationStatusAlert");

                        //logger.debug("Setting Template Fields to "+email+" #"+String.valueOf(purchase));
                        poEmail.setSender("tech@beerboard.com");
                        //logger.debug("*");
                        poEmail.setRecipient(emailAddr);
                        //logger.debug("*");
                        poEmail.setField("DATE", Calendar.getInstance().getTime().toString());
                        poEmail.setField("CUSTOMER", customerName.toString());
                        logger.debug("businessHour: " + String.valueOf(businessHour));
                        logger.debug("businessStart: " + String.valueOf(businessStart));
                        logger.debug("businessEnd: " + String.valueOf(businessEnd));

                        if ((noSoldList.length() > 0)) {
                            hasSold = 1;
                            poEmail.setField("NOSOLDHEADER", noSoldHeader.toString());
                            poEmail.setField("NOSOLD", noSoldList.toString());
                        } else {
                            hasSold = -1;
                            poEmail.setField("NOSOLDHEADER", "");
                            poEmail.setField("NOSOLD", "");
                        }
                        if ((noPouredList.length() > 0)) {
                            hasPoured = 1;
                            poEmail.setField("NOPOUREDHEADER", noPouredHeader.toString());
                            poEmail.setField("NOPOURED", noPouredList.toString());
                        } else {
                            hasPoured = -1;
                            poEmail.setField("NOPOUREDHEADER", "");
                            poEmail.setField("NOPOURED", "");
                        }
                        logger.debug("Sending...");
                        logger.debug("hasSold: " + hasSold);
                        logger.debug("hasPoured: " + hasPoured);
                        if (hasSold > 0 || hasPoured > 0) {
                            poEmail.send();
                            logger.debug("Email sent successfully for " + userName);
                        } else {
                            logger.debug("Email was not sent for " + userName);
                        }
                    } catch (MailException me) {
                        logger.debug("Error sending purchase message to " + emailAddr + ": " + me.toString());
                        addErrorDetail(toAppend, "Error sending mail: " + me.toString());
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

    /** RETIRED
     * The folowing code is to update or insert the report requirements for each user for the email address
     * that they provide - SR
     */
    private void sendAlerts(Element toHandle, Element toAppend) throws HandlerException {

        int addAlertCount = HandlerUtils.getOptionalInteger(toHandle, "reactivateAlerts");

        String select = "SELECT id, user, emailAddr, location, noSoldAlert, noPouredAlert, soldAlertCount, pouredAlertCount FROM emailReports WHERE (noSoldAlert =1 OR noPouredAlert = 1) AND time = 0";
        String selectLocations = "SELECT l.name, l.customer, c.name, l.easternOffset FROM location l left join customer c on c.id = l.customer LEFT JOIN locationDetails lD ON lD.location = l.id where lD.active=1 AND l.id = ? order by c.name, l.name";
        String selectLastPouredSoldPing = "SELECT l.id, IFNULL(l.lastSold,'2005-01-01 00:00:00'), IFNULL(l.lastPoured,'2005-01-01 00:00:00') FROM location l LEFT JOIN locationDetails lD ON lD.location = l.id WHERE lD.active=1 AND l.id = ?";

        PreparedStatement stmt = null;
        ResultSet rs = null, rsLocation = null;

        HashMap<Integer, String> soldPingList = new HashMap<Integer, String>();
        HashMap<Integer, String> pouredPingList = new HashMap<Integer, String>();

        try {
            // Resetting the time counter to check for active alerts for the next business day
            if (addAlertCount > 0) {
                stmt = transconn.prepareStatement("UPDATE emailReports SET time = 0 ");
                stmt.executeUpdate();
            }
            int alertId = -1;
            int customerId = -1;
            int locationId = -1;
            int offSet = 0;
            int hasSold = -1;
            int hasPoured = -1;
            int soldCount = -1;
            int pouredCount = -1;
            int soldUp = -1;
            int pouredUp = -1;

            String emailAddr, userName, locationName, customerName;
            StringBuilder noPouredHeader = new StringBuilder();
            StringBuilder noSoldHeader = new StringBuilder();

            emailAddr = "";
            customerName = "";

            long diff = 1000 * 60 * 60 * 1;

            Calendar buff1 = Calendar.getInstance();
            diff = buff1.getTimeInMillis() - diff;

            /* Retreiving one super user information at a time*/
            stmt = transconn.prepareStatement(select);
            rs = stmt.executeQuery();
            while (rs.next()) {

                StringBuilder noPouredList = new StringBuilder();
                StringBuilder noSoldList = new StringBuilder();

                alertId = rs.getInt(1);
                userName = rs.getString(2);
                emailAddr = rs.getString(3);
                locationId = rs.getInt(4);
                soldUp = rs.getInt(5);
                pouredUp = rs.getInt(6);
                soldCount = rs.getInt(7);
                pouredCount = rs.getInt(8);

                stmt = transconn.prepareStatement(selectLastPouredSoldPing);
                stmt.setInt(1, locationId);
                rsLocation = stmt.executeQuery();
                if (rsLocation.next()) {
                    soldPingList.put(new Integer(rsLocation.getInt(1)), new String(rsLocation.getString(2)));
                    pouredPingList.put(new Integer(rsLocation.getInt(1)), new String(rsLocation.getString(3)));
                }
                long startMillis = System.currentTimeMillis();

                stmt = transconn.prepareStatement(selectLocations);
                stmt.setInt(1, locationId);
                rsLocation = stmt.executeQuery();
                if (rsLocation.next()) {
                    locationName = rsLocation.getString(1);
                    customerId = rsLocation.getInt(2);
                    customerName = rsLocation.getString(3);
                    offSet = rsLocation.getInt(4);
                    if ((soldUp > 0) && (soldCount < 4)) {
                        noSoldList.append(getSoldList(locationId, locationName, customerName, soldPingList, offSet));
                        if (noSoldList.length() != 0) {
                            // Incrementing soldcount counter and time flag to stop alerts going out for the day
                            stmt = transconn.prepareStatement("UPDATE emailReports SET soldAlertCount = ?, time = 1 WHERE id = ?");
                            stmt.setInt(1, soldCount + 1);
                            stmt.setInt(2, alertId);
                            stmt.executeUpdate();
                            String emailTemplatePath = HandlerUtils.getSetting("email.templatePath");
                            if ((emailTemplatePath == null) || "".equals(emailTemplatePath)) {
                                emailTemplatePath = ".";
                            }
                            logger.debug("Packaging Email to send Sold Alerts");
                            try {
                                logger.debug("Loading Template");
                                TemplatedMessage poEmail =
                                        new TemplatedMessage("Location Status Alert",
                                        emailTemplatePath, "locationStatusAlert");

                                //logger.debug("Setting Template Fields to "+email+" #"+String.valueOf(purchase));
                                poEmail.setSender("tech@beerboard.com");
                                //logger.debug("*");
                                poEmail.setRecipient(emailAddr);
                                poEmail.setRecipientBCC("gatewayalert@beerboard.com");
                                //logger.debug("*");
                                poEmail.setField("DATE", Calendar.getInstance().getTime().toString());
                                poEmail.setField("CUSTOMER", customerName.toString());

                                if ((noSoldList.length() > 0)) {
                                    hasSold = 1;
                                    poEmail.setField("NOSOLDHEADER", noSoldHeader.toString());
                                    poEmail.setField("NOSOLD", noSoldList.toString());
                                } else {
                                    hasSold = -1;
                                    poEmail.setField("NOSOLDHEADER", "");
                                    poEmail.setField("NOSOLD", "");
                                }
                                poEmail.setField("NOPOUREDHEADER", "");
                                poEmail.setField("NOPOURED", "");

                                logger.debug("Sending...");
                                logger.debug("hasSold: " + hasSold);
                                if (hasSold > 0) {
                                    poEmail.send();
                                    /*
                                    stmt = transconn.prepareStatement(updateUserAlerts);
                                    stmt.setInt(1, 1);
                                    stmt.setInt(2, alertId);
                                    stmt.executeUpdate();
                                     */
                                    logger.debug("Email sent successfully for " + userName);
                                } else {
                                    logger.debug("Email was not sent for " + userName);
                                }
                            } catch (MailException me) {
                                logger.debug("Error sending purchase message to " + emailAddr + ": " + me.toString());
                                addErrorDetail(toAppend, "Error sending mail: " + me.toString());
                            }
                        }
                    } else if (soldCount > 3) {
                        //Setting time flag so that power-up messages go out
                        stmt = transconn.prepareStatement("UPDATE emailReports SET time = 1 WHERE id = ?");
                        stmt.setInt(1, alertId);
                        stmt.executeUpdate();
                    }
                    //logger.debug("Size of pouredList: " + pouredPingList.size());
                    if ((pouredUp > 0) && (pouredCount < 4)) {
                        noPouredList.append(getPouredList(locationId, locationName, customerName, pouredPingList, offSet));
                        if (noPouredList.length() != 0) {
                            // Incrementing pouredcount counter and time flag to stop alerts going out for the day
                            stmt = transconn.prepareStatement("UPDATE emailReports SET pouredAlertCount = ?, time = 1 WHERE id = ?");
                            stmt.setInt(1, pouredCount + 1);
                            stmt.setInt(2, alertId);
                            stmt.executeUpdate();

                            String emailTemplatePath = HandlerUtils.getSetting("email.templatePath");
                            if ((emailTemplatePath == null) || "".equals(emailTemplatePath)) {
                                emailTemplatePath = ".";
                            }
                            logger.debug("Packaging Email to send Poured Alerts");
                            try {
                                logger.debug("Loading Template");
                                TemplatedMessage poEmail =
                                        new TemplatedMessage("Location Status Alert",
                                        emailTemplatePath, "locationStatusAlert");

                                //logger.debug("Setting Template Fields to "+email+" #"+String.valueOf(purchase));
                                poEmail.setSender("tech@beerboard.com");
                                //logger.debug("*");
                                poEmail.setRecipient(emailAddr);
                                poEmail.setRecipientBCC("boxalert@beerboard.com");
                                //logger.debug("*");
                                poEmail.setField("DATE", Calendar.getInstance().getTime().toString());
                                poEmail.setField("CUSTOMER", customerName.toString());

                                if ((noPouredList.length() > 0)) {
                                    hasPoured = 1;
                                    poEmail.setField("NOPOUREDHEADER", noPouredHeader.toString());
                                    poEmail.setField("NOPOURED", noPouredList.toString());
                                } else {
                                    hasPoured = -1;
                                    poEmail.setField("NOPOUREDHEADER", "");
                                    poEmail.setField("NOPOURED", "");
                                }
                                poEmail.setField("NOSOLDHEADER", "");
                                poEmail.setField("NOSOLD", "");
                                logger.debug("Sending...");
                                logger.debug("hasPoured: " + hasPoured);
                                if (hasPoured > 0) {
                                    poEmail.send();
                                    /*
                                    stmt = transconn.prepareStatement(updateUserAlerts);
                                    stmt.setInt(1, 1);
                                    stmt.setInt(2, alertId);
                                    stmt.executeUpdate();
                                     */
                                    logger.debug("Email sent successfully for " + userName);
                                } else {
                                    logger.debug("Email was not sent for " + userName);
                                }
                            } catch (MailException me) {
                                logger.debug("Error sending purchase message to " + emailAddr + ": " + me.toString());
                                addErrorDetail(toAppend, "Error sending mail: " + me.toString());
                            }
                        }
                    } else if (pouredCount > 3) {
                        //Setting time flag so that power-up messages go out
                        stmt = transconn.prepareStatement("UPDATE emailReports SET time = 1 WHERE id = ?");
                        stmt.setInt(1, alertId);
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
            close(rsLocation);
        }


    }

    /**
     * The folowing code is to update or insert the report requirements for each user for the email address
     * that they provide - SR
     */
    private void getVarianceTextAlerts(Element toHandle, Element toAppend) throws HandlerException {


        String selectUserInfo = "SELECT u.name, c.name, l.id, l.name, l.easternOffset, " +
                "t.mobile, t.carrier, t.varianceAlert, t.alertTime, t.user, t.customer " +
                "FROM textAlerts t LEFT JOIN location l on l.id=t.location " +
                "LEFT JOIN customer c on c.id = t.customer LEFT JOIN user u on u.id = t.user " +
                "WHERE t.type = 1 AND t.alert1 =1";

        PreparedStatement stmt = null;
        ResultSet rs = null;
        String currentTime = Calendar.getInstance().getTime().toString();
        currentTime = currentTime.substring(11, 16);
        logger.debug("currentTime: " + currentTime);
        try {

            stmt = transconn.prepareStatement(selectUserInfo);
            rs = stmt.executeQuery();

            while (rs.next()) {

                if (currentTime.compareTo(rs.getString(9)) == 0) {

                    Element user = toAppend.addElement("locationdet");
                    user.addElement("user").addText(rs.getString(1));
                    user.addElement("customer").addText(rs.getString(2));
                    user.addElement("locationId").addText(rs.getString(3));
                    user.addElement("location").addText(rs.getString(4));
                    user.addElement("offset").addText(rs.getString(5));
                    user.addElement("mobile").addText(rs.getString(6));
                    user.addElement("carrier").addText(rs.getString(7));
                    user.addElement("varianceAlert").addText(rs.getString(8));
                    user.addElement("alertTime").addText(rs.getString(9));
                    user.addElement("userId").addText(rs.getString(10));
                    user.addElement("customerId").addText(rs.getString(11));

                } else {
                    logger.debug("Time did not match");
                    logger.debug("DB Time: " + rs.getString(8));
                }

            }

        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.toString());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
            close(rs);
        }


    }

    private void getLowStockTextAlerts(Element toHandle, Element toAppend) throws HandlerException {


        String selectUserInfo = "SELECT u.name, c.name, l.id, l.name, l.easternOffset, " +
                "t.mobile, t.carrier, t.lowStkAlertTime, t.user, t.customer " +
                "FROM textAlerts t LEFT JOIN location l on l.id=t.location " +
                "LEFT JOIN customer c on c.id = t.customer " +
                "LEFT JOIN user u on u.id = t.user " +
                "WHERE t.type = 1 AND t.alert2 =1";

        PreparedStatement stmt = null;
        ResultSet rs = null;
        String currentTime = Calendar.getInstance().getTime().toString();
        currentTime = currentTime.substring(11, 16);
        try {
            logger.debug("currentTime: " + currentTime);

            stmt = transconn.prepareStatement(selectUserInfo);
            rs = stmt.executeQuery();

            while (rs.next()) {

                if (currentTime.compareTo(rs.getString(8)) == 0) {

                    Element user = toAppend.addElement("locationdet");
                    user.addElement("user").addText(rs.getString(1));
                    user.addElement("customer").addText(rs.getString(2));
                    user.addElement("locationId").addText(rs.getString(3));
                    user.addElement("location").addText(rs.getString(4));
                    user.addElement("offset").addText(rs.getString(5));
                    user.addElement("mobile").addText(rs.getString(6));
                    user.addElement("carrier").addText(rs.getString(7));
                    user.addElement("alertTime").addText(rs.getString(8));
                    user.addElement("userId").addText(rs.getString(9));
                    user.addElement("customerId").addText(rs.getString(10));
                } else {
                    logger.debug("Time did not match");
                    logger.debug("DB Time: " + rs.getString(8));
                }
            }

        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.toString());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
            close(rs);
        }


    }

    private void getConcessionLowStockTextAlerts(Element toHandle, Element toAppend) throws HandlerException {


        String selectUserInfo = "SELECT u.name, z.name, c.name, l.id, l.name, " +
                "l.easternOffset, t.mobile, t.carrier, t.lowStkAlertTime, t.user, t.customer " +
                "FROM textAlerts t LEFT JOIN location l on l.id=t.location " +
                "LEFT JOIN customer c on c.id = t.customer " +
                "LEFT JOIN user u on u.id = t.user " +
                "LEFT JOIN zone z on z.id = t.zone " +
                "WHERE t.type = 2 and t.alert2 =1 and t.customer = 79 order by l.name";

        PreparedStatement stmt = null;
        ResultSet rs = null;

        try {
            stmt = transconn.prepareStatement(selectUserInfo);
            rs = stmt.executeQuery();

            while (rs.next()) {


                Element user = toAppend.addElement("locationdet");
                user.addElement("user").addText(rs.getString(1));
                user.addElement("zone").addText(rs.getString(2));
                user.addElement("customer").addText(rs.getString(3));
                user.addElement("locationId").addText(rs.getString(4));
                user.addElement("location").addText(rs.getString(5));
                user.addElement("offset").addText(rs.getString(6));
                user.addElement("mobile").addText(rs.getString(7));
                user.addElement("carrier").addText(rs.getString(8));
                user.addElement("alertTime").addText(rs.getString(9));
                user.addElement("userId").addText(rs.getString(10));
                user.addElement("customerId").addText(rs.getString(11));

            }

        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.toString());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
            close(rs);
        }


    }

    /**
     * The folowing code is to update or insert the report requirements for each user for the email address
     * that they provide - SR
     */
    private void getConcessionVarianceTextAlerts(Element toHandle, Element toAppend) throws HandlerException {


        java.util.Date date = new java.util.Date();

        String selectUserInfo = "SELECT u.name, c.name, l.id, l.name, l.easternOffset, t.mobile, " +
                "t.carrier, t.varianceAlert, t.alertTime, t.user, t.customer, e.preOpen, e.eventEnd, l.volAdjustment " +
                "FROM textAlerts t LEFT JOIN location l ON l.id = t.location " +
                "LEFT JOIN customer c ON c.id = t.customer LEFT JOIN user u ON u.id = t.user " +
                "LEFT JOIN eventHours e ON e.location = l.id " +
                "WHERE t.alert1 =1 AND c.type = 2 " +
                "AND e.date = ? AND e.eventStart < ? AND e.eventEnd > ?";

        PreparedStatement stmt = null;
        ResultSet rs = null;

        String currentMonth, currentDay;
        String currentDate = "2008-08-01";


        currentMonth = String.valueOf(date.getMonth() + 1);
        currentDay = String.valueOf(date.getDate());

        if ((currentMonth.length()) < 2) {
            currentMonth = "0" + String.valueOf(date.getMonth() + 1);
        }

        if ((currentDay.length()) < 2) {
            currentDay = "0" + String.valueOf(date.getDate());
        }

        String currentTime = Calendar.getInstance().getTime().toString();
        currentTime = currentTime.substring(11, 19);

        currentDate = String.valueOf(1900 + date.getYear()) + "-" + currentMonth + "-" + currentDay + " " + currentTime;
        logger.debug("currentDate: " + currentDate);

        try {

            stmt = transconn.prepareStatement(selectUserInfo);
            stmt.setString(1, currentDate.substring(0, 11));
            stmt.setString(2, currentDate);
            stmt.setString(3, currentDate);
            rs = stmt.executeQuery();

            while (rs.next()) {

                Element user = toAppend.addElement("locationdet");
                user.addElement("user").addText(rs.getString(1));
                user.addElement("customer").addText(rs.getString(2));
                user.addElement("locationId").addText(rs.getString(3));
                user.addElement("location").addText(rs.getString(4));
                user.addElement("offset").addText(rs.getString(5));
                user.addElement("mobile").addText(rs.getString(6));
                user.addElement("carrier").addText(rs.getString(7));
                user.addElement("varianceAlert").addText(rs.getString(8));
                user.addElement("alertTime").addText(rs.getString(9));
                user.addElement("userId").addText(rs.getString(10));
                user.addElement("customerId").addText(rs.getString(11));
                user.addElement("eventStart").addText(rs.getString(12));
                user.addElement("eventEnd").addText(rs.getString(13));
                user.addElement("volAdjustment").addText(rs.getString(14));


            }

        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.toString());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
            close(rs);
        }


    }

    /** RETIRED
     * The folowing code is to update or insert the report requirements for each user for the email address
     * that they provide - SR
     */
    private void addAlerts(Element toHandle, Element toAppend) throws HandlerException {

        int callerId = getCallerId(toHandle);
        String select = " SELECT emailAddr FROM emailReports WHERE emailAddr=? and customer=?";

        String insert = " INSERT INTO emailReports (user, customer, emailAddr, noSoldAlert, noPouredAlert) " +
                " VALUES (?, ?, ?, ?, ?)";

        String update = " UPDATE emailReports SET user = ?, customer = ?,emailAddr = ?, noSoldAlert = ?, noPouredAlert = ?" +
                " WHERE emailAddr = ? and customer = ?";


        PreparedStatement stmt = null;
        ResultSet rs = null;

        try {

            int userId = HandlerUtils.getRequiredInteger(toHandle, "userId");
            int customerId = HandlerUtils.getRequiredInteger(toHandle, "customerId");
            String emailAddr = HandlerUtils.getRequiredString(toHandle, "emailAddr");
            Boolean noSold = HandlerUtils.getRequiredBoolean(toHandle, "noSoldReport");
            Boolean noPoured = HandlerUtils.getRequiredBoolean(toHandle, "noPouredReport");

            //Check that this product doesn't already exist in inventory at this location
            stmt = transconn.prepareStatement(select);
            stmt.setString(1, emailAddr);
            stmt.setInt(2, customerId);
            rs = stmt.executeQuery();

            if (rs.next()) {
                stmt = transconn.prepareStatement(update);
                stmt.setInt(1, userId);
                stmt.setInt(2, customerId);
                stmt.setString(3, emailAddr);
                stmt.setBoolean(4, noSold);
                stmt.setBoolean(5, noPoured);
                stmt.setString(6, emailAddr);
                stmt.setInt(7, customerId);
                rs = stmt.executeQuery();
            } else {
                stmt = transconn.prepareStatement(insert);
                stmt.setInt(1, userId);
                stmt.setInt(2, customerId);
                stmt.setString(3, emailAddr);
                stmt.setBoolean(4, noSold);
                stmt.setBoolean(5, noPoured);
                rs = stmt.executeQuery();
            }


        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
            close(rs);
        }


    }

    /** RETIRED
     * The folowing code is to update or insert the report requirements for each user for the email address
     * that they provide - SR
     */
    private void addLocationReports(Element toHandle, Element toAppend) throws HandlerException {

        int callerId = getCallerId(toHandle);
        String select = " SELECT emailAddr FROM emailReports WHERE emailAddr=? and location=?";

        String insert = " INSERT INTO emailReports (user, location, customer, emailAddr, locationReports) " +
                " VALUES (?, ?, ?, ?, ?) ";

        String update = " UPDATE emailReports SET user = ?,location = ?,customer = ?,emailAddr = ?,locationReports = ?" +
                " WHERE emailAddr = ? and location = ?";


        PreparedStatement stmt = null;
        ResultSet rs = null;

        logger.portalAction("addLocationReports");

        try {

            int locationId = HandlerUtils.getRequiredInteger(toHandle, "locationId");
            int userId = HandlerUtils.getRequiredInteger(toHandle, "userId");
            int customerId = HandlerUtils.getRequiredInteger(toHandle, "customerId");
            String emailAddr = HandlerUtils.getRequiredString(toHandle, "emailAddr");
            Boolean locationReport1 = HandlerUtils.getRequiredBoolean(toHandle, "locationReport1");

            //Check that this product doesn't already exist in inventory at this location
            stmt = transconn.prepareStatement(select);
            stmt.setString(1, emailAddr);
            stmt.setInt(2, locationId);
            rs = stmt.executeQuery();

            if (rs.next()) {

                stmt = transconn.prepareStatement(update);
                stmt.setInt(1, userId);
                stmt.setInt(2, locationId);
                stmt.setInt(3, customerId);
                stmt.setString(4, emailAddr);
                stmt.setBoolean(5, locationReport1);
                stmt.setString(6, emailAddr);
                stmt.setInt(7, locationId);
                stmt.executeUpdate();
                // Log the action test
                stmt = transconn.prepareStatement("SELECT LAST_INSERT_ID()");
                stmt.executeUpdate();
                if (rs.next()) {
                    int newId = rs.getInt(1);
                    String logMessage = "Updated Report Requirements";
                    logger.portalDetail(callerId, "addInventory", locationId, "Reports", newId, logMessage, transconn);

                }

            } else {

                stmt = transconn.prepareStatement(insert);
                stmt.setInt(1, userId);
                stmt.setInt(2, locationId);
                stmt.setInt(3, customerId);
                stmt.setString(4, emailAddr);
                stmt.setBoolean(5, locationReport1);
                stmt.executeUpdate();
                // Log the action
                stmt = transconn.prepareStatement("SELECT LAST_INSERT_ID()");
                rs = stmt.executeQuery();
                if (rs.next()) {
                    int newId = rs.getInt(1);
                    String logMessage = "Added NEW Report Requirement";
                    logger.portalDetail(callerId, "addInventory", locationId, "Reports", newId, logMessage, transconn);
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

    /**
     * The folowing code is to update or insert the SMS alert requirements for each user for the location
     * that they provide - AD
     */
    private void addSMSAlerts(Element toHandle, Element toAppend) throws HandlerException {

        int callerId = getCallerId(toHandle);
        String select = " SELECT t.user, t.location, l.easternOffset FROM location l LEFT JOIN textAlerts t ON t.location = l.id WHERE t.user=? and t.location=?";

        String selectEasternOffset = "SELECT l.easternOffset FROM location l WHERE l.id =?";
        String insert = " INSERT INTO textAlerts (user, location, customer, mobile, carrier, alert1, alert2, varianceAlert, alertTime, lowStkAlertTime) " +
                " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ";

        String update = " UPDATE textAlerts SET user = ?,location = ?,customer = ?,mobile = ?,carrier = ?, alert1=?, alert2=?, varianceAlert=?, alertTime=?, lowStkAlertTime=?" +
                " WHERE user = ? and location = ?";


        PreparedStatement stmt = null;
        ResultSet rs = null;

        logger.portalAction("addSMSAlerts");

        try {

            int locationId = HandlerUtils.getRequiredInteger(toHandle, "locationId");
            int userId = HandlerUtils.getRequiredInteger(toHandle, "userId");
            int customerId = HandlerUtils.getRequiredInteger(toHandle, "customerId");
            String mobileVal = HandlerUtils.getRequiredString(toHandle, "mobile");
            String carrierVal = HandlerUtils.getRequiredString(toHandle, "carrier");
            Boolean smsalert1 = HandlerUtils.getRequiredBoolean(toHandle, "smsalert1");
            Boolean smsalert2 = HandlerUtils.getRequiredBoolean(toHandle, "smsalert2");
            float varianceAlert = HandlerUtils.getRequiredFloat(toHandle, "varianceValue");
            String alertTime = HandlerUtils.getRequiredString(toHandle, "alertTime");
            String lowStkAlertTime = HandlerUtils.getRequiredString(toHandle, "lowStkAlertTime");

            //Check that this product doesn't already exist in inventory at this location
            stmt = transconn.prepareStatement(select);
            stmt.setInt(1, userId);
            stmt.setInt(2, locationId);
            rs = stmt.executeQuery();

            if (rs.next()) {

                int easternOffsetVal = rs.getInt(3);
                String hrAlert = alertTime.substring(0, 2);
                int alertTimeVal = Integer.parseInt(hrAlert);
                alertTimeVal = alertTimeVal - easternOffsetVal;
                if (easternOffsetVal != 0 && alertTimeVal > 23) {
                    alertTimeVal = alertTimeVal - 24;
                }
                if (String.valueOf(alertTimeVal).length() < 2) {
                    alertTime = "0" + String.valueOf(alertTimeVal) + alertTime.substring(2);
                } else {
                    alertTime = String.valueOf(alertTimeVal) + alertTime.substring(2);
                }

                logger.debug("EasternOffstValue" + String.valueOf(easternOffsetVal));
                logger.debug("AlertTimeValue" + String.valueOf(alertTimeVal));
                logger.debug("alertTime----" + String.valueOf(alertTime));

                //low stock alert time
                String lowStkHrAlert = lowStkAlertTime.substring(0, 2);
                int lowStkAlertTimeVal = Integer.parseInt(lowStkHrAlert);
                lowStkAlertTimeVal = lowStkAlertTimeVal - easternOffsetVal;
                if (easternOffsetVal != 0 && lowStkAlertTimeVal > 23) {
                    lowStkAlertTimeVal = lowStkAlertTimeVal - 24;
                }
                if (String.valueOf(lowStkAlertTimeVal).length() < 2) {
                    lowStkAlertTime = "0" + String.valueOf(lowStkAlertTimeVal) + lowStkAlertTime.substring(2);
                } else {
                    lowStkAlertTime = String.valueOf(lowStkAlertTimeVal) + lowStkAlertTime.substring(2);
                }
                logger.debug("AlertTimeValue" + String.valueOf(lowStkAlertTimeVal));
                logger.debug("alertTime----" + String.valueOf(lowStkAlertTime));
                //
                stmt = transconn.prepareStatement(update);
                stmt.setInt(1, userId);
                stmt.setInt(2, locationId);
                stmt.setInt(3, customerId);
                stmt.setString(4, mobileVal);
                stmt.setString(5, carrierVal);
                stmt.setBoolean(6, smsalert1);
                stmt.setBoolean(7, smsalert2);
                stmt.setFloat(8, varianceAlert);
                stmt.setString(9, alertTime);
                stmt.setString(10, lowStkAlertTime);
                stmt.setInt(11, userId);
                stmt.setInt(12, locationId);
                stmt.executeUpdate();
                // Log the action test
                stmt = transconn.prepareStatement("SELECT LAST_INSERT_ID()");
                rs = stmt.executeQuery();
                if (rs.next()) {
                    int newId = rs.getInt(1);
                    String logMessage = "Updated SMS Alert Requirements";
                    logger.portalDetail(callerId, "addInventory", locationId, "SMS", newId, logMessage, transconn);

                }

            } else {

                stmt = transconn.prepareStatement(selectEasternOffset);
                stmt.setInt(1, locationId);
                rs = stmt.executeQuery();
                if (rs.next()) {
                    int easternOffsetVal = rs.getInt(1);
                    String hrAlert = alertTime.substring(0, 2);
                    int alertTimeVal = Integer.parseInt(hrAlert);
                    alertTimeVal = alertTimeVal - easternOffsetVal;

                    if (easternOffsetVal != 0 && alertTimeVal > 23) {
                        alertTimeVal = alertTimeVal - 24;
                    }
                    if (String.valueOf(alertTimeVal).length() < 2) {
                        alertTime = "0" + String.valueOf(alertTimeVal) + alertTime.substring(2);
                    } else {
                        alertTime = String.valueOf(alertTimeVal) + alertTime.substring(2);
                    }
                    //low stock alert time
                    String lowStkHrAlert = lowStkAlertTime.substring(0, 2);
                    int lowStkAlertTimeVal = Integer.parseInt(lowStkHrAlert);
                    lowStkAlertTimeVal = lowStkAlertTimeVal - easternOffsetVal;
                    if (easternOffsetVal != 0 && lowStkAlertTimeVal > 23) {
                        lowStkAlertTimeVal = lowStkAlertTimeVal - 24;
                    }
                    if (String.valueOf(lowStkAlertTimeVal).length() < 2) {
                        lowStkAlertTime = "0" + String.valueOf(lowStkAlertTimeVal) + lowStkAlertTime.substring(2);
                    } else {
                        lowStkAlertTime = String.valueOf(lowStkAlertTimeVal) + lowStkAlertTime.substring(2);
                    }
                    //
                }
                stmt = transconn.prepareStatement(insert);
                stmt.setInt(1, userId);
                stmt.setInt(2, locationId);
                stmt.setInt(3, customerId);
                stmt.setString(4, mobileVal);
                stmt.setString(5, carrierVal);
                stmt.setBoolean(6, smsalert1);
                stmt.setBoolean(7, smsalert1);
                stmt.setFloat(8, varianceAlert);
                stmt.setString(9, alertTime);
                stmt.setString(10, lowStkAlertTime);
                stmt.executeUpdate();
                // Log the action
                stmt = transconn.prepareStatement("SELECT LAST_INSERT_ID()");
                rs = stmt.executeQuery();
                if (rs.next()) {
                    int newId = rs.getInt(1);
                    String logMessage = "Added NEW SMS alert Requirement";
                    logger.portalDetail(callerId, "addInventory", locationId, "SMS", newId, logMessage, transconn);
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

    /**
     * The folowing code is to update or insert the report requirements for each user for the email address
     * that they provide - SR
     */
    private void getLocationTextAlerts(Element toHandle, Element toAppend) throws HandlerException {

        int callerId = getCallerId(toHandle);
        String select = " SELECT alert1, alert2 FROM textAlerts WHERE mobile=? and location=?";

        PreparedStatement stmt = null;
        ResultSet rs = null;

        logger.portalAction("getLocationTextAlerts");

        try {
            int locationId = -1;
            locationId = HandlerUtils.getOptionalInteger(toHandle, "locationId");
            int userId = HandlerUtils.getRequiredInteger(toHandle, "userId");
            int customerId = HandlerUtils.getRequiredInteger(toHandle, "customerId");
            String mobile = HandlerUtils.getRequiredString(toHandle, "mobile");

            if (locationId > 0) {

                stmt = transconn.prepareStatement(select);
                stmt.setString(1, mobile);
                stmt.setInt(2, locationId);
                rs = stmt.executeQuery();
                if (rs.next()) {
                    toAppend.addElement("locationTextAlert1").addText(HandlerUtils.nullToEmpty(rs.getString(1)));
                    toAppend.addElement("locationTextAlert2").addText(HandlerUtils.nullToEmpty(rs.getString(2)));
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

    /** RETIRED
     * The folowing code is to update or insert the report requirements for each user for the email address
     * that they provide - SR
     */
    private void getLocationReports(Element toHandle, Element toAppend) throws HandlerException {

        int callerId = getCallerId(toHandle);
        String select = " SELECT locationReports FROM emailReports WHERE emailAddr=? and location=?";

        PreparedStatement stmt = null;
        ResultSet rs = null;

        logger.portalAction("getLocationReports");

        try {
            int locationId = -1;
            locationId = HandlerUtils.getOptionalInteger(toHandle, "locationId");
            int userId = HandlerUtils.getRequiredInteger(toHandle, "userId");
            int customerId = HandlerUtils.getRequiredInteger(toHandle, "customerId");
            String emailAddr = HandlerUtils.getRequiredString(toHandle, "emailAddr");

            if (locationId > 0) {

                stmt = transconn.prepareStatement(select);
                stmt.setString(1, emailAddr);
                stmt.setInt(2, locationId);
                rs = stmt.executeQuery();
                if (rs.next()) {
                    toAppend.addElement("locationReport1").addText(HandlerUtils.nullToEmpty(rs.getString(1)));
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

    /**
     * The folowing code is to update or insert the sms alerts for each user for the user and location
     * that they provide - AD
     */
    private void getLocationSMSAlerts(Element toHandle, Element toAppend) throws HandlerException {

        int callerId = getCallerId(toHandle);
        String select = " SELECT alert1, alert2, alert3, varianceAlert, alertTime,lowStkAlertTime FROM textAlerts WHERE user=? and location=? and customer=?";
        String easternOffsetVal = "SELECT l.easternOffset from location l WHERE l.id = ?";
        PreparedStatement stmt = null;
        ResultSet rs = null;

        logger.portalAction("getLocationSMSAlerts");

        try {
            int locationId = -1;
            locationId = HandlerUtils.getOptionalInteger(toHandle, "locationId");
            int userId = HandlerUtils.getRequiredInteger(toHandle, "userId");
            int customerId = HandlerUtils.getRequiredInteger(toHandle, "customerId");


            if (locationId > 0) {

                stmt = transconn.prepareStatement(easternOffsetVal);
                stmt.setInt(1, locationId);
                rs = stmt.executeQuery();
                if (rs.next()) {
                    int easternOffsetVal1 = rs.getInt(1);
                    logger.debug("EasternOffset " + String.valueOf(easternOffsetVal1));


                    stmt = transconn.prepareStatement(select);
                    stmt.setInt(1, userId);
                    stmt.setInt(2, locationId);
                    stmt.setInt(3, customerId);
                    rs = stmt.executeQuery();
                    if (rs.next()) {


                        String hrAlert = rs.getString(5).substring(0, 2);
                        logger.debug("hrAlert " + hrAlert);
                        int alertTimeVal = Integer.parseInt(hrAlert);
                        logger.debug("alertTimeVal " + String.valueOf(alertTimeVal));

                        alertTimeVal = alertTimeVal + easternOffsetVal1;

                        if (easternOffsetVal1 != 0 && alertTimeVal < 0) {
                            alertTimeVal = alertTimeVal + 24;
                        }
                        logger.debug("alertTimeValDiff " + String.valueOf(alertTimeVal));
                        String alertTime;

                        if (String.valueOf(alertTimeVal).length() < 2) {
                            alertTime = "0" + String.valueOf(alertTimeVal) + rs.getString(5).substring(2);
                        } else {
                            alertTime = String.valueOf(alertTimeVal) + rs.getString(5).substring(2);
                        }

                        logger.debug("alertTime " + alertTime);

                        //for low stock alert time
                        String lowStkHrAlert = rs.getString(6).substring(0, 2);
                        logger.debug("hrAlert " + lowStkHrAlert);
                        int lowStkAlertTimeVal = Integer.parseInt(lowStkHrAlert);
                        logger.debug("alertTimeVal " + String.valueOf(lowStkAlertTimeVal));

                        lowStkAlertTimeVal = lowStkAlertTimeVal + easternOffsetVal1;

                        if (easternOffsetVal1 != 0 && lowStkAlertTimeVal < 0) {
                            lowStkAlertTimeVal = lowStkAlertTimeVal + 24;
                        }
                        logger.debug("alertTimeValDiff " + String.valueOf(lowStkAlertTimeVal));
                        String lowStkAlertTime;

                        if (String.valueOf(lowStkAlertTimeVal).length() < 2) {
                            lowStkAlertTime = "0" + String.valueOf(lowStkAlertTimeVal) + rs.getString(6).substring(2);
                        } else {
                            lowStkAlertTime = String.valueOf(lowStkAlertTimeVal) + rs.getString(6).substring(2);
                        }

                        logger.debug("alertTime " + lowStkAlertTime);
                        //
                        toAppend.addElement("smsalert1").addText(HandlerUtils.nullToEmpty(rs.getString(1)));
                        toAppend.addElement("smsalert2").addText(HandlerUtils.nullToEmpty(rs.getString(2)));
                        toAppend.addElement("smsalert3").addText(HandlerUtils.nullToEmpty(rs.getString(3)));
                        toAppend.addElement("varianceAlert").addText(HandlerUtils.nullToEmpty(rs.getString(4)));
                        toAppend.addElement("alertTime").addText(HandlerUtils.nullToEmpty(alertTime));
                        toAppend.addElement("lowStkAlertTime").addText(HandlerUtils.nullToEmpty(lowStkAlertTime));
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

    /**
     * The folowing code is to update or insert the report requirements for each user for the email address
     * that they provide - SR
     */
    private void checkRewards(Element toHandle, Element toAppend) throws HandlerException {

        String select = " SELECT showRewards FROM user WHERE username=?";

        PreparedStatement stmt = null;
        ResultSet rs = null;

        logger.portalAction("checkRewards");

        try {
            String username = HandlerUtils.getRequiredString(toHandle, "username");

            stmt = transconn.prepareStatement(select);
            stmt.setString(1, username);
            rs = stmt.executeQuery();
            if (rs.next()) {
                toAppend.addElement("showRewards").addText(HandlerUtils.nullToEmpty(rs.getString(1)));
            }


        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
            close(rs);
        }


    }

    /** RETIRED
     * The folowing code is to update or insert the report requirements for each user for the email address
     * that they provide - SR
     */
    private void addReports(Element toHandle, Element toAppend) throws HandlerException {

        int callerId = getCallerId(toHandle);
        String select = " SELECT emailAddr FROM emailReports WHERE emailAddr=? and location=?";

        String insert = " INSERT INTO emailReports (user, location, customer, emailAddr, report1, report2, report3, report4, report5, report6, exceptionsOnly) " +
                " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ";

        String update = " UPDATE emailReports SET user = ?,location = ?,customer = ?,emailAddr = ?,report1 = ?,report2 = ?,report3 = ?,report4 = ?,report5 = ?,report6 = ?,exceptionsOnly = ?" +
                " WHERE emailAddr = ? and location = ?";


        PreparedStatement stmt = null;
        ResultSet rs = null;

        logger.portalAction("addTest");

        try {

            int locationId = HandlerUtils.getRequiredInteger(toHandle, "locationId");
            int userId = HandlerUtils.getRequiredInteger(toHandle, "userId");
            int customerId = HandlerUtils.getRequiredInteger(toHandle, "customerId");
            String emailAddr = HandlerUtils.getRequiredString(toHandle, "emailAddr");
            Boolean report1 = HandlerUtils.getRequiredBoolean(toHandle, "Report1");
            Boolean report2 = HandlerUtils.getRequiredBoolean(toHandle, "Report2");
            Boolean report3 = HandlerUtils.getRequiredBoolean(toHandle, "Report3");
            Boolean report4 = HandlerUtils.getRequiredBoolean(toHandle, "Report4");
            Boolean report5 = HandlerUtils.getRequiredBoolean(toHandle, "Report5");
            Boolean report6 = HandlerUtils.getRequiredBoolean(toHandle, "Report6");
            Boolean exceptionsOnly = HandlerUtils.getRequiredBoolean(toHandle, "exceptionsOnly");

            //Check that this product doesn't already exist in inventory at this location
            stmt = transconn.prepareStatement(select);
            stmt.setString(1, emailAddr);
            stmt.setInt(2, locationId);
            rs = stmt.executeQuery();

            if (rs.next()) {

                stmt = transconn.prepareStatement(update);
                stmt.setInt(1, userId);
                stmt.setInt(2, locationId);
                stmt.setInt(3, customerId);
                stmt.setString(4, emailAddr);
                stmt.setBoolean(5, report1);
                stmt.setBoolean(6, report2);
                stmt.setBoolean(7, report3);
                stmt.setBoolean(8, report4);
                stmt.setBoolean(9, report5);
                stmt.setBoolean(10, report6);
                stmt.setBoolean(11, exceptionsOnly);
                stmt.setString(12, emailAddr);
                stmt.setInt(13, locationId);
                stmt.executeUpdate();

                // Log the action
                stmt = transconn.prepareStatement("SELECT LAST_INSERT_ID()");
                rs = stmt.executeQuery();
                if (rs.next()) {
                    int newId = rs.getInt(1);
                    String logMessage = "Updated Report Requirements";
                    logger.portalDetail(callerId, "addInventory", locationId, "Reports", newId, logMessage, transconn);

                }

            } else {
                stmt = transconn.prepareStatement(insert);
                stmt.setInt(1, userId);
                stmt.setInt(2, locationId);
                stmt.setInt(3, customerId);
                stmt.setString(4, emailAddr);
                stmt.setBoolean(5, report1);
                stmt.setBoolean(6, report2);
                stmt.setBoolean(7, report3);
                stmt.setBoolean(8, report4);
                stmt.setBoolean(9, report5);
                stmt.setBoolean(10, report6);
                stmt.setBoolean(11, exceptionsOnly);
                stmt.executeUpdate();
                // Log the action
                stmt = transconn.prepareStatement("SELECT LAST_INSERT_ID()");
                rs = stmt.executeQuery();
                if (rs.next()) {
                    int newId = rs.getInt(1);
                    String logMessage = "Added NEW Report Requirement";
                    logger.portalDetail(callerId, "addInventory", locationId, "Reports", newId, logMessage, transconn);
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

    /**
     * The folowing code is to update or insert the shift hour information for each location - SR
     */
    private void addVarianceValue(Element toHandle, Element toAppend) throws HandlerException {

        int callerId = getCallerId(toHandle);
        String select = " SELECT id FROM location WHERE id=?";

        String insert = " INSERT INTO location (varianceAlert) " +
                " VALUES (?) ";

        String update = " UPDATE location SET varianceAlert = ?" +
                " WHERE id = ?";


        PreparedStatement stmt = null;
        ResultSet rs = null;

        logger.portalAction("VarianceValue");

        try {

            int locationId = HandlerUtils.getRequiredInteger(toHandle, "locationId");

            String varianceAlert = HandlerUtils.getRequiredString(toHandle, "varianceAlert");

            //Check that this product doesn't already exist in inventory at this location
            stmt = transconn.prepareStatement(select);
            stmt.setInt(1, locationId);
            rs = stmt.executeQuery();

            if (rs.next()) {

                stmt = transconn.prepareStatement(update);
                stmt.setString(1, varianceAlert);
                stmt.setInt(2, locationId);
                stmt.executeUpdate();

                // Log the action
                stmt = transconn.prepareStatement("SELECT LAST_INSERT_ID()");
                rs = stmt.executeQuery();
                if (rs.next()) {
                    int newId = rs.getInt(1);
                    String logMessage = "Updated Shift Timing for a Location";
                    logger.portalDetail(callerId, "addInventory", locationId, "Reports", newId, logMessage, transconn);

                }

            } else {
                stmt = transconn.prepareStatement(insert);
                stmt.setString(1, varianceAlert);
                stmt.executeUpdate();
                // Log the action
                stmt = transconn.prepareStatement("SELECT LAST_INSERT_ID()");
                rs = stmt.executeQuery();
                if (rs.next()) {
                    int newId = rs.getInt(1);
                    String logMessage = "Added NEW Shift Timing for a location";
                    logger.portalDetail(callerId, "addInventory", locationId, "Reports", newId, logMessage, transconn);
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

    /**
     * The folowing code is to update or insert the shift hour information for each location - SR
     */
    private void addShifts(Element toHandle, Element toAppend) throws HandlerException {

        int callerId = getCallerId(toHandle);
        String select = " SELECT id FROM locationHours WHERE location=?";

        String insert = " INSERT INTO locationHours (location, customer, user, preOpenSun, openSun, closeSun, preOpenMon, openMon, closeMon, preOpenTue, openTue, closeTue, preOpenWed, openWed, closeWed, preOpenThu, openThu, closeThu, preOpenFri, openFri, closeFri, preOpenSat, openSat, closeSat) " +
                " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ";

        String update = " UPDATE locationHours SET location = ?, customer = ?, user = ?, preOpenSun = ?, openSun = ?, closeSun = ?, preOpenMon = ?, openMon = ?, closeMon = ?, preOpenTue = ?, openTue = ?, closeTue = ?, preOpenWed = ?, openWed = ?, closeWed = ?, preOpenThu = ?, openThu = ?, closeThu = ?, preOpenFri = ?, openFri = ?, closeFri = ?, preOpenSat = ?, openSat = ?, closeSat = ?" +
                " WHERE location = ?";


        PreparedStatement stmt = null;
        ResultSet rs = null;

        logger.portalAction("addShifts");

        try {

            int locationId = HandlerUtils.getRequiredInteger(toHandle, "locationId");
            int userId = HandlerUtils.getRequiredInteger(toHandle, "userId");
            int customerId = HandlerUtils.getRequiredInteger(toHandle, "customerId");

            String start1TimeSun = HandlerUtils.getRequiredString(toHandle, "Shift1StartSun");
            String start2TimeSun = HandlerUtils.getRequiredString(toHandle, "Shift2StartSun");
            String start3TimeSun = HandlerUtils.getRequiredString(toHandle, "Shift3StartSun");
            String start1TimeMon = HandlerUtils.getRequiredString(toHandle, "Shift1StartMon");
            String start2TimeMon = HandlerUtils.getRequiredString(toHandle, "Shift2StartMon");
            String start3TimeMon = HandlerUtils.getRequiredString(toHandle, "Shift3StartMon");
            String start1TimeTues = HandlerUtils.getRequiredString(toHandle, "Shift1StartTues");
            String start2TimeTues = HandlerUtils.getRequiredString(toHandle, "Shift2StartTues");
            String start3TimeTues = HandlerUtils.getRequiredString(toHandle, "Shift3StartTues");
            String start1TimeWed = HandlerUtils.getRequiredString(toHandle, "Shift1StartWed");
            String start2TimeWed = HandlerUtils.getRequiredString(toHandle, "Shift2StartWed");
            String start3TimeWed = HandlerUtils.getRequiredString(toHandle, "Shift3StartWed");
            String start1TimeThur = HandlerUtils.getRequiredString(toHandle, "Shift1StartThur");
            String start2TimeThur = HandlerUtils.getRequiredString(toHandle, "Shift2StartThur");
            String start3TimeThur = HandlerUtils.getRequiredString(toHandle, "Shift3StartThur");
            String start1TimeFri = HandlerUtils.getRequiredString(toHandle, "Shift1StartFri");
            String start2TimeFri = HandlerUtils.getRequiredString(toHandle, "Shift2StartFri");
            String start3TimeFri = HandlerUtils.getRequiredString(toHandle, "Shift3StartFri");
            String start1TimeSat = HandlerUtils.getRequiredString(toHandle, "Shift1StartSat");
            String start2TimeSat = HandlerUtils.getRequiredString(toHandle, "Shift2StartSat");
            String start3TimeSat = HandlerUtils.getRequiredString(toHandle, "Shift3StartSat");


            //Check that this product doesn't already exist in inventory at this location
            stmt = transconn.prepareStatement(select);
            stmt.setInt(1, locationId);
            rs = stmt.executeQuery();

            if (rs.next()) {

                stmt = transconn.prepareStatement(update);

                stmt.setInt(1, locationId);
                stmt.setInt(2, customerId);
                stmt.setInt(3, userId);
                stmt.setString(4, start1TimeSun);
                stmt.setString(5, start2TimeSun);
                stmt.setString(6, start3TimeSun);
                stmt.setString(7, start1TimeMon);
                stmt.setString(8, start2TimeMon);
                stmt.setString(9, start3TimeMon);
                stmt.setString(10, start1TimeTues);
                stmt.setString(11, start2TimeTues);
                stmt.setString(12, start3TimeTues);
                stmt.setString(13, start1TimeWed);
                stmt.setString(14, start2TimeWed);
                stmt.setString(15, start3TimeWed);
                stmt.setString(16, start1TimeThur);
                stmt.setString(17, start2TimeThur);
                stmt.setString(18, start3TimeThur);
                stmt.setString(19, start1TimeFri);
                stmt.setString(20, start2TimeFri);
                stmt.setString(21, start3TimeFri);
                stmt.setString(22, start1TimeSat);
                stmt.setString(23, start2TimeSat);
                stmt.setString(24, start3TimeSat);
                stmt.setInt(25, locationId);
                stmt.executeUpdate();
                // Log the action
                stmt = transconn.prepareStatement("SELECT LAST_INSERT_ID()");
                rs = stmt.executeQuery();
                if (rs.next()) {
                    int newId = rs.getInt(1);
                    String logMessage = "Updated Shift Timing for a Location";
                    logger.portalDetail(callerId, "addInventory", locationId, "Reports", newId, logMessage, transconn);

                }

            } else {
                stmt = transconn.prepareStatement(insert);

                stmt.setInt(1, locationId);
                stmt.setInt(2, customerId);
                stmt.setInt(3, userId);
                stmt.setString(4, start1TimeSun);
                stmt.setString(5, start2TimeSun);
                stmt.setString(6, start3TimeSun);
                stmt.setString(7, start1TimeMon);
                stmt.setString(8, start2TimeMon);
                stmt.setString(9, start3TimeMon);
                stmt.setString(10, start1TimeTues);
                stmt.setString(11, start2TimeTues);
                stmt.setString(12, start3TimeTues);
                stmt.setString(13, start1TimeWed);
                stmt.setString(14, start2TimeWed);
                stmt.setString(15, start3TimeWed);
                stmt.setString(16, start1TimeThur);
                stmt.setString(17, start2TimeThur);
                stmt.setString(18, start3TimeThur);
                stmt.setString(19, start1TimeFri);
                stmt.setString(20, start2TimeFri);
                stmt.setString(21, start3TimeFri);
                stmt.setString(22, start1TimeSat);
                stmt.setString(23, start2TimeSat);
                stmt.setString(24, start3TimeSat);
                stmt.executeUpdate();
                // Log the action
                stmt = transconn.prepareStatement("SELECT LAST_INSERT_ID()");
                rs = stmt.executeQuery();
                if (rs.next()) {
                    int newId = rs.getInt(1);
                    String logMessage = "Added NEW Shift Timing for a location";
                    logger.portalDetail(callerId, "addShifts", locationId, "Reports", newId, logMessage, transconn);
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

    private void getLogCategory(Element toHandle, Element toAppend)
            throws HandlerException {

        String selectLogCategory = "SELECT id, name FROM logCategory order by name";


        PreparedStatement stmt = null;
        ResultSet rs = null;

        try {
            stmt = transconn.prepareStatement(selectLogCategory);
            rs = stmt.executeQuery();
            while (rs != null && rs.next()) {

                int rsIndex = 1;
                Element locEl = toAppend.addElement("logCategory");
                locEl.addElement("logCategoryId").addText(HandlerUtils.nullToEmpty((rs.getString(rsIndex++))));
                locEl.addElement("logCategoryName").addText(HandlerUtils.nullToEmpty((rs.getString(rsIndex++))));
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
     * The folowing code is to update or insert the shift hour information for each location - SR
     */
    private void getLocationLogs(Element toHandle, Element toAppend) throws HandlerException {

        int locationId = -1;
        int customerId = -1;
        int logId = -1;

        locationId = HandlerUtils.getOptionalInteger(toHandle, "locationId");
        customerId = HandlerUtils.getOptionalInteger(toHandle, "customerId");
        logId = HandlerUtils.getOptionalInteger(toHandle, "logId");

        String selectCustomerLogs = " SELECT id, location, date, category, status, resolutionDate FROM techLogs WHERE customer=? ORDER BY date LIMIT 15";

        String selectLocationLogs = " SELECT id, location, date, category, issue, resolution, status, resolutionDate FROM techLogs WHERE location=? ORDER BY date LIMIT 15";

        String selectLogs = " SELECT id, location, date, category, issue, resolution, status, resolutionDate FROM techLogs WHERE id=?";

        String selectLocation = " SELECT name FROM location WHERE id=?";

        String selectLogCategory = " SELECT name FROM logCategory WHERE id=?";

        int location = -1;
        int catId = -1;
        PreparedStatement stmt = null;
        ResultSet rs = null;

        PreparedStatement stmt1 = null;
        ResultSet rs1 = null;

        try {
            if (customerId > 0) {

                stmt = transconn.prepareStatement(selectCustomerLogs);
                stmt.setInt(1, customerId);
                rs = stmt.executeQuery();

                while (rs.next()) {
                    int i = 1;
                    location = rs.getInt(2);
                    catId = rs.getInt(4);
                    Element logsEl = toAppend.addElement("logs");
                    logsEl.addElement("logId").addText(HandlerUtils.nullToEmpty(rs.getString(i++)));
                    logsEl.addElement("locationId").addText(HandlerUtils.nullToEmpty(rs.getString(i++)));
                    logsEl.addElement("date").addText(HandlerUtils.nullToEmpty((rs.getString(i++)).substring(0, 10)));
                    logsEl.addElement("categoryId").addText(HandlerUtils.nullToEmpty(rs.getString(i++)));
                    logsEl.addElement("status").addText(HandlerUtils.nullToEmpty(rs.getString(i++)));
                    logsEl.addElement("resolutionDate").addText(HandlerUtils.nullToEmpty((rs.getString(i++)).substring(0, 10)));

                    stmt1 = transconn.prepareStatement(selectLocation);
                    stmt1.setInt(1, location);
                    rs1 = stmt1.executeQuery();

                    if (rs1.next()) {
                        logsEl.addElement("locationName").addText(HandlerUtils.nullToEmpty(rs1.getString(1)));
                    }

                    stmt1 = transconn.prepareStatement(selectLogCategory);
                    stmt1.setInt(1, catId);
                    rs1 = stmt1.executeQuery();

                    if (rs1.next()) {
                        logsEl.addElement("category").addText(HandlerUtils.nullToEmpty(rs1.getString(1)));
                    }

                }


            }


            if (logId > 0) {

                stmt = transconn.prepareStatement(selectLogs);
                stmt.setInt(1, logId);
                rs = stmt.executeQuery();

                if (rs.next()) {
                    int i = 1;
                    location = rs.getInt(2);
                    catId = rs.getInt(4);
                    Element logsEl = toAppend.addElement("logs");
                    logsEl.addElement("logId").addText(HandlerUtils.nullToEmpty(rs.getString(i++)));
                    logsEl.addElement("locationId").addText(HandlerUtils.nullToEmpty(rs.getString(i++)));
                    logsEl.addElement("date").addText(HandlerUtils.nullToEmpty((rs.getString(i++)).substring(0, 10)));
                    logsEl.addElement("categoryId").addText(HandlerUtils.nullToEmpty(rs.getString(i++)));
                    logsEl.addElement("issue").addText(HandlerUtils.nullToEmpty(rs.getString(i++)));
                    logsEl.addElement("resolution").addText(HandlerUtils.nullToEmpty(rs.getString(i++)));
                    logsEl.addElement("status").addText(HandlerUtils.nullToEmpty(rs.getString(i++)));
                    logsEl.addElement("resolutionDate").addText(HandlerUtils.nullToEmpty((rs.getString(i++)).substring(0, 10)));

                    stmt1 = transconn.prepareStatement(selectLocation);
                    stmt1.setInt(1, location);
                    rs1 = stmt1.executeQuery();

                    if (rs1.next()) {
                        logsEl.addElement("locationName").addText(HandlerUtils.nullToEmpty(rs1.getString(1)));
                    }

                    stmt1 = transconn.prepareStatement(selectLogCategory);
                    stmt1.setInt(1, catId);
                    rs1 = stmt1.executeQuery();

                    if (rs1.next()) {
                        logsEl.addElement("category").addText(HandlerUtils.nullToEmpty(rs1.getString(1)));
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

    /**
     * The folowing code is to update or insert the shift hour information for each location - SR
     */
    private void datedLocationLogs(Element toHandle, Element toAppend) throws HandlerException {

        int locationId = -1;
        int customerId = -1;
        int logId = -1;

        customerId = HandlerUtils.getOptionalInteger(toHandle, "customerId");
        String startDate = HandlerUtils.getOptionalString(toHandle, "startDate");
        String endDate = HandlerUtils.getOptionalString(toHandle, "endDate");

        String selectCustomerLogs = " SELECT id, location, date, category, status, resolutionDate FROM techLogs t WHERE t.customer=? and t.date>? and t.date<?";

        String selectLocation = " SELECT name FROM location WHERE id=?";

        String selectLogCategory = " SELECT name FROM logCategory WHERE id=?";

        int location = -1;
        int catId = -1;
        PreparedStatement stmt = null;
        ResultSet rs = null;

        PreparedStatement stmt1 = null;
        ResultSet rs1 = null;

        try {
            if (customerId > 0) {

                stmt = transconn.prepareStatement(selectCustomerLogs);
                stmt.setInt(1, customerId);
                stmt.setString(2, startDate);
                stmt.setString(3, endDate);
                rs = stmt.executeQuery();

                while (rs.next()) {
                    int i = 1;
                    location = rs.getInt(2);
                    catId = rs.getInt(4);
                    Element logsEl = toAppend.addElement("logs");
                    logsEl.addElement("logId").addText(HandlerUtils.nullToEmpty(rs.getString(i++)));
                    logsEl.addElement("locationId").addText(HandlerUtils.nullToEmpty(rs.getString(i++)));
                    logsEl.addElement("date").addText(HandlerUtils.nullToEmpty((rs.getString(i++)).substring(0, 10)));
                    logsEl.addElement("categoryId").addText(HandlerUtils.nullToEmpty(rs.getString(i++)));
                    logsEl.addElement("status").addText(HandlerUtils.nullToEmpty(rs.getString(i++)));
                    logsEl.addElement("resolutionDate").addText(HandlerUtils.nullToEmpty((rs.getString(i++)).substring(0, 10)));

                    stmt1 = transconn.prepareStatement(selectLocation);
                    stmt1.setInt(1, location);
                    rs1 = stmt1.executeQuery();

                    if (rs1.next()) {
                        logsEl.addElement("locationName").addText(HandlerUtils.nullToEmpty(rs1.getString(1)));
                    }
                    stmt1 = transconn.prepareStatement(selectLogCategory);
                    stmt1.setInt(1, catId);
                    rs1 = stmt1.executeQuery();

                    if (rs1.next()) {
                        logsEl.addElement("category").addText(HandlerUtils.nullToEmpty(rs1.getString(1)));
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

    /**
     * The folowing code is to update or insert the shift hour information for each location - SR
     */
    private void addLocationLogs(Element toHandle, Element toAppend) throws HandlerException {

        int customerId = HandlerUtils.getRequiredInteger(toHandle, "customerId");
        int locationId = HandlerUtils.getRequiredInteger(toHandle, "locationId");
        int userId = HandlerUtils.getRequiredInteger(toHandle, "userId");
        String date = HandlerUtils.getRequiredString(toHandle, "date");
        String category = HandlerUtils.getRequiredString(toHandle, "category");
        String issue = HandlerUtils.getOptionalString(toHandle, "issue");
        String resolution = HandlerUtils.getOptionalString(toHandle, "resolution");
        String status = HandlerUtils.getRequiredString(toHandle, "status");
        String resolutionDate = HandlerUtils.getOptionalString(toHandle, "resolutionDate");
        int callerId = getCallerId(toHandle);

        String insertLogs = " INSERT INTO techLogs (customer, location, user, date, category, issue, status, resolution, resolutionDate) VALUES (?,?,?,?,?,?,?,?,?)";

        PreparedStatement stmt = null;
        ResultSet rs = null;

        try {
            if (issue.length() < 1) {
                addErrorDetail(toAppend, "Cannot log problem without describing the issue");
            } else {
                stmt = transconn.prepareStatement(insertLogs);
                stmt.setInt(1, customerId);
                stmt.setInt(2, locationId);
                stmt.setInt(3, userId);
                stmt.setString(4, date);
                stmt.setString(5, category);
                stmt.setString(6, issue);
                stmt.setString(7, status);
                if (resolution.length() > 0 && resolutionDate.length() > 0) {

                    stmt.setString(8, resolution);
                    stmt.setString(9, resolutionDate);

                } else if (resolution.length() > 0) {
                    stmt.setString(8, resolution);
                    stmt.setString(9, "");

                } else if (resolutionDate.length() > 0) {
                    stmt.setString(8, "");
                    stmt.setString(9, resolutionDate);
                } else {
                    stmt.setString(8, "");
                    stmt.setString(9, "");

                }
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

    /**
     * The folowing code is to update or insert the shift hour information for each location - SR
     */
    private void addTextAlertLogs(Element toHandle, Element toAppend) throws HandlerException {

        int customerId = HandlerUtils.getRequiredInteger(toHandle, "customerId");
        int locationId = HandlerUtils.getRequiredInteger(toHandle, "locationId");
        int userId = HandlerUtils.getRequiredInteger(toHandle, "userId");
        String mobile = HandlerUtils.getRequiredString(toHandle, "mobile");
        String carrier = HandlerUtils.getOptionalString(toHandle, "carrier");
        String messageType = HandlerUtils.getOptionalString(toHandle, "messageType");
        String message = HandlerUtils.getOptionalString(toHandle, "message");
        long epochSec = HandlerUtils.getOptionalLong(toHandle, "dateTime");
        Timestamp tstamp = new Timestamp(epochSec * 1000L);

        String addTextAlertLogs = "INSERT INTO textAlertLogs (customer, location, user, mobile, carrier, messageType, dateTime) VALUES " +
                "(?,?,?,?,?,?,?)";

        PreparedStatement stmt = null;
        ResultSet rs = null;

        try {
            logger.debug("Text Alert sent for user: " + String.valueOf(userId) + " with message: " + message);

            stmt = transconn.prepareStatement(addTextAlertLogs);
            stmt.setInt(1, customerId);
            stmt.setInt(2, locationId);
            stmt.setInt(3, userId);
            stmt.setString(4, mobile);
            stmt.setString(5, carrier);
            stmt.setString(6, message);
            stmt.setString(7, tstamp.toString());
            stmt.executeUpdate();

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

    //NischaySharma_10-Feb-2009_Start: Added new handler to get the customer points
    private void getCustomerPoints(Element toHandle, Element toAppend) throws HandlerException {

        int callerId = getCallerId(toHandle);
        String select = "";

        PreparedStatement stmt = null;
        ResultSet rs = null;

        logger.portalAction("getCustomerPoints");

        try {
            int customerId = HandlerUtils.getRequiredInteger(toHandle, "customerId");

            if (customerId > 0) {
                select = "select ifnull(latitude,0) as centerLat, ifnull(longitude,0) as centerLon, ifnull(zoomLevel,0) as zoomLevel from customer where id = ?;";
                stmt = transconn.prepareStatement(select);
                stmt.setInt(1, customerId);
                rs = stmt.executeQuery();
                while (rs.next()) {
                    Element p = toAppend.addElement("center");
                    p.addElement("centerLat").addText(HandlerUtils.nullToEmpty(rs.getString(1)));
                    p.addElement("centerLon").addText(HandlerUtils.nullToEmpty(rs.getString(2)));
                    p.addElement("zoomLevel").addText(HandlerUtils.nullToEmpty(rs.getString(3)));
                }

                select = "select ifnull(latitude,0) as latitude, ifnull(longitude,0) as longitude from customerPoint WHERE customer = ? ORDER BY sequence;";
                stmt = transconn.prepareStatement(select);
                stmt.setInt(1, customerId);
                rs = stmt.executeQuery();
                while (rs.next()) {
                    Element p = toAppend.addElement("customerPoints");
                    p.addElement("latitude").addText(HandlerUtils.nullToEmpty(rs.getString(1)));
                    p.addElement("longitude").addText(HandlerUtils.nullToEmpty(rs.getString(2)));
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
    //NischaySharma_10-Feb-2009_End

    //NischaySharma_11-Feb-2009_Start: Added new handler
    ///This method uses the customerid, startdate and enddate to calculate the variance of all the locations
    private void getCustomerVariance(Element toHandle, Element toAppend) throws HandlerException {

        int customerId = HandlerUtils.getRequiredInteger(toHandle, "customerId");
        String startDate = HandlerUtils.getRequiredTimestamp(toHandle, "startDate").toString();
        String endDate = HandlerUtils.getRequiredTimestamp(toHandle, "endDate").toString();


        PreparedStatement stmt = null;
        PreparedStatement pouredStmt = null;
        PreparedStatement soldStmt = null;
        ResultSet rs = null;
        ResultSet pouredRS = null;
        ResultSet soldRS = null;

        if (customerId == 0) {
            throw new HandlerException("Customer id cannot be 0.");
        }

        try {
            String select =
                    " SELECT l.id, l.name" +
                    " FROM customer c LEFT JOIN location l ON c.id=l.customer where c.id = ? ";
            stmt = transconn.prepareStatement(select);
            stmt.setInt(1, customerId);
            rs = stmt.executeQuery();
            double pouredValue = 0.0;
            double soldValue = 0.0;
            double variance = 0.0;
            while (rs.next()) {
                int locationId = rs.getInt(1);
                String pouredSelect = " SELECT value FROM (SELECT location, SUM(value) as 'VALUE' FROM pouredSummary ps LEFT JOIN product p on p.id = ps.product" +
                        " WHERE ps.date BETWEEN '" + startDate + "' and '" + endDate + "'" +
                        " and p.pType = 1 group by ps.location) as ps where ps.location = ?;";

                String soldSelect = " select value from (SELECT location, SUM(value) as 'VALUE' FROM soldSummary ss left join product p on p.id = ss.product" +
                        " where ss.date between '" + startDate + "' and '" + endDate + "'" +
                        " and p.pType = 1 group by ss.location) as ss where ss.location = ?;";

                pouredStmt = transconn.prepareStatement(pouredSelect);
                pouredStmt.setInt(1, locationId);
                pouredRS = pouredStmt.executeQuery();

                soldStmt = transconn.prepareStatement(soldSelect);
                soldStmt.setInt(1, locationId);
                soldRS = soldStmt.executeQuery();

                if (pouredRS.first()) {
                    pouredValue = pouredRS.getDouble(1);
                }
                if (soldRS.first()) {
                    soldValue = soldRS.getDouble(1);
                }

                if (pouredValue != 0.0) {
                    variance = ((soldValue - pouredValue) / pouredValue) * 100;
                }

                Element p = toAppend.addElement("location");
                p.addElement("locationId").addText(Integer.toString(locationId));
                p.addElement("variance").addText(Double.toString(variance));
                p.addElement("pouredOunces").addText(Double.toString((pouredValue)));

                soldValue = 0.0;
                pouredValue = 0.0;
                variance = 0.0;

            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(rs);
            close(stmt);
        }
    }
    //NischaySharma_11-Feb-2009_End

    //NischaySharma_11-Feb-2009_Start: added new handler GetUserLocations
    private void getUserLocations(Element toHandle, Element toAppend) throws HandlerException {

        int userId = HandlerUtils.getRequiredInteger(toHandle, "userId");

        PreparedStatement stmt = null;
        ResultSet rs = null;

        if (userId == 0) {
            throw new HandlerException("User id cannot be 0.");
        }

        try {
            String select =
                    " select ifnull(latitude,0) as centerLat, ifnull(longitude,0) as centerLon, ifnull(zoomLevel,0) as zoomLevel from location" +
                    " where id in (select location from user where id = ?); ";
            stmt = transconn.prepareStatement(select);
            stmt.setInt(1, userId);
            rs = stmt.executeQuery();
            while (rs.next()) {
                Element p = toAppend.addElement("location");
                p.addElement("centerLat").addText(HandlerUtils.nullToEmpty(rs.getString(1)));
                p.addElement("centerLon").addText(HandlerUtils.nullToEmpty(rs.getString(2)));
                p.addElement("zoomLevel").addText(HandlerUtils.nullToEmpty(rs.getString(3)));
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(rs);
            close(stmt);
        }
    }
    //NischaySharma_11-Feb-2009_End

    //NischaySharma_13-Feb-2009_Start: added new handler GetUserInfo
    private void getUserInfo(Element toHandle, Element toAppend) throws HandlerException {

        int customerId = HandlerUtils.getRequiredInteger(toHandle, "customerId");
        int locationId = HandlerUtils.getRequiredInteger(toHandle, "locationId");

        PreparedStatement stmt = null;
        ResultSet rs = null;

        if (customerId == 0 || locationId == 0) {
            throw new HandlerException("Customer id  and location id cannot be 0.");
        }

        try {
            String select =
                    " SELECT u.id, securityLevel FROM user u" +
                    " LEFT JOIN userMap umap ON u.id = umap.user" +
                    " WHERE customer = ? AND umap.location = ?";
            stmt = transconn.prepareStatement(select);
            stmt.setInt(1, customerId);
            stmt.setInt(2, locationId);
            rs = stmt.executeQuery();
            if (rs.first()) {
                toAppend.addElement("userMstrId").addText(HandlerUtils.nullToEmpty(rs.getString(1)));
                toAppend.addElement("securityLevel").addText(HandlerUtils.nullToEmpty(rs.getString(1)));
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(rs);
            close(stmt);
        }
    }
    //NischaySharma_13-Feb-2009_End

    //NischaySharma_16-Jun-2009_Start:  Adds and updates the coolers
    private void updateCoolers(Element toHandle, Element toAppend) throws HandlerException {

        Iterator coolers = toHandle.elementIterator("cooler");

        PreparedStatement stmt = null;
        ResultSet rs = null;
        String insertStmt = "INSERT INTO cooler (name, location, zone, system, line, alertPoint) VALUES (?,?,?,?,?,?)";
        String updateStmt = "UPDATE cooler SET name = ?, location = ?, zone = ?, system = ?, line = ?, alertPoint = ? WHERE id = ?";

        try {
            while (coolers.hasNext()) {
                Element cooler = (Element) coolers.next();
                int coolerId = HandlerUtils.getOptionalInteger(cooler, "coolerId");
                String name = HandlerUtils.getRequiredString(cooler, "name");
                int locationId = HandlerUtils.getRequiredInteger(cooler, "locationId");
                int zoneId = HandlerUtils.getOptionalInteger(cooler, "zoneId");
                int system = HandlerUtils.getRequiredInteger(cooler, "system");
                int line = HandlerUtils.getOptionalInteger(cooler, "line");
                int alertPoint = HandlerUtils.getOptionalInteger(cooler, "alertPoint");

                if (coolerId > 0) {
                    if (!checkForeignKey("cooler", "id", coolerId)) {
                        throw new HandlerException("Foreign Key Not found : cooler " + coolerId);
                    }
                    stmt = transconn.prepareStatement(updateStmt);
                    stmt.setString(1, name);
                    stmt.setInt(2, locationId);
                    stmt.setInt(3, zoneId);
                    stmt.setInt(4, system);
                    stmt.setInt(5, line);
                    stmt.setInt(6, alertPoint);
                    stmt.setInt(7, coolerId);
                    stmt.executeUpdate();
                } else {
                    stmt = transconn.prepareStatement(insertStmt);
                    stmt.setString(1, name);
                    stmt.setInt(2, locationId);
                    stmt.setInt(3, zoneId);
                    stmt.setInt(4, system);
                    stmt.setInt(5, line);
                    stmt.setInt(6, alertPoint);
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
    //NischaySharma_17-Jun-2009_End

    //NischaySharma_17-Jun-2009_Start: Method to add keg lines
    private void addKegLines(Element toHandle, Element toAppend) throws HandlerException {

        Iterator kegs = toHandle.elementIterator("keg");

        PreparedStatement stmt = null;
        ResultSet rs = null;
        String insertStmt = "INSERT INTO kegLine (name, cooler) VALUES (?, ?)";

        try {
            while (kegs.hasNext()) {
                Element keg = (Element) kegs.next();
                String kegLineName = HandlerUtils.getRequiredString(keg, "kegLineName");
                int coolerId = HandlerUtils.getRequiredInteger(keg, "coolerId");
                stmt = transconn.prepareStatement(insertStmt);
                stmt.setString(1, kegLineName);
                stmt.setInt(2, coolerId);
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
    //NischaySharma_17-Jun-2009_End

    //NischaySharma_25-Jun-2009_Start:  Added 2 new handlers and 1 method to help create the xml.
    private void getConcessionProductMap(Element toHandle, Element toAppend) throws HandlerException {
        int locationId = HandlerUtils.getOptionalInteger(toHandle, "locationId");
        int supplierId = HandlerUtils.getOptionalInteger(toHandle, "supplierId");
        int productId = HandlerUtils.getOptionalInteger(toHandle, "productId");

        String selectStmt = " SELECT c.id, p.name, c.product, l.name, c.location, s.name, c.supplier, p.category, p.segment FROM concessionProductMap c " +
                " LEFT JOIN product p on c.product = p.id " +
                " LEFT JOIN location l on c.location = l.id " +
                " LEFT JOIN supplier s on c.supplier = s.id " +
                " WHERE ";
        int paramsCount = 0;
        if (locationId >= 0) {
            paramsCount++;
        }
        if (supplierId >= 0) {
            paramsCount++;
        }
        if (productId >= 0) {
            paramsCount++;
        }
        if (paramsCount > 1) {
            throw new HandlerException("Exactly one of the following must " +
                    "be set: locationId supplierId productId");
        }
        PreparedStatement stmt = null;
        ResultSet productMap = null;
        try {
            if (locationId > 0) {
                selectStmt += " location = ?";
                stmt = transconn.prepareStatement(selectStmt);
                stmt.setInt(1, locationId);
                productMap = stmt.executeQuery();
                getConcessionProductMapXML(toAppend, productMap);
            } else if (supplierId > 0) {
                selectStmt += " supplier = ?";
                stmt = transconn.prepareStatement(selectStmt);
                stmt.setInt(1, supplierId);
                productMap = stmt.executeQuery();
                getConcessionProductMapXML(toAppend, productMap);
            } else if (productId > 0) {
                selectStmt += " product = ?";
                stmt = transconn.prepareStatement(selectStmt);
                stmt.setInt(1, supplierId);
                productMap = stmt.executeQuery();
                getConcessionProductMapXML(toAppend, productMap);

            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
            close(productMap);
        }
    }

    private void getConcessionProductMapXML(Element toAppend, ResultSet productMap) throws SQLException {
        while (productMap.next()) {
            Element product = toAppend.addElement("product");
            product.addElement("id").addText(HandlerUtils.nullToEmpty(productMap.getString(1)));
            product.addElement("product").addText(HandlerUtils.nullToEmpty(productMap.getString(2)));
            product.addElement("productId").addText(String.valueOf(productMap.getInt(3)));
            product.addElement("location").addText(HandlerUtils.nullToEmpty(productMap.getString(4)));
            product.addElement("locationId").addText(String.valueOf(productMap.getInt(5)));
            product.addElement("supplier").addText(HandlerUtils.nullToEmpty(productMap.getString(6)));
            product.addElement("supplierId").addText(String.valueOf(productMap.getInt(7)));
            product.addElement("category").addText(String.valueOf(productMap.getInt(8)));
            product.addElement("segment").addText(String.valueOf(productMap.getInt(9)));
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

    private void addConcessionProductSupplier(Element toHandle, Element toAppend) throws HandlerException {
        Iterator products = toHandle.elementIterator("product");
        PreparedStatement stmt = null;
        ResultSet rs = null;
        String updateStmt = "INSERT INTO concessionProductMap (product, location, supplier) VALUES(?,?,?)";
        try {
            while (products.hasNext()) {
                Element product = (Element) products.next();
                int productId = HandlerUtils.getRequiredInteger(product, "productId");
                int supplierId = HandlerUtils.getRequiredInteger(product, "supplierId");
                int locationId = HandlerUtils.getRequiredInteger(product, "locationId");
                stmt = transconn.prepareStatement(updateStmt);
                stmt.setInt(1, productId);
                stmt.setInt(2, locationId);
                stmt.setInt(3, supplierId);
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
    //NischaySharma_25-Jun-2009_End

    //NischaySharma_23-Jul-2009_Start
    private void getEmailReportMaster(Element toHandle, Element toAppend) throws HandlerException {
        ResultSet rs = null;
        PreparedStatement stmt = null;
        //NischaySharma_31-Jul-2009_Start: Changed query to fetch the email report duration also
        String selectStmt = "SELECT id, name, description, timing, emailReportDurations FROM reportMaster";
        //NischaySharma_31-Jul-2009_End
        try {
            stmt = transconn.prepareStatement(selectStmt);
            rs = stmt.executeQuery();
            while (rs.next()) {
                Element report = toAppend.addElement("ReportInfo");
                report.addElement("name").addText(HandlerUtils.nullToEmpty(rs.getString(2)));
                report.addElement("report").addText(String.valueOf(rs.getInt(1)));
                report.addElement("description").addText(HandlerUtils.nullToEmpty(rs.getString(3)));
                report.addElement("timing").addText(String.valueOf(rs.getTimestamp(4)));
                //NischaySharma_31-Jul-2009_Start: Added email report duration
                report.addElement("emailReportDurations").addText(HandlerUtils.nullToEmpty(rs.getString(5)));
                //NischaySharma_31-Jul-2009_End
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(rs);
            close(stmt);
        }
    }

    private void getEmailReportComponents(Element toHandle, Element toAppend) throws HandlerException {
        ResultSet rs = null;
        PreparedStatement stmt = null;
        String selectStmt = "SELECT id, name, description, headerWidths FROM reportComponents";
        try {
            stmt = transconn.prepareStatement(selectStmt);
            rs = stmt.executeQuery();
            while (rs.next()) {
                Element report = toAppend.addElement("ReportComponentInfo");
                report.addElement("name").addText(HandlerUtils.nullToEmpty(rs.getString(2)));
                report.addElement("reportComponent").addText(String.valueOf(rs.getInt(1)));
                report.addElement("description").addText(HandlerUtils.nullToEmpty(rs.getString(3)));
                report.addElement("headerWidths").addText(HandlerUtils.nullToEmpty(rs.getString(4)));
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(rs);
            close(stmt);
        }
    }

    private void getEmailReportComponentsMap(Element toHandle, Element toAppend) throws HandlerException {
        ResultSet rs = null;
        PreparedStatement stmt = null;
        String selectStmt = " SELECT rm.id, rm.name, rc.id, rc.name FROM reportComponentsMap rcm " +
                " LEFT JOIN reportMaster rm on rm.id = rcm.reportMaster " +
                " LEFT JOIN reportComponents rc on rc.id = rcm.reportComponent " +
                " ORDER BY rm.id, rcm.id ";
        try {
            stmt = transconn.prepareStatement(selectStmt);
            rs = stmt.executeQuery();
            int previousReport = -1;
            boolean isLastExecuted = false;
            while (rs.next()) {
                if (previousReport < 0) {
                    previousReport = rs.getInt(1);
                    Element report = toAppend.addElement("ReportInfo");
                    report.addElement("ReportName").addText(HandlerUtils.nullToEmpty(rs.getString(2)));
                    report.addElement("ReportID").addText(String.valueOf(rs.getInt(1)));
                    do {
                        Element components = report.addElement("Component");
                        components.addElement("ComponentName").addText(HandlerUtils.nullToEmpty(rs.getString(4)));
                        components.addElement("ComponentID").addText(HandlerUtils.nullToEmpty(rs.getString(3)));
                        if (rs.isLast()) {
                            previousReport = -1;
                            isLastExecuted = true;
                        } else {
                            rs.next();
                        }
                    } while (previousReport == rs.getInt(1));
                    if (isLastExecuted) {
                        break;
                    }
                    previousReport = -1;
                    rs.previous();
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
    //SundarRavindran_28-Jul-2009_Start

    private void getEmailTimeTable(Element toHandle, Element toAppend) throws HandlerException {
        ResultSet rs = null;
        PreparedStatement stmt = null;
        String selectStmt = " SELECT e.id, e.report, e.user, e.time, e.day FROM emailTimeTable e ";
        try {
            stmt = transconn.prepareStatement(selectStmt);
            rs = stmt.executeQuery();
            while (rs.next()) {
                Element timeInfo = toAppend.addElement("timeInfo");
                timeInfo.addElement("id").addText(String.valueOf(rs.getInt(1)));
                timeInfo.addElement("report").addText(String.valueOf(rs.getInt(2)));
                timeInfo.addElement("user").addText(String.valueOf(rs.getInt(3)));
                timeInfo.addElement("time").addText(String.valueOf(rs.getTimestamp(4)));
                timeInfo.addElement("day").addText(String.valueOf(rs.getInt(5)));
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(rs);
            close(stmt);
        }
    }
    //SundarRavindran_28-Jul-2009_End
    //NischaySharma_23-Jul-2009_End

    //NischaySharma_31-Jul-2009_Start
    private void getEmailReportDurations(Element toHandle, Element toAppend) throws HandlerException {
        ResultSet rs = null;
        PreparedStatement stmt = null;
        String selectStmt = " SELECT e.id, e.name, e.identifier FROM emailReportDurations e ";
        try {
            stmt = transconn.prepareStatement(selectStmt);
            rs = stmt.executeQuery();
            while (rs.next()) {
                Element emailReportDuration = toAppend.addElement("emailReportDuration");
                emailReportDuration.addElement("id").addText(String.valueOf(rs.getInt(1)));
                emailReportDuration.addElement("name").addText(HandlerUtils.nullToEmpty(rs.getString(2)));
                emailReportDuration.addElement("identifier").addText(HandlerUtils.nullToEmpty(rs.getString(3)));
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(rs);
            close(stmt);
        }
    }

    private void addEmailReportLog(Element toHandle, Element toAppend) throws HandlerException {
        //NischaySharma_26-May-2010_Start
        int emailReportLogType = HandlerUtils.getRequiredInteger(toHandle, "emailReportLogType");
        boolean isProblem = HandlerUtils.getRequiredBoolean(toHandle, "isProblem");
        int emailReport = HandlerUtils.getRequiredInteger(toHandle, "emailReport");
        String timeOfGeneration = HandlerUtils.getOptionalString(toHandle, "timeOfGeneration");
        PreparedStatement stmt = null;
        boolean isTimeOfGeneration = false;
        String insertStmt = "";
        if(null != timeOfGeneration && !timeOfGeneration.isEmpty())
        {
            insertStmt = " INSERT INTO emailReportLogs (emailReportLogType, isProblem, emailReport, timeOfGeneration) VALUES (?,?,?,?) ";
            isTimeOfGeneration = true;
        }
        else
        {
            insertStmt = " INSERT INTO emailReportLogs (emailReportLogType, isProblem, emailReport) VALUES (?,?,?) ";
        }
        
        try {
            stmt = transconn.prepareStatement(insertStmt);
            stmt.setInt(1, emailReportLogType);
            int bit = isProblem == true ? 1 : 0;
            stmt.setInt(2, bit);
            stmt.setInt(3, emailReport);
            if(isTimeOfGeneration)
            {
                stmt.setString(4, timeOfGeneration);
            }
            //stmt.setString(3, log);
            stmt.executeUpdate();
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
        }
        //NischaySharma_26-May-2010_End
    }

    private void addEmailReportArchive(Element toHandle, Element toAppend) throws HandlerException {
        //NischaySharma_26-May-2010_Start
        int user = HandlerUtils.getRequiredInteger(toHandle, "user");
        String fileId = HandlerUtils.getRequiredString(toHandle, "fileId");
        int reportMaster = HandlerUtils.getRequiredInteger(toHandle, "reportMaster");
        //String fileName = HandlerUtils.getRequiredString(toHandle, "fileName");
        PreparedStatement stmt = null;
        String insertStmt = " INSERT INTO emailReportArchives (user, fileId, reportMaster) VALUES (?, ?, ?) ";
        try {
            stmt = transconn.prepareStatement(insertStmt);
            stmt.setInt(1, user);
            stmt.setString(2, fileId);
            stmt.setInt(3, reportMaster);
            stmt.executeUpdate();
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
        }
        //NischaySharma_26-May-2010_End
    }

    private void getEmailReportLogtype(Element toHandle, Element toAppend) throws HandlerException {
        PreparedStatement stmt = null;
        ResultSet rs = null;
        String selectStmt = " SELECT e.id, e.name FROM emailReportLogtype e ";
        try {
            stmt = transconn.prepareStatement(selectStmt);
            rs = stmt.executeQuery();
            while (rs.next()) {
                Element emailReportLogtype = toAppend.addElement("emailReportLogtype");
                emailReportLogtype.addElement("id").addText(String.valueOf(rs.getInt(1)));
                emailReportLogtype.addElement("name").addText(HandlerUtils.nullToEmpty(rs.getString(2)));
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(rs);
            close(stmt);
        }
    }
    //NischaySharma_31-Jul-2009_End

    //NischaySharma_14-Apr-2010_Start
    private void addUpdateDeleteProductGrouping(Element toHandle, Element toAppend) throws HandlerException {

        int groupingId = HandlerUtils.getOptionalInteger(toHandle, "groupingId");
        String productGroupName = HandlerUtils.getOptionalString(toHandle, "productGroupName");
        int segment = HandlerUtils.getOptionalInteger(toHandle, "segment");

        if(segment < 0){segment = 1;}

        Iterator add = toHandle.elementIterator("add");
        Iterator del = toHandle.elementIterator("delete");
        int callerId = getCallerId(toHandle);

        String selectGrouping = " SELECT id FROM grouping WHERE id = ? ";
        String updateGrouping = " UPDATE grouping SET name = ?, segment = ? WHERE id = ? ";
        String insertGrouping = " INSERT INTO grouping (name, segment) VALUES(?,?) ";

        String selectProductGrouping = " SELECT id FROM productGrouping WHERE product = ? AND grouping = ? ";
        String selectProductGroupingId = " SELECT id FROM productGrouping WHERE id = ?";
        String insertProductGrouping = " INSERT INTO productGrouping (grouping, product) VALUES (?,?) ";
        String deleteProductGrouping = " DELETE FROM productGrouping WHERE id = ? ";
        String countProductGrouping = " SELECT id FROM productGrouping WHERE grouping = ? ";
        String deleteGrouping = " DELETE FROM grouping WHERE id = ? ";

        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            stmt = transconn.prepareStatement(selectGrouping);
            stmt.setInt(1, groupingId);
            rs = stmt.executeQuery();
            if (rs.next()) {
                stmt = transconn.prepareStatement(updateGrouping);
                stmt.setString(1, productGroupName);
                stmt.setInt(2, segment);
                stmt.setInt(3, groupingId);
                stmt.executeUpdate();

                boolean isDeleted = false;
                // Deleting Product Grouping
                while (del.hasNext()) {
                    Element proGrping = (Element) del.next();
                    int productGrpIngId = HandlerUtils.getRequiredInteger(proGrping, "productGroupingId");

                    stmt = transconn.prepareStatement(selectProductGroupingId);
                    stmt.setInt(1, productGrpIngId);
                    rs = stmt.executeQuery();
                    if (rs.next()) {
                        stmt = transconn.prepareStatement(deleteProductGrouping);
                        stmt.setInt(1, productGrpIngId);
                        stmt.executeUpdate();
                        isDeleted = true;
                    } else {
                        addErrorDetail(toAppend, "Product Grouping does not exist: " + productGrpIngId);
                    }
                }

                if(isDeleted)
                {
                    stmt = transconn.prepareStatement(countProductGrouping);
                    stmt.setInt(1, groupingId);
                    rs = stmt.executeQuery();
                    if(!rs.next())
                    {
                        stmt = transconn.prepareStatement(deleteGrouping);
                        stmt.setInt(1, groupingId);
                        stmt.executeUpdate();
                    }
                }
            }
            else {
                stmt = transconn.prepareStatement(insertGrouping);
                stmt.setString(1, productGroupName);
                stmt.setInt(2, segment);
                stmt.executeUpdate();
                String getLastId = " SELECT id From grouping WHERE name = ? ";
                stmt = transconn.prepareStatement(getLastId);
                stmt.setString(1, productGroupName);
                rs = stmt.executeQuery();
                if (rs.next()) {
                    groupingId = rs.getInt(1);
                } else {
                    logger.dbError("DB Error, mysql function last_insert_id failed");
                    throw new HandlerException("Database Error");
                }
            }

            toAppend.addElement("groupingId").addText(String.valueOf(groupingId));

         // Adding Product Grouping
            while (add.hasNext()) {
                Element product = (Element) add.next();
                int productId = HandlerUtils.getRequiredInteger(product, "productId");

                stmt = transconn.prepareStatement(selectProductGrouping);
                stmt.setInt(1, productId);
                stmt.setInt(2, groupingId);
                rs = stmt.executeQuery();
                if (!rs.next()) {
                    stmt = transconn.prepareStatement(insertProductGrouping);
                    stmt.setInt(1, groupingId);
                    stmt.setInt(2, productId);
                    stmt.executeUpdate();
                } else {
                    addErrorDetail(toAppend, "Product Grouping with specified product: " + productId + " already exists");
                }
            }


        } catch (SQLException sqle) {
            logger.dbError("Database error in updateProductSet: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
            close(rs);
        }
    }
    //NischaySharma_14-Apr-2010

    //NischaySharma_15-Apr-2010
    private void getGrouping(Element toHandle, Element toAppend) throws HandlerException {

        String select = " SELECT id, name, segment FROM grouping ";

        PreparedStatement stmt = null;
        ResultSet rs = null;

        try {
            stmt = transconn.prepareStatement(select);
            rs = stmt.executeQuery();
            while (rs.next()) {
                Element grouping = toAppend.addElement("grouping");
                grouping.addElement("id").addText(String.valueOf(rs.getInt(1)));
                grouping.addElement("name").addText(HandlerUtils.nullToEmpty(rs.getString(2)));
                grouping.addElement("segment").addText(String.valueOf(rs.getInt(3)));
            }

        } catch (SQLException sqle) {
            logger.dbError("Database error in getProductSet: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(rs);
            close(stmt);
        }

    }

    private void getProductGrouping(Element toHandle, Element toAppend) throws HandlerException {

        String select = " SELECT pg.id, pg.grouping, pg.product, p.name FROM productGrouping pg LEFT JOIN product p "
                        + " ON  pg.product = p.id WHERE pg.grouping = ? ";

        int groupingId = HandlerUtils.getRequiredInteger(toHandle, "groupingId");

        PreparedStatement stmt = null;
        ResultSet rs = null;

        try {
            stmt = transconn.prepareStatement(select);
            stmt.setInt(1, groupingId);
            rs = stmt.executeQuery();
            while (rs.next()) {
                Element prdGrp = toAppend.addElement("productGrouping");
                prdGrp.addElement("id").addText(String.valueOf(rs.getInt(1)));
                prdGrp.addElement("groupingId").addText(HandlerUtils.nullToEmpty(rs.getString(2)));
                prdGrp.addElement("productId").addText(HandlerUtils.nullToEmpty(rs.getString(3)));
                prdGrp.addElement("productName").addText(HandlerUtils.nullToEmpty(rs.getString(4)));
            }

        } catch (SQLException sqle) {
            logger.dbError("Database error in getProductSet: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(rs);
            close(stmt);
        }

    }

    private void getGroupingForGroup(Element toHandle, Element toAppend) throws HandlerException {

        String select = " SELECT gPG.id, g.id, g.name, g.segment FROM groupProductGroupingMap gPG LEFT JOIN grouping g ON g.id = gPG.grouping WHERE gPG.groups = ? ";
        String selectProductDetails = " SELECT pg.product, p.name FROM productGrouping pg LEFT JOIN product p ON pg.product = p.id WHERE pg.grouping = ? ";

        int groupId = HandlerUtils.getRequiredInteger(toHandle, "groupId");
        boolean getDetails = HandlerUtils.getOptionalBoolean(toHandle, "getDetails");
        PreparedStatement stmt = null;
        ResultSet rs = null, innerRS = null;

        try {
            stmt = transconn.prepareStatement(select);
            stmt.setInt(1, groupId);
            rs = stmt.executeQuery();
            while (rs.next()) {
                Element prdGrp = toAppend.addElement("groupPG");
                prdGrp.addAttribute("id", String.valueOf(rs.getInt(1)));
                prdGrp.addAttribute("groupingId", String.valueOf(rs.getInt(2)));
                prdGrp.addAttribute("name", HandlerUtils.nullToEmpty(rs.getString(3)));
                prdGrp.addAttribute("segment", String.valueOf(rs.getInt(4)));

                if(getDetails) {
                    stmt = transconn.prepareStatement(selectProductDetails);
                    stmt.setInt(1, rs.getInt(2));
                    innerRS = stmt.executeQuery();
                    while (innerRS.next()) {
                        Element prdDet = prdGrp.addElement("prodDetails");
                        prdDet.addAttribute("id", String.valueOf(innerRS.getInt(1)));
                        prdDet.addAttribute("name", HandlerUtils.nullToEmpty(innerRS.getString(2)));
                    }
                }
            }

        } catch (SQLException sqle) {
            logger.dbError("Database error in getProductSet: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(innerRS);
            close(rs);
            close(stmt);
        }

    }

    private void addUpdateDeleteGroupingForGroup(Element toHandle, Element toAppend) throws HandlerException {

        String selectGrouping               = " SELECT gPG.id FROM groupProductGroupingMap gPG WHERE gPG.groups = ? AND gPG.grouping = ? ";
        String deleteGroupProductGrouping   = " DELETE FROM groupProductGroupingMap WHERE id = ? ";
        String insertGrouping               = " INSERT INTO groupProductGroupingMap (groups, grouping) VALUES (?,?) ";

        int groupId = HandlerUtils.getRequiredInteger(toHandle, "groupId");
        Iterator add = toHandle.elementIterator("add");
        Iterator del = toHandle.elementIterator("delete");

        PreparedStatement stmt = null;
        ResultSet rs = null;

        try {
            // Deleting group product groupings
            while (del.hasNext()) {
                Element grouping = (Element) del.next();
                int groupingId = HandlerUtils.getRequiredInteger(grouping, "groupingId");

                stmt = transconn.prepareStatement(selectGrouping);
                stmt.setInt(1, groupId);
                stmt.setInt(2, groupingId);
                rs = stmt.executeQuery();
                int count = 0;
                while(rs.next())
                {
                    stmt = transconn.prepareStatement(deleteGroupProductGrouping);
                    stmt.setInt(1, rs.getInt(1));
                    count = stmt.executeUpdate();
                }
                if (count == 0) {
                    addErrorDetail(toAppend, "Grouping does not exist: " + groupingId);
                }
            }

            // Adding group product groupings
            while (add.hasNext()) {
                Element grouping = (Element) add.next();
                int groupingId = HandlerUtils.getRequiredInteger(grouping, "groupingId");

                stmt = transconn.prepareStatement(selectGrouping);
                stmt.setInt(1, groupId);
                stmt.setInt(2, groupingId);
                rs = stmt.executeQuery();
                if (!rs.next()) {
                    stmt = transconn.prepareStatement(insertGrouping);
                    stmt.setInt(1, groupId);
                    stmt.setInt(2, groupingId);
                    stmt.executeUpdate();
                } else {
                    addErrorDetail(toAppend, "Grouping Id: " + groupingId + " already exists");
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
    //NischaySharma_15-Apr-2010

    //NischaySharma_26-May-2010_Start

    private void getEmailReportLogs(Element toHandle, Element toAppend) throws HandlerException {

        String select = " SELECT emailReport, timeOfGeneration FROM emailReportLogs WHERE emailReportLogType = ? AND isProblem = ? AND timeOfGeneration between ? AND  ? ";

        int emailReportLogType = HandlerUtils.getRequiredInteger(toHandle, "emailReportLogType");
        int isProblem = HandlerUtils.getRequiredInteger(toHandle, "isProblem");
        String startDate = HandlerUtils.getRequiredString(toHandle, "startDate");
        String endDate = HandlerUtils.getRequiredString(toHandle, "endDate");

        PreparedStatement stmt = null;
        ResultSet rs = null;

        try {
            stmt = transconn.prepareStatement(select);
            stmt.setInt(1, emailReportLogType);
            stmt.setInt(2, isProblem);
            stmt.setString(3, startDate);
            stmt.setString(4, endDate);
            rs = stmt.executeQuery();
            while (rs.next()) {
                Element log = toAppend.addElement("getEmailReportLogDate");
                log.addAttribute("emailReport", rs.getString(1));
                log.addText(rs.getString(2));
            }

        } catch (SQLException sqle) {
            logger.dbError("Database error in getProductSet: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(rs);
            close(stmt);
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
}

