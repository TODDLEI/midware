/*
 * SQLAddHandler.java
 *
 * Created on August 24, 2005, 12:15 PM
 *
 */
package net.terakeet.soapware.handlers;

/**
 *
 * @author Ryan Garver
 */
import net.terakeet.soapware.Handler;
import net.terakeet.soapware.HandlerException;
import net.terakeet.soapware.HandlerUtils;
import net.terakeet.soapware.SOAPMessage;
import net.terakeet.soapware.handlers.auper.*;
import net.terakeet.soapware.RegisteredConnection;
import net.terakeet.soapware.DatabaseConnectionManager;
import net.terakeet.util.TemplatedMessage;
import net.terakeet.util.MailException;
import net.terakeet.soapware.security.*;
import net.terakeet.util.MidwareLogger;
import net.terakeet.usbn.*;
import org.apache.log4j.Logger;
import net.terakeet.soapware.handlers.report.*;
import net.terakeet.soapware.DateTimeParameter;
import org.dom4j.Element;
import java.sql.Timestamp;
import java.sql.Time;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.CallableStatement;
import java.util.*;
import java.text.*;
import org.apache.commons.lang3.text.WordUtils;
//import net.sourceforge.openforecast.*;
//import net.sourceforge.openforecast.models.DoubleExponentialSmoothingModel;

public class SQLAddHandler implements Handler {

    private LocationMap locationMap;
    private ProductMap productMap;
    private MidwareLogger logger;
    private static final String transConnName = "auper";
    private RegisteredConnection transconn;
    private SecureSession ss;
    private static final HashMap<Integer, Date> temperatureAlertCoolerCache = new HashMap<Integer, Date>();
    private static final HashMap<Integer, Double> highestTemperatureCoolerCache = new HashMap<Integer, Double>();
    private static SimpleDateFormat newDateFormat 
                                            = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private static SimpleDateFormat dateFormat
                                            = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");

    /** Creates a new instance of SQLAddHandler */
    public SQLAddHandler() {
        HandlerUtils.initializeClientKeyManager();
        logger = new MidwareLogger(SQLAddHandler.class.getName());
        transconn = null;
        productMap = null;
        locationMap = null;
    }

    public void handle(Element toHandle, Element toAppend) throws HandlerException {

        String clientKey = HandlerUtils.getOptionalString(toHandle, "clientKey");
        ss = ClientKeyManager.getSession(clientKey);

        String function = toHandle.getName();
        String responseNamespace = (String) SOAPMessage.getURIMap().get("tkmsg");

        logger = new MidwareLogger(SQLAddHandler.class.getName(), function);
        logger.debug("SQLAddHandler processing method: " + function);
        logger.xml("request: " + toHandle.asXML());
        //logger : (function+" by #"+ss.getLocation()+"-"+ss.getClientId());

        transconn = DatabaseConnectionManager.getNewConnection(transConnName,
                function + " (SQLAddHandler)");

        try {
            if ("remoteReading".equals(function)) {
                remoteReading(toHandle, responseFor(function, toAppend), ss);
            } else if ("picoflashPowerup".equals(function)) {
                picoflashPowerup(toHandle, responseFor(function, toAppend), ss);
                // The rest of these methods require a "L0-C1 Admin key
            } else if (ss.getLocation() == 0 && ss.getClientId() == 1 && ss.getSecurityLevel().canAdmin()) {
                if ("addBar".equals(function)) {
                    addBar(toHandle, responseFor(function, toAppend));
                } else if ("addCustomer".equals(function)) {
                    addCustomer(toHandle, responseFor(function, toAppend));
                } else if ("addInventory".equals(function)) {
                    addInventory(toHandle, responseFor(function, toAppend));
                } else if ("addLine".equals(function)) {
                    addLine(toHandle, responseFor(function, toAppend));
                } else if ("addSupplier".equals(function)) {
                    addSupplier(toHandle, responseFor(function, toAppend));
                } else if ("addSupplierAddress".equals(function)) {
                    addSupplierAddress(toHandle, responseFor(function, toAppend));
                } else if ("addLocationSupplier".equals(function)) {
                    addLocationSupplier(toHandle, responseFor(function, toAppend));
                } else if ("addSupplierRequest".equals(function)) {
                    addSupplierRequest(toHandle, responseFor(function, toAppend));
                } else if ("addEmailReports".equals(function)) {
                    addEmailReports(toHandle, responseFor(function, toAppend));
                } else if ("addTextAlerts".equals(function)) {
                    addTextAlerts(toHandle, responseFor(function, toAppend));
                } else if ("addProduct".equals(function)) {
                    addProduct(toHandle, responseFor(function, toAppend));
                } else if ("addProductRequest".equals(function)) {
                    addProductRequest(toHandle, responseFor(function, toAppend));
                } else if ("addRegion".equals(function)) {
                    addRegion(toHandle, responseFor(function, toAppend));
                } else if ("addProductSet".equals(function)) {
                    addProductSet(toHandle, responseFor(function, toAppend));
                } else if ("addSystem".equals(function)) {
                    addSystem(toHandle, responseFor(function, toAppend));
                } else if ("addSummary".equals(function)) {
                    addSummary(toHandle, responseFor(function, toAppend));
                } else if ("addExclusionSummary".equals(function)) {
                    addExclusionSummary(toHandle, responseFor(function, toAppend));
                } else if ("addExclusion".equals(function)) {
                    addExclusion(toHandle, responseFor(function, toAppend));
                } else if ("importBeverages".equals(function)) {
                    importBeverages(toHandle, responseFor(function, toAppend));
                } else if ("addBeverage".equals(function)) {
                    addBeverage(toHandle, responseFor(function, toAppend));
                } else if ("copyBeverage".equals(function)) {
                    copyBeverage(toHandle, responseFor(function, toAppend));
                } else if ("addBeverageSize".equals(function)) {
                    addBeverageSize(toHandle, responseFor(function, toAppend));
                } else if ("addUnitCount".equals(function)) {
                    addUnitCount(toHandle, responseFor(function, toAppend));
                } else if ("addStandUnitCount".equals(function)) {
                    addStandUnitCount(toHandle, responseFor(function, toAppend));
                } else if ("addUser".equals(function)) {
                    addUser(toHandle, responseFor(function, toAppend));
                } else if ("addGroups".equals(function)) {
                    addGroups(toHandle, responseFor(function, toAppend));
                } else if ("addGroupExclusion".equals(function)) {
                    addGroupExclusion(toHandle, responseFor(function, toAppend));
                } else if ("addGroupProductSet".equals(function)) {
                    addGroupProductSet(toHandle, responseFor(function, toAppend));
                } else if ("addRegionProductSet".equals(function)) {
                    addRegionProductSet(toHandle, responseFor(function, toAppend));
                } else if ("addGroupRegion".equals(function)) {
                    addGroupRegion(toHandle, responseFor(function, toAppend));
                } else if ("addUserRegion".equals(function)) {
                    addUserRegion(toHandle, responseFor(function, toAppend));
                } else if ("dbOverload".equals(function)) {
                    dbOverload(toHandle, responseFor(function, toAppend), ss);
                } else if ("manualSalesUpload".equals(function)) {
                    manualSalesUpload(toHandle, responseFor(function, toAppend));
                } else if ("addMiscProduct".equals(function)) {
                    addMiscProduct(toHandle, responseFor(function, toAppend));
                } else if ("addZone".equals(function)) {
                    addZone(toHandle, responseFor(function, toAppend));
                } else if ("addStation".equals(function)) {
                    addStation(toHandle, responseFor(function, toAppend));
                } else if ("addCostCenter".equals(function)) {
                    addCostCenter(toHandle, responseFor(function, toAppend));
                } else if ("addLineSummary".equals(function)) {
                    addLineSummary(toHandle, responseFor(function, toAppend));
                } else if ("addUnClaimedReadingData".equals(function)) {
                    addUnClaimedReadingData(toHandle, responseFor(function, toAppend));
                } else if ("adjustUnclaimedReading".equals(function)) {
                    adjustUnclaimedReading(toHandle, responseFor(function, toAppend));
                } else if ("addHourlySummary".equals(function)) {
                    addHourlySummary(toHandle, responseFor(function, toAppend));
                } else if ("addTierSummary".equals(function)) {
                    addTierSummary(toHandle, responseFor(function, toAppend));
                } else if ("addUserAlerts".equals(function)) {
                    addUserAlerts(toHandle, responseFor(function, toAppend));
                } else if ("addSuperUserAlerts".equals(function)) {
                    addSuperUserAlerts(toHandle, responseFor(function, toAppend));
                } else if ("addSuspensions".equals(function)) {
                    addSuspensions(toHandle, responseFor(function, toAppend));
                } else if ("innodbReadings".equals(function)) {
                    innodbReadings(toHandle, responseFor(function, toAppend));
                } else if ("archiveReadings".equals(function)) {
                    archiveReadings(toHandle, responseFor(function, toAppend));
                } else if ("addBevBox".equals(function)) {
                    addBevBox(toHandle, responseFor(function, toAppend));
                } else if ("addSaveTheBeerLog".equals(function)) {
                    addSaveTheBeerLog(toHandle, responseFor(function, toAppend));
                } else if ("addSaveTheBeerData".equals(function)) {
                    addSaveTheBeerData(toHandle, responseFor(function, toAppend));
                } else if ("addLineCleaning".equals(function)) {
                    addLineCleaning(toHandle, responseFor(function, toAppend));
                } else if ("addDemoData".equals(function)) {
                    addDemoData(toHandle, responseFor(function, toAppend));
                } else if ("addCustomerPeriods".equals(function)) {
                    addCustomerPeriods(toHandle, responseFor(function, toAppend));
                } else if ("addErrorLogs".equals(function)) {
                    addErrorLogs(toHandle, responseFor(function, toAppend));
                } else if ("addUpdateBBTVFonts".equals(function)) {
                    addUpdateBBTVFonts(toHandle, responseFor(function, toAppend));
                } else if ("addUpdateLocationFontMap".equals(function)) {
                    addUpdateLocationFontMap(toHandle, responseFor(function, toAppend));
                } else if ("addTestUser".equals(function)) {
                    addTestUser(toHandle, responseFor(function, toAppend));
                } else if ("addReports".equals(function)) {
                    addReports(toHandle, responseFor(function, toAppend));
                } else if ("addAlerts".equals(function)) {
                    addAlerts(toHandle, responseFor(function, toAppend));
                } else if ("addLocationReports".equals(function)) {
                    addLocationReports(toHandle, responseFor(function, toAppend));
                } else if ("addSMSAlerts".equals(function)) {
                    addSMSAlerts(toHandle, responseFor(function, toAppend));
                } else if ("addTestInventory".equals(function)) {
                    addTestInventory(toHandle, responseFor(function, toAppend));
                } else if ("addBeverage1".equals(function)) {
                    addBeverage1(toHandle, responseFor(function, toAppend));
                } else if ("addShifts".equals(function)) {
                    addShifts(toHandle, responseFor(function, toAppend));
                } else if ("addProduct1".equals(function)) {
                    addProduct1(toHandle, responseFor(function, toAppend));
                } else if ("addBeverageSize1".equals(function)) {
                    addBeverageSize1(toHandle, responseFor(function, toAppend));
                } else if ("importBeverages1".equals(function)) {
                    importBeverages1(toHandle, responseFor(function, toAppend));
                } else if ("addVarianceValue".equals(function)) {
                    addVarianceValue(toHandle, responseFor(function, toAppend));
                } else if ("addLocationLogs".equals(function)) {
                    addLocationLogs(toHandle, responseFor(function, toAppend));
                } else if ("addTextAlertLogs".equals(function)) {
                    addTextAlertLogs(toHandle, responseFor(function, toAppend));
                } else if ("addKegLines".equals(function)) {
                    addKegLines(toHandle, responseFor(function, toAppend));
                } else if ("addConcessionProductSupplier".equals(function)) {
                    addConcessionProductSupplier(toHandle, responseFor(function, toAppend));
                } else if ("addEmailReportLog".equals(function)) {
                    addEmailReportLog(toHandle, responseFor(function, toAppend));
                } else if ("addEmailReportArchive".equals(function)) {
                    addEmailReportArchive(toHandle, responseFor(function, toAppend));
                } else if ("addUpdateDeleteProductGrouping".equals(function)) {
                    addUpdateDeleteProductGrouping(toHandle, responseFor(function, toAppend));
                } else if ("addUpdateDeleteGroupingForGroup".equals(function)) {
                    addUpdateDeleteGroupingForGroup(toHandle, responseFor(function, toAppend));
                } else if ("addUpdateDeleteInventoryPrices".equals(function)) {
                    addUpdateDeleteInventoryPrices(toHandle, responseFor(function, toAppend));
                } else if ("addUpdateDeleteComingSoonProduct".equals(function)) {
                    addUpdateDeleteComingSoonProduct(toHandle, responseFor(function, toAppend));
                } else if ("brasstapAdjustments".equals(function)) {
                    brasstapAdjustments(toHandle, responseFor(function, toAppend));
                } else if ("addBottleBeer".equals(function)) {
                    addBottleBeer(toHandle, responseFor(function, toAppend));
                } else if ("cleanHootersData".equals(function)) {
                    cleanHootersData(toHandle, responseFor(function, toAppend));
                } else if ("cleanMaxLineCounter".equals(function)) {
                    cleanMaxLineCounter(toHandle, responseFor(function, toAppend));
                } else if ("addProductComment".equals(function)) {
                    addProductComment(toHandle, responseFor(function, toAppend));
                } else if ("addUpdateLocationLogo".equals(function)) {
                    addUpdateLocationLogo(toHandle, responseFor(function, toAppend));
                } else if ("authReportRequest".equals(function)) {
                    authReportRequest(toHandle, responseFor(function, toAppend));
                } else if ("copyTestBeverage".equals(function)) {
                    copyTestBeverage(toHandle, responseFor(function, toAppend));
                } else if ("addInventoryBeveragePrice".equals(function)) {
                    addInventoryBeveragePrice(toHandle, responseFor(function,toAppend));
                } else if ("addUserHistorySummary".equals(function)) {
                    addUserHistorySummary(toHandle, responseFor(function,toAppend));
                } else if ("addUpdateDeleteBeverageToIgnore".equals(function)) {
                    addUpdateDeleteBeverageToIgnore(toHandle, responseFor(function,toAppend));
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
                logger.midwareError("Non-handler exception thrown in SQLAddHandler: " + e.toString());
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

    private Element responseFor(String s, Element e) {
        String responseNamespace = (String) SOAPMessage.getURIMap().get("tkmsg");
        return e.addElement("m:" + s + "Response", responseNamespace);
    }

    private void addErrorDetail(Element toAppend, String message) {
        toAppend.addElement("error").addElement("detail").addText(message);
    }

    private int getCallerId(Element toHandle) throws HandlerException {
        return HandlerUtils.getRequiredInteger(HandlerUtils.getRequiredElement(toHandle, "caller"), "callerId");
    }

    private boolean checkForeignKey(String table, int value, RegisteredConnection transconn) throws SQLException {
        return checkForeignKey(table, "id", value, transconn);
    }

    private boolean checkForeignKey(String table, String field, int value, RegisteredConnection transconn) throws SQLException {

        PreparedStatement stmt = null;
        ResultSet rs = null;
        boolean result = false;

        String select =
                " SELECT " + field + " FROM " + table + " WHERE " + field + "=?";

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
    
    
    public boolean isValidAccessUser(int callerId, int location, boolean isCustomer){
        boolean isValid                     = true;
        int user[]                          = {60,198,166,201,212,3302,347}; //203,199         
        for(int i=0;i<user.length;i++){
            if(user[i]==callerId){
                if(!isCustomer){
                    if(location != 425) {
                        isValid                     = false;
                        logger.debug("Access Denied to Add for Location:"+location +"  callerId:"+callerId);
                    }
                } else {
                    if(location != 205) {
                        isValid                     = false;
                        logger.debug("Access Denied to Add for Customer:"+location +"  callerId:"+callerId);
                    }
                }
            }
        }
        return isValid;
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
    private void addInventory(Element toHandle, Element toAppend) throws HandlerException {
        int callerId = getCallerId(toHandle);
        int locationId = 0;

        String selectLocations = " SELECT id FROM location WHERE customer = ? ";

        PreparedStatement stmt = null;
        ResultSet rs = null, rsLocation = null;

        logger.portalAction("addInventory");

        boolean forCustomer = HandlerUtils.getOptionalBoolean(toHandle, "forCustomer");

        try {
            locationMap = new LocationMap(transconn);
            productMap = new ProductMap(transconn);
            if (forCustomer) {
                int customerId = HandlerUtils.getRequiredInteger(toHandle, "customerId");
                if(callerId > 0 && customerId > 0 && isValidAccessUser(callerId, customerId, true)){   
                    stmt                    = transconn.prepareStatement("SELECT id FROM customer WHERE id = ? AND groupId = 2");
                    stmt.setInt(1, customerId);
                    rs                      = stmt.executeQuery();
                    if (rs.next()) {
                        selectLocations     = "SELECT id FROM location WHERE customer IN (SELECT id FROM customer WHERE groupId = ?)";
                        customerId          = 2;
                    }              
                stmt = transconn.prepareStatement(selectLocations);
                stmt.setInt(1, customerId);
                rsLocation = stmt.executeQuery();
                while (rsLocation.next()) {
                    locationId = rsLocation.getInt(1);
                    addInventoryDetail(callerId, locationId, toHandle, toAppend);
                }
                } else {
                addErrorDetail(toAppend, "Invalid Access"  );
                }
            } else {
                locationId = HandlerUtils.getRequiredInteger(toHandle, "locationId");
                if(callerId > 0 && locationId > 0 && isValidAccessUser(callerId, locationId, false)){                
                    addInventoryDetail(callerId, locationId, toHandle, toAppend);
                } else {
                addErrorDetail(toAppend, "Invalid Access"  );
                }
            }
            locationMap = null;
            productMap = null;
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
            close(rs);
        }
    }

    private void addInventoryDetail(int callerId, int locationId, Element toHandle, Element toAppend) throws HandlerException {

        HashMap<Integer, HashMap> SupplierProductMap = new HashMap<Integer, HashMap>();
        HashMap<Integer, String> ProductPLUMap = new HashMap<Integer, String>();
        HashMap<Integer, Integer> ProductSupplierMap = new HashMap<Integer, Integer>();

        String select = " SELECT id FROM inventory WHERE product=? AND location=?";
        String selectSupplier = "SELECT sA.supplier FROM locationSupplier lS LEFT JOIN supplierAddress sA ON sA.id = lS.address WHERE lS.location=? ";
        String selectSupplierProducts = "SELECT pSM.product, pSM.plu FROM productSetMap pSM LEFT JOIN supplier s ON s.productSet = pSM.productSet WHERE s.id = ? ";
        String selectCoolerInventory = " SELECT id FROM inventory WHERE product=? AND location=? AND bottleSize=? AND cooler=? AND kegLine=? ";
        String insert = " INSERT INTO inventory (product,location,qtyOnHand,minimumQty,qtyToHave,plu,supplier,kegSize,bottleSize) " +
                " VALUES (?,?,?,?,?,?,?,?,?) ";
        String insertCoolerInventory = " INSERT INTO inventory (product,location,qtyOnHand,minimumQty,qtyToHave,plu,supplier,kegSize,bottleSize,cooler,kegLine) " +
                " VALUES (?,?,?,?,?,?,?,?,?,?,?) ";
        String updateIBU                    = "UPDATE productDescription SET ibu =  ? WHERE product =?";
        PreparedStatement stmt = null;
        ResultSet rs = null, rsSupplier = null;

        Iterator i = toHandle.elementIterator("product");

        try {

            stmt = transconn.prepareStatement(selectSupplier);
            stmt.setInt(1, locationId);
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
                int ibu = HandlerUtils.getOptionalInteger(prod, "ibu");
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
                    rs = stmt.executeQuery();
                    if (rs.next()) {
                        logger.generalWarning("Inventory exists at Loc#" + locationId + " for Prod# " + productId);
                        addErrorDetail(toAppend, "Product " + productMap.getProduct(productId) + " already exists at Location: " + locationMap.getLocation(locationId));
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
                            logger.portalDetail(callerId, "addInventory", locationId, "inventory", newId, logMessage, transconn);
                        }
                        if(ibu >=0 && ibu<=100){
                             stmt = transconn.prepareStatement(updateIBU);
                             stmt.setInt(1, ibu);
                             stmt.setInt(2, productId);
                             stmt.executeUpdate();
                        }
                    }
                } else {
                    logger.generalWarning("Foreign key check failed for Prod# " + productId + " or Loc# " + locationId + " or Sup# " + supplierId);
                    addErrorDetail(toAppend, "Unable to add product ID " + productId + "; a database problem occurred");
                }
            }
        } catch (SQLException sqle) {
            logger.dbError("SQL Exception: " + sqle.toString());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
            close(rs);
            close(rsSupplier);
        }
    }

    /**  Add draft-line readings for a location and system.  This is the method
     * to be used by the Picoflash USBN devce.
     *
     *  <locationId>000
     *  <systemId>000
     *  opt:<version>"X.X.X"
     *  <line>
     *      <index>0-15</index>
     *      <reading>000000.000000</reading>
     *  </line>
     *  <line>...</line>
     *
     *  There is an additional optional top-level parameter to indicate the time that the
     *  reading was taken.  This is only available for the whole reading-block, not individual
     *  lines.  There are four ways to specify this:
     *  <timestamp>yyyy-MM-dd'T'HH:mm:ssZ</timestamp>
     *      An absolute timestamp, in a parseable string
     *  <epochSec>000</epochSec>
     *      An absolute timestamp, in whole seconds (NOT MILLIS) from Jan 1, 1970
     *  <offsetSec>000</offsetSec>
     *      A relative timestamp, in whole seconds, indicating how long ago the
     *      reading was taken.  For example, an offsetSec value of 120 means the
     *      reading was taken two minutes ago.  A value of 0 means the reading
     *      was taken right now.
     *  (none provided)
     *      Interpreted that the reading was taken right now
     *
     *  The RECOMMENDED way is <offsetSec>
     */
    private void remoteReading(Element toHandle, Element toAppend, SecureSession ss) throws HandlerException {

        /* Each remoteReading call can contain a number of line readings.  These
         * readings share a common systemId (at a location) and each reading is
         * linked to an index within that system (typically 0-15).
         *
         * To process readings, the first step is to map the systemId-lineIndex
         * pair to a unique lineId.  If the pair doesn't map to a running line,
         * we can ignore the reading.
         *
         * Next, we need to differentiate between "New" readings and "Historical"
         * readings.  New readings are more current than anything in the
         * database, while historical readings fall before our most recent readings
         * for that line.  New readings are always added, but historical readings
         * must pass a set of criteria to make sure they fit with the other readings,
         * otherwise they are discarded.  Readings that are earlier than anything
         * in the database are considered a third type, "Early", and are always accepted.
         *
         * There are two cases for accepting a historical reading.  They depend
         * on the values of the adjacent readings, "previous" and "next"  :
         *  Case A: "Normal case" previous <= next
         *     The reading will be accepted iff its value is in the range [prev,next]
         *  Case B: "Spike/Gap case" previous > next
         *      The reading will be accepted iff its value is NOT in the range [next,prev]
         *      These are accepted because they narrow down the possible interval
         *      where the gap may have occurred, and provide valid readings.
         *      Example (P)rev (N)ext and (C)urrent and | for the spike/gap:
         *         if C<N then we know P | C N
         *         if C>P then we know P C | N
         *
         * Once we've accepted a reading, we need to decide if its superfluous.
         * A reading is superfluous if its value is similar to adjacent readings
         * or its timestamp is too close.
         *
         * If a reading is accepted and not superfluous, then it will be added to
         * the readings table.  Additionally, for New readings, the line counter
         * and location inventory will be updated if there was a change.
         */

        final double IGNORE_THRESHOLD       = 1000;
        final double TWO_HOURS_MILLIS       = 2 * 60 * 60 * 1000;

        DecimalFormat twoPlaces             = new DecimalFormat("0.00");
        DecimalFormat threePlaces           = new DecimalFormat("0.000");

        //Obtain the message parameters
        int location                        = HandlerUtils.getRequiredInteger(toHandle, "locationId");
        int system                          = HandlerUtils.getRequiredInteger(toHandle, "systemId");
        Date timestamp                      = HandlerUtils.getOptionalDate(toHandle, "timestamp");
        long epochSec                       = HandlerUtils.getOptionalLong(toHandle, "epochSec");
        long offsetSec                      = HandlerUtils.getOptionalLong(toHandle, "offsetSec");
        Date lastPouredTime                 = new Date(2007, 1, 1, 7, 0, 0);
        final boolean bypass                = false;
        if (bypass) {
            logger.readingAction("!!! DROPPING remote reading from Loc " + location + (offsetSec > 0 ? ", " + offsetSec + " sec old" : ""));
            return;
        }

        //check the SecureSession for permissions
        if (!(ss.getLocation() == location && ss.getSecurityLevel().canWrite())) {
            logger.readingAccessViolation(("RemoteReading Permission Problem for #" + ss.getLocation() + "-" + ss.getClientId() + " (claimed to be L" + location + ")"));
            //TODO:  Once early locations become complaint with an AES Client Key,
            //       enforce this exception for all locations
            if (location > 10) {
                throw new HandlerException("Permission Error");
            }
        }

        String selectType                   = " SELECT type, harpagonOffset, glanola FROM location WHERE id=?";
        String checkBevBox                  = " SELECT id, name, alert FROM bevBox WHERE location = ? AND startSystem = ?";
        String selectCooler                 = " SELECT id, alertPoint, offset FROM cooler WHERE location=? AND system=?";
        String isCoolerTempNeeded           = " SELECT id FROM coolerTemperature WHERE cooler = ? AND date > (NOW() - INTERVAL 15 MINUTE);";
        String selectLastPoured             = " SELECT lastPoured, lineCleaning, count FROM system WHERE location=? AND systemId=?";
        String selectLCLines                = " SELECT line FROM glanolaLineCleaning WHERE location = ? AND ? BETWEEN startTime AND endTime;";
        String selectLines                  = " SELECT line.lineIndex, line.id, line.ouncesPoured, line.lastPoured, line.lastType FROM line LEFT JOIN system ON line.system = system.id " +
                                            " WHERE system.location=? AND system.systemId=? AND line.status='RUNNING' AND line.product>0";
        //String selectPrevious               = " SELECT value, date FROM reading WHERE line=? AND date<? ORDER BY date DESC LIMIT 1";
        //String selectNext                   = " SELECT value, date FROM reading WHERE line=? AND date>? ORDER BY date ASC LIMIT 1";
        String selectInventory              = " SELECT inv.id, inv.kegSize, inv.qtyOnHand, inv.minimumQty FROM inventory AS inv " +
                                            " LEFT JOIN line ON inv.product = line.product WHERE line.id=? AND inv.location=? ";
        String selectInventoryWithKegLine   = " SELECT inv.id, inv.kegSize, inv.qtyOnHand, inv.minimumQty FROM line " +
                                            " LEFT JOIN kegLine kl ON line.kegLine=kl.id LEFT JOIN inventory inv ON inv.kegLine = kl.id " +
                                            " WHERE line.id=? AND inv.location=?";
        
        String insert                       = " INSERT INTO reading (line,value,date,quantity,type) VALUES (?,?,?,?,?)";
        String insertInterruptionLogs       = " INSERT INTO interruptionLogs (location, system, startTime, endTime, totalMinutes) VALUES (?,?,?,?,?)";
        String insertLineLastPour           = " INSERT INTO lineLastPour (id, value, date) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE value = ?, date = ?;";
        String updateLine                   = " UPDATE line SET ouncesPoured=?, lastPoured=?, lastType = ? WHERE id=?";
        String updateInventory              = " UPDATE inventory SET qtyOnHand=qtyOnHand-? WHERE id=?";
        String updateLineQtyOnHand          = " UPDATE line SET qtyOnHand=qtyOnHand-? WHERE id=?";
        String updateLastPoured             = " UPDATE location SET lastPoured=GREATEST(lastPoured,?) WHERE id=?";
        String updateLastPouredForSystem    = " UPDATE system SET lastPoured=GREATEST(lastPoured,?), lineCleaning=?, count=? WHERE location=? AND systemId=?";
        String updateBevBox                 = " UPDATE bevBox SET alert=0, lastPoured=GREATEST(lastPoured,?) WHERE id=?";
        String insertCoolerTemperature      = " INSERT INTO coolerTemperature (cooler,value,date) VALUES (?,?,?)";

        HashMap<Integer, Integer> lines     = new HashMap<Integer, Integer>(16);
        HashMap<Integer, Double> linesPoured= new HashMap<Integer, Double>(16);
        HashMap<Integer, java.sql.Timestamp> linesDate
                                            = new HashMap<Integer, java.sql.Timestamp>(16);
        HashMap<Integer, Integer> linesType = new HashMap<Integer, Integer>(16);
        ArrayList<Integer> lcLines          = new ArrayList<Integer>();
        boolean lineCleaningMode            = false, glanolaLocation = false;
        double temperature                  = 32.0, temperatureThreshold = 32.0;
        int cooler                          = 1, type = 0, lineCleaning = 0, locationType = 0, manualLineCleaning = -1, tempLineCleaning = -1, lineCleaningCount = -1,
                                            lineCountCheck = -1, temperatureOffset = 0;
        long adjustment                     = 0L;
        PreparedStatement stmt              = null;
        ResultSet rs                        = null;

        try {

            /*logger.ip("Location " + location);
            stmt                        = transconn.prepareStatement("SELECT SUBSTRING(REPLACE(ipAddress, '/',''), 1, LOCATE(':',ipAddress) - 2), REPLACE(message, 'Location ',''), id FROM ipLogs WHERE modified = 0;");
            rs                          = stmt.executeQuery();
            if (rs.next()) {
                String ipAddress        = rs.getString(1);
                int locationId          = rs.getInt(2);
                stmt                    = transconn.prepareStatement("DELETE FROM ipLogs WHERE id < ?");
                stmt.setInt(1, rs.getInt(3));
                stmt.executeUpdate();

                stmt                    = transconn.prepareStatement("SELECT id FROM ipLocationMap WHERE location = ?;");
                stmt.setInt(1, locationId);
                rs                      = stmt.executeQuery();
                if (!rs.next()) {
                    stmt                = transconn.prepareStatement("INSERT INTO ipLocationMap (ipAddress, location) VALUES (?,?)");
                    stmt.setString(1, ipAddress);
                    stmt.setInt(2, locationId);
                    stmt.executeUpdate();
                }
            }*/

            stmt                            = transconn.prepareStatement(selectType);
            stmt.setInt(1, location);
            rs                              = stmt.executeQuery();
            if (rs.next()) {
                locationType                = rs.getInt(1);
                adjustment                  = rs.getLong(2);
                glanolaLocation             = rs.getBoolean(3);
            }

            logger.readingAction("Remote Reading for System " + system + " at Location " + location + (glanolaLocation ? " a Glanola Location" : ""));

            /* Timestamp logic here.  There are currently four ways to specify a timestamp
             * In the priorty they are taken:
             *  (1)  <offsetSec>  The number of seconds ago the reading was taken
             *  (2)  <epochSec>   The absolute time is specified in seconds from the epoch
             *  (3)  <timestamp>  The absolute time as a date
             *  (4)  none         The current time according to the middleware
             */
            // Case (1)
            if (offsetSec >= 0) {
                // Set the timestamp to the current time minus the offset
                timestamp                   = new Date(new Date().getTime() - (offsetSec * 1000));
                logger.debug("Offset sec provided: " + offsetSec + ", timestamp is " + timestamp.toString());
                // Case (2)
            } else if (epochSec > 0) {
                //if the epoch was provided in seconds, convert to millis
                //we need to check this against the correction value first though
                timestamp                   = new Date(epochSec * 1000 + adjustment);
                logger.debug("Epoch sec provided: " + epochSec + ", adj is " + adjustment + ", timestamp is " + timestamp.toString());
                // Case (3)  Timestamp provided
            } else if (timestamp != null) {
                logger.debug("Timestamp provided: " + timestamp.toString());
            } else {
                //if no timestamp was provided, use the current time
                timestamp                   = new Date();
                logger.debug("No timestamp provided, using current time: " + timestamp.toString());
            }

            //Building a list of LC Lines
            stmt                            = transconn.prepareStatement(selectLCLines);
            stmt.setInt(1, location);
            stmt.setTimestamp(2, toSqlTimestamp(timestamp));
            rs                              = stmt.executeQuery();
            while (rs.next()) {
                lcLines.add(rs.getInt(1));
            }
            
            boolean belowEight              = false, aboveEight = false;
            // Build a map from line indices to line db-ids for this system (only take RUNNING lines)
            stmt                            = transconn.prepareStatement(selectLines);
            stmt.setInt(1, location);
            stmt.setInt(2, system);
            rs                              = stmt.executeQuery();
            while (rs.next()) {
                int lineIndex               = rs.getInt(1);
                int lineId                  = rs.getInt(2);
                lines.put(new Integer(lineIndex), new Integer(lineId));
                linesPoured.put(new Integer(lineId), rs.getDouble(3));
                linesDate.put(new Integer(lineId), rs.getTimestamp(4));
                linesType.put(new Integer(lineId), rs.getInt(5));
                if (lineIndex + 1 > 8) {
                    aboveEight              = true;
                } else {
                    belowEight              = true;
                }
            }

            if (aboveEight && belowEight) {
                lineCountCheck              = 2;
            } else {
                lineCountCheck              = 1;
            }

            lastPouredTime                  = new Date(2007, 1, 1, 7, 0, 0);
            lineCleaningMode                = lineCleaning(location, timestamp);

            // Obtain the last poured time for this location by system
            stmt                            = transconn.prepareStatement(selectLastPoured);
            stmt.setInt(1, location);
            stmt.setInt(2, system);
            rs = stmt.executeQuery();
            if (rs.next()) {
                lastPouredTime              = rs.getTimestamp(1);
                manualLineCleaning          = tempLineCleaning = rs.getInt(2);
                lineCleaningCount           = rs.getInt(3) + 1;
                
                if (lineCleaningMode) {
                    if (manualLineCleaning == 0) {
                        manualLineCleaning  = 10;
                    } else {
                        manualLineCleaning  = 11;
                    }
                } else if (!lineCleaningMode && (manualLineCleaning > 0)) {
                    if (manualLineCleaning == 11) {
                        manualLineCleaning  = 12;
                    }  else {
                        manualLineCleaning  = 0;
                    }
                } 
            } else {
                logger.debug("By-Passing System " + system + " at Location " + location);
                return;
            }

            long timeDiff                   = (Math.abs(timestamp.getTime() - lastPouredTime.getTime()) / 1000) / 60;

            // Record that a reading occured in the location table
            stmt                            = transconn.prepareStatement(updateLastPoured);
            stmt.setTimestamp(1, toSqlTimestamp(timestamp));
            stmt.setInt(2, location);
            stmt.executeUpdate();

            // Record that a reading occured in the system table
            stmt                            = transconn.prepareStatement(updateLastPouredForSystem);
            stmt.setTimestamp(1, toSqlTimestamp(timestamp));
            stmt.setInt(2, (lineCleaningCount == lineCountCheck) ? manualLineCleaning : tempLineCleaning);
            stmt.setInt(3, (lineCleaningCount == lineCountCheck) ? 0 : lineCleaningCount);
            stmt.setInt(4, location);
            stmt.setInt(5, system);
            stmt.executeUpdate();

            // Checking and updating bevBox Alerts
            stmt                            = transconn.prepareStatement(checkBevBox);
            stmt.setInt(1, location);
            stmt.setInt(2, system);
            rs                              = stmt.executeQuery();
            if (rs.next()) {
                if (rs.getInt(3) == 1) {
                    //deativateBevBoxAlert(location, rs.getString(2));
                }

                stmt                        = transconn.prepareStatement(updateBevBox);
                stmt.setTimestamp(1, toSqlTimestamp(timestamp));
                stmt.setInt(2, rs.getInt(1));
                stmt.executeUpdate();
            }

            if (timeDiff > 15 && timeDiff < 36000) {
                //logger.debug("Data Interruption found for system " + String.valueOf(system) + " Location " + String.valueOf(location) + " : Interrupted by " + String.valueOf(timeDiff) + " minutes");
                stmt                        = transconn.prepareStatement(insertInterruptionLogs);
                stmt.setInt(1, location);
                stmt.setInt(2, system);
                stmt.setTimestamp(3, toSqlTimestamp(lastPouredTime));
                stmt.setTimestamp(4, toSqlTimestamp(timestamp));
                stmt.setLong(5, timeDiff);
                stmt.executeUpdate();
                if (isAfterHours(timestamp, location)) {
                    timestamp               = afterHoursTimeAdjustment(timestamp, location);
                }
            }

            // Getting cooler Information
            boolean hasCooler               = false;
            stmt                            = transconn.prepareStatement(selectCooler);
            stmt.setInt(1, location);
            stmt.setInt(2, system);
            rs                              = stmt.executeQuery();
            if (rs.next()) {
                hasCooler                   = true;
                cooler                      = rs.getInt(1);
                temperatureThreshold        = rs.getDouble(2);
                temperatureOffset           = rs.getInt(3);
            }

            // checking if cooler temp is needed
            // condition to check is to see if we need data every 15 minutes
            boolean tempNeeded              = true;
            stmt                            = transconn.prepareStatement(isCoolerTempNeeded);
            stmt.setInt(1, cooler);
            rs                              = stmt.executeQuery();
            if (rs.next()) {
                tempNeeded                  = false;
            }

            // Process each remote reading
            Iterator el                     = toHandle.elementIterator("line");
            while (el.hasNext()) {
                
                Element line                = (Element) el.next();
                double reading              = HandlerUtils.getRequiredDouble(line, "reading");
                int index                   = HandlerUtils.getRequiredInteger(line, "index");
                lineCleaning                = HandlerUtils.getOptionalInteger(line, "lc");
                if (manualLineCleaning > 0) {
                    lineCleaning            = manualLineCleaning;
                }
                //logger.debug("lineCleaning: " + String.valueOf(lineCleaning));

                if (lines.containsKey(index)) {
                    LineReading prev = null, next = null, current = null;
                    int lineId              = lines.get(new Integer(index)).intValue();

                    if (hasCooler && tempNeeded) {
                        String temperatureString = HandlerUtils.getOptionalString(line, "t");
                        if (temperatureString == null || temperatureString.length() > 3) {
                            temperature = 32;
                        } else {
                            temperature = Double.valueOf(temperatureString);
                        }
                        temperature     = temperature + temperatureOffset;
                        if ((temperature < 120) && (temperature > -20)) {
                            //Add a temperature reading.
                            //logger.debug("Adding temperature reading for cooler # " + cooler + " with temperature " + String.valueOf(temperature) + " F");
                            stmt = transconn.prepareStatement(insertCoolerTemperature);
                            stmt.setInt(1, cooler);
                            stmt.setDouble(2, temperature);
                            stmt.setTimestamp(3, toSqlTimestamp(timestamp));
                            stmt.executeUpdate();
                            
                            stmt = transconn.prepareStatement("UPDATE cooler set lastValue= ? WHERE id= ?");                            
                            stmt.setDouble(1, temperature);
                            stmt.setInt(2, cooler);
                            stmt.executeUpdate();
                            if (isTemperatureAlert(cooler, timestamp, temperature, temperatureThreshold)) {
                                setAlert(1, cooler, temperature);
                            }
                            hasCooler = false;
                        }
                    }

                    //Set up the current/next/previous readings
                    current                 = new LineReading(lineId, reading, timestamp);
                    prev                    = new LineReading(lineId, linesPoured.get(lineId), linesDate.get(lineId));

                    double ozDifference     = current.spikeTolerantDifference(prev);
                    long timeDifference     = current.getTimeDifference(prev) / 1000;
                    if (ozDifference > 100000) { ozDifference = 0; }
                    //logger.debug("Oz Difference" + String.valueOf(ozDifference));
                    
                    if (timeDifference > 0) {
                        double ouncesPerSecond
                                            = ozDifference / timeDifference;
                        if (ozDifference > 1000 || (ozDifference < 0)) {
                            stmt            = transconn.prepareStatement("INSERT INTO spikedReading (location, line, quantity, ozPerSecond) VALUES (?, ?, ?, ?)");
                            stmt.setInt(1, location);
                            stmt.setInt(2, lineId);
                            stmt.setDouble(3, ozDifference);
                            stmt.setDouble(4, ouncesPerSecond);
                            stmt.executeUpdate();
                            
                            stmt            = transconn.prepareStatement(insert);
                            stmt.setInt(1, lineId);
                            stmt.setDouble(2, -1);
                            stmt.setTimestamp(3, toSqlTimestamp(timestamp));
                            stmt.setDouble(4, 0);
                            stmt.setInt(5, 0);
                            stmt.executeUpdate();

                            ozDifference    = 0.0;
                        } else if (ouncesPerSecond > 1.0) {
                            stmt    = transconn.prepareStatement("SELECT count + 1, IF(TIME_TO_SEC(TIMEDIFF(NOW(), startDate)) / 3600 > 1, 1, 0) "
                                    + " FROM spikedLineReading WHERE location = ? AND line = ?;");
                            stmt.setInt(1, location);
                            stmt.setInt(2, lineId);
                            rs      = stmt.executeQuery();
                            if (rs.next()) {
                                if (rs.getInt(1) >= 25 && rs.getInt(1) == 1) {
                                    stmt
                                    = transconn.prepareStatement("DELETE FROM spikedLineReading WHERE location = ? AND line = ?;");
                                    stmt.setInt(1, location);
                                    stmt.setInt(2, lineId);
                                    stmt.executeUpdate();
                                } else {
                                    stmt
                                    = transconn.prepareStatement("UPDATE spikedLineReading SET count = count + 1, startDate = startDate WHERE location = ? AND line = ?;");
                                    stmt.setInt(1, location);
                                    stmt.setInt(2, lineId);
                                    stmt.executeUpdate();
                                }
                            } else {
                                stmt= transconn.prepareStatement("INSERT INTO spikedLineReading (location, line, count) VALUES (?, ?, 1)");
                                stmt.setInt(1, location);
                                stmt.setInt(2, lineId);
                                stmt.executeUpdate();
                            }
                        } else {
                            stmt    = transconn.prepareStatement("DELETE FROM spikedLineReading WHERE location  =? AND line = ?;");
                            stmt.setInt(1, location);
                            stmt.setInt(2, lineId);
                            stmt.executeUpdate();
                        }
                    }
                    
                    int prevType            = linesType.get(lineId);
                    if (lcLines.contains(lineId)) {
                        logger.debug("Line Cleaning via App");
                        if (prevType == 0) {
                            lineCleaning    
                                            = 10;
                        } else {
                            lineCleaning    = 11;
                        } 
                    } else if (prevType > 0) {
                        lineCleaning        = (prevType == 11 ? 12 : 0);
                    }
                    
                    if (isNormalPour(lineCleaning)) {

                        if ((readingIsNeeded(prev, current, next))) {

                            if ((current.getValueDifference(prev) < -1)) {
                                //Storing the dipped reading after the spike value
                                stmt = transconn.prepareStatement(insert);
                                stmt.setInt(1, lineId);
                                stmt.setDouble(2, reading);
                                stmt.setTimestamp(3, toSqlTimestamp(new Date(timestamp.getTime() + (30 * 1000))));
                                stmt.setDouble(4, 0);
                                stmt.setInt(5, 0);
                                stmt.executeUpdate();

                                reading = -1.0;
                                current = new LineReading(lineId, reading, timestamp);
                                //logger.debug("Inserting Forced Spike for line #" + lineId + " at " + timestamp.toString());
                            }

                            //Add a reading.
                            logger.readingDetail("Adding reading for line #" + lineId + " : " + reading + (next != null ? " HISTORIC: " + timestamp.toString() : ""));

                            stmt = transconn.prepareStatement(insert);
                            stmt.setInt(1, lineId);
                            stmt.setDouble(2, reading);
                            stmt.setTimestamp(3, toSqlTimestamp(timestamp));
                            stmt.setDouble(4, ozDifference);
                            stmt.setInt(5, 0);
                            stmt.executeUpdate();
                            
                            //Update the line record
                            if (ozDifference > 0) {
                                stmt = transconn.prepareStatement(insertLineLastPour);
                                stmt.setInt(1, lineId);
                                stmt.setDouble(2, ozDifference);
                                stmt.setTimestamp(3, toSqlTimestamp(timestamp));
                                stmt.setDouble(4, ozDifference);
                                stmt.setTimestamp(5, toSqlTimestamp(timestamp));
                                stmt.executeUpdate();
                            }
                            
                            stmt = transconn.prepareStatement(updateLine);
                            stmt.setDouble(1, reading);
                            stmt.setTimestamp(2, toSqlTimestamp(timestamp));
                            stmt.setInt(3, lineCleaning);
                            stmt.setInt(4, lineId);
                            stmt.executeUpdate();

                            //Decide if we should update stock
                            if (ozDifference > 1.00) {
                                if (isAfterHours(timestamp, location)) {
                                    //logger.debug("Setting After Hours Alert for #" + lineId + " at " + timestamp.toString());
                                    setAlert(2, lineId, ozDifference);
                                }
                                String updateLog = "";

                                //get the inventory ID and the keg-size for the inventory
                                int invId = 0;
                                double kegSize = 1920.0;
                                double kegDifference = 0.0;
                                double qtyOnHand = 0.0;
                                double minimumQty = 0.0;

                                // customers of type=2 use the KegLine lookup
                                stmt = transconn.prepareStatement(locationType == 2 ? selectInventoryWithKegLine : selectInventory);
                                stmt.setInt(1, lineId);
                                stmt.setInt(2, location);
                                rs = stmt.executeQuery();
                                if (rs.next()) {
                                    invId = rs.getInt(1);
                                    kegSize = rs.getDouble(2);
                                    qtyOnHand = rs.getDouble(3);
                                    minimumQty = rs.getDouble(4);
                                    if (kegSize > 1.0) {
                                        kegDifference = ozDifference / kegSize;
                                    }
                                }
                                updateLog += "Poured " + twoPlaces.format(ozDifference) + "oz from L#" + lineId;

                                if (invId > 0) {
                                    if ((qtyOnHand - kegDifference) < minimumQty) {
                                        //setAlert(3, invId, (qtyOnHand - kegDifference));
                                    }
                                    //Update the quantity on hand for the inventory
                                    stmt = transconn.prepareStatement(updateInventory);
                                    stmt.setDouble(1, kegDifference);
                                    stmt.setInt(2, invId);
                                    stmt.executeUpdate();
                                    stmt = transconn.prepareStatement(updateLineQtyOnHand);
                                    stmt.setDouble(1, kegDifference);
                                    stmt.setInt(2, lineId);
                                    stmt.executeUpdate();
                                    //Make sure the inventory doesn't go negative
                                    // Changed Nov 3 / 06 to go negative again at the request of Jason Purdy
                                    //stmt = transconn.prepareStatement(updateInventoryAboveZero);
                                    //stmt.setInt(1, invId);
                                    //stmt.executeUpdate();
                                    updateLog += ", deducting " + threePlaces.format(kegDifference) + " kegs from inv#" + invId;
                                } else {
                                    logger.generalWarning("Unable to find inv record for line #" + lineId + " at loc #" + location);
                                }
                                /*stmt = transconn.prepareStatement("INSERT INTO pourTest (id,lastReading,poured) VALUES (?,?,?) ON DUPLICATE KEY UPDATE lastReading=?, poured =?;");
                                stmt.setInt(1, lineId);
                                stmt.setTimestamp(2, toSqlTimestamp(timestamp));
                                stmt.setDouble(3, ozDifference);
                                stmt.setTimestamp(4, toSqlTimestamp(timestamp));
                                stmt.setDouble(5, ozDifference);
                                stmt.executeUpdate();*/
                                
                                logger.debug(updateLog);
                            }
                        } else {
                            logger.readingDetail("Ignored line #" + lineId + " : " + twoPlaces.format(reading) + " Since last: " + current.getTimeDifference(prev) + " ms");
                        }
                    } else {
                        if (isEnterExitMode(lineCleaning)) {
                            
                            switch (lineCleaning) {
                                case 10:
                                    type    = 0;
                                    setLineCleaningLog(location, lineId, timestamp);
                                    break;
                                case 12:
                                    setLineCleaningLog(location, lineId, timestamp);
                                    type    = 1;
                                    break;
                                default:
                                    break;
                            }

                            //logger.debug("Inserting Forced Spike for Line Cleaning Enter/Exit Mode on line #" + lineId + " at " + timestamp.toString());

                            current         = new LineReading(lineId, reading, timestamp);
                            stmt            = transconn.prepareStatement(insert);
                            stmt.setInt(1, lineId);
                            stmt.setDouble(2, -1.0);
                            stmt.setTimestamp(3, toSqlTimestamp(timestamp));
                            stmt.setDouble(4, 0);
                            stmt.setInt(5, 0);
                            stmt.executeUpdate();

                            current         = new LineReading(lineId, reading, timestamp);
                            stmt            = transconn.prepareStatement(insert);
                            stmt.setInt(1, lineId);
                            stmt.setDouble(2, -1.0);
                            stmt.setTimestamp(3, toSqlTimestamp(timestamp));
                            stmt.setDouble(4, 0);
                            stmt.setInt(5, 1);
                            stmt.executeUpdate();

                            current         = new LineReading(lineId, reading, timestamp);
                            stmt            = transconn.prepareStatement(insert);
                            stmt.setInt(1, lineId);
                            stmt.setDouble(2, reading);
                            stmt.setTimestamp(3, toSqlTimestamp(new Date(timestamp.getTime() + (30 * 1000))));
                            stmt.setDouble(4, ozDifference);
                            stmt.setInt(5, type);
                            stmt.executeUpdate();
                        } else if (isLineCleaning(lineCleaning)) {
                            //logger.debug("Adding Line Cleaning reading for line #" + lineId + " at " + timestamp.toString());
                            type            = 1;
                            stmt            = transconn.prepareStatement(insert);
                            stmt.setInt(1, lineId);
                            stmt.setDouble(2, reading);
                            stmt.setTimestamp(3, toSqlTimestamp(timestamp));
                            stmt.setDouble(4, ozDifference);
                            stmt.setInt(5, type);
                            stmt.executeUpdate();
                        }

                        //Update the line record
                        stmt                = transconn.prepareStatement(updateLine);
                        stmt.setDouble(1, reading);
                        stmt.setTimestamp(2, toSqlTimestamp(timestamp));
                        stmt.setInt(3, lineCleaning);
                        stmt.setInt(4, lineId);
                        stmt.executeUpdate();
                    }
                } else if ((reading > 0.0) && (lines.size() > 0)) {
                    /*
                    double diffValue        = 0.0;
                    String selectUnknownLastLineReading
                                            = "SELECT value FROM unknownReading WHERE location = ? AND line = ? AND system = ? ORDER BY date DESC LIMIT 1";
                    String insertUnknownLineReading
                                            = "INSERT INTO unknownReading (location, line, system, value, quantity) VALUES (?, ?, ?, ?, ?)";

                    stmt                    = transconn.prepareStatement(selectUnknownLastLineReading);
                    stmt.setInt(1, location);
                    stmt.setInt(2, index);
                    stmt.setInt(3, system);
                    rs                      = stmt.executeQuery();
                    if (rs.next()) {
                        double prevValue    = rs.getInt(1);
                        if (reading >= prevValue) {
                            diffValue       = reading - prevValue;
                        }
                        //logger.debug("prevValue: " + prevValue);
                        //logger.debug("currValue: " + currValue);
                    } else {
                        diffValue           = 1;
                    }

                    if (diffValue > 0) {
                        stmt                = transconn.prepareStatement(insertUnknownLineReading);
                        stmt.setInt(1, location);
                        stmt.setInt(2, index);
                        stmt.setInt(3, system);
                        stmt.setDouble(4, reading);
                        stmt.setDouble(5, diffValue);
                        stmt.executeUpdate();
                    } */
                }
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error in remoteReading: " + sqle.toString());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
            close(rs);
        }
    }

    private Date afterHoursTimeAdjustment(Date timestamp, int location) throws HandlerException {

        PreparedStatement stmt = null;
        ResultSet rs = null;
        Date storeCloseTime = new Date(2007, 1, 1, 7, 0, 0);

        String selectStoreCloseTime = "SELECT " +
                " DATE_SUB(If(IFNULL(x.close, '02:00:00')>'12:0:0',concat(left(subdate(now(),1),11),IFNULL(x.close, '02:00:00')),concat(left(now(),11),IFNULL(x.close, '02:00:00'))), INTERVAL eO HOUR) Close " +
                " FROM (Select CASE DAYOFWEEK(NOW()-1000000) " +
                " WHEN 1 THEN Right(lH.closeSun,8) " +
                " WHEN 2 THEN Right(lH.closeMon,8) " +
                " WHEN 3 THEN Right(lH.closeTue,8) " +
                " WHEN 4 THEN Right(lH.closeWed,8) " +
                " WHEN 5 THEN Right(lH.closeThu,8) " +
                " WHEN 6 THEN Right(lH.closeFri,8) " +
                " WHEN 7 THEN Right(lH.closeSat,8) END close, " +
                " l.easternOffset eO " +
                " FROM location l LEFT JOIN locationHours lH ON lH.location = l.id WHERE l.id=?) AS x; ";
        try {
            stmt = transconn.prepareStatement(selectStoreCloseTime);
            stmt.setInt(1, location);
            rs = stmt.executeQuery();
            if (rs.next()) {
                storeCloseTime = rs.getTimestamp(1);
                long t2 = storeCloseTime.getTime();
                t2 -= 10 * 60 * 1000;
                storeCloseTime = new Date(t2);
            } else {
                storeCloseTime = timestamp;
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
            close(rs);
        }
        return storeCloseTime;
    }

    //new method to check and execute cooler temperature exceptions
    private boolean isTemperatureAlert(int cooler, Date timestamp, double currentTemp, double alertThreshold) {
        boolean isTempAlert = false;

        if (currentTemp > alertThreshold) {
            if (!temperatureAlertCoolerCache.containsKey(cooler)) {
                isTempAlert = false;
                temperatureAlertCoolerCache.put(cooler, new Date(timestamp.getTime() + (2400 * 1000)));
                highestTemperatureCoolerCache.put(cooler, currentTemp);
            } else {
                if (currentTemp > highestTemperatureCoolerCache.get(cooler)) {
                    highestTemperatureCoolerCache.put(cooler, currentTemp);
                }
                if (timestamp.after(temperatureAlertCoolerCache.get(cooler))) {
                    isTempAlert = true;
                    String selectException = "SELECT id FROM coolerException WHERE cooler = ? AND start BETWEEN ? AND ?;";
                    PreparedStatement stmt = null;
                    ResultSet rs = null;
                    try {
                        stmt = transconn.prepareStatement(selectException);
                        stmt.setInt(1, cooler);
                        stmt.setTimestamp(2, toSqlTimestamp(new Date(temperatureAlertCoolerCache.get(cooler).getTime() - (2430 * 1000))));
                        stmt.setTimestamp(3, toSqlTimestamp(timestamp));
                        rs = stmt.executeQuery();
                        if (rs.next()) {
                            String updateException = "UPDATE coolerException SET value=?, end = ? WHERE id = ?;";
                            stmt = transconn.prepareStatement(updateException);
                            stmt.setDouble(1, highestTemperatureCoolerCache.get(cooler));
                            stmt.setTimestamp(2, toSqlTimestamp(timestamp));
                            stmt.setInt(3, rs.getInt(1));
                            stmt.executeUpdate();
                        } else {
                            String insertException = "INSERT INTO coolerException (cooler, value, start, end) VALUES (?,?,?,?);";
                            stmt = transconn.prepareStatement(insertException);
                            stmt.setInt(1, cooler);
                            stmt.setDouble(2, highestTemperatureCoolerCache.get(cooler));
                            stmt.setTimestamp(3, toSqlTimestamp(new Date(temperatureAlertCoolerCache.get(cooler).getTime() - (2400 * 1000))));
                            stmt.setTimestamp(4, toSqlTimestamp(timestamp));
                            stmt.executeUpdate();
                        }
                    } catch (SQLException sqle) {
                        logger.dbError("Database error: " + sqle.getMessage());
                    } finally {
                        close(stmt);
                        close(rs);
                    }
                } else {
                    isTempAlert = false;
                }
            }
        } else {
            if (temperatureAlertCoolerCache.containsKey(cooler)) {
                temperatureAlertCoolerCache.remove(cooler);
            }
            if (highestTemperatureCoolerCache.containsKey(cooler)) {
                highestTemperatureCoolerCache.remove(cooler);
            }
            isTempAlert = false;
        }
        return isTempAlert;
    }

    private boolean isAfterHours(Date timestamp, int location) {

        PreparedStatement stmt = null;
        ResultSet rs = null;
        Date storeCloseTime = new Date(2007, 1, 1, 7, 0, 0);
        Date storePreOpenTime = new Date(2007, 1, 1, 7, 0, 0);
        boolean setAfterHours = false;

        String selectLocationType = "SELECT type FROM location WHERE id = ?; ";
        String selectRetailStoreCloseTime = "SELECT location FROM todayHours WHERE location = ? AND ? BETWEEN closeBefore and preOpen; ";
        String selectConcessionStoreCloseTime = "SELECT eventEnd, eventAfterHoursEnd FROM eventHours WHERE DATEDIFF(date, If(RIGHT(now(),8)<'07:00:00',left(subdate(now(),1),11),left(now(),11))) = 0 AND location=? ";

        try {

            stmt = transconn.prepareStatement(selectLocationType);
            stmt.setInt(1, location);
            rs = stmt.executeQuery();
            if (rs.next()) {
                switch(rs.getInt(1)) {
                    case 1:
                        stmt = transconn.prepareStatement(selectRetailStoreCloseTime);
                        stmt.setInt(1, location);
                        stmt.setString(2, dateFormat.format(timestamp));
                        rs = stmt.executeQuery();
                        if (rs.next()) {
                            setAfterHours = true;
                        } else {
                            setAfterHours = false;
                        }
                        break;
                    case 2:
                        stmt = transconn.prepareStatement(selectConcessionStoreCloseTime);
                        stmt.setInt(1, location);
                        rs = stmt.executeQuery();
                        while (rs.next()) {
                            storeCloseTime = rs.getTimestamp(1);
                            storePreOpenTime = rs.getTimestamp(2);
                            if ((timestamp.after(storeCloseTime)) && (timestamp.before(storePreOpenTime))) {
                                setAfterHours = true;
                                break;
                            } else {
                                setAfterHours = false;
                            }
                        }
                        break;
                    default:
                        setAfterHours = false;
                        break;
                }
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
        } finally {
            close(stmt);
            close(rs);
        }
        return setAfterHours;
    }

    private boolean isBusinessHour(Date timestamp, int location) {

        PreparedStatement stmt              = null;
        ResultSet rs                        = null;
        Date storeCloseTime                 = new Date(2007, 1, 1, 7, 0, 0);
        Date storePreOpenTime               = new Date(2007, 1, 1, 7, 0, 0);
        boolean isBusinessHour              = false;

        String selectLocationType           = "SELECT type FROM location WHERE id = ?; ";
        String selectRetailBusinessHours    = "SELECT " +
                                            " DATE_SUB(Concat(LEFT(NOW(),11),IFNULL(x.open,'11:00:00')), INTERVAL eO HOUR) Open, " +
                                            " DATE_SUB(If(x.close>'12:0:0',concat(LEFT(NOW(),11),IFNULL(x.close,'02:00:00')),concat(LEFT(ADDDATE(NOW(),1),11),IFNULL(x.close,'02:00:00'))), INTERVAL eO HOUR) Close, eO " +
                                            " FROM (Select CASE DAYOFWEEK(NOW()) " +
                                            " WHEN 1 THEN Right(lH.openSun,8) " +
                                            " WHEN 2 THEN Right(lH.openMon,8) " +
                                            " WHEN 3 THEN Right(lH.openTue,8) " +
                                            " WHEN 4 THEN Right(lH.openWed,8) " +
                                            " WHEN 5 THEN Right(lH.openThu,8) " +
                                            " WHEN 6 THEN Right(lH.openFri,8) " +
                                            " WHEN 7 THEN Right(lH.openSat,8) END open, " +
                                            " CASE DAYOFWEEK(NOW()) " +
                                            " WHEN 1 THEN Right(lH.closeSun,8) " +
                                            " WHEN 2 THEN Right(lH.closeMon,8) " +
                                            " WHEN 3 THEN Right(lH.closeTue,8) " +
                                            " WHEN 4 THEN Right(lH.closeWed,8) " +
                                            " WHEN 5 THEN Right(lH.closeThu,8) " +
                                            " WHEN 6 THEN Right(lH.closeFri,8) " +
                                            " WHEN 7 THEN Right(lH.closeSat,8) END close, " +
                                            " l.easternOffset eO " +
                                            " FROM locationHours lH RIGHT JOIN location l ON lH.location=l.id " +
                                            " WHERE l.id=?) AS x; ";
        String selectConcessionBusinessHours= "SELECT preOpen, eventEnd FROM eventHours WHERE DATEDIFF(date, If(RIGHT(now(),8)<'07:00:00',left(subdate(now(),1),11),left(now(),11))) = 0 AND location=? ";

        try {

            stmt                            = transconn.prepareStatement(selectLocationType);
            stmt.setInt(1, location);
            rs                              = stmt.executeQuery();
            if (rs.next()) {
                switch(rs.getInt(1)) {
                    case 1:
                        stmt                = transconn.prepareStatement(selectRetailBusinessHours);
                        stmt.setInt(1, location);
                        rs                  = stmt.executeQuery();
                        if (rs.next()) {
                            storeCloseTime  = rs.getTimestamp(1);
                            storePreOpenTime= rs.getTimestamp(2);
                            if ((timestamp.after(storeCloseTime)) && (timestamp.before(storePreOpenTime))) {
                                isBusinessHour
                                            = true;
                            } else {
                                isBusinessHour
                                            = false;
                            }
                        } 
                        break;
                    case 2:
                        stmt                = transconn.prepareStatement(selectConcessionBusinessHours);
                        stmt.setInt(1, location);
                        rs = stmt.executeQuery();
                        while (rs.next()) {
                            storeCloseTime  = rs.getTimestamp(1);
                            storePreOpenTime= rs.getTimestamp(2);
                            if ((timestamp.after(storeCloseTime)) && (timestamp.before(storePreOpenTime))) {
                                isBusinessHour
                                            = true;
                                break;
                            } else {
                                isBusinessHour
                                            = false;
                            }
                        } 
                        break;
                    default:
                        isBusinessHour       = false;
                        break;
                }
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
        } finally {
            close(stmt);
            close(rs);
        }
        return isBusinessHour;
    }

    private long getAlertOffsetInMillis(Integer tableId, Integer tableType) throws HandlerException {
        PreparedStatement stmt = null;
        ResultSet rs = null;

        long alertOffsetInMillis = 0;

        try {
            String selectAlertDescription = "";

            switch (tableType) {
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
                    selectAlertDescription += " ";
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
     * The folowing code is to insert the email report requirements for each user for the email address
     * that they provide - SR
     */
    private void addEmailReports(Element toHandle, Element toAppend) throws HandlerException {

        int callerId = getCallerId(toHandle);
        String select = " SELECT id FROM emailReport WHERE user=? AND report=? AND tableId=? AND tableType=?";

        String insert = " INSERT INTO emailReport (user, report, tableId, tableType, tableValues, minAlertThreshold, maxAlertThreshold, reportFormat, time, platform) " +
                " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        SimpleDateFormat f = new SimpleDateFormat("HH:mm:ss");
        PreparedStatement stmt = null, innerstmt = null;
        ResultSet rs = null;

        int userId = HandlerUtils.getRequiredInteger(toHandle, "userId");
        Iterator i = toHandle.elementIterator("reports");

        try {
            while (i.hasNext()) {
                Element prod = (Element) i.next();
                int report = HandlerUtils.getRequiredInteger(prod, "report");
                int tableId = HandlerUtils.getRequiredInteger(prod, "tableId");
                int tableType = HandlerUtils.getRequiredInteger(prod, "tableType");
                double minAlertThreshold = HandlerUtils.getRequiredDouble(prod, "minAlertThreshold");
                double maxAlertThreshold = HandlerUtils.getRequiredDouble(prod, "maxAlertThreshold");
                int reportFormat = HandlerUtils.getRequiredInteger(prod, "reportFormat");
                int platform = HandlerUtils.getRequiredInteger(prod, "platform");
                String tableValues = HandlerUtils.getOptionalString(prod, "tableValues");
                int reportId = 0;
                //Check that this alert doesn't already exist in alerts for this user
                stmt = transconn.prepareStatement(select);
                stmt.setInt(1, userId);
                stmt.setInt(2, report);
                stmt.setInt(3, tableId);
                stmt.setInt(4, tableType);
                rs = stmt.executeQuery();

                if (!rs.next() || (platform == 2)) {
                    stmt = transconn.prepareStatement(insert);
                    stmt.setInt(1, userId);
                    stmt.setInt(2, report);
                    stmt.setInt(3, tableId);
                    stmt.setInt(4, tableType);
                    stmt.setString(5, HandlerUtils.nullToString(tableValues, "0"));
                    stmt.setDouble(6, minAlertThreshold);
                    stmt.setDouble(7, maxAlertThreshold);
                    stmt.setInt(8, reportFormat);

                    Iterator j = prod.elementIterator("time");
                    String insertTimeTable      = " INSERT INTO emailTimeTable (user, report, time, day) VALUES (?, ?, ?, ?)";
                    
                    if (j.hasNext()) {
                        stmt.setInt(9, 1);
                        stmt.setInt(10, platform);
                        stmt.executeUpdate();

                        stmt = transconn.prepareStatement("SELECT LAST_INSERT_ID()");
                        rs = stmt.executeQuery();
                        if (rs.next()) {
                            reportId = rs.getInt(1);
                        }

                        
                        while (j.hasNext()) {
                            Element time = (Element) j.next();
                            String alertTime = HandlerUtils.getRequiredString(time, "reportTime");
                            int alertDay = HandlerUtils.getOptionalInteger(time, "reportDay");
                            if (platform == 2 && alertDay < 0) {
                                alertDay = 1;
                            }
                            logger.debug(alertTime);
                            java.util.Date d = f.parse(alertTime);
                            java.util.Date date = new java.util.Date(d.getTime() - getAlertOffsetInMillis(tableId, tableType));
                            innerstmt = transconn.prepareStatement(insertTimeTable);
                            innerstmt.setInt(1, userId);
                            innerstmt.setInt(2, reportId);
                            innerstmt.setString(3, dateToString(date));
                            innerstmt.setInt(4, alertDay);
                            innerstmt.executeUpdate();
                        }
                    } else if (platform == 2) {
                            stmt.setInt(9, 1);
                            stmt.setInt(10, platform);
                            stmt.executeUpdate();

                            stmt = transconn.prepareStatement("SELECT LAST_INSERT_ID()");
                            rs = stmt.executeQuery();
                            if (rs.next()) {
                                reportId = rs.getInt(1);
                            }

                            innerstmt = transconn.prepareStatement(insertTimeTable);
                            innerstmt.setInt(1, userId);
                            innerstmt.setInt(2, reportId);
                            innerstmt.setString(3, "10:10:00");
                            innerstmt.setInt(4, 1);
                            innerstmt.executeUpdate();
                    } else {
                            stmt.setInt(9, 0);
                            stmt.setInt(10, platform);
                            stmt.executeUpdate();
                    }
                    
                } else {
                    reportId = rs.getInt(1);

                    Iterator j = prod.elementIterator("time");
                    if (j.hasNext()) {
                        while (j.hasNext()) {
                            Element time = (Element) j.next();
                            String alertTime = HandlerUtils.getRequiredString(time, "reportTime");
                            java.util.Date d = f.parse(alertTime);
                            java.util.Date date = new java.util.Date(d.getTime() - getAlertOffsetInMillis(tableId, tableType));

                            String selectTimeTable = " SELECT id FROM emailTimeTable WHERE user = ? AND report = ? AND time = ?";
                            stmt = transconn.prepareStatement(selectTimeTable);
                            stmt.setInt(1, userId);
                            stmt.setInt(2, reportId);
                            stmt.setString(3, f.format(date));
                            rs = stmt.executeQuery();
                            if (!rs.next()) {
                                String insertTimeTable = " INSERT INTO emailTimeTable (user, report, time) VALUES (?, ?, ?)";
                                innerstmt = transconn.prepareStatement(insertTimeTable);
                                innerstmt.setInt(1, userId);
                                innerstmt.setInt(2, reportId);
                                innerstmt.setString(3, dateToString(date));
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
            close(stmt);
            close(innerstmt);
            close(rs);
        }
    }

    /**
     * The folowing code is to insert the alert requirements for each user for the email address
     * that they provide - SR
     */
    private void addTextAlerts(Element toHandle, Element toAppend) throws HandlerException {

        int callerId = getCallerId(toHandle);
        String select = " SELECT id FROM textAlert WHERE user=? AND alert=? AND tableId=? AND tableType=?";

        String insert = " INSERT INTO textAlert (user, alert, tableId, tableType, minAlertThreshold, maxAlertThreshold, alertTime, alertFrequency) " +
                " VALUES (?, ?, ?, ?, ?, ?, ?, ?)";

        int varianceCheck = 4;

        SimpleDateFormat f = new SimpleDateFormat("HH:mm:ss");
        PreparedStatement stmt = null;
        ResultSet rs = null;

        int userId = HandlerUtils.getRequiredInteger(toHandle, "userId");
        Iterator i = toHandle.elementIterator("alerts");

        try {
            while (i.hasNext()) {
                Element prod = (Element) i.next();
                int alert = HandlerUtils.getRequiredInteger(prod, "alert");
                int tableId = HandlerUtils.getRequiredInteger(prod, "tableId");
                int tableType = HandlerUtils.getRequiredInteger(prod, "tableType");
                double minAlertThreshold = HandlerUtils.getRequiredDouble(prod, "minAlertThreshold");
                double maxAlertThreshold = HandlerUtils.getRequiredDouble(prod, "maxAlertThreshold");
                String alertTimestamp = HandlerUtils.getRequiredString(prod, "alertTimestamp");
                int alertFrequency = HandlerUtils.getRequiredInteger(prod, "alertFrequency");

                //Check that this alert doesn't already exist in alerts for this user
                stmt = transconn.prepareStatement(select);
                stmt.setInt(1, userId);
                stmt.setInt(2, alert);
                stmt.setInt(3, tableId);
                stmt.setInt(4, tableType);
                rs = stmt.executeQuery();

                if (!rs.next() || (alert == varianceCheck)) {
                    stmt = transconn.prepareStatement(insert);
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

    /* companion method to remoteReading
     *  Contains logic to set alerts.
     */
    private void setAlert(Integer alertType, Integer unitId, Double value) {

        String checkAlert = " SELECT id FROM alerts WHERE alertType = ? AND unitId = ? AND status = 1 ";
        String insertAlert = " INSERT INTO alerts (alertType, unitId, value, status) VALUES (?,?,?,1)";
        String updateAlert = " UPDATE alerts SET value = value + ? WHERE id = ?";
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            stmt = transconn.prepareStatement(checkAlert);
            stmt.setInt(1, alertType);
            stmt.setInt(2, unitId);
            rs = stmt.executeQuery();
            if (!rs.next()) {
                stmt = transconn.prepareStatement(insertAlert);
                stmt.setInt(1, alertType);
                stmt.setInt(2, unitId);
                stmt.setDouble(3, value);
                stmt.executeUpdate();
                logger.debug("Set alert for unitId: " + unitId + ", alertType " + alertType + " with value: " + value);
            } else if (alertType == 2) {
                stmt = transconn.prepareStatement(updateAlert);
                stmt.setDouble(1, value);
                stmt.setInt(2, rs.getInt(1));
                stmt.executeUpdate();
                logger.debug("Updating after hours alert for unitId: " + unitId + ", alertType " + alertType + " with value: " + value);
            }
        } catch (Exception sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
        } finally {
            close(stmt);
        }
    }

    /** Detects if two readings are sufficiently different to be a "spike".  The
     *  change that occurs during a spike is ignored in a poured report.
     */
    private boolean isASpike(LineReading previous, LineReading current, double ozDifference) {
        //Current Implementation: a spike is any pour greater than 5 oz / sec over the period:
        //Also, the period must be at least five seconds, or the readings are ignored
        final double maxRate = 5.0 / 1000.0; //ounces per millisecond
        final long fiveSeconds = 5 * 1000;

        if (current == null) {
            //New Reading
            return false;
        }
        if (previous == null) {
            //Early Reading
            return true;
        }
        if (previous.getDate() == null || current.getDate() == null) {
            return false;
        }
        long timeDifference = current.getDate().getTime() - previous.getDate().getTime();
        if (timeDifference < fiveSeconds) {
            return false;
        }

        double rate = ozDifference / timeDifference;
        //logger.debug("Remote Reading has for pourRate of: " + String.valueOf(rate) + " at Line#: " + String.valueOf(current.getLine()));
        return (rate >= maxRate);
    }

    /* companion method to remoteReading
     *  Contains logic to decide if a reading is line cleaning.
     */
    private void setLineCleaningLog(int location, int line, Date timestamp) {

        DateTimeParameter lineCleaningDate = new DateTimeParameter(timestamp);
        String checkLineCleaningLog  = " SELECT id FROM lineCleaningLog WHERE location = ? AND line = ? AND date = ? ";
        String insertLineCleaningLog = " INSERT INTO lineCleaningLog (location, line, date, startTime) VALUES (?,?,?,?) ";
        String updateLineCleaningLog = " UPDATE lineCleaningLog SET endTime = ? WHERE id = ? ";

        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            stmt = transconn.prepareStatement(checkLineCleaningLog);
            stmt.setInt(1, location);
            stmt.setInt(2, line);
            stmt.setString(3, lineCleaningDate.toDateString());
            rs = stmt.executeQuery();
            if (!rs.next()) {
                stmt = transconn.prepareStatement(insertLineCleaningLog);
                stmt.setInt(1, location);
                stmt.setInt(2, line);
                stmt.setString(3, lineCleaningDate.toDateString());
                stmt.setTimestamp(4,toSqlTimestamp(timestamp));
                stmt.executeUpdate();
            } else {
                stmt = transconn.prepareStatement(updateLineCleaningLog);
                stmt.setTimestamp(1,toSqlTimestamp(timestamp));
                stmt.setInt(2, rs.getInt(1));
                stmt.executeUpdate();
            }
        } catch (Exception sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
        } finally {
            close(stmt);
            close(rs);
        }
    }

    /* companion method to remoteReading
     *  Contains logic to decide if a reading is line cleaning.
     */
    private boolean isLineCleaning(int lineCleaning) {
        if (lineCleaning == 11) {
            return true;
        } else {
            return false;
        }
    }

    /* companion method to remoteReading
     *  Contains logic to decide if a reading is normal pour.
     */
    private boolean isNormalPour(int lineCleaning) {
        if (lineCleaning <= 0) {
            return true;
        } else {
            return false;
        }
    }

    /* companion method to remoteReading
     *  Contains logic to decide if a reading is Enter Exit stage.
     */
    private boolean isEnterExitMode(int lineCleaning) {
        if (lineCleaning == 10 || lineCleaning == 12) {
            return true;
        } else {
            return false;
        }
    }

    /* companion method to remoteReading
     *  Contains logic to decide if a reading should be accepted or not.
     */
    private boolean readingIsAcceptable(LineReading prev, LineReading current, LineReading next) {
        //TODO: Logic is fine, but maybe clean up the presentation so the rules are more obvious
        //TODO: Unit test all cases
        if (current == null) {
            return false;
        }
        if (next == null || prev == null) {
            //New or Early Reading
            return true;
        }
        //all three are non-null, so we have a historical reading
        if (current.isInTimeSequence(prev, next)) {
            boolean isInRange = current.isInValueSequence(prev, next);
            if (next.isGreaterEqualTo(prev)) {
                return isInRange;
            } else {
                return !isInRange;
            }
        } else {
            return false;
        }
    }

    /* companion method to remoteReading
     * Contains logic to decide if a reading is superfluous
     */
    private boolean readingIsNeeded(LineReading prev, LineReading current, LineReading next) {
        /* New logic for when to add a reading or ignore it:
         *  A reading WILL be added if ANY of the following are true
         *   #1.  The ounces poured has changed by > than 0.01
         *   #2.  At least two hours have passed since the last reading.
         */
        final double TWO_HOURS_MILLIS = 2 * 60 * 60 * 1000;
        double ozDifference = 0.0;
        long timeDifference = 0;

        if (current == null) {
            //logger.debug("readingIsNeeded: READING IS NULL");
            return false;
        }
        if (next == null && prev == null) {
            //only reading we have
            //logger.debug("readingIsNeeded: (null-null)");
            return true;
        }
        if (next == null) {
            //New reading
            ozDifference = current.getValueDifference(prev);
            timeDifference = current.getTimeDifference(prev);
            //logger.debug("readingIsNeeded: (next-null) Oz:"+ozDifference+" T:"+timeDifference);
            return (Math.abs(ozDifference) > 0.01 || timeDifference > TWO_HOURS_MILLIS);
        } else if (prev == null) {
            //Early reading
            ozDifference = next.getValueDifference(current);
            timeDifference = next.getTimeDifference(current);
            //logger.debug("readingIsNeeded: (prev-null) Oz:"+ozDifference+" T:"+timeDifference);
            return (Math.abs(ozDifference) > 0.01 || timeDifference > TWO_HOURS_MILLIS);
        } else {
            //Historical reading
            ozDifference = next.getValueDifference(prev);
            timeDifference = next.getTimeDifference(prev);
            //logger.debug("readingIsNeeded: (historical) Oz:"+ozDifference+" T:"+timeDifference);
            return (Math.abs(ozDifference) > 100.0 || timeDifference > TWO_HOURS_MILLIS);
        }
    }

    /**
     * Deativating all alerts from the night before
     */
    private boolean lineCleaning(int locationId, Date timestamp) throws HandlerException {

        String checkCraftWorksAccount       = "SELECT id FROM location WHERE customer IN (102) AND id = ? AND " +
                                            " ? BETWEEN SUBDATE(CONCAT(LEFT(NOW(), 10), ' 06:00:00'), INTERVAL easternOffset HOUR) " +
                                            " AND SUBDATE(CONCAT(LEFT(NOW(), 10), ' 10:30:00'), INTERVAL easternOffset HOUR)";
        String checkBrassTapAccount         = "SELECT id FROM location WHERE customer IN (254,269,271,274,274) AND id = ? AND " +
                                            " ? BETWEEN SUBDATE(CONCAT(LEFT(NOW(), 10), ' 06:30:00'), INTERVAL easternOffset HOUR) " +
                                            " AND SUBDATE(CONCAT(LEFT(NOW(), 10), ' 11:00:00'), INTERVAL easternOffset HOUR)";
        String checkLineCleaningMode        = "SELECT id FROM lineCleaning WHERE location = ? AND ? BETWEEN startTime and endTime ";
        PreparedStatement stmt              = null;
        ResultSet rs                        = null;
        boolean lineCleaningMode            = false;
        
        try {
            int dayOfWeek                   = timestamp.getDay();
            stmt                            = transconn.prepareStatement(checkCraftWorksAccount);
            stmt.setInt(1, locationId);
            stmt.setTimestamp(2, toSqlTimestamp(timestamp));
            rs                              = stmt.executeQuery();
            if ((dayOfWeek != 0) && (dayOfWeek != 6) && rs.next()) {
                logger.debug("CW Line Cleaning: " + dayOfWeek);
                lineCleaningMode            = true;
            } else {
                stmt                        = transconn.prepareStatement(checkBrassTapAccount);
                stmt.setInt(1, locationId);
                stmt.setTimestamp(2, toSqlTimestamp(timestamp));
                rs                          = stmt.executeQuery();
                if ((dayOfWeek != 0) && (dayOfWeek != 6) && rs.next()) {
                    logger.debug("BT Line Cleaning: " + dayOfWeek);
                    lineCleaningMode        = true;
                } else {
                    stmt                    = transconn.prepareStatement(checkLineCleaningMode);
                    stmt.setInt(1, locationId);
                    stmt.setTimestamp(2, toSqlTimestamp(timestamp));
                    rs                      = stmt.executeQuery();
                    if (rs.next()) {
                        logger.debug("Regular Line Cleaning");
                        lineCleaningMode    = true;
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
        return lineCleaningMode;
    }

    /* companion method to remoteReading
     *  Contains logic to decide if a reading is line cleaning.
     */
    private void deativateBevBoxAlert(int location, String name) {

        PreparedStatement stmt              = null;
        ResultSet rs                        = null;
        StringBuilder emailBody             = new StringBuilder();
        try {
            emailBody.append("<tr align=justify><td colspan=4>Your bevBox: ");
            emailBody.append(name);
            emailBody.append(" has <strong> resumed service. </strong>It is now reporting draft beer pour data to our systems.</td></tr>");
            emailBody.append("<tr align=center valign=middle><td height=20 colspan=4>&nbsp;</td></tr>");
            emailBody.append("<tr align=justify><td colspan=4>Thank You,</td></tr>");
            emailBody.append("<tr align=justify><td colspan=4>US Beverage Net Support</td></tr>");
            emailBody.append("<tr align=center valign=middle><td height=35 colspan=4>&nbsp;</td></tr>");
            emailBody.append("<tr align=justify><td colspan=4><strong>This email was automatically generated; please do not reply.</strong></td></tr>");
            emailBody.append("<tr><td colspan=4>&nbsp;</td></tr>");
            boolean ccField                 = true;
            String sql                      = "SELECT uA.id, u.name, u.email FROM userAlerts uA LEFT JOIN user u ON u.id = uA.user " +
                                            " WHERE uA.alert = 1 AND uA.tableType = 2 AND uA.tableId = ?";
            stmt                            = transconn.prepareStatement(sql);
            stmt.setInt(1, location);
            rs                              = stmt.executeQuery();
            while (rs.next()) {
                sendMail(rs.getString(2), rs.getString(3), "Location Notification", emailBody, ccField);
                ccField                     = false;
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
        } finally {
            close(stmt);
            close(rs);
        }
    }
    private void confirmSummary(Element toHandle, Element toAppend) {

        PreparedStatement stmt              = null;
        ResultSet rs                        = null;
        StringBuilder emailBody             = new StringBuilder();
        try {
            emailBody.append("<tr align=justify><td colspan=4>Your Summar: ");
            emailBody.append(" has <strong> resumed service. </strong>It is now reporting draft beer pour data to our systems.</td></tr>");
            emailBody.append("<tr align=center valign=middle><td height=20 colspan=4>&nbsp;</td></tr>");
            emailBody.append("<tr align=justify><td colspan=4>Thank You,</td></tr>");
            emailBody.append("<tr align=justify><td colspan=4>US Beverage Net Support</td></tr>");
            emailBody.append("<tr align=center valign=middle><td height=35 colspan=4>&nbsp;</td></tr>");
            emailBody.append("<tr align=justify><td colspan=4><strong>This email was automatically generated; please do not reply.</strong></td></tr>");
            emailBody.append("<tr><td colspan=4>&nbsp;</td></tr>");
            boolean ccField                 = true;
            String sql                      = "SELECT uA.id, u.name, u.email FROM userAlerts uA LEFT JOIN user u ON u.id = uA.user " +
                                            " WHERE uA.alert = 1 AND uA.tableType = 2 AND uA.tableId = ?";
            stmt                            = transconn.prepareStatement(sql);
            rs                              = stmt.executeQuery();
            while (rs.next()) {
                sendMail(rs.getString(2), rs.getString(3), "Location Notification", emailBody, ccField);
                ccField                     = false;
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
        } finally {
            close(stmt);
            close(rs);
        }
    }

    public void sendMail(String userName, String emailAddr, String title, StringBuilder emailBody, boolean ccField) {

        String emailTemplatePath = HandlerUtils.getSetting("email.templatePath");
        if ((emailTemplatePath == null) || "".equals(emailTemplatePath)) {
            emailTemplatePath = ".";
        }
        logger.debug("Packaging Email");
        try {
            logger.debug("Loading Template");
            TemplatedMessage poEmail =
                    new TemplatedMessage("USBN Notification",
                    emailTemplatePath, "sendMail");

            //logger.debug("Setting Template Fields to "+email+" #"+String.valueOf(purchase));
            poEmail.setSender("tech@beerboard.com");
            //logger.debug("*");
            poEmail.setRecipient(emailAddr);
            if (ccField) {
                poEmail.setRecipientBCC("boxalert@beerboard.com");
            }
            //logger.debug("*");
            if ((emailBody.length() > 0)) {
                poEmail.setField("TITLE", title);
                poEmail.setField("BODY", emailBody.toString());
                poEmail.send();
                logger.debug("Email sent successfully for " + userName);
            } else {
                poEmail.setField("TITLE", "");
                poEmail.setField("BODY", "");
                poEmail.send();
                logger.debug("Email sent successfully for " + userName);
            }

        } catch (MailException me) {
            logger.debug("Error sending purchase message to " + emailAddr + ": " + me.toString());
        }
    }

    /**  Informs the middleware that a picoflash device has powered up
     *  <locationId>000
     *  <version>"X.X.X"
     *
     */
    private void picoflashPowerup(Element toHandle, Element toAppend, SecureSession ss) throws HandlerException {
        String version = HandlerUtils.getRequiredString(toHandle, "version");
        int location = HandlerUtils.getRequiredInteger(toHandle, "locationId");

        locationMap = new LocationMap(transconn);

        logger.debug("Pico powered up OK at " + locationMap.getLocation(location) + " (v" + version + ")");

        if (!(ss.getLocation() == location && ss.getSecurityLevel().canWrite())) {
            logger.readingAccessViolation(("picoflashPowerup Permission Problem for #" + locationMap.getLocation(ss.getLocation()) + "-" + ss.getClientId() + " (claimed to be L" + locationMap.getLocation(location) + ")"));
        }
        String sql =
                " UPDATE location SET picoPowerup=NOW(), picoVersion=? WHERE id=?";
        String insertReading =
                " INSERT INTO reading (line,value) VALUES (?,-1.0)";
        String selectLines =
                " SELECT line.id " +
                " FROM line LEFT JOIN system ON line.system = system.id " +
                " WHERE system.location=? AND line.status='RUNNING' AND line.product>0";

        PreparedStatement stmt = null;
        ResultSet rs = null;

        try {
            stmt = transconn.prepareStatement(sql);
            stmt.setString(1, version);
            stmt.setInt(2, location);
            stmt.executeUpdate();

            if (version.startsWith("4") && (location != 149) && (location != 300) && (location != 773) && (location != 812)) {
                stmt = transconn.prepareStatement(selectLines);
                stmt.setInt(1, location);
                rs = stmt.executeQuery();
                while (rs.next()) {
                    stmt = transconn.prepareStatement(insertReading);
                    stmt.setInt(1, rs.getInt(1));
                    stmt.executeUpdate();
                }
            }

            logger.debug("Powerup from L " + locationMap.getLocation(location) + ", version " + version);
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
            close(rs);
        }
        locationMap = null;
    }

    /** Converts a java.util.Date to a java.sql.Date
     */
    private java.sql.Timestamp toSqlTimestamp(Date d) {
        return new java.sql.Timestamp(d.getTime());
    }

    /**  Add data into the poured or sold summary table
     *  <type> poured | sold
     *  <locationId> 0000
     *  <overwrite>0|1     if 0, ONLY product data will be overwritten, if 1, the whole day will be cleared
     *  <summary>
     *    <productId>
     *    <date> all dates must be of the 'YYYY-MM-DD' format
     *    <ounces>
     *  </summary>
     *  <summary>
     *    ...
     *  </summary>
     */
    private void addSummary(Element toHandle, Element toAppend) throws HandlerException {
        int locationId = HandlerUtils.getRequiredInteger(toHandle, "locationId");
        int overwrite = HandlerUtils.getOptionalInteger(toHandle, "overwrite");
        SummaryType tableType = SummaryType.instanceOf("poured");
        String tableTypeString = HandlerUtils.getRequiredString(toHandle, "type");
        if (null != tableTypeString) {
            tableType = SummaryType.instanceOf(HandlerUtils.getRequiredString(toHandle, "type"));
        }
        String table = tableType.toString() + "Summary";
        int locationType = tableType.toLocationType();
        logger.debug("Table Type: " + table + "| Location Type: " + locationType);
        PreparedStatement stmt = null, dayClear = null, prodClear = null;
        boolean oldAutoCommit = true;
        boolean changedAutoCommit = false;
        DecimalFormat twoPlaces = new DecimalFormat("0.00");

        Set<String> cleared = new HashSet<String>();
        Iterator sums = toHandle.elementIterator("summary");
        int barId = 0, exclusion = 0, projection = 0;
        String bDate = "";
        try {
            oldAutoCommit = transconn.getAutoCommit();
            changedAutoCommit = true;
            transconn.setAutoCommit(false);

            switch (locationType){
                case 1:
                    barId = HandlerUtils.getRequiredInteger(toHandle, "barId");
                    exclusion = HandlerUtils.getRequiredInteger(toHandle, "exclusion");
                    stmt = transconn.prepareStatement(
                    "INSERT INTO " + table + " (location,bar,product,date,value,exclusion) " +
                    "VALUES (?,?,?,?,?,?)");
                    dayClear = transconn.prepareStatement("DELETE FROM " + table + " WHERE location=? AND bar=? AND date=?");
                    prodClear = transconn.prepareStatement("DELETE FROM " + table + " WHERE location=? AND bar=? AND date=? AND product=?");

                    //logger.debug("Adding " + tableType.toString() + " summaries for location " + locationId);
                    while (sums.hasNext()) {
                        Element summary = (Element) sums.next();
                        int productId = HandlerUtils.getRequiredInteger(summary, "productId");
                        double value = HandlerUtils.getRequiredDouble(summary, "ounces");
                        String date = HandlerUtils.getRequiredString(summary, "date");

                        if (overwrite > 0) {
                            if (!cleared.contains(date)) {
                                dayClear.setInt(1, locationId);
                                dayClear.setInt(2, barId);
                                dayClear.setString(3, date);
                                dayClear.execute();
                                logger.debug("Clearing " + table + " records for date: '" + date + "'");
                                cleared.add(date);
                            }
                        } else {
                            prodClear.setInt(1, locationId);
                            prodClear.setInt(2, barId);
                            prodClear.setString(3, date);
                            prodClear.setInt(4, productId);
                            prodClear.execute();
                            logger.debug("Cleared " + table + " row for P#" + productId + " on " + date);
                        }

                        int colCount = 1;
                        stmt.setInt(colCount++, locationId);
                        stmt.setInt(colCount++, barId);
                        stmt.setInt(colCount++, productId);
                        stmt.setString(colCount++, date);
                        stmt.setDouble(colCount++, value);
                        stmt.setInt(colCount++, exclusion);
                        stmt.executeUpdate();
                        //logger.debug("Adding summary: " + date + " P#" + productId + ", " + twoPlaces.format(value) + " oz");
                    }
                    break;
                case 2:
                    int stationId = HandlerUtils.getRequiredInteger(toHandle, "stationId");
                    int eventId = HandlerUtils.getRequiredInteger(toHandle, "eventId");
                    exclusion = HandlerUtils.getRequiredInteger(toHandle, "exclusion");
                    stmt = transconn.prepareStatement(
                    "INSERT INTO " + table + " (location,station,product,event,date,value,exclusion) " +
                    "VALUES (?,?,?,?,?,?,?)");
                    dayClear = transconn.prepareStatement("DELETE FROM " + table + " WHERE location=? AND station=? AND event=? AND date=? ");
                    prodClear = transconn.prepareStatement("DELETE FROM " + table + " WHERE location=? AND station=? AND product=? AND event=? AND date=? ");

                    //logger.debug("Adding " + tableType.toString() + " summaries for location " + locationId);

                    while (sums.hasNext()) {
                        Element summary = (Element) sums.next();
                        int productId = HandlerUtils.getRequiredInteger(summary, "productId");
                        double value = HandlerUtils.getRequiredDouble(summary, "ounces");
                        String date = HandlerUtils.getRequiredString(summary, "date");

                        if (overwrite > 0) {
                            if (!cleared.contains(date)) {
                                dayClear.setInt(1, locationId);
                                dayClear.setInt(2, stationId);
                                dayClear.setInt(3, eventId);
                                dayClear.setString(4, date);
                                dayClear.execute();
                                logger.debug("Clearing " + table + " records for date: '" + date + "'");
                                cleared.add(date);
                            }
                        } else {
                            prodClear.setInt(1, locationId);
                            prodClear.setInt(2, stationId);
                            prodClear.setInt(3, productId);
                            prodClear.setInt(4, eventId);
                            prodClear.setString(5, date);
                            prodClear.execute();
                            logger.debug("Cleared " + table + " row for P#" + productId + " on " + date);
                        }

                        int colCount = 1;
                        stmt.setInt(colCount++, locationId);
                        stmt.setInt(colCount++, stationId);
                        stmt.setInt(colCount++, productId);
                        stmt.setInt(colCount++, eventId);
                        stmt.setString(colCount++, date);
                        stmt.setDouble(colCount++, value);
                        stmt.setInt(colCount++, exclusion);
                        stmt.executeUpdate();
                        //logger.debug("Adding summary: " + date + " P#" + productId + ", " + twoPlaces.format(value) + " oz");
                    }
                    break;
                case 3:
                    projection              = HandlerUtils.getRequiredInteger(toHandle, "projection");
                    double totalPoured      = HandlerUtils.getRequiredDouble(toHandle, "pouredTotal");
                    bDate                   = HandlerUtils.getRequiredString(toHandle, "date");
                    stmt = transconn.prepareStatement(
                    "INSERT INTO " + table + " (location,product,date,value,share,projection) " +
                    "VALUES (?,?,?,?,?,?)");
                    dayClear = transconn.prepareStatement("DELETE FROM " + table + " WHERE location=? AND date=?");
                    prodClear = transconn.prepareStatement("DELETE FROM " + table + " WHERE location=? AND date=? AND product=?");

                    //logger.debug("Adding " + tableType.toString() + " summaries for location " + locationId);
                    while (sums.hasNext()) {
                        Element summary = (Element) sums.next();
                        int productId = HandlerUtils.getRequiredInteger(summary, "productId");
                        double value = HandlerUtils.getRequiredDouble(summary, "ounces");

                        if (overwrite > 0) {
                            if (!cleared.contains(bDate)) {
                                dayClear.setInt(1, locationId);
                                dayClear.setString(2, bDate);
                                dayClear.execute();
                                logger.debug("Clearing " + table + " records for date: '" + bDate + "'");
                                cleared.add(bDate);
                            }
                        } else {
                            prodClear.setInt(1, locationId);
                            prodClear.setString(2, bDate);
                            prodClear.setInt(3, productId);
                            prodClear.execute();
                            logger.debug("Cleared " + table + " row for P#" + productId + " on " + bDate);
                        }

                        double share = (value/totalPoured) * 100;

                        int colCount = 1;
                        stmt.setInt(colCount++, locationId);
                        stmt.setInt(colCount++, productId);
                        stmt.setString(colCount++, bDate);
                        stmt.setDouble(colCount++, value);
                        stmt.setDouble(colCount++, share);
                        stmt.setInt(colCount++, projection);
                        stmt.executeUpdate();
                        //logger.debug("Adding summary: " + date + " P#" + productId + ", " + twoPlaces.format(value) + " oz");
                    }
                    break;
                case 4:
                    /*double poured           = 0.0;
                    projection              = HandlerUtils.getRequiredInteger(toHandle, "projection");
                    bDate                   = HandlerUtils.getRequiredString(toHandle, "date");
                    logger.debug("Adding " + tableType.toString() + " summaries for location " + locationId);
                    PreparedStatement projectionStmt
                                            = transconn.prepareStatement("INSERT INTO projectionDetails (location,product,date,projection) VALUES (?,?,?,?); ");
                    stmt                    = transconn.prepareStatement("INSERT INTO " + table + " (location,product,date,value,share,projection) VALUES (?,?,?,?,?,?)");
                    dayClear                = transconn.prepareStatement("DELETE FROM " + table + " WHERE location=? AND date=?");
                    PreparedStatement dayDetailsClear
                                            = transconn.prepareStatement("DELETE FROM projectionDetails WHERE location=? AND date=?");
                    if (overwrite > 0) {
                        if (!cleared.contains(bDate)) {
                            dayClear.setInt(1, locationId);
                            dayClear.setString(2, bDate);
                            dayClear.execute();

                            dayDetailsClear.setInt(1, locationId);
                            dayDetailsClear.setString(2, bDate);
                            dayDetailsClear.execute();
                            
                            //logger.debug("Clearing " + table + " records for date: '" + bDate + "'");
                            cleared.add(bDate);
                        }
                    }
                    HashMap<Integer, Double> projectedValue
                                            = getProjectionMap(locationId, projection, bDate, sums);
                    cleared                 = new HashSet<String>();
                    Object[] key            = projectedValue.keySet().toArray();
                    Arrays.sort(key);
                    
                    for (int i = 0; i < key.length; i++) {
                        int productId       = Integer.valueOf(key[i].toString());
                        
                        if (productId == 0) {
                            poured = projectedValue.get(productId);
                            //logger.debug("Total Poured: " + twoPlaces.format(poured));
                        } else {
                            double share    = (projectedValue.get(productId)/poured) * 100;
                            //logger.debug("Storing projection for product: " + String.valueOf(productId) + ", " + twoPlaces.format(projectedValue.get(productId)) + " oz, " + twoPlaces.format(share) + " %");
                            int colCount    = 1;
                            stmt.setInt(colCount++, locationId);
                            stmt.setInt(colCount++, productId);
                            stmt.setString(colCount++, bDate);
                            stmt.setDouble(colCount++, projectedValue.get(productId));
                            stmt.setDouble(colCount++, share);
                            stmt.setInt(colCount++, projection);
                            stmt.executeUpdate();

                            //Storing projection information
                            projectionStmt.setInt(1, locationId);
                            projectionStmt.setInt(2, productId);
                            projectionStmt.setString(3, bDate);
                            projectionStmt.setInt(4, projection);
                            projectionStmt.executeUpdate();
                        }
                    }
                    close(projectionStmt);
                     * */
                    break;
                case 5:
                    stmt = transconn.prepareStatement(
                    "INSERT INTO " + table + " (location,product,date,value) " +
                    "VALUES (?,?,?,?)");
                    dayClear = transconn.prepareStatement("DELETE FROM " + table + " WHERE location=? AND date=?");
                    prodClear = transconn.prepareStatement("DELETE FROM " + table + " WHERE location=? AND date=? AND product=?");

                    //logger.debug("Adding " + tableType.toString() + " summaries for location " + locationId);
                    while (sums.hasNext()) {
                        Element summary = (Element) sums.next();
                        int productId = HandlerUtils.getRequiredInteger(summary, "productId");
                        double value = HandlerUtils.getRequiredDouble(summary, "kegs");
                        String date = HandlerUtils.getRequiredString(summary, "date");

                        if (overwrite > 0) {
                            if (!cleared.contains(date)) {
                                dayClear.setInt(1, locationId);
                                dayClear.setString(2, date);
                                dayClear.execute();
                                logger.debug("Clearing " + table + " records for date: '" + date + "'");
                                cleared.add(date);
                            }
                        } else {
                            prodClear.setInt(1, locationId);
                            prodClear.setString(2, date);
                            prodClear.setInt(3, productId);
                            prodClear.execute();
                            logger.debug("Cleared " + table + " row for P#" + productId + " on " + date);
                        }

                        int colCount = 1;
                        stmt.setInt(colCount++, locationId);
                        stmt.setInt(colCount++, productId);
                        stmt.setString(colCount++, date);
                        stmt.setDouble(colCount++, value);
                        stmt.executeUpdate();
                        //logger.debug("Adding summary: " + date + " P#" + productId + ", " + twoPlaces.format(value) + " oz");
                    }
                    break;
                default:
                    break;
            }
            transconn.commit();
            logger.debug("Summaries Committed");
        } catch (SQLException sqle) {
            try {
                transconn.rollback();
            } catch (Exception e) {
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
            close(stmt);
            close(dayClear);
            close(prodClear);
        }
    }

    private void addExclusionSummary(Element toHandle, Element toAppend) throws HandlerException {
        String tableType = HandlerUtils.getRequiredString(toHandle, "type");
        int overwrite = HandlerUtils.getOptionalInteger(toHandle, "overwrite");
        if (!("exclusion".equals(tableType))) {
            throw new HandlerException("Invalid Summary Type: " + tableType);
        }
        String table = tableType + "Summary";
        PreparedStatement stmt = null, dayClear = null;
        boolean oldAutoCommit = true;
        boolean changedAutoCommit = false;
        DecimalFormat twoPlaces = new DecimalFormat("0.00");

        try {
            oldAutoCommit = transconn.getAutoCommit();
            changedAutoCommit = true;
            transconn.setAutoCommit(false);

            stmt = transconn.prepareStatement(
                    "INSERT INTO " + table + " (location,bar,exclusion,date) " +
                    "VALUES (?,?,?,?)");
            dayClear = transconn.prepareStatement("DELETE FROM " + table + " WHERE location=? AND bar=? AND date=?");
            Iterator sums = toHandle.elementIterator("summary");
            while (sums.hasNext()) {
                Set<String> cleared = new HashSet<String>();

                Element summary = (Element) sums.next();
                int locationId = HandlerUtils.getRequiredInteger(summary, "locationId");
                int barId = HandlerUtils.getRequiredInteger(summary, "barId");
                int exclusion = HandlerUtils.getRequiredInteger(summary, "exclusion");
                String date = HandlerUtils.getRequiredString(summary, "date");
                logger.debug("Adding " + tableType + " summaries for location " + locationId);

                if (overwrite > 0) {
                    if (!cleared.contains(date)) {
                        dayClear.setInt(1, locationId);
                        dayClear.setInt(2, barId);
                        dayClear.setString(3, date);
                        dayClear.execute();
                        logger.debug("Clearing " + table + " records for date: '" + date + "'");
                        cleared.add(date);
                    }
                }

                int colCount = 1;
                stmt.setInt(colCount++, locationId);
                stmt.setInt(colCount++, barId);
                stmt.setInt(colCount++, exclusion);
                stmt.setString(colCount++, date);
                stmt.executeUpdate();
            }
            transconn.commit();
            logger.debug("Summaries Committed");
        } catch (SQLException sqle) {
            try {
                transconn.rollback();
            } catch (Exception e) {
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
            close(stmt);
        }

    }

    private void addTierSummary(Element toHandle, Element toAppend) throws HandlerException {

        String date                         = HandlerUtils.getRequiredString(toHandle, "date");
        int locationId                      = HandlerUtils.getOptionalInteger(toHandle, "locationId");
        PreparedStatement stmt              = null, addTier = null, dayClear = null, updateTables = null, barStmt = null;
        ResultSet rs                        = null, barRS = null;
        boolean oldAutoCommit               = true;
        boolean changedAutoCommit           = false;
        
        String selectTierValue              = " SELECT LID, Poured, Sold, Var, " +
                                            " IF (Var<-40, 4, IF(Var<-10,3,IF(Var<-5,2,IF(Var<0,1,IF(Var=0,5,IF(Var<10,1,4)))))) AS Cat FROM " +
                                            " (SELECT l.LID, ROUND(IFNULL(tPoured,0),2) Poured, ROUND(IFNULL(tSold,0),2) Sold, " +
                                            " IFNULL(ROUND(((tSold - tPoured)/tPoured)*100,2),0) Var FROM " +
                                            " (SELECT l.id LID FROM location l LEFT JOIN locationDetails lD ON lD.location = l.id " +
                                            " WHERE lD.active = 1 AND lD.suspended = 0 AND l.type = 1) AS l " +
                                            " LEFT JOIN " +
                                            " (SELECT l.id LID, SUM(oHS.value) tPoured FROM openHoursSummary oHS LEFT JOIN location l ON l.id = oHS.location " +
                                            " WHERE oHS.date = LEFT(?,11) " + (locationId > 0 ? " AND l.id = ? " : "") + " GROUP BY oHS.location) AS p " +
                                            " ON p.LID = l.LID " +
                                            " LEFT JOIN " +
                                            " (SELECT l.id LID, SUM(oHSS.value) tSold FROM openHoursSoldSummary oHSS LEFT JOIN location l ON l.id = oHSS.location " +
                                            " WHERE oHSS.date = LEFT(?,11) " + (locationId > 0 ? " AND l.id = ? " : "") + " GROUP BY oHSS.location) AS s " +
                                            " ON s.LID = l.LID) AS V " +
                                            " ORDER BY Cat, Var; ";
        String tableName[]                  = { "openHoursSummary", "openHoursSoldSummary", "pouredSummary", "soldSummary"};
        try {
            oldAutoCommit                   = transconn.getAutoCommit();
            changedAutoCommit               = true;
            transconn.setAutoCommit(false);

            stmt                            = transconn.prepareStatement(selectTierValue);
            addTier                         = transconn.prepareStatement("INSERT INTO tierSummary (location,tier,poured,sold,var,date) VALUES (?,?,?,?,?,?);");
            dayClear                        = transconn.prepareStatement("DELETE FROM tierSummary WHERE date=LEFT(?,11) " + (locationId > 0 ? " AND location = ? " : ""));

            dayClear.setString(1, date);
            if (locationId > 0) {
                dayClear.setInt(2, locationId);
            }
            dayClear.executeUpdate();

            int counter                     = 1;
            stmt.setString(counter++, date);
            if (locationId > 0) {
                stmt.setInt(counter++, locationId);
            }
            stmt.setString(counter++, date);
            if (locationId > 0) {
                stmt.setInt(counter++, locationId);
            }
            rs                              = stmt.executeQuery();
            while (rs.next()) {
                int location                = rs.getInt(1);
                double poured               = rs.getDouble(2);
                double sold                 = rs.getDouble(3);
                double variance             = rs.getDouble(4);
                int category                = rs.getInt(5);
                addTier.setInt(1, location);
                addTier.setInt(2, category);
                addTier.setDouble(3, poured);
                addTier.setDouble(4, sold);
                addTier.setDouble(5, variance);
                addTier.setString(6, date);
                addTier.executeUpdate();
                
                //Adding exclusion when generating tierSummaries
                //logger.debug("location " + location + ", date " + date + ", category " + category + ", poured " + poured + ", sold " + sold + ", variance " + variance);
                if (category > 3 && (poured > 0 || sold > 0)) {
                    //logger.debug("Adding exclusion for " + location + " on " + date);
                    int exclusion           = 3;
                    if (category == 4) {
                        exclusion           = (variance > 0 ? 5 : 6);
                    }
                    for (String table : tableName) {
                        updateTables        = transconn.prepareStatement("UPDATE " + table + " SET exclusion = ? WHERE location = ? AND date = LEFT(?,11);");
                        updateTables.setInt(1, exclusion);
                        updateTables.setInt(2, location);
                        updateTables.setString(3, date);
                        updateTables.executeUpdate();
                    }
                        
                    updateTables            = transconn.prepareStatement("DELETE FROM exclusionSummary WHERE location = ? AND date = LEFT(?,11);");
                    updateTables.setInt(1, location);
                    updateTables.setString(2, date);
                    updateTables.executeUpdate();

                    barStmt                 = transconn.prepareStatement("SELECT id FROM bar WHERE location = ?;");
                    barStmt.setInt(1, location);
                    barRS                   = barStmt.executeQuery();
                    while (barRS.next()) {
                        updateTables        = transconn.prepareStatement("INSERT INTO exclusionSummary (location, bar, exclusion, date) VALUES (?, ?, ?, LEFT(?,11));");
                        updateTables.setInt(1,location);
                        updateTables.setInt(2, barRS.getInt(1));
                        updateTables.setInt(3, exclusion);
                        updateTables.setString(4, date);
                        updateTables.executeUpdate();
                    }
                }
            }
            transconn.commit();
            logger.debug("Summaries Committed");
        } catch (SQLException sqle) {
            try {
                transconn.rollback();
            } catch (Exception e) {
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
            close(stmt);
            close(rs);
        }
    }

    private void addExclusion(Element toHandle, Element toAppend) throws HandlerException {
        
        String selectBars                   = " SELECT id FROM bar WHERE location = ? ";
        String selectExclusionSummary       = " SELECT bar, location, date FROM exclusionSummary WHERE id = ? ";
        String addExclusions                = " INSERT INTO exclusionSummary (location,bar,exclusion,date) " +
                                            " VALUES (?,?,?,?)";
        String deleteExclusions             = " DELETE FROM exclusionSummary WHERE id = ?";
        String[] retailSummaryTables        = {"poured","sold","preOpenHours","preOpenHoursSold","openHours","openHoursSold","afterHours","afterHoursSold"};
        String[] concessionSummaryTables    = {"eventPreOpenHours","eventPreOpenHoursSold","eventOpenHours","eventOpenHoursSold","eventAfterHours","eventAfterHoursSold"};
        PreparedStatement stmt              = null;
        ResultSet rs                        = null;
        ResultSet innerRS                   = null;

        try {
            boolean concessions             = HandlerUtils.getOptionalBoolean(toHandle, "concessions");
            Iterator add                    = toHandle.elementIterator("add");
            while (add.hasNext()) {
                Element addExclusion       = (Element) add.next();

                int locationId              = HandlerUtils.getRequiredInteger(addExclusion, "locationId");
                int barId                   = HandlerUtils.getOptionalInteger(addExclusion, "barId");
                int exclusion               = HandlerUtils.getRequiredInteger(addExclusion, "exclusion");
                String date                 = HandlerUtils.getRequiredString(addExclusion, "date");
                if (barId > 0) {
                    selectBars              += " AND id = ?";
                }
                stmt                        = transconn.prepareStatement(selectBars);
                stmt.setInt(1, locationId);
                if (barId > 0) {
                    stmt.setInt(2, barId);
                }
                rs                          = stmt.executeQuery();
                while (rs.next()) {
                    barId                   = rs.getInt(1);
                    logger.debug("Adding manual exclusion for bar# " + barId + " for loc# " + locationId + " on d: " + date);

                    int colCount = 1;
                    stmt                    = transconn.prepareStatement(addExclusions);
                    stmt.setInt(colCount++, locationId);
                    stmt.setInt(colCount++, barId);
                    stmt.setInt(colCount++, exclusion);
                    stmt.setString(colCount++, date);
                    stmt.executeUpdate();

                    if (concessions) {
                        stmt                = transconn.prepareStatement(" SELECT id FROM station WHERE bar = ? ");
                        stmt.setInt(1, barId);
                        innerRS             = stmt.executeQuery();
                        while (innerRS.next()) {
                            updateExclusions(concessionSummaryTables, exclusion, innerRS.getInt(1), date, " AND station = ? ");
                        }
                    } else {
                        updateExclusions(retailSummaryTables, exclusion, locationId, date, " AND location = ? ");
                    }
                }
            }

            Iterator del = toHandle.elementIterator("delete");
            while (del.hasNext()) {
                Element deleteExclusion     = (Element) del.next();
                int exclusionId             = HandlerUtils.getRequiredInteger(deleteExclusion, "exclusionId");

                stmt                        = transconn.prepareStatement(selectExclusionSummary);
                stmt.setInt(1, exclusionId);
                rs                          = stmt.executeQuery();
                if (rs.next()) {
                    int barId               = rs.getInt(1);
                    int locationId          = rs.getInt(2);
                    String date             = rs.getString(3);
                    
                    stmt                    = transconn.prepareStatement(deleteExclusions);
                    stmt.setInt(1, exclusionId);
                    stmt.executeUpdate();

                    if (concessions) {
                        stmt                = transconn.prepareStatement(" SELECT id FROM station WHERE bar = ? ");
                        stmt.setInt(1,barId);
                        innerRS                 = stmt.executeQuery();
                        while (innerRS.next()) {
                            updateExclusions(concessionSummaryTables, 0, innerRS.getInt(1), date, " AND station = ? ");
                        }
                    } else {
                        updateExclusions(retailSummaryTables, 0, locationId, date, " AND location = ? ");
                    }
                }
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
            close(innerRS);
            close(rs);
        }
    }

    private void updateExclusions(String[] summaryTables, int exclusion, int parameter, String date, String condition) throws HandlerException {

        PreparedStatement stmt              = null;
        try
        {
            for (int i=0; i<summaryTables.length; i++)
            {
                String updateSummaries      = " UPDATE " + summaryTables[i] + "Summary SET exclusion = ? WHERE date = ? " + condition;
                int colCount                = 1;
                stmt                        = transconn.prepareStatement(updateSummaries);
                stmt.setInt(colCount++, exclusion);
                stmt.setString(colCount++, date);
                stmt.setInt(colCount++, parameter);
                stmt.executeUpdate();
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
        }
    }
/*
    private HashMap<Integer, Double> getProjectionMap(int locationId, int projection, String date, Iterator sums) throws HandlerException {
        DecimalFormat twoPlaces             = new DecimalFormat("0.00");
        HashMap<Integer, Double> projectedValue
                                            = new HashMap<Integer, Double>();
        projectedValue.put(0, 0.0);
        int colCount                        = 1;
        int dayCount                        = 0;

        String query                        = "";
        PreparedStatement stmt              = null;
        ResultSet rs                        = null;
        double poured                       = 0;
        try {
            switch(projection){
                case 1:
                    HashMap<Integer, Double> prodVariance
                                                = new HashMap<Integer, Double>();
                    query                       = "SELECT line.product, ROUND(((IFNULL(sold.val, 0) - IFNULL(poured.val, 0))/IFNULL(poured.val, 1)),2), IFNULL(sold.val, 0) FROM " +
                                                " (SELECT DISTINCT l.product FROM lineSummary lS LEFT JOIN line l ON l.id = lS.line LEFT JOIN bar b ON b.id = l.bar " +
                                                " WHERE b.location = ? AND lS.date = ? ORDER BY l.product) AS line " +
                                                " LEFT JOIN " +
                                                " (SELECT product, SUM(value) val FROM openHoursSummary WHERE location = ? AND date BETWEEN SUBDATE(?, INTERVAL 29 DAY) AND SUBDATE(?, INTERVAL 1 DAY)" +
                                                " AND DAYOFWEEK(date) = DAYOFWEEK(?) GROUP BY location, product) AS poured ON poured.product = line.product " +
                                                " LEFT JOIN " +
                                                " (SELECT product, SUM(value) val FROM openHoursSoldSummary WHERE location = ? AND date BETWEEN SUBDATE(?, INTERVAL 29 DAY) AND SUBDATE(?, INTERVAL 1 DAY) " +
                                                " AND DAYOFWEEK(date) = DAYOFWEEK(?) GROUP BY location, product) AS sold ON sold.product = line.product;";

                    stmt                        = transconn.prepareStatement(query);
                    stmt.setInt(colCount++, locationId);
                    stmt.setString(colCount++, date);
                    stmt.setInt(colCount++, locationId);
                    stmt.setString(colCount++, date);
                    stmt.setString(colCount++, date);
                    stmt.setString(colCount++, date);
                    stmt.setInt(colCount++, locationId);
                    stmt.setString(colCount++, date);
                    stmt.setString(colCount++, date);
                    stmt.setString(colCount++, date);
                    rs                          = stmt.executeQuery();
                    while (rs.next()) {
                        if (rs.getDouble(3) > 0) {
                            prodVariance.put(rs.getInt(1), rs.getDouble(2));
                        }
                    }

                    while (sums.hasNext()) {
                        double avgProductVariance
                                            = 1.0;
                        Element summary     = (Element) sums.next();
                        int productId       = HandlerUtils.getRequiredInteger(summary, "productId");
                        double sold         = HandlerUtils.getRequiredDouble(summary, "ounces");
                        if (prodVariance.containsKey(productId)) {
                            avgProductVariance
                                            = prodVariance.get(productId) + 1;
                            double value    = sold/avgProductVariance;
                            poured          = poured + value;
                            projectedValue.put(productId, value);
                            //logger.debug("Adding summary: " + date + " P#" + productId + ", " + twoPlaces.format(sold) + " Oz" + ", " + twoPlaces.format(avgProductVariance) + " %" + ", " + twoPlaces.format(value) + " oz");
                        }
                    }
                    projectedValue.put(0, poured);
                    break;
                case 2:
                    HashMap<Integer, Double> prodShare
                                                = new HashMap<Integer, Double>();
                    String averagePouredQuery   = " SELECT SUM(value) val FROM bevSyncSummary WHERE location = ? " +
                                                " AND date BETWEEN SUBDATE(?, INTERVAL 28 DAY) AND SUBDATE(?, INTERVAL 1 DAY) " +
                                                " GROUP BY location, date;";
                    double totalPoured          = 0.0;
                    double averagePour          = 0.0;
                    colCount                    = 1;
                    stmt                        = transconn.prepareStatement(averagePouredQuery);
                    stmt.setInt(colCount++, locationId);
                    stmt.setString(colCount++, date);
                    stmt.setString(colCount++, date);
                    //stmt.setString(colCount++, date);
                    rs                          = stmt.executeQuery();
                    while (rs.next()) {
                        dayCount++;
                        totalPoured += rs.getDouble(1);
                    }
                    averagePour                 = totalPoured/dayCount;

                    String averageShareQuery    = " SELECT product, SUM(share)/100 share FROM bevSyncSummary WHERE location = ? " +
                                                " AND date BETWEEN SUBDATE(?, INTERVAL 28 DAY) AND SUBDATE(?, INTERVAL 1 DAY) " +
                                                " GROUP BY location, product;";
                    colCount                    = 1;
                    stmt                        = transconn.prepareStatement(averageShareQuery);
                    stmt.setInt(colCount++, locationId);
                    stmt.setString(colCount++, date);
                    stmt.setString(colCount++, date);
                    //stmt.setString(colCount++, date);
                    rs                          = stmt.executeQuery();
                    while (rs.next()) {
                        prodShare.put(rs.getInt(1), averagePour * (rs.getDouble(2)/dayCount));
                    }

                    while (sums.hasNext()) {
                        Element summary     = (Element) sums.next();
                        int productId       = HandlerUtils.getRequiredInteger(summary, "productId");
                        double value        = 0.0;
                        if (prodShare.containsKey(productId)) {
                            value           = prodShare.get(productId);
                        }
                        poured              = poured + value;
                        projectedValue.put(productId, value);
                        //logger.debug("Adding summary: " + date + " P#" + productId + ", " + twoPlaces.format(value) + " oz");
                    }
                    projectedValue.put(0, poured);
                    break;
                case 3:
                    DataSet observedData    = new DataSet();
                    HashMap<Integer, Double> missingProducts
                                            = new HashMap<Integer, Double>();
                    int productId           = 0;
                    dayCount                = -1;
                    double value            = 0.0;
                    double share            = 0.0;
                    double totalMissingShare= 0.0;
                    int dayDiff             = -1;
                    query                   = " SELECT line.product, IFNULL(poured.value,0), DATEDIFF(?, poured.date)/1 Diff, poured.share FROM " +
                                            " (SELECT DISTINCT l.product FROM lineSummary lS LEFT JOIN line l ON l.id = lS.line LEFT JOIN bar b ON b.id = l.bar " +
                                            " WHERE b.location = ? AND lS.date = ? ORDER BY l.product) AS line " +
                                            " LEFT JOIN " +
                                            " (SELECT product, value, share, date FROM bevSyncSummary WHERE location = ? AND " +
                                            " date BETWEEN SUBDATE(?, INTERVAL 28 DAY) AND SUBDATE(?, INTERVAL 1 DAY) " +
                                            " GROUP BY product, date) AS poured ON poured.product = line.product " +
                                            " WHERE poured.value > 0 ORDER BY line.product, Diff DESC; ";

                    stmt                    = transconn.prepareStatement(query);
                    stmt.setString(colCount++, date);
                    stmt.setInt(colCount++, locationId);
                    stmt.setString(colCount++, date);
                    stmt.setInt(colCount++, locationId);
                    stmt.setString(colCount++, date);
                    stmt.setString(colCount++, date);
                    //stmt.setString(colCount++, date);
                    rs                      = stmt.executeQuery();
                    while (rs.next()) {
                        logger.debug("Forecasting for P# " + rs.getInt(1) + " dd# " + rs.getInt(3) + " vol. " + rs.getDouble(2));
                        if (productId != rs.getInt(1) && productId > 0) {
                            DataSet requiredDataPoints  = getForecast(observedData, dayCount);

                            //  Required (future) data point
                            Iterator i      = requiredDataPoints.iterator();
                            while (i.hasNext()) {
                                DataPoint newdp
                                            = (DataPoint) i.next();
                                value       = newdp.getDependentValue();
                                poured      = poured + value;
                                projectedValue.put(productId, value);
                                //logger.debug("Adding summary: " + date + " P#" + productId + ", " + twoPlaces.format(value) + " oz, total Poured: " + twoPlaces.format(poured));
                            }
                            if (dayCount == dayDiff) {
                                missingProducts.put(productId, share);
                                totalMissingShare
                                            += share;
                            }
                            
                            dayCount        = -1;
                            productId       = rs.getInt(1);
                            dayDiff         = rs.getInt(3);
                            share           = rs.getDouble(4);
                            DataPoint dp    = new Observation(rs.getDouble(2));
                            dp.setIndependentValue("dayOfYear", dayDiff);
                            observedData.add(dp);
                            if (dayCount < dayDiff) {
                                dayCount    = dayDiff;
                            }
                        } else {

                            productId       = rs.getInt(1);
                            if (dayDiff - rs.getInt(3) > 1) {
                                for (int i = dayDiff - 1; i > rs.getInt(3); i--) {
                                    DataPoint dp    = new Observation(0.0);
                                    dp.setIndependentValue("dayOfYear", i);
                                    observedData.add(dp);
                                    //logger.debug("Filling gap for p# " + productId + " for period: " + i);
                                }
                            }

                            dayDiff         = rs.getInt(3);
                            share           = rs.getDouble(4);
                            DataPoint dp    = new Observation(rs.getDouble(2));
                            dp.setIndependentValue("dayOfYear", dayDiff);
                            observedData.add(dp);
                            if (dayCount < dayDiff) {
                                dayCount    = dayDiff;
                            }
                        }
                    }
                    if (dayCount >= 1) {
                        DataSet requiredDataPoints  = getForecast(observedData, dayCount);

                        Iterator i      = requiredDataPoints.iterator();
                        while (i.hasNext()) {
                            DataPoint newdp
                                        = (DataPoint) i.next();
                            value       = newdp.getDependentValue();
                            poured      = poured + value;
                            projectedValue.put(productId, value);
                        }
                        //logger.debug("Adding summary: " + date + " P#" + productId + ", " + twoPlaces.format(value) + " oz, total Poured: " + twoPlaces.format(poured));
                    }
                    

                    if (missingProducts.size() > 0) {
                        logger.debug("Size of missing products: " + missingProducts.size());
                        double forecastedShare
                                        = 100 - totalMissingShare;
                        for (Integer product : missingProducts.keySet()) {
                            logger.debug("Projection volume for missing product# " + product);
                            double productShare
                                        = missingProducts.get(product);
                            double productVolume
                                        = (poured/forecastedShare)*productShare;
                            projectedValue.put(product, productVolume);
                        }
                    }

                    projectedValue.put(0, poured);
                    break;
                default:
                    break;
            }
        }catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
            close(rs);
        }
        return projectedValue;
    }

    private DataSet getForecast(DataSet observedData, int dayCount) {

        DataSet requiredDataPoints          = new DataSet();

        if (dayCount > 1) {
            // Obtain a good forecasting model given this data set
            observedData.setPeriodsPerYear(dayCount);
            ForecastingModel forecaster     = DoubleExponentialSmoothingModel.getBestFitModel(observedData);
            forecaster.init(observedData);

            // Create Additional data points for which forecast values are required
            DataPoint dp                    = new Observation(0.0);
            dp.setIndependentValue("dayOfYear", dayCount);
            requiredDataPoints.add(dp);

            // Use the given forecasting model to forecast values for the required (future) data point
            forecaster.forecast(requiredDataPoints);
        } 
        return requiredDataPoints;
    }
*/
    /**
     * Deativating all alerts from the night before
     */
    private void addSuspensions(Element toHandle, Element toAppend) throws HandlerException {

        int locationId                      = HandlerUtils.getRequiredInteger(toHandle, "locationId");

        String selectSuspensions            = "SELECT id FROM locationDetails WHERE suspended = 1 AND location = ?; ";
        String suspendLocation              = "UPDATE locationDetails SET suspended = 1 WHERE location = ?";


        PreparedStatement stmt              = null;
        ResultSet rs                        = null;

        try {
            stmt                            = transconn.prepareStatement(selectSuspensions);
            stmt.setInt(1, locationId);
            rs                              = stmt.executeQuery();
            if (!rs.next()) {
                stmt                        = transconn.prepareStatement(suspendLocation);
                stmt.setInt(1, locationId);
                stmt.executeUpdate();

                String insertSuspensionLogs = "INSERT INTO suspensionLogs (location, startDate, endDate, description, active) VALUES (?,?,?,?,1);";
                String startDate            = HandlerUtils.getRequiredString(toHandle, "startDate");
                String endDate              = HandlerUtils.getRequiredString(toHandle, "endDate");
                String description          = HandlerUtils.getRequiredString(toHandle, "description");

                stmt                        = transconn.prepareStatement(insertSuspensionLogs);
                stmt.setInt(1, locationId);
                stmt.setString(2, startDate);
                stmt.setString(3, endDate);
                stmt.setString(4, description);
                stmt.executeUpdate();
            } else {
                addErrorDetail(toAppend, "Location #" + locationId + " has already been suspended");
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
            close(rs);
        }
    }

    private void addLineSummary(Element toHandle, Element toAppend) throws HandlerException {
        String date                         = HandlerUtils.getRequiredString(toHandle, "date");
        int location                        = HandlerUtils.getOptionalInteger(toHandle, "location");
        int overwrite                       = HandlerUtils.getOptionalInteger(toHandle, "overwrite");
        PreparedStatement stmt              = null;
        PreparedStatement dayClear          = null;
        String startDate                    = "", endDate = "";
        ResultSet rs                        = null, locationRS = null, hoursRS = null;

        String selectLocations              = "SELECT l.id FROM location l LEFT JOIN locationDetails lD ON lD.location = l.id WHERE lD.active = 1 AND lD.pouredUp = 1";
        if (location > 0) {
            selectLocations                 += " AND l.id = ? ";
        }
        String selectOpenHours              = "SELECT DATE_SUB(Concat(LEFT(SUBDATE(?, INTERVAL 1 DAY),11), ' ',IFNULL(x.open,'11:00:00')), INTERVAL eO HOUR) Open, " +
                                            " DATE_SUB(If(x.close>'12:0:0',concat(LEFT(SUBDATE(?, INTERVAL 1 DAY),11), ' ', " +
                                            " IFNULL(x.close,'02:00:00')),concat(LEFT(ADDDATE(SUBDATE(?, INTERVAL 1 DAY),1),11), ' ',IFNULL(x.close,'02:00:00'))), INTERVAL eO HOUR) Close " +
                                            " FROM (Select CASE DAYOFWEEK(SUBDATE(?, INTERVAL 1 DAY)) " +
                                            " WHEN 1 THEN Right(lH.openSun,8) " +
                                            " WHEN 2 THEN Right(lH.openMon,8) " +
                                            " WHEN 3 THEN Right(lH.openTue,8) " +
                                            " WHEN 4 THEN Right(lH.openWed,8) " +
                                            " WHEN 5 THEN Right(lH.openThu,8) " +
                                            " WHEN 6 THEN Right(lH.openFri,8) " +
                                            " WHEN 7 THEN Right(lH.openSat,8) END open, " +
                                            " CASE DAYOFWEEK(SUBDATE(?, INTERVAL 1 DAY)) " +
                                            " WHEN 1 THEN Right(lH.closeSun,8) " +
                                            " WHEN 2 THEN Right(lH.closeMon,8) " +
                                            " WHEN 3 THEN Right(lH.closeTue,8) " +
                                            " WHEN 4 THEN Right(lH.closeWed,8) " +
                                            " WHEN 5 THEN Right(lH.closeThu,8) " +
                                            " WHEN 6 THEN Right(lH.closeFri,8) " +
                                            " WHEN 7 THEN Right(lH.closeSat,8) END close, " +
                                            " l.easternOffset eO " +
                                            " FROM locationHours lH RIGHT JOIN location l ON lH.location=l.id " +
                                            " WHERE l.id=?) AS x; ";

        String insertLineSummary            = " INSERT INTO lineSummary (line, product, date, value, type) VALUES (?,?,?,?,?);";
        String deleteLineSummary            = " DELETE FROM lineSummary WHERE date = SUBDATE(?, INTERVAL 1 DAY) AND line = ?";
        try {
            
            stmt                            = transconn.prepareStatement(selectLocations);
            if (location > 0) {
                stmt.setInt(1, location);
            }
            locationRS                      = stmt.executeQuery();
            while (locationRS.next()) {
                location                    = locationRS.getInt(1);
                stmt                        = transconn.prepareStatement(selectOpenHours);
                stmt.setString(1, date);
                stmt.setString(2, date);
                stmt.setString(3, date);
                stmt.setString(4, date);
                stmt.setString(5, date);
                stmt.setInt(6, location);
                hoursRS                     = stmt.executeQuery();
                if (hoursRS.next()) {
                    startDate               = hoursRS.getString(1);
                    endDate                 = hoursRS.getString(2);
                    StringBuilder lineString= new StringBuilder("0");

                    String selectLines      = "SELECT l.id FROM line l LEFT JOIN bar b ON b.id = l.bar WHERE b.location = ? AND l.lastPoured > ? ";
                    stmt                    = transconn.prepareStatement(selectLines);
                    stmt.setInt(1, location);
                    stmt.setString(2, startDate);
                    rs                      = stmt.executeQuery();
                    while (rs.next()) {
                        lineString.append(", " + rs.getString(1));
                    }
                    
                    String selectLineQuantity           
                                            = " SELECT r.line, l.product, SUM(r.quantity) Qty FROM reading r LEFT JOIN line l ON l.id = r.line " +
                                            " WHERE r.date BETWEEN ? AND ? AND r.type = 0 AND r.line IN (" + lineString.toString() + ") " +
                                            " GROUP BY r.line ORDER BY r.line;";
                    stmt                    = transconn.prepareStatement(selectLineQuantity);
                    stmt.setString(1, startDate);
                    stmt.setString(2, endDate);
                    rs                      = stmt.executeQuery();
                    while (rs.next()) {
                        if (overwrite > 0) {
                            dayClear        = transconn.prepareStatement(deleteLineSummary);
                            dayClear.setString(1, date);
                            dayClear.setInt(2, rs.getInt(1));
                            dayClear.execute();
                            //logger.debug("Clearing lineSummary records for date: '" + date + "'");
                        }
                        
                        double qty          = rs.getDouble(3);
                        stmt                = transconn.prepareStatement(insertLineSummary);
                        stmt.setInt(1, rs.getInt(1));
                        stmt.setInt(2, rs.getInt(2));
                        stmt.setString(3, startDate.substring(0, 10));
                        stmt.setDouble(4, qty);
                        stmt.setInt(5, getVolumeType(qty));
                        stmt.executeUpdate();
                    }
                }
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(dayClear);
            close(stmt);
            close(rs);
            close(locationRS);
            close(hoursRS);
        }
    }

    private void addHourlySummary(Element toHandle, Element toAppend) throws HandlerException {
        
        int overwrite                       = HandlerUtils.getOptionalInteger(toHandle, "overwrite");
        int locationId                      = HandlerUtils.getOptionalInteger(toHandle, "locationId");
        int barId                           = HandlerUtils.getOptionalInteger(toHandle, "barId");
        int hourId                          = HandlerUtils.getOptionalInteger(toHandle, "hourId");
        String period                          = HandlerUtils.getOptionalString(toHandle, "period");

        PreparedStatement stmt              = null;
        ResultSet rs                        = null;

        String insertHourlySummary          = " INSERT INTO hourlySummary (location, bar, product, period, poured, sold,hour) VALUES (?,?,?,?,?,?,?);";

        try {
            stmt                            = transconn.prepareStatement("DELETE FROM hourlySummary WHERE location = ? AND bar = ? AND period = ?; ");
            if (overwrite > 0) {
                stmt.setInt(1,locationId);
                stmt.setInt(2,barId);
                stmt.setString(3,period);
                stmt.execute();
            }

            Iterator sums                   = toHandle.elementIterator("summary");
            while (sums.hasNext()) {
                Element summary             = (Element) sums.next();
                int productId               = HandlerUtils.getRequiredInteger(summary, "productId");
                double poured               = HandlerUtils.getRequiredDouble(summary, "poured");
                double sold                 = HandlerUtils.getRequiredDouble(summary, "sold");

                stmt                        = transconn.prepareStatement(insertHourlySummary);
                stmt.setInt(1,locationId);
                stmt.setInt(2,barId);
                stmt.setInt(3,productId);
                stmt.setString(4,period);
                stmt.setDouble(5, poured);
                stmt.setDouble(6, sold);
                stmt.setInt(7,hourId);
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

    private void addUnClaimedReadingData(Element toHandle, Element toAppend) throws HandlerException {
        
        String date                         = HandlerUtils.getRequiredString(toHandle, "date");
        int overwrite                       = HandlerUtils.getOptionalInteger(toHandle, "overwrite");
        String startDate                    = "", endDate = "";
        Double easternOffset                = 0.0;
        PreparedStatement stmt              = null;
        ResultSet rs                        = null, locationRS = null;
        LineString ls                       = null;

        String selectLocations              = "SELECT l.id FROM location l LEFT JOIN locationDetails lD ON lD.location = l.id WHERE lD.active = 1";
        String selectOpenHours              = "SELECT DATE_SUB(Concat(LEFT(SUBDATE(?, INTERVAL 1 DAY),11), ' ',IFNULL(x.open,'11:00:00')), INTERVAL eO HOUR) Open, " +
                                            " DATE_SUB(If(x.close>'12:0:0',concat(LEFT(SUBDATE(?, INTERVAL 1 DAY),11), ' ', " +
                                            " IFNULL(x.close,'02:00:00')),concat(LEFT(ADDDATE(SUBDATE(?, INTERVAL 1 DAY),1),11), ' ',IFNULL(x.close,'02:00:00'))), INTERVAL eO HOUR) Close, eO " +
                                            " FROM (Select CASE DAYOFWEEK(SUBDATE(?, INTERVAL 1 DAY)) " +
                                            " WHEN 1 THEN Right(lH.openSun,8) " +
                                            " WHEN 2 THEN Right(lH.openMon,8) " +
                                            " WHEN 3 THEN Right(lH.openTue,8) " +
                                            " WHEN 4 THEN Right(lH.openWed,8) " +
                                            " WHEN 5 THEN Right(lH.openThu,8) " +
                                            " WHEN 6 THEN Right(lH.openFri,8) " +
                                            " WHEN 7 THEN Right(lH.openSat,8) END open, " +
                                            " CASE DAYOFWEEK(SUBDATE(?, INTERVAL 1 DAY)) " +
                                            " WHEN 1 THEN Right(lH.closeSun,8) " +
                                            " WHEN 2 THEN Right(lH.closeMon,8) " +
                                            " WHEN 3 THEN Right(lH.closeTue,8) " +
                                            " WHEN 4 THEN Right(lH.closeWed,8) " +
                                            " WHEN 5 THEN Right(lH.closeThu,8) " +
                                            " WHEN 6 THEN Right(lH.closeFri,8) " +
                                            " WHEN 7 THEN Right(lH.closeSat,8) END close, " +
                                            " l.easternOffset eO " +
                                            " FROM locationHours lH RIGHT JOIN location l ON lH.location=l.id " +
                                            " WHERE l.id=?) AS x; ";

        try {
            stmt                            = transconn.prepareStatement(selectLocations);
            rs                              = stmt.executeQuery();
            while (rs.next()) {
                int location                = rs.getInt(1);
                stmt                        = transconn.prepareStatement(selectOpenHours);
                stmt.setString(1, date);
                stmt.setString(2, date);
                stmt.setString(3, date);
                stmt.setString(4, date);
                stmt.setString(5, date);
                stmt.setInt(6, location);
                locationRS                  = stmt.executeQuery();
                if (locationRS.next()) {
                    startDate               = locationRS.getString(1);
                    endDate                 = locationRS.getString(2);
                    easternOffset           = locationRS.getDouble(3);

                    stmt                    = transconn.prepareStatement("DELETE FROM unclaimedReadingData WHERE location = ? AND date BETWEEN ? AND ?;");
                    if (overwrite > 0) {
                        stmt.setInt(1,location);
                        stmt.setString(2,startDate);
                        stmt.setString(3,endDate);
                        stmt.executeUpdate();
                    }
                    ReportPeriod period     = null;
                    try {
                        Date start          = newDateFormat.parse(startDate);
                        Date end            = newDateFormat.parse(endDate);
                        period              = new ReportPeriod(PeriodType.DAILY, "7", start, end);
                    } catch (IllegalArgumentException e) {
                        throw new HandlerException(e.getMessage());
                    } catch (ParseException e) {
                        throw new HandlerException(e.getMessage());
                    }

                    ls                      = new LineString(transconn, 2, location, 0, 0, period, "");
                    String insertReadings   = "INSERT INTO unclaimedReadingData (type, location, product, productName, poured, sold, date, identifier) ( " +
                                            " SELECT * FROM " +
                                            " (SELECT 0 Type, ?, p.id ProdID, p.name Product, ROUND(SUM(r.quantity), 2) pQTY, 0 sQTY, " +
                                            " ADDDATE(r.date, INTERVAL ? HOUR) Date, r.id ID FROM reading r " +
                                            " LEFT JOIN line l on l.id = r.line LEFT JOIN product p on p.id = l.product " +
                                            " WHERE r.line IN (" + ls.getLineString() + ") " +
                                            " AND r.date BETWEEN ? AND ? AND r.type = 0 AND r.quantity > 1 GROUP BY r.date, p.id " +
                                            " UNION " +
                                            " SELECT 1 Type, ?, p.id ProdID, p.name Product, 0 pQTY, ROUND(SUM(i.ounces * s.quantity), 2) sQTY, " +
                                            " ADDDATE(s.date, INTERVAL ? HOUR) Date, s.sid ID FROM sales s " +
                                            " LEFT JOIN beverage b ON b.plu = s.pluNumber AND s.location = b.location " +
                                            " LEFT JOIN ingredient i ON i.beverage = b.id LEFT JOIN product p ON p.id = i.product " +
                                            " WHERE s.date BETWEEN ? AND ? AND s.location = ? GROUP BY s.date, p.id " +
                                            " ORDER BY Product, Date) as d);";

                    stmt                    = transconn.prepareStatement(insertReadings);
                    stmt.setInt(1, location);
                    stmt.setDouble(2, easternOffset);
                    stmt.setString(3, startDate);
                    stmt.setString(4, endDate);
                    stmt.setInt(5, location);
                    stmt.setDouble(6, easternOffset);
                    stmt.setString(7, startDate);
                    stmt.setString(8, endDate);
                    stmt.setInt(9, location);
                    stmt.executeUpdate();
                }
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
            close(rs);
            close(locationRS);
        }
    }

    private int minuteDiff(String startDate, String endDate) throws HandlerException {
        int minutes                         = 0;
        java.util.Date start                = null, end = null;
        try {
            //logger.debug("Start: " + startDate +  ", End: " + endDate);
            start                           = newDateFormat.parse(startDate);
            end                             = newDateFormat.parse(endDate);
            minutes                         = (int)(end.getTime() - start.getTime()) / (60 * 1000);
        } catch (ParseException pe) {
            String badDate                  = (null == start) ? "start" : "end";
            throw new HandlerException("Could not parse " + badDate + " date.");
        }
        return minutes;
    }

    private void adjustUnclaimedReading(Element toHandle, Element toAppend) throws HandlerException {

        String date                         = HandlerUtils.getRequiredString(toHandle, "date");
        int locationId                      = HandlerUtils.getOptionalInteger(toHandle, "locationId");
        String startDate                    = "", endDate = "";
        Double easternOffset                = 0.0;
        PreparedStatement stmt              = null;
        ResultSet rs                        = null, locationRS = null, innerRS = null, outerRS = null;
        HashMap<Integer, Integer> productBrewery
                                            = new HashMap<Integer, Integer>();

        String selectLocations              = "SELECT l.id FROM location l LEFT JOIN locationDetails lD ON lD.location = l.id WHERE lD.active = 1 AND lD.pouredUp = 1" +
                                            (locationId > 0 ? " AND l.id = ?;" : ";");
        String selectOpenHours              = "SELECT DATE_SUB(CONCAT(LEFT(SUBDATE(?, INTERVAL 1 DAY),11), ' ',IFNULL(x.open,'11:00:00')), INTERVAL eO HOUR) Open, " +
                                            " DATE_SUB(If(x.close>'12:0:0', CONCAT(LEFT(SUBDATE(?, INTERVAL 1 DAY),11), ' ', " +
                                            " IFNULL(x.close,'02:00:00')), CONCAT(LEFT(ADDDATE(SUBDATE(?, INTERVAL 1 DAY),1),11), ' ', IFNULL(x.close,'02:00:00'))), INTERVAL eO HOUR) Close, eO " +
                                            " FROM (Select CASE DAYOFWEEK(SUBDATE(?, INTERVAL 1 DAY)) " +
                                            " WHEN 1 THEN Right(lH.openSun,8) " +
                                            " WHEN 2 THEN Right(lH.openMon,8) " +
                                            " WHEN 3 THEN Right(lH.openTue,8) " +
                                            " WHEN 4 THEN Right(lH.openWed,8) " +
                                            " WHEN 5 THEN Right(lH.openThu,8) " +
                                            " WHEN 6 THEN Right(lH.openFri,8) " +
                                            " WHEN 7 THEN Right(lH.openSat,8) END open, " +
                                            " CASE DAYOFWEEK(SUBDATE(?, INTERVAL 1 DAY)) " +
                                            " WHEN 1 THEN Right(lH.closeSun,8) " +
                                            " WHEN 2 THEN Right(lH.closeMon,8) " +
                                            " WHEN 3 THEN Right(lH.closeTue,8) " +
                                            " WHEN 4 THEN Right(lH.closeWed,8) " +
                                            " WHEN 5 THEN Right(lH.closeThu,8) " +
                                            " WHEN 6 THEN Right(lH.closeFri,8) " +
                                            " WHEN 7 THEN Right(lH.closeSat,8) END close, " +
                                            " l.easternOffset eO " +
                                            " FROM locationHours lH RIGHT JOIN location l ON lH.location=l.id " +
                                            " WHERE l.id=?) AS x; ";
        String selectDirectPouredUnclaimed  = "SELECT product, productName, poured, date, id FROM unclaimedReadingData " +
                                            " WHERE location = ? AND date BETWEEN ? AND ? AND type = 0 ORDER BY product, date, type;";
        String selectDirectSoldUnclaimed    = "SELECT sold, date, id FROM unclaimedReadingData WHERE location = ? AND product = ? " +
                                            " AND date BETWEEN SUBDATE(?, INTERVAL 20 MINUTE) AND SUBDATE(?, INTERVAL - 20 MINUTE) " +
                                            " AND sold BETWEEN (? - 4) AND (? + 4) AND type = 1 AND directMatch = 0 ORDER BY date, type;";
        String selectUnclaimedReading       = "SELECT type, product, productName, poured, sold, date, id FROM unclaimedReadingData " +
                                            " WHERE location = ? AND date BETWEEN ? AND ? AND directMatch = 0 AND (poured > 0 OR sold > 0) ORDER BY product, date, type;";
        String insertUnclaimed              = "INSERT INTO tempUnclaimedReading (location, date, product, quantity, type, identifier) VALUES (?, ?, ?, ?, ?, ?);";
        
        try {
            stmt                            = transconn.prepareStatement(selectLocations);
            if (locationId > 0) {
                stmt.setInt(1, locationId);
            }
            outerRS                         = stmt.executeQuery();
            while (outerRS.next()) {
                int location                = outerRS.getInt(1);
                stmt                        = transconn.prepareStatement(selectOpenHours);
                stmt.setString(1, date);
                stmt.setString(2, date);
                stmt.setString(3, date);
                stmt.setString(4, date);
                stmt.setString(5, date);
                stmt.setInt(6, location);
                locationRS                  = stmt.executeQuery();
                if (locationRS.next()) {
                    startDate               = locationRS.getString(1);
                    endDate                 = locationRS.getString(2);
                    easternOffset           = locationRS.getDouble(3);
                }

                stmt                        = transconn.prepareStatement("UPDATE unclaimedReadingData SET color = 0, loss = 0, directMatch = 0 WHERE location = ? AND " +
                                            " date BETWEEN ? AND ?;");
                stmt.setDouble(1, location);
                stmt.setString(2, startDate);
                stmt.setString(3, endDate);
                stmt.executeUpdate();

                stmt                            = transconn.prepareStatement(selectDirectPouredUnclaimed);
                stmt.setDouble(1, location);
                stmt.setString(2, startDate);
                stmt.setString(3, endDate);
                rs                              = stmt.executeQuery();
                while (rs.next()) {
                    int productId               = rs.getInt(1);
                    String productName          = rs.getString(2);
                    Double pouredQty            = rs.getDouble(3);
                    String pouredDate           = rs.getString(4);
                    int pouredID                = rs.getInt(5);
                    
                    stmt                        = transconn.prepareStatement(selectDirectSoldUnclaimed);
                    stmt.setInt(1, location);
                    stmt.setInt(2, productId);
                    stmt.setString(3, pouredDate);
                    stmt.setString(4, pouredDate);
                    stmt.setDouble(5, pouredQty);
                    stmt.setDouble(6, pouredQty);
                    innerRS                     = stmt.executeQuery();
                    if (innerRS.next()) {
                        Double soldQty          = innerRS.getDouble(1);
                        String soldDate         = innerRS.getString(2);
                        int soldID              = innerRS.getInt(3);

                        stmt                    = transconn.prepareStatement("UPDATE unclaimedReadingData SET directMatch = ? WHERE id = ?;");
                        stmt.setInt(1, soldID);
                        stmt.setInt(2, pouredID);
                        stmt.executeUpdate();

                        stmt                    = transconn.prepareStatement("UPDATE unclaimedReadingData SET directMatch = ? WHERE id = ?;");
                        stmt.setInt(1, pouredID);
                        stmt.setInt(2, soldID);
                        stmt.executeUpdate();
                    }
                }
                
                int currentProduct              = 0, currentIdentifier = 0;
                Double poured                   = 0.00, sold = 0.00, diff = 0.00;
                String lastReportDate           = "";
                HashMap<Integer, Double> pourMap= new HashMap<Integer, Double>();
                HashMap<Integer, Double> soldMap= new HashMap<Integer, Double>();
                HashMap<Integer, String> product= new HashMap<Integer, String>();
                HashMap<Integer, String> Message= new HashMap<Integer, String>();
                int reportId                    = 1;

                stmt                            = transconn.prepareStatement(selectUnclaimedReading);
                stmt.setInt(1, location);
                stmt.setString(2, startDate);
                stmt.setString(3, endDate);
                rs                              = stmt.executeQuery();
                while (rs.next()) {
                    int type                    = rs.getInt(1);
                    int productId               = rs.getInt(2);
                    String productName          = rs.getString(3);
                    Double pouredQty            = rs.getDouble(4);
                    Double soldQty              = rs.getDouble(5);
                    String reportDate           = rs.getString(6);
                    int identifier              = rs.getInt(7);

                    product.put(productId, productName);
                    //logger.debug("Entry: " + type + ", " + productId + ", " + productName + ", " + pouredQty + ", " + soldQty + ", " + reportDate + ", " + identifier);

                    if (currentProduct > 0 && (currentProduct != productId)) {
                        for (Integer prod : pourMap.keySet()) {
                            if (pourMap.get(prod) > 2) {
                                //Message.put(reportId++, "Un-Claimed Beer: " + String.format("%.2f", pourMap.get(prod)) + " Oz. of " + product.get(prod) + " at " + lastReportDate);
                                stmt            = transconn.prepareStatement(insertUnclaimed);
                                stmt.setInt(1, location);
                                stmt.setString(2, lastReportDate);
                                stmt.setInt(3, prod);
                                stmt.setDouble(4, pourMap.get(prod));
                                stmt.setInt(5, 0);
                                stmt.setInt(6, currentIdentifier);
                                stmt.executeUpdate();
                            }
                        }
                        pourMap.clear();
                        for (Integer prod : soldMap.keySet()) {
                            if (soldMap.get(prod) > 2) {
                                //Message.put(reportId++, "Un-Claimed Beer: " + String.format("%.2f", pourMap.get(prod)) + " Oz. of " + product.get(prod) + " at " + lastReportDate);
                                stmt            = transconn.prepareStatement(insertUnclaimed);
                                stmt.setInt(1, location);
                                stmt.setString(2, lastReportDate);
                                stmt.setInt(3, prod);
                                stmt.setDouble(4, soldMap.get(prod));
                                stmt.setInt(5, 1);
                                stmt.setInt(6, currentIdentifier);
                                stmt.executeUpdate();
                            }
                        }
                        soldMap.clear();
                        poured                  = 0.00;
                        sold                    = 0.00;
                        diff                    = 0.00;
                    }

                    if (type == 0) {
                        if (soldMap.containsKey(productId)) {
                            if (minuteDiff(lastReportDate, reportDate) < 10) {
                                poured          = pouredQty;
                                sold            = soldMap.get(productId);
                                diff            = poured - sold;
                                if (diff >= 4) {
                                    //Message.put(reportId++, "Over-Poured Beer: " + String.format("%.2f", diff) + " Oz. of " + product.get(productId) + " at " + lastReportDate);
                                    pourMap.put(productId, poured - sold);
                                    //logger.debug("Adding Pour " + (poured - sold) + "Oz for: " + product.get(productId));
                                    currentProduct
                                                = productId;
                                    sold        = 0.0;
                                    soldMap.remove(productId);
                                    //logger.debug("Clearing Sold for: " + product.get(productId));
                                } else if (diff < -4) {
                                    soldMap.put(productId, sold - poured);
                                    //logger.debug("Adding Sold " + (sold - poured) + "Oz for: " + product.get(productId));
                                    currentProduct
                                                = productId;
                                    poured      = 0.0;
                                    pourMap.remove(productId);
                                    //logger.debug("Clearing Poured for: " + product.get(productId));
                                } else {
                                    currentProduct
                                                = 0;
                                    sold        = 0.0;
                                    soldMap.remove(productId);
                                    //logger.debug("Clearing Sold for: " + product.get(productId));
                                    poured      = 0.0;
                                    pourMap.remove(productId);
                                    //logger.debug("Clearing Poured for: " + product.get(productId));
                                }
                            } else {
                                for (Integer prod : soldMap.keySet()) {
                                    if (soldMap.get(prod) > 2) {
                                        //Message.put(reportId++, "Un-Claimed Beer: " + String.format("%.2f", pourMap.get(prod)) + " Oz. of " + product.get(prod) + " at " + lastReportDate);
                                        stmt    = transconn.prepareStatement(insertUnclaimed);
                                        stmt.setInt(1, location);
                                        stmt.setString(2, lastReportDate);
                                        stmt.setInt(3, prod);
                                        stmt.setDouble(4, soldMap.get(prod));
                                        stmt.setInt(5, 1);
                                        stmt.setInt(6, currentIdentifier);
                                        stmt.executeUpdate();
                                    }
                                }
                                soldMap.clear();
                                //logger.debug("Wipedout Sold Map - 1");
                                currentProduct  = productId;
                                poured          = pouredQty;
                                pourMap.put(currentProduct, poured);
                                //logger.debug("Adding Pour " + poured + "Oz for: " + product.get(productId));
                            }
                        } else {
                            if (currentProduct == 0) {
                                currentProduct  = productId;
                                poured          = pouredQty;
                                pourMap.put(currentProduct, poured);
                                //logger.debug("Adding Pour " + poured + "Oz for: " + product.get(productId));
                            } else if (pourMap.containsKey(productId)) {
                                poured          = pourMap.get(productId);
                                if (minuteDiff(lastReportDate, reportDate) < 10 && pouredQty <= 5) {
                                    poured      += pouredQty;
                                    pourMap.put(currentProduct, poured);
                                    //logger.debug("Adding Pour " + poured + "Oz for: " + product.get(productId));
                                } else {
                                    for (Integer prod : pourMap.keySet()) {
                                        if (pourMap.get(prod) > 2) {
                                            //Message.put(reportId++, "Un-Claimed Beer: " + String.format("%.2f", pourMap.get(prod)) + " Oz. of " + product.get(prod) + " at " + lastReportDate);
                                            stmt= transconn.prepareStatement(insertUnclaimed);
                                            stmt.setInt(1, location);
                                            stmt.setString(2, lastReportDate);
                                            stmt.setInt(3, prod);
                                            stmt.setDouble(4, pourMap.get(prod));
                                            stmt.setInt(5, 0);
                                            stmt.setInt(6, currentIdentifier);
                                            stmt.executeUpdate();
                                        }
                                    }
                                    pourMap.clear();
                                    currentProduct
                                                = productId;
                                    poured      = pouredQty;
                                    pourMap.put(currentProduct, poured);
                                    //logger.debug("Adding Pour " + poured + "Oz for: " + product.get(productId));
                                }
                            } else {
                                if (minuteDiff(lastReportDate, reportDate) > 10) {
                                    for (Integer prod : pourMap.keySet()) {
                                        if (pourMap.get(prod) > 2) {
                                            //Message.put(reportId++, "Un-Claimed Beer: " + String.format("%.2f", pourMap.get(prod)) + " Oz. of " + product.get(prod) + " at " + lastReportDate);
                                            stmt= transconn.prepareStatement(insertUnclaimed);
                                            stmt.setInt(1, location);
                                            stmt.setString(2, lastReportDate);
                                            stmt.setInt(3, prod);
                                            stmt.setDouble(4, pourMap.get(prod));
                                            stmt.setInt(5, 0);
                                            stmt.setInt(6, currentIdentifier);
                                            stmt.executeUpdate();
                                        }
                                    }
                                    pourMap.clear();
                                }
                                currentProduct  = productId;
                                poured          = pouredQty;
                                pourMap.put(currentProduct, poured);
                                //logger.debug("Adding Pour " + poured + "Oz for: " + product.get(productId));
                            }
                        }
                    } else {
                        if (pourMap.containsKey(productId)) {
                            if (minuteDiff(lastReportDate, reportDate) < 10) {
                                sold            = soldQty;
                                poured          = pourMap.get(productId);
                                diff            = poured - sold;
                                if (diff >= 4) {
                                    //Message.put(reportId++, "Over-Poured Beer: " + String.format("%.2f", diff) + " Oz. of " + product.get(productId) + " at " + lastReportDate);
                                    pourMap.put(productId, poured - sold);
                                    //logger.debug("Adding Pour " + (poured - sold) + "Oz for: " + product.get(productId));
                                    currentProduct
                                                = productId;
                                    sold        = 0.0;
                                    soldMap.remove(productId);
                                    //logger.debug("Clearing Sold for: " + product.get(productId));
                                } else if (diff < -4) {
                                    soldMap.put(productId, sold - poured);
                                    //logger.debug("Adding Sold " + (sold - poured) + "Oz for: " + product.get(productId));
                                    currentProduct
                                                = productId;
                                    poured      = 0.0;
                                    pourMap.remove(productId);
                                    //logger.debug("Clearing Poured for: " + product.get(productId));
                                } else {
                                    currentProduct
                                                = productId;
                                    sold        = 0.0;
                                    soldMap.remove(productId);
                                    //logger.debug("Clearing Sold for: " + product.get(productId));
                                    poured      = 0.0;
                                    pourMap.remove(productId);
                                    //logger.debug("Clearing Poured for: " + product.get(productId));
                                }
                            } else {
                                for (Integer prod : pourMap.keySet()) {
                                    if (pourMap.get(prod) > 2) {
                                        //Message.put(reportId++, "Un-Claimed Beer: " + String.format("%.2f", pourMap.get(prod)) + " Oz. of " + product.get(prod) + " at " + lastReportDate);
                                        stmt    = transconn.prepareStatement(insertUnclaimed);
                                        stmt.setInt(1, location);
                                        stmt.setString(2, lastReportDate);
                                        stmt.setInt(3, prod);
                                        stmt.setDouble(4, pourMap.get(prod));
                                        stmt.setInt(5, 0);
                                        stmt.setInt(6, currentIdentifier);
                                        stmt.executeUpdate();
                                    }
                                }
                                pourMap.clear();
                                currentProduct  = productId;
                                sold            = soldQty;
                                soldMap.put(currentProduct, sold);
                                //logger.debug("Adding Sold " + sold + "Oz for: " + product.get(productId));
                            }
                        } else {
                            if (currentProduct == 0) {
                                currentProduct  = productId;
                                sold            = soldQty;
                                soldMap.put(currentProduct, sold);
                                //logger.debug("Adding Sold " + sold + "Oz for: " + product.get(productId));
                            } else if (soldMap.containsKey(productId)) {
                                sold            = soldMap.get(productId);
                                if (minuteDiff(lastReportDate, reportDate) < 10 && soldQty <= 5) {
                                    sold        += soldQty;
                                    soldMap.put(currentProduct, sold);
                                    //logger.debug("Adding Sold " + sold + "Oz for: " + product.get(productId));
                                } else {
                                    for (Integer prod : soldMap.keySet()) {
                                        if (soldMap.get(prod) > 2) {
                                            //Message.put(reportId++, "Un-Claimed Beer: " + String.format("%.2f", pourMap.get(prod)) + " Oz. of " + product.get(prod) + " at " + lastReportDate);
                                            stmt= transconn.prepareStatement(insertUnclaimed);
                                            stmt.setInt(1, location);
                                            stmt.setString(2, lastReportDate);
                                            stmt.setInt(3, prod);
                                            stmt.setDouble(4, soldMap.get(prod));
                                            stmt.setInt(5, 1);
                                            stmt.setInt(6, currentIdentifier);
                                            stmt.executeUpdate();
                                        }
                                    }
                                    soldMap.clear();
                                    //logger.debug("Wipedout Sold Map - 2");
                                    currentProduct
                                                = productId;
                                    sold        = soldQty;
                                    soldMap.put(currentProduct, sold);
                                    //logger.debug("Adding Sold " + sold + "Oz for: " + product.get(productId));
                                }
                            } else {
                                if (minuteDiff(lastReportDate, reportDate) > 10) {
                                    for (Integer prod : soldMap.keySet()) {
                                        if (soldMap.get(prod) > 2) {
                                            //Message.put(reportId++, "Un-Claimed Beer: " + String.format("%.2f", pourMap.get(prod)) + " Oz. of " + product.get(prod) + " at " + lastReportDate);
                                            stmt= transconn.prepareStatement(insertUnclaimed);
                                            stmt.setInt(1, location);
                                            stmt.setString(2, lastReportDate);
                                            stmt.setInt(3, prod);
                                            stmt.setDouble(4, soldMap.get(prod));
                                            stmt.setInt(5, 1);
                                            stmt.setInt(6, currentIdentifier);
                                            stmt.executeUpdate();
                                        }
                                    }
                                    soldMap.clear();
                                    //logger.debug("Wipedout Sold Map - 3");
                                }
                                currentProduct  = productId;
                                sold            = soldQty;
                                soldMap.put(currentProduct, sold);
                                //logger.debug("Adding Sold " + sold + "Oz for: " + product.get(productId));
                            }
                        }
                    }
                    lastReportDate              = reportDate;
                    currentIdentifier           = identifier;
                }

                for (Integer prod : pourMap.keySet()) {
                    if (pourMap.get(prod) > 2) {
                        //Message.put(reportId++, "Un-Claimed Beer: " + String.format("%.2f", pourMap.get(prod)) + " Oz. of " + product.get(prod) + " at " + lastReportDate);
                        stmt                    = transconn.prepareStatement(insertUnclaimed);
                        stmt.setInt(1, location);
                        stmt.setString(2, lastReportDate);
                        stmt.setInt(3, prod);
                        stmt.setDouble(4, pourMap.get(prod));
                        stmt.setInt(5, 0);
                        stmt.setInt(6, currentIdentifier);
                        stmt.executeUpdate();
                    }
                }
                pourMap.clear();
                for (Integer prod : soldMap.keySet()) {
                    if (soldMap.get(prod) > 2) {
                        //Message.put(reportId++, "Un-Claimed Beer: " + String.format("%.2f", pourMap.get(prod)) + " Oz. of " + product.get(prod) + " at " + lastReportDate);
                        stmt                    = transconn.prepareStatement(insertUnclaimed);
                        stmt.setInt(1, location);
                        stmt.setString(2, lastReportDate);
                        stmt.setInt(3, prod);
                        stmt.setDouble(4, soldMap.get(prod));
                        stmt.setInt(5, 1);
                        stmt.setInt(6, currentIdentifier);
                        stmt.executeUpdate();
                    }
                }
                soldMap.clear();

                
                String selectUnClaimedPour      = "SELECT id, product, date, quantity FROM tempUnclaimedReading WHERE location = ? AND type = 0 ORDER BY product, date;";
                String selectUnClaimedProdSold  = "SELECT id, quantity FROM tempUnclaimedReading WHERE location = ? AND product = ? AND type = 1 " +
                                                " AND date BETWEEN SUBDATE(?, INTERVAL 120 MINUTE) AND ADDDATE(?, INTERVAL 120 MINUTE) ORDER BY date;";

                stmt                            = transconn.prepareStatement(selectUnClaimedPour);
                stmt.setInt(1, location);
                rs                              = stmt.executeQuery();
                while (rs.next()) {
                    int pouredId                = rs.getInt(1);
                    int pouredProduct           = rs.getInt(2);
                    String pouredDate           = rs.getString(3);
                    Double pouredQty            = rs.getDouble(4);

                    stmt                        = transconn.prepareStatement(selectUnClaimedProdSold);
                    stmt.setInt(1, location);
                    stmt.setInt(2, pouredProduct);
                    stmt.setString(3, pouredDate);
                    stmt.setString(4, pouredDate);
                    innerRS                     = stmt.executeQuery();
                    if (innerRS.next()) {
                        int soldId              = innerRS.getInt(1);
                        Double soldQty          = innerRS.getDouble(2);
                        diff                    = pouredQty - innerRS.getDouble(2);
                        if (diff > 4.00) {
                            stmt                = transconn.prepareStatement("UPDATE tempUnclaimedReading SET quantity = ?, date = date WHERE id = ?;");
                            stmt.setDouble(1, pouredQty - soldQty);
                            stmt.setInt(2, pouredId);
                            stmt.executeUpdate();
                            //logger.debug("Updating Pour " + (pouredQty - soldQty) + " Oz for Product: " + product.get(pouredProduct) + " with poured ID: " + pouredId);

                            stmt                = transconn.prepareStatement("DELETE FROM tempUnclaimedReading WHERE id = ?");
                            stmt.setInt(1, soldId);
                            stmt.executeUpdate();
                            //logger.debug("Clearing Sold for Product: " + product.get(pouredProduct) + " with sold ID: " + soldId);
                        } else if (diff < -4.00) {
                            stmt                = transconn.prepareStatement("UPDATE tempUnclaimedReading SET quantity = ?, date = date WHERE id = ?;");
                            stmt.setDouble(1, soldQty - pouredQty);
                            stmt.setInt(2, soldId);
                            stmt.executeUpdate();
                            //logger.debug("Updating Sold " + (soldQty - pouredQty) + " Oz for Product: " + product.get(pouredProduct) + " with sold ID: " + soldId);

                            stmt                = transconn.prepareStatement("DELETE FROM tempUnclaimedReading WHERE id = ?");
                            stmt.setInt(1, pouredId);
                            stmt.executeUpdate();
                            //logger.debug("Clearing Pour for Product: " + product.get(pouredProduct) + " with poured ID: " + pouredId);
                        } else {
                            stmt                = transconn.prepareStatement("DELETE FROM tempUnclaimedReading WHERE id = ?");
                            stmt.setInt(1, pouredId);
                            stmt.executeUpdate();
                            //logger.debug("Clearing Pour for Product: " + product.get(pouredProduct) + " with poured ID: " + pouredId);

                            stmt                = transconn.prepareStatement("DELETE FROM tempUnclaimedReading WHERE id = ?");
                            stmt.setInt(1, soldId);
                            stmt.executeUpdate();
                            //logger.debug("Clearing Sold for Product: " + product.get(pouredProduct) + " with sold ID: " + soldId);
                        }
                    }
                }

                stmt                            = transconn.prepareStatement(selectUnClaimedPour);
                stmt.setInt(1, location);
                rs                              = stmt.executeQuery();
                while (rs.next()) {
                    int pouredId                = rs.getInt(1);
                    int pouredProduct           = rs.getInt(2);
                    String pouredDate           = rs.getString(3);
                    Double pouredQty            = rs.getDouble(4);

                    stmt                        = transconn.prepareStatement(selectUnClaimedProdSold);
                    stmt.setInt(1, location);
                    stmt.setInt(2, pouredProduct);
                    stmt.setString(3, pouredDate);
                    stmt.setString(4, pouredDate);
                    innerRS                     = stmt.executeQuery();
                    if (innerRS.next()) {
                        int soldId              = innerRS.getInt(1);
                        Double soldQty          = innerRS.getDouble(2);
                        diff                    = pouredQty - innerRS.getDouble(2);
                        if (diff > 4.00) {
                            stmt                = transconn.prepareStatement("UPDATE tempUnclaimedReading SET quantity = ?, date = date WHERE id = ?;");
                            stmt.setDouble(1, pouredQty - innerRS.getDouble(2));
                            stmt.setInt(2, pouredId);
                            stmt.executeUpdate();
                            //logger.debug("Updating Pour " + (pouredQty - soldQty) + " Oz for Product: " + product.get(pouredProduct) + " with poured ID: " + pouredId);

                            stmt                = transconn.prepareStatement("DELETE FROM tempUnclaimedReading WHERE id = ?");
                            stmt.setInt(1, innerRS.getInt(1));
                            stmt.executeUpdate();
                            //logger.debug("Clearing Sold for Product: " + product.get(pouredProduct) + " with sold ID: " + soldId);
                        } else if (diff < -4.00) {
                            stmt                = transconn.prepareStatement("UPDATE tempUnclaimedReading SET quantity = ?, date = date WHERE id = ?;");
                            stmt.setDouble(1, innerRS.getDouble(2) - pouredQty);
                            stmt.setInt(2, innerRS.getInt(1));
                            stmt.executeUpdate();
                            //logger.debug("Updating Sold " + (soldQty - pouredQty) + " Oz for Product: " + product.get(pouredProduct) + " with sold ID: " + soldId);

                            stmt                = transconn.prepareStatement("DELETE FROM tempUnclaimedReading WHERE id = ?");
                            stmt.setInt(1, pouredId);
                            stmt.executeUpdate();
                            //logger.debug("Clearing Pour for Product: " + product.get(pouredProduct) + " with poured ID: " + pouredId);
                        } else {
                            stmt                = transconn.prepareStatement("DELETE FROM tempUnclaimedReading WHERE id = ?");
                            stmt.setInt(1, pouredId);
                            stmt.executeUpdate();
                            //logger.debug("Clearing Pour for Product: " + product.get(pouredProduct) + " with poured ID: " + pouredId);

                            stmt                = transconn.prepareStatement("DELETE FROM tempUnclaimedReading WHERE id = ?");
                            stmt.setInt(1, innerRS.getInt(1));
                            stmt.executeUpdate();
                            //logger.debug("Clearing Sold for Product: " + product.get(pouredProduct) + " with sold ID: " + soldId);
                        }
                    }
                }

                /*
                String selectProductBrewery     = "SELECT pSM.product, pS.id FROM productSet pS LEFT JOIN productSetMap pSM ON pSM.productSet = pS.id WHERE pS.productSetType = 7;";
                String selectUnClaimedBrewPour  = "SELECT tUR.id, tUR.quantity, tUR.product FROM tempUnclaimedReading tUR " +
                                                " WHERE tUR.location = ? AND tUR.date BETWEEN ? AND ? AND tUR.type = 0 ORDER BY tUR.product, tUR.date";
                String selectUnClaimedBrewSold  = "SELECT tUR.id, tUR.quantity, tUR.product FROM tempUnclaimedReading tUR " +
                                                " LEFT JOIN productSetMap pSM ON pSM.product = tUR.product WHERE tUR.location = ? AND tUR.type = 1 AND pSM.productSet = ? " +
                                                " AND tUR.date BETWEEN ? AND ? ORDER BY tUR.date;";

                stmt                            = transconn.prepareStatement(selectProductBrewery);
                rs                              = stmt.executeQuery();
                while (rs.next()) {
                    productBrewery.put(rs.getInt(1), rs.getInt(2));
                }

                stmt                            = transconn.prepareStatement(selectUnClaimedBrewPour);
                stmt.setInt(1, location);
                stmt.setString(2, startDate);
                stmt.setString(3, endDate);
                rs                              = stmt.executeQuery();
                while (rs.next()) {
                    int pouredId                = rs.getInt(1);
                    Double pouredQty            = rs.getDouble(2);
                    int pouredProduct           = rs.getInt(3);
                    int breweryId               = 0;
                    if (productBrewery.containsKey(pouredProduct)) {
                        breweryId               = productBrewery.get(pouredProduct);
                    }

                    int soldId                  = 0;
                    int soldProduct             = 0;
                    Double oldDiff              = 0.0;

                    stmt                        = transconn.prepareStatement(selectUnClaimedBrewSold);
                    stmt.setInt(1, location);
                    stmt.setInt(2, breweryId);
                    stmt.setString(3, startDate);
                    stmt.setString(4, endDate);
                    innerRS                     = stmt.executeQuery();
                    while (innerRS.next()) {
                        diff                    = Math.abs(pouredQty - innerRS.getDouble(2));
                        if (oldDiff == 0.0 || oldDiff > diff) {
                            oldDiff             = diff;
                            soldId              = innerRS.getInt(1);
                            soldProduct         = innerRS.getInt(3);
                        } else if (oldDiff > diff) {
                            oldDiff             = diff;
                            soldId              = innerRS.getInt(1);
                            soldProduct         = innerRS.getInt(3);
                        }
                    }

                    if (soldId > 0) {
                        stmt                    = transconn.prepareStatement("DELETE FROM tempUnclaimedReading WHERE id = ?");
                        stmt.setInt(1, pouredId);
                        stmt.executeUpdate();

                        stmt                    = transconn.prepareStatement("DELETE FROM tempUnclaimedReading WHERE id = ?");
                        stmt.setInt(1, soldId);
                        stmt.executeUpdate();
                        //logger.debug("By Brewery - Linking Product: " + product.get(pouredProduct) + " with Poured ID: " + pouredId + " & Product: " + product.get(soldProduct) + " with Sold ID: " + soldId);
                    }
                }

                String selectUnClaimedSold3     = "SELECT id, quantity, product FROM tempUnclaimedReading WHERE location = ? AND type = 1 ORDER BY date;";
                String selectUnClaimedPour3     = "SELECT id, quantity, product FROM tempUnclaimedReading WHERE location = ? AND type = 0 ORDER BY date;";

                stmt                            = transconn.prepareStatement(selectUnClaimedSold3);
                stmt.setInt(1, location);
                rs                              = stmt.executeQuery();
                while (rs.next()) {
                    int soldId                  = rs.getInt(1);
                    Double soldQty              = rs.getDouble(2);
                    int soldProduct             = rs.getInt(3);

                    int pouredId                = 0;
                    int pouredProduct           = 0;
                    Double oldDiff              = 0.0;

                    stmt                        = transconn.prepareStatement(selectUnClaimedPour3);
                    stmt.setInt(1, location);
                    innerRS                     = stmt.executeQuery();
                    while (innerRS.next()) {
                        diff                    = Math.abs(soldQty - innerRS.getDouble(2));
                        if (oldDiff == 0.0 || oldDiff > diff) {
                            oldDiff             = diff;
                            pouredId            = innerRS.getInt(1);
                            pouredProduct       = innerRS.getInt(3);
                        } else if (oldDiff > diff) {
                            oldDiff             = diff;
                            pouredId            = innerRS.getInt(1);
                            pouredProduct       = innerRS.getInt(3);
                        }
                    }

                    stmt                    = transconn.prepareStatement("DELETE FROM tempUnclaimedReading WHERE id = ?");
                    stmt.setInt(1, pouredId);
                    stmt.executeUpdate();

                    stmt                    = transconn.prepareStatement("DELETE FROM tempUnclaimedReading WHERE id = ?");
                    stmt.setInt(1, soldId);
                    stmt.executeUpdate();

                    //logger.debug("MisKeying - Linking Product: " + product.get(pouredProduct) + " with Poured ID: " + pouredId + " & Product: " + product.get(soldProduct) + " with Sold ID: " + soldId);
                }*/

                
                String selectUnclaimedData  = "SELECT product, type, poured, sold FROM unclaimedReadingData WHERE id = ?";
                String selectPreviousValue  = "SELECT id, poured, sold, loss FROM unclaimedReadingData WHERE directMatch = 0 AND location = ? AND date > ? " +
                                            " AND product = ? AND id < ? ORDER BY id DESC";
                stmt                        = transconn.prepareStatement("SELECT identifier, IF(type = 1, quantity, quantity * -1), date FROM tempUnclaimedReading WHERE location = ? AND date BETWEEN ? AND ?");
                stmt.setInt(1, location);
                stmt.setString(2, startDate);
                stmt.setString(3, endDate);
                innerRS                     = stmt.executeQuery();
                while (innerRS.next()) {
                    int id                  = innerRS.getInt(1);
                    double loss              = innerRS.getDouble(2);

                    
                    stmt                    = transconn.prepareStatement("UPDATE unclaimedReadingData SET color = 1, loss = ? WHERE id = ?");
                    stmt.setDouble(1, loss);
                    stmt.setInt(2, id);
                    stmt.executeUpdate();

                    stmt                    = transconn.prepareStatement(selectUnclaimedData);
                    stmt.setInt(1, id);
                    rs                      = stmt.executeQuery();
                    if (rs.next()) {
                        int prod            = rs.getInt(1);
                        int type            = rs.getInt(2);
                        double p            = rs.getDouble(3);
                        double s            = rs.getDouble(4);
                        //logger.debug("Product: " + prod + ", Poured: " + p + ", Sold: " + s + ", Loss: " + loss + ", Type: " + type);
                        switch (type) {
                            case 0:
                                if (loss > 0) {
                                    double outstandingLoss
                                            = p + loss;
                                    //logger.debug("Starting outstandingLoss: " + outstandingLoss);
                                    stmt    = transconn.prepareStatement(selectPreviousValue);
                                    stmt.setInt(1, location);
                                    stmt.setString(2, startDate);
                                    stmt.setInt(3, prod);
                                    stmt.setInt(4, id);
                                    rs      = stmt.executeQuery();
                                    while (rs.next()) {
                                        if (outstandingLoss <= 1) {
                                            break;
                                        }
                                        if (rs.getDouble(4) != 0) {
                                            break;
                                        } else if (rs.getDouble(2) != 0) {
                                            stmt
                                            = transconn.prepareStatement("UPDATE unclaimedReadingData SET color = 1, loss = ? WHERE id = ?");
                                            stmt.setDouble(1, 0);

                                            stmt.setInt(2, rs.getInt(1));
                                            stmt.executeUpdate();
                                            outstandingLoss
                                            = outstandingLoss + rs.getDouble(2);
                                        } else if (rs.getDouble(3) != 0) {
                                            stmt
                                            = transconn.prepareStatement("UPDATE unclaimedReadingData SET color = 1, loss = ? WHERE id = ?");
                                            stmt.setDouble(1, 0);
                                            stmt.setInt(2, rs.getInt(1));
                                            stmt.executeUpdate();
                                            outstandingLoss
                                            = outstandingLoss - rs.getDouble(3);
                                        }
                                        //logger.debug("Current Outstanding: " + outstandingLoss);
                                    }
                                } else {
                                    if ((loss + p) != 0) {
                                        double outstandingLoss
                                            = p + loss;
                                        //logger.debug("Starting outstandingLoss: " + outstandingLoss);
                                        stmt= transconn.prepareStatement(selectPreviousValue);
                                        stmt.setInt(1, location);
                                        stmt.setString(2, startDate);
                                        stmt.setInt(3, prod);
                                        stmt.setInt(4, id);
                                        rs  = stmt.executeQuery();
                                         while (rs.next()) {
                                            if (outstandingLoss >= -2) {
                                                break;
                                            }
                                            if (rs.getDouble(4) != 0) {
                                                break;
                                            } else if (rs.getDouble(2) != 0) {
                                                stmt
                                            = transconn.prepareStatement("UPDATE unclaimedReadingData SET color = 1, loss = ? WHERE id = ?");
                                                stmt.setDouble(1, 0);
                                                stmt.setInt(2, rs.getInt(1));
                                                stmt.executeUpdate();
                                                outstandingLoss
                                            = outstandingLoss + rs.getDouble(2);
                                            } else if (rs.getDouble(3) != 0) {
                                                stmt
                                            = transconn.prepareStatement("UPDATE unclaimedReadingData SET color = 1, loss = ? WHERE id = ?");
                                                stmt.setDouble(1, 0);
                                                stmt.setInt(2, rs.getInt(1));
                                                stmt.executeUpdate();
                                                outstandingLoss
                                            = outstandingLoss - rs.getDouble(3);
                                            }
                                            //logger.debug("Current Outstanding: " + outstandingLoss);
                                        }
                                    }
                                }
                                break;
                            case 1:
                                if (loss < 0) {
                                    double outstandingLoss
                                            = s - loss;
                                    //logger.debug("Starting outstandingLoss: " + outstandingLoss);
                                    stmt    = transconn.prepareStatement(selectPreviousValue);
                                    stmt.setInt(1, location);
                                    stmt.setString(2, startDate);
                                    stmt.setInt(3, prod);
                                    stmt.setInt(4, id);
                                    rs      = stmt.executeQuery();
                                    while (rs.next()) {
                                        if (outstandingLoss >= -2) {
                                            break;
                                        }
                                        if (rs.getDouble(4) != 0) {
                                            break;
                                        } else if (rs.getDouble(2) != 0) {
                                            stmt
                                            = transconn.prepareStatement("UPDATE unclaimedReadingData SET color = 1, loss = ? WHERE id = ?");
                                            stmt.setDouble(1, 0);
                                            stmt.setInt(2, rs.getInt(1));
                                            stmt.executeUpdate();
                                            outstandingLoss
                                            = outstandingLoss - rs.getDouble(2);
                                        } else if (rs.getDouble(3) != 0) {
                                            stmt
                                            = transconn.prepareStatement("UPDATE unclaimedReadingData SET color = 1, loss = ? WHERE id = ?");
                                            stmt.setDouble(1, 0);
                                            stmt.setInt(2, rs.getInt(1));
                                            stmt.executeUpdate();
                                            outstandingLoss
                                            = outstandingLoss + rs.getDouble(3);
                                        }
                                        //logger.debug("Current Outstanding: " + outstandingLoss);
                                    }
                                } else {
                                    if ((loss - s) != 0) {
                                        double outstandingLoss
                                            = loss - s;
                                        //logger.debug("Starting outstandingLoss: " + outstandingLoss);
                                        stmt= transconn.prepareStatement(selectPreviousValue);
                                        stmt.setInt(1, location);
                                        stmt.setString(2, startDate);
                                        stmt.setInt(3, prod);
                                        stmt.setInt(4, id);
                                        rs  = stmt.executeQuery();
                                        while (rs.next()) {
                                            if (outstandingLoss <= 2) {
                                                break;
                                            }
                                            if (rs.getDouble(4) != 0) {
                                                break;
                                            } else if (rs.getDouble(2) != 0) {
                                                stmt
                                            = transconn.prepareStatement("UPDATE unclaimedReadingData SET color = 1, loss = ? WHERE id = ?");
                                                stmt.setDouble(1, 0);
                                                stmt.setInt(2, rs.getInt(1));
                                                stmt.executeUpdate();
                                                outstandingLoss
                                            = outstandingLoss + rs.getDouble(2);
                                            } else if (rs.getDouble(3) != 0) {
                                                stmt
                                            = transconn.prepareStatement("UPDATE unclaimedReadingData SET color = 1, loss = ? WHERE id = ?");
                                                stmt.setDouble(1, 0);
                                                stmt.setInt(2, rs.getInt(1));
                                                stmt.executeUpdate();
                                                outstandingLoss
                                            = outstandingLoss - rs.getDouble(3);
                                            }
                                            //logger.debug("Current Outstanding: " + outstandingLoss);
                                        }
                                    }
                                }
                                break;
                        }
                    }
                }
                
                stmt                        = transconn.prepareStatement("DELETE FROM tempUnclaimedReading WHERE location = ?");
                stmt.setInt(1, location);
                stmt.executeUpdate();
                /**/
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
            close(rs);
            close(locationRS);
            close(innerRS);
            close(outerRS);
        }
    }

    private int getVolumeType (double qty) {
        if (qty == 0) {
            return 0;
        } else if (qty <= 100) {
            return 1;
        }  else if (qty <= 500) {
            return 2;
        }  else if (qty <= 1000) {
            return 3;
        }  else if (qty <= 2000) {
            return 4;
        }  else if (qty <= 5000) {
            return 5;
        }  else if (qty <= 10000) {
            return 6;
        } else {
            return 7;
        }
    }

    /**  Add a new supplier and first address
     *  <supplierName>"String"
     *  <addrStreet>
     *  <addrCity>
     *  <addrState>
     *  <addrZip>
     *  opt <requestId> The suppler ID for ths address. This request will be marked "added"
     *
     *  returns the database ID of the new supplier
     *  <supplierId>0000    
     */
    private void addSupplier(Element toHandle, Element toAppend) throws HandlerException {
        String name = HandlerUtils.getRequiredString(toHandle, "supplierName");
        String addrStreet = HandlerUtils.getRequiredString(toHandle, "addrStreet");
        String addrCity = HandlerUtils.getRequiredString(toHandle, "addrCity");
        String addrState = HandlerUtils.getRequiredString(toHandle, "addrState");
        String addrZip = HandlerUtils.getRequiredString(toHandle, "addrZip");
        int requestId = HandlerUtils.getOptionalInteger(toHandle, "requestId");
        int callerId = getCallerId(toHandle);

        PreparedStatement stmt = null;
        ResultSet rs = null;

        try {
            stmt = transconn.prepareStatement("INSERT INTO supplier (name) VALUES (?)");
            stmt.setString(1, name);
            stmt.executeUpdate();

            stmt = transconn.prepareStatement("SELECT LAST_INSERT_ID()");
            rs = stmt.executeQuery();
            if (rs.next()) {
                int supplierId = rs.getInt(1);
                if (insertSupplierAddress(supplierId, addrStreet, addrCity, addrState, addrZip, transconn)) {
                    toAppend.addElement("supplierId").addText(String.valueOf(supplierId));
                    String logMessage = "Added a new supplier, " + name + " and loc in " + addrCity + ", " + addrState;
                    logger.portalDetail(callerId, "addSupplier", 0, "supplier", supplierId, logMessage, transconn);
                    logger.debug(logMessage);

                    // if this supplier was associated with a request, mark the request as added
                    if (requestId > 0) {
                        stmt = transconn.prepareStatement("UPDATE supplierRequest SET status='added' WHERE id=?");
                        stmt.setInt(1, requestId);
                        stmt.executeUpdate();
                    }
                } else {
                    addErrorDetail(toAppend, "An error occurred while adding this address");
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

    /** Add an address to an existing supplier 
     *  <supplerId>000
     *  <addrStreet>
     *  <addrCity>
     *  <addrState>
     *  <addrZip>
     *  opt <requestId> The suppler ID for ths address. This request will be marked "added"
     */
    private void addSupplierAddress(Element toHandle, Element toAppend) throws HandlerException {
        int supplier = HandlerUtils.getRequiredInteger(toHandle, "supplierId");
        String addrStreet = HandlerUtils.getRequiredString(toHandle, "addrStreet");
        String addrCity = HandlerUtils.getRequiredString(toHandle, "addrCity");
        String addrState = HandlerUtils.getRequiredString(toHandle, "addrState");
        String addrZip = HandlerUtils.getRequiredString(toHandle, "addrZip");
        int requestId = HandlerUtils.getOptionalInteger(toHandle, "requestId");
        int callerId = getCallerId(toHandle);

        PreparedStatement stmt = null;
        try {
            if (insertSupplierAddress(supplier, addrStreet, addrCity, addrState, addrZip, transconn)) {
                String logMessage = "Added a supplier address in " + addrCity + ", " + addrState;
                logger.portalDetail(callerId, "updateSupplier", 0, "supplier", supplier, logMessage, transconn);
                logger.debug(logMessage);

                // if this supplier was associated with a request, mark the request as added
                if (requestId > 0) {
                    stmt = transconn.prepareStatement("UPDATE supplierRequest SET status='added' WHERE id=?");
                    stmt.setInt(1, requestId);
                    stmt.executeUpdate();
                }
            } else {
                addErrorDetail(toAppend, "An error occurred while adding this address");
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
        }
    }

    /** Companion method to addSupplier and addSupplierAddress
     *  Adds a single supplier address to the database
     *  @param supplier the ID of the supplier of this address
     *  @param street address: street
     *  @param city address: city
     *  @param state address: state (two char)
     *  @param zip address: zipcode
     *  @param transconn database connection
     *  @return true if successful, false otherwise
     */
    private boolean insertSupplierAddress(int supplier, String street, String city, String state, String zip,
            RegisteredConnection transconn) throws SQLException {
        PreparedStatement stmt = transconn.prepareStatement(
                " INSERT INTO supplierAddress (supplier,active,addrStreet,addrCity,addrState,addrZip) " +
                " VALUES (?,1,?,?,?,?) ");
        if (checkForeignKey("supplier", supplier, transconn)) {
            stmt.setInt(1, supplier);
            stmt.setString(2, street);
            stmt.setString(3, city);
            stmt.setString(4, state);
            stmt.setString(5, zip);
            stmt.executeUpdate();
            return true;
        } else {
            return false;
        }
    }

    /**  Assoicate a supplier (and address) with a specific location
     */
    private void addLocationSupplier(Element toHandle, Element toAppend) throws HandlerException {
        int address = HandlerUtils.getRequiredInteger(toHandle, "addressId");
        int location = HandlerUtils.getRequiredInteger(toHandle, "locationId");
        int callerId = getCallerId(toHandle);

        String check = "SELECT id FROM locationSupplier WHERE location=? AND address=?";
        String selectSupplier = "SELECT supplier FROM supplierAddress WHERE id=? ";
        String insert = "INSERT INTO locationSupplier(location,address) VALUES (?,?)";

        String selectSupplierProducts = " SELECT pSM.product, pSM.plu FROM productSetMap pSM LEFT JOIN supplier s ON s.productSet = pSM.productSet WHERE s.id = ? ";
        String selectInv = "SELECT id, product FROM inventory WHERE location = ? ";
        String updateInv = "UPDATE inventory SET plu=?,supplier=? WHERE id=? ";

        HashMap<Integer, String> ProductPLUMap = new HashMap<Integer, String>();

        PreparedStatement stmt = null;
        ResultSet rs = null;
        int supplierId = 0;
        try {
            stmt = transconn.prepareStatement(check);
            stmt.setInt(1, location);
            stmt.setInt(2, address);
            rs = stmt.executeQuery();
            if (!rs.next()) {
                stmt = transconn.prepareStatement(selectSupplier);
                stmt.setInt(1, address);
                rs = stmt.executeQuery();
                if (rs.next()) {
                    supplierId = rs.getInt(1);
                } else {
                    supplierId = 2;
                }

                stmt = transconn.prepareStatement(insert);
                stmt.setInt(1, location);
                stmt.setInt(2, address);
                stmt.executeUpdate();

                stmt = transconn.prepareStatement(selectSupplierProducts);
                stmt.setInt(1, supplierId);
                rs = stmt.executeQuery();
                while (rs.next()) {
                    ProductPLUMap.put(new Integer(rs.getInt(1)), rs.getString(2));
                }

                stmt = transconn.prepareStatement(selectInv);
                stmt.setInt(1, location);
                rs = stmt.executeQuery();
                while (rs.next()) {
                    if (ProductPLUMap.containsKey(rs.getInt(2))) {
                        stmt = transconn.prepareStatement(updateInv);
                        stmt.setString(1, ProductPLUMap.get(rs.getInt(2)));
                        stmt.setInt(2, supplierId);
                        stmt.setInt(3, rs.getInt(1));
                        stmt.executeUpdate();
                    }
                }
                String logMessage = "Added supplier address #" + address + " at L#" + location;
                logger.portalDetail(callerId, "addSupplier", location, logMessage, transconn);
            } else {
                addErrorDetail(toAppend, "That suppler already exists at this location");
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
            close(rs);
        }
    }

    /**  Add a new product to the master product list.
     *  Will also mark as "added" any pending requests to add a product with this name.
     *
     *  <productName>"String"
     *  opt:<quicksell>000

     */
    private void addProduct(Element toHandle, Element toAppend) throws HandlerException {
        //  (int id, String name, int qId)
        String name = HandlerUtils.getRequiredString(toHandle, "productName");
        int callerId = getCallerId(toHandle);
        int qId = HandlerUtils.getOptionalInteger(toHandle, "quicksell");
        if (qId < 0) {
            qId = 0;
        }

        String insert =
                " INSERT INTO product  (name, qId) VALUES (?,?)";
        String lookup =
                " SELECT id FROM product WHERE name=? LIMIT 1";
        String deleteRequest =
                " UPDATE productRequest SET status='added' WHERE productName=? AND status='open'";

        PreparedStatement stmt = null;
        ResultSet rs = null;

        logger.portalAction("addProduct");

        try {
            stmt = transconn.prepareStatement(lookup);
            stmt.setString(1, name);
            rs = stmt.executeQuery();
            if (rs.next()) {
                addErrorDetail(toAppend, "A product named " + name + " already exists");
            } else {
                stmt = transconn.prepareStatement(insert);
                stmt.setString(1, name);
                stmt.setInt(2, qId);
                stmt.executeUpdate();

                // Delete any pending ProductRequests
                stmt = transconn.prepareStatement(deleteRequest);
                stmt.setString(1, name);
                stmt.executeUpdate();

                // Log the action
                stmt = transconn.prepareStatement("SELECT LAST_INSERT_ID()");
                rs = stmt.executeQuery();
                if (rs.next()) {
                    int newId = rs.getInt(1);
                    String logMessage = "Added product " + name;
                    logger.portalDetail(callerId, "addProduct", 0, "product", newId, logMessage, transconn);
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

    /**  Update all fields for one or more inventory records
     *
     */
    private void addUnitCount(Element toHandle, Element toAppend) throws HandlerException {

        int callerId = getCallerId(toHandle);
        int location = HandlerUtils.getRequiredInteger(toHandle, "locationId");
        String eventTime = HandlerUtils.getRequiredString(toHandle, "eventTime");
        int countId = -1;
        String selectUnitCount = "SELECT id FROM unitCount " +
                "WHERE location = ? and zone=? and bar=? and station = ? and product=? " +
                "and date between (? - INTERVAL 1 HOUR) and (? + INTERVAL 1 HOUR)";

        String insertUnitCount = "INSERT INTO unitCount (location, zone, bar, station, product, count, date) " +
                "VALUES (?,?,?,?,?,?,?)";

        String updateUnitCount = "UPDATE unitCount SET count=? WHERE id=?";


        PreparedStatement stmt = null;
        ResultSet rs = null;

        Iterator i = toHandle.elementIterator("unitCount");

        try {
            while (i.hasNext()) {
                Element count = (Element) i.next();

                int zone = HandlerUtils.getOptionalInteger(count, "zoneId");
                int bar = HandlerUtils.getOptionalInteger(count, "barId");
                //NischaySharma_16-Apr-2009_Start: changed the tag from stationID to stationId and
                //productID to productId
                int station = HandlerUtils.getRequiredInteger(count, "stationId");
                int productId = HandlerUtils.getRequiredInteger(count, "productId");
                //NischaySharma_16-Apr-2009_End
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
                    countId = rs.getInt(1);
                    stmt = transconn.prepareStatement(updateUnitCount);
                    stmt.setFloat(1, unitCount);
                    stmt.setInt(2, countId);
                    stmt.executeUpdate();

                    String logMessage = "Updating unit counts for location: " + location + " zone: " + zone + " stand: " + bar + " station: " + station + " on " + eventTime;
                    logger.portalDetail(callerId, "addUnitCount", location, "zone", zone, logMessage, transconn);


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
                    logger.portalDetail(callerId, "addUnitCount", location, "zone", zone, logMessage, transconn);

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

    /**  Update all fields for one or more inventory records
     *
     */
    private void addStandUnitCount(Element toHandle, Element toAppend) throws HandlerException {

        int callerId = getCallerId(toHandle);
        int location = HandlerUtils.getRequiredInteger(toHandle, "locationId");
        String eventTime = HandlerUtils.getRequiredString(toHandle, "eventTime");
        int standCountId = -1;
        String selectStandUnitCount = "SELECT id FROM unitStandCount " +
                "WHERE location = ? and zone=? and bar=? and product=? " +
                "and date between (? - INTERVAL 1 HOUR) and (? + INTERVAL 1 HOUR)";

        String insertStandUnitCount = "INSERT INTO unitStandCount (location, zone, bar, product, count, date) " +
                "VALUES (?,?,?,?,?,?)";

        String updateStandUnitCount = "UPDATE unitStandCount SET count=? WHERE id=? ";

        PreparedStatement stmt = null;
        ResultSet rs = null;

        Iterator i = toHandle.elementIterator("standUnitCount");

        try {
            while (i.hasNext()) {
                Element scount = (Element) i.next();

                int zone = HandlerUtils.getRequiredInteger(scount, "zoneId");
                int bar = HandlerUtils.getRequiredInteger(scount, "barId");
                int productId = HandlerUtils.getRequiredInteger(scount, "productId");
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

                    standCountId = rs.getInt(1);
                    stmt = transconn.prepareStatement(updateStandUnitCount);
                    stmt.setFloat(1, standUnitCount);
                    stmt.setInt(2, standCountId);
                    //NischaySharma_16-Apr-2009_Start: Added executeUpdate statement
                    stmt.executeUpdate();
                    //NischaySharma_16-Apr-2009_End

                    String logMessage = "Updating stand unit counts for standId: " + standCountId + "count: " + standUnitCount + " location: " + location + " zone: " + zone + " stand:" + bar + " on " + eventTime;
                    logger.portalDetail(callerId, "addStandUnitCount", location, "zone", zone, logMessage, transconn);

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
                    logger.portalDetail(callerId, "addStandUnitCount", location, "zone", zone, logMessage, transconn);

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

    /**  Add a request for a new product.  This is intended to be used by non-admins
     *  to ask admins to add a product.
     *
     *  <productName>String
     *  <userId>000
     *  <locationId>000
     *  opt:<comment>String
     *
     */
    private void addProductRequest(Element toHandle, Element toAppend) throws HandlerException {

        String name = HandlerUtils.getRequiredString(toHandle, "productName");
        int userId = HandlerUtils.getRequiredInteger(toHandle, "userId");
        int locationId = HandlerUtils.getRequiredInteger(toHandle, "locationId");
        String comment = HandlerUtils.getOptionalString(toHandle, "comment");

        String check1 =
                " SELECT id FROM productRequest WHERE productName=? AND location=? AND status='open' LIMIT 1";
        String check2 =
                " SELECT id FROM product WHERE name=? LIMIT 1";
        String insert =
                " INSERT INTO productRequest (productName,user,location,comment,date,status) " +
                " VALUES (?,?,?,?,NOW(),'open') ";

        PreparedStatement stmt = null;
        ResultSet rs = null;

        logger.portalAction("addProductRequest by U#" + userId + " for L#" + locationId + ": '" + name + "'");

        try {
            stmt = transconn.prepareStatement(check1);
            stmt.setString(1, name);
            stmt.setInt(2, locationId);
            rs = stmt.executeQuery();
            if (rs.next()) {
                addErrorDetail(toAppend, "There is already a request for that product at this location");
            } else {
                stmt = transconn.prepareStatement(check2);
                stmt.setString(1, name);
                rs = stmt.executeQuery();
                if (rs.next()) {
                    addErrorDetail(toAppend, "That product already exists");
                } else {

                    stmt = transconn.prepareStatement(insert);
                    stmt.setString(1, name);
                    stmt.setInt(2, userId);
                    stmt.setInt(3, locationId);
                    stmt.setString(4, HandlerUtils.nullToEmpty(comment));
                    stmt.executeUpdate();

                    // Log the action
                    stmt = transconn.prepareStatement("SELECT LAST_INSERT_ID()");
                    rs = stmt.executeQuery();
                    if (rs.next()) {
                        int newId = rs.getInt(1);
                        String logMessage = "Added product request " + name;
                        logger.portalDetail(userId, "addProductRequest", locationId, "productRequest", newId, logMessage, transconn);
                    }
                }
            }

        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
        }

    }

    /**  Add a request for a new supplier.  This is intended to be used by non-admins
     *  to ask admins to add a supplier.  There are two types of requests, new supplier    
     *  and existing supplier
     *
     *  new supplier:
     *  <type>"new"
     *  <supplierName>String
     *  existing:
     *  <type>"existing"
     *  <supplerId>0000
     *
     *  Both types must include the address
     *  <addrStreet>
     *  <addrCity>
     *  <addrState>
     *  <addrZip>
     *
     *  and information about the caller
     *  <userId>000
     *  <locationId>000
     *  opt:<comment>String
     *
     */
    private void addSupplierRequest(Element toHandle, Element toAppend) throws HandlerException {
        int callerId = getCallerId(toHandle);
        int location = HandlerUtils.getRequiredInteger(toHandle, "locationId");
        String comment = HandlerUtils.getOptionalString(toHandle, "comment");
        String type = HandlerUtils.getRequiredString(toHandle, "type");

        String supplierName = "";
        int supplierId = 0;
        boolean existing = false;
        if ("new".equals(type)) {
            supplierName = HandlerUtils.getRequiredString(toHandle, "supplierName");
        } else if ("existing".equals(type)) {
            supplierId = HandlerUtils.getRequiredInteger(toHandle, "supplierId");
            existing = true;
        } else {
            throw new HandlerException("Invalid request type");
        }

        String addrStreet = HandlerUtils.getRequiredString(toHandle, "addrStreet");
        String addrCity = HandlerUtils.getRequiredString(toHandle, "addrCity");
        String addrState = HandlerUtils.getRequiredString(toHandle, "addrState");
        String addrZip = HandlerUtils.getRequiredString(toHandle, "addrZip");

        String insertNew = "INSERT INTO supplierRequest " +
                " (type,supplierName,addrStreet,addrCity,addrState,addrZip,user,location,comment,date,status) " +
                " VALUES ('new',?,?,?,?,?,?,?,?,NOW(),'open') ";
        String insertExisting = "INSERT INTO supplierRequest " +
                " (type,supplierId,addrStreet,addrCity,addrState,addrZip,user,location,comment,date,status) " +
                " VALUES ('existing',?,?,?,?,?,?,?,?,NOW(),'open') ";

        PreparedStatement stmt = null;

        try {
            stmt = transconn.prepareStatement(existing ? insertExisting : insertNew);
            int count = 1;
            if (existing) {
                assertForeignKey("supplier", supplierId, transconn);
                stmt.setInt(count++, supplierId);
            } else {
                stmt.setString(count++, supplierName);
            }
            stmt.setString(count++, addrStreet);
            stmt.setString(count++, addrCity);
            stmt.setString(count++, addrState);
            stmt.setString(count++, addrZip);
            stmt.setInt(count++, callerId);
            stmt.setInt(count++, location);
            stmt.setString(count++, comment);

            stmt.executeUpdate();

        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
        }

    }

    /**
     *  @deprecated not ever implemented
     */
    private void addLine(Element toHandle, Element toAppend) throws HandlerException {

        int index = HandlerUtils.getRequiredInteger(toHandle, "Index");
        int productRefId = HandlerUtils.getRequiredInteger(toHandle, "ProductId");
        int systemRefId = HandlerUtils.getRequiredInteger(toHandle, "SystemId");
        int barRefId = HandlerUtils.getRequiredInteger(toHandle, "BarId");
        int ouncesPoured = HandlerUtils.getRequiredInteger(toHandle, "OuncesPoured");
        String status = HandlerUtils.getRequiredString(toHandle, "Status");
        String date = HandlerUtils.getRequiredString(toHandle, "Date");
        String time = HandlerUtils.getRequiredString(toHandle, "Time");

        PreparedStatement stmt = null;
        ResultSet rs = null;

        logger.portalAction("addLine");

        try {
            boolean productCheckForeignKey = checkForeignKey("Products", productRefId, transconn);
            if (productCheckForeignKey) {
                boolean systemCheckForeignKey = checkForeignKey("Systems", systemRefId, transconn);
                if (systemCheckForeignKey) {
                    boolean barCheckForeignKey = checkForeignKey("Bars", barRefId, transconn);
                    if (barCheckForeignKey) {
                        String insert = "INSERT INTO Lines  (index,productRefId,systemRefId,barRefId, ouncesPoured, status,date,time) VALUES (?,?,?,?,?,?,?,?)";
                        stmt = transconn.prepareStatement(insert);
                        stmt.setInt(1, index);
                        stmt.setInt(2, productRefId);
                        stmt.setInt(3, systemRefId);
                        stmt.setInt(4, barRefId);
                        stmt.setInt(5, ouncesPoured);
                        stmt.setString(6, status);
                        stmt.setString(7, date);
                        stmt.setString(8, time);
                        stmt.executeUpdate();
                    } else {
                        logger.generalWarning("Invalid Reference ID Number: Bar : No records found.");
                    }
                } else {
                    logger.generalWarning("Invalid Reference ID Number: System : No records found.");
                }
            } else {
                logger.generalWarning("Invalid Reference ID Number: Product : No records found.");
            }


        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
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
    private void copyBeverage(Element toHandle, Element toAppend) throws HandlerException {

        int location = HandlerUtils.getRequiredInteger(toHandle, "location");
        int bar = HandlerUtils.getOptionalInteger(toHandle, "barId");
        int prodType = HandlerUtils.getRequiredInteger(toHandle, "prodID");
        int callerId = getCallerId(toHandle);

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

        PreparedStatement stmt = null;
        ResultSet rs = null;
        ResultSet ings = null;

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

                // check that the reference plu exists
                stmt = transconn.prepareStatement(checkPlu);
                stmt.setString(1, copyFrom);
                stmt.setInt(2, location);
                rs = stmt.executeQuery();
                int fromId;
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
                int simple = 0;
                String name = "";
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
            logger.dbError("SQL Exception: " + sqle.toString());
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

    /**  Copy every PLU recipe and beverage size from one location to another
     *  The caller ID will be checked for permissions: needs read access for the source
     *  and write access to the destination. This method now includes
     *  an optional "barId"  If this is set, the beverage will be associated with this
     *  bar.  If not, the beverage will have a 'null' bar field. 
     *
     *  Duplicate sizes or plus will NOT overwrite existing records
     */
    private void importBeverages(Element toHandle, Element toAppend) throws HandlerException {
        int fromLoc = HandlerUtils.getRequiredInteger(toHandle, "importFrom");
        int toLoc = HandlerUtils.getRequiredInteger(toHandle, "importTo");
        int bar = HandlerUtils.getOptionalInteger(toHandle, "barId");
        int callerId = getCallerId(toHandle);

        DecimalFormat twoPlaces = new DecimalFormat("0.00");

        String getSizes =
                " SELECT name,ounces FROM beverageSize WHERE location=?";
        String insertSize =
                " INSERT INTO beverageSize(name,ounces,location) VALUES (?,?,?)";
        String getBevs =
                " SELECT id,name,plu,ounces,simple FROM beverage WHERE location=?";
        String getLastId =
                " SELECT LAST_INSERT_ID()";
        String getExistingPlus =
                " SELECT DISTINCT plu FROM beverage WHERE location=?";
        String insertBev = (bar > 0 ? " INSERT INTO beverage (name,plu,ounces,simple,location,bar) VALUES (?,?,?,?,?,?) "
                : " INSERT INTO beverage (name,plu,ounces,simple,location) VALUES (?,?,?,?,?) ");
        String insertIng =
                " INSERT INTO ingredient (beverage,product,ounces) " +
                " SELECT ?,product,ounces FROM ingredient WHERE beverage=?";

        PreparedStatement stmt = null;
        ResultSet rs = null;
        ResultSet bevs = null;
        ResultSet sizes = null;
        PreparedStatement insertBevStmt = null;
        PreparedStatement insertIngStmt = null;
        PreparedStatement lastIdStmt = null;
        PreparedStatement insertSizeStmt = null;

        Set<String> existingPlus = new HashSet<String>();
        Set<String> existingSizeNames = new HashSet<String>();
        Set<String> existingSizeOunces = new HashSet<String>();

        try {
            if (WebPermission.permissionAt(callerId, fromLoc, transconn).canRead() && WebPermission.permissionAt(callerId, toLoc, transconn).canWrite()) {

                logger.debug("Importing Beverages from L#" + fromLoc + " to L#" + toLoc);
                insertSizeStmt = transconn.prepareStatement(insertSize);
                insertBevStmt = transconn.prepareStatement(insertBev);
                insertIngStmt = transconn.prepareStatement(insertIng);
                lastIdStmt = transconn.prepareStatement(getLastId);

                // Get Existing Sizes
                stmt = transconn.prepareStatement(getSizes);
                stmt.setInt(1, toLoc);
                rs = stmt.executeQuery();
                while (rs.next()) {
                    existingSizeNames.add(rs.getString(1).toLowerCase());
                    existingSizeOunces.add(twoPlaces.format(rs.getDouble(2)));
                }
                logger.debug("Checking " + existingSizeNames.size() + " existing sizes");
                //re-using stmt for getSizes
                stmt.setInt(1, fromLoc);
                sizes = stmt.executeQuery();
                int insertCount = 0;
                int failCount = 0;
                int newId = 0;
                // Copy Beverage Sizes
                while (sizes.next()) {
                    String sizeName = sizes.getString(1);
                    double ounces = sizes.getDouble(2);
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
                        insertSizeStmt.executeUpdate();
                        insertCount++;

                        String logMessage = "Adding a beverage size named '" + sizeName + "' " +
                                "(" + twoPlaces.format(ounces) + " oz)";
                        rs = lastIdStmt.executeQuery();
                        if (rs.next()) {
                            newId = rs.getInt(1);
                        }
                        logger.portalDetail(callerId, "addBeverageSize", toLoc, "beverageSize", newId, logMessage, transconn);
                    }
                }
                logger.debug("Added " + insertCount + " sizes OK" + (failCount > 0 ? ", failed: " + failCount : ""));

                // Get Existing PLUS
                stmt = transconn.prepareStatement(getExistingPlus);
                stmt.setInt(1, toLoc);
                rs = stmt.executeQuery();
                while (rs.next()) {
                    existingPlus.add(rs.getString(1));
                }
                logger.debug("Checking " + existingPlus.size() + " existing plus");

                stmt = transconn.prepareStatement(getBevs);
                stmt.setInt(1, fromLoc);
                bevs = stmt.executeQuery();

                insertCount = 0;
                failCount = 0;
                newId = 0;
                // Copy all the beverages
                while (bevs.next()) {
                    //id,name,plu,simple
                    int bevId = bevs.getInt(1);
                    String bevName = bevs.getString(2);
                    String plu = bevs.getString(3);
                    double ounces = bevs.getDouble(4);
                    int simple = bevs.getInt(5);
                    if (!existingPlus.contains(plu)) {
                        insertBevStmt.setString(1, bevName);
                        insertBevStmt.setString(2, plu);
                        insertBevStmt.setDouble(3, ounces);
                        insertBevStmt.setInt(4, simple);
                        insertBevStmt.setInt(5, toLoc);
                        if (bar > 0) {
                            insertBevStmt.setInt(6, bar);
                        }
                        insertBevStmt.executeUpdate();

                        rs = lastIdStmt.executeQuery();
                        if (rs.next()) {
                            newId = rs.getInt(1);
                        }
                        insertIngStmt.setInt(1, newId);
                        insertIngStmt.setInt(2, bevId);
                        insertIngStmt.executeUpdate();
                        insertCount++;

                        String logMessage = "Added plu#" + plu;
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
            logger.dbError("SQL Exception in importBeverages: " + sqle.toString());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
            close(insertBevStmt);
            close(insertIngStmt);
            close(lastIdStmt);
            close(rs);
            close(bevs);
            close(sizes);
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
    private void addBeverage(Element toHandle, Element toAppend) throws HandlerException {

        int location                        = 0;
        int bar                             = HandlerUtils.getOptionalInteger(toHandle, "barId");
        int prodType                        = HandlerUtils.getRequiredInteger(toHandle, "prodID");
        int callerId                        = getCallerId(toHandle);

        String selectLocations              = " SELECT id FROM location WHERE customer = ? ";
        PreparedStatement stmt              = null;
        ResultSet rs                        = null;
        logger.portalAction("addBeverage");
        boolean forCustomer                 = HandlerUtils.getOptionalBoolean(toHandle, "forCustomer");

        try {
            locationMap                     = new LocationMap(transconn);
            if (forCustomer) {
                int customerId              = HandlerUtils.getRequiredInteger(toHandle, "customerId");
                if(callerId > 0 && customerId > 0 && isValidAccessUser(callerId, customerId,true)) {  
                    stmt                    = transconn.prepareStatement("SELECT id FROM customer WHERE id = ? AND groupId = 2");
                    stmt.setInt(1, customerId);
                    rs                      = stmt.executeQuery();
                    if (rs.next()) {
                        selectLocations     = "SELECT id FROM location WHERE customer IN (SELECT id FROM customer WHERE groupId = ?)";
                        customerId          = 2;
                    }   
                    
                    stmt                    = transconn.prepareStatement(selectLocations);
                    stmt.setInt(1, customerId);
                    rs                      = stmt.executeQuery();
                    while (rs.next()) {
                        location            = rs.getInt(1);
                        addBeverageDetail(callerId, location, bar, prodType, toHandle, toAppend);
                    }
                } else {
                    addErrorDetail(toAppend, "Invalid Access"  );
                }
            } else {
                location                    = HandlerUtils.getRequiredInteger(toHandle, "location");
                if(callerId > 0 && location > 0 && isValidAccessUser(callerId, location,false)){
                    addBeverageDetail(callerId, location, bar, prodType, toHandle, toAppend);
                } else {
                    addErrorDetail(toAppend, "Invalid Access"  );
                }
            }
        } catch (SQLException sqle) {
            logger.dbError("SQL Exception: " + sqle.toString());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
            close(rs);
        }
        locationMap = null;
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
    private void addBeverageDetail(int callerId, int location, int bar, int prodType, Element toHandle, Element toAppend) throws HandlerException {

        String checkPlu =
                " SELECT p.name, bar.name FROM beverage bev LEFT JOIN bar ON bev.bar = bar.id LEFT JOIN ingredient i ON i.beverage = bev.id LEFT JOIN product p ON p.id = i.product " +
                " WHERE bev.location=? AND bev.plu=? LIMIT 1";
        String getLastId =
                " SELECT LAST_INSERT_ID()";
        String insertBev = (bar > 0 ? " INSERT INTO beverage (name, location, plu, simple, pType, bar) VALUES (?,?,?,?,?,?) "
                : " INSERT INTO beverage (name, location, plu, simple, pType) VALUES (?,?,?,?,?)");
        String insertIng =
                " INSERT INTO ingredient (beverage, product, ounces) " +
                " VALUES (?,?,?)";
        String updateBeverageSize = "UPDATE beverage SET ounces = ? WHERE id = ? ";
        String beverageSize = " SELECT id FROM beverageSize WHERE location = ? AND ounces = ? ";

        String deleteBeverage = "DELETE FROM beverage WHERE id=?";

        String deleteIngredient = "DELETE FROM ingredient WHERE beverage=?";

        PreparedStatement stmt = null;
        ResultSet rs = null;

        // A cache of product data delay sets (maps product -> datadelay hour)
        HashMap<Integer, Timestamp> dataDelayCache = new HashMap<Integer, Timestamp>();
        java.util.Date now = new java.util.Date();

        Iterator bevs = toHandle.elementIterator("beverage");

        try {
            while (bevs.hasNext()) {
                Element beverage = (Element) bevs.next();
                String plu = HandlerUtils.getRequiredString(beverage, "plu");
                String name = HandlerUtils.getRequiredString(beverage, "name");
                int simple = HandlerUtils.getOptionalInteger(beverage, "simple");
                if (simple < 0) {
                    simple = 1;
                }
                boolean isCustom = HandlerUtils.getOptionalBoolean(beverage, "isCustom");
                String detailMessage;
                //check PLU
                stmt = transconn.prepareStatement(checkPlu);
                stmt.setInt(1, location);
                stmt.setString(2, plu);
                rs = stmt.executeQuery();
                if (rs.next()) {
                    String beverageName = rs.getString(1);
                    String barName = rs.getString(2);
                    detailMessage = "PLU '" + plu + "' already exists for Product '" + beverageName + "' for location: " + locationMap.getLocation(location);
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
                    stmt.setInt(5, prodType);
                    if (bar > 0) {
                        stmt.setInt(6, bar);
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
                    String logMessage = "Added plu#" + plu +" to beverage "+name+ " ";
                    

                    // Adding Ingredients for the beverage
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
                        totalQuantity += quantity;
                        int product = HandlerUtils.getRequiredInteger(ingredient, "product");
                        stmt = transconn.prepareStatement(insertIng);
                        stmt.setInt(1, beverageId);
                        stmt.setInt(2, product);
                        stmt.setDouble(3, quantity);
                        stmt.executeUpdate();
                        dataDelayCache.put(product, toSqlTimestamp(now));
                    }

                    stmt = transconn.prepareStatement(updateBeverageSize);
                    stmt.setDouble(1, totalQuantity);
                    stmt.setInt(2, beverageId);
                    stmt.executeUpdate();
                    logMessage              +=String.valueOf(totalQuantity)+"oz";
                    logger.portalDetail(callerId, "addBeverage", location, "beverage", beverageId, logMessage, transconn);

                    /*stmt = transconn.prepareStatement(beverageSize);
                    stmt.setInt(1, location);
                    stmt.setDouble(2, totalQuantity);
                    rs = stmt.executeQuery();
                    if (!rs.next() && !isCustom) {
                        stmt = transconn.prepareStatement(deleteBeverage);
                        stmt.setInt(1, beverageId);
                        stmt.executeUpdate();

                        stmt = transconn.prepareStatement(deleteIngredient);
                        stmt.setInt(1, beverageId);
                        stmt.executeUpdate();
                        detailMessage = "Beverage: '" + name + "' with PLU: '" + plu + "' for location: " + locationMap.getLocation(location) + " was not added as no matching size of " + totalQuantity + " was found";
                        addErrorDetail(toAppend, detailMessage);
                    }*/
                }
            }
            if (dataDelayCache.size() > 0) {
                setDataMod(location, dataDelayCache);
            }
        } catch (SQLException sqle) {
            logger.dbError("SQL Exception: " + sqle.toString());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
            close(rs);
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
    private void addBeverageSize(Element toHandle, Element toAppend) throws HandlerException {

        int locationId                      = HandlerUtils.getRequiredInteger(toHandle, "locationId");
        int customerId                      = HandlerUtils.getRequiredInteger(toHandle, "customerId");
        int callerId                        = getCallerId(toHandle);
        int paramCount                      = 0;
        int parameter                       = 0;
        
        String getLocations                 = "SELECT id FROM location ";

        if (locationId > 0) {
            paramCount++;
            getLocations                    += " WHERE id = ?";
            parameter                       = locationId;
        } else if (customerId > 0) {
            paramCount++;
            getLocations                    += " WHERE customer = ?";
            parameter                       = customerId;
        }
        if (paramCount != 1) {
            throw new HandlerException("Exactly only one of the following must be set: locationId customerId");
        }
        
        String checkName                    = "SELECT id FROM beverageSize WHERE location=? AND LCASE(name)=?";
        String checkSize                    = "SELECT id FROM beverageSize WHERE location=? AND ounces BETWEEN (?-0.05) AND (?+0.05)";
        String insert                       = "INSERT INTO beverageSize (location,name,ounces) VALUES (?,?,?)";

        PreparedStatement stmt              = null;
        ResultSet rs                        = null, rs2 = null, locationRS = null;

        DecimalFormat twoPlaces             = new DecimalFormat("0.00");

        logger.portalAction("addBeverageSize");
        try {
            Iterator sizes                  = toHandle.elementIterator("size");
            while (sizes.hasNext()) {
                Element sizeEl              = (Element) sizes.next();
                String name                 = HandlerUtils.getRequiredString(sizeEl, "name");
                float ounces                = HandlerUtils.getRequiredFloat(sizeEl, "ounces");

                //check params
                stmt                        = transconn.prepareStatement(getLocations);
                stmt.setInt(1, parameter);
                locationRS                  = stmt.executeQuery();
                while (locationRS.next()) {
                    int location            = locationRS.getInt(1);
                    
                    //check params
                    stmt                    = transconn.prepareStatement(checkName);
                    stmt.setInt(1, location);
                    stmt.setString(2, name.toLowerCase());
                    rs                      = stmt.executeQuery();
                    stmt                    = transconn.prepareStatement(checkSize);
                    stmt.setInt(1, location);
                    stmt.setFloat(2, ounces);
                    stmt.setFloat(3, ounces);
                    rs2                     = stmt.executeQuery();
                    if (rs.next()) {
                        addErrorDetail(toAppend, "The name '" + name + "' already exists at this location.");
                    } else if (rs2.next()) {

                        addErrorDetail(toAppend, "The size '" + ounces + "' already exists at this location.");
                    } else if (ounces < 0.1) {
                        addErrorDetail(toAppend, "The size '" + ounces + "' is not allowed.");
                    } else {
                        // OK to add
                        stmt                = transconn.prepareStatement(insert);
                        stmt.setInt(1, location);
                        stmt.setString(2, name);
                        stmt.setFloat(3, ounces);
                        stmt.executeUpdate();

                        // Log the action
                        stmt                = transconn.prepareStatement("SELECT LAST_INSERT_ID()");
                        rs                  = stmt.executeQuery();
                        if (rs.next()) {
                            int newId       = rs.getInt(1);
                            String logMessage 
                                            = "Adding a beverage size named '" + name + "' " + "(" + twoPlaces.format(ounces) + " oz)";
                            logger.portalDetail(callerId, "addBeverageSize", location, "beverageSize", newId, logMessage, transconn);
                        }
                    }
                }
            }
        } catch (SQLException sqle) {
            logger.dbError("SQL Exception in addBeverageSize: " + sqle.toString());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
            close(rs);
            close(rs2);
            close(locationRS);
        }
    }

    /**  Add a new harpagon system to a location
     *
     *  <locationId>000
     *  <totalLines>1-32
     *  <systemIndex>0-99
     *
     */
    public void addSystem(Element toHandle, Element toAppend) throws HandlerException {
        int locationId                      = HandlerUtils.getRequiredInteger(toHandle, "locationId");
        int zoneId                          = HandlerUtils.getOptionalInteger(toHandle, "zoneId");
        int totalLines                      = HandlerUtils.getRequiredInteger(toHandle, "totalLines");
        int systemIndex                     = HandlerUtils.getRequiredInteger(toHandle, "systemIndex");
        int callerId                        = getCallerId(toHandle);

        String checkSystem                  = " SELECT id FROM system WHERE systemId=? AND location=? ";
        String checkCooler                  = " SELECT id FROM cooler WHERE location=? ";
        String checkProduct                 = " SELECT id FROM inventory WHERE location=? AND product = 4311";
        String getBar                       = " SELECT b.id, l.name FROM location l LEFT JOIN bar b ON l.id = b.location WHERE l.id=? ";
        if (zoneId > 0) {
            getBar                          += " AND b.zone = ? ";
        }

        String insertSystem                 = " INSERT INTO system (location, totalLines, systemId) VALUES (?,?,?) ";
        String insertCooler                 = " INSERT INTO cooler (name, location, system) VALUES (?,?,?) ";
        String insertProduct                = " INSERT INTO inventory (product, location) VALUES (4311,?) ";
        String insertLine                   = " INSERT INTO line (product, lineIndex,system,status,bar,lineNo) VALUES (4311,?,?,?,?,?) ";
        String insertCalibration            = " INSERT INTO calibration (location,system,line,value) VALUES (?,?,?,715) ";

        String getLastId                    = "SELECT LAST_INSERT_ID()";

        PreparedStatement stmt              = null;
        ResultSet rs                        = null;

        logger.portalAction("addSystem");

        try {
            if (checkForeignKey("location", locationId, transconn)) {
                int barId                   = 1;
                String coolerName           = "Cooler 1";

                // get the id# of a bar at ths location
                stmt                        = transconn.prepareStatement(getBar);
                stmt.setInt(1, locationId);
                if (zoneId > 0) {
                    stmt.setInt(2, zoneId);
                }
                rs                          = stmt.executeQuery();
                if (rs.next()) {
                    barId                   = rs.getInt(1);
                    coolerName              = rs.getString(2);
                } else {
                    throw new HandlerException("No bars exist at this location.");
                }

                // check that the system # doesn't exist at this location
                stmt                        = transconn.prepareStatement(checkSystem);
                stmt.setInt(1, systemIndex);
                stmt.setInt(2, locationId);
                rs                          = stmt.executeQuery();

                if (!rs.next()) {

                    // check that a cooler doesn't exist at this location
                    stmt                    = transconn.prepareStatement(checkCooler);
                    stmt.setInt(1, locationId);
                    rs                      = stmt.executeQuery();
                    if (!rs.next()) {
                        // add the cooler record
                        stmt                = transconn.prepareStatement(insertCooler);
                        stmt.setString(1, coolerName);
                        stmt.setInt(2, locationId);
                        stmt.setInt(3, systemIndex);
                        stmt.executeUpdate();
                    }
                    stmt                    = transconn.prepareStatement(checkProduct);
                    stmt.setInt(1, locationId);
                    rs                      = stmt.executeQuery();
                    if (!rs.next()) {
                        stmt                = transconn.prepareStatement(insertProduct);
                        stmt.setInt(1, locationId);
                        stmt.executeUpdate();
                    }

                    // add the system record
                    stmt                    = transconn.prepareStatement(insertSystem);
                    stmt.setInt(1, locationId);
                    stmt.setInt(2, totalLines);
                    stmt.setInt(3, systemIndex);
                    stmt.executeUpdate();

                    // get the id of the system we added
                    stmt                    = transconn.prepareStatement(getLastId);
                    rs                      = stmt.executeQuery();
                    int systemDbId;
                    if (rs.next()) {
                        systemDbId          = rs.getInt(1);
                    } else {
                        logger.dbError("SQL Last_Insert_Id FAILED in addSystem for " + "L: " + locationId + " #:" + totalLines + " I:" + systemIndex);
                        throw new HandlerException("database error");
                    }

                    String logMessage       = "Adding a " + totalLines + " line System " + systemIndex;
                    logger.portalDetail(callerId, "addSystem", locationId, "system", systemDbId, logMessage, transconn);


                    // add line records for the new system
                    for (int i = 0; i < totalLines; i++) {
                        int lineNo          = (i+1)+(systemIndex*40);
                        stmt                = transconn.prepareStatement(insertLine);
                        stmt.setInt(1, i);
                        stmt.setInt(2, systemDbId);
                        stmt.setString(3, "RUNNING"); //default state of the new line
                        stmt.setInt(4, barId);
                        stmt.setString(5,String.valueOf(lineNo));
                        stmt.executeUpdate();

                        stmt                = transconn.prepareStatement(insertCalibration);
                        stmt.setInt(1, locationId);
                        stmt.setInt(2, systemDbId);
                        stmt.setInt(3, i);
                        stmt.executeUpdate();
                    }

                } else {
                    addErrorDetail(toAppend, "System ID " + systemIndex + " already exists at this location");
                    logger.generalWarning("Sys Index " + systemIndex + " already exists for Loc#" + locationId);
                }
            } else {
                logger.generalWarning("Invalid Reference ID Number: Location " + locationId + ": No records found.");
            }

        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
            close(rs);
        }
    }

    private void addGroups(Element toHandle, Element toAppend) throws HandlerException {
        int groupType = HandlerUtils.getRequiredInteger(toHandle, "groupType");
        String groupName = HandlerUtils.getRequiredString(toHandle, "groupName");
        int callerId = getCallerId(toHandle);

        String insertGroup = "INSERT INTO groups (name, type) VALUES (?, ?)";
        String checkName = " SELECT id FROM groups WHERE type=? AND LCASE(name)=LCASE(?)";

        PreparedStatement stmt = null;
        ResultSet rs = null;

        try {
            boolean groupTypeCheckForeignKey = checkForeignKey("groupType", groupType, transconn);
            if (groupTypeCheckForeignKey) {
                stmt = transconn.prepareStatement(checkName);
                stmt.setInt(1, groupType);
                stmt.setString(2, groupName);
                rs = stmt.executeQuery();

                if (!rs.next()) {
                    stmt = transconn.prepareStatement(insertGroup);
                    int paramIndex = 1;
                    stmt.setString(paramIndex++, groupName);
                    stmt.setInt(paramIndex++, groupType);
                    stmt.executeUpdate();
                } else {
                    addErrorDetail(toAppend, "A group with that name already exists");
                }
            } else {
                addErrorDetail(toAppend, "Invalid Group Type ID");
                logger.generalWarning("Invalid Reference ID Number: Group Type : No records found.");
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
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
    private void addGroupRegion(Element toHandle, Element toAppend) throws HandlerException {

        Iterator i = toHandle.elementIterator("groupRegionMaster");
        int callerId = getCallerId(toHandle);

        String selectGroupRegion = " SELECT gRM.id FROM groupRegionMap gRM WHERE gRM.groups = ? AND gRM.regionMaster = ? ";
        String insertGroupRegion = " INSERT INTO groupRegionMap (groups, regionMaster, threshold) VALUES (?,?,?) ";

        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            while (i.hasNext()) {
                Element grou = (Element) i.next();
                int masterRegionId = HandlerUtils.getRequiredInteger(grou, "masterRegionId");
                int groupId = HandlerUtils.getRequiredInteger(grou, "groupId");
                double threshold = HandlerUtils.getRequiredDouble(grou, "threshold");
                stmt = transconn.prepareStatement(selectGroupRegion);
                stmt.setInt(1, groupId);
                stmt.setInt(2, masterRegionId);
                rs = stmt.executeQuery();
                if (!rs.next()) {
                    stmt = transconn.prepareStatement(insertGroupRegion);
                    stmt.setInt(1, groupId);
                    stmt.setInt(2, masterRegionId);
                    stmt.setDouble(3, threshold);
                    stmt.executeUpdate();
                } else {
                    logger.debug("Group Region already exists");
                }
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error in addGroupRegion: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
        }
    }

    private void addProductSet(Element toHandle, Element toAppend) throws HandlerException {

        String productSetName = HandlerUtils.getRequiredString(toHandle, "productSetName");
        int productSetType = HandlerUtils.getRequiredInteger(toHandle, "productSetType");

        Iterator i = toHandle.elementIterator("product");
        int callerId = getCallerId(toHandle);

        String selectProductSet = " SELECT id FROM productSet WHERE name = ? AND productSetType = ? ";
        String insertProductSet = " INSERT INTO productSet (name, productSetType) VALUES (?,?) ";
        String getLastId = " SELECT LAST_INSERT_ID()";
        String insertProductSetProductMap = " INSERT INTO productSetMap (productSet, product, plu) VALUES (?,?,?) ";

        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            stmt = transconn.prepareStatement(selectProductSet);
            stmt.setString(1, productSetName);
            stmt.setInt(2, productSetType);
            rs = stmt.executeQuery();
            if (!rs.next()) {
                stmt = transconn.prepareStatement(insertProductSet);
                stmt.setString(1, productSetName);
                stmt.setInt(2, productSetType);
                stmt.executeUpdate();

                int productSetId = -1;
                stmt = transconn.prepareStatement(getLastId);
                rs = stmt.executeQuery();
                if (rs.next()) {
                    productSetId = rs.getInt(1);
                } else {
                    logger.dbError("DB Error, mysql function last_insert_id failed");
                    throw new HandlerException("Database Error");
                }

                toAppend.addElement("productSetName").addText(HandlerUtils.nullToEmpty(productSetName));
                toAppend.addElement("productSetId").addText(String.valueOf(productSetId));

                while (i.hasNext()) {
                    Element pro = (Element) i.next();
                    int productId = HandlerUtils.getRequiredInteger(pro, "productId");
                    String productPlu = HandlerUtils.getRequiredString(pro, "productPlu");
                    stmt = transconn.prepareStatement(insertProductSetProductMap);
                    stmt.setInt(1, productSetId);
                    stmt.setInt(2, productId);
                    stmt.setString(3, productPlu);
                    stmt.executeUpdate();
                }
            } else {
                addErrorDetail(toAppend, "A Product Set with the same name and Product SetType already exists");
            }

        } catch (SQLException sqle) {
            logger.dbError("Database error in addProductSet: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
        }
    }

    private void addRegion(Element toHandle, Element toAppend) throws HandlerException {

        String regionName = HandlerUtils.getRequiredString(toHandle, "regionName");
        double threshold = HandlerUtils.getRequiredDouble(toHandle, "threshold");

        Iterator i = toHandle.elementIterator("county");
        int callerId = getCallerId(toHandle);

        String selectRegionMaster = " SELECT id FROM regionMaster WHERE name = ? ";
        String insertRegion = " INSERT INTO regionMaster (name, threshold, points) VALUES (?,?,GEOMFROMTEXT('POINT(0 0)')) ";
        String getLastId = " SELECT LAST_INSERT_ID()";
        String insertRegionCountyMap = " INSERT INTO regionCountyMap (region, county, description) VALUES (?,?,?) ";

        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            stmt = transconn.prepareStatement(selectRegionMaster);
            stmt.setString(1, regionName);
            rs = stmt.executeQuery();
            if (!rs.next()) {

                stmt = transconn.prepareStatement(insertRegion);
                stmt.setString(1, regionName);
                stmt.setDouble(2, threshold);
                stmt.executeUpdate();

                int regionId = -1;
                stmt = transconn.prepareStatement(getLastId);
                rs = stmt.executeQuery();
                if (rs.next()) {
                    regionId = rs.getInt(1);
                } else {
                    logger.dbError("DB Error, mysql function last_insert_id failed");
                    throw new HandlerException("Database Error");
                }
                toAppend.addElement("regionMaster").addText(String.valueOf(regionId));
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
            } else {
                addErrorDetail(toAppend, "A Region with the same name already exists");
            }

        } catch (SQLException sqle) {
            logger.dbError("Database error in addGroupRegion: " + sqle.getMessage());
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
    private void addGroupExclusion(Element toHandle, Element toAppend) throws HandlerException {

        Iterator i = toHandle.elementIterator("groupExclusion");
        int groupId = HandlerUtils.getRequiredInteger(toHandle, "groupId");
        int callerId = getCallerId(toHandle);

        String getLastId = " SELECT LAST_INSERT_ID()";
        String selectExclusion = " SELECT e.id FROM exclusion e WHERE e.type = ? AND e.tables = ? AND e.field = ? AND e.value = ? ";
        String createExclusion = " INSERT INTO exclusion (type, name, tables, field, value) VALUES (2,?,?,?,?)";
        String selectGroupExclusion = " SELECT gEM.id FROM groupExclusionMap gEM WHERE gEM.groups = ? AND gEM.exclusion = ? AND gEM.regionGroup = ?";
        String insertGroupExclusion = " INSERT INTO groupExclusionMap (groups, exclusion, regionGroup) VALUES (?,?,?) ";
        int exclusion = 0;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            while (i.hasNext()) {
                Element grou = (Element) i.next();
                int regionGroup = HandlerUtils.getRequiredInteger(grou, "regionGroup");
                int exclusionType = HandlerUtils.getRequiredInteger(grou, "exclusionType");
                String exclusionTable = HandlerUtils.getRequiredString(grou, "exclusionTable");
                String exclusionField = HandlerUtils.getRequiredString(grou, "exclusionField");
                String exclusionValue = HandlerUtils.getRequiredString(grou, "exclusionValue");

                stmt = transconn.prepareStatement(selectExclusion);
                stmt.setInt(1, exclusionType);
                stmt.setString(2, exclusionTable);
                stmt.setString(3, exclusionField);
                stmt.setString(4, exclusionValue);
                rs = stmt.executeQuery();
                if (rs.next()) {
                    exclusion = rs.getInt(1);
                    stmt = transconn.prepareStatement(selectGroupExclusion);
                    stmt.setInt(1, groupId);
                    stmt.setInt(2, exclusion);
                    stmt.setInt(3, regionGroup);
                    rs = stmt.executeQuery();
                    if (!rs.next()) {
                        stmt = transconn.prepareStatement(insertGroupExclusion);
                        stmt.setInt(1, groupId);
                        stmt.setInt(2, exclusion);
                        stmt.setInt(3, regionGroup);
                        stmt.executeUpdate();
                    } else {
                        logger.debug("Group Exclusion already exists");
                    }
                } else {
                    String exclusionName = exclusionTable + ":" + exclusionField + ":" + exclusionValue;
                    stmt = transconn.prepareStatement(createExclusion);
                    stmt.setString(1, exclusionName);
                    stmt.setString(2, exclusionTable);
                    stmt.setString(3, exclusionField);
                    stmt.setString(4, exclusionValue);
                    stmt.executeUpdate();

                    // get the id of the exclusion we added
                    stmt = transconn.prepareStatement(getLastId);
                    rs = stmt.executeQuery();
                    if (rs.next()) {
                        exclusion = rs.getInt(1);
                        stmt = transconn.prepareStatement(insertGroupExclusion);
                        stmt.setInt(1, groupId);
                        stmt.setInt(2, exclusion);
                        stmt.setInt(3, regionGroup);
                        stmt.executeUpdate();
                    } else {
                        logger.dbError("SQL Last_Insert_Id FAILED in addTestUser ");
                        throw new HandlerException("database error");
                    }
                }
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error in addGroupExclusion: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
        }
    }

    /** Update an existing supplier.  Right now, all you can change is the name.
     *  <supplierId> 0000
     *  <name> String
     */
    private void addGroupProductSet(Element toHandle, Element toAppend) throws HandlerException {

        Iterator i = toHandle.elementIterator("groupProductSet");
        int callerId = getCallerId(toHandle);

        String selectGroupProductSet = " SELECT gPS.id FROM groupProductSet gPS WHERE gPS.groups = ? AND gPS.productSet = ? ";
        String insertGroupProductSet = " INSERT INTO groupProductSet (groups, productSet) VALUES (?,?) ";

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
                if (!rs.next()) {
                    stmt = transconn.prepareStatement(insertGroupProductSet);
                    stmt.setInt(1, groupId);
                    stmt.setInt(2, productSet);
                    stmt.executeUpdate();
                } else {
                    logger.debug("Group Product Set already exists");
                }
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error in addGroupProductSet: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
        }
    }

    /** Update an existing supplier.  Right now, all you can change is the name.
     *  <supplierId> 0000
     *  <name> String
     */
    private void addRegionProductSet(Element toHandle, Element toAppend) throws HandlerException {

        Iterator i = toHandle.elementIterator("regionProductSet");
        int callerId = getCallerId(toHandle);

        String selectRegionProductSet = " SELECT rPS.id FROM regionProductSet rPS WHERE rPS.region = ? AND rPS.productSet = ? ";
        String insertRegionProductSet = " INSERT INTO regionProductSet (region, productSet) VALUES (?,?) ";

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
                if (!rs.next()) {
                    stmt = transconn.prepareStatement(insertRegionProductSet);
                    stmt.setInt(1, regionId);
                    stmt.setInt(2, productSet);
                    stmt.executeUpdate();
                } else {
                    logger.debug("Region Product Set already exists");
                }
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error in addRegionProductSet: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
        }
    }

    /** Update an existing supplier.  Right now, all you can change is the name.
     *  <supplierId> 0000
     *  <name> String
     */
    private void addUserRegion(Element toHandle, Element toAppend) throws HandlerException {

        Iterator i = toHandle.elementIterator("userRegions");
        int callerId = getCallerId(toHandle);

        String selectRegion = " SELECT r.id FROM region r LEFT JOIN userRegionMap uRM on uRM.region = r.id WHERE r.regionGroup = ? AND uRM.user = ? ";
        String selectRegionDetails = " SELECT rM.name, gRM.id, rM.threshold, AsText(rM.points), gRM.groups FROM regionMaster rM " +
                " LEFT JOIN groupRegionMap gRM ON gRM.regionMaster = rM.id WHERE gRM.id = ? ";
        String insertRegion = " INSERT INTO region (name, regionGroup, threshold, points) VALUES (?,?,?,GEOMFROMTEXT(?)) ";
        String insertUserRegionMap = " INSERT INTO userRegionMap (user, region) VALUES (?,?) ";
        String selectGroupProductSet = " SELECT productSet FROM groupProductSet WHERE groups = ? ";
        String insertRegionProductSet = " INSERT INTO regionProductSet (region, productSet) VALUES (?,?) ";
        String selectGroupRegionExclusion =
                " SELECT e.id FROM exclusion e LEFT JOIN groupExclusionMap gEM ON gEM.exclusion = e.id WHERE e.type = 1 AND gEM.groups=?";
        String insertRegionExclusionMap =
                " INSERT INTO regionExclusionMap (region,exclusion) " +
                " VALUES (?,?) ";
        String getLastId = " SELECT LAST_INSERT_ID()";

        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            while (i.hasNext()) {
                Element reg = (Element) i.next();
                int user = HandlerUtils.getRequiredInteger(reg, "user");
                int regionGroupId = HandlerUtils.getRequiredInteger(reg, "regionGroupId");
                int groups = -1, region = -1;
                stmt = transconn.prepareStatement(selectRegion);
                stmt.setInt(1, regionGroupId);
                stmt.setInt(2, user);
                rs = stmt.executeQuery();
                if (!rs.next()) {
                    stmt = transconn.prepareStatement(selectRegionDetails);
                    stmt.setInt(1, regionGroupId);
                    rs = stmt.executeQuery();
                    if (rs.next()) {
                        stmt = transconn.prepareStatement(insertRegion);
                        stmt.setString(1, rs.getString(1));
                        stmt.setInt(2, rs.getInt(2));
                        stmt.setDouble(3, rs.getDouble(3));
                        stmt.setString(4, rs.getString(4));
                        stmt.executeUpdate();

                        groups = rs.getInt(5);

                        // get the id of the region we added
                        stmt = transconn.prepareStatement(getLastId);
                        rs = stmt.executeQuery();
                        if (rs.next()) {
                            region = rs.getInt(1);

                            // creating region exclusion map
                            stmt = transconn.prepareStatement(selectGroupRegionExclusion);
                            stmt.setInt(1, groups);
                            rs = stmt.executeQuery();
                            while (rs.next()) {
                                stmt = transconn.prepareStatement(insertRegionExclusionMap);
                                stmt.setInt(1, region);
                                stmt.setInt(2, rs.getInt(1));
                                stmt.executeUpdate();
                            }

                            // creating user region map
                            stmt = transconn.prepareStatement(insertUserRegionMap);
                            stmt.setInt(1, user);
                            stmt.setInt(2, region);
                            stmt.executeUpdate();

                            // get the productSet for the group
                            stmt = transconn.prepareStatement(selectGroupProductSet);
                            stmt.setInt(1, groups);
                            rs = stmt.executeQuery();
                            while (rs.next()) {
                                // creating region productSet map
                                stmt = transconn.prepareStatement(insertRegionProductSet);
                                stmt.setInt(1, region);
                                stmt.setInt(2, rs.getInt(1));
                                stmt.executeUpdate();
                            }
                        } else {
                            logger.dbError("SQL Last_Insert_Id FAILED in addUserRegion ");
                            throw new HandlerException("database error");
                        }
                    }
                } else {
                    logger.debug("Region for the user already exists");
                }
            }

        } catch (SQLException sqle) {
            logger.dbError("Database error in addUserRegion: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
        }
    }
    
    private void addNewUserRegion(int user, int group) throws HandlerException {

        String select                       = " SELECT rM.name, gRM.id, gRM.threshold, AsText(rM.points), gRM.groups FROM regionMaster rM " +
                                            " LEFT JOIN groupRegionMap gRM ON gRM.regionMaster = rM.id " +
                                            " WHERE gRM.groups = ? ORDER BY rM.name ";
        String selectGroupProductSet        = " SELECT productSet FROM groupProductSet WHERE groups = ? ";
        String selectGroupRegionExclusion   = " SELECT e.id FROM exclusion e LEFT JOIN groupExclusionMap gEM ON gEM.exclusion = e.id WHERE e.type = 1 AND gEM.groups=? ";
        String getLastId                    = " SELECT LAST_INSERT_ID() ";

        String insertRegion                 = " INSERT INTO region (name, regionGroup, threshold, points) VALUES (?,?,?,GEOMFROMTEXT(?)) ";
        String insertUserRegionMap          = " INSERT INTO userRegionMap (user, region) VALUES (?,?) ";
        String insertRegionProductSet       = " INSERT INTO regionProductSet (region, productSet) VALUES (?,?) ";
        String insertRegionExclusionMap     = " INSERT INTO regionExclusionMap (region,exclusion) " +
                                            " VALUES (?,?) ";
        
        PreparedStatement stmt              = null;
        ResultSet rs                        = null, outerRS = null;
        try {
            stmt                            = transconn.prepareStatement(select);
            stmt.setInt(1, group);
            outerRS                              = stmt.executeQuery();
            while (outerRS.next()) {
                int groups = -1;
                
                stmt = transconn.prepareStatement(insertRegion);
                stmt.setString(1, outerRS.getString(1));
                stmt.setInt(2, outerRS.getInt(2));
                stmt.setDouble(3, outerRS.getDouble(3));
                stmt.setString(4, outerRS.getString(4));
                stmt.executeUpdate();

                groups = outerRS.getInt(5);

                // get the id of the region we added
                stmt = transconn.prepareStatement(getLastId);
                rs = stmt.executeQuery();
                if (rs.next()) {
                    int region = rs.getInt(1);

                    // creating user region map
                    stmt = transconn.prepareStatement(insertUserRegionMap);
                    stmt.setInt(1, user);
                    stmt.setInt(2, region);
                    stmt.executeUpdate();

                    // creating region exclusion map
                    stmt = transconn.prepareStatement(selectGroupRegionExclusion);
                    stmt.setInt(1, groups);
                    rs = stmt.executeQuery();
                    while (rs.next()) {
                        stmt = transconn.prepareStatement(insertRegionExclusionMap);
                        stmt.setInt(1, region);
                        stmt.setInt(2, rs.getInt(1));
                        stmt.executeUpdate();
                    }

                    // get the productSet for the group
                    stmt = transconn.prepareStatement(selectGroupProductSet);
                    stmt.setInt(1, groups);
                    rs = stmt.executeQuery();
                    while (rs.next()) {
                        // creating region productSet map
                        stmt = transconn.prepareStatement(insertRegionProductSet);
                        stmt.setInt(1, region);
                        stmt.setInt(2, rs.getInt(1));
                        stmt.executeUpdate();
                    }
                } else {
                    logger.dbError("SQL Last_Insert_Id FAILED in addNewUserRegion ");
                    throw new HandlerException("database error");
                }
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error in addNewUserRegion: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
        }
    }

    /**  Add a new normal user with Read Only access to one location.
     *   <fullName>"String"
     *   <username>"String"
     *   <password>"String" SHA-1 encoded
     *   <locationId>000
     */
    private void addUser(Element toHandle, Element toAppend) throws HandlerException {
        final int READ_ONLY_ACCESS = 7;

        String fullName = HandlerUtils.getRequiredString(toHandle, "fullName");
        String username = HandlerUtils.getRequiredString(toHandle, "username");
        String password = HandlerUtils.getRequiredString(toHandle, "password");
        String email = HandlerUtils.getRequiredString(toHandle, "email");
        String mobile = HandlerUtils.getRequiredString(toHandle, "mobile");
        String carrier = HandlerUtils.getRequiredString(toHandle, "carrier");
        int platform = HandlerUtils.getRequiredInteger(toHandle, "platform");
        int callerId = getCallerId(toHandle);


        String checkUser =
                " SELECT id FROM user WHERE username=? LIMIT 1";
        String getCustomer =
                " SELECT customer, groupId FROM location WHERE id=?";
        String insertUser =
                " INSERT INTO user (name,username,password,customer,groupId,email,mobile,carrier,platform) " +
                " VALUES (?,?,?,?,?,?,?,?,?) ";
        String getLastId =
                " SELECT LAST_INSERT_ID()";
        String insertMap =
                " INSERT INTO userMap (user,location,securityLevel) " +
                " VALUES (?,?,?) ";
        String insertUserGroupMap =
                " INSERT INTO userGroupMap (user,groups) " +
                " VALUES (?,?) ";
        String selectGroupUserExclusion =
                " SELECT e.id FROM exclusion e LEFT JOIN groupExclusionMap gEM ON gEM.exclusion = e.id WHERE gEM.groups=?";
        String insertUserExclusionMap =
                " INSERT INTO userExclusionMap (user,exclusion) " +
                " VALUES (?,?) ";

        PreparedStatement stmt = null;
        ResultSet rs = null;

        int customer = -1, groupId = -1;
        int userDbId;
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
                switch (platform) {
                    case 1:
                        int location = HandlerUtils.getRequiredInteger(toHandle, "locationId");
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
                        stmt.setInt(9, platform);
                        stmt.executeUpdate();

                        // get the id of the user we added
                        stmt = transconn.prepareStatement(getLastId);
                        rs = stmt.executeQuery();
                        if (rs.next()) {
                            userDbId = rs.getInt(1);
                        } else {
                            logger.dbError("SQL Last_Insert_Id FAILED in addTestUser ");
                            throw new HandlerException("database error");
                        }

                        logMessage = "Added bevManager user " + fullName + " as '" + username + "'";
                        logger.portalDetail(callerId, "addUser", 0, "user", userDbId, logMessage, transconn);

                        // add read-only access for this location into the user map
                        stmt = transconn.prepareStatement(insertMap);
                        stmt.setInt(1, userDbId);
                        stmt.setInt(2, location);
                        stmt.setInt(3, READ_ONLY_ACCESS);
                        stmt.executeUpdate();

                        logger.debug("User added");

                        toAppend.addElement("userFullname").addText(HandlerUtils.nullToEmpty(fullName));
                        toAppend.addElement("userId").addText(String.valueOf(userDbId));
                    case 2:
                        customer = -1;

                        int group = HandlerUtils.getRequiredInteger(toHandle, "group");

                        // add the bevSync user record
                        stmt = transconn.prepareStatement(insertUser);
                        stmt.setString(1, fullName);
                        stmt.setString(2, username);
                        stmt.setString(3, password);
                        stmt.setInt(4, customer);
                        stmt.setInt(5, groupId);
                        stmt.setString(6, email);
                        stmt.setString(7, mobile);
                        stmt.setString(8, carrier);
                        stmt.setInt(9, platform);
                        stmt.executeUpdate();

                        // get the id of the user we added
                        stmt = transconn.prepareStatement(getLastId);
                        rs = stmt.executeQuery();
                        if (rs.next()) {
                            userDbId = rs.getInt(1);
                        } else {
                            logger.dbError("SQL Last_Insert_Id FAILED in addTestUser ");
                            throw new HandlerException("database error");
                        }

                        stmt = transconn.prepareStatement(insertUserGroupMap);
                        stmt.setInt(1, userDbId);
                        stmt.setInt(2, group);
                        stmt.executeUpdate();

                        stmt = transconn.prepareStatement(selectGroupUserExclusion);
                        stmt.setInt(1, group);
                        rs = stmt.executeQuery();
                        while (rs.next()) {
                            stmt = transconn.prepareStatement(insertUserExclusionMap);
                            stmt.setInt(1, userDbId);
                            stmt.setInt(2, rs.getInt(1));
                            stmt.executeUpdate();
                        }

                        addNewUserRegion(userDbId, group);

                        logMessage = "Added bevSync user " + fullName + " as '" + username + "'";
                        logger.portalDetail(callerId, "addUser", 0, "user", userDbId, logMessage, transconn);

                        logger.debug("User added");

                        toAppend.addElement("userFullname").addText(HandlerUtils.nullToEmpty(fullName));
                        toAppend.addElement("userId").addText(String.valueOf(userDbId));
                    default:
                        break;
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

    /** Add a bar to an existing location
     *  <locationId>000
     *  <name>"String"
     *
     *  The new bar must have a unique name for this location, or an error detail will be returned
     *
     */
    private void addBar(Element toHandle, Element toAppend) throws HandlerException {

        logger.debug("addBar starting");

        int locationId = HandlerUtils.getRequiredInteger(toHandle, "locationId");
        //NischaySharma_02-Oct-2009_Start
        int zoneId = HandlerUtils.getOptionalInteger(toHandle, "zoneId");
        //NischaySharma_02-Oct-2009_End
        String name = HandlerUtils.getRequiredString(toHandle, "name");
        int callerId = getCallerId(toHandle);

        String insert =
                " INSERT INTO bar (location, name) VALUES (?,?)";
        String checkName =
                " SELECT id FROM bar WHERE location=? AND LCASE(name)=LCASE(?)";
        //NischaySharma_02-Oct-2009_Start
        String checkNameWZone =
                " SELECT id FROM bar WHERE zone=? AND LCASE(name)=LCASE(?)";
        String insertWZone = " INSERT INTO bar (location, zone, name) VALUES (?,?,?)";
        //NischaySharma_02-Oct-2009_End

        PreparedStatement stmt = null;
        ResultSet rs = null;

        logger.portalAction("addBar");


        try {
            boolean locationCheckForeignKey = checkForeignKey("location", locationId, transconn);
            if (locationCheckForeignKey) {

                if (zoneId <= 0) {

                    stmt = transconn.prepareStatement(checkName);
                    stmt.setInt(1, locationId);
                    stmt.setString(2, name);
                    rs = stmt.executeQuery();

                    if (!rs.next()) {
                        //NischaySharma_02-Oct-2009_Start
                        stmt = transconn.prepareStatement(insert);
                        stmt.setInt(1, locationId);
                        stmt.setString(2, name);
                        stmt.executeUpdate();
                    } else {
                        addErrorDetail(toAppend, "A bar with that name already exists");
                    }

                } else {

                    stmt = transconn.prepareStatement(checkNameWZone);
                    stmt.setInt(1, zoneId);
                    stmt.setString(2, name);
                    rs = stmt.executeQuery();

                    if (!rs.next()) {
                        stmt = transconn.prepareStatement(insertWZone);
                        stmt.setInt(1, locationId);
                        stmt.setInt(2, zoneId);
                        stmt.setString(3, name);
                        stmt.executeUpdate();

                    } else {
                        addErrorDetail(toAppend, "A bar with that name already exists for that zone");
                    }

                }
                //NischaySharma_02-Oct-2009_End
                // Log the action
                stmt = transconn.prepareStatement("SELECT LAST_INSERT_ID()");
                rs = stmt.executeQuery();
                if (rs.next()) {
                    int newId = rs.getInt(1);
                    String logMessage = "Adding a bar named '" + name + "'";
                    logger.portalDetail(callerId, "addBar", locationId, "bar", newId, logMessage, transconn);
                }


            } else {
                addErrorDetail(toAppend, "Invalid Location ID");
                logger.generalWarning("Invalid Reference ID Number: Location : No records found.");
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
     * @deprecated use addCustomer to do this
     */
    private void addLocation(Element toHandle, Element toAppend) throws HandlerException {
        //(int id, String name, String addrStreet, String addrCity, String addrZip, String addrState, int refId)

        int customerId = HandlerUtils.getRequiredInteger(toHandle, "CustomerId");
        String name = HandlerUtils.getRequiredString(toHandle, "Name");
        String addrStreet = HandlerUtils.getRequiredString(toHandle, "AddressStreet");
        String addrCity = HandlerUtils.getRequiredString(toHandle, "AddressCity");
        String addrZip = HandlerUtils.getRequiredString(toHandle, "AddressZip");
        String addrState = HandlerUtils.getRequiredString(toHandle, "AddressState");

        PreparedStatement stmt = null;
        ResultSet rs = null;

        logger.portalAction("addLocation");

        try {

            boolean customerCheckForeignKey = checkForeignKey("Customers", customerId, transconn);
            if (customerCheckForeignKey) {
                String insert = "INSERT INTO Locations (name,addrStreet, addrCity, addrZip, addrState,customerId) VALUES (?,?,?,?,?,?)";
                stmt = transconn.prepareStatement(insert);
                stmt.setString(1, name);
                stmt.setString(2, addrStreet);
                stmt.setString(3, addrCity);
                stmt.setString(4, addrZip);
                stmt.setString(5, addrState);
                stmt.setInt(6, customerId);
                stmt.executeUpdate();
            } else {
                logger.generalWarning("Invalid Reference ID Number: Customers : No records found.");
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
            close(rs);
        }

    }

    /** Create a new customer -OR- a new location (existing customer), with a single location and a single bar
     *
     *  Two options:
     *  To create a new customer and location:
     *      provide <customerName> field with the name of the new customer
     *  To create a new location at an existing location
     *      provide <customerId> field with the id of the existing customer
     *  If both are provided, customerName will be ignored
     *
     *  In addition to these two:
     *  <locationName>"String"
     *  <addrCity>"String"
     *  <addrState>"String" `NY`
     *  <addrZip>00000
     *  <addrStreet>"String"
     *
     *  For NEW customers, the ID of the new customer will be returned
     *  Return: <customerId>00000
     */
    private void addCustomer(Element toHandle, Element toAppend) throws HandlerException {

        String custName = HandlerUtils.getOptionalString(toHandle, "customerName");
        int custId = HandlerUtils.getOptionalInteger(toHandle, "customerId");
        String locationName = HandlerUtils.getRequiredString(toHandle, "locationName");
        String addrCity = HandlerUtils.getRequiredString(toHandle, "addrCity");
        String addrState = HandlerUtils.getRequiredString(toHandle, "addrState");
        String addrZip = HandlerUtils.getRequiredString(toHandle, "addrZip");
        String addrStreet = HandlerUtils.getRequiredString(toHandle, "addrStreet");
        String easternOffset = HandlerUtils.getRequiredString(toHandle, "easternOffset");
        //NischaySharma_03-Sep-2009_Start
        double latitude = HandlerUtils.getRequiredDouble(toHandle, "latitude");
        double longitude = HandlerUtils.getRequiredDouble(toHandle, "longitude");
        
        //NischaySharma_03-Sep-2009_End
        //NischaySharma_02-Oct-2009_Start
        boolean isConcessions = HandlerUtils.getOptionalBoolean(toHandle, "isConcessions");
        //NischaySharma_02-Oct-2009_End

        //NischaySharma_04-Feb_2010_Start
        int custType = isConcessions ? 2 : 1;


        int callerId = getCallerId(toHandle);

        if (custId <= 0 && (custName == null || custName.equals(""))) {
            throw new HandlerException("illegal arguments to addCustomer");
        }
        
        

        String insertCust = "INSERT INTO customer (name, type) VALUES (?,?)";
        //NischaySharma_03-Sep-2009_Start: Edited the insert statement to accept lat and lng
        String insertLoc = "INSERT INTO location (name,addrStreet,addrCity,addrState,addrZip,easternOffset,customer, latitude, longitude, type) " +
                " VALUES (?,?,?,?,?,?,?,?,?,?) ";
        //NischaySharma_03-Sep-2009_End
        String insertBar = "INSERT INTO bar (name,location) VALUES ('Main Bar',?)";
        String insertLocationDetails = "INSERT INTO locationDetails (location) VALUES (?)";
        String updateCoordIndex = "UPDATE location l LEFT JOIN state s ON s.USPSST = l.addrState LEFT JOIN zipList z ON z.zip = l.addrZip " +
                " LEFT JOIN county c ON c.state = s.FIPSST AND z.county = c.county " +
                " SET l.zipIndex = z.id, l.countyIndex = c.id, l.stateIndex = s.id WHERE l.id = ? ";
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

                toAppend.addElement("customerId").addText(String.valueOf(custId));
                String logMessage = "Created customer '" + custName + "'";
                logger.portalDetail(callerId, "addCustomer", 0, "customer", custId, logMessage, transconn);
            }

            // Now add the location
            stmt = transconn.prepareStatement(insertLoc);
            stmt.setString(1, locationName);
            stmt.setString(2, addrStreet);
            stmt.setString(3, addrCity);
            stmt.setString(4, addrState);
            stmt.setString(5, addrZip);
            stmt.setString(6, easternOffset);
            stmt.setInt(7, custId);
            //NischaySharma_03-Sep-2009_Start
            stmt.setDouble(8, latitude);
            stmt.setDouble(9, longitude);
            //NischaySharma_03-Sep-2009_End
            stmt.setInt(10, custType);
            //NischaySharma_04-Feb_2010_End           
            stmt.executeUpdate();

            int locId;
            stmt = transconn.prepareStatement(getLastId);
            rs = stmt.executeQuery();
            if (rs.next()) {
                locId = rs.getInt(1);
                stmt = transconn.prepareStatement(updateCoordIndex);
                stmt.setInt(1, locId);
                stmt.executeUpdate();
            } else {
                logger.dbError("second call to LAST_INSERT_ID in addCustomer failed to return a result");
                throw new HandlerException("Database Error");
            }

            String logMessage = "Created location '" + locationName + "' for Cust #" + custId;
            logger.portalDetail(callerId, "addLocation", locId, "location", locId, logMessage, transconn);

            //NischaySharma_02-Oct-2009_Start
            if (!isConcessions) {
                stmt = transconn.prepareStatement(insertBar);
                stmt.setInt(1, locId);
                stmt.executeUpdate();
            }
            stmt = transconn.prepareStatement(insertLocationDetails);
            stmt.setInt(1, locId);
            stmt.executeUpdate();
            //NischaySharma_02-Oct-2009_End

            logger.debug("Created a new location and bar");


        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
            close(rs);
        }

    }

    /** Database test, only callable by Admin Loc 0
     */
    private void dbOverload(Element toHandle, Element toAppend, SecureSession ss) throws HandlerException {
        final int TEST_SIZE = 40;
        logger.dbError("CALLING DB_OVERLOAD");
        if (ss.getLocation() == 0 && ss.getSecurityLevel().canAdmin()) {
            RegisteredConnection[] conns = new RegisteredConnection[TEST_SIZE];
            for (int i = 0; i < TEST_SIZE; i++) {
                logger.dbConnection("Creating #" + i);
                conns[i] = DatabaseConnectionManager.getNewConnection(transConnName);
            }
            for (int i = 0; i < TEST_SIZE; i++) {
                logger.dbConnection("Closing #" + i);
                close(conns[i]);
            }
            logger.dbConnection("DB_OVERLOAD_FINISHED");
        } else {
            logger.portalAccessViolation("Attempted to call dbOverload with a bad key");
            addErrorDetail(toAppend, "Bad Permission");
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
    private boolean manualSalesUpload(Element toHandle, Element toAppend) throws HandlerException {
        int location = HandlerUtils.getRequiredInteger(toHandle, "locationId");
        boolean storeAll = HandlerUtils.getOptionalBoolean(toHandle, "storeAll");

        boolean success = false;
        int stmtIndex = 1;

        List itemList = toHandle.elements("item");
        if (null == itemList || itemList.size() == 0) {
            return true;
        }
        double qty;
        String id;
        String checkId;
        long epoch;

        PreparedStatement stmt = null;
        ResultSet rs = null;
        int queryCount = 0;

        String sqlIns = null;
        String sqlSel = null;
        boolean oldAutoCommit = true;
        boolean changedAutoCommit = false;

        Hashtable<String, Boolean> idHash = new Hashtable<String, Boolean>();

        try {
            Calendar calEpoch = Calendar.getInstance();
            oldAutoCommit = transconn.getAutoCommit();

            if (!storeAll) {
                sqlSel = "SELECT plu FROM beverage where location = ?";
                stmt = transconn.prepareStatement(sqlSel);
                stmt.setInt(1, location);
                rs = stmt.executeQuery();
                queryCount++;
                while (null != rs && rs.next()) {
                    idHash.put(rs.getString(1), Boolean.TRUE);
                }
                close(rs);
            }

            // Check for the current value
            sqlSel =
                    " SELECT checkId FROM sales WHERE location = ? " +
                    " AND pluNumber = ? AND checkId = ? AND date = ?";
            // Insert the current value
            sqlIns =
                    " INSERT INTO sales (location, pluNumber, checkId, quantity, date)" +
                    " values (?,?,?,?,?)";
            changedAutoCommit = true;
            transconn.setAutoCommit(false);

            String qtyStr;
            boolean duplicate = false;
            int duplicateCount = 0;
            int insertedCount = 0;
            for (Object o : itemList) {
                Element item = (Element) o;
                epoch = HandlerUtils.getRequiredLong(item, "epoch");
                id = HandlerUtils.getRequiredString(item, "id");
                checkId = HandlerUtils.getRequiredString(item, "checkId");
                calEpoch.setTimeInMillis(epoch * 1000L);
                Timestamp tstamp = new Timestamp(calEpoch.getTimeInMillis());
                if (storeAll || idHash.containsKey(id)) {
                    stmtIndex = 1;
                    stmt = transconn.prepareStatement(sqlSel);
                    stmt.setInt(stmtIndex++, location);
                    stmt.setString(stmtIndex++, id);
                    stmt.setString(stmtIndex++, checkId);
                    stmt.setTimestamp(stmtIndex++, tstamp);
                    rs = stmt.executeQuery();
                    queryCount++;
                    duplicate = (null != rs && rs.next());
                    close(rs);
                    if (duplicate) {
                        duplicateCount++;
                    } else {
                        qtyStr = HandlerUtils.getRequiredString(item, "qty");
                        qty = Double.parseDouble(qtyStr);
                        logger.debug("Inserting: (" + location + ", " +
                                id + ", " + checkId + ", " + qty + ", " +
                                tstamp + ")");
                        stmtIndex = 1;
                        stmt = transconn.prepareStatement(sqlIns);
                        stmt.setInt(stmtIndex++, location);
                        stmt.setString(stmtIndex++, id);
                        stmt.setString(stmtIndex++, checkId);
                        stmt.setDouble(stmtIndex++, qty);
                        stmt.setTimestamp(stmtIndex++, tstamp);
                        stmt.executeUpdate();
                        queryCount++;
                        insertedCount++;
                    }
                }
            }

            transconn.commit();
            logger.dbAction("Committed " + queryCount + " queries");
            toAppend.addElement("insertedCount").addText(String.valueOf(insertedCount));
            toAppend.addElement("duplicateCount").addText(String.valueOf(duplicateCount));
            success = true;

        } catch (SQLException sqle) {
            if (null != transconn) {
                try {
                    logger.dbAction("manualSalesUpload: database rollback");
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
        }
        return success;
    }

    /**
     * Inserts a new misc product
     *  <locationId>
     *  <name>
     *  <plu>
     *  <supplierId>
     *
     * returns: nothing
     *
     */
    private void addMiscProduct(Element toHandle, Element toAppend) throws HandlerException {
        int location = HandlerUtils.getRequiredInteger(toHandle, "locationId");
        int callerId = getCallerId(toHandle);

        String insertProduct = "INSERT INTO miscProduct (location, name, plu, supplier) VALUES (?, ?, ?, ?)";
        PreparedStatement stmt = null;
        ResultSet rs = null;
        int newMiscProductId = 0;

        try {
            Iterator i = toHandle.elementIterator("product");
            while (i.hasNext()) {
                Element el = (Element) i.next();
                String name = HandlerUtils.getRequiredString(el, "name");
                String plu = HandlerUtils.getRequiredString(el, "plu");
                int supplier = HandlerUtils.getRequiredInteger(el, "supplierId");

                if (checkForeignKey("location", location, transconn) && checkForeignKey("supplier", supplier, transconn)) {

                    stmt = transconn.prepareStatement(insertProduct);
                    int paramIndex = 1;
                    stmt.setInt(paramIndex++, location);
                    stmt.setString(paramIndex++, name);
                    stmt.setString(paramIndex++, plu);
                    stmt.setInt(paramIndex++, supplier);

                    stmt.executeUpdate();

                    stmt = transconn.prepareStatement("SELECT LAST_INSERT_ID()");
                    rs = stmt.executeQuery();
                    if (rs.next()) {
                        newMiscProductId = rs.getInt(1);
                    }

                    String logMessage = "Added a new miscProduct, " + name + ", id " + String.valueOf(newMiscProductId) +
                            " location " + String.valueOf(location) + ", supplier id " + String.valueOf(supplier);
                    logger.portalDetail(callerId, "addMiscProduct", 0, "mistProduct", newMiscProductId, logMessage, transconn);
                    logger.debug(logMessage);
                } else {
                    addErrorDetail(toAppend, "Location or supplier keys are not valid");
                }
            }

        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
        }
    }

    //NischaySharma_02-Oct-2009_Start
    private void addZone(Element toHandle, Element toAppend) throws HandlerException {
        int locationId = HandlerUtils.getRequiredInteger(toHandle, "locationId");
        String zoneName = HandlerUtils.getRequiredString(toHandle, "zoneName");
        int callerId = getCallerId(toHandle);

        String insertZone = "INSERT INTO zone (name, location) VALUES (?, ?)";
        String insertZonePoint = "INSERT INTO zone_point (zone, new, points) VALUES (?, 1, GEOMFROMTEXT('POINT(0 0)'))";
        String checkName = " SELECT id FROM zone WHERE location=? AND LCASE(name)=LCASE(?)";

        PreparedStatement stmt = null;
        ResultSet rs = null;

        try {
            boolean locationCheckForeignKey = checkForeignKey("location", locationId, transconn);
            if (locationCheckForeignKey) {
                stmt = transconn.prepareStatement(checkName);
                stmt.setInt(1, locationId);
                stmt.setString(2, zoneName);
                rs = stmt.executeQuery();

                if (!rs.next()) {
                    stmt = transconn.prepareStatement(insertZone);
                    int paramIndex = 1;
                    stmt.setString(paramIndex++, zoneName);
                    stmt.setInt(paramIndex++, locationId);
                    stmt.executeUpdate();

                    // Log the action
                    stmt = transconn.prepareStatement("SELECT LAST_INSERT_ID()");
                    rs = stmt.executeQuery();
                    if (rs.next()) {
                        int newId = rs.getInt(1);
                        String logMessage = "Adding a zone named '" + zoneName + "'";
                        logger.portalDetail(callerId, "addZone", locationId, "zone", newId, logMessage, transconn);

                        stmt = transconn.prepareStatement(insertZonePoint);
                        stmt.setInt(1, newId);
                        stmt.executeUpdate();

                    }
                } else {
                    addErrorDetail(toAppend, "A zone with that name already exists");
                }
            } else {
                addErrorDetail(toAppend, "Invalid Location ID");
                logger.generalWarning("Invalid Reference ID Number: Location : No records found.");
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
            close(rs);
        }
    }

    private void addStation(Element toHandle, Element toAppend) throws HandlerException {
        int standId = HandlerUtils.getRequiredInteger(toHandle, "standId");
        String stationName = HandlerUtils.getRequiredString(toHandle, "stationName");
        int callerId = getCallerId(toHandle);

        String insertStation = "INSERT INTO station (name, bar) VALUES (?, ?)";
        String checkName = " SELECT id FROM station WHERE bar=? AND LCASE(name)=LCASE(?)";

        PreparedStatement stmt = null;
        ResultSet rs = null;

        try {
            boolean standCheckForeignKey = checkForeignKey("bar", standId, transconn);
            if (standCheckForeignKey) {
                stmt = transconn.prepareStatement(checkName);
                stmt.setInt(1, standId);
                stmt.setString(2, stationName);
                rs = stmt.executeQuery();

                if (!rs.next()) {
                    stmt = transconn.prepareStatement(insertStation);
                    int paramIndex = 1;
                    stmt.setString(paramIndex++, stationName);
                    stmt.setInt(paramIndex++, standId);
                    stmt.executeUpdate();

                    // Log the action
                    stmt = transconn.prepareStatement("SELECT LAST_INSERT_ID()");
                    rs = stmt.executeQuery();
                    if (rs.next()) {
                        int newId = rs.getInt(1);
                        String logMessage = "Adding a station named '" + stationName + "'";
                        logger.portalDetail(callerId, "addStation", standId, "station", newId, logMessage, transconn);
                    }
                } else {
                    addErrorDetail(toAppend, "A station with that name already exists");
                }
            } else {
                addErrorDetail(toAppend, "Invalid Location ID");
                logger.generalWarning("Invalid Reference ID Number: Location : No records found.");
            }

        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
            close(rs);
        }
    }
    //NischaySharma_02-Oct-2009_End

    private void addCostCenter(Element toHandle, Element toAppend) throws HandlerException {
        int costCenterId = HandlerUtils.getRequiredInteger(toHandle, "ccId");
        String costCenterName = HandlerUtils.getRequiredString(toHandle, "costCenterName");
        int locationId = HandlerUtils.getRequiredInteger(toHandle, "locationId");
        int barId = HandlerUtils.getOptionalInteger(toHandle, "barId");
        int zoneId = HandlerUtils.getOptionalInteger(toHandle, "zoneId");
        int standId = HandlerUtils.getOptionalInteger(toHandle, "standId");
        int stationId = HandlerUtils.getOptionalInteger(toHandle, "stationId");
        int callerId = getCallerId(toHandle);


        String checkCostCenter              = " SELECT id FROM costCenter WHERE location = ? AND bar = ? AND ccID = ? ";
        String insertCostCenter             = "INSERT INTO costCenter (bar, name, ccID, location) VALUES (?, ?, ?, ?)";
        String insertCostCenterByStation    = "INSERT INTO costCenter (bar, zone, station, name, ccID, location) VALUES (?, ?, ?, ?, ?, ?)";

        PreparedStatement stmt = null;
        ResultSet rs = null;

        try {
            boolean standCheckForeignKey = checkForeignKey("location", locationId, transconn);
            if (standCheckForeignKey) {
                stmt = transconn.prepareStatement(checkCostCenter);
                stmt.setInt(1, locationId);
                stmt.setInt(2, costCenterId);
                stmt.setInt(3, (barId > 0 ? barId : standId));
                rs = stmt.executeQuery();
                if (!rs.next()) {
                    int paramIndex = 1;
                    if (barId > 0) {
                        stmt = transconn.prepareStatement(insertCostCenter);
                        stmt.setInt(paramIndex++, barId);
                    } else {
                        stmt = transconn.prepareStatement(insertCostCenterByStation);
                        stmt.setInt(paramIndex++, standId);
                        stmt.setInt(paramIndex++, zoneId);
                        stmt.setInt(paramIndex++, stationId);
                    }
                    stmt.setString(paramIndex++, costCenterName);
                    stmt.setInt(paramIndex++, costCenterId);
                    stmt.setInt(paramIndex++, locationId);
                    stmt.executeUpdate();

                    // Log the action
                    stmt = transconn.prepareStatement("SELECT LAST_INSERT_ID()");
                    rs = stmt.executeQuery();
                    if (rs.next()) {
                        int newId = rs.getInt(1);
                        String logMessage = "Adding a cost center named '" + costCenterName + "'";
                        logger.portalDetail(callerId, "addCostCenter", costCenterId, "costCenter", newId, logMessage, transconn);
                    }
                } else {
                    addErrorDetail(toAppend, "A cost center with that id already exists");
                }
            } else {
                addErrorDetail(toAppend, "Invalid Location ID");
                logger.generalWarning("Invalid Reference ID Number: Location : No records found.");
            }

        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
            close(rs);
        }
    }

    private String getUserAlertType(int alert) throws HandlerException {

        String selectAlerts                 = "SELECT description FROM userAlertType WHERE id = ?";
        PreparedStatement stmt              = null;
        ResultSet rs                        = null;
        String alertType                    = "Invalid Alert";
        try {
            stmt = transconn.prepareStatement(selectAlerts);
            stmt.setInt(1, alert);
            rs = stmt.executeQuery();
            if (rs.next()) {
                alertType                   = rs.getString(1);
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error in getUserAlertType: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(rs);
            close(stmt);
        }
        return alertType;
    }

    /**
     * The folowing code is to insert the user alert requirements for each user
     * that they provide - SR
     */
    private void addUserAlerts(Element toHandle, Element toAppend) throws HandlerException {

        int callerId                        = getCallerId(toHandle);
        String select                       = " SELECT id FROM userAlerts WHERE user=? AND alert=? AND tableId=? AND tableType=?; ";
        String insert                       = " INSERT INTO userAlerts (user, alert, tableId, tableType, time) " +
                                            " VALUES (?, ?, ?, ?, ?); ";
        SimpleDateFormat f                  = new SimpleDateFormat("HH:mm:ss");
        PreparedStatement stmt              = null, innerstmt = null;
        ResultSet rs                        = null;
        
        int userId                          = HandlerUtils.getRequiredInteger(toHandle, "userId");

        try {
            Iterator i                      = toHandle.elementIterator("addAlerts");
            while (i.hasNext()) {
                Element alerts              = (Element) i.next();
                int alert                   = HandlerUtils.getRequiredInteger(alerts, "alert");
                int tableId                 = HandlerUtils.getRequiredInteger(alerts, "tableId");
                int tableType               = HandlerUtils.getRequiredInteger(alerts, "tableType");
                
                //Check that this alert doesn't already exist in alerts for this user
                stmt                        = transconn.prepareStatement(select);
                stmt.setInt(1, userId);
                stmt.setInt(2, alert);
                stmt.setInt(3, tableId);
                stmt.setInt(4, tableType);
                rs                          = stmt.executeQuery();
                if (!rs.next()) {
                    int timeId              = 0;
                    stmt                    = transconn.prepareStatement(insert);
                    stmt.setInt(1, userId);
                    stmt.setInt(2, alert);
                    stmt.setInt(3, tableId);
                    stmt.setInt(4, tableType);
                    
                    Iterator j              = alerts.elementIterator("time");
                    if (j.hasNext()) {
                        String insertTimeTable
                                            = " INSERT INTO userAlertsTimeTable (time, day) VALUES (?, ?)";
                        
                        Element time        = (Element) j.next();
                        String alertTime    = HandlerUtils.getRequiredString(time, "alertTime");
                        int alertDay        = HandlerUtils.getOptionalInteger(time, "alertDay");
                        java.util.Date d    = f.parse(alertTime);
                        java.util.Date date = new java.util.Date(d.getTime() - getAlertOffsetInMillis(tableId, tableType));
                        
                        innerstmt           = transconn.prepareStatement(insertTimeTable);
                        innerstmt.setString(1, dateToString(date));
                        innerstmt.setInt(2, alertDay);
                        innerstmt.executeUpdate();

                        innerstmt           = transconn.prepareStatement("SELECT LAST_INSERT_ID()");
                        rs                  = innerstmt.executeQuery();
                        if (rs.next()) {
                            timeId          = rs.getInt(1);
                        }
                    }
                    stmt.setInt(5, timeId);
                    stmt.executeUpdate();
                } else {
                    logger.generalWarning("Alert '" + getUserAlertType(alert)  + "' already exists ");
                    addErrorDetail(toAppend, "Alert '" + getUserAlertType(alert)  + "' already exists ");
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
            close(innerstmt);
            close(rs);
        }
    }

    /**
     * The folowing code is to insert the super user alert requirements for each user
     * that they provide - SR
     */
    private void addSuperUserAlerts(Element toHandle, Element toAppend) throws HandlerException {

        int callerId                        = getCallerId(toHandle);
        String confirmSuperUser             = " SELECT id FROM user WHERE customer = 0 AND id=?; ";
        
        String select                       = " SELECT time FROM superUserAlerts WHERE user=? AND alert=?; ";
        String insert                       = " INSERT INTO superUserAlerts (user, alert, time) " +
                                            " VALUES (?, ?, ?); ";
        SimpleDateFormat f                  = new SimpleDateFormat("HH:mm:ss");
        PreparedStatement stmt              = null, innerstmt = null;
        ResultSet rs                        = null, innerrs = null;

        int userId                          = HandlerUtils.getRequiredInteger(toHandle, "userId");

        try {

            stmt                            = transconn.prepareStatement(confirmSuperUser);
            stmt.setInt(1, userId);
            rs                              = stmt.executeQuery();
            if (rs.next()) {
                Iterator i                  = toHandle.elementIterator("addAlerts");
                while (i.hasNext()) {
                    Element alerts          = (Element) i.next();
                    int alert               = HandlerUtils.getRequiredInteger(alerts, "alert");
                    
                    //Check that this alert doesn't already exist in alerts for this user
                    stmt                    = transconn.prepareStatement(select);
                    stmt.setInt(1, userId);
                    stmt.setInt(2, alert);
                    rs                      = stmt.executeQuery();
                    if (!rs.next()) {
                        int timeId          = 0;
                        stmt                = transconn.prepareStatement(insert);
                        stmt.setInt(1, userId);
                        stmt.setInt(2, alert);

                        Iterator j          = alerts.elementIterator("time");
                        if (j.hasNext()) {
                            Element time    = (Element) j.next();
                            String alertTime= HandlerUtils.getRequiredString(time, "alertTime");
                            int alertDay    = HandlerUtils.getOptionalInteger(time, "alertDay");
                            String insertTimeTable
                                            = " INSERT INTO userAlertsTimeTable (time, day) VALUES (?, ?)";

                            java.util.Date d= f.parse(alertTime);
                            java.util.Date date
                                            = new java.util.Date(d.getTime());

                            innerstmt       = transconn.prepareStatement(insertTimeTable);
                            innerstmt.setString(1, dateToString(date));
                            innerstmt.setInt(2, alertDay);
                            innerstmt.executeUpdate();

                            innerstmt       = transconn.prepareStatement("SELECT LAST_INSERT_ID()");
                            rs              = innerstmt.executeQuery();
                            if (rs.next()) {
                                timeId      = rs.getInt(1);
                            }
                        }
                        stmt.setInt(3, timeId);
                        stmt.executeUpdate();
                    } else if (alert == 4 || alert == 14) {
                        int timeId          = 0;
                        boolean timeExists  = false;
                        
                        Iterator j          = alerts.elementIterator("time");
                        if (j.hasNext()) {
                            Element time    = (Element) j.next();
                            String alertTime= HandlerUtils.getRequiredString(time, "alertTime");
                            int alertDay    = HandlerUtils.getOptionalInteger(time, "alertDay");
                            java.util.Date d= f.parse(alertTime);
                            java.util.Date date
                                            = new java.util.Date(d.getTime());

                            // checking for more than one allowable alert time
                            do {
                                timeId      = rs.getInt(1);
                                String selectTime
                                            = " SELECT id FROM userAlertsTimeTable WHERE id=? AND time=? AND day = ?; ";
                                stmt        = transconn.prepareStatement(selectTime);
                                stmt.setInt(1, timeId);
                                stmt.setString(2, dateToString(date));
                                stmt.setInt(3, alertDay);
                                innerrs     = stmt.executeQuery();
                                if (innerrs.next()) {
                                    timeExists
                                            = true;
                                }
                            } while (rs.next());


                            if (!timeExists) {
                                String insertTimeTable
                                            = " INSERT INTO userAlertsTimeTable (time, day) VALUES (?, ?)";
                                innerstmt   = transconn.prepareStatement(insertTimeTable);
                                innerstmt.setString(1, dateToString(date));
                                innerstmt.setInt(2, alertDay);
                                innerstmt.executeUpdate();

                                innerstmt    = transconn.prepareStatement("SELECT LAST_INSERT_ID()");
                                rs           = innerstmt.executeQuery();
                                if (rs.next()) {
                                    timeId   = rs.getInt(1);
                                }

                                stmt         = transconn.prepareStatement(insert);
                                stmt.setInt(1, userId);
                                stmt.setInt(2, alert);
                                stmt.setInt(3, timeId);
                                stmt.executeUpdate();
                            } else {
                                logger.generalWarning("Alert '" + getUserAlertType(alert)  + "' already exists for the specified time");
                                addErrorDetail(toAppend, "Alert '" + getUserAlertType(alert)  + "' already exists for the specified time");
                            }
                        } else {
                            logger.generalWarning("Alert '" + getUserAlertType(alert)  + "' already exists ");
                            addErrorDetail(toAppend, "Alert '" + getUserAlertType(alert)  + "' already exists ");
                        }
                    } else {
                        logger.generalWarning("Alert '" + getUserAlertType(alert)  + "' already exists ");
                        addErrorDetail(toAppend, "Alert '" + getUserAlertType(alert)  + "' already exists ");
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
            close(stmt);
            close(innerstmt);
            close(innerrs);
            close(rs);
        }
    }

    private void archiveReadings(Element toHandle, Element toAppend) throws HandlerException {

        String date                         = HandlerUtils.getRequiredString(toHandle, "date");
        String year                         = HandlerUtils.getRequiredString(toHandle, "year");
        boolean reading                     = HandlerUtils.getOptionalBoolean(toHandle, "reading");
        boolean sales                       = HandlerUtils.getOptionalBoolean(toHandle, "sales");
        boolean pulses                      = HandlerUtils.getOptionalBoolean(toHandle, "pulses");
        boolean cooler                      = HandlerUtils.getOptionalBoolean(toHandle, "cooler");
        boolean dataMod                     = HandlerUtils.getOptionalBoolean(toHandle, "dataMod");
        int interval                        = HandlerUtils.getRequiredInteger(toHandle, "interval");
        
        String selectArchiverDate           = "SELECT (lastDate + INTERVAL ? HOUR), lastDate FROM archiverDate WHERE id = 1;";
        String updateArchiverDate           = "UPDATE archiverDate SET lastDate = ? WHERE id = 1;";
        
        String insertReadingArchive         = "INSERT INTO reading" + year + " (date, line, value, quantity, type) " +
                                            " (SELECT date, line, value, quantity, type FROM reading WHERE date < ?);";
        String deleteReading                = "DELETE FROM reading WHERE date < ?";
        
        String insertSalesArchive           = "INSERT INTO sales" + year + " (location, pluNumber, costCenter, quantity, date, checkId, reportRecordId) " +
                                            " (SELECT location, pluNumber, costCenter, quantity, date, checkId, reportRecordId FROM sales WHERE date < ?);";
        String deleteSales                  = "DELETE FROM sales WHERE date < ?";

        String insertPulsesArchive          = "INSERT INTO pluses" + year + " (date, line, value, quantity, type) " +
                                            " (SELECT date, line, value, quantity, type FROM pluses WHERE date < ?);";
        String deletePulses                 = "DELETE FROM pluses WHERE date < ?";
        
        String selectCoolers                = "SELECT c.id FROM cooler c LEFT JOIN locationDetails lD ON lD.location = c.location WHERE lD.active = 1 AND c.system < 999";
        String insertCoolerArchive          = "INSERT INTO coolerTemperature" + year + " (cooler, value, date) " +
                                            " (SELECT cooler, value, date FROM coolerTemperature WHERE cooler = ? AND date < ?);";
        
        String deleteCooler                 = "DELETE FROM coolerTemperature WHERE cooler = ? AND date < ?";

        String selectDataModId              = "SELECT id FROM dataModNew WHERE id < 12010960 ORDER BY id DESC LIMIT 1;";
        String deleteDataMod                = "DELETE FROM dataModNew WHERE id BETWEEN (? - 200000) AND (? - 100001);";
        
        PreparedStatement stmt              = null;
        ResultSet rs                        = null;

        try {
            String startDate                = "";
            stmt                            = transconn.prepareStatement(selectArchiverDate);
            stmt.setInt(1, interval);
            rs                              = stmt.executeQuery();
            if (rs.next()) {
                date                        = rs.getString(1);
                startDate                   = rs.getString(2);
            }
            
            if (reading) {
                logger.debug("Adding Reading Archive: " + date);
                stmt                        = transconn.prepareStatement(insertReadingArchive);
                stmt.setString(1, date);
                stmt.executeUpdate();

                //logger.debug("Deleting Reading: " + date);
                stmt                        = transconn.prepareStatement(deleteReading);
                stmt.setString(1, date);
                stmt.executeUpdate();
            }

            if (pulses) {
                logger.debug("Adding Pulses Archive: " + date);
                stmt                        = transconn.prepareStatement(insertPulsesArchive);
                stmt.setString(1, date);
                stmt.executeUpdate();

                //logger.debug("Deleting Reading: " + date);
                stmt                        = transconn.prepareStatement(deletePulses);
                stmt.setString(1, date);
                stmt.executeUpdate();
            }
            if (sales) {
                logger.debug("Adding Sales Archive: " + date);
                stmt                        = transconn.prepareStatement(insertSalesArchive);
                stmt.setString(1, date);
                stmt.executeUpdate();

                //logger.debug("Deleting Sales: " + date);
                stmt                        = transconn.prepareStatement(deleteSales);
                stmt.setString(1, date);
                stmt.executeUpdate();
            }

            if (cooler) {
                logger.debug("Retreiving Coolers");
                stmt                        = transconn.prepareStatement(selectCoolers);
                rs                          = stmt.executeQuery();
                while (rs.next()) {
                    logger.debug("Adding Cooler Archive: " + date);
                    stmt                    = transconn.prepareStatement(insertCoolerArchive);
                    stmt.setInt(1, rs.getInt(1));
                    stmt.setString(2, date);
                    stmt.executeUpdate();

                    logger.debug("Deleting Cooler: " + date);
                    stmt                    = transconn.prepareStatement(deleteCooler);
                    stmt.setInt(1, rs.getInt(1));
                    stmt.setString(2, date);
                    stmt.executeUpdate();
                }
                close(rs);
            }

            if (dataMod) {
                stmt                        = transconn.prepareStatement(selectDataModId);
                rs                          = stmt.executeQuery();
                if (rs.next()) {
                    stmt                        = transconn.prepareStatement(deleteDataMod);
                    stmt.setInt(1, rs.getInt(1));
                    stmt.setInt(2, rs.getInt(1));
                    stmt.executeUpdate();
                }
            }
            
            stmt                            = transconn.prepareStatement(updateArchiverDate);
            stmt.setString(1, date);
            stmt.executeUpdate();
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
        }
    }


    private void innodbReadings(Element toHandle, Element toAppend) throws HandlerException {

        boolean reading                     = HandlerUtils.getOptionalBoolean(toHandle, "reading");
        boolean sales                       = HandlerUtils.getOptionalBoolean(toHandle, "sales");
        boolean cooler                      = HandlerUtils.getOptionalBoolean(toHandle, "cooler");
        int interval                        = HandlerUtils.getOptionalInteger(toHandle, "interval");
        
        String selectInnodbId               = "SELECT reading, sales, salesOunces, pulses, cooler FROM innodbId WHERE id = 1;";
        String updateInnodbId               = "UPDATE innodbId SET reading = ?, sales = ?, salesOunces = ?, pulses = ?, cooler = ? WHERE id = 1;";
        
        String insertReading                = "INSERT INTO reading (date, line, value, quantity, type) " +
                                            " (SELECT date, line, value, quantity, type FROM reading WHERE id BETWEEN ? AND ?);";
        
        String insertSales                  = "INSERT INTO sales (location, pluNumber, costCenter, quantity, date, checkId, reportRecordId) " +
                                            " (SELECT location, pluNumber, costCenter, quantity, date, checkId, reportRecordId FROM sales2015 WHERE sid BETWEEN ? AND ?);";

        String insertSalesOunces            = "INSERT INTO salesOunce (location, product, costCenter, quantity, date, checkId, reportRecordId) " +
                                            " (SELECT location, product, costCenter, quantity, date, checkId, reportRecordId FROM salesOunce WHERE sid BETWEEN ? AND ?);";

        String insertPulses                 = "INSERT INTO pulse (date, line, value, location, system) " +
                                            " (SELECT date, line, value, location, system FROM pulses WHERE id BETWEEN ? AND ?);";
        
        String insertCooler                 = "INSERT INTO coolerTemperature (cooler, value, date) " +
                                            " (SELECT cooler, value, date FROM coolerTemperature WHERE id BETWEEN ? AND ?);";
        
        PreparedStatement stmt              = null;
        ResultSet rs                        = null;

        try {
            stmt                            = transconn.prepareStatement(selectInnodbId);
            rs                              = stmt.executeQuery();
            if (rs.next()) {
                int minReading              = rs.getInt(1);
                int minSales                = rs.getInt(2);
                int minSalesOunces          = rs.getInt(3);
                int minPulses               = rs.getInt(4);
                int minCooler               = rs.getInt(5);
                int maxReading              = minReading + interval;
                int maxSales                = minSales + interval;
                int maxSalesOunces          = minSalesOunces + interval;
                int maxPulses               = minPulses + interval;
                int maxCooler               = minCooler + interval;
            
            if (reading) {
                stmt                        = transconn.prepareStatement(insertReading);
                stmt.setInt(1, minReading);
                stmt.setInt(2, maxReading - 1);
                stmt.executeUpdate();
            }
            
            if (sales) {
                stmt                        = transconn.prepareStatement(insertSales);
                stmt.setInt(1, minSales);
                stmt.setInt(2, maxSales - 1);
                stmt.executeUpdate();
            }
            
            stmt                            = transconn.prepareStatement(updateInnodbId);
            stmt.setInt(1, maxReading);
            stmt.setInt(2, maxSales);
            stmt.setInt(3, maxSalesOunces);
            stmt.setInt(4, maxPulses);
            stmt.setInt(5, maxCooler);
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

    /** Add a bar to an existing location
     *  <locationId>000
     *  <name>"String"
     *
     *  The new bar must have a unique name for this location, or an error detail will be returned
     *
     */
    private void addBevBox(Element toHandle, Element toAppend) throws HandlerException {

        int locationId                      = HandlerUtils.getOptionalInteger(toHandle, "locationId");
        int bossId                          = HandlerUtils.getOptionalInteger(toHandle, "bossId");
        String name                         = HandlerUtils.getRequiredString(toHandle, "name");
        int totalSystems                    = HandlerUtils.getOptionalInteger(toHandle, "totalSystems");
        int startSystem                     = HandlerUtils.getOptionalInteger(toHandle, "startSystem");
        int systemInterval                  = HandlerUtils.getRequiredInteger(toHandle, "systemInterval");
        String version                      = HandlerUtils.getRequiredString(toHandle, "version");
        String mac                          = HandlerUtils.getRequiredString(toHandle, "mac");
        int dhcp                            = HandlerUtils.getRequiredInteger(toHandle, "dhcp");
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

        String checkName                    = " SELECT LCASE(b.name), b.startSystem, b.totalSystems FROM bevBox b ";
        ArrayList<Integer> systemsArray     = new ArrayList<Integer>();
        ArrayList<String> namesArray        = new ArrayList<String>();

        PreparedStatement stmt              = null;
        ResultSet rs                        = null;

        try {
            boolean checkForeignKey         = false;
            int parameter                   = 0;
            if (locationId >= 0) {
                checkForeignKey             = checkForeignKey("location", locationId, transconn);
                checkName                   += " WHERE b.location = ? ";
                parameter                   = locationId;
            }
            if (bossId >= 0) {
                checkForeignKey             = checkForeignKey("BOSS_Location", bossId, transconn);
                checkName                   += " LEFT JOIN BOSS_Location BL ON BL.usbn_location = b.location WHERE BL.id = ? ";
                parameter                   = bossId;

                stmt                        = transconn.prepareStatement("SELECT usbn_location FROM BOSS_Location WHERE id = ?");
                stmt.setInt(1, bossId);
                rs                          = stmt.executeQuery();
                if (rs.next()) {
                    locationId              = rs.getInt(1);
                }
            }
            
            if (checkForeignKey) {
                stmt                        = transconn.prepareStatement(checkName);
                stmt.setInt(1, parameter);
                rs                          = stmt.executeQuery();
                while (rs.next()) {
                    for (int i = rs.getInt(2); i < rs.getInt(3); i++) {
                        systemsArray.add(i);
                    }
                    namesArray.add(rs.getString(1));
                }

                for (int i = startSystem; i < (startSystem + totalSystems); i++) {
                    if (systemsArray.contains(i)) { throw new HandlerException("One or more of the System numbers for " + name + " already exists for another bevBox");}
                }

                if (!namesArray.contains(name)) {

                    if (locationId < 1) {

                    }

                    String insert           = " INSERT INTO bevBox (location, name, totalSystems, startSystem, systemInterval, version, mac, dhcp) VALUES (?,?,?,?,?,?,?,?)";
                    stmt                    = transconn.prepareStatement(insert);
                    stmt.setInt(1, locationId);
                    stmt.setString(2, name);
                    stmt.setInt(3, totalSystems);
                    stmt.setInt(4, startSystem);
                    stmt.setInt(5, systemInterval);
                    stmt.setString(6, version);
                    stmt.setString(7, mac);
                    stmt.setInt(8, dhcp);
                    stmt.executeUpdate();
                    
                    stmt                    = transconn.prepareStatement("SELECT LAST_INSERT_ID()");
                    rs                      = stmt.executeQuery();
                    if (rs.next()) {
                        int newId           = rs.getInt(1);
                        String logMessage   = "Adding a bevBox named '" + name + "'";
                        logger.portalDetail(callerId, "addBevBox", locationId, "bevBox", newId, logMessage, transconn);

                        boolean details     = HandlerUtils.getOptionalBoolean(toHandle, "details");
                        if (details) {
                            String ip       = HandlerUtils.getRequiredString(toHandle, "ip");
                            String gateway  = HandlerUtils.getRequiredString(toHandle, "gateway");
                            String netmask  = HandlerUtils.getRequiredString(toHandle, "netmask");
                            String dns1     = HandlerUtils.getRequiredString(toHandle, "dns1");
                            String dns2     = HandlerUtils.getRequiredString(toHandle, "dns2");

                            String updateDetails
                                            = "UPDATE bevBox SET ip = INET_ATON(?), gateway = INET_ATON(?), netmask = INET_ATON(?), dns1 = INET_ATON(?), dns = INET_ATON(?) " +
                                            " WHERE id = ? ";
                            stmt            = transconn.prepareStatement(updateDetails);
                            stmt.setString(1, ip);
                            stmt.setString(2, gateway);
                            stmt.setString(3, netmask);
                            stmt.setString(4, dns1);
                            stmt.setString(5, dns2);
                            stmt.setInt(6, newId);
                            stmt.executeUpdate();
                        }
                        
                        boolean coords      = HandlerUtils.getOptionalBoolean(toHandle, "coords");
                        if (coords) {
                            double latitude = HandlerUtils.getRequiredDouble(toHandle, "latitude");
                            double longitude= HandlerUtils.getRequiredDouble(toHandle, "longitude");
                            String updateCoords
                                            = "UPDATE bevBox SET latitude = ?, longitude = ? WHERE id = ? ";
                            stmt            = transconn.prepareStatement(updateCoords);
                            stmt.setDouble(1, latitude);
                            stmt.setDouble(2, longitude);
                            stmt.setInt(3, newId);
                            stmt.executeUpdate();
                        }
                    }
                } else {
                    addErrorDetail(toAppend, "A bevBox with that name already exists");
                }
            } else {
                addErrorDetail(toAppend, "Invalid Location ID");
                logger.generalWarning("Invalid Reference ID Number: Location : No records found.");
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
            close(rs);
        }

    }

    private void addSaveTheBeerLog(Element toHandle, Element toAppend) throws HandlerException {

        int userId                          = HandlerUtils.getRequiredInteger(toHandle, "userId");
        String insert                       = "INSERT INTO saveTheBeer (user) VALUES (?);";
        PreparedStatement stmt              = null;

        try {
            stmt                            = transconn.prepareStatement(insert);
            stmt.setInt(1, userId);
            stmt.executeUpdate();
        } catch (SQLException sqle) {
            logger.dbError("Database error in addLoginLog: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
        }
    }

    private void addSaveTheBeerData(Element toHandle, Element toAppend) throws HandlerException {
        String date                         = HandlerUtils.getRequiredString(toHandle, "date");
        int overwrite                       = HandlerUtils.getOptionalInteger(toHandle, "overwrite");
        PreparedStatement stmt              = null;
        ResultSet rs                        = null, dataRS = null;

        String selectCustomer               = "SELECT id, name FROM customer WHERE isActive = 1;";
        String selectRanking                = "SELECT l.id ID, ROUND((((SUM(t.sold) - SUM(t.poured))/SUM(t.poured))*100),2) Var FROM tierSummary t " +
                                            " LEFT JOIN location l ON l.id = t.location LEFT JOIN locationDetails lD ON lD.location = l.id WHERE t.tier < 4 " +
                                            " AND lD.active = 1 AND l.customer = ? AND t.date BETWEEN SUBDATE(?,INTERVAL 30 DAY) AND SUBDATE(?,INTERVAL 1 DAY) " +
                                            " GROUP BY l.id ORDER BY var DESC";
        String selectData                   = "SELECT l.id ID, SUM(t.poured)/15 Poured, ((SUM(t.poured)*.2)-(SUM(t.poured) - SUM(t.sold)))/15 Saved, (SUM(t.poured) - SUM(t.sold))/15 Loss, " +
                                            " ROUND((((SUM(t.sold) - SUM(t.poured))/SUM(t.poured))*100),2) Var FROM tierSummary t LEFT JOIN location l ON l.id = t.location " +
                                            " LEFT JOIN locationDetails lD ON lD.location = l.id WHERE t.tier < 4 AND lD.active = 1 AND l.customer = ? AND " +
                                            " t.date BETWEEN SUBDATE(?,INTERVAL 7 DAY) AND SUBDATE(?,INTERVAL 1 DAY) GROUP BY l.id ORDER BY var DESC";

        String insertSTBSummary             = "INSERT INTO saveTheBeerSummary (location, poured, saved, loss, variance, rank, date) VALUES (?,?,?,?,?,?,?);";
        String deleteSTBSummary             = "DELETE FROM saveTheBeerSummary WHERE date = ? AND location = ?";

        try {
            stmt                            = transconn.prepareStatement(selectCustomer);
            rs                              = stmt.executeQuery();
            while (rs.next()) {
                HashMap<Integer,Integer> rankMap
                                            = new HashMap<Integer,Integer>();
                int customerId              = rs.getInt(1);
                int rank                    = 1;
                int tempLID                 = 0;
                stmt                        = transconn.prepareStatement(selectRanking);
                stmt.setInt(1, customerId);
                stmt.setString(2, date);
                stmt.setString(3, date);
                dataRS                      = stmt.executeQuery();
                while (dataRS.next()) {
                    tempLID                 = dataRS.getInt(1);
                    rankMap.put(dataRS.getInt(1), rank++);
                }
                if (rankMap.size() <= 1) {
                    rankMap.put(tempLID, 0);
                }

                stmt                        = transconn.prepareStatement(selectData);
                stmt.setInt(1, customerId);
                stmt.setString(2, date);
                stmt.setString(3, date);
                dataRS                      = stmt.executeQuery();
                while (dataRS.next()) {
                    int locationId          = dataRS.getInt(1);
                    if (overwrite > 0) {
                        stmt                = transconn.prepareStatement(deleteSTBSummary);
                        stmt.setInt(1, locationId);
                        stmt.setString(2, date);
                        stmt.executeUpdate();
                    }
                    
                    int counter             = 1;
                    double saved            = dataRS.getDouble(3);
                    double loss             = dataRS.getDouble(4);
                    stmt                    = transconn.prepareStatement(insertSTBSummary);
                    stmt.setInt(counter++, dataRS.getInt(1));
                    stmt.setDouble(counter++, dataRS.getDouble(2));
                    stmt.setDouble(counter++, (saved < 0.0) ? 0.0 : saved);
                    stmt.setDouble(counter++, (loss < 0.0) ? 0.0 : loss);
                    stmt.setDouble(counter++, dataRS.getDouble(5));
                    stmt.setInt(counter++, rankMap.get(locationId));
                    stmt.setString(counter++, date);
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

    private void addLineCleaning(Element toHandle, Element toAppend) throws HandlerException {
        int locationId                      = HandlerUtils.getRequiredInteger(toHandle, "locationId");
        int delayMinutes                    = HandlerUtils.getOptionalInteger(toHandle, "delayMinutes");
        String startTime                    = HandlerUtils.getRequiredString(toHandle, "time");
        if (delayMinutes < 0) {
            updateLineCleaning(locationId, startTime);
        }
        int callerId                        = getCallerId(toHandle);
        int osPlatform                      = HandlerUtils.getOptionalInteger(toHandle, "osPlatform");
        int mobileUserId                    = HandlerUtils.getOptionalInteger(toHandle, "mobileUserId");         
        String selectLineCleaning           = "SELECT id,TIMESTAMPDIFF(MINUTE,startTime,endTime) FROM lineCleaning WHERE location = ? AND NOW() BETWEEN startTime and endTime";
        String selectGlanolaLineCleaning    = "SELECT id,TIMESTAMPDIFF(MINUTE,startTime,endTime) FROM glanolaLineCleaning WHERE location = ? AND NOW() BETWEEN startTime and endTime";
        
        String insertLineCleaning           = "INSERT INTO lineCleaning (location, startTime, endTime) VALUES (?, NOW(), ADDDATE(NOW(), INTERVAL ? MINUTE))";
        String insertGlanolaLineCleaning    = "INSERT INTO glanolaLineCleaning (location, line, startTime, endTime) VALUES (?, ?, NOW(), ADDDATE(NOW(), INTERVAL ? MINUTE))";
        String updateGlanolaLineCleaning    = "UPDATE glanolaLineCleaning SET endTime = NOW() WHERE location = ? AND line = ? AND NOW() BETWEEN startTime and endTime;";
        PreparedStatement stmt              = null;
        ResultSet rs                        = null;

        try {
            boolean isCleaning              = false;
            stmt                            = transconn.prepareStatement(selectLineCleaning);
            stmt.setInt(1, locationId);
            rs                              = stmt.executeQuery();
            if (rs.next()) {
                isCleaning                  = true;
            } else {
                stmt                        = transconn.prepareStatement(selectGlanolaLineCleaning);
                stmt.setInt(1, locationId);
                rs                          = stmt.executeQuery();
                if (rs.next()) {
                    isCleaning              = true;
                } 
            }
            if(!isCleaning && callerId > 0 && locationId > 0 && isValidAccessUser(callerId, locationId, false)){
                boolean locationClean       = true;
                Iterator lineIterator       = toHandle.elementIterator("line");
                while (lineIterator.hasNext()) {
                    locationClean           = false;
                    Element line            = (Element) lineIterator.next();
                    int lineId              = HandlerUtils.getRequiredInteger(line, "id");
                    stmt                    = transconn.prepareStatement(updateGlanolaLineCleaning);
                    stmt.setInt(1, locationId);
                    stmt.setInt(2, lineId);
                    //stmt.executeUpdate();

                    stmt                    = transconn.prepareStatement(insertGlanolaLineCleaning);
                    stmt.setInt(1, locationId);
                    stmt.setInt(2, lineId);
                    stmt.setInt(3, delayMinutes);
                    stmt.executeUpdate();
                }
                if(locationClean){
                    stmt                    = transconn.prepareStatement(insertLineCleaning);
                    stmt.setInt(1, locationId);
                    stmt.setInt(2, delayMinutes);
                    stmt.executeUpdate();
                }
                logger.debug("Added Line Cleaning");
                if(osPlatform != 1 && callerId > 0) {
                    if(mobileUserId < 1){
                        mobileUserId        = getMobileUserId(callerId);
                    }
                    addUserHistory(callerId, "addLineCleaning", locationId, "Add Line Cleaning", mobileUserId,0);
                } else {
                    logger.portalDetail(callerId, "addLineCleaning", locationId, "addLineCleaning",0, "addLineCleaning "+startTime +" : "+delayMinutes, transconn);
                }
            } else {
                if(isCleaning) {
                    addErrorDetail(toAppend, "Line Cleaning alrady initiated!"  );
                } else {
                    addErrorDetail(toAppend, "Invalid Access"  );
                }
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
        }
    }

    private void updateLineCleaning(int locationId, String endTime) throws HandlerException {

        String selectLineCleaning           = "SELECT id FROM lineCleaning WHERE location = ? AND ? BETWEEN startTime and endTime";
        String selectGlanolaLineCleaning    = "SELECT id FROM glanolaLineCleaning WHERE location = ? AND ? BETWEEN startTime and endTime";
        String updateLineCleaning           = "UPDATE lineCleaning SET endTime = ? WHERE id = ?";
        String updateGlanolaLineCleaning    = "UPDATE glanolaLineCleaning SET endTime = ? WHERE id = ?";
        PreparedStatement stmt              = null;
        ResultSet rs = null;

        try {
            stmt                            = transconn.prepareStatement(selectLineCleaning);
            stmt.setInt(1, locationId);
            stmt.setString(2, endTime);
            rs                              = stmt.executeQuery();
            if (rs.next()) {
                stmt                        = transconn.prepareStatement(updateLineCleaning);
                stmt.setString(1, endTime);
                stmt.setInt(2, rs.getInt(1));
                stmt.executeUpdate();
            }
            
            stmt                            = transconn.prepareStatement(selectGlanolaLineCleaning);
            stmt.setInt(1, locationId);
            stmt.setString(2, endTime);
            rs                              = stmt.executeQuery();
            while(rs.next()) {
                stmt                        = transconn.prepareStatement(updateGlanolaLineCleaning);
                stmt.setString(1, endTime);
                stmt.setInt(2, rs.getInt(1));
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

    private void addDemoData(Element toHandle, Element toAppend) throws HandlerException {
        String date                         = HandlerUtils.getRequiredString(toHandle, "date");
        String updateReadingData            = "UPDATE reading SET date = ADDDATE(date, INTERVAL 66 DAY) WHERE line IN (48922, 48923, 48924, 48925, 48926, 48927, 48928, " +
                                            " 48929, 48930, 48931, 48932, 48933, 48934, 48935, 48936, 48937, 48938, 48939, 48940, 48941, 48942, 48943, 48944, 48945, 48946, " +
                                            " 48947, 48948, 48949, 48950, 48951, 48952, 48953, 49089, 49090, 49091, 49092, 49093, 49094, 49095, 49096, 49097, 49098, 49099, " +
                                            " 49100, 49101, 49102, 49103, 49104, 49105, 49106, 49107, 49108, 49109, 49110, 49111, 49112, 49113, 49114, 49115, 49116, 49396, " +
                                            " 51997, 51998, 51999, 52000, 52001, 52002, 52003, 52004, 52005, 52006, 52007, 52008, 52009, 52010, 52011, 52012, 58978, 58979, " +
                                            " 58980, 58981, 58982, 58983, 58984, 58985, 58986, 58987, 58988, 58989, 58990, 58991, 58992, 58993, 58994, 58995, 58996, 58997, " +
                                            " 58998, 58999, 59000, 59001, 59002, 59003, 59004, 59005, 59006, 59007, 59102, 59103, 59104, 59105, 59106, 59107, 59108, 59109, " +
                                            " 59110, 59111, 59112, 59113, 59114, 59115, 59116, 59117, 59118, 59119, 59120, 59121, 59122, 59123, 59124, 59125, 59126, 59127, " +
                                            " 59128, 59129, 59130, 59721, 60512, 60513, 60514, 60515, 61533, 61534, 61535, 61536, 61537, 61538, 61539, 61540, 61541, 61542, " +
                                            " 61543, 61544, 61545, 61546, 61547, 61548, 61549, 61550, 61551, 61552, 61553, 77311, 90536, 90537, 90538, 99911, 99955, 99956, " +
                                            " 101819, 102748, 102749, 102750, 102751, 102752, 102753, 102754, 102755, 102756, 102757, 102758, 102759, 102760, 102761, 102762, " +
                                            " 102763, 102764, 103208, 103209, 107156, 107157, 107158, 107159, 107160, 107161, 107162, 107163, 107164, 107165, 107166, 107986, " +
                                            " 107987, 107988, 107989, 107990, 107991, 107992, 110326, 110327, 110328, 110329, 110330, 110331, 110332, 110333, 110334, 110754, " +
                                            " 115986, 115987, 115988, 115989, 115990, 115991, 115992, 115993, 117832, 117833, 117834, 117835, 117836, 117837, 117838, 117839, " +
                                            " 119700) AND date BETWEEN SUBDATE(?, INTERVAL 1 DAY) AND ?;";
        String updateSalesData              = "UPDATE sales SET date = ADDDATE(date, INTERVAL 66 DAY) WHERE location = 136 AND date BETWEEN SUBDATE(?, INTERVAL 1 DAY) " +
                                            " AND ?;";
        PreparedStatement stmt              = null;

        try {
            stmt                            = transconn.prepareStatement(updateReadingData);
            stmt.setString(1, date);
            stmt.setString(2, date);
            stmt.executeUpdate();

            stmt                            = transconn.prepareStatement(updateSalesData);
            stmt.setString(1, date);
            stmt.setString(2, date);
            stmt.executeUpdate();
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
        }
    }

    private void addCustomerPeriods(Element toHandle, Element toAppend) throws HandlerException {

        int callerId                        = getCallerId(toHandle);
        int customerId                      = HandlerUtils.getRequiredInteger(toHandle, "customerId");
        String date                         = HandlerUtils.getRequiredString(toHandle, "date");
        String start                        = HandlerUtils.getRequiredString(toHandle, "start");
        String end                          = HandlerUtils.getRequiredString(toHandle, "end");
        String details                      = HandlerUtils.getRequiredString(toHandle, "details");

        String insertCustomerPeriods        = " INSERT INTO customerPeriods (customer, date, start, end, description) VALUES (?, ?, ?, ?, ?); ";

        PreparedStatement stmt              = null;
        try {
            stmt                            = transconn.prepareStatement(insertCustomerPeriods);
            stmt.setInt(1,customerId);
            stmt.setString(2,date);
            stmt.setString(3,start);
            stmt.setString(4,end);
            stmt.setString(5,details);
            stmt.executeUpdate();
        } catch (SQLException sqle) {
            logger.dbError("Database error in addCustomerPeriods: "+sqle.toString());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
        }
    }
    
    
    private void addErrorLogs(Element toHandle, Element toAppend) throws HandlerException {

        
        int platform                        = HandlerUtils.getRequiredInteger(toHandle, "platform");
        DateTimeParameter timestamp         = HandlerUtils.getRequiredTimestamp(toHandle, "timestamp");
        String message                      = HandlerUtils.getRequiredString(toHandle, "message");
       

        String insertErrorLog        = " INSERT INTO errorLogs (platform, timestamp, message ) VALUES (?, ?, ?); ";

        PreparedStatement stmt              = null;
        try {
            stmt                            = transconn.prepareStatement(insertErrorLog);
            stmt.setInt(1,platform);
            stmt.setString(2,timestamp.toString());
            stmt.setString(3,message);
            stmt.executeUpdate();
        } catch (SQLException sqle) {
            logger.dbError("Database error in addErrorLogs: "+sqle.toString());
            throw new HandlerException(sqle);
        } finally {
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
    
    
    private void addUpdateLocationLogo(Element toHandle, Element toAppend) throws Exception {
        int callerId                        = getCallerId(toHandle); 
        int location                        = HandlerUtils.getRequiredInteger(toHandle, "locationId");
        String logo                         = HandlerUtils.getOptionalString(toHandle, "logo");
        String mobile                       = HandlerUtils.getOptionalString(toHandle, "mobile");
        String pdf                          = HandlerUtils.getOptionalString(toHandle, "pdf");
        String bbtv                         = HandlerUtils.getOptionalString(toHandle, "bbtv");
        String online                       = HandlerUtils.getOptionalString(toHandle, "online");
        
        
        
        PreparedStatement stmt              = null;
        ResultSet rs                        = null;
        String selectLogo                   = "SELECT  logo FROM locationGraphics  WHERE  location = ? ";
        String insertLogo                   = "INSERT INTO locationGraphics (location, logo,mobile,pdf,bbtv,online) VALUES (?,?,?,?,?,?)";
        String updateLogo                   = "UPDATE locationGraphics SET logo = ?  WHERE location = ? ; ";
        String updateMobile                 = "UPDATE locationGraphics SET mobile = ?  WHERE location = ? ; ";
        String updatePdf                    = "UPDATE locationGraphics SET pdf = ?  WHERE location = ? ; ";
        String updateBBTV                   = "UPDATE locationGraphics SET bbtv = ?  WHERE location = ? ; ";
        String updateOnline                 = "UPDATE locationGraphics SET online = ?  WHERE location = ? ; ";
        String updateBeerBoard              = "UPDATE locationBeerBoardMap SET logo = ?  WHERE location = ? ; ";
        try {
            stmt                            = transconn.prepareStatement(selectLogo);
            stmt.setInt(1, location);
            rs                              = stmt.executeQuery();
            if(rs.next()) {
                if(logo!=null && !logo.equals("")){
                    stmt                    = transconn.prepareStatement(updateLogo);
                    stmt.setString(1, logo);
                    stmt.setInt(2, location);
                    stmt.executeUpdate();
                }
                if(mobile!=null && !mobile.equals("")){
                    stmt                    = transconn.prepareStatement(updateMobile);
                    stmt.setString(1, mobile);
                    stmt.setInt(2, location);
                    stmt.executeUpdate();
                }
                if(bbtv!=null && !bbtv.equals("")){
                    stmt                    = transconn.prepareStatement(updateBBTV);
                    stmt.setString(1, bbtv);
                    stmt.setInt(2, location);
                    stmt.executeUpdate();
                }
                if(pdf!=null && !pdf.equals("")){
                    stmt                    = transconn.prepareStatement(updatePdf);
                    stmt.setString(1, pdf);
                    stmt.setInt(2, location);
                    stmt.executeUpdate();
                }
                if(online!=null && !online.equals("")){
                    stmt                    = transconn.prepareStatement(updateOnline);
                    stmt.setString(1, online);
                    stmt.setInt(2, location);
                    stmt.executeUpdate();
                }
                
                 
            } else {
                stmt                        = transconn.prepareStatement(insertLogo);
                stmt.setInt(1, location);
                stmt.setString(2, HandlerUtils.nullToEmpty(logo));              
                stmt.setString(3, HandlerUtils.nullToEmpty(mobile));   
                stmt.setString(4, HandlerUtils.nullToEmpty(pdf));   
                stmt.setString(5, HandlerUtils.nullToEmpty(bbtv));   
                stmt.setString(6, HandlerUtils.nullToEmpty(online));   
                stmt.executeUpdate();
                
            }
            stmt                            = transconn.prepareStatement(updateBeerBoard);
            stmt.setInt(1, 1);
            stmt.setInt(2, location);
            stmt.executeUpdate();
            logger.portalDetail(callerId, "addUpdateLocationLogo", location, "locationLogo", 0, "Logo-"+logo, transconn);
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
            close(rs);
        }
    }
    
    
    private void addUpdateBBTVFonts(Element toHandle, Element toAppend) throws HandlerException {

       int callerId                         = getCallerId(toHandle);
       String name                          = HandlerUtils.getRequiredString(toHandle, "name");
       String file                          = HandlerUtils.getRequiredString(toHandle, "file");
              

       PreparedStatement stmt               = null;
       ResultSet rs                         = null;                   
       
       String insertFont                = "INSERT INTO fonts (name, fileName) VALUES (?, ?); ";
       String updateFont                = "UPDATE fonts SET fileName= ?  WHERE id = ? ; ";
       String selectFont                = "SELECT id FROM fonts WHERE name = ?; ";

       try {
           stmt                            = transconn.prepareStatement(selectFont);
           stmt.setString(1, name);
           rs                              = stmt.executeQuery();
           if (rs.next()) {
           stmt                            = transconn.prepareStatement(updateFont);               
           stmt.setString(1, file);
           stmt.setString(2, name);
           stmt.executeUpdate();
           } else {
               stmt                            = transconn.prepareStatement(insertFont);
               stmt.setString(1, name);
               stmt.setString(2, file);
               stmt.executeUpdate();
               } 
       }catch (SQLException sqle) {
           logger.dbError("Database error: "+sqle.toString());
           throw new HandlerException(sqle);
       } finally {            
           close(stmt);
           close(rs);
       }
    }
    
    
    private void addUpdateLocationFontMap(Element toHandle, Element toAppend) throws HandlerException {

       int callerId                         = getCallerId(toHandle);
       int location                         = HandlerUtils.getRequiredInteger(toHandle, "locationId");
       int count                            = HandlerUtils.getRequiredInteger(toHandle, "count");
     
       

       PreparedStatement stmt               = null;
       ResultSet rs                         = null;
       
       String insertFontMap                 = "INSERT INTO locationFontMap (location, type, font, size)  VALUES (?, ?, ?, ?); ";
       String selectFontMap                 = "SELECT id FROM locationFontMap WHERE location = ? AND type = ? ; ";
       String updateFontMap                 = "UPDATE  locationFontMap SET font = ?, size = ? WHERE location = ? AND type = ? ; ";
        String selectCount                  = "SELECT menuItemCount FROM locationMenu WHERE location = ? ; ";
       String insertCount                   = "INSERT INTO locationMenu (location, menuItemCount) VALUES (?, ?); ";
       String updateCount                   = "UPDATE locationMenu SET menuItemCount = ? WHERE location= ?";
        String updateUserTVAccess           = "UPDATE locationBeerBoardMap SET specials=? WHERE location = ? ";

       try {
           
           stmt                             = transconn.prepareStatement(selectCount);
           stmt.setInt(1,location);
           rs = stmt.executeQuery();
           if (rs.next()) {
               stmt                         = transconn.prepareStatement(updateCount);
               stmt.setInt(1, count);
               stmt.setInt(2, location);              
               
              stmt.executeUpdate();
           
               
           } else {
               stmt                         = transconn.prepareStatement(insertCount);
               stmt.setInt(1, location);
               stmt.setInt(2, count);              
               
              stmt.executeUpdate();
               logger.debug("Location:"+ location+" Count:"+count);
               
           }
           Iterator i                       = toHandle.elementIterator("menu");
           while (i.hasNext()) {
               Element menu                 = (Element) i.next();
               int type                     = HandlerUtils.getRequiredInteger(menu, "type");
               int font                     = HandlerUtils.getRequiredInteger(menu, "font");
               int size                     = HandlerUtils.getRequiredInteger(menu, "size");
              
               stmt                         = transconn.prepareStatement(selectFontMap);
               stmt.setInt(1,location);
               stmt.setInt(2,type);
               rs = stmt.executeQuery();
               if (rs.next()) {
                   stmt                     = transconn.prepareStatement(updateFontMap);
                   stmt.setInt(1, font);
                   stmt.setInt(2, size);
                   stmt.setInt(3, location);
                   stmt.setInt(4, type);                   
                   
                   stmt.executeUpdate();
               } else {
                   stmt                     = transconn.prepareStatement(insertFontMap);
                   stmt.setInt(1, location);
                   stmt.setInt(2, type);
                   stmt.setInt(3, font);
                   stmt.setInt(4, size);
                   stmt.executeUpdate();
                   logger.debug("type:"+ type+" font:"+font+"  size:"+size);
               }
           }
           stmt                         = transconn.prepareStatement(updateUserTVAccess);
           stmt.setInt(1, 1);
           stmt.setInt(2, location);              
           
           stmt.executeUpdate();
          
       } catch (SQLException sqle) {
           logger.dbError("Database error: "+sqle.toString());
           throw new HandlerException(sqle);
       } finally {            
           close(stmt);
       }
    }
    
    
    //GenericHandler
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
        String select = " SELECT id FROM inventory WHERE product=? AND location=?";
        String selectSupplierProducts = "SELECT pSM.product, pSM.plu FROM productSetMap pSM LEFT JOIN supplier s ON s.productSet = pSM.productSet WHERE s.id = ? ";
        String insert = " INSERT INTO inventory (product,location,qtyOnHand,minimumQty,qtyToHave,plu,supplier,kegSize,bottleSize) " +
                " VALUES (?,?,?,?,?,?,?,?,?) ";
        String selectCoolerInventory = " SELECT id FROM inventory WHERE product=? AND location=? AND bottleSize=? AND cooler=? AND kegLine=? ";
        String insertCoolerInventory = " INSERT INTO inventory (product,location,qtyOnHand,minimumQty,qtyToHave,plu,supplier,kegSize,bottleSize,cooler,kegLine) " +
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
    
    
    /**  Add a new product to the master product list along with the product type value.
     *  Will also mark as "added" any pending requests to add a product with this name.
     *
     *  <productName>"String"
     *  opt:<quicksell>000
     *---------SR
     */
    private void addProduct1(Element toHandle, Element toAppend) throws HandlerException {
        
        String name                         = HandlerUtils.getRequiredString(toHandle, "productName");
        int callerId                        = getCallerId(toHandle);
        int qId                             = HandlerUtils.getOptionalInteger(toHandle, "quicksell");
        int prodType                        = HandlerUtils.getOptionalInteger(toHandle, "prodType");
        int category                        = HandlerUtils.getRequiredInteger(toHandle, "category");
        int segment                         = 10;
        String boardName                    = HandlerUtils.getRequiredString(toHandle, "boardName");
        String abv                          = HandlerUtils.getOptionalString(toHandle, "abv");
        String origin                       = HandlerUtils.getOptionalString(toHandle, "origin");
        String seasonality                  = HandlerUtils.getOptionalString(toHandle, "seasonality");
        int bbtvCategory                    = 2;
        int ibu                             = HandlerUtils.getOptionalInteger(toHandle, "ibu");
        int calorie                         = HandlerUtils.getOptionalInteger(toHandle, "calories");
        String description                  = HandlerUtils.getOptionalString(toHandle, "description");
        
        int osPlatform                      = HandlerUtils.getOptionalInteger(toHandle, "osPlatform");
        int locationId                      = HandlerUtils.getOptionalInteger(toHandle, "locationId");
        int mobileUserId                    = HandlerUtils.getOptionalInteger(toHandle, "mobileUserId");   
        String bDB                          = HandlerUtils.getOptionalString(toHandle, "breweryDB");

        int newProductId                    = 0, approved = 0;
        if (qId < 0) {
            qId                             = 0;
        }
        if (ibu < 0) {
            ibu                             = 0;
        }
        if(calorie < 0){
            calorie                         = 1;
        }
        if(seasonality ==null || seasonality.equals("")){
            seasonality                     = "Year-Round";
        }
        if(abv ==null || abv.equals("")){
            abv                             = "0.0";
        }
        if(origin== null){
            origin                          = " ";
        }
        
        
        String insert                       = "INSERT INTO product (name, qId, pType, category, segment, isActive, approved) VALUES (?,?,?,?,?,?,?)";
        String lookup                       = "SELECT id FROM product WHERE name=? AND pType=? LIMIT 1";
        String deleteRequest                = "UPDATE productRequest SET status='added' WHERE productName=? AND status='open'";
        
        PreparedStatement stmt              = null;
        ResultSet rs                        = null;

        logger.portalAction("addProduct1");        

        try {          

            if (prodType < 0) {
                prodType                    = 1;
            }
            //String s1                       =  name.substring(0,1).toUpperCase();		
            //name                            = s1+name.substring(1);
            

            stmt                            = transconn.prepareStatement(lookup);
            stmt.setString(1, name);
            stmt.setInt(2, prodType);
            rs                              = stmt.executeQuery();
            if (rs.next()) {
               // addErrorDetail(toAppend, "A product named " + name + " already exists"); 
                toAppend.addElement("productId").addText(String.valueOf(rs.getInt(1)));
                
                
            } else {     
                WordUtils w                 =new WordUtils();
                name                        = w.capitalize(name);
                stmt                        = transconn.prepareStatement(insert);
                stmt.setString(1, name);
                stmt.setInt(2, qId);
                stmt.setInt(3, prodType);
                stmt.setInt(4, category);
                stmt.setInt(5, segment);
                stmt.setInt(6, 1);
                stmt.setInt(7, approved);
                stmt.executeUpdate();

                // Delete any pending ProductRequests
                stmt                        = transconn.prepareStatement(deleteRequest);
                stmt.setString(1, name);
                stmt.executeUpdate();

                // Log the action
                stmt                        = transconn.prepareStatement("SELECT LAST_INSERT_ID()");
                rs                          = stmt.executeQuery();
                if (rs.next()) {
                    newProductId            = rs.getInt(1);
                    String logMessage = "Added product " + name;
                    logger.portalDetail(callerId, "addProduct", locationId, "product", newProductId, logMessage, transconn);

                    String insertProductDescription 
                                            = " INSERT INTO productDescription (product, boardName, abv, category, origin, seasonality,ibu,breweryDB, calorie) VALUES" +
                                            " (?,?,?,?,?,?,?,?,?)";
                    
                     String insertBrewStyleMap 
                                            = " INSERT INTO brewStyleMap (product, brewery, style) VALUES (?,0,0)";
                    stmt                    = transconn.prepareStatement(insertProductDescription);
                    stmt.setInt(1, newProductId);
                    stmt.setString(2, "");
                    stmt.setString(3, abv);
                    stmt.setInt(4, bbtvCategory);
                    stmt.setString(5, origin);
                    stmt.setString(6, seasonality);
                    stmt.setInt(7, ibu);
                    stmt.setString(8, HandlerUtils.nullToEmpty(bDB));
                    stmt.setInt(9,calorie);
                    stmt.executeUpdate();
                    
                    if(description!=null && !description.equals("")){
                        stmt                        = transconn.prepareStatement( "INSERT INTO productDesc(product,description) VALUES(?, ?)");
                        stmt.setString(2, description);
                        stmt.setInt(1, newProductId);
                        stmt.executeUpdate();
                    }
                   /* if(locationId > 0) {
                        stmt                        = transconn.prepareStatement( "INSERT INTO customBeerName (location,product,name,description) Values(?,?,?,?);");
                        stmt.setInt(1, locationId);
                        stmt.setInt(2, newProductId);
                        stmt.setString(3, name);
                        stmt.setString(4, HandlerUtils.nullToEmpty(description));
                        stmt.executeUpdate();
                    }*/
                    
                    stmt                    = transconn.prepareStatement(insertBrewStyleMap);
                    stmt.setInt(1, newProductId);
                    stmt.executeUpdate();

                    //Adding productSet information
                    Iterator i              = toHandle.elementIterator("productSetMap");
                    addProductSetMap(newProductId,i,toAppend,callerId);
                    String insertLog        = "INSERT INTO productChangeLog (product,type,date,user) VALUES (?,1,now(),?)";
                    stmt                    = transconn.prepareStatement(insertLog);
                    stmt.setInt(1, newProductId);                    
                    stmt.setInt(2, callerId);                    
                    stmt.executeUpdate();
                }
                if(osPlatform > 1 && callerId>0){
              if(mobileUserId <1){
                  mobileUserId              = getMobileUserId(callerId);
              }
              addUserHistory(callerId, "addProduct", locationId, "Added New Product", mobileUserId,newProductId);
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
    
    
     // Adding in productSet information for the product
    private void addProductSetMap(int product, Iterator i, Element toAppend, int callerId) throws HandlerException {
        String selectProductSet = " SELECT productSetType, name FROM productSet WHERE id = ? ";
        String selectProductSetMap = " SELECT pSM.id FROM productSetMap pSM LEFT JOIN productSet pS ON pS.id = pSM.productSet "
                                    + " WHERE pS.productSetType = ? AND pSM.product = ? ";
        String insertProductSetProductMap = " INSERT INTO productSetMap (productSet, product) VALUES (?,?) ";
        String updateProductSetProductMap = " UPDATE productSetMap SET productSet = ? WHERE id = ? ";
        String updateBrewMap                = " UPDATE brewStyleMap SET brewery = ? WHERE product = ? ";
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
                   // String s1=productSetName.substring(0,1).toUpperCase();		
                   // productSetName          =s1+productSetName.substring(1);
                    
                    String selectProductSetName = " SELECT id FROM productSet WHERE name = ? AND productSetType = ? ";
                    stmt = transconn.prepareStatement(selectProductSetName);
                    stmt.setString(1, productSetName);
                    stmt.setInt(2, productSetType);
                    rs = stmt.executeQuery();
                    if (!rs.next()) {
                        WordUtils w         = new WordUtils();
                        productSetName      = w.capitalize(productSetName);

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
                
                stmt = transconn.prepareStatement(selectProductSet);
                stmt.setInt(1, productSet);
                rs = stmt.executeQuery();
                if (rs.next()) {
                   if(rs.getInt(1) ==7){
                        toAppend.addElement("breweryId").addText(String.valueOf(productSet));
                        toAppend.addElement("brewery").addText(HandlerUtils.nullToEmpty(rs.getString(2)));
                    } else if(rs.getInt(1) ==9){
                        toAppend.addElement("styleId").addText(String.valueOf(productSet));
                        toAppend.addElement("style").addText(HandlerUtils.nullToEmpty(rs.getString(2)));
                    } 
                }
            }
            toAppend.addElement("productId").addText(String.valueOf(product));
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
            close(rs);
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
        String selectLocations = " SELECT id FROM location WHERE customer = ? ";
        logger.portalAction("addBeverageSize");
        try {
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
    
    private void addUpdateDeleteInventoryPrices(Element toHandle, Element toAppend) throws HandlerException {
         int callerId                       = getCallerId(toHandle); 
         int locationId                     = HandlerUtils.getRequiredInteger(toHandle, "locationId");      
         PreparedStatement stmt             = null;
         ResultSet rs                       = null, rsInventory = null;
         String selectPrices                = " SELECT id FROM inventoryPrices WHERE inventory = ? AND size = ? AND value = ? ";
         String checkPrices                 = " SELECT id FROM inventoryPrices WHERE inventory = ? AND size = ?  ";
         String insertPrices                = " INSERT INTO inventoryPrices (inventory, size,value) VALUES (?,?,?) ";
         String updatePrices                = " UPDATE inventoryPrices SET value = ? WHERE id = ? ";
         String updateBBTV                  = " UPDATE locationBeerBoardMap SET css = 1 WHERE location = ? ";
         String updateBrassTapLocation      = " UPDATE brasstapLocations SET priceUpdate = 1 WHERE usbnID = ? ";
         
         try {
             if(callerId > 0 && locationId > 0 && isValidAccessUser(callerId, locationId,false)){
            Iterator i                      = toHandle.elementIterator("inventoryPrices");
            while (i.hasNext()) {
                 Element prices             = (Element) i.next();
                 int id                     = HandlerUtils.getOptionalInteger(prices, "priceId");
                 int size                   = HandlerUtils.getOptionalInteger(prices, "size");
                 double value               = HandlerUtils.getRequiredDouble(prices, "value");
                 int inventory              = HandlerUtils.getRequiredInteger(prices, "inventoryId");   
                 boolean forceUpdate        = HandlerUtils.getOptionalBoolean(prices, "forceUpdate");
                 
                 if (id <= 0 && value > 0) {                   
                     String selectInventory = "SELECT id FROM inventory WHERE location = ? ";
                     if(inventory > 0){
                         selectInventory    +=" AND id = "+String.valueOf(inventory);
                     }
                     stmt                   = transconn.prepareStatement(selectInventory);
                     stmt.setInt(1,locationId);
                     rsInventory            = stmt.executeQuery();
                     while(rsInventory.next()){
                         inventory          = rsInventory.getInt(1);
                         if(inventory> 0){
                             stmt           = transconn.prepareStatement(checkPrices);
                             stmt.setInt(1,inventory);
                             stmt.setInt(2,size);                             
                             rs = stmt.executeQuery();
                             if(forceUpdate) {
                                 if (rs.next()) {                                 
                                     id= rs.getInt(1);
                                     stmt   = transconn.prepareStatement(updatePrices);
                                     stmt.setDouble(1, value);
                                     stmt.setInt(2, id);
                                     stmt.executeUpdate();
                                 } else {
                                     stmt   = transconn.prepareStatement(insertPrices);
                                     stmt.setInt(1, inventory);
                                     stmt.setInt(2,size);
                                     stmt.setDouble(3, value);
                                     stmt.executeUpdate(); 
                                 } 
                             }else {
                                 if (!rs.next()) {   
                                     stmt   = transconn.prepareStatement(insertPrices);
                                     stmt.setInt(1, inventory);
                                     stmt.setInt(2,size);
                                     stmt.setDouble(3, value);
                                     stmt.executeUpdate();    
                                 }
                             }
                         }
                     }
                 } else {
                     if(value> 0){ 
                         stmt               = transconn.prepareStatement(selectPrices);
                         stmt.setInt(1,inventory);
                         stmt.setInt(2,size);
                         stmt.setDouble(3, value);
                         //logger.debug("Inventory:"+inventory+" Value:"+value);
                         rs                 = stmt.executeQuery();
                         if (rs.next()) {                               
                            // toAppend.addElement("errorMessage").addText("Price Already Present");                             
                         } else {
                             stmt           = transconn.prepareStatement(updatePrices);
                             stmt.setDouble(1, value);
                             stmt.setInt(2, id);
                             stmt.executeUpdate();
                         }
                     } else if(id >0) {
                         stmt               = transconn.prepareStatement("DELETE FROM inventoryPrices WHERE id = ?");
                         stmt.setInt(1, id);
                         stmt.executeUpdate();
                     }
                 }
             }
            logger.portalDetail(callerId, "inventoryPrices", locationId, "addUpdateDeleteInventoryPrices",0, "addUpdateDeleteInventoryPrices", transconn);

             stmt                           = transconn.prepareStatement(updateBBTV);
             stmt.setInt(1, locationId);
             stmt.executeUpdate();
             
             stmt                           = transconn.prepareStatement(updateBrassTapLocation);
             stmt.setInt(1, locationId);
             stmt.executeUpdate();
             } else {
                addErrorDetail(toAppend, "Invalid Access"  );
            }
         } catch (SQLException sqle) {
             logger.dbError("Database error: " + sqle.getMessage());
             throw new HandlerException(sqle);
         } finally {
             close(rsInventory);
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
    
    
    private void addUpdateDeleteComingSoonProduct(Element toHandle, Element toAppend) throws HandlerException {
        
        int callerId                        = getCallerId(toHandle);
        int location                        = HandlerUtils.getRequiredInteger(toHandle, "locationId");        
      
        
        String selectProduct                = "SELECT id FROM comingSoonProducts  WHERE location = ? AND product =?; ";   
        String selectInventory              = "SELECT id FROM inventory WHERE location = ? AND product = ? ";
        String checkLineProduct             = "SELECT IFNULL((SELECT l.id FROM line as l LEFT JOIN product AS p ON p.id = l.product LEFT JOIN bar b ON b.id = l.bar WHERE l.status = 'RUNNING' AND b.location=? AND p.id=? LIMIT 1),0),"
                                            + "IFNULL((SELECT id FROM comingSoonProducts WHERE location=? AND product =? LIMIT 1),0);";
        String insertInventory              = "INSERT INTO inventory (location, product, kegSize) VALUES (?, ?, 1984);";
        String insertProduct                = "INSERT INTO comingSoonProducts (location,product,line) VALUES (?, ?, ?); ";        
        String deleteProduct                = "DELETE FROM comingSoonProducts WHERE id=?;";        
        String updateProduct                = "UPDATE comingSoonProducts SET product = ? ,line = ? WHERE id = ?; ";     
        String checkChanges                 = "SELECT id FROM comingSoonProducts where id = ? AND product = ? AND line=?;";
        
        PreparedStatement stmt              = null;
        ResultSet rs                        = null;
        try {
            if(callerId > 0 && location > 0 && isValidAccessUser(callerId, location, false)){
             Iterator addCSProduct   = toHandle.elementIterator("addProduct");
             while (addCSProduct.hasNext()) {
                Element csProduct           = (Element) addCSProduct.next();
                int lineId                  = HandlerUtils.getRequiredInteger(csProduct, "lineId");
                int productId               = HandlerUtils.getRequiredInteger(csProduct, "productId");
                String productName          = HandlerUtils.nullToEmpty(HandlerUtils.getOptionalString(csProduct, "productName"));
                if(productId==0 && productName!=null && !productName.equals("")){
                    productId                 = addProduct(productName.trim(), callerId, location);
                }
                    
                
                stmt                        = transconn.prepareStatement(checkLineProduct);
                stmt.setInt(1, location);
                stmt.setInt(2, productId);
                stmt.setInt(3, location);
                stmt.setInt(4, productId);
                rs                          = stmt.executeQuery();   
                int line                    = -1,csproduct = -1;
                if (rs.next()) { 
                    line                    = rs.getInt(1);
                    csproduct               = rs.getInt(2);
                    if(line==0 && csproduct==0){
                        stmt                = transconn.prepareStatement(insertProduct);
                        stmt.setInt(1, location);
                        stmt.setInt(2, productId);
                        stmt.setInt(3, lineId);                  
                        stmt.executeUpdate();       
                        String logMessage
                                            = "Added " + net.terakeet.soapware.handlers.report.ProductMap.staticLookup(productId, transconn) + " to on-deck";
                        logger.portalDetail(callerId, "comingSoonProduct", location, "addUpdateDeleteComingSoonProduct",productId, logMessage, transconn);
                    } else {
                        toAppend.addElement("errorMessage").addText("Product "+productName+" already Assigned");
                    }
                }
                
                stmt                        = transconn.prepareStatement(selectInventory);
                stmt.setInt(1, location);
                stmt.setInt(2, productId);
                rs                          = stmt.executeQuery();   
                if (!rs.next()) { 
                    stmt                    = transconn.prepareStatement(insertInventory);
                    stmt.setInt(1, location);
                    stmt.setInt(2, productId);
                    stmt.executeUpdate();
                }
            }

             Iterator updateCSProduct   = toHandle.elementIterator("updateProduct");
             while (updateCSProduct.hasNext()) {
                Element csProduct           = (Element) updateCSProduct.next();
                int id                      = HandlerUtils.getRequiredInteger(csProduct, "id");
                int lineId                  = HandlerUtils.getRequiredInteger(csProduct, "lineId");
                int productId               = HandlerUtils.getRequiredInteger(csProduct, "productId");
                String productName          = HandlerUtils.nullToEmpty(HandlerUtils.getOptionalString(csProduct, "productName"));
                if(productId==0 && productName!=null && !productName.equals("")){
                    productId                 = addProduct(productName.trim(), callerId, location);
                }
                stmt                        = transconn.prepareStatement(checkChanges);
                stmt.setInt(1, id);
                stmt.setInt(2, productId);
                stmt.setInt(3, lineId);
                rs                          = stmt.executeQuery();   
                if(!rs.next()){
                    stmt                    = transconn.prepareStatement(checkLineProduct);
                    stmt.setInt(1, location);
                    stmt.setInt(2, productId);
                    stmt.setInt(3, location);
                    stmt.setInt(4, productId);
                    rs                      = stmt.executeQuery();   
                    int line                = -1,csproduct = -1;
                    if (rs.next()) { 
                        line                = rs.getInt(1);
                        csproduct           = rs.getInt(2);
                        if(line==0 && csproduct==0){
                            stmt            = transconn.prepareStatement(updateProduct);
                            stmt.setInt(1, productId);
                            stmt.setInt(2, lineId);                                    
                            stmt.setInt(3, id);
                            stmt.executeUpdate();   
                        } else {
                             stmt           = transconn.prepareStatement("SELECT line,product FROM comingSoonProducts where id = ?;");
                             stmt.setInt(1, id);
                             rs             = stmt.executeQuery();
                             if(rs.next()) {
                                 if(rs.getInt(2)!=productId) {
                                     toAppend.addElement("errorMessage").addText("Product "+productName+" already Assigned");
                                 }
                             }
                            stmt            = transconn.prepareStatement("UPDATE comingSoonProducts SET line = ? WHERE id = ?; ");                        
                            stmt.setInt(1, lineId);                                    
                            stmt.setInt(2, id);
                            stmt.executeUpdate(); 
                            
                            

                        }
                    }
                }
                
                stmt                        = transconn.prepareStatement(selectInventory);
                stmt.setInt(1, location);
                stmt.setInt(2, productId);
                rs                          = stmt.executeQuery();   
                if (!rs.next()) { 
                    stmt                    = transconn.prepareStatement(insertInventory);
                    stmt.setInt(1, location);
                    stmt.setInt(2, productId);
                    stmt.executeUpdate();
                }
            }            
        
             Iterator deleteCSProduct       = toHandle.elementIterator("deleteProduct");
             while (deleteCSProduct.hasNext()) {
                Element csProduct           = (Element) deleteCSProduct.next();
                int id                  = HandlerUtils.getRequiredInteger(csProduct, "id");                
                stmt                        = transconn.prepareStatement(deleteProduct);
                stmt.setInt(1, id);
                stmt.executeUpdate();
                logger.portalDetail(callerId, "comingSoonProduct", location, "addUpdateDeleteComingSoonProduct",id, "deleteComingSoonProduct", transconn);
            }             
             
            stmt                        = transconn.prepareStatement("UPDATE locationBeerBoardMap SET marketing = 1 WHERE location = ?");
            stmt.setInt(1, location);
            stmt.executeUpdate();
            
            stmt            = transconn.prepareStatement("UPDATE locationBeerBoardMap SET css=1, api=1 WHERE location = ?");
            stmt.setInt(1, location);
            stmt.executeUpdate();
            
               
            } else {
                addErrorDetail(toAppend, "Invalid Access"  );
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error in addRemoveComingSoonProducts: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(rs);
            close(stmt);
        }

    }
    
     private int  addProduct(String productName, int callerId, int location) throws HandlerException {
         
        
         PreparedStatement stmt             = null;
         ResultSet rs                       = null;
         int productId                      = 0;
         String sql                         = "SELECT id FROM product WHERE ptype=1 AND isActive=1 AND name=?  LIMIT 1;";
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
            if(productId ==0) {
                WordUtils w                 = new WordUtils();
                productName                 = w.capitalize(productName);
                stmt                        = transconn.prepareStatement(insertProduct);                    
                stmt.setString(1,productName);
                stmt.executeUpdate();
                stmt                        = transconn.prepareStatement(getLastId);
                rs                          = stmt.executeQuery();                   
                if (rs.next()) { 
                    productId               = rs.getInt(1);

                }
                if(productId > 0){
                    String logMessage       = "Added product " + productName;
                    logger.portalDetail(callerId, "addProduct", location, "product", productId, logMessage, transconn);
                    stmt                    = transconn.prepareStatement(insertProductDescription);      
                    stmt.setInt(1, productId);
                    stmt.setString(2,"");
                    stmt.executeUpdate();

                    stmt                    = transconn.prepareStatement(insertBrewStyleMap);      
                    stmt.setInt(1, productId);                        
                    stmt.executeUpdate();

                    stmt                    = transconn.prepareStatement(insertLog);      
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
    
    
    

    private void brasstapAdjustments(Element toHandle, Element toAppend) throws HandlerException {

        String selectNewBrassTapItems       = "SELECT id, beerName FROM brasstapItems WHERE beerID = 0 ORDER BY beerName";
        String selectBrassTapBeerID         = "SELECT beerID FROM brasstapItems WHERE beerName = ? AND beerID != 0 ORDER BY ID LIMIT 1;";
        String updateBrassTapBeerID         = "UPDATE brasstapItems SET beerID = ? WHERE id = ?;";

        String selectNewBrassTapPLU         = "SELECT bI.beerName, bI.plu, bI.ounces, bP.usbnID, bI.id FROM brasstapItems bI LEFT JOIN brasstapProducts bP ON bP.beerID = bI.beerID " +
                                            " WHERE bI.beerID > 0 AND bI.added = 0 ORDER BY bI.beerName, bI.ounces LIMIT 500;";
        String selectOldBrassTapPLU         = "SELECT bI.beerName, bI.plu, bI.ounces, bP.usbnID, bI.id FROM brasstapItems bI LEFT JOIN brasstapProducts bP ON bP.beerID = bI.beerID " +
                                            " WHERE bI.beerID > 0 AND bI.added = 1 AND bI.toUpdate = 0 ORDER BY bI.beerName, bI.ounces LIMIT 500;";
        String selectBrassTapOldLocations   = "SELECT usbnID FROM brasstapLocations WHERE updated = 1";
        String selectBrassTapNewLocations   = "SELECT usbnID FROM brasstapLocations WHERE updated = 0";
        String getInventory                 = "SELECT product FROM inventory WHERE location = ? ";
        String getBeverage                  = "SELECT id FROM beverage WHERE location = ? AND plu = ? ";
        String insertBrassTapBeverage       = "INSERT INTO beverage (location, name, plu, ounces, simple, pType) VALUES (?, ?, ?, ?, 1, 1);";
        String insertBrassTapIngredient     = "INSERT INTO ingredient (beverage, product, ounces) VALUES (?, ?, ?);";
        String insertBrassTapInventory      = "INSERT INTO inventory (location, product) VALUES (?, ?);";

        PreparedStatement stmt              = null;
        ResultSet rs                        = null, innerRS = null, inventoryRS = null, beverageRS = null;

        Set<Integer> existingInventory      = new HashSet<Integer>();
        
        try {

            // Updating Brass Tap Beer IDs to the new PLU entries
            stmt                            = transconn.prepareStatement(selectNewBrassTapItems);
            rs                              = stmt.executeQuery();
            while (rs.next()) {
                stmt                        = transconn.prepareStatement(selectBrassTapBeerID);
                stmt.setString(1, rs.getString(2));
                innerRS                     = stmt.executeQuery();
                if (innerRS.next()) {
                    stmt                    = transconn.prepareStatement(updateBrassTapBeerID);
                    stmt.setInt(1, innerRS.getInt(1));
                    stmt.setInt(2, rs.getInt(1));
                    stmt.executeUpdate();
                }
            }

            // Adding the new PLU entries to old Brass Tap locations
            stmt                            = transconn.prepareStatement(selectNewBrassTapPLU);
            rs                              = stmt.executeQuery();
            while (rs.next()) {
                String beverageName         = rs.getString(1);
                String plu                  = rs.getString(2);
                Double ounces               = rs.getDouble(3);
                int product                 = rs.getInt(4);
                int itemId                  = rs.getInt(5);
                
                stmt                        = transconn.prepareStatement(selectBrassTapOldLocations);
                innerRS                     = stmt.executeQuery();
                while (innerRS.next()) {
                    int locationId          = innerRS.getInt(1);

                    stmt                    = transconn.prepareStatement(getInventory);
                    stmt.setInt(1, locationId);
                    inventoryRS             = stmt.executeQuery();
                    while (inventoryRS.next()) {
                        existingInventory.add(inventoryRS.getInt(1));
                    }

                    stmt                    = transconn.prepareStatement(getBeverage);
                    stmt.setInt(1, locationId);
                    stmt.setString(2, plu);
                    beverageRS              = stmt.executeQuery();
                    if (!beverageRS.next()) {
                        int beverageId      = 0;
                        stmt                = transconn.prepareStatement(insertBrassTapBeverage);
                        stmt.setInt(1, locationId);
                        stmt.setString(2, beverageName);
                        stmt.setString(3, plu);
                        stmt.setDouble(4, ounces);
                        stmt.executeUpdate();

                        stmt                = transconn.prepareStatement("SELECT LAST_INSERT_ID()");
                        beverageRS          = stmt.executeQuery();
                        if (beverageRS.next()) {
                            beverageId      = beverageRS.getInt(1);
                            stmt            = transconn.prepareStatement(insertBrassTapIngredient);
                            stmt.setInt(1, beverageId);
                            stmt.setInt(2, product);
                            stmt.setDouble(3, ounces);
                            stmt.executeUpdate();

                            if (!existingInventory.contains(product)) {
                                stmt        = transconn.prepareStatement(insertBrassTapInventory);
                                stmt.setInt(1, locationId);
                                stmt.setInt(2, product);
                                stmt.executeUpdate();
                            }
                        }
                    }
                }
                
                stmt                        = transconn.prepareStatement("UPDATE brasstapItems SET added = 1 WHERE id = ?;");
                stmt.setInt(1, itemId);
                stmt.executeUpdate();
            }

            // Adding the old PLU entries to new Brass Tap locations
            stmt                            = transconn.prepareStatement(selectOldBrassTapPLU);
            rs                              = stmt.executeQuery();
            if (!rs.next()) {
                stmt                        = transconn.prepareStatement("UPDATE brasstapLocations SET updated = 1");
                stmt.executeUpdate();
            } else {
                rs.beforeFirst();
            }
            while (rs.next()) {
                String beverageName         = rs.getString(1);
                String plu                  = rs.getString(2);
                Double ounces               = rs.getDouble(3);
                int product                 = rs.getInt(4);
                int itemId                  = rs.getInt(5);

                stmt                        = transconn.prepareStatement(selectBrassTapNewLocations);
                innerRS                     = stmt.executeQuery();
                while (innerRS.next()) {
                    int locationId          = innerRS.getInt(1);

                    stmt                    = transconn.prepareStatement(getInventory);
                    stmt.setInt(1, locationId);
                    inventoryRS             = stmt.executeQuery();
                    while (inventoryRS.next()) {
                        existingInventory.add(inventoryRS.getInt(1));
                    }

                    stmt                    = transconn.prepareStatement(getBeverage);
                    stmt.setInt(1, locationId);
                    stmt.setString(2, plu);
                    beverageRS              = stmt.executeQuery();
                    if (!beverageRS.next()) {
                        int beverageId      = 0;
                        stmt                = transconn.prepareStatement(insertBrassTapBeverage);
                        stmt.setInt(1, locationId);
                        stmt.setString(2, beverageName);
                        stmt.setString(3, plu);
                        stmt.setDouble(4, ounces);
                        stmt.executeUpdate();

                        stmt                = transconn.prepareStatement("SELECT LAST_INSERT_ID()");
                        beverageRS          = stmt.executeQuery();
                        if (beverageRS.next()) {
                            beverageId      = beverageRS.getInt(1);
                            stmt            = transconn.prepareStatement(insertBrassTapIngredient);
                            stmt.setInt(1, beverageId);
                            stmt.setInt(2, product);
                            stmt.setDouble(3, ounces);
                            stmt.executeUpdate();

                            if (!existingInventory.contains(product)) {
                                stmt        = transconn.prepareStatement(insertBrassTapInventory);
                                stmt.setInt(1, locationId);
                                stmt.setInt(2, product);
                                stmt.executeUpdate();
                            }
                        }
                    }
                }

                stmt                        = transconn.prepareStatement("UPDATE brasstapItems SET toUpdate = 1 WHERE id = ?;");
                stmt.setInt(1, itemId);
                stmt.executeUpdate();
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(inventoryRS);
            close(beverageRS);
            close(innerRS);
            close(rs);
            close(stmt);
        }
    }
    
    
     private void addBottleBeer(Element toHandle, Element toAppend) throws HandlerException {
        int callerId                        = getCallerId(toHandle);
        int location                        = HandlerUtils.getRequiredInteger(toHandle, "locationId");
       
        PreparedStatement stmt              = null;
        ResultSet rs                        = null;
        String checkBeer                    = "SELECT id FROM bottleBeer WHERE product = ? AND location=? AND type = ?;";
        String insertBeer = " INSERT INTO bottleBeer (location, product, type,price) VALUES (?, ?, ?, ?) ";
        try {
            if(callerId > 0 && location > 0 && isValidAccessUser(callerId, location, false)){
           Iterator addBeer   = toHandle.elementIterator("addBottle");
             while (addBeer.hasNext()) {
                Element addProduct          = (Element) addBeer.next();
                int type                    = HandlerUtils.getRequiredInteger(addProduct, "type");
                int productId               = HandlerUtils.getRequiredInteger(addProduct, "productId");
                String price                = HandlerUtils.getRequiredString(addProduct, "price");
                if(price==null || price.equals("")){
                    price                   = "0";
                }
                
                stmt                        = transconn.prepareStatement(checkBeer);
                stmt.setInt(1, productId);
                stmt.setInt(2, location);
                stmt.setInt(3, type);                
                rs                          = stmt.executeQuery();                   
                if (!rs.next()) { 
                    stmt                    = transconn.prepareStatement(insertBeer);
                    stmt.setInt(1, location);
                    stmt.setInt(2, productId);
                    stmt.setInt(3, type); 
                    stmt.setDouble(4, Double.parseDouble(price));
                    stmt.executeUpdate();    
                    logger.portalDetail(callerId, "addBottleBeer", location, "addBottleBeer", productId, "addBottleBeer", transconn);
                    
                }
            }
             
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
     
     
      private void addProductComment(Element toHandle, Element toAppend) throws HandlerException {        
        
        int callerId                        = getCallerId(toHandle);
        int product                         = HandlerUtils.getRequiredInteger(toHandle, "productId");
        String comment                      = HandlerUtils.nullToEmpty(HandlerUtils.getRequiredString(toHandle, "comment"));
       
        PreparedStatement stmt              = null;
        ResultSet rs                        = null;
        //String checkCmnt                    = "SELECT id FROM productComment WHERE product = ?;";
        String insertCmnt                   = " INSERT INTO productComment ( product, comment,user, updateTime) VALUES (?, ?, ?,now()) ";
        //String updateCmnt                   = " UPDATE productComment SET   comment = ?  WHERE id= ? AND product = ? ";
        try {
            
            stmt                            = transconn.prepareStatement(insertCmnt);
            stmt.setInt(1, product); 
            stmt.setString(2, comment);
            stmt.setInt(3, callerId);
            stmt.executeUpdate();       

        
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
        }
     
    }


     private void cleanHootersData(Element toHandle, Element toAppend) throws HandlerException {

        PreparedStatement stmt              = null;
        ResultSet rs                        = null, innerRS = null;
        String getLocation                  = "SELECT id FROM location WHERE customer IN (SELECT id FROM customer WHERE id = 4);";
        String getSales                     = "SELECT GROUP_CONCAT(sid) FROM (SELECT sid, reportRecordId, COUNT(sid) as c FROM sales WHERE location = ? " +
                                            " AND date BETWEEN '2015-05-16 07:00:00' AND '2015-05-17 07:00:00' GROUP BY reportRecordId ORDER BY COUNT(sid) DESC) as s WHERE c > 1;";
        try {
            stmt                            = transconn.prepareStatement(getLocation);
            rs                              = stmt.executeQuery();
            while (rs.next()) {
                stmt                        = transconn.prepareStatement(getSales);
                stmt.setInt(1, rs.getInt(1));
                innerRS                     = stmt.executeQuery();
                if (innerRS.next()) {
                    String sidList          = innerRS.getString(1);
                    logger.debug(sidList);
                    stmt                    = transconn.prepareStatement("DELETE FROM sales WHERE sid IN (" + sidList + ")");
                    stmt.executeUpdate();
                }
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
            close(rs);
            close(innerRS);
        }
    }


     private void cleanMaxLineCounter(Element toHandle, Element toAppend) throws HandlerException {

        PreparedStatement stmt              = null;
        ResultSet rs                        = null, innerRS = null;
        String getLines                     = "SELECT line, id FROM reading WHERE date = '2015-05-18 01:13:40' AND value < 0 AND type = 0;";
        String getSales                     = "SELECT id, quantity FROM reading WHERE line = ? AND date > '2015-05-18 01:13:40' AND type = 0 ORDER BY id LIMIT 1;";
        try {
            stmt                            = transconn.prepareStatement(getLines);
            rs                              = stmt.executeQuery();
            while (rs.next()) {
                stmt                        = transconn.prepareStatement(getSales);
                stmt.setInt(1, rs.getInt(1));
                innerRS                     = stmt.executeQuery();
                while (innerRS.next() && innerRS.getDouble(2) > 0) {
                    stmt                    = transconn.prepareStatement("UPDATE reading SET date = date, quantity = ? WHERE id = ?");
                    stmt.setDouble(1, 0.0);
                    stmt.setInt(2, innerRS.getInt(1));
                    stmt.executeUpdate();
                }
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
            close(rs);
            close(innerRS);
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
    
    
    private void addInventoryBeveragePrice(Element toHandle, Element toAppend) throws HandlerException {
        int location                        = HandlerUtils.getRequiredInteger(toHandle, "locationId");
        String checkInventory               = "SELECT id FROM inventory WHERE location= ? AND product = ?;";
        String insertInventory              = " INSERT INTO inventory (product,location)  VALUES (?,?) ";

        String checkPlu                     =" SELECT id,bev.name FROM beverage bev  WHERE bev.location=? AND bev.plu=? LIMIT 1";
        String getLastId                    =" SELECT LAST_INSERT_ID()";
        String insertBev                    = " INSERT INTO beverage (name, location, plu,ounces,price, simple, pType) VALUES (?,?,?,?,?,1,1)";
        String insertIng                    =" INSERT INTO ingredient (beverage, product, ounces)  VALUES (?,?,?)";
        String selectBeverage               ="SELECT product,plu,size,price,productName,id FROM addBeverage WHERE location =? AND isInsert=0;";
        String checkSize                    = "SELECT id FROM beverageSize WHERE location=? AND ounces BETWEEN (?-0.05) AND (?+0.05)";
        String selectBeverageSize           ="SELECT DISTINCT size FROM addBeverage WHERE location =? AND isInsert=0;";
        String insertBeverageSize           = "INSERT INTO beverageSize (location,name,ounces) VALUES (?,?,?)";
        String updateBeverageList           = "UPDATE addBeverage SET isInsert =1 WHERE id= ?";
        String insertProduct                = "INSERT INTO product (name, qId, pType, category, segment, isActive, approved) VALUES (?,0,1,2,10,1,0)";
        String insertLog                    = "INSERT INTO productChangeLog (product,type,date) VALUES (?,1,now())";
        String insertProductDescription     = " INSERT INTO productDescription (product, boardName, abv, category, origin, seasonality,ibu,breweryDB) VALUES" +
                                            " (?,?,0.0,2,'','',0,'')";
        String insertBrewStyleMap           = " INSERT INTO brewStyleMap (product, brewery, style) VALUES (?,0,0)";
        String selectSize                   = "SELECT id,ounces FROM beverageSize WHERE location=?; ";
        String selectPrices                = " SELECT id FROM inventoryPrices WHERE inventory = ? AND size = ? AND value = ? ";
        String insertPrices                = " INSERT INTO inventoryPrices (inventory, size,value) VALUES (?,?,?) ";

         Map<Float,Integer> sizeMap      = new HashMap< Float, Integer>();


        PreparedStatement stmt              = null;
        ResultSet rs                        = null, rsDetails = null;
        try {
            stmt                        = transconn.prepareStatement(selectBeverageSize);
            stmt.setInt(1, location);
            rs                          = stmt.executeQuery();
            while(rs.next()) {
                float  ounces          = rs.getFloat(1);
                stmt                    = transconn.prepareStatement(checkSize);
                stmt.setInt(1, location);
                stmt.setFloat(2, ounces);
                stmt.setFloat(3, ounces);
                rsDetails               = stmt.executeQuery();
                if (!rsDetails.next()) {
                    stmt                = transconn.prepareStatement(insertBeverageSize);
                    stmt.setInt(1, location);
                    stmt.setString(2, String.valueOf(ounces)+"Oz");
                    stmt.setFloat(3, ounces);
                    stmt.executeUpdate();
                    stmt                    = transconn.prepareStatement(getLastId);
                    rsDetails               = stmt.executeQuery();
                    if (rsDetails.next()) {
                        int newId           = rsDetails.getInt(1);
                        String logMessage   = "Adding a beverage size named '" + String.valueOf(ounces)+"Oz" + "' " + "(" +ounces+ " oz)";
                        logger.portalDetail(201, "addBeverageSize", location, "beverageSize", newId, logMessage, transconn);
                    }

                }
            }

            stmt                        = transconn.prepareStatement(selectSize);
            stmt.setInt(1, location);
            rs                          = stmt.executeQuery();
            while(rs.next()) {

                sizeMap.put(rs.getFloat(2), rs.getInt(1));
            }
            WordUtils w                     = new WordUtils();
            stmt                            = transconn.prepareStatement(selectBeverage);
            stmt.setInt(1, location);
            rs                              = stmt.executeQuery();
            while(rs.next()) {
                int productId               = rs.getInt(1);
                String plu                  = rs.getString(2);
                float size                  = rs.getFloat(3);
                double price                = rs.getDouble(4);
                String productName          = rs.getString(5);                
                productName                 = w.capitalize(productName);
                int id                      = rs.getInt(6);
                int inventory               = 0;
                if(productId == 0) {
                stmt                       = transconn.prepareStatement("SELECT id FROM product WHERE ptype=1 AND name =?");
                stmt.setString(1, productName);
                rsDetails                  = stmt.executeQuery();
                if (rsDetails.next()) {
                    productId              = rsDetails.getInt(1);
                    stmt                       = transconn.prepareStatement("UPDATE addBeverage SET product =? WHERE productName= ? AND isInsert=0");
                    stmt.setInt(1, productId);
                    stmt.setString(2, productName);
                    stmt.executeUpdate();

                }
                }
                if(productId == 0) {

                    stmt                       = transconn.prepareStatement(insertProduct);
                    stmt.setString(1,productName);
                    stmt.executeUpdate();
                    stmt                       = transconn.prepareStatement(getLastId);
                    rsDetails                  = stmt.executeQuery();
                    if (rsDetails.next()) {
                        productId              = rsDetails.getInt(1);

                    }
                    if(productId > 0){
                        String logMessage = "Added product " + productName;
                        logger.portalDetail(201, "addProduct", location, "product", productId, logMessage, transconn);
                        stmt                       = transconn.prepareStatement(insertProductDescription);
                        stmt.setInt(1, productId);
                        stmt.setString(2,productName);
                        stmt.executeUpdate();

                        stmt                       = transconn.prepareStatement(insertBrewStyleMap);
                        stmt.setInt(1, productId);
                        stmt.executeUpdate();

                        stmt                       = transconn.prepareStatement(insertLog);
                        stmt.setInt(1, productId);
                        stmt.executeUpdate();

                        stmt                       = transconn.prepareStatement("UPDATE addBeverage SET product =? WHERE id= ?");
                        stmt.setInt(1, productId);
                        stmt.setInt(2, id);
                        stmt.executeUpdate();

                        stmt                       = transconn.prepareStatement("UPDATE addBeverage SET product =? WHERE productName= ? AND isInsert=0");
                        stmt.setInt(1, productId);
                        stmt.setString(2, productName);
                        stmt.executeUpdate();
                    }
                }
                if(productId > 0){
                stmt                       = transconn.prepareStatement(checkInventory);
                stmt.setInt(1, location);
                stmt.setInt(2, productId);
                rsDetails                  = stmt.executeQuery();
                if (rsDetails.next()) {
                    inventory              = rsDetails.getInt(1);
                } else{
                    stmt                       = transconn.prepareStatement(insertInventory);
                    stmt.setInt(2, location);
                    stmt.setInt(1, productId);
                    stmt.executeUpdate();
                    stmt                       = transconn.prepareStatement(getLastId);
                    rsDetails                  = stmt.executeQuery();
                    if (rsDetails.next()) {
                        inventory              = rsDetails.getInt(1);

                    }
                    String logMessage = "Added product " + productId + " to inventory";
                    logger.portalDetail(201, "addInventory", location, "inventory", inventory, logMessage, transconn);
                }
                }
                if(inventory>0){
                    logger.debug("Inventory:"+inventory +": "+ productName);
                    stmt                        = transconn.prepareStatement(checkPlu);
                    stmt.setInt(1, location);
                    stmt.setString(2, plu);
                    rsDetails                    = stmt.executeQuery();
                    if (!rsDetails.next()) {
                        stmt                = transconn.prepareStatement(insertBev);
                            stmt.setString(1, productName);
                            stmt.setInt(2, location);
                            stmt.setString(3, plu);
                            stmt.setDouble(4, size);
                            stmt.setDouble(5, price);
                            stmt.executeUpdate();
                            int beverageId      = -1;
                            stmt                = transconn.prepareStatement(getLastId);
                            rsDetails = stmt.executeQuery();
                            if (rsDetails.next()) {
                                String logMessage = "Added plu#" + plu;
                                logger.portalDetail(201, "addBeverage", location, "beverage", beverageId, logMessage, transconn);
                                beverageId      = rsDetails.getInt(1);
                                logger.debug("Beverage:"+beverageId);
                                stmt            = transconn.prepareStatement(insertIng);
                                stmt.setInt(1, beverageId);
                                stmt.setInt(2, productId);
                                stmt.setDouble(3, size);
                                stmt.executeUpdate();
                            }
                            stmt                       = transconn.prepareStatement(updateBeverageList);
                            stmt.setInt(1, id);
                            stmt.executeUpdate();
                    }

                    int sizeId              = sizeMap.get(size);
                    if(price> 0) {
                    stmt                        = transconn.prepareStatement(selectPrices);
                    stmt.setInt(1, inventory);
                    stmt.setInt(2, sizeId);
                    stmt.setDouble(3, price);
                    rsDetails                    = stmt.executeQuery();
                    if (!rsDetails.next()) {
                        stmt                        = transconn.prepareStatement(insertPrices);
                        stmt.setInt(1, inventory);
                        stmt.setInt(2, sizeId);
                        stmt.setDouble(3, price);
                        stmt.executeUpdate();

                    }
                    }
                }
           }
        } catch (SQLException sqle) {
            logger.dbError("Database error in addBeveage from list: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(rsDetails);
            close(rs);
            close(stmt);
        }

    }
    
    private void addUserHistorySummary(Element toHandle, Element toAppend) throws HandlerException {
        
        int overwrite                       = HandlerUtils.getOptionalInteger(toHandle, "overwrite");        
        String startDate                    = HandlerUtils.getOptionalString(toHandle, "startDate");        

        PreparedStatement stmt              = null;
        ResultSet rs                        = null;

        String insertSummary                = "INSERT INTO userHistorySummary (location,user,task,count,date) "
                                            + " ( SELECT location,user, task, COUNT(id), DATE(?) FROM userHistory WHERE timestamp BETWEEN  ? AND ADDDATE(?, INTERVAl 1 DAY) AND  location>0 AND task >0 AND user > 0 GROUP BY location, user,task);";
        String insertMobileSummary          = "INSERT INTO userHistoryMobileSummary (location,user,task,count,date) "
                                            + " ( SELECT location,user, task, COUNT(id), DATE(?) FROM userHistoryMobile WHERE timestamp BETWEEN  ? AND ADDDATE(?, INTERVAl 1 DAY) AND  location>0 AND task >0 AND user > 0 GROUP BY location, user,task);";
        String selectSummary                = "SELECT location,user, task, COUNT(id) FROM userHistory WHERE timestamp BETWEEN  ? AND ADDDATE(?, INTERVAl 1 DAY) AND  location>0 AND task >0 AND user > 0 GROUP BY location, user,task ORDER BY location, user,task;";

        try {
            if (overwrite > 0) {  
                stmt                            = transconn.prepareStatement("DELETE FROM userHistorySummary WHERE  date = DATE(?); ");                          
                stmt.setString(1,startDate);
                stmt.execute();
                
                stmt                            = transconn.prepareStatement("DELETE FROM userHistoryMobileSummary WHERE  date = DATE(?); ");                          
                stmt.setString(1,startDate);
                stmt.execute();
            }
            stmt                        = transconn.prepareStatement(insertSummary);
            stmt.setString(1,startDate);
            stmt.setString(2,startDate);
            stmt.setString(3,startDate);
            stmt.executeUpdate();
            
            stmt                        = transconn.prepareStatement(insertMobileSummary);
            stmt.setString(1,startDate);
            stmt.setString(2,startDate);
            stmt.setString(3,startDate);
            stmt.executeUpdate();
           
            logger.debug("UserHistory Summary for "+startDate);
           
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
            close(rs);
        }
    }
    
    
    private void addUpdateDeleteBeverageToIgnore(Element toHandle, Element toAppend) throws HandlerException {        
        int callerId                        = getCallerId(toHandle);
        int location                        = HandlerUtils.getRequiredInteger(toHandle, "locationId");

        String checkPlu                     =" SELECT id,name FROM beverageToIgnore   WHERE location=? AND plu=? LIMIT 1";
        String getLastId                    =" SELECT LAST_INSERT_ID()";
        String insertBev                    = "INSERT INTO beverageToIgnore (name, location, plu ) VALUES (?,?,?)";        
        String updateBeverage               = "UPDATE beverageToIgnore SET plu=? WHERE id=?";
        String deleteBeverage               = "DELETE FROM beverageToIgnore WHERE id=?";

        PreparedStatement stmt              = null;
        ResultSet rs                        = null ;        
        
        try {
            if(callerId > 0 && location > 0 && isValidAccessUser(callerId, location,false)){
                 Iterator delBev                = toHandle.elementIterator("deleteBeverage");
             while (delBev.hasNext()) {
                Element bevPlu              = (Element) delBev.next();                
                String plu                  = HandlerUtils.nullToEmpty(HandlerUtils.getRequiredString(bevPlu, "plu"));
                stmt                        = transconn.prepareStatement(checkPlu);
                stmt.setInt(1, location);
                stmt.setString(2, plu);
                rs                          = stmt.executeQuery();
                if(rs.next()){                    
                    stmt                    = transconn.prepareStatement(deleteBeverage);
                    stmt.setInt(1, rs.getInt(1));
                    stmt.executeUpdate();
                     
                } else {
                        addErrorDetail(toAppend, "Not able to fine PLU #"+plu   );
                    }

            }
              Iterator updateBev             = toHandle.elementIterator("updateBeverage");
             while (updateBev.hasNext()) {
                Element bevPlu              = (Element) updateBev.next();                   
                String newPlu               = HandlerUtils.nullToEmpty(HandlerUtils.getRequiredString(bevPlu, "newPlu"));                
                String oldPlu               = HandlerUtils.nullToEmpty(HandlerUtils.getOptionalString(bevPlu, "oldPlu"));                  
                 
                // logger.debug("id: "+id +"plu "+ plu +" " );
                stmt                        = transconn.prepareStatement(checkPlu);
                stmt.setInt(1, location);
                stmt.setString(2, oldPlu);
                rs                          = stmt.executeQuery();
                if (rs.next())  {
                    int id                  = rs.getInt(1);
                    stmt                    = transconn.prepareStatement(checkPlu);
                    stmt.setInt(1, location);
                    stmt.setString(2, newPlu);
                    rs                      = stmt.executeQuery();
                    if (!rs.next())  {
                        stmt                = transconn.prepareStatement(updateBeverage);
                        stmt.setString(1, newPlu);                    
                        stmt.setInt(2, id);
                        stmt.executeUpdate();
                    } else {
                        addErrorDetail(toAppend, "PLU #"+newPlu +" already exist!"  );
                    }

                    
                }

            }
             
             Iterator beverage   = toHandle.elementIterator("addBeverage");
             while (beverage.hasNext()) {
                Element bevPlu              = (Element) beverage.next();                                
                String plu                  = HandlerUtils.nullToEmpty(HandlerUtils.getRequiredString(bevPlu, "plu"));                
                String name                 = HandlerUtils.nullToEmpty(HandlerUtils.getOptionalString(bevPlu, "name"));                  
                 
                // logger.debug("id: "+id +"plu "+ plu +" " );
                stmt                        = transconn.prepareStatement(checkPlu);
                stmt.setInt(1, location);
                stmt.setString(2, plu);
                rs                          = stmt.executeQuery();
                if (!rs.next())  {
                    stmt                = transconn.prepareStatement(insertBev);
                    stmt.setString(1, name);
                    stmt.setInt(2, location);
                    stmt.setString(3, plu);
                    stmt.executeUpdate();
                } else {
                     addErrorDetail(toAppend, "PLU #"+plu +" already exist!"  );
                }

            }
             
            

            
            } else {
                addErrorDetail(toAppend, "Invalid Access"  );
            }
           
        } catch (SQLException sqle) {
            logger.dbError("Database error in addUpdateDeleteBeverageToIgnore: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            
            close(rs);
            close(stmt);
        }

    }

}
