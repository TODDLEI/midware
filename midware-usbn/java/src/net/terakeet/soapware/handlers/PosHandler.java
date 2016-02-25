package net.terakeet.soapware.handlers;
/*
 * PosHandler.java
 *
 */

import net.terakeet.soapware.*;
import net.terakeet.soapware.security.*;
import net.terakeet.util.MidwareLogger;
import net.terakeet.util.BreadCrumb;
import net.terakeet.util.BOSSClancy;
import net.terakeet.util.TemplatedMessage;
import net.terakeet.soapware.handlers.report.*;
import net.terakeet.util.MailException;

import org.dom4j.Element;
import java.text.ParseException;
import java.io.*;
import java.util.*;
import java.util.regex.*;
import java.sql.*;
import java.util.Set;
import java.text.SimpleDateFormat;
import org.json.JSONObject;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.XML;

/** Class that contains methods to handle sales data from POS devices. */
public class PosHandler implements Handler {

    private LocationMap locationMap;
    private MidwareLogger logger;
    private RegisteredConnection conn;
    static final String connName = "auper";
    private String function;
    private static SimpleDateFormat dbDateFormat
                                            = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public PosHandler() {
        HandlerUtils.initializeClientKeyManager();
        logger = new MidwareLogger(PosHandler.class.getName());
        conn = null;
    }

    public void handle(Element toHandle, Element toAppend) throws HandlerException {
        String clientKey = HandlerUtils.getOptionalString(toHandle, "clientKey");
        SecureSession ss = ClientKeyManager.getSession(clientKey);

        function = toHandle.getName();
        String responseNamespace = (String) SOAPMessage.getURIMap().get("tkmsg");

        logger = new MidwareLogger(PosHandler.class.getName(), function);
        logger.debug("PosHandler processing method: " + function);
        logger.xml("request: " + toHandle.asXML());

        conn = DatabaseConnectionManager.getNewConnection(connName,
                function + " (PosHandler)");

        try {
            if ("posPing".equals(function)) {
                posPing(toHandle, responseFor(function, toAppend), ss);
            } else if ("posError".equals(function)) {
                posError(toHandle, responseFor(function, toAppend), ss);
            } else if ("getPosIds".equals(function)) {
                getPosIds(toHandle, responseFor(function, toAppend), ss);
            } else if ("getSkuIds".equals(function)) {
                getSkuIds(toHandle, responseFor(function, toAppend), ss);
            } else if ("getGlobalLocationIds".equals(function)) {
                getGlobalLocationIds(toHandle, responseFor(function, toAppend), ss);
            } else if ("getLocationId".equals(function)) {
                getLocationId(toHandle, responseFor(function, toAppend), ss);
            } else if ("getLastTransaction".equals(function)) {
                getLastTransaction(toHandle, responseFor(function, toAppend), ss);
            } else if ("getLastBrassTapProductId".equals(function)) {
                getLastBrassTapProductId(toHandle, responseFor(function, toAppend), ss);
            } else if ("getBrassTapChanges".equals(function)) {
                getBrassTapChanges(toHandle, responseFor(function, toAppend), ss);
            } else if ("getLastBrassTapPLUId".equals(function)) {
                getLastBrassTapPLUId(toHandle, responseFor(function, toAppend), ss);
            } else if ("storeAlohaPLUData".equals(function)) {
                storeAlohaPLUData(toHandle, responseFor(function, toAppend), ss);
            } else if ("posDetail".equals(function)) {
                posDetail(toHandle, responseFor(function, toAppend), ss);
            } else if ("storeBrassTapProducts".equals(function)) {
                storeBrassTapProducts(toHandle, responseFor(function, toAppend), ss);
            } else if ("storePOSitouchCheck".equals(function)) {
                storeSendOrderChecksWrapper(toHandle, responseFor(function, toAppend), ss);
            } else if ("storeMicrosSIMCheck".equals(function)) {
                storeMicrosSIMCheck(toHandle, responseFor(function, toAppend), ss);
            } else if ("storeAppleOneData".equals(function)) {
                storeDbfData(toHandle, responseFor(function, toAppend), ss);
            } else if ("storeDigitalDiningData".equals(function)) {
                storeDbfData(toHandle, responseFor(function, toAppend), ss);
            } else if ("storePOSiTouchProcessorDBF".equals(function)) {
                storeDbfData(toHandle, responseFor(function, toAppend), ss);
            } else if ("storeAlohaData".equals(function)) {
                storeDbfData(toHandle, responseFor(function, toAppend), ss);
            } else if ("storeDigitalDiningMultiBarData".equals(function)) {
                storeDbfMultiBarData(toHandle, responseFor(function, toAppend), ss);
            } else if ("storeAlohaMultiBarData".equals(function)) {
                storeDbfMultiBarData(toHandle, responseFor(function, toAppend), ss);
            } else if ("storeLionwiseData".equals(function)) {
                storeDbfMultiBarData(toHandle, responseFor(function, toAppend), ss);
            } else if ("storeHSIData".equals(function)) {
                storeDbfMultiBarData(toHandle, responseFor(function, toAppend), ss);
            } else if ("storeDbfMultiBarData".equals(function)) {
                storeDbfMultiBarData(toHandle, responseFor(function, toAppend), ss);
            } else if ("storeGlobalPurchaseData".equals(function)) {
                storeGlobalPurchaseData(toHandle, responseFor(function, toAppend), ss);
            } else if ("storeMicrosE7Data".equals(function)) {
                storeMicros9700ChecksWrapper(toHandle, responseFor(function, toAppend), ss);
            } else if ("storeMicrosTangentData".equals(function)) {
                storeMicros9700ChecksWrapper(toHandle, responseFor(function, toAppend), ss);
            } else if ("storeMicros9700MultiBarData".equals(function)) {
                storeMicros9700ChecksWrapper(toHandle, responseFor(function, toAppend), ss);
            } else if ("storeMicros9700Data".equals(function)) {
                storeMicros9700ChecksWrapper(toHandle, responseFor(function, toAppend), ss);
            }else if ("storeJBMData".equals(function)) {
                storeMicros9700ChecksWrapper(toHandle, responseFor(function, toAppend), ss);
            }  else if ("storeMicrosMultiBarData".equals(function)) {
                storeMicrosMultiBarData(toHandle, responseFor(function, toAppend), ss);
            } else if ("storeMicrosData".equals(function)) {
                storeMicrosData(toHandle, responseFor(function, toAppend), ss);
            } else if ("storeMicros8700Data".equals(function)) {
                storeMicrosData(toHandle, responseFor(function, toAppend), ss);
            } else if ("storeGlobalData".equals(function)) {
                storeChecksWrapper(toHandle, responseFor(function, toAppend), ss);
            } else if ("storeMicros8700Check".equals(function)) {
                storeChecksWrapper(toHandle, responseFor(function, toAppend), ss);
            } else if ("storeQuestData".equals(function)) {
                storeChecksWrapper(toHandle, responseFor(function, toAppend), ss);
            } else if ("storeHSIData".equals(function)) {
                storeChecksWrapper(toHandle, responseFor(function, toAppend), ss);
            } else if ("storeMaitreDCheck".equals(function)) {
                storeChecksWrapper(toHandle, responseFor(function, toAppend), ss);
            } else if ("storePixelPointCheck".equals(function)) {
                storeChecksWrapper(toHandle, responseFor(function, toAppend), ss);
            } else if ("storeCheck".equals(function)) {
                storeChecksWrapper(toHandle, responseFor(function, toAppend), ss);
            } else if ("storePOSitouchCheck2".equals(function)) {
                storeChecksWrapper(toHandle, responseFor(function, toAppend), ss);
            } else if ("storePOSitouchMultiBarCheck".equals(function)) {
                storeMultiBarCheckWrapper(toHandle, responseFor(function, toAppend), ss);
            } else if ("storeVectorPlusMultiBarCheck".equals(function)) {
                storeMultiBarCheckWrapper(toHandle, responseFor(function, toAppend), ss);
            } else if ("storeMultiBarCheck".equals(function)) {
                storeMultiBarCheckWrapper(toHandle, responseFor(function, toAppend), ss);
            } else if ("storeEmbedCheck".equals(function)) {
                storeMultiBarCheckWrapper(toHandle, responseFor(function, toAppend), ss);
            } else if ("storeOpenCheckData".equals(function)) {
                storeOpenCheckData(toHandle, responseFor(function, toAppend), ss);
            } else if ("storeAramarkCheckData".equals(function)) {
                storeAramarkCheckData(toHandle, responseFor(function, toAppend), ss);
            } else if ("storeHootersData".equals(function)) {
                storeRealTimeChecksWrapper(toHandle, responseFor(function, toAppend), ss);
            } else if ("getExportLocations".equals(function)) {
                getExportLocations(toHandle, responseFor(function, toAppend), ss);
            } else if ("getLocationDataDump".equals(function)) {
                getLocationDataDump(toHandle, responseFor(function, toAppend), ss);
            } else if ("getBreadcrumbData".equals(function)) {
                getBreadcrumbData(toHandle, responseFor(function,toAppend));
            } else if ("getBOSSClancyData".equals(function)) {
                getBOSSClancyData(toHandle, responseFor(function,toAppend));
            } else if ("storeAlohaScreenData".equals(function)) {
                storeAlohaScreenData(toHandle, responseFor(function,toAppend), ss);
            } else {
                logger.generalWarning("Unknown function '" + function + "'.");
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
            int queryCount = conn.getQueryCount();
            logger.dbAction("Executed " + queryCount + " quer" + (queryCount == 1 ? "y" : "ies"));
            conn.close();
        }
        logger.xml("response: " + toAppend.asXML());

    }

    private Element responseFor(String s, Element e) {
        String responseNamespace = (String) SOAPMessage.getURIMap().get("tkmsg");
        return e.addElement("m:" + s + "Response", responseNamespace);
    }

    private void close(Statement s) {
        if (s != null) {
            try {
                s.close();
                s = null;
            } catch (SQLException sqle) {
            }
        }
    }

    private void close(ResultSet rs) {
        if (rs != null) {
            try {
                rs.close();
                rs = null;
            } catch (SQLException sqle) {
            }
        }
    }

    private void close(Connection c) {
        if (c != null) {
            try {
                c.close();
                c = null;
            } catch (SQLException sqle) {
            }
        }
    }

    private void close(RegisteredConnection c) {
        if (c != null) {
            c.close();
        }
    }

    /** Class representing a detail item. */
    class ItemTuple {

        private String id;
        private int costCenter;
        private double qty;
        private long epoch;

        public ItemTuple(String id, double qty, long epoch) {
            this.id = id;
            this.qty = qty;
            this.epoch = epoch;
        }

        public ItemTuple(String id, int costCenter, double qty, long epoch) {
            this.id = id;
            this.costCenter = costCenter;
            this.qty = qty;
            this.epoch = epoch;
        }

        public ItemTuple(String id, int costCenter, double qty) {
            this.id = id;
            this.costCenter = costCenter;
            this.qty = qty;
        }

        public String getId() {
            return id;
        }

        public int getCostCenter() {
            return costCenter;
        }

        public double getQty() {
            return qty;
        }

        public long getEpoch() {
            return epoch;
        }
    }

    /** Pings from the POS device are used to sync time
     *  and determine if the POS system is down.
     */
    private void posPing(Element toHandle, Element toAppend, SecureSession ss) throws HandlerException {

        int location                        = HandlerUtils.getRequiredInteger(toHandle, "locationId");
        String processorName                = HandlerUtils.getOptionalString(toHandle, "processorName");
        String processorVersion             = HandlerUtils.getOptionalString(toHandle, "processorVersion");
        String gatewayVersion               = HandlerUtils.getOptionalString(toHandle, "gatewayVersion");
        if (location != ss.getLocation()) {
            logger.readingAccessViolation("Called posPing as L#" + location + ", but authenticated as L#" + ss.getLocation());
        }

        java.util.Date now                  = new java.util.Date();
        Long epoch                          = new Long(now.getTime() / 1000L); // seconds
        toAppend.addElement("epoch").addText(epoch.toString());
        toAppend.addElement("success").addText("true");

        PreparedStatement stmt              = null;
        ResultSet rs                        = null;

        try {
            StringBuilder emailBody         = new StringBuilder();

            // record that a sales reading happened in the location table
            String select                   = "SELECT uA.id, u.name, u.email, l.name FROM userAlerts uA LEFT JOIN user u ON u.id = uA.user LEFT JOIN location l ON l.id = uA.tableId " +
                                            " WHERE uA.alert = 2 AND uA.tableType = 2 AND uA.active = 1 AND uA.tableId = ?";
            String updateLocation           = "UPDATE location SET lastSold=NOW(), processorName=?, processorVersion=?, gatewayVersion=? WHERE id=?";
            String updateLocationDetails    = "UPDATE locationDetails SET soldUp = 1 WHERE location = ? ";
            String updateUserAlerts         = "UPDATE userAlerts SET count = 0, active = 0 WHERE id = ? ";


            int index                       = 1;
            stmt                            = conn.prepareStatement(updateLocation);
            if (null == processorName) {
                stmt.setNull(index, Types.VARCHAR);
            } else {
                stmt.setString(index, processorName);
            }
            index++;
            if (null == processorVersion) {
                stmt.setNull(index, Types.VARCHAR);
            } else {
                stmt.setString(index, processorVersion);
            }
            index++;
            if (null == gatewayVersion) {
                stmt.setNull(index, Types.VARCHAR);
            } else {
                stmt.setString(index, gatewayVersion);
            }
            index++;
            stmt.setInt(index++, location);
            stmt.executeUpdate();

            stmt                            = conn.prepareStatement(updateLocationDetails);
            stmt.setInt(1, location);
            stmt.executeUpdate();

            /*
            stmt                            = conn.prepareStatement(select);
            stmt.setInt(1, location);
            rs                              = stmt.executeQuery();
            while (rs.next()) {
                stmt                        = conn.prepareStatement(updateUserAlerts);
                stmt.setInt(1, rs.getInt(1));
                stmt.executeUpdate();
                logger.debug("Resetting sold alert(s) for " + rs.getString(4));

                emailBody.append("<tr align=justify><td colspan=4>Your USBN Gateway at ");
                emailBody.append(HandlerUtils.nullToEmpty(rs.getString(4))); // Last Poured Reading
                emailBody.append(" has <strong> resumed service. </strong>It is now reporting draft beer sales data to our systems.</td></tr>");
                emailBody.append("<tr align=center valign=middle><td height=20 colspan=4>&nbsp;</td></tr>");
                emailBody.append("<tr align=justify><td colspan=4>Thank You,</td></tr>");
                emailBody.append("<tr align=justify><td colspan=4>US Beverage Net Support</td></tr>");
                emailBody.append("<tr align=center valign=middle><td height=35 colspan=4>&nbsp;</td></tr>");
                emailBody.append("<tr align=justify><td colspan=4><strong>This email was automatically generated; please do not reply.</strong></td></tr><tr><td colspan=4>&nbsp;</td></tr>");
                sendMail(rs.getString(2), rs.getString(3), "Location Notification", emailBody);
            }*/
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
        } finally {
            close(stmt);
            close(rs);
            locationMap = null;
        }
    }

    /** Stores an error from a POS client
     */
    private void posError(Element toHandle, Element toAppend, SecureSession ss) throws HandlerException {

        int location                        = HandlerUtils.getRequiredInteger(toHandle, "locationId");
        if (location != ss.getLocation()) {
            logger.readingAccessViolation("Called posPing as L#" + location + ", but authenticated as L#" + ss.getLocation());
            throw new HandlerException("Access Violation");
        }

        boolean success                     = false;
        long epoch                          = HandlerUtils.getRequiredLong(toHandle, "epoch"); // seconds
        Timestamp ts                        = new Timestamp(epoch * 1000L); // milliseconds
        String processorName                = HandlerUtils.getRequiredString(toHandle, "processorName");
        String processorVersion             = HandlerUtils.getRequiredString(toHandle, "processorVersion");
        String gatewayVersion               = HandlerUtils.getRequiredString(toHandle, "gatewayVersion");
        String message                      = HandlerUtils.getRequiredString(toHandle, "message");
        String level                        = HandlerUtils.getRequiredString(toHandle, "level");
        level                               = level.toUpperCase();
        if (!("WARN".equals(level) || "ERROR".equals(level) || "FATAL".equals(level))) {
            throw new HandlerException("Bad error level: " + level);
        }

        PreparedStatement stmt              = null;
        try {
            if (!message.contains("OutOfStock") || !message.contains("PrepOrder") || !message.contains("TimeClockTransaction") || !message.contains("CanceledCheck")) {
            String sql                      = " INSERT INTO clientError (datetime, location, level, message, processorName, processorVersion, gatewayVersion) VALUES (?, ?, ?, ?, ?, ?, ?)";
            stmt                            = conn.prepareStatement(sql);
            int index                       = 1;
            stmt.setTimestamp(index++, ts);
            stmt.setInt(index++, location);
            stmt.setString(index++, level);
            stmt.setString(index++, message);
            stmt.setString(index++, processorName);
            stmt.setString(index++, processorVersion);
            stmt.setString(index++, gatewayVersion);
            stmt.executeUpdate();
            success                         = true;
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
        } finally {
            close(stmt);
        }
        toAppend.addElement("success").addText(String.valueOf(success));
    }

    private void getGlobalLocationIds(Element toHandle, Element toAppend, SecureSession ss) throws HandlerException {

        String sql = "SELECT store FROM customerStoreId";

        PreparedStatement stmt = null;
        ResultSet rs = null;

        try {
            stmt = conn.prepareStatement(sql);
            rs = stmt.executeQuery();
            while (rs.next()) {
                String id = rs.getString(1);
                if (null != id) {
                    toAppend.addElement("id").addText(id);
                }
            }

        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            logger.dbAction("Executed 1 query.");
            close(rs);
            close(stmt);
        }

    }

    private void getLocationId(Element toHandle, Element toAppend, SecureSession ss) throws HandlerException {



        int store = HandlerUtils.getRequiredInteger(toHandle, "storeId");

        String getLocationId = "SELECT location FROM customerStoreId WHERE store=?";

        PreparedStatement stmt = null;
        ResultSet rs = null;

        try {

            stmt = conn.prepareStatement(getLocationId);
            stmt.setInt(1, store);
            rs = stmt.executeQuery();
            if (rs.next()) {
                toAppend.addElement("locationId").addText(String.valueOf(rs.getInt(1)));
            }

        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            logger.dbAction("Executed 1 query.");
            close(rs);
            close(stmt);
        }

    }

    private void getPosIds(Element toHandle, Element toAppend, SecureSession ss) throws HandlerException {
        int location = HandlerUtils.getRequiredInteger(toHandle, "locationId");
        if (location != ss.getLocation()) {
            logger.readingAccessViolation("Called + " + function + " as L#" + location +
                    ", but authenticated as L#" + ss.getLocation());
            throw new HandlerException("Access Violation");
        }

        String sql = "SELECT plu FROM beverage WHERE location = ? ORDER BY plu";

        PreparedStatement stmt = null;
        ResultSet rs = null;

        try {
            stmt = conn.prepareStatement(sql);
            stmt.setInt(1, location);
            rs = stmt.executeQuery();
            while (rs.next()) {
                String id = rs.getString(1);
                if (null != id) {
                    toAppend.addElement("id").addText(id);
                }
            }

        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            logger.dbAction("Executed 1 query.");
            close(rs);
            close(stmt);
        }

    }

    private void getLastTransaction(Element toHandle, Element toAppend, SecureSession ss) throws HandlerException {
        int location = HandlerUtils.getRequiredInteger(toHandle, "locationId");
        if (location != ss.getLocation()) {
            logger.readingAccessViolation("Called + " + function + " as L#" + location +
                    ", but authenticated as L#" + ss.getLocation());
            throw new HandlerException("Access Violation");
        }

        String sql = "SELECT sid, pluNumber, IFNULL(costCenter, 0), quantity, date, IFNULL(checkId, 0), IFNULL(reportRecordId, 0), " +
                    " CONCAT(IF(TIME(date)<'07:00:00', LEFT(SUBDATE(date, INTERVAL 1 DAY),11), LEFT(date,11)),'07:00:00') bDate" +
                    " FROM sales WHERE location = ? AND date BETWEEN CONCAT(LEFT(SUBDATE(NOW(), INTERVAL 14 DAY),11),' 07:00:00') AND ADDDATE(NOW(), INTERVAL 1 DAY) " +
                    " ORDER BY date DESC LIMIT 1 ";

        PreparedStatement stmt = null;
        ResultSet rs = null;

        try {
            stmt = conn.prepareStatement(sql);
            stmt.setInt(1, location);
            rs = stmt.executeQuery();
            if (rs.next()) {
                toAppend.addElement("sid").addText(rs.getString(1));
                toAppend.addElement("plu").addText(rs.getString(2));
                toAppend.addElement("costCenter").addText(rs.getString(3));
                toAppend.addElement("qty").addText(rs.getString(4));
                toAppend.addElement("date").addText(rs.getString(5));
                toAppend.addElement("check").addText(rs.getString(6));
                toAppend.addElement("recordId").addText(rs.getString(7));
                toAppend.addElement("bDate").addText(rs.getString(8));
            } else {
                SimpleDateFormat newDateFormat
                                            = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                Calendar rightNow           = Calendar.getInstance();
                rightNow.set(Calendar.HOUR_OF_DAY, 0);
                rightNow.set(Calendar.MINUTE, 0);
                rightNow.set(Calendar.SECOND, 0);
                toAppend.addElement("sid").addText("0");
                toAppend.addElement("plu").addText("0");
                toAppend.addElement("costCenter").addText("0");
                toAppend.addElement("qty").addText("0");
                toAppend.addElement("date").addText(newDateFormat.format(rightNow.getTime()));
                toAppend.addElement("check").addText("0");
                toAppend.addElement("recordId").addText("0");
                toAppend.addElement("bDate").addText(newDateFormat.format(rightNow.getTime()));
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            logger.dbAction("Executed 1 query.");
            close(rs);
            close(stmt);
        }
    }

    private void getLastBrassTapProductId(Element toHandle, Element toAppend, SecureSession ss) throws HandlerException {
        int location                        = HandlerUtils.getRequiredInteger(toHandle, "locationId");
        if (location != ss.getLocation()) {
            logger.readingAccessViolation("Called + " + function + " as L#" + location + ", but authenticated as L#" + ss.getLocation());
            throw new HandlerException("Access Violation");
        }

        String sql                          = "SELECT beerID FROM brasstapProducts ORDER BY beerID DESC LIMIT 1;";
        PreparedStatement stmt              = null;
        ResultSet rs                        = null;

        try {
            stmt                            = conn.prepareStatement(sql);
            rs                              = stmt.executeQuery();
            if (rs.next()) {
                toAppend.addElement("beerID").addText(rs.getString(1));
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            logger.dbAction("Executed 1 query.");
            close(rs);
            close(stmt);
        }
    }

    private void getBrassTapChanges(Element toHandle, Element toAppend, SecureSession ss) throws HandlerException {

        String sqlSelectLocation            = "SELECT locationID, date_updated, usbnID, wipeout, lineUpdate, priceUpdate FROM brasstapLocations";
        String selectLineChanges            = "SELECT IFNULL(bP1.beerID, 0), l.statusChange, l.local, IFNULL(bP2.beerID, 0), l.product, bLC.id FROM brasstapLineChanges bLC " +
                                            " LEFT JOIN line l ON l.system = bLC.system AND l.lineIndex = bLC.lineIndex LEFT JOIN brasstapProducts bP1 ON bP1.usbnID = l.product " +
                                            " LEFT JOIN brasstapProducts bP2 ON bP2.usbnID = bLC.product WHERE l.status IN ('RUNNING', 'EMPTY') AND l.product != bLC.product " +
                                            " AND bLC.location = ? AND l.product != 4311 ORDER BY l.system, l.lineIndex;";
        String sqlPriceChanges              = "SELECT iP.value FROM inventoryPrices iP LEFT JOIN inventory i ON i.id = iP.inventory LEFT JOIN beverageSize b ON b.id = iP.size " +
                                            " WHERE i.location = ? AND i.product = ?;";
        String selectLines                  = "SELECT bP.beerId, l.statusChange, l.local, l.product FROM location lo LEFT JOIN brasstapLocations bL ON bL.usbnID = lo.id " +
                                            " LEFT JOIN bar b ON b.location = bL.usbnID LEFT JOIN line l ON l.bar = b.id " +
                                            " LEFT JOIN brasstapProducts bP ON bP.usbnID = l.product LEFT JOIN product p ON p.id = l.product " +
                                            " WHERE l.status = 'RUNNING' AND p.id NOT IN (4311) AND lo.id = ? ORDER BY bP.beerId;";
        String sqlPrices                    = "SELECT bP.beerId, iP.value FROM location lo LEFT JOIN brasstapLocations bL ON bL.usbnID = lo.id " +
                                            " LEFT JOIN bar b ON b.location = bL.usbnID LEFT JOIN line l ON l.bar = b.id " +
                                            " LEFT JOIN brasstapProducts bP ON bP.usbnID = l.product LEFT JOIN product p ON p.id = l.product " +
                                            " LEFT JOIN inventory i ON i.location = lo.id AND i.product = l.product " +
                                            " LEFT JOIN inventoryPrices iP ON iP.inventory = i.id LEFT JOIN beverageSize bS ON bS.id = iP.size " +
                                            " WHERE l.status = 'RUNNING' AND p.id NOT IN (4311) AND lo.id = ? ORDER BY bP.beerId;";

        
        PreparedStatement stmt              = null;
        ResultSet rs                        = null, outerRS = null, priceRS = null;
        HashMap<Integer, Integer> addedProduct
                                            = new HashMap<Integer, Integer>();
        HashMap<Integer, Integer> deletedProduct
                                            = new HashMap<Integer, Integer>();

        try {
            stmt                            = conn.prepareStatement(sqlSelectLocation);
            outerRS                         = stmt.executeQuery();
            while (outerRS.next()) {
                int brasstapID              = outerRS.getInt(1);
                String date_updated         = outerRS.getString(2);
                int location                = outerRS.getInt(3);
                int wipeout                 = outerRS.getInt(4);
                int lineUpdate              = outerRS.getInt(5);
                int priceUpdate             = outerRS.getInt(6);

                if (lineUpdate == 1) {
                    stmt                    = conn.prepareStatement(selectLineChanges);
                    stmt.setInt(1, location);
                    rs                      = stmt.executeQuery();
                    while (rs.next()) {
                        Double price        = 6.0;
                        if (rs.getInt(1) != 0 && (!addedProduct.containsKey(rs.getInt(1)))) {
                            Element lineUpdateE1
                                            = toAppend.addElement("addLines");
                            lineUpdateE1.addElement("beerID").addText(String.valueOf(rs.getInt(1)));
                            lineUpdateE1.addElement("locationID").addText(String.valueOf(brasstapID));
                            lineUpdateE1.addElement("date_added").addText(HandlerUtils.nullToEmpty(rs.getString(2)));
                            lineUpdateE1.addElement("beer_type").addText("Draft");
                            lineUpdateE1.addElement("local_beer").addText(String.valueOf(rs.getInt(3)));
                            lineUpdateE1.addElement("wipeOut").addText("0");
                            stmt            = conn.prepareStatement(sqlPriceChanges);
                            stmt.setInt(1, location);
                            stmt.setInt(2, rs.getInt(5));
                            priceRS         = stmt.executeQuery();
                            if (priceRS.next()) {
                                price       = priceRS.getDouble(1);
                            }
                            lineUpdateE1.addElement("beer_price").addText(String.valueOf(price));
                            addedProduct.put(rs.getInt(1), 1);
                        }
                        if (rs.getInt(4) != 0 && (!deletedProduct.containsKey(rs.getInt(4)))) {
                            Element lineUpdateE1
                                            = toAppend.addElement("delLines");
                            lineUpdateE1.addElement("beerID").addText(String.valueOf(rs.getInt(4)));
                            lineUpdateE1.addElement("locationID").addText(String.valueOf(brasstapID));
                            lineUpdateE1.addElement("beer_type").addText(HandlerUtils.nullToEmpty("Draft"));
                            deletedProduct.put(rs.getInt(4), 1);
                            
                            stmt            = conn.prepareStatement("UPDATE brasstapLineChanges SET product = ? WHERE id = ?;");
                            stmt.setInt(1, rs.getInt(5));
                            stmt.setInt(2, rs.getInt(6));
                            stmt.executeUpdate();
                        }
                    }
                }
                
                addedProduct                = new HashMap<Integer, Integer>();
                if (wipeout == 1) {
                    stmt                    = conn.prepareStatement(selectLines);
                    stmt.setInt(1, location);
                    rs                      = stmt.executeQuery();
                    while (rs.next()) {
                        Double price        = 6.0;
                        if ((!addedProduct.containsKey(rs.getInt(1)))) {
                            Element lineUpdateE1
                                            = toAppend.addElement("addLines");
                            lineUpdateE1.addElement("beerID").addText(String.valueOf(rs.getInt(1)));
                            lineUpdateE1.addElement("locationID").addText(String.valueOf(brasstapID));
                            lineUpdateE1.addElement("date_added").addText(HandlerUtils.nullToEmpty(rs.getString(2)));
                            lineUpdateE1.addElement("beer_type").addText("Draft");
                            lineUpdateE1.addElement("local_beer").addText(String.valueOf(rs.getInt(3)));
                            lineUpdateE1.addElement("wipeOut").addText("1");
                            stmt            = conn.prepareStatement(sqlPriceChanges);
                            stmt.setInt(1, location);
                            stmt.setInt(2, rs.getInt(4));
                            priceRS         = stmt.executeQuery();
                            if (priceRS.next()) {
                                price       = priceRS.getDouble(1);
                            }
                            lineUpdateE1.addElement("beer_price").addText(String.valueOf(price));
                            addedProduct.put(rs.getInt(1), 1);
                        }
                    }
                }

                addedProduct                = new HashMap<Integer, Integer>();
                if (priceUpdate == 1) {
                    stmt                    = conn.prepareStatement(sqlPrices);
                    stmt.setInt(1, location);
                    rs                      = stmt.executeQuery();
                    while (rs.next()) {
                        if ((!addedProduct.containsKey(rs.getInt(1)))) {
                            Element priceUpdateE1
                                            = toAppend.addElement("priceUpdates");
                            priceUpdateE1.addElement("beerID").addText(String.valueOf(rs.getInt(1)));
                            priceUpdateE1.addElement("beer_price").addText(String.valueOf(rs.getDouble(2)));
                            priceUpdateE1.addElement("locationID").addText(String.valueOf(brasstapID));
                            priceUpdateE1.addElement("beer_type").addText("Draft");
                            addedProduct.put(rs.getInt(1), 1);
                        }
                    }
                }

                //Update last check in time
                stmt                        = conn.prepareStatement("UPDATE brasstapLocations SET wipeout = 0, lineUpdate = 0, priceUpdate = 0, date_updated = NOW() WHERE usbnID = ?;");
                stmt.setInt(1, location);
                stmt.executeUpdate();
                /**/
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            logger.dbAction("Executed 1 query.");
            close(rs);
            close(outerRS);
            close(priceRS);
            close(stmt);
        }
    }

    private boolean storeBrassTapProducts(Element toHandle, Element toAppend, SecureSession ss) throws HandlerException {

        int location                        = HandlerUtils.getRequiredInteger(toHandle, "locationId");
        if (location != ss.getLocation()) {
            logger.readingAccessViolation("Called + " + function + " as L#" + location + ", but authenticated as L#" + ss.getLocation());
            throw new HandlerException("Access Violation");
        }

        boolean success                     = false;
        HashMap<String, Integer> StyleMap   = new HashMap<String, Integer>();
        String sqlSelectStyles              = "SELECT style, styleId FROM brasstapStyles;";
        PreparedStatement stmt              = null;
        ResultSet rs                        = null;

        try {
            stmt                            = conn.prepareStatement(sqlSelectStyles);
            rs                              = stmt.executeQuery();
            while (rs.next()) {
                StyleMap.put(rs.getString(1), rs.getInt(2));
            }

            List beerList                   = toHandle.elements("beer");
            if (null != beerList && beerList.size() > 0) {

                String insertNewUSBNProduct = "INSERT INTO product (name, pos) VALUES (?, 1)";
                String insertNewProductDesc = "INSERT INTO productDescription (product, boardName, abv, origin, category) VALUES (?, ?, ?, ?, 2)";
                String insertNewBTProduct   = "INSERT INTO brasstapProducts (usbnID, beerID, name, brewery, brewery_location, style, clarity, color, abv_info, description) " +
                                            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

                String selectBrewery        = "SELECT id FROM productSet WHERE name = ? AND productSetType = 7;";
                String insertNewBrewery     = "INSERT INTO productSet (name, productSetType) VALUES (?, 7)";
                String insertNewProductSetMP= "INSERT INTO productSetMap (product, productSet) VALUES (?, ?)";
                String sqlSelectLastTransId = "SELECT LAST_INSERT_ID()";
                
                for (Object o : beerList) {
                    Element beer            = (Element) o;
                    int usbnID              = 0;
                    int beerID              = HandlerUtils.getRequiredInteger(beer, "beerID");
                    String brewery          = HandlerUtils.getRequiredString(beer, "brewery");
                    String brewery_location = HandlerUtils.getRequiredString(beer, "brewery_location");
                    String style            = HandlerUtils.getRequiredString(beer, "style");
                    String clarity          = HandlerUtils.getRequiredString(beer, "clarity");
                    String color            = HandlerUtils.getRequiredString(beer, "color");
                    String name             = HandlerUtils.getRequiredString(beer, "name");
                    String abvString        = HandlerUtils.getRequiredString(beer, "abv_info");
                    String description      = HandlerUtils.getRequiredString(beer, "description");
                    Double abv_info         = 0.0;

                    try {
                        abv_info            = Double.parseDouble(abvString);
                    } catch (Exception e) {
                        abv_info            = 0.5;
                    }

                    //adding USBN product
                    stmt                    = conn.prepareStatement(insertNewUSBNProduct);
                    stmt.setString(1, name);
                    stmt.executeUpdate();

                    stmt                    = conn.prepareStatement(sqlSelectLastTransId);
                    rs                      = stmt.executeQuery();
                    if (rs.next()) {
                        usbnID              = rs.getInt(1);

                        //adding USBN product description
                        stmt                = conn.prepareStatement(insertNewProductDesc);
                        stmt.setInt(1, usbnID);
                        stmt.setString(2, name);
                        stmt.setDouble(3, abv_info);
                        stmt.setString(4, brewery_location);
                        stmt.executeUpdate();

                        //adding Brass Tap product
                        stmt                = conn.prepareStatement(insertNewBTProduct);
                        stmt.setInt(1, usbnID);
                        stmt.setInt(2, beerID);
                        stmt.setString(3, name);
                        stmt.setString(4, brewery);
                        stmt.setString(5, brewery_location);
                        stmt.setString(6, style);
                        stmt.setString(7, clarity);
                        stmt.setString(8, color);
                        stmt.setDouble(9, abv_info);
                        stmt.setString(10, description);
                        stmt.executeUpdate();

                        // adding brewery info
                        int breweryId       = 0;
                        stmt                = conn.prepareStatement(selectBrewery);
                        stmt.setString(1, brewery);
                        rs                  = stmt.executeQuery();
                        if (rs.next()) {
                            breweryId       = rs.getInt(1);
                        } else {
                            stmt            = conn.prepareStatement(insertNewBrewery);
                            stmt.setString(1, brewery);
                            stmt.executeUpdate();

                            stmt            = conn.prepareStatement(sqlSelectLastTransId);
                            rs              = stmt.executeQuery();
                            if (rs.next()) {
                                breweryId   = rs.getInt(1);
                            }
                        }

                        stmt                = conn.prepareStatement(insertNewProductSetMP);
                        stmt.setInt(1, usbnID);
                        stmt.setInt(2, breweryId);
                        stmt.executeUpdate();

                        // adding style info
                        int styleId         = (StyleMap.containsKey(style) ? StyleMap.get(style) : 2433);
                        stmt                = conn.prepareStatement(insertNewProductSetMP);
                        stmt.setInt(1, usbnID);
                        stmt.setInt(2, styleId);
                        stmt.executeUpdate();

                        // adding product update information
                        String insertLog    = "INSERT INTO productChangeLog (product,type,date) VALUES (?,1,now())";
                        stmt                = conn.prepareStatement(insertLog);
                        stmt.setInt(1, usbnID);
                        stmt.executeUpdate();
                    }
                }
            }

            List pluList                    = toHandle.elements("newBeerPLU");
            if (null != pluList && pluList.size() > 0) {
                String insertNewBrassTapPLU = "INSERT INTO brasstapLoyaltyID (plu, beerID) VALUES (?, ?)";
                String updateBrassTapBeerID = "UPDATE brasstapItems bI LEFT JOIN brasstapLoyaltyID bLI ON bLI.plu = bI.plu SET bI.beerID = bLI.beerID " +
                                            " WHERE bLI.id IS NOT NULL AND bI.beerID = 0;";
                String checkPLU             = "SELECT id FROM brasstapLoyaltyID WHERE plu = ?;";
                for (Object o : pluList) {
                    Element plu             = (Element) o;
                    String pluString        = HandlerUtils.getRequiredString(plu, "plu");
                    String beerID           = HandlerUtils.getRequiredString(plu, "beerID");

                    stmt                    = conn.prepareStatement(checkPLU);
                    stmt.setString(1, pluString);
                    rs                      = stmt.executeQuery();
                    if (!rs.next()) {
                        stmt                = conn.prepareStatement(insertNewBrassTapPLU);
                        stmt.setString(1, pluString);
                        stmt.setString(2, beerID);
                        stmt.executeUpdate();
                    }
                }
                stmt                        = conn.prepareStatement(updateBrassTapBeerID);
                stmt.executeUpdate();
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            logger.dbAction("Executed 1 query.");
            close(rs);
            close(stmt);
        }
        return success;
    }

    private void getLastBrassTapPLUId(Element toHandle, Element toAppend, SecureSession ss) throws HandlerException {
        int location                        = HandlerUtils.getRequiredInteger(toHandle, "locationId");
        if (location != ss.getLocation()) {
            logger.readingAccessViolation("Called + " + function + " as L#" + location + ", but authenticated as L#" + ss.getLocation());
            throw new HandlerException("Access Violation");
        }

        String selectSizeRange              = "SELECT id, endsWith, ounces, min, max, lastRecord FROM brasstapSizeRange;";
        PreparedStatement stmt              = null;
        ResultSet rs                        = null;

        try {
            stmt                            = conn.prepareStatement(selectSizeRange);
            rs                              = stmt.executeQuery();
            while (rs.next()) {
                Element beerPLUEl           = toAppend.addElement("beerPLU");
                beerPLUEl.addElement("id").addText(rs.getString(1));
                beerPLUEl.addElement("endsWith").addText(rs.getString(2));
                beerPLUEl.addElement("ounces").addText(rs.getString(3));
                beerPLUEl.addElement("min").addText(rs.getString(4));
                beerPLUEl.addElement("max").addText(rs.getString(5));
                beerPLUEl.addElement("lastRecord").addText(rs.getString(6));
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            logger.dbAction("Executed 1 query.");
            close(rs);
            close(stmt);
        }
    }

    private boolean storeAlohaPLUData(Element toHandle, Element toAppend, SecureSession ss) throws HandlerException {

        boolean success                     = false;
        PreparedStatement stmt              = null;
        ResultSet rs                        = null, innerRS = null;

        try {
            List pluList                    = toHandle.elements("newPLU");
            if (null != pluList && pluList.size() > 0) {
                String insertNewBrassTapPLU = "INSERT INTO brasstapItems (plu, shortname, chitname, longname, beerName, ounces) VALUES (?, ?, ?, ?, ?, ?)";
                String checkPLU             = "SELECT id FROM brasstapItems WHERE plu = ?;";
                for (Object o : pluList) {
                    Element plu             = (Element) o;
                    String pluString        = HandlerUtils.getRequiredString(plu, "plu");
                    String shortName        = HandlerUtils.getRequiredString(plu, "shortName");
                    String chitName         = HandlerUtils.getRequiredString(plu, "chitName");
                    String longName         = HandlerUtils.getRequiredString(plu, "longName");
                    String beerName         = HandlerUtils.getRequiredString(plu, "beerName");
                    Double ounces           = HandlerUtils.getRequiredDouble(plu, "ounces");

                    stmt                    = conn.prepareStatement(checkPLU);
                    stmt.setString(1, pluString);
                    rs                      = stmt.executeQuery();
                    if (!rs.next()) {
                        stmt                = conn.prepareStatement(insertNewBrassTapPLU);
                        stmt.setString(1, pluString);
                        stmt.setString(2, shortName);
                        stmt.setString(3, chitName);
                        stmt.setString(4, longName);
                        stmt.setString(5, beerName);
                        stmt.setDouble(6, ounces);
                        stmt.executeUpdate();
                    }
                }

                String selectSizeRange      = "SELECT id, min, max, lastRecord FROM brasstapSizeRange;";
                stmt                        = conn.prepareStatement(selectSizeRange);
                rs                          = stmt.executeQuery();
                while (rs.next()) {
                    String selectMaxPLU     = "SELECT plu FROM brasstapItems WHERE plu BETWEEN ? AND ? ORDER BY plu DESC LIMIT 1;";
                    stmt                    = conn.prepareStatement(selectMaxPLU);
                    stmt.setInt(1, rs.getInt(2));
                    stmt.setInt(2, rs.getInt(3));
                    innerRS                 = stmt.executeQuery();
                    if (innerRS.next()) {
                        if (innerRS.getInt(1) != rs.getInt(4)) {
                            stmt            = conn.prepareStatement("UPDATE brasstapSizeRange SET lastRecord = ? WHERE id = ?;");
                            stmt.setInt(1, innerRS.getInt(1));
                            stmt.setInt(2, rs.getInt(1));
                            stmt.executeUpdate();
                        }
                    }
                }
                success                     = true;
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            logger.dbAction("Executed 1 query.");
            close(innerRS);
            close(rs);
            close(stmt);
        }
        return success;
    }

    private boolean checkForeignKey(String table, String field, int value) throws SQLException, HandlerException {

        PreparedStatement stmt = null;
        ResultSet rs = null;
        boolean result = false;

        String select = "SELECT " + field + " FROM " + table +
                " WHERE " + field + " = ?";

        stmt = conn.prepareStatement(select);
        stmt.setInt(1, value);
        rs = stmt.executeQuery();


        result = rs.next();

        close(rs);
        close(stmt);
        return result;
    }

    private boolean checkExportKey(String exportKey, int type, int value) throws SQLException, HandlerException {

        PreparedStatement stmt              = null;
        ResultSet rs                        = null;
        boolean result                      = false;

        String select                       = "SELECT IFNULL(c.id, 0), IFNULL(l.id, 0) FROM exportKeyMap e LEFT JOIN customer c ON c.id = e.customer " +
                                            " LEFT JOIN location l ON l.id = e.location WHERE e.exportKey = " + exportKey;

        stmt                                = conn.prepareStatement(select);
        rs                                  = stmt.executeQuery();
        while (rs.next()) {
            switch(type) {
                case 1:
                    if (rs.getInt(1) == value) {
                        result              = true;
                        rs.last();
                    }
                    break;
                case 2:
                    if (rs.getInt(2) == value) {
                        result              = true;
                        rs.last();
                    }
                    break;
            }
        }

        close(rs);
        close(stmt);
        return result;
    }

    private void getExportLocations(Element toHandle, Element toAppend, SecureSession ss) throws HandlerException {

        String exportKey                    = HandlerUtils.getOptionalString(toHandle, "exportKey");
        int locationId                      = HandlerUtils.getOptionalInteger(toHandle, "locationId");
        int refCustomerId                   = HandlerUtils.getOptionalInteger(toHandle, "customerId");

        PreparedStatement stmt              = null;
        ResultSet rs                        = null;

        int paramsSet                       = 0;
        if (locationId >= 0) {
            paramsSet++;
        }
        if (refCustomerId >= 0) {
            paramsSet++;
        }
        if (paramsSet > 1) {
            throw new HandlerException("Only one parameter can be set for getLocations.");
        }

        try {
            String select                   = " SELECT l.id, l.name, l.addrStreet, l.addrCity, l.addrZip, l.addrState, l.customer, c.name, l.brixMin, l.brixMax, " +
                                            " l.lastPoured, l.lastSold, TIME_TO_SEC(TIMEDIFF(NOW(),l.lastPoured)) AS pouredAge," +
                                            " TIME_TO_SEC(TIMEDIFF(NOW(),l.lastSold)) AS soldAge, l.picoPowerup, l.picoVersion, l.easternOffset, " +
                                            " l.latitude, l.longitude, l.zoomLevel, l.varianceAlert, l.draftLines, l.concept, l.volImpact, l.type, l.volAdjustment " +
                                            " FROM customer c LEFT JOIN location l ON c.id=l.customer LEFT JOIN locationDetails lD ON lD.location = l.id ";
            if (refCustomerId >= 0) {
                String selectByCustomerId = select + " WHERE lD.active = 1 AND l.customer = ? ";
                selectByCustomerId          += " ORDER BY l.name ASC";

                stmt                        = conn.prepareStatement(selectByCustomerId);
                stmt.setInt(1, refCustomerId);
                rs                          = stmt.executeQuery();
                getLocationXML(toAppend, rs);
            } else if (locationId >= 0) {
                String selectById           = select + " WHERE l.id = ? ";
                stmt                        = conn.prepareStatement(selectById);
                stmt.setInt(1, locationId);
                rs                          = stmt.executeQuery();
                getLocationXML(toAppend, rs);
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(rs);
            close(stmt);
        }
    }

    private void getLocationXML(Element toAppend, ResultSet rs) throws SQLException {
        PreparedStatement stmt = null;
        ResultSet rs1 = null;

        String selectProductType = "SELECT DISTINCT p.pType FROM inventory i" +
                " LEFT JOIN location l ON l.id = i.location LEFT JOIN product p ON p.id = i.product" +
                " WHERE l.id = ? ORDER BY p.pType;";

        while (rs.next()) {
            int colCount = 1;

            Element locationEl = toAppend.addElement("location");
            locationEl.addElement("locationId").addText(String.valueOf(rs.getInt(colCount++)));
            locationEl.addElement("locationName").addText(HandlerUtils.nullToEmpty(rs.getString(colCount++)));
            locationEl.addElement("addressStreet").addText(HandlerUtils.nullToEmpty(rs.getString(colCount++)));
            locationEl.addElement("addressCity").addText(HandlerUtils.nullToEmpty(rs.getString(colCount++)));
            locationEl.addElement("addressZip").addText(HandlerUtils.nullToEmpty(rs.getString(colCount++)));
            locationEl.addElement("addressState").addText(HandlerUtils.nullToEmpty(rs.getString(colCount++)));
            locationEl.addElement("customerId").addText(String.valueOf(rs.getInt(colCount++)));
            locationEl.addElement("customerName").addText(HandlerUtils.nullToEmpty(rs.getString(colCount++)));
            locationEl.addElement("brixMin").addText(HandlerUtils.nullToEmpty(rs.getString(colCount++)));
            locationEl.addElement("brixMax").addText(HandlerUtils.nullToEmpty(rs.getString(colCount++)));
            locationEl.addElement("lastPoured").addText(HandlerUtils.nullToEmpty(rs.getString(colCount++)));
            locationEl.addElement("lastSold").addText(HandlerUtils.nullToEmpty(rs.getString(colCount++)));
            locationEl.addElement("pouredAge").addText(HandlerUtils.nullToEmpty(rs.getString(colCount++)));
            locationEl.addElement("soldAge").addText(HandlerUtils.nullToEmpty(rs.getString(colCount++)));
            locationEl.addElement("picoPowerup").addText(HandlerUtils.nullToEmpty(rs.getString(colCount++)));
            locationEl.addElement("picoVersion").addText(HandlerUtils.nullToEmpty(rs.getString(colCount++)));
            locationEl.addElement("easternOffset").addText(HandlerUtils.nullToEmpty(rs.getString(colCount++)));
            locationEl.addElement("latitude").addText(HandlerUtils.nullToEmpty(rs.getString(colCount++)));
            locationEl.addElement("longitude").addText(HandlerUtils.nullToEmpty(rs.getString(colCount++)));
            locationEl.addElement("zoomLevel").addText(HandlerUtils.nullToEmpty(rs.getString(colCount++)));
            //NischaySharma_11-Feb-2009_Start: Added new element to the response xml "varianceAlert"
            locationEl.addElement("varianceAlert").addText(HandlerUtils.nullToEmpty(rs.getString(colCount++)));
            //NischaySharma_11-Feb-2009_End
            locationEl.addElement("draftLines").addText(HandlerUtils.nullToEmpty(rs.getString(colCount++)));
            locationEl.addElement("concept").addText(HandlerUtils.nullToEmpty(rs.getString(colCount++)));
            locationEl.addElement("volImpact").addText(HandlerUtils.nullToEmpty(rs.getString(colCount++)));

            //NischaySharma_26-Feb-2010_Start: Added new element to the response xml "locationType"
            //and "volAdjustment"
            locationEl.addElement("locationType").addText(String.valueOf(rs.getInt(colCount++)));
            locationEl.addElement("volAdjustment").addText(String.valueOf(rs.getInt(colCount++)));
            //NischaySharma_26-Feb-2010_End

            stmt = conn.prepareStatement(selectProductType);
            stmt.setInt(1, rs.getInt(1));
            rs1 = stmt.executeQuery();
            Element pType = locationEl.addElement("pType");
            while (rs1.next()) {
                pType.addElement("type").addText(HandlerUtils.nullToEmpty(rs1.getString(1)));
            }

        }

    }

    private void getLocationDataDump(Element toHandle, Element toAppend, SecureSession ss) throws HandlerException {

        String exportKey                    = HandlerUtils.getOptionalString(toHandle, "exportKey");
        String startDate                    = HandlerUtils.getOptionalString(toHandle, "startDate");
        String endDate                      = HandlerUtils.getOptionalString(toHandle, "endDate");
        String lastExportDate               = HandlerUtils.getOptionalString(toHandle, "lastExportDate");
        int locationId                      = HandlerUtils.getRequiredInteger(toHandle, "locationId");
        PreparedStatement stmt              = null;
        ResultSet rs                        = null, locationRS = null;
        boolean invalidExportKey            = true;
        
        try {

            String checkExportKey           = "SELECT customer, location FROM exportKeyMap WHERE exportKey = ?";
            stmt                            = conn.prepareStatement(checkExportKey);
            stmt.setString(1, exportKey);
            rs                              = stmt.executeQuery();
            while (rs.next()) {
                if (rs.getInt(1) > 0) {
                    stmt                    = conn.prepareStatement("SELECT id FROM location WHERE customer = ? AND id = ?");
                    stmt.setInt(1, rs.getInt(1));
                    stmt.setInt(2, locationId);
                    locationRS              = stmt.executeQuery();
                    if (locationRS.next()) {
                        invalidExportKey    = false;
                        rs.last();
                    }
                } else {
                    if (rs.getInt(1) == locationId) { 
                        invalidExportKey    = false;
                        rs.last();
                    }
                }
            }
            if (invalidExportKey) {
                    throw new HandlerException("Invalid Export Key " + exportKey + " for location: " + locationId);
            }

            String dateCondition            = "";

            if (startDate != null && endDate != null) {
                dateCondition               = "'" + startDate + "' AND '" + endDate + "'";
            } else if (lastExportDate != null) {
                dateCondition               = "'" + lastExportDate + "' AND ADDDATE('" + lastExportDate + "', INTERVAL 10 DAY)";
            } else {
                throw new HandlerException("Date Parameter was not provided");
            }

            String selectCustomerData       = "SELECT IFNULL(p.Date, s.Date) Date, l.LID LID, SUBSTRING(l.Loc,1,30) Location, " +
                                            " IFNULL(p.Product, s.Product) Product, IFNULL(p.ProductID, s.ProductID) ProductID, " +
                                            " IF(IFNULL(p.Category, s.Category) = 1, 'Domestic', 'Better Beer') Type, " +
                                            " IF(IFNULL(p.Category, s.Category) = 1, 'Domestic', IF(IFNULL(p.Category, s.Category) = 2, 'Craft', " +
                                            " IF(IFNULL(p.Category, s.Category) = 3, 'Import', 'House'))) Category, IFNULL(p.ABV, s.ABV) ABV, " +
                                            " ROUND(IFNULL(tPoured,0),2) Poured, ROUND(IFNULL(tSold,0),2) Sold, " +
                                            " IFNULL(ROUND(((tSold - tPoured)/tPoured)*100,2),0) Var " +
                                            " FROM (SELECT l.id LID, l.name Loc FROM location l LEFT JOIN locationDetails lD ON lD.location = l.id " +
                                            " WHERE lD.active = 1 AND lD.suspended = 0 AND l.type = 1 AND (lD.soldUp = 1 OR lD.pouredUp = 1) AND l.id = ?) AS l " +
                                            " LEFT JOIN " +
                                            " (SELECT l.id LID, p.id ProductID, p.name Product, pD.category Category, IFNULL(pD.abv, 0) ABV, oHS.date Date, " +
                                            " SUM(oHS.value) tPoured FROM openHoursSummary oHS " +
                                            " LEFT JOIN location l ON l.id = oHS.location LEFT JOIN product p ON p.id = oHS.product " +
                                            " LEFT JOIN productDescription pD ON pD.product = p.id " +
                                            " WHERE oHS.date BETWEEN " + dateCondition + " AND l.id = ? " +
                                            " GROUP BY oHS.location, oHS.date, oHS.product) AS p ON p.LID = l.LID " +
                                            " LEFT JOIN " +
                                            " (SELECT l.id LID, p.id ProductID, p.name Product, pD.category Category, IFNULL(pD.abv, 0) ABV, oHSS.date Date, " +
                                            " SUM(oHSS.value) tSold FROM openHoursSoldSummary oHSS " +
                                            " LEFT JOIN location l ON l.id = oHSS.location LEFT JOIN product p ON p.id = oHSS.product " +
                                            " LEFT JOIN productDescription pD ON pD.product = p.id " +
                                            " WHERE oHSS.date BETWEEN " + dateCondition + " AND l.id = ? " +
                                            " GROUP BY oHSS.location, oHSS.date, oHSS.product) AS s " +
                                            " ON s.LID = l.LID AND s.ProductID = p.ProductID AND s.Date = p.Date " +
                                            " WHERE IFNULL(p.Product, s.Product) IS NOT NULL " +
                                            " ORDER BY IFNULL(p.Date, s.Date), l.Loc, p.category, p.Product;";

            String selectProductStyle       = "SELECT pSM.product, pS.name FROM productSet pS LEFT JOIN productSetMap pSM ON pSM.productSet = pS.id " +
                                            " WHERE pS.productSetType = 9 ORDER BY pS.name, pSM.product;";

            HashMap<Integer, String> styleMap
                                            = new HashMap<Integer, String>();
            stmt                            = conn.prepareStatement(selectProductStyle);
            rs                              = stmt.executeQuery();
            while (rs.next()) {
                styleMap.put(rs.getInt(1), rs.getString(2));
            }

            stmt                            = conn.prepareStatement(selectCustomerData);
            stmt.setInt(1, locationId);
            stmt.setInt(2, locationId);
            stmt.setInt(3, locationId);
            rs                              = stmt.executeQuery();
            while (rs.next()) {
                int count                   = 1;
                Element dataEl              = toAppend.addElement("data");
                dataEl.addElement("Date").addText(rs.getString(count++));
                dataEl.addElement("LID").addText(rs.getString(count++));
                dataEl.addElement("Location").addText(rs.getString(count++));
                dataEl.addElement("Product").addText(rs.getString(count++));
                int productId               = rs.getInt(count++);
                dataEl.addElement("ProductID").addText(String.valueOf(productId));
                dataEl.addElement("Type").addText(rs.getString(count++));
                dataEl.addElement("Category").addText(rs.getString(count++));
                dataEl.addElement("Style").addText(styleMap.get(productId));
                dataEl.addElement("ABV").addText(rs.getString(count++));
                dataEl.addElement("Poured").addText(rs.getString(count++));
                dataEl.addElement("Sold").addText(rs.getString(count++));
                dataEl.addElement("Var").addText(rs.getString(count++));
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        }  catch (Exception e) {
            logger.dbError("Database error: " + e.getMessage());
            throw new HandlerException(e);
        } finally {
            logger.dbAction("Executed 1 query.");
            close(rs);
            close(stmt);
        }
    }

    private void getSkuIds(Element toHandle, Element toAppend, SecureSession ss) throws HandlerException {
        int location = HandlerUtils.getRequiredInteger(toHandle, "locationId");
        if (location != ss.getLocation()) {
            logger.readingAccessViolation("Called + " + function + " as L#" + location +
                    ", but authenticated as L#" + ss.getLocation());
            throw new HandlerException("Access Violation");
        }

        String sql = "SELECT plu FROM inventory WHERE location = ?";

        PreparedStatement stmt = null;
        ResultSet rs = null;

        try {
            stmt = conn.prepareStatement(sql);
            stmt.setInt(1, location);
            rs = stmt.executeQuery();
            while (rs.next()) {
                String id = rs.getString(1);
                if (null != id) {
                    toAppend.addElement("id").addText(id);
                }
            }

        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            logger.dbAction("Executed 1 query.");
            close(rs);
            close(stmt);
        }

    }

    private boolean storeStandardCheck(Element toHandle, int location)
            throws HandlerException {

        int checkId = HandlerUtils.getRequiredInteger(toHandle, "checkId");
        long epoch = HandlerUtils.getRequiredLong(toHandle, "epoch");
        boolean storeAll = HandlerUtils.getOptionalBoolean(toHandle, "storeAll");

        boolean success = false;
        int stmtIndex = 1;

        List itemList = toHandle.elements("item");
        if (null == itemList || itemList.size() == 0) {
            return true;
        }
        double qty;
        String id;

        PreparedStatement stmt = null;
        ResultSet rs = null;
        int queryCount = 0;

        String sql = null;
        boolean oldAutoCommit = true;
        boolean changedAutoCommit = false;

        Hashtable<String, Boolean> idHash = new Hashtable<String, Boolean>();

        try {
            Calendar calEpoch = Calendar.getInstance();
            calEpoch.setTimeInMillis(epoch * 1000L);
            Calendar calNow = Calendar.getInstance();
            calNow.add(Calendar.DAY_OF_MONTH, 1);
            if (calEpoch.after(calNow)) {
                calEpoch.add(Calendar.MONTH, -1);
            }
            Timestamp tstamp = new Timestamp(calEpoch.getTimeInMillis());
            oldAutoCommit = conn.getAutoCommit();

            if (!storeAll) {
                sql = "SELECT plu FROM beverage where location = ?";
                stmt = conn.prepareStatement(sql);
                stmt.setInt(1, location);
                rs = stmt.executeQuery();
                queryCount++;
                while (null != rs && rs.next()) {
                    idHash.put(rs.getString(1), Boolean.TRUE);
                }
                close(rs);
            }

            // Insert the current values for a check
            sql = "INSERT INTO sales (location, checkId, pluNumber, quantity, date)" +
                    " values (?,?,?,?,?)";
            changedAutoCommit = true;
            conn.setAutoCommit(false);

            String qtyStr;
            boolean store = false;
            for (Object o : itemList) {
                Element item = (Element) o;
                id = HandlerUtils.getRequiredString(item, "id");
                if (storeAll || idHash.containsKey(id)) {
                    qtyStr = HandlerUtils.getRequiredString(item, "qty");
                    qty = Double.parseDouble(qtyStr);
                    //logger.debug("Inserting: (" + location + ", " + checkId + ", " + id + ", " + qty + ", " + tstamp + ")");
                    stmtIndex = 1;
                    stmt = conn.prepareStatement(sql);
                    stmt.setInt(stmtIndex++, location);
                    stmt.setInt(stmtIndex++, checkId);
                    stmt.setString(stmtIndex++, id);
                    stmt.setDouble(stmtIndex++, qty);
                    stmt.setTimestamp(stmtIndex++, tstamp);
                    stmt.executeUpdate();
                    queryCount++;
                }
            }

            // record that a sales reading happened in the location table
            sql = " UPDATE location SET lastSold=GREATEST(lastSold,?) WHERE id=?";
            stmt = conn.prepareStatement(sql);
            stmt.setTimestamp(1, tstamp);
            stmt.setInt(2, location);
            stmt.executeUpdate();
            queryCount++;

            conn.commit();
            logger.dbAction("Committed " + queryCount + " queries");
            success = true;

        } catch (SQLException sqle) {
            if (null != conn) {
                try {
                    logger.dbAction("storeStandardCheck: database rollback");
                    conn.rollback();
                } catch (SQLException ignore) {
                }
            }
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            if (null != conn && changedAutoCommit) {
                try {
                    conn.setAutoCommit(oldAutoCommit);
                } catch (SQLException ignore) {
                }
            }

            close(rs);
            close(stmt);
        }
        return success;
    }

    private boolean storeGlobalPurchaseData(Element toHandle, Element toAppend, SecureSession ss) throws HandlerException {

        int locationId = HandlerUtils.getRequiredInteger(toHandle, "locationId");

        boolean success = false;
        int queryCount = 0, invoiceIndex = 0;

        HashMap<Integer, ArrayList> invoiceData = new HashMap<Integer, ArrayList>();
        HashMap<String, ArrayList> SupplierData = new HashMap<String, ArrayList>();
        HashMap<String, HashMap> SupplierProductMap = new HashMap<String, HashMap>();
        HashMap<String, Integer> ProductPLUMap = new HashMap<String, Integer>();

        List itemPurchaseList = toHandle.elements("purchase");
        if (null == itemPurchaseList || itemPurchaseList.size() == 0) {
            return true;
        }

        PreparedStatement stmt = null;
        ResultSet rs = null;

        String selectSupplierLocation = "SELECT ls.account, s.id, s.name, s.productSet, l.id, c.name, l.name FROM supplier s LEFT JOIN supplierAddress sa ON sa.supplier = s.id " +
                " LEFT JOIN locationSupplier ls ON ls.address = sa.id LEFT JOIN location l ON l.id = ls.location LEFT JOIN customer c ON c.id = l.customer" +
                " WHERE ls.account IS NOT NULL ORDER BY c.name, l.name, s.name ";

        String selectSupplierProducts = "SELECT pSM.product, pSM.plu FROM productSetMap pSM WHERE pSM.productSet = ? ";

        String selectPurchase = "SELECT id FROM purchase WHERE invoice = ?";

        String selectInvProduct = "SELECT id, product, qtyOnHand FROM inventory WHERE location = ? AND supplier = ? AND plu = ?";

        String selectLineProduct = "SELECT l.id FROM line l LEFT JOIN bar b ON b.id = l.bar WHERE l.status = 'RUNNING' AND b.location = ? AND l.product = ? ";

        String selectPurchaseDetail = "SELECT id FROM purchaseDetail WHERE purchase = ? AND product = ? AND quantity = ?";

        String insertPurchase = "INSERT INTO purchase (location, supplier, invoice, date, status, receivedDate) VALUES " +
                " (?,?,?,?,?,?)";

        String insertPurchaseDetail = "INSERT INTO purchaseDetail (purchase, product, quantity, productPlu, productName, productSize, productUPC) VALUES " +
                " (?,?,?,?,?,?,?)";

        String updateInventory = "UPDATE inventory SET qtyOnHand=qtyOnHand+?, isActive = 1 WHERE id=?";

        boolean oldAutoCommit = true;
        boolean changedAutoCommit = false;

        try {
            stmt = conn.prepareStatement(selectSupplierLocation);
            rs = stmt.executeQuery();
            while (rs.next()) {
                ArrayList supplierArray = new ArrayList();
                supplierArray.add(rs.getString(2));
                supplierArray.add(rs.getString(3));
                supplierArray.add(rs.getString(4));
                supplierArray.add(rs.getString(5));
                supplierArray.add(rs.getString(6));
                supplierArray.add(rs.getString(7));
                SupplierData.put(rs.getString(1), supplierArray);
            }
            oldAutoCommit = conn.getAutoCommit();
            for (Object o : itemPurchaseList) {
                Element item = (Element) o;
                String invoice = HandlerUtils.getRequiredString(item, "invoice");
                String account = HandlerUtils.getRequiredString(item, "account");
                String productPLU = HandlerUtils.getRequiredString(item, "productId");
                String productName = HandlerUtils.getRequiredString(item, "productName");
                String productSize = HandlerUtils.getRequiredString(item, "productSize");
                String productUPC = HandlerUtils.getRequiredString(item, "productUPC");
                String date = HandlerUtils.getRequiredString(item, "date");
                String qtyString = HandlerUtils.getRequiredString(item, "quantity");
                Double qty = Double.parseDouble(qtyString);

                int purchase = 0, slocation = 0, supplier = 0, supplierProductSet = 0, inventory = 0, product = 0;
                double qtyOnHand = 0;

                changedAutoCommit = true;
                conn.setAutoCommit(false);

                if (SupplierData.containsKey(account)) {
                    invoiceIndex++;
                    ArrayList invoiceArray = new ArrayList();
                    ArrayList<String> supplierArray = SupplierData.get(account);

                    supplier = Integer.valueOf(supplierArray.get(0));
                    supplierProductSet = Integer.valueOf(supplierArray.get(2));
                    slocation = Integer.valueOf(supplierArray.get(3));

                    if (SupplierProductMap.containsKey(account)) {
                        ProductPLUMap = SupplierProductMap.get(account);
                    } else {
                        stmt = conn.prepareStatement(selectSupplierProducts);
                        stmt.setInt(1, supplierProductSet);
                        rs = stmt.executeQuery();
                        while (rs.next()) {
                            ProductPLUMap.put(rs.getString(2), new Integer(rs.getInt(1)));
                        }
                        SupplierProductMap.put(account, ProductPLUMap);
                    }

                    stmt = conn.prepareStatement(selectPurchase);
                    stmt.setString(1, invoice);
                    rs = stmt.executeQuery();
                    if (rs.next()) {
                        purchase = rs.getInt(1);
                    } else {
                        stmt = conn.prepareStatement(insertPurchase);
                        stmt.setInt(1, slocation);
                        stmt.setInt(2, supplier);
                        stmt.setString(3, invoice);
                        stmt.setString(4, date);
                        stmt.setString(5, "RECEIVED");
                        stmt.setString(6, date);
                        stmt.executeUpdate();
                        queryCount++;

                        stmt = conn.prepareStatement("SELECT LAST_INSERT_ID()");
                        rs = stmt.executeQuery();
                        if (rs.next()) {
                            purchase = rs.getInt(1);
                        }
                    }

                    if (ProductPLUMap.containsKey(productPLU) && (purchase > 0)) {
                        invoiceArray.add(supplierArray.get(1));
                        invoiceArray.add(supplierArray.get(4));
                        invoiceArray.add(supplierArray.get(5));
                        invoiceArray.add(invoice);
                        invoiceArray.add(productName);
                        invoiceArray.add(qtyString);

                        stmt = conn.prepareStatement(selectInvProduct);
                        stmt.setInt(1, slocation);
                        stmt.setInt(2, supplier);
                        stmt.setString(3, productPLU);
                        rs = stmt.executeQuery();
                        if (rs.next()) {
                            inventory = rs.getInt(1);
                            product = rs.getInt(2);
                            qtyOnHand = rs.getDouble(3);

                            stmt = conn.prepareStatement(selectPurchaseDetail);
                            stmt.setInt(1, purchase);
                            stmt.setInt(2, product);
                            stmt.setDouble(3, qty);
                            rs = stmt.executeQuery();
                            if (!rs.next()) {
                                stmt = conn.prepareStatement(insertPurchaseDetail);
                                stmt.setInt(1, purchase);
                                stmt.setInt(2, product);
                                stmt.setDouble(3, qty);
                                stmt.setString(4, productPLU);
                                stmt.setString(5, productName);
                                stmt.setString(6, productSize);
                                stmt.setString(7, productUPC);
                                stmt.executeUpdate();
                                queryCount++;

                                stmt = conn.prepareStatement(updateInventory);
                                stmt.setDouble(1, qty);
                                stmt.setInt(2, inventory);
                                stmt.executeUpdate();
                                queryCount++;

                                stmt = conn.prepareStatement(selectLineProduct);
                                stmt.setInt(1, slocation);
                                stmt.setInt(2, product);
                                rs = stmt.executeQuery();
                                if (rs.next()) {
                                    invoiceArray.add("---------");
                                } else {
                                    invoiceArray.add("Product Lines NA");
                                }
                            }
                        } else {
                            invoiceArray.add("Product Inv NA");
                        }
                    } else {
                        //invoiceArray.add("Product SKU NA");
                    }
                    if (invoiceArray.size() == 7) {
                        invoiceData.put(invoiceIndex, invoiceArray);
                    }
                }
            }
            close(rs);
            conn.commit();
            logger.dbAction("Committed " + queryCount + " queries");
            success = true;
            if (invoiceData.size() > 0) {
                createPurchaseEmail(invoiceData);
            }
        } catch (SQLException sqle) {
            if (null != conn) {
                try {
                    logger.dbAction("storeGlobalPurchaseData: database rollback");
                    conn.rollback();
                } catch (SQLException ignore) {
                }
            }
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            if (null != conn && changedAutoCommit) {
                try {
                    conn.setAutoCommit(oldAutoCommit);
                } catch (SQLException ignore) {
                }
            }

            close(rs);
            close(stmt);
        }
        return success;
    }

    private void storeRealTimeChecksWrapper(Element toHandle, Element toAppend, SecureSession ss) throws HandlerException {
        int location = HandlerUtils.getRequiredInteger(toHandle, "locationId");

        logger.readingAction(function + "#" + ss.getLocation() + "-" + ss.getClientId());
        if (61 != location && location != ss.getLocation()) {
            logger.readingAccessViolation("Called " + function + " as L#" + location +
                    ", but authenticated as L#" + ss.getLocation());
            //throw new HandlerException("Access Violation");
        }

        boolean success = storeRealTimeMultiBarChecks(toHandle, location);
        toAppend.addElement("success").addText(success ? "true" : "false");
    }

    private boolean storeRealTimeMultiBarChecks(Element toHandle, int location)
            throws HandlerException {

        long startOfChecksEpoch = HandlerUtils.getOptionalLong(toHandle,
                "reportStart");
        long endOfChecksEpoch = HandlerUtils.getOptionalLong(toHandle,
                "reportEnd");
        boolean storeAll = HandlerUtils.getOptionalBoolean(toHandle, "storeAll");
        List allChecks = toHandle.elements("item");
        if (null == allChecks || allChecks.size() == 0) {
            return true;
        }

        boolean success = false;
        int stmtIndex = 1;

        PreparedStatement stmt = null;
        ResultSet rs = null;
        int queryCount = 0;

        String sql = null;
        boolean oldAutoCommit = true;
        boolean changedAutoCommit = false;

        Calendar calEpoch = Calendar.getInstance();
        Timestamp tsLast = new Timestamp(calEpoch.getTimeInMillis());
        Hashtable<String, Boolean> idHash = new Hashtable<String, Boolean>();
        Hashtable<String, Integer> productHash = new Hashtable<String, Integer>();
        Hashtable<String, String> productPLUHash
                                            = new Hashtable<String, String>();
        // A cache of product data delay sets (maps product -> datadelay hour)
        HashMap<Integer, ArrayList> dataDelayCache = new HashMap<Integer, ArrayList>();

        ArrayList<Integer> costCenterArray = new ArrayList();
        ArrayList<Integer> missingCostCenterArray = new ArrayList();
        int locationType = 1, zoneId = 0, barId = 0, stationId = 0;

        try {
            java.sql.Date startOfChecks = null, endOfChecks = null;
            if (startOfChecksEpoch > 0) {
                startOfChecksEpoch *= 1000L;
                startOfChecks = new java.sql.Date(startOfChecksEpoch);
            }
            if (endOfChecksEpoch > 0) {
                endOfChecksEpoch *= 1000L;
                endOfChecks = new java.sql.Date(endOfChecksEpoch);
            }

            String checkId;
            int costCenter;
            double qty, offset = 0.00;
            String id;
            long epoch;

            oldAutoCommit = conn.getAutoCommit();
            changedAutoCommit = true;
            conn.setAutoCommit(false);

            // check to see if the check is ignored or empty
            List itemList = toHandle.elements("item");

            sql = "SELECT l.easternOffset, l.type FROM location l WHERE l.id = ? ";
            stmt = conn.prepareStatement(sql);
            stmt.setInt(1, location);
            rs = stmt.executeQuery();
            if (rs.next()) {
                offset = rs.getDouble(1);
                locationType = rs.getInt(2);
            }

            sql = "SELECT c.ccID FROM costCenter c WHERE c.location = ? ";
            stmt = conn.prepareStatement(sql);
            stmt.setInt(1, location);
            rs = stmt.executeQuery();
            while (rs.next()) {
                costCenterArray.add(rs.getInt(1));
            }
            
            // get all the allowable PLU's
            if (!storeAll) {
                sql = "SELECT b.plu, i.product, GROUP_CONCAT(CONCAT(i.product, '-', i.ounces)) FROM beverage b "
                    + " left join ingredient i on i.beverage = b.id WHERE b.location = ? GROUP BY b.id ORDER BY b.plu ;";
                stmt = conn.prepareStatement(sql);
                stmt.setInt(1, location);
                rs = stmt.executeQuery();
                queryCount++;
                while (null != rs && rs.next()) {
                    idHash.put(rs.getString(1), Boolean.TRUE);
                    productHash.put(rs.getString(1), rs.getInt(2));
                    productPLUHash.put(rs.getString(1), rs.getString(3));
                }
                close(rs);
            }
                    
            // Insert the current values for a check
            sql                             = "INSERT INTO sales (location, pluNumber, costCenter, quantity, date, checkId, reportRecordId)" +
                                            " values (?,?,?,?,?,?,?)";
            String insertSalesOunces        = "INSERT INTO salesOunce (location, product, costCenter, quantity, date, checkId, reportRecordId)" +
                                            " values (?,?,?,?,?,?,?)";
            String qtyStr;
            for (Object o : itemList) {
                Element item = (Element) o;
                id = HandlerUtils.getRequiredString(item, "id");
                checkId = HandlerUtils.getRequiredString(item, "checkId");
                costCenter = HandlerUtils.getRequiredInteger(item, "costCenter");
                if (storeAll || idHash.containsKey(id)) {
                    epoch = HandlerUtils.getRequiredLong(item, "epoch");
                    if (epoch < 0) {
                        continue;
                    }
                    calEpoch.setTimeInMillis(epoch * 1000L);
                    Timestamp tstamp = new Timestamp(calEpoch.getTimeInMillis());
                    qtyStr = HandlerUtils.getRequiredString(item, "qty");
                    qty = Double.parseDouble(qtyStr);
                    String recordId = HandlerUtils.getOptionalString(item, "recordId");

                    String deleteCheckRecords  
                                            = "DELETE FROM sales WHERE location = ? AND checkId = ? AND reportRecordId = ? AND " +
                                            " date BETWEEN ? AND ?";
                    String deleteCheckOunces= "DELETE FROM salesOunce WHERE location = ? AND checkId = ? AND reportRecordId = ? AND " +
                                            " date BETWEEN ? AND ?";
                    stmt                    = conn.prepareStatement(deleteCheckRecords);
                    stmt.setInt(1, location);
                    stmt.setString(2, checkId);
                    stmt.setString(3, recordId);
                    stmt.setTimestamp(4, toSqlTimestamp(startOfChecks));
                    stmt.setTimestamp(5, toSqlTimestamp(endOfChecks));
                    stmt.executeUpdate();

                    stmt                    = conn.prepareStatement(deleteCheckOunces);
                    stmt.setInt(1, location);
                    stmt.setString(2, checkId);
                    stmt.setString(3, recordId);
                    stmt.setTimestamp(4, toSqlTimestamp(startOfChecks));
                    stmt.setTimestamp(5, toSqlTimestamp(endOfChecks));
                    stmt.executeUpdate();

                    //logger.debug("Inserting: (" + location + ", " + (null == checkId ? "NULL" : checkId) + ", " + costCenter + ", " + id + ", " + qty + ", " + tstamp + ")");
                    stmtIndex = 1;
                    stmt = conn.prepareStatement(sql);
                    stmt.setInt(stmtIndex++, location);
                    stmt.setString(stmtIndex++, id);
                    stmt.setInt(stmtIndex++, costCenter);
                    stmt.setDouble(stmtIndex++, qty);
                    stmt.setTimestamp(stmtIndex++, tstamp);
                    if (null == checkId) {
                        stmt.setNull(stmtIndex++, java.sql.Types.VARCHAR);
                    } else {
                        stmt.setString(stmtIndex++, checkId);
                    }
                    if (null == recordId) {
                        stmt.setNull(stmtIndex++, java.sql.Types.VARCHAR);
                    } else {
                        stmt.setString(stmtIndex++, recordId);
                    }
                    stmt.executeUpdate();
                    queryCount++;
                    
                    if (productPLUHash.containsKey(id)) {
                        String productOuncesString  
                                            = productPLUHash.get(id);
                        for (String productOunces : productOuncesString.split(",")) {
                            String[] parts  = productOunces.split("-");
                            int product     = Integer.valueOf(parts[0]);
                            double ounces   = Double.parseDouble(parts[1]);
                            stmtIndex       = 1;
                            stmt            = conn.prepareStatement(insertSalesOunces);
                            stmt.setInt(stmtIndex++, location);
                            stmt.setInt(stmtIndex++, product);
                            stmt.setInt(stmtIndex++, costCenter);
                            stmt.setDouble(stmtIndex++, qty * ounces);
                            stmt.setTimestamp(stmtIndex++, tstamp);
                            stmt.setString(stmtIndex++, checkId);
                            stmt.setString(stmtIndex++, recordId);
                            stmt.executeUpdate();
                            queryCount++;
                        }
                    }
                    
                    if (productHash.size() > 0 && productHash.containsKey(id)) {
                        dataDelayCache = checkDataDelay(productHash.get(id), offset, tstamp, dataDelayCache);
                    }
                    //Adding all the missing cost centers to be added to the cost center table
                    if (!costCenterArray.contains(costCenter)){
                        missingCostCenterArray.add(costCenter);
                        costCenterArray.add(costCenter);
                    }
                }
            }

            // selecting the first bar that was created for the retail location
            if (locationType == 1) {
                    storeRetailerMissingCostCenter(missingCostCenterArray, location);
            }

            // record that a sales reading happened in the location table
            sql = " UPDATE location SET lastSold=GREATEST(lastSold,?) WHERE id=?";
            stmt = conn.prepareStatement(sql);
            stmt.setTimestamp(1, tsLast);
            stmt.setInt(2, location);
            stmt.executeUpdate();
            queryCount++;

            conn.commit();
            logger.dbAction("Committed " + queryCount + " queries");

            if (dataDelayCache.size() > 0) {
                logger.debug("Inserting Data Delays");
                setDataDelay(location, offset, dataDelayCache);
            }

            success = true;

        } catch (SQLException sqle) {
            if (null != conn) {
                try {
                    logger.dbAction("storeChecks: database rollback");
                    conn.rollback();
                } catch (SQLException ignore) {
                }
            }
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            if (null != conn && changedAutoCommit) {
                try {
                    conn.setAutoCommit(oldAutoCommit);
                } catch (SQLException ignore) {
                }
            }

            close(rs);
            close(stmt);
        }
        return success;
    }

    private void storeMicros9700ChecksWrapper(Element toHandle, Element toAppend, SecureSession ss) throws HandlerException {
        int location = HandlerUtils.getRequiredInteger(toHandle, "locationId");

        logger.readingAction(function + "#" + ss.getLocation() + "-" + ss.getClientId());
        if (61 != location && location != ss.getLocation()) {
            logger.readingAccessViolation("Called " + function + " as L#" + location +
                    ", but authenticated as L#" + ss.getLocation());
            throw new HandlerException("Access Violation");
        }

        boolean success = storeMultiBarChecks(toHandle, location);
        toAppend.addElement("success").addText(success ? "true" : "false");
    }

    private boolean storeMultiBarChecks(Element toHandle, int location)
            throws HandlerException {

        long startOfChecksEpoch = HandlerUtils.getOptionalLong(toHandle,
                "reportStart");
        long endOfChecksEpoch = HandlerUtils.getOptionalLong(toHandle,
                "reportEnd");
        boolean storeAll = HandlerUtils.getOptionalBoolean(toHandle, "storeAll");
        List allChecks = toHandle.elements("item");
        if (null == allChecks || allChecks.size() == 0) {
            return true;
        }

        boolean success = false;
        int stmtIndex = 1;

        PreparedStatement stmt = null;
        ResultSet rs = null;
        int queryCount = 0;

        String sql = null;
        boolean oldAutoCommit = true;
        boolean changedAutoCommit = false;

        Calendar calEpoch = Calendar.getInstance();
        Timestamp tsLast = new Timestamp(calEpoch.getTimeInMillis());
        Hashtable<String, Boolean> idHash = new Hashtable<String, Boolean>();
        Hashtable<String, Integer> productHash = new Hashtable<String, Integer>();
        // A cache of product data delay sets (maps product -> datadelay hour)
        HashMap<Integer, ArrayList> dataDelayCache = new HashMap<Integer, ArrayList>();

        ArrayList<Integer> costCenterArray = new ArrayList();
        ArrayList<Integer> missingCostCenterArray = new ArrayList();
        int locationType = 1, zoneId = 0, barId = 0, stationId = 0;

        try {
            java.sql.Date startOfChecks = null, endOfChecks = null;
            if (startOfChecksEpoch > 0) {
                startOfChecksEpoch *= 1000L;
                startOfChecks = new java.sql.Date(startOfChecksEpoch);
            }
            if (endOfChecksEpoch > 0) {
                endOfChecksEpoch *= 1000L;
                endOfChecks = new java.sql.Date(endOfChecksEpoch);
            }

            String checkId;
            int costCenter;
            double qty, offset = 0.00;
            String id;
            long epoch;

            oldAutoCommit = conn.getAutoCommit();
            changedAutoCommit = true;
            conn.setAutoCommit(false);

            // check to see if the check is ignored or empty
            List itemList = toHandle.elements("item");

            sql = "SELECT l.easternOffset, l.type FROM location l WHERE l.id = ? ";
            stmt = conn.prepareStatement(sql);
            stmt.setInt(1, location);
            rs = stmt.executeQuery();
            if (rs.next()) {
                offset = rs.getDouble(1);
                locationType = rs.getInt(2);
            }

            sql = "SELECT c.ccID FROM costCenter c WHERE c.location = ? ";
            stmt = conn.prepareStatement(sql);
            stmt.setInt(1, location);
            rs = stmt.executeQuery();
            while (rs.next()) {
                costCenterArray.add(rs.getInt(1));
            }

            // get all the allowable PLU's
            if (!storeAll) {
                sql = "SELECT b.plu, i.product FROM beverage b LEFT JOIN ingredient i ON i.beverage = b.id WHERE b.location = ?;";
                stmt = conn.prepareStatement(sql);
                stmt.setInt(1, location);
                rs = stmt.executeQuery();
                queryCount++;
                while (null != rs && rs.next()) {
                    idHash.put(rs.getString(1), Boolean.TRUE);
                    productHash.put(rs.getString(1), rs.getInt(2));
                }
                close(rs);
            }

            // Insert the current values for a check
            String sqlInsert = "INSERT INTO sales (location, pluNumber, costCenter, quantity, date, checkId, reportRecordId)" +
                    " values (?,?,?,?,?,?,?)";
            String qtyStr;
            for (Object o : itemList) {
                Element item = (Element) o;
                id = HandlerUtils.getRequiredString(item, "id");
                checkId = HandlerUtils.getRequiredString(item, "checkId");
                costCenter = HandlerUtils.getRequiredInteger(item, "costCenter");
                if (storeAll || idHash.containsKey(id)) {
                    epoch = HandlerUtils.getRequiredLong(item, "epoch");
                    calEpoch.setTimeInMillis(epoch * 1000L);
                    Timestamp tstamp = new Timestamp(calEpoch.getTimeInMillis());
                    qtyStr = HandlerUtils.getRequiredString(item, "qty");
                    qty = Double.parseDouble(qtyStr);
                    String recordId = HandlerUtils.getOptionalString(item, "recordId");
                    //logger.debug("Inserting: (" + location + ", " + (null == checkId ? "NULL" : checkId) + ", " + costCenter + ", " + id + ", " + qty + ", " + tstamp + ")");
                    stmtIndex = 1;
                    stmt = conn.prepareStatement(sqlInsert);
                    stmt.setInt(stmtIndex++, location);
                    stmt.setString(stmtIndex++, id);
                    stmt.setInt(stmtIndex++, costCenter);
                    stmt.setDouble(stmtIndex++, qty);
                    stmt.setTimestamp(stmtIndex++, tstamp);
                    if (null == checkId) {
                        stmt.setNull(stmtIndex++, java.sql.Types.VARCHAR);
                    } else {
                        stmt.setString(stmtIndex++, checkId);
                    }
                    if (null == recordId) {
                        stmt.setNull(stmtIndex++, java.sql.Types.VARCHAR);
                    } else {
                        stmt.setString(stmtIndex++, recordId);
                    }
                    if (productHash.size() > 0 && productHash.containsKey(id)) {
                        dataDelayCache = checkDataDelay(productHash.get(id), offset, tstamp, dataDelayCache);
                    }
                    stmt.executeUpdate();
                    queryCount++;
                    
                    //Adding all the missing cost centers to be added to the cost center table
                    if (!costCenterArray.contains(costCenter)){
                        missingCostCenterArray.add(costCenter);
                        costCenterArray.add(costCenter);
                    }
                }
            }

            // selecting the first bar that was created for the retail location
            if (locationType == 1) {
                    storeRetailerMissingCostCenter(missingCostCenterArray, location);
            }

            // record that a sales reading happened in the location table
            sql = " UPDATE location SET lastSold=GREATEST(lastSold,?) WHERE id=?";
            stmt = conn.prepareStatement(sql);
            stmt.setTimestamp(1, tsLast);
            stmt.setInt(2, location);
            stmt.executeUpdate();
            queryCount++;

            conn.commit();
            logger.dbAction("Committed " + queryCount + " queries");

            if (dataDelayCache.size() > 0) {
                logger.debug("Inserting Data Delays");
                setDataDelay(location, offset, dataDelayCache);
            }
            
            success = true;

        } catch (SQLException sqle) {
            if (null != conn) {
                try {
                    logger.dbAction("storeChecks: database rollback");
                    conn.rollback();
                } catch (SQLException ignore) {
                }
            }
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            if (null != conn && changedAutoCommit) {
                try {
                    conn.setAutoCommit(oldAutoCommit);
                } catch (SQLException ignore) {
                }
            }

            close(rs);
            close(stmt);
        }
        return success;
    }



    private boolean storeSendOrderChecks(Element toHandle, int location)
            throws HandlerException {

        long startOfChecksEpoch = HandlerUtils.getOptionalLong(toHandle,
                "startOfChecksEpoch");
        boolean storeAll = HandlerUtils.getOptionalBoolean(toHandle, "storeAll");
        List allChecks = toHandle.elements("check");
        if (null == allChecks || allChecks.size() == 0) {
            return true;
        }

        boolean success = false;
        int stmtIndex = 1;

        PreparedStatement stmt = null;
        ResultSet rs = null;
        int queryCount = 0;

        String sql = null;
        boolean oldAutoCommit = true;
        boolean changedAutoCommit = false;

        Calendar calEpoch = Calendar.getInstance();
        Timestamp tsLast = new Timestamp(calEpoch.getTimeInMillis());
        Hashtable<String, Boolean> idHash = new Hashtable<String, Boolean>();
        Hashtable<String, Integer> productHash = new Hashtable<String, Integer>();
        // A cache of product data delay sets (maps product -> datadelay hour)
        HashMap<Integer, ArrayList> dataDelayCache = new HashMap<Integer, ArrayList>();

        try {
            java.sql.Date startOfChecks = null, endOfChecks = null;
            if (startOfChecksEpoch > 0) {
                startOfChecksEpoch *= 1000L;
                startOfChecks = new java.sql.Date(startOfChecksEpoch);
                Calendar endCal = Calendar.getInstance();
                endCal.setTimeInMillis(startOfChecksEpoch);
                endCal.add(Calendar.DAY_OF_MONTH, 1);
                endOfChecks = new java.sql.Date(endCal.getTimeInMillis());
            }
            //logger.debug(startOfChecks.toString());
            String checkId;
            double qty, offset = 0.00;
            String id;
            long epoch;

            oldAutoCommit = conn.getAutoCommit();
            changedAutoCommit = true;
            conn.setAutoCommit(false);

            sql = "SELECT l.easternOffset, l.lastSold FROM location l WHERE l.id = ? ";
            stmt = conn.prepareStatement(sql);
            stmt.setInt(1, location);
            rs = stmt.executeQuery();
            if (rs.next()) {
                offset = rs.getDouble(1);
            }

            for (Object checkObj : allChecks) {
                Element elCheck = (Element) checkObj;
                checkId = HandlerUtils.getOptionalString(elCheck, "checkId");
                //logger.debug("Check Point 1");

                // check to see if this is a revision check
                int revision = HandlerUtils.getOptionalInteger(elCheck, "revision");
                boolean ignoreCheck = false, correctCheck = false;
                if (null != checkId && null != startOfChecks && revision > 0) {
                    //logger.debug("Check Point 2");
                    // see if a more recent version of the check is already stored
                    sql = "SELECT count(sid) FROM sales " +
                            "WHERE location = ? " +
                            " AND checkId = ? " +
                            " AND date BETWEEN ? AND ? " +
                            " GROUP BY checkId " +
                            " ORDER BY sid DESC";
                    stmt = conn.prepareStatement(sql);
                    stmt.setInt(1, location);
                    stmt.setString(2, checkId);
                    stmt.setTimestamp(3, toSqlTimestamp(startOfChecks));
                    stmt.setTimestamp(4, toSqlTimestamp(endOfChecks));
                    rs = stmt.executeQuery();
                    queryCount++;
                    int revCheck = -1;
                    if (null != rs && rs.next()) {
                        revCheck = rs.getInt(1);
                    }
                    close(rs);
                    if (revCheck > -1) {
                        if (revision != revCheck) {
                            logger.debug("Previous/Current check count: " + revCheck + "/" + revision);
                            // delete previous versions
                            sql = "DELETE FROM sales " +
                                    "WHERE location = ? " +
                                    " AND checkId = ? " +
                                    " AND date BETWEEN ? AND ?";
                            stmt = conn.prepareStatement(sql);
                            stmt.setInt(1, location);
                            stmt.setString(2, checkId);
                            stmt.setTimestamp(3, toSqlTimestamp(startOfChecks));
                            stmt.setTimestamp(4, toSqlTimestamp(endOfChecks));
                            stmt.executeUpdate();
                            logger.debug("Revising check number: " + checkId);
                            revision = 1;
                            queryCount++;
                        } else {
                            ignoreCheck = true;
                        }
                    }
                } else {
                    revision = 0;
                }

                // check to see if the check is ignored or empty
                List itemList = elCheck.elements("item");
                if (ignoreCheck || null == itemList || itemList.size() == 0) {
                    //logger.debug("Check Point 4");
                    continue;
                }

                // get all the allowable PLU's
                if (!storeAll) {
                    //logger.debug("Check Point 5");
                    sql = "SELECT b.plu, i.product FROM beverage b LEFT JOIN ingredient i ON i.beverage = b.id WHERE b.location = ?;";
                    stmt = conn.prepareStatement(sql);
                    stmt.setInt(1, location);
                    rs = stmt.executeQuery();
                    queryCount++;
                    while (null != rs && rs.next()) {
                        idHash.put(rs.getString(1), Boolean.TRUE);
                        productHash.put(rs.getString(1), rs.getInt(2));
                    }
                    close(rs);
                }

                // Insert the current values for a check
                sql = "INSERT INTO sales (location, checkId, pluNumber, quantity, date, reportRecordId)" +
                        " values (?,?,?,?,?,?)";
                String qtyStr;
                boolean store = false;
                for (Object o : itemList) {
                    //logger.debug("Check Point 6");
                    Element item = (Element) o;
                    id = HandlerUtils.getRequiredString(item, "id");
                    if (storeAll || idHash.containsKey(id)) {
                        epoch = HandlerUtils.getRequiredLong(item, "epoch");
                        calEpoch.setTimeInMillis(epoch * 1000L);
                        Timestamp tstamp = new Timestamp(calEpoch.getTimeInMillis());
                        qtyStr = HandlerUtils.getRequiredString(item, "qty");
                        qty = Double.parseDouble(qtyStr);
                        //logger.debug("Inserting: (" + location + ", " + (null == checkId ? "NULL" : checkId) + ", " + id + ", " + qty + ", " + tstamp + ", " + revision + ")");
                        stmtIndex = 1;
                        stmt = conn.prepareStatement(sql);
                        stmt.setInt(stmtIndex++, location);
                        if (null == checkId) {
                            stmt.setNull(stmtIndex++, java.sql.Types.VARCHAR);
                        } else {
                            stmt.setString(stmtIndex++, checkId);
                        }
                        stmt.setString(stmtIndex++, id);
                        stmt.setDouble(stmtIndex++, qty);
                        stmt.setTimestamp(stmtIndex++, tstamp);
                        stmt.setLong(stmtIndex++, revision);
                        stmt.executeUpdate();
                        if (productHash.size() > 0 && productHash.containsKey(id)) {
                            dataDelayCache = checkDataDelay(productHash.get(id), offset, tstamp, dataDelayCache);
                        }
                        queryCount++;
                    }
                }

            }

            // record that a sales reading happened in the location table
            sql = " UPDATE location SET lastSold=GREATEST(lastSold,?) WHERE id=?";
            stmt = conn.prepareStatement(sql);
            stmt.setTimestamp(1, tsLast);
            stmt.setInt(2, location);
            stmt.executeUpdate();
            queryCount++;

            conn.commit();
            logger.dbAction("Committed " + queryCount + " queries");
            success = true;

            if (dataDelayCache.size() > 0) {
                logger.debug("Inserting Data Delays");
                setDataDelay(location, offset, dataDelayCache);
            }

        } catch (SQLException sqle) {
            if (null != conn) {
                try {
                    logger.dbAction("storeChecks: database rollback");
                    conn.rollback();
                } catch (SQLException ignore) {
                }
            }
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            if (null != conn && changedAutoCommit) {
                try {
                    conn.setAutoCommit(oldAutoCommit);
                } catch (SQLException ignore) {
                }
            }

            close(rs);
            close(stmt);
        }
        return success;
    }

    private boolean storeChecks(Element toHandle, int location)
            throws HandlerException {

        long startOfChecksEpoch = HandlerUtils.getOptionalLong(toHandle,
                "startOfChecksEpoch");
        boolean storeAll = HandlerUtils.getOptionalBoolean(toHandle, "storeAll");
        List allChecks = toHandle.elements("check");
        if (null == allChecks || allChecks.size() == 0) {
            return true;
        }


        boolean success = false;
        int stmtIndex = 1;

        PreparedStatement stmt = null;
        ResultSet rs = null;
        int queryCount = 0;

        String sql = null;
        boolean oldAutoCommit = true;
        boolean changedAutoCommit = false;

        Calendar calEpoch = Calendar.getInstance();
        Timestamp tsLast = new Timestamp(calEpoch.getTimeInMillis());
        Hashtable<String, Boolean> idHash = new Hashtable<String, Boolean>();
        Hashtable<String, Integer> productHash = new Hashtable<String, Integer>();
        // A cache of product data delay sets (maps product -> datadelay hour)
        HashMap<Integer, ArrayList> dataDelayCache = new HashMap<Integer, ArrayList>();

        try {
            java.sql.Date startOfChecks = null, endOfChecks = null;
            if (startOfChecksEpoch > 0) {
                startOfChecksEpoch *= 1000L;
                startOfChecks = new java.sql.Date(startOfChecksEpoch);
                Calendar endCal = Calendar.getInstance();
                endCal.setTimeInMillis(startOfChecksEpoch);
                endCal.add(Calendar.DAY_OF_MONTH, 1);
                endOfChecks = new java.sql.Date(endCal.getTimeInMillis());
            }

            String checkId;
            double qty, offset = 0.00;
            String id;
            long epoch;

            oldAutoCommit = conn.getAutoCommit();
            changedAutoCommit = true;
            conn.setAutoCommit(false);

            sql = "SELECT l.easternOffset, l.lastSold FROM location l WHERE l.id = ? ";
            stmt = conn.prepareStatement(sql);
            stmt.setInt(1, location);
            rs = stmt.executeQuery();
            if (rs.next()) {
                offset = rs.getDouble(1);
            }

            for (Object checkObj : allChecks) {
                Element elCheck = (Element) checkObj;
                checkId = HandlerUtils.getOptionalString(elCheck, "checkId");

                // check to see if this is a revision check
                long revision = HandlerUtils.getOptionalLong(elCheck, "revision");
                boolean ignoreCheck = false;
                if (null != checkId && null != startOfChecks && revision > 0) {
                    // see if a more recent version of the check is already stored
                    sql = "SELECT reportRecordId FROM sales " +
                            "WHERE location = ? " +
                            " AND checkId = ? " +
                            " AND date BETWEEN ? AND ? " +
                            " ORDER BY reportRecordId DESC";
                    stmt = conn.prepareStatement(sql);
                    stmt.setInt(1, location);
                    stmt.setString(2, checkId);
                    stmt.setTimestamp(3, toSqlTimestamp(startOfChecks));
                    stmt.setTimestamp(4, toSqlTimestamp(endOfChecks));
                    rs = stmt.executeQuery();
                    queryCount++;
                    long revCheck = -1;
                    if (null != rs && rs.next()) {
                        revCheck = rs.getLong("reportRecordId");
                    }
                    close(rs);
                    if (revCheck > -1) {
                        if (revision > revCheck) {
                            // delete previous versions
                            sql = "DELETE FROM sales " +
                                    "WHERE location = ? " +
                                    " AND checkId = ? " +
                                    " AND date BETWEEN ? AND ?";
                            stmt = conn.prepareStatement(sql);
                            stmt.setInt(1, location);
                            stmt.setString(2, checkId);
                            stmt.setTimestamp(3, toSqlTimestamp(startOfChecks));
                            stmt.setTimestamp(4, toSqlTimestamp(endOfChecks));
                            stmt.executeUpdate();
                            queryCount++;
                        } else {
                            ignoreCheck = true;
                        }
                    }
                } else {
                    revision = 0;
                }

                // check to see if the check is ignored or empty
                List itemList = elCheck.elements("item");
                if (ignoreCheck || null == itemList || itemList.size() == 0) {
                    continue;
                }

                // get all the allowable PLU's
                if (!storeAll) {
                    sql = "SELECT b.plu, i.product FROM beverage b LEFT JOIN ingredient i ON i.beverage = b.id WHERE b.location = ?;";
                    stmt = conn.prepareStatement(sql);
                    stmt.setInt(1, location);
                    rs = stmt.executeQuery();
                    queryCount++;
                    while (null != rs && rs.next()) {
                        idHash.put(rs.getString(1), Boolean.TRUE);
                        productHash.put(rs.getString(1), rs.getInt(2));
                    }
                    close(rs);
                }

                // Insert the current values for a check
                sql = "INSERT INTO sales (location, checkId, pluNumber, quantity, date, reportRecordId)" +
                        " values (?,?,?,?,?,?)";
                String qtyStr;
                boolean store = false;
                for (Object o : itemList) {
                    Element item = (Element) o;
                    id = HandlerUtils.getRequiredString(item, "id");
                    if (storeAll || idHash.containsKey(id)) {
                        epoch = HandlerUtils.getRequiredLong(item, "epoch");
                        calEpoch.setTimeInMillis(epoch * 1000L);
                        Timestamp tstamp = new Timestamp(calEpoch.getTimeInMillis());
                        qtyStr = HandlerUtils.getRequiredString(item, "qty");
                        qty = Double.parseDouble(qtyStr);
                        //logger.debug("Inserting: (" + location + ", " + (null == checkId ? "NULL" : checkId) + ", " + id + ", " + qty + ", " + tstamp + ", " + revision + ")");
                        stmtIndex = 1;
                        stmt = conn.prepareStatement(sql);
                        stmt.setInt(stmtIndex++, location);
                        if (null == checkId) {
                            stmt.setNull(stmtIndex++, java.sql.Types.VARCHAR);
                        } else {
                            stmt.setString(stmtIndex++, checkId);
                        }
                        stmt.setString(stmtIndex++, id);
                        stmt.setDouble(stmtIndex++, qty);
                        stmt.setTimestamp(stmtIndex++, tstamp);
                        stmt.setLong(stmtIndex++, revision);
                        stmt.executeUpdate();
                        if (productHash.size() > 0 && productHash.containsKey(id)) {
                            dataDelayCache = checkDataDelay(productHash.get(id), offset, tstamp, dataDelayCache);
                        }
                        queryCount++;
                    }
                }

            }

            // record that a sales reading happened in the location table
            sql = " UPDATE location SET lastSold=GREATEST(lastSold,?) WHERE id=?";
            stmt = conn.prepareStatement(sql);
            stmt.setTimestamp(1, tsLast);
            stmt.setInt(2, location);
            stmt.executeUpdate();
            queryCount++;

            conn.commit();
            logger.dbAction("Committed " + queryCount + " queries");
            success = true;

            if (dataDelayCache.size() > 0) {
                logger.debug("Inserting Data Delays");
                setDataDelay(location, offset, dataDelayCache);
            }

        } catch (SQLException sqle) {
            if (null != conn) {
                try {
                    logger.dbAction("storeChecks: database rollback");
                    conn.rollback();
                } catch (SQLException ignore) {
                }
            }
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            if (null != conn && changedAutoCommit) {
                try {
                    conn.setAutoCommit(oldAutoCommit);
                } catch (SQLException ignore) {
                }
            }

            close(rs);
            close(stmt);
        }
        return success;
    }

    private boolean storePOSiChecks(Element toHandle, int location) throws HandlerException {

        long startOfChecksEpoch = HandlerUtils.getOptionalLong(toHandle,
                "startOfChecksEpoch");
        boolean storeAll = HandlerUtils.getOptionalBoolean(toHandle, "storeAll");
        List allChecks = toHandle.elements("check");
        if (null == allChecks || allChecks.size() == 0) {
            return true;
        }

        logger.debug("Processing POSitouch Checks");
        boolean success = false;
        int stmtIndex = 1;

        PreparedStatement stmt = null;
        ResultSet rs = null;
        int queryCount = 0;

        String sql = null;
        boolean oldAutoCommit = true;
        boolean changedAutoCommit = false;

        Calendar calEpoch = Calendar.getInstance();
        Timestamp tsLast = new Timestamp(calEpoch.getTimeInMillis());
        Hashtable<String, Boolean> idHash = new Hashtable<String, Boolean>();
        Hashtable<String, Integer> productHash = new Hashtable<String, Integer>();
        // A cache of product data delay sets (maps product -> datadelay hour)
        HashMap<Integer, ArrayList> dataDelayCache = new HashMap<Integer, ArrayList>();

        try {
            java.sql.Date startOfChecks = null, endOfChecks = null;
            if (startOfChecksEpoch > 0) {
                startOfChecksEpoch *= 1000L;
                startOfChecks = new java.sql.Date(startOfChecksEpoch);
                Calendar endCal = Calendar.getInstance();
                endCal.setTimeInMillis(startOfChecksEpoch);
                endCal.add(Calendar.DAY_OF_MONTH, 1);
                endOfChecks = new java.sql.Date(endCal.getTimeInMillis());
            }

            String checkId;
            double qty, offset = 0.00;
            String id;
            long epoch;

            oldAutoCommit = conn.getAutoCommit();
            changedAutoCommit = true;
            conn.setAutoCommit(false);

            sql = "SELECT l.easternOffset, l.lastSold FROM location l WHERE l.id = ? ";
            stmt = conn.prepareStatement(sql);
            stmt.setInt(1, location);
            rs = stmt.executeQuery();
            if (rs.next()) {
                offset = rs.getDouble(1);
            }

            for (Object checkObj : allChecks) {
                Element elCheck = (Element) checkObj;
                checkId = HandlerUtils.getOptionalString(elCheck, "checkId");

                // check to see if this is a revision check
                long revision = HandlerUtils.getOptionalLong(elCheck, "revision");
                boolean ignoreCheck = false;
                if (null != checkId && null != startOfChecks) {
                    // see if a more recent version of the check is already stored
                    sql = "SELECT COUNT(sid) FROM sales " +
                            "WHERE location = ? " +
                            " AND checkId = ? " +
                            " AND date BETWEEN ? AND ? " +
                            " GROUP BY checkId";
                    stmt = conn.prepareStatement(sql);
                    stmt.setInt(1, location);
                    stmt.setString(2, checkId);
                    stmt.setTimestamp(3, toSqlTimestamp(startOfChecks));
                    stmt.setTimestamp(4, toSqlTimestamp(endOfChecks));
                    rs = stmt.executeQuery();
                    queryCount++;
                    if (null != rs && rs.next()) {
                        revision = rs.getInt(1);
                        // delete previous versions
                        sql = "DELETE FROM sales " +
                                "WHERE location = ? " +
                                " AND checkId = ? " +
                                " AND date BETWEEN ? AND ?";
                        stmt = conn.prepareStatement(sql);
                        stmt.setInt(1, location);
                        stmt.setString(2, checkId);
                        stmt.setTimestamp(3, toSqlTimestamp(startOfChecks));
                        stmt.setTimestamp(4, toSqlTimestamp(endOfChecks));
                        stmt.executeUpdate();
                        logger.debug("Revising check number: " + checkId);
                        queryCount++;
                    }
                    close(rs);
                } else {
                    revision = 0;
                }

                // check to see if the check is ignored or empty
                List itemList = elCheck.elements("item");
                if (ignoreCheck || null == itemList || itemList.size() == 0) {
                    continue;
                }

                // get all the allowable PLU's
                if (!storeAll) {
                    sql = "SELECT b.plu, i.product FROM beverage b LEFT JOIN ingredient i ON i.beverage = b.id WHERE b.location = ?;";
                    stmt = conn.prepareStatement(sql);
                    stmt.setInt(1, location);
                    rs = stmt.executeQuery();
                    queryCount++;
                    while (null != rs && rs.next()) {
                        idHash.put(rs.getString(1), Boolean.TRUE);
                        productHash.put(rs.getString(1), rs.getInt(2));
                    }
                    close(rs);
                }

                // Insert the current values for a check
                sql = "INSERT INTO sales (location, checkId, pluNumber, quantity, date, reportRecordId)" +
                        " values (?,?,?,?,?,?)";
                String qtyStr;
                boolean store = false;
                for (Object o : itemList) {
                    Element item = (Element) o;
                    id = HandlerUtils.getRequiredString(item, "id");
                    if (storeAll || idHash.containsKey(id)) {
                        epoch = HandlerUtils.getRequiredLong(item, "epoch");
                        calEpoch.setTimeInMillis(epoch * 1000L);
                        Timestamp tstamp = new Timestamp(calEpoch.getTimeInMillis());
                        qtyStr = HandlerUtils.getRequiredString(item, "qty");
                        qty = Double.parseDouble(qtyStr);
                        //logger.debug("Inserting: (" + location + ", " + (null == checkId ? "NULL" : checkId) + ", " + id + ", " + qty + ", " + tstamp + ", " + revision + ")");
                        stmtIndex = 1;
                        stmt = conn.prepareStatement(sql);
                        stmt.setInt(stmtIndex++, location);
                        if (null == checkId) {
                            stmt.setNull(stmtIndex++, java.sql.Types.VARCHAR);
                        } else {
                            stmt.setString(stmtIndex++, checkId);
                        }
                        stmt.setString(stmtIndex++, id);
                        stmt.setDouble(stmtIndex++, qty);
                        stmt.setTimestamp(stmtIndex++, tstamp);
                        stmt.setLong(stmtIndex++, revision);
                        stmt.executeUpdate();
                        if (productHash.size() > 0 && productHash.containsKey(id)) {
                            dataDelayCache = checkDataDelay(productHash.get(id), offset, tstamp, dataDelayCache);
                        }
                        queryCount++;
                    }
                }

            }

            // record that a sales reading happened in the location table
            sql = " UPDATE location SET lastSold=GREATEST(lastSold,?) WHERE id=?";
            stmt = conn.prepareStatement(sql);
            stmt.setTimestamp(1, tsLast);
            stmt.setInt(2, location);
            stmt.executeUpdate();
            queryCount++;

            conn.commit();
            logger.dbAction("Committed " + queryCount + " queries");
            success = true;

            if (dataDelayCache.size() > 0) {
                logger.debug("Inserting Data Delays");
                setDataDelay(location, offset, dataDelayCache);
            }

        } catch (SQLException sqle) {
            if (null != conn) {
                try {
                    logger.dbAction("storeChecks: database rollback");
                    conn.rollback();
                } catch (SQLException ignore) {
                }
            }
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            if (null != conn && changedAutoCommit) {
                try {
                    conn.setAutoCommit(oldAutoCommit);
                } catch (SQLException ignore) {
                }
            }

            close(rs);
            close(stmt);
        }
        return success;
    }

    private void storeMultiBarCheckWrapper(Element toHandle, Element toAppend, SecureSession ss) throws HandlerException {
        int location = HandlerUtils.getRequiredInteger(toHandle, "locationId");

        logger.readingAction(function + "#" + ss.getLocation() + "-" + ss.getClientId());
        if (61 != location && location != ss.getLocation()) {
            logger.readingAccessViolation("Called " + function + " as L#" + location +
                    ", but authenticated as L#" + ss.getLocation());
            throw new HandlerException("Access Violation");
        }

        boolean success = storePositouchMultiBarCheck(toHandle, location);
        toAppend.addElement("success").addText(success ? "true" : "false");
    }

    private boolean storePositouchMultiBarCheck(Element toHandle, int location)
            throws HandlerException {

        long startOfChecksEpoch = HandlerUtils.getOptionalLong(toHandle,
                "startOfChecksEpoch");
        boolean storeAll = HandlerUtils.getOptionalBoolean(toHandle, "storeAll");
        List allChecks = toHandle.elements("check");
        if (null == allChecks || allChecks.size() == 0) {
            return true;
        }


        boolean success = false;
        int stmtIndex = 1;

        PreparedStatement stmt = null;
        ResultSet rs = null;
        int queryCount = 0;

        String sql = null;
        boolean oldAutoCommit = true;
        boolean changedAutoCommit = false;

        Calendar calEpoch = Calendar.getInstance();
        Timestamp tsLast = new Timestamp(calEpoch.getTimeInMillis());
        Hashtable<String, Boolean> idHash = new Hashtable<String, Boolean>();
        Hashtable<String, Integer> productHash = new Hashtable<String, Integer>();
        // A cache of product data delay sets (maps product -> datadelay hour)
        HashMap<Integer, ArrayList> dataDelayCache = new HashMap<Integer, ArrayList>();
        ArrayList<Integer> costCenterArray = new ArrayList();
        ArrayList<Integer> missingCostCenterArray = new ArrayList();

        try {
            java.sql.Date startOfChecks = null, endOfChecks = null;
            if (startOfChecksEpoch > 0) {
                startOfChecksEpoch *= 1000L;
                startOfChecks = new java.sql.Date(startOfChecksEpoch);
                Calendar endCal = Calendar.getInstance();
                endCal.setTimeInMillis(startOfChecksEpoch);
                endCal.add(Calendar.DAY_OF_MONTH, 1);
                endOfChecks = new java.sql.Date(endCal.getTimeInMillis());
            }

            String checkId;
            int costCenter;
            double qty, offset = 0.00;
            String id;
            long epoch;
            int locationType = 1, zoneId = 0, barId = 0, stationId = 0;
            oldAutoCommit = conn.getAutoCommit();
            changedAutoCommit = true;
            conn.setAutoCommit(false);

            sql = "SELECT l.easternOffset, l.type FROM location l WHERE l.id = ? ";
            stmt = conn.prepareStatement(sql);
            stmt.setInt(1, location);
            rs = stmt.executeQuery();
            if (rs.next()) {
                offset = rs.getDouble(1);
                locationType = rs.getInt(2);
            }

            sql = "SELECT c.ccID FROM costCenter c WHERE c.location = ? ";
            stmt = conn.prepareStatement(sql);
            stmt.setInt(1, location);
            rs = stmt.executeQuery();
            while (rs.next()) {
                costCenterArray.add(rs.getInt(1));
            }

            for (Object checkObj : allChecks) {
                Element elCheck = (Element) checkObj;
                checkId = HandlerUtils.getOptionalString(elCheck, "checkId");

                // check to see if this is a revision check
                long revision = HandlerUtils.getOptionalLong(elCheck, "revision");
                boolean ignoreCheck = false;
                if (null != checkId && null != startOfChecks && revision > 0) {
                    // see if a more recent version of the check is already stored
                    sql = "SELECT reportRecordId FROM sales " +
                            "WHERE location = ? " +
                            " AND checkId = ? " +
                            " AND date BETWEEN ? AND ? " +
                            " ORDER BY reportRecordId DESC";
                    stmt = conn.prepareStatement(sql);
                    stmt.setInt(1, location);
                    stmt.setString(2, checkId);
                    stmt.setTimestamp(3, toSqlTimestamp(startOfChecks));
                    stmt.setTimestamp(4, toSqlTimestamp(endOfChecks));
                    rs = stmt.executeQuery();
                    queryCount++;
                    long revCheck = -1;
                    if (null != rs && rs.next()) {
                        revCheck = rs.getLong("reportRecordId");
                    }
                    close(rs);
                    if (revCheck > -1) {
                        if (revision > revCheck) {
                            // delete previous versions
                            sql = "DELETE FROM sales " +
                                    "WHERE location = ? " +
                                    " AND checkId = ? " +
                                    " AND date BETWEEN ? AND ?";
                            stmt = conn.prepareStatement(sql);
                            stmt.setInt(1, location);
                            stmt.setString(2, checkId);
                            stmt.setTimestamp(3, toSqlTimestamp(startOfChecks));
                            stmt.setTimestamp(4, toSqlTimestamp(endOfChecks));
                            stmt.executeUpdate();
                            queryCount++;
                        } else {
                            ignoreCheck = true;
                        }
                    }
                } else {
                    revision = 0;
                }

                // check to see if the check is ignored or empty
                List itemList = elCheck.elements("item");
                if (ignoreCheck || null == itemList || itemList.size() == 0) {
                    continue;
                }

                // get all the allowable PLU's
                if (!storeAll) {
                    sql = "SELECT b.plu, i.product FROM beverage b LEFT JOIN ingredient i ON i.beverage = b.id WHERE b.location = ?;";
                    stmt = conn.prepareStatement(sql);
                    stmt.setInt(1, location);
                    rs = stmt.executeQuery();
                    queryCount++;
                    while (null != rs && rs.next()) {
                        idHash.put(rs.getString(1), Boolean.TRUE);
                        productHash.put(rs.getString(1), rs.getInt(2));
                    }
                    close(rs);
                }

                // Insert the current values for a check
                sql = "INSERT INTO sales (location, costCenter, checkId, pluNumber, quantity, date, reportRecordId)" +
                        " values (?,?,?,?,?,?,?)";
                String qtyStr;
                boolean store = false;
                for (Object o : itemList) {
                    Element item = (Element) o;
                    id = HandlerUtils.getRequiredString(item, "id");
                    if (storeAll || idHash.containsKey(id)) {
                        epoch = HandlerUtils.getRequiredLong(item, "epoch");
                        calEpoch.setTimeInMillis(epoch * 1000L);
                        Timestamp tstamp = new Timestamp(calEpoch.getTimeInMillis());
                        qtyStr = HandlerUtils.getRequiredString(item, "qty");
                        costCenter = HandlerUtils.getRequiredInteger(item, "costCenter");
                        qty = Double.parseDouble(qtyStr);
                        //logger.debug("Inserting: (" + location + ", " + (null == checkId ? "NULL" : checkId) + ", " + id + ", " + qty + ", " + tstamp + ", " + revision + ")");
                        stmtIndex = 1;
                        stmt = conn.prepareStatement(sql);
                        stmt.setInt(stmtIndex++, location);
                        stmt.setInt(stmtIndex++, costCenter);
                        if (null == checkId) {
                            stmt.setNull(stmtIndex++, java.sql.Types.VARCHAR);
                        } else {
                            stmt.setString(stmtIndex++, checkId);
                        }
                        stmt.setString(stmtIndex++, id);
                        stmt.setDouble(stmtIndex++, qty);
                        stmt.setTimestamp(stmtIndex++, tstamp);
                        stmt.setLong(stmtIndex++, revision);
                        stmt.executeUpdate();
                        if (productHash.size() > 0 && productHash.containsKey(id)) {
                            dataDelayCache = checkDataDelay(productHash.get(id), offset, tstamp, dataDelayCache);
                        }
                        queryCount++;

                        //Adding all the missing cost centers to be added to the cost center table
                        if (!costCenterArray.contains(costCenter)){
                            missingCostCenterArray.add(costCenter);
                            costCenterArray.add(costCenter);
                        }

                    }
                }

            }

            // selecting the first bar that was created for the retail location
            if (locationType == 1) {
                    storeRetailerMissingCostCenter(missingCostCenterArray, location);
            }

            // record that a sales reading happened in the location table
            sql = " UPDATE location SET lastSold=GREATEST(lastSold,?) WHERE id=?";
            stmt = conn.prepareStatement(sql);
            stmt.setTimestamp(1, tsLast);
            stmt.setInt(2, location);
            stmt.executeUpdate();
            queryCount++;

            conn.commit();
            logger.dbAction("Committed " + queryCount + " queries");
            success = true;

            if (dataDelayCache.size() > 0) {
                logger.debug("Inserting Data Delays");
                setDataDelay(location, offset, dataDelayCache);
            }

        } catch (SQLException sqle) {
            if (null != conn) {
                try {
                    logger.dbAction("storeChecks: database rollback");
                    conn.rollback();
                } catch (SQLException ignore) {
                }
            }
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            if (null != conn && changedAutoCommit) {
                try {
                    conn.setAutoCommit(oldAutoCommit);
                } catch (SQLException ignore) {
                }
            }

            close(rs);
            close(stmt);
        }
        return success;
    }

    private void storeDbfData(Element toHandle, Element toAppend, SecureSession ss)
            throws HandlerException {

        int location = HandlerUtils.getRequiredInteger(toHandle, "locationId");
        boolean storeAll = HandlerUtils.getOptionalBoolean(toHandle, "storeAll");

        // A cache of product data delay sets (maps product -> datadelay hour)
        HashMap<Integer, ArrayList> dataDelayCache = new HashMap<Integer, ArrayList>();

        if (location != ss.getLocation()) {
            logger.readingAccessViolation("Called " + function + " as L#" + location +
                    ", but authenticated as L#" + ss.getLocation());
            throw new HandlerException("Access Violation");
        }

        boolean success = false;
        int stmtIndex = 1;
        int inventory;
        boolean oldAutoCommit = true;
        boolean changedAutoCommit = false;
        double quantity, currInv, newInv, offset = 0.00;
        List itemList = toHandle.elements("item");
        if (null != itemList && itemList.size() > 0) {
            PreparedStatement stmt = null;
            PreparedStatement stmtDel = null;
            ResultSet rs = null;
            int queryCount = 0;

            String sql = null;

            Hashtable<String, Boolean> idHash = new Hashtable<String, Boolean>();
            Hashtable<String, Integer> productHash = new Hashtable<String, Integer>();
            Hashtable<String, Integer> xmlRecCount = new Hashtable<String, Integer>();
            Vector<Hashtable<String, Object>> records = new Vector<Hashtable<String, Object>>();
            Hashtable<String, Boolean> ignoreHash = new Hashtable<String, Boolean>();

            try {
                oldAutoCommit = conn.getAutoCommit();

                if (!storeAll) {
                    sql = "SELECT plu FROM beverage where location = ?";
                    stmt = conn.prepareStatement(sql);
                    stmt.setInt(1, location);
                    rs = stmt.executeQuery();
                    queryCount++;
                    while (rs.next()) {
                        idHash.put(rs.getString(1), Boolean.TRUE);
                    }
                    close(rs);
                }

                sql = "SELECT l.easternOffset, l.lastSold FROM location l WHERE l.id = ? ";
                stmt = conn.prepareStatement(sql);
                stmt.setInt(1, location);
                rs = stmt.executeQuery();
                if (rs.next()) {
                    offset = rs.getDouble(1);
                }

                sql = "SELECT b.plu, i.product FROM beverage b left join ingredient i on i.beverage = b.id WHERE b.location = ? ORDER BY b.plu;";
                stmt = conn.prepareStatement(sql);
                stmt.setInt(1, location);
                rs = stmt.executeQuery();
                while (rs.next()) {
                    productHash.put(rs.getString(1), rs.getInt(2));
                }
                close(rs);

                String qtyStr;
                long dataStart = -1, dataEnd = -1;
                for (Object o : itemList) {
                    Element item = (Element) o;
                    String id = HandlerUtils.getRequiredString(item, "id");
                    if (storeAll || idHash.containsKey(id)) {
                        // create a new hash record
                        Hashtable<String, Object> record = new Hashtable<String, Object>();
                        record.put("id", id);
                        qtyStr = HandlerUtils.getRequiredString(item, "qty");
                        record.put("qty", new Double(qtyStr));
                        long epoch = HandlerUtils.getRequiredLong(item, "epoch");
                        Timestamp tstamp = new Timestamp(epoch * 1000L);
                        record.put("tstamp", tstamp);
                        String recordId = HandlerUtils.getRequiredDigitString(item, "recordId");
                        record.put("recordId", recordId);
                        records.add(record);

                        // expand the data epoch range
                        if (dataStart < 0 || epoch < dataStart) {
                            dataStart = epoch;
                        }
                        if (dataEnd < 0 || epoch > dataEnd) {
                            dataEnd = epoch;
                        }

                        // keep track of each record ID count in the xml
                        Integer countObj = xmlRecCount.get(recordId);
                        int countInt = 0;
                        if (null != countObj) {
                            countInt = countObj.intValue();
                        }
                        xmlRecCount.put(recordId, new Integer(++countInt));
                    }
                }

                if (location == 109) {
                    logger.debug("Increasing the timestamp range for Applebee's");
                    dataStart = dataStart - 600;
                    dataEnd = dataEnd + 600;
                }

                Timestamp tsStart = new Timestamp(dataStart * 1000L);
                Timestamp tsEnd = new Timestamp(dataEnd * 1000L);

                // Check the existing record counts
                sql = "SELECT reportRecordId, count(reportRecordId)" +
                        " FROM sales WHERE location = ? AND" +
                        " date BETWEEN ? AND ? GROUP BY reportRecordId";
                stmt = conn.prepareStatement(sql);
                stmt.setInt(1, location);
                stmt.setTimestamp(2, tsStart);
                stmt.setTimestamp(3, tsEnd);
                rs = stmt.executeQuery();
                queryCount++;
                Hashtable<Long, Boolean> recordQtyHash = null;
                int recordCount = 0;

                // query to remove previous record entries
                String sqlDel = "DELETE FROM sales WHERE location=?" +
                        " AND date BETWEEN ? AND ? AND reportRecordId=?";
                stmtDel = conn.prepareStatement(sqlDel);

                // remove old records
                changedAutoCommit = true;
                conn.setAutoCommit(false);
                while (rs.next()) {
                    recordCount++;
                    String recordStr = rs.getString(1);
                    int dbNum = rs.getInt(2);
                    Integer i = xmlRecCount.get(recordStr);
                    int xmlNum = -1;
                    if (null != i) {
                        xmlNum = i.intValue();
                    }
                    // if the counts are the same, ignore the existing records
                    // otherwise, remove the records so we can add the new ones
                    if (dbNum == xmlNum) {
                        ignoreHash.put(recordStr, Boolean.TRUE);

                    } else {
                        stmtDel.setInt(1, location);
                        stmtDel.setTimestamp(2, tsStart);
                        stmtDel.setTimestamp(3, tsEnd);
                        stmtDel.setString(4, recordStr);
                        stmtDel.executeUpdate();
                        queryCount++;

                    }
                }
                close(rs);
                close(stmtDel);
                logger.debug("Found " + recordCount + " record IDs.");

                // add the new records
                // Insert the current values for a check
                sql = "INSERT INTO sales (location, pluNumber, quantity, date, reportRecordId)" +
                        " VALUES (?,?,?,?,?)";
                int numInserted = 0;
                int numIgnored = 0;
                for (Hashtable<String, Object> rec : records) {
                    String recordId = (String) rec.get("recordId");
                    Boolean b = ignoreHash.get(recordId);
                    if (null != b && b.booleanValue()) {
                        numIgnored++;
                    } else {
                        String id = (String) rec.get("id");
                        double qty = ((Double) rec.get("qty")).doubleValue();
                        Timestamp tstamp = (Timestamp) rec.get("tstamp");
                        stmtIndex = 1;
                        stmt = conn.prepareStatement(sql);
                        stmt.setInt(stmtIndex++, location);
                        stmt.setString(stmtIndex++, id);
                        stmt.setDouble(stmtIndex++, qty);
                        stmt.setTimestamp(stmtIndex++, tstamp);
                        stmt.setString(stmtIndex++, recordId);
                        stmt.executeUpdate();
                        if (productHash.size() > 0 && productHash.containsKey(id)) {
                            dataDelayCache = checkDataDelay(productHash.get(id), offset, tstamp, dataDelayCache);
                        }
                        queryCount++;
                        numInserted++;
                    }
                }

                if (tsEnd != null) {
                    // record that a sales reading happened in the location table
                    sql = " UPDATE location SET lastSold=GREATEST(lastSold,?) WHERE id=?";
                    stmt = conn.prepareStatement(sql);
                    stmt.setTimestamp(1, tsEnd);
                    stmt.setInt(2, location);
                    stmt.executeUpdate();
                    queryCount++;
                }

                conn.commit();
                logger.dbAction("Committed " + queryCount + " queries");

                logger.debug("Ignored record IDs: " + numIgnored);
                logger.debug("Inserted records: " + numInserted);

                success = true;

                if (dataDelayCache.size() > 0) {
                    logger.debug("Inserting Data Delays");
                    setDataDelay(location, offset, dataDelayCache);
                }

            } catch (SQLException sqle) {
                if (null != conn) {
                    try {
                        logger.dbAction(function + ": database rollback");
                        conn.rollback();
                    } catch (SQLException ignore) {
                    }
                }
                logger.dbError("Database error: " + sqle.getMessage());
                throw new HandlerException(sqle);
            } catch (Exception e) {
                throw new HandlerException(e);
            } finally {
                if (null != conn && changedAutoCommit) {
                    try {
                        conn.setAutoCommit(oldAutoCommit);
                    } catch (SQLException ignore) {
                    }
                }

                close(rs);
                close(stmt);
                close(stmtDel);
            }
        }
        toAppend.addElement("success").addText(success ? "true" : "false");
    }

    private void storeDbfMultiBarData(Element toHandle, Element toAppend, SecureSession ss)
            throws HandlerException {

        int location = HandlerUtils.getRequiredInteger(toHandle, "locationId");
        boolean storeAll = HandlerUtils.getOptionalBoolean(toHandle, "storeAll");

        // A cache of product data delay sets (maps product -> datadelay hour)

        if (location != ss.getLocation()) {
            logger.readingAccessViolation("Called " + function + " as L#" + location +
                    ", but authenticated as L#" + ss.getLocation());
            throw new HandlerException("Access Violation");
        }
        
        ArrayList<Integer> costCenterArray  = new ArrayList();
        HashMap<Integer, ArrayList> dataDelayCache 
                                            = new HashMap<Integer, ArrayList>();
        ArrayList<Integer> missingCostCenterArray 
                                            = new ArrayList();
        ResultSet rs                        = null;
        PreparedStatement stmt              = null;
        String sql                          = null;
        double offset                       = 0.00;
        int locationType                    = 0;
        int posAutomation                   = 0;
        
        try {
            sql                             = "SELECT l.easternOffset, l.type, lD.posAutomation FROM location l LEFT JOIN locationDetails lD ON lD.location = l.id WHERE l.id = ? ";
            stmt                            = conn.prepareStatement(sql);
            stmt.setInt(1, location);
            rs                              = stmt.executeQuery();
            if (rs.next()) {
                offset                      = rs.getDouble(1);
                locationType                = rs.getInt(2);
                posAutomation               = rs.getInt(3);
            }
        } catch (Exception e) {
            throw new HandlerException(e);
        } finally {
            close(rs);
            close(stmt);
        }

        boolean success = false;
        int stmtIndex = 1;
        int inventory;
        boolean oldAutoCommit = true;
        boolean changedAutoCommit = false;
        double quantity, currInv, newInv;

        try { oldAutoCommit = conn.getAutoCommit();

        
        List itemList = toHandle.elements("item");
        if (null != itemList && itemList.size() > 0) {
            PreparedStatement stmtDel = null;
            int queryCount = 0;

            Hashtable<String, Boolean> idHash = new Hashtable<String, Boolean>();
            Hashtable<String, Integer> productHash = new Hashtable<String, Integer>();
            Hashtable<String, Integer> xmlRecCount = new Hashtable<String, Integer>();
            Vector<Hashtable<String, Object>> records = new Vector<Hashtable<String, Object>>();
            Hashtable<String, Boolean> ignoreHash = new Hashtable<String, Boolean>();

            try {

                if (!storeAll) {
                    sql = "SELECT plu FROM beverage where location = ?";
                    stmt = conn.prepareStatement(sql);
                    stmt.setInt(1, location);
                    rs = stmt.executeQuery();
                    queryCount++;
                    while (rs.next()) {
                        idHash.put(rs.getString(1), Boolean.TRUE);
                    }
                    close(rs);
                }

                sql = "SELECT c.ccID FROM costCenter c WHERE c.location = ? ";
                stmt = conn.prepareStatement(sql);
                stmt.setInt(1, location);
                rs = stmt.executeQuery();
                while (rs.next()) {
                    costCenterArray.add(rs.getInt(1));
                }

                sql = "SELECT b.plu, i.product FROM beverage b left join ingredient i on i.beverage = b.id WHERE b.location = ? ORDER BY b.plu ;";
                stmt = conn.prepareStatement(sql);
                stmt.setInt(1, location);
                rs = stmt.executeQuery();
                while (rs.next()) {
                    productHash.put(rs.getString(1), rs.getInt(2));
                }
                close(rs);

                String qtyStr;
                String costCenterStr;
                long dataStart = -1, dataEnd = -1;
                for (Object o : itemList) {
                    Element item = (Element) o;
                    String id = HandlerUtils.getRequiredString(item, "id");
                    if (storeAll || idHash.containsKey(id)) {
                        // create a new hash record
                        Hashtable<String, Object> record = new Hashtable<String, Object>();
                        record.put("id", id);
                        costCenterStr = HandlerUtils.getRequiredString(item, "costCenter");
                        record.put("costCenter", new Integer(costCenterStr));
                        qtyStr = HandlerUtils.getRequiredString(item, "qty");
                        record.put("qty", new Double(qtyStr));
                        long epoch = HandlerUtils.getRequiredLong(item, "epoch");
                        Timestamp tstamp = new Timestamp(epoch * 1000L);
                        record.put("tstamp", tstamp);
                        String recordId = HandlerUtils.getRequiredDigitString(item, "recordId");
                        record.put("recordId", recordId);
                        records.add(record);

                        // expand the data epoch range
                        if (dataStart < 0 || epoch < dataStart) {
                            dataStart = epoch;
                        }
                        if (dataEnd < 0 || epoch > dataEnd) {
                            dataEnd = epoch;
                        }

                        // keep track of each record ID count in the xml
                        Integer countObj = xmlRecCount.get(recordId);
                        int countInt = 0;
                        if (null != countObj) {
                            countInt = countObj.intValue();
                        }
                        xmlRecCount.put(recordId, new Integer(++countInt));
                    }
                }

                Timestamp tsStart = new Timestamp(dataStart * 1000L);
                Timestamp tsEnd = new Timestamp(dataEnd * 1000L);

                // Check the existing record counts
                sql = "SELECT reportRecordId, count(reportRecordId)" +
                        " FROM sales WHERE location = ? AND" +
                        " date BETWEEN ? AND ? GROUP BY reportRecordId";
                stmt = conn.prepareStatement(sql);
                stmt.setInt(1, location);
                stmt.setTimestamp(2, tsStart);
                stmt.setTimestamp(3, tsEnd);
                rs = stmt.executeQuery();
                queryCount++;
                Hashtable<Long, Boolean> recordQtyHash = null;
                int recordCount = 0;

                // query to remove previous record entries
                String sqlDel = "DELETE FROM sales WHERE location=?" +
                        " AND date BETWEEN ? AND ? AND reportRecordId=?";
                stmtDel = conn.prepareStatement(sqlDel);

                // remove old records
                changedAutoCommit = true;
                conn.setAutoCommit(false);
                while (rs.next()) {
                    recordCount++;
                    String recordStr = rs.getString(1);
                    int dbNum = rs.getInt(2);
                    Integer i = xmlRecCount.get(recordStr);
                    int xmlNum = -1;
                    if (null != i) {
                        xmlNum = i.intValue();
                    }
                    // if the counts are the same, ignore the existing records
                    // otherwise, remove the records so we can add the new ones
                    if (dbNum == xmlNum) {
                        ignoreHash.put(recordStr, Boolean.TRUE);

                    } else {
                        stmtDel.setInt(1, location);
                        stmtDel.setTimestamp(2, tsStart);
                        stmtDel.setTimestamp(3, tsEnd);
                        stmtDel.setString(4, recordStr);
                        stmtDel.executeUpdate();
                        queryCount++;
                    }
                }
                close(rs);
                close(stmtDel);
                logger.debug("Found " + recordCount + " record IDs.");

                // add the new records
                // Insert the current values for a check
                sql = "INSERT INTO sales (location, pluNumber, costCenter, quantity, date, reportRecordId)" +
                        " VALUES (?,?,?,?,?,?)";
                int numInserted = 0;
                int numIgnored = 0;
                for (Hashtable<String, Object> rec : records) {
                    String recordId = (String) rec.get("recordId");
                    Boolean b = ignoreHash.get(recordId);
                    if (null != b && b.booleanValue()) {
                        numIgnored++;
                    } else {
                        String id = (String) rec.get("id");
                        int costCenter = (Integer) rec.get("costCenter");
                        double qty = ((Double) rec.get("qty")).doubleValue();
                        Timestamp tstamp = (Timestamp) rec.get("tstamp");
                        stmtIndex = 1;
                        stmt = conn.prepareStatement(sql);
                        stmt.setInt(stmtIndex++, location);
                        stmt.setString(stmtIndex++, id);
                        stmt.setInt(stmtIndex++, costCenter);
                        stmt.setDouble(stmtIndex++, qty);
                        stmt.setTimestamp(stmtIndex++, tstamp);
                        stmt.setString(stmtIndex++, recordId);
                        stmt.executeUpdate();
                        if (productHash.size() > 0 && productHash.containsKey(id)) {
                            dataDelayCache = checkDataDelay(productHash.get(id), offset, tstamp, dataDelayCache);
                        }
                        queryCount++;
                        numInserted++;

                        //Adding all the missing cost centers to be added to the cost center table
                        if (!costCenterArray.contains(costCenter)){
                            missingCostCenterArray.add(costCenter);
                            costCenterArray.add(costCenter);
                        }
                    }
                }

                // selecting the first bar that was created for the retail location
                if (locationType == 1) {
                    storeRetailerMissingCostCenter(missingCostCenterArray, location);
                }

                if (tsEnd != null) {
                    // record that a sales reading happened in the location table
                    sql = " UPDATE location SET lastSold=GREATEST(lastSold,?) WHERE id=?";
                    stmt = conn.prepareStatement(sql);
                    stmt.setTimestamp(1, tsEnd);
                    stmt.setInt(2, location);
                    stmt.executeUpdate();
                    queryCount++;
                }

                conn.commit();
                logger.dbAction("Committed " + queryCount + " queries");
                logger.debug("Ignored record IDs: " + numIgnored);
                logger.debug("Inserted records: " + numInserted);

                success = true;
                if (dataDelayCache.size() > 0) {
                    logger.debug("Inserting Data Delays");
                    setDataDelay(location, offset, dataDelayCache);
                }
            } catch (SQLException sqle) {
                if (null != conn) {
                    try {
                        logger.dbAction(function + ": database rollback");
                        conn.rollback();
                    } catch (SQLException ignore) {
                    }
                }
                logger.dbError("Database error: " + sqle.getMessage());
                throw new HandlerException(sqle);
            } catch (Exception e) {
                throw new HandlerException(e);
            } finally {
                close(rs);
                close(stmt);
                close(stmtDel);
            }
        }

        List unknownItemList = toHandle.elements("unknownItem");
        if (posAutomation > 0 && null != unknownItemList && unknownItemList.size() > 0) {
            storeUnknownItemList(location, unknownItemList);
        }
        
        conn.commit();

        if (null != conn && changedAutoCommit) {
                conn.setAutoCommit(oldAutoCommit);
        }
        } catch (SQLException ignore) { }
        toAppend.addElement("success").addText(success ? "true" : "false");
    }

    private void storeUnknownItemList(int location, List unknownItemList) throws HandlerException {
        
            PreparedStatement stmt          = null;
            ResultSet rs                    = null;
            int queryCount                  = 0;
            String sql                      = null;

            try {
                String qtyStr;
                String costCenterStr;
                ArrayList<String> newPlus   = new ArrayList<String>();
                ArrayList<String> pluToIgnore
                                            = new ArrayList<String>();
                stmt                        = conn.prepareStatement("SELECT plu FROM beverageToIgnore WHERE location = ?;");
                stmt.setInt(1, location);
                rs                          = stmt.executeQuery();
                while(rs.next()) {
                    pluToIgnore.add(rs.getString(1));
                }

                sql                         = "INSERT INTO sales (location, pluNumber, costCenter, quantity, date, reportRecordId, checkId)" +
                                            " VALUES (?,?,?,?,?,?,?)";
                int numInserted             = 0;
                int numIgnored              = 0;
                for (Object o : unknownItemList) {
                    Element item            = (Element) o;
                    String id               = HandlerUtils.getRequiredString(item, "id");
                    if (pluToIgnore.contains(id)) { continue; }
                    // create a new hash record
                    costCenterStr           = HandlerUtils.getRequiredString(item, "costCenter");
                    int costCenter          = Integer.parseInt(costCenterStr);
                    qtyStr                  = HandlerUtils.getRequiredString(item, "qty");
                    qtyStr                  = qtyStr.trim();
                    if (qtyStr.contains("/"))
                    {
                        String[] parts      = qtyStr.split("/");
                        if (parts.length == 2) {
                            double splitValue
                                            = Double.valueOf(parts[0]) / Double.valueOf(parts[1]);
                            qtyStr          = String.valueOf(splitValue);
                        } else {
                            qtyStr          = "0.5";
                        }
                    }
                    double qty              = Double.parseDouble(qtyStr);
                    long epoch              = HandlerUtils.getRequiredLong(item, "epoch");
                    Timestamp tstamp        = new Timestamp(epoch * 1000L);
                    String recordId         = HandlerUtils.getRequiredDigitString(item, "recordId");
                    String name             = HandlerUtils.getOptionalString(item, "productName");
                    if (name == null || name.length() < 2) {
                        name                = HandlerUtils.getOptionalString(item, "name");
                    }
                    String checkId          = HandlerUtils.getOptionalString(item, "checkId");
                    if (checkId == null || checkId.length() < 2) {
                        checkId             = "1";
                    }

                    int stmtIndex           = 1;
                    stmt                    = conn.prepareStatement(sql);
                    stmt.setInt(stmtIndex++, location);
                    stmt.setString(stmtIndex++, id);
                    stmt.setInt(stmtIndex++, costCenter);
                    stmt.setDouble(stmtIndex++, qty);
                    stmt.setTimestamp(stmtIndex++, tstamp);
                    stmt.setString(stmtIndex++, recordId);
                    stmt.setString(stmtIndex++, checkId);
                    stmt.executeUpdate();
                    queryCount++;
                    numInserted++;

                    //Adding all the missing cost centers to be added to the cost center table
                    if (!newPlus.contains(id)) {
                        Double ounces       = 0.0;
                        HashMap<String, Double> hintOunces
                                            = new HashMap<String, Double>();
                        HashMap<String, Integer> hintType
                                            = new HashMap<String, Integer>();
                        HashMap<String, Integer> unknownPlus
                                            = new HashMap<String, Integer>();
                        stmt                = conn.prepareStatement("SELECT id, plu FROM beverage WHERE  location = ? AND name = 'Unknown Product'");
                        stmt.setInt(1, location);
                        rs                  = stmt.executeQuery();
                        while(rs.next()) {
                            unknownPlus.put(rs.getString(2), rs.getInt(1));
                        }

                        stmt                = conn.prepareStatement("SELECT complex, hint, type, ounces FROM posBeverageSize WHERE location = ? AND defaultValue = 0;");
                        stmt.setInt(1, location);
                        rs                  = stmt.executeQuery();
                        while(rs.next()) {
                            if (rs.getInt(1) == 0) {
                                ounces      = rs.getDouble(4);
                            } else {
                                hintOunces.put(rs.getString(2), rs.getDouble(4));
                                hintType.put(rs.getString(2), rs.getInt(3));
                            }
                        }

                        if (hintOunces.size() > 0) {
                            stmt            = conn.prepareStatement("SELECT ounces FROM posBeverageSize WHERE location = ? AND defaultValue = 1;");
                            stmt.setInt(1, location);
                            rs              = stmt.executeQuery();
                            if (rs.next()) {
                                ounces      = rs.getDouble(1);
                            }
                        }

                        newPlus             = addPLUProduct(location, name, id, ounces, hintOunces, hintType, newPlus);
                    }
                }

                logger.dbAction("Committed " + queryCount + " queries");

                logger.debug("Ignored record IDs: " + numIgnored);
                logger.debug("Inserted records: " + numInserted);
            } catch (SQLException sqle) {
                if (null != conn) {
                    try {
                        logger.dbAction(function + ": database rollback");
                        conn.rollback();
                    } catch (SQLException ignore) {
                    }
                }
                logger.dbError("Database error: " + sqle.getMessage());
                throw new HandlerException(sqle);
            } catch (Exception e) {
                throw new HandlerException(e);
            } finally {
                close(rs);
                close(stmt);
            }
    }

    private int storeBottleRecords(int location, Hashtable<String, Integer> invHash, Vector<Hashtable<String, Object>> records) throws HandlerException {

        PreparedStatement stmt              = null;
        ResultSet rs                        = null;
        int bottleCount                     = 0, bottleInvCount = 0;
        int queryCount                      = 1;
        try {
            // Theoritical onHand Calculation

            for (Hashtable<String, Object> rec : records) {
                String id                   = (String) rec.get("id");
                String costCenterStr        = (String) rec.get("costCenter");
                String qtyStr               = (String) rec.get("qty");
                String checkId              = (String) rec.get("checkId");
                String recordId             = (String) rec.get("recordId");
                Long epoch                  = (Long) rec.get("epoch");

                Timestamp salesStart        = new Timestamp((epoch - 60) * 1000L);
                Timestamp salesEnd          = new Timestamp((epoch + 60) * 1000L);

                String sql                  = "SELECT s.reportRecordId FROM sales s WHERE s.location = ? AND s.date BETWEEN ? AND ? " +
                                            " AND s.pluNumber=? AND s.costCenter=? AND s.quantity=? AND s.checkId=? AND s.reportRecordId=? ";
                stmt                        = conn.prepareStatement(sql);
                stmt.setInt(1, location);
                stmt.setTimestamp(2, salesStart);
                stmt.setTimestamp(3, salesEnd);
                stmt.setString(4, id);
                stmt.setString(5, costCenterStr);
                stmt.setString(6, qtyStr);
                stmt.setString(7, checkId);
                stmt.setString(8, recordId);
                rs                          = stmt.executeQuery();
                queryCount++;
                if (!rs.next()) {
                    bottleCount++;
                    double quantity         = Double.parseDouble(qtyStr) / Double.valueOf(invHash.get(id));

                    String selectInvQuantity= "SELECT iv.id, iv.qtyOnHand FROM ingredient i left join inventory iv on iv.product = i.product left join beverage b on b.id = i.beverage where iv.location = ? and b.pType = 3 and b.location =? and b.plu=? order by b.name";
                    String sqlUpdateInv     = "UPDATE inventory SET qtyOnHand=? WHERE id=?";
                    stmt                    = conn.prepareStatement(selectInvQuantity);
                    stmt.setInt(1, location);
                    stmt.setInt(2, location);
                    stmt.setString(3, id);
                    rs                      = stmt.executeQuery();
                    queryCount++;
                    if (rs.next()) {
                        bottleInvCount++;
                        int inventory       = rs.getInt(1);
                        double currInv      = rs.getDouble(2);
                        double newInv       = currInv - quantity;
                        stmt                = conn.prepareStatement(sqlUpdateInv);
                        stmt.setDouble(1, newInv);
                        stmt.setInt(2, inventory);
                        stmt.executeUpdate();
                        queryCount++;
                    }
                }
            }
            logger.debug("Total new bottle records: " + bottleCount);
            logger.debug("Committed bottle inventory records: " + bottleInvCount);
        } catch (SQLException sqle) {
            if (null != conn) {
                try {
                    logger.dbAction(function + ": database rollback");
                    conn.rollback();
                } catch (SQLException ignore) {
                }
            }
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } catch (Exception e) {
            throw new HandlerException(e);
        } finally {
            close(rs);
            close(stmt);
        }
        return queryCount;
    }

    private int storeDraftRecords(int location, long dataStart, long dataEnd, double offset, ArrayList<Integer> costCenterArray,
            Hashtable<String, Integer> xmlRecCount, Hashtable<String, Integer> productHash, Hashtable<String, String> productPLUHash, 
            Vector<Hashtable<String, Object>> records) throws HandlerException {

        PreparedStatement stmt              = null;
        PreparedStatement stmtDel           = null, stmtDelSalesOunces = null;
        ResultSet rs                        = null;
        int queryCount                      = 1;

        // A cache of product data delay sets (maps product -> datadelay hour)
        HashMap<Integer, ArrayList> dataDelayCache
                                            = new HashMap<Integer, ArrayList>();
        ArrayList<Integer> missingCostCenterArray
                                            = new ArrayList();
        int locationType                    = 1;

        Hashtable<String, Boolean> ignoreHash
                                            = new Hashtable<String, Boolean>();
        boolean oldAutoCommit               = true;
        boolean changedAutoCommit           = false;

        try {

            stmt                            = conn.prepareStatement("SELECT type FROM location WHERE id = ?");
            stmt.setInt(1, location);
            rs                              = stmt.executeQuery();
            if (rs.next()) {
                locationType                = rs.getInt(1);
            }

            oldAutoCommit                   = conn.getAutoCommit();
            Timestamp tsStart               = new Timestamp(dataStart * 1000L);
            Timestamp tsEnd                 = new Timestamp(dataEnd * 1000L);

            // Check the existing record counts
            String sql                      = "SELECT IFNULL(checkId, 0), COUNT(checkId) FROM sales WHERE location = ? AND " +
                                            " date BETWEEN ? AND ? GROUP BY checkId";
            stmt                            = conn.prepareStatement(sql);
            stmt.setInt(1, location);
            stmt.setTimestamp(2, tsStart);
            stmt.setTimestamp(3, tsEnd);
            rs                              = stmt.executeQuery();
            queryCount++;

            int recordCount                 = 0;
            // query to remove previous record entries
            String sqlDel                   = "DELETE FROM sales WHERE location = ? AND date BETWEEN ? AND ? AND checkId = ?";
            String sqlDelSalesOuces         = "DELETE FROM salesOunce WHERE location = ? AND date BETWEEN ? AND ? AND checkId = ?";
            stmtDel                         = conn.prepareStatement(sqlDel);
            stmtDelSalesOunces              = conn.prepareStatement(sqlDelSalesOuces);
            
            // remove old records
            changedAutoCommit               = true;
            conn.setAutoCommit(false);
            while (rs.next()) {
                recordCount++;
                String checkIdStr           = rs.getString(1);
                int dbNum                   = rs.getInt(2);
                Integer i                   = xmlRecCount.get(checkIdStr);
                int xmlNum                  = -1;
                if (null != i) {
                    xmlNum                  = i.intValue();
                }
                // if the counts are the same, ignore the existing records
                // otherwise, remove the records so we can add the new ones
                if (dbNum == xmlNum) {
                    ignoreHash.put(checkIdStr, Boolean.TRUE);
                } else {
                    stmtDel.setInt(1, location);
                    stmtDel.setTimestamp(2, tsStart);
                    stmtDel.setTimestamp(3, tsEnd);
                    stmtDel.setString(4, checkIdStr);
                    stmtDel.executeUpdate();
                    queryCount++;
                    stmtDelSalesOunces.setInt(1, location);
                    stmtDelSalesOunces.setTimestamp(2, tsStart);
                    stmtDelSalesOunces.setTimestamp(3, tsEnd);
                    stmtDelSalesOunces.setString(4, checkIdStr);
                    stmtDelSalesOunces.executeUpdate();
                    queryCount++;
                }
            }
            close(rs);
            close(stmtDel);
            logger.debug("Found " + recordCount + " record IDs.");
            // add the new records
            // Insert the current values for a check
            sql                         = "INSERT INTO sales (location, pluNumber, costCenter, quantity, date, checkId, reportRecordId)" +
                                        " VALUES (?,?,?,?,?,?,?)";
            String insertSalesOunces    = "INSERT INTO salesOunce (location, product, costCenter, quantity, date, checkId, reportRecordId)" +
                                        " VALUES (?,?,?,?,?,?,?)";
            int numInserted             = 0;
            int numIgnored              = 0;
            for (Hashtable<String, Object> rec : records) {
                String checkId          = (String) rec.get("checkId");
                Boolean b               = ignoreHash.get(checkId);
                if (null != b && b.booleanValue()) {
                    numIgnored++;
                } else {
                    String id           = (String) rec.get("id");
                    int costCenter      = (Integer) rec.get("costCenter");
                    double qty          = ((Double) rec.get("qty")).doubleValue();
                    Long epoch          = (Long) rec.get("epoch");
                    String recordId     = (String) rec.get("recordId");
                    Timestamp tstamp    = new Timestamp(epoch * 1000L);

                    int stmtIndex       = 1;
                    stmt = conn.prepareStatement(sql);
                    stmt.setInt(stmtIndex++, location);
                    stmt.setString(stmtIndex++, id);
                    stmt.setInt(stmtIndex++, costCenter);
                    stmt.setDouble(stmtIndex++, qty);
                    stmt.setTimestamp(stmtIndex++, tstamp);
                    stmt.setString(stmtIndex++, checkId);
                    stmt.setString(stmtIndex++, recordId);
                    stmt.executeUpdate();
                    
                    if (productPLUHash.containsKey(id)) {
                        String productOuncesString  
                                            = productPLUHash.get(id);
                        for (String productOunces : productOuncesString.split(",")) {
                            String[] parts      = productOunces.split("-");
                            int product     = Integer.valueOf(parts[0]);
                            double ounces   = Double.parseDouble(parts[1]);
                            stmtIndex       = 1;
                            stmt            = conn.prepareStatement(insertSalesOunces);
                            stmt.setInt(stmtIndex++, location);
                            stmt.setInt(stmtIndex++, product);
                            stmt.setInt(stmtIndex++, costCenter);
                            stmt.setDouble(stmtIndex++, qty * ounces);
                            stmt.setTimestamp(stmtIndex++, tstamp);
                            stmt.setString(stmtIndex++, checkId);
                            stmt.setString(stmtIndex++, recordId);
                            stmt.executeUpdate();
                        }
                    }
                    
                    if (productHash.size() > 0 && productHash.containsKey(id)) {
                        dataDelayCache  = checkDataDelay(productHash.get(id), offset, tstamp, dataDelayCache);
                    }
                    queryCount++;
                    numInserted++;

                    //Adding all the missing cost centers to be added to the cost center table
                    if (!costCenterArray.contains(costCenter)){
                        missingCostCenterArray.add(costCenter);
                        costCenterArray.add(costCenter);
                    }
                }
            }

            // selecting the first bar that was created for the retail location
            if (locationType == 1) {
                storeRetailerMissingCostCenter(missingCostCenterArray, location);
            }

            if (tsEnd != null) {
                // record that a sales reading happened in the location table
                sql                         = " UPDATE location SET lastSold=GREATEST(lastSold,?) WHERE id=?";
                stmt                        = conn.prepareStatement(sql);
                stmt.setTimestamp(1, tsEnd);
                stmt.setInt(2, location);
                stmt.executeUpdate();
                queryCount++;
            }

            conn.commit();
            logger.debug("Ignored record IDs: " + numIgnored);
            logger.debug("Inserted records: " + numInserted);

            if (dataDelayCache.size() > 0) {
                logger.debug("Inserting Data Delays");
                setDataDelay(location, offset, dataDelayCache);
            }
        } catch (SQLException sqle) {
            if (null != conn) {
                try {
                    logger.dbAction(function + ": database rollback");
                    conn.rollback();
                } catch (SQLException ignore) {
                }
            }
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } catch (Exception e) {
            throw new HandlerException(e);
        } finally {
            if (null != conn && changedAutoCommit) {
                try {
                    conn.setAutoCommit(oldAutoCommit);
                } catch (SQLException ignore) {
                }
            }
            close(rs);
            close(stmt);
            close(stmtDel);
        }
        return queryCount;
    }

    private int storeDraftChecks(int location, long dataStart, long dataEnd, double offset, ArrayList<Integer> costCenterArray,
            Hashtable<String, Integer> xmlRecCount, Hashtable<String, Integer> productHash, Hashtable<String, String> productPLUHash, Vector<Hashtable<String, Object>> records) throws HandlerException {

        PreparedStatement stmt              = null;
        PreparedStatement stmtDel           = null;
        ResultSet rs                        = null;
        int queryCount                      = 1;

        // A cache of product data delay sets (maps product -> datadelay hour)
        HashMap<Integer, ArrayList> dataDelayCache
                                            = new HashMap<Integer, ArrayList>();
        ArrayList<Integer> missingCostCenterArray
                                            = new ArrayList();
        int locationType                    = 1;

        boolean oldAutoCommit               = true;
        boolean changedAutoCommit           = false;

        try {

            stmt                            = conn.prepareStatement("SELECT type FROM location WHERE id = ?");
            stmt.setInt(1, location);
            rs                              = stmt.executeQuery();
            if (rs.next()) {
                locationType                = rs.getInt(1);
            }

            oldAutoCommit                   = conn.getAutoCommit();
            Timestamp tsStart               = new Timestamp(dataStart * 1000L);
            Timestamp tsEnd                 = new Timestamp(dataEnd * 1000L);
            changedAutoCommit               = true;
            conn.setAutoCommit(false);
            ArrayList<String> checks        = new ArrayList<String>();
            
            // add the new records
            // Insert the current values for a check
            String sql                      = "INSERT INTO sales (location, pluNumber, costCenter, quantity, date, checkId, reportRecordId)" +
                                            " VALUES (?,?,?,?,?,?,?)";
            String insertSalesOunces        = "INSERT INTO salesOunce (location, product, costCenter, quantity, date, checkId, reportRecordId)" +
                                            " VALUES (?,?,?,?,?,?,?)";
            int numInserted                 = 0;
            int numIgnored                  = 0;
            for (Hashtable<String, Object> rec : records) {
                String checkId              = (String) rec.get("checkId");

                if (!checks.contains(checkId)) {
                    stmt                    = conn.prepareStatement("DELETE FROM sales WHERE checkId = ? AND date BETWEEN ? AND ?;");
                    stmt.setString(1, checkId);
                    stmt.setTimestamp(2, tsStart);
                    stmt.setTimestamp(3, tsEnd);
                    stmt.executeUpdate();
                    
                    stmt                    = conn.prepareStatement("DELETE FROM salesOunce WHERE checkId = ? AND date BETWEEN ? AND ?;");
                    stmt.setString(1, checkId);
                    stmt.setTimestamp(2, tsStart);
                    stmt.setTimestamp(3, tsEnd);
                    stmt.executeUpdate();
                    checks.add(checkId);
                }

                String id                   = (String) rec.get("id");
                int costCenter              = (Integer) rec.get("costCenter");
                double qty                  = ((Double) rec.get("qty")).doubleValue();
                Long epoch                  = (Long) rec.get("epoch");
                String recordId             = (String) rec.get("recordId");
                Timestamp tstamp            = new Timestamp(epoch * 1000L);

                int stmtIndex               = 1;
                stmt                        = conn.prepareStatement(sql);
                stmt.setInt(stmtIndex++, location);
                stmt.setString(stmtIndex++, id);
                stmt.setInt(stmtIndex++, costCenter);
                stmt.setDouble(stmtIndex++, qty);
                stmt.setTimestamp(stmtIndex++, tstamp);
                stmt.setString(stmtIndex++, checkId);
                stmt.setString(stmtIndex++, recordId);
                stmt.executeUpdate();
                
                if (productPLUHash.containsKey(id)) {
                    String productOuncesString  
                                            = productPLUHash.get(id);
                    for (String productOunces : productOuncesString.split(",")) {
                        String[] parts      = productOunces.split("-");
                        int product         = Integer.valueOf(parts[0]);
                        double ounces       = Double.parseDouble(parts[1]);
                        stmtIndex           = 1;
                        stmt                = conn.prepareStatement(insertSalesOunces);
                        stmt.setInt(stmtIndex++, location);
                        stmt.setInt(stmtIndex++, product);
                        stmt.setInt(stmtIndex++, costCenter);
                        stmt.setDouble(stmtIndex++, qty * ounces);
                        stmt.setTimestamp(stmtIndex++, tstamp);
                        stmt.setString(stmtIndex++, checkId);
                        stmt.setString(stmtIndex++, recordId);
                        stmt.executeUpdate();
                    }
                }
                
                if (productHash.size() > 0 && productHash.containsKey(id)) {
                    dataDelayCache          = checkDataDelay(productHash.get(id), offset, tstamp, dataDelayCache);
                }
                queryCount++;
                numInserted++;

                //Adding all the missing cost centers to be added to the cost center table
                if (!costCenterArray.contains(costCenter)){
                    missingCostCenterArray.add(costCenter);
                    costCenterArray.add(costCenter);
                }
            }

            // selecting the first bar that was created for the retail location
            if (locationType == 1) {
                storeRetailerMissingCostCenter(missingCostCenterArray, location);
            }


            // record that a sales reading happened in the location table
            sql                             = " UPDATE location SET lastSold = GREATEST(lastSold, NOW()) WHERE id=?";
            stmt                            = conn.prepareStatement(sql);
            stmt.setInt(1, location);
            stmt.executeUpdate();
            queryCount++;

            conn.commit();
            logger.debug("Ignored record IDs: " + numIgnored);
            logger.debug("Inserted records: " + numInserted);

            if (dataDelayCache.size() > 0) {
                logger.debug("Inserting Data Delays");
                setDataDelay(location, offset, dataDelayCache);
            }
        } catch (SQLException sqle) {
            if (null != conn) {
                try {
                    logger.dbAction(function + ": database rollback");
                    conn.rollback();
                } catch (SQLException ignore) {
                }
            }
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } catch (Exception e) {
            throw new HandlerException(e);
        } finally {
            if (null != conn && changedAutoCommit) {
                try {
                    conn.setAutoCommit(oldAutoCommit);
                } catch (SQLException ignore) {
                }
            }
            close(rs);
            close(stmt);
            close(stmtDel);
        }
        return queryCount;
    }

    private int storeDoubleGatewayDraftRecords(int location, long dataStart, long dataEnd, double offset, ArrayList<Integer> costCenterArray,
            Hashtable<String, Integer> xmlRecCount, Hashtable<String, Integer> productHash, Hashtable<String, String> productPLUHash, Vector<Hashtable<String, Object>> records) throws HandlerException {

        PreparedStatement stmt              = null;
        PreparedStatement stmtDel           = null, stmtDelSalesOunces = null;
        ResultSet rs                        = null;
        int queryCount                      = 1;

        // A cache of product data delay sets (maps product -> datadelay hour)
        HashMap<Integer, ArrayList> dataDelayCache
                                            = new HashMap<Integer, ArrayList>();
        ArrayList<Integer> missingCostCenterArray
                                            = new ArrayList();
        int locationType                    = 1;

        Hashtable<String, Boolean> ignoreHash
                                            = new Hashtable<String, Boolean>();
        boolean oldAutoCommit               = true;
        boolean changedAutoCommit           = false;

        try {

            logger.debug("Double Gateway");

            stmt                            = conn.prepareStatement("SELECT type FROM location WHERE id = ?");
            stmt.setInt(1, location);
            rs                              = stmt.executeQuery();
            if (rs.next()) {
                locationType                = rs.getInt(1);
            }

            oldAutoCommit                   = conn.getAutoCommit();
            Timestamp tsStart               = new Timestamp(dataStart * 1000L);
            Timestamp tsEnd                 = new Timestamp(dataEnd * 1000L);

            // Check the existing record counts
            String sql                      = "SELECT CONCAT(IFNULL(costCenter, 0), ':', IFNULL(checkId, 0)), SUM(quantity) FROM sales WHERE location = ? AND " +
                                            " date BETWEEN ? AND ? GROUP BY checkId, costCenter";
            stmt                            = conn.prepareStatement(sql);
            stmt.setInt(1, location);
            stmt.setTimestamp(2, tsStart);
            stmt.setTimestamp(3, tsEnd);
            rs                              = stmt.executeQuery();
            queryCount++;

            int recordCount                 = 0;
            // query to remove previous record entries
            String sqlDel                   = "DELETE FROM sales WHERE location = ? AND date BETWEEN ? AND ? AND checkId = ? AND costCenter = ?";
            stmtDel                         = conn.prepareStatement(sqlDel);
            sqlDel                          = "DELETE FROM salesOunce WHERE location = ? AND date BETWEEN ? AND ? AND checkId = ? AND costCenter = ?";
            stmtDelSalesOunces              = conn.prepareStatement(sqlDel);
            // remove old records
            changedAutoCommit               = true;
            conn.setAutoCommit(false);
            while (rs.next()) {
                recordCount++;
                String checkIdStr           = rs.getString(1);
                int dbNum                   = rs.getInt(2);
                String[] parts              = checkIdStr.split(":");

                Integer i                   = xmlRecCount.get(checkIdStr);
                int xmlNum                  = -1;
                if (null != i) {
                    xmlNum                  = i.intValue();
                } else {
                    xmlNum                  = dbNum;
                }

                // if the counts are the same, ignore the existing records
                // otherwise, remove the records so we can add the new ones
                if (dbNum == xmlNum) {
                    ignoreHash.put(checkIdStr, Boolean.TRUE);
                } else {
                    stmtDel.setInt(1, location);
                    stmtDel.setTimestamp(2, tsStart);
                    stmtDel.setTimestamp(3, tsEnd);
                    stmtDel.setString(4, parts[1]);
                    stmtDel.setString(5, parts[0]);
                    stmtDel.executeUpdate();
                    queryCount++;
                    stmtDelSalesOunces.setInt(1, location);
                    stmtDelSalesOunces.setTimestamp(2, tsStart);
                    stmtDelSalesOunces.setTimestamp(3, tsEnd);
                    stmtDelSalesOunces.setString(4, parts[1]);
                    stmtDelSalesOunces.setString(5, parts[0]);
                    stmtDelSalesOunces.executeUpdate();
                    queryCount++;
                }
            }
            close(rs);
            close(stmtDel);
            logger.debug("Found " + recordCount + " record IDs.");

            // add the new records
            // Insert the current values for a check
            sql                         = "INSERT INTO sales (location, pluNumber, costCenter, quantity, date, checkId, reportRecordId)" +
                                        " VALUES (?,?,?,?,?,?,?)";
            String insertSalesOunces    = "INSERT INTO salesOunce (location, product, costCenter, quantity, date, checkId, reportRecordId)" +
                                        " VALUES (?,?,?,?,?,?,?)";
            int numInserted             = 0;
            int numIgnored              = 0;
            for (Hashtable<String, Object> rec : records) {
                String checkId          = (String) rec.get("checkId");
                int costCenter          = (Integer) rec.get("costCenter");
                String costCenterCheckId= String.valueOf(costCenter) + ":" + checkId;
                Boolean b               = ignoreHash.get(costCenterCheckId);
                if (null != b && b.booleanValue()) {
                    numIgnored++;
                } else {
                    String id           = (String) rec.get("id");
                    double qty          = ((Double) rec.get("qty")).doubleValue();
                    Long epoch          = (Long) rec.get("epoch");
                    String recordId     = (String) rec.get("recordId");
                    Timestamp tstamp    = new Timestamp(epoch * 1000L);

                    int stmtIndex       = 1;
                    stmt                = conn.prepareStatement(sql);
                    stmt.setInt(stmtIndex++, location);
                    stmt.setString(stmtIndex++, id);
                    stmt.setInt(stmtIndex++, costCenter);
                    stmt.setDouble(stmtIndex++, qty);
                    stmt.setTimestamp(stmtIndex++, tstamp);
                    stmt.setString(stmtIndex++, checkId);
                    stmt.setString(stmtIndex++, recordId);
                    stmt.executeUpdate();
                    queryCount++;
                    
                    if (productPLUHash.containsKey(id)) {
                        String productOuncesString  
                                            = productPLUHash.get(id);
                        for (String productOunces : productOuncesString.split(",")) {
                            String[] parts  = productOunces.split("-");
                            int product     = Integer.valueOf(parts[0]);
                            double ounces   = Double.parseDouble(parts[1]);
                            stmtIndex       = 1;
                            stmt            = conn.prepareStatement(insertSalesOunces);
                            stmt.setInt(stmtIndex++, location);
                            stmt.setInt(stmtIndex++, product);
                            stmt.setInt(stmtIndex++, costCenter);
                            stmt.setDouble(stmtIndex++, qty * ounces);
                            stmt.setTimestamp(stmtIndex++, tstamp);
                            stmt.setString(stmtIndex++, checkId);
                            stmt.setString(stmtIndex++, recordId);
                            stmt.executeUpdate();
                        }
                    }
                    if (productHash.size() > 0 && productHash.containsKey(id)) {
                        dataDelayCache  = checkDataDelay(productHash.get(id), offset, tstamp, dataDelayCache);
                    }
                    queryCount++;
                    numInserted++;

                    //Adding all the missing cost centers to be added to the cost center table
                    if (!costCenterArray.contains(costCenter)){
                        missingCostCenterArray.add(costCenter);
                        costCenterArray.add(costCenter);
                    }
                }
            }

            // selecting the first bar that was created for the retail location
            if (locationType == 1) {
                storeRetailerMissingCostCenter(missingCostCenterArray, location);
            }

            if (tsEnd != null) {
                // record that a sales reading happened in the location table
                sql                         = " UPDATE location SET lastSold=GREATEST(lastSold,?) WHERE id=?";
                stmt                        = conn.prepareStatement(sql);
                stmt.setTimestamp(1, tsEnd);
                stmt.setInt(2, location);
                stmt.executeUpdate();
                queryCount++;
            }

            conn.commit();
            logger.debug("Ignored record IDs: " + numIgnored);
            logger.debug("Inserted records: " + numInserted);

            if (dataDelayCache.size() > 0) {
                logger.debug("Inserting Data Delays");
                setDataDelay(location, offset, dataDelayCache);
            }
        } catch (SQLException sqle) {
            if (null != conn) {
                try {
                    logger.dbAction(function + ": database rollback");
                    conn.rollback();
                } catch (SQLException ignore) {
                }
            }
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } catch (Exception e) {
            throw new HandlerException(e);
        } finally {
            if (null != conn && changedAutoCommit) {
                try {
                    conn.setAutoCommit(oldAutoCommit);
                } catch (SQLException ignore) {
                }
            }
            close(rs);
            close(stmt);
            close(stmtDel);
        }
        return queryCount;
    }

    private int storeConcessionsDraftRecords(int location, long dataStart, long dataEnd, double offset, ArrayList<Integer> costCenterArray, Hashtable<String, Integer> xmlRecCount,
            Hashtable<String, Integer> productHash, Vector<Hashtable<String, Object>> records) throws HandlerException {

        PreparedStatement stmt              = null;
        PreparedStatement stmtDel           = null;
        ResultSet rs                        = null;
        int queryCount                      = 1;

        // A cache of product data delay sets (maps product -> datadelay hour)
        HashMap<Integer, ArrayList> dataDelayCache
                                            = new HashMap<Integer, ArrayList>();
        ArrayList<Integer> missingCostCenterArray
                                            = new ArrayList();
        try {

            Timestamp tsStart               = new Timestamp(dataStart * 1000L);
            Timestamp tsEnd                 = new Timestamp(dataEnd * 1000L);
            // add the new records
            // Insert the current values for a check
            String sql                      = "INSERT INTO sales (location, pluNumber, costCenter, quantity, date, checkId, reportRecordId)" +
                                            " VALUES (?,?,?,?,?,?,?)";
            int numInserted                 = 0, numIgnored = 0;
            for (Hashtable<String, Object> rec : records) {
                String checkId              = (String) rec.get("checkId");
                String id                   = (String) rec.get("id");
                int costCenter              = (Integer) rec.get("costCenter");
                double qty                  = ((Double) rec.get("qty")).doubleValue();
                Long epoch                  = (Long) rec.get("epoch");
                String recordId             = (String) rec.get("recordId");
                Timestamp tstamp            = new Timestamp(epoch * 1000L);
                
                if (recordId != null && (recordId.length() > 1)) {
                    // query to remove previous record entries
                    String sqlDel           = "DELETE FROM sales WHERE location=? "
                                            + " AND date > SUBDATE(?, INTERVAL 1 HOUR) AND reportRecordId=?";
                    stmt                    = conn.prepareStatement(sqlDel);
                    stmt.setInt(1, location);
                    stmt.setTimestamp(2, tstamp);
                    stmt.setString(3, recordId);
                    stmt.executeUpdate();
                    queryCount++;
                }
                
                int stmtIndex               = 1;
                stmt                        = conn.prepareStatement(sql);
                stmt.setInt(stmtIndex++, location);
                stmt.setString(stmtIndex++, id);
                stmt.setInt(stmtIndex++, costCenter);
                stmt.setDouble(stmtIndex++, qty);
                stmt.setTimestamp(stmtIndex++, tstamp);
                stmt.setString(stmtIndex++, checkId);
                stmt.setString(stmtIndex++, recordId);
                stmt.executeUpdate();
                if (productHash.size() > 0 && productHash.containsKey(id)) {
                    dataDelayCache          = checkDataDelay(productHash.get(id), offset, tstamp, dataDelayCache);
                }
                queryCount++;
                numInserted++;

                //Adding all the missing cost centers to be added to the cost center table
                if (!costCenterArray.contains(costCenter)){
                    missingCostCenterArray.add(costCenter);
                    costCenterArray.add(costCenter);
                }
            }

            if (tsEnd != null) {
                // record that a sales reading happened in the location table
                sql                         = " UPDATE location SET lastSold=GREATEST(lastSold,?) WHERE id=?";
                stmt                        = conn.prepareStatement(sql);
                stmt.setTimestamp(1, tsEnd);
                stmt.setInt(2, location);
                stmt.executeUpdate();
                queryCount++;
            }

            logger.debug("Ignored record IDs: " + numIgnored);
            logger.debug("Inserted records: " + numInserted);

            if (dataDelayCache.size() > 0) {
                logger.debug("Inserting Data Delays");
                setDataDelay(location, offset, dataDelayCache);
            }
        } catch (SQLException sqle) {
            if (null != conn) {
                try {
                    logger.dbAction(function + ": database rollback");
                    conn.rollback();
                } catch (SQLException ignore) {
                }
            }
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } catch (Exception e) {
            throw new HandlerException(e);
        } finally { 
            close(rs);
            close(stmt);
            close(stmtDel);
        }
        return queryCount;
    }

    private int storeUnknownRecords(int locationId, List unknownItemList) throws HandlerException {

        int queryCount                      = 0;
        PreparedStatement stmt              = null;
        ResultSet rs                        = null;
        ArrayList<String> plus              = new ArrayList<String>();
        HashMap<String, Double> hintOunces  = new HashMap<String, Double>();
        HashMap<String, Integer> hintType   = new HashMap<String, Integer>();
        Double ounces                       = 14.5;
        
        for (Object o : unknownItemList) {
            Element item                    = (Element) o;
            String item_id                  = HandlerUtils.getRequiredString(item, "id");
            String name                     = HandlerUtils.getOptionalString(item, "productName");
            int costCenter                  = HandlerUtils.getRequiredInteger(item, "costCenter");
            double quantity                 = HandlerUtils.getRequiredDouble(item, "qty");
            long epoch                      = HandlerUtils.getRequiredLong(item, "epoch");
            String checkId                  = HandlerUtils.getRequiredDigitString(item, "checkId");
            String reportRecordId           = HandlerUtils.getRequiredDigitString(item, "recordId");
            Timestamp tstamp                = new Timestamp(epoch * 1000L);

            if (name == null) { name = ""; }
            if (name.contains("Btl") || name.contains("Bottle") || name.contains("bottle")) { continue; }
            
            try {

                name                        = name.replace(".", "").trim();
                if (name.length() < 3) {
                    name                    = "Unknown Product";
                }
                stmt                        = conn.prepareStatement("SELECT id FROM posBeverageSize WHERE location = ?");
                stmt.setInt(1, locationId);
                rs                          = stmt.executeQuery();
                if (rs.next()) {
                    queryCount++;
                    int stmtIndex           = 1;
                    stmt                    = conn.prepareStatement("INSERT INTO sales (location, checkId, costcenter, reportRecordId, pluNumber, quantity, date) " +
                                            " VALUES (?, ?, ?, ?, ?, ?, ?);");
                    stmt.setInt(stmtIndex++, locationId);
                    stmt.setString(stmtIndex++, checkId);
                    stmt.setInt(stmtIndex++, costCenter);
                    stmt.setString(stmtIndex++, reportRecordId);
                    stmt.setString(stmtIndex++, item_id);
                    stmt.setDouble(stmtIndex++, quantity);
                    stmt.setTimestamp(stmtIndex++, tstamp);
                    stmt.executeUpdate();

                    stmt                    = conn.prepareStatement("SELECT complex, hint, type, ounces FROM posBeverageSize WHERE location = ?;");
                    stmt.setInt(1, locationId);
                    rs                      = stmt.executeQuery();
                    while(rs.next()) {
                        queryCount++;
                        if (rs.getInt(1) == 0) {
                            ounces          = rs.getDouble(4);
                        } else {
                            hintOunces.put(rs.getString(2), rs.getDouble(4));
                            hintType.put(rs.getString(2), rs.getInt(3));
                        }
                    }

                    if (!plus.contains(item_id)) {
                        plus                = addPLUProduct(locationId, name, item_id, ounces, hintOunces, hintType, plus);
                    }
                } else {
                    Timestamp tsStart       = new Timestamp((epoch - 60) * 1000L);
                    Timestamp tsEnd         = new Timestamp((epoch + 60) * 1000L);
                    String sql              = "SELECT sid FROM unknownSales WHERE location = ? AND pluNumber = ? AND reportRecordId = ? " +
                                            " AND date BETWEEN ? AND ? GROUP BY reportRecordId";
                    stmt                    = conn.prepareStatement(sql);
                    stmt.setInt(1, locationId);
                    stmt.setString(2, item_id);
                    stmt.setString(3, reportRecordId);
                    stmt.setTimestamp(4, tsStart);
                    stmt.setTimestamp(5, tsEnd);
                    rs                      = stmt.executeQuery();
                    if (!rs.next()) {
                        queryCount++;
                        sql                 = "INSERT INTO unknownSales (location, pluNumber, productName, costCenter, quantity, date, checkId, reportRecordId)" +
                                            " VALUES (?,?,?,?,?,?,?,?)";
                        int stmtIndex       = 1;
                        stmt                = conn.prepareStatement(sql);
                        stmt.setInt(stmtIndex++, locationId);
                        stmt.setString(stmtIndex++, item_id);
                        stmt.setString(stmtIndex++, name);
                        stmt.setInt(stmtIndex++, costCenter);
                        stmt.setDouble(stmtIndex++, quantity);
                        stmt.setTimestamp(stmtIndex++, tstamp);
                        stmt.setString(stmtIndex++, checkId);
                        stmt.setString(stmtIndex++, reportRecordId);
                        stmt.executeUpdate();
                    }
                }
            } catch (SQLException sqle) {
                if (null != conn) {
                    try {
                        logger.dbAction(function + ": database rollback");
                        conn.rollback();
                    } catch (SQLException ignore) {
                    }
                }
                logger.dbError("Database error: " + sqle.getMessage());
                throw new HandlerException(sqle);
            } catch (Exception e) {
                throw new HandlerException(e);
            } finally {
                close(rs);
                close(stmt);
            }
        }
        return queryCount;
    }

    private void storeOpenCheckData(Element toHandle, Element toAppend, SecureSession ss) throws HandlerException {

        int location                        = HandlerUtils.getRequiredInteger(toHandle, "locationId");
        boolean storeAll                    = HandlerUtils.getOptionalBoolean(toHandle, "storeAll");


        if (location != ss.getLocation()) {
            logger.readingAccessViolation("Called " + function + " as L#" + location + ", but authenticated as L#" + ss.getLocation());
            throw new HandlerException("Access Violation");
        }
        
        ArrayList<Integer> costCenterArray  = new ArrayList();
        ResultSet rs                        = null;
        PreparedStatement stmt              = null;
        String sql                          = null;
        int checks                          = 0;
        double offset                       = 0.00;
        int locationType                    = 0;
        int posAutomation                   = 0;
        
        try {
            sql                             = "SELECT l.easternOffset, l.type, lD.posAutomation, IF(l.processorName = 'PixelPointProcessor', 1, IF(l.processorName = 'VolanteProcessor', 1, 0)) FROM location l LEFT JOIN locationDetails lD ON lD.location = l.id WHERE l.id = ? ";
            stmt                            = conn.prepareStatement(sql);
            stmt.setInt(1, location);
            rs                              = stmt.executeQuery();
            if (rs.next()) {
                offset                      = rs.getDouble(1);
                locationType                = rs.getInt(2);
                posAutomation               = rs.getInt(3);
                checks                      = rs.getInt(4);
            }
        } catch (Exception e) {
            throw new HandlerException(e);
        } finally {
            close(rs);
            close(stmt);
        }

        boolean success                     = false;
        int queryCount                      = 0;

        List itemList                       = toHandle.elements("item");
        if (null != itemList && itemList.size() > 0) {

            Hashtable<String, Boolean> idHash
                                            = new Hashtable<String, Boolean>();
            Hashtable<String, Integer> productHash
                                            = new Hashtable<String, Integer>();
            Hashtable<String, String> productPLUHash
                                            = new Hashtable<String, String>();
            Hashtable<String, Integer> xmlRecCount
                                            = new Hashtable<String, Integer>();
            Vector<Hashtable<String, Object>> records
                                            = new Vector<Hashtable<String, Object>>();

            try {
                if (!storeAll) {
                    sql                     = "SELECT plu FROM beverage where location = ?";
                    stmt                    = conn.prepareStatement(sql);
                    stmt.setInt(1, location);
                    rs                      = stmt.executeQuery();
                    queryCount++;
                    while (rs.next()) {
                        idHash.put(rs.getString(1), Boolean.TRUE);
                    }
                    close(rs);
                }


                sql                         = "SELECT c.ccID FROM costCenter c WHERE c.location = ? ";
                stmt                        = conn.prepareStatement(sql);
                stmt.setInt(1, location);
                rs                          = stmt.executeQuery();
                while (rs.next()) {
                    costCenterArray.add(rs.getInt(1));
                }

                sql                         = "SELECT b.plu, i.product, GROUP_CONCAT(CONCAT(i.product, '-', i.ounces)) FROM beverage b "
                                            + " left join ingredient i on i.beverage = b.id WHERE b.location = ? GROUP BY b.id ORDER BY b.plu ;";
                stmt                        = conn.prepareStatement(sql);
                stmt.setInt(1, location);
                rs                          = stmt.executeQuery();
                while (rs.next()) {
                    productHash.put(rs.getString(1), rs.getInt(2));
                    productPLUHash.put(rs.getString(1), rs.getString(3));
                }
                close(rs);

                String qtyStr;
                String costCenterStr;
                long dataStart              = -1, dataEnd = -1;
                for (Object o : itemList) {
                    Element item            = (Element) o;
                    String id               = HandlerUtils.getRequiredString(item, "id");
                    if (storeAll || idHash.containsKey(id)) {
                        // create a new hash record
                        Hashtable<String, Object> record
                                            = new Hashtable<String, Object>();
                        record.put("id", id);
                        costCenterStr       = HandlerUtils.getRequiredString(item, "costCenter");
                        record.put("costCenter", new Integer(costCenterStr));
                        qtyStr              = HandlerUtils.getRequiredString(item, "qty");
                        qtyStr              = qtyStr.trim();
                        if (qtyStr.contains("/"))
                        {
                            String[] parts  = qtyStr.split("/");
                            if (parts.length == 2) {
                                double splitValue
                                            = Double.valueOf(parts[0]) / Double.valueOf(parts[1]);
                                qtyStr      = String.valueOf(splitValue);
                            } else {
                                qtyStr      = "0.5";
                            }
                        }
                        record.put("qty", new Double(qtyStr));
                        long epoch          = HandlerUtils.getRequiredLong(item, "epoch");
                        record.put("epoch", epoch);
                        String checkId      = HandlerUtils.getRequiredDigitString(item, "checkId");
                        record.put("checkId", checkId);
                        String recordId     = HandlerUtils.getRequiredDigitString(item, "recordId");
                        record.put("recordId", recordId);
                        records.add(record);
                        if (epoch < dataStart || dataStart < 0) {
                            dataStart = epoch;
                        }
                        if (epoch > dataEnd || dataEnd < 0) {
                            dataEnd = epoch;
                        }

                        int countInt        = 1;
                        if (location == 957 || location == 964) {
                            Double qty      = new Double(qtyStr);
                            countInt        = qty.intValue();
                            checkId         = costCenterStr + ":" + checkId;
                        }

                        // keep track of each record ID count in the xml
                        Integer countObj    = xmlRecCount.get(checkId);
                        if (null != countObj) {
                            countInt        = countInt + countObj.intValue();
                        }
                        xmlRecCount.put(checkId, new Integer(countInt));
                    }
                }
                if (records.size() > 0) {
                    if (location == 300) {
                        queryCount          += storeConcessionsDraftRecords(location, dataStart, dataEnd, offset, costCenterArray, xmlRecCount, productHash, records);
                    } else if (location == 957 || location == 964) {
                        queryCount          += storeDoubleGatewayDraftRecords(location, dataStart, dataEnd, offset, costCenterArray, xmlRecCount, productHash, productPLUHash, records);
                    } else if (checks == 1) {
                        queryCount          += storeDraftChecks(location, dataStart, dataEnd, offset, costCenterArray, xmlRecCount, productHash, productPLUHash, records);
                    } else {
                        queryCount          += storeDraftRecords(location, dataStart, dataEnd, offset, costCenterArray, xmlRecCount, productHash, productPLUHash, records);
                    }
                    success                 = true;
                }
                logger.dbAction("Committed " + queryCount + " queries");
            } catch (SQLException sqle) {
                if (null != conn) {
                    try {
                        logger.dbAction(function + ": database rollback");
                        conn.rollback();
                    } catch (SQLException ignore) {
                    }
                }
                logger.dbError("Database error: " + sqle.getMessage());
                throw new HandlerException(sqle);
            } catch (Exception e) {
                throw new HandlerException(e);
            } finally {
                close(rs);
                close(stmt);
            }
        }

        List unknownItemList                = toHandle.elements("unknownItem");
        if (posAutomation > 0 && null != unknownItemList && unknownItemList.size() > 0) {
            storeUnknownItemList(location, unknownItemList);
        }

        toAppend.addElement("success").addText(success ? "true" : "false");
    }

    private void storeAramarkCheckData(Element toHandle, Element toAppend, SecureSession ss) throws HandlerException {

        int location                        = HandlerUtils.getRequiredInteger(toHandle, "locationId");
        boolean storeAll                    = HandlerUtils.getOptionalBoolean(toHandle, "storeAll");

        ArrayList<Integer> costCenterArray  = new ArrayList();

        if (location != ss.getLocation()) {
            logger.readingAccessViolation("Called " + function + " as L#" + location + ", but authenticated as L#" + ss.getLocation());
            throw new HandlerException("Access Violation");
        }

        boolean success                     = false;
        double offset                       = 0.00;
        int queryCount                      = 0;
        List unknownItemList                = toHandle.elements("unknownItem");
        if (null != unknownItemList && unknownItemList.size() > 0) {
            queryCount                      += storeUnknownRecords(location, unknownItemList);
        }
        
        List itemList                       = toHandle.elements("item");
        if (null != itemList && itemList.size() > 0) {
            PreparedStatement stmt          = null;
            ResultSet rs                    = null;
            int locationType                = 0;
            String sql                      = null;

            Hashtable<String, Boolean> idHash
                                            = new Hashtable<String, Boolean>();
            Hashtable<String, Integer> productHash
                                            = new Hashtable<String, Integer>();
            Hashtable<String, Integer> xmlRecCount
                                            = new Hashtable<String, Integer>();
            Vector<Hashtable<String, Object>> records
                                            = new Vector<Hashtable<String, Object>>();

            try {
                if (!storeAll) {
                    sql                     = "SELECT plu FROM beverage where location = ?";
                    stmt                    = conn.prepareStatement(sql);
                    stmt.setInt(1, location);
                    rs                      = stmt.executeQuery();
                    queryCount++;
                    while (rs.next()) {
                        idHash.put(rs.getString(1), Boolean.TRUE);
                    }
                    close(rs);
                }

                sql                         = "SELECT l.easternOffset, l.type FROM location l WHERE l.id = ? ";
                stmt                        = conn.prepareStatement(sql);
                stmt.setInt(1, location);
                rs                          = stmt.executeQuery();
                if (rs.next()) {
                    offset                  = rs.getDouble(1);
                    locationType            = rs.getInt(2);
                }

                sql                         = "SELECT c.ccID FROM costCenter c WHERE c.location = ? ";
                stmt                        = conn.prepareStatement(sql);
                stmt.setInt(1, location);
                rs                          = stmt.executeQuery();
                while (rs.next()) {
                    costCenterArray.add(rs.getInt(1));
                }

                sql                         = "SELECT b.plu, i.product FROM beverage b left join ingredient i on i.beverage = b.id WHERE b.location = ? ORDER BY b.plu ;";
                stmt                        = conn.prepareStatement(sql);
                stmt.setInt(1, location);
                rs                          = stmt.executeQuery();
                while (rs.next()) {
                    productHash.put(rs.getString(1), rs.getInt(2));
                }
                close(rs);

                String qtyStr;
                String costCenterStr;
                long dataStart              = -1, dataEnd = -1;
                for (Object o : itemList) {
                    Element item = (Element) o;
                    String id               = HandlerUtils.getRequiredString(item, "id");
                    if (storeAll || idHash.containsKey(id)) {
                        // create a new hash record
                        Hashtable<String, Object> record
                                            = new Hashtable<String, Object>();
                        record.put("id", id);
                        costCenterStr       = HandlerUtils.getRequiredString(item, "costCenter");
                        record.put("costCenter", new Integer(costCenterStr));
                        qtyStr              = HandlerUtils.getRequiredString(item, "qty");
                        record.put("qty", new Double(qtyStr));
                        long epoch          = HandlerUtils.getRequiredLong(item, "epoch");
                        epoch               = epoch - 14400;
                        record.put("epoch", epoch);
                        String checkId      = HandlerUtils.getRequiredDigitString(item, "checkId");
                        record.put("checkId", checkId);
                        String recordId     = HandlerUtils.getRequiredDigitString(item, "recordId");
                        record.put("recordId", recordId);
                        records.add(record);
                        if (epoch < dataStart || dataStart < 0) {
                            dataStart       = epoch;
                        }
                        if (epoch > dataEnd || dataEnd < 0) {
                            dataEnd         = epoch;
                        }
                        // keep track of each record ID count in the xml
                        Integer countObj    = xmlRecCount.get(checkId);
                        int countInt        = 0;
                        if (null != countObj) {
                            countInt        = countObj.intValue();
                        }
                        xmlRecCount.put(checkId, new Integer(++countInt));
                    }
                }
                if (records.size() > 0) {
                    queryCount              += storeConcessionsDraftRecords(location, dataStart, dataEnd, offset, costCenterArray, xmlRecCount, productHash, records);
                    success                 = true;
                }
                logger.dbAction("Committed " + queryCount + " queries");
            } catch (SQLException sqle) {
                if (null != conn) {
                    try {
                        logger.dbAction(function + ": database rollback");
                        conn.rollback();
                    } catch (SQLException ignore) {
                    }
                }
                logger.dbError("Database error: " + sqle.getMessage());
                throw new HandlerException(sqle);
            } catch (Exception e) {
                throw new HandlerException(e);
            } finally {
                close(rs);
                close(stmt);
            }
        }

        toAppend.addElement("success").addText(success ? "true" : "false");
    }

    // selecting the first bar that was created for the retail location
    private void storeRetailerMissingCostCenter(ArrayList<Integer> missingCostCenterArray, int location) throws HandlerException {

        String sql = "SELECT b.id FROM bar b WHERE b.location = ? ORDER BY b.id LIMIT 1 ";
        PreparedStatement stmt = null;
        ResultSet rs = null;
        int barId = 0;
        try {
            int arrayLength = missingCostCenterArray.size();
            if (arrayLength > 0) {
                stmt = conn.prepareStatement(sql);
                stmt.setInt(1, location);
                rs = stmt.executeQuery();
                if (rs.next()) {
                    barId = rs.getInt(1);
                }
                // adding all the missing costcenters
                for (int i = 0; i < arrayLength; i++) {
                    sql = " INSERT INTO costCenter (name, ccID, location, bar) VALUES (?,?,?,?) ";
                    stmt = conn.prepareStatement(sql);
                    stmt.setString(1, "Cost Center " + missingCostCenterArray.get(i));
                    stmt.setInt(2, missingCostCenterArray.get(i));
                    stmt.setInt(3, location);
                    stmt.setInt(4, barId);
                    stmt.executeUpdate();
                }
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error in storeRetailerMissingCostCenter: " + sqle.getMessage());
        } finally {
            close(stmt);
            close(rs);
        }
    }
    private void storePOSitouchCheck(Element toHandle, Element toAppend, SecureSession ss) throws HandlerException {
        int location = HandlerUtils.getRequiredInteger(toHandle, "locationId");

        logger.readingAction(function + "#" + ss.getLocation() + "-" + ss.getClientId());
        if (location != ss.getLocation()) {
            logger.readingAccessViolation("Called " + function + " as L#" + location +
                    ", but authenticated as L#" + ss.getLocation());
            throw new HandlerException("Access Violation");
        }

        boolean success = storeStandardCheck(toHandle, location);
        toAppend.addElement("success").addText(success ? "true" : "false");
    }

    private void storeMicrosMultiBarData(Element toHandle, Element toAppend, SecureSession ss) throws HandlerException {

        int location                        = HandlerUtils.getRequiredInteger(toHandle, "locationId");
        long epoch                          = HandlerUtils.getRequiredLong(toHandle, "epoch");
        long reportStart                    = HandlerUtils.getRequiredLong(toHandle, "reportStart");
        long reportEnd                      = HandlerUtils.getRequiredLong(toHandle, "reportEnd");
        boolean storeAll                    = HandlerUtils.getOptionalBoolean(toHandle, "storeAll");

        if (location != ss.getLocation()) {
            logger.readingAccessViolation("Called " + function + " as L#" + location + ", but authenticated as L#" + ss.getLocation());
            throw new HandlerException("Access Violation");
        }

        boolean success                     = false;
        int stmtIndex                       = 1;

        List itemList                       = toHandle.elements("item");
        if (null != itemList && itemList.size() > 0) {
            String id;
            double qty, offset              = 0.00;
            int cCenter;
            Hashtable<String, ItemTuple> dbSums
                                            = new Hashtable<String, ItemTuple>();
            Hashtable<String, Boolean> idHash
                                            = new Hashtable<String, Boolean>();
            Hashtable<String, Integer> productHash
                                            = new Hashtable<String, Integer>();
            // A cache of product data delay sets (maps product -> datadelay hour)
            HashMap<Integer, ArrayList> dataDelayCache
                                            = new HashMap<Integer, ArrayList>();
            ArrayList<Integer> costCenterArray
                                            = new ArrayList();
            ArrayList<Integer> missingCostCenterArray
                                            = new ArrayList();
            int locationType                = 1, zoneId = 0, barId = 0, stationId = 0;

            PreparedStatement stmt          = null;
            ResultSet rs                    = null;
            int queryCount                  = 0;

            String sql                      = null;
            boolean oldAutoCommit           = true;
            boolean changedAutoCommit       = false;

            try {
                Timestamp tsEpoch           = new Timestamp(epoch * 1000L);
                Timestamp tsStart           = new Timestamp(reportStart * 1000L);
                Timestamp tsEnd             = new Timestamp(reportEnd * 1000L);
                oldAutoCommit               = conn.getAutoCommit();

                sql                         = "SELECT l.easternOffset, l.type FROM location l WHERE l.id = ? ";
                stmt                        = conn.prepareStatement(sql);
                stmt.setInt(1, location);
                rs                          = stmt.executeQuery();
                if (rs.next()) {
                    offset                  = rs.getDouble(1);
                    locationType            = rs.getInt(2);
                }

                sql                         = "SELECT c.ccID FROM costCenter c WHERE c.location = ? ";
                stmt                        = conn.prepareStatement(sql);
                stmt.setInt(1, location);
                rs                          = stmt.executeQuery();
                while (rs.next()) {
                    costCenterArray.add(rs.getInt(1));
                }
                
                if (!storeAll) {
                    sql                     = "SELECT b.plu, i.product FROM beverage b LEFT JOIN ingredient i ON i.beverage = b.id WHERE b.location = ?;";
                    stmt                    = conn.prepareStatement(sql);
                    stmt.setInt(1, location);
                    rs                      = stmt.executeQuery();
                    queryCount++;
                    while (rs.next()) {
                        idHash.put(rs.getString(1), Boolean.TRUE);
                        productHash.put(rs.getString(1), rs.getInt(2));
                    }
                    close(rs);
                }

                // get all of the current item sums for the day
                sql                         = "SELECT pluNumber, costCenter, SUM(quantity), MAX(date) FROM sales WHERE location = ? AND date >= ?" +
                                            " AND date <= ? GROUP BY pluNumber, costCenter";
                stmt                        = conn.prepareStatement(sql);
                stmt.setInt(1, location);
                stmt.setTimestamp(2, tsStart);
                stmt.setTimestamp(3, tsEnd);
                rs                          = stmt.executeQuery();
                queryCount++;
                int itemCount               = 0;
                while (rs.next()) {
                    String idStr            = rs.getString(1);
                    int costCenter          = rs.getInt(2);
                    double qtySum           = rs.getDouble(3);
                    Timestamp tsItem        = rs.getTimestamp(4);
                    long itemEpoch          = tsStart.getTime();
                    if (null != tsItem) {
                        itemEpoch           = tsItem.getTime();
                    }
                    
                    ItemTuple tuple         = new ItemTuple(idStr, costCenter, qtySum, itemEpoch);
                    String ccAndId          = String.valueOf(costCenter) + idStr;
                    dbSums.put(ccAndId, tuple);
                    itemCount++;
                }
                close(rs);
                //logger.debug("Found " + itemCount + " item sums.");

                // Insert the current values for a check
                sql                         = "INSERT INTO sales (location, pluNumber, costCenter, quantity, date) values (?,?,?,?,?)";
                changedAutoCommit           = true;
                conn.setAutoCommit(false);

                // logging counts
                int numItems                = 0, numAdded = 0, numChanged = 0, numUnchanged = 0;
                String qtyStr;
                Timestamp tstamp            = null;
                Hashtable<String, ItemTuple> idQty 
                                            = new Hashtable<String, ItemTuple>();
                
                for (Object o : itemList) {
                    Element item            = (Element) o;
                    
                    id                      = HandlerUtils.getRequiredString(item, "id");
                    String[] idParts        = id.split("-");
                    if (2 == idParts.length && !idHash.containsKey(id)) {
                        id                  = idParts[0];
                    }

                    String costCenterString = HandlerUtils.getRequiredString(item, "costCenter");
                    int costCenter          = 1;
                    try {
                        costCenter          = Integer.parseInt(costCenterString);
                    } catch (NumberFormatException e) {
                        costCenter          = 1;
                    }

                    String ccAndId          = String.valueOf(costCenter) + id;
                    ItemTuple idQtyTuple    = idQty.get(ccAndId);
                    
                    Double dQty             = new Double(0.0D);
                    if (null != idQtyTuple) {
                        dQty                = idQtyTuple.getQty();
                    }
                    qtyStr                  = HandlerUtils.getRequiredString(item, "qty");
                    if (qtyStr.compareTo("''") == 0) {
                        qtyStr              = "0";
                    }
                    dQty                    = new Double(dQty.doubleValue() + Double.parseDouble(qtyStr));

                    long itemEpoch          = HandlerUtils.getOptionalLong(item, "epoch");

                    if (itemEpoch <= 0L) {
                        itemEpoch           = epoch;
                    }
                    ItemTuple idCostCenterTuple
                                            = new ItemTuple(id, costCenter, dQty, itemEpoch);
                    idQty.put(ccAndId, idCostCenterTuple);
                }

                for (String strid : idQty.keySet()) {
                    Timestamp itemTimestamp = new Timestamp(reportStart * 1000L);
                    numItems++;

                    ItemTuple dbCostCenterTuple
                                            = idQty.get(strid);
                    ItemTuple dbTuple       = dbSums.get(strid);


                    if (null != dbCostCenterTuple) {
                        qty                 = dbCostCenterTuple.getQty();
                        cCenter             = dbCostCenterTuple.getCostCenter();
                        strid               = dbCostCenterTuple.getId();
                        itemTimestamp       = new Timestamp(dbCostCenterTuple.getEpoch() * 1000L);
                    } else {
                        qty                 = new Double(0.0D);
                        cCenter             = 0;
                        strid               = "1";
                        itemTimestamp       = new Timestamp(reportStart * 1000L);
                    }

                    if (storeAll || idHash.containsKey(strid)) {

                        double qtyDiff      = qty;
                        long oldEpoch       = 0L;
                        boolean update      = false;

                        if (null != dbTuple) {
                            qtyDiff         = qty - dbTuple.getQty();
                            oldEpoch        = dbTuple.getEpoch();
                            if (tsEpoch.getTime() >= oldEpoch && Math.abs(qtyDiff) >= 0.01D) {
                                update      = true;
                                numChanged++;
                            } else {
                                numUnchanged++;
                            }
                        } else {
                            update          = true;
                            numAdded++;
                        }

                        if (update) {
                            stmtIndex       = 1;
                            stmt            = conn.prepareStatement(sql);
                            stmt.setInt(stmtIndex++, location);
                            stmt.setString(stmtIndex++, strid);
                            stmt.setInt(stmtIndex++, cCenter);
                            stmt.setDouble(stmtIndex++, qtyDiff);
                            stmt.setTimestamp(stmtIndex++, tsEpoch);
                            stmt.executeUpdate();
                            if (productHash.size() > 0 && productHash.containsKey(strid)) {
                                dataDelayCache
                                            = checkDataDelay(productHash.get(strid), offset, itemTimestamp, dataDelayCache);
                            }
                            queryCount++;
                            //Adding all the missing cost centers to be added to the cost center table
                            if (!costCenterArray.contains(cCenter)){
                                missingCostCenterArray.add(cCenter);
                            }
                        }
                    }
                }
                
                // selecting the first bar that was created for the retail location
                if (locationType == 1) {
                    storeRetailerMissingCostCenter(missingCostCenterArray, location);
                }

                // record that a sales reading happened in the location table
                if (tstamp != null) {
                    sql                     = " UPDATE location SET lastSold=GREATEST(lastSold,?) WHERE id=?";
                    stmt                    = conn.prepareStatement(sql);
                    stmt.setTimestamp(1, tstamp);
                    stmt.setInt(2, location);
                    stmt.executeUpdate();
                    queryCount++;
                }

                conn.commit();
                //logger.dbAction("Committed " + queryCount + " queries");
                //logger.debug("Total items: " + numItems);
                //logger.debug("      Added: " + numAdded);
                //logger.debug("    Changed: " + numChanged);
                //logger.debug("  Unchanged: " + numUnchanged);

                if (dataDelayCache.size() > 0) {
                    //logger.debug("Inserting Data Delays");
                    setDataDelay(location, offset, dataDelayCache);
                }

                success                         = true;
            } catch (SQLException sqle) {
                if (null != conn) {
                    try {
                        logger.dbAction(function + ": database rollback");
                        conn.rollback();
                    } catch (SQLException ignore) {
                    }
                }
                logger.dbError("Database error: " + sqle.getMessage());
                throw new HandlerException(sqle);
            } catch (Exception e) {
                throw new HandlerException(e);
            } finally {
                if (null != conn && changedAutoCommit) {
                    try {
                        conn.setAutoCommit(oldAutoCommit);
                    } catch (SQLException ignore) {
                    }
                }
                close(rs);
                close(stmt);
            }
        }
        toAppend.addElement("success").addText(success ? "true" : "false");
    }

    private void storeMicros9700MultiBarData(Element toHandle, Element toAppend, SecureSession ss)
            throws HandlerException {

        int location = HandlerUtils.getRequiredInteger(toHandle, "locationId");
        long reportStart = HandlerUtils.getRequiredLong(toHandle, "reportStart");
        long reportEnd = HandlerUtils.getRequiredLong(toHandle, "reportEnd");
        boolean storeAll = HandlerUtils.getOptionalBoolean(toHandle, "storeAll");

        if (location != ss.getLocation()) {
            logger.readingAccessViolation("Called " + function + " as L#" + location +
                    ", but authenticated as L#" + ss.getLocation());
            throw new HandlerException("Access Violation");
        }

        boolean success = false;
        int stmtIndex = 1;

        List itemList = toHandle.elements("item");
        if (null != itemList && itemList.size() > 0) {
            String id;
            String checkId = "123";
            double qty;
            int cCenter;
            Timestamp tsEpoch;

            Hashtable<String, ItemTuple> dbSums = new Hashtable<String, ItemTuple>();
            Hashtable<String, Boolean> idHash = new Hashtable<String, Boolean>();

            PreparedStatement stmt = null;
            ResultSet rs = null;
            int queryCount = 0;

            String sql = null;
            boolean oldAutoCommit = true;
            boolean changedAutoCommit = false;

            try {
                Timestamp tsStart = new Timestamp(reportStart * 1000L);
                Timestamp tsEnd = new Timestamp(reportEnd * 1000L);
                tsEpoch = new Timestamp(reportStart * 1000L);
                oldAutoCommit = conn.getAutoCommit();

                if (!storeAll) {
                    sql = "SELECT plu FROM beverage where location = ?";
                    stmt = conn.prepareStatement(sql);
                    stmt.setInt(1, location);
                    rs = stmt.executeQuery();
                    queryCount++;
                    while (rs.next()) {
                        idHash.put(rs.getString(1), Boolean.TRUE);
                    }
                    close(rs);
                }

                // get all of the current item sums for the day
                sql = "SELECT pluNumber, costCenter, SUM(quantity), MAX(date)" +
                        " FROM sales WHERE location = ? AND date >= ?" +
                        " AND date <= ? GROUP BY pluNumber, costCenter";
                stmt = conn.prepareStatement(sql);
                stmt.setInt(1, location);
                stmt.setTimestamp(2, tsStart);
                stmt.setTimestamp(3, tsEnd);
                rs = stmt.executeQuery();
                queryCount++;
                int itemCount = 0;
                while (rs.next()) {
                    String idStr = rs.getString(1);
                    int costCenter = rs.getInt(2);
                    double qtySum = rs.getDouble(3);
                    Timestamp tsItem = rs.getTimestamp(4);
                    long itemEpoch = tsStart.getTime();
                    if (null != tsItem) {
                        itemEpoch = tsItem.getTime();
                    }
                    ItemTuple tuple = new ItemTuple(idStr, costCenter, qtySum, itemEpoch);
                    String ccAndId = String.valueOf(costCenter) + idStr;
                    dbSums.put(ccAndId, tuple);
                    itemCount++;
                }
                close(rs);
                logger.debug("Found " + itemCount + " item sums.");

                // Insert the current values for a check
                sql = "INSERT INTO sales (location, pluNumber, costCenter, quantity, date, checkId)" +
                        " values (?,?,?,?,?,?)";
                changedAutoCommit = true;
                conn.setAutoCommit(false);

                // logging counts
                int numItems = 0;
                int numAdded = 0;
                int numChanged = 0;
                int numUnchanged = 0;

                String qtyStr;
                Timestamp tstamp = null;
                Hashtable<String, ItemTuple> idQty =
                        new Hashtable<String, ItemTuple>();

                for (Object o : itemList) {
                    Element item = (Element) o;
                    id = HandlerUtils.getRequiredString(item, "id");
                    String[] idParts = id.split("-");
                    if (2 == idParts.length && !idHash.containsKey(id)) {
                        id = idParts[0];
                    }

                    long epoch = HandlerUtils.getRequiredLong(item, "epoch");
                    tsEpoch = new Timestamp(epoch * 1000L);

                    int costCenter = HandlerUtils.getRequiredInteger(item, "costCenter");

                    String ccAndId = String.valueOf(costCenter) + id;

                    checkId = HandlerUtils.getRequiredString(item, "checkId");

                    ItemTuple idQtyTuple = idQty.get(ccAndId);
                    Double dQty;
                    if (null != idQtyTuple) {
                        dQty = idQtyTuple.getQty();
                    } else {
                        dQty = new Double(0.0D);
                    }

                    if (null == dQty) {
                        dQty = new Double(0.0D);
                    }
                    qtyStr = HandlerUtils.getRequiredString(item, "qty");
                    dQty = new Double(dQty.doubleValue() +
                            Double.parseDouble(qtyStr));

                    ItemTuple idCostCenterTuple = new ItemTuple(id, costCenter, dQty);

                    idQty.put(ccAndId, idCostCenterTuple);

                }

                for (String strid : idQty.keySet()) {
                    numItems++;

                    ItemTuple dbCostCenterTuple = idQty.get(strid);
                    ItemTuple dbTuple = dbSums.get(strid);

                    if (null != dbCostCenterTuple) {
                        qty = dbCostCenterTuple.getQty();
                        cCenter = dbCostCenterTuple.getCostCenter();
                        strid = dbCostCenterTuple.getId();
                    } else {
                        qty = new Double(0.0D);
                        cCenter = 0;
                        strid = "1";
                    }

                    if (storeAll || idHash.containsKey(strid)) {

                        double qtyDiff = qty;
                        long oldEpoch = 0L;
                        boolean update = false;

                        if (null != dbTuple) {
                            qtyDiff = qty - dbTuple.getQty();
                            oldEpoch = dbTuple.getEpoch();
                            if (tsEpoch.getTime() >= oldEpoch && Math.abs(qtyDiff) >= 0.01D) {
                                update = true;
                                numChanged++;
                            } else {
                                numUnchanged++;
                            }
                        } else {
                            update = true;
                            numAdded++;
                        }

                        if (update) {
                            stmtIndex = 1;
                            stmt = conn.prepareStatement(sql);
                            stmt.setInt(stmtIndex++, location);
                            stmt.setString(stmtIndex++, strid);
                            stmt.setInt(stmtIndex++, cCenter);
                            stmt.setDouble(stmtIndex++, qtyDiff);
                            stmt.setTimestamp(stmtIndex++, tsEpoch);
                            stmt.setString(stmtIndex++, checkId);
                            stmt.executeUpdate();
                            queryCount++;
                        }

                    }
                }

                // record that a sales reading happened in the location table
                if (tstamp != null) {
                    sql = " UPDATE location SET lastSold=GREATEST(lastSold,?) WHERE id=?";
                    stmt = conn.prepareStatement(sql);
                    stmt.setTimestamp(1, tstamp);
                    stmt.setInt(2, location);
                    stmt.executeUpdate();
                    queryCount++;
                }

                conn.commit();
                logger.dbAction("Committed " + queryCount + " queries");

                logger.debug("Total items: " + numItems);
                logger.debug("      Added: " + numAdded);
                logger.debug("    Changed: " + numChanged);
                logger.debug("  Unchanged: " + numUnchanged);

                success = true;

            } catch (SQLException sqle) {
                if (null != conn) {
                    try {
                        logger.dbAction(function + ": database rollback");
                        conn.rollback();
                    } catch (SQLException ignore) {
                    }
                }
                logger.dbError("Database error: " + sqle.getMessage());
                throw new HandlerException(sqle);
            } catch (Exception e) {
                throw new HandlerException(e);
            } finally {
                if (null != conn && changedAutoCommit) {
                    try {
                        conn.setAutoCommit(oldAutoCommit);
                    } catch (SQLException ignore) {
                    }
                }

                close(rs);
                close(stmt);
            }
        }
        toAppend.addElement("success").addText(success ? "true" : "false");
    }

    private void storeMicrosData(Element toHandle, Element toAppend, SecureSession ss)
            throws HandlerException {

        int location = HandlerUtils.getRequiredInteger(toHandle, "locationId");
        long epoch = HandlerUtils.getRequiredLong(toHandle, "epoch");
        long reportStart = HandlerUtils.getRequiredLong(toHandle, "reportStart");
        long reportEnd = HandlerUtils.getRequiredLong(toHandle, "reportEnd");
        boolean storeAll = HandlerUtils.getOptionalBoolean(toHandle, "storeAll");

        if (location != ss.getLocation()) {
            logger.readingAccessViolation("Called " + function + " as L#" + location +
                    ", but authenticated as L#" + ss.getLocation());
            throw new HandlerException("Access Violation");
        }

        boolean success = false;
        int stmtIndex = 1;

        List itemList = toHandle.elements("item");
        if (null != itemList && itemList.size() > 0) {
            String id;
            double qty, offset = 0.00;

            Hashtable<String, ItemTuple> dbSums = new Hashtable<String, ItemTuple>();
            Hashtable<String, Boolean> idHash = new Hashtable<String, Boolean>();
            Hashtable<String, Integer> productHash = new Hashtable<String, Integer>();
            // A cache of product data delay sets (maps product -> datadelay hour)
            HashMap<Integer, ArrayList> dataDelayCache = new HashMap<Integer, ArrayList>();

            PreparedStatement stmt = null;
            ResultSet rs = null;
            int queryCount = 0;

            String sql = null;
            boolean oldAutoCommit = true;
            boolean changedAutoCommit = false;

            try {
                Timestamp tsEpoch = new Timestamp(epoch * 1000L);
                Timestamp tsStart = new Timestamp(reportStart * 1000L);
                Timestamp tsEnd = new Timestamp(reportEnd * 1000L);
                oldAutoCommit = conn.getAutoCommit();

                sql = "SELECT l.easternOffset, l.lastSold FROM location l WHERE l.id = ? ";
                stmt = conn.prepareStatement(sql);
                stmt.setInt(1, location);
                rs = stmt.executeQuery();
                if (rs.next()) {
                    offset = rs.getDouble(1);
                }

                if (!storeAll) {
                    sql = "SELECT b.plu, i.product FROM beverage b LEFT JOIN ingredient i ON i.beverage = b.id WHERE b.location = ?;";
                    stmt = conn.prepareStatement(sql);
                    stmt.setInt(1, location);
                    rs = stmt.executeQuery();
                    queryCount++;
                    while (rs.next()) {
                        idHash.put(rs.getString(1), Boolean.TRUE);
                        productHash.put(rs.getString(1), rs.getInt(2));
                    }
                    close(rs);
                }

                // get all of the current item sums for the day
                sql = "SELECT pluNumber, SUM(quantity), MAX(date)" +
                        " FROM sales WHERE location = ? AND date >= ?" +
                        " AND date <= ? GROUP BY pluNumber";
                stmt = conn.prepareStatement(sql);
                stmt.setInt(1, location);
                stmt.setTimestamp(2, tsStart);
                stmt.setTimestamp(3, tsEnd);
                rs = stmt.executeQuery();
                queryCount++;
                Hashtable<Long, Boolean> recordQtyHash = null;
                int itemCount = 0;
                while (rs.next()) {
                    String idStr = rs.getString(1);
                    double qtySum = rs.getDouble(2);
                    Timestamp tsItem = rs.getTimestamp(3);
                    long itemEpoch = tsStart.getTime();
                    if (null != tsItem) {
                        itemEpoch = tsItem.getTime();
                    }
                    ItemTuple tuple = new ItemTuple(idStr, qtySum, itemEpoch);
                    dbSums.put(idStr, tuple);
                    itemCount++;
                }
                close(rs);
                logger.debug("Found " + itemCount + " item sums.");

                // Insert the current values for a check
                sql = "INSERT INTO sales (location, pluNumber, quantity, date)" +
                        " values (?,?,?,?)";
                changedAutoCommit = true;
                conn.setAutoCommit(false);

                // logging counts
                int numItems = 0;
                int numAdded = 0;
                int numChanged = 0;
                int numUnchanged = 0;

                String qtyStr;
                Timestamp tstamp = null;
                Hashtable<String, Double> idQty =
                        new Hashtable<String, Double>();
                for (Object o : itemList) {
                    Element item = (Element) o;
                    id = HandlerUtils.getRequiredString(item, "id");
                    String[] idParts = id.split("-");
                    if (2 == idParts.length && !idHash.containsKey(id)) {
                        id = idParts[0];
                    }
                    Double dQty = idQty.get(id);
                    if (null == dQty) {
                        dQty = new Double(0.0D);
                    }
                    qtyStr = HandlerUtils.getRequiredString(item, "qty");
                    dQty = new Double(dQty.doubleValue() +
                            Double.parseDouble(qtyStr));
                    idQty.put(id, dQty);
                }
                for (String strid : idQty.keySet()) {
                    numItems++;
                    if (storeAll || idHash.containsKey(strid)) {

                        ItemTuple dbTuple = dbSums.get(strid);
                        qty = idQty.get(strid).doubleValue();
                        double qtyDiff = qty;
                        long oldEpoch = 0L;
                        boolean update = false;
                        if (null != dbTuple) {
                            qtyDiff = qty - dbTuple.getQty();
                            oldEpoch = dbTuple.getEpoch();
                            if (tsEpoch.getTime() >= oldEpoch && Math.abs(qtyDiff) >= 0.01D) {
                                update = true;
                                numChanged++;
                            } else {
                                numUnchanged++;
                            }
                        } else {
                            update = true;
                            numAdded++;
                        }

                        if (update) {
                            stmtIndex = 1;
                            stmt = conn.prepareStatement(sql);
                            stmt.setInt(stmtIndex++, location);
                            stmt.setString(stmtIndex++, strid);
                            stmt.setDouble(stmtIndex++, qtyDiff);
                            stmt.setTimestamp(stmtIndex++, tsEpoch);
                            stmt.executeUpdate();
                            if (productHash.size() > 0 && productHash.containsKey(strid)) {
                                dataDelayCache = checkDataDelay(productHash.get(strid), offset, tsEpoch, dataDelayCache);
                            }
                            queryCount++;
                        }
                    }
                }

                // record that a sales reading happened in the location table
                if (tstamp != null) {
                    sql = " UPDATE location SET lastSold=GREATEST(lastSold,?) WHERE id=?";
                    stmt = conn.prepareStatement(sql);
                    stmt.setTimestamp(1, tstamp);
                    stmt.setInt(2, location);
                    stmt.executeUpdate();
                    queryCount++;
                }

                conn.commit();
                logger.dbAction("Committed " + queryCount + " queries");

                logger.debug("Total items: " + numItems);
                logger.debug("      Added: " + numAdded);
                logger.debug("    Changed: " + numChanged);
                logger.debug("  Unchanged: " + numUnchanged);

                if (dataDelayCache.size() > 0) {
                    logger.debug("Inserting Data Delays");
                    setDataDelay(location, offset, dataDelayCache);
                }
                success = true;

            } catch (SQLException sqle) {
                if (null != conn) {
                    try {
                        logger.dbAction(function + ": database rollback");
                        conn.rollback();
                    } catch (SQLException ignore) {
                    }
                }
                logger.dbError("Database error: " + sqle.getMessage());
                throw new HandlerException(sqle);
            } catch (Exception e) {
                throw new HandlerException(e);
            } finally {
                if (null != conn && changedAutoCommit) {
                    try {
                        conn.setAutoCommit(oldAutoCommit);
                    } catch (SQLException ignore) {
                    }
                }

                close(rs);
                close(stmt);
            }
        }
        toAppend.addElement("success").addText(success ? "true" : "false");
    }

    private void storeMicrosSIMCheck(Element toHandle, Element toAppend, SecureSession ss) throws HandlerException {

        int location = HandlerUtils.getRequiredInteger(toHandle, "locationId");
        int checkId = HandlerUtils.getRequiredInteger(toHandle, "checkId");
        long epoch = HandlerUtils.getRequiredLong(toHandle, "epoch");
        boolean storeAll = HandlerUtils.getOptionalBoolean(toHandle, "storeAll");
        String cycleMode = HandlerUtils.getOptionalString(toHandle, "cycleMode");
        int cycleValue = HandlerUtils.getOptionalInteger(toHandle, "cycleValue");
        if (null == cycleMode || "".equals(cycleMode)) {
            cycleMode = "dailyHour";
        } else if (!("hoursToLive".equals(cycleMode))) {
            throw new HandlerException("Invalid cycleMode");
        }
        if (cycleValue < 0) {
            cycleValue = 4;
        }

        String success = "false";

        Hashtable<String, Boolean> idHash = new Hashtable<String, Boolean>();

        List itemList = toHandle.elements("item");
        double qty;
        String id;

        logger.readingAction("storeMicrosSIMCheck by #" + ss.getLocation() + "-" + ss.getClientId());
        if (location != ss.getLocation()) {
            logger.readingAccessViolation("Called storeMicrosSIMCheck as L#" + location +
                    ", but authenticated as L#" + ss.getLocation());
            //TODO: throw handler exception for access violation
        }

        PreparedStatement stmt = null;
        ResultSet rs = null;
        int queryCount = 0;

        String sql = null;
        boolean oldAutoCommit = true;

        try {
            Timestamp tstamp = new Timestamp(epoch * 1000L);
            Calendar cal = Calendar.getInstance();
            if ("hoursToLive".equals(cycleMode)) {
                cal.add(Calendar.HOUR_OF_DAY, (-1 * cycleValue));
            } else {
                long nowMs = cal.getTimeInMillis();
                cal.set(Calendar.HOUR_OF_DAY, cycleValue);
                cal.set(Calendar.MINUTE, 0);
                cal.set(Calendar.SECOND, 0);
                cal.set(Calendar.MILLISECOND, 0);
                if (cal.getTimeInMillis() > nowMs) {
                    cal.add(Calendar.DAY_OF_MONTH, -1);
                }
            }
            Timestamp tsDelete = new Timestamp(cal.getTimeInMillis());
            oldAutoCommit = conn.getAutoCommit();

            if (!storeAll) {
                sql = "SELECT plu FROM beverage where location = ?";
                stmt = conn.prepareStatement(sql);
                stmt.setInt(1, location);
                rs = stmt.executeQuery();
                queryCount++;
                while (null != rs && rs.next()) {
                    idHash.put(rs.getString(1), Boolean.TRUE);
                }
            }

            // Delete any current values for a check
            sql = "DELETE FROM sales WHERE location = ? AND checkId = ?" +
                    " AND date > ?";
            conn.setAutoCommit(false);
            stmt = conn.prepareStatement(sql);
            int stmtIndex = 1;
            stmt.setInt(stmtIndex++, location);
            stmt.setInt(stmtIndex++, checkId);
            stmt.setTimestamp(stmtIndex++, tsDelete);
            stmt.executeUpdate();
            queryCount++;

            // Insert the current values for a check
            sql = "INSERT INTO sales (location, checkId, pluNumber, quantity, date)" +
                    " values (?,?,?,?,?)";

            boolean store = false;
            for (Object o : itemList) {
                Element item = (Element) o;
                id = HandlerUtils.getRequiredString(item, "id");
                String qtyStr;
                if (storeAll || idHash.containsKey(id)) {
                    qtyStr = HandlerUtils.getRequiredString(item, "qty");
                    qty = Double.parseDouble(qtyStr);
                    //logger.debug("Inserting: (" + location + ", " + checkId + ", " + id + ", " + qty + ", " + tstamp + ")");
                    stmtIndex = 1;
                    stmt = conn.prepareStatement(sql);
                    stmt.setInt(stmtIndex++, location);
                    stmt.setInt(stmtIndex++, checkId);
                    stmt.setString(stmtIndex++, id);
                    stmt.setDouble(stmtIndex++, qty);
                    stmt.setTimestamp(stmtIndex++, tstamp);
                    stmt.executeUpdate();
                    queryCount++;
                }
            }

            // record that a sales reading happened in the location table
            sql = " UPDATE location SET lastSold=GREATEST(lastSold,?) WHERE id=?";
            stmt = conn.prepareStatement(sql);
            stmt.setTimestamp(1, tstamp);
            stmt.setInt(2, location);
            stmt.executeUpdate();
            queryCount++;


            conn.commit();
            logger.dbAction("Commited " + queryCount + " queries");
            success = "true";

        } catch (SQLException sqle) {
            if (null != conn) {
                try {
                    logger.dbAction("PosDetail: database rollback");
                    conn.rollback();
                } catch (SQLException ignore) {
                }
            }
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            if (null != conn && null != sql) {
                try {
                    conn.setAutoCommit(oldAutoCommit);
                } catch (SQLException ignore) {
                }
            }

            close(rs);
            close(stmt);
        }
        toAppend.addElement("success").addText(success);
    }

    private void posDetail(Element toHandle, Element toAppend, SecureSession ss) throws HandlerException {

        int location = HandlerUtils.getRequiredInteger(toHandle, "locationId");
        int checkId = HandlerUtils.getRequiredInteger(toHandle, "checkId");
        long epoch = HandlerUtils.getRequiredLong(toHandle, "epoch");
        boolean storeAll = HandlerUtils.getOptionalBoolean(toHandle, "storeAll");
        String cycleMode = HandlerUtils.getOptionalString(toHandle, "cycleMode");
        int cycleValue = HandlerUtils.getOptionalInteger(toHandle, "cycleValue");
        if (null == cycleMode || "".equals(cycleMode)) {
            cycleMode = "dailyHour";
        } else if (!("hoursToLive".equals(cycleMode))) {
            throw new HandlerException("Invalid cycleMode");
        }
        if (cycleValue < 0) {
            cycleValue = 4;
        }

        String success = "false";

        Hashtable<String, Boolean> idHash = new Hashtable<String, Boolean>();

        List itemList = toHandle.elements("item");
        double qty;
        String id;

        logger.readingAction("posDetail by #" + ss.getLocation() + "-" + ss.getClientId());
        if (location != ss.getLocation()) {
            logger.readingAccessViolation("Called posDetail as L#" + location +
                    ", but authenticated as L#" + ss.getLocation());
            //TODO: throw handler exception for access violation
        }

        PreparedStatement stmt = null;
        ResultSet rs = null;
        int queryCount = 0;

        String sql = null;
        boolean oldAutoCommit = true;

        try {
            Timestamp tstamp = new Timestamp(epoch * 1000L);
            Calendar cal = Calendar.getInstance();

            if ("hoursToLive".equals(cycleMode)) {
                cal.add(Calendar.HOUR_OF_DAY, (-1 * cycleValue));
            } else {
                long nowMs = cal.getTimeInMillis();
                cal.set(Calendar.HOUR_OF_DAY, cycleValue);
                cal.set(Calendar.MINUTE, 0);
                cal.set(Calendar.SECOND, 0);
                cal.set(Calendar.MILLISECOND, 0);
                if (cal.getTimeInMillis() > nowMs) {
                    cal.add(Calendar.DAY_OF_MONTH, -1);
                }
            }
            Timestamp tsDelete = new Timestamp(cal.getTimeInMillis());
            oldAutoCommit = conn.getAutoCommit();

            if (!storeAll) {
                sql = "SELECT plu FROM beverage where location = ?";
                stmt = conn.prepareStatement(sql);
                stmt.setInt(1, location);
                rs = stmt.executeQuery();
                queryCount++;
                while (null != rs && rs.next()) {
                    idHash.put(rs.getString(1), Boolean.TRUE);
                }
            }

            // Delete any current values for a check
            sql = "DELETE FROM sales WHERE location = ? AND checkId = ?" +
                    " AND date > ?";
            conn.setAutoCommit(false);
            stmt = conn.prepareStatement(sql);
            int stmtIndex = 1;
            stmt.setInt(stmtIndex++, location);
            stmt.setInt(stmtIndex++, checkId);
            stmt.setTimestamp(stmtIndex++, tsDelete);
            stmt.executeUpdate();
            queryCount++;

            // Insert the current values for a check
            sql = "INSERT INTO sales (location, checkId, pluNumber, quantity, date)" +
                    " values (?,?,?,?,?)";

            boolean store = false;
            for (Object o : itemList) {
                Element item = (Element) o;
                id = HandlerUtils.getRequiredString(item, "id");
                String qtyStr;
                if (storeAll || idHash.containsKey(id)) {
                    qtyStr = HandlerUtils.getRequiredString(item, "qty");
                    qty = Double.parseDouble(qtyStr);
                    //logger.debug("Inserting: (" + location + ", " + checkId + ", " + id + ", " + qty + ", " + tstamp + ")");
                    stmtIndex = 1;
                    stmt = conn.prepareStatement(sql);
                    stmt.setInt(stmtIndex++, location);
                    stmt.setInt(stmtIndex++, checkId);
                    stmt.setString(stmtIndex++, id);
                    stmt.setDouble(stmtIndex++, qty);
                    stmt.setTimestamp(stmtIndex++, tstamp);
                    stmt.executeUpdate();
                    queryCount++;
                }
            }

            // record that a sales reading happened in the location table
            sql = " UPDATE location SET lastSold=GREATEST(lastSold,?) WHERE id=?";
            stmt = conn.prepareStatement(sql);
            stmt.setTimestamp(1, tstamp);
            stmt.setInt(2, location);
            stmt.executeUpdate();
            queryCount++;

            conn.commit();
            logger.dbAction("Commited " + queryCount + " queries");
            success = "true";

        } catch (SQLException sqle) {
            if (null != conn) {
                try {
                    logger.dbAction("PosDetail: database rollback");
                    conn.rollback();
                } catch (SQLException ignore) {
                }
            }
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            if (null != conn && null != sql) {
                try {
                    conn.setAutoCommit(oldAutoCommit);
                } catch (SQLException ignore) {
                }
            }

            close(rs);
            close(stmt);
        }
        toAppend.addElement("success").addText(success);
    }

    private void storeChecksWrapper(Element toHandle, Element toAppend, SecureSession ss) throws HandlerException {
        int location = HandlerUtils.getRequiredInteger(toHandle, "locationId");

        logger.readingAction(function + "#" + ss.getLocation() + "-" + ss.getClientId());
        if (61 != location && location != ss.getLocation()) {
            logger.readingAccessViolation("Called " + function + " as L#" + location +
                    ", but authenticated as L#" + ss.getLocation());
            throw new HandlerException("Access Violation");
        }

        boolean success = (location == 239 ? storePOSiChecks(toHandle, location) : storeChecks(toHandle, location));
        toAppend.addElement("success").addText(success ? "true" : "false");
    }

    private void storePOSiChecksWrapper(Element toHandle, Element toAppend, SecureSession ss) throws HandlerException {
        int location = HandlerUtils.getRequiredInteger(toHandle, "locationId");

        logger.readingAction(function + "#" + ss.getLocation() + "-" + ss.getClientId());
        if (61 != location && location != ss.getLocation()) {
            logger.readingAccessViolation("Called " + function + " as L#" + location +
                    ", but authenticated as L#" + ss.getLocation());
            throw new HandlerException("Access Violation");
        }

        boolean success = storePOSiChecks(toHandle, location);
        toAppend.addElement("success").addText(success ? "true" : "false");
    }

    private void storeSendOrderChecksWrapper(Element toHandle, Element toAppend, SecureSession ss) throws HandlerException {
        int location = HandlerUtils.getRequiredInteger(toHandle, "locationId");

        logger.readingAction(function + "#" + ss.getLocation() + "-" + ss.getClientId());
        if (61 != location && location != ss.getLocation()) {
            logger.readingAccessViolation("Called " + function + " as L#" + location +
                    ", but authenticated as L#" + ss.getLocation());
            throw new HandlerException("Access Violation");
        }

        boolean success = storeSendOrderChecks(toHandle, location);
        toAppend.addElement("success").addText(success ? "true" : "false");
    }

    private void getBreadcrumbData(Element toHandle, Element toAppend) throws HandlerException {

        int locationId                      = HandlerUtils.getRequiredInteger(toHandle, "locationId");
        String username                     = HandlerUtils.getRequiredString(toHandle, "username");
        String password                     = HandlerUtils.getRequiredString(toHandle, "password");
        String categoryString               = HandlerUtils.getRequiredString(toHandle, "category");
        int previous                        = HandlerUtils.getRequiredInteger(toHandle, "previous");
        Double ounces                       = 14.5;
        PreparedStatement stmt              = null;
        ResultSet rs                        = null, rsDetail = null;
        ArrayList<String> plus              = new ArrayList<String>();
        HashMap<String, Double> hintOunces  = new HashMap<String, Double>();
        HashMap<String, Integer> hintType   = new HashMap<String, Integer>();
        HashMap<String, Integer> unknownPlus= new HashMap<String, Integer>();
        
        try {

            net.terakeet.soapware.handlers.SQLBeerBoardMobileHandler.signHttpsCertificate();
            
            Calendar currentDate            = Calendar.getInstance();
            if (currentDate.get(Calendar.HOUR_OF_DAY) < 7) {
                currentDate.add(Calendar.DATE, -1);
            }
            currentDate.set(Calendar.HOUR_OF_DAY, 7);
            currentDate.set(Calendar.MINUTE, 0);
            currentDate.set(Calendar.SECOND, 0);
            currentDate.add(Calendar.DATE, previous);
            String businessDate             = dbDateFormat.format(currentDate.getTime());
            String breadcrumbDate           = (dbDateFormat.format(currentDate.getTime())).replaceAll(" ", "T") + "%2D05:00";
            logger.debug("Business Date: " + businessDate);
            stmt                            = conn.prepareStatement("SELECT complex, hint, type, ounces FROM posBeverageSize WHERE location = ?;");
            stmt.setInt(1, locationId);
            rs                              = stmt.executeQuery();
            while (rs.next()) {
                if (rs.getInt(1) == 0) {
                    ounces                  = rs.getDouble(4);
                } else {
                    hintOunces.put(rs.getString(2), rs.getDouble(4));
                    hintType.put(rs.getString(2), rs.getInt(3));
                }
            }
            
            stmt                            = conn.prepareStatement("SELECT plu FROM beverage WHERE location = ?;");
            stmt.setInt(1, locationId);
            rs                              = stmt.executeQuery();
            while(rs.next()) {
                plus.add(rs.getString(1));
            }
            
            stmt                = conn.prepareStatement("SELECT id, plu FROM beverage WHERE location = ? AND name = 'Unknown Product'");
            stmt.setInt(1, locationId);
            rs                  = stmt.executeQuery();
            while(rs.next()) {
                unknownPlus.put(rs.getString(2), rs.getInt(1));
            }

            
            BreadCrumb bc                   = new BreadCrumb();
            String cateogryData             = bc.getCategoryData(username, password);
            //logger.debug(cateogryData);
            String beer_category_id         = "";
            JSONObject jsonCategory         = new JSONObject(cateogryData);
            JSONArray jaCategory            = jsonCategory.getJSONArray("objects");
            for(int i = 0; i < jaCategory.length(); i++) {
                JSONObject jsonObj          = jaCategory.getJSONObject(i);
                String category             = jsonObj.getString("name");
                //logger.debug("category: " + category);
                if (category.equalsIgnoreCase(categoryString)) {
                    beer_category_id        = jsonObj.getString("id");
                    //logger.debug("category_name: " + jsonObj.getString("name"));
                    //logger.debug("beer_category_id: " + jsonObj.getString("id"));
                    //logger.debug("parent_id: " + jsonObj.getString("parent_id"));
                }
            }

            ArrayList<String> reportList    = new ArrayList<String>();
            stmt                            = conn.prepareStatement("SELECT reportRecordId FROM breadcrumbRecordId WHERE location = ? AND date > SUBDATE(?, INTERVAL 1 DAY);");
            stmt.setInt(1, locationId);
            stmt.setString(2, businessDate);
            rs                              = stmt.executeQuery();
            while (rs.next()) {
                reportList.add(rs.getString(1));
            }

            int knownCounter                = 0, unknownCounter = 0;
            HashMap<String, Integer> checkCount
                                            = new HashMap<String, Integer>();
            String data                     = bc.getCheckData(username, password, breadcrumbDate);
            //logger.debug(data);
            JSONObject json                 = new JSONObject(data);
            JSONArray ja1                   = json.getJSONArray("objects");
            //logger.debug("JSONArray Length:" + ja1.length());
            for(int i=0; i<ja1.length(); i++) {
                JSONObject jsonObj          = ja1.getJSONObject(i);
                //logger.debug("JSONArray Items Length:" + ja1.length());
                String type_id              = ja1.getJSONObject(i).getString("type_id");
                //logger.debug("type_id: " + type_id);
                if (type_id.equalsIgnoreCase("1")) {
                    String zone             = ja1.getJSONObject(i).getString("zone");
                    String zone_id          = ja1.getJSONObject(i).getString("zone_id");
                    //logger.debug("zone: " + zone);
                    //logger.debug("zone_id: " + zone_id);
                }

                JSONArray jaItems           = jsonObj.getJSONArray("items");
                for(int j = 0; j < jaItems.length(); j++) {
                    //logger.debug("JSONArray Item Details Length:" + jaItems.length());
                    String category_id      = jaItems.getJSONObject(j).getString("category_id");
                    //logger.debug("category_id: " + category_id);
                    if (category_id.equalsIgnoreCase(beer_category_id)) {
                        String id           = jaItems.getJSONObject(j).getString("id");
                        String check_id     = jaItems.getJSONObject(j).getString("check_id");
                        String name         = jaItems.getJSONObject(j).getString("name");
                        String item_id      = jaItems.getJSONObject(j).getString("item_id");
                        String quantity     = jaItems.getJSONObject(j).getString("quantity");
                        String date         = jaItems.getJSONObject(j).getString("date");
                        date                = date.replaceAll("-05:00", "").replaceAll("T", " ");
                        int reportRecordId  = 0;
                        name                = name.replaceAll("'", "\'").trim();
                        //logger.debug("name: " + name + ", item_id: " + item_id + ", check_id: " + check_id + ", quantity: " + quantity + ", date: " + date + ", Valid PLU: " + (plus.contains(item_id) ? "Yes" : "No"));
                        /* Testing for checkCount
                        if (checkCount.containsKey(check_id)) {
                            int cCount      = checkCount.get(check_id);
                            checkCount.put(check_id, cCount + 1);
                        } else {
                            checkCount.put(check_id, 1);
                        }*/

                        if(jaItems.getJSONObject(j).has("voidcomp") && jaItems.getJSONObject(j).getJSONObject("voidcomp").getString("type").equalsIgnoreCase("void")) {
                            quantity        = "-" + quantity;
                            logger.debug("name: " + name + ", date: " + date + ", quantity: " + quantity + ", new quantity: " + "-" + quantity);
                        }
                        if (!reportList.contains(id)) {
                            stmt            = conn.prepareStatement("INSERT INTO breadcrumbRecordId (location, reportRecordId, date) VALUES (?, ?, ?)");
                            stmt.setInt(1, locationId);
                            stmt.setString(2, id);
                            stmt.setString(3, date);
                            stmt.executeUpdate();

                            stmt            = conn.prepareStatement("SELECT LAST_INSERT_ID()");
                            rs              = stmt.executeQuery();
                            if (rs.next()) {
                                reportRecordId
                                            = rs.getInt(1);
                            }

                            stmt            = conn.prepareStatement("INSERT INTO sales (location, checkId, pluNumber, quantity, date, reportRecordId) VALUES (?, ?, ?, ?, ?, ?);");
                            stmt.setInt(1, locationId);
                            stmt.setString(2, check_id);
                            stmt.setString(3, item_id);
                            stmt.setString(4, quantity);
                            stmt.setString(5, date);
                            stmt.setInt(6, reportRecordId);
                            stmt.executeUpdate();
                        }

                        if (name.length() < 3) {
                            name            = "Unknown Product";
                        }
                        
                        if (unknownPlus.containsKey(item_id) && !name.equalsIgnoreCase("Unknown Product")) {
                            //logger.debug("Unknown Product: " + name + ", item_id: " + item_id + ", check_id: " + check_id + ", quantity: " + quantity + ", date: " + date + ", Valid PLU: " + (plus.contains(item_id) ? "Yes" : "No"));
                            
                            int beverage    = unknownPlus.get(item_id);
                            stmt            = conn.prepareStatement("DELETE FROM ingredient WHERE beverage = ?");
                            stmt.setInt(1, beverage);
                            stmt.executeUpdate();

                            stmt            = conn.prepareStatement("DELETE FROM beverage WHERE id = ?");
                            stmt.setInt(1, beverage);
                            stmt.executeUpdate();

                            plus.remove(item_id);
                            unknownPlus.remove(item_id);
                        }

                        if (!plus.contains(item_id)) {
                            plus            = addPLUProduct(locationId, name, item_id, ounces, hintOunces, hintType, plus);
                            unknownCounter++;
                        } else {
                            knownCounter++;
                        }

                        /**/
                    }
                }

            }
            logger.debug("Known Counter: " + knownCounter + ", Unknown Counter: " + unknownCounter);

            /* Testing check counts
            for (String check : checkCount.keySet()) {
                logger.debug("Check: " + check + ", Count: " + checkCount.get(check));
            }*/

            stmt                            = conn.prepareStatement("UPDATE location SET lastSold = NOW() WHERE id = ?");
            stmt.setInt(1, locationId);
            stmt.executeUpdate();
        } catch (SQLException sqle) {
            logger.dbError("Database error: "+sqle.toString());
        } catch (Exception e) {
            logger.dbError("JSON/URI error: " + e.getMessage());
        } finally {
            close(rsDetail);
            close(rs);
            close(stmt);
        }
    }


    private void getBOSSClancyData(Element toHandle, Element toAppend) {

        int locationId                      = 985;
        PreparedStatement stmt              = null;
        ResultSet rs                        = null;
        ArrayList<String> plus              = new ArrayList<String>();
        HashMap<String, Double> hintOunces  = new HashMap<String, Double>();
        HashMap<String, Integer> hintType   = new HashMap<String, Integer>();
        HashMap<String, Integer> unknownPlus= new HashMap<String, Integer>();
        Double ounces                       = 14.5;

        try {
            net.terakeet.soapware.handlers.SQLBeerBoardMobileHandler.signHttpsCertificate();
            stmt                            = conn.prepareStatement("SELECT plu FROM beverage WHERE location = ?;");
            stmt.setInt(1, locationId);
            rs                              = stmt.executeQuery();
            while(rs.next()) {
                plus.add(rs.getString(1));
            }
            
            stmt                = conn.prepareStatement("SELECT id, plu FROM beverage WHERE  location = ? AND name = 'Unknown Product'");
            stmt.setInt(1, locationId);
            rs                  = stmt.executeQuery();
            while(rs.next()) {
                unknownPlus.put(rs.getString(2), rs.getInt(1));
            }

            stmt                            = conn.prepareStatement("SELECT complex, hint, type, ounces FROM posBeverageSize WHERE location = ?;");
            stmt.setInt(1, locationId);
            rs                              = stmt.executeQuery();
            while(rs.next()) {
                if (rs.getInt(1) == 0) {
                    ounces                  = rs.getDouble(4);
                } else {
                    hintOunces.put(rs.getString(2), rs.getDouble(4));
                    hintType.put(rs.getString(2), rs.getInt(3));
                }
            }

            int minId                       = 88000;
            BOSSClancy bc                   = new BOSSClancy();

            JSONObject objToken             = new JSONObject(bc.getOAuthToken());
            String token                    = objToken.getString("access_token");
            //logger.debug("Token:"+token);
            JSONObject objMaxId             = new JSONObject(bc.getSalesMaxId(token));
            int maxId                       = objMaxId.getInt("MaxId");
            minId                           = maxId - 10000;
            logger.debug("Max ID: " + maxId + ", Min ID: " + minId);
            if(objMaxId.has("MaxId") && minId < maxId) {

                ArrayList<String> reportList
                                            = new ArrayList<String>();
                ArrayList<Integer> costCenterArray
                                            = new ArrayList();
                ArrayList<Integer> missingCostCenterArray
                                            = new ArrayList();
                stmt                        = conn.prepareStatement("SELECT reportRecordId FROM sales WHERE location = ? AND reportRecordId > ?;");
                stmt.setInt(1, locationId);
                stmt.setInt(2, minId);
                rs                          = stmt.executeQuery();
                while (rs.next()) {
                    reportList.add(rs.getString(1));
                }

                stmt                        = conn.prepareStatement("SELECT c.ccID FROM costCenter c WHERE c.location = ? ");
                stmt.setInt(1, locationId);
                rs                          = stmt.executeQuery();
                while (rs.next()) {
                    costCenterArray.add(rs.getInt(1));
                }

        	JSONArray data              = new JSONArray(bc.getSales(1, minId, maxId,token));
                //logger.debug("data: " + data.toString());
                int knownCounter            = 0, unknownCounter = 0;
                for (int i = 0; i < data.length(); i++) {
                    JSONObject POS          = data.getJSONObject(i);
                    //logger.debug("Id: " + POS.getString("Id"));
                    //logger.debug("Pos: " + POS.getString("Pos"));
                    //logger.debug("DateTimeStamp: " + POS.getString("DateTimeStamp"));
                    //logger.debug("Code: " + POS.getString("Code"));
                    //logger.debug("Name: " + POS.getString("Name"));
                    //logger.debug("Quantity: " + POS.getString("Quantity"));
                    //logger.debug("Amount: " + POS.getString("Amount"));
                    String reportRecordId   =  POS.getString("Id");
                    String item_id          =  POS.getString("Code");
                    String costCenter       =  POS.getString("Pos");
                    String quantity         =  POS.getString("Quantity");
                    String date             =  POS.getString("DateTimeStamp");
                    String name             =  POS.getString("Name").replaceAll(".", "").trim();
                    date                    = date.replaceAll("T", " ");
                    date                    = date.substring(0,19);
                    //logger.debug("Date Time: " + date);
                    if (!reportList.contains(reportRecordId)) {
                        stmt                = conn.prepareStatement("INSERT INTO sales (location, reportRecordId, costCenter, "
                                            + " pluNumber, quantity, date) VALUES (?, ?, ?, ?, ?, SUBDATE(?, INTERVAL 5 HOUR));");
                        stmt.setInt(1, locationId);
                        stmt.setString(2, reportRecordId);
                        stmt.setString(3, costCenter);
                        stmt.setString(4, item_id);
                        stmt.setString(5, quantity);
                        stmt.setString(6, date);
                        stmt.executeUpdate();
                        
                        if (!costCenterArray.contains(Integer.valueOf(costCenter))){
                            missingCostCenterArray.add(Integer.valueOf(costCenter));
                            costCenterArray.add(Integer.valueOf(costCenter));
                        }
                    }

                    if (name.length() < 3) {
                        name                = "Unknown Product";
                    }
                    if (unknownPlus.containsKey(item_id)) {
                            int beverage= unknownPlus.get(item_id);
                            stmt        = conn.prepareStatement("DELETE FROM ingredient WHERE beverage = ?");
                            stmt.setInt(1, beverage);
                            stmt.executeUpdate();

                            stmt        = conn.prepareStatement("DELETE FROM beverage WHERE id = ?");
                            stmt.setInt(1, beverage);
                            stmt.executeUpdate();

                            plus.remove(item_id);
                            unknownPlus.remove(item_id);
                        }

                    if (!plus.contains(item_id)) {
                        plus                = addPLUProduct(locationId, name, item_id, ounces, hintOunces, hintType, plus);
                        unknownCounter++;
                    } else {
                        knownCounter++;
                    }
                }
                logger.debug("Known Counter: " + knownCounter + ", Unknown Counter: " + unknownCounter);
                storeRetailerMissingCostCenter(missingCostCenterArray, locationId);
                
                stmt                        = conn.prepareStatement("UPDATE location SET lastSold = NOW() WHERE id = ?");
                stmt.setInt(1, locationId);
                stmt.executeUpdate();
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: "+sqle.toString());
        } catch (Exception e) {
            logger.dbError("JSON/URI error: " + e.getMessage());
        } finally {
            close(rs);
            close(stmt);
        }
    }

    private ArrayList<String> addPLUProduct(int locationId, String name, String item_id, Double ounces, HashMap<String, Double> hintOunces, HashMap<String, Integer> hintType, ArrayList<String> plus) {

        PreparedStatement stmt              = null;
        ResultSet rs                        = null;
        String productName                  = name;
        try {
            if (hintOunces.size() > 0) {
                for (String hint : hintOunces.keySet()) {
                    switch (hintType.get(hint)) {
                        case 1:
                            if (name.startsWith(hint)) {
                                ounces      = hintOunces.get(hint);
                                name        = "|||" + name;
                                productName = name.replace("|||" + hint, "").trim();
                            }
                            break;
                        case 2:
                            if (name.endsWith(hint)) {
                                ounces      = hintOunces.get(hint);
                                name        = name + "|||";
                                productName = name.replace(hint + "|||", "").trim();
                            }
                            break;
                        case 3:
                            if (name.contains(hint)) {
                                ounces      = hintOunces.get(hint);
                                productName = name.replace(hint, "").trim();
                            }
                            break;
                        default:
                            break;
                    }
                }
            }
            
            if (ounces <= 0.0) {
                String decimalPattern       = "([0-9]*)\\.([0-9]*)";  
                String integerPattern       = "([0-9]*)";  
                String[] ouncesPatterns     = {"oz", "0z"};
                String[] productSplits      = name.split(" ");
                String lastSplit            = productSplits[productSplits.length - 1];
                //logger.debug("Last Split: " + lastSplit);
                for (String pattern : ouncesPatterns) {
                    if (lastSplit.toLowerCase().endsWith(pattern)) {
                        logger.debug("Matched Pattern: " + pattern);
                        String ounceString  = lastSplit.substring(0, lastSplit.length() - 2);
                        logger.debug("Ounce String: " + ounceString);
                        if (Pattern.matches(decimalPattern, ounceString) || Pattern.matches(integerPattern, ounceString)) {
                            ounces          = Double.valueOf(ounceString);
                            productName     = name.replace(" " + lastSplit, "");
                            logger.debug("Name: " + name + ", Ounces: " + ounces);
                        }
                    }
                }
            }
            if (ounces <= 0.0) { ounces = 14.5; }
            //logger.debug("Name: " + name + ", Ounces: " + ounces);

            int product                     = 0;
            stmt                            = conn.prepareStatement("SELECT id FROM product WHERE name = ?");
            stmt.setString(1, productName);
            rs                              = stmt.executeQuery();
            if (rs.next()) {
                product                     = rs.getInt(1);
            } else {
                stmt                        = conn.prepareStatement("INSERT INTO product (name, pos) VALUES (?, 1)");
                stmt.setString(1, productName);
                stmt.executeUpdate();

                stmt                        = conn.prepareStatement("SELECT LAST_INSERT_ID()");
                rs                          = stmt.executeQuery();
                if (rs.next()) {
                    product                 = rs.getInt(1);
                }
                stmt                        = conn.prepareStatement("INSERT INTO productDescription (product) VALUES (?)");
                stmt.setInt(1, product);
                stmt.executeUpdate();

                stmt                        = conn.prepareStatement("INSERT INTO brewStyleMap (product, brewery, style) VALUES (?,0,0)");
                stmt.setInt(1, product);
                stmt.executeUpdate();

                stmt                        = conn.prepareStatement("INSERT INTO productChangeLog (product,type,date) VALUES (?,1,NOW())");
                stmt.setInt(1, product);
                stmt.executeUpdate();

                stmt                        = conn.prepareStatement("INSERT INTO userHistory (user, location, task, targetType, target, description, timestamp) " +
                                            " VALUES (21, ?, 25, 'product', ?, 'Added product " + productName + "', NOW())");
                stmt.setInt(1, locationId);
                stmt.setInt(2, product);
                stmt.executeUpdate();
            }
            

            stmt                            = conn.prepareStatement("SELECT id FROM beverage WHERE plu = ? AND location = ?");
            stmt.setString(1, item_id);
            stmt.setInt(2, locationId);
            rs                              = stmt.executeQuery();
            if (!rs.next()) {
                stmt                        = conn.prepareStatement("INSERT INTO beverage (name, location, plu, ounces, simple, pType) VALUES (?,?,?,?,1,1)");
                stmt.setString(1, name);
                stmt.setInt(2, locationId);
                stmt.setString(3, item_id);
                stmt.setDouble(4, ounces);
                stmt.executeUpdate();

                stmt                        = conn.prepareStatement("SELECT LAST_INSERT_ID()");
                rs                          = stmt.executeQuery();
                if (rs.next()) {
                    stmt                    = conn.prepareStatement("INSERT INTO ingredient (beverage, product, ounces) VALUES (?,?,?)");
                    stmt.setInt(1, rs.getInt(1));
                    stmt.setInt(2, product);
                    stmt.setDouble(3, ounces);
                    stmt.executeUpdate();


                    stmt                    = conn.prepareStatement("SELECT id FROM inventory WHERE location = ? AND product = ?");
                    stmt.setInt(1, locationId);
                    stmt.setInt(2, product);
                    rs                      = stmt.executeQuery();
                    if (!rs.next()) {
                        stmt                = conn.prepareStatement(" INSERT INTO inventory (location, product) VALUES (?,?) ");
                        stmt.setInt(1, locationId);
                        stmt.setInt(2, product);
                        stmt.executeUpdate();
                    }
                }
            }
            plus.add(item_id);
        } catch (SQLException sqle) {
            logger.dbError("Database error: "+sqle.toString());
        } catch (Exception e) {
            logger.dbError("JSON/URI error: " + e.getMessage());
        } finally {
            close(rs);
            close(stmt);
        }
        return plus;
    }

    /** Update an existing supplier.  Right now, all you can change is the name.
     *  <supplierId> 0000
     *  <name> String
     */
    private HashMap<Integer, ArrayList> checkDataDelay(int product, Double offset, Timestamp timestamp, HashMap<Integer, ArrayList> dataDelayCache) {

        java.util.Date now                  = new java.util.Date();
        now.setMinutes(0);
        now.setSeconds(0);
        
        java.util.Date cutOffTime           = new java.util.Date();
        cutOffTime.setMinutes(0);
        cutOffTime.setSeconds(0);
        GregorianCalendar gc                = new GregorianCalendar();
        gc.setTime(cutOffTime);
        gc.add(Calendar.DAY_OF_MONTH, -7);
        cutOffTime                          = gc.getTime();
        
        if (timestamp.before(now) && timestamp.after(cutOffTime)) {
            ArrayList updateArray = new ArrayList();
            if (dataDelayCache.size() > 0) {
                if (dataDelayCache.containsKey(product)) {
                    updateArray = dataDelayCache.get(product);
                    updateArray.add(timestamp);
                    dataDelayCache.put(product, updateArray);
                } else {
                    updateArray.add(timestamp);
                }
                dataDelayCache.put(product, updateArray);
            } else {
                updateArray.add(timestamp);
                dataDelayCache.put(product, updateArray);
            }
        } 
        return dataDelayCache;
    }

    private void setDataDelay(int location, Double offset, HashMap<Integer, ArrayList> dataDelayCache) {
        String insertDataDelay = "INSERT INTO dataModNew (location, modType, modId, start, end, date) VALUES (?,?,?,?,?,?);";
        long oneHour = 60 * 60 * 1000;
        PreparedStatement stmt = null;
        try {
            for (Integer product : dataDelayCache.keySet()) {

                java.util.Date startTime = new java.util.Date();
                java.util.Date endTime = new java.util.Date();
                java.util.Date busDate = new java.util.Date();

                int i = 1;
                ArrayList<Timestamp> timestampArray = dataDelayCache.get(product);
                Collections.sort(timestampArray);
                int arraySize = timestampArray.size();

                if (arraySize > 0) {
                    startTime = new java.util.Date(timestampArray.get(0).getTime() - oneHour);
                    startTime.setMinutes(0);
                    startTime.setSeconds(0);
                    endTime = new java.util.Date(timestampArray.get(arraySize - 1).getTime() + oneHour);
                    endTime.setMinutes(0);
                    endTime.setSeconds(0);
                    busDate = getBusinessDate(new java.util.Date(timestampArray.get(0).getTime()), offset);
                    busDate.setHours(7 - offset.intValue());
                    busDate.setMinutes(0);
                    busDate.setSeconds(0);
                    
                    stmt = conn.prepareStatement(insertDataDelay);
                    stmt.setInt(i++, location);
                    stmt.setInt(i++, 4);
                    stmt.setInt(i++, product);
                    stmt.setTimestamp(i++, toSqlTimestamp(startTime));
                    stmt.setTimestamp(i++, toSqlTimestamp(endTime));
                    stmt.setTimestamp(i++, toSqlTimestamp(busDate));
                    stmt.executeUpdate();
                }
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error in setDataDelay: " + sqle.getMessage());
        } finally {
            close(stmt);
        }
    }

    private java.sql.Timestamp toSqlTimestamp(java.util.Date d) {
        return new java.sql.Timestamp(d.getTime());
    }

    private java.util.Date getBusinessDate(java.util.Date d, Double offset) {
        java.util.Date busDate = new java.util.Date();
        if (d.getHours() + offset.intValue() < 8) {
            busDate = new java.util.Date(d.getTime() - (24 * 60 * 60 * 1000));
        } else {
            busDate = d;
        }
        return busDate;
    }

    private void createPurchaseEmail(HashMap<Integer, ArrayList> invoiceData) {
        StringBuilder emailBody = new StringBuilder();
        emailBody.append("<tr align=center valign=middle><td height=15 colspan=4><table><tr align=center valign=middle><td colspan=7 ></td></tr><tr><td colspan=7>&nbsp;</td></tr>");
        emailBody.append("<tr><td width=100 align=left><strong>Supplier</strong></td><td width=100 align=left><strong>Customer</strong></td><td width=100 align=left><strong>Location</strong></td><td width=100 align=center><strong>Invoice</strong></td><td width=100 align=center><strong>Product</strong></td><td width=100 align=center><strong>Quantity</strong></td><td width=160 align=center><strong>Status</strong></td></tr>");

        try {
            for (Integer index : invoiceData.keySet()) {
                ArrayList<String> invoiceArray = invoiceData.get(index);
                int arraySize = invoiceArray.size();
                if (arraySize == 7) {
                    emailBody.append("<tr><td align=left width=100 style=font-size:xx-small height=30>");
                    emailBody.append(HandlerUtils.nullToEmpty(invoiceArray.get(0).length() >= 21 ? invoiceArray.get(0).substring(0, 21) : invoiceArray.get(0))); // Supplier Name
                    emailBody.append("</td><td align=left width=100 style=font-size:xx-small>");
                    emailBody.append(HandlerUtils.nullToEmpty(invoiceArray.get(1).length() >= 21 ? invoiceArray.get(1).substring(0, 21) : invoiceArray.get(1))); // Customer Name
                    emailBody.append("</td><td align=left width=100 style=font-size:xx-small>");
                    emailBody.append(HandlerUtils.nullToEmpty(invoiceArray.get(2).length() >= 21 ? invoiceArray.get(2).substring(0, 21) : invoiceArray.get(2))); // Location Name
                    emailBody.append("</td><td align=center width=100 style=font-size:xx-small>");
                    emailBody.append(HandlerUtils.nullToEmpty(invoiceArray.get(3))); // Invoice Number
                    emailBody.append("</td><td align=center width=100 style=font-size:xx-small>");
                    emailBody.append(HandlerUtils.nullToEmpty(invoiceArray.get(4).length() >= 21 ? invoiceArray.get(4).substring(0, 21) : invoiceArray.get(4))); // Product Name
                    emailBody.append("</td><td align=center width=100 style=font-size:xx-small>");
                    emailBody.append(HandlerUtils.nullToEmpty(invoiceArray.get(5))); // Quantity Delivered
                    emailBody.append("</td><td align=center width=100 style=font-size:xx-small>");
                    emailBody.append(HandlerUtils.nullToEmpty(invoiceArray.get(6))); // Status
                    emailBody.append("</td></tr>");
                }
            }
            emailBody.append("<tr align=left valign=middle><td colspan=7 height=60></td></tr>");
            emailBody.append("<tr align=left valign=middle><td style=font-size:x-small><strong>---------</strong></td><td>:</td><td align=left colspan=2 style=font-size:x-small> No Issues Found</td></tr>");
            emailBody.append("<tr align=left valign=middle><td style=font-size:x-small><strong>Product Lines NA</strong></td><td>:</td><td align=left colspan=2 style=font-size:x-small> Not Assigned a draft line</td></tr>");
            emailBody.append("<tr align=left valign=middle><td style=font-size:x-small><strong>Product Inv NA</strong></td><td>:</td><td align=left colspan=2 style=font-size:x-small> Not found in Inventory</td></tr>");
            emailBody.append("</table></td></tr>");
            sendMail("Invoice", "invoices@beerboard.com", "Product Delivery Notification", emailBody);
        } catch (Exception e) {
            logger.dbError("Error in createPurchaseEmail: " + e.getMessage());
        } finally {
        }
    }

    public void sendMail(String userName, String emailAddr, String title, StringBuilder emailBody) {
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
            logger.debug("Error sending message to " + emailAddr + ": " + me.toString());
        }
    }


    private void storeAlohaScreenData(Element toHandle, Element toAppend, SecureSession ss) throws HandlerException {

        int locationId                      = HandlerUtils.getOptionalInteger(toHandle, "locationId");

        PreparedStatement stmt              = null;
        ResultSet rs                        = null;

        try {
            List screen                     = toHandle.elements("screen");
            if (null != screen && screen.size() > 0) {
                String checkScreen             = "SELECT id FROM alohaScreen WHERE location = ? AND screenId = ?;";
                for (Object o : screen) {
                    Element scr             = (Element) o;
                    String screenNumber     = HandlerUtils.getRequiredString(scr, "id");
                    String screenName       = HandlerUtils.getRequiredString(scr, "name");
                    String fieldName        = HandlerUtils.getRequiredString(scr, "fieldName");
                    String item             = HandlerUtils.getRequiredString(scr, "item");
                    String itemName         = HandlerUtils.getRequiredString(scr, "itemName");

                    int screenId            = -1;
                    stmt                    = conn.prepareStatement(checkScreen);
                    stmt.setInt(1, locationId);
                    stmt.setString(2, screenNumber);
                    rs                      = stmt.executeQuery();
                    if (!rs.next()) {
                        stmt                = conn.prepareStatement("INSERT INTO alohaScreen (location, screenId, screenName) VALUES (?, ?, ?)");
                        stmt.setInt(1, locationId);
                        stmt.setString(2, screenNumber);
                        stmt.setString(3, screenName);
                        stmt.executeUpdate();

                        stmt                = conn.prepareStatement("SELECT LAST_INSERT_ID();");
                        rs                  = stmt.executeQuery();
                        if (rs.next()) {
                            screenId        = rs.getInt(1);
                        }
                    } else {
                        screenId            = rs.getInt(1);
                    }

                    String selectScreenItem = "SELECT id FROM alohaScreenItems WHERE screen = ? AND plu = ?;";
                    stmt                    = conn.prepareStatement(selectScreenItem);
                    stmt.setInt(1, screenId);
                    stmt.setString(2, item);
                    rs                      = stmt.executeQuery();
                    if (!rs.next()) {
                        stmt                = conn.prepareStatement("INSERT INTO alohaScreenItems (screen, fieldName, plu, pluName, current) VALUES (?, ?, ?, ?, DATE(NOW()))");
                        stmt.setInt(1, screenId);
                        stmt.setString(2, fieldName);
                        stmt.setString(3, item);
                        stmt.setString(4, itemName);
                        stmt.executeUpdate();
                    } else {
                        stmt                = conn.prepareStatement("UPDATE alohaScreenItems SET current = DATE(NOW()) WHERE id = ?");
                        stmt.setInt(1, rs.getInt(1));
                        stmt.executeUpdate();
                    }
                }

                stmt                        = conn.prepareStatement("SELECT id FROM alohaScreen WHERE location = ?");
                stmt.setInt(1, locationId);
                rs                          = stmt.executeQuery();
                while (rs.next()) {
                    stmt                    = conn.prepareStatement("DELETE FROM alohaScreenItems WHERE screen = ? AND current < DATE(NOW())");
                    stmt.setInt(1, rs.getInt(1));
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
}
