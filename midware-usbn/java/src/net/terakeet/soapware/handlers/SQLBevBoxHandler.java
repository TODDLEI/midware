    /*
     * SQLBevBoxHandler.java
     *
     * Created on March 7, 2011, 12:00
     *
     */

package net.terakeet.soapware.handlers;

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

public class SQLBevBoxHandler implements Handler {
    
    private MidwareLogger logger;
    private static final String transConnName
                                            = "auper";
    private RegisteredConnection transconn;
    private SecureSession ss;
    private DecimalFormat cf;
    private LocationMap locationMap;
    private static SimpleDateFormat dateFormat =
            new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private static SimpleDateFormat dateStringFormat =
            new SimpleDateFormat("yyyy-MM-dd");
    private static final HashMap<Integer, Date> temperatureAlertCoolerCache = new HashMap<Integer, Date>();
    private static final HashMap<Integer, Double> highestTemperatureCoolerCache = new HashMap<Integer, Double>();

    /**
     * Creates a new instance of SQLBevBoxHandler
     */
    public SQLBevBoxHandler() throws HandlerException {
        HandlerUtils.initializeClientKeyManager();
        logger                              = new MidwareLogger(SQLBevBoxHandler.class.getName());
        transconn                           = null;
        locationMap                         = null;
        cf                                  = (DecimalFormat) NumberFormat.getInstance(Locale.US);
    }
    
    public void handle(Element toHandle, Element toAppend) throws HandlerException{
        
        String function                     = toHandle.getName();
        String responseNamespace            = (String)SOAPMessage.getURIMap().get("tkmsg");
        
        String clientKey                    = HandlerUtils.getOptionalString(toHandle,"clientKey");
        ss                                  = ClientKeyManager.getSession(clientKey);
        
        logger                              = new MidwareLogger(SQLBevBoxHandler.class.getName(), function);
        logger.debug("SQLBevBoxHandler processing method: "+function);
        logger.xml("request: " + toHandle.asXML());
        
        transconn                           = DatabaseConnectionManager.getNewConnection(transConnName, function + " (SQLBevBoxHandler)");
        
        cf.applyPattern("#.####");
        try {
            if ("picoflashPowerup".equals(function)) {
                picoflashPowerup(toHandle, responseFor(function, toAppend), ss);
            } else if ("remoteReading".equals(function)) {
                remoteReading(toHandle, responseFor(function, toAppend), ss);
            } else if ("addReading".equals(function)) {
                addReading(toHandle, responseFor(function, toAppend));
            } else if ("addComponentReading".equals(function)) {
                addReading(toHandle, responseFor(function, toAppend));
            } else if (ss.getLocation() == 0 && ss.getClientId() == 1 && ss.getSecurityLevel().canAdmin()) {
                if ("authBeerBoardUser".equals(function)) {
                    //authBeerBoardUser(toHandle, responseFor(function,toAppend));
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
                logger.midwareError("Non-handler exception thrown in SQLBevBoxHandler: "+e.toString());
                logger.midwareError("XML: " + toHandle.asXML());
                throw new HandlerException(e);
            }
        } finally {
            // Log database use
            int queryCount                  = transconn.getQueryCount();
            logger.dbAction("Executed " + queryCount + " report quer" + (queryCount == 1 ? "y" : "ies"));

            // Log transacctional database use
            queryCount                      = transconn.getQueryCount();
            logger.dbAction("Executed " + queryCount + " transactional quer" + (queryCount == 1 ? "y" : "ies"));

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

    /**  Informs the middleware that a picoflash device has powered up
     *  <locationId>000
     *  <version>"X.X.X"
     *
     */
    private void picoflashPowerup(Element toHandle, Element toAppend, SecureSession ss) throws HandlerException {
        String version                      = HandlerUtils.getRequiredString(toHandle, "version");
        int location                        = HandlerUtils.getRequiredInteger(toHandle, "locationId");
        locationMap                         = new LocationMap(transconn);
        //logger.debug("Pico powered up OK at " + locationMap.getLocation(location) + " (v" + version + ")");

        if (!(ss.getLocation() == location && ss.getSecurityLevel().canWrite())) {
            logger.readingAccessViolation(("picoflashPowerup Permission Problem for #" + locationMap.getLocation(ss.getLocation()) + "-" + ss.getClientId() + " (claimed to be L" + locationMap.getLocation(location) + ")"));
        }
        String sql                          = " UPDATE location SET picoPowerup=NOW(), picoVersion=? WHERE id=?";
        String insertReading                = " INSERT INTO reading (line,value) VALUES (?,-1.0)";
        String selectLines                  = " SELECT line.id FROM line LEFT JOIN system ON line.system = system.id " +
                                            " WHERE system.location=? AND line.status='RUNNING' AND line.product>0";

        PreparedStatement stmt              = null;
        ResultSet rs                        = null;

        try {
            stmt                            = transconn.prepareStatement(sql);
            stmt.setString(1, version);
            stmt.setInt(2, location);
            stmt.executeUpdate();

            if (version.startsWith("4")) {
                stmt                        = transconn.prepareStatement(selectLines);
                stmt.setInt(1, location);
                rs                          = stmt.executeQuery();
                while (rs.next()) {
                    stmt                    = transconn.prepareStatement(insertReading);
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
        locationMap                         = null;
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

        logger.readingAction("Remote Reading for System " + system + " at Location " + location);

        String selectType                   = " SELECT type, harpagonOffset FROM location WHERE id=?";
        String checkBevBox                  = " SELECT id, name, alert FROM bevBox WHERE location = ? AND startSystem = ?";
        String checkLineCleaningMode        = " SELECT id FROM lineCleaning WHERE location = ? AND ? BETWEEN startTime and endTime ";
        String selectCooler                 = " SELECT id, alertPoint FROM cooler WHERE location=? AND system=?";
        String isCoolerTempNeeded           = " SELECT id FROM coolerTemperature WHERE cooler = ? AND date > (NOW() - INTERVAL 15 MINUTE);";
        String selectLastPoured             = " SELECT lastPoured, lineCleaning, count FROM system WHERE location=? AND systemId=?";
        String selectLines                  = " SELECT line.lineIndex, line.id, line.ouncesPoured, line.lastPoured FROM line LEFT JOIN system ON line.system = system.id " +
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
        String updateLine                   = " UPDATE line SET ouncesPoured=?, lastPoured=? WHERE id=?";
        String updateInventory              = " UPDATE inventory SET qtyOnHand=qtyOnHand-? WHERE id=?";
        String updateLastPoured             = " UPDATE location SET lastPoured=GREATEST(lastPoured,?) WHERE id=?";
        String updateLastPouredForSystem    = " UPDATE system SET lastPoured=GREATEST(lastPoured,?), lineCleaning=?, count=? WHERE location=? AND systemId=?";
        String updateBevBox                 = " UPDATE bevBox SET alert=0, lastPoured=GREATEST(lastPoured,?) WHERE id=?";
        String insertCoolerTemperature      = " INSERT INTO coolerTemperature (cooler,value,date) VALUES (?,?,?)";

        HashMap<Integer, Integer> lines     = new HashMap<Integer, Integer>(16);
        HashMap<Integer, Double> linesPoured= new HashMap<Integer, Double>(16);
        HashMap<Integer, java.sql.Timestamp> linesDate
                                            = new HashMap<Integer, java.sql.Timestamp>(16);

        boolean lineCleaningMode            = false;
        double temperature                  = 32.0, temperatureThreshold = 32.0, ozDifference = 0.0;
        int cooler                          = 1, type = 0, lineCleaning = 0, locationType = 0, manualLineCleaning = -1, tempLineCleaning = -1, lineCleaningCount = -1, lineCountCheck = -1;
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
            }

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
            stmt                            = transconn.prepareStatement(checkLineCleaningMode);
            stmt.setInt(1, location);
            stmt.setTimestamp(2, toSqlTimestamp(timestamp));
            rs                              = stmt.executeQuery();
            if (rs.next()) {
                lineCleaningMode            = true;
            }

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
                    int lineId = lines.get(new Integer(index)).intValue();

                    if (hasCooler && tempNeeded) {
                        String temperatureString = HandlerUtils.getOptionalString(line, "t");
                        if (temperatureString == null || temperatureString.length() > 3) {
                            temperature = 32;
                        } else {
                            temperature = Double.valueOf(temperatureString);
                        }

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


                    ozDifference = current.spikeTolerantDifference(prev);
                    //logger.debug("Oz Difference" + String.valueOf(ozDifference));

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
                            stmt = transconn.prepareStatement(updateLine);
                            stmt.setDouble(1, reading);
                            stmt.setTimestamp(2, toSqlTimestamp(timestamp));
                            stmt.setInt(3, lineId);
                            stmt.executeUpdate();

                            //Decide if we should update stock
                            if (ozDifference > 1.00) {
                                if (isAfterHours(timestamp, location)) {
                                    logger.debug("Setting After Hours Alert for #" + lineId + " at " + timestamp.toString());
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
                                    //Make sure the inventory doesn't go negative
                                    // Changed Nov 3 / 06 to go negative again at the request of Jason Purdy
                                    //stmt = transconn.prepareStatement(updateInventoryAboveZero);
                                    //stmt.setInt(1, invId);
                                    //stmt.executeUpdate();
                                    updateLog += ", deducting " + threePlaces.format(kegDifference) + " kegs from inv#" + invId;
                                } else {
                                    logger.generalWarning("Unable to find inv record for line #" + lineId + " at loc #" + location);
                                }
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

                            current         = new LineReading(lineId, reading, timestamp);
                            stmt            = transconn.prepareStatement(insert);
                            stmt.setInt(1, lineId);
                            stmt.setDouble(2, reading);
                            stmt.setTimestamp(3, toSqlTimestamp(new Date(timestamp.getTime() - (30 * 1000))));
                            stmt.setDouble(4, ozDifference);
                            stmt.setInt(5, type);
                            stmt.executeUpdate();

                            //logger.debug("Inserting Forced Spike for Line Cleaning Enter/Exit Mode on line #" + lineId + " at " + timestamp.toString());
                            reading         = -1.0;

                            current         = new LineReading(lineId, reading, timestamp);
                            stmt            = transconn.prepareStatement(insert);
                            stmt.setInt(1, lineId);
                            stmt.setDouble(2, reading);
                            stmt.setTimestamp(3, toSqlTimestamp(timestamp));
                            stmt.setDouble(4, 0);
                            stmt.setInt(5, 0);
                            stmt.executeUpdate();

                            current         = new LineReading(lineId, reading, timestamp);
                            stmt            = transconn.prepareStatement(insert);
                            stmt.setInt(1, lineId);
                            stmt.setDouble(2, reading);
                            stmt.setTimestamp(3, toSqlTimestamp(timestamp));
                            stmt.setDouble(4, 0);
                            stmt.setInt(5, 1);
                            stmt.executeUpdate();
                        } else if (isLineCleaning(lineCleaning)) {
                            //logger.debug("Adding Line Cleaning reading for line #" + lineId + " at " + timestamp.toString());
                            type = 1;
                            stmt = transconn.prepareStatement(insert);
                            stmt.setInt(1, lineId);
                            stmt.setDouble(2, reading);
                            stmt.setTimestamp(3, toSqlTimestamp(timestamp));
                            stmt.setDouble(4, ozDifference);
                            stmt.setInt(5, type);
                            stmt.executeUpdate();
                        }

                        //Update the line record
                        stmt = transconn.prepareStatement(updateLine);
                        stmt.setDouble(1, reading);
                        stmt.setTimestamp(2, toSqlTimestamp(timestamp));
                        stmt.setInt(3, lineId);
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

                    if (diffValue >= 1) {
                        stmt                = transconn.prepareStatement(insertUnknownLineReading);
                        stmt.setInt(1, location);
                        stmt.setInt(2, index);
                        stmt.setInt(3, system);
                        stmt.setDouble(4, reading);
                        stmt.setDouble(5, diffValue);
                        stmt.executeUpdate();
                    }*/
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

    /** Converts a java.util.Date to a java.sql.Date
     */
    private java.sql.Timestamp toSqlTimestamp(Date d) {
        return new java.sql.Timestamp(d.getTime());
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
                        isBusinessHour      = false;
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

    /* companion method to remoteReading
     * Contains logic to decide if a reading is superfluous
     */
    private boolean readingIsNeeded(Date prev, Date current) {
        /* New logic for when to add a reading or ignore it:
         *  A reading WILL be added if ANY of the following are true
         *   #1.  The ounces poured has changed by > than 0.01
         *   #2.  At least two hours have passed since the last reading.
         */
        final double ONE_HOURS_MILLIS       = 1 * 60 * 60 * 1000;
        long timeDifference                 = current.getTime()-prev.getTime();
        
        //logger.debug("readingIsNeeded: T:"+timeDifference);
        return (timeDifference > ONE_HOURS_MILLIS);
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
    
    private void addReading(Element toHandle, Element toAppend) throws HandlerException {
        
        String encryptMAC                   = HandlerUtils.getRequiredString(toHandle, "key");
        int temperature                     = HandlerUtils.getRequiredInteger(toHandle, "t");
        int sequence                        = HandlerUtils.getRequiredInteger(toHandle, "seq");
        Date timestamp                      = new Date();
        ArrayList<Integer> lines            = new ArrayList<Integer>();
        HashMap<Integer, Integer> linesMap  = new HashMap<Integer, Integer>();
        HashMap<Integer, Double> linesCalibration
                                            = new HashMap<Integer, Double>();
        HashMap<Integer, Double> linesPoured= new HashMap<Integer, Double>();
        HashMap<Integer, Integer> linesLastType    
                                            = new HashMap<Integer, Integer>();
        ArrayList<Integer> lcLines          = new ArrayList<Integer>();
        
        String selectbevBoxDetails          = "SELECT l.id, l.type, s.id, s.systemId, b.systemInterval, b.dhcp, INET_NTOA(b.ip), INET_NTOA(b.gateway), INET_NTOA(b.netmask), " +
                                            " INET_NTOA(b.dns1), INET_NTOA(b.dns2), b.mac, b.id, l.glanola, IF(boost = 0, 1, 6) FROM bevBox b LEFT JOIN system s ON s.location = b.location AND s.systemId = b.startSystem " +
                                            " LEFT JOIN location l ON l.id = b.location WHERE b.encryptMAC = ? ; ";
        String selectMaxLine                = "SELECT MAX(lineIndex + 1) FROM line WHERE system = ? AND status = 'RUNNING'; ";
        String selectCooler                 = "SELECT id, alertPoint, offset FROM cooler WHERE location = ? AND system = ?; ";
        String isCoolerTempNeeded           = "SELECT id FROM coolerTemperature WHERE cooler = ? AND date > (NOW() - INTERVAL 15 MINUTE);";
        String insertCoolerTemperature      = "INSERT INTO coolerTemperature (cooler, value, date) VALUES (?, ?, ?)";
        String selectLastLinePulse          = "SELECT value, date FROM pulses WHERE location = ? AND line = ? AND system = ? ORDER BY id DESC LIMIT 1";
        String insertLinePulses             = "INSERT INTO pulses (location, line, system, value) VALUES (?, ?, ?, ?)";
        String updateLastPoured             = "UPDATE location SET lastPoured=GREATEST(lastPoured,?) WHERE id = ?";
        String updateBevBoxLastPoured       = "UPDATE bevBox SET lastPoured = GREATEST(lastPoured,?) WHERE id = ?";
        String updateBevBoxProvision        = "UPDATE bevBox SET encryptMAC = ? WHERE mac = ?";
        String selectLCLines                = "SELECT line FROM glanolaLineCleaning WHERE location = ? AND ? BETWEEN startTime and endTime;";
        String selectLines                  = "SELECT l.lineIndex, l.id, c.value/10000, l.ouncesPoured, l.lastType FROM line l LEFT JOIN system s ON s.id = l.system "
                                            + " LEFT JOIN calibration c ON c.line = l.lineIndex AND c.system = l.system AND c.location = s.location " +
                                              " WHERE l.status = 'RUNNING' AND c.active = 1 AND s.location = ? AND s.systemId = ?;";
        
        PreparedStatement stmt              = null;
        ResultSet rs                        = null, locationRS = null, innerRS = null;
        double temperatureThreshold         = 32.0;

        try {
            
            stmt                            = transconn.prepareStatement(selectbevBoxDetails);
            stmt.setString(1, encryptMAC);
            locationRS                      = stmt.executeQuery();
            if (locationRS.next()) {
                //logger.debug(locationRS.getString(12));
                int locationId              = locationRS.getInt(1);
                int locationType            = locationRS.getInt(2);
                int system                  = locationRS.getInt(3);
                int systemId                = locationRS.getInt(4);
                int bevBoxId                = locationRS.getInt(13);
                boolean glanolaLocation     = locationRS.getBoolean(14);
                int boost                   = locationRS.getInt(15);
                
                logger.readingAction("Add Reading for System " + system + " at Location " + locationId + (glanolaLocation ? " a Glanola Location" : ""));

                /*logger.ip("Location " + locationId);
                stmt                        = transconn.prepareStatement("SELECT SUBSTRING(REPLACE(ipAddress, '/',''), 1, LOCATE(':',ipAddress) - 2), REPLACE(message, 'Location ',''), id FROM ipLogs WHERE modified = 0;");
                rs                          = stmt.executeQuery();
                if (rs.next()) {
                    String ipAddress        = rs.getString(1);
                    int location            = rs.getInt(2);
                    stmt                    = transconn.prepareStatement("DELETE FROM ipLogs WHERE id = ?");
                    stmt.setInt(1, rs.getInt(3));
                    stmt.executeUpdate();

                    stmt                    = transconn.prepareStatement("SELECT id FROM ipLocationMap WHERE location = ?;");
                    stmt.setInt(1, location);
                    rs                      = stmt.executeQuery();
                    if (!rs.next()) {
                        stmt                = transconn.prepareStatement("INSERT INTO ipLocationMap (ipAddress, location) VALUES (?,?)");
                        stmt.setString(1, ipAddress);
                        stmt.setInt(2, location);
                        stmt.executeUpdate();
                    }
                }*/

                // Record that a reading occured in the location table
                stmt                        = transconn.prepareStatement(updateBevBoxLastPoured);
                stmt.setTimestamp(1, toSqlTimestamp(timestamp));
                stmt.setInt(2, bevBoxId);
                stmt.executeUpdate();
                
                stmt                        = transconn.prepareStatement(updateLastPoured);
                stmt.setTimestamp(1, toSqlTimestamp(timestamp));
                stmt.setInt(2, locationId);
                stmt.executeUpdate();
                
                stmt                        = transconn.prepareStatement(selectCooler);
                stmt.setInt(1, locationId);
                stmt.setInt(2, systemId);
                rs                          = stmt.executeQuery();
                if (rs.next()) {
                    int coolerId            = rs.getInt(1);
                    temperatureThreshold    = rs.getDouble(2);
                    temperature             = temperature + rs.getInt(3);
                    stmt                    = transconn.prepareStatement(isCoolerTempNeeded);
                    stmt.setInt(1, coolerId);
                    rs                      = stmt.executeQuery();
                    if (!rs.next()) {
                        stmt                = transconn.prepareStatement(insertCoolerTemperature);
                        stmt.setInt(1, coolerId);
                        stmt.setDouble(2, temperature);
                        stmt.setTimestamp(3, toSqlTimestamp(timestamp));
                        stmt.executeUpdate();
                        
                        stmt = transconn.prepareStatement("UPDATE cooler set lastValue= ? WHERE id= ?");                            
                        stmt.setDouble(1, temperature);
                        stmt.setInt(2, coolerId);
                        stmt.executeUpdate();
                    }
                    if (isTemperatureAlert(coolerId, timestamp, Double.valueOf(temperature), temperatureThreshold)) {
                        setAlert(1, coolerId, Double.valueOf(temperature));
                    }
                }
                
                stmt                        = transconn.prepareStatement(selectLCLines);
                stmt.setInt(1, locationId);
                stmt.setTimestamp(2, toSqlTimestamp(timestamp));
                rs                          = stmt.executeQuery();
                while (rs.next()) {
                    lcLines.add(rs.getInt(1));
                }

                stmt                        = transconn.prepareStatement(selectLines);
                stmt.setInt(1, locationId);
                stmt.setInt(2, systemId);
                rs                          = stmt.executeQuery();
                while (rs.next()) {
                    int lineIndex           = rs.getInt(1);
                    int lineId              = rs.getInt(2);
                    lines.add(lineIndex);
                    linesMap.put(lineIndex, lineId);
                    linesCalibration.put(lineId, rs.getDouble(3));
                    linesPoured.put(lineId, rs.getDouble(4));
                    linesLastType.put(lineId, rs.getInt(5));
                }
                
                boolean lineCleaningMode    = lineCleaning(locationId, timestamp);
                Calendar now                = Calendar.getInstance();
                Date current                = now.getTime();
                Date previous               = now.getTime();

                Iterator el                 = toHandle.elementIterator("line");
                while (el.hasNext()) {
                    int diffValue           = 0;
                    boolean storeSpike      = false;
                    boolean storeLineCleaningSpike      
                                            = false;
                    Element line            = (Element) el.next();
                    int index               = HandlerUtils.getRequiredInteger(line, "i");
                    int currCount           = HandlerUtils.getRequiredInteger(line, "count");
                    
                    if ("E171732E6D38".equalsIgnoreCase(encryptMAC) && index >= 24) {
                        locationId          = 1013;
                        system              = 2256;
                        systemId            = 0;
                        logger.readingAction("Add Reading for Line " + (index + 1) + " System " + system + " at Location " + locationId);

                        // Record that a reading occured in the location table
                        stmt                = transconn.prepareStatement(updateLastPoured);
                        stmt.setTimestamp(1, toSqlTimestamp(timestamp));
                        stmt.setInt(2, locationId);
                        stmt.executeUpdate();

                        stmt                = transconn.prepareStatement(selectLCLines);
                        stmt.setInt(1, locationId);
                        stmt.setTimestamp(2, toSqlTimestamp(timestamp));
                        rs                  = stmt.executeQuery();
                        while (rs.next()) {
                            lcLines.add(rs.getInt(1));
                        }

                        stmt                = transconn.prepareStatement(selectLines);
                        stmt.setInt(1, locationId);
                        stmt.setInt(2, systemId);
                        rs                  = stmt.executeQuery();
                        while (rs.next()) {
                            int lineIndex   = rs.getInt(1);
                            int lineId      = rs.getInt(2);
                            lines.add(lineIndex);
                            linesMap.put(lineIndex, lineId);
                            linesCalibration.put(lineId, rs.getDouble(3));
                            linesPoured.put(lineId, rs.getDouble(4));
                            linesLastType.put(lineId, rs.getInt(5));
                        }

                        lineCleaningMode    = lineCleaning(locationId, timestamp);
                    }

                    if (linesMap.containsKey(index)) {
                        stmt                = transconn.prepareStatement(selectLastLinePulse);
                        stmt.setInt(1, locationId);
                        stmt.setInt(2, index);
                        stmt.setInt(3, systemId);
                        rs                  = stmt.executeQuery();
                        if (rs.next()) {
                            int prevCount   = rs.getInt(1);

                            if (currCount >= prevCount) {
                                diffValue   = currCount - prevCount;
                            } else {
                                storeSpike  = true;
                                diffValue   = currCount;
                                //logger.debug("Value has spiked");
                            }

                            try {
                                String preDate
                                            = rs.getString(2);
                                previous    = dateFormat.parse(preDate);
                            } catch(Exception e) {
                                e.printStackTrace();
                            }
                        } else {
                            diffValue       = 1;
                        }

                        if ((diffValue >= 1) || storeSpike || readingIsNeeded(previous, current)) {
                            //logger.debug("diffValue: " + diffValue);
                            stmt            = transconn.prepareStatement(insertLinePulses);
                            stmt.setInt(1, locationId);
                            stmt.setInt(2, index);
                            stmt.setInt(3, systemId);
                            stmt.setInt(4, currCount);
                            stmt.executeUpdate();

                            String insertNewReading
                                            = "INSERT INTO reading (line, value, date, quantity, type) VALUES (?, ?, ?, ?, ?);";
                            String insertLineLastPour           
                                            = " INSERT INTO lineLastPour (id, value, date) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE value = ?, date = ?;";
                            String updateLineValue
                                            = "UPDATE line SET ouncesPoured = ?, lastPoured=?, lastType = ? WHERE id = ?";

                            double ozDifference;
                            
                            int lineId      = linesMap.get(index);
                            //logger.debug("lineId: " + lineId);
                            double calibValue  
                                            = linesCalibration.get(lineId);
                            double prevReading 
                                            = (linesPoured.get(lineId) < 0 ? 0 : linesPoured.get(lineId));
                            int prevType    = linesLastType.get(lineId);

                            //logger.debug("calibValue: " + calibValue);
                            ozDifference    = calibValue * diffValue;
                            long timeDifference
                                            = (current.getTime() - previous.getTime()) / 1000;
                            if (timeDifference > 0) {
                                double ouncesPerSecond
                                            = ozDifference / timeDifference;
                                if (ozDifference > 1000) {
                                    stmt    = transconn.prepareStatement("INSERT INTO spikedReading (location, line, quantity, ozPerSecond) VALUES (?, ?, ?, ?)");
                                    stmt.setInt(1, locationId);
                                    stmt.setInt(2, lineId);
                                    stmt.setDouble(3, ozDifference);
                                    stmt.setDouble(4, ouncesPerSecond);
                                    stmt.executeUpdate();

                                    stmt    = transconn.prepareStatement(insertNewReading);
                                    stmt.setInt(1, lineId);
                                    stmt.setDouble(2, -1);
                                    stmt.setTimestamp(3, toSqlTimestamp(new Date(timestamp.getTime() - (30 * 1000))));
                                    stmt.setDouble(4, 0);
                                    stmt.setInt(5, 0);
                                    stmt.executeUpdate();
                                    ozDifference
                                            = 0.0;
                                } else if (ouncesPerSecond > 1.0) {
                                    stmt    = transconn.prepareStatement("SELECT count + 1, IF(TIME_TO_SEC(TIMEDIFF(NOW(), startDate)) / 3600 > 1, 1, 0) "
                                            + " FROM spikedLineReading WHERE location = ? AND line = ?;");
                                    stmt.setInt(1, locationId);
                                    stmt.setInt(2, lineId);
                                    rs      = stmt.executeQuery();
                                    if (rs.next()) {
                                        if (rs.getInt(1) >= 25 && rs.getInt(1) == 1) {
                                            stmt
                                            = transconn.prepareStatement("DELETE FROM spikedLineReading WHERE location = ? AND line = ?;");
                                            stmt.setInt(1, locationId);
                                            stmt.setInt(2, lineId);
                                            stmt.executeUpdate();
                                        } else {
                                            stmt
                                            = transconn.prepareStatement("UPDATE spikedLineReading SET count = count + 1, startDate = startDate WHERE location = ? AND line = ?;");
                                            stmt.setInt(1, locationId);
                                            stmt.setInt(2, lineId);
                                            stmt.executeUpdate();
                                        }
                                    } else {
                                        stmt= transconn.prepareStatement("INSERT INTO spikedLineReading (location, line, count) VALUES (?, ?, 1)");
                                        stmt.setInt(1, locationId);
                                        stmt.setInt(2, lineId);
                                        stmt.executeUpdate();
                                    }
                                } else {
                                    stmt    = transconn.prepareStatement("DELETE FROM spikedLineReading WHERE location = ? AND line = ?;");
                                    stmt.setInt(1, locationId);
                                    stmt.setInt(2, lineId);
                                    stmt.executeUpdate();
                                }
                            }

                            if (lcLines.contains(lineId)) {
                                logger.debug("Line Cleaning via App");
                                lineCleaningMode    
                                            = true;
                            }

                            if (prevType == 0 && lineCleaningMode) {
                                 storeLineCleaningSpike
                                            = true;
                                setLineCleaningLog(locationId, lineId, timestamp);
                                //logger.debug("Entering Line Cleaning Mode");
                            } else if (prevType == 1 && !lineCleaningMode) {
                                //logger.debug("Exiting Line Cleaning Mode");
                                setLineCleaningLog(locationId, lineId, timestamp);
                                storeLineCleaningSpike
                                            = true;
                            }

                            if (storeLineCleaningSpike) {
                                stmt        = transconn.prepareStatement(insertNewReading);
                                stmt.setInt(1, lineId);
                                stmt.setDouble(2, -1);
                                stmt.setTimestamp(3, toSqlTimestamp(new Date(timestamp.getTime() - (30 * 1000))));
                                stmt.setDouble(4, 0);
                                stmt.setInt(5, 0);
                                stmt.executeUpdate();

                                stmt        = transconn.prepareStatement(insertNewReading);
                                stmt.setInt(1, lineId);
                                stmt.setDouble(2, -1);
                                stmt.setTimestamp(3, toSqlTimestamp(new Date(timestamp.getTime() - (30 * 1000))));
                                stmt.setDouble(4, 0);
                                stmt.setInt(5, 1);
                                stmt.executeUpdate();
                            }

                            if (storeSpike) {
                                stmt        = transconn.prepareStatement(insertNewReading);
                                stmt.setInt(1, lineId);
                                stmt.setDouble(2, -1);
                                stmt.setTimestamp(3, toSqlTimestamp(new Date(timestamp.getTime() - (30 * 1000))));
                                stmt.setDouble(4, 0);
                                stmt.setInt(5, 0);
                                stmt.executeUpdate();
                                prevReading = 0.0;
                            }

                            if (ozDifference == 0.0715) {
                                ozDifference= 0.0;
                            }
                            double currReading
                                            = prevReading + ozDifference;
                            //logger.debug("prevReading: " + prevReading);
                            //logger.debug("ozDifference: " + ozDifference);
                            //logger.debug("currReading: " + currReading);

                            if (!lineCleaningMode) {

                                String selectInventory
                                            = " SELECT inv.id, inv.kegSize, inv.qtyOnHand, inv.minimumQty FROM inventory AS inv " +
                                            " LEFT JOIN line ON inv.product = line.product WHERE line.id=? AND inv.location=? ";
                                String selectInventoryWithKegLine
                                            = " SELECT inv.id, inv.kegSize, inv.qtyOnHand, inv.minimumQty FROM line " +
                                            " LEFT JOIN kegLine kl ON line.kegLine=kl.id LEFT JOIN inventory inv ON inv.kegLine = kl.id " +
                                            " WHERE line.id=? AND inv.location=?";
                                String updateInventory
                                            = " UPDATE inventory SET qtyOnHand=qtyOnHand-? WHERE id=?";
                                String updateLineQtyOnHand
                                            = " UPDATE line SET qtyOnHand = qtyOnHand-? WHERE id=?";

                                // customers of type=2 use the KegLine lookup
                                stmt        = transconn.prepareStatement(locationType == 2 ? selectInventoryWithKegLine : selectInventory);
                                stmt.setInt(1, lineId);
                                stmt.setInt(2, locationId);
                                rs          = stmt.executeQuery();
                                if (rs.next()) {
                                    //get the inventory ID and the keg-size for the inventory
                                    double kegDifference
                                            = 0.0;
                                    int invId
                                            = rs.getInt(1);
                                    double kegSize
                                            = rs.getDouble(2);
                                    double qtyOnHand
                                            = rs.getDouble(3);
                                    double minimumQty
                                            = rs.getDouble(4);
                                    if (kegSize > 1.0) {
                                        kegDifference
                                            = ozDifference / kegSize;
                                    }

                                    if (invId > 0) {
                                        DecimalFormat twoPlaces
                                            = new DecimalFormat("0.00");
                                        if ((qtyOnHand - kegDifference) < minimumQty) {
                                            //setAlert(3, invId, (qtyOnHand - kegDifference));
                                        }

                                        stmt= transconn.prepareStatement(updateLineQtyOnHand);
                                        stmt.setDouble(1, kegDifference);
                                        stmt.setInt(2, lineId);
                                        stmt.executeUpdate();
                                        
                                        if (qtyOnHand < -10.0) {
                                            kegDifference
                                            = 0.0;
                                        }

                                        //Update the quantity on hand for the inventory and line QtyOnHand
                                        stmt= transconn.prepareStatement(updateInventory);
                                        stmt.setDouble(1, kegDifference);
                                        stmt.setInt(2, invId);
                                        stmt.executeUpdate();
                                        //logger.debug("Poured " + twoPlaces.format(ozDifference) + "oz from L#" + lineId);
                                    } else {
                                        logger.generalWarning("Unable to find inv record for line #" + lineId + " at loc #" + locationId);
                                    }
                                }
                            }

                            stmt            = transconn.prepareStatement(insertNewReading);
                            stmt.setInt(1, lineId);
                            stmt.setDouble(2, currReading);
                            stmt.setTimestamp(3, toSqlTimestamp(timestamp));
                            stmt.setDouble(4, ozDifference);
                            stmt.setInt(5, (lineCleaningMode ? 1 : 0));
                            stmt.executeUpdate();

                            //Update the line record
                            if (ozDifference > 0) {
                                stmt        = transconn.prepareStatement(insertLineLastPour);
                                stmt.setInt(1, lineId);
                                stmt.setDouble(2, ozDifference);
                                stmt.setTimestamp(3, toSqlTimestamp(timestamp));
                                stmt.setDouble(4, ozDifference);
                                stmt.setTimestamp(5, toSqlTimestamp(timestamp));
                                stmt.executeUpdate();
                            }
                            
                            stmt            = transconn.prepareStatement(updateLineValue);
                            stmt.setDouble(1, currReading);
                            stmt.setTimestamp(2, toSqlTimestamp(timestamp));
                            stmt.setInt(3, (lineCleaningMode ? 1 : 0));
                            stmt.setInt(4, lineId);
                            stmt.executeUpdate();
                        }
                    } else if ((currCount > 0) && (locationId != 425) && (lines.size() > 0)) {
                        diffValue           = 0;
                        String selectUnknownLastLinePulse
                                            = "SELECT value FROM unknownPulses WHERE location = ? AND line = ? AND system = ? ORDER BY id DESC LIMIT 1";
                        String insertUnknownLinePulses
                                            = "INSERT INTO unknownPulses (location, line, system, value, quantity) VALUES (?, ?, ?, ?, ?)";

                        stmt                = transconn.prepareStatement(selectUnknownLastLinePulse);
                        stmt.setInt(1, locationId);
                        stmt.setInt(2, index);
                        stmt.setInt(3, systemId);
                        rs                 = stmt.executeQuery();
                        if (rs.next()) {
                            int prevCount  = rs.getInt(1);
                            if (currCount >= prevCount) {
                                diffValue  = currCount - prevCount;
                            } else {
                                diffValue  = currCount;
                            }
                            //logger.debug("prevValue: " + prevValue);
                            //logger.debug("currValue: " + currValue);
                        } else {
                            diffValue      = 1;
                        }

                        if (diffValue > 0) {
                            stmt           = transconn.prepareStatement(insertUnknownLinePulses);
                            stmt.setInt(1, locationId);
                            stmt.setInt(2, index);
                            stmt.setInt(3, systemId);
                            stmt.setInt(4, currCount);
                            stmt.setInt(5, diffValue);
                            stmt.executeUpdate();
                        }
                    }
                }
           
                toAppend.addElement("interval").addText(String.valueOf(locationRS.getInt(5) * (60 / boost)));
                toAppend.addElement("linecount").addText(String.valueOf("40"));
                toAppend.addElement("dhcp").addText(String.valueOf(locationRS.getInt(6)));
                toAppend.addElement("ip").addText(HandlerUtils.nullToEmpty(locationRS.getString(7)));
                toAppend.addElement("gateway").addText(HandlerUtils.nullToEmpty(locationRS.getString(8)));
                toAppend.addElement("netmask").addText(HandlerUtils.nullToEmpty(locationRS.getString(9)));
                toAppend.addElement("dns1").addText(HandlerUtils.nullToEmpty(locationRS.getString(10)));
                toAppend.addElement("dns2").addText(HandlerUtils.nullToEmpty(locationRS.getString(11)));

            } else {
                String encryptionText       = "E155792E6B91";
                String decryptMAC           = "";
                for (int i = 0; i < encryptMAC.length(); i = i + 2) {
                    int inValue             = Integer.parseInt(encryptMAC.substring(i, i + 2), 16);
                    int eValue              = Integer.parseInt(encryptionText.substring(i, i + 2), 16);
                    int outValue            = inValue ^ eValue;
                    String outStr           = Integer.toHexString(outValue);
                    decryptMAC              = decryptMAC + "." + (outStr.length() > 1 ? outStr.toUpperCase() : "0" + outStr.toUpperCase());
                }
                decryptMAC                  = decryptMAC.substring(1);
                logger.debug(encryptMAC);
                logger.debug(decryptMAC);
            
                stmt                        = transconn.prepareStatement(updateBevBoxProvision);
                stmt.setString(1, encryptMAC);
                stmt.setString(2, decryptMAC);
                stmt.executeUpdate();
                toAppend.addElement("interval").addText("10");
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
            close(rs);
            close(innerRS);
            close(locationRS);
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

    /**
     * Deativating all alerts from the night before
     */
    private void addComponentReading(Element toHandle, Element toAppend) throws HandlerException {
        logger.debug("Success");
        toAppend.addElement("interval").addText("10");
    }
}
