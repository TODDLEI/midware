/*
 * MidwareLogger.java
 *
 * Created on April 13, 2006, 10:49 AM
 *
 */

package net.terakeet.util;

import org.apache.log4j.*;
import net.terakeet.soapware.RegisteredConnection;
import net.terakeet.soapware.DatabaseConnectionManager;
import net.terakeet.soapware.HandlerException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/** MidwareLogger
 *
 */
public class MidwareLogger {
    
    /** ********** Actions
     *  connection             
     *  dbAction                
     *  dbConnection
     *  debug
     *  generalWarning
     *  handlerException
     *  midwareError
     *  portalAction
     *  portalDetail
     *  portalAccessViolation
     *  readingAction
     *  readingDetail
     *  readingAccessViolation
     *  xml
     *  queue
     *  
     *
     * *********** Output Loggers
     *
     *   The DEBUG Logger
     *   Contains general debugging statements for creating and testing code
     *   May also contain full XML.  The resolution of the debug logger is
     *   intended to be closely controlled by the log4j.properties file to 
     *   provide a debug-look at only certain classes.
     *   INFO - Specific debugging statements from the target class
     *   DEBUG - Includes XML Requests/Responses
     *   Any other logging statements that indicate a problem or error will also hit
     *   the debug logger.    
     *
     *   The DATABASE Logger
     *   Contains connection information
     *   WARN - Database Exceptions
     *   INFO - Every Database Connection
     *   DEBUG - Every Database Query/Action
     *
     *   The XML Logger
     *   DEBUG - Contains a record of the exact XML of every request & response
     *
     *   The CONNECTION Logger
     *   TCP Related Stuff, Clients, Connections, Etc
     *
     *   The READING/SALES Logger
     *   WARN - Security Violations
     *   INFO - Contains a record each time we get HarpagonReading or Sales Reading
     *   DEBUG - Maybe some extra info
     *
     *   The PORTAL ACTION Logger
     *   Records access by individual users.  Log-ins, modifications.
     *   WARN - Access Violations
     *   INFO - The type of action taken
     *   DEBUG - The exact action, and what was changed
     *
     *   The ACCESS VIOLATION Logger
     *   This tracks Warnings from reading/sales and portal logger.  Whenever
     *   someone tries to do something they shouldn't be able to do, this is tracked.
     *   
     *   The MIDWARE EXCEPTION Logger
     *   Tracks any handler exceptions or internal problems
     *
     *   The QUEUE ACTIVITY Logger
     *   Logs actions from the middleware queue
     *
     */
    
    private Logger debug;
    private Logger database;
    private Logger xml;
    private Logger connection;
    private Logger reading;
    private Logger portal;
    private Logger accessViolation;
    private Logger error;
    private Logger queue;
    private Logger ip;
    private RegisteredConnection transconn ;
    static final String transConnName = "auper";

    private String methodName;
    
    /**  Create a new logger with stored information about the calling class and
     *  the middleware method that has been called.
     *  @param clazz the calling class, for example ReportHandler
     *  @param method the middleware method, for example getSalesReport
     */
    public MidwareLogger(String clazz, String method) {
        String loggerName;
        if (method == null) {
            loggerName = clazz;
            methodName = "";
        } else {
            loggerName = clazz+"."+method;
            methodName = "("+method+") "; 
        }
        
        debug = Logger.getLogger(clazz);
        database = Logger.getLogger("custom.database");
        xml = Logger.getLogger("custom.xml");
        connection = Logger.getLogger("custom.connection");
        ip = Logger.getLogger("custom.ip");
        reading = Logger.getLogger("custom.reading");
        portal = Logger.getLogger("custom.portalAction");
        accessViolation = Logger.getLogger("custom.accessViolation");
        error = Logger.getLogger("custom.error");    
        queue = Logger.getLogger("custom.queue");
    }
    
    /**  Create a new logger with stored information about the calling class.  Note
     * that handler classes should NOT use this constructor, in favor of using the
     * constructor that takes a methodName as well (String class, String methodName). 
     *  @param clazz the calling class, for example MessageListener
     */
    public MidwareLogger(String clazz) {
        this(clazz,null);
    }
    
    /*  Logging Actions
     */ 
    public void debug(String s) {
        debug.info(methodName+s);
    }
    
    /**  An action for database connectivty events.  Opened/Closed connections
     *
     */
    public void dbConnection(String s) {
        debug.info(s);
        database.info(methodName+s);
    }
    
    /** An action for specific database actions, like queries.  This is appropriate for either 
     * fulltext of queries, or just summaries like "ran two queries". 
     *
     */
    public void dbAction(String s) {
        debug.debug(methodName+s);
        database.debug(methodName+s);
    }
    
    /**  An action for database errors and exceptions
     */
    public void dbError(String s) {
        database.warn(methodName+s);
        error.warn(methodName+s);
    }
    
    /** Records complete XML requests and responses
     */
    public void xml(String s) {
        debug.debug(s);
        xml.debug(s);
    }
    
    /**  Records MessageListener-level TCP logging info.
     *    NOTE Might replace this action/log with debug-level log for the MessageListener
     */
    public void connection(String s) {
        connection.debug(s);
    }

    /**  Records Method-level IP logging info.
     */
    public void ip(String s) {
        ip.debug(s);
    }
    
    /** Records permission errors as they relate to remote-reading activities like
     *  harpagon readings and POS readings.
     */
    public void readingAccessViolation(String s) {
        debug.warn(s);
        reading.warn(methodName+s);
        accessViolation.warn(methodName+s);
    }
    
    /** Records harpagon/pos readings, as they occur.  This should NOT log the details of these
     * readings, merely indicate that a reading occurred
     */
    public void readingAction(String s) {
        debug.info(s);
        reading.info(s);
    }
    
    /** Records a summary or detail of harapgon/pos readings.
     */
    public void readingDetail(String s) {
        debug.debug(s);
        reading.debug(s);
    }
    
    /** Records a warning related to a harpagon/pos reading
     */
    public void readingWarning(String s) {
        debug.warn(s);
        reading.warn(s);
    }
    
    /**  Records if a user tried to peform an action on the management portal 
     *  that he didn't have permission to do.
     */
    public void portalAccessViolation(String s) {
        portal.warn(methodName+s);
        accessViolation.warn(methodName+s);
    }
    
    /**  Records any actions that a user took on the management portal such as
     *  adding a customer, or changing products.  This should only record a 
     *  summary of the action taken, and not include the details of the change.
     *  Good:  "User 34452 updated 3 products for location 31"
     *  BAD:  "User 345345 added Bud Light, 19.0oz beverage for location 31"
     */
    public void portalAction(String s) {
        portal.info(methodName+s);    
    }
    
    /** Records the specific actions that a user took on the management portal,
     *  including the exact change that was made
     */
    public void portalDetail(String s) {
        portal.debug(s);
    }
    
    /** Records the specific actions that a user took on the management portal.
     *  These records will be added to a database log, if a connection is provided.
     *  Two optional parameters, targetId and targetTable, allow you to identify which
     *  record is being updated (which should also be included in the message). To
     *  use target identification, provide the name of the table and the id of the record.
     *  For example, if draft line #456 was modified, targetClass would be 'line'
     *  and the targetd would be '456'.  To omit these fields, pass targetTable as
     *  null or the empty string or pass targetId as <= 0, or use the alternative
     *  signature for this method.
     *
     *  @param userId the id of the user performing the action
     *  @param action the short string code of the action.  This must match the 
     *      'abbrev' field of the 'task' table.  For example, user login might be
     *      'login'.  If no abbrev is found, this will be recorded as an unknown action.
     *  @param locationId the optional location id.  Will be used only if > 0. 
     *  @param targetTable the name of the table which holds the target Id. See above
     *  @param targetId the database id of the target. 
     *  @param message the text message that should include details of the action
     *  @param conn an optional database connection to log this to the database.
     *      If this field is null, no database logging will occur.
     */
    public void portalDetail(int userId, String action, int locationId, String targetTable, int targetId, String message, RegisteredConnection conn) throws HandlerException {

        transconn = DatabaseConnectionManager.getNewConnection(transConnName, "(MidwareLogger)");
        if (conn != null) {
            String checkAction = 
                    " SELECT id FROM task WHERE abbrev=? LIMIT 1";
            String insertPartialLog = 
                    " INSERT INTO userHistory (user,task,description,location) " +
                    " VALUES (?,?,?,?) ";
            String insertFullLog = 
                    " INSERT INTO userHistory (user,task,description,location,targetType,target) " +
                    " VALUES (?,?,?,?,?,?) ";
            
            PreparedStatement stmt = null;
            ResultSet rs = null;
            try {
                
                // check that the action exists (has a task id)
                int taskId = 0;
                stmt = conn.prepareStatement(checkAction);
                stmt.setString(1, action);
                rs = stmt.executeQuery();
                if (rs.next()) {
                    taskId = rs.getInt(1);
                } else {
                    // if the task doesn't exist, we'll insert it as task 0 and
                    // prepend the name of the supplied task to the message;
                    message = "UNKNOWN ("+action+") "+message;
                }
                if (locationId <= 0) {
                    locationId = -1;
                }
                if(targetId!=101 && targetId!=96){
                if (targetId >= 0 && targetTable != null && targetTable.length() > 0) {
                    stmt = transconn.prepareStatement(insertFullLog);
                    stmt.setInt(1,userId);
                    stmt.setInt(2,taskId);
                    stmt.setString(3, message);
                    stmt.setInt(4,locationId);
                    stmt.setString(5,targetTable);
                    stmt.setInt(6,targetId);
                    stmt.executeUpdate();
                } else {
                    stmt = transconn.prepareStatement(insertPartialLog);
                    stmt.setInt(1,userId);
                    stmt.setInt(2,taskId);
                    stmt.setString(3, message);
                    stmt.setInt(4,locationId);
                    stmt.executeUpdate();
                }     
                }
            } catch (SQLException sqle) {
                dbError("Error in portalDetail: "+sqle.getMessage());
            }
        }
        transconn.close();
        portal.debug(action+" by U#"+userId+(locationId > 0 ? " L#"+locationId: "")+": "+message);           
    }
    
    
    public void portalVisitDetail(int userId, String action, int locationId, String targetTable, int targetId, int minute, String message, RegisteredConnection conn) throws HandlerException {

        transconn = DatabaseConnectionManager.getNewConnection(transConnName, "(MidwareLogger)");
        if (conn != null) {
            String checkAction = 
                    " SELECT id FROM task WHERE abbrev=? LIMIT 1";
            String insertPartialLog = 
                    " INSERT INTO userHistory (user,task,description,location) " +
                    " VALUES (?,?,?,?) ";
            String insertFullLog = 
                    " INSERT INTO userHistory (user,task,description,location,targetType,target) " +
                    " VALUES (?,?,?,?,?,?) ";
            String checkLog                 = "SELECT id FROM userHistory WHERE  user = ? AND task=? AND location = ? AND timestamp between DATE_SUB(now(), INTERVAL ? MINUTE ) AND now();";
            
            PreparedStatement stmt = null;
            ResultSet rs = null;
            try {
                
                // check that the action exists (has a task id)
                int taskId = 0;
                stmt = conn.prepareStatement(checkAction);
                stmt.setString(1, action);
                rs = stmt.executeQuery();
                if (rs.next()) {
                    taskId = rs.getInt(1);
                } else {
                    // if the task doesn't exist, we'll insert it as task 0 and
                    // prepend the name of the supplied task to the message;
                    message = "UNKNOWN ("+action+") "+message;
                }
                if (locationId <= 0) {
                    locationId = -1;
                }
                if(taskId > 0 && locationId > 0){
                    stmt = conn.prepareStatement(checkLog);
                    stmt.setInt(1, userId);
                    stmt.setInt(2, taskId);
                    stmt.setInt(3, locationId);
                    stmt.setInt(4, minute);
                    rs = stmt.executeQuery();
                    if (!rs.next()) {
                        if (targetId >= 0 && targetTable != null && targetTable.length() > 0) {
                            stmt = transconn.prepareStatement(insertFullLog);
                            stmt.setInt(1,userId);
                            stmt.setInt(2,taskId);
                            stmt.setString(3, message);
                            stmt.setInt(4,locationId);
                            stmt.setString(5,targetTable);
                            stmt.setInt(6,targetId);
                            stmt.executeUpdate();
                        } else {
                            stmt = transconn.prepareStatement(insertPartialLog);
                            stmt.setInt(1,userId);
                            stmt.setInt(2,taskId);
                            stmt.setString(3, message);
                            stmt.setInt(4,locationId);
                            stmt.executeUpdate();
                        }   
                    }
                }
            } catch (SQLException sqle) {
                dbError("Error in portalDetail: "+sqle.getMessage());
            }
        }
        transconn.close();
        portal.debug(action+" by U#"+userId+(locationId > 0 ? " L#"+locationId: "")+": "+message);           
    }
    
    /**  A shortened signature to add a portal detail record without specifying a target.
     *  See the longer signature for more information.
     */
    public void portalDetail(int userId, String action, int locationId, String message, RegisteredConnection conn) throws HandlerException {
        portalDetail(userId,action,locationId,"",-1,message,conn);
    }
    
    /**  Used for any non-fatal handler exception
     */
    public void handlerException(String s) {
        error.warn(methodName+s);
    }
    
    /**  High priority debudding statement
     */
    public void generalWarning(String s) {
        debug.warn(methodName+s);
    }
    
    /**  Used for serious errors that indicate a prograaming problem or a fatal
     * or unrecoverable middleware error.  
     */
    public void midwareError(String s) {
        error.error(methodName+s);
    }
    
    public void queueDebug(String s) {
        queue.debug(s);
    }
    public void queueError(String s) {
        queue.error(s);
    }
    
    
    public static void main(String[] args) {
        PropertyConfigurator.configure("log4j.properties");
        
        MidwareLogger logger = new MidwareLogger(MidwareLogger.class.getName(), "testLogger");
        
        logger.debug("Starting test");
        logger.connection("Connected to Client");
        logger.dbConnection("Connected");
        logger.dbAction("Query 1");
        logger.dbAction("Query 2");
        logger.debug("Throwing DB Error");
        logger.handlerException("SQL Exception in DB");
        logger.dbError("Database ERROR!");
        logger.dbConnection("DB Closed due to error");
        logger.connection("Connection Closed");
        logger.debug("Database stuff finished");
        logger.connection("Connected to Client");
        logger.readingAction("Reading coming in from L12");
        logger.readingDetail("Line 1 : 45");
        logger.readingDetail("Line 2 : 50");
        logger.readingDetail("Line 3 : 60");
        logger.readingDetail("Line 4 : 80");
        logger.readingDetail("Line 5 : 85");
        logger.readingDetail("Line 6 : 90");
        logger.connection("Connection Reset");
        logger.connection("Connected to Client");
        logger.readingAction("Reading coming in from L13");
        logger.readingDetail("Line 1 : 60");
        logger.readingDetail("Line 2 : 70");
        logger.readingDetail("Line 3 : 80");
        logger.readingDetail("Line 4 : 95");
        logger.readingDetail("Line 5 : 100");
        logger.readingDetail("Line 6 : 85");
        logger.connection("Connection Reset");
        logger.debug("Finished legit readings");
        logger.connection("Connected to Client");
        logger.readingAccessViolation("Client Key Mismatch: L12 authenticated as L15");
//        debug.debug("Program Started");
//        xml.debug("<request>HEY</request>");
//        database.info("Connecting to DB");
//        database.debug("Query 1");
//        database.debug("Query 2");
//        database.debug("Query 3");
//        error.warn("ERROR IN QUERY 3");
//        database.info("Closing Conn");
//        debug.info("info!");
//        debug.warn("warn!");
//        debug.fatal("fatal!");
//        debug.debug("Program Finished");
        
        
    }
    
}

