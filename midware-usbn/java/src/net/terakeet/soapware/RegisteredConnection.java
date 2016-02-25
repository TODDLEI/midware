/*
 * RegisteredConnection.java
 *
 * Created on December 21, 2005, 10:49 AM
 *
 */
package net.terakeet.soapware;

import java.sql.*;
import java.sql.DriverManager;
import java.util.LinkedList;
import net.terakeet.util.MidwareLogger;

/** RegisteredConnection.
 *  A thin wrapper for a java.sql.Connection that encapsulates a few methods and
 *  maintains some additional information about the connection.  Intended to be
 *  used as a replacement for java.sql.Connection in the middleware.  To obtain
 *  an instance of this class, use the factory methods in DatabaseConnectionManager.
 *
 */
public class RegisteredConnection implements Comparable {

    /** how long before this connection expires: 10 minutes */
    private static final long DEFAULT_TIMEOUT = 10 * 60 * 1000;
    private Connection transconn;
    private Connection conn;
    private long expires;
    private long timeOfCreation;
    private String startTime;
    private String name;
    private String loggingDescription;
    private boolean shareable;
    private Object usersLock;
    private int users;
    private int queryCount;
    private int prepareCount;
    private double queryTime, worstTime;
    private LinkedList<LoggedStatement> statements;
    private MidwareLogger logger;
    private boolean dataLogger;
    private boolean xmlLogger;

    /** Create a new registered connection.  Intended to be called only by
     * the DatabaseConnectionManager
     * @param c the connection to wrap
     * @param n name of the connection
     * @param share true if this is a "public" connection
     */
    public RegisteredConnection(Connection c, String n, boolean share) {
        this(c, n, share, DEFAULT_TIMEOUT);
    }

    /** Create a new registered connection.  Intended to be called only by 
     * the DatabaseConnectionManager
     * @param c the connection to wrap
     * @param n name of the connection
     * @param logName a description of the caller to be used in the logging
     * @param share true if this is a "public" connection
     */
    public RegisteredConnection(Connection c, String n, String logName, boolean share) {
        this(c, n, share, DEFAULT_TIMEOUT, logName);
    }

    /** Create a new registered connection.  Intended to be called only by 
     * the DatabaseConnectionManager
     * @param c the connection to wrap
     * @param n name of the connection
     * @param share true if this is a "public" connection
     * @param timeout the millis before this connection expires
     */
    public RegisteredConnection(Connection c, String n, boolean share, long timeout) {
        this(c, n, share, timeout, "Unknown");
    }

    /** Create a new registered connection.  Intended to be called only by
     * the DatabaseConnectionManager
     * @param c the connection to wrap
     * @param n name of the connection
     * @param share true if this is a "public" connection
     * @param timeout the millis before this connection expires
     */
    public RegisteredConnection(Connection c, String n, boolean share, long timeout,
            String logName) {
        if (c == null) {
            throw new NullPointerException("Connection is null");
        }
        if (timeout < 0) {
            throw new IllegalArgumentException("timeout is negative");
        }
        
        java.text.DateFormat dateParse      = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        try {
            Date resultdate                 = new Date(System.currentTimeMillis());
            this.startTime                  = dateParse.format(resultdate);
        } catch (Exception e) {
        }
        
        this.loggingDescription = logName;
        this.conn = c;
        this.name = n;
        this.shareable = share;
        this.timeOfCreation = System.currentTimeMillis();
        this.expires = timeOfCreation + timeout;
        this.prepareCount = 0;
        this.queryCount = 0;
        this.queryTime = 0D;
        this.worstTime = 0D;
        this.usersLock = new Object();
        this.statements = new LinkedList<LoggedStatement>();
        this.logger = new MidwareLogger(RegisteredConnection.class.getName());
        this.dataLogger = Boolean.parseBoolean(HandlerUtils.getSetting("logging.databaseLog"));
        this.xmlLogger = Boolean.parseBoolean(HandlerUtils.getSetting("logging.xmlLog"));
        synchronized (usersLock) {
            this.users = 1;
        }
    }

    /** encapsulates java.sql.Connection.isClosed
     */
    public boolean isClosed() throws SQLException {
        return conn.isClosed();
    }

    /** gives a connection additional time before it expires.  The renewed connection
     * will expire "timeout" millis from the current time; the old expiriation time
     * has no effect on the new one.
     * @param timeout the milliseconds from now that the connection should expire
     */
    public void renew(long timeout) {
        long newTime = System.currentTimeMillis() + timeout;
        if (newTime > expires) {
            expires = newTime;
        }
    }

    /** Call the renew() method with the default timeout, 10 seconds
     */
    public void renew() {
        renew(DEFAULT_TIMEOUT);
    }

    /**  Adds a users to this connection, if its still available.  If the
     *  connection is still open, this will add a user and return true.  If the
     *  connection is closed, it will return false.
     */
    public boolean addUser() {
        boolean result = false;
        synchronized (usersLock) {
            if (users > 0) {
                users++;
                result = true;
            }
        }
        return result;
    }

    /** encapsulates java.sql.Connection.prepareStatement
     */
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        if (conn != null && !conn.isClosed()) {
            prepareCount++;
            LoggedStatement result = new LoggedStatement(conn.prepareStatement(sql), this);
            statements.add(result);
            return result;
        } else {
            throw new SQLException("Can't call prepareStatement, RegisteredConnection has been closed");
        }
    }

    /** encapsulates java.sql.Connection.getAutoCommit
     * "Retrieves the current auto-commit mode for this Connection  object."
     */
    public boolean getAutoCommit() throws SQLException {
        return conn.getAutoCommit();
    }

    /** encapsulates java.sql.Connection.setAutoCommit
     * "Sets this connection's auto-commit mode to the given state."
     */
    public void setAutoCommit(boolean ac) throws SQLException {
        conn.setAutoCommit(ac);
    }

    /** encapsulates java.sql.Connection.commit
     * "Makes all changes made since the previous commit/rollback permanent and 
     *  releases any database locks currently held by this Connection object."
     */
    public void commit() throws SQLException {
        conn.commit();
    }

    /** encapsulates java.sql.Connection.rollback
     * "Undoes all changes made in the current transaction and releases any 
     *  database locks currently held by this Connection object."
     */
    public void rollback() throws SQLException {
        conn.rollback();
    }

    /** encapsulates java.sql.Connection.close.
     *  will also de-register the connection in the DatabaseConnectionManager
     */
    public void close() {
        boolean doClose = false;
        synchronized (usersLock) {
            if (--users <= 0) {
                users = -9999;
                doClose = true;
            }
        }
        if (doClose) {
            DatabaseConnectionManager.close(this);
            notifyOfExpiration();
        }
    }

    private void updateQueryCounts(int moreQueries, double timeTaken, double otherWorst) {
        queryCount += moreQueries;
        queryTime += timeTaken;
        worstTime = Math.max(worstTime, otherWorst);
    }

    /** intended to be called only by DatabaseConnectionManager
     */
    public void notifyOfExpiration() {
        try {
            // first check our statement list to make sure they have closed
            for (LoggedStatement ls : statements) {
                if (ls != null) {
                    updateQueryCounts(ls.getQueryCount(), ls.getTotalTime(),
                            ls.getWorstTime());
//                     try {
//                         ls.close();
//                     } catch (Exception ignored) {}
                }
            }
            statements.clear();
            if(dataLogger)
            {
                doLogging();
            }
            conn.close();
            logger.debug("Closing connection");
        } catch (SQLException e) {
            logger.dbError(e.toString());
        }
    }

    private void doLogging() {
        PreparedStatement stmt = null;
        try {
            transconn = DriverManager.getConnection(
                    HandlerUtils.getSetting("auper.tdsPrefix") + "://" + HandlerUtils.getSetting("auper.server") + "/" + HandlerUtils.getSetting("auper.database"),
                    HandlerUtils.getSetting("auper.user"),
                    HandlerUtils.getSetting("auper.password"));

            int count = 0;
            double duration = (System.currentTimeMillis() - timeOfCreation) / 1000D;
            stmt = transconn.prepareStatement(
                    " INSERT INTO databaseLog " +
                    " (description, prepareCount, queryCount, totalTime, " +
                    "  queryTime, avgTime, maxTime, startTime) " +
                    " VALUES (?,?,?,?,?,?,?,?) ");
            stmt.setString(++count, loggingDescription);
            stmt.setInt(++count, prepareCount);
            stmt.setInt(++count, queryCount);
            stmt.setDouble(++count, duration);
            stmt.setDouble(++count, queryTime);
            stmt.setDouble(++count, queryCount > 0 ? queryTime / queryCount : 0D);
            stmt.setDouble(++count, worstTime);
            stmt.setString(++count, startTime);
            if (duration > 10.00) {
            stmt.executeUpdate();
            }
        } catch (Exception e) {
            logger.dbError(e.toString());
        } finally {
            try {
                stmt.close();
                transconn.close();
            } catch (SQLException sqle) {
                logger.dbError(sqle.toString());
            }
        }
    }

    public boolean isExpired() {
        if (System.currentTimeMillis() > expires) {
            return true;
        } else {
            return false;
        }
    }

    public String getName() {
        return name;
    }

    public boolean isShareable() {
        return shareable;
    }

    public int getUsers() {
        int result = 0;
        synchronized (usersLock) {
            if (users > 0) {
                result = users;
            }
        }
        return result;
    }

    public int getQueryCount() {
        return prepareCount;
    }

    /** compares by expiration timestamp
     */
    public int compareTo(Object o) throws ClassCastException {
        RegisteredConnection rc = (RegisteredConnection) o;
        if (this.expires < rc.expires) {
            return -1;
        } else if (this.expires > rc.expires) {
            return 1;
        } else {
            return 0;
        }
    }

    /** Compares by name, shareable flag, and expiration timestamp
     */
    public boolean equals(Object o) {
        if (o instanceof RegisteredConnection) {
            RegisteredConnection rc = (RegisteredConnection) o;
            if (this.expires == rc.expires && this.name.equals(rc.name) && this.shareable == rc.shareable) {
                return true;
            }
        }
        return false;
    }

    public int hashCode() {
        Long l = new Long(expires);
        int result = l.hashCode() * name.hashCode();
        return result;
    }
}
