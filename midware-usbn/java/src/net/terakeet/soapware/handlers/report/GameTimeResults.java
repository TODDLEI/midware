/*
 * GameTimeResults.java
 *
 * Created on September 1, 2005, 1:51 PM
 *
 */

package net.terakeet.soapware.handlers.report;
import net.terakeet.soapware.RegisteredConnection;
import net.terakeet.soapware.HandlerException;
import net.terakeet.util.MidwareLogger;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.PreparedStatement;
import java.util.Date;

/**
 *  A wrapper class for a java.sql.ResultSet
 * @author Administrator
 */
public class GameTimeResults {
    
    private static MidwareLogger logger = new MidwareLogger(GameTimeResults.class.getName());
    
    private ReportPeriod period;
    private ResultSet rs = null;
    private PreparedStatement stmt = null;
    
    private int gameId;
    private String description;
    private Date d;
    
    /**
     * Creates a new instance of GameTimeResults
     */
    private GameTimeResults(ReportPeriod period, String subQuery, RegisteredConnection conn)
    throws HandlerException {
        this.period = period;
        try {
    
            String sql =
                " SELECT e.id, e.date, e.eventDesc eventHours e " +
                " WHERE e.location IN (" + subQuery + ") AND e.eventStart BETWEEN ? AND ? " +
                " ORDER BY e.date, e.id ";
            logger.debug("Report Query: " + sql);
            stmt = conn.prepareStatement(sql);
            stmt.setTimestamp(1, new java.sql.Timestamp(period.getStartDate().getTime()));
            stmt.setTimestamp(2, new java.sql.Timestamp(period.getEndDate().getTime()));
            rs = stmt.executeQuery();
            logger.debug("Report Query Complete");
            
        } catch (SQLException sqle) {
            throw new HandlerException(sqle);
        }
    }
    
    private GameTimeResults(ReportPeriod p, ResultSet results, PreparedStatement st) throws HandlerException {
        this.period = p;
        this.rs = results;
        this.stmt = st;
    }
    
     
    /**
     * Factory method to create a new result for a given zone id.
     */
    public static GameTimeResults getResultsByZone(ReportPeriod period,
            int zone, RegisteredConnection conn)
            throws HandlerException {
        String sql =
                " SELECT e.id, e.date, e.eventDesc " +
                " FROM zone z LEFT JOIN eventHours e ON e.location = z.location " +
                " WHERE z.id = ? AND e.eventStart BETWEEN ? AND ? " +
                " ORDER BY e.date, e.id ";
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            stmt = conn.prepareStatement(sql);
            int fieldCount = 0;
            stmt.setInt(++fieldCount, zone);
            stmt.setTimestamp(++fieldCount, new java.sql.Timestamp(period.getStartDate().getTime()));
            stmt.setTimestamp(++fieldCount, new java.sql.Timestamp(period.getEndDate().getTime()));
            logger.debug("Executing getResultsByZone query");
            long startMillis = System.currentTimeMillis();
            rs = stmt.executeQuery();
            long elapsedTime = System.currentTimeMillis()-startMillis;
            logger.debug("Query complete took "+elapsedTime+" ms");
        } catch (SQLException sqle) {
            logger.dbError("Error in getResultsByZone "+sqle.getMessage());
            throw new HandlerException(sqle);
        }
        
        return new GameTimeResults(period,rs,stmt);
    }
    
     /**
     * Factory method to create a new result for a given zone id.
     */
    public static GameTimeResults getResultsByStation(ReportPeriod period,
            int station, RegisteredConnection conn)
            throws HandlerException {
        String sql =
                " SELECT e.id, e.date, e.eventDesc " +
                " FROM station st LEFT JOIN bar b ON b.id = st.bar " +
                " LEFT JOIN eventHours e ON e.location = b.location " +
                " WHERE st.id = ? AND e.eventStart BETWEEN ? AND ? " +
                " ORDER BY e.date, e.id ";
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            stmt = conn.prepareStatement(sql);
            int fieldCount = 0;
            stmt.setInt(++fieldCount, station);
            stmt.setTimestamp(++fieldCount, new java.sql.Timestamp(period.getStartDate().getTime()));
            stmt.setTimestamp(++fieldCount, new java.sql.Timestamp(period.getEndDate().getTime()));
            logger.debug("Executing getResultsByStation query");
            long startMillis = System.currentTimeMillis();
            rs = stmt.executeQuery();
            long elapsedTime = System.currentTimeMillis()-startMillis;
            logger.debug("Query complete took "+elapsedTime+" ms");
        } catch (SQLException sqle) {
            logger.dbError("Error in getResultsByStation "+sqle.getMessage());
            throw new HandlerException(sqle);
        }
        
        return new GameTimeResults(period,rs,stmt);
    }
    
    /**
     * Factory method to create a new result for a given bar id.
     */
    public static GameTimeResults getResultsByBar(ReportPeriod period,
            int bar, RegisteredConnection conn)
            throws HandlerException {
        String sql =
                " SELECT e.id, e.date, e.eventDesc " +
                " FROM bar b LEFT JOIN eventHours e ON e.location = b.location " +
                " WHERE b.id = ? AND e.eventStart BETWEEN ? AND ? " +
                " ORDER BY e.date, e.id ";
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            stmt = conn.prepareStatement(sql);
            int fieldCount = 0;
            stmt.setInt(++fieldCount, bar);
            stmt.setTimestamp(++fieldCount, new java.sql.Timestamp(period.getStartDate().getTime()));
            stmt.setTimestamp(++fieldCount, new java.sql.Timestamp(period.getEndDate().getTime()));
            logger.debug("Executing getResultsByBar query");
            long startMillis = System.currentTimeMillis();
            rs = stmt.executeQuery();
            long elapsedTime = System.currentTimeMillis()-startMillis;
            logger.debug("Query complete took "+elapsedTime+" ms");
        } catch (SQLException sqle) {
            logger.dbError("Error in getResultsByBar "+sqle.getMessage());
            throw new HandlerException(sqle);
        }
        
        return new GameTimeResults(period,rs,stmt);
    }

    /**
     * Factory method to create a new result for a given location id.
     *  product is optional, and will be ignored if <= 0
     */
    public static GameTimeResults getResultsByEventIdsString(ReportPeriod period,
            String eventIdsString, RegisteredConnection conn)
            throws HandlerException {
        String sql =
                " SELECT e.id, e.date, e.eventDesc " +
                " FROM eventHours e " +
                " WHERE e.id IN (" + eventIdsString +" ) " +
                " ORDER BY e.date, e.id ";
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            stmt = conn.prepareStatement(sql);
            logger.debug("Executing getResultsByLocation query");
            long startMillis = System.currentTimeMillis();
            rs = stmt.executeQuery();
            long elapsedTime = System.currentTimeMillis()-startMillis;
            logger.debug("Query complete took "+elapsedTime+" ms");
        } catch (SQLException sqle) {
            logger.dbError("Error in getResultsByLocation "+sqle.getMessage());
            throw new HandlerException(sqle);
        }

        return new GameTimeResults(period,rs,stmt);
    }
    
    /**
     * Factory method to create a new result for a given location id.
     *  product is optional, and will be ignored if <= 0
     */
    public static GameTimeResults getResultsByLocation(ReportPeriod period,
            int location, RegisteredConnection conn)
            throws HandlerException {
        String sql = 
                " SELECT e.id, e.date, e.eventDesc " +
                " FROM eventHours e " +
                " WHERE e.location = ? AND e.eventStart BETWEEN ? AND ? " +
                " ORDER BY e.date, e.id ";
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            stmt = conn.prepareStatement(sql);
            int fieldCount = 0;
            stmt.setInt(++fieldCount, location);
            stmt.setTimestamp(++fieldCount, new java.sql.Timestamp(period.getStartDate().getTime()));
            stmt.setTimestamp(++fieldCount, new java.sql.Timestamp(period.getEndDate().getTime()));
            logger.debug("Executing getResultsByLocation query");
            long startMillis = System.currentTimeMillis();
            rs = stmt.executeQuery();
            long elapsedTime = System.currentTimeMillis()-startMillis;
            logger.debug("Query complete took "+elapsedTime+" ms");
        } catch (SQLException sqle) {
            logger.dbError("Error in getResultsByLocation "+sqle.getMessage());
            throw new HandlerException(sqle);
        }
        
        return new GameTimeResults(period,rs,stmt);
    }
    
    /**
     * Factory method to create a new result for a given customer id.
     *  product is optional, and will be ignored if <= 0
     */   
     public static GameTimeResults getResultsByCustomer(ReportPeriod period,
            int customer, RegisteredConnection conn)
            throws HandlerException {
        String sql =
                " SELECT e.id, e.date, e.eventDesc " +
                " FROM location l LEFT JOIN eventHours e ON e.location = l.id " +
                " WHERE l.customer = ? AND e.eventStart BETWEEN ? AND ? " +
                " ORDER BY e.date, e.id ";
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            stmt = conn.prepareStatement(sql);
            int fieldCount = 0;
            stmt.setInt(++fieldCount, customer);
            stmt.setTimestamp(++fieldCount, new java.sql.Timestamp(period.getStartDate().getTime()));
            stmt.setTimestamp(++fieldCount, new java.sql.Timestamp(period.getEndDate().getTime()));
            logger.debug("Executing getResultsByCustomer query");
            long startMillis = System.currentTimeMillis();
            rs = stmt.executeQuery();
            long elapsedTime = System.currentTimeMillis()-startMillis;
            logger.debug("Query complete took "+elapsedTime+" ms");
        } catch (SQLException sqle) {
            logger.dbError("Error in getResultsByCustomer "+sqle.getMessage());
            throw new HandlerException(sqle);
        }
        
        return new GameTimeResults(period,rs,stmt);
    }
    
    /**
     * Get the next reading.
     */
    public boolean next() throws SQLException {
        boolean next = (null != rs && rs.next());
        if (next) {
            gameId = rs.getInt(1);
            d = new Date(rs.getTimestamp(2).getTime());
            description = rs.getString(3);
            //logger.debug("getting next reading");
        } else {
            //logger.debug("no more readings");
        }
        return next;
    }
    
    /**
     * Get the line id of the current reading.
     */
    public int getGameId() {
        return gameId;
    }
    
    /**
     * Get the date of the current reading.
     */
    public Date getDate() {
        return d;
    }
    /**
     * Get the value of the current reading.
     */
    public String getDescription() {
        return description;
    }
    
    /**
     * Close the database connections.
     */
    public void close() {
        if (null != rs) {
            try {
                rs.close();
                rs = null;
                stmt.close();
                stmt = null;
            } catch (SQLException sqle) {
                // ignore this
            }
        }
    }

    
    /**
     * Finalize method to close connections.
     */
    @Override
    protected void finalize() throws Throwable {
        try {
            close();
        } finally {
            super.finalize();
        }
    }
}
