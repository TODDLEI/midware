/*
 * GameReportResults.java
 *
 * Created on September 1, 2005, 1:51 PM
 *
 */

package net.terakeet.soapware.handlers.report;
import net.terakeet.soapware.DateParameter;
import net.terakeet.soapware.RegisteredConnection;
import net.terakeet.soapware.HandlerException;
import net.terakeet.util.MidwareLogger;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.PreparedStatement;
import java.util.Date;
import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;

/**
 *  A wrapper class for a java.sql.ResultSet
 * @author Administrator
 */
public class GameReportResults {
    
    private static MidwareLogger logger = new MidwareLogger(GameReportResults.class.getName());
    
    private ReportPeriod period;
    private ResultSet rs = null;
    private PreparedStatement stmt = null;
    
    private int station, product, gameId;
    private double value;
    private Date d;
    
    /**
     * Creates a new instance of GameReportResults
     */
    private GameReportResults(ReportPeriod period, DataType dataType, PeriodShiftType periodShift, String subQuery, RegisteredConnection conn)
    throws HandlerException {

        DateParameter validatedStartDate = new DateParameter(period.getStartDate());
        DateParameter validatedEndDate = new DateParameter(period.getEndDate());
        
        this.period = period;
        try {
    
            String sql = "SELECT station, product, event, date, value" +
                    " FROM event" + periodShift.toHoursString() + dataType.toString() + "Summary " +
                    " WHERE date BETWEEN ? AND ? AND type = ? " +
                    " AND location IN (" + subQuery + ")" +
                    " ORDER BY date";
            logger.debug("Report Query: " + sql);
            stmt = conn.prepareStatement(sql);
            stmt.setString(1, validatedStartDate.toString());
            stmt.setString(2, validatedEndDate.toString());
            rs = stmt.executeQuery();
            logger.debug("Report Query Complete");
            
        } catch (SQLException sqle) {
            throw new HandlerException(sqle);
        }
    }
    
    private GameReportResults(ReportPeriod p, ResultSet results, PreparedStatement st) throws HandlerException {
        this.period = p;
        this.rs = results;
        this.stmt = st;
    }
    
     
    /**
     * Factory method to create a new result for a given zone id.
     */
    public static GameReportResults getResultsByZone(ReportPeriod period, DataType dataType, PeriodShiftType periodShift,
            int zone, int product, RegisteredConnection conn) throws HandlerException {

        DateParameter validatedStartDate = new DateParameter(period.getStartDate());
        DateParameter validatedEndDate = new DateParameter(period.getEndDate());
        String sql =
                " SELECT e.station, e.product, e.event, e.date, e.value " +
                " FROM event" + periodShift.toHoursString() + dataType.toString() + "Summary e LEFT JOIN station st ON st.id = e.station " +
                " LEFT JOIN bar b ON b.id = st.bar LEFT JOIN zone z ON z.id = b.zone " +
                " WHERE z.id = ? AND e.date BETWEEN ? AND ? ";
                if (product > 0) {
                    sql+=" AND e.product=? ";
                }
                sql+= " ORDER BY e.station, e.product, e.date ";
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            stmt = conn.prepareStatement(sql);
            int fieldCount = 0;
            stmt.setInt(++fieldCount, zone);
            if (product > 0) {
                stmt.setInt(++fieldCount, product);
            }
            stmt.setString(++fieldCount, validatedStartDate.toString());
            stmt.setString(++fieldCount, validatedEndDate.toString());
            logger.debug("Executing getResultsByZone query");
            long startMillis = System.currentTimeMillis();
            rs = stmt.executeQuery();
            long elapsedTime = System.currentTimeMillis()-startMillis;
            logger.debug("Query complete took "+elapsedTime+" ms");
        } catch (SQLException sqle) {
            logger.dbError("Error in getResultsByZone "+sqle.getMessage());
            throw new HandlerException(sqle);
        }
        
        return new GameReportResults(period,rs,stmt);
    }
    
     /**
     * Factory method to create a new result for a given zone id.
     */
    public static GameReportResults getResultsByStation(ReportPeriod period, DataType dataType, PeriodShiftType periodShift,
            int station, int product, RegisteredConnection conn) throws HandlerException {

        DateParameter validatedStartDate = new DateParameter(period.getStartDate());
        DateParameter validatedEndDate = new DateParameter(period.getEndDate());
        String sql =
                " SELECT station, product, event, date, value " +
                " FROM event" + periodShift.toHoursString() + dataType.toString() + "Summary " +
                " WHERE station = ? AND date BETWEEN ? AND ? ";
                if (product > 0) {
                    sql+=" AND product=? ";
                }
                sql+= " ORDER BY station, product, date ";
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            stmt = conn.prepareStatement(sql);
            int fieldCount = 0;
            stmt.setInt(++fieldCount, station);
            if (product > 0) {
                stmt.setInt(++fieldCount, product);
            }
            stmt.setString(++fieldCount, validatedStartDate.toString());
            stmt.setString(++fieldCount, validatedEndDate.toString());
            logger.debug("Executing getResultsByStation query");
            long startMillis = System.currentTimeMillis();
            rs = stmt.executeQuery();
            long elapsedTime = System.currentTimeMillis()-startMillis;
            logger.debug("Query complete took "+elapsedTime+" ms");
        } catch (SQLException sqle) {
            logger.dbError("Error in getResultsByStation "+sqle.getMessage());
            throw new HandlerException(sqle);
        }
        
        return new GameReportResults(period,rs,stmt);
    }
    
    /**
     * Factory method to create a new result for a given bar id.
     */
    public static GameReportResults getResultsByBar(ReportPeriod period, DataType dataType, PeriodShiftType periodShift,
            int bar, int product, RegisteredConnection conn) throws HandlerException {

        DateParameter validatedStartDate = new DateParameter(period.getStartDate());
        DateParameter validatedEndDate = new DateParameter(period.getEndDate());
        String sql =
                " SELECT e.station, e.product, e.event, e.date, e.value " +
                " FROM event" + periodShift.toHoursString() + dataType.toString() + "Summary e LEFT JOIN station st ON st.id = e.station " +
                " WHERE st.bar = ? AND e.date BETWEEN ? AND ? ";
                if (product > 0) {
                    sql+=" AND e.product=? ";
                }
                sql+= " ORDER BY e.station, e.product, e.date ";
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            stmt = conn.prepareStatement(sql);
            int fieldCount = 0;
            stmt.setInt(++fieldCount, bar);
            if (product > 0) {
                stmt.setInt(++fieldCount, product);
            }
            stmt.setString(++fieldCount, validatedStartDate.toString());
            stmt.setString(++fieldCount, validatedEndDate.toString());
            logger.debug("Executing getResultsByBar query");
            long startMillis = System.currentTimeMillis();
            rs = stmt.executeQuery();
            long elapsedTime = System.currentTimeMillis()-startMillis;
            logger.debug("Query complete took "+elapsedTime+" ms");
        } catch (SQLException sqle) {
            logger.dbError("Error in getResultsByBar "+sqle.getMessage());
            throw new HandlerException(sqle);
        }
        
        return new GameReportResults(period,rs,stmt);
    }

    /**
     * Factory method to create a new result for a given location id.
     *  product is optional, and will be ignored if <= 0
     */
    public static GameReportResults getResultsByLocation(ReportPeriod period, DataType dataType, PeriodShiftType periodShift,
            int location, int product, RegisteredConnection conn) throws HandlerException {

        DateParameter validatedStartDate = new DateParameter(period.getStartDate());
        DateParameter validatedEndDate = new DateParameter(period.getEndDate());

        String sql =
                " SELECT station, product, event, date, value " +
                " FROM event" + periodShift.toHoursString() + dataType.toString() + "Summary " +
                " WHERE location = ? AND date BETWEEN ? AND ? ";
                if (product > 0) {
                    sql+=" AND product=? ";
                }
                sql+= " ORDER BY station, product, date ";
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            stmt = conn.prepareStatement(sql);
            int fieldCount = 0;
            stmt.setInt(++fieldCount, location);
            if (product > 0) {
                stmt.setInt(++fieldCount, product);
            }
            stmt.setString(++fieldCount, validatedStartDate.toString());
            stmt.setString(++fieldCount, validatedEndDate.toString());
            logger.debug("Executing getResultsByLocation query");
            long startMillis = System.currentTimeMillis();
            rs = stmt.executeQuery();
            long elapsedTime = System.currentTimeMillis()-startMillis;
            logger.debug("Query complete took "+elapsedTime+" ms");
        } catch (SQLException sqle) {
            logger.dbError("Error in getResultsByLocation "+sqle.getMessage());
            throw new HandlerException(sqle);
        }

        return new GameReportResults(period,rs,stmt);
    }
    
    /**
     * Factory method to create a new result for a given location id.
     *  product is optional, and will be ignored if <= 0
     */
    public static GameReportResults getResultsByEventIdsString(ReportPeriod period, DataType dataType, PeriodShiftType periodShift,
            String eventIdsString, String stationIdsCondition, int product, RegisteredConnection conn) throws HandlerException {

        String sql =
                " SELECT station, product, event, date, value " +
                " FROM event" + periodShift.toHoursString() + dataType.toString() + "Summary " +
                " WHERE event IN (" + eventIdsString +" ) " + stationIdsCondition;
                if (product > 0) {
                    sql+=" AND product=? ";
                }
                sql+= " ORDER BY station, product, date ";
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            stmt = conn.prepareStatement(sql);
            int fieldCount = 0;
            if (product > 0) {
                stmt.setInt(++fieldCount, product);
            }
            logger.debug("Executing getResultsByLocation query");
            long startMillis = System.currentTimeMillis();
            rs = stmt.executeQuery();
            long elapsedTime = System.currentTimeMillis()-startMillis;
            logger.debug("Query complete took "+elapsedTime+" ms");
        } catch (SQLException sqle) {
            logger.dbError("Error in getResultsByLocation "+sqle.getMessage());
            throw new HandlerException(sqle);
        }
        
        return new GameReportResults(period,rs,stmt);
    }
    
    /**
     * Factory method to create a new result for a given customer id.
     *  product is optional, and will be ignored if <= 0
     */   
     public static GameReportResults getResultsByCustomer(ReportPeriod period, DataType dataType, PeriodShiftType periodShift,
            int customer, int product, RegisteredConnection conn) throws HandlerException {

        DateParameter validatedStartDate = new DateParameter(period.getStartDate());
        DateParameter validatedEndDate = new DateParameter(period.getEndDate());
        
        String sql =
                " SELECT e.station, e.product, e.event, e.date, e.value " +
                " FROM event" + periodShift.toHoursString() + dataType.toString() + "Summary e LEFT JOIN location l ON l.id = e.location " +
                " WHERE l.customer = ? AND e.date BETWEEN ? AND ? ";
                if (product > 0) {
                    sql+=" AND e.product=? ";
                }
                sql+= " ORDER BY e.station, e.product, e.date ";
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            stmt = conn.prepareStatement(sql);
            int fieldCount = 0;
            stmt.setInt(++fieldCount, customer);
            if (product > 0) {
                stmt.setInt(++fieldCount, product);
            }
            stmt.setString(++fieldCount, validatedStartDate.toString());
            stmt.setString(++fieldCount, validatedEndDate.toString());
            logger.debug("Executing getResultsByCustomer query");
            long startMillis = System.currentTimeMillis();
            rs = stmt.executeQuery();
            long elapsedTime = System.currentTimeMillis()-startMillis;
            logger.debug("Query complete took "+elapsedTime+" ms");
        } catch (SQLException sqle) {
            logger.dbError("Error in getResultsByCustomer "+sqle.getMessage());
            throw new HandlerException(sqle);
        }
        
        return new GameReportResults(period,rs,stmt);
    }
    
    /**
     * Get the next reading.
     */
    public boolean next() throws SQLException {
        boolean next = (null != rs && rs.next());
        if (next) {
            station = rs.getInt(1);
            product = rs.getInt(2);
            gameId  = rs.getInt(3);
            d = new Date(rs.getTimestamp(4).getTime());
            value = rs.getDouble(5);
            //logger.debug("getting next reading");
        } else {
            //logger.debug("no more readings");
        }
        return next;
    }

    /**
     * Get the station id of the current reading.
     */
    public int getGameIndex() {
        return gameId;
    }
    
    /**
     * Get the station id of the current reading.
     */
    public int getStation() {
        return station;
    }

    /**
     * Get the product id of the current reading.
     */
    public int getProduct() {
        return product;
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
    public double getValue() {
        return value;
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
