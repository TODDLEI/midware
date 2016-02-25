/*
 * ReportResults.java
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
public class SalesResults {
    
    private static MidwareLogger logger = new MidwareLogger(SalesResults.class.getName());
    
    private ReportPeriod period;
    private ResultSet rs = null;
    private PreparedStatement stmt = null;
    
    private String plu;
    private double value;
    private Date d;
    private int location;
    private int costCenter;
    
    private SalesResults(ReportPeriod p, ResultSet results, PreparedStatement st) throws HandlerException {
        this.period = p;
        this.rs = results;
        this.stmt = st;
    }
    
    /**
     * Factory method to create a new result for a given location id.
     */
    public static SalesResults getResultsByCustomer(ReportPeriod period, int type, int customer, RegisteredConnection conn) throws HandlerException {
        
        String sql1 = " SELECT s.pluNumber, s.date, s.quantity, s.location, IFNULL(s.costCenter,0) FROM sales s LEFT JOIN location l ON l.id = s.location " +
                    " WHERE l.customer = ? AND date BETWEEN ? AND ? ORDER BY date ";
        
        String sql                          ="SELECT s.pluNumber, s.date, s.quantity, s.location, IFNULL(s.costCenter,0) FROM sales s WHERE s.location IN (SELECT l.id FROM location l LEFT JOIN locationDetails lD ON lD.location = l.id   WHERE lD.active=1 AND l.customer = ?) AND s.date "
                                            + " BETWEEN ? AND ? ORDER BY s.date";
        
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
        return new SalesResults(period,rs,stmt);
    }

    /**
     * Factory method to create a new result for a given location id.
     */
    public static SalesResults getResultsBySpecificLocations(ReportPeriod period, int type, String specificLocations, RegisteredConnection conn) throws HandlerException {
        String barType                      = "";
        if (type > 0) {
            barType                         = " b.type = ? AND ";
        }

        String sql                          = " SELECT s.pluNumber, s.date, s.quantity, s.location, IFNULL(s.costCenter,0) FROM sales s " +
                                            " LEFT JOIN bar b on b.location = s.location ";
        String sqlCostCenter                = " SELECT c.id FROM costCenter c LEFT JOIN bar b ON b.location = c.location " +
                                            " WHERE b.location IN (" + specificLocations + ") ";

        PreparedStatement stmt              = null;
        ResultSet rs                        = null;
        try {
            stmt                            = conn.prepareStatement(sqlCostCenter);
            rs                              = stmt.executeQuery();
            if (rs.next()) {
                sql                         += " RIGHT JOIN costCenter c ON c.bar = b.id AND c.ccID = s.costCenter ";
            } else {
                sql                         += " RIGHT JOIN beverage bev ON bev.location = b.location AND bev.plu = s.pluNumber ";
            }
            sql                             += " WHERE " +  barType + " b.location IN (" + specificLocations + ") AND s.date BETWEEN ? AND ? ORDER BY s.date ";
            stmt                            = conn.prepareStatement(sql);
            int fieldCount                  = 0;
            if (type > 0) {
                stmt.setInt(++fieldCount, type);
            }
            stmt.setTimestamp(++fieldCount, new java.sql.Timestamp(period.getStartDate().getTime()));
            stmt.setTimestamp(++fieldCount, new java.sql.Timestamp(period.getEndDate().getTime()));
            logger.debug("Executing getResultsBySpecificLocations query");
            long startMillis                = System.currentTimeMillis();
            rs                              = stmt.executeQuery();
            long elapsedTime                = System.currentTimeMillis()-startMillis;
            logger.debug("Query complete took "+elapsedTime+" ms");
        } catch (SQLException sqle) {
            logger.dbError("Error in getResultsBySpecificLocations "+sqle.getMessage());
            throw new HandlerException(sqle);
        }
        return new SalesResults(period,rs,stmt);
    }

    /**
     * Factory method to create a new result for a given location id.
     */
    public static SalesResults getResultsByLocation(ReportPeriod period, int type, int location, RegisteredConnection conn) throws HandlerException {
        String barType                      = "";
        if (type > 0) {
            barType                         = " b.type = ? AND ";
        }
        String sql                          = " SELECT s.pluNumber, s.date, s.quantity, s.location, IFNULL(s.costCenter,0) FROM sales s ";
        String sqlCostCenter                = " SELECT c.id FROM costCenter c WHERE c.location = ? ";

        PreparedStatement stmt              = null;
        ResultSet rs                        = null;
        try {
            stmt                            = conn.prepareStatement(sqlCostCenter);
            stmt.setInt(1, location);
            rs                              = stmt.executeQuery();
            if (rs.next()) {
                sql                         += " LEFT JOIN bar b on b.location = s.location RIGHT JOIN costCenter c ON c.bar = b.id AND c.ccID = s.costCenter WHERE " +  barType;
            } else {
                type                        = 0;
                sql                         += " RIGHT JOIN beverage bev ON bev.location = s.location AND bev.plu = s.pluNumber WHERE ";
            }
            sql                             += " s.location = ? AND s.date BETWEEN ? AND ? ORDER BY s.date ";
            stmt                            = conn.prepareStatement(sql);
            int fieldCount                  = 0;
            if (type > 0) {
                stmt.setInt(++fieldCount, type);
            }
            stmt.setInt(++fieldCount, location);
            stmt.setTimestamp(++fieldCount, new java.sql.Timestamp(period.getStartDate().getTime()));
            stmt.setTimestamp(++fieldCount, new java.sql.Timestamp(period.getEndDate().getTime()));
            logger.debug("Executing getResultsByLocation query");
            long startMillis                = System.currentTimeMillis();
            rs                              = stmt.executeQuery();
            long elapsedTime                = System.currentTimeMillis()-startMillis;
            logger.debug("Query complete took "+elapsedTime+" ms");
        } catch (SQLException sqle) {
            logger.dbError("Error in getResultsByLocation "+sqle.getMessage());
            throw new HandlerException(sqle);
        }
        return new SalesResults(period,rs,stmt);
    }
    
    public static SalesResults getResultsByLocationProduct(ReportPeriod period, int type, int location, int product, RegisteredConnection conn) throws HandlerException {
        String barType                      = "";
        if (type > 0) {
            barType                         = " b.type = ? AND ";
        }
        String sql                          = " SELECT s.pluNumber, s.date, s.quantity, s.location, IFNULL(s.costCenter,0) FROM sales s ";
        String sqlCostCenter                = " SELECT c.id FROM costCenter c WHERE c.location = ? ";

        PreparedStatement stmt              = null;
        ResultSet rs                        = null;
        try {
            //stmt                            = conn.prepareStatement(sqlCostCenter);
            //stmt.setInt(1, location);
           // rs                              = stmt.executeQuery();
            if (product > 0) {
                sql                         += " RIGHT JOIN beverage bev ON bev.location = s.location AND bev.plu = s.pluNumber  LEFT JOIN ingredient AS ing ON bev.id = ing.beverage WHERE"
                                            + " s.location = ?  AND ing.product = ? AND s.date BETWEEN ? AND ? ORDER BY s.date ;";
            } else {
                sql                         += " RIGHT JOIN beverage bev ON bev.location = s.location AND bev.plu = s.pluNumber WHERE ";
            }
            //sql                             += " s.location = ? AND s.date BETWEEN ? AND ? ORDER BY s.date ";
            stmt                            = conn.prepareStatement(sql);
            int fieldCount                  = 0;
            if (type > 0) {
                stmt.setInt(++fieldCount, type);
            }
            stmt.setInt(++fieldCount, location);
            stmt.setInt(++fieldCount, product);
            stmt.setTimestamp(++fieldCount, new java.sql.Timestamp(period.getStartDate().getTime()));
            stmt.setTimestamp(++fieldCount, new java.sql.Timestamp(period.getEndDate().getTime()));
            logger.debug("Executing getResultsByLocationProduct query");
            long startMillis                = System.currentTimeMillis();
            rs                              = stmt.executeQuery();
            long elapsedTime                = System.currentTimeMillis()-startMillis;
            logger.debug("Query complete took "+elapsedTime+" ms");
        } catch (SQLException sqle) {
            logger.dbError("Error in getResultsByLocationProduct "+sqle.getMessage());
            throw new HandlerException(sqle);
        }
        return new SalesResults(period,rs,stmt);
    }
    
    
    

    /**
     * Factory method to create a new result for a given location id.
     */
    public static SalesResults getResultsByBar(ReportPeriod period, int type, int bar, RegisteredConnection conn) throws HandlerException {
        String barType                      = "";
        if (type > 0) {
            barType                         = " b.type = ? AND ";
        }
        String sql                          = " SELECT s.pluNumber, s.date, s.quantity, s.location, IFNULL(s.costCenter,0) FROM sales s LEFT JOIN bar b on b.location = s.location ";
        String sqlCostCenter                = " SELECT c.id FROM costCenter c LEFT JOIN bar b ON b.location = c.location WHERE b.id = ? ";
        
        PreparedStatement stmt              = null;
        ResultSet rs                        = null;
        try {
            stmt                            = conn.prepareStatement(sqlCostCenter);
            stmt.setInt(1, bar);
            rs                              = stmt.executeQuery();
            if (rs.next()) {
                sql                         += " RIGHT JOIN costCenter c ON c.bar = b.id AND c.ccID = s.costCenter ";
            } else {
                sql                         += " RIGHT JOIN beverage bev ON bev.bar = b.id AND bev.plu = s.pluNumber ";
            }
            sql                             += " WHERE " +  barType + " b.id = ? AND s.date BETWEEN ? AND ? ORDER BY s.date ";
            stmt                            = conn.prepareStatement(sql);
            int fieldCount                  = 0;
            if (type > 0) {
                stmt.setInt(++fieldCount, type);
            }
            stmt.setInt(++fieldCount, bar);
            stmt.setTimestamp(++fieldCount, new java.sql.Timestamp(period.getStartDate().getTime()));
            stmt.setTimestamp(++fieldCount, new java.sql.Timestamp(period.getEndDate().getTime()));
            logger.debug("Executing getResultsByBar query");
            long startMillis                = System.currentTimeMillis();
            rs                              = stmt.executeQuery();
            long elapsedTime                = System.currentTimeMillis()-startMillis;
            logger.debug("Query complete took "+elapsedTime+" ms");
        } catch (SQLException sqle) {
            logger.dbError("Error in getResultsByBar "+sqle.getMessage());
            throw new HandlerException(sqle);
        }        
        return new SalesResults(period,rs,stmt);
    }

    /**
     * Factory method to create a new result for a given location id.
     */
    public static SalesResults getResultsByZone(ReportPeriod period, int type, int zone, RegisteredConnection conn) throws HandlerException {
        String barType                      = "";
        if (type > 0) {
            barType                         = " b.type = ? AND ";
        }
        String sql =
                " SELECT s.pluNumber, s.date, s.quantity, s.location, IFNULL(s.costCenter,0) FROM sales s LEFT JOIN costCenter c ON c.location = s.location AND c.ccID = s.costCenter " +
                " LEFT JOIN bar b ON b.id = c.bar WHERE " +  barType + " c.zone = ? AND s.date BETWEEN ? AND ? ORDER BY s.date ";
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            stmt = conn.prepareStatement(sql);
            int fieldCount = 0;
            if (type > 0) {
                stmt.setInt(++fieldCount, type);
            }
            stmt.setInt(++fieldCount, zone);
            stmt.setTimestamp(++fieldCount, new java.sql.Timestamp(period.getStartDate().getTime()));
            stmt.setTimestamp(++fieldCount, new java.sql.Timestamp(period.getEndDate().getTime()));
            logger.debug("Executing getResultsByZones query");
            long startMillis = System.currentTimeMillis();
            rs = stmt.executeQuery();
            long elapsedTime = System.currentTimeMillis()-startMillis;
            logger.debug("Query complete took "+elapsedTime+" ms");
        } catch (SQLException sqle) {
            logger.dbError("Error in getResultsByZones "+sqle.getMessage());
            throw new HandlerException(sqle);
        }
        return new SalesResults(period,rs,stmt);
    }


    /**
     * Factory method to create a new result for a given location id.
     */
    public static SalesResults getResultsByStation(ReportPeriod period, int type, int station, RegisteredConnection conn) throws HandlerException {
        String barType  = "";
        if (type > 0) {
            barType = " b.type = ? AND ";
        }
        String sql =
                " SELECT s.pluNumber, s.date, s.quantity, s.location, IFNULL(s.costCenter,0) FROM sales s LEFT JOIN bar b on b.location = s.location " +
                " LEFT JOIN station st ON st.bar = b.id RIGHT JOIN costCenter c ON c.station = st.id AND c.ccID = s.costCenter " +
                " WHERE " + barType + " st.id = ? AND s.date BETWEEN ? AND ? ORDER BY s.date ";
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            stmt = conn.prepareStatement(sql);
            int fieldCount = 0;
            if (type > 0) {
                stmt.setInt(++fieldCount, type);
            }
            stmt.setInt(++fieldCount, station);
            stmt.setTimestamp(++fieldCount, new java.sql.Timestamp(period.getStartDate().getTime()));
            stmt.setTimestamp(++fieldCount, new java.sql.Timestamp(period.getEndDate().getTime()));
            logger.debug("Executing getResultsByStation query");
            long startMillis = System.currentTimeMillis();
            rs = stmt.executeQuery();
            long elapsedTime = System.currentTimeMillis()-startMillis;
            logger.debug("Query complete took "+elapsedTime+" ms");
        } catch (SQLException sqle) {
            logger.dbError("Error in getResultsByStations "+sqle.getMessage());
            throw new HandlerException(sqle);
        }
        return new SalesResults(period,rs,stmt);
    }

    /**
     * Factory method to create a new result for a given location id.
     */
    public static SalesResults getResultsByLocationCostCenter(ReportPeriod period, int type, int location, String exclusionCostCenter, String inclusionCostCenter, RegisteredConnection conn) throws HandlerException {
        String barType                      = "";
        if (type > 0) {
            barType                         = " AND b.type = ? ";
        }
        String sql                          = " SELECT s.pluNumber, s.date, s.quantity, s.location, IFNULL(s.costCenter,0) FROM sales s " +
                                            " LEFT JOIN bar b on b.location = s.location RIGHT JOIN costCenter c ON c.bar = b.id AND c.ccID = s.costCenter WHERE " +
                                            " s.location = ? AND s.date BETWEEN ? AND ? " + barType;
        if (exclusionCostCenter.length() > 0) {
            sql                             += " AND s.costCenter NOT IN (" + exclusionCostCenter + ")";
        }
        if (inclusionCostCenter.length() > 0) {
            sql                             += " AND s.costCenter IN (" + inclusionCostCenter + ")";
        }
        sql                                 += " ORDER BY s.date ";

        PreparedStatement stmt              = null;
        ResultSet rs                        = null;
        try {
            stmt                            = conn.prepareStatement(sql);
            int fieldCount                  = 0;
            if (type > 0) {
                stmt.setInt(++fieldCount, type);
            }
            stmt.setInt(++fieldCount, location);
            stmt.setTimestamp(++fieldCount, new java.sql.Timestamp(period.getStartDate().getTime()));
            stmt.setTimestamp(++fieldCount, new java.sql.Timestamp(period.getEndDate().getTime()));
            logger.debug("Executing getResultsByLocationCostCenter query");
            long startMillis                = System.currentTimeMillis();
            rs                              = stmt.executeQuery();
            long elapsedTime                = System.currentTimeMillis()-startMillis;
            logger.debug("Query complete took "+elapsedTime+" ms");
        } catch (SQLException sqle) {
            logger.dbError("Error in getResultsByLocation "+sqle.getMessage());
            throw new HandlerException(sqle);
        }
        return new SalesResults(period,rs,stmt);
    }

    /**
     * Factory method to create a new result for a given location id.
     */
    public static SalesResults getResultsByZoneCostCenter(ReportPeriod period, int type, int zone, String exclusionCostCenter, String inclusionCostCenter, RegisteredConnection conn) throws HandlerException {
        String barType                      = "";
        if (type > 0) {
            barType                         = " AND b.type = ? ";
        }
        String sql                          = " SELECT s.pluNumber, s.date, s.quantity, s.location, IFNULL(s.costCenter,0) FROM sales s LEFT JOIN costCenter c ON c.location = s.location AND c.ccID = s.costCenter " +
                                            " LEFT JOIN bar b ON b.id = c.bar WHERE c.zone = ? AND s.date BETWEEN ? AND ? " +  barType;
        if (exclusionCostCenter.length() > 0) {
            sql                             += " AND s.costCenter NOT IN (" + exclusionCostCenter + ")";
        }
        if (inclusionCostCenter.length() > 0) {
            sql                             += " AND s.costCenter IN (" + inclusionCostCenter + ")";
        }
        sql                                 += " ORDER BY s.date ";
        
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            stmt = conn.prepareStatement(sql);
            int fieldCount = 0;
            if (type > 0) {
                stmt.setInt(++fieldCount, type);
            }
            stmt.setInt(++fieldCount, zone);
            stmt.setTimestamp(++fieldCount, new java.sql.Timestamp(period.getStartDate().getTime()));
            stmt.setTimestamp(++fieldCount, new java.sql.Timestamp(period.getEndDate().getTime()));
            logger.debug("Executing getResultsByZones query");
            long startMillis = System.currentTimeMillis();
            rs = stmt.executeQuery();
            long elapsedTime = System.currentTimeMillis()-startMillis;
            logger.debug("Query complete took "+elapsedTime+" ms");
        } catch (SQLException sqle) {
            logger.dbError("Error in getResultsByZones "+sqle.getMessage());
            throw new HandlerException(sqle);
        }
        return new SalesResults(period,rs,stmt);
    }


    /**
     * Factory method to create a new result for a given location id.
     */
    public static SalesResults getResultsByBarCostCenter(ReportPeriod period, int type, int bar, String exclusionCostCenter, String inclusionCostCenter, RegisteredConnection conn) throws HandlerException {
        String barType                      = "";
        if (type > 0) {
            barType                         = " AND b.type = ? ";
        }
        String sql                          = " SELECT s.pluNumber, s.date, s.quantity, s.location, IFNULL(s.costCenter,0) FROM sales s LEFT JOIN bar b on b.location = s.location " +
                                            " RIGHT JOIN costCenter c ON c.bar = b.id AND c.ccID = s.costCenter " +
                                            " WHERE b.id = ? AND s.date BETWEEN ? AND ? " +  barType;
        if (exclusionCostCenter.length() > 0) {
            sql                             += " AND s.costCenter NOT IN (" + exclusionCostCenter + ")";
        }
        if (inclusionCostCenter.length() > 0) {
            sql                             += " AND s.costCenter IN (" + inclusionCostCenter + ")";
        }
        sql                                 += " ORDER BY s.date ";
        
        PreparedStatement stmt              = null;
        ResultSet rs                        = null;
        try {
            stmt                            = conn.prepareStatement(sql);
            int fieldCount                  = 0;
            if (type > 0) {
                stmt.setInt(++fieldCount, type);
            }
            stmt.setInt(++fieldCount, bar);
            stmt.setTimestamp(++fieldCount, new java.sql.Timestamp(period.getStartDate().getTime()));
            stmt.setTimestamp(++fieldCount, new java.sql.Timestamp(period.getEndDate().getTime()));
            logger.debug("Executing getResultsByBar query");
            long startMillis                = System.currentTimeMillis();
            rs                              = stmt.executeQuery();
            long elapsedTime                = System.currentTimeMillis()-startMillis;
            logger.debug("Query complete took "+elapsedTime+" ms");
        } catch (SQLException sqle) {
            logger.dbError("Error in getResultsByBar "+sqle.getMessage());
            throw new HandlerException(sqle);
        }
        return new SalesResults(period,rs,stmt);
    }
    
    
    public static SalesResults getResultsByBarProductCostCenter(ReportPeriod period, int type, int bar, int product, String exclusionCostCenter, String inclusionCostCenter, RegisteredConnection conn) throws HandlerException {
        String barType                      = "";
        if (type > 0) {
            barType                         = " AND b.type = ? ";
        }
        String sql                          = " SELECT s.pluNumber, s.date, s.quantity, s.location, IFNULL(s.costCenter,0) FROM sales s LEFT JOIN bar b on b.location = s.location " +
                                            " RIGHT JOIN costCenter c ON c.bar = b.id AND c.ccID = s.costCenter "
                                            + " RIGHT JOIN beverage bev ON bev.location = s.location AND bev.plu = s.pluNumber  LEFT JOIN ingredient AS ing ON bev.id = ing.beverage WHERE " +
                                            "  b.id = ? AND ing.product = ? AND s.date BETWEEN ? AND ? " +  barType;
        if (exclusionCostCenter.length() > 0) {
            sql                             += " AND s.costCenter NOT IN (" + exclusionCostCenter + ")";
        }
        if (inclusionCostCenter.length() > 0) {
            sql                             += " AND s.costCenter IN (" + inclusionCostCenter + ")";
        }
        sql                                 += " ORDER BY s.date ";
        
        PreparedStatement stmt              = null;
        ResultSet rs                        = null;
        try {
            stmt                            = conn.prepareStatement(sql);
            int fieldCount                  = 0;
            if (type > 0) {
                stmt.setInt(++fieldCount, type);
            }
            stmt.setInt(++fieldCount, bar);
            stmt.setInt(++fieldCount, product);
            stmt.setTimestamp(++fieldCount, new java.sql.Timestamp(period.getStartDate().getTime()));
            stmt.setTimestamp(++fieldCount, new java.sql.Timestamp(period.getEndDate().getTime()));
            logger.debug("Executing getResultsByBar query");
            long startMillis                = System.currentTimeMillis();
            rs                              = stmt.executeQuery();
            long elapsedTime                = System.currentTimeMillis()-startMillis;
            logger.debug("Query complete took "+elapsedTime+" ms");
        } catch (SQLException sqle) {
            logger.dbError("Error in getResultsByBar "+sqle.getMessage());
            throw new HandlerException(sqle);
        }
        return new SalesResults(period,rs,stmt);
    }


    /**
     * Factory method to create a new result for a given location id.
     */
    public static SalesResults getResultsByStationCostCenter(ReportPeriod period, int type, int station, String exclusionCostCenter, String inclusionCostCenter, RegisteredConnection conn) throws HandlerException {
        String barType                      = "";
        if (type > 0) {
            barType                         = " AND b.type = ? ";
        }
        String sql                          = " SELECT s.pluNumber, s.date, s.quantity, s.location, IFNULL(s.costCenter,0) FROM sales s LEFT JOIN bar b on b.location = s.location " +
                                            " LEFT JOIN station st ON st.bar = b.id RIGHT JOIN costCenter c ON c.station = st.id AND c.ccID = s.costCenter " +
                                            " WHERE st.id = ? AND s.date BETWEEN ? AND ? " + barType;
        if (exclusionCostCenter.length() > 0) {
            sql                             += " AND s.costCenter NOT IN (" + exclusionCostCenter + ")";
        }
        if (inclusionCostCenter.length() > 0) {
            sql                             += " AND s.costCenter IN (" + inclusionCostCenter + ")";
        }
        sql                                 += " ORDER BY s.date ";
        
        PreparedStatement stmt              = null;
        ResultSet rs                        = null;
        try {
            stmt                            = conn.prepareStatement(sql);
            int fieldCount                  = 0;
            if (type > 0) {
                stmt.setInt(++fieldCount, type);
            }
            stmt.setInt(++fieldCount, station);
            stmt.setTimestamp(++fieldCount, new java.sql.Timestamp(period.getStartDate().getTime()));
            stmt.setTimestamp(++fieldCount, new java.sql.Timestamp(period.getEndDate().getTime()));
            logger.debug("Executing getResultsByStation query");
            long startMillis                = System.currentTimeMillis();
            rs = stmt.executeQuery();
            long elapsedTime                = System.currentTimeMillis()-startMillis;
            logger.debug("Query complete took "+elapsedTime+" ms");
        } catch (SQLException sqle) {
            logger.dbError("Error in getResultsByStations "+sqle.getMessage());
            throw new HandlerException(sqle);
        }
        return new SalesResults(period,rs,stmt);
    }
    
    /**
     * Get the next reading.
     */
    public boolean next() throws SQLException {
        boolean next = (null != rs && rs.next());
        if (next) {
            plu = rs.getString(1);
            d = new Date(rs.getTimestamp(2).getTime());
            value = rs.getDouble(3);
            location = rs.getInt(4);
            costCenter = rs.getInt(5);
        } 
        return next;
    }
    
    public String getPlu() {
        return plu;
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

    public int getCostCenter()
    {
        return costCenter;
    }

    public int getLocation()
    {
        return location;
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
