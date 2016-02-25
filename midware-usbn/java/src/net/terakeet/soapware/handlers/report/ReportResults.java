/*
 * ReportResults.java
 *
 * Created on September 1, 2005, 1:51 PM
 *
 */

package net.terakeet.soapware.handlers.report;
import net.terakeet.soapware.RegisteredConnection;
import net.terakeet.soapware.HandlerException;
import net.terakeet.soapware.Handler;
import net.terakeet.util.MidwareLogger;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.PreparedStatement;
import java.util.Date;
import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;
import net.terakeet.soapware.handlers.ReportHandler;

/**
 *  A wrapper class for a java.sql.ResultSet
 * @author Administrator
 */
public class ReportResults {
    
    private static MidwareLogger logger = new MidwareLogger(ReportResults.class.getName());
    private static Map<Integer,ProductData> lineCache = null;
    
    private ReportPeriod period;
    private ResultSet rs = null;
    private PreparedStatement stmt = null;
    
    private int line;
    private double value;
    private double quantity;
    private Date d;
    
    /**
     * Creates a new instance of ReportResults
     */
    private ReportResults (ReportPeriod period, boolean lineCleaning, String lineString, RegisteredConnection conn) throws HandlerException {
        this.period                         = period;
        try {
    
            String sql                      = "SELECT line, date, value, quantity FROM reading WHERE date BETWEEN ? AND ? AND type = ? " +
                                            " AND line IN (" + lineString + ") ORDER BY date";
            stmt                            = conn.prepareStatement(sql);
            stmt.setTimestamp(1, new java.sql.Timestamp(period.getStartDate().getTime()));
            stmt.setTimestamp(2, new java.sql.Timestamp(period.getEndDate().getTime()));
            if (lineCleaning) {
                stmt.setInt(3, 1);
            } else {
                stmt.setInt(3, 0);
            }
            rs                              = stmt.executeQuery();
            logger.debug("Report Query Complete");
        } catch (SQLException sqle) {
            throw new HandlerException(sqle);
        }
    }
    
    private ReportResults(ReportPeriod p, ResultSet results, PreparedStatement st) throws HandlerException {
        this.period = p;
        this.rs = results;
        this.stmt = st;
    }
    
    /**
     * Factory method to create a new result for a given line id.
     */
    public static ReportResults getResultsByLine(ReportPeriod period, int type, boolean lineCleaning, int line, RegisteredConnection conn) throws HandlerException {
        String subQuery = String.valueOf(line);
        return new ReportResults(period, lineCleaning, subQuery, conn);
    }

    /**
     * Factory method to create a new result for a given location id.
     *  product is optional, and will be ignored if <= 0
     */
    public static ReportResults getResultsByLineString(ReportPeriod period, boolean byProduct, boolean lineCleaning, String lines, RegisteredConnection conn)
            throws HandlerException {
        String sql                          = " SELECT line, date, value, quantity FROM reading WHERE line IN (" + lines + ") AND date BETWEEN ? AND ? AND type = ? " +
                                            " ORDER BY line, date ";
        PreparedStatement stmt              = null;
        ResultSet rs                        = null;
        try {
            stmt                            = conn.prepareStatement(sql);
            int fieldCount                  = 0;
            stmt.setTimestamp(++fieldCount, new java.sql.Timestamp(period.getStartDate().getTime()));
            stmt.setTimestamp(++fieldCount, new java.sql.Timestamp(period.getEndDate().getTime()));
            if (lineCleaning) {
                stmt.setInt(++fieldCount, 1);
            } else {
                stmt.setInt(++fieldCount, 0);
            }
            logger.debug("Executing getResultsByLineString query");
            long startMillis                = System.currentTimeMillis();
            rs                              = stmt.executeQuery();
            if (byProduct) {
                setProductDataByLines(lines, conn);
            }
            long elapsedTime                = System.currentTimeMillis()-startMillis;
            logger.debug("Query complete took "+elapsedTime+" ms");
        } catch (SQLException sqle) {
            logger.dbError("Error in getResultsByLineString "+sqle.getMessage());
            throw new HandlerException(sqle);
        }
        return new ReportResults(period,rs,stmt);
    }
    
     
    /**
     * Factory method to create a new result for a given zone id.
     */
    public static ReportResults getResultsByZone(ReportPeriod period, int type, boolean lineCleaning,
            int zone, int product, RegisteredConnection conn)
            throws HandlerException {
        String barType                      = "";
        if (type > 0) {
            barType                         = " b.type = ? AND ";
        }
        String sql =
                " SELECT line, date, value, quantity " +
                " FROM reading" +
                " WHERE line IN (SELECT l.id" +
                " FROM line l" +
                " LEFT JOIN bar b ON b.id = l.bar" +
                " WHERE " + barType + " b.zone = ?";
        if (product > 0) {
            sql+=" AND l.product=? ";
        }
        sql +=  ") AND date BETWEEN ? AND ? AND type = ? " +
                " ORDER BY line, date ";
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            stmt = conn.prepareStatement(sql);
            int fieldCount = 0;
            stmt.setInt(++fieldCount, zone);
            if (product > 0) {
                stmt.setInt(++fieldCount, product);
            }
            stmt.setTimestamp(++fieldCount, new java.sql.Timestamp(period.getStartDate().getTime()));
            stmt.setTimestamp(++fieldCount, new java.sql.Timestamp(period.getEndDate().getTime()));
            if (lineCleaning) {
                stmt.setInt(++fieldCount, 1);
            } else {
                stmt.setInt(++fieldCount, 0);
            }
            logger.debug("Executing getResultsByZone query");
            long startMillis = System.currentTimeMillis();
            rs = stmt.executeQuery();
            long elapsedTime = System.currentTimeMillis()-startMillis;
            logger.debug("Query complete took "+elapsedTime+" ms");
        } catch (SQLException sqle) {
            logger.dbError("Error in getResultsByZone "+sqle.getMessage());
            throw new HandlerException(sqle);
        }
        
        return new ReportResults(period,rs,stmt);
    }
    
     /**
     * Factory method to create a new result for a given zone id.
     */
    public static ReportResults getResultsByStation(ReportPeriod period, int type, boolean lineCleaning,
            int station, int product, RegisteredConnection conn)
            throws HandlerException {
        String sql =
                " SELECT line, date, value, quantity " +
                " FROM reading" +
                " WHERE line IN (SELECT l.id" +
                " FROM line l" +
                " WHERE l.station = ?";
        if (product > 0) {
            sql+=" AND l.product=? ";
        }
        sql +=  ") AND date BETWEEN ? AND ? AND type = ? " +
                " ORDER BY line, date ";
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            stmt = conn.prepareStatement(sql);
            int fieldCount = 0;
            stmt.setInt(++fieldCount, station);
            if (product > 0) {
                stmt.setInt(++fieldCount, product);
            }
            stmt.setTimestamp(++fieldCount, new java.sql.Timestamp(period.getStartDate().getTime()));
            stmt.setTimestamp(++fieldCount, new java.sql.Timestamp(period.getEndDate().getTime()));
            if (lineCleaning) {
                stmt.setInt(++fieldCount, 1);
            } else {
                stmt.setInt(++fieldCount, 0);
            }
            logger.debug("Executing getResultsByStation query");
            long startMillis = System.currentTimeMillis();
            rs = stmt.executeQuery();
            long elapsedTime = System.currentTimeMillis()-startMillis;
            logger.debug("Query complete took "+elapsedTime+" ms");
        } catch (SQLException sqle) {
            logger.dbError("Error in getResultsByStation "+sqle.getMessage());
            throw new HandlerException(sqle);
        }
        
        return new ReportResults(period,rs,stmt);
    }
    
    /**
     * Factory method to create a new result for a given bar id.
     */
    public static ReportResults getResultsByBar(ReportPeriod period, int type, boolean lineCleaning,
            int bar, int product, RegisteredConnection conn) throws HandlerException {
        int year                            = period.getEndDate().getYear() + 1900;
        String barType                      = "";
        if (type > 0) {
            barType                         = " b.type = ? AND ";
        }
        String sql =
                " SELECT line, date, value, quantity " +
                " FROM reading " +
                " WHERE line IN (SELECT l.id" +
                " FROM line l" +
                " LEFT JOIN bar b ON b.id = l.bar" +
                " WHERE " + barType + " b.id = ?";
        if (product > 0) {
            sql+=" AND l.product=? ";
        }
        sql +=  ") AND date BETWEEN ? AND ? AND type = ? " +
                " ORDER BY line, date ";
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            stmt = conn.prepareStatement(sql);
            int fieldCount = 0;
            if (type > 0) {
                stmt.setInt(++fieldCount, type);
            }
            stmt.setInt(++fieldCount, bar);
            if (product > 0) {
                stmt.setInt(++fieldCount, product);
            }
            stmt.setTimestamp(++fieldCount, new java.sql.Timestamp(period.getStartDate().getTime()));
            stmt.setTimestamp(++fieldCount, new java.sql.Timestamp(period.getEndDate().getTime()));
            if (lineCleaning) {
                stmt.setInt(++fieldCount, 1);
            } else {
                stmt.setInt(++fieldCount, 0);
            }
            logger.debug("Executing getResultsByBar query");
            long startMillis = System.currentTimeMillis();
            rs = stmt.executeQuery();
            long elapsedTime = System.currentTimeMillis()-startMillis;
            logger.debug("Query complete took "+elapsedTime+" ms");
        } catch (SQLException sqle) {
            logger.dbError("Error in getResultsByBar "+sqle.getMessage());
            throw new HandlerException(sqle);
        }
        
        return new ReportResults(period,rs,stmt);
    }
    
    /**
     * Factory method to create a new result for a given location id.
     *  product is optional, and will be ignored if <= 0
     */
    public static ReportResults getResultsByLocation(ReportPeriod period, int type, boolean byProduct, boolean lineCleaning,
            int location, int product, RegisteredConnection conn) throws HandlerException {
        int year                            = period.getEndDate().getYear() + 1900;
        String barType                      = "";
        if (type > 0) {
            barType                         = " b.type = ? AND ";
        }
        String sql = 
                " SELECT line, date, value, quantity " +
                " FROM reading " +
                " WHERE line IN (SELECT l.id" +
                " FROM line l" +
                " LEFT JOIN bar b ON b.id = l.bar" +
                " LEFT JOIN location lo ON lo.id = b.location" +
                " WHERE " + barType + " lo.id = ?";
        if (product > 0) {
            sql+=" AND l.product=? ";
        }
        sql +=  ") AND date BETWEEN ? AND ? AND type = ? " +
                " ORDER BY line, date ";
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            stmt = conn.prepareStatement(sql);
            int fieldCount = 0;
            if (type > 0) {
                stmt.setInt(++fieldCount, type);
            }
            stmt.setInt(++fieldCount, location);
            if (product > 0) {
                stmt.setInt(++fieldCount, product);
            }
            stmt.setTimestamp(++fieldCount, new java.sql.Timestamp(period.getStartDate().getTime()));
            stmt.setTimestamp(++fieldCount, new java.sql.Timestamp(period.getEndDate().getTime()));
            if (lineCleaning) {
                stmt.setInt(++fieldCount, 1);
            } else {
                stmt.setInt(++fieldCount, 0);
            }
            logger.debug("Executing getResultsByLocation query");
            long startMillis = System.currentTimeMillis();
           // logger.debug("Executing:"+sql);
            rs = stmt.executeQuery();            
            if (byProduct) {
                setProductDataByLocation(2, location, conn);
            }
           
            long elapsedTime = System.currentTimeMillis()-startMillis;
            logger.debug("Query complete took "+elapsedTime+" ms");
        } catch (SQLException sqle) {
            logger.dbError("Error in getResultsByLocation "+sqle.getMessage());
            throw new HandlerException(sqle);
        }
        
        return new ReportResults(period,rs,stmt);
    }
    
    /**
     * Factory method to create a new result for a given customer id.
     *  product is optional, and will be ignored if <= 0
     */   
     public static ReportResults getResultsByCustomer(ReportPeriod period, int type, boolean byProduct, boolean lineCleaning, int customer, String specificLocations, int product, RegisteredConnection conn) throws HandlerException {
        String barType                      = "";
        int year                            = period.getEndDate().getYear() + 1900;
        if (type > 0) {
            barType                         = " b.type = ? AND ";
        }
        String sql = 
                " SELECT line, date, value, quantity " +
                " FROM reading " +
                " WHERE line IN (SELECT l.id" +
                " FROM line l" +
                " LEFT JOIN bar b ON b.id = l.bar" +
                " LEFT JOIN location lo ON lo.id = b.location LEFT JOIN locationDetails lD ON lD.location =lo.id " +
                " WHERE " + barType + " lo.customer = ? AND lD.active=1";
        
        if (!(specificLocations == null || specificLocations.equals(""))) {
            sql += " AND lo.id IN ( " + specificLocations + " ) ";
        }
        
        if (product > 0) {
            sql+=" AND l.product=? ";
        }
        sql +=  " ) AND date BETWEEN ? AND ? AND type = ? " +
                " ORDER BY line, date";
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            stmt = conn.prepareStatement(sql);
            int fieldCount = 0;
            if (type > 0) {
                stmt.setInt(++fieldCount, type);
            }
            stmt.setInt(++fieldCount, customer);
            if (product > 0) {
                stmt.setInt(++fieldCount, product);
            }
            stmt.setTimestamp(++fieldCount, new java.sql.Timestamp(period.getStartDate().getTime()));
            stmt.setTimestamp(++fieldCount, new java.sql.Timestamp(period.getEndDate().getTime()));
            if (lineCleaning) {
                stmt.setInt(++fieldCount, 1);
            } else {
                stmt.setInt(++fieldCount, 0);
            }
            logger.debug("Executing getResultsByCustomer query");
            long startMillis = System.currentTimeMillis();
            rs = stmt.executeQuery();
            if (byProduct) {
                setProductDataByLocation(1, customer, conn);
            }
            long elapsedTime = System.currentTimeMillis()-startMillis;
            logger.debug("Query complete took "+elapsedTime+" ms");
        } catch (SQLException sqle) {
            logger.dbError("Error in getResultsByCustomer "+sqle.getMessage());
            throw new HandlerException(sqle);
        }
        
        return new ReportResults(period,rs,stmt);
    }

    /**
     * Factory method to create a new result for a given customer id.
     *  product is optional, and will be ignored if <= 0
     */
     public static ReportResults getResultsBySpecificLocations(ReportPeriod period, int type, boolean byProduct, boolean lineCleaning, String specificLocations,
             int product, RegisteredConnection conn) throws HandlerException {
        String barType                      = "";
        if (type > 0) {
            barType                         = " b.type = ? AND ";
        }
        String sql =
                " SELECT line, date, value, quantity " +
                " FROM reading" +
                " WHERE line IN (SELECT l.id" +
                " FROM line l" +
                " LEFT JOIN bar b ON b.id = l.bar" +
                " WHERE " + barType + " b.location IN ( " + specificLocations + " ) ";
        if (product > 0) {
            sql+=" AND l.product=? ";
        }
        sql +=  " ) AND date BETWEEN ? AND ? AND type = ? " +
                " ORDER BY line, date";
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            stmt = conn.prepareStatement(sql);
            int fieldCount = 0;
            if (type > 0) {
                stmt.setInt(++fieldCount, type);
            }
            if (product > 0) {
                stmt.setInt(++fieldCount, product);
            }
            stmt.setTimestamp(++fieldCount, new java.sql.Timestamp(period.getStartDate().getTime()));
            stmt.setTimestamp(++fieldCount, new java.sql.Timestamp(period.getEndDate().getTime()));
            if (lineCleaning) {
                stmt.setInt(++fieldCount, 1);
            } else {
                stmt.setInt(++fieldCount, 0);
            }
            logger.debug("Executing getResultsBySpecificLocations query");
            long startMillis = System.currentTimeMillis();
            rs = stmt.executeQuery();
            if (byProduct) {
                setProductDataBySpecificLocations(specificLocations, conn);
            }
            long elapsedTime = System.currentTimeMillis()-startMillis;
            logger.debug("Query complete took "+elapsedTime+" ms");
        } catch (SQLException sqle) {
            logger.dbError("Error in getResultsByCustomer "+sqle.getMessage());
            throw new HandlerException(sqle);
        }

        return new ReportResults(period,rs,stmt);
    }

    /**
     * Factory method to create a new result for a given location id.
     *  product is optional, and will be ignored if <= 0
     */
    public static ReportResults getResultsByUserRegion(ReportPeriod period, int type, boolean lineCleaning,
            int user, int product, RegisteredConnection conn) throws HandlerException {
        String barType                      = "";
        if (type > 0) {
            barType                         = " b.type = ? AND ";
        }
        String sql =
                " SELECT line, date, value, quantity " +
                " FROM reading" +
                " WHERE line IN (SELECT l.id " +
                " FROM line l LEFT JOIN bar b ON b.id = l.bar " +
                " LEFT JOIN location lo ON lo.id = b.location " +
                " LEFT JOIN regionCountyMap rCM ON rCM.county = lo.countyIndex " +
                " LEFT JOIN userRegionMap uRM ON uRM.region = rCM.region " +
                " WHERE " + barType + " uRM.user = ? ";
        if (product > 0) {
            sql+=" AND l.product=? ";
        }
        sql +=  ") AND date BETWEEN ? AND ? AND type = ? " +
                " ORDER BY line, date ";
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            stmt = conn.prepareStatement(sql);
            int fieldCount = 0;
            if (type > 0) {
                stmt.setInt(++fieldCount, type);
            }
            stmt.setInt(++fieldCount, user);
            if (product > 0) {
                stmt.setInt(++fieldCount, product);
            }
            stmt.setTimestamp(++fieldCount, new java.sql.Timestamp(period.getStartDate().getTime()));
            stmt.setTimestamp(++fieldCount, new java.sql.Timestamp(period.getEndDate().getTime()));
            if (lineCleaning) {
                stmt.setInt(++fieldCount, 1);
            } else {
                stmt.setInt(++fieldCount, 0);
            }
            logger.debug("Executing getResultsByUserRegion query");
            long startMillis = System.currentTimeMillis();
            rs = stmt.executeQuery();
            long elapsedTime = System.currentTimeMillis()-startMillis;
            logger.debug("Query complete took "+elapsedTime+" ms");
        } catch (SQLException sqle) {
            logger.dbError("Error in getResultsByUserRegion "+sqle.getMessage());
            throw new HandlerException(sqle);
        }

        return new ReportResults(period,rs,stmt);
    }


    /**
     * Factory method to create a new result for a given zone id.
     */
    public static ReportResults getResultsByZoneConcessions(ReportPeriod period, int type, boolean lineCleaning, int zone, int product, String exclusionLines, String inclusionLines, RegisteredConnection conn)
            throws HandlerException {
        String barType                      = "";
        if (type > 0) {
            barType                         = " b.type = ? AND ";
        }
        String sql =
                " SELECT line, date, value, quantity " +
                " FROM reading" +
                " WHERE line IN (SELECT l.id" +
                " FROM line l" +
                " LEFT JOIN bar b ON b.id = l.bar" +
                " WHERE " + barType + " b.zone = ?";
        if (product > 0) {
            sql+=" AND l.product=? ";
        }
        sql +=  ") AND date BETWEEN ? AND ? AND type = ? ";
        if (exclusionLines.length() > 0) {
            sql                             += " AND line NOT IN (" + exclusionLines + ")";
        }
        if (inclusionLines.length() > 0) {
            sql                             += " AND line IN (" + inclusionLines + ")";
        }
        sql +=  " ORDER BY line, date ";
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            stmt = conn.prepareStatement(sql);
            int fieldCount = 0;
            stmt.setInt(++fieldCount, zone);
            if (product > 0) {
                stmt.setInt(++fieldCount, product);
            }
            stmt.setTimestamp(++fieldCount, new java.sql.Timestamp(period.getStartDate().getTime()));
            stmt.setTimestamp(++fieldCount, new java.sql.Timestamp(period.getEndDate().getTime()));
            if (lineCleaning) {
                stmt.setInt(++fieldCount, 1);
            } else {
                stmt.setInt(++fieldCount, 0);
            }
            logger.debug("Executing getResultsByZone query");
            long startMillis = System.currentTimeMillis();
            rs = stmt.executeQuery();
            long elapsedTime = System.currentTimeMillis()-startMillis;
            logger.debug("Query complete took "+elapsedTime+" ms");
        } catch (SQLException sqle) {
            logger.dbError("Error in getResultsByZone "+sqle.getMessage());
            throw new HandlerException(sqle);
        }

        return new ReportResults(period,rs,stmt);
    }

     /**
     * Factory method to create a new result for a given zone id.
     */
    public static ReportResults getResultsByStationConcessions(ReportPeriod period, int type, boolean lineCleaning, int station, int product, String exclusionLines, String inclusionLines, RegisteredConnection conn)
            throws HandlerException {
        String sql =
                " SELECT line, date, value, quantity " +
                " FROM reading" +
                " WHERE line IN (SELECT l.id" +
                " FROM line l" +
                " WHERE l.station = ?";
        if (product > 0) {
            sql+=" AND l.product=? ";
        }
        sql +=  ") AND date BETWEEN ? AND ? AND type = ? ";
        if (exclusionLines.length() > 0) {
            sql                             += " AND line NOT IN (" + exclusionLines + ")";
        }
        if (inclusionLines.length() > 0) {
            sql                             += " AND line IN (" + inclusionLines + ")";
        }
        sql +=  " ORDER BY line, date ";
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            stmt = conn.prepareStatement(sql);
            int fieldCount = 0;
            stmt.setInt(++fieldCount, station);
            if (product > 0) {
                stmt.setInt(++fieldCount, product);
            }
            stmt.setTimestamp(++fieldCount, new java.sql.Timestamp(period.getStartDate().getTime()));
            stmt.setTimestamp(++fieldCount, new java.sql.Timestamp(period.getEndDate().getTime()));
            if (lineCleaning) {
                stmt.setInt(++fieldCount, 1);
            } else {
                stmt.setInt(++fieldCount, 0);
            }
            logger.debug("Executing getResultsByStation query");
            long startMillis = System.currentTimeMillis();
            rs = stmt.executeQuery();
            long elapsedTime = System.currentTimeMillis()-startMillis;
            logger.debug("Query complete took "+elapsedTime+" ms");
        } catch (SQLException sqle) {
            logger.dbError("Error in getResultsByStation "+sqle.getMessage());
            throw new HandlerException(sqle);
        }

        return new ReportResults(period,rs,stmt);
    }

    /**
     * Factory method to create a new result for a given bar id.
     */
    public static ReportResults getResultsByBarConcessions(ReportPeriod period, int type, boolean lineCleaning, int bar, int product, String exclusionLines, String inclusionLines, RegisteredConnection conn) throws HandlerException {
        int year                            = period.getEndDate().getYear() + 1900;
        String barType                      = "";
        if (type > 0) {
            barType                         = " b.type = ? AND ";
        }
        String sql =
                " SELECT line, date, value, quantity " +
                " FROM reading " +
                " WHERE line IN (SELECT l.id" +
                " FROM line l" +
                " LEFT JOIN bar b ON b.id = l.bar" +
                " WHERE " + barType + " b.id = ?";
        if (product > 0) {
            sql+=" AND l.product=? ";
        }
        sql +=  ") AND date BETWEEN ? AND ? AND type = ? ";
        if (exclusionLines.length() > 0) {
            sql                             += " AND line NOT IN (" + exclusionLines + ")";
        }
        if (inclusionLines.length() > 0) {
            sql                             += " AND line IN (" + inclusionLines + ")";
        }
        sql +=  " ORDER BY line, date ";
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            stmt = conn.prepareStatement(sql);
            int fieldCount = 0;
            if (type > 0) {
                stmt.setInt(++fieldCount, type);
            }
            stmt.setInt(++fieldCount, bar);
            if (product > 0) {
                stmt.setInt(++fieldCount, product);
            }
            stmt.setTimestamp(++fieldCount, new java.sql.Timestamp(period.getStartDate().getTime()));
            stmt.setTimestamp(++fieldCount, new java.sql.Timestamp(period.getEndDate().getTime()));
            if (lineCleaning) {
                stmt.setInt(++fieldCount, 1);
            } else {
                stmt.setInt(++fieldCount, 0);
            }
            logger.debug("Executing getResultsByBar query");
            long startMillis = System.currentTimeMillis();
            rs = stmt.executeQuery();
            long elapsedTime = System.currentTimeMillis()-startMillis;
            logger.debug("Query complete took "+elapsedTime+" ms");
        } catch (SQLException sqle) {
            logger.dbError("Error in getResultsByBar "+sqle.getMessage());
            throw new HandlerException(sqle);
        }

        return new ReportResults(period,rs,stmt);
    }

    /**
     * Factory method to create a new result for a given location id.
     *  product is optional, and will be ignored if <= 0
     */
    public static ReportResults getResultsByLocationConcessions(ReportPeriod period, int type, boolean byProduct, boolean lineCleaning, int location, int product, String exclusionLines, String inclusionLines, RegisteredConnection conn) throws HandlerException {
        int year                            = period.getEndDate().getYear() + 1900;
        String barType                      = "";
        if (type > 0) {
            barType                         = " b.type = ? AND ";
        }
        String sql =
                " SELECT line, date, value, quantity " +
                " FROM reading " +
                " WHERE line IN (SELECT l.id" +
                " FROM line l" +
                " LEFT JOIN bar b ON b.id = l.bar" +
                " LEFT JOIN location lo ON lo.id = b.location" +
                " WHERE " + barType + " lo.id = ?";
        if (product > 0) {
            sql+=" AND l.product=? ";
        }
        sql +=  ") AND date BETWEEN ? AND ? AND type = ? ";
        if (exclusionLines.length() > 0) {
            sql                             += " AND line NOT IN (" + exclusionLines + ")";
        }
        if (inclusionLines.length() > 0) {
            sql                             += " AND line IN (" + inclusionLines + ")";
        }
        sql +=  " ORDER BY line, date ";
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            stmt = conn.prepareStatement(sql);
            int fieldCount = 0;
            if (type > 0) {
                stmt.setInt(++fieldCount, type);
            }
            stmt.setInt(++fieldCount, location);
            if (product > 0) {
                stmt.setInt(++fieldCount, product);
            }
            stmt.setTimestamp(++fieldCount, new java.sql.Timestamp(period.getStartDate().getTime()));
            stmt.setTimestamp(++fieldCount, new java.sql.Timestamp(period.getEndDate().getTime()));
            if (lineCleaning) {
                stmt.setInt(++fieldCount, 1);
            } else {
                stmt.setInt(++fieldCount, 0);
            }
            logger.debug("Executing getResultsByLocation query");
            long startMillis = System.currentTimeMillis();
            rs = stmt.executeQuery();
            if (byProduct) {
                setProductDataByLocation(2, location, conn);
            }
            long elapsedTime = System.currentTimeMillis()-startMillis;
            logger.debug("Query complete took "+elapsedTime+" ms");
        } catch (SQLException sqle) {
            logger.dbError("Error in getResultsByLocation "+sqle.getMessage());
            throw new HandlerException(sqle);
        }

        return new ReportResults(period,rs,stmt);
    }
    
    /**
     *  Looks up the name of the product that corresponds to the lineId
     */
    public static String lineToProduct(int lineId, RegisteredConnection conn) {
        String result = "Unknown";
        ResultSet rSet = null;
        PreparedStatement s = null;
        try {
            String sql = "SELECT product.name FROM line LEFT JOIN product ON line.product = product.id" +
                    " WHERE line.id=?";
            s = conn.prepareStatement(sql);
            s.setInt(1,lineId);
            rSet = s.executeQuery();
            if (rSet.next()) {
                result = rSet.getString(1);
            }
        } catch (Exception e) {
            logger.dbError(e.toString());
        } finally {
            try {rSet.close();} catch (Exception e) {}
            try {s.close();} catch (Exception e) {}
        }
        return result;
    }
    
   
    public static ProductData findProduct(Integer lineId, RegisteredConnection conn) {
        ProductData product = ProductData.getUnknownInstance();
        //logger.debug("Finding product for line "+lineId);
        try {
            product = checkCache(lineId);
            if (product == null) { //need to go to database
                String sql = "SELECT product.id, product.name, line.unit FROM line LEFT JOIN product ON line.product = product.id" +
                    " WHERE line.id=?";
                PreparedStatement s = conn.prepareStatement(sql);
                s.setInt(1,lineId);
                ResultSet rSet = s.executeQuery();
                if (rSet.next()) {
                    product = new ProductData(rSet.getInt(1), 0.0, rSet.getString(2), rSet.getDouble(3));
                } else {
                    product = ProductData.getUnknownInstance();
                }
                try {rSet.close();} catch (Exception e) {}
                try {s.close();} catch (Exception e) {}
                putCache(lineId,product);
            }
        } catch (Exception e) {
            logger.dbError("Exception in findProduct: "+e.toString());
            //TODO: handle exception
        }
        return product;
    }
    
    
  

    public static ProductData setProductDataByLocation(Integer paramLevel, Integer parameter, RegisteredConnection conn) {
        ProductData product                 = ProductData.getUnknownInstance();
        logger.debug("Finding product for parameter: "+parameter);
        try {
            String sql                      = " SELECT product.id, product.name, line.unit, line.id FROM line LEFT JOIN product ON line.product = product.id LEFT JOIN bar ON bar.id = line.bar ";
            if (paramLevel == 1) {
                sql                         += " LEFT JOIN location ON location.id = bar.location WHERE location.customer=?";
            } else {
                sql                         += " WHERE bar.location=?";
            }
            
            PreparedStatement s             = conn.prepareStatement(sql);
            s.setInt(1,parameter);
            ResultSet rSet                  = s.executeQuery();
            while (rSet.next()) {
                product = new ProductData(rSet.getInt(1), 0.0, rSet.getString(2), rSet.getDouble(3));
                putCache(rSet.getInt(4),product);
            }
            try {rSet.close();} catch (Exception e) {}
            try {s.close();} catch (Exception e) {}
        } catch (Exception e) {
            logger.dbError("Exception in setProductDataByLocation: "+e.toString());
            //TODO: handle exception
        }
        return product;
    }

    public static ProductData setProductDataByLines(String lines, RegisteredConnection conn) {
        ProductData product                 = ProductData.getUnknownInstance();
        try {
            String sql                      = " SELECT product.id, product.name, line.unit, line.id FROM line LEFT JOIN product ON line.product = product.id WHERE line.id IN (" + lines + ")";
            PreparedStatement s             = conn.prepareStatement(sql);
            ResultSet rSet                  = s.executeQuery();
            while (rSet.next()) {
                product                     = new ProductData(rSet.getInt(1), 0.0, rSet.getString(2), rSet.getDouble(3));
                putCache(rSet.getInt(4),product);
            }
            try {rSet.close();} catch (Exception e) {}
            try {s.close();} catch (Exception e) {}
        } catch (Exception e) {
            logger.dbError("Exception in setProductDataByLines: "+e.toString());
            //TODO: handle exception
        }
        return product;
    }


    public static ProductData setProductDataBySpecificLocations(String specificLocations, RegisteredConnection conn) {
        ProductData product                 = ProductData.getUnknownInstance();
        try {
            String sql                      = " SELECT product.id, product.name, line.unit, line.id FROM line LEFT JOIN product ON line.product = product.id " +
                                            " LEFT JOIN bar ON bar.id = line.bar WHERE bar.location IN (" + specificLocations + ")";

            PreparedStatement s             = conn.prepareStatement(sql);
            ResultSet rSet                  = s.executeQuery();
            while (rSet.next()) {
                product = new ProductData(rSet.getInt(1), 0.0, rSet.getString(2), rSet.getDouble(3));
                putCache(rSet.getInt(4),product);
            }
            try {rSet.close();} catch (Exception e) {}
            try {s.close();} catch (Exception e) {}
        } catch (Exception e) {
            logger.dbError("Exception in setProductDataBySpecificLocations: "+e.toString());
            //TODO: handle exception
        }
        return product;
    }
    
    /**  Consolidates line-readings in product-readings.
     *  Takes a map of (line-ids, ouncesPoured), and returns a map of (product-id, productData)
     *  Product data contains the product name, and the ounces poured.  If multiple lines map
     *  to the same product, one product record will be returned with a sum of those lines.
     */
    public static Map<Integer,ProductData> linesToProducts(Map<Integer, String> locationMap, Map<Integer,Double> lineMap, RegisteredConnection conn) {
        Map<Integer,ProductData> result     = new HashMap<Integer,ProductData>(lineMap.size());
        try {
            for (Integer key : lineMap.keySet() ) {
                try {
                    
                    int locationId = Integer.valueOf(locationMap.get(key).toString().split("\\|")[0]);
                               
                    ProductData product = findProduct(key,conn);
                    // now we have a non-null product, either from the cache or the db
                    // check the previous value in our result set, if any
                    ProductData previousRecord = result.get(product.getId());
                    double previousPoured = 0.0;
                    if (previousRecord != null) {
                        previousPoured = previousRecord.getPoured();
                    }
                    //build the new record
                    ProductData newRecord = new ProductData(locationId,
                            product.getId(),
                            previousPoured + lineMap.get(key).doubleValue(),
                            product.getName(),
                            product.getUnits()
                            );
                    result.put(newRecord.getId(),newRecord);

                } catch (Exception e) {
                    logger.generalWarning("Exception in linesToProduct: "+e.toString());
                    //TODO: report exception
                }
            }
        } catch (Exception sqle) {
            logger.dbError("SQL Exception in linesToProduct "+sqle.toString());
        } 
        return result;
    }
    
    private static ProductData checkCache(Integer line) {
        ProductData result = null;
        if (lineCache != null ) {
            result = lineCache.get(line);
        }
        return result;
    }
    
    private static void putCache(Integer line, ProductData product) {
        if (line == null) { throw new NullPointerException("line arg to putCache is null"); }
        if (product == null) { throw new NullPointerException("product arg to putCache is null"); }
        
        if (lineCache == null) {
            lineCache = new HashMap<Integer,ProductData>();
        }
        lineCache.put(line,product);
    }
    
    public static void clearLineCache() {
        lineCache = null;
    }
    
    /**
     * Get the next reading.
     */
    public boolean next() throws SQLException {
        boolean next = (null != rs && rs.next());
        if (next) {
            line = rs.getInt(1);
            d = new Date(rs.getTimestamp(2).getTime());
            value = rs.getDouble(3);
            quantity = rs.getDouble(4);
            //logger.debug("getting next reading");
        } else {
            //logger.debug("no more readings");
        }
        return next;
    }

    /**
     * Remove current reading.
     */
    public void remove() throws SQLException {
        boolean currentRow = (null != rs);
        if (currentRow) {
            rs.deleteRow();
            //logger.debug("getting next reading");
        } 
    }
    
    /**
     * Get the line id of the current reading.
     */
    public int getLine() {
        return line;
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
     * Get the value of the current reading.
     */
    public double getQuantity() {
        return quantity;
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
