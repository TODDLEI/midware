/*
 * LineCleaningReportResults.java
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
import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;

/**
 *  A wrapper class for a java.sql.ResultSet
 * @author Administrator
 */
public class LineCleaningReportResults {
    
    private static MidwareLogger logger = new MidwareLogger(LineCleaningReportResults.class.getName());
    private static Map<Integer,ProductData> lineCache = null;
    
    private ReportPeriod period;
    private ResultSet rs = null;
    private PreparedStatement stmt = null;
    
    private int line;
    private double value;
    private Date d;
    
    /**
     * Creates a new instance of LineCleaningReportResults
     */
    private LineCleaningReportResults(ReportPeriod period, String subQuery, RegisteredConnection conn)
    throws HandlerException {
        this.period = period;
        try {
    
            String sql = "SELECT line, endDate, value" +
                    " FROM removedReading" +
                    " WHERE endDate BETWEEN ? AND ? " +
                    " AND line IN (" + subQuery + ")" +
                    " ORDER BY line, endDate";
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
    
    private LineCleaningReportResults(ReportPeriod p, ResultSet results, PreparedStatement st) throws HandlerException {
        this.period = p;
        this.rs = results;
        this.stmt = st;
    }
    
    /**
     * Factory method to create a new result for a given line id.
     */
    public static LineCleaningReportResults getResultsByLine(ReportPeriod period,
            int line, RegisteredConnection conn)
            throws HandlerException {
        String subQuery = String.valueOf(line);
        return new LineCleaningReportResults(period, subQuery, conn);
    }
    
     
    /**
     * Factory method to create a new result for a given zone id.
     */
    public static LineCleaningReportResults getResultsByZone(ReportPeriod period,
            int zone, int product, RegisteredConnection conn)
            throws HandlerException {
        String sql = 
                " SELECT r.line,r.endDate,r.value " +
                " FROM zone z LEFT JOIN bar b on z.id = b.zone" +
                " LEFT JOIN line ln ON b.id=ln.bar " +
                " LEFT JOIN removedReading r ON r.line=ln.id " +
                " WHERE z.id=?";
        if (product > 0) {
            sql+=" AND ln.product=? ";
        }
        sql +=  " AND r.endDate BETWEEN ? AND ? " +
                " ORDER BY r.line, r.endDate ";
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
            logger.debug("Executing getResultsByZone query");
            long startMillis = System.currentTimeMillis();
            rs = stmt.executeQuery();
            long elapsedTime = System.currentTimeMillis()-startMillis;
            logger.debug("Query complete took "+elapsedTime+" ms");
        } catch (SQLException sqle) {
            logger.dbError("Error in getResultsByZone "+sqle.getMessage());
            throw new HandlerException(sqle);
        }
        
        return new LineCleaningReportResults(period,rs,stmt);
    }
    
     /**
     * Factory method to create a new result for a given zone id.
     */
    public static LineCleaningReportResults getResultsByStation(ReportPeriod period,
            int station, int product, RegisteredConnection conn)
            throws HandlerException {
        String sql = 
                " SELECT r.line,r.endDate,r.value " +
                " FROM station s" +
                " LEFT JOIN line ln ON s.id=ln.station " +
                " LEFT JOIN removedReading r ON r.line=ln.id " +
                " WHERE s.id=?";
        if (product > 0) {
            sql+=" AND ln.product=? ";
        }
        sql +=  " AND r.endDate BETWEEN ? AND ? " +
                " ORDER BY r.line, r.endDate ";
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
            logger.debug("Executing getResultsByStation query");
            long startMillis = System.currentTimeMillis();
            rs = stmt.executeQuery();
            long elapsedTime = System.currentTimeMillis()-startMillis;
            logger.debug("Query complete took "+elapsedTime+" ms");
        } catch (SQLException sqle) {
            logger.dbError("Error in getResultsByStation "+sqle.getMessage());
            throw new HandlerException(sqle);
        }
        
        return new LineCleaningReportResults(period,rs,stmt);
    }
    
    /**
     * Factory method to create a new result for a given bar id.
     */
    public static LineCleaningReportResults getResultsByBar(ReportPeriod period,
            int bar, int product, RegisteredConnection conn)
            throws HandlerException {
        String sql = 
                " SELECT r.line,r.endDate,r.value " +
                " FROM bar b " +
                " LEFT JOIN line ln ON b.id=ln.bar " +
                " LEFT JOIN removedReading r ON r.line=ln.id " +
                " WHERE b.id=?";
        if (product > 0) {
            sql+=" AND ln.product=? ";
        }
        sql +=  " AND r.endDate BETWEEN ? AND ? " +
                " ORDER BY r.line, r.endDate ";
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            stmt = conn.prepareStatement(sql);
            int fieldCount = 0;
            stmt.setInt(++fieldCount, bar);
            if (product > 0) {
                stmt.setInt(++fieldCount, product);
            }
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
        
        return new LineCleaningReportResults(period,rs,stmt);
    }
    
    /**
     * Factory method to create a new result for a given location id.
     *  product is optional, and will be ignored if <= 0
     */
    public static LineCleaningReportResults getResultsByCounty(ReportPeriod period,
            int county, int product, RegisteredConnection conn)
            throws HandlerException {
        String sql = 
                " SELECT line, endDate, value" +
                " FROM removedReading " +
                " WHERE line IN (SELECT l.id " +
                " FROM line l LEFT JOIN bar b ON b.id = l.bar " +
                " LEFT JOIN location lo ON lo.id = b.location " +
                " WHERE lo.countyIndex = ? ";
        if (product > 0) {
            sql+=" AND l.product = ? ";
        }
        sql +=  ") AND endDate BETWEEN ? AND ? " +
                " ORDER BY line, endDate ";
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            stmt = conn.prepareStatement(sql);
            int fieldCount = 0;
            stmt.setInt(++fieldCount, county);
            if (product > 0) {
                stmt.setInt(++fieldCount, product);
            }
            stmt.setTimestamp(++fieldCount, new java.sql.Timestamp(period.getStartDate().getTime()));
            stmt.setTimestamp(++fieldCount, new java.sql.Timestamp(period.getEndDate().getTime()));
            logger.debug("Executing getResultsByCounty query");
            long startMillis = System.currentTimeMillis();
            rs = stmt.executeQuery();
            long elapsedTime = System.currentTimeMillis()-startMillis;
            logger.debug("Query complete took "+elapsedTime+" ms");
        } catch (SQLException sqle) {
            logger.dbError("Error in getResultsByCounty "+sqle.getMessage());
            throw new HandlerException(sqle);
        }
        
        return new LineCleaningReportResults(period,rs,stmt);
    }
    
    /**
     * Factory method to create a new result for a given location id.
     *  product is optional, and will be ignored if <= 0
     */
    public static LineCleaningReportResults getResultsByRegion(ReportPeriod period,
            int region, int product, RegisteredConnection conn)
            throws HandlerException {
        String sql = 
                " SELECT line, endDate, value" +
                " FROM removedReading" +
                " WHERE line IN (SELECT l.id" +
                " FROM line l LEFT JOIN bar b ON b.id = l.bar " +
                " LEFT JOIN location lo ON lo.id = b.location " +
                " LEFT JOIN regionCountyMap rCM ON rCM.county = lo.countyIndex " +
                " WHERE rCM.region = ?";
        if (product > 0) {
            sql+=" AND l.product=? ";
        }
        sql +=  ") AND endDate BETWEEN ? AND ? " +
                " ORDER BY line, endDate ";
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            stmt = conn.prepareStatement(sql);
            int fieldCount = 0;
            stmt.setInt(++fieldCount, region);
            if (product > 0) {
                stmt.setInt(++fieldCount, product);
            }
            stmt.setTimestamp(++fieldCount, new java.sql.Timestamp(period.getStartDate().getTime()));
            stmt.setTimestamp(++fieldCount, new java.sql.Timestamp(period.getEndDate().getTime()));
            logger.debug("Executing getResultsByRegion query");
            long startMillis = System.currentTimeMillis();
            rs = stmt.executeQuery();
            long elapsedTime = System.currentTimeMillis()-startMillis;
            logger.debug("Query complete took "+elapsedTime+" ms");
        } catch (SQLException sqle) {
            logger.dbError("Error in getResultsByRegion "+sqle.getMessage());
            throw new HandlerException(sqle);
        }
        
        return new LineCleaningReportResults(period,rs,stmt);
    }
    
    /**
     * Factory method to create a new result for a given location id.
     *  product is optional, and will be ignored if <= 0
     */
    public static LineCleaningReportResults getResultsByUserRegion(ReportPeriod period,
            int user, int product, RegisteredConnection conn)
            throws HandlerException {
        String sql = 
                " SELECT line, endDate, value" +
                " FROM removedReading" +
                " WHERE line IN (SELECT l.id " +
                " FROM line l LEFT JOIN bar b ON b.id = l.bar " +
                " LEFT JOIN location lo ON lo.id = b.location " +
                " LEFT JOIN regionCountyMap rCM ON rCM.county = lo.countyIndex " +
                " LEFT JOIN userRegionMap uRM ON uRM.region = rCM.region " +
                " WHERE uRM.user = ? ";
        if (product > 0) {
            sql+=" AND l.product=? ";
        }
        sql +=  ") AND endDate BETWEEN ? AND ? " +
                " ORDER BY line, endDate ";
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            stmt = conn.prepareStatement(sql);
            int fieldCount = 0;
            stmt.setInt(++fieldCount, user);
            if (product > 0) {
                stmt.setInt(++fieldCount, product);
            }
            stmt.setTimestamp(++fieldCount, new java.sql.Timestamp(period.getStartDate().getTime()));
            stmt.setTimestamp(++fieldCount, new java.sql.Timestamp(period.getEndDate().getTime()));
            logger.debug("Executing getResultsByUserRegion query");
            long startMillis = System.currentTimeMillis();
            rs = stmt.executeQuery();
            long elapsedTime = System.currentTimeMillis()-startMillis;
            logger.debug("Query complete took "+elapsedTime+" ms");
        } catch (SQLException sqle) {
            logger.dbError("Error in getResultsByUserRegion "+sqle.getMessage());
            throw new HandlerException(sqle);
        }
        
        return new LineCleaningReportResults(period,rs,stmt);
    }
    
    /**
     * Factory method to create a new result for a given location id.
     *  product is optional, and will be ignored if <= 0
     */
    public static LineCleaningReportResults getResultsByLocation(ReportPeriod period,
            int location, int product, RegisteredConnection conn)
            throws HandlerException {
        String sql = 
                " SELECT line, endDate, value" +
                " FROM removedReading" +
                " WHERE line IN (SELECT l.id" +
                " FROM line l" +
                " LEFT JOIN bar b ON b.id = l.bar" +
                " LEFT JOIN location lo ON lo.id = b.location" +
                " WHERE lo.id = ?";
        if (product > 0) {
            sql+=" AND l.product=? ";
        }
        sql +=  ") AND endDate BETWEEN ? AND ? " +
                " ORDER BY line, endDate ";
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            stmt = conn.prepareStatement(sql);
            int fieldCount = 0;
            stmt.setInt(++fieldCount, location);
            if (product > 0) {
                stmt.setInt(++fieldCount, product);
            }
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
        
        return new LineCleaningReportResults(period,rs,stmt);
    }
    
    /**
     * Factory method to create a new result for a given customer id.
     *  product is optional, and will be ignored if <= 0
     */   
     public static LineCleaningReportResults getResultsByCustomer(ReportPeriod period,
            int customer, int product, RegisteredConnection conn)
            throws HandlerException {
        String sql = 
                " SELECT line, endDate, value" +
                " FROM removedReading" +
                " WHERE line IN (SELECT l.id" +
                " FROM line l" +
                " LEFT JOIN bar b ON b.id = l.bar" +
                " LEFT JOIN location lo ON lo.id = b.location" +
                " WHERE lo.customer = ?";
        
        if (product > 0) {
            sql+=" AND l.product=? ";
        }
        sql +=  ") AND endDate BETWEEN ? AND ? " +
                " ORDER BY line, endDate";
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            stmt = conn.prepareStatement(sql);
            int fieldCount = 0;
            stmt.setInt(++fieldCount, customer);
            if (product > 0) {
                stmt.setInt(++fieldCount, product);
            }
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
        
        return new LineCleaningReportResults(period,rs,stmt);
    }
    
    /**
     * Factory method to create a new result for a given customer id.
     *  product is optional, and will be ignored if <= 0
     */   
     public static LineCleaningReportResults getResultsBySupplier(ReportPeriod period,
            int supplier, int product, RegisteredConnection conn)
            throws HandlerException {
        String sql = 
                " SELECT r.line,r.endDate,r.value " +
                " FROM supplier s LEFT JOIN supplierAddress sA ON s.id = sA.supplier " +
                " LEFT JOIN locationSupplier lS on lS.address = sA.id " +
                " LEFT JOIN location lo ON lo.id = lS.location "+
                " LEFT JOIN bar b ON lo.id=b.location " +
                " LEFT JOIN line ln ON b.id=ln.bar " +
                " LEFT JOIN removedReading r ON r.line=ln.id " +
                " WHERE s.id=?";
        if (product > 0) {
            sql+=" AND ln.product=? ";
        }
        sql +=  " AND r.endDate BETWEEN ? AND ? " +
                " ORDER BY r.line, r.endDate ";
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            stmt = conn.prepareStatement(sql);
            int fieldCount = 0;
            stmt.setInt(++fieldCount, supplier);
            if (product > 0) {
                stmt.setInt(++fieldCount, product);
            }
            stmt.setTimestamp(++fieldCount, new java.sql.Timestamp(period.getStartDate().getTime()));
            stmt.setTimestamp(++fieldCount, new java.sql.Timestamp(period.getEndDate().getTime()));
            logger.debug("Executing getResultsBySupplier query");
            long startMillis = System.currentTimeMillis();
            rs = stmt.executeQuery();
            long elapsedTime = System.currentTimeMillis()-startMillis;
            logger.debug("Query complete took "+elapsedTime+" ms");
        } catch (SQLException sqle) {
            logger.dbError("Error in getResultsBySupplier "+sqle.getMessage());
            throw new HandlerException(sqle);
        }
        
        return new LineCleaningReportResults(period,rs,stmt);
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
        // logger.debug("Finding product for line "+lineId);
        try {
            String sql = "SELECT product.id, product.name, line.unit FROM line LEFT JOIN product ON line.product = product.id" +
                    " WHERE line.id=?";
            product = checkCache(lineId);
            if (product == null) { //need to go to database
                // logger.debug("Not in cache");
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
    
    /**  Consolidates line-readings in product-readings.
     *  Takes a map of (line-ids, ouncesPoured), and returns a map of (product-id, productData)
     *  Product data contains the product name, and the ounces poured.  If multiple lines map
     *  to the same product, one product record will be returned with a sum of those lines.
     */
    public static Map<Integer,ProductData> linesToProducts(Map<Integer,Double> lineMap, RegisteredConnection conn) {
        Map<Integer,ProductData> result = new HashMap<Integer,ProductData>(lineMap.size());
        ResultSet rSet = null;
        
        Iterator<Integer> i = lineMap.keySet().iterator();
        String sql = "SELECT product.id, product.name, line.unit FROM line LEFT JOIN product ON line.product = product.id" +
                " WHERE line.id=?";
        try {
            for (Integer key : lineMap.keySet() ) {
                try {
                    ProductData product = findProduct(key,conn);
                    // now we have a non-null product, either from the cache or the db

                    // check the previous value in our result set, if any
                    ProductData previousRecord = result.get(product.getId());
                    double previousPoured = 0.0;
                    if (previousRecord != null) {
                        previousPoured = previousRecord.getPoured();
                    }
                    //build the new record
                    ProductData newRecord = new ProductData(product.getId(),
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
     * Get the next removedReading.
     */
    public boolean next() throws SQLException {
        boolean next = (null != rs && rs.next());
        if (next) {
            line = rs.getInt(1);
            d = new Date(rs.getTimestamp(2).getTime());
            value = rs.getDouble(3);
            //logger.debug("getting next removedReading");
        } else {
            //logger.debug("no more readings");
        }
        return next;
    }
    
    /**
     * Get the line id of the current removedReading.
     */
    public int getLine() {
        return line;
    }
    
    /**
     * Get the date of the current removedReading.
     */
    public Date getDate() {
        return d;
    }
    /**
     * Get the value of the current removedReading.
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
