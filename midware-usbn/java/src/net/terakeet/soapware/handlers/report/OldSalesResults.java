/*
 * OldSalesResults.java
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
public class OldSalesResults {
    
    private static MidwareLogger logger = new MidwareLogger(OldSalesResults.class.getName());
    
    private ReportPeriod period;
    private ResultSet rs = null;
    private PreparedStatement stmt = null;
    
    private String plu;
    private double value;
    private Date d;
    //NischaySharma_12-Feb-2009_Start: Added new members to support the response XML
    private String zoneName;
    private String barName;
    private String stationName;
    private String zoneId;
    private String barId;
    private String stationId;
    //NischaySharma_12-Feb-2009_End
    
    private OldSalesResults(ReportPeriod p, ResultSet results, PreparedStatement st) throws HandlerException {
        this.period = p;
        this.rs = results;
        this.stmt = st;
    }            
    /**
     * Factory method to create a new result for a given location id.
     */
    public static OldSalesResults getResultsByCustomer(ReportPeriod period,
            int customer, RegisteredConnection conn)
            throws HandlerException {
        
        String sql =
                " SELECT s.pluNumber, s.date, s.quantity FROM sales s" +
                " LEFT JOIN location l ON l.id = s.location" +
                " WHERE l.customer = ? "	+
	        " AND date BETWEEN ? AND ? " +
                " ORDER BY date ";
        
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
        return new OldSalesResults(period,rs,stmt);
    }
    
    
    
    
    /**
     * Factory method to create a new result for a given location id.
     */
    public static OldSalesResults getResultsByCustomerCostCenter(ReportPeriod period,
            int customer, String specificLocations, RegisteredConnection conn)
            throws HandlerException {
        //NischaySharma_12-Feb-2009_Start: Changed the query to return Zone, Bar and Station also (id + name)
        String sql =
                " SELECT pluNumber, date, quantity, IFNULL(st.name,'UNKNOWN STATION'), IFNULL(b.name,'UNKNOWN BAR'), IFNULL(z.name,'UNKNOWN ZONE')," +
                " IFNULL(st.id,0), IFNULL(b.id,0), IFNULL(z.id,0) FROM sales s" +
                " LEFT JOIN costCenter c on c.ccID = s.costCenter AND c.location = s.location" +
                " LEFT JOIN station st on st.id = c.station" +
                " LEFT JOIN bar b on b.id = c.bar " +
                " LEFT JOIN zone z on z.id = c.zone" +
                " LEFT JOIN location l on l.id = s.location AND l.id = c.location" +
                " WHERE l.customer = ? ";

        if (!(specificLocations == null || specificLocations.equals(""))) {
            sql += " AND l.id IN ( " + specificLocations + " ) ";
        }

        sql += " AND date BETWEEN ? AND ? " +
                    " ORDER BY date ";
        //NischaySharma_12-Feb-2009_End
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            stmt = conn.prepareStatement(sql);
            int fieldCount = 0;
            stmt.setInt(++fieldCount, customer);
            stmt.setTimestamp(++fieldCount, new java.sql.Timestamp(period.getStartDate().getTime()));
            stmt.setTimestamp(++fieldCount, new java.sql.Timestamp(period.getEndDate().getTime()));
            logger.debug("Executing getResultsByCustomerByCostCenter query");
            long startMillis = System.currentTimeMillis();
            rs = stmt.executeQuery();
            long elapsedTime = System.currentTimeMillis()-startMillis;
            logger.debug("Query complete took "+elapsedTime+" ms");
        } catch (SQLException sqle) {
            logger.dbError("Error in getResultsByCustomer "+sqle.getMessage());
            throw new HandlerException(sqle);
        }        
        return new OldSalesResults(period,rs,stmt);
    }
    
    
    /**
     * Factory method to create a new result for a given location id.
     */
    public static OldSalesResults getResultsByCustomerPlu(ReportPeriod period,
            int customer, String specificLocations, RegisteredConnection conn)
            throws HandlerException {
        String sql = 
                " SELECT s.pluNumber, s.date, s.quantity, 'UNKNOWN STATION', IFNULL(b.name,BAR.name)," +
                " 'UNKNOWN ZONE', 0, IFNULL(b.id,BAR.id), 0" +
                " FROM location l" +
                " LEFT JOIN sales s ON s.location = l.id" +
                " LEFT JOIN beverage bev ON l.id = bev.location AND bev.plu = s.pluNumber AND bev.location = s.location" +
                " LEFT JOIN bar b ON b.id = bev.bar" +
                " LEFT JOIN (SELECT id, name, location FROM bar) AS BAR ON BAR.location = l.id" +
                " WHERE s.date BETWEEN ? AND ? AND s.quantity IS NOT NULL AND l.customer = ? ";
        
        if (!(specificLocations == null || specificLocations.equals(""))) {
            sql += " AND l.id IN ( " + specificLocations + " ) ";
        }

        sql += " ORDER BY s.location, s.date;";
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            stmt = conn.prepareStatement(sql);
            int fieldCount = 0;
            stmt.setTimestamp(++fieldCount, new java.sql.Timestamp(period.getStartDate().getTime()));
            stmt.setTimestamp(++fieldCount, new java.sql.Timestamp(period.getEndDate().getTime()));
            stmt.setInt(++fieldCount, customer);
            logger.debug("Executing getResultsByCustomerByPlu query");
            long startMillis = System.currentTimeMillis();
            rs = stmt.executeQuery();
            long elapsedTime = System.currentTimeMillis()-startMillis;
            logger.debug("Query complete took "+elapsedTime+" ms");
        } catch (SQLException sqle) {
            logger.dbError("Error in getResultsByCustomerByPlu "+sqle.getMessage());
            throw new HandlerException(sqle);
        }        
        return new OldSalesResults(period,rs,stmt);
    }
    
        
    
    
    /**
     * Factory method to create a new result for a given location id.
     */
    public static OldSalesResults getResultsByLocation(ReportPeriod period,
            int location, RegisteredConnection conn)
            throws HandlerException {
        
        String sql =
                " SELECT pluNumber, date, quantity FROM sales" +
                " WHERE location = ? "	+
	        " AND date BETWEEN ? AND ? " +
                " ORDER BY date ";
        
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
        return new OldSalesResults(period,rs,stmt);
    }
    
    
    
    
    /**
     * Factory method to create a new result for a given location id.
     */
    public static OldSalesResults getResultsByLocationCostCenter(ReportPeriod period,
            int location, RegisteredConnection conn)
            throws HandlerException {
        //NischaySharma_12-Feb-2009_Start: Changed the query to return Zone, Bar and Station also (id + name)
        String sql =
                " SELECT pluNumber, date, quantity, IFNULL(st.name,'UNKNOWN STATION'), IFNULL(b.name,'UNKNOWN BAR'), IFNULL(z.name,'UNKNOWN ZONE')," +
                " IFNULL(st.id,0), IFNULL(b.id,0), IFNULL(z.id,0) FROM sales s" +
                " LEFT JOIN costCenter c on c.ccID = s.costCenter AND c.location = s.location" +
                " LEFT JOIN station st on st.id = c.station" +
                " LEFT JOIN bar b on b.id = c.bar " +
                " LEFT JOIN zone z on z.id = c.zone" +
                " LEFT JOIN location l on l.id = s.location AND l.id = c.location" +
                " WHERE l.id = ? "	+
	        " AND date BETWEEN ? AND ? " +
                " ORDER BY date ";
        //NischaySharma_12-Feb-2009_End
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            stmt = conn.prepareStatement(sql);
            int fieldCount = 0;
            stmt.setInt(++fieldCount, location);
            stmt.setTimestamp(++fieldCount, new java.sql.Timestamp(period.getStartDate().getTime()));
            stmt.setTimestamp(++fieldCount, new java.sql.Timestamp(period.getEndDate().getTime()));
            logger.debug("Executing getResultsByLocationByCostCenter query");
            long startMillis = System.currentTimeMillis();
            rs = stmt.executeQuery();
            long elapsedTime = System.currentTimeMillis()-startMillis;
            logger.debug("Query complete took "+elapsedTime+" ms");
        } catch (SQLException sqle) {
            logger.dbError("Error in getResultsByLocationByCostCenter "+sqle.getMessage());
            throw new HandlerException(sqle);
        }        
        return new OldSalesResults(period,rs,stmt);
    }
    
    
    /**
     * Factory method to create a new result for a given location id.
     */
    public static OldSalesResults getResultsByLocationPlu(ReportPeriod period,
            int location, RegisteredConnection conn)
            throws HandlerException {
        String sql = 
                " SELECT s.pluNumber, s.date, s.quantity," +
                " 'UNKNOWN STATION', IFNULL(b.name,'UNKNOWN BAR'), 'UNKNOWN ZONE', 0, IFNULL(b.id,0), 0" +
                " FROM sales s left join beverage bev on bev.plu = s.pluNumber" +
                " LEFT JOIN bar b on b.id = bev.bar" +
                " WHERE s.location = ? AND bev.location = ?"	+
	        " AND s.date BETWEEN ? AND ? " +
                " ORDER BY s.date ";
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            stmt = conn.prepareStatement(sql);
            int fieldCount = 0;
            stmt.setInt(++fieldCount, location);
            stmt.setInt(++fieldCount, location);
            stmt.setTimestamp(++fieldCount, new java.sql.Timestamp(period.getStartDate().getTime()));
            stmt.setTimestamp(++fieldCount, new java.sql.Timestamp(period.getEndDate().getTime()));
            logger.debug("Executing getResultsByLocationByPlu query");
            long startMillis = System.currentTimeMillis();
            rs = stmt.executeQuery();
            long elapsedTime = System.currentTimeMillis()-startMillis;
            logger.debug("Query complete took "+elapsedTime+" ms");
        } catch (SQLException sqle) {
            logger.dbError("Error in getResultsByLocationByPlu "+sqle.getMessage());
            throw new HandlerException(sqle);
        }        
        return new OldSalesResults(period,rs,stmt);
    }
    
    
     
    
    
    /**
     * Factory method to create a new result for a given location id.
     */
    public static OldSalesResults getResultsByZones(ReportPeriod period,
            int location, int zone, RegisteredConnection conn)
            throws HandlerException {
        String sql = 
                " SELECT s.pluNumber, s.date, s.quantity " +
                " FROM sales s LEFT JOIN station st on st.id = s.station " +
                " LEFT JOIN bar b on b.id = st.bar " +
                " WHERE s.location = ? AND b.zone = ?"	+
	        " AND date BETWEEN ? AND ? " +
                " ORDER BY date ";
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            stmt = conn.prepareStatement(sql);
            int fieldCount = 0;
            stmt.setInt(++fieldCount, location);
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
        return new OldSalesResults(period,rs,stmt);
    }


    /**
     * Factory method to create a new result for a given location id.
     */
    public static OldSalesResults getResultsByBarPlu(ReportPeriod period,
            int bar, RegisteredConnection conn)
            throws HandlerException {
        String sql =
                " SELECT s.pluNumber, s.date, s.quantity," +
                " 'UNKNOWN STATION', IFNULL(b.name,'UNKNOWN BAR'), 'UNKNOWN ZONE', 0, IFNULL(b.id,0), 0" +
                " FROM sales s left join beverage bev on bev.plu = s.pluNumber and s.location = bev.location" +
                " LEFT JOIN bar b on b.id = bev.bar" +
                " WHERE bev.bar = ?"	+
	        " AND s.date BETWEEN ? AND ? " +
                " ORDER BY s.date ";
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            stmt = conn.prepareStatement(sql);
            int fieldCount = 0;
            stmt.setInt(++fieldCount, bar);
            stmt.setTimestamp(++fieldCount, new java.sql.Timestamp(period.getStartDate().getTime()));
            stmt.setTimestamp(++fieldCount, new java.sql.Timestamp(period.getEndDate().getTime()));
            logger.debug("Executing getResultsByBarPlu query");
            long startMillis = System.currentTimeMillis();
            rs = stmt.executeQuery();
            long elapsedTime = System.currentTimeMillis()-startMillis;
            logger.debug("Query complete took "+elapsedTime+" ms");
        } catch (SQLException sqle) {
            logger.dbError("Error in getResultsByBarPlu "+sqle.getMessage());
            throw new HandlerException(sqle);
        }
        return new OldSalesResults(period,rs,stmt);
    }



    
    
    /**
     * Factory method to create a new result for a given location id.
     */
    public static OldSalesResults getResultsByBars(ReportPeriod period,
            int location, int bar, RegisteredConnection conn)
            throws HandlerException {
        String sql = 
                " SELECT s.pluNumber, s.date, s.quantity," +
                " 'UNKNOWN STATION', IFNULL(b.name,'UNKNOWN BAR'), 'UNKNOWN ZONE', 0, IFNULL(b.id,0), 0" +
                " FROM sales s left join beverage bev on bev.plu = s.pluNumber and s.location = bev.location" +
                " LEFT JOIN bar b on b.id = bev.bar" +
                " WHERE s.location = ? and bev.bar = ?"	+
	        " AND s.date BETWEEN ? AND ? " +
                " ORDER BY s.date ";
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            stmt = conn.prepareStatement(sql);
            int fieldCount = 0;
            stmt.setInt(++fieldCount, location);
            stmt.setInt(++fieldCount, bar);
            stmt.setTimestamp(++fieldCount, new java.sql.Timestamp(period.getStartDate().getTime()));
            stmt.setTimestamp(++fieldCount, new java.sql.Timestamp(period.getEndDate().getTime()));
            logger.debug("Executing getResultsByBars query");
            long startMillis = System.currentTimeMillis();
            rs = stmt.executeQuery();
            long elapsedTime = System.currentTimeMillis()-startMillis;
            logger.debug("Query complete took "+elapsedTime+" ms");
        } catch (SQLException sqle) {
            logger.dbError("Error in getResultsByBars "+sqle.getMessage());
            throw new HandlerException(sqle);
        }        
        return new OldSalesResults(period,rs,stmt);
    }
    
    
    /**
     * Factory method to create a new result for a given location id.
     */
    public static OldSalesResults getResultsByStations(ReportPeriod period,
            int location, int station, RegisteredConnection conn)
            throws HandlerException {
        String sql = 
                " SELECT pluNumber, date, quantity " +
                " FROM sales " +
                " WHERE location = ? and station = ?"	+
	        " AND date BETWEEN ? AND ? " +
                " ORDER BY date ";
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            stmt = conn.prepareStatement(sql);
            int fieldCount = 0;
            stmt.setInt(++fieldCount, location);
            stmt.setInt(++fieldCount, station);
            stmt.setTimestamp(++fieldCount, new java.sql.Timestamp(period.getStartDate().getTime()));
            stmt.setTimestamp(++fieldCount, new java.sql.Timestamp(period.getEndDate().getTime()));
            logger.debug("Executing getResultsByStations query");
            long startMillis = System.currentTimeMillis();
            rs = stmt.executeQuery();
            long elapsedTime = System.currentTimeMillis()-startMillis;
            logger.debug("Query complete took "+elapsedTime+" ms");
        } catch (SQLException sqle) {
            logger.dbError("Error in getResultsByStations "+sqle.getMessage());
            throw new HandlerException(sqle);
        }        
        return new OldSalesResults(period,rs,stmt);
    }
    
    /**
     * Factory method to create a new result for a given location id.
     */
    public static OldSalesResults getResultsByZoneCostCenters(ReportPeriod period,
            int location, int zone, RegisteredConnection conn)
            throws HandlerException {
        String sql =
                " SELECT pluNumber, date, quantity, IFNULL(st.name,'UNKNOWN STATION'), IFNULL(b.name,'UNKNOWN BAR'), IFNULL(z.name,'UNKNOWN ZONE')," +
                " IFNULL(st.id,0), IFNULL(b.id,0), IFNULL(z.id,0) FROM sales s" +
                " LEFT JOIN costCenter c on c.ccID = s.costCenter AND c.location = s.location" +
                " LEFT JOIN station st on st.id = c.station" +
                " LEFT JOIN bar b on b.id = c.bar " +
                " LEFT JOIN zone z on z.id = c.zone" +
                " LEFT JOIN location l on l.id = s.location AND l.id = c.location" +
                " WHERE l.id = ? AND z.id = ?"	+
	        " AND date BETWEEN ? AND ? " +
                " ORDER BY date ";
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            stmt = conn.prepareStatement(sql);
            int fieldCount = 0;
            stmt.setInt(++fieldCount, location);
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
        return new OldSalesResults(period,rs,stmt);
    }
    
    
    /**
     * Factory method to create a new result for a given location id.
     */
    public static OldSalesResults getResultsByBarCostCenters(ReportPeriod period,
            int location, int bar, RegisteredConnection conn)
            throws HandlerException {
        String sql =
                " SELECT pluNumber, date, quantity, IFNULL(st.name,'UNKNOWN STATION'), IFNULL(b.name,'UNKNOWN BAR'), IFNULL(z.name,'UNKNOWN ZONE')," +
                " IFNULL(st.id,0), IFNULL(b.id,0), IFNULL(z.id,0) FROM sales s" +
                " LEFT JOIN costCenter c on c.ccID = s.costCenter AND c.location = s.location" +
                " LEFT JOIN station st on st.id = c.station" +
                " LEFT JOIN bar b on b.id = c.bar " +
                " LEFT JOIN zone z on z.id = c.zone" +
                " LEFT JOIN location l on l.id = s.location AND l.id = c.location" +
                " WHERE l.id = ? AND b.id = ?"	+
	        " AND date BETWEEN ? AND ? " +
                " ORDER BY date ";
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            stmt = conn.prepareStatement(sql);
            int fieldCount = 0;
            stmt.setInt(++fieldCount, location);
            stmt.setInt(++fieldCount, bar);
            stmt.setTimestamp(++fieldCount, new java.sql.Timestamp(period.getStartDate().getTime()));
            stmt.setTimestamp(++fieldCount, new java.sql.Timestamp(period.getEndDate().getTime()));
            logger.debug("Executing getResultsByBars query");
            long startMillis = System.currentTimeMillis();
            rs = stmt.executeQuery();
            long elapsedTime = System.currentTimeMillis()-startMillis;
            logger.debug("Query complete took "+elapsedTime+" ms");
        } catch (SQLException sqle) {
            logger.dbError("Error in getResultsByBars "+sqle.getMessage());
            throw new HandlerException(sqle);
        }        
        return new OldSalesResults(period,rs,stmt);
    }
    
    
    /**
     * Factory method to create a new result for a given location id.
     */
    public static OldSalesResults getResultsByStationCostCenters(ReportPeriod period,
            int location, int station, RegisteredConnection conn)
            throws HandlerException {
        String sql =
                " SELECT pluNumber, date, quantity, IFNULL(st.name,'UNKNOWN STATION'), IFNULL(b.name,'UNKNOWN BAR'), IFNULL(z.name,'UNKNOWN ZONE')," +
                " IFNULL(st.id,0), IFNULL(b.id,0), IFNULL(z.id,0) FROM sales s" +
                " LEFT JOIN costCenter c on c.ccID = s.costCenter AND c.location = s.location" +
                " LEFT JOIN station st on st.id = c.station" +
                " LEFT JOIN bar b on b.id = c.bar " +
                " LEFT JOIN zone z on z.id = c.zone" +
                " LEFT JOIN location l on l.id = s.location AND l.id = c.location" +
                " WHERE l.id = ? AND st.id = ?"	+
	        " AND date BETWEEN ? AND ? " +
                " ORDER BY date ";
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            stmt = conn.prepareStatement(sql);
            int fieldCount = 0;
            stmt.setInt(++fieldCount, location);
            stmt.setInt(++fieldCount, station);
            stmt.setTimestamp(++fieldCount, new java.sql.Timestamp(period.getStartDate().getTime()));
            stmt.setTimestamp(++fieldCount, new java.sql.Timestamp(period.getEndDate().getTime()));
            logger.debug("Executing getResultsByStations query");
            long startMillis = System.currentTimeMillis();
            rs = stmt.executeQuery();
            long elapsedTime = System.currentTimeMillis()-startMillis;
            logger.debug("Query complete took "+elapsedTime+" ms");
        } catch (SQLException sqle) {
            logger.dbError("Error in getResultsByStations "+sqle.getMessage());
            throw new HandlerException(sqle);
        }        
        return new OldSalesResults(period,rs,stmt);
    }
    
    /**
     * Factory method to create a new result for a given location id.
     */
    public static OldSalesResults getResultsByCostCenters(ReportPeriod period,
            int location, int bar, RegisteredConnection conn)
            throws HandlerException {
        String sql = 
                " SELECT s.pluNumber, s.date, s.quantity " +
                " FROM sales s left join costCenter c on c.ccID = s.costCenter" +
                " WHERE s.location = ? and c.bar = ?"	+
	        " AND s.date BETWEEN ? AND ? " +
                " ORDER BY s.date ";
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            stmt = conn.prepareStatement(sql);
            int fieldCount = 0;
            stmt.setInt(++fieldCount, location);
            stmt.setInt(++fieldCount, bar);
            stmt.setTimestamp(++fieldCount, new java.sql.Timestamp(period.getStartDate().getTime()));
            stmt.setTimestamp(++fieldCount, new java.sql.Timestamp(period.getEndDate().getTime()));
            logger.debug("Executing getResultsByCostCenters query");
            long startMillis = System.currentTimeMillis();
            rs = stmt.executeQuery();
            long elapsedTime = System.currentTimeMillis()-startMillis;
            logger.debug("Query complete took "+elapsedTime+" ms");
        } catch (SQLException sqle) {
            logger.dbError("Error in getResultsByCostCenters "+sqle.getMessage());
            throw new HandlerException(sqle);
        }        
        return new OldSalesResults(period,rs,stmt);
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
            try
            {
                stationName = rs.getString(4);
                barName = rs.getString(5);
                zoneName = rs.getString(6);
                stationId = rs.getString(7);
                barId = rs.getString(8);
                zoneId = rs.getString(9);
            }
            catch(Exception e)
            {
                stationName = "Unknown Station";
                barName = "Unknown Bar";
                zoneName = "Unknown Zone";
                stationId = "0";
                barId = "0";
                zoneId = "0";
            }

            //logger.debug("getting next reading");
        } else {
            //logger.debug("no more readings");
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

    //NischaySharma_12-Feb-2009_Start: Added new method to support getting value of Zone
    public String ZoneName()
    {
        if(barName == null){return "Unknown Zone";}
        return zoneName;
    }
    //NischaySharma_12-Feb-2009_End

    //NischaySharma_12-Feb-2009_Start:  Added new method to support getting value of Bar
    public String BarName()
    {
        if(barName == null){return "Unknown Bar";}
        return barName;
    }
    //NischaySharma_12-Feb-2009_End

    //NischaySharma_12-Feb-2009_Start:  Added new method to support getting value of Station
    public String StationName()
    {
        if(stationName == null){return "Unknown Station";}
        return stationName;
    }
    //NischaySharma_12-Feb-2009_End

    //NischaySharma_12-Feb-2009_Start: Added new method to support getting value of ZoneId
    public String ZoneId()
    {
        if(zoneId == null){return "0";}
        return zoneId;
    }
    //NischaySharma_12-Feb-2009_End

    //NischaySharma_12-Feb-2009_Start:  Added new method to support getting value of BarId
    public String BarId()
    {
        if(barId == null){return "0";}
        return barId;
    }
    //NischaySharma_12-Feb-2009_End

    //NischaySharma_12-Feb-2009_Start:  Added new method to support getting value of StationId
    public String StationId()
    {
        if(stationId == null){return "0";}
        return stationId;
    }
    //NischaySharma_12-Feb-2009_End

    
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
