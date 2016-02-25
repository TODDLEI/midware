/*
 * PeriodStructure.java
 *
 * Created on August 29, 2005, 10:50 AM
 *
 */
package net.terakeet.soapware.handlers.report;

import java.util.Map;
import java.util.HashMap;
import java.util.Date;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.Iterator;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import org.apache.log4j.Logger;
import net.terakeet.soapware.*;
import net.terakeet.util.MidwareLogger;
import java.sql.ResultSet;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/** PeriodStructure
 *
 */
public class PeriodStructure {

    private static MidwareLogger logger = new MidwareLogger(PeriodStructure.class.getName());
    private PeriodStructure previous;
    private HashMap<Integer, Double> previousCache;
    private HashMap<Integer, LinePeriod> linePeriods;
    private HashMap<Integer, String> locationCache;
    private HashMap<Integer, String> barCache;
    private HashMap<Integer, Integer> productCache;
    private HashMap<Integer, Integer> productCategoryCache;
    private HashMap<Integer, String> stationCache;
    private HashMap<Integer, String> zoneCache;
    private Date startDate;

    /** Creates a new instance of PeriodStructure 
     *  @param previous the previous PeriodStructure, can be null if this is the first
     *  @param date the start date of this structure
     */
    public PeriodStructure(PeriodStructure previous, Date date) {
        this.previous = previous;
        previousCache = new HashMap<Integer, Double>();
        linePeriods = new HashMap<Integer, LinePeriod>();
        locationCache = new HashMap<Integer, String>();
        barCache = new HashMap<Integer, String>();
        productCache = new HashMap<Integer, Integer>();
        productCategoryCache = new HashMap<Integer, Integer>();
        stationCache = new HashMap<Integer, String>();
        zoneCache = new HashMap<Integer, String>();
        startDate = date;
    }

    /** Gets a LinePeriod for the line, initializing it if necessary.
     *  @param line the line ID for the period
     */
    protected LinePeriod getLinePeriod(int line) {
        Integer lineKey = new Integer(line);
        LinePeriod lp = linePeriods.get(lineKey);
        if (null == lp) {
            lp = new LinePeriod(line);
            linePeriods.put(lineKey, lp);
        }
        return lp;
    }

    protected double getPreviousReading(LinePeriod oldlp, int line, Date beforeFirstDate, RegisteredConnection conn) {
        Integer lineKey = new Integer(line);
        double value = 0.0;
        //logger.debug("getPreviousReading for date("+startDate.toString()+"), line("+line+"):");
        if (previousCache.containsKey(lineKey)) {
            //logger.debug("PS: ... retrieving from cached");
            value = previousCache.get(lineKey).doubleValue();
        } else {
            if (getLinePeriod(line).startsWithASpike()) {
                // if the period starts with a spike, don't lookup a previous value
                if (oldlp.getFirstQuantity() > oldlp.getFirstValue()) {
                    value                   = 0;
                } else {
                    value                   = getLinePeriod(line).getFirstValue();
                }
                //logger.debug("PS:  ... using first reading spike value for line: " + line + " with value: " + value);
            } else if (null == previous) {
                /*insert the db fetch here and cache it
                //  Important:  We only take database readings if they are less than 12 hours away from our start date
                final long MAX_AGE = 14 * 24 * 60 * 60 * 1000; // 12 hours
                try {
                    logger.debug("PS:  ... connecting to db for line: " + line + " with start date: " + beforeFirstDate.toString());
                    String sql = "SELECT value,date FROM reading WHERE line=? AND date<? AND type = 0 ORDER BY date DESC LIMIT 1";
                    // fetch prevous reading and add it to the cache

                    PreparedStatement ps = conn.prepareStatement(sql);
                    ps.setInt(1, line);
                    ps.setTimestamp(2, new java.sql.Timestamp(beforeFirstDate.getTime()));
                    ResultSet rs = ps.executeQuery();
                    if (rs.next()) {
                        value = rs.getDouble(1);
                        long timeDiff = beforeFirstDate.getTime() - rs.getTimestamp(2).getTime();
                        Date lastDate = new Date(rs.getTimestamp(2).getTime());
                        // if the reading is too old, we ignore the value and just use our first reading
                        if (timeDiff > MAX_AGE) {
                            logger.debug("PS: Ignoring database previous reading, too old." +
                                    " Line " + line + ", value " + value + " oz, age " + timeDiff + " ms");
                            value = getLinePeriod(line).getFirstValue();
                        }
                        // if the reading is too old, we ignore the value and just use our first reading
                        if (value < 0) {
                            logger.debug("PS: Ignoring database previous reading, the value is a SPIKE." +
                                    " Line " + line + ", value " + value + " oz, age " + timeDiff + " ms");
                            value = getLinePeriod(line).getFirstValue();
                        }
                        //logger.debug("  ... retrieved from db "+String.valueOf(value));
                        if(isASpike(value,lastDate,getLinePeriod(line).getFirstValue(),beforeFirstDate)) {
                            logger.debug("PS: SPIKE on L"+line+" from "+value+" at "+lastDate+" to "+getLinePeriod(line).getFirstValue()+" at "+beforeFirstDate);
                            value = getLinePeriod(line).getFirstValue();
                        }
                    } else {
                        value = getLinePeriod(line).getFirstValue();
                        //logger.debug("PS: ... no result from db, using first value");
                    }
                } catch (SQLException sqle) {
                    logger.dbError("Database error: " + sqle.getMessage());
                    value = getLinePeriod(line).getFirstValue();
                }*/
                if (oldlp.getFirstQuantity() > oldlp.getFirstValue()) {
                    value                   = 0;
                } else {
                    value                   = oldlp.getFirstValue() - oldlp.getFirstQuantity();
                }
                //logger.debug("Line: " + line + ", first date: " + oldlp.getFirstDate() + ", first value: " + oldlp.getFirstValue() + ", first qty: " + oldlp.getFirstQuantity());
            } else {
                LinePeriod lp = previous.getLinePeriod(line);
                if (lp.getReadingCount() > 0) {
                    // if the previous period is not spiking, add its last value, otherwise ignore it
                    if (!lp.endsWithASpike()) {
                        if (Math.abs(oldlp.getFirstQuantity()) > 0.0) {
                            value = lp.getLastValue();
                        } else {
                            value = oldlp.getFirstValue();
                        }
                        //logger.debug("PS:  ... if not ends with spike for: " + line + " with value: " + value);
                    } else {
                        value = -1.00;
                        //logger.debug("PS:  ... if ends with spike for: " + line + " with value: " + value);
                    }
                } else {
                    value = previous.getPreviousReading(oldlp, line, beforeFirstDate, conn);
                    //logger.debug("PS:  ... using previous PeriodStructure for: " + line + " with value: " + value);
                }
                if (Math.abs(value) < -1.00) {
                    value = getLinePeriod(line).getFirstValue();
                }
            }
            //logger.debug("PS:  ... adding to PeriodStructure cache for line: " + line + " with value: " + value);
            previousCache.put(lineKey, new Double(value));
        }
        return value;
    }

    private boolean isASpike(double prev, Date prevDate, double current, Date currentDate) {
        //Current Implementation: a spike is any pour greater than 5 oz / sec over the period:
        //Also, the period must be at least five seconds, or the readings are ignored
        final double maxRate = 5.0 / 1000.0; //ounces per millisecond
        final long fiveSeconds = 5 * 1000;

        if (prevDate == null || currentDate == null ) { return false; }
        long timeDifference = currentDate.getTime()-prevDate.getTime();
        if (timeDifference < fiveSeconds) { return false; }
        double ozDifference = current - prev;
        double rate = ozDifference / timeDifference;
        //logger.debug("pourRate: " + String.valueOf(rate));
        return (rate >= maxRate);
    }

    /** Add a line reading to this period.
     *  @param line the line id
     *  @param value the value of the line
     *  @param d the datetime of the reading
     */
    public void averageTempReading(int system, double value, Date d) {
        LinePeriod lp = getLinePeriod(system);
        lp.addReading(value, d);
    }

    /** Add a line reading to this period.
     *  @param line the line id
     *  @param value the value of the line
     *  @param d the datetime of the reading
     */
    public void addReading(int line, double value, Date d, double quantity) {
        LinePeriod lp = getLinePeriod(line);
        lp.addReading(value, quantity, d);
    }

    /** Add a line reading to this period.
     *  @param line the line id
     *  @param value the value of the line
     *  @param d the datetime of the reading
     */
    public HashMap<Integer, LinePeriod> addTotalReading(int line, double value, Date d, double quantity, HashMap<Integer, LinePeriod> lps) {
        Integer lineKey                     = new Integer(line);
        LinePeriod lp                       = lps.get(lineKey);
        if (null == lp) {
            lp                              = new LinePeriod(line);
        }
        lp.addReading(value, quantity, d);
        lps.put(lineKey, lp);
        return lps;
    }

    /** Add an offset for spikes to this period.  This value will
     *  be added to the line result.  For example, if the first reading is
     *  15, the last reading is 20, and the offset is 8, then the result will
     *  be 13 (20 - 15 + 8).  Multiple offsets may be added; they will be summed.
     *
     *  @param line the line id
     *  @param value the spike value
     */
    public void addOffset(int line, double value) {
        LinePeriod lp = getLinePeriod(line);
        lp.addOffset(value);
    }

    /** Returns net values for all the lines for this period.  Will return a map
     *  between line-ids and net-value.  
     *
     *  @return a map from line-id to net-value.
     */
    public Map<Integer, Double> getValues(RegisteredConnection conn) {
        LinePeriod lp = null;
        double net = 0.0;
        HashMap<Integer, Double> values = new HashMap<Integer, Double>();
        for (Integer i : linePeriods.keySet()) {
            lp = linePeriods.get(i);
            //setData(conn, i);
            if (lp.getReadingCount() > 0) {
                double last = lp.getLastValue();
                Date beforeFirstDate = lp.getFirstDate();
                double first = getPreviousReading(lp, i.intValue(), beforeFirstDate, conn);
                double offset = lp.getOffset();
                double quantity = lp.getFirstQuantity();
                net = last - first + offset;
                if (first <= 0.0) {
                    double lpFirst = lp.getFirstValue();
                    //logger.debug("LP starts with spike: " + lp.startsWithASpike());
                    //logger.debug("LP First Value: " + lp.getFirstValue());
                    if (quantity > 0 && lp.startsWithASpike()) {
                        net = quantity;
                    } else if (lpFirst > 100) {
                        logger.debug("PS Line: " + i.intValue() + " LP First Value: " + lp.getFirstValue() + " at " + lp.getFirstDate());
                        net = quantity;
                    } else if (first <= -1.0) {
                        net = 0.0;
                    }
                    //ogger.debug("PS: " + i.intValue() + " - " + lp.getLastDate() + " - " + last + " - " + first + " + "+offset+" = "+net+" debugging: "+lp.getFirstQuantity());
                } else {
                    //logger.debug("PS: :: " + i.intValue() + " - " + last + " - " + first + " + " + offset + " = " + net + " debugging: " + lp.getFirstQuantity());
                }
            } else {
                // just use the offset
                net = lp.getOffset();
            }
            values.put(i, new Double(net));
        }
        return values;
    }

    /** Returns net values for all the lines for this period.  Will return a map
     *  between line-ids and net-value.
     *
     *  @return a map from line-id to net-value.
     */
    public Map<Integer, Double> getTotalValues(RegisteredConnection conn, HashMap<Integer, LinePeriod> lps) {
        LinePeriod lp = null;
        double net = 0.0;
        HashMap<Integer, Double> values = new HashMap<Integer, Double>();
        previousCache = new HashMap<Integer, Double>();
        previous = null;
        linePeriods = lps;
        for (Integer i : lps.keySet()) {
            lp = lps.get(i);
            //setData(conn, i);
            if (lp.getReadingCount() > 0) {
                double last = lp.getLastValue();
                Date beforeFirstDate = lp.getFirstDate();
                double first = getPreviousReading(lp, i.intValue(), beforeFirstDate, conn);
                double offset = lp.getOffset();
                net = last - first + offset;
                //net = lp.getLastValue() - getPreviousReading(i.intValue(), beforeFirstDate, conn) + lp.getOffset();
                //logger.debug("PS Total: :: "+i.intValue()+" - "+beforeFirstDate.toString()+" - "+last+" - "+first+" + "+offset+" = "+net);
            } else {
                // just use the offset
                net = lp.getOffset();
            }

            values.put(i, new Double(net));
        }
        return values;
    }

    /** Returns net values for all the lines for this period.  Will return a map
     *  between line-ids and net-value.
     *
     *  @return a map from line-id to net-value.
     */
    public Map<Integer, Double> getLastValues(RegisteredConnection conn) {
        LinePeriod lp = null;
        double net = 0.0;
        HashMap<Integer, Double> values = new HashMap<Integer, Double>();
        for (Integer i : linePeriods.keySet()) {
            lp = linePeriods.get(i);
            //setData(conn, i);
            if (lp.getReadingCount() > 0) {
                double last = lp.getLastValue();
                net = last;
                //logger.debug(":: "+i.intValue()+" - "+last+" = "+net);
            } else {
                // just use the offset
                net = lp.getOffset();
            }
            values.put(i, new Double(net));
        }
        return values;
    }
    public Map<Integer, Double> getTotalsLastValues(RegisteredConnection conn, HashMap<Integer, LinePeriod> lps) {
        LinePeriod lp = null;
        double net = 0.0;
        HashMap<Integer, Double> values = new HashMap<Integer, Double>();
        for (Integer i : lps.keySet()) {
            lp = lps.get(i);
            //setData(conn, i);
            if (lp.getReadingCount() > 0) {
                double last = lp.getLastValue();
                net = last;
                //logger.debug(":: "+i.intValue()+" - "+last+" = "+net);
            } else {
                // just use the offset
                net = lp.getOffset();
            }
            values.put(i, new Double(net));
        }
        return values;
    }

    /** Returns net values for all the lines for this period.  Will return a map
     *  between line-ids and net-value.  
     *
     *  @return a map from line-id to net-value.
     */
    public Map<Integer, Integer> getProduct(RegisteredConnection conn) {
        String sql = "SELECT p.id, p.name FROM line l " +
                "LEFT JOIN product p on p.id = l.product " +
                "WHERE l.id = ?";

        if (productCache.isEmpty()) {
            for (Integer i : linePeriods.keySet()) {
                try {
                    //logger.debug("PS  ... connecting to db for line: " + i + " to getProduct: ");
                    PreparedStatement ps = conn.prepareStatement(sql);
                    ps.setInt(1, i);
                    ResultSet rs = ps.executeQuery();
                    if (rs.next()) {
                        productCache.put(i, rs.getInt(1));
                    }
                } catch (SQLException sqle) {
                    logger.dbError("Database error: " + sqle.getMessage());
                }
            }
        }
        return productCache;
    }

    /** Returns net values for all the lines for this period.  Will return a map
     *  between line-ids and net-value.
     *
     *  @return a map from line-id to net-value.
     */
    public void setData(RegisteredConnection conn, Integer parameter, String condition) {
        String sql = " SELECT l.id, IFNULL(loc.id,0), IFNULL(loc.name,'Unknown Location') FROM line l " +
                " LEFT JOIN bar b on b.id = l.bar LEFT JOIN location loc on loc.id = b.location " + condition + "; ";

        try {
            if (locationCache.isEmpty()) {
                PreparedStatement ps = conn.prepareStatement(sql);
                if (parameter >= 0) {
                    ps.setInt(1, parameter);
                }
                ResultSet rs = ps.executeQuery();
                while (rs.next()) {
                    locationCache.put(rs.getInt(1), rs.getString(2) + "|" + rs.getString(3));
                    /*barCache.put(rs.getInt(1), rs.getString(4) + "|" + rs.getString(5));
                    productCache.put(rs.getInt(1), rs.getInt(6));
                    productCategoryCache.put(rs.getInt(1), rs.getInt(8));
                    stationCache.put(rs.getInt(1), rs.getString(9) + "|" + rs.getString(10));
                    zoneCache.put(rs.getInt(1), rs.getString(11) + "|" + rs.getString(12));*/
                }
            }
            //logger.debug("Created Sets");
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
        }
    }

    /** Returns net values for all the lines for this period.  Will return a map
     *  between line-ids and net-value.
     *
     *  @return a map from line-id to net-value.
     */
    public void setData(RegisteredConnection conn, Integer line) {
        String sql = " SELECT IFNULL(loc.id,0), IFNULL(loc.name,'Unknown Location'), IFNULL(b.id,0), IFNULL(b.name,'Unknown Bar'), " +
                " IFNULL(p.id,0), IFNULL(p.name,'Unknown Product'), IFNULL(p.category,0), " +
                " IFNULL(st.id,0), IFNULL(st.name,'Unknown Station'), IFNULL(z.id,0), IFNULL(z.name,'Unknown Zone') FROM line l " +
                " LEFT JOIN station st on st.id = l.station " +
                " LEFT JOIN bar b on b.id = l.bar " +
                " LEFT JOIN zone z on z.id = b.zone " +
                " LEFT JOIN location loc on loc.id = b.location " +
                " LEFT JOIN product p on p.id = l.product " +
                " WHERE l.id = ?; ";

        try {
            //logger.debug("PS - Creating Data Set for Line# " + line);
            PreparedStatement ps = conn.prepareStatement(sql);
            ps.setInt(1, line);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                locationCache.put(line, rs.getString(1) + "|" + rs.getString(2));
                //barCache.put(line, rs.getString(3) + "|" + rs.getString(4));
                //productCache.put(line, rs.getInt(5));
                //productCategoryCache.put(line, rs.getInt(7));
                //stationCache.put(line, rs.getString(8) + "|" + rs.getString(9));
                //zoneCache.put(line, rs.getString(10) + "|" + rs.getString(11));
                //logger.debug("Created Sets");
            } else {
                locationCache.put(line, "0|Unknown Location");
                //barCache.put(line, "0|Unknown Bar");
                //productCache.put(line, 0);
                //productCategoryCache.put(line, 0);
                //stationCache.put(line, "0|Unknown Station");
                //zoneCache.put(line, "0|Unknown Zone");
                //logger.debug("Created Empty Sets");
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
        }

    }

    /** Returns net values for all the lines for this period.  Will return a map
     *  between line-ids and net-value.  
     *
     *  @return a map from line-id to net-value.
     */
    public Map<Integer, String> getZone(RegisteredConnection conn) {
        String sql = "SELECT z.id, z.name FROM line l " +
                "LEFT JOIN bar b on b.id = l.bar " +
                "LEFT JOIN zone z on z.id = b.zone " +
                "WHERE l.id = ?";
        String result = "0|Unknown Zone";

        if (zoneCache.isEmpty()) {
            for (Integer i : linePeriods.keySet()) {
                try {
                    //logger.debug("PS  ... connecting to db for line: " + i + " to getZone: ");
                    PreparedStatement ps = conn.prepareStatement(sql);
                    ps.setInt(1, i);
                    ResultSet rs = ps.executeQuery();
                    if (rs.next()) {
                        //NischaySharma_12-Feb-2009_Start: result is changed to contain both Id and Name
                        if (rs.getString(1) != null) {
                            result = rs.getString(1) + "|" + rs.getString(2);
                        }
                        //NischaySharma_12-Feb-2009_End
                    }
                    if (result == null) {
                        result = "0|Unknown Zone";
                    }
                    zoneCache.put(i, result);
                } catch (SQLException sqle) {
                    logger.dbError("Database error: " + sqle.getMessage());
                }
            }
        }
        return zoneCache;
    }

    /** Returns net values for all the lines for this period.  Will return a map
     *  between line-ids and net-value.  
     *
     *  @return a map from line-id to net-value.
     */
    public Map<Integer, String> getBar(RegisteredConnection conn) {
        String sql = "SELECT b.id, b.name FROM line l " +
                "LEFT JOIN bar b on b.id = l.bar " +
                "WHERE l.id = ?";
        String result = "0|Unknown Bar";

        if (barCache.isEmpty()) {
            for (Integer i : linePeriods.keySet()) {
                try {
                    //logger.debug("PS  ... connecting to db for line: " + i + " to getBar: ");
                    PreparedStatement ps = conn.prepareStatement(sql);
                    ps.setInt(1, i);
                    ResultSet rs = ps.executeQuery();
                    if (rs.next()) {
                        //NischaySharma_12-Feb-2009_Start: result is changed to contain both Id and Name
                        if (rs.getString(1) != null) {
                            result = rs.getString(1) + "|" + rs.getString(2);
                        }
                        //NischaySharma_12-Feb-2009_End
                    }
                    if (result == null) {
                        result = "0|Unknown Bar";
                    }
                    barCache.put(i, result);
                } catch (SQLException sqle) {
                    logger.dbError("Database error: " + sqle.getMessage());
                }
            }
        }
        return barCache;
    }

    /** Returns net values for all the lines for this period.  Will return a map
     *  between line-ids and net-value.  
     *
     *  @return a map from line-id to net-value.
     */
    public Map<Integer, String> getStation(RegisteredConnection conn) {
        String sql = "SELECT st.id, st.name FROM line l " +
                "LEFT JOIN station st on st.id = l.station " +
                "WHERE l.id = ?";
        String result = "0|Unknown Station";

        if (stationCache.isEmpty()) {
            for (Integer i : linePeriods.keySet()) {
                try {
                    //logger.debug("PS  ... connecting to db for line: " + i + " to getStation: ");
                    PreparedStatement ps = conn.prepareStatement(sql);
                    ps.setInt(1, i);
                    ResultSet rs = ps.executeQuery();
                    if (rs.next()) {
                        //NischaySharma_12-Feb-2009_Start: result is changed to contain both Id and Name
                        if (rs.getString(1) != null) {
                            result = rs.getString(1) + "|" + rs.getString(2);
                        }
                        //NischaySharma_12-Feb-2009_End
                    }
                    if (result == null) {
                        result = "0|Unknown Station";
                    }
                    stationCache.put(i, result);
                } catch (SQLException sqle) {
                    logger.dbError("Database error: " + sqle.getMessage());
                }
            }
        }
        return stationCache;
    }

    //NischaySharma_11-Feb-2009_Start: Add new methoid to fecth the location for the line 
    // in the given period
    /** Returns net values for all the locations for this period.  Will return a map
     *  between line-ids and net-value.
     *
     *  @return a map from line-id to net-value.
     */
    public Map<Integer, String> getLocation(RegisteredConnection conn) {
        String sql = "SELECT loc.id, loc.name FROM line l " +
                "LEFT JOIN bar b on b.id = l.bar " +
                "LEFT JOIN location loc on loc.id = b.location " +
                "WHERE l.id = ?";
        String result = "0|Unknown Location";

        if (locationCache.isEmpty()) {
            for (Integer i : linePeriods.keySet()) {
                try {
                    //logger.debug("PS  ... connecting to db for line: " + i + " to getLocation: ");
                    PreparedStatement ps = conn.prepareStatement(sql);
                    ps.setInt(1, i);
                    ResultSet rs = ps.executeQuery();
                    if (rs.next()) {
                        //NischaySharma_13-Feb-2009_Start: result is changed to contain both Id and Name
                        if (rs.getString(1) != null) {
                            result = rs.getString(1) + "|" + rs.getString(2);
                        }
                        //NischaySharma_13-Feb-2009_End
                    }
                    if (result == null) {
                        result = "0|Unknown Location";
                    }
                    locationCache.put(i, result);
                } catch (SQLException sqle) {
                    logger.dbError("Database error: " + sqle.getMessage());
                }
            }
        }
        return locationCache;
    }
    //NischaySharma_11-Feb-2009_End

    //NischaySharma_12-Feb-2009_Start: Add new method to fecth the product category for the line
    /** Returns net values for all the locations for this period.  Will return a map
     *  between line-ids and net-value.
     *
     *  @return a map from line-id to net-value.
     */
    public Map<Integer, Integer> getProductCategory(RegisteredConnection conn) {
        String sql = "SELECT pro.id, pro.category FROM line l " +
                "LEFT JOIN product pro on pro.id = l.product " +
                "WHERE l.id = ?";

        if (productCategoryCache.isEmpty()) {
            for (Integer i : linePeriods.keySet()) {
                try {
                    //logger.debug("PS  ... connecting to db for line: " + i + " to getProductCategory: ");
                    PreparedStatement ps = conn.prepareStatement(sql);
                    ps.setInt(1, i);
                    ResultSet rs = ps.executeQuery();
                    if (rs.next()) {
                        productCategoryCache.put(i, rs.getInt(2));
                    }
                } catch (SQLException sqle) {
                    logger.dbError("Database error: " + sqle.getMessage());
                }
            }
        }
        return productCategoryCache;
    }
    //NischaySharma_12-Feb-2009_End
}
