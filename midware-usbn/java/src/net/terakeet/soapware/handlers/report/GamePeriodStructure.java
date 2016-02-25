/*
 * GamePeriodStructure.java
 *
 * Created on August 29, 2005, 10:50 AM
 *
 */
package net.terakeet.soapware.handlers.report;

import java.util.*;
import org.apache.log4j.Logger;
import net.terakeet.util.MidwareLogger;
import net.terakeet.soapware.*;
import java.sql.ResultSet;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/** GamePeriodStructure
 *
 */
public class GamePeriodStructure {
    private static MidwareLogger logger = new MidwareLogger(PeriodStructure.class.getName());
    private GamePeriodStructure previous;
    private HashMap<Integer, GameSet> gameSets;
    private HashMap<Integer, String> locationCache;
    private HashMap<Integer, String> barCache;
    private HashMap<Integer, Integer> productCache;
    private HashMap<Integer, Integer> productCategoryCache;
    private HashMap<Integer, String> stationCache;
    private HashMap<Integer, String> zoneCache;
    private int gameId;

    /** Creates a new instance of GamePeriodStructure
     *  @param previous the previous GamePeriodStructure, can be null if this is the first
     *  @param date the start date of this structure
     */
    public GamePeriodStructure(GamePeriodStructure previous, int i) {
        this.previous = previous;
        gameSets = new HashMap<Integer, GameSet>();
        locationCache = new HashMap<Integer, String>();
        barCache = new HashMap<Integer, String>();
        productCache = new HashMap<Integer, Integer>();
        productCategoryCache = new HashMap<Integer, Integer>();
        stationCache = new HashMap<Integer, String>();
        zoneCache = new HashMap<Integer, String>();
        gameId = i;
    }

    /** Gets a LinePeriod for the line, initializing it if necessary.
     *  @param line the line ID for the period
     */
    protected GameSet getGameSet(int station, int product, int game) {
        Integer gameKey = new Integer(Integer.parseInt(String.valueOf(station)+String.valueOf(product)));
        GameSet gs = gameSets.get(gameKey);
        if (null == gs) {
            gs = new GameSet(station, product, game);
            gameSets.put(gameKey, gs);
        }
        return gs;
    }

    /** Add a line reading to this period.
     *  @param line the line id
     *  @param value the value of the line
     *  @param d the datetime of the reading
     */
    public void addReading(int station, int product, int game, double value, Date d) {
        GameSet gs = getGameSet(station, product, game);
        gs.addReading(value, d);
    }

    /** Returns net values for all the lines for this period.  Will return a map
     *  between line-ids and net-value.
     *
     *  @return a map from line-id to net-value.
     */
    public Map<GameSet, Double> getValues(RegisteredConnection conn) {
        GameSet gs = null;
        double net = 0.0;
        HashMap<GameSet, Double> values = new HashMap<GameSet, Double>();
        for (Integer i : gameSets.keySet()) {
            gs = gameSets.get(i);
            if (gs.getReadingCount() > 0) {
                //setData(conn, gs.getStation());
                net = gs.getFirstValue() + gs.getOffset();
                //logger.debug(":: "+i.intValue()+" - "+gs.getFirstValue()+" - "+gs.getOffset()+" = "+net);
            } else {
                // just use the offset
                net = gs.getOffset();
            }
            values.put(gameSets.get(i), new Double(net));
        }
        return values;
    }

    /** Returns net values for all the lines for this period.  Will return a map
     *  between line-ids and net-value.
     *
     *  @return a map from line-id to net-value.
     */
    public Map<String, GameSet> getStationValues(RegisteredConnection conn) {
        GameSet gs = null;
        HashMap<String, GameSet> values = new HashMap<String, GameSet>();
        
        for (Integer i : gameSets.keySet()) {
            gs = gameSets.get(i);
            values.put(gs.getStation() + ":" + gs.getProduct(), gs);
        }
        return new TreeMap<String, GameSet>(values);
    }

    /** Returns net values for all the lines for this period.  Will return a map
     *  between line-ids and net-value.
     *
     *  @return a map from line-id to net-value.
     */
    public void setData(RegisteredConnection conn, Integer parameter, String condition) {
        String sql = " SELECT l.id, l.name, z.id, z.name, b.id, b.name, st.id, st.name FROM station st LEFT JOIN bar b ON b.id = st.bar " +
                " LEFT JOIN zone z ON z.id = b.zone LEFT JOIN location l ON l.id = z.location " + condition +
                " ORDER BY l.name, z.name, b.name, st.name;  ";

        try {
            PreparedStatement ps = conn.prepareStatement(sql);
            ps.setInt(1, parameter);
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                locationCache.put(rs.getInt(7), rs.getString(1) + "|" + rs.getString(2));
                zoneCache.put(rs.getInt(7), rs.getString(3) + "|" + rs.getString(4));
                barCache.put(rs.getInt(7), rs.getString(5) + "|" + rs.getString(6));
                stationCache.put(rs.getInt(7), rs.getString(7) + "|" + rs.getString(8));
                //logger.debug("Created Sets");
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
    public void setData(RegisteredConnection conn, Integer station) {
        String sql = " SELECT l.id, l.name, z.id, z.name, b.id, b.name, st.id, st.name FROM station st LEFT JOIN bar b ON b.id = st.bar " +
                " LEFT JOIN zone z ON z.id = b.zone LEFT JOIN location l ON l.id = z.location" +
                " WHERE st.id = ? ORDER BY l.name, z.name, b.name, st.name; ";

        try {
            //logger.debug("Creating Data Set for Line# " + line);
            PreparedStatement ps = conn.prepareStatement(sql);
            ps.setInt(1, station);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                locationCache.put(station, rs.getString(1) + "|" + rs.getString(2));
                zoneCache.put(station, rs.getString(3) + "|" + rs.getString(4));
                barCache.put(station, rs.getString(5) + "|" + rs.getString(6));
                stationCache.put(station, rs.getString(7) + "|" + rs.getString(8));
                //logger.debug("Created Sets");
            } else {
                locationCache.put(station, "0|Unknown Location");
                zoneCache.put(station, "0|Unknown Zone");
                barCache.put(station, "0|Unknown Bar");
                stationCache.put(station, "0|Unknown Station");
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
    public Map<Integer, String> getStation(RegisteredConnection conn) {
        String sql = "SELECT st.id, st.name FROM station st " +
                " WHERE st.id = ?";
        String result = "0|Unknown Station";
        GameSet gs = null;
        if (stationCache.isEmpty()) {
            for (Integer i : gameSets.keySet()) {
                gs = gameSets.get(i);
                try {
                    PreparedStatement ps = conn.prepareStatement(sql);
                    ps.setInt(1, gs.getStation());
                    ResultSet rs = ps.executeQuery();
                    if (rs.next()) {
                        if (rs.getString(1) != null) {
                            result = rs.getString(1) + "|" + rs.getString(2);
                        }
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

    /** Returns net values for all the lines for this period.  Will return a map
     *  between line-ids and net-value.
     *
     *  @return a map from line-id to net-value.
     */
    public Map<Integer, String> getBar(RegisteredConnection conn) {
        String sql = "SELECT b.id, b.name FROM station st LEFT JOIN bar b ON b.id = st.bar " +
                " WHERE st.id = ?";
        String result = "0|Unknown Bar";
        GameSet gs = null;
        if (barCache.isEmpty()) {
            for (Integer i : gameSets.keySet()) {
                gs = gameSets.get(i);
                try {
                    PreparedStatement ps = conn.prepareStatement(sql);
                    ps.setInt(1, gs.getStation());
                    ResultSet rs = ps.executeQuery();
                    if (rs.next()) {
                        if (rs.getString(1) != null) {
                            result = rs.getString(1) + "|" + rs.getString(2);
                        }
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
    public Map<Integer, String> getZone(RegisteredConnection conn) {
        String sql = "SELECT z.id, z.name FROM station st LEFT JOIN bar b ON b.id = st.bar LEFT JOIN zone z ON z.id = b.zone " +
                " WHERE st.id = ?";
        String result = "0|Unknown Zone";
        GameSet gs = null;
        if (zoneCache.isEmpty()) {
            for (Integer i : gameSets.keySet()) {
                gs = gameSets.get(i);
                try {
                    PreparedStatement ps = conn.prepareStatement(sql);
                    ps.setInt(1, gs.getStation());
                    ResultSet rs = ps.executeQuery();
                    if (rs.next()) {
                        if (rs.getString(1) != null) {
                            result = rs.getString(1) + "|" + rs.getString(2);
                        }
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
    public Map<Integer, String> getLocation(RegisteredConnection conn) {
        String sql = "SELECT l.id, l.name FROM station st LEFT JOIN bar b ON b.id = st.bar LEFT JOIN location l ON l.id = b.location " +
                " WHERE st.id = ?";
        String result = "0|Unknown Location";
        GameSet gs = null;
        if (locationCache.isEmpty()) {
            for (Integer i : gameSets.keySet()) {
                gs = gameSets.get(i);
                try {
                    PreparedStatement ps = conn.prepareStatement(sql);
                    ps.setInt(1, gs.getStation());
                    ResultSet rs = ps.executeQuery();
                    if (rs.next()) {
                        if (rs.getString(1) != null) {
                            result = rs.getString(1) + "|" + rs.getString(2);
                        }
                    }
                    if (result == null) {
                        result = "0|Unknown Location";
                    }
                    stationCache.put(i, result);
                } catch (SQLException sqle) {
                    logger.dbError("Database error: " + sqle.getMessage());
                }
            }
        }
        return locationCache;
    }
}
