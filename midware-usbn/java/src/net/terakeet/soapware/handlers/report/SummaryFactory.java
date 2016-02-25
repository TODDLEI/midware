/*
 * SummaryFactory.java
 *
 * Created on January 2, 2007, 4:12 PM
 *
 * (c) 2007 Terakeet Corp.
 */

package net.terakeet.soapware.handlers.report;

import java.util.Set;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Set;
import java.util.HashSet;
import java.util.List;
import java.util.ArrayList;
import java.util.Date;
import net.terakeet.soapware.RegisteredConnection;
import net.terakeet.util.MidwareLogger;

/**  A Summary Factory gathers poured and sold data from the MySQL database, and packages it
 *  into ReportSummaries.  These summaries are returned in sequence as needed.
 */
public class SummaryFactory {
       
    ReportDescriptor rd;
    ArrayList<ElapsedTime> queryTimes;
    String sqlError;
    RegisteredConnection conn;
    ResultSet soldRs,pouredRs;
    MidwareLogger logger;
    
    boolean pouredFinished;
    boolean soldFinished;
    
    ReportSummary next;
    
    int summaryCount; // summaries returned
    private final int summariesPerSet = 500;
    
    /** Create a new SummaryFactory from a ReportDescriptor and an open connection.
     *  @param descriptor describes the report
     *  @param connection an open connection
     */
    public SummaryFactory(ReportDescriptor descriptor, RegisteredConnection connection) {
        rd = descriptor;
        conn = connection;
        queryTimes = new ArrayList<ElapsedTime>();
        sqlError = ""; 
        
        pouredFinished = !(rd.includePoured() || rd.includeVariance());
        soldFinished = !(rd.includeSold() || rd.includeVariance());
        logger = new MidwareLogger(SummaryFactory.class.getName());
        summaryCount = 0;
    }

    private boolean loadNext() {
        if (!pouredFinished) {
            if (pouredRs == null) {
                pouredRs = loadPoured();
            }
            try {
                if (pouredRs != null && pouredRs.next()) {
                    next = summaryFromPouredSummary(pouredRs);
                    return true;
                } else {
                    logger.debug(" Summary Factory has no more poured Summaries");
                    pouredFinished = true;
                }
            } catch (SQLException sqle) {
                sqlError = sqle.toString();
                pouredFinished = true;
            }
        }
        if (!soldFinished) {
            if (soldRs == null) {
                soldRs = loadSold();
            }
            try {
                if (soldRs != null && soldRs.next()) {
                    next = summaryFromSoldSummary(soldRs);
                    return true;
                } else {
                    logger.debug(" Summary Factory has no more sold Summaries");
                    soldFinished = true;
                }
            } catch (SQLException sqle) {
                sqlError = sqle.toString();
                soldFinished = true;
            }
        }
        return false;
    }
    
    private String queryString(String table, FilterType type) {
        //todo: param checking
        StringBuilder result = new StringBuilder();       
        if (type == FilterType.LOCATION || type == FilterType.PRODUCT) {
            result.append(" SELECT location, product, date, value ");
            result.append(" FROM ");
            result.append(table);
            result.append(" WHERE ");
            result.append(type.toString().toLowerCase());
            result.append("=? AND date BETWEEN ? AND ?");
        } else if (type == FilterType.CUSTOMER) {
            result.append(" SELECT s.location, s.product, s.date, s.value ");
            result.append(" FROM location l LEFT JOIN ");
            result.append(table);
            result.append(" s ON s.location = l.id ");
            result.append(" WHERE l.customer=? AND s.date BETWEEN ? AND ?");           
        } else if (type == FilterType.STATE) {
            result.append(" SELECT s.location, s.product, s.date, s.value ");
            result.append(" FROM location l LEFT JOIN ");
            result.append(table);
            result.append(" s ON s.location = l.id ");
            result.append(" WHERE l.addrState=? AND date BETWEEN ? AND ?");                  
        } else {
            throw new IllegalArgumentException("Unknown type passed to SummaryFactory.queryString: "+type);
        }
        return result.toString();
    }
    
    private ResultSet loadSold() {
        ResultSet rs = null;
        logger.debug(" SummaryFactory loading SALES readings");
        try {
            PreparedStatement stmt = null;
            ElapsedTime timer = new ElapsedTime("Sales Query");
            String query = queryString("soldSummary",rd.getFilterType());
            logger.debug(" Executing Sold Query: "+query);
            stmt = conn.prepareStatement(query);
            stmt.setString(1,rd.getFilter());
            stmt.setString(2,rd.getStartDate());
            stmt.setString(3,rd.getEndDate());
            rs = stmt.executeQuery();
            timer.stopTimer();
            queryTimes.add(timer);
        } catch (SQLException sqle) {
            sqlError = sqle.toString();
        }     
        return rs;
    }
        
    private ResultSet loadPoured() {
        ResultSet rs = null;
        logger.debug(" SummaryFactory loading POURED readings");
        try {
            PreparedStatement stmt = null;
            ElapsedTime timer = new ElapsedTime("Poured Query");
            String query = queryString("pouredSummary",rd.getFilterType());
            logger.debug(" Executing Poured Query: "+query);
            stmt = conn.prepareStatement(query);          
            stmt.setString(1,rd.getFilter());
            stmt.setString(2,rd.getStartDate());
            stmt.setString(3,rd.getEndDate());
            rs = stmt.executeQuery();
            timer.stopTimer();
            queryTimes.add(timer);
        } catch (SQLException sqle) {
            sqlError = sqle.toString();
        } 
        return rs;
    }
    
    /** Returns the number of seconds it took to execute the database queries
     *  @return a set of elapsed times
     */
    public ArrayList<ElapsedTime> getQueryTimes() {
        return queryTimes;
    }
    
    /** Returns true if this factory has more summaries to create through getNext(), false otherwise
     */
    public boolean hasMoreSummaries() {
        if (next != null) {
            return true;
        } else {
            return loadNext();
        }
    }

    
    private ReportSummary summaryFromPouredSummary(ResultSet rs) throws SQLException{
        int location = rs.getInt(1);
        int product = rs.getInt(2);
        //Date date = new Date(rs.getDate(3).getTime());
        String date = rs.getString(3);
        double ounces = rs.getDouble(4);
        
        return new ReportSummary(location,product,ounces,date,OunceType.POURED);
    }
    
    private ReportSummary summaryFromSoldSummary(ResultSet rs) throws SQLException{
        int location = rs.getInt(1);
        int product = rs.getInt(2);
        //Date date = new Date(rs.getDate(3).getTime());
        String date = rs.getString(3);
        double ounces = rs.getDouble(4);
        
        return new ReportSummary(location,product,ounces,date,OunceType.SOLD);
    }
    
    private ReportSummary getSingle() {
        ReportSummary result = null;
        if (next != null) {
            result = next;
        } else if (loadNext()) {
            result = next;
        }
        next = null; // Clear our next
        return result;
    }
    
    /** Returns one or more summaries.  If this factory is out of summaries, an
     *  empty Set will be returned, never null.
     */
    public List<ReportSummary> getNext() {
        ArrayList<ReportSummary> result = new ArrayList<ReportSummary>(summariesPerSet);
        for (int i=0; i<summariesPerSet; i++) {
            ReportSummary toAdd = getSingle();
            if (toAdd != null) {
                //logger.debug(" SummaryFactory returning: "+toAdd.toString());
                result.add(toAdd);
                summaryCount++;
            } else {
                logger.debug(" Summary Factory is out of readings.  Total Returned: "+summaryCount);
                break;
            }
        }
        return result;
    }
    
}
