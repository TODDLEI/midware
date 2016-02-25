package net.terakeet.soapware.handlers;
/*
 * ReportHandler.java
 *
 * Created on August 28, 2005, 7:15 PM
 *
 * To change this template, choose Tools | Options and locate the template under
 * the Source Creation and Management node. Right-click the template and choose
 * Open. You can then make changes to the template in the Source Editor.
 */

import net.terakeet.soapware.DateParameter;
import net.terakeet.soapware.handlers.report.*;
import net.terakeet.soapware.Handler;
import net.terakeet.usbn.SQLQueryExtractor;
import net.terakeet.soapware.HandlerException;
import net.terakeet.soapware.HandlerUtils;
import net.terakeet.soapware.SOAPMessage;
import net.terakeet.soapware.DatabaseConnectionManager;
import net.terakeet.soapware.RegisteredConnection;
import net.terakeet.soapware.handlers.report.*;
import net.terakeet.util.MidwareLogger;
import net.terakeet.usbn.LineReading;
import org.dom4j.Element;
import java.util.Date;
import java.util.Collection;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;
import java.util.SortedSet;
import java.util.Iterator;
import java.util.Vector;
import java.util.ArrayList;
import java.text.SimpleDateFormat;
import java.text.ParseException;
import java.sql.*;
import java.text.DecimalFormat;
import java.util.LinkedList;
import net.terakeet.util.printableMenu;

public class ReportHandler implements Handler {

    private MidwareLogger logger;
    private static SimpleDateFormat dateFormat =
            new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
    private static SimpleDateFormat newDateFormat =
            new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private static SimpleDateFormat dbDateFormat =
            new SimpleDateFormat("yyyy-MM-dd");
    private ProductMap productMap;
    private CategoryMap categoryMap;
    private SegmentMap segmentMap;
    private EventMap eventMap;
    private LineOffsetMap lineOffsetMap;
    private LineMap lineTypeMap;
    private BarMap barMap;
    private StationMap stationMap;
    private LocationMap locationMap;
    private ParentLevelMap parentLevelMap;
    private ChildLevelMap childLevelMap;
    private RegionProductMap regionProductMap;
    private RegionExclusionMap regionExclusionMap;
    private RegisteredConnection conn;
    static final String connName = "report";
    //NischaySharma_18-Sep-2009_Start
    private final String DailyVariance = "DailyVariance";
    private final String DailyBrandPerf = "DailyBrandPerf";
    private final String DailyInterrupt = "DailyInterrupt";
    private final String DailyLineCleaning = "DailyLineCleaning";
    //NischaySharma_18-Sep-2009_End

    /**
     * Creates a new instance of ReportHandler
     */
    public ReportHandler() {
        logger = new MidwareLogger(ReportHandler.class.getName());
        productMap = null;
        barMap = null;
        stationMap = null;
        categoryMap = null;
        segmentMap = null;
        eventMap = null;
        locationMap = null;
        lineOffsetMap = null;
        lineTypeMap = null;
        conn = null;
    }

    public void handle(Element toHandle, Element toAppend) throws HandlerException {

        String function = toHandle.getName();
        //String responseNamespace = (String)SOAPMessage.getURIMap().get("tkmsg");

        logger = new MidwareLogger(ReportHandler.class.getName(), function);
        logger.debug("ReportHandler processing method: " + function);
        logger.xml("request: " + toHandle.asXML());

        conn = DatabaseConnectionManager.getNewConnection(connName, function + " (ReportHandler)");

        long startTime = System.currentTimeMillis();
        try {
            if ("getReport".equals(function)) {
                getReportNew(toHandle, responseFor(function, toAppend));
            } else if ("getReportNew".equals(function)) {
                getReportNew(toHandle, responseFor(function, toAppend));
            } else if ("getConcessionsPouredReport".equals(function)) {
                getConcessionsPouredReport(toHandle, responseFor(function, toAppend));
            } else if ("getConcessionsSalesReport".equals(function)) {
                getConcessionsSalesReport(toHandle, responseFor(function, toAppend));
            } else if ("getGameTimeReport".equals(function)) {
                getGameTimeReport(toHandle, responseFor(function, toAppend));
            } else if ("getDashBoardReport".equals(function)) {
                getDashBoardReport(toHandle, responseFor(function, toAppend));
            } else if ("getLineCleaningReport".equals(function)) {
                getLineCleaningReport(toHandle, responseFor(function, toAppend));
            } else if ("getDataDelayReport".equals(function)) {
                getDataDelayReport(toHandle, responseFor(function, toAppend));
            } else if ("getSummaryReport".equals(function)) {
                getSummaryReport(toHandle, responseFor(function, toAppend));
            } else if ("getSummaryTotalReport".equals(function)) {
                getSummaryTotalReport(toHandle, responseFor(function, toAppend));
            } else if ("getSalesReport".equals(function)) {
                getSalesReport(toHandle, responseFor(function, toAppend));
            } else if ("getSalesReport".equals(function)) {
                getSalesReport(toHandle, responseFor(function, toAppend));
            } else if ("summaryReport".equals(function)) {
                summaryReport(toHandle, responseFor(function, toAppend));
            } else if ("getSalesReportNew".equals(function)) {
                getSalesReportNew(toHandle, responseFor(function, toAppend));
            } else if ("getRealTimeReport".equals(function)) {
                getRealTimeReport(toHandle, responseFor(function, toAppend));
            } else if ("getPreOpenHoursSummaryReport".equals(function)) {
                getPreOpenHoursSummaryReport(toHandle, responseFor(function, toAppend));
            } else if ("getAfterHoursSummaryReport".equals(function)) {
                getAfterHoursSummaryReport(toHandle, responseFor(function, toAppend));
            } else if ("getCorporateReportDaily".equals(function)) {
                getCorporateReportDaily(toHandle, responseFor(function, toAppend));
            } else if ("getBreweryReport".equals(function)) {
                getBreweryReport(toHandle, responseFor(function, toAppend));
            } else if ("getBreweryReport".equals(function)) {
                getBreweryReport(toHandle, responseFor(function, toAppend));
            }else if ("getUnclaimData".equals(function)) {
                getUnclaimData(toHandle, responseFor(function, toAppend));
            } else if ("generatePreOpenHoursSummary".equals(function)) {
                generatePreOpenHoursSummary(toHandle, responseFor(function, toAppend));
            } else if ("getBeerBoardIndexReport".equals(function)) {
                getBeerBoardIndexReport(toHandle, responseFor(function, toAppend));
            }  else {
                logger.generalWarning("Unknown function '" + function + "'.");
            }
        } catch (Exception e) {
            if (e instanceof HandlerException) {
                throw (HandlerException) e;
            } else {
                logger.midwareError("Non-handler exception thrown in ReportHandler: " + e.toString());
                throw new HandlerException(e);
            }
        } finally {
            // Log elapsed time
            long endTime = System.currentTimeMillis();
            DecimalFormat twoPlaces = new DecimalFormat("0.00");
            double elapsedSeconds = (endTime - startTime) / 1000.0;
            logger.debug("Report complete, took " + twoPlaces.format(elapsedSeconds) + " sec");

            // Log database use
            int queryCount = conn.getQueryCount();
            logger.dbAction("Executed " + queryCount + " report quer" + (queryCount == 1 ? "y" : "ies"));

            conn.close();
        }
        logger.debug("S - Closed all connections");
        logger.xml("response: " + toAppend.asXML());
    }

    private String nullToEmpty(String s) {
        return (null == s) ? "" : s;
    }
    
    private int getCallerId(Element toHandle) throws HandlerException {
       try {
           int callerId                     = HandlerUtils.getOptionalInteger(HandlerUtils.getRequiredElement(toHandle,"caller"),"callerId");;           
           int securityLevel                = HandlerUtils.getOptionalInteger(HandlerUtils.getRequiredElement(toHandle,"caller"),"securityLevel"); 
           if(securityLevel == 0) {
               callerId                     = 0;
           }
        return callerId;
        
        } catch(Exception e){
            logger.debug(e.getMessage());
            return 0;
        }
    }


    private Element responseFor(String s, Element e) {
        String responseNamespace = (String) SOAPMessage.getURIMap().get("tkmsg");
        return e.addElement("m:" + s + "Response", responseNamespace);
    }

    private void close(Statement s) {
        if (s != null) {
            try {
                s.close();
            } catch (SQLException sqle) {
            }
        }
    }

    private void close(ResultSet rs) {
        if (rs != null) {
            try {
                rs.close();
            } catch (SQLException sqle) {
            }
        }
    }

    private void close(Connection c) {
        if (c != null) {
            try {
                c.close();
            } catch (SQLException sqle) {
            }
        }
    }

    private void close(RegisteredConnection c) {
        c.close();
    }

    private void addErrorDetail(Element toAppend, String message) {
        toAppend.addElement("error").addElement("detail").addText(message);
    }

    private void addNoteDetail(Element toAppend, String message) {
        toAppend.addElement("note").addElement("detail").addText(message);
    }

    /** Determines which period the date d falls in, given a report period.
     * If there are N periods in the report, the result will fall in the range
     * [0,N-1].  Will throw an exception if the date is not in the report interval
     *
     *  @param per the reporting period
     *  @return the zero-indexed period position
     */
    private int classifyPeriod(ReportPeriod per, Date d) {
        return -1;
    }

    /** Calculates the number of periods in a report.  For a daily report over
     * six days, there would be six periods.  Based on the start and end times,
     * the internval may contain leading/trailing partial periods, which will
     * also be included in the total.  For example, if a weekly report over 28
     * days did not start on a Monday, there would be 5 periods in the report,
     * not 4.
     *
     *   @param per the reporting period
     *   @return the number of periods
     *
     */
    private int maxPeriods(ReportPeriod per) {
        return -1;
    }

    /** Converts a line-value map to a product-value map, combining lines that
     *   are pouring the same product.
     *
     *   @param lines a map from line-id to values
     *   @return a map from product-id to values
     */
    private Map<Integer, Double> linesToProducts(Map<Integer, Double> lines) {
        return null;
    }

    /** Determines if the difference between two values is great enough to be
     * considered a spike.
     *   @param v1 the earlier reading
     *   @param v2 the later reading
     *   @return true if the difference is a spike, false otherwise
     */
    private boolean detectSpike(double v1, double v2) {
        return false;
    }

    private boolean validateProduct() {
        return true;
    }

    private void checkRecReportParams(Element toHandle, Element toAppend) throws HandlerException {
        Element productList = HandlerUtils.getRequiredElement(toHandle, "product");

    }

    private Date getRequiredDate(Element toHandle, String tagName) throws HandlerException {
        return HandlerUtils.getRequiredTimestamp(toHandle, tagName).toDate();
    }

    private String getExclusionDesciption(int exclusion) throws HandlerException {

        String selectExclusionDescription = "SELECT description FROM exclusionDescription WHERE exclusion = ? ";
        String exclusionDescription = " No information avaiable";
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            stmt = conn.prepareStatement(selectExclusionDescription);
            stmt.setInt(1, exclusion);
            rs = stmt.executeQuery();
            if (rs.next()) {
                exclusionDescription = rs.getString(1);
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(rs);
            close(stmt);
        }
        return exclusionDescription;
    }

    private void appendProjectionDetailsXML(Element toAppend, ResultSet rs) throws HandlerException {

        Element projectionData = toAppend.addElement("pDetails");
        try {
            while (rs.next()) {
                projectionData.addElement("pDate").addText(HandlerUtils.nullToEmpty(rs.getString(1)));
                projectionData.addElement("pType").addText(HandlerUtils.nullToEmpty(rs.getString(2)));
                projectionData.addElement("pDetail").addText(HandlerUtils.nullToEmpty(rs.getString(3)));
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(rs);
        }
    }

    private void appendExclusionSummaryReportByLocationXML(Element toAppend, ResultSet rs) throws HandlerException {

        barMap = new BarMap(conn);
        locationMap = new LocationMap(conn);

        Element exclusionData = toAppend.addElement("exclusionData");
        try {
            while (rs.next()) {
                Element details = exclusionData.addElement("exclusionDetails");
                details.addElement("locationId").addText(String.valueOf(rs.getInt(1)));
                details.addElement("locationName").addText(HandlerUtils.nullToEmpty(locationMap.getLocation(rs.getInt(1))));
                details.addElement("barId").addText(String.valueOf(rs.getInt(2)));
                details.addElement("barName").addText(HandlerUtils.nullToEmpty(barMap.getBar(rs.getInt(2))));
                details.addElement("exclusionId").addText(String.valueOf(rs.getInt(3)));
                details.addElement("exclusionDescription").addText(HandlerUtils.nullToEmpty(getExclusionDesciption(rs.getInt(3))));
                details.addElement("exclusionDate").addText(HandlerUtils.nullToEmpty(rs.getString(4)));
                //logger.debug("Ex Date:"+rs.getString(4));
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(rs);
        }
        barMap = null;
        locationMap = null;
    }

    private void appendSummaryReportByLocationXML(Element toAppend, ResultSet rs) throws SQLException {

        Map<Date, ArrayList> summarySet = new HashMap<Date, ArrayList>();
        Map<Integer, Date> dateArray = new HashMap<Integer, Date>();

        Date previous = null;
        ArrayList al = new ArrayList();

        productMap = new ProductMap(conn);
        barMap = new BarMap(conn);
        locationMap = new LocationMap(conn);

        int i = 0;
        int j = 0;

        while (rs.next()) {

            if (previous == null) {
                previous = new Date(rs.getTimestamp(5).getTime());
            }

            if (previous.compareTo(new Date(rs.getTimestamp(5).getTime())) == 0) {
                al.add(new SummaryStructure(rs.getInt(1), rs.getInt(2), rs.getInt(3), rs.getDouble(4)));
                i++;
            } else {
                summarySet.put(previous, al);
                i = 0;
                j++;
                dateArray.put(j, previous);
                al = new ArrayList();
                al.add(new SummaryStructure(rs.getInt(1), rs.getInt(2), rs.getInt(3), rs.getDouble(4)));
                i++;
                previous = new Date(rs.getTimestamp(5).getTime());
            }

        }
        close(rs);
        if (i > 0) {
            summarySet.put(previous, al);
            j++;
            dateArray.put(j, previous);
            i = 0;
        }

        for (i = 1; i <= j; i++) {
            ArrayList<SummaryStructure> arrayss = summarySet.get(dateArray.get(i));


            Element period = toAppend.addElement("period");
            period.addElement("periodDate").addText(String.valueOf(dateFormat.format(dateArray.get(i))));
             //logger.debug("period Date:"+String.valueOf(dateFormat.format(dateArray.get(i))));

             boolean isBrasstap          = false;
             int locCheck                = 0;

            Map<Integer, String> locationList = new HashMap<Integer, String>();
            for (SummaryStructure newss : arrayss) {
                try {
                if(locCheck!=newss.getLocation()){
                        isBrasstap = checkBrasstapLocation(1, newss.getLocation(), conn);
                        locCheck    = newss.getLocation();
                    }

                Element details = period.addElement("product");
                details.addElement("product").addText(String.valueOf(newss.getProduct()));
                 if(!isBrasstap){
                        details.addElement("name").addText(HandlerUtils.nullToEmpty(productMap.getProduct(newss.getProduct())));
                    } else {

                        details.addElement("name").addText(HandlerUtils.nullToEmpty(getBrasstapProductName(newss.getProduct())));
                    }
               // details.addElement("name").addText(HandlerUtils.nullToEmpty(productMap.getProduct(newss.getProduct())));
                details.addElement("ounces").addText(String.valueOf(newss.getValue()));
                details.addElement("customerId").addText(String.valueOf(locationMap.getCustomer(newss.getLocation())));
                details.addElement("locationId").addText(String.valueOf(newss.getLocation()));
                details.addElement("locationName").addText(HandlerUtils.nullToEmpty(locationMap.getLocation(newss.getLocation())));
                details.addElement("barId").addText(String.valueOf(newss.getBar()));
                details.addElement("barName").addText(HandlerUtils.nullToEmpty(barMap.getBar(newss.getBar())));
                } catch(Exception e){}


            }


        }

        productMap = null;
        barMap = null;
        locationMap = null;
    }

    private void appendSummaryReportForBeerBoardXML(Element toAppend, int reportGroupBy, HashMap<Integer, String> childMap, ResultSet rs) throws SQLException {

        Map<Date, ArrayList> summarySet = new HashMap<Date, ArrayList>();
        Map<Integer, Date> dateArray = new HashMap<Integer, Date>();

        Date previous = null;
        ArrayList al = new ArrayList();

        productMap = new ProductMap(conn);
        barMap = new BarMap(conn);
        locationMap = new LocationMap(conn);

        int i = 0;
        int j = 0;

        while (rs.next()) {

            if (previous == null) {
                previous = new Date(rs.getTimestamp(5).getTime());
            }

            if (previous.compareTo(new Date(rs.getTimestamp(5).getTime())) == 0) {
                al.add(new SummaryStructure(rs.getInt(1), rs.getInt(2), rs.getInt(3), rs.getDouble(4)));
                i++;
            } else {
                summarySet.put(previous, al);
                i = 0;
                j++;
                dateArray.put(j, previous);
                al = new ArrayList();
                al.add(new SummaryStructure(rs.getInt(1), rs.getInt(2), rs.getInt(3), rs.getDouble(4)));
                i++;
                previous = new Date(rs.getTimestamp(5).getTime());
            }

        }
        close(rs);
        if (i > 0) {
            summarySet.put(previous, al);
            j++;
            dateArray.put(j, previous);
            i = 0;
        }        
        
        for (i = 1; i <= j; i++) {
            ArrayList<SummaryStructure> arrayss = summarySet.get(dateArray.get(i));
            

            Element period = toAppend.addElement("period");
            period.addElement("periodDate").addText(String.valueOf(dateFormat.format(dateArray.get(i)))); 
             //logger.debug("period Date:"+String.valueOf(dateFormat.format(dateArray.get(i))));
            
             boolean isBrasstap          = false;
             int locCheck                = 0;
             
            Map<Integer, String> locationList = new HashMap<Integer, String>();
            for (SummaryStructure newss : arrayss) {
                try {
                if(locCheck!=newss.getLocation()){
                        isBrasstap = checkBrasstapLocation(1, newss.getLocation(), conn);
                        locCheck    = newss.getLocation();
                    }
                
                Element details = period.addElement("product");
                int productId = newss.getProduct();
                int locationId = newss.getLocation();
                details.addElement("product").addText(String.valueOf(newss.getProduct()));
                if(!isBrasstap){                        
                        details.addElement("name").addText(HandlerUtils.nullToEmpty(productMap.getProduct(newss.getProduct())));
                    } else {
                        
                        details.addElement("name").addText(HandlerUtils.nullToEmpty(getBrasstapProductName(newss.getProduct())));
                    }
                   // details.addElement("name").addText(HandlerUtils.nullToEmpty(productMap.getProduct(newss.getProduct())));
                    details.addElement("ounces").addText(String.valueOf(newss.getValue()));
                    details.addElement("customerId").addText(String.valueOf(locationMap.getCustomer(newss.getLocation())));
                    details.addElement("locationId").addText(String.valueOf(newss.getLocation()));
                    details.addElement("locationName").addText(HandlerUtils.nullToEmpty(locationMap.getLocation(newss.getLocation())));
                    details.addElement("barId").addText(String.valueOf(newss.getBar()));
                    details.addElement("barName").addText(HandlerUtils.nullToEmpty(barMap.getBar(newss.getBar())));
                    
                    if (reportGroupBy == 2) {
                        String childItems = (childMap.containsKey(locationId) ? childMap.get(locationId) : "0:Other State");
                        details.addElement("childLevelId").addText(HandlerUtils.nullToEmpty(childItems.split(":")[0]));
                        details.addElement("childLevelName").addText(HandlerUtils.nullToEmpty(childItems.split(":")[1]));

                    } else if (reportGroupBy == 1 || reportGroupBy == 3) {
                        String childItems = (childMap.containsKey(productId) ? childMap.get(productId) : "0:Other");
                        details.addElement("childLevelId").addText(HandlerUtils.nullToEmpty(childItems.split(":")[0]));
                        details.addElement("childLevelName").addText(HandlerUtils.nullToEmpty(childItems.split(":")[1]));
                    }
                } catch(Exception e){}
            }
        }

        productMap = null;
        barMap = null;
        locationMap = null;
    }

    private void appendSummaryReportByLevelsXML(Element toAppend, ResultSet rs, int userId, int parentLevel) throws SQLException {

        Map<Date, ArrayList> summarySet = new HashMap<Date, ArrayList>();
        Map<Integer, Date> dateArray = new HashMap<Integer, Date>();
        // A cache of Region Exclusion sets (maps Region -> Exclusion)
        Map<Integer, RegionExclusionMap> exclusionMapCache = new HashMap<Integer, RegionExclusionMap>();

        Date previous = null;
        ArrayList al = new ArrayList();

        productMap = new ProductMap(conn);
        childLevelMap = new ChildLevelMap(conn, parentLevel);

        int i = 0;
        int j = 0;
        int childValue = 0;
        while (rs.next()) {

            if (childValue != rs.getInt(1)) {
                childValue = rs.getInt(1);
                if (exclusionMapCache.containsKey(childValue)) {
                    regionExclusionMap = exclusionMapCache.get(childValue);
                } else {
                    // we need to do a db lookup and add the ingredients to the cache
                    regionExclusionMap = new RegionExclusionMap(conn, userId, parentLevel, childValue);
                    exclusionMapCache.put(childValue, regionExclusionMap);
                }
            }

            if (!regionExclusionMap.hasValue(rs.getInt(2))) {
                if (previous == null) {
                    previous = new Date(rs.getTimestamp(4).getTime());
                }

                if (previous.compareTo(new Date(rs.getTimestamp(4).getTime())) == 0) {
                    al.add(new SummaryStructureByLevel(rs.getInt(1), rs.getInt(2), rs.getDouble(3)));
                    i++;
                } else {
                    summarySet.put(previous, al);
                    i = 0;
                    j++;
                    dateArray.put(j, previous);
                    al = new ArrayList();
                    al.add(new SummaryStructureByLevel(rs.getInt(1), rs.getInt(2), rs.getDouble(3)));
                    i++;
                    previous = new Date(rs.getTimestamp(4).getTime());
                }
            }
        }
        close(rs);

        if (i > 0) {
            summarySet.put(previous, al);
            j++;
            dateArray.put(j, previous);
            i = 0;
        }

        for (i = 1; i <= j; i++) {
            ArrayList<SummaryStructureByLevel> arrayss = summarySet.get(dateArray.get(i));

            Element period = toAppend.addElement("period");
            period.addElement("periodDate").addText(HandlerUtils.nullToEmpty(String.valueOf(dateFormat.format(dateArray.get(i)))));

            for (SummaryStructureByLevel newss : arrayss) {
                Element details = period.addElement("product");
                details.addElement("product").addText(HandlerUtils.nullToEmpty(String.valueOf(newss.getProduct())));
                details.addElement("name").addText(HandlerUtils.nullToEmpty(productMap.getProduct(newss.getProduct())));
                details.addElement("ounces").addText(HandlerUtils.nullToEmpty(String.valueOf(newss.getValue())));
                details.addElement("childLevelId").addText(HandlerUtils.nullToEmpty(String.valueOf(newss.getChildLevel())));
                details.addElement("childLevelName").addText(HandlerUtils.nullToEmpty(childLevelMap.getChildLevel(newss.getChildLevel())));

            }

        }

        productMap = null;
        childLevelMap = null;
        regionExclusionMap = null;
    }

    private void appendSummaryReportByCategoryXML(Element toAppend, ResultSet rs, int userId, int parentLevel, String startDate) throws SQLException {

        Map<Integer, ArrayList> summarySet = new HashMap<Integer, ArrayList>();
        Map<Integer, Integer> categoryArray = new HashMap<Integer, Integer>();

        Element period = toAppend.addElement("period");
        period.addElement("periodDate").addText(startDate);

        Date previous = null;
        ArrayList al = new ArrayList();

        productMap = new ProductMap(conn);
        childLevelMap = new ChildLevelMap(conn, parentLevel);

        int i = 0;
        int j = 0;
        int childValue = 0;
        while (rs.next()) {

            if (childValue != rs.getInt(1)) {
                childValue = rs.getInt(1);
                regionExclusionMap = new RegionExclusionMap(conn, userId, parentLevel, rs.getInt(1));
            }

            if (!regionExclusionMap.hasValue(rs.getInt(2))) {
                if (childValue == rs.getInt(1)) {
                    al.add(new SummaryStructureByLevel(rs.getInt(1), rs.getInt(2), rs.getDouble(3)));
                    i++;
                } else {
                    summarySet.put(childValue, al);
                    i = 0;
                    j++;
                    categoryArray.put(j, childValue);
                    al = new ArrayList();
                    al.add(new SummaryStructureByLevel(rs.getInt(1), rs.getInt(2), rs.getDouble(3)));
                    i++;
                    previous = new Date(rs.getTimestamp(4).getTime());
                }
            }
        }
        close(rs);

        if (i > 0) {
            summarySet.put(childValue, al);
            j++;
            categoryArray.put(j, childValue);
            i = 0;
        }

        for (i = 1; i <= j; i++) {
            ArrayList<SummaryStructureByLevel> arrayss = summarySet.get(categoryArray.get(i));

            for (SummaryStructureByLevel newss : arrayss) {
                Element details = period.addElement("product");
                details.addElement("product").addText(String.valueOf(newss.getProduct()));
                details.addElement("name").addText(HandlerUtils.nullToEmpty(productMap.getProduct(newss.getProduct())));
                details.addElement("ounces").addText(String.valueOf(newss.getValue()));
                details.addElement("childLevelId").addText(String.valueOf(newss.getChildLevel()));
                details.addElement("childLevelName").addText(HandlerUtils.nullToEmpty(childLevelMap.getChildLevel(newss.getChildLevel())));

            }

        }

        productMap = null;
        childLevelMap = null;
        regionExclusionMap = null;
    }

    private void appendSummaryReportSimpleDataXML(Element toAppend, ResultSet rs, int userId, int parentLevel, String startDate) throws SQLException {

        Map<Date, ArrayList> summarySet = new HashMap<Date, ArrayList>();
        Map<Integer, Date> dateArray = new HashMap<Integer, Date>();

        // A cache of Region Exclusion sets (maps Region -> Exclusion)
        Map<Integer, RegionExclusionMap> exclusionMapCache = new HashMap<Integer, RegionExclusionMap>();
        // A cache of Region Product sets (maps Region -> Product)
        Map<Integer, RegionProductMap> productMapCache = new HashMap<Integer, RegionProductMap>();

        Date previous = null;
        ArrayList al = new ArrayList();
        int parent = 0, child = 0;

        Element parentTag = null, childTag = null;

        ArrayList<SummaryStructureSimpleData> arrayss = al;
        //NischaySharma_24-Aug-2009_Start
        Element period = toAppend.addElement("period");
        period.addElement("periodDate").addText(startDate);
        Element poured = period.addElement("poured");
        //NischaySharma_24-Aug-2009_End

        parentLevelMap = new ParentLevelMap(conn, parentLevel);
        childLevelMap = new ChildLevelMap(conn, parentLevel);
        productMap = new ProductMap(conn);

        double ounces = 0.0, userOunces = 0.0, totalOunces = 0.0, totalUserOunces = 0.0;
        while (rs.next()) {

            if (parent != rs.getInt(1)) {
                parent = rs.getInt(1);

                //regionExclusionMap = new RegionExclusionMap(conn, userId, parentLevel - 1, rs.getInt(1));
                //regionProductMap = new RegionProductMap(conn, userId, parentLevel, rs.getInt(1),"");
                if (exclusionMapCache.containsKey(parent)) {
                    regionExclusionMap = exclusionMapCache.get(parent);
                } else {
                    // we need to do a db lookup and add the ingredients to the cache
                    regionExclusionMap = new RegionExclusionMap(conn, userId, parentLevel - 1, parent);
                    exclusionMapCache.put(parent, regionExclusionMap);
                }

                if (productMapCache.containsKey(parent)) {
                    regionProductMap = productMapCache.get(parent);
                } else {
                    // we need to do a db lookup and add the ingredients to the cache
                    regionProductMap = new RegionProductMap(conn, userId, parentLevel, parent,"");
                    productMapCache.put(parent, regionProductMap);
                }

                if (ounces > 0.0) {
                    childTag.addElement("ounces").addText(String.valueOf(ounces));
                    ounces = 0.0;
                }
                if (userOunces > 0.0) {
                    childTag.addElement("userOunces").addText(String.valueOf(userOunces));
                    userOunces = 0.0;
                }
                //NischaySharma_24-Aug-2009_Start
                parentTag = poured.addElement("parent");
                //NischaySharma_24-Aug-2009_End
                parentTag.addElement("parentLevelId").addText(String.valueOf(parent));
                parentTag.addElement("parentLevelName").addText(HandlerUtils.nullToEmpty(parentLevelMap.getParentLevel(parent)));
            }

            if (child != rs.getInt(2)) {
                if (ounces > 0.0) {
                    childTag.addElement("ounces").addText(String.valueOf(ounces));
                    ounces = 0.0;
                }
                if (userOunces > 0.0) {
                    childTag.addElement("userOunces").addText(String.valueOf(userOunces));
                    userOunces = 0.0;
                }
                child = rs.getInt(2);
                childTag = parentTag.addElement("child");
                childTag.addElement("childLevelId").addText(String.valueOf(rs.getInt(2)));
                childTag.addElement("childLevelName").addText(HandlerUtils.nullToEmpty(childLevelMap.getChildLevel(rs.getInt(2))));
            }

            if (!regionExclusionMap.hasValue(rs.getInt(3))) {

                ounces += rs.getDouble(4);
                totalOunces += rs.getDouble(4);

                if (regionProductMap.hasProduct(rs.getInt(3))) {
                    totalUserOunces += rs.getDouble(4);
                    userOunces += rs.getDouble(4);
                }

            }

        }
        close(rs);
        if (ounces > 0.0) {
            childTag.addElement("ounces").addText(String.valueOf(ounces));
            ounces = 0.0;
        }
        if (userOunces > 0.0) {
            childTag.addElement("userOunces").addText(String.valueOf(userOunces));
            userOunces = 0.0;
        }

        //NischaySharma_24-Aug-2009_Start
        poured.addElement("totalOunces").addText(String.valueOf(totalOunces));
        poured.addElement("totalUserOunces").addText(String.valueOf(totalUserOunces));
        //NischaySharma_24-Aug-2009_End

        productMap = null;
        parentLevelMap = null;
        childLevelMap = null;
        regionProductMap = null;
        regionExclusionMap = null;
    }

    private void getSummaryTotalReport(Element toHandle, Element toAppend) throws HandlerException {
        String startDate                    = HandlerUtils.getRequiredString(toHandle, "startDate");
        String endDate                      = HandlerUtils.getRequiredString(toHandle, "endDate");

        int paramType                       = HandlerUtils.getOptionalInteger(toHandle, "paramType");
        int paramValue                      = HandlerUtils.getOptionalInteger(toHandle, "paramValue");
        int userId                          = HandlerUtils.getOptionalInteger(toHandle, "userId");

        boolean includePoured               = HandlerUtils.getOptionalBoolean(toHandle, "includePoured");
        boolean includeSold                 = HandlerUtils.getOptionalBoolean(toHandle, "includeSold");

        String userLocationExclusions       = "0";

        PreparedStatement stmt              = null;
        ResultSet rs                        = null, locationRS = null;

        DateParameter validatedStartDate    = new DateParameter(startDate);
        DateParameter validatedEndDate      = new DateParameter(endDate);

        if (!validatedStartDate.isValid()) {
            logger.debug("Aborted report, invalid start date '" + startDate + "'");
            addErrorDetail(toAppend, "Invalid Start Date");
        } else if (!validatedEndDate.isValid()) {
            logger.debug("Aborted report, invalid end date '" + endDate + "'");
            addErrorDetail(toAppend, "Invalid End Date");
        }

        toAppend.addElement("startDate").addText(startDate);
        toAppend.addElement("endDate").addText(endDate);

        if (userId >= 0) {
            try {
                String selectUserExclusions = " SELECT e.tables, e.value FROM exclusion e LEFT JOIN userExclusionMap uEM ON uEM.exclusion = e.id WHERE e.type = 2 AND uEM.user = ? ";
                stmt                        = conn.prepareStatement(selectUserExclusions);
                stmt.setInt(1, userId);
                rs                          = stmt.executeQuery();
                while (rs.next()) {
                    String selectLocations  = " SELECT l.id FROM location l ";
                    if (rs.getString(1).equals("county")) {
                        selectLocations     += " LEFT JOIN county c ON c.id = l.countyIndex WHERE c.id = ?";
                    } else if (rs.getString(1).equals("location")) {
                        selectLocations     += " WHERE l.id = ? ";
                    }
                    stmt                    = conn.prepareStatement(selectLocations);
                    stmt.setInt(1, Integer.valueOf(rs.getString(2)));
                    locationRS              = stmt.executeQuery();
                    while (locationRS.next()) {
                        userLocationExclusions
                                            += ", " + locationRS.getString(1);
                    }
                    locationRS.close();
                }
            } catch (SQLException sqle) {
                throw new HandlerException(sqle);
            } finally {
            }
        }

        if (includeSold) {
            String soldTableType            = HandlerUtils.getOptionalString(toHandle, "soldSummaryType");
            if (soldTableType == null) {
                soldTableType               = "sold";
            }
            if (!("sold".equals(soldTableType) || "openHoursSold".equals(soldTableType) || "preOpenHoursSold".equals(soldTableType) || "afterHoursSold".equals(soldTableType))) {
                throw new HandlerException("Invalid Summary Type: " + soldTableType);
            }
            
            String selectSold               = "SELECT SUM(s.value) FROM " + soldTableType + "Summary s ";

            switch(paramType) {
                case 1:
                    selectSold              += " LEFT JOIN location l ON l.id = s.location LEFT JOIN locationDetails lD ON lD.location = l.id WHERE lD.active = 1 AND l.customer = ? " +
                                            " AND s.date BETWEEN ? AND ? ";
                    break;
                case 2:
                    selectSold              += " LEFT JOIN location l on l.id = s.location LEFT JOIN locationDetails lD ON lD.location = l.id WHERE lD.active = 1 AND l.id = ? " +
                                            " AND s.date BETWEEN ? AND ? ";
                    break;
                case 3:
                    selectSold              += " LEFT JOIN location l on l.id = s.location LEFT JOIN locationDetails lD ON lD.location = l.id WHERE lD.active = 1 AND s.bar = ? " +
                                            " AND s.date BETWEEN ? AND ? ";
                    break;
                case 4:
                    selectSold              += " LEFT JOIN location l on l.id = s.location LEFT JOIN locationDetails lD ON lD.location = l.id " +
                                            " LEFT JOIN bar b ON b.id = s.bar WHERE lD.active = 1 AND b.zone = ? AND s.date BETWEEN ? AND ? ";
                    break;
                case 5:
                    selectSold              += " LEFT JOIN location l on l.id = s.location LEFT JOIN locationDetails lD ON lD.location = l.id " +
                                            " LEFT JOIN station st ON st.bar = s.bar WHERE lD.active = 1 AND st.id = ? AND s.date BETWEEN ? AND ? ";
                    break;
                default:
                    selectSold              += " WHERE s.date BETWEEN ? AND ? ";
                    break;
            }

            try {
                int colCount                = 1;
                Element soldData            = toAppend.addElement("soldData");
                stmt                        = conn.prepareStatement(selectSold);
                if (userId > 0) {
                   stmt.setInt(colCount++, userId);
                }
                stmt.setInt(colCount++, paramValue);
                stmt.setString(colCount++, validatedStartDate.toString());
                stmt.setString(colCount++, validatedEndDate.toString());
                rs                          = stmt.executeQuery();
                if (rs.next()) {
                    soldData.addElement("ounces").addText(String.valueOf(rs.getDouble(1)));
                }
            } catch (SQLException sqle) {
                throw new HandlerException(sqle);
            } finally {
                close(stmt);
                close(stmt);
            }
        }


        if (includePoured) {
            String pouredTableType          = HandlerUtils.getOptionalString(toHandle, "pouredSummaryType");
            if (pouredTableType == null) {
                pouredTableType             = "poured";
            }
            if (!("poured".equals(pouredTableType) || "preOpenHours".equals(pouredTableType) || "openHours".equals(pouredTableType) || "afterHours".equals(pouredTableType) || "lineCleaning".equals(pouredTableType) || "bevSync".equals(pouredTableType))) {
                throw new HandlerException("Invalid Summary Type: " + pouredTableType);
            }

            String selectPoured             = "SELECT SUM(p.value) FROM " + pouredTableType + "Summary p ";

            switch(paramType) {
                case 1:
                    selectPoured            += " LEFT JOIN location l ON l.id = p.location LEFT JOIN locationDetails lD ON lD.location = l.id WHERE lD.active = 1 AND l.customer = ? " +
                                            " AND p.date BETWEEN ? AND ? ";
                    break;
                case 2:
                    selectPoured            += " LEFT JOIN location l on l.id = p.location LEFT JOIN locationDetails lD ON lD.location = l.id WHERE lD.active = 1 AND l.id = ? " +
                                            " AND p.date BETWEEN ? AND ? ";
                    break;
                case 3:
                    selectPoured            += " LEFT JOIN location l on l.id = p.location LEFT JOIN locationDetails lD ON lD.location = l.id WHERE lD.active = 1 AND p.bar = ? " +
                                            " AND p.date BETWEEN ? AND ? ";
                    break;
                case 4:
                    selectPoured            += " LEFT JOIN location l on l.id = p.location LEFT JOIN locationDetails lD ON lD.location = l.id " +
                                            " LEFT JOIN station st ON st.bar = p.bar WHERE lD.active = 1 AND st.id = ? AND p.date BETWEEN ? AND ? ";
                    break;
                case 5:
                    selectPoured            += " LEFT JOIN location l on l.id = p.location LEFT JOIN locationDetails lD ON lD.location = l.id " +
                                            " LEFT JOIN bar b ON b.id = p.bar WHERE lD.active = 1 AND b.zone = ? AND p.date BETWEEN ? AND ? ";
                    break;
                case 6:
                    selectPoured            += " LEFT JOIN location l on l.id = p.location LEFT JOIN locationDetails lD ON lD.location = l.id " +
                                            " LEFT JOIN locationSupplier map ON map.location = l.id " +
                                            " LEFT JOIN supplierAddress a ON map.address = a.id LEFT JOIN supplier s ON a.supplier=s.id " +
                                            " WHERE lD.active = 1 AND s.id = ? AND p.date BETWEEN ? AND ? ";
                    break;
                case 7:
                    selectPoured            += " LEFT JOIN location l ON l.id = p.location LEFT JOIN locationDetails lD ON lD.location = l.id " +
                                            " LEFT JOIN regionCountyMap rCM ON rCM.county = l.countyIndex LEFT JOIN groupRegionMap gRM ON gRM.regionMaster = rCM.region " +
                                            " LEFT JOIN region r ON r.regionGroup = gRM.id LEFT JOIN userRegionMap uRM ON uRM.region = r.id " +
                                            " WHERE lD.active = 1 AND lD.data = 1 AND l.id NOT IN (" + userLocationExclusions + ") AND uRM.user = ? AND l.countyIndex = ? " +
                                            " AND p.date BETWEEN ? AND ? ";
                    break;
                case 8:
                    selectPoured            += " LEFT JOIN location l ON l.id = p.location LEFT JOIN locationDetails lD ON lD.location = l.id " +
                                            " LEFT JOIN regionCountyMap rCM ON rCM.county = l.countyIndex  LEFT JOIN groupRegionMap gRM ON gRM.regionMaster = rCM.region" +
                                            " LEFT JOIN region r ON r.regionGroup = gRM.id LEFT JOIN userRegionMap uRM ON uRM.region = r.id " +
                                            " WHERE lD.active = 1 AND lD.data = 1 AND l.id NOT IN (" + userLocationExclusions + ") AND uRM.user = ? AND uRM.region = ? " +
                                            " AND p.date BETWEEN ? AND ? ";
                    break;
                case 9:
                    selectPoured            += " LEFT JOIN location l ON l.id = p.location LEFT JOIN locationDetails lD ON lD.location = l.id " +
                                            " LEFT JOIN regionCountyMap rCM ON rCM.county = l.countyIndex  LEFT JOIN groupRegionMap gRM ON gRM.regionMaster = rCM.region" +
                                            " LEFT JOIN region r ON r.regionGroup = gRM.id LEFT JOIN userRegionMap uRM ON uRM.region = r.id " +
                                            " WHERE lD.active = 1 AND lD.data = 1 AND l.id NOT IN (" + userLocationExclusions + ") AND uRM.user = ? AND uRM.user = ? " +
                                            " AND p.date BETWEEN ? AND ? ";
                    break;
                default:
                    selectPoured            += " WHERE p.date BETWEEN ? AND ? ";
                    break;
            }

            try {
                int colCount                = 1;
                Element pouredData          = toAppend.addElement("pouredData");
                stmt                        = conn.prepareStatement(selectPoured);
                if (userId > 0) {
                   stmt.setInt(colCount++, userId);
                }
                stmt.setInt(colCount++, paramValue);
                stmt.setString(colCount++, validatedStartDate.toString());
                stmt.setString(colCount++, validatedEndDate.toString());
                rs                          = stmt.executeQuery();
                if (rs.next()) {
                    pouredData.addElement("ounces").addText(String.valueOf(rs.getDouble(1)));
                }
            } catch (SQLException sqle) {
                throw new HandlerException(sqle);
            } finally {
                close(stmt);
                close(stmt);
            }
        }
    }

    private void getSummaryReport(Element toHandle, Element toAppend) throws HandlerException {
         int callerId                       = getCallerId(toHandle);         
        String startDate                    = HandlerUtils.getRequiredString(toHandle, "startDate");
        String endDate                      = HandlerUtils.getRequiredString(toHandle, "endDate");
        String periodStr                    = HandlerUtils.getRequiredString(toHandle, "periodType");
        String periodDetail                 = HandlerUtils.getRequiredString(toHandle, "periodDetail");
        String specificLocationsString      = HandlerUtils.getOptionalString(toHandle, "specificLocations");

        int station                         = HandlerUtils.getOptionalInteger(toHandle, "stationId");
        int bar                             = HandlerUtils.getOptionalInteger(toHandle, "barId");
        int zone                            = HandlerUtils.getOptionalInteger(toHandle, "zoneId");
        int location                        = HandlerUtils.getOptionalInteger(toHandle, "locationId");
        int customer                        = HandlerUtils.getOptionalInteger(toHandle, "customerId");
        int group                           = HandlerUtils.getOptionalInteger(toHandle, "groupId");
        int supplier                        = HandlerUtils.getOptionalInteger(toHandle, "supplierId");
        int county                          = HandlerUtils.getOptionalInteger(toHandle, "countyId");
        int region                          = HandlerUtils.getOptionalInteger(toHandle, "regionId");
        //int locationRegion                  = HandlerUtils.getOptionalInteger(toHandle, "locationRegion");
        int user                            = HandlerUtils.getOptionalInteger(toHandle, "userId");
        int product                         = HandlerUtils.getOptionalInteger(toHandle, "specificProduct");
        int bevSync                         = HandlerUtils.getOptionalInteger(toHandle, "bevSync");
        int reportGroupBy                   = HandlerUtils.getOptionalInteger(toHandle, "reportGroupBy");
        String locations                    = HandlerUtils.getOptionalString(toHandle, "locations");

        boolean includeExclusion            = HandlerUtils.getOptionalBoolean(toHandle, "includeExclusion");
        boolean includePoured               = HandlerUtils.getOptionalBoolean(toHandle, "includePoured");
        boolean includeSold                 = HandlerUtils.getOptionalBoolean(toHandle, "includeSold");
        boolean byProductCategory           = HandlerUtils.getOptionalBoolean(toHandle, "byProductCategory");
        boolean simpleData                  = HandlerUtils.getOptionalBoolean(toHandle, "simpleData");
        boolean forChart                    = HandlerUtils.getOptionalBoolean(toHandle, "forChart");
        boolean showProjectionDetails       = HandlerUtils.getOptionalBoolean(toHandle, "showProjectionDetails");
        
        boolean byLocation                  = false, groupUser = false;

        String specificLocations            = " ", soldSpecificProduct = " ", pouredSpecificProduct = " ", selectedLevel = " ",
                                            selectedValues = " r.id, l.countyIndex, ", soldExclusion = " ", pouredExclusion = " ", specificStates = " ", bevSyncLocation = " ";
        String groupPouredLevel             = " ", groupSoldLevel = " ", groupSoldValue = " s.value, ", groupPouredValue = " p.value, ";
        String userLocationExclusions       = "0", dataLocationExclusions = "3, 4, 5, 6", userLocationRequired = " ";

        String exclusionQuery               = "SELECT eS.location, eS.bar, eS.exclusion, eS.date FROM exclusionSummary eS ";
        int exclusionParameter              = 0;
        PreparedStatement stmt              = null;
        ResultSet rs                        = null, locationRS = null;

        if (!(specificLocationsString == null || specificLocationsString.equals(""))) {
            groupUser                       = true;
        }
        PeriodType periodType               = PeriodType.parseString(periodStr);
        if (null == periodType) {
            throw new HandlerException("Invalid period type: " + periodStr);
        }

        if (periodType == PeriodType.MONTHLY) {
            groupPouredLevel                = " GROUP BY MONTH(p.date), p.location, p.product ";
            groupPouredValue                = " SUM(p.value), ";
            groupSoldLevel                  = " GROUP BY MONTH(s.date), s.location, s.product ";
            groupSoldValue                  = " SUM(s.value), ";
        } else if (forChart) {
            groupPouredLevel                = " GROUP BY p.date ";
            groupPouredValue                = " SUM(p.value), ";
            groupSoldLevel                  = " GROUP BY s.date ";
            groupSoldValue                  = " SUM(s.value), ";
        }

        int paramsSet = 0;
        if (station >= 0) {
            exclusionQuery                  += " LEFT JOIN station st ON st.bar = eS.bar WHERE eS.date BETWEEN ? AND ? AND st.id = ? ";
            exclusionParameter              = station;
            paramsSet++;
        }
        if(bevSync>0) {
            if(location > 0) {
                exclusionQuery              += " LEFT JOIN location l ON l.id = eS.location LEFT JOIN locationDetails lD ON lD.location = eS.location WHERE eS.date BETWEEN ? AND ? AND lD.active = 1 AND lD.beerboard = 1 AND l.id = "+location+" ";
                bevSyncLocation             = " AND l.id = " + location + " ";
            } else {
                try {
                    String selectCreatives  = "SELECT GROUP_CONCAT(b.location ORDER BY b.location SEPARATOR ',') FROM bevSyncCustomerLocationMap b LEFT JOIN locationDetails lD ON lD.location=b.location WHERE b.customer = ? AND lD.active = 1; ";
                    stmt                    = conn.prepareStatement(selectCreatives);
                    stmt.setInt(1,bevSync);
                    rs                      = stmt.executeQuery();
                    if(rs.next()) {
                        if(rs.getString(1)!=null &&!rs.getString(1).equals("")) {
                            locations       = rs.getString(1);
                        } else {
                            locations       = "0";
                        }
                    }
                    exclusionQuery          += " LEFT JOIN location l ON l.id = eS.location LEFT JOIN locationDetails lD ON lD.location = eS.location WHERE eS.date BETWEEN ? AND ? AND lD.active = 1 AND lD.beerboard = 1 AND l.id IN( "+locations+") ";
                    bevSyncLocation         = " AND l.id IN ("+locations+") ";
                 } catch (SQLException sqle) {
                throw new HandlerException(sqle);
            } finally {
            }
                
            }
          
        }
        
        
        if (bar >= 0) {
            exclusionQuery                  += " WHERE eS.date BETWEEN ? AND ? AND eS.bar = ? ";
            exclusionParameter              = bar;
            paramsSet++;
        }
        if (zone >= 0) {
            exclusionQuery                  += " LEFT JOIN bar b ON b.id = eS.bar WHERE eS.date BETWEEN ? AND ? AND b.zone = ? ";
            exclusionParameter              = zone;
            paramsSet++;
        }
        if (location >= 0 && bevSync < 1) {
            exclusionQuery                  += " WHERE eS.date BETWEEN ? AND ? AND eS.location = ? ";
            exclusionParameter              = location;
            paramsSet++;
        }
        if (customer >= 0) {            
            exclusionQuery                  += " LEFT JOIN location l ON l.id = eS.location WHERE eS.date BETWEEN ? AND ? AND l.customer = ? ";
            exclusionParameter              = customer;
            
            if(region > 0){
                specificLocationsString     = getRegionLocations(customer,region);
            }
            if (!(specificLocationsString == null || specificLocationsString.equals(""))) {
                specificLocations           = " AND location IN (" + specificLocationsString + ")";
            } else {
                try {
                    stmt                    = conn.prepareStatement("SELECT GROUP_CONCAT(l.id) FROM location l LEFT JOIN locationDetails lD ON lD.location = l.id " +
                                            " WHERE l.customer = ? AND lD.active = 1");
                    stmt.setInt(1, customer);
                    rs                      = stmt.executeQuery();
                    if (rs.next()) {
                        specificLocations   = " AND location IN (" + rs.getString(1) + ")";
                    }
                } catch (SQLException sqle) {
                    throw new HandlerException(sqle);
                } finally {
                }
            }
            paramsSet++;
        }
        if (group >= 0) {
            exclusionQuery                  += " LEFT JOIN location l ON l.id = eS.location LEFT JOIN customer c ON c.id = l.customer " +
                                            " WHERE eS.date BETWEEN ? AND ? AND c.groupId = ? ";
            exclusionParameter              = group;
            if (!(specificLocationsString == null || specificLocationsString.equals(""))) {
                specificLocations           = " AND location IN ( " + specificLocationsString + " )";
            } else {
                try {
                    stmt                    = conn.prepareStatement("SELECT GROUP_CONCAT(l.id) FROM location l LEFT JOIN locationDetails lD ON lD.location = l.id " +
                                            " LEFT JOIN customer c ON c.id = l.customer WHERE c.groupId = ? AND lD.active = 1");
                    stmt.setInt(1, group);
                    rs                      = stmt.executeQuery();
                    if (rs.next()) {
                        specificLocations   = " AND location IN (" + rs.getString(1) + ")";
                    }
                } catch (SQLException sqle) {
                    throw new HandlerException(sqle);
                } finally {
                }
            }
            paramsSet++;
        }
        if (user > 0 && groupUser) {
            exclusionQuery                  += " LEFT JOIN location l ON l.id = eS.location WHERE eS.date BETWEEN ? AND ? ";
            exclusionParameter              = 0;
            if (!(specificLocationsString == null || specificLocationsString.equals(""))) {
                specificLocations           = " AND location IN ( " + specificLocationsString + " )";
            }
            paramsSet++;
        }
        if (supplier >= 0) {
            exclusionQuery                  += " LEFT JOIN location l on l.id = eS.location LEFT JOIN locationSupplier map ON map.location = l.id " +
                                            " LEFT JOIN supplierAddress a ON map.address = a.id LEFT JOIN supplier s ON a.supplier = s.id " +
                                            " WHERE eS.date BETWEEN ? AND ? AND  s.id = ? ";
            exclusionParameter              = supplier;
            paramsSet++;
        }
        if (county >= 0) {
            exclusionQuery                  += " LEFT JOIN location l ON l.id = eS.location WHERE eS.date BETWEEN ? AND ? AND l.countyIndex = ? ";
            exclusionParameter              = county;
            paramsSet++;
        }
        
        if (user > 0 && !groupUser) {
            exclusionQuery                  += " WHERE eS.date BETWEEN ? AND ? ";
            exclusionParameter              = 0;
            paramsSet                       = 1;
            try {
                String selectUserExclusions = " SELECT e.tables, e.value FROM exclusion e LEFT JOIN userExclusionMap uEM ON uEM.exclusion = e.id WHERE e.type = 2 AND uEM.user = ? ";
                stmt                        = conn.prepareStatement(selectUserExclusions);
                stmt.setInt(1, user);
                rs                          = stmt.executeQuery();
                while (rs.next()) {
                    String selectLocations  = " SELECT l.id FROM location l ";
                    if (rs.getString(1).equals("county")) {
                        selectLocations     += " LEFT JOIN county c ON c.id = l.countyIndex WHERE c.id = ?";
                    } else if (rs.getString(1).equals("location")) {
                        selectLocations     += " WHERE l.id = ? ";
                    }
                    stmt                    = conn.prepareStatement(selectLocations);
                    stmt.setInt(1, Integer.valueOf(rs.getString(2)));
                    locationRS              = stmt.executeQuery();
                    while (locationRS.next()) {
                        userLocationExclusions
                                            += ", " + locationRS.getString(1);
                    }
                    locationRS.close();
                }
            } catch (SQLException sqle) {
                throw new HandlerException(sqle);
            } finally {
            }
        }

        if (paramsSet != 1 && bevSync <1) {
            throw new HandlerException("Exactly one of the following must be set: groupId zoneId barId stationId locationId customerId supplierId countyId regionId userId");
        }

        if (product > 0) {
            soldSpecificProduct             = " AND s.product = ? ";
            pouredSpecificProduct           = " AND p.product = ? ";
        } 

        if (location >= 0 && bevSync < 1) {
            selectedLevel                   = " AND l.id = ? ";
            selectedValues                  = " l.id, p.product, ";
        } else if (county >= 0) {
            selectedLevel                   = " AND rCM.county = ? ";
            selectedValues                  = " l.id, p.product, ";
        } 

        if (!includeExclusion) {
            soldExclusion                   = " AND s.exclusion = 0 ";
            pouredExclusion                 = " AND p.exclusion = 0 ";
        }

        // Maps product ids to RRecs (oz values);
        Map<Date, ArrayList> summarySet     = new HashMap<Date, ArrayList>();
        Map<Integer, Date> dateArray        = new HashMap<Integer, Date>();

        DateParameter validatedStartDate    = new DateParameter(startDate);
        DateParameter validatedEndDate      = new DateParameter(endDate);

        if (!validatedStartDate.isValid()) {
            logger.debug("Aborted report, invalid start date '" + startDate + "'");
            addErrorDetail(toAppend, "Invalid Start Date");
        } else if (!validatedEndDate.isValid()) {
            logger.debug("Aborted report, invalid end date '" + endDate + "'");
            addErrorDetail(toAppend, "Invalid End Date");
        }

        toAppend.addElement("startDate").addText(startDate);
        toAppend.addElement("endDate").addText(endDate);

        if (includeSold) {
            dataLocationExclusions          += ", 2";
            String soldTableType            = HandlerUtils.getOptionalString(toHandle, "soldSummaryType");
            if (soldTableType == null) {
                soldTableType               = "sold";
            }
            if (!("sold".equals(soldTableType) || "openHoursSold".equals(soldTableType) || "preOpenHoursSold".equals(soldTableType) || "afterHoursSold".equals(soldTableType))) {
                throw new HandlerException("Invalid Summary Type: " + soldTableType);
            }
            String soldTable = soldTableType + "Summary";
            logger.debug(soldTable);
            logger.debug(soldExclusion);
            String selectBarSold = "SELECT s.location, s.bar, s.product, " + groupSoldValue + " s.date FROM " + soldTable + " s " +
                    " WHERE s.bar = ? AND s.date BETWEEN ? AND ? " + soldSpecificProduct + soldExclusion + groupSoldLevel +
                    " ORDER BY s.date, s.location, s.product ";

            String selectLocationSold = "SELECT s.location, s.bar, s.product, " + groupSoldValue + " s.date FROM " + soldTable + " s " +
                    " WHERE s.location = ? AND s.date BETWEEN ? AND ? " + soldSpecificProduct + soldExclusion + groupSoldLevel +
                    " ORDER BY s.date, s.location, s.product ";

            String selectCustomerSold = "SELECT s.location, s.bar, s.product, " + groupSoldValue + " s.date FROM " + soldTable + " s " +
                    " WHERE s.date BETWEEN ? AND ? " + specificLocations + soldSpecificProduct + soldExclusion + groupSoldLevel +
                    " ORDER BY s.date, s.location, s.product ";

            String selectGroupSold = "SELECT s.location, s.bar, s.product, " + groupSoldValue + " s.date FROM " + soldTable + " s " +
                    " WHERE s.date BETWEEN ? AND ? " + specificLocations + soldSpecificProduct + soldExclusion + groupSoldLevel +
                    " ORDER BY s.date, s.location, s.product ";

            String selectGroupUserSold = "SELECT s.location, s.bar, s.product, " + groupSoldValue + " s.date FROM " + soldTable + " s " +
                    " WHERE s.date BETWEEN ? AND ? " + specificLocations + soldSpecificProduct + soldExclusion + groupSoldLevel +
                    " ORDER BY s.date, s.location, s.product ";
            
            
            String selectBevSyncSold = "SELECT s.location, s.bar, s.product, " + groupSoldValue + " s.date FROM " + soldTable + " s " +
                    " LEFT JOIN location l ON l.id = s.location LEFT JOIN locationDetails lD ON lD.location = l.id " +
                    " WHERE lD.active = 1 AND lD.beerboard = 1 AND s.date BETWEEN ? AND ? " + specificStates + bevSyncLocation + groupSoldLevel +
                    " ORDER BY s.date, s.location, s.product ";

            try {

                Element soldData = toAppend.addElement("soldData");

                if (bar >= 0) {
                    stmt = conn.prepareStatement(selectBarSold);
                    stmt.setInt(1, bar);
                    stmt.setString(2, validatedStartDate.toString());
                    stmt.setString(3, validatedEndDate.toString());
                    if (product > 0) {
                        stmt.setInt(4, product);
                    }
                    rs = stmt.executeQuery();
                    appendSummaryReportByLocationXML(soldData, rs);
                    logger.debug("Executing getSummaryReport for Bars - Sold query");
                } else if (location >= 0 && bevSync < 1) {
                    stmt = conn.prepareStatement(selectLocationSold);
                    stmt.setInt(1, location);
                    stmt.setString(2, validatedStartDate.toString());
                    stmt.setString(3, validatedEndDate.toString());
                    if (product > 0) {
                        stmt.setInt(4, product);
                    }
                    rs = stmt.executeQuery();
                    appendSummaryReportByLocationXML(soldData, rs);
                    logger.debug("Executing getSummaryReport for Location - Sold query");
                } else if (customer >= 0) {
                    stmt = conn.prepareStatement(selectCustomerSold);
                    stmt.setString(1, validatedStartDate.toString());
                    stmt.setString(2, validatedEndDate.toString());
                    if (product > 0) {
                        stmt.setInt(3, product);
                    }
                    rs = stmt.executeQuery();
                    logger.debug("Executing getSummaryReport for Customer - Sold query");
                    appendSummaryReportByLocationXML(soldData, rs);
                } else if (group >= 0) {
                    stmt = conn.prepareStatement(selectGroupSold);
                    stmt.setString(1, validatedStartDate.toString());
                    stmt.setString(2, validatedEndDate.toString());
                    if (product > 0) {
                        stmt.setInt(3, product);
                    }
                    rs = stmt.executeQuery();
                    logger.debug("Executing getSummaryReport for Group - Sold query");
                    appendSummaryReportByLocationXML(soldData, rs);
                } else if (user > 0 && groupUser) {
                    stmt = conn.prepareStatement(selectGroupUserSold);
                    stmt.setString(1, validatedStartDate.toString());
                    stmt.setString(2, validatedEndDate.toString());
                    if (product > 0) {
                        stmt.setInt(3, product);
                    }
                    rs = stmt.executeQuery();
                    logger.debug("Executing getSummaryReport for User - Sold query");
                    appendSummaryReportByLocationXML(soldData, rs);
                } else if(bevSync > 0) {
                    stmt = conn.prepareStatement(selectBevSyncSold);
                    stmt.setString(1, validatedStartDate.toString());
                    stmt.setString(2, validatedEndDate.toString());
                    rs = stmt.executeQuery();
                    logger.debug("Executing getSummaryReport for bevSync State - Sold query");
                    appendSummaryReportByLocationXML(soldData, rs);
                }
                rs.close();


            } catch (SQLException sqle) {
                throw new HandlerException(sqle);
            } finally {
            }

        }

        if (includePoured) {
            dataLocationExclusions += ", 1";
            String pouredTableType = HandlerUtils.getOptionalString(toHandle, "pouredSummaryType");
            if (pouredTableType == null) {
                pouredTableType = "poured";
            }
            if (!("poured".equals(pouredTableType) || "preOpenHours".equals(pouredTableType) || "openHours".equals(pouredTableType) || "afterHours".equals(pouredTableType) || "lineCleaning".equals(pouredTableType) || "bevSync".equals(pouredTableType))) {
                throw new HandlerException("Invalid Summary Type: " + pouredTableType);
            }

            String pouredTable = pouredTableType + "Summary";
            logger.debug(pouredTable);

            String selectBarPoured = "SELECT p.location, p.bar, p.product, " + groupPouredValue + " p.date FROM " + pouredTable + " p " +
                    " WHERE p.bar = ? AND p.date BETWEEN ? AND ? " + pouredSpecificProduct + pouredExclusion + groupPouredLevel +
                    " ORDER BY p.date, p.location, p.product ";

            String selectLocationPoured = "SELECT p.location, p.bar, p.product," + groupPouredValue + " p.date FROM " + pouredTable + " p " +
                    " WHERE p.location = ? AND p.date BETWEEN ? AND ? " + pouredSpecificProduct + pouredExclusion + groupPouredLevel +
                    " ORDER BY p.date, p.location, p.product ";

            String selectCustomerPoured = "SELECT p.location, p.bar, p.product, " + groupPouredValue + " p.date FROM " + pouredTable + " p " +
                    " WHERE p.date BETWEEN ? AND ? " + specificLocations + pouredSpecificProduct + pouredExclusion + groupPouredLevel +
                    " ORDER BY p.date, p.location, p.product ";

            String selectGroupPoured = "SELECT p.location, p.bar, p.product, " + groupPouredValue + " p.date FROM " + pouredTable + " p " +
                    " WHERE p.date BETWEEN ? AND ? " + specificLocations + pouredSpecificProduct + pouredExclusion + groupPouredLevel +
                    " ORDER BY p.date, p.location, p.product ";

            String selectGroupUserPoured = "SELECT p.location, p.bar, p.product, " + groupPouredValue + " p.date FROM " + pouredTable + " p " +
                    " WHERE p.date BETWEEN ? AND ? " + specificLocations + pouredSpecificProduct + pouredExclusion + groupPouredLevel +
                    " ORDER BY p.date, p.location, p.product ";

            String selectSupplierPoured = "SELECT p.location, p.bar, p.product, p.value, p.date FROM " + pouredTable + " p " +
                    " LEFT JOIN location l on l.id = p.location LEFT JOIN locationDetails lD ON lD.location = l.id " +
                    " LEFT JOIN locationSupplier map ON map.location = l.id " +
                    " LEFT JOIN supplierAddress a ON map.address = a.id LEFT JOIN supplier s ON a.supplier=s.id " +
                    " WHERE lD.active = 1 AND s.id = ? AND p.date BETWEEN ? AND ? " + pouredSpecificProduct + pouredExclusion +
                    " ORDER BY p.date, p.location, p.product";

            String selectCountyPoured = "SELECT p.location, p.product, SUM(p.value), " + validatedStartDate.toString() + " FROM " + pouredTable + " p " +
                    " LEFT JOIN location l ON l.id = p.location LEFT JOIN locationDetails lD ON lD.location = l.id " +
                    " LEFT JOIN regionCountyMap rCM ON rCM.county = l.countyIndex " +
                    " LEFT JOIN groupRegionMap gRM ON gRM.regionMaster = rCM.region" +
                    " LEFT JOIN region r ON r.regionGroup = gRM.id " +
                    " LEFT JOIN userRegionMap uRM ON uRM.region = r.id " +
                    " WHERE lD.active = 1 AND lD.data = 1 AND l.id NOT IN (" + userLocationExclusions + ") AND uRM.user = ? AND l.countyIndex = ? AND p.date BETWEEN ? AND ? " + pouredSpecificProduct + pouredExclusion +
                    " GROUP BY p.location, p.product " +
                    " ORDER BY p.location, p.product";

            String selectRegionPoured = "SELECT l.countyIndex, p.product, SUM(p.value), " + validatedStartDate.toString() + " FROM " + pouredTable + " p " +
                    " LEFT JOIN location l ON l.id = p.location LEFT JOIN locationDetails lD ON lD.location = l.id " +
                    " LEFT JOIN regionCountyMap rCM ON rCM.county = l.countyIndex " +
                    " LEFT JOIN groupRegionMap gRM ON gRM.regionMaster = rCM.region" +
                    " LEFT JOIN region r ON r.regionGroup = gRM.id " +
                    " LEFT JOIN userRegionMap uRM ON uRM.region = r.id " +
                    " WHERE lD.active = 1 AND lD.data = 1 AND l.id NOT IN (" + userLocationExclusions + ") AND uRM.user = ? AND uRM.region = ? AND p.date BETWEEN ? AND ? " + pouredSpecificProduct + pouredExclusion +
                    " GROUP BY l.countyIndex, p.product " +
                    " ORDER BY l.countyIndex, p.product ";

            String selectRegionByLocationPoured = "SELECT l.id, p.product, SUM(p.value), " + validatedStartDate.toString() + " FROM " + pouredTable + " p " +
                    " LEFT JOIN location l ON l.id = p.location LEFT JOIN locationDetails lD ON lD.location = l.id " +
                    " LEFT JOIN regionCountyMap rCM ON rCM.county = l.countyIndex " +
                    " LEFT JOIN groupRegionMap gRM ON gRM.regionMaster = rCM.region" +
                    " LEFT JOIN region r ON r.regionGroup = gRM.id " +
                    " LEFT JOIN userRegionMap uRM ON uRM.region = r.id " +
                    " WHERE lD.active = 1 AND lD.data = 1 AND l.id NOT IN (" + userLocationExclusions + ") AND uRM.user = ? " + selectedLevel +
                    " AND p.date BETWEEN ? AND ? " + pouredSpecificProduct + pouredExclusion +
                    " GROUP BY l.id, p.product " +
                    " ORDER BY l.id, p.product ";

            String selectUserRegionPoured = "SELECT r.id, p.product, SUM(p.value), " + validatedStartDate.toString() + " FROM " + pouredTable + " p " +
                    " LEFT JOIN location l ON l.id = p.location LEFT JOIN locationDetails lD ON lD.location = l.id " +
                    " LEFT JOIN regionCountyMap rCM ON rCM.county = l.countyIndex " +
                    " LEFT JOIN groupRegionMap gRM ON gRM.regionMaster = rCM.region" +
                    " LEFT JOIN region r ON r.regionGroup = gRM.id " +
                    " LEFT JOIN userRegionMap uRM ON uRM.region = r.id " +
                    " WHERE lD.active = 1 AND lD.data = 1 AND l.id NOT IN (" + userLocationExclusions + ") AND uRM.user = ? AND p.date BETWEEN ? AND ? " + pouredSpecificProduct + pouredExclusion +
                    " GROUP BY rCM.region, p.product " +
                    " ORDER BY rCM.region, p.product ";

            String selectByLocationProductCategoryPoured = "SELECT IF(pD.category = 0, 3, pD.category), p.product, SUM(p.value), p.date FROM " + pouredTable + " p " +
                    " LEFT JOIN product pr ON pr.id = p.product LEFT JOIN productDescription pD ON pD.product = pr.id " +
                    " WHERE p.location = ? AND p.date BETWEEN ? AND ? " + pouredSpecificProduct + pouredExclusion +
                    " GROUP BY pD.category, p.product " +
                    " ORDER BY p.date, pD.category, p.product ";

            String selectByUserProductCategoryPoured = "SELECT IF(pD.category = 0, 3, pD.category), p.product, SUM(p.value), p.date FROM " + pouredTable + " p " +
                    " LEFT JOIN product pr ON pr.id = p.product LEFT JOIN productDescription pD ON pD.product = pr.id " +
                    " LEFT JOIN location l ON l.id = p.location LEFT JOIN locationDetails lD ON lD.location = l.id " +
                    " LEFT JOIN regionCountyMap rCM ON rCM.county = l.countyIndex " +
                    " LEFT JOIN groupRegionMap gRM ON gRM.regionMaster = rCM.region" +
                    " LEFT JOIN region r ON r.regionGroup = gRM.id " +
                    " LEFT JOIN userRegionMap uRM ON uRM.region = r.id " +
                    " WHERE lD.active = 1 AND lD.data = 1 AND l.id NOT IN (" + userLocationExclusions + ") AND uRM.user = ? " + selectedLevel +
                    " AND p.date BETWEEN ? AND ? " + pouredSpecificProduct + pouredExclusion +
                    " GROUP BY pD.category, p.product " +
                    " ORDER BY p.date, pD.category, p.product";

            String selectByUserSimpleDataPoured = "SELECT " + selectedValues + " p.product, SUM(p.value) FROM " + pouredTable + " p " +
                    " LEFT JOIN location l ON l.id = p.location LEFT JOIN locationDetails lD ON lD.location = l.id " +
                    " LEFT JOIN regionCountyMap rCM ON rCM.county = l.countyIndex " +
                    " LEFT JOIN groupRegionMap gRM ON gRM.regionMaster = rCM.region" +
                    " LEFT JOIN region r ON r.regionGroup = gRM.id " +
                    " LEFT JOIN userRegionMap uRM ON uRM.region = r.id " +
                    " WHERE lD.active = 1 AND lD.data = 1 AND  lD.beerboard =1 AND l.id NOT IN (" + userLocationExclusions + ") AND uRM.user = ? " + selectedLevel +
                    " AND p.date BETWEEN ? AND ? " + pouredSpecificProduct + pouredExclusion +
                    " GROUP BY " + selectedValues + " p.product " +
                    " ORDER BY " + selectedValues + " p.product ";
            
            String selectBevSyncPoured = "SELECT p.location, p.bar, p.product, " + groupPouredValue + " p.date FROM " + pouredTable + " p " +
                    " LEFT JOIN location l ON l.id = p.location LEFT JOIN locationDetails lD ON lD.location = l.id " +
                    " WHERE lD.active = 1 AND lD.beerboard =1 AND p.date BETWEEN ? AND ? " + specificStates + bevSyncLocation + pouredSpecificProduct + pouredExclusion + groupPouredLevel+
                    "  ORDER BY p.date, p.location, p.product ";
            
            String selectBevSyncProductCategoryPoured = "SELECT IF(pD.category = 0, 3, pD.category), p.product, SUM(p.value), p.date FROM " + pouredTable + " p LEFT JOIN product pr ON pr.id = p.product " +
                    " LEFT JOIN productDescription pD ON pD.product = pr.id LEFT JOIN location l ON l.id = p.location LEFT JOIN locationDetails lD ON lD.location = p.location " +
                    " WHERE lD.beerboard=1 AND p.date BETWEEN ? AND ? " + specificStates + bevSyncLocation + pouredSpecificProduct + pouredExclusion +
                    " GROUP BY pD.category, p.product " +
                    " ORDER BY p.date, pD.category, p.product ";



            try {

                int parentLevel = 0, colCount = 1, param1 = -1, param2 = -1;

                Element pouredData = toAppend.addElement("pouredData");


                if (simpleData) {
                    if (location >= 0 && bevSync < 1) {
                        parentLevel = 4;
                        param1 = location;
                    } else if (county >= 0) {
                        parentLevel = 4;
                        param1 = county;
                    }  else if (user >= 0) {
                        parentLevel = 2;
                    }
                    stmt = conn.prepareStatement(selectByUserSimpleDataPoured);
                    stmt.setInt(colCount++, user);
                    if (param1 > 0) {
                        stmt.setInt(colCount++, param1);
                    }
                    stmt.setString(colCount++, validatedStartDate.toString());
                    stmt.setString(colCount++, validatedEndDate.toString());
                    if (product > 0) {
                        stmt.setInt(colCount++, product);
                    }
                    rs = stmt.executeQuery();
                    logger.debug("Executing getSummaryReport for Simple Data - Poured query");
                    appendSummaryReportSimpleDataXML(pouredData, rs, user, parentLevel, startDate);
                } else if (byProductCategory) {
                    parentLevel = 4;
                    stmt = conn.prepareStatement(selectByUserProductCategoryPoured);
                    if(bevSync > 0) {
                        stmt = conn.prepareStatement(selectBevSyncProductCategoryPoured);
                        stmt.setString(colCount++, validatedStartDate.toString());
                        stmt.setString(colCount++, validatedEndDate.toString());
                        rs = stmt.executeQuery();
                    logger.debug("Executing getSummaryReport by Product Category - Poured query");
                    appendSummaryReportByLevelsXML(pouredData, rs, user, parentLevel);
                    }else{
                        if (location >= 0 && bevSync < 1) {
                        stmt = conn.prepareStatement(selectByLocationProductCategoryPoured);
                        param2 = location;
                    } else if (county >= 0) {
                        param1 = user;
                        param2 = county;
                    } else if (user >= 0) {
                        param2 = user;
                    }
                    if (param1 > 0) {
                        stmt.setInt(colCount++, param1);
                    }
                    stmt.setInt(colCount++, param2);
                    stmt.setString(colCount++, validatedStartDate.toString());
                    stmt.setString(colCount++, validatedEndDate.toString());
                    if (product > 0) {
                        stmt.setInt(colCount++, product);
                    }
                    rs = stmt.executeQuery();
                    logger.debug("Executing getSummaryReport by Product Category - Poured query");
                    appendSummaryReportByLevelsXML(pouredData, rs, user, parentLevel);
                    }
                } else if (bar >= 0) {
                    stmt = conn.prepareStatement(selectBarPoured);
                    stmt.setInt(1, bar);
                    stmt.setString(2, validatedStartDate.toString());
                    stmt.setString(3, validatedEndDate.toString());
                    if (product > 0) {
                        stmt.setInt(4, product);
                    }
                    rs = stmt.executeQuery();
                    logger.debug("Executing getSummaryReport for Bars - Poured query");
                    appendSummaryReportByLocationXML(pouredData, rs);
                } else if (location >= 0 && bevSync < 1) {
                    stmt = conn.prepareStatement(selectLocationPoured);
                    stmt.setInt(1, location);
                    stmt.setString(2, validatedStartDate.toString());
                    stmt.setString(3, validatedEndDate.toString());
                    if (product > 0) {
                        stmt.setInt(4, product);
                    }
                    rs = stmt.executeQuery();
                    logger.debug("Executing getSummaryReport for Location - Poured query");
                    appendSummaryReportByLocationXML(pouredData, rs);
                } else if (customer >= 0) {
                    stmt = conn.prepareStatement(selectCustomerPoured);
                    stmt.setString(1, validatedStartDate.toString());
                    stmt.setString(2, validatedEndDate.toString());
                    if (product > 0) {
                        stmt.setInt(3, product);
                    }
                    rs = stmt.executeQuery();
                    logger.debug("Executing getSummaryReport for Customer - Poured query");
                    appendSummaryReportByLocationXML(pouredData, rs);
                } else if (group >= 0) {
                    stmt = conn.prepareStatement(selectGroupPoured);
                    stmt.setString(1, validatedStartDate.toString());
                    stmt.setString(2, validatedEndDate.toString());
                    if (product > 0) {
                        stmt.setInt(3, product);
                    }
                    rs = stmt.executeQuery();
                    logger.debug("Executing getSummaryReport for Group - Poured query");
                    appendSummaryReportByLocationXML(pouredData, rs);
                } else if (user > 0 && groupUser) {
                    stmt = conn.prepareStatement(selectGroupUserPoured);
                    stmt.setString(1, validatedStartDate.toString());
                    stmt.setString(2, validatedEndDate.toString());
                    if (product > 0) {
                        stmt.setInt(3, product);
                    }
                    rs = stmt.executeQuery();
                    logger.debug("Executing getSummaryReport for User - Poured query");
                    appendSummaryReportByLocationXML(pouredData, rs);
                } else if (supplier >= 0) {
                    stmt = conn.prepareStatement(selectSupplierPoured);
                    stmt.setInt(1, supplier);
                    stmt.setString(2, validatedStartDate.toString());
                    stmt.setString(3, validatedEndDate.toString());
                    if (product > 0) {
                        stmt.setInt(4, product);
                    }
                    rs = stmt.executeQuery();
                    logger.debug("Executing getSummaryReport for Supplier - Poured query");
                    appendSummaryReportByLocationXML(pouredData, rs);
                } else if (county >= 0) {
                    parentLevel = 3;
                    stmt = conn.prepareStatement(selectCountyPoured);
                    stmt.setInt(1, user);
                    stmt.setInt(2, county);
                    stmt.setString(3, validatedStartDate.toString());
                    stmt.setString(4, validatedEndDate.toString());
                    if (product > 0) {
                        stmt.setInt(5, product);
                    }
                    rs = stmt.executeQuery();
                    logger.debug("Executing getSummaryReport for County - Poured query");
                    appendSummaryReportByLevelsXML(pouredData, rs, user, parentLevel);
                } else if (user > 0 && !groupUser) {
                    parentLevel = 1;
                    stmt = conn.prepareStatement(selectUserRegionPoured);
                    stmt.setInt(1, user);
                    stmt.setString(2, validatedStartDate.toString());
                    stmt.setString(3, validatedEndDate.toString());
                    if (product > 0) {
                        stmt.setInt(4, product);
                    }
                    rs = stmt.executeQuery();
                    logger.debug("Executing getSummaryReport for User Region - Poured query");
                    appendSummaryReportByLevelsXML(pouredData, rs, user, parentLevel);
                } else if (bevSync > 0) {
                   //logger.debug(selectBevSyncPoured);
                    HashMap<Integer, String> childMap = new HashMap<Integer, String>();
                    switch (reportGroupBy) {
                        case 0:
                            stmt = conn.prepareStatement("SELECT id, name FROM product");
                            rs = stmt.executeQuery();
                            while (rs.next()) {
                                childMap.put(rs.getInt(1), rs.getString(2));
                            }
                            break;
                        case 1:
                            stmt = conn.prepareStatement("SELECT p.id, CONCAT(pS.id, ':', pS.name) FROM product p LEFT JOIN productSetMap pSM ON pSM.product = p.id " +
                                    " LEFT JOIN productSet pS ON pS.id = pSM.productSet WHERE pS.productSetType = 7 AND pS.id IS NOT NULL;");
                            rs = stmt.executeQuery();
                            while (rs.next()) {
                                childMap.put(rs.getInt(1), rs.getString(2));
                            }
                            break;
                        case 2:
                            stmt = conn.prepareStatement("SELECT l.id, CONCAT(IFNULL(s.id, 0), ':', IFNULL(s.USPSST, '')) FROM location l " +
                                    " LEFT JOIN state s ON s.USPSST = l.addrState WHERE LENGTH(l.addrState) = 2;");
                            rs = stmt.executeQuery();
                            while (rs.next()) {
                                childMap.put(rs.getInt(1), rs.getString(2));
                            }
                            break;
                        case 3:
                            stmt = conn.prepareStatement("SELECT p.id, CONCAT(IFNULL(bS.id, pS.id), ':', IFNULL(bS.style,pS.name)), CONCAT(pS.id, ':', pS.name) " +
                                    " FROM product p LEFT JOIN productSetMap pSM ON pSM.product = p.id LEFT JOIN productSet pS ON pS.id = pSM.productSet " +
                                    " LEFT JOIN beerStylesMap bSM ON bSM.productSet = pS.id LEFT JOIN beerStyles bS ON bS.id = bSM.style WHERE pS.productSetType = 9 " +
                                    " AND pS.id IS NOT NULL ORDER BY bS.majorStyle, pS.name;");
                            rs = stmt.executeQuery();
                            while (rs.next()) {
                                childMap.put(rs.getInt(1), rs.getString(2));
                            }
                            break;
                    }
                    stmt = conn.prepareStatement(selectBevSyncPoured);                   
                    stmt.setString(1, validatedStartDate.toString());
                    stmt.setString(2, validatedEndDate.toString());                    
                    rs = stmt.executeQuery();
                    logger.debug("Executing getSummaryReport for bevSync - Poured query");
                    appendSummaryReportForBeerBoardXML(pouredData, reportGroupBy, childMap, rs);
                }
                rs.close();
            } catch (SQLException sqle) {
                throw new HandlerException(sqle);
            } finally {
            }
            if(callerId > 0){
                logger.portalVisitDetail(callerId, "getSummaryReport", location, "getSummaryReport",0,10, "", conn);
            }
        }
        
        /**/
        if (!includeExclusion) {
            int paramCount = 1;
            exclusionQuery += " AND eS.exclusion in (" + dataLocationExclusions + ")  ORDER BY eS.location, eS.bar, eS.exclusion, eS.date ";
            try {
                stmt = conn.prepareStatement(exclusionQuery);
                stmt.setString(paramCount++, validatedStartDate.toString());
                stmt.setString(paramCount++, validatedEndDate.toString());
                if (exclusionParameter > 0) {
                    stmt.setInt(paramCount++, exclusionParameter);
                }
                //logger.debug("Reached exclusion block");
                rs = stmt.executeQuery();
                appendExclusionSummaryReportByLocationXML(toAppend, rs);
            } catch (SQLException sqle) {
                throw new HandlerException(sqle);
            } finally {
                close(rs);
            }
        }
        
        if (showProjectionDetails) {
            int parameter                   = 0;
            int paramCount                  = 1;
            String projectionDetailsQuery   = "";
            if (location > 0 && bevSync < 1) {
                projectionDetailsQuery      = "SELECT DISTINCT pD.date, pT.name, p.name FROM projectionDetails pD LEFT JOIN projectionType pT ON pT.id = pD.projection " +
                                            " LEFT JOIN product p ON p.id = pD.product LEFT JOIN location l ON l.id = pD.location LEFT JOIN locationDetails lD ON lD.location = l.id " +
                                            " WHERE lD.active = 1 AND lD.data = 1 AND l.id = ? AND pD.date BETWEEN ? AND ? " +
                                            " GROUP BY pD.date, pD.product ORDER BY pD.date, p.name ";
                parameter                   = location;
            } else if (county > 0) {
                projectionDetailsQuery      = "SELECT DISTINCT pD.date, pT.name, l.name FROM projectionDetails pD LEFT JOIN projectionType pT ON pT.id = pD.projection " +
                                            " LEFT JOIN location l ON l.id = pD.location LEFT JOIN locationDetails lD ON lD.location = l.id " +
                                            " WHERE lD.active = 1 AND lD.data = 1 AND l.countyIndex = ? AND pD.date BETWEEN ? AND ? " +
                                            " GROUP BY pD.date, pD.location ORDER BY pD.date, l.name ";
                parameter                   = county;
            } else if (user > 0 && !groupUser) {
                projectionDetailsQuery      = "SELECT DISTINCT pD.date, pT.name, l.name FROM projectionDetails pD LEFT JOIN projectionType pT ON pT.id = pD.projection " +
                                            " LEFT JOIN location l ON l.id = pD.location LEFT JOIN locationDetails lD ON lD.location = l.id " +
                                            " LEFT JOIN regionCountyMap rCM ON rCM.county = l.countyIndex " +
                                            " LEFT JOIN groupRegionMap gRM ON gRM.regionMaster = rCM.region" +
                                            " LEFT JOIN region r ON r.regionGroup = gRM.id " +
                                            " LEFT JOIN userRegionMap uRM ON uRM.region = r.id " +
                                            " WHERE lD.active = 1 AND lD.data = 1 AND l.id NOT IN (" + userLocationExclusions + ") " +
                                            " AND uRM.user = ? AND pD.date BETWEEN ? AND ? " +
                                            " GROUP BY pD.date, pD.location ORDER BY pD.date, l.name ";
                parameter                   = user;
            } if (bevSync > 0) {
                projectionDetailsQuery      = "SELECT DISTINCT pD.date, pT.name, p.name FROM projectionDetails pD LEFT JOIN projectionType pT ON pT.id = pD.projection " +
                                            " LEFT JOIN product p ON p.id = pD.product LEFT JOIN location l ON l.id = pD.location LEFT JOIN locationDetails lD ON lD.location = l.id " +
                                            " WHERE lD.active = 1 AND lD.data = 1 AND lD.beerboard = 1 AND pD.date BETWEEN ? AND ? " + specificStates + bevSyncLocation +
                                            " GROUP BY pD.date, pD.product ORDER BY pD.date, p.name ";
               
            }

            try {
                stmt = conn.prepareStatement(projectionDetailsQuery);
                if(parameter > 0) {
                    stmt.setInt(paramCount++, parameter);
                }                
                stmt.setString(paramCount++, validatedStartDate.toString());
                stmt.setString(paramCount++, validatedEndDate.toString());
                rs = stmt.executeQuery();
                appendProjectionDetailsXML(toAppend, rs);
            } catch (SQLException sqle) {
                throw new HandlerException(sqle);
            } finally {
                close(rs);
            }
            
        }
         
    }

    private boolean isTimeValid(ReportDateSet lineDateSet, Date readingDate) {
        Date startDate = lineDateSet.getStartDate();
        Date endDate = lineDateSet.getEndDate();
        if (readingDate.after(startDate) && readingDate.before(endDate)) {
            return true;
        } else {
            /*
            logger.debug("start time: " + startDate.toString());
            logger.debug("end time: " + endDate.toString());
            logger.debug("data time: " + readingDate.toString());
             */
            return false;
        }
    }

    private String dateToString(Date toConvert) {
        String convertedDate = newDateFormat.format(toConvert);
        return convertedDate;
    }

    public Date setStartDate(int periodShift, int customer, String specificLocation, Date start) {
        String startDate = dateToString(start);
        Date returnDate = new Date();
        PreparedStatement stmt = null;
        ResultSet rs = null;
        String sqlQuery = null;
        String selectionString = (customer > 0 ? " WHERE l.customer=? " : " WHERE l.id IN (" + specificLocation + ") ");
        try {
            switch (periodShift) {
                case 0:
                    sqlQuery = "SELECT MIN(DATE_SUB(Concat(left(?,11),IFNULL(x.preOpen,'07:00:00')), INTERVAL eO HOUR)) preOpen" +
                            " FROM (Select CASE DAYOFWEEK(?)" +
                            " WHEN 1 THEN Right(lH.preOpenSun,8)" +
                            " WHEN 2 THEN Right(lH.preOpenMon,8)" +
                            " WHEN 3 THEN Right(lH.preOpenTue,8)" +
                            " WHEN 4 THEN Right(lH.preOpenWed,8)" +
                            " WHEN 5 THEN Right(lH.preOpenThu,8)" +
                            " WHEN 6 THEN Right(lH.preOpenFri,8)" +
                            " WHEN 7 THEN Right(lH.preOpenSat,8) END preOpen," +
                            " l.easternOffset eO " +
                            " FROM locationHours lH RIGHT JOIN location l ON lH.location=l.id" + selectionString +
                            " ) AS x;";
                    stmt = conn.prepareStatement(sqlQuery);
                    stmt.setString(1, startDate);
                    stmt.setString(2, startDate);
                    if (customer > 0) {
                        stmt.setInt(3, customer);
                    }
                    rs = stmt.executeQuery();
                    if (rs.next()) {
                        returnDate = newDateFormat.parse(rs.getString(1));
                    }
                    break;
                case 1:
                    sqlQuery = "SELECT MIN(DATE_SUB(Concat(left(?,11),IFNULL(x.preOpen,'07:00:00')), INTERVAL eO HOUR)) preOpen" +
                            " FROM (Select CASE DAYOFWEEK(?)" +
                            " WHEN 1 THEN Right(lH.preOpenSun,8)" +
                            " WHEN 2 THEN Right(lH.preOpenMon,8)" +
                            " WHEN 3 THEN Right(lH.preOpenTue,8)" +
                            " WHEN 4 THEN Right(lH.preOpenWed,8)" +
                            " WHEN 5 THEN Right(lH.preOpenThu,8)" +
                            " WHEN 6 THEN Right(lH.preOpenFri,8)" +
                            " WHEN 7 THEN Right(lH.preOpenSat,8) END preOpen," +
                            " l.easternOffset eO " +
                            " FROM locationHours lH RIGHT JOIN location l ON lH.location=l.id" + selectionString +
                            " ) AS x;";
                    stmt = conn.prepareStatement(sqlQuery);
                    stmt.setString(1, startDate);
                    stmt.setString(2, startDate);
                    if (customer > 0) {
                        stmt.setInt(3, customer);
                    }
                    rs = stmt.executeQuery();
                    if (rs.next()) {
                        returnDate = newDateFormat.parse(rs.getString(1));
                    }
                    break;
                case 2:
                    sqlQuery = "SELECT MIN(DATE_SUB(Concat(left(?,11),IFNULL(x.open,'11:00:00')), INTERVAL eO HOUR)) Open" +
                            " FROM (Select CASE DAYOFWEEK(?)" +
                            " WHEN 1 THEN Right(lH.openSun,8)" +
                            " WHEN 2 THEN Right(lH.openMon,8)" +
                            " WHEN 3 THEN Right(lH.openTue,8)" +
                            " WHEN 4 THEN Right(lH.openWed,8)" +
                            " WHEN 5 THEN Right(lH.openThu,8)" +
                            " WHEN 6 THEN Right(lH.openFri,8)" +
                            " WHEN 7 THEN Right(lH.openSat,8) END open," +
                            " l.easternOffset eO" +
                            " FROM locationHours lH RIGHT JOIN location l ON lH.location=l.id" + selectionString +
                            " ) AS x;";
                    stmt = conn.prepareStatement(sqlQuery);
                    stmt.setString(1, startDate);
                    stmt.setString(2, startDate);
                    if (customer > 0) {
                        stmt.setInt(3, customer);
                    }
                    rs = stmt.executeQuery();
                    if (rs.next()) {
                        returnDate = newDateFormat.parse(rs.getString(1));
                    }
                    break;
                case 3:
                    sqlQuery = "SELECT MIN(DATE_SUB(If(IFNULL(x.close,'02:00:00')>'12:0:0',concat(left(?,11),IFNULL(x.close,'02:00:00')),concat(left(adddate(?,INTERVAL 1 DAY),11),IFNULL(x.close,'02:00:00'))), INTERVAL eO HOUR)) Close" +
                            " FROM (Select CASE DAYOFWEEK(?)" +
                            " WHEN 1 THEN Right(lH.closeSun,8)" +
                            " WHEN 2 THEN Right(lH.closeMon,8)" +
                            " WHEN 3 THEN Right(lH.closeTue,8)" +
                            " WHEN 4 THEN Right(lH.closeWed,8)" +
                            " WHEN 5 THEN Right(lH.closeThu,8)" +
                            " WHEN 6 THEN Right(lH.closeFri,8)" +
                            " WHEN 7 THEN Right(lH.closeSat,8) END close," +
                            " l.easternOffset eO" +
                            " FROM locationHours lH RIGHT JOIN location l ON lH.location=l.id" + selectionString +
                            " ) AS x;";
                    stmt = conn.prepareStatement(sqlQuery);
                    stmt.setString(1, startDate);
                    stmt.setString(2, startDate);
                    stmt.setString(3, startDate);
                    if (customer > 0) {
                        stmt.setInt(4, customer);
                    }
                    rs = stmt.executeQuery();
                    if (rs.next()) {
                        returnDate = newDateFormat.parse(rs.getString(1));
                    }
                    break;
                case 4:
                    sqlQuery = "SELECT MIN(DATE_SUB(Concat(left(?,11),IFNULL(x.preOpen,'07:00:00')), INTERVAL eO HOUR)) preOpen" +
                            " FROM (Select CASE DAYOFWEEK(?)" +
                            " WHEN 1 THEN Right(lH.preOpenSun,8)" +
                            " WHEN 2 THEN Right(lH.preOpenMon,8)" +
                            " WHEN 3 THEN Right(lH.preOpenTue,8)" +
                            " WHEN 4 THEN Right(lH.preOpenWed,8)" +
                            " WHEN 5 THEN Right(lH.preOpenThu,8)" +
                            " WHEN 6 THEN Right(lH.preOpenFri,8)" +
                            " WHEN 7 THEN Right(lH.preOpenSat,8) END preOpen," +
                            " l.easternOffset eO " +
                            " FROM locationHours lH RIGHT JOIN location l ON lH.location=l.id" + selectionString +
                            " ) AS x;";
                    stmt = conn.prepareStatement(sqlQuery);
                    stmt.setString(1, startDate);
                    stmt.setString(2, startDate);
                    if (customer > 0) {
                        stmt.setInt(3, customer);
                    }
                    rs = stmt.executeQuery();
                    if (rs.next()) {
                        returnDate = newDateFormat.parse(rs.getString(1));
                    }
                    break;
                default:
                    break;
            }

        } catch (Exception sqle) {
            logger.dbError("Method error: " + sqle.getMessage());
        } finally {
            close(stmt);
            close(rs);
        }
        logger.debug("New Start Date: " + returnDate.toString() + " for perdiodShift: " + String.valueOf(periodShift));
        return returnDate;
    }

    public Date setEndDate(int periodShift, int customer, String specificLocation, Date end) {
        String endDate = dateToString(end);
        Date returnDate = new Date();
        PreparedStatement stmt = null;
        ResultSet rs = null;
        String sqlQuery = null;
        String selectionString = (customer > 0 ? " WHERE l.customer=? " : " WHERE l.id IN (" + specificLocation + ") ");
        try {
            switch (periodShift) {
                case 0:
                    sqlQuery = "SELECT MAX(DATE_SUB(CONCAT(LEFT(ADDDATE(?,INTERVAL 1 DAY),11),IFNULL(x.preOpen,'07:00:00')), INTERVAL eO HOUR)) preOpen" +
                            " FROM (Select CASE DAYOFWEEK(ADDDATE(?,INTERVAL 1 DAY))" +
                            " WHEN 1 THEN Right(lH.preOpenSun,8)" +
                            " WHEN 2 THEN Right(lH.preOpenMon,8)" +
                            " WHEN 3 THEN Right(lH.preOpenTue,8)" +
                            " WHEN 4 THEN Right(lH.preOpenWed,8)" +
                            " WHEN 5 THEN Right(lH.preOpenThu,8)" +
                            " WHEN 6 THEN Right(lH.preOpenFri,8)" +
                            " WHEN 7 THEN Right(lH.preOpenSat,8) END preOpen," +
                            " l.easternOffset eO" +
                            " FROM locationHours lH RIGHT JOIN location l ON lH.location=l.id" + selectionString +
                            " ) AS x;";
                    stmt = conn.prepareStatement(sqlQuery);
                    stmt.setString(1, endDate);
                    stmt.setString(2, endDate);
                    if (customer > 0) {
                        stmt.setInt(3, customer);
                    }
                    rs = stmt.executeQuery();
                    if (rs.next()) {
                        returnDate = newDateFormat.parse(rs.getString(1));
                    }
                    break;
                case 1:
                    sqlQuery = "SELECT MAX(DATE_SUB(Concat(left(?,11),IFNULL(x.open,'11:00:00')), INTERVAL eO HOUR)) Open" +
                            " FROM (Select CASE DAYOFWEEK(?)" +
                            " WHEN 1 THEN Right(lH.openSun,8)" +
                            " WHEN 2 THEN Right(lH.openMon,8)" +
                            " WHEN 3 THEN Right(lH.openTue,8)" +
                            " WHEN 4 THEN Right(lH.openWed,8)" +
                            " WHEN 5 THEN Right(lH.openThu,8)" +
                            " WHEN 6 THEN Right(lH.openFri,8)" +
                            " WHEN 7 THEN Right(lH.openSat,8) END open," +
                            " l.easternOffset eO" +
                            " FROM locationHours lH RIGHT JOIN location l ON lH.location=l.id" + selectionString +
                            " ) AS x;";
                    stmt = conn.prepareStatement(sqlQuery);
                    stmt.setString(1, endDate);
                    stmt.setString(2, endDate);
                    if (customer > 0) {
                        stmt.setInt(3, customer);
                    }
                    rs = stmt.executeQuery();
                    if (rs.next()) {
                        returnDate = newDateFormat.parse(rs.getString(1));
                    }
                    break;
                case 2:
                    sqlQuery = "SELECT MAX(DATE_SUB(If(IFNULL(x.close,'02:00:00')>'12:0:0',concat(left(?,11),IFNULL(x.close,'02:00:00')),concat(left(ADDDATE(?,INTERVAL 1 DAY),11),IFNULL(x.close,'02:00:00'))), INTERVAL eO HOUR)) Close" +
                            " FROM (Select CASE DAYOFWEEK(?)" +
                            " WHEN 1 THEN Right(lH.closeSun,8)" +
                            " WHEN 2 THEN Right(lH.closeMon,8)" +
                            " WHEN 3 THEN Right(lH.closeTue,8)" +
                            " WHEN 4 THEN Right(lH.closeWed,8)" +
                            " WHEN 5 THEN Right(lH.closeThu,8)" +
                            " WHEN 6 THEN Right(lH.closeFri,8)" +
                            " WHEN 7 THEN Right(lH.closeSat,8) END close," +
                            " l.easternOffset eO" +
                            " FROM locationHours lH RIGHT JOIN location l ON lH.location=l.id" + selectionString +
                            " ) AS x;";
                    stmt = conn.prepareStatement(sqlQuery);
                    stmt.setString(1, endDate);
                    stmt.setString(2, endDate);
                    stmt.setString(3, endDate);
                    if (customer > 0) {
                        stmt.setInt(4, customer);
                    }
                    rs = stmt.executeQuery();
                    if (rs.next()) {
                        returnDate = newDateFormat.parse(rs.getString(1));
                    }
                    break;
                case 3:
                    sqlQuery = "SELECT MAX(DATE_SUB(CONCAT(LEFT(ADDDATE(?,INTERVAL 1 DAY),11),IFNULL(x.preOpen,'07:00:00')), INTERVAL eO HOUR)) preOpen" +
                            " FROM (Select CASE DAYOFWEEK(ADDDATE(?,INTERVAL 1 DAY))" +
                            " WHEN 1 THEN Right(lH.preOpenSun,8)" +
                            " WHEN 2 THEN Right(lH.preOpenMon,8)" +
                            " WHEN 3 THEN Right(lH.preOpenTue,8)" +
                            " WHEN 4 THEN Right(lH.preOpenWed,8)" +
                            " WHEN 5 THEN Right(lH.preOpenThu,8)" +
                            " WHEN 6 THEN Right(lH.preOpenFri,8)" +
                            " WHEN 7 THEN Right(lH.preOpenSat,8) END preOpen," +
                            " l.easternOffset eO" +
                            " FROM locationHours lH RIGHT JOIN location l ON lH.location=l.id" + selectionString +
                            " ) AS x;";
                    stmt = conn.prepareStatement(sqlQuery);
                    stmt.setString(1, endDate);
                    stmt.setString(2, endDate);
                    if (customer > 0) {
                        stmt.setInt(3, customer);
                    }
                    rs = stmt.executeQuery();
                    if (rs.next()) {
                        returnDate = newDateFormat.parse(rs.getString(1));
                    }
                    break;
                case 4:
                    sqlQuery = "SELECT MAX(DATE_SUB(CONCAT(LEFT(ADDDATE(?,INTERVAL 1 DAY),11),IFNULL(x.preOpen,'07:00:00')), INTERVAL eO HOUR)) preOpen" +
                            " FROM (Select CASE DAYOFWEEK(ADDDATE(?,INTERVAL 1 DAY))" +
                            " WHEN 1 THEN Right(lH.preOpenSun,8)" +
                            " WHEN 2 THEN Right(lH.preOpenMon,8)" +
                            " WHEN 3 THEN Right(lH.preOpenTue,8)" +
                            " WHEN 4 THEN Right(lH.preOpenWed,8)" +
                            " WHEN 5 THEN Right(lH.preOpenThu,8)" +
                            " WHEN 6 THEN Right(lH.preOpenFri,8)" +
                            " WHEN 7 THEN Right(lH.preOpenSat,8) END preOpen," +
                            " l.easternOffset eO" +
                            " FROM locationHours lH RIGHT JOIN location l ON lH.location=l.id" + selectionString +
                            " ) AS x;";
                    stmt = conn.prepareStatement(sqlQuery);
                    stmt.setString(1, endDate);
                    stmt.setString(2, endDate);
                    if (customer > 0) {
                        stmt.setInt(3, customer);
                    }
                    rs = stmt.executeQuery();
                    if (rs.next()) {
                        returnDate = newDateFormat.parse(rs.getString(1));
                    }
                    break;
                default:
                    break;
            }
        } catch (Exception sqle) {
            logger.dbError("Method error: " + sqle.getMessage());
        } finally {
            close(stmt);
            close(rs);
        }
        logger.debug("New End Date: " + returnDate.toString());
        return returnDate;
    }

    private void getBreweryReport(Element toHandle, Element toAppend) throws HandlerException {

        int customer                        = HandlerUtils.getOptionalInteger(toHandle, "customerId");
        boolean breweryPoured               = HandlerUtils.getOptionalBoolean(toHandle, "breweryPoured");
        boolean top20Brand                  = HandlerUtils.getOptionalBoolean(toHandle, "top20Brand");
        boolean styleMovement               = HandlerUtils.getOptionalBoolean(toHandle, "styleMovement");
        boolean categoryMovement            = HandlerUtils.getOptionalBoolean(toHandle, "categoryMovement");
        boolean top5Brands                  = HandlerUtils.getOptionalBoolean(toHandle, "top5Brands");
        boolean top5Trending                = HandlerUtils.getOptionalBoolean(toHandle, "top5Trending");

        PreparedStatement stmt              = null;
        ResultSet rs                        = null;

        int paramsSet                       = 0, parentLevel = 0, groupId = 0;
        try {

            Date startDate                  = dateFormat.parse(HandlerUtils.getRequiredString(toHandle, "startDate"));
            Date endDate                    = dateFormat.parse(HandlerUtils.getRequiredString(toHandle, "endDate"));
            stmt                            = conn.prepareStatement("SELECT groupId FROM customer WHERE id = ?");
            stmt.setInt(1, customer);
            rs                              = stmt.executeQuery();
            if (rs.next()) {
                groupId                     = (rs.getInt(1) > 1 ? rs.getInt(1) : 0 );
            }

            if (breweryPoured) {
                logger.debug("Brewery Poured Report");
                Element pouredData          = toAppend.addElement("poured");
                getBreweryPouredData(customer, groupId, newDateFormat.format(startDate).substring(0, 10), newDateFormat.format(endDate).substring(0, 10), pouredData);
            }

            if (top20Brand) {
                logger.debug("Brewery Top 20 Report");
                Element top20Data           = toAppend.addElement("top20");
                getBreweryTop20Data(customer, groupId, newDateFormat.format(startDate).substring(0, 10), newDateFormat.format(endDate).substring(0, 10), top20Data);
            }

            if (styleMovement) {
                logger.debug("Brewery Style Movement Report");
                Element movementData        = toAppend.addElement("movement");
                getBreweryStyleMovementData(customer, groupId, newDateFormat.format(startDate).substring(0, 10), newDateFormat.format(endDate).substring(0, 10), movementData);
            }

            if (categoryMovement) {
                logger.debug("Brewery Cateogry Movement Report");
                Element movementData        = toAppend.addElement("cmovement");
                getBreweryCategoryMovementData(customer, groupId, newDateFormat.format(startDate).substring(0, 10), newDateFormat.format(endDate).substring(0, 10), movementData);
            }

            if (top5Brands) {
                logger.debug("Brewery Top 5 Report");
                Element top5Data            = toAppend.addElement("top5");
                getBreweryTop5Data(customer, groupId, newDateFormat.format(startDate).substring(0, 10), newDateFormat.format(endDate).substring(0, 10), top5Data);
            }

            if (top5Trending) {
                logger.debug("Brewery Top 5 Trending Report");
                Element top5TrendingData    = toAppend.addElement("top5Trending");
                getBreweryTop5TrendingData(customer, groupId, newDateFormat.format(startDate).substring(0, 10), newDateFormat.format(endDate).substring(0, 10), top5TrendingData);
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error in getBreweryReport: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } catch (ParseException sqle) {
            logger.dbError("Database error in getBreweryReport: " + sqle.getMessage());
            throw new HandlerException(sqle);
        }  finally {
            close(rs);
            close(stmt);
        }
    }

    private void getBreweryPouredData(int customer, int value, String startDate, String endDate, Element pouredData) throws HandlerException {

        String condition                    = "c.groupId";
        if (value == 0) 
        {
            value                           = customer;
            condition                       = "c.id";
        }

        String selectLines                  = "SELECT p.id, COUNT(li.id) FROM line li LEFT JOIN bar b ON b.id = li.bar " +
                                            " LEFT JOIN location l ON l.id = b.location LEFT JOIN customer c ON c.id = l.customer " +
                                            " LEFT JOIN product p ON p.id = li.product WHERE " + condition + " = ? AND li.status = 'RUNNING' AND p.id NOT IN (4311,10661) " +
                                            " GROUP BY p.id ORDER BY p.name;";
        String selectPoured                 = "SELECT p.id, SUM(oHS.value) FROM openHoursSummary oHS LEFT JOIN location l ON l.id = oHS.location " +
                                            " LEFT JOIN customer c ON c.id = l.customer LEFT JOIN product p ON p.id = oHS.product " +
                                            " WHERE " + condition + " = ? AND oHS.date BETWEEN ? AND ? AND p.id NOT IN(4311,10661) GROUP BY p.id ORDER BY p.name;";
        String selectBrewery                = "SELECT pS.id, pS.name FROM productSet pS LEFT JOIN productSetMap pSM ON pSM.productSet = pS.id " +
                                            " WHERE pSM.product = ? AND pS.productSetType = 7;";

        PreparedStatement stmt              = null;
        ResultSet rs                        = null, rsLocation = null;

        HashMap<Integer, Integer> productCount
                                            = new HashMap<Integer, Integer>();
        HashMap<Integer, Integer> lineCount = new HashMap<Integer, Integer>();
        HashMap<Integer, Double> volumeCount= new HashMap<Integer, Double>();
        HashMap<Integer, String> breweryName= new HashMap<Integer, String>();

        try {
            int totalLine                   = 0;
            Double totalVolume              = 0.0;
            stmt                            = conn.prepareStatement(selectLines);
            stmt.setInt(1, value);
            rsLocation                      = stmt.executeQuery();
            while (rsLocation.next()) {
                productCount.put(rsLocation.getInt(1), rsLocation.getInt(2));
            }

            stmt                            = conn.prepareStatement(selectPoured);
            stmt.setInt(1, value);
            stmt.setString(2, startDate);
            stmt.setString(3, endDate);
            rsLocation                      = stmt.executeQuery();
            while (rsLocation.next()) {
                int line                    = 0;
                Double volume               = 0.0;
                int productId               = rsLocation.getInt(1);
                
                stmt                        = conn.prepareStatement(selectBrewery);
                stmt.setInt(1, productId);
                rs                          = stmt.executeQuery();
                if (rs.next()) {
                    int breweryId           = rs.getInt(1);
                    if (!productCount.containsKey(productId)) {
                        productCount.put(productId, 1);
                    }
                    breweryName.put(breweryId, rs.getString(2));
                    totalVolume             = totalVolume + rsLocation.getDouble(2);
                    totalLine               = totalLine + productCount.get(productId);

                    if (volumeCount.containsKey(breweryId)) {
                        volume              = volumeCount.get(breweryId);
                    }
                    volume                  = volume + rsLocation.getDouble(2);
                    volumeCount.put(breweryId, volume);

                    if (lineCount.containsKey(breweryId)) {
                        line                = lineCount.get(breweryId);
                    }
                    line                    = line + productCount.get(productId);
                    lineCount.put(breweryId, line);
                } else {
                    int breweryId           = 0;
                    if (!productCount.containsKey(productId)) {
                        productCount.put(productId, 1);
                    }
                    breweryName.put(breweryId, "Others");
                    totalVolume             = totalVolume + rsLocation.getDouble(2);
                    totalLine               = totalLine + productCount.get(productId);

                    if (volumeCount.containsKey(breweryId)) {
                        volume              = volumeCount.get(breweryId);
                    }
                    volume                  = volume + rsLocation.getDouble(2);
                    volumeCount.put(breweryId, volume);

                    if (lineCount.containsKey(breweryId)) {
                        line                = lineCount.get(breweryId);
                    }
                    line                    = line + productCount.get(productId);
                    lineCount.put(breweryId, line);
                }
            }

            for (int breweryId : breweryName.keySet()) {
                Element var                 = pouredData.addElement("poured");
                var.addAttribute("brewery", HandlerUtils.nullToString(String.valueOf(breweryId), "Unknown"));
                var.addAttribute("breweryName", HandlerUtils.nullToString(breweryName.get(breweryId), "Unknown"));
                int lines                   = lineCount.get(breweryId);
                Double volume               = volumeCount.get(breweryId);
                Double share                = (volume / totalVolume) * 100;
                Double lineVolume           = volume / lines;

                var.addAttribute("lines", HandlerUtils.nullToString(String.valueOf(lines), "0"));
                var.addAttribute("poured", HandlerUtils.nullToString(String.valueOf(volume), "0"));
                var.addAttribute("share", HandlerUtils.nullToString(String.valueOf(share), "0"));
                var.addAttribute("lineVolume", HandlerUtils.nullToString(String.valueOf(lineVolume), "0"));
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error in getBreweryPouredData: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(rsLocation);
            close(rs);
            close(stmt);
        }
    }

    private void getBreweryTop20Data(int customer, int value, String startDate, String endDate, Element top20Data) throws HandlerException {

        String condition                    = "c.groupId";
        if (value == 0)
        {
            value                           = customer;
            condition                       = "c.id";
        }

        String selectLines                  = "SELECT p.id, COUNT(li.id) FROM line li LEFT JOIN bar b ON b.id = li.bar " +
                                            " LEFT JOIN location l ON l.id = b.location LEFT JOIN customer c ON c.id = l.customer " +
                                            " LEFT JOIN product p ON p.id = li.product WHERE " + condition + " = ? AND li.status = 'RUNNING' AND p.id NOT IN(4311,10661) " +
                                            " GROUP BY p.id ORDER BY p.name;";
        String selectPoured                 = "SELECT p.id, p.name, SUM(oHS.value) FROM openHoursSummary oHS LEFT JOIN location l ON l.id = oHS.location " +
                                            " LEFT JOIN customer c ON c.id = l.customer LEFT JOIN product p ON p.id = oHS.product " +
                                            " WHERE " + condition + " = ? AND oHS.date BETWEEN ? AND ? AND p.id NOT IN(4311,10661) GROUP BY p.id ORDER BY p.name;";

        PreparedStatement stmt              = null;
        ResultSet rs                        = null, rsLocation = null;

        HashMap<Integer, Integer> productCount
                                            = new HashMap<Integer, Integer>();
        HashMap<Integer, Integer> lineCount = new HashMap<Integer, Integer>();
        HashMap<Integer, Double> volumeCount= new HashMap<Integer, Double>();
        HashMap<Integer, String> productName= new HashMap<Integer, String>();

        try {
            int totalLine                   = 0;
            Double totalVolume              = 0.0;

            stmt                            = conn.prepareStatement(selectLines);
            stmt.setInt(1, value);
            rsLocation                      = stmt.executeQuery();
            while (rsLocation.next()) {
                productCount.put(rsLocation.getInt(1), rsLocation.getInt(2));
            }
            
            stmt                            = conn.prepareStatement(selectPoured);
            stmt.setInt(1, value);
            stmt.setString(2, startDate);
            stmt.setString(3, endDate);
            rsLocation                      = stmt.executeQuery();
            while (rsLocation.next()) {
                int line                    = 0;
                Double volume               = 0.0;
                int productId               = rsLocation.getInt(1);
                if (!productCount.containsKey(productId)) {
                    productCount.put(productId, 1);
                }
                productName.put(productId, rsLocation.getString(2));
                
                totalVolume                 = totalVolume + rsLocation.getDouble(3);
                totalLine                   = totalLine + productCount.get(productId);

                if (volumeCount.containsKey(productId)) {
                    volume                  = volumeCount.get(productId);
                }
                volume                      = volume + rsLocation.getDouble(3);
                volumeCount.put(productId, volume);

                if (lineCount.containsKey(productId)) {
                    line                    = lineCount.get(productId);
                }
                line                        = line + productCount.get(productId);
                lineCount.put(productId, line);
            }

            for (int productId : productName.keySet()) {
                Element var                 = top20Data.addElement("top20");
                var.addAttribute("product", HandlerUtils.nullToString(String.valueOf(productId), "Unknown"));
                var.addAttribute("productName", HandlerUtils.nullToString(productName.get(productId), "Unknown"));
                int lines                   = lineCount.get(productId);
                Double volume               = volumeCount.get(productId);
                Double share                = (volume / totalVolume) * 100;
                Double lineVolume           = volume / lines;

                var.addAttribute("lines", HandlerUtils.nullToString(String.valueOf(lines), "0"));
                var.addAttribute("poured", HandlerUtils.nullToString(String.valueOf(volume), "0"));
                var.addAttribute("share", HandlerUtils.nullToString(String.valueOf(share), "0"));
                var.addAttribute("lineVolume", HandlerUtils.nullToString(String.valueOf(lineVolume), "0"));
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error in getBreweryTop20Data: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(rsLocation);
            close(rs);
            close(stmt);
        }
    }

    private void getBreweryStyleMovementData(int customer, int value, String startDate, String endDate, Element styleMovementData) throws HandlerException {

        String condition                    = "c.groupId";
        if (value == 0)
        {
            value                           = customer;
            condition                       = "c.id";
        }

        String selectLines                  = "SELECT p.id, COUNT(li.id) FROM line li LEFT JOIN bar b ON b.id = li.bar " +
                                            " LEFT JOIN location l ON l.id = b.location LEFT JOIN customer c ON c.id = l.customer " +
                                            " LEFT JOIN product p ON p.id = li.product WHERE " + condition + " = ? AND li.status = 'RUNNING' AND p.id NOT IN (4311, 10661) " +
                                            " GROUP BY p.id ORDER BY p.name;";
        String selectPoured                 = "SELECT p.id, SUM(oHS.value) FROM openHoursSummary oHS LEFT JOIN location l ON l.id = oHS.location " +
                                            " LEFT JOIN customer c ON c.id = l.customer LEFT JOIN product p ON p.id = oHS.product " +
                                            " WHERE " + condition + " = ? AND oHS.date BETWEEN ? AND ? AND p.id NOT IN (4311, 10661) GROUP BY p.id ORDER BY p.name;";
        String selectStyle                  = "SELECT pS.id, pS.name FROM productSet pS LEFT JOIN productSetMap pSM ON pSM.productSet = pS.id " +
                                            " WHERE pSM.product = ? AND pS.productSetType = 9;";

        PreparedStatement stmt              = null;
        ResultSet rs                        = null, rsLocation = null;

        HashMap<Integer, Integer> productCount
                                            = new HashMap<Integer, Integer>();
        HashMap<Integer, Integer> lineCount = new HashMap<Integer, Integer>();
        HashMap<Integer, Double> volumeCount= new HashMap<Integer, Double>();
        HashMap<Integer, String> styleName  = new HashMap<Integer, String>();

        try {
            int totalLine                   = 0;
            Double totalVolume              = 0.0;

            stmt                            = conn.prepareStatement(selectLines);
            stmt.setInt(1, value);
            rsLocation                      = stmt.executeQuery();
            while (rsLocation.next()) {
                productCount.put(rsLocation.getInt(1), rsLocation.getInt(2));
            }

            stmt                            = conn.prepareStatement(selectPoured);
            stmt.setInt(1, value);
            stmt.setString(2, startDate);
            stmt.setString(3, endDate);
            rsLocation                      = stmt.executeQuery();
            while (rsLocation.next()) {
                int productId               = rsLocation.getInt(1);
                int line                    = 0;
                Double volume               = 0.0;

                stmt                        = conn.prepareStatement(selectStyle);
                stmt.setInt(1, productId);
                rs                          = stmt.executeQuery();
                if (rs.next()) {
                    int styleId             = rs.getInt(1);
                    styleName.put(styleId, rs.getString(2));
                    if (!productCount.containsKey(productId)) {
                        productCount.put(productId, 1);
                    }
                    totalVolume             = totalVolume + rsLocation.getDouble(2);
                    totalLine               = totalLine + productCount.get(productId);

                    if (volumeCount.containsKey(styleId)) {
                        volume              = volumeCount.get(styleId);
                    }
                    volume                  = volume + rsLocation.getDouble(2);
                    volumeCount.put(styleId, volume);

                    if (lineCount.containsKey(styleId)) {
                        line                = lineCount.get(styleId);
                    }
                    line                    = line + productCount.get(productId);
                    lineCount.put(styleId, line);
                } else {
                    int styleId             = 0;
                    styleName.put(styleId, "Others");
                    if (!productCount.containsKey(productId)) {
                        productCount.put(productId, 1);
                    }
                    totalVolume             = totalVolume + rsLocation.getDouble(2);
                    totalLine               = totalLine + productCount.get(productId);

                    if (volumeCount.containsKey(styleId)) {
                        volume              = volumeCount.get(styleId);
                    }
                    volume                  = volume + rsLocation.getDouble(2);
                    volumeCount.put(styleId, volume);

                    if (lineCount.containsKey(styleId)) {
                        line                = lineCount.get(styleId);
                    }
                    line                    = line + productCount.get(productId);
                    lineCount.put(styleId, line);
                }
            }

            for (int styleId : styleName.keySet()) {
                Element var                 = styleMovementData.addElement("movement");
                var.addAttribute("style", HandlerUtils.nullToString(String.valueOf(styleId), "Unknown"));
                var.addAttribute("styleName", HandlerUtils.nullToString(styleName.get(styleId), "Unknown"));
                int lines                   = lineCount.get(styleId);
                Double volume               = volumeCount.get(styleId);
                Double share                = (volume / totalVolume) * 100;
                Double lineVolume           = volume / lines;

                var.addAttribute("lines", HandlerUtils.nullToString(String.valueOf(lines), "0"));
                var.addAttribute("poured", HandlerUtils.nullToString(String.valueOf(volume), "0"));
                var.addAttribute("share", HandlerUtils.nullToString(String.valueOf(share), "0"));
                var.addAttribute("lineVolume", HandlerUtils.nullToString(String.valueOf(lineVolume), "0"));
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error in getBreweryStyleMovementData: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(rsLocation);
            close(rs);
            close(stmt);
        }
    }

    private void getBreweryCategoryMovementData(int customer, int value, String startDate, String endDate, Element styleMovementData) throws HandlerException {

        String condition                    = "c.groupId";
        if (value == 0)
        {
            value                           = customer;
            condition                       = "c.id";
        }

        String selectLines                  = "SELECT p.id, COUNT(li.id) FROM line li LEFT JOIN bar b ON b.id = li.bar " +
                                            " LEFT JOIN location l ON l.id = b.location LEFT JOIN customer c ON c.id = l.customer " +
                                            " LEFT JOIN product p ON p.id = li.product WHERE " + condition + " = ? AND li.status = 'RUNNING' AND p.id NOT IN (4311,10661) " +
                                            " GROUP BY p.id ORDER BY p.name;";
        String selectPoured                 = "SELECT p.id, SUM(oHS.value) FROM openHoursSummary oHS LEFT JOIN location l ON l.id = oHS.location " +
                                            " LEFT JOIN customer c ON c.id = l.customer LEFT JOIN product p ON p.id = oHS.product " +
                                            " WHERE " + condition + " = ? AND oHS.date BETWEEN ? AND ? AND p.id NOT IN (4311,10661) GROUP BY p.id ORDER BY p.name;";
        String selectCategory               = "SELECT category, IF(category = 1, 'Domestic', IF(category = 3, 'Import', IF(category = 2, 'Craft', 'Custom'))) " +
                                            " FROM productDescription WHERE product = ?;";

        PreparedStatement stmt              = null;
        ResultSet rs                        = null, rsLocation = null;

        HashMap<Integer, Integer> productCount
                                            = new HashMap<Integer, Integer>();
        HashMap<Integer, Integer> lineCount = new HashMap<Integer, Integer>();
        HashMap<Integer, Double> volumeCount= new HashMap<Integer, Double>();
        HashMap<Integer, String> categoryName
                                            = new HashMap<Integer, String>();

        try {
            int totalLine                   = 0;
            Double totalVolume              = 0.0;

            stmt                            = conn.prepareStatement(selectLines);
            stmt.setInt(1, value);
            rsLocation                      = stmt.executeQuery();
            while (rsLocation.next()) {
                productCount.put(rsLocation.getInt(1), rsLocation.getInt(2));
            }

            stmt                            = conn.prepareStatement(selectPoured);
            stmt.setInt(1, value);
            stmt.setString(2, startDate);
            stmt.setString(3, endDate);
            rsLocation                      = stmt.executeQuery();
            while (rsLocation.next()) {
                int productId               = rsLocation.getInt(1);
                int line                    = 0;
                Double volume               = 0.0;

                stmt                        = conn.prepareStatement(selectCategory);
                stmt.setInt(1, productId);
                rs                          = stmt.executeQuery();
                if (rs.next()) {
                    int categoryId          = rs.getInt(1);
                    categoryName.put(categoryId, rs.getString(2));
                    if (!productCount.containsKey(productId)) {
                        productCount.put(productId, 1);
                    }
                    totalVolume             = totalVolume + rsLocation.getDouble(2);
                    totalLine               = totalLine + productCount.get(productId);

                    if (volumeCount.containsKey(categoryId)) {
                        volume              = volumeCount.get(categoryId);
                    }
                    volume                  = volume + rsLocation.getDouble(2);
                    volumeCount.put(categoryId, volume);

                    if (lineCount.containsKey(categoryId)) {
                        line                = lineCount.get(categoryId);
                    }
                    line                    = line + productCount.get(productId);
                    lineCount.put(categoryId, line);
                } else {
                    int categoryId          = 0;
                    categoryName.put(categoryId, "Others");
                    if (!productCount.containsKey(productId)) {
                        productCount.put(productId, 1);
                    }
                    totalVolume             = totalVolume + rsLocation.getDouble(2);
                    totalLine               = totalLine + productCount.get(productId);

                    if (volumeCount.containsKey(categoryId)) {
                        volume              = volumeCount.get(categoryId);
                    }
                    volume                  = volume + rsLocation.getDouble(2);
                    volumeCount.put(categoryId, volume);

                    if (lineCount.containsKey(categoryId)) {
                        line                = lineCount.get(categoryId);
                    }
                    line                    = line + productCount.get(productId);
                    lineCount.put(categoryId, line);
                }
            }

            for (int categoryId : categoryName.keySet()) {
                Element var                 = styleMovementData.addElement("cmovement");
                var.addAttribute("category", HandlerUtils.nullToString(String.valueOf(categoryId), "Unknown"));
                var.addAttribute("categoryName", HandlerUtils.nullToString(categoryName.get(categoryId), "Unknown"));
                int lines                   = lineCount.get(categoryId);
                Double volume               = volumeCount.get(categoryId);
                Double share                = (volume / totalVolume) * 100;
                Double lineVolume           = volume / lines;

                var.addAttribute("lines", HandlerUtils.nullToString(String.valueOf(lines), "0"));
                var.addAttribute("poured", HandlerUtils.nullToString(String.valueOf(volume), "0"));
                var.addAttribute("share", HandlerUtils.nullToString(String.valueOf(share), "0"));
                var.addAttribute("lineVolume", HandlerUtils.nullToString(String.valueOf(lineVolume), "0"));
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error in getBreweryCategoryMovementData: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(rsLocation);
            close(rs);
            close(stmt);
        }
    }

    private void getBreweryTop5Data(int customer, int value, String startDate, String endDate, Element top5Data) throws HandlerException {

        String condition                    = "c.groupId";
        if (value == 0)
        {
            value                           = customer;
            condition                       = "c.id";
        }

        String selectPoured                 = "SELECT p.id, SUM(oHS.value), COUNT(li.id) FROM openHoursSummary oHS LEFT JOIN location l ON l.id = oHS.location " +
                                            " LEFT JOIN customer c ON c.id = l.customer LEFT JOIN product p ON p.id = oHS.product " +
                                            " LEFT JOIN bar b ON b.location = l.id LEFT JOIN line li ON li.bar = b.id AND li.product = p.id " +
                                            " WHERE " + condition + " = ? AND oHS.date BETWEEN ? AND ? AND li.lastPoured > ? AND p.id NOT IN (4311,10661) " +
                                            " GROUP BY p.id ORDER BY p.name;";
        String selectBrewery                = "SELECT pS.id, pS.name FROM productSet pS LEFT JOIN productSetMap pSM ON pSM.productSet = pS.id " +
                                            " WHERE pSM.product = ? AND pS.productSetType = 7;";

        PreparedStatement stmt              = null;
        ResultSet rs                        = null, rsLocation = null;

        HashMap<Integer, Integer> lineCount = new HashMap<Integer, Integer>();
        HashMap<Integer, Double> volumeCount= new HashMap<Integer, Double>();
        HashMap<Integer, String> breweryName= new HashMap<Integer, String>();

        try {
            int totalLine                   = 0;
            Double totalVolume              = 0.0;

            stmt                            = conn.prepareStatement(selectPoured);
            stmt.setInt(1, value);
            stmt.setString(2, startDate);
            stmt.setString(3, endDate);
            stmt.setString(4, startDate);
            rsLocation                      = stmt.executeQuery();
            while (rsLocation.next()) {
                int line                    = 0;
                Double volume               = 0.0;

                stmt                        = conn.prepareStatement(selectBrewery);
                stmt.setInt(1, rsLocation.getInt(1));
                rs                          = stmt.executeQuery();
                if (rs.next()) {
                    int breweryId           = rs.getInt(1);
                    breweryName.put(breweryId, rs.getString(2));
                    totalVolume             = totalVolume + rsLocation.getDouble(2);
                    totalLine               = totalLine + rsLocation.getInt(3);

                    if (volumeCount.containsKey(breweryId)) {
                        volume              = volumeCount.get(breweryId);
                    }
                    volume                  = volume + rsLocation.getDouble(2);
                    volumeCount.put(breweryId, volume);

                    if (lineCount.containsKey(breweryId)) {
                        line                = lineCount.get(breweryId);
                    }
                    line                    = line + rsLocation.getInt(3);
                    lineCount.put(breweryId, line);
                }
            }

            for (int breweryId : breweryName.keySet()) {
                Element var                 = top5Data.addElement("poured");
                var.addAttribute("brewery", HandlerUtils.nullToString(String.valueOf(breweryId), "Unknown"));
                var.addAttribute("breweryName", HandlerUtils.nullToString(breweryName.get(breweryId), "Unknown"));
                int lines                   = lineCount.get(breweryId);
                Double volume               = volumeCount.get(breweryId);
                Double share                = (volume / totalVolume) * 100;
                Double lineVolume           = volume / lines;

                var.addAttribute("lines", HandlerUtils.nullToString(String.valueOf(lines), "0"));
                var.addAttribute("poured", HandlerUtils.nullToString(String.valueOf(volume), "0"));
                var.addAttribute("share", HandlerUtils.nullToString(String.valueOf(share), "0"));
                var.addAttribute("lineVolume", HandlerUtils.nullToString(String.valueOf(lineVolume), "0"));
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error in getBreweryTop5Data: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(rsLocation);
            close(rs);
            close(stmt);
        }
    }

    private void getBreweryTop5TrendingData(int customer, int value, String startDate, String endDate, Element top5TrendingData) throws HandlerException {

        String condition                    = "c.groupId";
        if (value == 0)
        {
            value                           = customer;
            condition                       = "c.id";
        }

        String selectPoured                 = "SELECT p.id, SUM(oHS.value), COUNT(li.id) FROM openHoursSummary oHS LEFT JOIN location l ON l.id = oHS.location " +
                                            " LEFT JOIN customer c ON c.id = l.customer LEFT JOIN product p ON p.id = oHS.product " +
                                            " LEFT JOIN bar b ON b.location = l.id LEFT JOIN line li ON li.bar = b.id AND li.product = p.id " +
                                            " WHERE " + condition + " = ? AND oHS.date BETWEEN ? AND ? AND li.lastPoured > ? AND p.id NOT IN(4311,10661) GROUP BY p.id ORDER BY p.name;";
        String selectBrewery                = "SELECT pS.id, pS.name FROM productSet pS LEFT JOIN productSetMap pSM ON pSM.productSet = pS.id " +
                                            " WHERE pSM.product = ? AND pS.productSetType = 7;";

        PreparedStatement stmt              = null;
        ResultSet rs                        = null, rsLocation = null;

        HashMap<Integer, Integer> lineCount = new HashMap<Integer, Integer>();
        HashMap<Integer, Double> volumeCount= new HashMap<Integer, Double>();
        HashMap<Integer, String> breweryName= new HashMap<Integer, String>();

        try {
            int totalLine                   = 0;
            Double totalVolume              = 0.0;

            stmt                            = conn.prepareStatement(selectPoured);
            stmt.setInt(1, value);
            stmt.setString(2, startDate);
            stmt.setString(3, endDate);
            stmt.setString(4, startDate);
            rsLocation                      = stmt.executeQuery();
            while (rsLocation.next()) {
                int line                    = 0;
                Double volume               = 0.0;

                stmt                        = conn.prepareStatement(selectBrewery);
                stmt.setInt(1, rsLocation.getInt(1));
                rs                          = stmt.executeQuery();
                if (rs.next()) {
                    int breweryId           = rs.getInt(1);
                    breweryName.put(breweryId, rs.getString(2));
                    totalVolume             = totalVolume + rsLocation.getDouble(2);
                    totalLine               = totalLine + rsLocation.getInt(3);

                    if (volumeCount.containsKey(breweryId)) {
                        volume              = volumeCount.get(breweryId);
                    }
                    volume                  = volume + rsLocation.getDouble(2);
                    volumeCount.put(breweryId, volume);

                    if (lineCount.containsKey(breweryId)) {
                        line                = lineCount.get(breweryId);
                    }
                    line                    = line + rsLocation.getInt(3);
                    lineCount.put(breweryId, line);
                }
            }

            for (int breweryId : breweryName.keySet()) {
                Element var                 = top5TrendingData.addElement("poured");
                var.addAttribute("brewery", HandlerUtils.nullToString(String.valueOf(breweryId), "Unknown"));
                var.addAttribute("breweryName", HandlerUtils.nullToString(breweryName.get(breweryId), "Unknown"));
                int lines                   = lineCount.get(breweryId);
                Double volume               = volumeCount.get(breweryId);
                Double share                = (volume / totalVolume) * 100;
                Double lineVolume           = volume / lines;

                var.addAttribute("lines", HandlerUtils.nullToString(String.valueOf(lines), "0"));
                var.addAttribute("poured", HandlerUtils.nullToString(String.valueOf(volume), "0"));
                var.addAttribute("share", HandlerUtils.nullToString(String.valueOf(share), "0"));
                var.addAttribute("lineVolume", HandlerUtils.nullToString(String.valueOf(lineVolume), "0"));
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error in getBreweryTop5TrendingData: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(rsLocation);
            close(rs);
            close(stmt);
        }
    }

    private void getDashBoardReport(Element toHandle, Element toAppend) throws HandlerException {

        ReportType reportType               = ReportType.instanceOf("Yesterday");
        String reportTypeString             = HandlerUtils.getOptionalString(toHandle, "reportType");
        if (null != reportTypeString) {
            reportType                      = ReportType.instanceOf(HandlerUtils.getOptionalString(toHandle, "reportType"));
        }
        PeriodType periodType               = PeriodType.DAILY;
        String periodStr                    = HandlerUtils.getOptionalString(toHandle, "periodType");
        if (null != periodStr) {
            periodType                      = PeriodType.parseString(HandlerUtils.getOptionalString(toHandle, "periodType"));
        }
        if (null == periodType) {
            throw new HandlerException("Invalid period type: " + periodStr);
        }

        int user                            = HandlerUtils.getOptionalInteger(toHandle, "userId");
        int location                        = HandlerUtils.getOptionalInteger(toHandle, "locationId");
        int customer                        = HandlerUtils.getOptionalInteger(toHandle, "customerId");
        int region                          = HandlerUtils.getOptionalInteger(toHandle, "regionId");

        boolean concessions                 = HandlerUtils.getOptionalBoolean(toHandle, "concessions");

        boolean variance                    = HandlerUtils.getOptionalBoolean(toHandle, "variance");
        boolean temperature                 = HandlerUtils.getOptionalBoolean(toHandle, "temperature");
        boolean performance                 = HandlerUtils.getOptionalBoolean(toHandle, "performance");
        boolean inventory                   = HandlerUtils.getOptionalBoolean(toHandle, "inventory");
        boolean category                    = HandlerUtils.getOptionalBoolean(toHandle, "category");
        boolean afterHours                  = HandlerUtils.getOptionalBoolean(toHandle, "afterHours");
        boolean varianceChart               = HandlerUtils.getOptionalBoolean(toHandle, "varianceChart");
        boolean keyStats                    = HandlerUtils.getOptionalBoolean(toHandle, "keyStats");
        boolean unclaimed                   = HandlerUtils.getOptionalBoolean(toHandle, "unclaimed");

        PreparedStatement stmt              = null;
        ResultSet rs                        = null;

        int paramsSet                       = 0, parentLevel = 0, value = 0;
        if (location >= 0) {
            parentLevel                     = 1;
            value                           = location;
            paramsSet++;
        }
        if (customer >= 0) {
            String checkRoot                = "SELECT u.isManager FROM user u WHERE u.id = ? ";
            try {
                stmt                        = conn.prepareStatement(checkRoot);
                stmt.setInt(1, user);
                rs                          = stmt.executeQuery();
                if (rs.next()) {
                    switch(rs.getInt(1)) {
                        case 0:
                            parentLevel     = 3;
                            value           = user;
                            break;
                        case 1:
                            parentLevel     = 2;
                            value           = customer;
                            break;
                        default:
                            parentLevel     = 2;
                            value           = customer;
                            break;
                    }
                }
            } catch (SQLException sqle) {
                logger.dbError("Database error in getDashBoardReport: " + sqle.getMessage());
                throw new HandlerException(sqle);
            } finally {
                close(rs);
                close(stmt);
            }
            paramsSet++;
        }
        if (paramsSet != 1) {
            throw new HandlerException("Exactly one of the following must be set: locationId customerId ");
        }
        

        if (variance) {
            logger.debug("Dashboard Variance Report");
            Element varData                 = toAppend.addElement("variance");
            if (concessions) {
                varData.addElement("showVar").addText("true");
                getDashBoardConcessionVariance(parentLevel, value, reportType,region, varData);
            } else {
                if (parentLevel == 1) {
                    varData.addElement("showVar").addText("false");
                } else {
                    varData.addElement("showVar").addText("true");
                }
                getDashBoardVariance(parentLevel, value, reportType,region, varData);
            }
        }

        if (temperature) {
            logger.debug("Dashboard Temperature Report");
            Element tempData                = toAppend.addElement("temperature");
            getDashBoardTemperature(parentLevel, value, reportType, region,tempData);
        }

        if (performance) {
            logger.debug("Dashboard Performance Report");
            Element perfData                = toAppend.addElement("performance");
            getDashBoardPerformance(parentLevel, value, reportType, region, perfData);
        }

        if (inventory) {
            logger.debug("Dashboard Inventory Report");
            Element invData                 = toAppend.addElement("inventory");
            getDashBoardInventory(parentLevel, value, reportType,region, invData);
        }

        if (category) {
            logger.debug("Dashboard Category Report");
            Element catData                 = toAppend.addElement("category");
            getDashBoardCategory(parentLevel, value, reportType,region, catData);
        }

        if (afterHours) {
            logger.debug("Dashboard After Hours Report");
            Element afhData                 = toAppend.addElement("afterHours");
            getDashBoardAfterHours(parentLevel, value, reportType,region, afhData);
        }

        if (varianceChart) {
            logger.debug("Dashboard Variance Chart Report");
            Element varChartData            = toAppend.addElement("varianceChart");
            getDashBoardVarianceChart(parentLevel, value, concessions, reportType, periodType,region, varChartData);
        }

        if (keyStats) {
            logger.debug("Dashboard Key Stats Report");
            Element keyStatsData            = toAppend.addElement("keyStats");
            getDashBoardKeyStats(parentLevel, value, reportType, region,keyStatsData);
            getDashBoardCategory(parentLevel, value, ReportType.Monthly, region,keyStatsData);
            getDashBoardTier(ReportType.Monthly, keyStatsData);
        }
        
        if (unclaimed) {
            logger.debug("Dashboard Unclaimed Report");
            Element unclaimData            = toAppend.addElement("unclaimed");
            getDashBoardUnclaim(parentLevel, value, reportType, region,unclaimData);
           
        }
    }


    private void getDashBoardConcessionVariance(int parentLevel, int value, ReportType reportType,int region, Element varData) throws HandlerException {

        String selectLocation               = "Select l.id, l.varianceAlert FROM location l ";
        String Grouping                     = "";
        switch(parentLevel) {
            case 1:
                Grouping                    += "b.name, ";
                selectLocation              += " WHERE l.id = ?";
                break;
            case 2:
                Grouping                    += "l.name, ";
                selectLocation              += " WHERE l.customer = ? ";
                if(region > 0){
                    selectLocation          += " AND l.region =? ";
                }
                break;
            case 3:
                Grouping                    += "l.name, ";
                selectLocation              += " LEFT JOIN userMap uM ON uM.location = l.id WHERE uM.user = ?";
                break;
            default:
                break;
        }
        
        String eventString                  = "0";
        ArrayList<String> eventArray        = new ArrayList<String>();

        String dateString                   = "";
        if (reportType == ReportType.YTD) {  dateString = " AND date > SUBDATE(?, INTERVAL " + reportType.toDays() + " DAY) ";  }
        
        String selectEventString            = "SELECT id, eventDesc, date FROM eventHours WHERE location = ? AND date < ? " + dateString +
                                            " ORDER BY eventEnd DESC LIMIT " + reportType.toDays();


        PreparedStatement stmt              = null;
        ResultSet rs                        = null, rsLocation = null;

        HashMap<Integer,ArrayList<HashMap<String,Double>>> dataMap
                                            = new HashMap<Integer,ArrayList<HashMap<String,Double>>>();
        DateParameter validatedEndDate      = new DateParameter(reportType.toEndDate());
        HashMap<Integer,Double> thresholdMap= new HashMap<Integer,Double>();

        try {

            stmt                            = conn.prepareStatement(selectLocation);
            stmt.setInt(1, value);
            if(region> 0) {
                stmt.setInt(2, region);
            }
            rsLocation                      = stmt.executeQuery();
            while (rsLocation.next()) {

                int locationId              = rsLocation.getInt(1);
                thresholdMap.put(locationId, rsLocation.getDouble(2));

                String startDate            = "", endDate = "";

                stmt                        = conn.prepareStatement(selectEventString);
                stmt.setInt(1, locationId);
                stmt.setString(2, validatedEndDate.toString());
                if (reportType == ReportType.YTD) {  stmt.setString(3, validatedEndDate.toString()); }
                rs                          = stmt.executeQuery();
                while (rs.next()) {
                    eventString             += ", " + rs.getString(1);
                    eventArray.add(rs.getString(2));
                    if (rs.isFirst()) { endDate = rs.getString(3); }
                    if (rs.isLast()) { startDate = rs.getString(3); }
                }

                String selectPoured         = "SELECT eOHS.date, CONCAT(" + Grouping + "'|', b.location), IFNULL(SUM(eOHS.value),0.0), b.id FROM eventOpenHoursSummary eOHS " +
                                            " LEFT JOIN station st on st.id = eOHS.station LEFT JOIN bar b ON b.id = st.bar " +
                                            " WHERE eOHS.event IN ( " + eventString + " ) GROUP BY eOHS.date, b.id ORDER BY eOHS.date, b.id";
                String selectSold           = "SELECT eOHSS.date, CONCAT(" + Grouping + "'|', b.location), IFNULL(SUM(eOHSS.value),0.0), b.id FROM eventOpenHoursSoldSummary eOHSS " +
                                            " LEFT JOIN station st on st.id = eOHSS.station LEFT JOIN bar b ON b.id = st.bar " +
                                            " WHERE eOHSS.event IN ( " + eventString + " ) GROUP BY eOHSS.date, b.id ORDER BY eOHSS.date, b.id";

                String selectCategoryPoured = "SELECT c.name, IFNULL(SUM(eOHS.value),0.0) FROM eventOpenHoursSummary eOHS " +
                                            " LEFT JOIN product p on eOHS.product = p.id LEFT JOIN category c ON c.id = p.category " +
                                            " WHERE p.pType=1 and eOHS.event IN ( " + eventString + " ) GROUP BY c.id ORDER BY c.name ";
                String selectCategorySold   = "SELECT c.name, IFNULL(SUM(eOHSS.value),0.0) FROM eventOpenHoursSoldSummary eOHSS " +
                                            " LEFT JOIN product p on eOHSS.product = p.id LEFT JOIN category c ON c.id = p.category " +
                                            " WHERE p.pType=1 and eOHSS.event IN ( " + eventString + " ) GROUP BY c.id ORDER BY c.name ";
                
                String selectExclusion      = "SELECT eS.date, eS.bar FROM exclusionSummary eS LEFT JOIN location l ON l.id = eS.location WHERE l.id = ? " +
                                            " AND eS.date BETWEEN ? AND ADDDATE(?, INTERVAL 1 DAY) ORDER BY eS.date, eS.bar ";

                ArrayList<HashMap<String,Double>> pouredSoldArray
                                            = new ArrayList<HashMap<String,Double>>();
                HashMap<String,Double> pouredMap
                                            = new HashMap<String,Double>();
                HashMap<String,Double> soldMap
                                            = new HashMap<String,Double>();
                HashMap<String,Double> pouredCategoryMap
                                            = new HashMap<String,Double>();
                HashMap<String,Double> soldCategoryMap
                                            = new HashMap<String,Double>();

                ArrayList<Integer> barArray = new ArrayList<Integer>();
                HashMap<String,ArrayList<Integer>> exclusionMap
                                            = new HashMap<String,ArrayList<Integer>>();

                int count                       = 1;
                stmt                        = conn.prepareStatement(selectExclusion);
                stmt.setInt(count++, locationId);
                stmt.setString(count++, startDate);
                stmt.setString(count++, endDate);
                rs                          = stmt.executeQuery();
                while (rs.next()) {
                    barArray                = new ArrayList<Integer>();
                    String date             = rs.getString(1);
                    if (exclusionMap.containsKey(date)) {
                        barArray            = exclusionMap.get(date);
                    }
                    barArray.add(rs.getInt(2));
                    exclusionMap.put(date, barArray);
                }
                
                stmt                        = conn.prepareStatement(selectPoured);
                rs                          = stmt.executeQuery();
                while (rs.next()) {
                    String date             = rs.getString(1);
                    String locationString   = rs.getString(2);
                    double ounces           = rs.getDouble(3);
                    boolean include         = true;
                    if (exclusionMap.containsKey(date)) {
                        barArray            = exclusionMap.get(date);
                        if (barArray.contains(rs.getInt(4))) {
                            include         = false;
                        }
                    }
                    if (include) {
                        if (pouredMap.containsKey(locationString)) {
                            ounces          += pouredMap.get(locationString);
                        }
                        pouredMap.put(locationString, ounces);
                    }
                }
                
                stmt                        = conn.prepareStatement(selectSold);
                rs                          = stmt.executeQuery();
                while (rs.next()) {
                    String date             = rs.getString(1);
                    String locationString   = rs.getString(2);
                    double ounces           = rs.getDouble(3);
                    boolean include         = true;
                    if (exclusionMap.containsKey(date)) {
                        barArray            = exclusionMap.get(date);
                        if (barArray.contains(rs.getInt(4))) {
                            include         = false;
                        }
                    }
                    if (include) {
                        if (soldMap.containsKey(locationString)) {
                            ounces          += soldMap.get(locationString);
                        }
                        soldMap.put(locationString, ounces);
                    }
                }

                stmt                        = conn.prepareStatement(selectCategoryPoured);
                rs                          = stmt.executeQuery();
                while (rs.next()) {
                    pouredCategoryMap.put(rs.getString(1), rs.getDouble(2));
                }

                stmt                        = conn.prepareStatement(selectCategorySold);
                rs                          = stmt.executeQuery();
                while (rs.next()) {
                    soldCategoryMap.put(rs.getString(1), rs.getDouble(2));
                }
                
                pouredSoldArray.add(pouredMap);
                pouredSoldArray.add(soldMap);
                pouredSoldArray.add(pouredCategoryMap);
                pouredSoldArray.add(soldCategoryMap);
                dataMap.put(locationId, pouredSoldArray);
            }

            for (Integer j : thresholdMap.keySet()) {
                Element thresold            = varData.addElement("threshold");
                thresold.addAttribute("location", String.valueOf(j));
                thresold.addAttribute("value", HandlerUtils.nullToString(String.valueOf(thresholdMap.get(j)), "0"));
            }

            for (Integer i : dataMap.keySet()) {
                ArrayList<HashMap<String,Double>> pourSold
                                            = dataMap.get(i);
                HashMap<String,Double> pouredMap
                                            = pourSold.get(0);
                HashMap<String,Double> soldMap
                                            = pourSold.get(1);
                HashMap<String,Double> pouredCategoryMap
                                            = pourSold.get(2);
                HashMap<String,Double> soldCategoryMap
                                            = pourSold.get(3);
                String sold                 = "", soldCategory = "";
                for (String j : pouredMap.keySet()) {
                    sold                    = String.valueOf(soldMap.get(j));
                    Element var             = varData.addElement("data");
                    var.addAttribute("id", String.valueOf(j).split("\\|")[1]);
                    var.addAttribute("name", String.valueOf(j).split("\\|")[0]);
                    var.addAttribute("poured", HandlerUtils.nullToString(String.valueOf(pouredMap.get(j)), "0"));
                    var.addAttribute("sold", sold.equalsIgnoreCase("null") ? "0" : sold);
                    soldMap.remove(j);
                }
                for (String j : soldMap.keySet()) {
                    Element var             = varData.addElement("data");
                    var.addAttribute("id", String.valueOf(j).split("\\|")[1]);
                    var.addAttribute("name", String.valueOf(j).split("\\|")[0]);
                    var.addAttribute("poured", "0");
                    var.addAttribute("sold", HandlerUtils.nullToString(String.valueOf(soldMap.get(j)), "0"));
                }
                for (String j : pouredCategoryMap.keySet()) {
                    soldCategory            = String.valueOf(soldCategoryMap.get(j));
                    Element var             = varData.addElement("category");
                    var.addAttribute("name", String.valueOf(j));
                    var.addAttribute("poured", HandlerUtils.nullToString(String.valueOf(pouredCategoryMap.get(j)), "0"));
                    var.addAttribute("sold", soldCategory.equalsIgnoreCase("null") ? "0" : soldCategory);
                    soldCategoryMap.remove(j);
                }
                for (String j : soldCategoryMap.keySet()) {
                    Element var             = varData.addElement("category");
                    var.addAttribute("name", String.valueOf(j));
                    var.addAttribute("poured", "0");
                    var.addAttribute("sold", HandlerUtils.nullToString(String.valueOf(soldCategoryMap.get(j)), "0"));
                }
                for (i=0; i<eventArray.size(); i++)
                {
                    Element event           = varData.addElement("events");
                    event.addAttribute("name", HandlerUtils.nullToString(eventArray.get(i), "0"));
                }
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error in getDashBoardConcessionVariance: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(rsLocation);
            close(rs);
            close(stmt);
        }
    }


    private void getDashBoardVariance(int parentLevel, int value, ReportType reportType, int region,Element varData) throws HandlerException {

        String selectLocation               = "Select l.id, MAX(l.varianceAlert) FROM location l ";
        String Grouping                     = "";
        String  selectProdName              = "p.name, ";
        boolean isBrasstap                  = checkBrasstapLocation(parentLevel, value,conn);
        switch(parentLevel) {
            case 1:
                Grouping                    += "p.name, ";
                 selectProdName              = "p.name, ";
                selectLocation              += " WHERE l.id = ? ";
                if(isBrasstap) {
                    selectProdName          =" IFNULL((SELECT name FROM brasstapProducts WHERE usbnId=p.id LIMIT 1),p.name),";
                }
                break;
            case 2:
                Grouping                    += "l.name, ";
                 selectProdName              = "l.name, ";
                selectLocation              += " WHERE l.customer = ? ";
                if(region > 0){
                    selectLocation          += " AND l.region = ? ";
                }               
                break;
            case 3:
                Grouping                    += "l.name, ";
                selectProdName              = "l.name, ";
                selectLocation              += " LEFT JOIN userMap uM ON uM.location = l.id WHERE uM.user = ? ";
                break;
            default:
                break;
        }
        selectLocation                      += " GROUP BY l.id ";

        String selectPoured                 = "SELECT CONCAT(" + selectProdName + "'|', l.id), SUM(oHS.value) FROM openHoursSummary oHS LEFT JOIN product p on oHS.product = p.id " +
                                            " LEFT JOIN location l ON l.id = oHS.location WHERE p.pType=1 and oHS.date BETWEEN ? AND ? " +
                                            " AND l.id = ? GROUP BY " + Grouping.replace(",", "") + " ORDER BY " + Grouping.replace(",", "");
        String selectSold                   = "SELECT CONCAT(" + selectProdName + "'|', l.id), SUM(oHSS.value) FROM openHoursSoldSummary oHSS LEFT JOIN product p on oHSS.product = p.id " +
                                            " LEFT JOIN location l ON l.id = oHSS.location WHERE p.pType=1 and oHSS.date BETWEEN ? AND ? " +
                                            " AND l.id = ? GROUP BY " + Grouping.replace(",", "") + " ORDER BY " + Grouping.replace(",", "");

        PreparedStatement stmt              = null;
        ResultSet rs                        = null, rsLocation = null;

        HashMap<Integer,ArrayList<HashMap<String,Double>>> dataMap
                                            = new HashMap<Integer,ArrayList<HashMap<String,Double>>>();
        DateParameter validatedStartDate    = new DateParameter(reportType.toStartDate());
        DateParameter validatedEndDate      = new DateParameter(reportType.toEndDate());
        HashMap<Integer,Double> thresholdMap= new HashMap<Integer,Double>();

        try {
            stmt                            = conn.prepareStatement(selectLocation);
            stmt.setInt(1, value);
            if(region >0){
                stmt.setInt(2, region);
            }
            rsLocation                      = stmt.executeQuery();
            while (rsLocation.next()) {
                ArrayList<HashMap<String,Double>> pouredSoldArray
                                            = new ArrayList<HashMap<String,Double>>();
                HashMap<String,Double> pouredMap
                                            = new HashMap<String,Double>();
                HashMap<String,Double> soldMap
                                            = new HashMap<String,Double>();

                int locationId              = rsLocation.getInt(1);
                thresholdMap.put(locationId, rsLocation.getDouble(2));

                stmt                        = conn.prepareStatement(selectPoured);
                stmt.setString(1, validatedStartDate.toString());
                stmt.setString(2, validatedEndDate.toString());
                stmt.setInt(3, locationId);
                rs                          = stmt.executeQuery();
                while (rs.next()) {
                    pouredMap.put(rs.getString(1), rs.getDouble(2));
                }

                stmt                        = conn.prepareStatement(selectSold);
                stmt.setString(1, validatedStartDate.toString());
                stmt.setString(2, validatedEndDate.toString());
                stmt.setInt(3, locationId);
                rs                          = stmt.executeQuery();
                while (rs.next()) {
                    soldMap.put(rs.getString(1), rs.getDouble(2));
                }
                pouredSoldArray.add(pouredMap);
                pouredSoldArray.add(soldMap);
                dataMap.put(locationId, pouredSoldArray);
            }

            for (Integer j : thresholdMap.keySet()) {
                Element thresold            = varData.addElement("threshold");
                thresold.addAttribute("location", String.valueOf(j));
                thresold.addAttribute("value", HandlerUtils.nullToString(String.valueOf(thresholdMap.get(j)), "0"));
            }

            for (Integer i : dataMap.keySet()) {
                ArrayList<HashMap<String,Double>> pourSold
                                            = dataMap.get(i);
                HashMap<String,Double> pouredMap
                                            = pourSold.get(0);
                HashMap<String,Double> soldMap
                                            = pourSold.get(1);
                String sold                 = "";
                for (String j : pouredMap.keySet()) {
                    sold                    = String.valueOf(soldMap.get(j));
                    Element var             = varData.addElement("data");
                    var.addAttribute("id", String.valueOf(j).split("\\|")[1]);
                    var.addAttribute("name", String.valueOf(j).split("\\|")[0]);
                    var.addAttribute("poured", HandlerUtils.nullToString(String.valueOf(pouredMap.get(j)), "0"));
                    var.addAttribute("sold", sold.equalsIgnoreCase("null") ? "0" : sold);
                    soldMap.remove(j);
                }
                for (String j : soldMap.keySet()) {
                    Element var             = varData.addElement("data");
                    var.addAttribute("id", String.valueOf(j).split("\\|")[1]);
                    var.addAttribute("name", String.valueOf(j).split("\\|")[0]);
                    var.addAttribute("poured", "0");
                    var.addAttribute("sold", HandlerUtils.nullToString(String.valueOf(soldMap.get(j)), "0"));
                }
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error in getDashBoardVariance: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(rsLocation);
            close(rs);
            close(stmt);
        }
    }

    private void getDashBoardCategory(int parentLevel, int value, ReportType reportType,int region, Element catData) throws HandlerException {

        String selectLocation               = "Select l.id FROM location l ";
        switch(parentLevel) {
            case 1:
                selectLocation              += " WHERE l.id = ?";
                break;
            case 2:
                selectLocation              += " WHERE l.customer = ?";
                if(region > 0) {
                    selectLocation          += " AND l.region = "+String.valueOf(region);
                }
                break;
            case 3:
                selectLocation              += " LEFT JOIN userMap uM ON uM.location = l.id WHERE uM.user = ?";
                break;
            default:
                break;
        }

        String selectPoured                 = "Select c.name, SUM(ps.value) FROM openHoursSummary ps LEFT JOIN product p on ps.product = p.id " +
                                            " LEFT JOIN category c ON c.id = p.category LEFT JOIN location l ON l.id = ps.location " +
                                            " WHERE p.pType=1 and ps.date BETWEEN ? AND ? AND l.id = ? GROUP BY c.id ORDER BY c.name ";
        String selectSold                   = "Select c.name, SUM(ss.value) FROM openHoursSoldSummary ss LEFT JOIN product p on ss.product = p.id " +
                                            " LEFT JOIN category c ON c.id = p.category LEFT JOIN location l ON l.id = ss.location " +
                                            " WHERE p.pType=1 and ss.date BETWEEN ? AND ? AND l.id = ? GROUP BY c.id ORDER BY c.name ";

        PreparedStatement stmt              = null;
        ResultSet rs                        = null, rsLocation = null;

        int locationId                      = 0;
        HashMap<Integer,ArrayList<HashMap<String,Double>>> dataMap
                                            = new HashMap<Integer,ArrayList<HashMap<String,Double>>>();
        DateParameter validatedStartDate    = new DateParameter(reportType.toStartDate());
        DateParameter validatedEndDate      = new DateParameter(reportType.toEndDate());

        try {
            stmt                            = conn.prepareStatement(selectLocation);
            stmt.setInt(1, value);
            rsLocation                      = stmt.executeQuery();
            while (rsLocation.next()) {
                ArrayList<HashMap<String,Double>> pouredSoldArray
                                            = new ArrayList<HashMap<String,Double>>();
                HashMap<String,Double> pouredMap
                                            = new HashMap<String,Double>();
                HashMap<String,Double> soldMap
                                            = new HashMap<String,Double>();
                locationId                  = rsLocation.getInt(1);

                stmt                        = conn.prepareStatement(selectPoured);
                stmt.setString(1, validatedStartDate.toString());
                stmt.setString(2, validatedEndDate.toString());
                stmt.setInt(3, locationId);
                rs                          = stmt.executeQuery();
                while (rs.next()) {
                    pouredMap.put(rs.getString(1), rs.getDouble(2));
                }

                stmt                        = conn.prepareStatement(selectSold);
                stmt.setString(1, validatedStartDate.toString());
                stmt.setString(2, validatedEndDate.toString());
                stmt.setInt(3, locationId);
                rs                          = stmt.executeQuery();
                while (rs.next()) {
                    soldMap.put(rs.getString(1), rs.getDouble(2));
                }
                pouredSoldArray.add(pouredMap);
                pouredSoldArray.add(soldMap);
                dataMap.put(locationId, pouredSoldArray);
            }

            for (Integer i : dataMap.keySet()) {
                ArrayList<HashMap<String,Double>> pourSold
                                            = dataMap.get(i);
                HashMap<String,Double> pouredMap
                                            = pourSold.get(0);
                HashMap<String,Double> soldMap
                                            = pourSold.get(1);
                String sold                 = "";
                for (String j : pouredMap.keySet()) {
                    sold                    = String.valueOf(soldMap.get(j));
                    Element var             = catData.addElement("category");
                    var.addAttribute("name", String.valueOf(j));
                    var.addAttribute("poured", HandlerUtils.nullToString(String.valueOf(pouredMap.get(j)), "0"));
                    var.addAttribute("sold", sold.equalsIgnoreCase("null") ? "0" : sold);
                    soldMap.remove(j);
                }
                for (String j : soldMap.keySet()) {
                    Element var             = catData.addElement("category");
                    var.addAttribute("name", String.valueOf(j));
                    var.addAttribute("poured", "0");
                    var.addAttribute("sold", HandlerUtils.nullToString(String.valueOf(soldMap.get(j)), "0"));
                }
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error in getDashBoardCategory: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(rsLocation);
            close(rs);
            close(stmt);
        }
    }

    private void getDashBoardPerformance(int parentLevel, int value, ReportType reportType,int region, Element varData) throws HandlerException {

        String selectLocation = "Select l.id FROM location l ";
        String  selectProdName              = "p.name, ";
        boolean isBrasstap                  = checkBrasstapLocation(parentLevel, value,conn);
        switch(parentLevel) {
            case 1:
                selectLocation += " WHERE l.id = ?";
               
                if(isBrasstap) {
                    selectProdName          =" IFNULL((SELECT name FROM brasstapProducts WHERE usbnId=p.id LIMIT 1),p.name),";
                }
                break;
            case 2:
                selectLocation += " WHERE l.customer = ?";
                 if(isBrasstap) {
                    selectProdName          =" IFNULL((SELECT name FROM brasstapProducts WHERE usbnId=p.id LIMIT 1),p.name),";
                }
                if(region >0){
                    selectLocation          += " AND l.region = "+String.valueOf(region);
                }
                break;
            case 3:
                selectLocation += " LEFT JOIN userMap uM ON uM.location = l.id WHERE uM.user = ?";
                break;
            default:
                break;
        }

        String selectPoured = "Select "+selectProdName+" SUM(ps.value) FROM openHoursSummary ps LEFT JOIN product p on ps.product = p.id " +
                " LEFT JOIN location l ON l.id = ps.location WHERE p.pType=1 and ps.date BETWEEN ? AND ? " +
                " AND l.id = ? GROUP BY p.id ORDER BY p.name ";

        PreparedStatement stmt = null;
        ResultSet rs = null, rsLocation = null;

        DateParameter validatedStartDate = new DateParameter(reportType.toStartDate());
        DateParameter validatedEndDate = new DateParameter(reportType.toEndDate());
        
        try {
            stmt = conn.prepareStatement(selectLocation);
            stmt.setInt(1, value);
            rsLocation = stmt.executeQuery();
            while (rsLocation.next()) {
                stmt = conn.prepareStatement(selectPoured);
                stmt.setString(1, validatedStartDate.toString());
                stmt.setString(2, validatedEndDate.toString());
                stmt.setInt(3, rsLocation.getInt(1));
                rs = stmt.executeQuery();
                while (rs.next()) {
                    Element var = varData.addElement("product");
                    var.addAttribute("name", HandlerUtils.nullToString(String.valueOf(rs.getString(1)), "Unknown"));
                    var.addAttribute("poured", HandlerUtils.nullToString(String.valueOf(rs.getDouble(2)), "0"));
                }
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error in getDashBoardVariance: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(rsLocation);
            close(rs);
            close(stmt);
        }
    }

    private void getDashBoardAfterHours(int parentLevel, int value, ReportType reportType,int region, Element afterHoursData) throws HandlerException {

        String selectLocation = "Select l.id FROM location l ";
        String Grouping = "";
        switch(parentLevel) {
            case 1:
                Grouping += "REPLACE(aHS.date,'-','.'), ";
                selectLocation += " WHERE l.id = ?";
                break;
            case 2:
                Grouping += "CONCAT(l.name,' (',REPLACE(aHS.date,'-','.'), ')'), ";
                selectLocation += " WHERE l.customer = ?";
                if(region > 0){
                    selectLocation              += " AND l.region = "+String.valueOf(region);
                }
                break;
            case 3:
                Grouping += "CONCAT(l.name,' (',REPLACE(aHS.date,'-','.'), ')'), ";
                selectLocation += " LEFT JOIN userMap uM ON uM.location = l.id WHERE uM.user = ?";
                break;
            default:
                break;
        }

        String selectPoured = "Select " + Grouping + " SUM(aHS.value) FROM afterHoursSummary aHS LEFT JOIN product p on aHS.product = p.id " +
                " LEFT JOIN location l ON l.id = aHS.location WHERE p.pType=1 and aHS.date BETWEEN ? AND ? " +
                " AND l.id = ? GROUP BY l.name, aHS.date ORDER BY l.name, aHS.date";
        String selectSold = "Select " + Grouping + " SUM(aHS.value) FROM afterHoursSoldSummary aHS LEFT JOIN product p on aHS.product = p.id " +
                " LEFT JOIN location l ON l.id = aHS.location WHERE p.pType=1 and aHS.date BETWEEN ? AND ? " +
                " AND l.id = ? GROUP BY l.name, aHS.date ORDER BY l.name, aHS.date";

        PreparedStatement stmt = null;
        ResultSet rs = null, rsLocation = null;

        int locationId = 0;
        HashMap<Integer,ArrayList<HashMap<String,Double>>> dataMap = new HashMap<Integer,ArrayList<HashMap<String,Double>>>();
        DateParameter validatedStartDate = new DateParameter(reportType.toStartDate());
        DateParameter validatedEndDate = new DateParameter(reportType.toEndDate());

        try {
            stmt = conn.prepareStatement(selectLocation);
            stmt.setInt(1, value);
            rsLocation = stmt.executeQuery();
            while (rsLocation.next()) {
                ArrayList<HashMap<String,Double>> pouredSoldArray = new ArrayList<HashMap<String,Double>>();
                HashMap<String,Double> pouredMap = new HashMap<String,Double>();
                HashMap<String,Double> soldMap = new HashMap<String,Double>();

                locationId = rsLocation.getInt(1);

                stmt = conn.prepareStatement(selectPoured);
                stmt.setString(1, validatedStartDate.toString());
                stmt.setString(2, validatedEndDate.toString());
                stmt.setInt(3, locationId);
                rs = stmt.executeQuery();
                while (rs.next()) {
                    pouredMap.put(rs.getString(1), rs.getDouble(2));
                }

                stmt = conn.prepareStatement(selectSold);
                stmt.setString(1, validatedStartDate.toString());
                stmt.setString(2, validatedEndDate.toString());
                stmt.setInt(3, locationId);
                rs = stmt.executeQuery();
                while (rs.next()) {
                    soldMap.put(rs.getString(1), rs.getDouble(2));
                }
                pouredSoldArray.add(pouredMap);
                pouredSoldArray.add(soldMap);
                dataMap.put(locationId, pouredSoldArray);
            }

            for (Integer i : dataMap.keySet()) {
                ArrayList<HashMap<String,Double>> pourSold = dataMap.get(i);
                HashMap<String,Double> pouredMap = pourSold.get(0);
                HashMap<String,Double> soldMap = pourSold.get(1);
                String sold = "";
                for (String j : pouredMap.keySet()) {
                    sold = String.valueOf(soldMap.get(j));
                    Element var = afterHoursData.addElement("afterHours");
                    var.addAttribute("name", String.valueOf(j));
                    var.addAttribute("poured", HandlerUtils.nullToString(String.valueOf(pouredMap.get(j)), "0"));
                    var.addAttribute("sold", sold.equalsIgnoreCase("null") ? "0" : sold);
                    soldMap.remove(j);
                }
                for (String j : soldMap.keySet()) {
                    Element var = afterHoursData.addElement("afterHours");
                    var.addAttribute("name", String.valueOf(j));
                    var.addAttribute("poured", "0");
                    var.addAttribute("sold", HandlerUtils.nullToString(String.valueOf(soldMap.get(j)), "0"));
                }
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error in getDashBoardAfterHours: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(rsLocation);
            close(rs);
            close(stmt);
        }
    }

    private void getDashBoardInventory(int parentLevel, int value, ReportType reportType,int region, Element varData) throws HandlerException {

        String selectLocation = "Select l.id FROM location l ";
        String selectProdName = "";
        boolean isBrasstap                  = checkBrasstapLocation(parentLevel, value, conn);
        switch(parentLevel) {
            case 1:
                selectProdName = "p.name, ";
                if(isBrasstap) {
                    selectProdName          =" IFNULL((SELECT name FROM brasstapProducts WHERE usbnId=p.id LIMIT 1),p.name),";
                }
                selectLocation += " WHERE l.id = ?";
                break;
            case 2:
                selectProdName = "CONCAT(SUBSTRING(l.name,1,10),'-',SUBSTRING(p.name,1,10)), ";
                selectLocation += " WHERE l.customer = ?";
                if(region > 0){
                    selectLocation              += " AND l.region = "+String.valueOf(region);
                }
                break;
            case 3:
                selectProdName = "CONCAT(SUBSTRING(l.name,1,10),'-',SUBSTRING(p.name,1,10)), ";
                selectLocation += " LEFT JOIN userMap uM ON uM.location = l.id WHERE uM.user = ?";
                break;
            default:
                break;
        }
        String selectCurrentLines = "SELECT l.product FROM line l LEFT JOIN bar b ON b.id = l.bar " +
                " WHERE b.location = ? AND l.status = 'RUNNING'";

        String selectInventory = "SELECT p.id, " + selectProdName + " IF(i.qtyOnHand < 0, 0.0, i.qtyOnHand) FROM inventory i " +
                " LEFT JOIN location l ON l.id = i.location LEFT JOIN product p ON p.id = i.product " +
                " WHERE i.qtyOnHand <= i.minimumQty AND p.pType = ? AND l.id = ? ORDER BY i.qtyOnHand ";

        PreparedStatement stmt = null;
        ResultSet rs = null, rsLocation = null;

        try {
            stmt = conn.prepareStatement(selectLocation);
            stmt.setInt(1, value);
            rsLocation = stmt.executeQuery();
            while (rsLocation.next()) {
                ArrayList<Integer> curProduct = new ArrayList<Integer>();
                stmt = conn.prepareStatement(selectCurrentLines);
                stmt.setInt(1, rsLocation.getInt(1));
                rs = stmt.executeQuery();
                while (rs.next()) {
                    curProduct.add(rs.getInt(1));
                }

                stmt = conn.prepareStatement(selectInventory);
                stmt.setInt(1, 1);
                stmt.setInt(2, rsLocation.getInt(1));
                rs = stmt.executeQuery();
                while (rs.next()) {
                    if (curProduct.contains(rs.getInt(1))) {
                        Element var = varData.addElement("product");
                        var.addAttribute("name", HandlerUtils.nullToString(String.valueOf(rs.getString(2)), "Unknown"));
                        var.addAttribute("qtyOnHand", HandlerUtils.nullToString(String.valueOf(rs.getDouble(3)), "0"));
                    }
                }
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error in getDashBoardVariance: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(rsLocation);
            close(rs);
            close(stmt);
        }
    }

    private void getDashBoardTemperature(int parentLevel, int value, ReportType reportType,int region, Element tempData) throws HandlerException {

        String selectCooler = "Select c.id, l.easternOffset FROM cooler c LEFT JOIN location l ON l.id = c.location ";
        switch(parentLevel) {
            case 1:
                selectCooler += " WHERE l.id = ?";
                break;
            case 2:
                selectCooler += " WHERE l.customer = ? ";
                if(region >0){
                    selectCooler += " AND l.region = ? ";
                }
                selectCooler                +=" ORDER BY c.id";
                break;
            case 3:
                selectCooler += " LEFT JOIN userMap uM ON uM.location = l.id WHERE uM.user = ? ORDER BY c.id";
                break;
            default:
                break;
        }
        String selectCoolerTemperature = "SELECT ROUND(cT.value,2) FROM coolerTemperature cT WHERE cT.cooler = ? ORDER BY cT.date DESC LIMIT 1; ";
        String selectCoolerAverage = "SELECT c.id, SUBSTRING(c.name,1,15), ROUND(AVG(cT.value),2), c.alertPoint FROM coolerTemperature cT LEFT JOIN cooler c ON c.id = cT.cooler " +
                " WHERE cT.date BETWEEN ? AND ? AND cT.cooler = ? GROUP BY cT.cooler; ";
        String selectCoolerExceptions = "SELECT value, ADDDATE(start, INTERVAL ? HOUR), ADDDATE(end, INTERVAL ? HOUR) FROM coolerException WHERE start BETWEEN ? AND ? AND cooler = ? ORDER BY start; ";
        PreparedStatement stmt = null;
        ResultSet rs = null, rsCooler = null, rsCoolerTemp = null;

        DateParameter validatedStartDate = new DateParameter(reportType.toStartDate());
        DateParameter validatedEndDate = new DateParameter(reportType.toEndDate());
        try {
            stmt = conn.prepareStatement(selectCooler);
            stmt.setInt(1, value);
            if(region > 0) {
                stmt.setInt(2, region);
            }
            rsCooler = stmt.executeQuery();
            while (rsCooler.next()) {
                double easternOffset        = rsCooler.getDouble(2);
                stmt = conn.prepareStatement(selectCoolerAverage);
                stmt.setString(1, validatedStartDate.toString());
                stmt.setString(2, validatedEndDate.toString());
                stmt.setInt(3, rsCooler.getInt(1));
                rsCoolerTemp = stmt.executeQuery();
                while (rsCoolerTemp.next()) {
                    Element var = tempData.addElement("cooler");
                    stmt = conn.prepareStatement(selectCoolerTemperature);
                    stmt.setInt(1, rsCoolerTemp.getInt(1));
                    rs = stmt.executeQuery();
                    if (rs.next()) {
                        var.addAttribute("name", HandlerUtils.nullToString(String.valueOf(rsCoolerTemp.getString(2)), "Unknown"));
                        var.addAttribute("avg", HandlerUtils.nullToString(String.valueOf(rsCoolerTemp.getDouble(3)), "0"));
                        var.addAttribute("value", HandlerUtils.nullToString(String.valueOf(rs.getString(1)), "0"));
                        var.addAttribute("threshold", HandlerUtils.nullToString(String.valueOf(rsCoolerTemp.getDouble(4)), "0"));
                    }
                    stmt = conn.prepareStatement(selectCoolerExceptions);
                    stmt.setDouble(1, easternOffset);
                    stmt.setDouble(2, easternOffset);
                    stmt.setTimestamp(3, toSqlTimestamp(reportType.toStartDate()));
                    stmt.setTimestamp(4, toSqlTimestamp(reportType.toEndDate()));
                    stmt.setInt(5, rsCoolerTemp.getInt(1));
                    rs = stmt.executeQuery();
                    while (rs.next()) {
                        Element excep = var.addElement("exceptions");
                        excep.addAttribute("value", HandlerUtils.nullToString(String.valueOf(rs.getDouble(1)), "0"));
                        excep.addAttribute("start", HandlerUtils.nullToEmpty(rs.getString(2)));
                        excep.addAttribute("end", HandlerUtils.nullToEmpty(rs.getString(3)));
                    }
                }
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error in getDashBoardVariance: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(rsCooler);
            close(rsCoolerTemp);
            close(rs);
            close(stmt);
        }
    }

    private void getDashBoardVarianceChart(int parentLevel, int value, boolean concessions, ReportType reportType, PeriodType periodType,int region, Element varChartData) throws HandlerException {

        String selectLocation               = " ";
        switch(parentLevel) {
            case 0:
                selectLocation              += " WHERE b.id = ? ";
                break;
            case 1:
                selectLocation              += " WHERE l.id = ? ";
                break;
            case 2:
                selectLocation              += " WHERE l.customer = ? ";
                if(region > 0) {
                    selectLocation              += " AND l.region = "+String.valueOf(region);
                }
                break;
            case 3:
                selectLocation              += " LEFT JOIN userMap uM ON uM.location = l.id WHERE uM.user = ? ";
                break;
            default:
                selectLocation              += " WHERE l.id = ? ";
                break;
        }

        PreparedStatement stmt              = null;
        ResultSet rs                        = null;

        DateParameter validatedStartDate    = new DateParameter(reportType.toStartDate());
        DateParameter validatedEndDate      = new DateParameter(reportType.toEndDate());

        try {
            int count                       = 1;
            String selectThreshold          = "SELECT l.id, MAX(l.varianceAlert) FROM location l " + selectLocation + " GROUP BY l.id ";
            HashMap<Integer,Double> thresholdMap
                                            = new HashMap<Integer,Double>();
            stmt                            = conn.prepareStatement(selectThreshold);
            stmt.setInt(count++, value);
            rs                              = stmt.executeQuery();
            while (rs.next()) {
                thresholdMap.put(rs.getInt(1), rs.getDouble(2));
            }

            if (concessions) {
                String selectExclusion      = "SELECT eS.date, eS.bar FROM exclusionSummary eS LEFT JOIN location l ON l.id = eS.location " + selectLocation +
                                            " AND eS.date BETWEEN ? AND ? ORDER BY eS.date, eS.bar ";

                String selectPoured         = "SELECT CONCAT(eH.date, '|', l.id), SUM(eOHS.value), b.id FROM eventOpenHoursSummary eOHS LEFT JOIN eventHours eH ON eH.id = eOHS.event " +
                                            " LEFT JOIN station st on st.id = eOHS.station LEFT JOIN bar b ON b.id = st.bar LEFT JOIN location l ON l.id = b.location " +
                                            selectLocation + " AND eH.date BETWEEN ? AND ? GROUP BY eH.id, b.id ORDER BY eH.id DESC ";
                String selectSold           = "SELECT CONCAT(eH.date, '|', l.id), SUM(eOHSS.value), b.id FROM eventOpenHoursSoldSummary eOHSS LEFT JOIN eventHours eH ON eH.id = eOHSS.event " +
                                            " LEFT JOIN station st on st.id = eOHSS.station LEFT JOIN bar b ON b.id = st.bar LEFT JOIN location l ON l.id = eOHSS.location " +
                                            selectLocation + " AND eH.date BETWEEN ? AND ? GROUP BY eH.id, b.id ORDER BY eH.id DESC ";

                ArrayList<Integer> barArray = new ArrayList<Integer>();
                HashMap<String,ArrayList<Integer>> exclusionMap
                                            = new HashMap<String,ArrayList<Integer>>();

                count                       = 1;
                stmt                        = conn.prepareStatement(selectExclusion);
                stmt.setInt(count++, value);
                stmt.setString(count++, validatedStartDate.toString());
                stmt.setString(count++, validatedEndDate.toString());
                rs                          = stmt.executeQuery();
                while (rs.next()) {
                    barArray                = new ArrayList<Integer>();
                    String date             = rs.getString(1);
                    if (exclusionMap.containsKey(date)) {
                        barArray            = exclusionMap.get(date);
                    }
                    barArray.add(rs.getInt(2));
                    exclusionMap.put(date, barArray);
                }

                HashMap<Integer,ArrayList<HashMap<String,Double>>> dataMap
                                            = new HashMap<Integer,ArrayList<HashMap<String,Double>>>();
                ArrayList<HashMap<String,Double>> pouredSoldArray
                                            = new ArrayList<HashMap<String,Double>>();
                HashMap<String,Double> pouredMap
                                            = new HashMap<String,Double>();
                HashMap<String,Double> soldMap
                                            = new HashMap<String,Double>();
                count                       = 1;
                stmt                        = conn.prepareStatement(selectPoured);
                stmt.setInt(count++, value);
                stmt.setString(count++, validatedStartDate.toString());
                stmt.setString(count++, validatedEndDate.toString());
                rs                          = stmt.executeQuery();
                while (rs.next()) {
                    String date             = rs.getString(1);
                    double ounces           = rs.getDouble(2);
                    boolean include         = true;
                    if (exclusionMap.containsKey(date.split("\\|")[0])) {
                        barArray            = exclusionMap.get(date.split("\\|")[0]);
                        if (barArray.contains(rs.getInt(3))) {
                            include         = false;
                        }
                    }
                    if (include) {
                        if (pouredMap.containsKey(date)) {
                            ounces          += pouredMap.get(date);
                        }
                        pouredMap.put(date, ounces);
                    }
                }

                count                       = 1;
                stmt                        = conn.prepareStatement(selectSold);
                stmt.setInt(count++, value);
                stmt.setString(count++, validatedStartDate.toString());
                stmt.setString(count++, validatedEndDate.toString());
                rs                          = stmt.executeQuery();
                while (rs.next()) {
                    String date             = rs.getString(1);
                    double ounces           = rs.getDouble(2);
                    boolean include         = true;
                    if (exclusionMap.containsKey(date.split("\\|")[0])) {
                        barArray            = exclusionMap.get(date.split("\\|")[0]);
                        if (barArray.contains(rs.getInt(3))) {
                            include         = false;
                        }
                    }
                    if (include) {
                        if (soldMap.containsKey(date)) {
                            ounces          += soldMap.get(date);
                        }
                        soldMap.put(date, ounces);
                    }
                }
                pouredSoldArray.add(pouredMap);
                pouredSoldArray.add(soldMap);
                dataMap.put(value, pouredSoldArray);

                for (Integer i : dataMap.keySet()) {
                    ArrayList<HashMap<String,Double>> pourSold
                                            = dataMap.get(i);
                    pouredMap               = pourSold.get(0);
                    soldMap                 = pourSold.get(1);
                    String sold             = "";
                    for (String j : pouredMap.keySet()) {
                        sold                = String.valueOf(soldMap.get(j));
                        if (!sold.equalsIgnoreCase("null")) {
                            Element var     = varChartData.addElement("data");
                            var.addAttribute("id", String.valueOf(j).split("\\|")[1]);
                            var.addAttribute("date", String.valueOf(j).split("\\|")[0]);
                            var.addAttribute("poured", HandlerUtils.nullToString(String.valueOf(pouredMap.get(j)), "0"));
                            var.addAttribute("sold", sold);
                            soldMap.remove(j);
                        }
                    }
                }
            } else {
               if (periodType == PeriodType.WEEKLY) {
                    Date d                  = reportType.toDayOfWeek(reportType.toEndDate(),1);
                    validatedStartDate      = new DateParameter(reportType.addDays(d, 27));
                    validatedEndDate        = new DateParameter(d);
                }
                logger.debug(validatedStartDate.toString());
                logger.debug(validatedEndDate.toString());
                String selectData           = "SELECT location, date, SUM(poured), SUM(sold) FROM (SELECT t.location, l.name, t.date, t.poured, t.sold FROM tierSummary t " +
                                            " LEFT JOIN location l ON l.id = t.location " + selectLocation + " AND t.date BETWEEN ? AND ? AND t.tier < 4) AS t " +
                                            " GROUP BY " + (periodType == PeriodType.MONTHLY ? "MONTH(date)" : "date")  + ", location ORDER BY date, name; ";

                count                       = 1;
                stmt                        = conn.prepareStatement(selectData);
                stmt.setInt(count++, value);
                stmt.setString(count++, validatedStartDate.toString());
                stmt.setString(count++, validatedEndDate.toString());
                rs                          = stmt.executeQuery();
                while (rs.next()) {
                    Element var             = varChartData.addElement("data");
                    var.addAttribute("id", String.valueOf(rs.getInt(1)));
                    var.addAttribute("date", HandlerUtils.nullToString(rs.getString(2), "Unknown"));
                    var.addAttribute("poured", String.valueOf(rs.getDouble(3)));
                    var.addAttribute("sold", String.valueOf(rs.getDouble(4)));
                }
            }

            for (Integer j : thresholdMap.keySet()) {
                Element thresold        = varChartData.addElement("location");
                thresold.addAttribute("id", String.valueOf(j));
                thresold.addAttribute("threshold", HandlerUtils.nullToString(String.valueOf(thresholdMap.get(j)), "0"));
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error in getDashBoardVarianceChart: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(rs);
            close(stmt);
        }
    }


    private void getDashBoardKeyStats(int parentLevel, int value, ReportType reportType,int region, Element varChartData) throws HandlerException {

        String selectLocation               = " ";
        switch(parentLevel) {
            case 1:
                selectLocation              += " WHERE l.id = ?";
                break;
            case 2:
                selectLocation              += " WHERE l.customer = ?";
                if(region > 0){
                    selectLocation              += " AND l.region = "+String.valueOf(region);
                }
                break;
            case 3:
                selectLocation              += " LEFT JOIN userMap uM ON uM.location = l.id WHERE uM.user = ?";
                break;
            default:
                selectLocation              += " WHERE l.id = ?";
                break;
        }
        String selectLWData                 = "SELECT l.id, l.name, s.poured, s.saved, s.loss, s.variance, s.rank, l.customer FROM saveTheBeerSummary s " +
                                            " LEFT JOIN location l ON l.id = s.location " + selectLocation + " AND s.date = ?;";
        String selectLMData                 = "SELECT l.id, l.name, SUM(s.poured), SUM(s.saved), SUM(s.loss), AVG(s.variance) FROM saveTheBeerSummary s " +
                                            " LEFT JOIN location l ON l.id = s.location " + selectLocation + " AND s.date BETWEEN SUBDATE(?, INTERVAL 27 DAY) AND ? " +
                                            " GROUP BY l.id;";
        String selectLHData                 = "SELECT l.id, l.name, SUM(s.poured), SUM(s.saved), SUM(s.loss), AVG(s.variance) FROM saveTheBeerSummary s " +
                                            " LEFT JOIN location l ON l.id = s.location " + selectLocation + " AND s.date BETWEEN SUBDATE(?, INTERVAL 167 DAY) AND ? " +
                                            " GROUP BY l.id;";

        DateParameter validatedDate         = new DateParameter(reportType.toDayOfWeek(reportType.toEndDate(), 2));
        logger.debug("Key Stats for: " + validatedDate.toString());

        PreparedStatement stmt              = null;
        ResultSet rs                        = null, rsLocation = null;

        try {
            stmt                            = conn.prepareStatement(selectLWData);
            stmt.setInt(1, value);
            stmt.setString(2, validatedDate.toString());
            rs                              = stmt.executeQuery();
            if (rs.next()) {
                Element excep               = varChartData.addElement("lastWeek");
                excep.addAttribute("id", HandlerUtils.nullToEmpty(String.valueOf(rs.getInt(1))));
                excep.addAttribute("name", HandlerUtils.nullToEmpty(rs.getString(2)));
                excep.addAttribute("poured", HandlerUtils.nullToString(String.valueOf(rs.getDouble(3)), "0"));
                excep.addAttribute("saved", HandlerUtils.nullToString(String.valueOf(rs.getDouble(4)), "0"));
                excep.addAttribute("loss", HandlerUtils.nullToString(String.valueOf(rs.getDouble(5)), "0"));
                excep.addAttribute("var", HandlerUtils.nullToString(String.valueOf(rs.getDouble(6)), "0"));
                excep.addAttribute("tier", HandlerUtils.nullToEmpty(String.valueOf(getTier(rs.getDouble(6)))));
                excep.addAttribute("rank", HandlerUtils.nullToEmpty(String.valueOf((parentLevel == 1) ? rs.getInt(7) : 0)));
                excep.addAttribute("count", HandlerUtils.nullToEmpty(String.valueOf(getLocationCount(rs.getInt(8)))));
            }
            stmt                            = conn.prepareStatement(selectLMData);
            stmt.setInt(1, value);
            stmt.setString(2, validatedDate.toString());
            stmt.setString(3, validatedDate.toString());
            rs                              = stmt.executeQuery();
            if (rs.next()) {
                Element excep               = varChartData.addElement("lastMonth");
                excep.addAttribute("id", HandlerUtils.nullToEmpty(String.valueOf(rs.getInt(1))));
                excep.addAttribute("name", HandlerUtils.nullToEmpty(rs.getString(2)));
                excep.addAttribute("poured", HandlerUtils.nullToString(String.valueOf(rs.getDouble(3)), "0"));
                excep.addAttribute("saved", HandlerUtils.nullToString(String.valueOf(rs.getDouble(4)), "0"));
                excep.addAttribute("loss", HandlerUtils.nullToString(String.valueOf(rs.getDouble(5)), "0"));
                excep.addAttribute("var", HandlerUtils.nullToString(String.valueOf(rs.getDouble(6)), "0"));
                excep.addAttribute("tier", HandlerUtils.nullToEmpty(String.valueOf(getTier(rs.getDouble(6)))));
            }
            stmt                            = conn.prepareStatement(selectLHData);
            stmt.setInt(1, value);
            stmt.setString(2, validatedDate.toString());
            stmt.setString(3, validatedDate.toString());
            rs                              = stmt.executeQuery();
            if (rs.next()) {
                Element excep               = varChartData.addElement("lastHYear");
                excep.addAttribute("id", HandlerUtils.nullToEmpty(String.valueOf(rs.getInt(1))));
                excep.addAttribute("name", HandlerUtils.nullToEmpty(rs.getString(2)));
                excep.addAttribute("poured", HandlerUtils.nullToString(String.valueOf(rs.getDouble(3)), "0"));
                excep.addAttribute("saved", HandlerUtils.nullToString(String.valueOf(rs.getDouble(4)), "0"));
                excep.addAttribute("loss", HandlerUtils.nullToString(String.valueOf(rs.getDouble(5)), "0"));
                excep.addAttribute("var", HandlerUtils.nullToString(String.valueOf(rs.getDouble(6)), "0"));
                excep.addAttribute("tier", HandlerUtils.nullToEmpty(String.valueOf(getTier(rs.getDouble(6)))));
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error in getDashBoardKeyStats: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(rsLocation);
            close(rs);
            close(stmt);
        }
    }
    
    
    private void getDashBoardUnclaim(int parentLevel, int value, ReportType reportType, int region, Element varChartData) throws HandlerException {

        String selectLocation               = "", startDate = "", endDate = "";
        switch(parentLevel) {
            case 1:
                selectLocation              += " WHERE l.id = ?";
                break;
            case 3:
                selectLocation              += " LEFT JOIN userMap uM ON uM.location = l.id WHERE uM.user = ?";
                break;
            default:
                selectLocation              += " WHERE l.id = ?";
                break;
        }
        
        String selectUnclaimData            = "SELECT lH.product, lH.productName, SUM(lH.loss) FROM unclaimedReadingData lH LEFT JOIN location l ON lH.location = l.id " +
                                            selectLocation + " AND lH.color > 0 AND lH.date BETWEEN ? AND ? GROUP by lH.product ORDER BY lH.location, lH.productName;";
        String selectOpenHours              = "SELECT DATE_SUB(CONCAT(LEFT(SUBDATE(?, INTERVAL 1 DAY),11), ' ',IFNULL(x.open,'11:00:00')), INTERVAL eO HOUR) Open, " +
                                            " DATE_SUB(If(x.close>'12:0:0', CONCAT(LEFT(SUBDATE(?, INTERVAL 1 DAY),11), ' ', " +
                                            " IFNULL(x.close,'02:00:00')), CONCAT(LEFT(ADDDATE(SUBDATE(?, INTERVAL 1 DAY),1),11), ' ', " +
                                            " IFNULL(x.close,'02:00:00'))), INTERVAL eO HOUR) Close, eO " +
                                            " FROM (SELECT CASE DAYOFWEEK(SUBDATE(?, INTERVAL 1 DAY)) " +
                                            " WHEN 1 THEN Right(lH.openSun,8) " +
                                            " WHEN 2 THEN Right(lH.openMon,8) " +
                                            " WHEN 3 THEN Right(lH.openTue,8) " +
                                            " WHEN 4 THEN Right(lH.openWed,8) " +
                                            " WHEN 5 THEN Right(lH.openThu,8) " +
                                            " WHEN 6 THEN Right(lH.openFri,8) " +
                                            " WHEN 7 THEN Right(lH.openSat,8) END open, " +
                                            " CASE DAYOFWEEK(SUBDATE(?, INTERVAL 1 DAY)) " +
                                            " WHEN 1 THEN Right(lH.closeSun,8) " +
                                            " WHEN 2 THEN Right(lH.closeMon,8) " +
                                            " WHEN 3 THEN Right(lH.closeTue,8) " +
                                            " WHEN 4 THEN Right(lH.closeWed,8) " +
                                            " WHEN 5 THEN Right(lH.closeThu,8) " +
                                            " WHEN 6 THEN Right(lH.closeFri,8) " +
                                            " WHEN 7 THEN Right(lH.closeSat,8) END close, " +
                                            " l.easternOffset eO " +
                                            " FROM locationHours lH RIGHT JOIN location l ON lH.location = l.id " + selectLocation + ") AS x; ";

        PreparedStatement stmt              = null;
        ResultSet rs                        = null;

        try {
            DateParameter validatedDate     = new DateParameter(reportType.toEndDate());
            stmt                            = conn.prepareStatement(selectOpenHours);
            stmt.setString(1, validatedDate.toString().substring(0, 10));
            stmt.setString(2, validatedDate.toString().substring(0, 10));
            stmt.setString(3, validatedDate.toString().substring(0, 10));
            stmt.setString(4, validatedDate.toString().substring(0, 10));
            stmt.setString(5, validatedDate.toString().substring(0, 10));
            stmt.setInt(6, value);
            rs                              = stmt.executeQuery();
            if (rs.next()) {
                startDate                   = rs.getString(1);
                endDate                     = rs.getString(2);
            }

            stmt                            = conn.prepareStatement(selectUnclaimData);
            stmt.setInt(1, value);
            stmt.setString(2, startDate);
            stmt.setString(3, endDate);
            rs                              = stmt.executeQuery();
            while (rs.next()) {
                Element product             = varChartData.addElement("productLoss");
                product.addAttribute("id", HandlerUtils.nullToEmpty(String.valueOf(rs.getInt(1))));
                product.addAttribute("name", HandlerUtils.nullToEmpty(rs.getString(2)));
                product.addAttribute("loss", HandlerUtils.nullToString(String.valueOf(rs.getDouble(3)), "0"));
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error in getDashBoardKeyStats: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(rs);
            close(stmt);
        }
    }

    private void getDashBoardTier(ReportType reportType, Element tierData) throws HandlerException {

        String selectTierCount              = " SELECT t.tier, ROUND(SUM(t.cnt)/COUNT(t.date),0) FROM (SELECT tier, COUNT(id) AS cnt, date FROM tierSummary " +
                                            " WHERE tier < 4 AND date BETWEEN SUBDATE(?, INTERVAL 27 DAY) AND ? GROUP BY tier, date) AS t GROUP BY t.tier; ";

        PreparedStatement stmt              = null;
        ResultSet rs                        = null;
        DateParameter validatedDate         = new DateParameter(reportType.toDayOfWeek(reportType.toEndDate(), 2));

        try {
            Element tier                    = tierData.addElement("tier");
            double totalCount               = 0;
            stmt                            = conn.prepareStatement(selectTierCount);
            stmt.setString(1, validatedDate.toString());
            stmt.setString(2, validatedDate.toString());
            rs                              = stmt.executeQuery();
            while (rs.next()) {
                tier.addAttribute("tier" + rs.getInt(1)  + "Count", rs.getString(2));
                totalCount                  += rs.getDouble(2);
            }
            tier.addAttribute("totalCount", String.valueOf(totalCount));
        } catch (SQLException sqle) {
            logger.dbError("Database error in getDashBoardTier: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(rs);
            close(stmt);
        }
    }

    private int getTier(double var) {
        int tier                            = 0;
        if (var < -10) {
            tier                            = 3;
        } else if (var < -5) {
            tier                            = 2;
        } else if (var < 10) {
            tier                            = 1;
        }
        return tier;
    }

    private int getLocationCount(int customerId) throws HandlerException {
        int count                           = 1;
        String selectLocationCount          = " SELECT count(l.id) FROM location l LEFT JOIN locationDetails lD ON lD.location = l.id " +
                                            " WHERE lD.active = 1 AND l.customer = ? GROUP BY l.customer";
        PreparedStatement stmt              = null;
        ResultSet rs                        = null;

        try {
            stmt                            = conn.prepareStatement(selectLocationCount);
            stmt.setInt(1, customerId);
            rs                              = stmt.executeQuery();
            if (rs.next()) {
                count                       = rs.getInt(1);
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error in getLocationCount: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(rs);
            close(stmt);
        }
        return count;
    }

    /** Converts a java.util.Date to a java.sql.Date
     */
    private java.sql.Timestamp toSqlTimestamp(Date d) {
        return new java.sql.Timestamp(d.getTime());
    }

    private void getReport(Element toHandle, Element toAppend) throws HandlerException {

        Date start                          = getRequiredDate(toHandle, "startDate");
        Date end                            = getRequiredDate(toHandle, "endDate");
        String periodStr                    = HandlerUtils.getRequiredString(toHandle, "periodType");
        String periodDetail                 = HandlerUtils.getRequiredString(toHandle, "periodDetail");
        PeriodShiftType periodShift         = PeriodShiftType.instanceOf("EntireDay");
        String periodShiftString            = HandlerUtils.getOptionalString(toHandle, "periodShift");
        if (null != periodShiftString) {
            periodShift                     = PeriodShiftType.instanceOf(HandlerUtils.getOptionalString(toHandle, "periodShift"));
        }
        int line                            = HandlerUtils.getOptionalInteger(toHandle, "lineId");
        int station                         = HandlerUtils.getOptionalInteger(toHandle, "stationId");
        int bar                             = HandlerUtils.getOptionalInteger(toHandle, "barId");
        int zone                            = HandlerUtils.getOptionalInteger(toHandle, "zoneId");
        int regionId                        = HandlerUtils.getOptionalInteger(toHandle, "regionId");
        int location                        = HandlerUtils.getOptionalInteger(toHandle, "locationId");
        int customer                        = HandlerUtils.getOptionalInteger(toHandle, "customerId");
        int user                            = HandlerUtils.getOptionalInteger(toHandle, "userId");
        int btype                           = HandlerUtils.getOptionalInteger(toHandle, "type");
        int ptype                           = HandlerUtils.getOptionalInteger(toHandle, "ptype");
        String specificLocationsString      = HandlerUtils.getOptionalString(toHandle, "specificLocations");
        int parameter                       = 0;
        String conditionString              = "";


        logger.debug("PT:"+periodStr +"pD "+periodDetail+ "PS"+periodShiftString);

        //new: filter by a specific product
        int product                         = HandlerUtils.getOptionalInteger(toHandle, "specificProduct");

        String byLineString                 = HandlerUtils.getOptionalString(toHandle, "byLine");
        String byProductString              = HandlerUtils.getOptionalString(toHandle, "byProduct");
        boolean byLine                      = (!"false".equalsIgnoreCase(byLineString));
        boolean byProduct                   = (!"false".equalsIgnoreCase(byProductString));

        boolean lineCleaning                = HandlerUtils.getOptionalBoolean(toHandle, "lineCleaning");
        boolean forChart                    = HandlerUtils.getOptionalBoolean(toHandle, "forChart");

        int paramsSet                       = 0, paramLevel = 0;
        if (line >= 0) {
            paramLevel                      = 7;
            parameter                       = line;
            conditionString                 = " WHERE l.id = ? ";
            paramsSet++;
        }
        if (user >=0 && specificLocationsString != null && specificLocationsString.length() > 0) {
            paramLevel                      = 6;
            parameter                       = -1;
            conditionString                 = " WHERE b.location IN (" + specificLocationsString + ") ";
            start                           = setStartDate(periodShift.toSQLQueryInt(), 0, specificLocationsString, getRequiredDate(toHandle, "startDate"));
            end                             = setEndDate(periodShift.toSQLQueryInt(), 0, specificLocationsString, getRequiredDate(toHandle, "startDate"));
            periodDetail                    = String.valueOf(start.getHours() + 1);
            paramsSet++;
        }
        if (station >= 0) {
            paramLevel                      = 5;
            parameter                       = station;
            conditionString                 = " WHERE l.station = ? ";
            paramsSet++;
        }
        if (bar >= 0) {
            paramLevel                      = 4;
            parameter                       = bar;
            conditionString                 = " WHERE b.id = ? ";
            paramsSet++;
        }
        if (zone >= 0) {
            paramLevel                      = 3;
            parameter                       = zone;
            conditionString                 = " WHERE b.zone = ? ";
            paramsSet++;
        }
        if (location >= 0) {
            paramLevel                      = 2;
            parameter                       = location;
            conditionString                 = " WHERE b.location = ? ";
            paramsSet++;
        }
        if (customer >= 0) {
            paramLevel                      = 1;
            parameter                       = customer;
            conditionString                 = " WHERE loc.customer = ? ";
            if(regionId > 0){
                specificLocationsString     =getRegionLocations(customer,regionId);
            }
            start                           = setStartDate(periodShift.toSQLQueryInt(), customer, specificLocationsString, getRequiredDate(toHandle, "startDate"));
            end                             = setEndDate(periodShift.toSQLQueryInt(), customer, specificLocationsString, getRequiredDate(toHandle, "startDate"));
            periodDetail                    = String.valueOf(start.getHours() + 1);
            paramsSet++;
        }
        if (paramsSet != 1) {
            throw new HandlerException("Exactly one of the following must " +
                    "be set: lineId stationId barId zoneId locationId customerId userId");
        }

        PeriodType periodType               = PeriodType.parseString(periodStr);
        if (null == periodType) {
            throw new HandlerException("Invalid period type: " + periodStr);
        }
        ReportPeriod period                 = null, chartPeriod = null;
        try {
            period                          = new ReportPeriod(periodType, periodDetail, start, end);
        } catch (IllegalArgumentException e) {
            throw new HandlerException(e.getMessage());
        }

        SortedSet<DatePartition> dps        = DatePartitionFactory.createPartitions(period);
        //logger.debug("Created partitions: \n"+DatePartitionFactory.partitionReport(dps));
        DatePartitionTree dpt               = new DatePartitionTree(dps);


        ReportResults rrs                   = null, crrs = null;

        switch (paramLevel) {
            case 1:
                lineOffsetMap               = new LineOffsetMap(conn, periodShift.toSQLQueryInt(), customer, specificLocationsString, dateToString(getRequiredDate(toHandle, "startDate")));
                crrs                        = ReportResults.getResultsByCustomer(period, btype, byProduct, lineCleaning, customer, specificLocationsString, product, conn);
                break;
            case 2:
                rrs                         = ReportResults.getResultsByLocation(period, btype, byProduct, lineCleaning, location, product, conn);
                break;
            case 3:
                rrs                         = ReportResults.getResultsByZone(period, btype, lineCleaning, zone, product, conn);
                break;
            case 4:
                rrs                         = ReportResults.getResultsByBar(period, btype, lineCleaning, bar, product, conn);
                break;
            case 5:
                rrs                         = ReportResults.getResultsByStation(period, btype, lineCleaning, station, product, conn);
                break;
            case 6:
                lineOffsetMap               = new LineOffsetMap(conn, periodShift.toSQLQueryInt(), 0, specificLocationsString, dateToString(getRequiredDate(toHandle, "startDate")));
                crrs                        = ReportResults.getResultsBySpecificLocations(period, btype, byProduct, lineCleaning, specificLocationsString, product, conn);
                break;
            case 7:
                rrs                         = ReportResults.getResultsByLine(period, btype, lineCleaning, line, conn);
                break;
            default:
                break;
        }

        PeriodStructure pss[]               = null, chartPSS[] = null;
        PeriodStructure ps                  = null;
        DatePartitionTree chartDPT          = null;
        SortedSet<DatePartition> chartDPS   = null;
        HashMap<Integer, LinePeriod> linePeriods
                                            = new HashMap<Integer, LinePeriod>();
        int dpsSize                         = dps.size();
        int index;
        if (dpsSize > 0) {
            pss                             = new PeriodStructure[dpsSize];
            //this is a temporary array of the DatePartitions used to construct the PeriodStructures start dates
            Object[] dpa                    = dps.toArray();
            for (int i = 0; i < dpsSize; i++) {
                // create a new PeriodStructure and link it to the previous one (or null for the first)
                ps                          = new PeriodStructure(ps, ((DatePartition) dpa[i]).getDate());
                pss[i]                      = ps;
            }
            if (forChart) {
                try {
                    periodDetail            = "7";
                    if (customer >= 0 || user >= 0) {
                        periodDetail        = String.valueOf(start.getHours() + 1);
                    }
                    chartPeriod             = new ReportPeriod(PeriodType.DAILY, periodDetail, start, end);
                } catch (IllegalArgumentException e) {
                    throw new HandlerException(e.getMessage());
                }
                chartDPS                    = DatePartitionFactory.createPartitions(chartPeriod);
                //logger.debug("Created partitions: \n"+DatePartitionFactory.partitionReport(chartDPS));
                chartDPT                    = new DatePartitionTree(chartDPS);
                int chartDPSSize            = chartDPS.size();
                chartPSS                    = new PeriodStructure[chartDPSSize];
                //this is a temporary array of the DatePartitions used to construct the PeriodStructures start dates
                Object[] chartDPA           = chartDPS.toArray();
                for (int i = 0; i < chartDPSSize; i++) {
                    // create a new PeriodStructure and link it to the previous one (or null for the first)
                    ps                      = new PeriodStructure(ps, ((DatePartition) chartDPA[i]).getDate());
                    chartPSS[i]             = ps;
                }
            }
            int debugCounter                = 0;
            try {
                if (customer >= 0 || user >= 0) {
                    while (crrs.next()) {
                        if (isTimeValid(lineOffsetMap.getLineOffset(crrs.getLine()), crrs.getDate())) {
                            index           = dpt.getIndex(crrs.getDate());
                            pss[index].addReading(crrs.getLine(), crrs.getValue(), crrs.getDate(), crrs.getQuantity());
                            if (forChart) {
                                index       = chartDPT.getIndex(crrs.getDate());
                                linePeriods = chartPSS[index].addTotalReading(crrs.getLine(), crrs.getValue(), crrs.getDate(), crrs.getQuantity(), linePeriods);
                            }
                            debugCounter++;
                            //logger.debug("#"+debugCounter+" ["+index+"]: "+"L "+crrs.getLine()+" V: "+crrs.getValue()+" D: "+crrs.getDate().toString());
                        } else {
                            //logger.debug("Invalid Time slot for line :" + String.valueOf(crrs.getLine()) + " with End Date " + String.valueOf(lineOffsetMap.getLineOffset(crrs.getLine()).getEndDate()) +" reported at " + crrs.getDate().toString());
                        }

                    }
                    crrs.close();
                } else {
                    while (rrs.next()) {
                        index           = dpt.getIndex(rrs.getDate());
                        pss[index].addReading(rrs.getLine(), rrs.getValue(), rrs.getDate(), rrs.getQuantity());
                        if (forChart) {
                            index       = chartDPT.getIndex(rrs.getDate());
                            linePeriods = chartPSS[index].addTotalReading(rrs.getLine(), rrs.getValue(), rrs.getDate(), rrs.getQuantity(), linePeriods);
                        }
                        debugCounter++;
                        //logger.debug("#"+debugCounter+" ["+index+"]: "+"L "+rrs.getLine()+" V: "+rrs.getValue()+" D: "+rrs.getDate().toString());
                    }
                    rrs.close();
                }
                lineOffsetMap = null;
            } catch (SQLException sqle) {
                throw new HandlerException(sqle);
            } finally {
            }
            logger.debug("Processed " + debugCounter + " readings");

            toAppend.addElement("startDate").addText(dateFormat.format(start));
            toAppend.addElement("endDate").addText(dateFormat.format(end));

            index = 0;
            ArrayList<String> dateList      = new ArrayList<String>();
                   //
            for (DatePartition dp : dps) {
                //logger.debug("PS Entered DP loop");
                pss[index].setData(conn, parameter, conditionString);
                 if(periodType.toString().equals(PeriodType.DAILY.toString())){
                        if(dateList!=null ){
                            if(!dateList.contains(newDateFormat.format(dp.getDate()).substring(0,10))){
                                appendPeriodReportXML("period", toAppend, dp, pss[index], forChart, byLine, byProduct,periodType.toString(),true);
                            } else {
                                appendPeriodReportXML("period", toAppend, dp, pss[index], forChart, byLine, byProduct,periodType.toString(),false);
                            }
                        } else {
                            appendPeriodReportXML("period", toAppend, dp, pss[index], forChart, byLine, byProduct,periodType.toString(),true);
                        }
                        dateList.add(newDateFormat.format(dp.getDate()).substring(0,10));
                    } else {
                        appendPeriodReportXML("period", toAppend, dp, pss[index], forChart, byLine, byProduct, periodType.toString(),true);
                    }
                index++;
            }
            index = 0;
            if (forChart) {
                double value                = 0.0;
                ps                          = chartPSS[index];
                Map<Integer, Double> lineMap
                                            = ps.getTotalValues(conn,linePeriods);
                if (null != lineMap && lineMap.size() > 0) {
                    for (Integer i : lineMap.keySet()) {
                        value               += lineMap.get(i);
                    }
                }
                Element elValue             = toAppend.addElement("totalPoured");
                elValue.addElement("value").addText(String.valueOf(value));
            }
            ReportResults.clearLineCache();
        }
    }

    private void getReportNew(Element toHandle, Element toAppend) throws HandlerException {
        int callerId                        = getCallerId(toHandle);
        Date start                          = getRequiredDate(toHandle, "startDate");
        Date end                            = getRequiredDate(toHandle, "endDate");
        String periodStr                    = HandlerUtils.getRequiredString(toHandle, "periodType");
        String periodDetail                 = HandlerUtils.getRequiredString(toHandle, "periodDetail");
        boolean lineCleaning                = HandlerUtils.getOptionalBoolean(toHandle, "lineCleaning");
        boolean forChart                    = HandlerUtils.getOptionalBoolean(toHandle, "forChart");
        //new: filter by a specific product
        int product                         = HandlerUtils.getOptionalInteger(toHandle, "specificProduct");
        if (forChart && product > 0) {
            periodStr                       = "MINUTELY";
            periodDetail                    = "10";
        }
        PeriodShiftType periodShift         = PeriodShiftType.instanceOf("EntireDay");
        String periodShiftString            = HandlerUtils.getOptionalString(toHandle, "periodShift");
        if (null != periodShiftString) {
            periodShift                     = PeriodShiftType.instanceOf(HandlerUtils.getOptionalString(toHandle, "periodShift"));
        }
        int line                            = HandlerUtils.getOptionalInteger(toHandle, "lineId");
        int station                         = HandlerUtils.getOptionalInteger(toHandle, "stationId");
        int bar                             = HandlerUtils.getOptionalInteger(toHandle, "barId");
        int zone                            = HandlerUtils.getOptionalInteger(toHandle, "zoneId");
        int regionId                        = HandlerUtils.getOptionalInteger(toHandle, "regionId");
        int location                        = HandlerUtils.getOptionalInteger(toHandle, "locationId");
        int customer                        = HandlerUtils.getOptionalInteger(toHandle, "customerId");
        int user                            = HandlerUtils.getOptionalInteger(toHandle, "userId");
        int btype                           = HandlerUtils.getOptionalInteger(toHandle, "type");
        int ptype                           = HandlerUtils.getOptionalInteger(toHandle, "ptype");
        String specificLocationsString      = HandlerUtils.getOptionalString(toHandle, "specificLocations");
        int parameter                       = 0;
        String conditionString              = "";
        
        //logger.debug("PT:"+periodStr +"pD "+periodDetail+ "PS"+periodShiftString);


        String byLineString                 = HandlerUtils.getOptionalString(toHandle, "byLine");
        String byProductString              = HandlerUtils.getOptionalString(toHandle, "byProduct");
        boolean byLine                      = (!"false".equalsIgnoreCase(byLineString));
        boolean byProduct                   = (!"false".equalsIgnoreCase(byProductString));

        int paramsSet                       = 0, paramLevel = 0;
        if (line >= 0) {
            paramLevel                      = 7;
            parameter                       = line;
            conditionString                 = " WHERE l.id = ? ";
            paramsSet++;
            logger.debug("line:"+line);
        }
        if (user >=0 && specificLocationsString != null && specificLocationsString.length() > 0) {
            paramLevel                      = 6;
            parameter                       = -1;
            conditionString                 = " WHERE b.location IN (" + specificLocationsString + ") ";
            start                           = setStartDate(periodShift.toSQLQueryInt(), 0, specificLocationsString, getRequiredDate(toHandle, "startDate"));
            end                             = setEndDate(periodShift.toSQLQueryInt(), 0, specificLocationsString, getRequiredDate(toHandle, "startDate"));
            periodDetail                    = String.valueOf(start.getHours() + 1);
            paramsSet++;
        }
        if (station >= 0) {
            paramLevel                      = 5;
            parameter                       = station;
            conditionString                 = " WHERE l.station = ? ";
            paramsSet++;            
        }
        if (bar >= 0) {
            paramLevel                      = 4;
            parameter                       = bar;
            conditionString                 = " WHERE b.id = ? ";
            paramsSet++;
        }
        if (zone >= 0) {
            paramLevel                      = 3;
            parameter                       = zone;
            conditionString                 = " WHERE b.zone = ? ";
            paramsSet++;            
        }
        if (location >= 0) {
            paramLevel                      = 2;
            parameter                       = location;
            conditionString                 = " WHERE b.location = ? ";
            paramsSet++;
            
        }
        if (customer >= 0) {
            paramLevel                      = 1;
            parameter                       = customer;
            conditionString                 = " WHERE loc.customer = ? ";
            if(regionId > 0){
                specificLocationsString     =getRegionLocations(customer,regionId);
            }
            start                           = setStartDate(periodShift.toSQLQueryInt(), customer, specificLocationsString, getRequiredDate(toHandle, "startDate"));
            end                             = setEndDate(periodShift.toSQLQueryInt(), customer, specificLocationsString, getRequiredDate(toHandle, "startDate"));
            periodDetail                    = String.valueOf(start.getHours() + 1);
            paramsSet++;

        }

        if (paramsSet != 1) {
            throw new HandlerException("Exactly one of the following must " +
                    "be set: lineId stationId barId zoneId locationId customerId userId");
        }

        PeriodType periodType               = PeriodType.parseString(periodStr);
        if (null == periodType) {
            throw new HandlerException("Invalid period type: " + periodStr);
        }
        ReportPeriod period                 = null, chartPeriod = null;
        try {
            period                          = new ReportPeriod(periodType, periodDetail, start, end);
        } catch (IllegalArgumentException e) {
            throw new HandlerException(e.getMessage());
        }

        SortedSet<DatePartition> dps        = DatePartitionFactory.createPartitions(period);
        //logger.debug("Created partitions: \n"+DatePartitionFactory.partitionReport(dps));
        DatePartitionTree dpt               = new DatePartitionTree(dps);

        StringBuilder lineString            = new StringBuilder();
        LineString ls                       = null;
        ReportResults rrs                   = null;        

        switch (paramLevel) {
            case 1:
                lineOffsetMap               = new LineOffsetMap(conn, periodShift.toSQLQueryInt(), customer, specificLocationsString, dateToString(getRequiredDate(toHandle, "startDate")));
                ls                          = new LineString(conn, paramLevel, customer, btype, product, period, specificLocationsString);
                lineString                  = ls.getLineString();
                break;
            case 2:
                ls                          = new LineString(conn, paramLevel, location, btype, product, period, specificLocationsString);
                lineString                  = ls.getLineString();
                break;
            case 3:
                ls                          = new LineString(conn, paramLevel, zone, btype, product, period, specificLocationsString);
                lineString                  = ls.getLineString();
                break;
            case 4:
                ls                          = new LineString(conn, paramLevel, bar, btype, product, period, specificLocationsString);
                lineString                  = ls.getLineString();
                break;
            case 5:
                ls                          = new LineString(conn, paramLevel, station, btype, product, period, specificLocationsString);
                lineString                  = ls.getLineString();
                break;
            case 6:
                lineOffsetMap               = new LineOffsetMap(conn, periodShift.toSQLQueryInt(), 0, specificLocationsString, dateToString(getRequiredDate(toHandle, "startDate")));
                ls                          = new LineString(conn, paramLevel, 0, btype, product, period, specificLocationsString);
                lineString                  = ls.getLineString();
                break;
            case 7:
                ls                          = new LineString(conn, paramLevel, line, btype, product, period, specificLocationsString);
                lineString                  = ls.getLineString();
                break;
            default:
                break;
        }
        //logger.debug("Line String: " + lineString.toString());
        rrs                                 = ReportResults.getResultsByLineString(period, byProduct, lineCleaning, lineString.toString(), conn);

        PeriodStructure pss[]               = null, chartPSS[] = null;
        PeriodStructure ps                  = null;
        DatePartitionTree chartDPT          = null;
        SortedSet<DatePartition> chartDPS   = null;
        HashMap<Integer, LinePeriod> linePeriods
                                            = new HashMap<Integer, LinePeriod>();
        int dpsSize                         = dps.size();
        int index;
        if (dpsSize > 0) {
            pss                             = new PeriodStructure[dpsSize];
            //this is a temporary array of the DatePartitions used to construct the PeriodStructures start dates
            Object[] dpa                    = dps.toArray();
            for (int i = 0; i < dpsSize; i++) {
                // create a new PeriodStructure and link it to the previous one (or null for the first)
                ps                          = new PeriodStructure(ps, ((DatePartition) dpa[i]).getDate());
                pss[i]                      = ps;
            }
            if (forChart) {
                try {
                    periodDetail            = "7";
                    if (customer >= 0 || user >= 0) {
                        periodDetail        = String.valueOf(start.getHours() + 1);
                    }
                    chartPeriod             = new ReportPeriod(PeriodType.DAILY, periodDetail, start, end);
                } catch (IllegalArgumentException e) {
                    throw new HandlerException(e.getMessage());
                }
                chartDPS                    = DatePartitionFactory.createPartitions(chartPeriod);
                //logger.debug("Created partitions: \n"+DatePartitionFactory.partitionReport(chartDPS));
                chartDPT                    = new DatePartitionTree(chartDPS);
                int chartDPSSize            = chartDPS.size();
                chartPSS                    = new PeriodStructure[chartDPSSize];
                //this is a temporary array of the DatePartitions used to construct the PeriodStructures start dates
                Object[] chartDPA           = chartDPS.toArray();
                for (int i = 0; i < chartDPSSize; i++) {
                    // create a new PeriodStructure and link it to the previous one (or null for the first)
                    ps                      = new PeriodStructure(ps, ((DatePartition) chartDPA[i]).getDate());
                    chartPSS[i]             = ps;
                }
            }
            int debugCounter                = 0;
            try {
                if (customer >= 0 || user >= 0) {
                    while (rrs.next()) {
                        if (isTimeValid(lineOffsetMap.getLineOffset(rrs.getLine()), rrs.getDate())) {
                            index           = dpt.getIndex(rrs.getDate());
                            pss[index].addReading(rrs.getLine(), rrs.getValue(), rrs.getDate(), rrs.getQuantity());
                            
                            if (forChart) {
                                index       = chartDPT.getIndex(rrs.getDate());
                                linePeriods = chartPSS[index].addTotalReading(rrs.getLine(), rrs.getValue(), rrs.getDate(), rrs.getQuantity(), linePeriods);
                                
                            }
                            debugCounter++;
                            //logger.debug("#"+debugCounter+" ["+index+"]: "+"L "+rrs.getLine()+" V: "+rrs.getValue()+" D: "+rrs.getDate().toString());
                        } else {
                            //logger.debug("Invalid Time slot for line :" + String.valueOf(crrs.getLine()) + " with End Date " + String.valueOf(lineOffsetMap.getLineOffset(crrs.getLine()).getEndDate()) +" reported at " + crrs.getDate().toString());
                        }

                    }
                } else {
                    while (rrs.next()) {
                        index           = dpt.getIndex(rrs.getDate());
                        pss[index].addReading(rrs.getLine(), rrs.getValue(), rrs.getDate(), rrs.getQuantity());
                        
                        if (forChart) {
                            index       = chartDPT.getIndex(rrs.getDate());
                            linePeriods = chartPSS[index].addTotalReading(rrs.getLine(), rrs.getValue(), rrs.getDate(), rrs.getQuantity(), linePeriods);
                            
                        }
                        debugCounter++;
                        //logger.debug("#"+debugCounter+" ["+index+"]: "+"L "+rrs.getLine()+" V: "+rrs.getValue()+" D: "+rrs.getDate().toString());
                    }
                }
                rrs.close();
                lineOffsetMap               = null;
            } catch (SQLException sqle) {
                throw new HandlerException(sqle);
            } finally {
            }
            logger.debug("Processed " + debugCounter + " readings");

            toAppend.addElement("startDate").addText(dateFormat.format(start));
            toAppend.addElement("endDate").addText(dateFormat.format(end));

            index                           = 0;
            ArrayList<String> dateList      = new ArrayList<String>();              
                   //
            for (DatePartition dp : dps) {
                logger.debug("PS Entered DP loop: " + dp.getDate().toString());
                pss[index].setData(conn, parameter, conditionString);                
                 if(periodType.toString().equals(PeriodType.DAILY.toString())){
                        if(dateList!=null ){
                            if(!dateList.contains(newDateFormat.format(dp.getDate()).substring(0,10))){
                                appendPeriodReportXML("period", toAppend, dp, pss[index], forChart, byLine, byProduct,periodType.toString(),true);
                            } else {
                                appendPeriodReportXML("period", toAppend, dp, pss[index], forChart, byLine, byProduct,periodType.toString(),false);
                            }
                        } else {
                            appendPeriodReportXML("period", toAppend, dp, pss[index], forChart, byLine, byProduct,periodType.toString(),true);
                        }
                        dateList.add(newDateFormat.format(dp.getDate()).substring(0,10));
                    } else {
                        appendPeriodReportXML("period", toAppend, dp, pss[index], forChart, byLine, byProduct,periodType.toString(),true);
                    }                
                index++;
            }
            index                           = 0;
            if (forChart) {
                double value                = 0.0;
                ps                          = chartPSS[index];
                Map<Integer, Double> lineMap
                                            = ps.getTotalValues(conn,linePeriods);
                if (null != lineMap && lineMap.size() > 0) {
                    for (Integer i : lineMap.keySet()) {
                        value               += lineMap.get(i);
                    }
                }
                Element elValue             = toAppend.addElement("totalPoured");
                elValue.addElement("value").addText(String.valueOf(value));
            }
            ReportResults.clearLineCache();
        }
        if(callerId > 0){
            logger.portalVisitDetail(callerId, "getReport", location, "getReport",0,10, "", conn);
        }
    }

    private void getConcessionsPouredReport(Element toHandle, Element toAppend) throws HandlerException {
        int callerId                        = getCallerId(toHandle);
        Date start                          = getRequiredDate(toHandle, "startDate");
        Date end                            = getRequiredDate(toHandle, "endDate");
        String specialBarString             = HandlerUtils.getOptionalString(toHandle, "specialBarString");
        String cateredBarString             = HandlerUtils.getOptionalString(toHandle, "cateredBarString");
        String periodStr                    = HandlerUtils.getRequiredString(toHandle, "periodType");
        String periodDetail                 = HandlerUtils.getRequiredString(toHandle, "periodDetail");
        PeriodShiftType periodShift         = PeriodShiftType.instanceOf("EntireDay");
        String periodShiftString            = HandlerUtils.getOptionalString(toHandle, "periodShift");
        if (null != periodShiftString) {
            periodShift                     = PeriodShiftType.instanceOf(HandlerUtils.getOptionalString(toHandle, "periodShift"));
        }
        int station                         = HandlerUtils.getOptionalInteger(toHandle, "stationId");
        int bar                             = HandlerUtils.getOptionalInteger(toHandle, "barId");
        int zone                            = HandlerUtils.getOptionalInteger(toHandle, "zoneId");
        int location                        = HandlerUtils.getOptionalInteger(toHandle, "locationId");
        int btype                           = HandlerUtils.getOptionalInteger(toHandle, "type");
        int product                         = HandlerUtils.getOptionalInteger(toHandle, "specificProduct");
        boolean forChart                    = HandlerUtils.getOptionalBoolean(toHandle, "forChart");
        String byLineString                 = HandlerUtils.getOptionalString(toHandle, "byLine");
        boolean byLine                      = (!"false".equalsIgnoreCase(byLineString));
        String byProductString              = HandlerUtils.getOptionalString(toHandle, "byProduct");
        boolean byProduct                   = (!"false".equalsIgnoreCase(byProductString));

        int parameter                       = 0;
        String conditionString              = "";

        int paramsSet                       = 0, paramLevel = 0;
        if (station >= 0) {
            paramLevel                      = 1;
            parameter                       = station;
            conditionString                 = " WHERE l.station = ? ";
            paramsSet++;
        }
        if (bar >= 0) {
            paramLevel                      = 2;
            parameter                       = bar;
            conditionString                 = " WHERE b.id = ? ";
            paramsSet++;
        }
        if (zone >= 0) {
            paramLevel                      = 3;
            parameter                       = zone;
            conditionString                 = " WHERE b.zone = ? ";
            paramsSet++;
        }
        if (location >= 0) {
            paramLevel                      = 4;
            parameter                       = location;
            conditionString                 = " WHERE b.location = ? ";
            paramsSet++;
        }
        if (paramsSet != 1) {
            throw new HandlerException("Exactly one of the following must be set: stationId barId zoneId locationId");
        }

        PreparedStatement stmt              = null;
        ResultSet rs                        = null;
        
        String exclusionLines               = null, specialEventLines = null, cateredEventLines = null;
        try {

            if (null != specialBarString) {
                stmt                        = conn.prepareStatement("SELECT GROUP_CONCAT(id) FROM line WHERE bar IN (" + specialBarString + ")");
                rs                          = stmt.executeQuery();
                if (rs.next()) {
                    exclusionLines          = specialEventLines = rs.getString(1);
                }
            }

            if (null != cateredBarString) {
                stmt                        = conn.prepareStatement("SELECT GROUP_CONCAT(id) FROM line WHERE bar IN (" + cateredBarString + ")");
                rs                          = stmt.executeQuery();
                if (rs.next()) {
                    cateredEventLines       = rs.getString(1);
                    if (exclusionLines != null) {
                        exclusionLines      += ", " + cateredEventLines;
                    } else {
                        exclusionLines      = cateredEventLines;
                    }
                }
            }
        } catch (SQLException sqle) {
            throw new HandlerException(sqle);
        } finally {
            close(rs);
            close(stmt);
        }

        PeriodType periodType               = PeriodType.parseString(periodStr);
        if (null == periodType) {
            throw new HandlerException("Invalid period type: " + periodStr);
        }
        ReportPeriod period                 = null, chartPeriod = null, specialEventPeriod = null, cateredEventPeriod = null, partitionPeriod = null;
        try {
            period                          = new ReportPeriod(periodType, periodDetail, start, end);
            if (null != specialBarString) {
                Date specialStart           = getRequiredDate(toHandle, "specialStartDate");
                Date specialEnd             = getRequiredDate(toHandle, "specialEndDate");
                specialEventPeriod          = new ReportPeriod(periodType, periodDetail, specialStart, specialEnd);
                if (start.after(specialStart)) {
                    start                   = specialStart;
                }
                if (end.before(specialEnd)) {
                    end                     = specialEnd;
                }
            }
            if (null != cateredBarString) {
                Date cateredStart           = getRequiredDate(toHandle, "cateredStartDate");
                Date cateredEnd             = getRequiredDate(toHandle, "cateredEndDate");
                cateredEventPeriod          = new ReportPeriod(periodType, periodDetail, cateredStart, cateredEnd);
            } else {
                cateredEventPeriod          = new ReportPeriod(periodType, periodDetail, start, start);
            }
            partitionPeriod                 = new ReportPeriod(periodType, periodDetail, start, end);
        } catch (IllegalArgumentException e) {
            throw new HandlerException(e.getMessage());
        }



        SortedSet<DatePartition> dps        = DatePartitionFactory.createPartitions(partitionPeriod);
        //logger.debug("Created partitions: \n"+DatePartitionFactory.partitionReport(dps));
        DatePartitionTree dpt               = new DatePartitionTree(dps);

        SortedSet<DatePartition> catereddps = DatePartitionFactory.createPartitions(cateredEventPeriod);
        //logger.debug("Created catered partitions: \n"+DatePartitionFactory.partitionReport(catereddps));
        DatePartitionTree catereddpt        = new DatePartitionTree(catereddps);

        //logger.debug("cateredEventLines: " + cateredEventLines);

        ReportResults rrs                   = null, specialRSS = null, cateredRSS = null;

        switch (paramLevel) {
            case 1:
                rrs                         = ReportResults.getResultsByStationConcessions(period, btype, false, station, product, (exclusionLines == null ? "" : exclusionLines), "", conn);
                if (null != specialEventLines) {
                    specialRSS             = ReportResults.getResultsByStationConcessions(specialEventPeriod, btype, false, station, product, "", specialEventLines, conn);
                }
                if (null != cateredEventLines) {
                    cateredRSS             = ReportResults.getResultsByStationConcessions(cateredEventPeriod, btype, false, station, product, "", cateredEventLines, conn);
                }
                break;
            case 2:
                rrs                         = ReportResults.getResultsByBarConcessions(period, btype, false, bar, product, (exclusionLines == null ? "" : exclusionLines), "", conn);
                if (null != specialEventLines) {
                    specialRSS             = ReportResults.getResultsByBarConcessions(specialEventPeriod, btype, false, bar, product, "", specialEventLines, conn);
                }
                if (null != cateredEventLines) {
                    cateredRSS             = ReportResults.getResultsByBarConcessions(cateredEventPeriod, btype, false, bar, product, "", cateredEventLines, conn);
                }
                break;
            case 3:
                rrs                         = ReportResults.getResultsByZoneConcessions(period, btype, false, zone, product, (exclusionLines == null ? "" : exclusionLines), "", conn);
                if (null != specialEventLines) {
                    specialRSS             = ReportResults.getResultsByZoneConcessions(specialEventPeriod, btype, false, zone, product, "", specialEventLines, conn);
                }
                if (null != cateredEventLines) {
                    cateredRSS             = ReportResults.getResultsByZoneConcessions(cateredEventPeriod, btype, false, zone, product, "", cateredEventLines, conn);
                }
                break;
            case 4:
                rrs                         = ReportResults.getResultsByLocationConcessions(period, btype, byProduct, false, location, product, (exclusionLines == null ? "" : exclusionLines), "", conn);
                if (null != specialEventLines) {
                    specialRSS             = ReportResults.getResultsByLocationConcessions(specialEventPeriod, btype, byProduct, false, location, product, "", specialEventLines, conn);
                }
                if (null != cateredEventLines) {
                    cateredRSS             = ReportResults.getResultsByLocationConcessions(cateredEventPeriod, btype, byProduct, false, location, product, "", cateredEventLines, conn);
                }
                break;
            default:
                break;
        }

        PeriodStructure pss[]               = null, cepss[] = null, chartPSS[] = null;
        PeriodStructure ps                  = null;
        DatePartitionTree chartDPT          = null;
        SortedSet<DatePartition> chartDPS   = null;
        HashMap<Integer, LinePeriod> linePeriods
                                            = new HashMap<Integer, LinePeriod>();
        int dpsSize                         = dps.size();
        int catereddpsSize                  = catereddps.size();
        int index;
        if (dpsSize > 0) {
            pss                             = new PeriodStructure[dpsSize];
            //this is a temporary array of the DatePartitions used to construct the PeriodStructures start dates
            Object[] dpa                    = dps.toArray();
            for (int i = 0; i < dpsSize; i++) {
                // create a new PeriodStructure and link it to the previous one (or null for the first)
                ps                          = new PeriodStructure(ps, ((DatePartition) dpa[i]).getDate());
                pss[i]                      = ps;
            }
            
            if (catereddpsSize > 0 && cateredEventLines != null) {
                cepss                       = new PeriodStructure[catereddpsSize];
                dpa                         = catereddps.toArray();
                for (int i = 0; i < catereddpsSize; i++) {
                    // create a new PeriodStructure and link it to the previous one (or null for the first)
                    ps                          = new PeriodStructure(ps, ((DatePartition) dpa[i]).getDate());
                    cepss[i]                    = ps;
                }
            }

            if (forChart) {
                try {
                    periodDetail            = "7";
                    chartPeriod             = new ReportPeriod(PeriodType.DAILY, periodDetail, start, end);
                } catch (IllegalArgumentException e) {
                    throw new HandlerException(e.getMessage());
                }
                chartDPS                    = DatePartitionFactory.createPartitions(chartPeriod);
                //logger.debug("Created partitions: \n"+DatePartitionFactory.partitionReport(chartDPS));
                chartDPT                    = new DatePartitionTree(chartDPS);
                int chartDPSSize            = chartDPS.size();
                chartPSS                    = new PeriodStructure[chartDPSSize];
                //this is a temporary array of the DatePartitions used to construct the PeriodStructures start dates
                Object[] chartDPA           = chartDPS.toArray();
                for (int i = 0; i < chartDPSSize; i++) {
                    // create a new PeriodStructure and link it to the previous one (or null for the first)
                    ps                      = new PeriodStructure(ps, ((DatePartition) chartDPA[i]).getDate());
                    chartPSS[i]             = ps;
                }
            }
            
            int debugCounter                = 0;
            try {
                while (rrs.next()) {
                    index                   = dpt.getIndex(rrs.getDate());
                    pss[index].addReading(rrs.getLine(), rrs.getValue(), rrs.getDate(), rrs.getQuantity());
                    if (forChart) {
                        index               = chartDPT.getIndex(rrs.getDate());
                        linePeriods         = chartPSS[index].addTotalReading(rrs.getLine(), rrs.getValue(), rrs.getDate(), rrs.getQuantity(), linePeriods);
                    }
                    debugCounter++;
                    //logger.debug("#"+debugCounter+" ["+index+"]: "+"L "+rrs.getLine()+" V: "+rrs.getValue()+" D: "+rrs.getDate().toString());
                }
                rrs.close();
            } catch (SQLException sqle) {
                throw new HandlerException(sqle);
            } finally {
            }
            
            if (specialRSS != null) {
                try {
                    while (specialRSS.next()) {
                        index               = dpt.getIndex(specialRSS.getDate());
                        pss[index].addReading(specialRSS.getLine(), specialRSS.getValue(), specialRSS.getDate(), specialRSS.getQuantity());
                        if (forChart) {
                            index           = chartDPT.getIndex(specialRSS.getDate());
                            linePeriods     = chartPSS[index].addTotalReading(specialRSS.getLine(), specialRSS.getValue(), specialRSS.getDate(), specialRSS.getQuantity(), linePeriods);
                        }
                        debugCounter++;
                        //logger.debug("#"+debugCounter+" ["+index+"]: "+"L "+specialRSS.getLine()+" V: "+specialRSS.getValue()+" D: "+specialRSS.getDate().toString());
                    }
                    specialRSS.close();
                } catch (SQLException sqle) {
                    throw new HandlerException(sqle);
                } finally {
                }
            }
            logger.debug("Processed " + debugCounter + " readings");
            if (null != cateredEventLines) {
            debugCounter                    = 0;
            if (cateredRSS != null) {
                try {
                    while (cateredRSS.next()) {
                        index               = catereddpt.getIndex(cateredRSS.getDate());
                        cepss[index].addReading(cateredRSS.getLine(), cateredRSS.getValue(), cateredRSS.getDate(), cateredRSS.getQuantity());
                        debugCounter++;
                        //logger.debug("#"+debugCounter+" ["+index+"]: "+"L "+cateredRSS.getLine()+" V: "+cateredRSS.getValue()+" D: "+cateredRSS.getDate().toString());
                    }
                    cateredRSS.close();
                } catch (SQLException sqle) {
                    throw new HandlerException(sqle);
                } finally {
                }
                logger.debug("Processed Catered " + debugCounter + " readings");
            }
            }
            toAppend.addElement("startDate").addText(dateFormat.format(start));
            toAppend.addElement("endDate").addText(dateFormat.format(end));

            index                           = 0;
            ArrayList<String> dateList      = new ArrayList<String>();              
            for (DatePartition dp : dps) {
                //logger.debug("PS Entered DP loop");
                pss[index].setData(conn, parameter, conditionString);
                
                 if(periodType.toString().equals(PeriodType.DAILY.toString())){
                        if(dateList!=null ){
                            if(!dateList.contains(newDateFormat.format(dp.getDate()).substring(0,10))){
                                appendPeriodReportXML("period", toAppend, dp, pss[index], forChart, byLine, byProduct,periodType.toString(),true);
                            } else {
                                appendPeriodReportXML("period", toAppend, dp, pss[index], forChart, byLine, byProduct,periodType.toString(),false);
                            }
                        } else {
                            appendPeriodReportXML("period", toAppend, dp, pss[index], forChart, byLine, byProduct,periodType.toString(),true);
                        }
                        dateList.add(newDateFormat.format(dp.getDate()).substring(0,10));
                    } else {
                        appendPeriodReportXML("period", toAppend, dp, pss[index], forChart, byLine, byProduct,periodType.toString(),true);
                    }
                //appendPeriodReportXML("period", toAppend, dp, pss[index], forChart, byLine, byProduct,periodType.toString());
                index++;
            }
            if (null != cateredEventLines) {
            index                           = 0;
            dateList                        = new ArrayList<String>();
            for (DatePartition dp : catereddps) {
                //logger.debug("PS Entered Catered DP loop");
                cepss[index].setData(conn, parameter, conditionString);

                 if(periodType.toString().equals(PeriodType.DAILY.toString())){
                        if(dateList!=null ){
                            if(!dateList.contains(newDateFormat.format(dp.getDate()).substring(0,10))){
                                appendPeriodReportXML("cateredPeriod", toAppend, dp, cepss[index], forChart, byLine, byProduct, periodType.toString(),true);
                            } else {
                                appendPeriodReportXML("cateredPeriod", toAppend, dp, cepss[index], forChart, byLine, byProduct, periodType.toString(),false);
                            }
                        } else {
                            appendPeriodReportXML("cateredPeriod", toAppend, dp, cepss[index], forChart, byLine, byProduct, periodType.toString(),true);
                        }
                        dateList.add(newDateFormat.format(dp.getDate()).substring(0,10));
                    } else {
                        appendPeriodReportXML("cateredPeriod", toAppend, dp, cepss[index], forChart, byLine, byProduct, periodType.toString(),true);
                    }
                //appendPeriodReportXML("period", toAppend, dp, pss[index], forChart, byLine, byProduct,periodType.toString());
                index++;
            }
            }
            index                           = 0;
            if (forChart) {
                double value                = 0.0;
                ps                          = chartPSS[index];
                Map<Integer, Double> lineMap
                                            = ps.getTotalValues(conn,linePeriods);
                if (null != lineMap && lineMap.size() > 0) {
                    for (Integer i : lineMap.keySet()) {
                        value               += lineMap.get(i);
                    }
                }
                Element elValue             = toAppend.addElement("totalPoured");
                elValue.addElement("value").addText(String.valueOf(value));
            }
            ReportResults.clearLineCache();
        }
        if(callerId > 0){
            logger.portalVisitDetail(callerId, "getReport", location, "getConcessionPouredReport",0,10, "", conn);
        }
    }
    
    private void getUnknownReading(String location,String startDate,String periodType,Element elPeriod,boolean poured) throws HandlerException {
          
          
        PreparedStatement stmt              = null;
        ResultSet rs                        = null, typeRs = null,rsDetails =null;
        String condition                    = "?";
        if(periodType.equals(PeriodType.HOURLY.toString())) {
            condition                       = "DATE_ADD(?,INTERVAL 59 MINUTE)";
        }else if(periodType.equals(PeriodType.DAILY.toString())) {
            condition                       = "DATE_ADD(?,INTERVAL 1 DAY)";
        } else if(periodType.equals(PeriodType.MINUTELY.toString())){
            condition                       = "DATE_ADD(?,INTERVAL 14 MINUTE)";
            
        }
        
        String unknownPoured                = "SELECT r.location,sum(r.value),l.name,c.id,c.name FROM unknownReading r LEFT JOIN location l ON l.id=r.location"
                                            + " LEFT JOIN customer c ON c.id=l.customer  WHERE r.location IN ("+location+")  AND r.date between ? AND "+condition+" Group BY r.location;";
        String unknownSold                  = "SELECT r.location,sum(r.quantity),l.name,c.id,c.name FROM unknownSales r LEFT JOIN location l ON l.id=r.location"
                                            + " LEFT JOIN customer c ON c.id=l.customer  WHERE r.location IN ("+location+")  AND r.date between ? AND "+condition+" Group BY r.location;";
       //logger.debug("period:"+periodType+" Date:"+startDate);
       //  logger.debug("location:"+location);
       
        try {
            if(poured){
            stmt                = conn.prepareStatement(unknownPoured);           
            stmt.setString(1, startDate);
            stmt.setString(2, startDate);
            rs                  = stmt.executeQuery();
            while(rs.next()) {
               Element elProd = elPeriod.addElement("product");
               elProd.addElement("locationId").addText(String.valueOf(rs.getInt(1)));
               elProd.addElement("productId").addText(String.valueOf(0));
               elProd.addElement("productName").addText(HandlerUtils.nullToEmpty("Unknown Product"));
               elProd.addElement("name").addText(HandlerUtils.nullToEmpty("Unknown Product"));
               elProd.addElement("ounces").addText(HandlerUtils.nullToEmpty(rs.getString(2)));
               elProd.addElement("value").addText(HandlerUtils.nullToEmpty(rs.getString(2)));
               elProd.addElement("locationName").addText(HandlerUtils.nullToEmpty(rs.getString(3)));
               elProd.addElement("customerId").addText(String.valueOf(rs.getInt(4)));
               elProd.addElement("customerName").addText(HandlerUtils.nullToEmpty(rs.getString(5)));
               elProd.addElement("barId").addText(String.valueOf(0));
               elProd.addElement("barName").addText(HandlerUtils.nullToEmpty("Unknown Bar"));
               //logger.debug("Location:"+HandlerUtils.nullToEmpty(rs.getString(3))+" Date:"+startDate+" Value:"+HandlerUtils.nullToEmpty(rs.getString(2)));
               
            }
            } else {
                stmt                = conn.prepareStatement(unknownSold);           
            stmt.setString(1, startDate);
            stmt.setString(2, startDate);
            rs                  = stmt.executeQuery();
            while(rs.next()) {
               Element elProd = elPeriod.addElement("product");
               elProd.addElement("identifierType").addText(String.valueOf(1));
               elProd.addElement("identifier").addText(String.valueOf(rs.getInt(1)));
               elProd.addElement("locationId").addText(String.valueOf(rs.getInt(1)));
               elProd.addElement("productId").addText(String.valueOf(0));
               elProd.addElement("product").addText(String.valueOf(0));
               elProd.addElement("productName").addText(HandlerUtils.nullToEmpty("Unknown Product"));
               elProd.addElement("name").addText(HandlerUtils.nullToEmpty("Unknown Product"));
               elProd.addElement("ounces").addText(HandlerUtils.nullToEmpty(rs.getString(2)));
               elProd.addElement("value").addText(HandlerUtils.nullToEmpty(rs.getString(2)));
               elProd.addElement("locationName").addText(HandlerUtils.nullToEmpty(rs.getString(3)));
               elProd.addElement("customerId").addText(String.valueOf(rs.getInt(4)));
               elProd.addElement("customerName").addText(HandlerUtils.nullToEmpty(rs.getString(5)));
               elProd.addElement("barId").addText(String.valueOf(0));
               elProd.addElement("barName").addText(HandlerUtils.nullToEmpty("Unknown Bar"));
               //logger.debug("Location Sold:"+HandlerUtils.nullToEmpty(rs.getString(3))+"  Value:"+HandlerUtils.nullToEmpty(rs.getString(2)));
            }
                
            }
            
            

        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } catch (Exception e) {
            logger.debug(e.getMessage());
        }finally {
            close(rsDetails);
            close(typeRs);
            close(stmt);
            close(rs);
        }
    }

    private void appendPeriodReportXML(String periodTag, Element toAppend, DatePartition dp, PeriodStructure ps, boolean forChart, boolean byLine, boolean byProduct,String periodType,boolean unknown) {
        Element elPeriod                    = toAppend.addElement(periodTag);
        elPeriod.addElement("periodDate").addText(dateFormat.format(dp.getDate()));
        String locations                    = "0";
        Map<Integer, Double> lineMap        = ps.getValues(conn);        
        if (null != lineMap && lineMap.size() > 0) {
            if (forChart) {                
                Map<Integer, String> locationMap
                                            = ps.getLocation(conn);
                HashMap<Integer, Double> valueMap
                                            = new HashMap<Integer, Double>();
                for (Integer i : lineMap.keySet()) {
                    int locationId          = Integer.valueOf(locationMap.get(i).toString().split("\\|")[0]);                    
                    double value            = lineMap.get(i);
                    //logger.debug("Line: " + i + ", Value: " + value);
                    if (valueMap.containsKey(locationId)) {
                        value               += valueMap.get(locationId);
                    } 
                    valueMap.put(locationId, value);
                }
                for (Integer location : valueMap.keySet()) {
                    Element elLine = elPeriod.addElement("data");
                    elLine.addElement("locationId").addText(String.valueOf(location));
                    elLine.addElement("value").addText(String.valueOf(valueMap.get(location)));
                    locations               +=","+String.valueOf(location);
                }
                try {
                    if(unknown){
                        //getUnknownReading(locations,newDateFormat.format(dp.getDate()),periodType,elPeriod,true);
                    }
                 }catch(Exception e) {
                    logger.debug(e.getMessage());
                }
            }
            
            if (byLine) {
                for (Integer i : lineMap.keySet()) {
                    Element elLine = elPeriod.addElement("line");
                    elLine.addElement("lineId").addText(HandlerUtils.nullToEmpty(i.toString()));
                    elLine.addElement("value").addText(HandlerUtils.nullToEmpty(lineMap.get(i).toString()));
                }
            }
            if (byProduct) {
                 try { 
                ArrayList<String> dateList      = new ArrayList<String>();
                Map<Integer, String> locationMap= ps.getLocation(conn);
                Map<Integer, String> allLocationMap= new HashMap<Integer, String>();
                Map<Integer, ProductData> prMap = ReportResults.linesToProducts(locationMap, lineMap, conn);
                boolean isBrasstap          = false;
                int locCheck                = 0;
                for (ProductData d : prMap.values()) {
                    if (d.getId() == 4311 && d.getId() == 10661 && d.getPoured() <= 0) { continue; }
                    Element elProd = elPeriod.addElement("product");
                    elProd.addElement("locationId").addText(String.valueOf(d.getLocation()));
                    if(locCheck!=d.getLocation()){
                        isBrasstap = checkBrasstapLocation(1, d.getLocation(), conn);
                        locCheck    = d.getLocation();
                    }
                    elProd.addElement("productId").addText(String.valueOf(d.getId()));
                    if(!isBrasstap){                        
                        elProd.addElement("productName").addText(HandlerUtils.nullToEmpty(d.getName()));
                    } else {
                        
                        elProd.addElement("productName").addText(HandlerUtils.nullToEmpty(getBrasstapProductName(d.getId())));
                    }
                    
                    elProd.addElement("value").addText(String.valueOf(d.getPoured()));
                    if(!allLocationMap.containsKey(d.getLocation())){
                        allLocationMap.put(d.getLocation(), "Location"+d.getLocation());
                    }
                }
                for(int location:allLocationMap.keySet()){
                   locations               +=","+String.valueOf(location); 
                }
                                  
                    if(unknown){
                        //getUnknownReading(locations,newDateFormat.format(dp.getDate()),periodType,elPeriod,true);
                    }
                }catch(Exception e) {
                    logger.debug(e.getMessage());
                }
            }
        }
    }

    private String getStationsString (int parentLevel, int parentValue) throws HandlerException {

        String selectStations               = " SELECT st.id FROM station st ";
        String stationsString               = " AND station IN (0";
        PreparedStatement stmt              = null;
        ResultSet rs                        = null;
        int parameter                       = -1;
        try {
            switch(parentLevel){
                case 3:
                    selectStations += " LEFT JOIN bar b ON b.id = st.bar WHERE b.zone = ? ";
                    parameter = parentValue;
                    break;
                case 4:
                    selectStations += " WHERE st.bar = ? ";
                    parameter = parentValue;
                    break;
                case 5:
                    selectStations += " WHERE st.id = ? ";
                    parameter = parentValue;
                    break;
            }
            if (parameter > 0) {
                stmt = conn.prepareStatement(selectStations);
                stmt.setInt(1, parameter);
                rs = stmt.executeQuery();
                while (rs.next()) {
                    stationsString += ", " + rs.getInt(1);
                }
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(rs);
            close(stmt);
        }
        stationsString += ") ";
        return stationsString;
    }

    private void getGameTimeReport(Element toHandle, Element toAppend) throws HandlerException {
        int callerId                        = getCallerId(toHandle);   
        Date start                          = getRequiredDate(toHandle, "startDate");
        Date end                            = getRequiredDate(toHandle, "endDate");
        DataType dataType                   = DataType.instanceOf("Poured");
        String dataTypeString               = HandlerUtils.getRequiredString(toHandle, "dataType");
        if (null != dataTypeString) {
            dataType                        = DataType.instanceOf(HandlerUtils.getRequiredString(toHandle, "dataType"));
        }
        String periodStr = HandlerUtils.getRequiredString(toHandle, "periodType");
        String periodDetail = HandlerUtils.getRequiredString(toHandle, "periodDetail");
        PeriodShiftType periodShift = PeriodShiftType.instanceOf("EntireDay");
        String periodShiftString = HandlerUtils.getOptionalString(toHandle, "periodShift");
        if (null != periodShiftString) {
            periodShift = PeriodShiftType.instanceOf(HandlerUtils.getOptionalString(toHandle, "periodShift"));
        }
        int station = HandlerUtils.getOptionalInteger(toHandle, "stationId");
        int bar = HandlerUtils.getOptionalInteger(toHandle, "barId");
        int zone = HandlerUtils.getOptionalInteger(toHandle, "zoneId");
        int location = HandlerUtils.getOptionalInteger(toHandle, "locationId");
        int customer = HandlerUtils.getOptionalInteger(toHandle, "customerId");
        String eventIdsString = HandlerUtils.getOptionalString(toHandle, "eventIds");
        boolean exclusion = HandlerUtils.getOptionalBoolean(toHandle, "exclusion");
        int type = HandlerUtils.getOptionalInteger(toHandle, "type");

        String exclusionQuery = "SELECT eH.id, eS.bar, eS.exclusion FROM exclusionSummary eS ";
        HashMap<Integer, HashSet> exclusionMap = new HashMap<Integer, HashSet>();

        //new: filter by a specific product
        int product = HandlerUtils.getOptionalInteger(toHandle, "specificProduct");

        int grouping = HandlerUtils.getRequiredInteger(toHandle, "grouping");

        int paramsSet = 0, exclusionParameter = 0, parentLevel = 0;
        String conditionString = "", stationConditionString = "";
        
        if (customer >= 0) {
            exclusionQuery += " LEFT JOIN eventHours eH ON eH.location = eS.location AND eH.date = eS.date " +
                    " LEFT JOIN location l ON l.id = eS.location WHERE eS.date BETWEEN ? AND ? AND l.customer = ? ";
            exclusionParameter = customer;
            parentLevel = 1;
            paramsSet++;
        }
        if (location >= 0) {
            exclusionQuery += " LEFT JOIN eventHours eH ON eH.location = eS.location AND eH.date = eS.date " +
                    " WHERE eS.date BETWEEN ? AND ? AND eS.location = ? ";
            exclusionParameter = location;
            parentLevel = 2;
            paramsSet++;
        }
        if (zone >= 0) {
            exclusionQuery += " LEFT JOIN eventHours eH ON eH.location = eS.location AND eH.date = eS.date LEFT JOIN bar b ON b.id = eS.bar " +
                    " WHERE eS.date BETWEEN ? AND ? AND b.zone = ? ";
            exclusionParameter = zone;
            parentLevel = 3;
            paramsSet++;
        }
        if (bar >= 0) {
            exclusionQuery += " LEFT JOIN eventHours eH ON eH.location = eS.location AND eH.date = eS.date " +
                    " WHERE eS.date BETWEEN ? AND ? AND eS.bar = ? ";
            exclusionParameter = bar;
            parentLevel = 4;
            paramsSet++;
        }
        if (station >= 0) {
            exclusionQuery += " LEFT JOIN eventHours eH ON eH.location = eS.location AND eH.date = eS.date " +
                    " LEFT JOIN station st ON st.bar = eS.bar WHERE eS.date BETWEEN ? AND ? AND st.id = ? ";
            exclusionParameter = station;
            parentLevel = 5;
            paramsSet++;
        }
        if (null != eventIdsString) {
            parentLevel = HandlerUtils.getRequiredInteger(toHandle, "filterType");
            exclusionParameter = HandlerUtils.getRequiredInteger(toHandle, "filterValue");

            exclusionQuery += " LEFT JOIN eventHours eH ON eH.location = eS.location AND eH.date = eS.date WHERE eH.id IN (" + eventIdsString + ") ";
            paramsSet++;
        }
        if (paramsSet != 1) {
            throw new HandlerException("Exactly one of the following must " +
                    "be set: lineId stationId barId zoneId locationId customerId supplierId countyId regionId userId");
        }

        switch(parentLevel){
            case 0:
                conditionString = " ";
                break;
            case 1:
                conditionString = " WHERE l.customer = ? ";
                break;
            case 2:
                conditionString = " WHERE l.id = ? ";
                break;
            case 3:
                conditionString = " WHERE z.id = ? ";
                stationConditionString   = getStationsString(parentLevel, exclusionParameter);
                break;
            case 4:
                conditionString = " WHERE b.id = ? ";
                stationConditionString   = getStationsString(parentLevel, exclusionParameter);
                break;
            case 5:
                conditionString = " WHERE st.id = ? ";
                stationConditionString   = getStationsString(parentLevel, exclusionParameter);
                break;
            default:
                conditionString = " ";
                break;
        }

        PeriodType periodType = PeriodType.parseString(periodStr);
        if (null == periodType) {
            throw new HandlerException("Invalid period type: " + periodStr);
        }
        ReportPeriod period = null;
        try {
            period = new ReportPeriod(periodType, periodDetail,
                    start, end);
        } catch (IllegalArgumentException e) {
            throw new HandlerException(e.getMessage());
        }

        DateParameter validatedStartDate = new DateParameter(period.getStartDate());
        DateParameter validatedEndDate = new DateParameter(period.getEndDate());
        if (!validatedStartDate.isValid()) {
            logger.debug("Aborted report, invalid start date '" + start + "'");
            addErrorDetail(toAppend, "Invalid Start Date");
        } else if (!validatedEndDate.isValid()) {
            logger.debug("Aborted report, invalid end date '" + end + "'");
            addErrorDetail(toAppend, "Invalid End Date");
        }

        GameReportResults grs = null;
        GameTimeResults gtr = null;

        if (zone >= 0) {
            grs = GameReportResults.getResultsByZone(period, dataType, periodShift, zone, product, conn);
            gtr = GameTimeResults.getResultsByZone(period, zone, conn);
        } else if (bar >= 0) {
            grs = GameReportResults.getResultsByBar(period, dataType, periodShift, bar, product, conn);
            gtr = GameTimeResults.getResultsByBar(period, bar, conn);
        } else if (station >= 0) {
            grs = GameReportResults.getResultsByStation(period, dataType, periodShift, station, product, conn);
            gtr = GameTimeResults.getResultsByStation(period, station, conn);
        } else if (location >= 0) {
            grs = GameReportResults.getResultsByLocation(period, dataType, periodShift, location, product, conn);
            gtr = GameTimeResults.getResultsByLocation(period, location, conn);
        } else if (customer >= 0) {
            grs = GameReportResults.getResultsByCustomer(period, dataType, periodShift, customer, product, conn);
            gtr = GameTimeResults.getResultsByCustomer(period, customer, conn);
        } else if (null != eventIdsString) {
            grs = GameReportResults.getResultsByEventIdsString(period, dataType, periodShift, eventIdsString, stationConditionString, product, conn);
            gtr = GameTimeResults.getResultsByEventIdsString(period, eventIdsString, conn);
        }

        SortedSet<GamePartition> gps = GamePartitionFactory.createPartitions(gtr);
        //logger.debug("Created partitions: \n"+GamePartitionFactory.partitionReport(gps));

        int gpsSize = gps.size();
        int index;
        if (gpsSize > 0) {
            GamePeriodStructure gss[] = null;
            GamePeriodStructure gs = null;
            GamePartitionHash gph = new GamePartitionHash(gps);

            productMap = new ProductMap(conn);
            eventMap = new EventMap(conn);
            categoryMap = new CategoryMap(conn);
            barMap = new BarMap(conn, parentLevel, exclusionParameter, type);
            stationMap = new StationMap(conn, parentLevel, exclusionParameter);

            gss = new GamePeriodStructure[gpsSize];
            //this is a temporary array of the DatePartitions used to construct the PeriodStructures start dates
            Object[] gpa = gps.toArray();
            for (int i = 0; i < gpsSize; i++) {
                // create a new GamePeriodStructure and link it to the previous one (or null for the first)
                gs = new GamePeriodStructure(gs, ((GamePartition) gpa[i]).getGameIndex());
                gss[i] = gs;
            }

            int debugCounter = 0;
            try {
                while (grs.next()) {
                    index = gph.getGameIndex(grs.getGameIndex());
                    if (index < 0) {continue;}
                    gss[index].addReading(grs.getStation(), grs.getProduct(), grs.getGameIndex(), grs.getValue(), grs.getDate());
                    debugCounter++;
                    //logger.debug("#"+debugCounter+" ["+index+"]: "+"St "+grs.getStation()+" P "+grs.getProduct()+" GI "+grs.getGameIndex()+" V: "+grs.getValue()+" D: "+grs.getDate().toString());
                }
                grs.close();
            } catch (SQLException sqle) {
                throw new HandlerException(sqle);
            } finally {
            }
            logger.debug("Processed " + debugCounter + " readings");
            toAppend.addElement("startDate").addText(dateFormat.format(start));
            toAppend.addElement("endDate").addText(dateFormat.format(end));
            toAppend.addElement("eventCount").addText(String.valueOf(gpsSize));

            if (exclusion) {
                PreparedStatement stmt = null;
                ResultSet rs = null;
                int paramCount = 1;
                exclusionQuery += " AND eS.exclusion IN (4,?) ORDER BY eS.location, eS.bar, eS.exclusion, eH.id ";

                try {
                    stmt = conn.prepareStatement(exclusionQuery);
                    if ((exclusionParameter > 0) && null == eventIdsString) {
                        stmt.setString(paramCount++, validatedStartDate.toString());
                        stmt.setString(paramCount++, validatedEndDate.toString());
                        stmt.setInt(paramCount++, exclusionParameter);
                    }
                    stmt.setInt(paramCount++, dataType.toExclusionInt());
                    //logger.debug("Reached exclusion block");
                    rs = stmt.executeQuery();
                    exclusionMap = appendExclusionSummaryReportByEventXML(toAppend, rs);
                    rs.close();
                } catch (SQLException sqle) {
                    throw new HandlerException(sqle);
                } finally {
                }
            }

            index = 0;
            for (GamePartition gp : gps) {
                //logger.debug("Entered DP loop");
                gss[index].setData(conn, exclusionParameter, conditionString);
                appendEventPeriodReportXML(toAppend, gp, gss[index], grouping, exclusionMap.get(gp.getGameIndex()));
                index++;
            }
            ReportResults.clearLineCache();
            eventMap = null;
            categoryMap = null;
            productMap = null;
            barMap = null;
            stationMap = null;
        }
        if(callerId > 0){
                logger.portalVisitDetail(callerId, "getSummaryReport", location, "getGameTimeReport",0,10, "", conn);
            }
    }



    private void appendEventPeriodReportXML(Element toAppend, GamePartition gp, GamePeriodStructure gps, int grouping, HashSet barSet) {

        Element elPeriod = toAppend.addElement("game").addAttribute("id", String.valueOf(gp.getGameIndex()));
        elPeriod.addAttribute("date", String.valueOf(eventMap.getEventDate(gp.getGameIndex())));
        elPeriod.addAttribute("desc", HandlerUtils.nullToEmpty(eventMap.getEventDesc(gp.getGameIndex())));

        Map<GameSet, Double> gameMap = gps.getValues(conn);
        Map<Integer, String> standMap = gps.getBar(conn);
        int barId = -1, categoryId = -1;
        HashSet<Integer> barCount = new HashSet<Integer>();

        switch(grouping){
            case 1:
                if (null != gameMap && gameMap.size() > 0) {
                    double totalOunces = 0.0;
                    Element elEvent = elPeriod.addElement("data");
                    Set<GameSet> gameKeys = gameMap.keySet();
                    for (GameSet i : gameKeys) {
                        if (stationMap.hasStation(i.getStation()) && ((barSet != null && !barSet.contains(stationMap.getBar(i.getStation()))) || null == barSet)) {
                            if (barMap.hasBar(stationMap.getBar(i.getStation()))) {
                                barCount.add(stationMap.getBar(i.getStation()));
                                totalOunces += i.getFirstValue();
                            }
                        }
                    }
                    elEvent.addElement("value").addText(String.valueOf(totalOunces));
                }
                break;
            case 2:
                if (null != gameMap && gameMap.size() > 0) {
                    HashMap<Integer, Double> productSet = new HashMap<Integer, Double>();

                    double totalProductOunces = 0.0;
                    Element elEvent = elPeriod.addElement("data");
                    Set<GameSet> gameKeys = gameMap.keySet();
                    for (GameSet i : gameKeys) {
                        if (stationMap.hasStation(i.getStation()) && ((barSet != null && !barSet.contains(stationMap.getBar(i.getStation()))) || null == barSet)) {
                            if (barMap.hasBar(stationMap.getBar(i.getStation()))) {
                                barCount.add(stationMap.getBar(i.getStation()));
                                if (productSet.containsKey(i.getProduct())) {
                                    totalProductOunces = productSet.get(i.getProduct());
                                    totalProductOunces += i.getFirstValue();
                                    productSet.put(i.getProduct(), totalProductOunces);
                                } else {
                                    productSet.put(i.getProduct(), i.getFirstValue());
                                }
                            }
                        }
                    }
                    for (Integer product : productSet.keySet()) {
                        Element elProduct = elEvent.addElement("productData");
                        elProduct.addElement("product").addText(String.valueOf(product));
                        elProduct.addElement("productName").addText(HandlerUtils.nullToEmpty(productMap.getProduct(product)));
                        elProduct.addElement("value").addText(String.valueOf(productSet.get(product)));
                    }
                }
                break;
            case 3:
                Map<String, GameSet> gameStationMap = gps.getStationValues(conn);
                Map<Integer, String> locationMap = gps.getLocation(conn);
                Map<Integer, String> zoneMap = gps.getZone(conn);

                if (null != gameStationMap && gameStationMap.size() > 0) {
                    Element elEvent = null, elProduct= null;
                    Set<String> gameKeys = gameStationMap.keySet();
                    for (String gString : gameKeys) {
                        GameSet gs = gameStationMap.get(gString);
                        if ((barSet != null && !barSet.contains(Integer.valueOf(standMap.get(gs.getStation()).toString().split("\\|")[0]))) || null == barSet) {
                            categoryId = productMap.getCategory(gs.getProduct());
                            //logger.debug("Current Stand: " + standMap.get(gs.getStation()) + " Current Station: " + gs.getStation() + " for Current Game: " + gs.getGameId() + " with Current Product: " + gs.getProduct());
                            if (barMap.hasBar(Integer.valueOf(standMap.get(gs.getStation()).toString().split("\\|")[0]))) {
                                barCount.add(Integer.valueOf(standMap.get(gs.getStation()).toString().split("\\|")[0]));
                                if (barId != Integer.valueOf(standMap.get(gs.getStation()).toString().split("\\|")[0])) {
                                    barId = Integer.valueOf(standMap.get(gs.getStation()).toString().split("\\|")[0]);
                                    elEvent = elPeriod.addElement("data");
                                    elEvent.addAttribute("loc", locationMap.get(gs.getStation()).toString().split("\\|")[0]);
                                    elEvent.addAttribute("zone",zoneMap.get(gs.getStation()).toString().split("\\|")[0]);
                                    elEvent.addAttribute("bar", standMap.get(gs.getStation()).toString().split("\\|")[0]);
                                    elProduct = elEvent.addElement("prod");
                                    elProduct.addAttribute("id", String.valueOf(categoryId));
                                    elProduct.addAttribute("name", HandlerUtils.nullToEmpty(categoryMap.getCategory(categoryId)));
                                    elProduct.addAttribute("stat", String.valueOf(gs.getStation()));
                                    elProduct.addText(String.valueOf(gs.getFirstValue()));
                                } else if (barId == Integer.valueOf(standMap.get(gs.getStation()).toString().split("\\|")[0])) {
                                    elProduct = elEvent.addElement("prod");
                                    elProduct.addAttribute("id", String.valueOf(categoryId));
                                    elProduct.addAttribute("name", HandlerUtils.nullToEmpty(categoryMap.getCategory(categoryId)));
                                    elProduct.addAttribute("stat", String.valueOf(gs.getStation()));
                                    elProduct.addText(String.valueOf(gs.getFirstValue()));
                                }
                            }
                        }
                    }
                }
                break;
            case 4:
                if (null != gameMap && gameMap.size() > 0) {
                    double totalOunces = 0.0;
                    Element elEvent = null;
                    Set<GameSet> gameKeys = gameMap.keySet();
                    for (GameSet i : gameKeys) {
                        if (stationMap.hasStation(i.getStation()) && ((barSet != null && !barSet.contains(stationMap.getBar(i.getStation()))) || null == barSet)) {
                            if (barId != stationMap.getBar(i.getStation())) {
                                if(barId < 0) {
                                    totalOunces += i.getFirstValue();
                                } else {
                                    elEvent = elPeriod.addElement("data");
                                    elEvent.addAttribute("bar", String.valueOf(barId));
                                    elEvent.addAttribute("vol", String.valueOf(totalOunces));
                                    totalOunces = i.getFirstValue();
                                }
                                barId = stationMap.getBar(i.getStation());
                            } else {
                                totalOunces += i.getFirstValue();
                            }
                        }
                    }
                }
                break;
            default:
                break;
        }
        elPeriod.addAttribute("standCount", String.valueOf(barCount.size()));
    }

    private HashMap<Integer, HashSet> appendExclusionSummaryReportByEventXML(Element toAppend, ResultSet rs) throws HandlerException {
        HashMap<Integer, HashSet> exclusionMap = new HashMap<Integer, HashSet>();

        Element exclusionData = toAppend.addElement("exclusion");
        try {
            int exclusionCounter = 0;
            while (rs.next()) {
                // Adding excluded bar information
                int gameId = rs.getInt(1);
                int barId = rs.getInt(2);
                if(barMap.hasBar(barId)) {
                    if (exclusionMap.containsKey(gameId)) {
                        HashSet barSet = exclusionMap.get(gameId);
                        barSet.add(barId);
                        exclusionCounter++;
                    } else {
                        HashSet barSet = new HashSet();
                        barSet.add(barId);
                        exclusionCounter++;
                        exclusionMap.put(gameId,barSet);
                    }

                    Element game = exclusionData.addElement("game");
                    game.addAttribute("gameId", String.valueOf(gameId));
                    game.addAttribute("barId", String.valueOf(barId));
                    game.addText(HandlerUtils.nullToEmpty(getExclusionDesciption(rs.getInt(3))));
                }
            }
            Element standExclusion = exclusionData.addElement("standExclusions");
            standExclusion.addAttribute("count", String.valueOf(exclusionCounter));
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(rs);
        }
        return exclusionMap;
    }

    private void getLineCleaningReport(Element toHandle, Element toAppend) throws HandlerException {

        Date start                          = getRequiredDate(toHandle, "startDate");
        Date end                            = getRequiredDate(toHandle, "endDate");
        String periodStr                    = HandlerUtils.getRequiredString(toHandle, "periodType");
        String periodDetail                 = HandlerUtils.getRequiredString(toHandle, "periodDetail");
        PeriodShiftType periodShift         = PeriodShiftType.instanceOf("EntireDay");
        String periodShiftString            = HandlerUtils.getOptionalString(toHandle, "periodShift");
        if (null != periodShiftString) {
            periodShift                     = PeriodShiftType.instanceOf(HandlerUtils.getOptionalString(toHandle, "periodShift"));
        }
        int line = HandlerUtils.getOptionalInteger(toHandle, "lineId");
        int station = HandlerUtils.getOptionalInteger(toHandle, "stationId");
        int bar = HandlerUtils.getOptionalInteger(toHandle, "barId");
        int zone = HandlerUtils.getOptionalInteger(toHandle, "zoneId");
        int location = HandlerUtils.getOptionalInteger(toHandle, "locationId");
        int customer = HandlerUtils.getOptionalInteger(toHandle, "customerId");
        int supplier = HandlerUtils.getOptionalInteger(toHandle, "supplierId");
        int county = HandlerUtils.getOptionalInteger(toHandle, "countyId");
        int region = HandlerUtils.getOptionalInteger(toHandle, "regionId");
        int user = HandlerUtils.getOptionalInteger(toHandle, "userId");

        int parameter = 0;
        String conditionString = "";

        //new: filter by a specific product
        int product = HandlerUtils.getOptionalInteger(toHandle, "specificProduct");

        String byLineString = HandlerUtils.getOptionalString(toHandle, "byLine");
        String byProductString = HandlerUtils.getOptionalString(toHandle, "byProduct");
        boolean byLine = (!"false".equalsIgnoreCase(byLineString));
        boolean byProduct = (!"false".equalsIgnoreCase(byProductString));

        int paramsSet = 0;
        if (line >= 0) {
            parameter = line;
            conditionString = " WHERE l.id = ? ";
            paramsSet++;
        }
        if (zone >= 0) {
            parameter = zone;
            conditionString = " WHERE z.id = ? ";
            paramsSet++;
        }
        if (bar >= 0) {
            parameter = bar;
            conditionString = " WHERE b.id = ? ";
            paramsSet++;
        }
        if (station >= 0) {
            parameter = station;
            conditionString = " WHERE st.id = ? ";
            paramsSet++;
        }
        if (location >= 0) {
            parameter = location;
            conditionString = " WHERE loc.id = ? ";
            paramsSet++;
        }
        if (customer >= 0) {
            parameter = customer;
            conditionString = " WHERE loc.customer = ? ";
            if(region > 0) {
                conditionString             += " AND loc.region ="+String.valueOf(region);
            }
                
            start = setStartDate(periodShift.toSQLQueryInt(), customer, "", getRequiredDate(toHandle, "startDate"));
            end = setEndDate(periodShift.toSQLQueryInt(), customer, "", getRequiredDate(toHandle, "startDate"));
            periodDetail = String.valueOf(start.getHours() + 1);
            paramsSet++;
            
        }
        if (supplier >= 0) {
            parameter = supplier;
            conditionString = "LEFT JOIN locationSupplier lS on lS.location = loc.id " +
                    " LEFT JOIN supplierAddress sA ON sA.id = lS.address " +
                    " WHERE sA.supplier = ? ";
            paramsSet++;
        }
        if (county >= 0) {
            parameter = county;
            conditionString = " WHERE loc.countyIndex = ? ";
            paramsSet++;
        }       
        if (paramsSet != 1) {
            throw new HandlerException("Exactly one of the following must " +
                    "be set: lineId stationId barId zoneId locationId customerId supplierId countyId regionId userId");
        }

        PeriodType periodType = PeriodType.parseString(periodStr);
        if (null == periodType) {
            throw new HandlerException("Invalid period type: " + periodStr);
        }
        ReportPeriod period = null;
        try {
            period = new ReportPeriod(periodType, periodDetail,
                    start, end);
        } catch (IllegalArgumentException e) {
            throw new HandlerException(e.getMessage());
        }

        SortedSet<DatePartition> dps = DatePartitionFactory.createPartitions(period);
        //logger.debug("Created partitions: \n"+DatePartitionFactory.partitionReport(dps));
        DatePartitionTree dpt = new DatePartitionTree(dps);

        LineCleaningReportResults rrs = null;
        if (line >= 0) {
            rrs = LineCleaningReportResults.getResultsByLine(period, line, conn);
        } else if (zone >= 0) {
            rrs = LineCleaningReportResults.getResultsByZone(period, zone, product, conn);
        } else if (bar >= 0) {
            rrs = LineCleaningReportResults.getResultsByBar(period, bar, product, conn);
        } else if (station >= 0) {
            rrs = LineCleaningReportResults.getResultsByStation(period, station, product, conn);
        } else if (location >= 0) {
            rrs = LineCleaningReportResults.getResultsByLocation(period, location, product, conn);
        } else if (customer >= 0) {
            rrs = LineCleaningReportResults.getResultsByCustomer(period, customer, product, conn);
        } else if (supplier >= 0) {
            rrs = LineCleaningReportResults.getResultsBySupplier(period, supplier, product, conn);
        } else if (county >= 0) {
            rrs = LineCleaningReportResults.getResultsByCounty(period, county, product, conn);
        } else if (user >= 0) {
            rrs = LineCleaningReportResults.getResultsByUserRegion(period, user, product, conn);
        }

        PeriodStructure pss[] = null;
        PeriodStructure ps = null;
        int dpsSize = dps.size();
        int index = -1;

        if (dpsSize > 0) {
            pss = new PeriodStructure[dpsSize];
            //this is a temporary array of the DatePartitions used to construct the PeriodStructures start dates
            Object[] dpa = dps.toArray();
            for (int i = 0; i < dpsSize; i++) {
                // create a new PeriodStructure and link it to the previous one (or null for the first)
                ps = new PeriodStructure(ps, ((DatePartition) dpa[i]).getDate());
                pss[i] = ps;
            }
            int debugCounter = 0;
            try {
                int currentLine = 0;
                double value = 0.0;
                Date d = new Date();

                while (rrs.next()) {
                    //logger.debug("i:"+index+", dtp.index:"+dpt.getIndex(rrs.getDate())+", currentLine:"+currentLine+" rrs.line:"+rrs.getLine());
                    if (index < 0) {
                        index = dpt.getIndex(rrs.getDate());
                        currentLine = rrs.getLine();
                        value = rrs.getValue();
                        d = rrs.getDate();
                        //logger.debug("1#"+debugCounter+" ["+index+"]: "+"L "+currentLine+" V: "+value+" D: "+d.toString());
                    } else if ((index == dpt.getIndex(rrs.getDate())) && (currentLine == rrs.getLine())) {
                        value += rrs.getValue();
                        logger.debug("New Value = " + value);
                        //logger.debug("2#"+debugCounter+" ["+index+"]: "+"L "+currentLine+" V: "+value+" D: "+d.toString());
                    } else if ((index != dpt.getIndex(rrs.getDate())) && (currentLine == rrs.getLine())) {
                        pss[index].addReading(currentLine, value, d, 0.0);
                        debugCounter++;
                        //logger.debug("3#"+debugCounter+" ["+index+"]: "+"L "+currentLine+" V: "+value+" D: "+d.toString());

                        index = dpt.getIndex(rrs.getDate());
                        value = rrs.getValue();
                        d = rrs.getDate();
                    } else if (currentLine != rrs.getLine()) {
                        pss[index].addReading(currentLine, value, d, 0.0);
                        debugCounter++;
                        //logger.debug("4#"+debugCounter+" ["+index+"]: "+"L "+currentLine+" V: "+value+" D: "+d.toString());

                        index = dpt.getIndex(rrs.getDate());
                        currentLine = rrs.getLine();
                        value = rrs.getValue();
                        d = rrs.getDate();
                    } 
                }
                if (index > 0) {
                    pss[index].addReading(currentLine, value, d, 0.0);
                    debugCounter++;
                    //logger.debug("5#"+debugCounter+" ["+index+"]: "+"L "+currentLine+" V: "+value+" D: "+d.toString());
                }
                rrs.close();
            } catch (SQLException sqle) {
                throw new HandlerException(sqle);
            } finally {
            }
            logger.debug("Processed " + debugCounter + " readings");

            toAppend.addElement("startDate").addText(dateFormat.format(start));
            toAppend.addElement("endDate").addText(dateFormat.format(end));

            productMap = new ProductMap(conn);
            index = 0;
            for (DatePartition dp : dps) {
                // logger.debug("Entered DP loop");
                pss[index].setData(conn, parameter, conditionString);
                appendPeriodLineCleaningReportXML(toAppend, dp, pss[index], byLine, byProduct);

                index++;
            }
            LineCleaningReportResults.clearLineCache();

            productMap = null;
        }
    }

    private void appendPeriodLineCleaningReportXML(Element toAppend, DatePartition dp, PeriodStructure ps, boolean byLine, boolean byProduct) {

        Element elPeriod = toAppend.addElement("period");
        elPeriod.addElement("periodDate").addText(dateFormat.format(dp.getDate()));

        Map<Integer, Double> lineMap = ps.getLastValues(conn);
        Map<Integer, String> zoneMap = ps.getZone(conn);
        Map<Integer, String> barMap = ps.getBar(conn);
        Map<Integer, String> stationMap = ps.getStation(conn);
        Map<Integer, Integer> productIDMap = ps.getProduct(conn);
        Map<Integer, String> locationMap = ps.getLocation(conn);
        Map<Integer, Integer> productCategory = ps.getProductCategory(conn);

        if (null != lineMap && lineMap.size() > 0) {
            try {
             boolean isBrasstap          = false;
             int locCheck                = 0;
            if (byLine) {
                Set<Integer> lineKeys = lineMap.keySet();
                for (Integer i : lineKeys) {
                    int locationId = Integer.parseInt(locationMap.get(i).toString().split("\\|")[0]);
                    if(locCheck!=locationId){
                        isBrasstap = checkBrasstapLocation(1, locationId, conn);
                        locCheck    = locationId;
                    }
                    
                    Element elLine = elPeriod.addElement("line");
                    elLine.addElement("lineId").addText(i.toString());
                    elLine.addElement("lineName").addText(productMap.getProduct((i)));
                    elLine.addElement("locationName").addText(locationMap.get(i).toString().split("\\|")[1]);
                    elLine.addElement("productID").addText(productIDMap.get(i).toString());
                    //elLine.addElement("ProductName").addText(productMap.getProduct((productIDMap.get(i))));
                    elLine.addElement("productCategory").addText(productCategory.get(i).toString());
                    elLine.addElement("zoneName").addText(zoneMap.get(i).toString().split("\\|")[1]);
                    elLine.addElement("barName").addText(barMap.get(i).toString().split("\\|")[1]);
                    elLine.addElement("stationName").addText(stationMap.get(i).toString().split("\\|")[1]);
                    elLine.addElement("stationId").addText(stationMap.get(i).toString().split("\\|")[0]);
                    elLine.addElement("barId").addText(barMap.get(i).toString().split("\\|")[0]);
                    elLine.addElement("zoneId").addText(zoneMap.get(i).toString().split("\\|")[0]);
                    elLine.addElement("value").addText(lineMap.get(i).toString());
                    elLine.addElement("locationId").addText(locationMap.get(i).toString().split("\\|")[0]);
                    
                    if(!isBrasstap){                        
                        elLine.addElement("ProductName").addText(productMap.getProduct((productIDMap.get(i))));
                    } else {
                        elLine.addElement("ProductName").addText(HandlerUtils.nullToEmpty(getBrasstapProductName(locationId)));                        
                    }
                }
            }
            if (byProduct) {
                Map<Integer, ProductData> prMap = LineCleaningReportResults.linesToProducts(lineMap, conn);
                for (ProductData d : prMap.values()) {
                    int locationId = d.getLocation();
                     if(locCheck!=locationId){
                        isBrasstap = checkBrasstapLocation(1, locationId, conn);
                        locCheck    = locationId;
                    }
                    Element elProd = elPeriod.addElement("product");
                    elProd.addElement("productId").addText(String.valueOf(d.getId()));
                    //elProd.addElement("productName").addText(d.getName());
                    elProd.addElement("value").addText(String.valueOf(d.getPoured()));
                    elProd.addElement("unit").addText(String.valueOf(d.getUnits()));
                    
                    if(!isBrasstap){                        
                        elProd.addElement("productName").addText(d.getName());
                    } else {
                        elProd.addElement("productName").addText(HandlerUtils.nullToEmpty(getBrasstapProductName(locationId)));                        
                    }
                    
                }
            }
            } catch(Exception e){}
        }
            

    }

    
    /** Run a summary report from either a reportDescriptor String or individual parameters
     */
    private void summaryReport(Element toHandle, Element toAppend) throws HandlerException {
        ReportDescriptor rd = null;
        String rdToParse = HandlerUtils.getOptionalString(toHandle, "reportDescriptor");
        if (rdToParse != null && rdToParse.length() > 0) {
            // If a reportDescriptor is provided, parse it directly
            rd = new ReportDescriptor(rdToParse);
        } else {
            // Otherwise, take the individual parameters
            String startDate = HandlerUtils.getRequiredString(toHandle, "startDate");
            String endDate = HandlerUtils.getRequiredString(toHandle, "endDate");
            FilterType filterType = FilterType.instanceOf(HandlerUtils.getRequiredString(toHandle, "filterType"));
            String filterValue = HandlerUtils.getRequiredString(toHandle, "filterValue");
            GroupingType groupA = GroupingType.instanceOf(HandlerUtils.getRequiredString(toHandle, "grouping"));
            GroupingType groupB = GroupingType.instanceOf(HandlerUtils.getRequiredString(toHandle, "valueGrouping"));
            boolean includePoured = HandlerUtils.getOptionalBoolean(toHandle, "includePoured");
            boolean includeSold = HandlerUtils.getOptionalBoolean(toHandle, "includeSold");
            //validate parameters
            // valid date formats:  'YYYY-MM-DD' 'MM-DD-YYYY'  valid separators are - \ / 
            DateParameter validatedStartDate = new DateParameter(startDate);
            DateParameter validatedEndDate = new DateParameter(endDate);

            if (!validatedStartDate.isValid()) {
                logger.debug("Aborted report, invalid start date '" + startDate + "'");
                addErrorDetail(toAppend, "Invalid Start Date");
            } else if (!validatedEndDate.isValid()) {
                logger.debug("Aborted report, invalid end date '" + endDate + "'");
                addErrorDetail(toAppend, "Invalid End Date");
            } else {
                rd = new ReportDescriptor(groupA, groupB,
                        filterType, filterValue, validatedStartDate.toString(), validatedEndDate.toString(), includePoured, includeSold, false);
            }
        }

        int cacheSize = HandlerUtils.getOptionalInteger(toHandle, "cacheSize");
        cacheSize = (cacheSize <= 0) ? 50 : cacheSize;
        boolean attachLegend = HandlerUtils.getOptionalBoolean(toHandle, "attachLegend");
        if (rd != null) {
            ElapsedTime totalTime = new ElapsedTime("Total Time");
            logger.debug("Running Report: '" + rd.toString() + "'");
            logger.debug("Initializing SummaryFactory");
            SummaryFactory sf = new SummaryFactory(rd, conn);
            logger.debug("Initializing Grouper");
            Grouper grouper = new Grouper(rd, sf, conn, cacheSize);
            logger.debug("Processing Summaries...");
            Map<GroupingKey, GroupBox> result = grouper.process();
            logger.debug("Summaries Complete, formatting...");
            XmlFormatter output = new XmlFormatter(rd, result, conn);
            output.format(toAppend, attachLegend);
            //  Add the query times
            totalTime.stopTimer();
            ArrayList<ElapsedTime> timers = new ArrayList<ElapsedTime>();
            timers.addAll(sf.getQueryTimes());
            timers.addAll(grouper.getBenchmarkData());
            timers.addAll(output.getBenchmarkData());
            timers.add(totalTime);
            for (ElapsedTime et : timers) {
                String timeStr = et.toString();
                addNoteDetail(toAppend, timeStr);
                logger.debug("     Time Report: " + timeStr);
            }
            logger.debug("Test Summary Complete");

        }
    }

    /**  This method takes a product/location and a time period and provides the maximum
     * possible resolution to the quantity poured during the period.  The readings will
     * start at zero, and will be in ounces
     *
     * arguments:
     * <locationId>
     * <productId>
     * <startDate>
     * <endDate>
     * result:
     * <reading>
     *   <timestamp>Apr 03 2005 14:35:00</timestamp>
     *   <reading>45.345</reading>
     * <reading>
     * <reading/>
     *  ...
     * <reading/>
     */
    private void getDataDelayReport(Element toHandle, Element toAppend) throws HandlerException {

        int componentId = HandlerUtils.getRequiredInteger(toHandle, "componentId");
        int modExcluded = HandlerUtils.getOptionalInteger(toHandle, "modExcluded");
        SQLUpdateHandler updateHandler   = new SQLUpdateHandler();
        String selectComponentInfo = " SELECT cLM.id, cLM.location, cLM.lastCheckId FROM componentLocationMap cLM WHERE cLM.component = ? ";
        String selectNewDataModInfo = " SELECT dM.id, dM.modType, dM.modId, dM.start, dM.end FROM dataModNew dM " +
                " WHERE dM.location = ? AND dM.id > ? ";
        if (modExcluded > 0) {
            selectNewDataModInfo += " AND dM.modType NOT IN (?) ";
        }
        selectNewDataModInfo += " ORDER BY dM.id; ";
        String selectLocation = " SELECT l.id, l.name FROM location l LEFT JOIN locationDetails lD ON lD.location = l.id WHERE lD.active = 1 ";
        String selectBar = " SELECT b.id, b.name FROM bar b WHERE b.location = ? ";

        int locationId = 0, location = 0, lastCheckId = 0, newLastCheckId = 0, modType = 0, bar = 0, componentMapId = 0, i = 0;
        String locationName = null, barName = null;
        PreparedStatement stmt = null;
        ResultSet rs = null, rsLocation = null, rsNewData = null, rsBar = null;

        try {
            stmt = conn.prepareStatement(selectComponentInfo);
            stmt.setInt(1, componentId);
            rs = stmt.executeQuery();
            while (rs.next()) {
                componentMapId = rs.getInt(1);
                locationId = rs.getInt(2);
                lastCheckId = rs.getInt(3);
                newLastCheckId = lastCheckId;

                if (locationId > 0) {
                    selectLocation += " AND l.id = " + locationId + ";";
                }
                stmt = conn.prepareStatement(selectLocation);
                rsLocation = stmt.executeQuery();
                while (rsLocation.next()) {
                    location = rsLocation.getInt(1);
                    locationName = rsLocation.getString(2);

                    stmt = conn.prepareStatement(selectNewDataModInfo);
                    stmt.setInt(1, location);
                    stmt.setInt(2, lastCheckId);
                    if (modExcluded > 0) {
                        stmt.setInt(3, modExcluded);
                    }
                    rsNewData = stmt.executeQuery();

                    Element elLocation = toAppend.addElement("location");
                    elLocation.addElement("locationId").addText(String.valueOf(location));
                    elLocation.addElement("locationName").addText(HandlerUtils.nullToEmpty(locationName));

                    while (rsNewData.next()) {
                        newLastCheckId = rsNewData.getInt(1);
                        modType = rsNewData.getInt(2);
                        int product = rsNewData.getInt(3);
                        String startStr = rsNewData.getString(4);
                        String endStr = rsNewData.getString(5);

                        stmt = conn.prepareStatement(selectBar);
                        stmt.setInt(1, location);
                        rsBar = stmt.executeQuery();
                        while (rsBar.next()) {
                            bar = rsBar.getInt(1);
                            barName = rsBar.getString(2);

                            Element elBar = elLocation.addElement("bar");
                            elBar.addElement("barId").addText(String.valueOf(bar));
                            elBar.addElement("barName").addText(HandlerUtils.nullToEmpty(barName));

                            Date start = null, end = null;
                            try {
                                start = newDateFormat.parse(startStr);
                                end = newDateFormat.parse(endStr);
                            } catch (ParseException pe) {
                                String badDate = (null == start) ? "start" : "end";
                                throw new HandlerException("Could not parse " + badDate + " date.");
                            }
                            start.setMinutes(0);
                            end.setMinutes(0);
                            start.setSeconds(1);
                            end.setSeconds(0);

                            String perdiodDetail = String.valueOf(start.getHours() + 1);

                            ReportPeriod period = new ReportPeriod(PeriodType.HOURLY, perdiodDetail, start, end);

                            switch (modType) {
                                case 1:
                                    break;
                                case 2:
                                    break;
                                case 3:
                                    break;
                                case 4:
                                    break;
                                default:
                                    break;
                            }

                            Element pouredPeriod = elBar.addElement("poured");
                            sendPouredData(pouredPeriod, bar, product, period, start, end);

                            Element soldPeriod = elBar.addElement("sold");
                            //sendSoldData(soldPeriod, location, bar, product, period, start, end);
                        }
                    }
                }
                updateHandler.updateComponentStatus(newLastCheckId, componentMapId);
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(rsBar);
            close(rsNewData);
            close(rs);
            close(stmt);
        }
    }

    private class startEndDate {

        private Date start = null, end = null;

        public startEndDate(Date startDate, Date endDate) {
            start = startDate;
            end = endDate;
        }

        public Date getStartDate() {
            return start;
        }

        public Date getEndDate() {
            return end;
        }
    }

    private void sendPouredData(Element toAppend, int bar, int product, ReportPeriod period, Date start, Date end) throws HandlerException {

        SortedSet<DatePartition> dps = DatePartitionFactory.createPartitions(period);
        //logger.debug("Created partitions: \n" + DatePartitionFactory.partitionReport(dps));
        DatePartitionTree dpt = new DatePartitionTree(dps);

        ReportResults rrs = ReportResults.getResultsByBar(period, 0, false, bar, product, conn);

        PeriodStructure pss[] = null;
        PeriodStructure ps = null;
        int dpsSize = dps.size();
        int index;
        if (dpsSize > 0) {
            pss = new PeriodStructure[dpsSize];
            //this is a temporary array of the DatePartitions used to construct the PeriodStructures start dates
            Object[] dpa = dps.toArray();
            for (int i = 0; i < dpsSize; i++) {
                // create a new PeriodStructure and link it to the previous one (or null for the first)
                ps = new PeriodStructure(ps, ((DatePartition) dpa[i]).getDate());
                pss[i] = ps;
            }
            int debugCounter = 0;
            try {
                while (rrs.next()) {
                    index = dpt.getIndex(rrs.getDate());
                    pss[index].addReading(rrs.getLine(), rrs.getValue(), rrs.getDate(), rrs.getQuantity());
                    debugCounter++;
                    if (debugCounter % 20 == 0) {
                        //log the reading for debugging
                        //logger.debug("#"+debugCounter+" ["+index+"]: "+"L "+rrs.getLine()+" V: "+rrs.getValue()+" D: "+rrs.getDate().toString());
                    }
                }
                rrs.close();
            } catch (SQLException sqle) {
                throw new HandlerException(sqle);
            } finally {
            }
            logger.debug("Processed " + debugCounter + " readings");

            productMap = new ProductMap(conn);
            index = 0;
            for (DatePartition dp : dps) {
                // logger.debug("Entered DP loop");
                appendDataModPouredXML(toAppend, dp, pss[index]);
                index++;
            }
            ReportResults.clearLineCache();
            productMap = null;
        }
    }

    private void appendDataModPouredXML(Element toAppend, DatePartition dp, PeriodStructure ps) {

        Element elPeriod = toAppend.addElement("period");
        elPeriod.addElement("periodDate").addText(dateFormat.format(dp.getDate()));

        Map<Integer, Double> lineMap = ps.getValues(conn);
        Map<Integer, Integer> productIDMap = ps.getProduct(conn);

        if (null != lineMap && lineMap.size() > 0) {

            Set<Integer> lineKeys = lineMap.keySet();
            for (Integer i : lineKeys) {
                Element elLine = elPeriod.addElement("line");
                elLine.addElement("lineId").addText(i.toString());
                elLine.addElement("productID").addText(productIDMap.get(i).toString());
                elLine.addElement("value").addText(lineMap.get(i).toString());
            }
        }
    }

    /*private void sendSoldData(Element toAppend, int location, int bar, int productFilter, ReportPeriod period, Date start, Date end)
            throws HandlerException {
        String selectCostCenter = "SELECT id FROM costCenter WHERE location = ?";
        PreparedStatement stmt = null;
        ResultSet rs1 = null;
        int costCenters = 0;
        SalesResults rs = null;

        // A map of all product ids and their names
        Map<Integer, String> productNames = new HashMap<Integer, String>();

        // A cache of beverage ingredient sets (maps PLU -> RRecSet)
        Map<String, Set<ReconciliationRecord>> ingredCache = new HashMap<String, Set<ReconciliationRecord>>();

        // Maps Period Ids to ProductSets(product ids to RRecs (oz values));
        HashMap<Integer, HashMap<Integer, ReconciliationRecord>> resultSet = new HashMap<Integer, HashMap<Integer, ReconciliationRecord>>();

        SortedSet<DatePartition> dps = DatePartitionFactory.createPartitions(period);
        //logger.debug("Created partitions: \n" + DatePartitionFactory.partitionReport(dps));
        DatePartitionTree dpt = new DatePartitionTree(dps);

        try {

            rs = SalesResults.getResultsByBar(period, bar, conn);

            int totalRecords = 0;
            int rsCounter = 0;
            while (rs.next()) {
                rsCounter++;
                String product = rs.getPlu();
                double value = rs.getValue();
                Date timestamp = rs.getDate();

                Set<ReconciliationRecord> baseSet;
                baseSet = getBaseSetFromCache(product, location, bar, ingredCache);

                Set<ReconciliationRecord> rSet = ReconciliationRecord.recordByBaseSet(baseSet, value);
                totalRecords += rSet.size();
                // loop through all the returned RRs and add them to the appropriate product set.
                for (ReconciliationRecord rr : rSet) {

                    // here is where we check the product filter (used to be in the query)
                    if (productFilter > 0 && productFilter != rr.getProductId()) {
                        continue;
                    }

                    //NischaySharma_12-Feb-2009_Start: Setting the Zone, Bar and Station Names + Ids in the ReconciliationRecord
                    rr.setStation(stationName);
                    rr.setBar(barName);
                    rr.setZone(zoneName);
                    rr.setStationId(stationId);
                    rr.setBarId(barId);
                    rr.setZoneId(zoneId);
                    //Nischaysharma_12-Feb-2009_End

                    Integer productKey = new Integer(rr.getProductId());
                    Integer dateKey = new Integer(dpt.getIndex(timestamp));

                    //Add the product ID to our master List
                    if (!productNames.containsKey(productKey)) {
                        productNames.put(productKey, rr.getName());
                        //logger.debug("Added " + rr.getName() + " (" + productKey + ") to productNames");
                    }

                    //Warning: the following debug statement will spam the logs. For large reports, expect thousands.
                    //logger.debug(":>" + timestamp + "  (" + dateKey + ")[" + product + "]");

                    HashMap<Integer, ReconciliationRecord> productSet = resultSet.get(dateKey);
                    // add a new productSet if it doesn't exist
                    if (productSet == null) {
                        productSet = new HashMap<Integer, ReconciliationRecord>();
                        resultSet.put(dateKey, productSet);
                    }

                    // get the rrec (creating a new one if needed)
                    ReconciliationRecord existingRecord = productSet.get(productKey);
                    if (existingRecord != null) {
                        //logger.debug("Adding Volume for plu: " + product + " with product: " + productKey + " with value: " + rr.getValue() + " at time: " + timestamp + " for location: " + locationMap.getLocationByBar(Integer.parseInt(rr.getBarId())));
                        existingRecord.add(rr);
                    } else {
                        productSet.put(productKey, rr);
                    }
                }
            }
            logger.debug("Processed " + totalRecords + " (" + rsCounter + ") reconciliation record(s)");

            // create the XML response
            int index = 0;
            for (DatePartition dp : dps) {
                //logger.debug("Entered DP loop");
                Integer dateKey = new Integer(index);
                HashMap<Integer, ReconciliationRecord> productSet = resultSet.get(dateKey);

                Collection<ReconciliationRecord> recs = new HashSet<ReconciliationRecord>();
                if (productSet != null) {
                    recs = productSet.values();
                }

                Element periodEl = toAppend.addElement("period");
                periodEl.addElement("periodDate").addText(dateFormat.format(dp.getDate()));
                // iterate through our rrecs
                Set<Integer> masterList = new HashSet<Integer>(productNames.keySet());
                // use the product set to create the XML to return.
                //logger.debug("Period " + index + ": " + recs.size() + " record(s)");
                for (ReconciliationRecord r : recs) {
                    Element p = periodEl.addElement("product");
                    p.addElement("zoneId").addText(r.getZoneId());
                    p.addElement("barId").addText(r.getBarId());
                    p.addElement("stationId").addText(r.getStationId());
                    p.addElement("product").addText(String.valueOf(r.getProductId()));
                    p.addElement("ounces").addText(String.valueOf(r.getValue()));
                    p.addElement("name").addText(r.getName());
                    masterList.remove(new Integer(r.getProductId()));
                }
                // add empty entries for products that had no sales
                if ((masterList.size() > 0)) {
                    //logger.debug("Adding " + masterList.size() + " empty records");
                    for (Integer pid : masterList) {
                        Element p = periodEl.addElement("product");
                        p.addElement("product").addText(String.valueOf(pid.intValue()));
                        p.addElement("ounces").addText("0.0");
                        p.addElement("name").addText(HandlerUtils.nullToEmpty(productNames.get(pid)));
                    }
                }
                index++;
            }
        } catch (SQLException sqle) {
            throw new HandlerException(sqle);
        } finally {
            close(rs1);
            close(stmt);
        }
    }*/

    /**  This method takes a product/location and a time period and provides the maximum
     * possible resolution to the quantity poured during the period.  The readings will
     * start at zero, and will be in ounces
     *
     * arguments:
     * <locationId> 
     * <productId>
     * <startDate>
     * <endDate>
     * result:
     * <reading>
     *   <timestamp>Apr 03 2005 14:35:00</timestamp>
     *   <reading>45.345</reading>
     * <reading>
     * <reading/>
     *  ...
     * <reading/>
     */
    private void getRealTimeReport(Element toHandle, Element toAppend) throws HandlerException {
        // TODO: check param
        int location = HandlerUtils.getRequiredInteger(toHandle, "locationId");
        int product = HandlerUtils.getRequiredInteger(toHandle, "productId");
        String startStr = HandlerUtils.getRequiredString(toHandle, "startDate");
        String endStr = HandlerUtils.getRequiredString(toHandle, "endDate");
        boolean includePoured = HandlerUtils.getOptionalBoolean(toAppend, "includePoured", true);
        boolean includeSold = HandlerUtils.getOptionalBoolean(toAppend, "includeSold", true);


        boolean lineCleaning = HandlerUtils.getOptionalBoolean(toHandle, "lineCleaning");
        Date start = null, end = null;
        try {
            start = dateFormat.parse(startStr);
            end = dateFormat.parse(endStr);
        } catch (ParseException pe) {
            String badDate = (null == start) ? "start" : "end";
            throw new HandlerException("Could not parse " + badDate + " date.");
        }

        ReportPeriod period = new ReportPeriod(PeriodType.FULL, "0", start, end);

        if (includePoured) {
            ReportResults rrs = ReportResults.getResultsByLocation(period, 0, false, lineCleaning, location, product, conn);
            /*  The results will be in ascending order */
            Vector<LineReading> result = new Vector<LineReading>();
            double offset = 0.0;
            int resultCount = 0;
            int pruned = 0;
            try {
                while (rrs.next()) {
                    resultCount++;
                    LineReading reading = new LineReading(0, rrs.getValue() + offset, rrs.getDate());
                    // first reading
                    if (result.isEmpty()) {
                        result.add(reading);
                    } else {
                        LineReading last = result.lastElement();
                        double difference = reading.getValueDifference(last);
                        if (difference > 1) {
                            result.add(reading);
                        } else if (difference < -1) {
                            // if the reading is negative, we have a spike or line-clear.
                            // we should ignore the reading, and add the difference to subsequent readings
                            offset += (-difference);
                        } else {
                            // no change, so we may be able to remove the previous reading
                            if (result.size() > 1) {
                                LineReading lastlast = result.elementAt(result.size() - 2);
                                if (last.getValueDifference(lastlast) > 1) {
                                    // keep the reading
                                } else {
                                    // remove the last reading
                                    result.removeElementAt(result.size() - 1);
                                    pruned++;
                                }
                            }
                            result.add(reading);
                        }
                    }
                }
            } catch (SQLException sqle) {
                logger.dbError(sqle.toString());
            } finally {
                rrs.close();
            }
            logger.debug("Processed " + resultCount + " readings, pruned " + pruned + ", offset is " + offset);
            //process the result Vector into an XML response
            if (!result.isEmpty()) {
                double baseValue = result.firstElement().getValue();
                for (LineReading lr : result) {
                    Element r = toAppend.addElement("pouredReading");
                    r.addElement("timestamp").addText(lr.getDate().toString());
                    r.addElement("value").addText(String.valueOf(lr.getValue() - baseValue));
                }
            }
        }
        if (includeSold) {

            SalesResults sr = SalesResults.getResultsByLocation(period, 0, location, conn);
            Map<String, Set<ReconciliationRecord>> ingredCache = new HashMap<String, Set<ReconciliationRecord>>();
            LinkedList<ReportSummary> result = new LinkedList<ReportSummary>();

            int rsCounter = 0;
            try {
                double total = 0D;
                while (sr.next()) {
                    rsCounter++;
                    String plu = sr.getPlu();
                    double value = sr.getValue();

                    Set<ReconciliationRecord> baseSet = getBaseSetFromCache(plu, location, 0, ingredCache);
                    Set<ReconciliationRecord> rSet = ReconciliationRecord.recordByBaseSet(baseSet, value);
                    // loop through all the returned RRs and add them to the appropriate product set.  
                    for (ReconciliationRecord rr : rSet) {
                        // here is where we check the product filter (used to be in the query)
                        if (product > 0 && product != rr.getProductId()) {
                            continue;
                        }
                        total += rr.getValue();
                        result.add(new ReportSummary(location, product, total,
                                sr.getDate().toString(), OunceType.SOLD));
                    }
                }
            } catch (SQLException sqle) {
                logger.dbError(sqle.toString());
            } finally {
                sr.close();
            }
            logger.debug("Processed " + rsCounter + " sales records");
            //process the result Vector into an XML response
            if (!result.isEmpty()) {
                for (ReportSummary rs : result) {
                    Element r = toAppend.addElement("soldReading");
                    r.addElement("timestamp").addText(rs.getTimestamp());
                    r.addElement("value").addText(String.valueOf(rs.getOunces()));
                }
            }
        }
    }

    /**
     *  TODO:  Document ths method
     */
    private void getSalesReport(Element toHandle, Element toAppend) throws HandlerException {

        int location = HandlerUtils.getRequiredInteger(toHandle, "locationId");
        int zone = HandlerUtils.getOptionalInteger(toHandle, "zoneId");
        int bar = HandlerUtils.getOptionalInteger(toHandle, "barId");
        int station = HandlerUtils.getOptionalInteger(toHandle, "stationId");
        String start = HandlerUtils.getRequiredString(toHandle, "startDate");
        String end = HandlerUtils.getRequiredString(toHandle, "endDate");

        int CostCenters = 0;

        //todo:  make sure start & end are valid date strings
        logger.debug("location" + String.valueOf(location));
        logger.debug("start" + start);
        logger.debug("end" + end);


        PreparedStatement stmt = null;
        ResultSet rs = null;

        String selectSales = "SELECT pluNumber, quantity FROM sales WHERE location = ? " +
                " AND date BETWEEN ? AND ? ";

        String selectZoneSales = "SELECT s.pluNumber, s.quantity FROM sales s left join station st on st.id = s.station left join bar b on b.id = st.bar WHERE s.location = ? and b.zone = ? " +
                " AND s.date BETWEEN ? AND ? ";

        String selectBarSales = "SELECT s.pluNumber, s.quantity FROM sales s left join station st on st.id = s.station WHERE s.location = ? and st.bar = ? " +
                " AND s.date BETWEEN ? AND ? ";

        String selectCostCenter = "SELECT ccID FROM costCenter WHERE location = ? ";

        String selectZoneCostCenterSales = "SELECT s.pluNumber, s.quantity FROM sales s left join costCenter c on c.ccID = s.costCenter WHERE s.location = ? and c.zone = ? " +
                " AND s.date BETWEEN ? AND ? ";

        String selectBarCostCenterSales = "SELECT s.pluNumber, s.quantity FROM sales s left join costCenter c on c.ccID = s.costCenter WHERE s.location = ? and c.bar = ? " +
                " AND s.date BETWEEN ? AND ? ";

        String selectStationCostCenterSales = "SELECT s.pluNumber, s.quantity FROM sales s left join costCenter c on c.ccID = s.costCenter WHERE s.location = ? and c.station = ? " +
                " AND s.date BETWEEN ? AND ? ";

        String selectStationSales = "SELECT pluNumber, quantity FROM sales WHERE location = ? and station = ?" +
                " AND date BETWEEN ? AND ? ";

        // A cache of beverage ingredient sets (maps PLU -> RRecSet)
        Map<String, Set<ReconciliationRecord>> ingredCache = new HashMap<String, Set<ReconciliationRecord>>();

        // Maps product ids to RRecs (oz values);
        Map<Integer, ReconciliationRecord> productSet = new HashMap<Integer, ReconciliationRecord>();

        try {

            stmt = conn.prepareStatement(selectCostCenter);
            stmt.setInt(1, location);
            rs = stmt.executeQuery();
            if (rs.next()) {
                logger.debug("Found Cost Centers for Multiple Stations");
                CostCenters = 1;
            }

            if ((zone >= 0) && (CostCenters <= 0)) {
                stmt = conn.prepareStatement(selectZoneSales);
                stmt.setInt(1, location);
                stmt.setInt(2, zone);
                stmt.setString(3, start);
                stmt.setString(4, end);
                logger.debug("Executing getSalesReport for Zones query");
            } else if ((zone >= 0) && (CostCenters > 0)) {
                stmt = conn.prepareStatement(selectZoneCostCenterSales);
                stmt.setInt(1, location);
                stmt.setInt(2, zone);
                stmt.setString(3, start);
                stmt.setString(4, end);
                logger.debug("Executing getSalesReport for Zones Cost Centers query");
            } else if ((bar >= 0) && (CostCenters <= 0)) {
                stmt = conn.prepareStatement("SELECT COUNT(id), id FROM bar WHERE location = ? ORDER BY id ASC");
                stmt.setInt(1, location);
                rs = stmt.executeQuery();
                if (rs.next() && (rs.getInt(1) > 1) && (bar != rs.getInt(2))) {
                    location = -1;
                }
                stmt = conn.prepareStatement(selectSales);
                stmt.setInt(1, location);
                stmt.setString(2, start);
                stmt.setString(3, end);
                logger.debug("Executing getSalesReport for Bars query");
            } else if ((bar >= 0) && (CostCenters > 0)) {
                stmt = conn.prepareStatement(selectBarCostCenterSales);
                stmt.setInt(1, location);
                stmt.setInt(2, bar);
                stmt.setString(3, start);
                stmt.setString(4, end);
                logger.debug("Executing getSalesReport for Bars Cost Centers query");
            } else if ((station >= 0) && (CostCenters <= 0)) {
                stmt = conn.prepareStatement(selectStationSales);
                stmt.setInt(1, location);
                stmt.setInt(2, station);
                stmt.setString(3, start);
                stmt.setString(4, end);
                logger.debug("Executing getSalesReport for Stations query");
            } else if ((station >= 0) && (CostCenters > 0)) {
                stmt = conn.prepareStatement(selectStationCostCenterSales);
                stmt.setInt(1, location);
                stmt.setInt(2, station);
                stmt.setString(3, start);
                stmt.setString(4, end);
                logger.debug("Executing getSalesReport for Stations Cost Center query");
            } else {
                stmt = conn.prepareStatement(selectSales);
                stmt.setInt(1, location);
                stmt.setString(2, start);
                stmt.setString(3, end);
                logger.debug("Executing getSalesReport query");
            }

            if (CostCenters > 0) {
                bar = 0;
                logger.debug("Cost Center Account");
            }

            long startMillis = System.currentTimeMillis();
            rs = stmt.executeQuery();
            long elapsedTime = System.currentTimeMillis() - startMillis;
            logger.debug("Query complete took " + elapsedTime + " ms");

            int totalRecords = 0;
            while (rs.next()) {
                String product = rs.getString(1);
                double value = rs.getDouble(2);

                Set<ReconciliationRecord> baseSet = null;
                if (ingredCache.containsKey(product)) {
                    baseSet = ingredCache.get(product);
                } else { // we need to do a db lookup and add the ingredients to the cache
                    baseSet = ReconciliationRecord.recordByPlu(product, location, bar, 1.0, conn);
                    ingredCache.put(product, baseSet);
                }
                Set<ReconciliationRecord> rSet = ReconciliationRecord.recordByBaseSet(baseSet, value);
                totalRecords += rSet.size();
                // loop through all RRs and add them to the product set.  
                for (ReconciliationRecord rr : rSet) {
                    Integer key = new Integer(rr.getProductId());
                    ReconciliationRecord existingRecord = productSet.get(key);
                    if (existingRecord != null) {
                        existingRecord.add(rr);
                    } else {
                        productSet.put(key, rr);
                    }
                }
            }
            logger.debug("Processed " + totalRecords + " reconciliation record(s)");

            Element periodEl = toAppend.addElement("period");
            periodEl.addElement("periodDate").addText(HandlerUtils.nullToEmpty(start));

            // use the product set to create the XML to return.
            Collection<ReconciliationRecord> recs = productSet.values();
            for (ReconciliationRecord r : recs) {
                Element p = toAppend.addElement("product");
                p.addElement("product").addText(String.valueOf(r.getProductId()));
                p.addElement("ounces").addText(String.valueOf(r.getValue()));
                p.addElement("name").addText(HandlerUtils.nullToEmpty(r.getName()));
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(rs);
            close(stmt);
        }
    }

    private HashMap<String,Integer> getCostCenterMap(Integer tableType, Integer tableId, String tableString) throws HandlerException {
        PreparedStatement stmt              = null;
        ResultSet rs                        = null;

        HashMap<String,Integer> costCenterMap
                                            = new HashMap<String,Integer>();

        try {
            String selectAlertDescription   = "";


            switch (tableType) {
                case 1:
                    selectAlertDescription  += " SELECT c.id, c.ccId, l.id FROM costCenter c LEFT JOIN location l ON l.id = c.location WHERE l.customer = ? ";
                    break;

                case 2:
                    selectAlertDescription  += " SELECT id, ccId, location FROM costCenter WHERE location = ? ";
                    break;

                case 3:
                    selectAlertDescription  += " SELECT id, ccId, location FROM costCenter WHERE zone = ? ";
                    break;

                case 4:
                    selectAlertDescription  += " SELECT id, ccId, location FROM costCenter WHERE bar = ?";
                    break;

                case 5:
                    selectAlertDescription  += " SELECT id, ccId, location FROM costCenter WHERE station = ? ";
                    break;

                case 6:
                    selectAlertDescription  += " SELECT id, ccId, location FROM costCenter WHERE id > ? AND location in (" + tableString + ")";
                    break;
            }
            stmt                            = conn.prepareStatement(selectAlertDescription);
            stmt.setInt(1, tableId);
            rs                              = stmt.executeQuery();
            while (rs.next()) {
                costCenterMap.put(rs.getString(3) + "-" + rs.getString(2),rs.getInt(1));
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(rs);
            close(stmt);
        }
        return costCenterMap;
    }

    /**
     *  This method supports subdivision of the period and product filtering
     *
     *  This method now supports and optional barId
     *  If the barId is set, then only PLUs matching this bar will be returned.
     *  If the barId is not set, then all PLUs will be returned.
     */
    private void getSalesReportNew(Element toHandle, Element toAppend) throws HandlerException {
        int callerId                        = getCallerId(toHandle);
        int customer                        = HandlerUtils.getOptionalInteger(toHandle, "customerId");
        int location                        = HandlerUtils.getOptionalInteger(toHandle, "locationId");
        int zone                            = HandlerUtils.getOptionalInteger(toHandle, "zoneId");
        int regionId                        = HandlerUtils.getOptionalInteger(toHandle, "regionId");
        int bar                             = HandlerUtils.getOptionalInteger(toHandle, "barId");
        int station                         = HandlerUtils.getOptionalInteger(toHandle, "stationId");
        int productFilter                   = HandlerUtils.getOptionalInteger(toHandle, "specificProduct");
        int btype                           = HandlerUtils.getOptionalInteger(toHandle, "type");
        int ptype                           = HandlerUtils.getOptionalInteger(toHandle, "ptype");
        boolean forChart                    = HandlerUtils.getOptionalBoolean(toHandle, "forChart");
        boolean byProduct                   = HandlerUtils.getOptionalBoolean(toHandle, "byProduct");
        String specificLocationsString      = HandlerUtils.getOptionalString(toHandle, "specificLocations");
        String startStr                     = HandlerUtils.getRequiredString(toHandle, "startDate");
        String endStr                       = HandlerUtils.getRequiredString(toHandle, "endDate");
        String periodStr                    = HandlerUtils.getRequiredString(toHandle, "periodType");
        String periodDetail                 = HandlerUtils.getRequiredString(toHandle, "periodDetail");
        if (forChart && productFilter > 0) {
            periodStr                       = "MINUTELY";
            periodDetail                    = "10";
        }
        PeriodShiftType periodShift         = PeriodShiftType.instanceOf("EntireDay");
        String periodShiftString            = HandlerUtils.getOptionalString(toHandle, "periodShift");
        if (null != periodShiftString) {
            periodShift                     = PeriodShiftType.instanceOf(HandlerUtils.getOptionalString(toHandle, "periodShift"));
        }
        boolean byGroup                     = ("true".equalsIgnoreCase(HandlerUtils.getOptionalString(toHandle, "byGroup")));
        
        boolean isBrasstap                  = false;
        
        int paramsSet = 0;
        if (station >= 0) {
            paramsSet++;
        }
        if (bar >= 0) {
            paramsSet++;
        }
        if (zone >= 0) {
            paramsSet++;
        }
        if (location >= 0) {
            paramsSet++;
            isBrasstap                      = checkBrasstapLocation(1, location, conn);
        }
        if (customer >= 0) {
            isBrasstap                      = checkBrasstapLocation(2, customer, conn);
            if(regionId > 0){
                specificLocationsString     =getRegionLocations(customer,regionId);
            }
            paramsSet++;
        }
        if (specificLocationsString != null && specificLocationsString.length() > 0) {
            paramsSet = 0;
            paramsSet++;
        }
        if (paramsSet != 1) {
            throw new HandlerException("Exactly one of the following must be set: customerId locationId zoneId barId station");
        }

        // A cache of beverage ingredient sets (maps PLU -> RRecSet)
        Map<String, Set<ReconciliationRecord>> ingredCache
                                            = new HashMap<String, Set<ReconciliationRecord>>();
        // Maps Period Ids to ProductSets(product ids to RRecs (oz values));
        HashMap<Integer, HashMap<String, ReconciliationRecord>> resultSet
                                            = new HashMap<Integer, HashMap<String, ReconciliationRecord>>();
        // Map for costCenters
        HashMap<String,Integer> costCenterMap
                                            = new HashMap<String,Integer>();


        //parse the dates
        Date start                          = null, end = null;
        try {
            start                           = dateFormat.parse(startStr);
            end                             = dateFormat.parse(endStr);
            if (customer >= 0) {
                start                       = setStartDate(periodShift.toSQLQueryInt(), customer, specificLocationsString, dateFormat.parse(startStr));
                end                         = setEndDate(periodShift.toSQLQueryInt(), customer, specificLocationsString, dateFormat.parse(startStr));
                periodDetail                = String.valueOf(start.getHours() + 1);
                locationMap                 = new LocationMap(conn, periodShift.toSQLQueryInt(), customer, dateToString(dateFormat.parse(startStr)));
            } else if (specificLocationsString != null && specificLocationsString.length() > 0) {
                start                       = setStartDate(periodShift.toSQLQueryInt(), 0, specificLocationsString, dateFormat.parse(startStr));
                end                         = setEndDate(periodShift.toSQLQueryInt(), 0, specificLocationsString, dateFormat.parse(startStr));
                periodDetail                = String.valueOf(start.getHours() + 1);
                locationMap                 = new LocationMap(conn, periodShift.toSQLQueryInt(), specificLocationsString, dateToString(dateFormat.parse(startStr)));
            }
        } catch (ParseException pe) {
            logger.debug(""+pe.getMessage());
            String badDate = (null == start) ? "start" : "end";
            throw new HandlerException("Could not parse " + badDate + " date.");
        }

        //set up the period structure
        PeriodType periodType               = PeriodType.parseString(periodStr);
        if (null == periodType) {
            throw new HandlerException("Invalid period type: " + periodStr);
        }
        ReportPeriod period                 = null;
        try {
            period                          = new ReportPeriod(periodType, periodDetail, start, end);
        } catch (IllegalArgumentException e) {
            throw new HandlerException(e.getMessage());
        }

        SortedSet<DatePartition> dps        = DatePartitionFactory.createPartitions(period);
        //logger.debug("Created partitions: \n" + DatePartitionFactory.partitionReport(dps));
        DatePartitionTree dpt = new DatePartitionTree(dps);

        SalesResults rs                     = null;
        try {
            if (zone > 0) {
                //type value does not affect by customer information
                rs                          = SalesResults.getResultsByZone(period, btype, zone, conn);
                costCenterMap               = getCostCenterMap(3, zone, specificLocationsString);
            } else if (bar > 0) {
                rs                          = SalesResults.getResultsByBar(period, btype, bar, conn);
                costCenterMap               = getCostCenterMap(4, bar, specificLocationsString);
            } else if (station > 0) {
                rs                          = SalesResults.getResultsByStation(period, btype, station, conn);
                costCenterMap               = getCostCenterMap(5, station, specificLocationsString);
            } else if (specificLocationsString != null && specificLocationsString.length() > 0) {
                rs                          = SalesResults.getResultsBySpecificLocations(period, btype, specificLocationsString, conn);
                costCenterMap               = getCostCenterMap(6, 0, specificLocationsString);
            } else if (customer > 0) {
                rs                          = SalesResults.getResultsByCustomer(period, btype, customer, conn);
                costCenterMap               = getCostCenterMap(1, customer, specificLocationsString);
            } else if (location > 0) {
                rs                          = SalesResults.getResultsByLocation(period, btype, location, conn);
                costCenterMap               = getCostCenterMap(2, location, specificLocationsString);
            }

            boolean validTimeStamp          = true;
            int totalRecords                = 0;
            int rsCounter                   = 0;
            HashMap<Integer, String> allLocationMap
                                            = new HashMap<Integer, String>();
            
            while (rs.next()) {
                rsCounter++;
                String plu                  = rs.getPlu();
                double value                = rs.getValue();
                Date timestamp              = rs.getDate();
                int locationId              = rs.getLocation();
                int costCenter              = 0;
                if (costCenterMap.containsKey(locationId+"-"+rs.getCostCenter())) {
                    costCenter              = costCenterMap.get(locationId+"-"+rs.getCostCenter());
                }
                
                if(!allLocationMap.containsKey(locationId)) {
                            allLocationMap.put(locationId,"location"+locationId);
                        }
                        

                if (customer > 0 || (specificLocationsString != null && specificLocationsString.length() > 0)) {
                    validTimeStamp          = isTimeValid(locationMap.getLocationDateSet(locationId), timestamp);
                } else {
                    validTimeStamp = true;
                }

                if (validTimeStamp) {
                    //logger.debug(timestamp.toString());

                    Set<ReconciliationRecord> baseSet
                                            = getBaseSetFromCache(plu, locationId, costCenter, ingredCache);

                    Set<ReconciliationRecord> rSet
                                            = ReconciliationRecord.recordByBaseSet(baseSet, value);
                    totalRecords            += rSet.size();
                    // loop through all the returned RRs and add them to the appropriate product set.
                    for (ReconciliationRecord rr : rSet) {

                        // here is where we check the product filter (used to be in the query)
                        if (productFilter > 0 && productFilter != rr.getProductId()) {
                            continue;
                        }

                        String productKey   = String.valueOf(rr.getProductId());
                        Integer dateKey     = new Integer(dpt.getIndex(timestamp));

                        if (byGroup) {
                            productKey      = String.valueOf(rr.getIdentifierType()) + String.valueOf(rr.getIdentifier()) + productKey;
                        }

                        HashMap<String, ReconciliationRecord> productSet
                                            = resultSet.get(dateKey);
                        // add a new productSet if it doesn't exist
                        if (productSet == null) {
                            productSet      = new HashMap<String, ReconciliationRecord>();
                            resultSet.put(dateKey, productSet);
                        }

                        // get the rrec (creating a new one if needed)
                        ReconciliationRecord existingRecord
                                            = productSet.get(productKey);
                        if (existingRecord != null) {
                            //logger.debug("Adding Volume for plu: " + plu + " with product: " + productKey + " with value: " + rr.getValue() + " at time: " + timestamp);
                            existingRecord.add(rr);
                        } else {
                            productSet.put(productKey, rr);
                        }
                    }

                }
            }
            logger.debug("Processed " + totalRecords + " (" + rsCounter + ") reconciliation record(s)");

             String locations        ="0";
            for(int loc:allLocationMap.keySet()){
                locations               +=","+String.valueOf(loc); 
            }
            // create the XML response
            ArrayList<String> dateList      = new ArrayList<String>();
            int index                       = 0;
            for (DatePartition dp : dps) {
                //logger.debug("Entered DP loop");
                Integer dateKey             = new Integer(index);
                HashMap<String, ReconciliationRecord> productSet
                                            = resultSet.get(dateKey);

                Collection<ReconciliationRecord> recs
                                            = new HashSet<ReconciliationRecord>();
                if (productSet != null) {
                    recs                    = productSet.values();
                }

                Element periodEl            = toAppend.addElement("period");
                periodEl.addElement("periodDate").addText(dateFormat.format(dp.getDate()));
                // iterate through our rrecs
                // use the product set to create the XML to return.
                // logger.debug("Period " + index + ": " + recs.size() + " record(s)");
                if (forChart) {
                    HashMap<Integer, Double> valueMap
                                            = new HashMap<Integer, Double>();
                    for (ReconciliationRecord r : recs) {
                        int locationId      = r.getLocation();
                        double value        = r.getValue();
                        if (valueMap.containsKey(locationId)) {
                            value           += valueMap.get(locationId);
                        }
                        valueMap.put(locationId, value);
                    }
                    
                    for (Integer locationId : valueMap.keySet()) {
                        Element p           = periodEl.addElement("data");
                        p.addElement("locationId").addText(String.valueOf(locationId));
                        p.addElement("ounces").addText(String.valueOf(valueMap.get(locationId)));
                    }                    
                } else if (byProduct) {
                    HashMap<Integer, String> nameMap
                                            = new HashMap<Integer, String>();                   
                    HashMap<Integer, Double> valueMap
                                            = new HashMap<Integer, Double>();
                    for (ReconciliationRecord r : recs) {                        
                        int productId       = r.getProductId();
                        String productName  = r.getName();
                        double value        = r.getValue();
                        if (!nameMap.containsKey(productId)) {
                            nameMap.put(productId, productName);
                        }
                        if (valueMap.containsKey(productId)) {
                            value           += valueMap.get(productId);
                        }
                        valueMap.put(productId, value);
                        
                    }
                    for (Integer productId : valueMap.keySet()) {
                        Element p           = periodEl.addElement("product");
                        p.addElement("product").addText(String.valueOf(productId));
                        
                        if(!isBrasstap){
                            p.addElement("name").addText(HandlerUtils.nullToEmpty(nameMap.get(productId)));                            
                        } else {
                            p.addElement("name").addText(HandlerUtils.nullToEmpty(getBrasstapProductName(productId)));                            
                        }                        
                        p.addElement("ounces").addText(String.valueOf(valueMap.get(productId)));
                    }
                    
                } else {
                    for (ReconciliationRecord r : recs) {
                        Element p           = periodEl.addElement("product");
                        p.addElement("identifierType").addText(String.valueOf(r.getIdentifierType()));
                        p.addElement("identifier").addText(String.valueOf(r.getIdentifier()));
                        p.addElement("product").addText(String.valueOf(r.getProductId()));
                        if(!isBrasstap){
                            p.addElement("name").addText(HandlerUtils.nullToEmpty(r.getName()));                            
                        } else {
                            p.addElement("name").addText(HandlerUtils.nullToEmpty(getBrasstapProductName(r.getProductId())));                            
                        }
                        p.addElement("ounces").addText(String.valueOf(r.getValue()));
                    }
                }
                if(periodType.toString().equals(PeriodType.DAILY.toString())){
                if(dateList!=null ){
                    if(!dateList.contains(newDateFormat.format(dp.getDate()).substring(0,10))){
                        //getUnknownReading(locations,newDateFormat.format(dp.getDate()),periodType.toString(),periodEl,false);
                    }
                } else {
                        //getUnknownReading(locations,newDateFormat.format(dp.getDate()),periodType.toString(),periodEl,false);
                }
                dateList.add(newDateFormat.format(dp.getDate()).substring(0,10));
                } else  {
                     //getUnknownReading(locations,newDateFormat.format(dp.getDate()),periodType.toString(),periodEl,false);
                }
                index++;
            }
            logger.debug("Rec Report Complete");
            if(callerId > 0){
                //logger.portalVisitDetail(callerId, "getReport", location, "getSalesReportNew",0,10, "", conn);
            }
            
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        }catch(Exception e) {
                        logger.debug(e.getMessage());
                    } finally {
            rs.close();
        }
    }

    /**
     *  This method supports subdivision of the period and product filtering
     *
     *  This method now supports and optional barId
     *  If the barId is set, then only PLUs matching this bar will be returned.  
     *  If the barId is not set, then all PLUs will be returned.  
     */
    private void getConcessionsSalesReport(Element toHandle, Element toAppend) throws HandlerException {
        int callerId                        = getCallerId(toHandle);
        int location                        = HandlerUtils.getOptionalInteger(toHandle, "locationId");
        int zone                            = HandlerUtils.getOptionalInteger(toHandle, "zoneId");
        int bar                             = HandlerUtils.getOptionalInteger(toHandle, "barId");
        int station                         = HandlerUtils.getOptionalInteger(toHandle, "stationId");
        int productFilter                   = HandlerUtils.getOptionalInteger(toHandle, "specificProduct");
        int btype                           = HandlerUtils.getOptionalInteger(toHandle, "type");
        String startStr                     = HandlerUtils.getRequiredString(toHandle, "startDate");
        String endStr                       = HandlerUtils.getRequiredString(toHandle, "endDate");
        String specialBarString             = HandlerUtils.getOptionalString(toHandle, "specialBarString");
        String cateredBarString             = HandlerUtils.getOptionalString(toHandle, "cateredBarString");
        String periodStr                    = HandlerUtils.getRequiredString(toHandle, "periodType");
        String periodDetail                 = HandlerUtils.getRequiredString(toHandle, "periodDetail");
        PeriodShiftType periodShift         = PeriodShiftType.instanceOf("EntireDay");
        String periodShiftString            = HandlerUtils.getOptionalString(toHandle, "periodShift");
        if (null != periodShiftString) {
            periodShift                     = PeriodShiftType.instanceOf(HandlerUtils.getOptionalString(toHandle, "periodShift"));
        }
        boolean byGroup                     = ("true".equalsIgnoreCase(HandlerUtils.getOptionalString(toHandle, "byGroup")));
        boolean forChart                    = HandlerUtils.getOptionalBoolean(toHandle, "forChart");
        boolean byProduct                   = HandlerUtils.getOptionalBoolean(toHandle, "byProduct");
        int paramsSet = 0;
        if (station >= 0) {
            paramsSet++;
        }
        if (bar >= 0) {
            paramsSet++;
        }
        if (zone >= 0) {
            paramsSet++;
        }
        if (location >= 0) {
            paramsSet++;
        }
        if (paramsSet != 1) {
            throw new HandlerException("Exactly one of the following must be set: locationId zoneId barId stationId");
        }

        // A cache of beverage ingredient sets (maps PLU -> RRecSet)
        Map<String, Set<ReconciliationRecord>> ingredCache
                                            = new HashMap<String, Set<ReconciliationRecord>>();
        // Maps Period Ids to ProductSets(product ids to RRecs (oz values));
        HashMap<Integer, HashMap<String, ReconciliationRecord>> resultSet
                                            = new HashMap<Integer, HashMap<String, ReconciliationRecord>>();
        // Maps Period Ids to ProductSets(product ids to RRecs (oz values));
        HashMap<Integer, HashMap<String, ReconciliationRecord>> cateredEventResultSet
                                            = new HashMap<Integer, HashMap<String, ReconciliationRecord>>();
        // Map for costCenters
        HashMap<String,Integer> costCenterMap
                                            = new HashMap<String,Integer>();

        String exclusionCostCenter          = null, inclusionCostCenter = null, specialEventCostCenter = null, cateredEventCostCenter = null;

        PreparedStatement stmt              = null;
        ResultSet rs                        = null;

        SalesResults sr                     = null, specialEventSR = null, cateredEventSR = null;
        
        try {

            //parse the dates
            Date start                      = null, end = null;
            Date specialStart               = null, specialEnd = null;
            Date cateredStart               = null, cateredEnd = null;
            try {
                start                       = dateFormat.parse(startStr);
                end                         = dateFormat.parse(endStr);

                if (null != specialBarString) {
                    String specialStartStr  = HandlerUtils.getRequiredString(toHandle, "specialStartDate");
                    String specialEndStr    = HandlerUtils.getRequiredString(toHandle, "specialEndDate");
                    specialStart            = dateFormat.parse(specialStartStr);
                    specialEnd              = dateFormat.parse(specialEndStr);

                    stmt                    = conn.prepareStatement("SELECT GROUP_CONCAT(ccID) FROM costCenter WHERE bar IN (" + specialBarString + ")");
                    rs                      = stmt.executeQuery();
                    if (rs.next()) {
                        exclusionCostCenter = specialEventCostCenter = rs.getString(1);
                    }
                }

                if (null != cateredBarString) {
                    String cateredStartStr  = HandlerUtils.getRequiredString(toHandle, "cateredStartDate");
                    String cateredEndStr    = HandlerUtils.getRequiredString(toHandle, "cateredEndDate");
                    cateredStart            = dateFormat.parse(cateredStartStr);
                    cateredEnd              = dateFormat.parse(cateredEndStr);
                    
                    stmt                    = conn.prepareStatement("SELECT GROUP_CONCAT(ccID) FROM costCenter WHERE bar IN (" + cateredBarString + ")");
                    rs                      = stmt.executeQuery();
                    if (rs.next() && rs.getString(1) != null) {
                        cateredEventCostCenter
                                            = rs.getString(1);
                        if (exclusionCostCenter != null) {
                            exclusionCostCenter
                                            += ", " + cateredEventCostCenter;
                        }
                        else
                        {
                            exclusionCostCenter
                                            = cateredEventCostCenter;
                        }
                    }
                }
            } catch (ParseException pe) {
                String badDate = (null == start) ? "start" : "end";
                throw new HandlerException("Could not parse " + badDate + " date.");
            }

            //set up the period structure
            PeriodType periodType           = PeriodType.parseString(periodStr);
            if (null == periodType) {
                throw new HandlerException("Invalid period type: " + periodStr);
            }
            ReportPeriod period             = null, specialPeriod = null, cateredPeriod = null, partitionPeriod = null;
            try {
                period                      = new ReportPeriod(periodType, periodDetail, start, end);
                logger.debug("start:"+start +" end:"+end);

                if (null != specialBarString) {
                    specialPeriod           = new ReportPeriod(periodType, periodDetail, specialStart, specialEnd);
                    if (start.after(specialStart)) {
                        start               = specialStart;
                    }
                    if (end.before(specialEnd)) {
                        end                 = specialEnd;
                    }
                }

                if (null != cateredBarString) {
                    cateredPeriod           = new ReportPeriod(periodType, periodDetail, cateredStart, cateredEnd);
                }
                partitionPeriod             = new ReportPeriod(periodType, periodDetail, start, end);
            } catch (IllegalArgumentException e) {
                throw new HandlerException(e.getMessage());
            }

            SortedSet<DatePartition> dps    = DatePartitionFactory.createPartitions(partitionPeriod);
            //logger.debug("Created partitions: \n" + DatePartitionFactory.partitionReport(dps));
            DatePartitionTree dpt           = new DatePartitionTree(dps);

            if (location > 0) {
                if (exclusionCostCenter != null) {
                    stmt                    = conn.prepareStatement("SELECT GROUP_CONCAT(ccID) FROM costCenter WHERE ccID NOT IN (" + exclusionCostCenter + ") AND location = ?");
                    stmt.setInt(1, location);
                    rs                      = stmt.executeQuery();
                    if (rs.next()) {
                        inclusionCostCenter = rs.getString(1);
                    }
                }
                sr                          = SalesResults.getResultsByLocationCostCenter(period, btype, location, "", (inclusionCostCenter == null ? "" : inclusionCostCenter), conn);
                if (null != specialEventCostCenter) {
                    specialEventSR          = SalesResults.getResultsByLocationCostCenter(specialPeriod, btype, location, "", specialEventCostCenter, conn);
                }
                if (null != cateredEventCostCenter) {
                    cateredEventSR          = SalesResults.getResultsByLocationCostCenter(cateredPeriod, btype, location, "", cateredEventCostCenter, conn);
                }
                costCenterMap               = getCostCenterMap(2, location, "");
            } else if (zone > 0) {
                if (exclusionCostCenter != null) {
                    stmt                    = conn.prepareStatement("SELECT GROUP_CONCAT(ccID) FROM costCenter WHERE ccID NOT IN (" + exclusionCostCenter + ") AND zone = ?");
                    stmt.setInt(1, zone);
                    rs                      = stmt.executeQuery();
                    if (rs.next()) {
                        inclusionCostCenter = rs.getString(1);
                    }
                }
                sr                          = SalesResults.getResultsByZoneCostCenter(period, btype, zone, "", (inclusionCostCenter == null ? "" : inclusionCostCenter), conn);
                if (null != specialEventCostCenter) {
                    specialEventSR          = SalesResults.getResultsByZoneCostCenter(specialPeriod, btype, zone, "", specialEventCostCenter, conn);
                }
                if (null != cateredEventCostCenter) {
                    cateredEventSR          = SalesResults.getResultsByZoneCostCenter(cateredPeriod, btype, zone, "", cateredEventCostCenter, conn);
                }
                costCenterMap               = getCostCenterMap(3, zone, "");
            } else if (bar > 0) {
                if (exclusionCostCenter != null) {
                    stmt                    = conn.prepareStatement("SELECT GROUP_CONCAT(ccID) FROM costCenter WHERE ccID NOT IN (" + exclusionCostCenter + ") AND bar = ?");
                    stmt.setInt(1, bar);
                    rs                      = stmt.executeQuery();
                    if (rs.next()) {
                        inclusionCostCenter = rs.getString(1);
                    }
                }
                sr                          = SalesResults.getResultsByBarCostCenter(period, btype, bar, "", (inclusionCostCenter == null ? "" : inclusionCostCenter), conn);
                if (null != specialEventCostCenter) {
                    //specialEventSR          = SalesResults.getResultsByBarCostCenter(specialPeriod, btype, bar, "", specialEventCostCenter, conn);
                }
                if (null != cateredEventCostCenter) {
                    //cateredEventSR          = SalesResults.getResultsByBarCostCenter(cateredPeriod, btype, bar, "", cateredEventCostCenter, conn);
                }
                costCenterMap               = getCostCenterMap(4, bar, "");
            } else if (station > 0) {
                if (exclusionCostCenter != null) {
                    stmt                    = conn.prepareStatement("SELECT GROUP_CONCAT(ccID) FROM costCenter WHERE ccID NOT IN (" + exclusionCostCenter + ") AND station = ?");
                    stmt.setInt(1, station);
                    rs                      = stmt.executeQuery();
                    if (rs.next()) {
                        inclusionCostCenter = rs.getString(1);
                    }
                }
                sr                          = SalesResults.getResultsByStationCostCenter(period, btype, station, "", (inclusionCostCenter == null ? "" : inclusionCostCenter), conn);
                if (null != specialEventCostCenter) {
                    //specialEventSR          = SalesResults.getResultsByStationCostCenter(specialPeriod, btype, station, "", specialEventCostCenter, conn);
                }
                if (null != cateredEventCostCenter) {
                    //cateredEventSR          = SalesResults.getResultsByStationCostCenter(cateredPeriod, btype, station, "", cateredEventCostCenter, conn);
                }
                costCenterMap               = getCostCenterMap(5, station, "");
            } 
            
            int totalRecords                = 0;
            int rsCounter                   = 0;
            HashMap<Integer, String> allLocationMap
                                            = new HashMap<Integer, String>();
            while (sr.next()) {
                rsCounter++;
                String plu                  = sr.getPlu();
                double value                = sr.getValue();
                Date timestamp              = sr.getDate();
                int locationId              = sr.getLocation();
                int costCenter              = 0;
                if (costCenterMap.containsKey(locationId+"-"+sr.getCostCenter())) {
                    costCenter              = costCenterMap.get(locationId+"-"+sr.getCostCenter());
                }
                if(!allLocationMap.containsKey(locationId)) {
                    allLocationMap.put(locationId, "Location" + locationId);
                }

                Set<ReconciliationRecord> baseSet
                                            = getBaseSetFromCache(plu, locationId, costCenter, ingredCache);

                Set<ReconciliationRecord> rSet
                                            = ReconciliationRecord.recordByBaseSet(baseSet, value);
                totalRecords                += rSet.size();
                // loop through all the returned RRs and add them to the appropriate product set.
                for (ReconciliationRecord rr : rSet) {

                    // here is where we check the product filter (used to be in the query)
                    if (productFilter > 0 && productFilter != rr.getProductId()) {
                        continue;
                    }

                    String productKey       = String.valueOf(rr.getProductId());
                    Integer dateKey         = new Integer(dpt.getIndex(timestamp));

                    if (byGroup) {
                        productKey          = String.valueOf(rr.getIdentifierType()) + String.valueOf(rr.getIdentifier()) + productKey;
                    }

                    HashMap<String, ReconciliationRecord> productSet
                                            = resultSet.get(dateKey);
                    // add a new productSet if it doesn't exist
                    if (productSet == null) {
                        productSet          = new HashMap<String, ReconciliationRecord>();
                        resultSet.put(dateKey, productSet);
                    }

                    // get the rrec (creating a new one if needed)
                    ReconciliationRecord existingRecord
                                            = productSet.get(productKey);
                    if (existingRecord != null) {
                        //logger.debug("Adding Volume for plu: " + plu + " with product: " + productKey + " with value: " + rr.getValue() + " at time: " + timestamp);
                        existingRecord.add(rr);
                    } else {
                        productSet.put(productKey, rr);
                    }
                }
            }
            if (specialEventSR != null) {
                while (specialEventSR.next()) {
                    rsCounter++;
                    String plu              = specialEventSR.getPlu();
                    double value            = specialEventSR.getValue();
                    Date timestamp          = specialEventSR.getDate();
                    int locationId          = specialEventSR.getLocation();
                    int costCenter          = 0;
                    if (costCenterMap.containsKey(locationId+"-"+specialEventSR.getCostCenter())) {
                        costCenter          = costCenterMap.get(locationId+"-"+specialEventSR.getCostCenter());
                    }

                    Set<ReconciliationRecord> baseSet
                                            = getBaseSetFromCache(plu, locationId, costCenter, ingredCache);

                    Set<ReconciliationRecord> rSet
                                            = ReconciliationRecord.recordByBaseSet(baseSet, value);
                    totalRecords            += rSet.size();
                    // loop through all the returned RRs and add them to the appropriate product set.
                    for (ReconciliationRecord rr : rSet) {

                        // here is where we check the product filter (used to be in the query)
                        if (productFilter > 0 && productFilter != rr.getProductId()) {
                            continue;
                        }

                        String productKey   = String.valueOf(rr.getProductId());
                        Integer dateKey     = new Integer(dpt.getIndex(timestamp));

                        if (byGroup) {
                            productKey      = String.valueOf(rr.getIdentifierType()) + String.valueOf(rr.getIdentifier()) + productKey;
                        }

                        HashMap<String, ReconciliationRecord> productSet
                                            = resultSet.get(dateKey);
                        // add a new productSet if it doesn't exist
                        if (productSet == null) {
                            productSet      = new HashMap<String, ReconciliationRecord>();
                            resultSet.put(dateKey, productSet);
                        }

                        // get the rrec (creating a new one if needed)
                        ReconciliationRecord existingRecord
                                            = productSet.get(productKey);
                        if (existingRecord != null) {
                            //logger.debug("Adding Volume for plu: " + plu + " with product: " + productKey + " with value: " + rr.getValue() + " at time: " + timestamp);
                            existingRecord.add(rr);
                        } else {
                            productSet.put(productKey, rr);
                        }
                    }
                }
            }
            if (cateredEventSR != null) {
                while (cateredEventSR.next()) {
                    rsCounter++;
                    String plu              = cateredEventSR.getPlu();
                    double value            = cateredEventSR.getValue();
                    Date timestamp          = cateredEventSR.getDate();
                    int locationId          = cateredEventSR.getLocation();
                    int costCenter          = 0;
                    if (costCenterMap.containsKey(locationId+"-"+cateredEventSR.getCostCenter())) {
                        costCenter          = costCenterMap.get(locationId+"-"+cateredEventSR.getCostCenter());
                    }

                    Set<ReconciliationRecord> baseSet
                                            = getBaseSetFromCache(plu, locationId, costCenter, ingredCache);

                    Set<ReconciliationRecord> rSet
                                            = ReconciliationRecord.recordByBaseSet(baseSet, value);
                    totalRecords            += rSet.size();
                    // loop through all the returned RRs and add them to the appropriate product set.
                    for (ReconciliationRecord rr : rSet) {

                        // here is where we check the product filter (used to be in the query)
                        if (productFilter > 0 && productFilter != rr.getProductId()) {
                            continue;
                        }

                        String productKey   = String.valueOf(rr.getProductId());
                        Integer dateKey     = new Integer(dpt.getIndex(timestamp));

                        if (byGroup) {
                            productKey      = String.valueOf(rr.getIdentifierType()) + String.valueOf(rr.getIdentifier()) + productKey;
                        }

                        HashMap<String, ReconciliationRecord> productSet
                                            = cateredEventResultSet.get(dateKey);
                        // add a new productSet if it doesn't exist
                        if (productSet == null) {
                            productSet      = new HashMap<String, ReconciliationRecord>();
                            cateredEventResultSet.put(dateKey, productSet);
                        }

                        // get the rrec (creating a new one if needed)
                        ReconciliationRecord existingRecord
                                            = productSet.get(productKey);
                        if (existingRecord != null) {
                            //logger.debug("Adding Volume for plu: " + plu + " with product: " + productKey + " with value: " + rr.getValue() + " at time: " + timestamp);
                            existingRecord.add(rr);
                        } else {
                            productSet.put(productKey, rr);
                        }
                    }
                }
            }
            logger.debug("Processed " + totalRecords + " (" + rsCounter + ") reconciliation record(s)");
            
            String locations                = "0";
            for(int loc : allLocationMap.keySet()){
                locations                   += "," + String.valueOf(loc);
            }
            // create the XML response
            ArrayList<String> dateList      = new ArrayList<String>();
            int index                       = 0;
            for (DatePartition dp : dps) {
                //logger.debug("Entered DP loop");
                Integer dateKey             = new Integer(index);
                HashMap<String, ReconciliationRecord> productSet
                                            = resultSet.get(dateKey);
                HashMap<String, ReconciliationRecord> cateredEventProductSet
                                            = cateredEventResultSet.get(dateKey);

                Collection<ReconciliationRecord> recs
                                            = new HashSet<ReconciliationRecord>();
                Collection<ReconciliationRecord> cateredEventRecs
                                            = new HashSet<ReconciliationRecord>();
                if (productSet != null) {
                    recs                    = productSet.values();
                }
                if (cateredEventProductSet != null) {
                    cateredEventRecs        = cateredEventProductSet.values();
                }

                Element periodEl            = toAppend.addElement("period");
                periodEl.addElement("periodDate").addText(dateFormat.format(dp.getDate()));
                // iterate through our rrecs
                // use the product set to create the XML to return.
                //logger.debug("Period " + index + ": " + recs.size() + " record(s)");
                //logger.debug("Catered Event Period " + index + ": " + cateredEventRecs.size() + " record(s)");
                if (forChart) {
                    HashMap<Integer, Double> valueMap
                                            = new HashMap<Integer, Double>();
                    for (ReconciliationRecord r : recs) {
                        int locationId      = r.getLocation();
                        double value        = r.getValue();
                        if (valueMap.containsKey(locationId)) {
                            value           += valueMap.get(locationId);
                        }
                        valueMap.put(locationId, value);
                    }
                    
                    
                    for (Integer locationId : valueMap.keySet()) {
                        Element p           = periodEl.addElement("data");
                        p.addElement("locationId").addText(String.valueOf(locationId));
                        p.addElement("ounces").addText(String.valueOf(valueMap.get(locationId)));                        
                        
                    }                    
                } else if (byProduct) {
                    HashMap<Integer, String> nameMap
                                            = new HashMap<Integer, String>();
                    HashMap<Integer, Double> valueMap
                                            = new HashMap<Integer, Double>();
                    for (ReconciliationRecord r : recs) {
                        int locationId      = r.getLocation();
                        int productId       = r.getProductId();
                        String productName  = r.getName();
                        double value        = r.getValue();
                        if (!nameMap.containsKey(productId)) {
                            nameMap.put(productId, productName);
                        }
                        if (valueMap.containsKey(productId)) {
                            value           += valueMap.get(productId);
                        }
                        valueMap.put(productId, value);
                    }
                    for (Integer productId : valueMap.keySet()) {
                        Element p           = periodEl.addElement("product");
                        p.addElement("product").addText(String.valueOf(productId));
                        p.addElement("name").addText(HandlerUtils.nullToEmpty(nameMap.get(productId)));
                        p.addElement("ounces").addText(String.valueOf(valueMap.get(productId)));
                    }

                    // cateredEventData
                    nameMap                 = new HashMap<Integer, String>();
                    valueMap                = new HashMap<Integer, Double>();
                    for (ReconciliationRecord r : cateredEventRecs) {
                        int locationId      = r.getLocation();
                        int productId       = r.getProductId();
                        String productName  = r.getName();
                        double value        = r.getValue();
                        if (!nameMap.containsKey(productId)) {
                            nameMap.put(productId, productName);
                        }
                        if (valueMap.containsKey(productId)) {
                            value           += valueMap.get(productId);
                        }
                        valueMap.put(productId, value);
                    }
                    for (Integer productId : valueMap.keySet()) {
                        Element p           = periodEl.addElement("cateredEventProduct");
                        p.addElement("product").addText(String.valueOf(productId));
                        p.addElement("name").addText(HandlerUtils.nullToEmpty(nameMap.get(productId)));
                        p.addElement("ounces").addText(String.valueOf(valueMap.get(productId)));
                    }
                    
                } else {
                    for (ReconciliationRecord r : recs) {
                        Element p           = periodEl.addElement("product");
                        p.addElement("identifierType").addText(String.valueOf(r.getIdentifierType()));
                        p.addElement("identifier").addText(String.valueOf(r.getIdentifier()));
                        p.addElement("product").addText(String.valueOf(r.getProductId()));
                        p.addElement("ounces").addText(String.valueOf(r.getValue()));
                        p.addElement("name").addText(HandlerUtils.nullToEmpty(r.getName()));
                    }
                    for (ReconciliationRecord r : cateredEventRecs) {
                        Element p           = periodEl.addElement("cateredEventProduct");
                        p.addElement("identifierType").addText(String.valueOf(r.getIdentifierType()));
                        p.addElement("identifier").addText(String.valueOf(r.getIdentifier()));
                        p.addElement("product").addText(String.valueOf(r.getProductId()));
                        p.addElement("ounces").addText(String.valueOf(r.getValue()));
                        p.addElement("name").addText(HandlerUtils.nullToEmpty(r.getName()));
                    }
                }

                if(periodType.toString().equals(PeriodType.DAILY.toString())) {
                    if(dateList!=null ){
                        if(!dateList.contains(newDateFormat.format(dp.getDate()).substring(0,10))){
                            //getUnknownReading(locations,newDateFormat.format(dp.getDate()),periodType.toString(),periodEl,false);
                        }
                    } else {
                            //getUnknownReading(locations,newDateFormat.format(dp.getDate()),periodType.toString(),periodEl,false);
                    }
                    dateList.add(newDateFormat.format(dp.getDate()).substring(0,10));
                } else {
                     //getUnknownReading(locations,newDateFormat.format(dp.getDate()),periodType.toString(),periodEl,false);
                }
                index++;
            }
            logger.debug("Rec Report Complete");
            if(callerId > 0){
               // logger.portalDetail(callerId, "getSalesReport", location, "getConcessionSalesReportNew",0, "", conn);
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            sr.close();
            if (specialEventSR != null) {
                specialEventSR.close();
            }
            if (cateredEventSR != null) {
                cateredEventSR.close();
            }
        }
    }

    private Set<ReconciliationRecord> getBaseSetFromCache(String plu, int location, int costCenter, Map<String, Set<ReconciliationRecord>> cache) {
        Set<ReconciliationRecord> baseSet   = null;
        String cacheIdentifier              = String.valueOf(location) + "-" + String.valueOf(costCenter) + "-" + plu;
        if (cache.containsKey(cacheIdentifier)) {
            baseSet                         = cache.get(cacheIdentifier);
        } else {
            // we need to do a db lookup and add the ingredients to the cache
            baseSet                         = ReconciliationRecord.recordByPlu(plu, location, costCenter, 1.0, conn);
            cache.put(cacheIdentifier, baseSet);
        }
        return baseSet;
    }

    //SundarRavindra_22-Sep-2009_Start
    private void getPreOpenHoursSummaryReport(Element toHandle, Element toAppend) throws HandlerException {
        int corporateId = HandlerUtils.getOptionalInteger(toHandle, "corporateId");
        int customerId = HandlerUtils.getOptionalInteger(toHandle, "customerId");
        int locationId = HandlerUtils.getOptionalInteger(toHandle, "locationId");
        int barId = HandlerUtils.getOptionalInteger(toHandle, "barId");
        int productId = HandlerUtils.getOptionalInteger(toHandle, "productId");
        String startDate = HandlerUtils.getRequiredString(toHandle, "startDate");
        String endDate = HandlerUtils.getRequiredString(toHandle, "endDate");
        boolean includePoured = HandlerUtils.getOptionalBoolean(toHandle, "includePoured");
        boolean includeSold = HandlerUtils.getOptionalBoolean(toHandle, "includeSold");

        DateParameter validatedStartDate = new DateParameter(startDate);
        DateParameter validatedEndDate = new DateParameter(endDate);

        int paramCount = 0;
        if (corporateId > 0) {
            paramCount++;
        }
        if (customerId > 0) {
            paramCount++;
        }
        if (locationId > 0) {
            paramCount++;
        }
        if (barId > 0) {
            paramCount++;
        }
        if (productId > 0) {
            paramCount++;
        }

        if (paramCount > 1 || paramCount == 0) {
            throw new HandlerException("Only one parameter can be set for getPreOpenHoursSummaryReport: corporateId or customerId or locationId or productId.");
        }

        PreparedStatement stmt = null;
        ResultSet rs = null;
        Date previous = null;
        ArrayList al = new ArrayList();
        Map<Date, ArrayList> summarySet = new HashMap<Date, ArrayList>();
        Map<Integer, Date> dateArray = new HashMap<Integer, Date>();


        int innerCounter = 0;
        int dateCounter = 0;

        toAppend.addElement("startDate").addText(startDate);
        toAppend.addElement("endDate").addText(endDate);

        try {
            if (includePoured) {
                //NischaySharma_22-Sep-2009_Start
                String select = " SELECT a.location, a.bar, a.product, a.value, a.date, c.name, l.customer FROM preOpenHoursSummary a ";

                if (corporateId > 0) {
                    select += " LEFT JOIN location l ON l.id = a.location ";
                    select += " LEFT JOIN customer c ON c.id = l.customer ";
                    select += " WHERE a.date BETWEEN ? AND ? ORDER BY a.date, a.location, a.bar, a.product;";
                    stmt = conn.prepareStatement(select);
                    stmt.setString(1, validatedStartDate.toString());
                    stmt.setString(2, validatedEndDate.toString());
                } else if (customerId > 0) {
                    select += " LEFT JOIN location l ON l.id = a.location ";
                    select += " LEFT JOIN customer c ON c.id = l.customer ";
                    select += " WHERE a.date BETWEEN ? AND ? AND l.customer = ? ORDER BY a.date, a.location, a.bar, a.product;";
                    stmt = conn.prepareStatement(select);
                    stmt.setString(1, validatedStartDate.toString());
                    stmt.setString(2, validatedEndDate.toString());
                    stmt.setInt(3, customerId);
                } else if (locationId > 0) {
                    select += " LEFT JOIN location l ON l.id = a.location ";
                    select += " LEFT JOIN customer c ON c.id = l.customer ";
                    select += " WHERE a.date BETWEEN ? AND ? AND a.location = ? ORDER BY a.date, a.location, a.bar, a.product;";
                    stmt = conn.prepareStatement(select);
                    stmt.setString(1, validatedStartDate.toString());
                    stmt.setString(2, validatedEndDate.toString());
                    stmt.setInt(3, locationId);
                } else if (barId > 0) {
                    select += " LEFT JOIN location l ON l.id = a.location ";
                    select += " LEFT JOIN customer c ON c.id = l.customer ";
                    select += " WHERE a.date BETWEEN ? AND ? AND a.bar = ? ORDER BY a.date, a.location, a.bar, a.product;";
                    stmt = conn.prepareStatement(select);
                    stmt.setString(1, validatedStartDate.toString());
                    stmt.setString(2, validatedEndDate.toString());
                    stmt.setInt(3, barId);
                } else if (productId > 0) {
                    select += " LEFT JOIN location l ON l.id = a.location ";
                    select += " LEFT JOIN customer c ON c.id = l.customer ";
                    select += " WHERE a.date BETWEEN ? AND ? AND a.product = ? ORDER BY a.date, a.location, a.bar, a.product;";
                    stmt = conn.prepareStatement(select);
                    stmt.setString(1, validatedStartDate.toString());
                    stmt.setString(2, validatedEndDate.toString());
                    stmt.setInt(3, productId);
                }

                rs = stmt.executeQuery();
                while (rs.next()) {

                    if (previous == null) {
                        previous = new Date(rs.getTimestamp(5).getTime());
                    }

                    if (previous.compareTo(new Date(rs.getTimestamp(5).getTime())) == 0) {
                        al.add(new PreOpenHoursSummaryStructure(rs.getInt(7), rs.getString(6), rs.getInt(1), locationMap.getLocation(rs.getInt(1)), rs.getInt(2), barMap.getBar(rs.getInt(2)), rs.getInt(3), productMap.getProduct(rs.getInt(3)), rs.getDouble(4)));
                        innerCounter++;
                    } else {
                        summarySet.put(previous, al);
                        innerCounter = 0;
                        dateCounter++;
                        dateArray.put(dateCounter, previous);
                        al = new ArrayList();
                        al.add(new PreOpenHoursSummaryStructure(rs.getInt(7), rs.getString(6), rs.getInt(1), locationMap.getLocation(rs.getInt(1)), rs.getInt(2), barMap.getBar(rs.getInt(2)), rs.getInt(3), productMap.getProduct(rs.getInt(3)), rs.getDouble(4)));
                        previous = new Date(rs.getTimestamp(5).getTime());
                    }
                }

                if (innerCounter > 0) {
                    summarySet.put(previous, al);
                    dateCounter++;
                    dateArray.put(dateCounter, previous);
                    innerCounter = 0;
                }

                if (dateArray.size() > 0) {

                    for (innerCounter = 1; innerCounter <= dateCounter; innerCounter++) {
                        ArrayList<PreOpenHoursSummaryStructure> arrayss = summarySet.get(dateArray.get(innerCounter));

                        Element pouredData = toAppend.addElement("pouredData");
                        Element period = pouredData.addElement("period");
                        period.addElement("periodDate").addText(String.valueOf(dateFormat.format(dateArray.get(innerCounter))));

                        for (PreOpenHoursSummaryStructure newss : arrayss) {
                            if (newss.ProductId() == 4311 && newss.ProductId() == 10661 && newss.Value() <= 0) { continue; }
                            Element product = period.addElement("product");
                            product.addElement("customerId").addText(String.valueOf(newss.CustomerId()));
                            product.addElement("customer").addText(HandlerUtils.nullToEmpty(newss.CustomerName()));
                            product.addElement("locationId").addText(String.valueOf(newss.LocationId()));
                            product.addElement("location").addText(HandlerUtils.nullToEmpty(newss.LocationName()));
                            product.addElement("barId").addText(String.valueOf(newss.BarId()));
                            product.addElement("bar").addText(HandlerUtils.nullToEmpty(newss.BarName()));
                            product.addElement("productId").addText(String.valueOf(newss.ProductId()));
                            product.addElement("product").addText(HandlerUtils.nullToEmpty(newss.ProductName()));
                            product.addElement("value").addText(String.valueOf(newss.Value()));
                            //NischaySharma_22-Sep-2009_End
                        }
                    }
                }
            }

            if (includeSold) {
                //NischaySharma_22-Sep-2009_Start
                String select = " SELECT a.location, a.bar, a.product, a.value, a.date, c.name, l.customer FROM preOpenHoursSoldSummary a ";

                if (corporateId > 0) {
                    select += " LEFT JOIN location l ON l.id = a.location ";
                    select += " LEFT JOIN customer c ON c.id = l.customer ";
                    select += " WHERE a.date BETWEEN ? AND ? ORDER BY a.date, a.location, a.bar, a.product;";
                    stmt = conn.prepareStatement(select);
                    stmt.setString(1, validatedStartDate.toString());
                    stmt.setString(2, validatedEndDate.toString());
                } else if (customerId > 0) {
                    select += " LEFT JOIN location l ON l.id = a.location ";
                    select += " LEFT JOIN customer c ON c.id = l.customer ";
                    select += " WHERE a.date BETWEEN ? AND ? AND l.customer = ? ORDER BY a.date, a.location, a.bar, a.product;";
                    stmt = conn.prepareStatement(select);
                    stmt.setString(1, validatedStartDate.toString());
                    stmt.setString(2, validatedEndDate.toString());
                    stmt.setInt(3, customerId);
                } else if (locationId > 0) {
                    select += " LEFT JOIN location l ON l.id = a.location ";
                    select += " LEFT JOIN customer c ON c.id = l.customer ";
                    select += " WHERE a.date BETWEEN ? AND ? AND a.location = ? ORDER BY a.date, a.location, a.bar, a.product;";
                    stmt = conn.prepareStatement(select);
                    stmt.setString(1, validatedStartDate.toString());
                    stmt.setString(2, validatedEndDate.toString());
                    stmt.setInt(3, locationId);
                } else if (barId > 0) {
                    select += " LEFT JOIN location l ON l.id = a.location ";
                    select += " LEFT JOIN customer c ON c.id = l.customer ";
                    select += " WHERE a.date BETWEEN ? AND ? AND a.bar = ? ORDER BY a.date, a.location, a.bar, a.product;";
                    stmt = conn.prepareStatement(select);
                    stmt.setString(1, validatedStartDate.toString());
                    stmt.setString(2, validatedEndDate.toString());
                    stmt.setInt(3, barId);
                } else if (productId > 0) {
                    select += " LEFT JOIN location l ON l.id = a.location ";
                    select += " LEFT JOIN customer c ON c.id = l.customer ";
                    select += " WHERE a.date BETWEEN ? AND ? AND a.product = ? ORDER BY a.date, a.location, a.bar, a.product;";
                    stmt = conn.prepareStatement(select);
                    stmt.setString(1, validatedStartDate.toString());
                    stmt.setString(2, validatedEndDate.toString());
                    stmt.setInt(3, productId);
                }

                rs = stmt.executeQuery();
                while (rs.next()) {

                    if (previous == null) {
                        previous = new Date(rs.getTimestamp(5).getTime());
                    }

                    if (previous.compareTo(new Date(rs.getTimestamp(5).getTime())) == 0) {
                        al.add(new PreOpenHoursSummaryStructure(rs.getInt(7), rs.getString(6), rs.getInt(1), locationMap.getLocation(rs.getInt(1)), rs.getInt(2), barMap.getBar(rs.getInt(2)), rs.getInt(3), productMap.getProduct(rs.getInt(3)), rs.getDouble(4)));
                        innerCounter++;
                    } else {
                        summarySet.put(previous, al);
                        innerCounter = 0;
                        dateCounter++;
                        dateArray.put(dateCounter, previous);
                        al = new ArrayList();
                        al.add(new PreOpenHoursSummaryStructure(rs.getInt(7), rs.getString(6), rs.getInt(1), locationMap.getLocation(rs.getInt(1)), rs.getInt(2), barMap.getBar(rs.getInt(2)), rs.getInt(3), productMap.getProduct(rs.getInt(3)), rs.getDouble(4)));
                        previous = new Date(rs.getTimestamp(5).getTime());
                    }
                }

                if (innerCounter > 0) {
                    summarySet.put(previous, al);
                    dateCounter++;
                    dateArray.put(dateCounter, previous);
                    innerCounter = 0;
                }

                if (dateArray.size() > 0) {
                    for (innerCounter = 1; innerCounter <= dateCounter; innerCounter++) {
                        ArrayList<PreOpenHoursSummaryStructure> arrayss = summarySet.get(dateArray.get(innerCounter));

                        Element soldData = toAppend.addElement("soldData");
                        Element period = soldData.addElement("period");
                        period.addElement("periodDate").addText(String.valueOf(dateFormat.format(dateArray.get(innerCounter))));

                        for (PreOpenHoursSummaryStructure newss : arrayss) {
                            if (newss.ProductId() == 4311 && newss.ProductId() == 10661&& newss.Value() <= 0) { continue; }
                            Element product = period.addElement("product");
                            product.addElement("customerId").addText(String.valueOf(newss.CustomerId()));
                            product.addElement("customer").addText(HandlerUtils.nullToEmpty(newss.CustomerName()));
                            product.addElement("locationId").addText(String.valueOf(newss.LocationId()));
                            product.addElement("location").addText(HandlerUtils.nullToEmpty(newss.LocationName()));
                            product.addElement("barId").addText(String.valueOf(newss.BarId()));
                            product.addElement("bar").addText(HandlerUtils.nullToEmpty(newss.BarName()));
                            product.addElement("productId").addText(String.valueOf(newss.ProductId()));
                            product.addElement("product").addText(HandlerUtils.nullToEmpty(newss.ProductName()));
                            product.addElement("value").addText(String.valueOf(newss.Value()));
                            //NischaySharma_22-Sep-2009_End
                        }
                    }
                }
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(rs);
            close(stmt);
            productMap = null;
            locationMap = null;
            barMap = null;
        }
    }
    //SundarRavindran_22-Sep-2009_End

    //NischaySharma_21-Jul-2009_Start
    //SundarRavindra_22-Jul-2009_Start
    private void getAfterHoursSummaryReport(Element toHandle, Element toAppend) throws HandlerException {
        int customerId = HandlerUtils.getOptionalInteger(toHandle, "customerId");
        int locationId = HandlerUtils.getOptionalInteger(toHandle, "locationId");
        int barId = HandlerUtils.getOptionalInteger(toHandle, "barId");
        int productId = HandlerUtils.getOptionalInteger(toHandle, "productId");
        String startDate = HandlerUtils.getRequiredString(toHandle, "startDate");
        String endDate = HandlerUtils.getRequiredString(toHandle, "endDate");
        boolean includePoured = HandlerUtils.getOptionalBoolean(toHandle, "includePoured");
        boolean includeSold = HandlerUtils.getOptionalBoolean(toHandle, "includeSold");

        DateParameter validatedStartDate = new DateParameter(startDate);
        DateParameter validatedEndDate = new DateParameter(endDate);

        int paramCount = 0;
        if (customerId > 0) {
            paramCount++;
        }
        if (locationId > 0) {
            paramCount++;
        }
        if (barId > 0) {
            paramCount++;
        }
        if (productId > 0) {
            paramCount++;
        }

        if (paramCount > 1 || paramCount == 0) {
            throw new HandlerException("Only one parameter can be set for getAfterHoursSummaryReport: customerId or locationId or productId.");
        }

        PreparedStatement stmt = null;
        ResultSet rs = null;
        Date previous = null;
        ArrayList al = new ArrayList();
        Map<Date, ArrayList> summarySet = new HashMap<Date, ArrayList>();
        Map<Integer, Date> dateArray = new HashMap<Integer, Date>();
        productMap = new ProductMap(conn);
        locationMap = new LocationMap(conn);
        barMap = new BarMap(conn);

        int innerCounter = 0;
        int dateCounter = 0;

        toAppend.addElement("startDate").addText(startDate);
        toAppend.addElement("endDate").addText(endDate);

        try {

            if (includePoured) {

                String selectPoured = " SELECT a.location, a.bar, a.product, a.value, a.date FROM afterHoursSummary a ";

                if (customerId > 0) {
                    selectPoured += " LEFT JOIN location l ON l.id = a.location ";
                    selectPoured += " WHERE a.date BETWEEN ? AND ? AND l.customer = ? ORDER BY a.date, a.location, a.bar, a.product;";
                    stmt = conn.prepareStatement(selectPoured);
                    stmt.setString(1, validatedStartDate.toString());
                    stmt.setString(2, validatedEndDate.toString());
                    stmt.setInt(3, customerId);
                } else if (locationId > 0) {
                    selectPoured += " WHERE a.date BETWEEN ? AND ? AND a.location = ? ORDER BY a.date, a.location, a.bar, a.product;";
                    stmt = conn.prepareStatement(selectPoured);
                    stmt.setString(1, validatedStartDate.toString());
                    stmt.setString(2, validatedEndDate.toString());
                    stmt.setInt(3, locationId);
                } else if (barId > 0) {
                    selectPoured += " WHERE a.date BETWEEN ? AND ? AND a.bar = ? ORDER BY a.date, a.location, a.bar, a.product;";
                    stmt = conn.prepareStatement(selectPoured);
                    stmt.setString(1, validatedStartDate.toString());
                    stmt.setString(2, validatedEndDate.toString());
                    stmt.setInt(3, barId);
                } else if (productId > 0) {
                    selectPoured += " WHERE a.date BETWEEN ? AND ? AND a.product = ? ORDER BY a.date, a.location, a.bar, a.product;";
                    stmt = conn.prepareStatement(selectPoured);
                    stmt.setString(1, validatedStartDate.toString());
                    stmt.setString(2, validatedEndDate.toString());
                    stmt.setInt(3, productId);
                }

                rs = stmt.executeQuery();
                while (rs.next()) {

                    if (previous == null) {
                        previous = new Date(rs.getTimestamp(5).getTime());
                    }

                    if (previous.compareTo(new Date(rs.getTimestamp(5).getTime())) == 0) {
                        al.add(new AfterHoursSummaryStructure(rs.getInt(1), locationMap.getLocation(rs.getInt(1)), rs.getInt(2), barMap.getBar(rs.getInt(2)), rs.getInt(3), productMap.getProduct(rs.getInt(3)), rs.getDouble(4)));
                        innerCounter++;
                    } else {
                        summarySet.put(previous, al);
                        innerCounter = 0;
                        dateCounter++;
                        dateArray.put(dateCounter, previous);
                        al = new ArrayList();
                        al.add(new AfterHoursSummaryStructure(rs.getInt(1), locationMap.getLocation(rs.getInt(1)), rs.getInt(2), barMap.getBar(rs.getInt(2)), rs.getInt(3), productMap.getProduct(rs.getInt(3)), rs.getDouble(4)));
                        previous = new Date(rs.getTimestamp(5).getTime());
                    }
                }

                if (innerCounter > 0) {
                    summarySet.put(previous, al);
                    dateCounter++;
                    dateArray.put(dateCounter, previous);
                    innerCounter = 0;
                }

                if (dateArray.size() > 0) {
                    for (innerCounter = 1; innerCounter <= dateCounter; innerCounter++) {
                        ArrayList<AfterHoursSummaryStructure> arrayss = summarySet.get(dateArray.get(innerCounter));

                        Element pouredData = toAppend.addElement("pouredData");
                        Element period = pouredData.addElement("period");
                        period.addElement("periodDate").addText(String.valueOf(dateFormat.format(dateArray.get(innerCounter))));

                        for (AfterHoursSummaryStructure newss : arrayss) {
                            if (newss.ProductId() == 4311 && newss.ProductId() ==10661&& newss.Value() <= 0) { continue; }
                            Element product = period.addElement("product");
                            product.addElement("locationId").addText(String.valueOf(newss.LocationId()));
                            product.addElement("location").addText(HandlerUtils.nullToEmpty(newss.LocaionName()));
                            product.addElement("barId").addText(String.valueOf(newss.BarId()));
                            product.addElement("bar").addText(HandlerUtils.nullToEmpty(newss.BarName()));
                            product.addElement("productId").addText(String.valueOf(newss.ProductId()));
                            product.addElement("product").addText(HandlerUtils.nullToEmpty(newss.ProductName()));
                            product.addElement("value").addText(String.valueOf(newss.Value()));
                        }
                    }
                }
            }

            if (includeSold) {

                String selectSold = " SELECT a.location, a.bar, a.product, a.value, a.date FROM afterHoursSoldSummary a ";

                if (customerId > 0) {
                    selectSold += " LEFT JOIN location l ON l.id = a.location ";
                    selectSold += " WHERE a.date BETWEEN ? AND ? AND l.customer = ? ORDER BY a.date, a.location, a.bar, a.product;";
                    stmt = conn.prepareStatement(selectSold);
                    stmt.setString(1, validatedStartDate.toString());
                    stmt.setString(2, validatedEndDate.toString());
                    stmt.setInt(3, customerId);
                } else if (locationId > 0) {
                    selectSold += " WHERE a.date BETWEEN ? AND ? AND a.location = ? ORDER BY a.date, a.location, a.bar, a.product;";
                    stmt = conn.prepareStatement(selectSold);
                    stmt.setString(1, validatedStartDate.toString());
                    stmt.setString(2, validatedEndDate.toString());
                    stmt.setInt(3, locationId);
                } else if (barId > 0) {
                    selectSold += " WHERE a.date BETWEEN ? AND ? AND a.bar = ? ORDER BY a.date, a.location, a.bar, a.product;";
                    stmt = conn.prepareStatement(selectSold);
                    stmt.setString(1, validatedStartDate.toString());
                    stmt.setString(2, validatedEndDate.toString());
                    stmt.setInt(3, barId);
                } else if (productId > 0) {
                    selectSold += " WHERE a.date BETWEEN ? AND ? AND a.product = ? ORDER BY a.date, a.location, a.bar, a.product;";
                    stmt = conn.prepareStatement(selectSold);
                    stmt.setString(1, validatedStartDate.toString());
                    stmt.setString(2, validatedEndDate.toString());
                    stmt.setInt(3, productId);
                }

                rs = stmt.executeQuery();
                while (rs.next()) {

                    if (previous == null) {
                        previous = new Date(rs.getTimestamp(5).getTime());
                    }

                    if (previous.compareTo(new Date(rs.getTimestamp(5).getTime())) == 0) {
                        al.add(new AfterHoursSummaryStructure(rs.getInt(1), locationMap.getLocation(rs.getInt(1)), rs.getInt(2), barMap.getBar(rs.getInt(2)), rs.getInt(3), productMap.getProduct(rs.getInt(3)), rs.getDouble(4)));
                        innerCounter++;
                    } else {
                        summarySet.put(previous, al);
                        innerCounter = 0;
                        dateCounter++;
                        dateArray.put(dateCounter, previous);
                        al = new ArrayList();
                        al.add(new AfterHoursSummaryStructure(rs.getInt(1), locationMap.getLocation(rs.getInt(1)), rs.getInt(2), barMap.getBar(rs.getInt(2)), rs.getInt(3), productMap.getProduct(rs.getInt(3)), rs.getDouble(4)));
                        previous = new Date(rs.getTimestamp(5).getTime());
                    }
                }

                if (innerCounter > 0) {
                    summarySet.put(previous, al);
                    dateCounter++;
                    dateArray.put(dateCounter, previous);
                    innerCounter = 0;
                }

                if (dateArray.size() > 0) {
                    for (innerCounter = 1; innerCounter <= dateCounter; innerCounter++) {
                        ArrayList<AfterHoursSummaryStructure> arrayss = summarySet.get(dateArray.get(innerCounter));

                        Element soldData = toAppend.addElement("soldData");
                        Element period = soldData.addElement("period");
                        period.addElement("periodDate").addText(String.valueOf(dateFormat.format(dateArray.get(innerCounter))));

                        for (AfterHoursSummaryStructure newss : arrayss) {
                            if (newss.ProductId() == 4311 && newss.ProductId()==10661 && newss.Value() <= 0) { continue; }
                            Element product = period.addElement("product");
                            product.addElement("locationId").addText(String.valueOf(newss.LocationId()));
                            product.addElement("location").addText(HandlerUtils.nullToEmpty(newss.LocaionName()));
                            product.addElement("barId").addText(String.valueOf(newss.BarId()));
                            product.addElement("bar").addText(HandlerUtils.nullToEmpty(newss.BarName()));
                            product.addElement("productId").addText(String.valueOf(newss.ProductId()));
                            product.addElement("product").addText(HandlerUtils.nullToEmpty(newss.ProductName()));
                            product.addElement("value").addText(String.valueOf(newss.Value()));
                        }
                    }
                }
            }

        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(rs);
            close(stmt);
            productMap = null;
            locationMap = null;
            barMap = null;
        }
    }
    //SundarRavindran_22-Jul-2009_End
    //NischaySharma_21-Jul-2009_End

    //NischaySharma_17-Sep-2009_Start
    private void getCorporateReportDaily(Element toHandle, Element toAppend) throws HandlerException {
        String reportName = HandlerUtils.getRequiredString(toHandle, "reportName");
        String sortEntity = HandlerUtils.getOptionalString(toHandle, "sortEntity");
        String startDate = HandlerUtils.getRequiredString(toHandle, "startDate");
        String endDate = HandlerUtils.getRequiredString(toHandle, "endDate");

        DateParameter validatedStartDate = new DateParameter(startDate);
        DateParameter validatedEndDate = new DateParameter(endDate);

        if (reportName.equals(DailyVariance)) {
            getCRDVarianceReport(toAppend, validatedStartDate, validatedEndDate, sortEntity);
        } else if (reportName.equals(DailyBrandPerf)) {
            getCRDBrandPerfReport(toAppend, validatedStartDate, validatedEndDate, sortEntity);
        } else if (reportName.equals(DailyInterrupt)) {
            getCRDInterruptReport(toAppend, validatedStartDate, validatedEndDate, sortEntity);
        } else if (reportName.equals(DailyLineCleaning)) {
            getCRDLineCleaningReport(toAppend, validatedStartDate, validatedEndDate, sortEntity);
        }
    }

    private void getCRDVarianceReport(Element toAppend, DateParameter validatedStartDate, DateParameter validatedEndDate, String sortEntity) throws HandlerException {
        PreparedStatement stmt = null;
        ResultSet rs = null;

        String select = " SELECT x.Customer, x.Location, IFNULL(ROUND(x.Poured,1),0) Poured, IFNULL(ROUND(y.Sold,1),0) Sold, IFNULL(ROUND(100*(y.Sold-x.Poured)/x.Poured,1),0) \"Var_%\" FROM " + " (SELECT c.name Customer, l.name Location, l.pouredUp pUp, SUM(ps.value) Poured FROM customer c " + " JOIN location l ON c.id=l.customer " + " JOIN pouredSummary ps ON l.id=ps.location " + " JOIN product p ON p.id=ps.product " + " Where ps.date BETWEEN ? and ? AND p.pType=1 " + " GROUP BY c.name, l.name) AS x " + " LEFT JOIN " + " (SELECT c.name Customer, l.name Location, l.soldUp sUp, SUM(ss.value) Sold FROM customer c " + " JOIN location l ON c.id=l.customer " + " JOIN soldSummary ss ON l.id=ss.location " + " JOIN product p ON p.id=ss.product " + " Where ss.date BETWEEN ? and ? AND p.pType=1 " + " GROUP BY c.name, l.name) AS y " + " ON x.Customer=y.Customer AND x.Location=y.Location " + " WHERE x.Poured > 0 OR y.Sold > 0 ";
        if (sortEntity.equals("customer")) {
            select += " ORDER BY x.Customer ";
        } else if (sortEntity.equals("location")) {
            select += " ORDER BY x.Location ";
        }

        try {
            stmt = conn.prepareStatement(select);
            stmt.setString(1, validatedStartDate.toString());
            stmt.setString(2, validatedStartDate.toString());
            stmt.setString(3, validatedStartDate.toString());
            stmt.setString(4, validatedStartDate.toString());
            rs = stmt.executeQuery();
            String customerName = null;
            Element customer = null;
            Element getCRDVarianceReportResponse = toAppend.addElement("getCRDVarianceReportResponse");
            while (rs.next()) {
                if (null == customerName || !customerName.equals(rs.getString(1))) {
                    customerName = rs.getString(1);
                    customer = getCRDVarianceReportResponse.addElement("customer");
                    customer.addAttribute("name", customerName);
                }
                customer.addElement("location").addText(rs.getString(2));
                customer.addElement("poured").addText(String.valueOf(rs.getDouble(3)));
                customer.addElement("sold").addText(String.valueOf(rs.getDouble(4)));
                customer.addElement("variance").addText(String.valueOf(rs.getDouble(5)));
            }

        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(rs);
            close(stmt);
        }

    }

    private void getCRDInterruptReport(Element toAppend, DateParameter validatedStartDate, DateParameter validatedEndDate, String sortEntity) throws HandlerException {
        PreparedStatement stmt = null;
        ResultSet rs = null;

        String select = " SELECT c.name Customer, l.name Location, i.startTime Start, i.endTime End, i.totalMinutes Total " + " FROM interruptionLogs i LEFT JOIN location l on l.id = i.location LEFT JOIN customer c on c.id = l.customer " + " LEFT JOIN " + " (Select l.id location, " + " CASE DAYOFWEEK(NOW()-1000000) " + " WHEN 1 THEN Right(lH.closeSun,8) " + " WHEN 2 THEN Right(lH.closeMon,8) " + " WHEN 3 THEN Right(lH.closeTue,8) " + " WHEN 4 THEN Right(lH.closeWed,8) " + " WHEN 5 THEN Right(lH.closeThu,8) " + " WHEN 6 THEN Right(lH.closeFri,8) " + " WHEN 7 THEN Right(lH.closeSat,8) END close1 " + " FROM locationHours lH LEFT JOIN location l on l.id = lH.location) AS lH ON lH.location = l.id " + " WHERE i.endTime > CONCAT(LEFT(IF(IFNULL(lH.close1,'02:00:00') > '07:00:00',SUBDATE(NOW(),1),NOW()) ,11),IFNULL(lH.close1,'02:00:00')) " + " AND i.startTime < CONCAT(LEFT(IF(IFNULL(lH.close1,'02:00:00') > '07:00:00',SUBDATE(NOW(),1),NOW()) ,11),IFNULL(lH.close1,'02:00:00')) " + " AND i.totalMinutes > 20 " + " ORDER BY c.name, l.name, i.startTime; ";


        try {
            stmt = conn.prepareStatement(select);
            rs = stmt.executeQuery();
            String customerName = null;
            Element customer = null;
            Element getCRDInterruptReportResponse = toAppend.addElement("getCRDInterruptReportResponse");
            while (rs.next()) {
                if (null == customerName || !customerName.equals(rs.getString(1))) {
                    customerName = rs.getString(1);
                    customer = getCRDInterruptReportResponse.addElement("customer");
                    customer.addAttribute("name", customerName);
                }
                customer.addElement("location").addText(rs.getString(2));
                customer.addElement("startTime").addText(String.valueOf(rs.getDate(3)));
                customer.addElement("endTime").addText(String.valueOf(rs.getDate(4)));
                customer.addElement("totalMinutes").addText(String.valueOf(rs.getInt(5)));
            }

        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(rs);
            close(stmt);
        }

    }

    private void getCRDBrandPerfReport(Element toAppend, DateParameter validatedStartDate, DateParameter validatedEndDate, String sortEntity) throws HandlerException {
        PreparedStatement stmt = null;
        ResultSet rs = null;

        String select = " SELECT CASE p.category " + " WHEN (1) THEN \"Economy\" " + " WHEN (2) THEN \"Premium\" " + " WHEN (3) THEN \"Super Premium\" " + " WHEN (4) THEN \"House\" " + " ELSE \"Unknown\" END Class, " + " COUNT(distinct location) Locs, p.name Product, " + " ROUND(sum(ps.value),1) Ounces " + " FROM product p " + " JOIN pouredSummary ps ON p.id=ps.product " + " JOIN location lo ON ps.location=lo.id " + " Where ps.date = ? AND ps.value IS NOT NULL AND pType=1 " + " GROUP BY p.name " + " ORDER BY category ";

        try {
            stmt = conn.prepareStatement(select);
            stmt.setString(1, validatedStartDate.toString());
            rs = stmt.executeQuery();
            String categoryName = null;
            Element category = null;
            Element getCRDBrandPerfReportResponse = toAppend.addElement("getCRDBrandPerfReportResponse");
            while (rs.next()) {
                if (null == categoryName || !categoryName.equals(rs.getString(1))) {
                    categoryName = rs.getString(1);
                    category = getCRDBrandPerfReportResponse.addElement("category");
                    category.addAttribute("name", categoryName);
                }
                category.addElement("nooflocs").addText(rs.getString(2));
                category.addElement("productName").addText(String.valueOf(rs.getString(3)));
                category.addElement("poured").addText(String.valueOf(rs.getString(4)));
            }

        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(rs);
            close(stmt);
        }

    }

    private void getCRDLineCleaningReport(Element toAppend, DateParameter validatedStartDate, DateParameter validatedEndDate, String sortEntity) throws HandlerException {
        PreparedStatement stmt = null;
        ResultSet rs = null;

        String select = " select x.Customer, x.Location, Sum(x.Poured) Poured from " + " (SELECT c.name Customer, l.name Location,r.line, ROUND(MAX(r.value),1) - ROUND(MIN(r.value),1) Poured, " + " ROUND(MAX(r.value),1) MAXVAL, ROUND(MIN(r.value),1) MINVAL " + " FROM customer c JOIN location l ON c.id=l.customer " + " JOIN system s ON l.id=s.location JOIN line ON s.id=line.system JOIN bar b ON line.bar=b.id " + " JOIN product p ON p.id=line.product JOIN reading r ON line.id=r.line " + " WHERE line.status=\"RUNNING\" AND p.pType=1 and l.id=LID AND r.type = 0 AND r.date BETWEEN addtime(CONCAT(LEFT(NOW(),11),\"04:00:00\"),concat(MID(EOS,2,1),\":0:0\")) AND " + " addtime(CONCAT(LEFT(NOW(),11),\"11:00:00\"),concat(MID(EOS,2,1),\":0:0\")) " + " GROUP BY c.name, l.name, s.systemId, b.name, line.lineIndex, line.bar " + " Having Poured > 0) as x " + " GROUP BY x.Customer, x.Location ";

        try {
            stmt = conn.prepareStatement(select);
            rs = stmt.executeQuery();
            String categoryName = null;
            Element category = null;
            Element getCRDLineCleaningReportResponse = toAppend.addElement("getCRDLineCleaningReportResponse");
            while (rs.next()) {
                if (null == categoryName || !categoryName.equals(rs.getString(1))) {
                    categoryName = rs.getString(1);
                    category = getCRDLineCleaningReportResponse.addElement("customer");
                    category.addAttribute("name", categoryName);
                }
                category.addElement("location").addText(rs.getString(2));
                category.addElement("poured").addText(String.valueOf(rs.getDouble(3)));
            }

        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(rs);
            close(stmt);
        }

    }
    //NischaySharma_17-Sep-2009_End
    
     private String getRegionLocations(int customer,int region) throws HandlerException {
        String selectRegionLocation         = "SELECT GROUP_CONCAT(id) FROM location WHERE customer= ? AND region = ?;";
        PreparedStatement stmt              = null;
        ResultSet rs                        = null;
        String locations                    ="";
        
         try {
             stmt                           = conn.prepareStatement(selectRegionLocation);
             stmt.setInt(1, customer);
             stmt.setInt(2, region);
             rs                             = stmt.executeQuery();
             if(rs.next()) {
                 locations                  = HandlerUtils.nullToEmpty(rs.getString(1));
             }
             
         } catch (SQLException sqle) {
             logger.dbError("Database error: " + sqle.getMessage());
             throw new HandlerException(sqle);
         } finally {
             close(rs);
             close(stmt);
        }
         return locations;
                
    }
     
     public static boolean checkBrasstapLocation(int paramType,int paramId,RegisteredConnection conn1)  throws HandlerException {
         boolean isBrasstap                 = false;
         /*PreparedStatement stmt             = null;
         ResultSet rs                       = null;
         int locationId                     = 0, customerId = 0;
         
         try {
             if(paramType == 1) {
                 locationId                 = paramId;
             }
             if(paramType == 2) {
                 customerId                 = paramId;
             }
             
              if(locationId  >0  ) {
                stmt = conn1.prepareStatement("SELECT customer FROM location where id=?;");
                stmt.setInt(1, locationId);                
                rs = stmt.executeQuery();
                if(rs.next()){
                    customerId  = rs.getInt(1);
                }
            }
            if(customerId > 0) {
                
            stmt = conn1.prepareStatement("SELECT l.customer FROM brasstapLocations bL LEFT JOIN location l ON l.id=bL.usbnID WHERE l.customer =?;");
            stmt.setInt(1, customerId);                
            rs = stmt.executeQuery();
            if(rs.next()){
                isBrasstap                  = true;
            }
            
            if(!isBrasstap && customerId == 269 ) {
                isBrasstap                  = true;
            }
                
            }            
             
             
         } catch (SQLException sqle) {            
             throw new HandlerException(sqle);
         } finally {
              if (rs != null) { try {rs.close();} catch (Exception e) {} }
              if (stmt != null) { try {stmt.close();} catch (Exception e) {} }             
             
        }*/
         return isBrasstap;
         
     }
     
     
     String getBrasstapProductName(int product)  throws HandlerException {
         String name                        = "";
         PreparedStatement stmt             = null;
         ResultSet rs                       = null;
         int locationId                     = 0, customerId = 0;
         
         
         try {
              if(product  >0  ) {
                stmt = conn.prepareStatement("SELECT IFNULL(b.name,p.name),p.id FROM product p LEFT JOIN brasstapProducts b ON b.usbnID= p.id where p.id=?;");
                stmt.setInt(1, product);                
                rs = stmt.executeQuery();
                if(rs.next()){
                    name  = rs.getString(1);
                    //logger.debug("BName:"+name +":"+ rs.getInt(2));
                }
                
            }
         } catch (SQLException sqle) {            
             throw new HandlerException(sqle);
         } finally {
              if (rs != null) { try {rs.close();} catch (Exception e) {} }
              if (stmt != null) { try {stmt.close();} catch (Exception e) {} }             
             
        }
         return name;
         
     }
     
     
     private void getUnclaimData(Element toHandle, Element toAppend) throws HandlerException {
        
        int callerId                        = getCallerId(toHandle);
        int location                        = HandlerUtils.getRequiredInteger(toHandle, "locationId");
        int product                         = HandlerUtils.getOptionalInteger(toHandle, "productId");
        String startDate                    = HandlerUtils.getRequiredString(toHandle, "startDate");    
        LineString ls                       = null;       
        String endDate                      = "";
        PeriodShiftType periodShift         = PeriodShiftType.instanceOf("open");
        String periodShiftString            = HandlerUtils.getOptionalString(toHandle, "periodShift");        
        Date start                          = null, end = null;
        try {
            if (null != periodShiftString) {
                periodShift                 = PeriodShiftType.instanceOf(HandlerUtils.getOptionalString(toHandle, "periodShift"));                
                start                       = setStartDate(periodShift.toSQLQueryInt(), 0, String.valueOf(location), dbDateFormat.parse(startDate.substring(0,10)));
                end                         = setEndDate(periodShift.toSQLQueryInt(), 0, String.valueOf(location), dbDateFormat.parse(startDate.substring(0,10)));
                startDate                   =newDateFormat.format(start);
                endDate                     = newDateFormat.format(end);
                logger.debug("SD:"+newDateFormat.format(start));
                logger.debug("ED:"+newDateFormat.format(end)); 
            }
            } catch (ParseException pe) {
            logger.debug(""+pe.getMessage());
            String badDate = (null == start) ? "start" : "end";
            throw new HandlerException("Could not parse " + badDate + " date.");
        }
        ReportPeriod period     = null;
        period              = new ReportPeriod(PeriodType.DAILY, "7", start, end);
        ls                      = new LineString(conn, 2, location, 0, 0, period, "");
        String selectReadings   = "SELECT 0 Type,  p.id ProdID, p.name Product, ROUND(SUM(r.quantity), 2) pQTY, 0 sQTY, " +
                                " ADDDATE(r.date, INTERVAL ? HOUR) Date, r.id ID FROM reading r " +
                                " LEFT JOIN line l on l.id = r.line LEFT JOIN product p on p.id = l.product " +
                                " WHERE r.line IN (" + ls.getLineString() + ") " +
                                " AND r.date BETWEEN ? AND ? AND r.type = 0 AND r.quantity > 1 " + (product > 0 ? " AND l.product = ? " : "") + " GROUP BY r.date, p.id " +
                                " UNION " +
                                " SELECT 1 Type, p.id ProdID, p.name Product, 0 pQTY, ROUND(SUM(i.ounces * s.quantity), 2) sQTY, " +
                                " ADDDATE(s.date, INTERVAL ? HOUR) Date, s.sid ID FROM sales s " +
                                " LEFT JOIN beverage b ON b.plu = s.pluNumber AND s.location = b.location " +
                                " LEFT JOIN ingredient i ON i.beverage = b.id LEFT JOIN product p ON p.id = i.product " +
                                " WHERE s.date BETWEEN ? AND ? AND s.location = ? " + (product > 0 ? " AND i.product = ? " : "") + " GROUP BY s.date, p.id " +
                                " ORDER BY Product, Date";


        String selectData                   = " SELECT type, product, productName, poured, sold, loss, date, id, color FROM unclaimedReadingData " +
                                            " WHERE location = ? AND date BETWEEN ? AND ? " + (product > 0 ? " AND product = ? " : "") + " ORDER BY productName, date; ";

        PreparedStatement stmt              = null;
        ResultSet rs                        = null;
        try {   
            double easternOffset            =0.0;
            
            stmt                            = conn.prepareStatement("SELECT easternOffset FROM location WHERE id=?");
            stmt.setInt(1, location);
            rs                              = stmt.executeQuery();
            if (rs.next()) {   
                easternOffset               = rs.getDouble(1);
            }
            int param                       =1;
            stmt                            = conn.prepareStatement(selectReadings);            
            stmt.setDouble(param++, easternOffset);
            stmt.setString(param++, startDate);
            stmt.setString(param++, endDate);    
            if(product>0){
                stmt.setInt(param++, product);
            }
            stmt.setDouble(param++, easternOffset);            
            stmt.setString(param++, startDate);
            stmt.setString(param++, endDate);
            stmt.setInt(param++, location);
            if(product>0){
                stmt.setInt(param++, product);
            }
            rs                              = stmt.executeQuery();
            while (rs.next()) {   
                Element dataEl              = toAppend.addElement("rawData");
                dataEl.addElement("type").addText(String.valueOf(rs.getInt(1)));                
                dataEl.addElement("productId").addText(String.valueOf(rs.getInt(2)));                
                dataEl.addElement("productName").addText(HandlerUtils.nullToEmpty(rs.getString(3)));                
                dataEl.addElement("poured").addText(HandlerUtils.nullToEmpty(rs.getString(4)));                
                dataEl.addElement("sold").addText(HandlerUtils.nullToEmpty(rs.getString(5)));
                dataEl.addElement("loss").addText(HandlerUtils.nullToEmpty(String.valueOf(0)));
                dataEl.addElement("date").addText(HandlerUtils.nullToEmpty(rs.getString(6)));
                dataEl.addElement("id").addText(String.valueOf(rs.getInt(7)));                
                dataEl.addElement("color").addText("#D1FFBA");
                
            }
            
            
            
            /*stmt                            = conn.prepareStatement(selectData);
            stmt.setInt(1, location);
            stmt.setString(2, startDate);
            stmt.setString(3, endDate);
            if (product > 0) {
                stmt.setInt(4, product);
            }
            rs                              = stmt.executeQuery();
            while (rs.next()) {   
                Element dataEl              = toAppend.addElement("rawData");
                dataEl.addElement("type").addText(String.valueOf(rs.getInt(1)));                
                dataEl.addElement("productId").addText(String.valueOf(rs.getInt(2)));                
                dataEl.addElement("productName").addText(HandlerUtils.nullToEmpty(rs.getString(3)));                
                dataEl.addElement("poured").addText(HandlerUtils.nullToEmpty(rs.getString(4)));                
                dataEl.addElement("sold").addText(HandlerUtils.nullToEmpty(rs.getString(5)));
                dataEl.addElement("loss").addText(HandlerUtils.nullToEmpty(rs.getString(6)));
                dataEl.addElement("date").addText(HandlerUtils.nullToEmpty(rs.getString(7)));
                dataEl.addElement("id").addText(String.valueOf(rs.getInt(8)));
                dataEl.addElement("colorType").addText(String.valueOf(rs.getInt(9)));
                if(rs.getInt(9) == 0){
                    dataEl.addElement("color").addText("#D1FFBA");
                } else {
                    dataEl.addElement("color").addText("#FFFF99");
                }
            } */
            if(callerId > 0){
                logger.portalDetail(callerId, "getRawData", location, "getRawData",product, "", conn);
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error in getRawData: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(rs);
            close(stmt);
        }
    } 
    
     private void generatePreOpenHoursSummary(Element toHandle, Element toAppend) throws HandlerException {        
        
        String endDate                      = "";
        PeriodShiftType periodShift         = PeriodShiftType.instanceOf("preopen");
        String startDate                    = HandlerUtils.getRequiredString(toHandle, "startDate");  
        String periodShiftString            = HandlerUtils.getOptionalString(toHandle, "periodShift"); 
        int overwrite                       = HandlerUtils.getOptionalInteger(toHandle, "overwrite");
        String date                         = HandlerUtils.getRequiredString(toHandle, "date");  
        SummaryType pouredType              = SummaryType.instanceOf("poured");
        String tableTypeString              = HandlerUtils.getRequiredString(toHandle, "pouredType");
        if (null != tableTypeString) {
            pouredType                      = SummaryType.instanceOf(HandlerUtils.getRequiredString(toHandle, "pouredType"));
        }
        String pouredTable                  = pouredType.toString() + "Summary";
        
        SummaryType soldType                = SummaryType.instanceOf("sold");
        String soldTypeString               = HandlerUtils.getRequiredString(toHandle, "soldType");
        if (null != soldTypeString) {
            soldType                        = SummaryType.instanceOf(HandlerUtils.getRequiredString(toHandle, "soldType"));
        }
        String soldTable                    = soldType.toString() + "Summary";
        
        Date start                          = null, end = null;
        
        String selectLocations              = "SELECT l.id, l.easternOffset, l.name FROM customer c LEFT JOIN location l ON c.id=l.customer LEFT JOIN locationDetails lD ON lD.location = l.id"
                                            + " WHERE lD.active = 1 AND lD.preInstall = 1 AND l.type = ?  ORDER BY l.name ASC"; //AND l.id=714 1760
        String selectBar                    = "SELECT id, name, location, zone, latitude, longitude, type FROM bar WHERE location = ?";
        String selectCostCenter             = "SELECT ccID FROM costCenter WHERE location = ? ";

        PreparedStatement stmt              = null, dayClear =null;
        ResultSet rs                        = null, rsBar=null;
        try {   
            double easternOffset            = 0.0;            
            int locCount                    =0;
            stmt                            = conn.prepareStatement(selectLocations);            
            stmt.setInt(1, 1);
            rs                              = stmt.executeQuery();
            while (rs.next()) {   
                int locationId              = rs.getInt(1);
                easternOffset               = rs.getDouble(2);
                logger.debug(rs.getString(3)+" : "+locationId);
                try {
                    if (null != periodShiftString) {  
                        periodShift         = PeriodShiftType.instanceOf(periodShiftString);
                        //logger.debug("PeriodShift:"+periodShiftString +" : "+periodShift.toSQLQueryInt());
                        start               = setStartDate(periodShift.toSQLQueryInt(), 0, String.valueOf(locationId),dbDateFormat.parse(date));
                        end                 = setEndDate(periodShift.toSQLQueryInt(), 0, String.valueOf(locationId), dbDateFormat.parse(date));
                        startDate           = newDateFormat.format(start);
                        endDate             = newDateFormat.format(end);
                        logger.debug("SD:"+newDateFormat.format(start));
                        logger.debug("ED:"+newDateFormat.format(end));  
                        
                    }
                }catch (ParseException pe) {
                    logger.debug(""+pe.getMessage());
                    String badDate = (null == start) ? "start" : "end";
                    throw new HandlerException("Could not parse " + badDate + " date.");
                } 
               
                stmt                        = conn.prepareStatement(selectBar);            
                stmt.setInt(1, locationId);
                rsBar                       = stmt.executeQuery();
                while (rsBar.next()) {   
                    int barId               = rsBar.getInt(1);
                    //logger.debug("Bar:"+barId);
                    SOAPMessage msg         = new SOAPMessage();                    
                    SOAPMessage prmsg       = new SOAPMessage();                    
                    SOAPMessage srmsg       = new SOAPMessage();                    
                    SOAPMessage psrqmsg     = new SOAPMessage();                    
                    SOAPMessage ssrqmsg     = new SOAPMessage();                    
                    SOAPMessage smrmsg      = new SOAPMessage();                    
                    Element request         = msg.getBodyElement();
                    Element salesRequest    = msg.getBodyElement();
                    Element pouredResponse  = prmsg.getBodyElement();                    
                    Element salesResponse   = srmsg.getBodyElement();    
                    Element pouredSummaryRequest  
                                            = psrqmsg.getBodyElement();
                    Element soldSummaryRequest  
                                            = ssrqmsg.getBodyElement();
                    Element summaryResponse = smrmsg.getBodyElement();
                    request.addElement("startDate").addText(startDate);
                    request.addElement("endDate").addText(endDate);
                    request.addElement("periodType").addText("full");
                    request.addElement("byLine").addText("false");
                    request.addElement("periodDetail").addText("0");
                    request.addElement("barId").addText(String.valueOf(barId));
                    
                    salesRequest.addElement("startDate").addText(startDate);
                    salesRequest.addElement("endDate").addText(endDate);
                    salesRequest.addElement("periodType").addText("full");
                    salesRequest.addElement("byLine").addText("false");
                    salesRequest.addElement("periodDetail").addText("0");
                    salesRequest.addElement("barId").addText(String.valueOf(barId));
                    
                    
                    request.setName("getReportNew");
                    getReportNew(request, pouredResponse);  
                    
                    Iterator periodEl      = pouredResponse.elementIterator("period");
                    while (periodEl.hasNext()) {
                        Iterator productEl  = ((Element) periodEl.next()).elementIterator("product");
                        while (productEl.hasNext()) {
                            Element product = (Element) productEl.next();
                            int productId   = HandlerUtils.getRequiredInteger(product, "productId");
                            String pName    = HandlerUtils.getRequiredString(product, "productName");
                            double value    = HandlerUtils.getRequiredDouble(product, "value");
                            
                            Element sr = pouredSummaryRequest.addElement("summary");
                            sr.addElement("productId").addText(String.valueOf(productId));
                            sr.addElement("ounces").addText(String.valueOf(value));
                            sr.addElement("date").addText(HandlerUtils.nullToEmpty(date));
                            //logger.debug("Method Poured: "+productId+"-"+pName+":"+value);
                        }
                    }
                    
                    salesRequest.addElement("locationId").addText(String.valueOf(locationId));  
                    salesRequest.setName("getSalesReport");
                    getSalesReport(salesRequest, salesResponse);                    
                    Iterator productSEl      = salesResponse.elementIterator("product");                    
                    while (productSEl.hasNext()) { 
                        
                        Element product     = (Element) productSEl.next();                        
                        int productId       = HandlerUtils.getOptionalInteger(product, "product");
                        String productName  = HandlerUtils.getOptionalString(product, "name");
                        double value        = HandlerUtils.getRequiredDouble(product, "ounces");                       
                        Element sr          = soldSummaryRequest.addElement("summary");
                        sr.addElement("productId").addText(String.valueOf(productId));
                        sr.addElement("ounces").addText(String.valueOf(value));
                        sr.addElement("date").addText(HandlerUtils.nullToEmpty(date));
                       // logger.debug("Method Sold: "+productId+"-"+productName+":"+value);
                        
                    }   
                    pouredSummaryRequest.addElement("clientKey").addText("7d58317f9276019ce8dc7bebac913090");
                    pouredSummaryRequest.addElement("type").addText(tableTypeString);
                    pouredSummaryRequest.addElement("exclusion").addText("0");
                    pouredSummaryRequest.addElement("overwrite").addText("1");
                    pouredSummaryRequest.addElement("locationId").addText(String.valueOf(locationId));
                    pouredSummaryRequest.addElement("barId").addText(String.valueOf(barId));
                   
                    soldSummaryRequest.addElement("clientKey").addText("7d58317f9276019ce8dc7bebac913090");
                    soldSummaryRequest.addElement("type").addText(soldTypeString);
                    soldSummaryRequest.addElement("exclusion").addText("0");
                    soldSummaryRequest.addElement("overwrite").addText("1");
                    soldSummaryRequest.addElement("locationId").addText(String.valueOf(locationId));
                    soldSummaryRequest.addElement("barId").addText(String.valueOf(barId));
                    
                    SQLAddHandler add        = new SQLAddHandler();
                    pouredSummaryRequest.setName("addSummary");
                    soldSummaryRequest.setName("addSummary");
                    //logger.debug("Method Name:"+pouredSummaryRequest.getName()); 
                    add.handle(pouredSummaryRequest, summaryResponse);
                    add.handle(soldSummaryRequest, summaryResponse);
                   
                    
                }
                locCount++;
                

            }
            logger.debug("Total location "+locCount+" Report Generated");
        } catch (SQLException sqle) {
            logger.dbError("Database error in generatePreOpenHoursSummary: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } catch(Exception e){
            logger.debug(e.getMessage());
        }finally {
            close(rsBar);
            close(rs);
            close(dayClear);
            close(stmt);
        }
    } 
     
    private void getBeerBoardIndexReport(Element toHandle, Element toAppend) throws HandlerException {

        int callerId                        = getCallerId(toHandle);
        int location                        = HandlerUtils.getRequiredInteger(toHandle, "locationId");  
        int days                            = HandlerUtils.getRequiredInteger(toHandle, "days");    
        ArrayList<String> menus             = null;
        PreparedStatement stmt              = null;
        ResultSet rs                        = null;     
        
        Map<Integer, String> lineMap        = new HashMap<Integer, String>();
        Map<Integer, String> handleMap      = new HashMap<Integer, String>();
        Map<Integer, Double> handleValueMap = new HashMap<Integer, Double>();        
        try {
            String selectDate               = "SELECT CONCAT(date_format(SUBDATE(NOW(), INTERVAL ? + 1 DAY),'%m/%d/%Y'),' - ', date_format(SUBDATE(NOW(), INTERVAL 1 DAY), '%m/%d/%Y' ))";
            String selectDayCount           = "SELECT COUNT(id) FROM tierSummary WHERE date > SUBDATE(NOW(), INTERVAL ? + 1 DAY) AND location = ? "
                                            + " AND tier < 4;";
            String selectDaysToExclude      = "SELECT IFNULL(REPLACE(CONCAT('\\'', GROUP_CONCAT(DISTINCT date), '\\''), ',', '\\',\\''), '') FROM tierSummary "
                                            + " WHERE date > SUBDATE(NOW(), INTERVAL ? + 1 DAY) AND location = ? AND tier = 4;";
            
            double tapSum                   = 0; int tapCount = 0;
            String customerName             = "", locationName = "";
            String period                   = "", daysToExculde = "";
            int dayCount                    = days;
            
            stmt                            = conn.prepareStatement(selectDaysToExclude);
            stmt.setInt(1, days);
            stmt.setInt(2, location);
            rs                              = stmt.executeQuery();
            if (rs.next()) {
                if (rs.getString(1) != null) { daysToExculde = rs.getString(1); }
            }
            
            stmt                            = conn.prepareStatement(selectDayCount);
            stmt.setInt(1, days);
            stmt.setInt(2, location);
            rs                              = stmt.executeQuery();
            if (rs.next()) {
                dayCount                    = rs.getInt(1);
            }
            
            stmt                            = conn.prepareStatement(selectDate);
            stmt.setInt(1, days);
            rs                              = stmt.executeQuery();
            if (rs.next()) {
                period                      = rs.getString(1);
            }
            
            String selectSummary            = "SELECT c.name Customer, l.name Location, li.lineNo Handle, GROUP_CONCAT(DISTINCT p.name) Products, SUM(lS.value) Volume "
                                            + " FROM location l LEFT JOIN customer c ON c.id = l.customer LEFT JOIN locationDetails lD ON lD.location = l.id "
                                            + " LEFT JOIN system s ON s.location = l.id LEFT JOIN line li ON li.system = s.id "
                                            + " LEFT JOIN lineSummary lS ON lS.line = li.id LEFT JOIN product p ON p.id = li.product "
                                            + " WHERE lD.active = 1 AND l.id = ? AND lD.pouredUp = 1 AND lS.date > SUBDATE(NOW(), INTERVAL ? + 2 DAY) AND lS.date < DATE(NOW()) "
                                            + (daysToExculde.length() > 0 ? " AND lS.date NOT IN (" + daysToExculde + ")" : "")
                                            + " GROUP BY s.id, li.lineIndex ORDER BY volume DESC, CAST(li.lineNo AS UNSIGNED); " ;
            logger.debug(selectSummary);
            
            stmt                            = conn.prepareStatement(selectSummary);
            stmt.setInt(1, location);
            stmt.setInt(2, days);
            rs                              = stmt.executeQuery();
            while (rs.next()) {
                customerName                = rs.getString(1);
                locationName                = rs.getString(2);
                String handle               = rs.getString(3);
                String productName          = rs.getString(4);
                double volume               = rs.getDouble(5);
                if (volume > 10.0) {
                    double ozPerDay         = (volume / dayCount);
                    double kegTurnOver      = (ozPerDay * 30) / 1984;

                    tapSum                  += kegTurnOver;
                    tapCount++;
                    handle                  = String.valueOf(tapCount);
                    lineMap.put(tapCount,handle);
                    handleMap.put(tapCount, productName);
                    handleValueMap.put(tapCount, kegTurnOver);
                }
            }  
            double average                  = tapSum/tapCount;
            printableMenu menuTemplate      = new printableMenu();            
            menus                           = menuTemplate.getBeerBoardIndexTemplate(customerName, locationName, period, tapSum, average, tapCount, dayCount, lineMap, handleMap, handleValueMap, toAppend, logger);
            for(int i=0;i<menus.size();i++){
                Element dataEl              = toAppend.addElement("printDoc");
                dataEl.addElement("page").addText(String.valueOf(i+1));
                dataEl.addElement("html").addText(menus.get(i));
            }
            String mTop                     = "0.cm";
            String mRight                   = "0.1cm";
            String mBottom                  = "0.0cm";
            String mLeft                    = "0.1cm";
            String height                   = "11.0in";
            String width                    = "8.5in";
            String vMargin                  = ".5in";
            int zoom                        = 100;
            toAppend.addElement("mTop").addText(mTop);
            toAppend.addElement("mRight").addText(mRight);
            toAppend.addElement("mBottom").addText(mBottom);
            toAppend.addElement("mLeft").addText(mLeft);
            toAppend.addElement("height").addText(String.valueOf(height));
            toAppend.addElement("width").addText(String.valueOf(width));

            toAppend.addElement("vMargin").addText(vMargin);
            toAppend.addElement("zoom").addText(String.valueOf(zoom));
            toAppend.addElement("head").addText("");
            toAppend.addElement("headerHtml").addText("");
            toAppend.addElement("footerHtml").addText("");
            toAppend.addElement("waterMark").addText(String.valueOf(0));
            toAppend.addElement("pageBackgroundColor").addText("");
            /*if(waterMark>0){
                toAppend.addElement("waterMarkBackground").addText(String.valueOf(waterMarkBackground));
                toAppend.addElement("waterMarkUrl").addText(waterMarkUrl);
                toAppend.addElement("xOffset").addText(String.valueOf(xOffset));
                toAppend.addElement("yOffset").addText(String.valueOf(yOffset));
            }*/

        } catch(Exception e) {
            logger.debug(e.getMessage());
        } finally {
            close(rs);
            close(stmt);
        }
         
    } 

}
