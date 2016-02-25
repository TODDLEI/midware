/*
 * Grouper.java
 *
 * Created on January 2, 2007, 4:18 PM
 *
 * (c) 2007 Terakeet Corp.
 */

package net.terakeet.soapware.handlers.report;

import java.util.*;
//import java.sql.Timestamp;
import java.util.regex.*;
import java.sql.*;
import java.text.*;
import net.terakeet.soapware.*;
import net.terakeet.util.MidwareLogger;
import net.terakeet.util.LRUCache;
//import org.dom4j.jaxb.JAXBModifier;

/**
 * The grouper class provides methods to group the results by the customized criterions.
 * Results will be sorted by the column and row criteria from the ReportDescriptor, output to a table-like XML result.
 * (The XML processing will be done by the XmlFormatter class).
 * Usage:
 * 1. Instantiate a Grouper class with the ReportDescripter and SummaryFactory class.
 * 2. Call Grouper.process method, and expect to get the result in a Map class.
 */
public class Grouper {
    
    private SummaryFactory sfMember;
    private ReportDescriptor rdMember;
//    private ArrayList<ReportSummary> rsList;
    private MidwareLogger logger;
    private ArrayList<ElapsedTime> timeList;
    private RegisteredConnection conn;
    //private final static Pattern noTime = Pattern.compile("^(\\d{4}-\\d{2}-\\d{2})");
    private LRUCache<String,String> dayToWeekCache;
    
    private class CompResult {
        public int numResult;
        public String[] names;
    }
    
    public Grouper(ReportDescriptor descriptor, SummaryFactory sf, RegisteredConnection connection, int cacheSize) {
        conn = connection;
        sfMember = sf;
        rdMember = descriptor;
        
//        rsList = new ArrayList<ReportSummary>();
        logger = new MidwareLogger(Grouper.class.getName());
        timeList = new ArrayList<ElapsedTime>();
        if(0 < cacheSize){
            dayToWeekCache = new LRUCache<String, String>(cacheSize);
        } else {
            dayToWeekCache = null;
        }
    }
    
    public ArrayList<ElapsedTime> getBenchmarkData(){
        return timeList;
    }
    
    /**
     * Group the results according to the parameters.
     */
    public Map<GroupingKey,GroupBox> process() throws HandlerException{
//        Map<GroupingKey, GroupBox> result = new Hashtable<GroupingKey, GroupBox>();
        Map<GroupingKey, GroupBox> result = new TreeMap<GroupingKey, GroupBox>();
        ElapsedTime timer = new ElapsedTime("Grouping and Delivery Time");
        GroupingKeyFactory groupKeyFactory = GroupingKeyFactory.getInstance(rdMember.getPrimaryGrouping(),rdMember,conn);
        GroupingKeyFactory valueKeyFactory = GroupingKeyFactory.getInstance(rdMember.getValueGrouping(),rdMember,conn);
        while(sfMember.hasMoreSummaries()){
            for (ReportSummary rs : sfMember.getNext()) {
                GroupingKey groupKey = groupKeyFactory.getKey(rs); // findKey(rdMember.getPrimaryGrouping(),rs);
                GroupingKey valueKey = valueKeyFactory.getKey(rs); //findKey(rdMember.getValueGrouping(),rs);
                GroupBox gBox = getOrInitialize(result,groupKey);
                ValueBox vBox = getOrInitialize(gBox,valueKey);
                vBox.consume(rs);
                //logger.debug("Keys for "+rs.toString()+": G="+groupKey.toString()+",    V="+valueKey.toString());
            }
        }
        timer.stopTimer();
        // subtract the SF's time from our time
        double myTime = timer.getTime();
        for (ElapsedTime sfTime: sfMember.getQueryTimes()) {
            myTime -= sfTime.getTime();
        }
        timeList.add(new ElapsedTime(myTime,timer.getDescription()));
        return result;
    }
    
    /**  Find the box in the specified map, or create it if it doesn't exist */
    private GroupBox getOrInitialize(Map<GroupingKey,GroupBox> map, GroupingKey key) {
        GroupBox gBox = map.get(key);
        if(null == gBox){
            //logger.debug("> > > Making a new GroupBox for "+key.getValue());
            gBox = new GroupBox();
            map.put(key, gBox);
        }
        return gBox;
    }
    
    /**  Find the box in the specified map, or create it if it doesn't exist */
    private ValueBox getOrInitialize(GroupBox map, GroupingKey key) {
        ValueBox vBox = map.get(key);
        if(null == vBox){
            //logger.debug("> > > Making a new ValueBox for "+key.getValue());
            vBox = new ValueBox();
            map.put(key, vBox);
        }
        return vBox;
    }
/*    
    private void close(Statement s) {
        if (s != null) {
            try { s.close(); } catch (SQLException sqle) { }
        }
    }
    private void close(ResultSet rs) {
        if (rs != null) {
            try { rs.close(); } catch (SQLException sqle) { }
        }
    }
    private void close(Connection c) {
        if (c != null) {
            try { c.close(); } catch (SQLException sqle) { }
        }
    }
    private void close(RegisteredConnection c) {
        c.close();
    }    
    
*/    
    
    
    
    
    
//    /**
//     * This is the public method for the users to call.
//     * This will provide a map containing the grouped
//     */
//    public Map<GroupingKey,GroupBox> process() throws HandlerException{
//        Map<GroupingKey, GroupBox> ret = new Hashtable<GroupingKey, GroupBox>();
//        ElapsedTime timer = new ElapsedTime("One round of process");
//        
//        // same grouping and valueset types make no sense.
////        if(rdMember.getPrimaryGrouping().equals(rdMember.getValueGrouping())){
////            return null;
////        }
//        
//        while(sfMember.hasMoreSummaries()){
//            Set<ReportSummary> rsNext = sfMember.getNext();
//            for(ReportSummary sum : rsNext){
//                GroupingKey key = new GroupingKey(rdMember.getPrimaryGrouping(),
//                        getValueByType(rdMember.getPrimaryGrouping(), sum));
//                GroupBox box = ret.get(key);
//                if(null == box){
//                    box = new GroupBox();
//                    ret.put(key, box);
//                }
////                logger.debug("Grouping box created/updated with key " + key.getValue());
//                GroupingKey boxKey = new GroupingKey(rdMember.getValueGrouping(),
//                        getValueByType(rdMember.getValueGrouping(), sum));
//                ValueBox vBox = box.get(boxKey);
//                if(null == vBox){
//                    vBox = new ValueBox();
//                    box.put(boxKey, vBox);
//                }
//                vBox.consume(sum);
////                logger.debug("Value box created/updated with key " + key.getValue());
//            }
//            
////            rsList.addAll(sfMember.getNext());
//        }
//        timer.stopTimer();
//        timeList.add(timer);
//        
        // For debug only
//        logger.debug(String.valueOf(rsList.size()) + " ReportSummary objects received from the SummaryFactory.");
        /////////////////////////////////////
/*
        CompResult cr = getColumns(rdMember.getPrimaryGrouping(), rdMember.getStartDate(), rdMember.getEndDate());
        if(null == cr){
            logger.debug("Invalid date format or Invalid date range in ReportDescriptor!");
            throw new HandlerException("Invalid date format or Invalid date range in ReportDescriptor!");
        }
        logger.debug(String.valueOf(cr.numResult) + " results from getColumns, they are: " + cr.names.toString());
 */
        // Column by DAY, WEEK, DAY_OF_WEEK, MONTH
/*        if(cr.numResult > 0){
            for(int i = 0; i < cr.numResult; i++){
                ReportSummary sum = rsList.get(i);
                GroupingKey key = new GroupingKey(rdMember.getPrimaryGrouping(), cr.names[i]);
                GroupBox box = new GroupBox();
                logger.debug("Creating new group box with key "  + key.getValue());
                for(int j = 0; j < rsList.size(); j++){
                    if(validateItem(sum)){
                        ValueBox vBox = new ValueBox();
                        logger.debug("Starting typeMatch");
                        if(typeMatch(key, sum)){
                            GroupingKey boxKey = new GroupingKey(rdMember.getValueGrouping(),
                                    getValueByType(rdMember.getValueGrouping(), sum));
                            vBox.consume(sum);
                            box.put(boxKey, vBox);
                            logger.debug("Creating new valud box with key " + boxKey.getValue());
                        }
                    }
                }
                ret.put(key, box);
            }
        }
        // Column by CUSTOMER, LOCATION, PRODUCT
        else { */
/*            for(ReportSummary sum : rsList){
//                if(validateItem(sum)){
                    GroupingKey key = new GroupingKey(rdMember.getPrimaryGrouping(),
                            getValueByType(rdMember.getPrimaryGrouping(), sum));
                    GroupBox box = ret.get(key);
                    if(null == box){
                        box = new GroupBox();
                        ret.put(key, box);
                    }
                    logger.debug("Grouping box created/updated with key " + key.getValue());
                    GroupingKey boxKey = new GroupingKey(rdMember.getValueGrouping(),
                            getValueByType(rdMember.getValueGrouping(), sum));
                    ValueBox vBox = box.get(boxKey);
                    if(null == vBox){
                        vBox = new ValueBox();
                        box.put(boxKey, vBox);
                    }
                    vBox.consume(sum);
                    logger.debug("Value box created/updated with key " + key.getValue());
//                }
            }
//        }
// */
//        return ret;
//    }    
//    /**
//     * Returns the number of columns the grouper object will generate.
//     * This information is essential in creating the group boxes.
//     * For groupingtypes that the column number cannot be determined, a "0" is returned.
//     * @param type The grouping type, how we are going to calculate the number
//     * @param startDate The start date of this search
//     * @param endDate The end date of this search
//     * @return For DAY, DAY_OF_WEEK, WEEK, MONTH, returns both the number of result and the result names;
//     *         For the rest, number of result is 0 and the name array is left untouched.
//     * @author Qi Liu
//     */
//    private CompResult getColumns(GroupingType type, String startDate, String endDate){
//        CompResult result = new CompResult();
//        
//        Calendar start = getTimestamp(startDate);
//        Calendar end = getTimestamp(endDate);
//        if((null == start) || (null == end)){
//            return null;
//        }
//        
//        long startFig = start.getTimeInMillis();
//        long endFig = end.getTimeInMillis();
//        if((endFig - startFig) <= 0){
//            return null;
//        }
//        
//        // Use integer calculation, truncate the decimals
//        int startHr  = (int)(startFig / 3600000); //60*60*1000
//        int endHr = (int)(endFig / 3600000);
//        int startDay = (int)startHr / 24;
//        int endDay = (int)endHr / 24;
//        
//        switch(type){
//            case DAY:
//                result.numResult = (endDay - startDay);
//                long curDay = startFig;
//                Calendar caTmp = new GregorianCalendar();
//                for(int i = 0; i < result.numResult; i++){
//                    caTmp.setTimeInMillis(curDay);
//                    result.names[i] = getDateString(caTmp);
//                    curDay += DAYINTERVAL;
//                }
//                break;
//                
//                // need some work here, no dates are shown.
//                // more work are done in the parsing methods. It is currently OK with it.
//            case WEEK:
//                result.numResult = ((endDay - (end.get(Calendar.DAY_OF_WEEK) - 1) - startDay - (8 - start.get(Calendar.DAY_OF_WEEK))) / 7 + 2);
//                result.names = new String[result.numResult];
//                for(int i = 0; i < result.numResult; i++){
//                    result.names[i] = "Week #" + String.valueOf(i + 1);
//                }
//                break;
//                
//            case MONTH:
//                result.numResult = (end.get(Calendar.YEAR) - start.get(Calendar.YEAR)) * 12 + end.get(Calendar.MONTH) - start.get(Calendar.MONTH);
//                result.names = new String[result.numResult];
//                int curMon = start.get(Calendar.MONTH);
//                int curYear = start.get(Calendar.YEAR);
//                for(int i = 0; i < result.numResult; i++){
//                    result.names[i] = getMonthString(curMon++) + " " + String.valueOf(curYear);
//                    if(curMon > 12){
//                        curMon = 1;
//                        curYear++;
//                    }
//                }
//                break;
//                
//            case DAY_OF_WEEK:
//                result.numResult = 7;
//                result.names = new String[result.numResult];
//                for(int i = 0; i < 6; i++){
//                    result.names[i] = getDayOfWeekString(i + 2);
//                }
//                result.names[6] = getDayOfWeekString(1);
//                break;
//                
//            default:
//                result.numResult = 0;
//                break;
//        }
//        
//        return result;
//    }
    
//    /**
//     * Validate the ReportSummary item with our filter and start/end date in the ReportDescriptor.
//     *  @deprecated we don't validate items anymore
//     */
//    private boolean validateItem(ReportSummary summary){
//        try{
//            long startFig = getTimestamp(rdMember.getStartDate()).getTimeInMillis();
//            long endFig = getTimestamp(rdMember.getEndDate()).getTimeInMillis();
//            long currentFig = getTimestamp(summary.getTimestamp()).getTimeInMillis();
////            logger.debug("Validation params: "+startFig+", "+endFig+", "+currentFig);
//            
//            if( (currentFig > endFig) || (currentFig < startFig) ){
//                logger.debug("validateItem failed.");
//                return false;
//            }
//            
//            // dealing with filter from the ReportDescriptor
//            int n = -1;
//            switch(rdMember.getFilterType()){
//                case CUSTOMER:
//                    n = Integer.valueOf(rdMember.getFilter());
//                    if(n != getCustomerId(summary.getLocation())){
//                        return false;
//                    }
//                    break;
//                    
//                case LOCATION:
//                    n = Integer.valueOf(rdMember.getFilter());
//                    if(n != summary.getLocation()){
//                        return false;
//                    }
//                    break;
//                    
//                case STATE:
//                    return false;
//                    
//                default:
//                    return false;
//            }
//        } catch(Exception exp){
//            return false;
//        }
//        
//        //logger.debug("validateItem complete!");
//        return true;
//    }
    
    
//    /**
//     * Decide if the given ReportSummary mathes the GroupingKey.
//     * This information is also based on the rdMember (the report descriptor inside the class)'s start
//     * and end date.
//     *
//     * Actually, for current status, if the program goes to this point, it pre-supposes that
//     * only the date-related types can appear in the type of the GroupingKey.
//     *
//     * @deprecated we dont check the types of summaries anymore
//     *
//     * @author Qi Liu
//     */
//    private boolean typeMatch(GroupingKey key, ReportSummary summary){
//        String name = key.getValue();
//        Calendar day = getTimestamp(summary.getTimestamp());
//        if(null == day){
//            return false;
//        }
//        
//        switch(key.getType()){
//            case DAY:
//                if(getDateString(day).equals(name)){
//                    return true;
//                }
//                return false;
//                
//            case WEEK:
//                String num = name.substring(name.indexOf("#") + 1);
//                int n = Integer.valueOf(num);
//                Calendar rdStart = getTimestamp(rdMember.getStartDate());
//                Calendar rdEnd = getTimestamp(rdMember.getEndDate());
//                if((null == rdStart) || (null == rdEnd)){
//                    return false;
//                }
//                int startDay = rdStart.get(Calendar.DAY_OF_WEEK);
//                long startFig = rdStart.getTimeInMillis();
//                long tmpFig = startFig + DAYINTERVAL * ((startDay == 1)?0:(8 - startDay));
//                long endFig = 0;
//                if(1 == n){
//                    endFig = tmpFig;
//                } else {
//                    startFig = tmpFig + 7 * (n - 1) * DAYINTERVAL;
//                    endFig = tmpFig + 7 * n * DAYINTERVAL;
//                }
//                long currentFig = day.getTimeInMillis();
//                if( (currentFig > startFig) && (currentFig < endFig) ){
//                    return true;
//                }
//                return false;
//                
//            case MONTH:
//                if(getMonthString(day.get(Calendar.MONTH)).equals(name)){
//                    return true;
//                }
//                return false;
//                
//            case DAY_OF_WEEK:
//                if(getDayOfWeekString(day.get(Calendar.DAY_OF_WEEK)).equals(name)){
//                    return true;
//                }
//                return false;
//                
//            case PRODUCT:
//            case LOCATION:
//            case CUSTOMER:
//            default:
//                return false;
//        }
//    }
}
