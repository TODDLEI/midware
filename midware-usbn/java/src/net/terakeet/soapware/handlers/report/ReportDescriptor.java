/*
 * ReportDescriptor.java
 *
 * Created on January 2, 2007, 3:59 PM
 *
 */

package net.terakeet.soapware.handlers.report;

import net.terakeet.soapware.*;


/**
 */
public class ReportDescriptor {
    
    private GroupingType primaryGrouping;
    private GroupingType valueSet;
    private FilterType filterType;
    private String filter;
    private String startDate;
    private String endDate;
    private boolean poured;
    private boolean sold;
    private boolean variance;
    
    /** Create a new ReportDescriptor
     *  @param groupSetType column grouping
     *  @param valueSetType row grouping
     *  @param typeOfFilter filter
     *  @param filterValue value
     *  @param dateStart the start date ('YYYY-MM-DD')
     *  @param dateEnd the end date ('YYYY-MM-DD')
     *  @param includePoured a poured report
     *  @param includeSold a sold report
     *  @param includeVaraince a variance report
     */
    public ReportDescriptor(GroupingType groupSetType, GroupingType valueSetType, 
            FilterType typeOfFilter, String filterValue, String dateStart, String dateEnd, 
            boolean includePoured, boolean includeSold, boolean includeVariance) {
        primaryGrouping = groupSetType;
        valueSet = valueSetType;
        filterType = typeOfFilter;
        filter = filterValue;
        startDate = dateStart;
        endDate = dateEnd;
        poured = includePoured;
        sold = includeSold;
        variance = includeVariance;
    }
    
    /**
     * A string like this will pass through:
     * "day_of_week product location 1 2007-01-01 2007-01-31 true false false"
     */
    public ReportDescriptor(String content) throws HandlerException{
        if(!fromString(content)){
            throw new HandlerException("Bad parameters for constructing ReportDescriptor!");
        }
    }
    
    public GroupingType getPrimaryGrouping() {
        return primaryGrouping;
    }
    public GroupingType getValueGrouping() {
        return valueSet;
    }
    public FilterType getFilterType() {
        return filterType;
    }
    public String getFilter() {
        return filter;
    }
    public String getStartDate() {
        return startDate;
    }
    public String getEndDate() {
        return endDate;
    }
    public boolean includePoured() {
        return poured;
    }
    public boolean includeSold() {
        return sold;
    }
    public boolean includeVariance() {
        return variance;
    }
    
    /**
     * fill in a ReportDescriptor from a string.
     */
    private boolean fromString(String src){
        String[] parts = src.split(" ");
        int cnt = 0;
        if(parts.length >= 9){
            return false;
        }
        
        primaryGrouping = GroupingType.instanceOf(parts[cnt++]);
        valueSet = GroupingType.instanceOf(parts[cnt++]);
        filterType = FilterType.instanceOf(parts[cnt++]);
        filter = parts[cnt++];
        startDate = parts[cnt++];
        endDate = parts[cnt++];
        poured = "P".equalsIgnoreCase(parts[cnt++]);
        sold = "S".equalsIgnoreCase(parts[cnt++]);
        variance = "V".equalsIgnoreCase(parts[cnt++]);
        assert(cnt == 9);
        return true;
    }
    
    /**
     * override the toString method to dump the ReportDescriptor class to string. 
     * a simple serialization process.
     */
    public String toString(){
        StringBuilder ret = new StringBuilder();
        
        ret.append(primaryGrouping.toString() + " ");
        ret.append(valueSet.toString() + " ");
        ret.append(filterType.toString() + " ");
        ret.append(filter + " ");
        ret.append(startDate + " ");
        ret.append(endDate + " ");
        ret.append((poured ? "P":"-") + " ");
        ret.append((sold ? "S":"-") + " ");
        ret.append(variance ? "V":"-");
        
        return ret.toString();
    }
}
