/*
 * SimpleGroupingKeyFactory.java
 *
 * Created on January 12, 2007, 3:55 PM
 *
 * (c) 2007 Terakeet Corp.
 */

package net.terakeet.soapware.handlers.report;

/**
 * Returns the GroupingKey for PRODUCT, LOCATION and NONE.
 */
public class SimpleGroupingKeyFactory extends GroupingKeyFactory{
    
    GroupingType type;
    
    static final String noneKey = " "; // The single key to use for all NONE types
    
    public SimpleGroupingKeyFactory(GroupingType myType) {
        type = myType;
    }

    public GroupingKey getKey(ReportSummary rs) {
        String value = "";
        switch(type){
            case PRODUCT:
                value = String.valueOf(rs.getProduct());
                break;               
            case LOCATION:
                value = String.valueOf(rs.getLocation());
                break;
            case NONE:
                value = noneKey;
                break;
            default:
                throw new UnsupportedOperationException("SimpleGroupingFactory can't make keys of type: "+type);
        }
        return new GroupingKey(type,value);
                
    }
    
}
