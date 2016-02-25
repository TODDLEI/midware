/*
 * GroupingKeyFactory.java
 *
 * Created on January 12, 2007, 3:50 PM
 *
 * (c) 2007 Terakeet Corp.
 */

package net.terakeet.soapware.handlers.report;

import net.terakeet.soapware.RegisteredConnection;

// GroupingKeyFactory grouping1 = GroupingKeyFactory.getInstance(GroupingType.LOCATION,....)

/**
 * This is an abstract class for generating GroupingKey for the Grouper class.
 */
public abstract class GroupingKeyFactory {   
    
    public static GroupingKeyFactory getInstance(GroupingType type, ReportDescriptor rd, RegisteredConnection conn) {
        switch (type) {
            case PRODUCT:
            case LOCATION:
            case NONE:
                return new SimpleGroupingKeyFactory(type);
            case CUSTOMER:
                return new CustomerKeyFactory(rd,conn);
            case DAY:
            case MONTH:
            case DAY_OF_WEEK:
            case WEEK:
                return new DateKeyFactory(type);
            default:
                throw new IllegalArgumentException("Unsupported type in GroupingKeyFactory: "+type);           
        }
    }
    
    public abstract GroupingKey getKey(ReportSummary rs);
    
}
