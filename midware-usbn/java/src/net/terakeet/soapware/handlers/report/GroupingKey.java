/*
 * GroupingKey.java
 *
 * Created on January 2, 2007, 4:20 PM
 *
 * (c) 2007 Terakeet Corp.
 */

package net.terakeet.soapware.handlers.report;

import java.lang.Comparable;

/**
 * The grouping key used to categorize and sort result get from the SummaryFactory.
 */
public class GroupingKey implements Comparable<GroupingKey>{
    
    private GroupingType type;
    private String value;
    
    public GroupingKey() {
    }
    
    public GroupingKey(GroupingType newType, String newValue){
        type = newType;
        value = newValue;
    }
    
    public void setType(GroupingType newType){
        type = newType;
    }
    
    public GroupingType getType(){
        return type;
    }
    
    public void setValue(String newValue){
        value = newValue;
    }
    
    public String getValue(){
        return value;
    }
    
    public String toString() {
        return type.toString()+": "+value;
    }
    
    public boolean equals(Object o) {
        if (o instanceof GroupingKey) {
            GroupingKey key2 = (GroupingKey) o;
            return (this.type == key2.type && this.value.equals(key2.value));
        }
        return false;
    }
    
    public int hashCode() {
        return type.hashCode() + value.hashCode() * 37;
    }
    
    /**
     * This implements the compareTo method of the Comparable interface.
     * If the type of two GroupingKeys are not equivalent, a ClassCastException will be throwed.
     * If the type is "NONE" or "UNKNOWN", 0 is returned.
     */
    public int compareTo(GroupingKey o){
        if(o.type != type){
            throw new ClassCastException();
            //return 0;
        }
        
        switch(type){
            case DAY:
            case WEEK:
                return value.compareTo(o.getValue());
            
            case MONTH: return (getMonthFig(value) - getMonthFig(o.getValue()));
            
            case DAY_OF_WEEK: return (getDayOfWeekFig(value) - getDayOfWeekFig(o.getValue()));
            
            case LOCATION:
            case CUSTOMER:
            case PRODUCT:
                return (Integer.parseInt(value) - Integer.parseInt(o.getValue()));

            case NONE:
            default:
                return 0;
        }
    }
    
    private int getMonthFig(String m){
        if(m.equals("January")){
            return 0;
        } else if(m.equals("February")){
            return 1;
        } else if(m.equals("March")){
            return 2;
        } else if(m.equals("April")){
            return 3;
        } else if(m.equals("May")){
            return 4;
        } else if(m.equals("June")){
            return 5;
        } else if(m.equals("July")){
            return 6;
        } else if(m.equals("August")){
            return 7;
        } else if(m.equals("September")){
            return 8;
        } else if(m.equals("October")){
            return 9;
        } else if(m.equals("November")){
            return 10;
        } else if(m.equals("December")){
            return 11;
        } else{
            return -1;
        }
    }
    
    private int getDayOfWeekFig(String dow){
        if(dow.equals("Monday")){
            return 2;
        } else if(dow.equals("Tuesday")){
            return 3;
        } else if(dow.equals("Wednesday")){
            return 4;
        } else if(dow.equals("Thursday")){
            return 5;
        } else if(dow.equals("Friday")){
            return 6;
        } else if(dow.equals("Saturday")){
            return 7;
        } else if(dow.equals("Sunday")){
            return 8;
        } else{
            return -1;
        }
    }
}
