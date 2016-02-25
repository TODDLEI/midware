/*
 * DatePartitionTree.java
 *
 * Created on August 29, 2005, 2:05 PM
 *
 */

package net.terakeet.soapware.handlers.report;

import java.util.Collection;
import java.util.TreeSet;
import java.util.Date;
import java.util.NoSuchElementException;

/** DatePartitionTree
 *
 */
public class DatePartitionTree {
    
    private TreeSet<DatePartition> ts;
    
    /** Creates a new instance of DatePartitionTree */
    public DatePartitionTree(Collection<DatePartition> dps) {
        if (dps == null) {
            throw new NullPointerException("dps is null");
        }
        if (dps.size() < 1) {
            throw new IllegalArgumentException("dps is empty");
        }
        ts = new TreeSet<DatePartition>(dps);
    }
    
    public int getIndex(Date d) {
        int result = 0;
        try {
            DatePartition dp = ts.headSet(new DatePartition(d)).last();
            result = dp.getIndex();
        } catch (NoSuchElementException nsee) {
            //TODO:  confirm that no-effect is the desired behavior for this condition
        }
        return result;
    }
    
    public int size() {
        return ts.size();
    }
}
