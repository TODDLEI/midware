/*
 * LineReading.java
 *
 * Created on March 1, 2006, 10:58 AM
 *
 */

package net.terakeet.usbn;

import java.util.Date;

/** LineReading
 *
 */
public class LineReading {
    
    double value;
    int line;
    Date date;
    
    /** Creates a new instance of LineReading */
    public LineReading(int l, double v, Date d) {
        value = v;
        line = l;
        date = new Date(d.getTime());
    }
    
    public Date getDate() {
        return new Date(date.getTime());
    }
    
    public double getValue() {
        return value;
    }
    
    public int getLine() {
        return line;
    }
    
    public long getTimeDifference(LineReading prev) {
        if (prev == null) return 0;
        return date.getTime()-prev.date.getTime();
    }
    
    public double getValueDifference(LineReading prev) {
        if (prev == null) return value;
        return value-prev.value;
    }
    
    public boolean isSpike() {
        return (value < -0.1);
    }
    
    public boolean isMoreRecentThan(LineReading lr) {
        return (date.compareTo(lr.date)>=0);
    }
    
    public boolean isGreaterEqualTo(LineReading lr) {
        return (lr.isSpike() || this.isSpike() || this.value >= lr.value);
    }
    
    public boolean isLesserEqualTo(LineReading lr) {
        return (lr.isSpike() || this.isSpike() || this.value <= lr.value);
    }
    
    /** Checks that this reading fits between two other readings in terms of
     * timestamp 
     */
    public boolean isInTimeSequence(LineReading prev, LineReading next) {
        return
           (this.isMoreRecentThan(prev)
            && next.isMoreRecentThan(this)
            );
    }
   
    /** Checks that this reading fits between two other readings in terms of line value
     */
    public boolean isInValueSequence(LineReading prev, LineReading next) {
         return
           (this.isGreaterEqualTo(prev)
            && next.isGreaterEqualTo(this)
            );       
    }
    
    /** Returns the difference between this reading and the previous reading.
     *  Negative results will be returned as zero.  If either this reading or
     *  the previous reading is a spike, the result will be zero.  
     */
    public double spikeTolerantDifference(LineReading prev) {
        if (prev == null || this.isSpike() || prev.isSpike() ) {
            return 0d;
        } else {
            return Math.max(this.value-prev.value, -1);
        }
            
    }
    
}
