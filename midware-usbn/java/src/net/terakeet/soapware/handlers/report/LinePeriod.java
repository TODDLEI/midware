/*
 * LinePeriod.java
 *
 * Created on August 30, 2005, 4:29 PM
 *
 * A representation of a period of readings on a line.
 */

package net.terakeet.soapware.handlers.report;

import java.util.Date;
import net.terakeet.util.MidwareLogger;

/**
 *
 * @author Administrator
 */
public class LinePeriod {
    
    private int line;
    private double firstQuantity = 0.0;
    private double firstValue = 0.0;
    private Date firstDate = null;
    private double lastValue = 0.0;
    private Date lastDate = null;
    private int readingCount = 0;
    private double offset = 0.0;
    private boolean spikeNext = false;
    private boolean firstSpike = false;
    
    private static MidwareLogger logger = new MidwareLogger(LinePeriod.class.getName());
    
    /* reading decision constants */
    // This is the cutoff for the difference between readings that means we should check for a spike.
    // It doesn't mean its a spike, only that we should call the spike-checking method
    private static final double LARGE_ENOUGH_TO_CHECK_FOR_SPIKE = 200.0;
    // If a dip occurs, and the new reading is within this value from zero, its treated as a line reset
    private static final double SMALL_ENOUGH_TO_COUNT_AS_A_RESET = 0.01;
    // When checking for a rollover (1M to 0), and the top reading is within this value from 1M, it satisfies
    // the top requirement for a rollover
    private static final double ROLLOVER_TOP_PROXIMITY = 200.0;
    // When checking for a rollover, and the bottom reading is within this value from 0, it satisfies
    // the bottom requirement for a rollover
    private static final double ROLLOVER_BOTTOM_PROXIMITY = 200.0;  
    // The maximum value of a harapgon, where rollover occurs
    private static final double HARPAGON_MAX = 1000000.0; // One million
    
    
    /** Creates a new instance of LinePeriod */
    public LinePeriod(int line) {
        this.line = line;
    }
    
    /** Updates the line period values for a reading.
     *  These readings MUST be added in chronological order
     * @param value the line reading value
     * @param d the line reading date
     */
    public void addReading(double value, Date d) {
        if (null == firstDate || d.before(firstDate)) {
            if (value >= 0) {               
                firstDate = d;
                firstValue = value;
            } else {
                //logger.debug("FIRST READING IS A SPIKE at "+d);
                firstSpike = true;
                readingCount++;
                return;
            }
        }
        if (null == lastDate || d.after(lastDate)) {
            /* There are four cases to consider:
             * #1  Normal increase.  No action needed
             * #2  Spike increase.  If the growth is too fast (according to some function), then
             *      we need to ignore the change over this period.  We still set the new value
             *      as our current value, but we have to subtract the difference from our offset                    
             * #3   Decrease.  Any decrease means the lines were reset.  We need to add the
             *      difference to our offset but we have three options.
             *          Option A "a dip"   Like going from 15,000 to 13,000.  This should never
             *          happen in normal operation, but nonetheless, we should add the difference
             *          to our offset to ignore the movement over this period.
             *          Option B "a reset"  Goes from 15,000 to like 12.  In this case, the
             *          harapgon was reset to zero and we assume the 12 ounces on the reading
             *          were poured legit following the reset.  So we add the old value to
             *          the offset and ignore the new value.
             *          Option C "a rollover"  occurs when the harpagon ticks from 999,999.99
             *          to 0.0.  In this case we add a flat offset of 1,000,000. 
             * #4   Forced spike.  Used for line cleaning, we set a special flag that forces
             *      the next reading to be treated as a spike. A negative value in the reading
             *      table indicates a forced spike.    
             *
             */
            double difference = value - lastValue;
            //check large normal changes for spikes
            if (difference > LARGE_ENOUGH_TO_CHECK_FOR_SPIKE) {
                if ((spikeNext || isASpike(lastValue,lastDate,value,d)) && lastDate != null) {
                    //logger.debug("SPIKE on L"+line+" from "+lastValue+" at "+lastDate+" to "+value+" at "+d);
                    addOffset(lastValue - value);
                    spikeNext = false;
                    lastDate = d;
                    lastValue = value;
                } else {
                    //normal reading
                    lastDate = d;
                    lastValue = value;
                }
            // small normal changes
            } else if (difference >= 0.0) {
                if (spikeNext) {
                    //logger.debug("FORCED SPIKE on L"+line+" from "+lastValue+" at "+lastDate+" to "+value+" at "+d);
                    addOffset(lastValue - value);
                    spikeNext = false;
                    lastDate = d;
                    lastValue = value;
                } else {                    
                    //normal reading
                    lastDate = d;
                    lastValue = value;
                }
            // check for forced spikes
            } else if (value < 0) {
                //logger.debug("ADDING SPIKE FLAG on L"+line+" from "+lastValue+" at "+lastDate+" to "+value+" at "+d);
                spikeNext = true;
            // we have a dip, reset, or rollover since the difference is < 0
            } else {
                // Rollover
                if ((HARPAGON_MAX-lastValue) < ROLLOVER_TOP_PROXIMITY
                            && value < ROLLOVER_BOTTOM_PROXIMITY) {
                    logger.debug("ROLLOVER on L"+line+" from "+lastValue+" at "+lastDate+" to "+value+" at "+d);
                    addOffset(HARPAGON_MAX);
                    lastDate = d;
                    lastValue = value;
                    spikeNext = false;
                // Reset
                } else if (value < SMALL_ENOUGH_TO_COUNT_AS_A_RESET) {
                    logger.debug("LINE RESET on L"+line+" from "+lastValue+" at "+lastDate+" to "+value+" at "+d);
                    addOffset(lastValue);
                    lastDate = d;
                    lastValue = value;
                    spikeNext = false;    
                // Dip
                } else {
                    logger.debug("DIP on L"+line+" from "+lastValue+" at "+lastDate+" to "+value+" at "+d);
                    addOffset(lastValue-value);
                    lastDate = d;
                    lastValue = value;
                    spikeNext = false;
                }
            }        
        }
        readingCount++;
    }
    
    public void addReading(double value, double quantity, Date d) {
        if (null == firstDate || d.before(firstDate)) {
            //logger.debug("Adding first reading @ "+d+" for: "+value+" with v:"+quantity+"oz");
            if (value >= 0) {
                firstDate = d;
                firstValue = value;
                firstQuantity = quantity;
            } else {
                //logger.debug("FIRST READING IS A SPIKE at "+d);
                firstSpike = true;
                readingCount++;
                return;
            }
        }
        if (null == lastDate || d.after(lastDate)) {
            /* There are four cases to consider:
             * #1  Normal increase.  No action needed
             * #2  Spike increase.  If the growth is too fast (according to some function), then
             *      we need to ignore the change over this period.  We still set the new value
             *      as our current value, but we have to subtract the difference from our offset
             * #3   Decrease.  Any decrease means the lines were reset.  We need to add the
             *      difference to our offset but we have three options.
             *          Option A "a dip"   Like going from 15,000 to 13,000.  This should never
             *          happen in normal operation, but nonetheless, we should add the difference
             *          to our offset to ignore the movement over this period.
             *          Option B "a reset"  Goes from 15,000 to like 12.  In this case, the
             *          harapgon was reset to zero and we assume the 12 ounces on the reading
             *          were poured legit following the reset.  So we add the old value to
             *          the offset and ignore the new value.
             *          Option C "a rollover"  occurs when the harpagon ticks from 999,999.99
             *          to 0.0.  In this case we add a flat offset of 1,000,000.
             * #4   Forced spike.  Used for line cleaning, we set a special flag that forces
             *      the next reading to be treated as a spike. A negative value in the reading
             *      table indicates a forced spike.
             *
             */
            double difference = value - lastValue;
            //check large normal changes for spikes
            if (difference > LARGE_ENOUGH_TO_CHECK_FOR_SPIKE) {
                if ((spikeNext || isASpike(lastValue,lastDate,value,d)) && lastDate != null) {
                    //logger.debug("SPIKE on L"+line+" from "+lastValue+" at "+lastDate+" to "+value+" at "+d);
                    addOffset(lastValue - value);
                    spikeNext = false;
                    lastDate = d;
                    lastValue = value;
                } else {
                    //normal reading
                    lastDate = d;
                    lastValue = value;
                }
            // small normal changes
            } else if (difference >= 0.0) {
                if (spikeNext) {
                    //logger.debug("FORCED SPIKE on L"+line+" from "+lastValue+" at "+lastDate+" to "+value+" at "+d);
                    addOffset(lastValue - value);
                    spikeNext = false;
                    lastDate = d;
                    lastValue = value;
                } else {
                    //normal reading
                    lastDate = d;
                    lastValue = value;
                }
            // check for forced spikes
            } else if (value < 0) {
                //logger.debug("ADDING SPIKE FLAG on L"+line+" from "+lastValue+" at "+lastDate+" to "+value+" at "+d);
                spikeNext = true;
            // we have a dip, reset, or rollover since the difference is < 0
            } else {
                // Rollover
                if ((HARPAGON_MAX-lastValue) < ROLLOVER_TOP_PROXIMITY
                            && value < ROLLOVER_BOTTOM_PROXIMITY) {
                    logger.debug("ROLLOVER on L"+line+" from "+lastValue+" at "+lastDate+" to "+value+" at "+d);
                    addOffset(HARPAGON_MAX);
                    lastDate = d;
                    lastValue = value;
                    spikeNext = false;
                // Reset
                } else if (value < SMALL_ENOUGH_TO_COUNT_AS_A_RESET) {
                    logger.debug("LINE RESET on L"+line+" from "+lastValue+" at "+lastDate+" to "+value+" at "+d);
                    addOffset(lastValue);
                    lastDate = d;
                    lastValue = value;
                    spikeNext = false;
                // Dip
                } else {
                    logger.debug("DIP on L"+line+" from "+lastValue+" at "+lastDate+" to "+value+" at "+d);
                    addOffset(lastValue-value);
                    lastDate = d;
                    lastValue = value;
                    spikeNext = false;
                }
            }
        }
        readingCount++;
    }
    
    /** Detects if two readings are sufficiently different to be a "spike".  The 
     *  change that occurs during a spike is ignored in a poured report.
     */
    private boolean isASpike(double prev, Date prevDate, double current, Date currentDate) {
        //Current Implementation: a spike is any pour greater than 5 oz / sec over the period:
        //Also, the period must be at least five seconds, or the readings are ignored
        final double maxRate = 5.0 / 1000.0; //ounces per millisecond
        final long fiveSeconds = 5 * 1000;
        
        if (prevDate == null || currentDate == null ) { return false; }
        long timeDifference = currentDate.getTime()-prevDate.getTime();
        if (timeDifference < fiveSeconds) { return false; }
        double ozDifference = current - prev;
        double rate = ozDifference / timeDifference;
        //logger.debug("pourRate: " + String.valueOf(rate));
        return (rate >= maxRate);
    }
    
    /** Adds an offset for line readings in a period.
     * @param offset an offset for the line readings
     */
    public void addOffset(double offset) {
        this.offset += offset;
    }

    /** Gets the offset of for line readings in a period.
     * @return the offset for the line readings
     */
    public double getOffset() {
        return offset;
    }

    /** Gets the number of line reading in the period.
     * @return the number of line readings
     */
    public int getReadingCount() {
        return readingCount;
    }

    /** Gets the value of the first line reading in the period.
     * @return the first line reading value
     */
    public double getFirstQuantity() {
        return firstQuantity;
    }

    /** Gets the value of the first line reading in the period.
     * @return the first line reading value
     */
    public double getFirstValue() {
        return firstValue;
    }

    /** Gets the date of the first line reading in the period.
     * @return the first line reading date
     */
    public Date getFirstDate() {
        return firstDate;
    }
    
    /** Gets the value of the last line reading in the period.
     * @return the last line reading value
     */
    public double getLastValue() {
        return lastValue;
    }

    /** Gets the date of the last line reading in the period.
     * @return the last line reading date
     */
    public Date getLastDate() {
        return lastDate;
    }
    
    /** Returns true if the first value in this period is a spike
     */
    public boolean startsWithASpike() {
        return firstSpike;
    }

    /** Returns true if the last value in this period is a spike
     */
    public boolean endsWithASpike() {
        return spikeNext;
    }
    
}
