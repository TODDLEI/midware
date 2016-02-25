/*
 * GameSet.java
 *
 * Created on August 30, 2005, 4:29 PM
 *
 * A representation of a period of readings on a line.
 */

package net.terakeet.soapware.handlers.report;

import java.util.Date;

/**
 *
 * @author Administrator
 */
public class GameSet {
    
    private int station, product, game;
    private double firstValue = 0.0;
    private Date firstDate = null;
    private int readingCount = 0;
    private double offset = 0.0;
    
    /** Creates a new instance of GameSet */
    public GameSet(int station, int product, int game) {
        this.station = station;
        this.product = product;
        this.game = game;
    }
    
    /** Updates the line period values for a reading.
     *  These readings MUST be added in chronological order
     * @param value the line reading value
     * @param d the line reading date
     */
    public void addReading(double value, Date d) {
        firstDate = d;
        firstValue = value;
        readingCount++;
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
    public double getFirstValue() {
        return firstValue;
    }

    /** Gets the date of the first line reading in the period.
     * @return the first line reading date
     */
    public Date getFirstDate() {
        return firstDate;
    }

    /** Gets the number of line reading in the period.
     * @return the number of line readings
     */
    public int getGameId() {
        return game;
    }

    /** Gets the value of the first line reading in the period.
     * @return the first line reading value
     */
    public int getStation() {
        return station;
    }

    /** Gets the date of the first line reading in the period.
     * @return the first line reading date
     */
    public int getProduct() {
        return product;
    }
    
}
