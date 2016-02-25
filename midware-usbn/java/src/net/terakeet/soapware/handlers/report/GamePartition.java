/*
 * GamePartition.java
 *
 * Created on August 29, 2005, 1:39 PM
 *
 */

package net.terakeet.soapware.handlers.report;

/** GamePartition
 * A lightweight wrapper for an immutable tuple of Date and int.  
 * The methods compareTo,equals,and hashCode are passed
 * directly to the encapsulated Date.   
 */
public class GamePartition implements Comparable {
    
    private final int i;
    private final int gameId;

    /** Creates a new instance of GamePartition */
    public GamePartition(int gameId, int i) {
        this.i = i;
        this.gameId = gameId;
    }
    
    /** Creates a new instance of GamePartition without an index */
    public GamePartition(int gameId) {
        this(gameId, -1);
    }

    public int getIndex() {
        return i;
    }
    
    public int getGameIndex() {
        return gameId;
    }
    
    public int compareTo(Object o) {
        if (o instanceof GamePartition) {
            GamePartition d2 = (GamePartition) o;

            if (gameId==d2.gameId) {
                return 0;
            } else {
                return 1;
            }
        } else {
            throw new ClassCastException("Argument to compareTo is not a GamePartition");
        }
    }
    
    public String toString() {
        return "("+String.valueOf(gameId)+") "+String.valueOf(i);
    }
    
}
