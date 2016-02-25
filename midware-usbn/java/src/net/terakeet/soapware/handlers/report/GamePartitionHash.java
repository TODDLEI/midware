/*
 * GamePartitionHash.java
 *
 * Created on August 29, 2005, 2:05 PM
 *
 */

package net.terakeet.soapware.handlers.report;

import java.util.Collection;
import java.util.HashMap;
import java.util.NoSuchElementException;
import java.util.Iterator;
/** DatePartitionTree
 *
 */
public class GamePartitionHash {
    
    private HashMap<Integer,Integer> hm;
    
    /** Creates a new instance of DatePartitionTree */
    public GamePartitionHash(Collection<GamePartition> gps) {
         hm = new HashMap<Integer,Integer>();
        if (gps == null) {
            throw new NullPointerException("gps is null");
        }
        if (gps.size() < 1) {
            throw new IllegalArgumentException("gps is empty");
        }
        Iterator<GamePartition> i = gps.iterator();

        while (i.hasNext()) {
            GamePartition gp = i.next();
            hm.put(gp.getGameIndex(), gp.getIndex());
        }
    }
    
    public int getGameIndex(int gameId) {
        int result = -1;
        try {
            Integer temp = hm.get(gameId);
            if (null != temp) {
                result = temp;
            }
        } catch (NoSuchElementException nsee) {
            //TODO:  confirm that no-effect is the desired behavior for this condition
        }
        return result;
    }
    
    public int size() {
        return hm.size();
    }
}
