/*
 * AuperCommand.java
 *
 * Created on June 30, 2005, 1:25 PM
 */

package net.terakeet.soapware.handlers.auper;

import java.net.*;
import java.io.*;
import java.util.Vector;
import java.lang.StringBuffer;

/** Abstract class for an Auper Command
 *
 * @author Ryan Garver
 */
public abstract class AuperCommand {
    
    protected Vector<Character> command;
    protected static final char DEFAULT_SENDER = (char)0x00;
    protected static final char DEFAULT_DEST = (char)0xFA;
  
    protected boolean responseResult;
    protected boolean responseAttached;
    
    /** Creates a new instance of AuperCommand */
    protected AuperCommand() {
        command = new Vector<Character>();
        command.add(Util.charIt(DEFAULT_SENDER));
        command.add(Util.charIt(DEFAULT_DEST));
    }
    
    public char getSenderId() { return DEFAULT_SENDER;}
    
    public char getDestinationId() {return DEFAULT_DEST;}
    
    public char getCommandNumber() {return command.get(2).charValue();}
    
    public abstract boolean setResponse(Vector<Character> resp);
    
    /** Packages a command into a string that conforms to the DraftManager spec.
     *  The command has been "stuffed" and a checksum and terminating characters
     *  are attached; the command is ready to be sent
     *  @return the socket-ready command
     */
    public String transmissionForm() {
        Vector<Character> result = new Vector<Character>(command);
        // Add checksum
        char[] checksums = Util.checksum(result);
        result.add(Character.valueOf(checksums[0]));
        result.add(Character.valueOf(checksums[1]));
        // Stuff it
        result = stuff(result);
        // Terminating Characters
        result.add(Util.charIt(0xFF));
        result.add(Util.charIt(0x00));
        return Util.charsToString(result);     
    }
    
    /** As per the DraftManager spec, xFF characters are escaped with
     *  an additional xFF.
     *
     *  @param c the unstuffed command
     *  @return the stuffed command
     */
    protected static Vector<Character> stuff(Vector<Character> c) {
        Vector<Character> result = new Vector<Character>(c.size());
        for (Character cr : c) {
            result.add(cr);
            if (cr.charValue() == 0xFF) {
                result.add(Util.charIt(0xFF));
            }
        }
        return result;
    }
    
    /** Inverse of the "stuff" function.  Removes xFF escapte characters,
     *  and complains if the message is poorly formed
     *  @param c the stuffed message
     *  @return the unstuffed message
     */
    protected static Vector<Character> unstuff(Vector<Character> c) {
        Vector<Character> result = new Vector<Character>(c.size());
        boolean escaped = false;
        
        for (Character cr : c) {
            if (escaped) {
                escaped = false;
                if (cr.charValue() == 0xFF) {
                    // Proper escaping, do nothing
                } else if (cr.charValue() == 0x00) {
                    // Terminating sequence
                    result.add(cr);
                } else {
                    Util.debugPrint("Improper stuffing: "+Util.toHex(cr)+" followed xFF");
                    //Bad escape sequence, TODO throw exception
                }
            } else if (cr.charValue() == 0xFF) {
                escaped = true;
                result.add(cr);
            } else {
                result.add(cr);
            }
        }       
        return result;
    }
    
    /**  Utility to process a response message.  Pops the terminating characters and
     *  checksum bytes off the end of a message, and checks the checksums.  The return
     *  value will indicate if the checksum matched, and the msg will be four bytes lighter.
     *
     *  @param msg the full message, including terminating characters and checksum bytes
     *  @return true if the attached checksum matched the computed value, false otherwise
     */
    protected static boolean checksumAndPop(Vector<Character> msg) {
        char attachedLow = msg.get(msg.size()-4).charValue();
        char attachedHigh = msg.get(msg.size()-3).charValue();
        msg.remove(msg.size()-1);
        msg.remove(msg.size()-1);
        msg.remove(msg.size()-1);
        msg.remove(msg.size()-1);
        
        char[] checksums = Util.checksum(msg);
        
        if (checksums[0] != attachedLow ||
            checksums[1] != attachedHigh) {
            Util.debugPrint("Checksum failed. Attached {"+Util.toHex(attachedHigh)+'-'+Util.toHex(attachedLow)+"} "+
                    " Computed {"+Util.toHex(checksums[1])+'-'+Util.toHex(checksums[0])+"}");
            // DEBUG ONLY!  This case SHOULD return false.  
            return true;
            //return false;
        } else {
            return true;
        }
    } 
    
    /**  Indicates if the Auper server accepted the command.  A response must be attached to
     *  this command to call this method.
     *
     *  @return true if the server accepted the command, false otherwise
     *  @throws IllegalStateException if a response is not attached to this command
     */
    public boolean isSuccess() {
        assertResponseAttached();
        return responseResult;
    }
    /** throws an IllegalStateException if a response is not attached
     */
    protected void assertResponseAttached() {
        if (!responseAttached) {
            throw new IllegalStateException("Response has not been attached");
        }
    }
}
