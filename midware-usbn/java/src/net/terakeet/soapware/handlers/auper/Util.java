/*
 * Util.java
 *
 * Created on June 30, 2005, 12:36 PM
 */

package net.terakeet.soapware.handlers.auper;

import java.net.*;
import java.io.*;
import java.util.Vector;
import java.util.List;
import java.lang.StringBuffer;

/**  Utility Class containg static methods for other Auper methods.
 *
 * @author Ryan Garver
 */
public class Util {
  
    /**  Primitive logging statement to print to the console.
     *  Easily disabled.
     *  @param s the line to print
     */
    public static void debugPrint(String s) {
        System.out.println(s);
    }
    
    /** Convert a single character to hex code.  15 = 0f.
     *  @param c the character to convert
     *  @return a two character hex string, lowercase and zero padded
     */
    public static String toHex(char c) {      
        String result = (c < 16 ? "0" : "") + Integer.toString(c, 16);
        if (result.length() <= 2) {
            return result;
        } else {
            return result.substring(result.length()-2, result.length());
        }
    }
    
    /** Convert a single character to hex code.  15 = 0f.
     *  @param c the character to convert
     *  @return a two character hex string, lowercase and zero padded
     */
    public static String toHex(Character c) {
        return toHex(c.charValue());
    }
    
    /** Converts a string into a vector of character
     *  @param s the string to convert
     *  @return a new vector containing the characters in the string
     */
    public static List<Character> stringToChars(String s) {
        List<Character> result = new Vector<Character>(s.length());
        for (int i=0; i<s.length(); i++) {
            result.add(new Character(s.charAt(i)));
        }
        return result;
    }
    
    /** Takes a vector of characters and assembles them into a string
     *  @param c the character vector
     *  @return the assembled string
     */
    public static String charsToString(List<Character> c) { 
        StringBuffer sb = new StringBuffer(c.size());
        for (Character cs : c) {
            sb.append(cs);
        }
        return sb.toString();
    }
    
    /** Typecasts an int to a new Character
     *  @param i the int
     *  @return a new Character
     */
    public static Character charIt(int i) {
        return new Character((char) i);
    }
    
    /** Takes a vector of characters and converts each byte to a 
     *  hex string, and returns one string containing all the hex.
     *  @param c the character vector
     *  @return a hex string
     */
    public static String toHexString(List<Character> c) {
        StringBuffer sb = new StringBuffer(c.size());
        for (Character cr : c) {
            sb.append(toHex(cr));
        }
        return sb.toString();
    }
    
    /**  Sums the bytes in a vector and returns the two lowest 
     *  significant bytes in the result.
     *  @param c the vector to checksum
     *  @return a two-element array. The 0th is the lowest byte, and
     *      the 1st is the next lowest.
     */
    public static char[] checksum(List<Character> c) {
        int sum = 0;
        char[] result = new char[2];
        for (Character cr : c) {
            // FOR A %'ed CHECKSUM: sum = (sum + cr.charValue()) % 0xFFFF;
            sum += cr.charValue();           
        }
        result[0] = (char)(sum & 0xFF); // LOW
        result[1] = (char)((sum >> 8) & 0xFF);   // HIGH
        
        return result;
    }
    
    /**  Converts an integer (length) into a four-element vector,
     *  containing the four least significant bytes in order from
     *  least signficant to most significant.
     *  @param len the int to convert
     *  @return a 4-element vector containing four bytes, from least
     *      to most significant
     */
    public static List<Character> hexLength(int len) {
        List<Character> result= new Vector<Character>(4);
        result.add(charIt(len & 0xFF));
        len = len >> 8;
        result.add(charIt(len & 0xFF));
        len = len >> 8;
        result.add(charIt(len & 0xFF));
        len = len >> 8;
        result.add(charIt(len & 0xFF));
        
        return result;
    }
    
    public static List<Character> unstuff(List<Character> c) {
        List<Character> result = new Vector<Character>(c.size());
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
    
    /** Converts a Binary Coded Decimal into a double.
     * A binary coded decimal holds two decimal places per byte, with the
     * least significant bytes coming first.  The digits are taken at
     * face value, without conversion.  For example, the three-byte
     * sequence x55,x08,x90 converts to 900,855.
     *
     * Since thre resulting number can be a decimal number, you must specify
     * where the decimal place resides.  In the above example, if you specified
     * 4 whole number places, the result would be 9,008.55
    */
    public static double decodeBcd(List<Character> bcd, int wholePlaces) {
        int decimalPlaces = bcd.size() * 2 - wholePlaces;
        StringBuffer result = new StringBuffer(bcd.size()*2);
        int i = 0;
        while (i < bcd.size()) {
            Character c = bcd.get(i++);
            String toAdd = toHex(c);
            result.insert(0,toAdd);
            // debugPrint("adding: {"+toAdd+"} L"+toAdd.length());
        }
        double divisor = Math.pow(10.0, decimalPlaces);
        //debugPrint("divisor is "+divisor);
        //debugPrint("result pre-division is "+result.toString());
        //debugPrint("decimal portion is "+decimal.toString());
        return Double.parseDouble(result.toString()) / divisor;
    }
    
    public static double decodeBcd_OLD(List<Character> bcd, int wholePlaces) {
        int wholeBytes = wholePlaces / 2;
        int decimalBytes = bcd.size()-wholeBytes;
        StringBuffer whole = new StringBuffer(wholePlaces);
        StringBuffer decimal = new StringBuffer(bcd.size() - wholePlaces);
        int i = 0;
        while (i < decimalBytes) {
            Character c = bcd.get(i++);
            decimal.insert(0,toHex(c));
            //debugPrint("to decimal: "+toHex(c));
        }
        while (i < bcd.size()) {
            Character c = bcd.get(i++);
            whole.insert(0,toHex(c));
            //debugPrint("to whole: "+toHex(c));
        }
        double divisor = Math.pow(10.0, (decimalBytes * 2));
        //debugPrint("divisor is "+divisor);
        //debugPrint("whole portion is "+whole.toString());
        //debugPrint("decimal portion is "+decimal.toString());
        return Double.parseDouble(whole.toString()) + 
               (Double.parseDouble(decimal.toString()) / divisor);
    }
    
    /** Written by Michael Hart
     */
    public static byte[] convertHexStringtoByteArray(String hex) {
        java.util.Vector<Byte> res = new java.util.Vector<Byte>();
        String part;
        int pos = 0; //position in hex string
        final int len = 2; //always get 2 items.
        while (pos < hex.length()) {
            part = hex.substring(pos,pos+len);
            pos += len;
            int byteVal = Integer.parseInt(part,16);
            res.add(new Byte((byte)byteVal));
        }
        if (res.size() > 0) {
            byte[] b = new byte[res.size()];
            for (int i=0; i<res .size(); i++) {
                Byte a = res.elementAt(i);
                b[i] = a.byteValue();
            }
            return b;
        } else {
            return null;
        }
    }
    
    /** Written by Michael Hart
     */
    public static String convertByteArrayToHexString(byte[] buf) {
        StringBuffer sbuff = new StringBuffer();
        for (int i=0; i<buf .length; i++) {
            int b = buf[i];
            if (b < 0) b = b & 0xFF;
            if (b<16) sbuff.append("0");
            sbuff.append(Integer.toHexString(b).toUpperCase());
        }
        return sbuff.toString();
    }
    
    
    /** Utility Class, not instantiable */
    private Util() {
    }
    
}
