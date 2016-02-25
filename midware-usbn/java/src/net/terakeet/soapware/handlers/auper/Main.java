/*
 * Main.java
 *
 * Created on June 29, 2005, 9:57 AM
 */

package net.terakeet.soapware.handlers.auper;

import java.net.*;
import java.io.*;
import java.util.Vector;
import java.lang.StringBuffer;
import java.util.Set;
import java.util.Iterator;


/**
 *
 * @author Ryan Garver
 */
public class Main {
    
    /** Creates a new instance of  Main */
    public Main() {
    }
    
    private static String toHex(char c) {      
        return (c < 16 ? "0" : "") + Integer.toString(c, 16);
    }
    
    private static String toHex(Character c) {
        return toHex(c.charValue());
    }
    
    private static Vector<Character> stringToCharacters(String s) {
        Vector<Character> result = new Vector<Character>(s.length());
        for (int i=0; i<s.length(); i++) {
            result.add(new Character(s.charAt(i)));
        }
        return result;
    }
    
    private static String charsToString(Vector<Character> c) { 
        StringBuffer sb = new StringBuffer(c.size());
        for (Character cs : c) {
            sb.append(cs);
        }
        return sb.toString();
    }
    
    private static Character charIt(int i) {
        return new Character((char) i);
    }
    
    public static char[] checksum(Vector<Character> c) {
        int sum = 0;
        char[] result = new char[2];
        for (Character cr : c) {
            sum += cr.charValue();
        }
        result[0] = (char)(sum & 0xFF); // LOW
        result[1] = (char)(sum >> 8);   // HIGH
        
        return result;
    }
    
    public static Vector<Character> hexLength(int len) {
        Vector<Character> result= new Vector<Character>(4);
        result.add(charIt(len & 0xFF));
        len = len >> 8;
        result.add(charIt(len & 0xFF));
        len = len >> 8;
        result.add(charIt(len & 0xFF));
        len = len >> 8;
        result.add(charIt(len & 0xFF));
        
        return result;
    }
    
    public static void stuff(Vector<Character> c) {
        Vector<Character> result = new Vector<Character>(c.size());
        for (Character cr : c) {
            result.add(cr);
            if (cr.charValue() == 0xFF) {
                result.add(charIt(0xFF));
            }
        }
        c = result;
    }
    
    public static void unstuff(Vector<Character> c) {
        Vector<Character> result = new Vector<Character>(c.size());
        boolean escaped = false;
        
        for (Character cr : c) {
            if (escaped) {
                escaped = false;
                if (cr.charValue() == 0xFF) {
                    // Proper escaping, do nothing
                } else if (cr.charValue() == 0x00) {
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
        
        c = result;
    }
    
    public static void prepareMessage(Vector<Character> c){
        char[] checksums = checksum(c);
        c.add(Character.valueOf(checksums[0]));
        c.add(Character.valueOf(checksums[1]));
        stuff(c);
        c.add(charIt(0xFF));
        c.add(charIt(0x00));
    }
    
    
    
    public static void debugPrint(String s) {
        System.out.println(s);
    }
    
    public static String toString(Vector<Character> c) {
        StringBuffer sb = new StringBuffer(c.size());
        for (Character cr : c) {
            sb.append(toHex(cr));
        }
        return sb.toString();
    }
    

    

    
    
    private static void testOutput() {
        Vector<Character> x1a = new Vector<Character>();
        x1a.add(new Character((char)0x00));
        x1a.add(new Character((char)0xFA));
        x1a.add(new Character((char)0x64));
        Vector<Character> x1b = new Vector<Character>();
        x1b.add(new Character((char)0xFA));
        x1b.add(new Character((char)0x00));
        x1b.add(new Character((char)0x03));
        x1b.add(new Character((char)0x64));
        x1b.add(new Character((char)0x01));  
        Vector<Character> x2a = new Vector<Character>();
        x2a.add(new Character((char)0xA0));
        x2a.add(new Character((char)0xFA));
        x2a.add(new Character((char)0x65));        
        
        debugPrint(toHex((char)0));
        debugPrint(toHex((char)0xF0));
        debugPrint(toHex((char)0x64));
        
        debugPrint("String 1a: "+toString(x1a));
        prepareMessage(x1a);
        debugPrint("Hex      : "+toString(x1a));
        debugPrint("Str      : "+charsToString(x1a));
        debugPrint("String 1b: "+toString(x1b));
        prepareMessage(x1b);
        debugPrint("Hex      : "+toString(x1b));
        debugPrint("Str      : "+charsToString(x1b));
        debugPrint("String 2a: "+toString(x2a));
        prepareMessage(x2a);
        debugPrint("Hex      : "+toString(x2a));
        debugPrint("Str      : "+charsToString(x2a));
        unstuff(x2a);
        debugPrint("Decode   : "+toString(x2a));
        
    }
    
    public static void test66() throws AuperException{
        AuperConnection conn = new AuperConnection();
        
        Command66 c66 = new Command66("<?xml version=\"1.0\" encoding=\"ISO-8859-1\" ?><InventoryReport>" +
                "<Product index=\"12\"><QtyInStock>6</QtyInStock><MinimumQty>2.00</MinimumQty></Product></InventoryReport>");
        
        boolean success = conn.sendCommand(c66);
        conn.close();
        if (success && c66.isSuccess()) {
            debugPrint("Command Succeeded");
        } else {
            debugPrint("Command Failed");
        }
    }
    
    
    public static void test64() throws AuperException {
         AuperConnection conn = new AuperConnection();
        
        Command64 c64 = new Command64();
        
        boolean success = conn.sendCommand(c64);
        conn.close();
        if (success && c64.isSuccess()) {
            debugPrint("Command Succeeded");
        } else {
            debugPrint("Command Failed");
        }     
        
    }
    
    public static void test65() throws AuperException {
        AuperConnection conn = new AuperConnection();
        
        Command65 c65 = new Command65();
        
        boolean success = conn.sendCommand(c65);
        conn.close();
        if (success && c65.isSuccess()) {
            debugPrint("Command Succeeded");
            debugPrint(c65.getXmlString());
        } else {
            debugPrint("Command Failed");
        }     
        
    }
    
    public static void test6665() throws AuperException {
        AuperConnection conn = new AuperConnection();
          
        Command66 c66 = new Command66("<?xml version=\"1.0\" encoding=\"ISO-8859-1\" ?><InventoryReport>" +
                "<Product index=\"12\"><QtyInStock>7</QtyInStock><MinimumQty>2.00</MinimumQty></Product></InventoryReport>");
        
        boolean success = conn.sendCommand(c66);
        if (success && c66.isSuccess()) {
            debugPrint("Command Succeeded");
        } else {
            debugPrint("Command Failed");
        }
        
        Command65 c65 = new Command65();
        
        success = conn.sendCommand(c65);
        if (success && c65.isSuccess()) {
            debugPrint("Command Succeeded");
            debugPrint(c65.getXmlString());
        } else {
            debugPrint("Command Failed");
        }  
    }
    
    private static void testBcd() {
        Vector<Character> bcd1 = new Vector<Character>();
        bcd1.add(new Character((char)0x68));
        bcd1.add(new Character((char)0x18));
        bcd1.add(new Character((char)0x64));
        bcd1.add(new Character((char)0x08));
        bcd1.add(new Character((char)0x13));
        bcd1.add(new Character((char)0x00));
        double d1 = Util.decodeBcd(bcd1,6);
        debugPrint("BCD Value is "+String.valueOf(d1));
        debugPrint("Expecting: 1308.641868");
        
        Vector<Character> bcd2 = new Vector<Character>();
        bcd2.add(new Character((char)0x78));
        bcd2.add(new Character((char)0x56));
        bcd2.add(new Character((char)0x34));
        bcd2.add(new Character((char)0x12));
        double d2 = Util.decodeBcd(bcd2,3);
        debugPrint("BCD Value is "+String.valueOf(d2));
        debugPrint("Expecting: 123.45678");
    }
    
    private static void testManager() throws AuperException{
        Set<AuperReading> mySet = AuperManager.getReadings();
        Iterator<AuperReading> i = mySet.iterator();
        while (i.hasNext()) {
            AuperReading r = i.next();
            debugPrint("Reading for #"+r.getPid()+" : "+r.getKegs().asKegs());
        }
    }
    
    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        try {
           testBcd();
        } catch (Exception ae) {
            debugPrint("Exception "+ae.getMessage());
        }
         debugPrint("Finished");    
    }
    
}
