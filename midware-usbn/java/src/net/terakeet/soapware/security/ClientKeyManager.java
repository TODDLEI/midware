package net.terakeet.soapware.security;
/*
 * ClientKeyManager.java
 *
 * Created on February 20, 2006, 3:20 PM
 *
 */


import javax.crypto.*;
import java.security.*;
import java.io.*;
import java.util.Random;
import java.util.Date;
import net.terakeet.soapware.*;
import org.apache.log4j.Logger;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.PropertyConfigurator;

/**
 * ClientKeyManager class
 */
public class ClientKeyManager {
    
    static Logger logger = Logger.getLogger(ClientKeyManager.class.getName());
    //static Logger accessLog = Logger.getLogger("net.terakeet.")
    private static String KEY_PATH = "client.key";
    private static final String KEY_INIT = "AES"; //"DES";
    private static final String CIPHER_INIT = "AES/ECB/PKCS5Padding";//"DES/ECB/PKCS5Padding"; 
    public final static int MIDWARE_CLASS = SecureSession.MIDWARE_CLASS;
    private static SecretKey cachedKey = null;
    private static Object keyLock = new Object();
    
    private ClientKeyManager() {}
    
    /** Obtain a secureSession object from an encrypted client key.
     * This method will attempts to decrypt the supplied key using the 
     * middlewares secret key, and if successful, build a SecureSession.  If the 
     * decryption fails for any reason, the "No Access" session will be returned.
     * This method will never return null.
     *
     * @param clientKey A 32-character hex string
     * @return a session built from the decoded key, or the "No Access" session on failure 
     */
    public static SecureSession getSession(String clientKey) {
        String decryptedString="";
        SecretKey aesKey = readKey();
        //TODO:  Add additional parsing for the client key like interior whitespace removal etc.
        if (clientKey != null) { clientKey = clientKey.trim(); }
        //decrypt
        try {
            if (clientKey != null && clientKey.length() == 32 && aesKey != null) {
                Cipher aesCipher = Cipher.getInstance(CIPHER_INIT);
                aesCipher.init(Cipher.DECRYPT_MODE, aesKey);
                byte[] decrypted = aesCipher.doFinal(hexToBytes(clientKey));
                decryptedString = new String(decrypted);
            }
        } catch (NoSuchAlgorithmException nsae) {
            logger.warn("CKM: No Such Algorithm Exception "+nsae.toString());
        } catch (NoSuchPaddingException nspe) {
            logger.warn("CKM: No Such Padding Exception");
        } catch (InvalidKeyException ike) {
            logger.warn("CKM: Invalid Key Exception");
        } catch (IllegalBlockSizeException ibse) {
            logger.warn("CKM: IllegalBlockSizeException");
        } catch (BadPaddingException bpe) {
            logger.warn("CKM: Bad Padding Exception");
        } catch (Exception e) {
            logger.warn("CKM: General Exception: "+e.toString());
        } finally {
            return SecureSession.getInstance(decryptedString);
        }
    }
    
    /**  Change the path to the secret key.
     *
     */
    public static void setKeyPath(String path) {
        if (path==null || path.length() == 0) {
            logger.warn("Attempted to setKeyPath to a null or empty string");
        } else {
            KEY_PATH = path;
            logger.debug("Changing key path to {"+path+"}");
        }
    }
    
    public static boolean isKeyLoaded() {
        boolean result = false;
        synchronized(keyLock) {
            if (cachedKey != null) {
                result = true;
            }
        }
        return result;
    }
    
    /**  Create a new client key.  The encrypted key can be distributed to 
     * clients for use in their middleware communications.
     *  @param location the client's location ID (1-99999)
     *  @param developer an identifier for the developer(or partner or vendor)
     *         who is using the key.  (1-99) Terakeet = 01 
     *  @param security the security level of this key. (1-999) See SecurityLevel class
     *  @return a 32 character hex string containing the encrypted key.
     *
     */
    public static String createClientKey(int location, int developer, int security) {
        String locationId = zeroFill(location,5);
        String securityId = zeroFill(security,3);
        String developerId = zeroFill(developer,3);
        String randomThree = zeroFill(randRange(1, 999),3);
        String classId = String.valueOf(MIDWARE_CLASS);
        
        String clearString = classId + developerId + securityId + randomThree + locationId;
        
        String cipherString = "";
        //encrypt
        try {
            Cipher aesCipher = Cipher.getInstance(CIPHER_INIT);
            SecretKey aesKey = readKey();
            aesCipher.init(Cipher.ENCRYPT_MODE, aesKey);
            byte[] cleartext = clearString.getBytes();
            byte[] ciphertext = aesCipher.doFinal(cleartext);
            cipherString = bytesToHex(ciphertext);
        } catch (NoSuchAlgorithmException nsae) {
            logger.warn("CKM: No Such Algorithm Exception");
        } catch (NoSuchPaddingException nspe) {
            logger.warn("CKM: No Such Padding Exception");
        } catch (InvalidKeyException ike) {
            logger.warn("CKM: Invalid Key Exception");
        } catch (IllegalBlockSizeException ibse) {
            logger.warn("CKM: IllegalBlockSizeException");
        } catch (BadPaddingException bpe) {
            logger.warn("CKM: Bad Padding Exception");
        } finally {
            return cipherString;
        }
    }
    
    private static void workbench() {
        try {
            KeyGenerator keygen = KeyGenerator.getInstance(KEY_INIT);
            SecretKey aesKey = keygen.generateKey();
            
            Cipher aesCipher = Cipher.getInstance(CIPHER_INIT);
            aesCipher.init(Cipher.ENCRYPT_MODE, aesKey);
            
            String clearString = "200110000000011";
            byte[] cleartext = clearString.getBytes();
            byte[] ciphertext = aesCipher.doFinal(cleartext);
            String cipherString = bytesToHex(ciphertext);
            //String cipherString = new String(ciphertext);
            pr("Clear: {"+clearString+"} ("+clearString.length()+")");
            pr("Cipher: {"+cipherString+"} ("+cipherString.length()+")");
            //pr("Clear: {"+bytesToHex(cleartext)+"} ");
            //String hexed = new String(hexToBytes(bytesToHex(cleartext)));
            //pr("Hexified: "+hexed);
            
            //decrypt
            aesCipher.init(Cipher.DECRYPT_MODE, aesKey);
            byte[] decrypted = aesCipher.doFinal(hexToBytes(cipherString));
            pr("Decrypted: {"+(new String(decrypted))+"} ");
        } catch (NoSuchAlgorithmException nsae) {
            pr("No Such Algorithm Exception");
        } catch (NoSuchPaddingException nspe) {
            pr("No Such Padding Exception");
        } catch (InvalidKeyException ike) {
            pr("Invalid Key Exception");
        } catch (IllegalBlockSizeException ibse) {
            pr("IllegalBlockSizeException");
        } catch (BadPaddingException bpe) {
            pr("Bad Padding Exception");
        }
    }
    
    /** Writes a new key to a file, "client.key"
     *
     * This file contains two serialized objects
     *  A Secretkey, and a String of the date the key was created.
     */
    private static void writeNewKey() {
        try {
            KeyGenerator keygen = KeyGenerator.getInstance(KEY_INIT);
            SecretKey aesKey = keygen.generateKey();
            synchronized(keyLock) {
                cachedKey = aesKey;
                FileOutputStream out = new FileOutputStream(new File(KEY_PATH));
                ObjectOutputStream s = new ObjectOutputStream(out);
                s.writeObject(aesKey);
                s.writeObject(new Date().toString());
                s.flush();
                s.close();
                out.close();
                logger.warn("NEW SECRET KEY WRITTEN");
            }
        } catch (IOException ioe) {
            pr("IO Exception: "+ioe.toString());
        } catch (NoSuchAlgorithmException nsae) {
            pr("No Such Algorithm Exception");
        }
        
    }
    
    /** Reads a key from a file, "client.key"
     */
    private static SecretKey readKey() {
        SecretKey result = null;
        try {
            synchronized(keyLock) {
                if (cachedKey != null) {
                    result = cachedKey;
                } else {
                    FileInputStream in = new FileInputStream(new File(KEY_PATH));
                    ObjectInputStream s = new ObjectInputStream(in);
                    result = (SecretKey)s.readObject();
                    String dateStamp = (String)s.readObject(); // not used anywhere
                    s.close();
                    in.close();
                    logger.debug("Existing secret key loaded from disk");
                    cachedKey = result;
                }
            }
        } catch (Exception e) {
            logger.warn("Exception reading key: "+e.toString());
        }
        return result;
    }
    
    
    /** Reads a key from a file, "client.key" and returns the creation date
     */
    private static String checkKey() {
        String result = "Not Found";
        try {
            synchronized(keyLock) {
                FileInputStream in = new FileInputStream(new File(KEY_PATH));
                ObjectInputStream s = new ObjectInputStream(in);
                s.readObject(); // the key itself
                result = (String)s.readObject(); // the date
                s.close();
                in.close();
            }
        } catch (Exception e) {
            logger.warn("Exception reading key: "+e.toString());
        }
        return result;
    }
    
    /** Converts an array of bytes to a hex string.
     * Each byte is forced to be positive by adding 128 to each value.
     */
    private static String bytesToHex(byte[] b) {
        StringBuilder result = new StringBuilder(32);
        for (int i=0; i< b.length; i++) {
            result.append(padHex(Integer.toHexString(b[i]+128)));
        }
        return result.toString();
    }
    
    /**  Reads a 32-character hex string to a byte array.
     *  Reverses the conversion performed in bytesToHex by subracting 128
     *  from each hex value before storing a byte.
     */
    private static byte[] hexToBytes(String s) {
        int BYTE_LEN = 16;
        
        if (s.length() != BYTE_LEN*2) {
            throw new IllegalArgumentException("Must be exactly "+BYTE_LEN*2+" chars in length");
        }
        byte[] result = new byte[BYTE_LEN];
        for (int i=0; i<BYTE_LEN; i++) {
            String sub = s.substring(i*2, i*2+2);
            result[i] = (byte)(Short.decode("0x"+sub).shortValue()-128);
        }
        return result;
    }
    
    /** Returns a two-character string, padding with a leading zero if necessary
     *  padHex("F") == "0F"
     *  padHex("A3") == "A3"
     */
    private static String padHex(String s) {
        if (s.length()==1) {
            return "0"+s;
        } else {
            return s;
        }
    }
    
    /**  Pad an int with leading zeroes and return it as a string.  If the number
     *  already has more places than desired, it will be returned intact.
     *  Examples:   zeroFill(45,5) == "00045"
     *              zeroFill(101,2) == "101"
     *              zeroFill(-10,3) == illegal
     *  @param n the number to pad, must be non-negative
     *  @param places the number of total places in the result, must be positive
     *  @return the padded string
     */
    private static String zeroFill(int n, int places) {
        if (n<0 || places<1) {
            throw new IllegalArgumentException("n must be >= 0, places must be >= 1");
        }
        StringBuilder result = new StringBuilder(places);
        String number = String.valueOf(n);
        for(int i=number.length(); i<places; i++) {
            result.append('0');
        }
        result.append(number);
        return result.toString();
    }
    
    
    private static Random rand =
            new Random((new java.util.Date()).getTime());
    
    /** Rolls a random, uniformly distributed integer in the range [lower,upper] incl.
     * @param lower the lower bound
     * @param upper the upper bound
     * @return the random result
     */
    private static int randRange(int lower, int upper){
        if (lower > upper) {
            throw new IllegalArgumentException(lower + " was > than " + upper);
        }
        return lower + rand.nextInt(1+upper-lower);
    }
    
    private static void pr(String s) {
        System.out.println(s);
    }
    
    /*  Unit test for the crypto.  Reads a key from a file and performs N encryptions/decryptions.
     *  N is specified within the method: 10,000.
     */
    public static void test() {
        final int TESTS = 10000;
        //write a key
        //writeNewKey();
        //read a key
        SecretKey aesKey = readKey();
        
        try {
            int successes = 0;
            pr("Testing "+TESTS+" cases...");
            for (int i=0; i<TESTS; i++) {
                
                //create random test string
                String locationId = zeroFill(randRange(1, 999999),6);
                String securityId = zeroFill(randRange(0, 999),3);
                String clientId = zeroFill(randRange(0, 999),3);
                String clearString = locationId+"_"+clientId+"_"+securityId;
                
                //encrypt
                Cipher aesCipher = Cipher.getInstance(CIPHER_INIT);
                aesCipher.init(Cipher.ENCRYPT_MODE, aesKey);
                byte[] cleartext = clearString.getBytes();
                byte[] ciphertext = aesCipher.doFinal(cleartext);
                String cipherString = bytesToHex(ciphertext);
                
                //decrypt
                aesCipher.init(Cipher.DECRYPT_MODE, aesKey);
                byte[] decrypted = aesCipher.doFinal(hexToBytes(cipherString));
                String result = new String(decrypted);
                
                //check
                if (result.equals(clearString)) {
                    successes++;
                } else {
                    pr("FAILED #"+i);
                    pr("Clear: {"+clearString+"} ("+clearString.length()+")");
                    pr("Cipher: {"+cipherString+"} ("+cipherString.length()+")");
                    pr("Decrypted: {"+result+"} ");
                }
            }
            pr("Complete. "+successes+" OK.");
            
        } catch (NoSuchAlgorithmException nsae) {
            pr("No Such Algorithm Exception");
        } catch (NoSuchPaddingException nspe) {
            pr("No Such Padding Exception");
        } catch (InvalidKeyException ike) {
            pr("Invalid Key Exception");
        } catch (IllegalBlockSizeException ibse) {
            pr("IllegalBlockSizeException");
        } catch (BadPaddingException bpe) {
            pr("Bad Padding Exception");
        }
        
    }
    
    
    private static void createAndOutputNewKey() {
        //           Location,ClientId,Security
        String client = createClientKey(0, 1, 500);
        logger.info("Using Secret Key from "+checkKey());
        logger.info("NEW Client key is "+client);
        SecureSession ss = getSession(client);
        logger.info("Loc# "+ss.getLocation()+"-"+ss.getClientId());
        logger.info("Access: "+ss.getSecurityLevel());
    }
    
    
    /**  Main method to create a new client key
     */
    public static void main(String[] args) {
        //BasicConfigurator.configure();       
        //writeNewKey();        
        //test();
        //workbench();     
        
        //createAndOutputNewKey();
        SecureSession ss = ClientKeyManager.getSession("060a154c7021cbf73b47fa02dc02e8f3");
        //SecureSession ss = ClientKeyManager.getSession("060a154c7021cbf73b47fa02dc02e8f");
        //SecureSession ss = ClientKeyManager.getSession("9b781c4d0ce307c1be1e494f6b838858");
        System.out.println(ss.toString());
        
        //PropertyConfigurator.configure("log4j.props");
        
        //Logger accessLog = Logger.getLogger("net.terakeet.access");
        //Logger accessWarn = Logger.getLogger("net.terakeet.access.warning");
        //accessLog.info("remoteReading by #11-301");
        //accessLog.info("remoteReading by #11-301");
        //accessWarn.warn("FAILED remoteReading by #12-301: Location mismatch (11)");

    }
}
