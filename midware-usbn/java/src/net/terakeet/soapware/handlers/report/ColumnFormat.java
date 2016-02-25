/*
 * ColumnFormat.java
 *
 * Created on September 29, 2005, 1:48 PM
 *
 */

package net.terakeet.soapware.handlers.report;

/** ColumnFormat
 *
 */
public class ColumnFormat {
    
    private final String name;
    
    /** Creates a new instance of ColumnFormat */
    private ColumnFormat(String n) {
        name = n;
    }
    
    public static ColumnFormat instanceOf(String toParse) {
        if (toParse == null) {
            throw new NullPointerException("toParse is null");
        }
        if (toParse.equals("productName")) {
            return PRODUCT_NAME;
        }
        return null;
    }
    
    public static final ColumnFormat PRODUCT_NAME = new ColumnFormat("productName"); 
    public static final ColumnFormat PRODUCT_ID = new ColumnFormat("productId");
    public static final ColumnFormat PRODUCT_POS = new ColumnFormat("productPos");
    public static final ColumnFormat QUANTITY_OUNCES = new ColumnFormat("quanOz");
    public static final ColumnFormat QUANTITY_KEGS = new ColumnFormat("quanKegs");
    public static final ColumnFormat QUANTITY_CASES = new ColumnFormat("quanCases");
    public static final ColumnFormat QUANTITY_CUSTOM = new ColumnFormat("quanCustom");
    
}
