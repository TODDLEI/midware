/**
 * QuickSellHandler.java
 *
 * @author Patrick Danial
 * @version $Id: QuickSellHandler.java,v 1.4 2010/09/30 19:43:27 sravindran Exp $
 */
package net.terakeet.soapware.handlers;

// package imports.
import net.terakeet.soapware.Handler;
import net.terakeet.soapware.HandlerException;
import net.terakeet.soapware.HandlerUtils;
import net.terakeet.soapware.SOAPMessage;
import net.terakeet.soapware.handlers.auper.*;
import org.apache.log4j.Logger;
import org.dom4j.Element;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.CallableStatement;
import java.lang.String;
import java.util.Iterator;
import java.util.Set;



public class QuickSellHandler implements Handler {
    static Logger logger = Logger.getLogger(DummyHandler.class.getName());
    static String driverClass = HandlerUtils.getTDSDriverName();
    static String mysqlDriverClass = HandlerUtils.getSetting("auper.tdsDriver");
    static String connectionURL = null;
    static String mysqlConnectionURL = null;
    
    /**
     * Generic constructor. Loads an appropriate database driver.
     * @throws Handler Exception if there's a problem loading the
     * database driver.
     */
    public QuickSellHandler() throws HandlerException {
        HandlerUtils.initializeDriver(driverClass);
        HandlerUtils.initializeDriver(mysqlDriverClass);
        if (connectionURL == null) {
            connectionURL = HandlerUtils.getTDSPrefix() + "://" +
                    HandlerUtils.getSetting("quickSell.server") + "/" +
                    HandlerUtils.getSetting("quickSell.database") + ";" +
                    "USER=" + HandlerUtils.getSetting("quickSell.user") + ";" +
                    "PASSWORD=" + HandlerUtils.getSetting("quickSell.password") + ";" +
                    "APPNAME=premier_jTDS;PROGNAME=premier";
        }
        if (mysqlConnectionURL == null) {
            mysqlConnectionURL = HandlerUtils.getSetting("auper.tdsPrefix") + "://" +
                    HandlerUtils.getSetting("auper.server") + "/" +
                    HandlerUtils.getSetting("auper.database") + "?" +
                    "user=" + HandlerUtils.getSetting("auper.user") + "&" +
                    "password=" + HandlerUtils.getSetting("auper.password");
            
        }
    }
    
    public void handle(Element toHandle, Element toAppend) throws HandlerException {
        logger.debug("request: " + toHandle.asXML());
        
        String function = toHandle.getName();
        String responseNamespace = (String)SOAPMessage.getURIMap().get("tkmsg");
        
        if ("getCashier".equals(function)) {
            Element resp = toAppend.addElement("m:getCashiersResponse",
                    responseNamespace);
            getCashier(toHandle, resp);
        } else if ("getCustomers".equals(function)) {
            Element resp = toAppend.addElement("m:getCustomersResponse",
                    responseNamespace);
            getCustomers(toHandle, resp);
        } else if ("getItemMovement".equals(function)) {
            Element resp = toAppend.addElement("m:getItemMovementResponse",
                    responseNamespace);
            getItemMovement(toHandle, resp);
        } else if ("getAccountReport".equals(function)) {
            Element resp = toAppend.addElement("m:getAccountReportResponse",
                    responseNamespace);
            getAccountReport(toHandle, resp);
        } else if ("getSupplierSales".equals(function)) {
            Element resp = toAppend.addElement("m:getSupplierSalesResponse",
                    responseNamespace);
            getSupplierSales(toHandle, resp);
        } else if ("placeQuicksellOrder".equals(function)) {
            Element resp = toAppend.addElement("m:placeQuicksellOrderResponse",
                    responseNamespace);
            placeOrder(toHandle, resp);
        } else if ("getAllSuppliers".equals(function)) {
            getAllSuppliers(toHandle, responseFor(function,toAppend));
        } else if ("getAllCustomers".equals(function)) {
            getAllCustomers(toHandle, responseFor(function,toAppend));
        } else if ("getItemCatalog".equals(function)) {
            getItemCatalog(toHandle, responseFor(function,toAppend));
        } else if ("processReading".equals(function)) {
            processReading(toHandle, responseFor(function,toAppend));
        } else {
            logger.warn("Unknown function '" + function + "'.");
        }
        //logger.debug("response: " + toAppend.asXML());
    }
    
    private String nullToEmpty(String s) {
        return (null == s) ? "" : s;
    }
    
    private void close(Statement s) {
        if (s != null) {
            try { s.close(); } catch (SQLException sqle) { }
        }
    }
    private void close(ResultSet rs) {
        if (rs != null) {
            try { rs.close(); } catch (SQLException sqle) { }
        }
    }
    private void close(Connection c) {
        if (c != null) {
            try { c.close(); } catch (SQLException sqle) { }
        }
    }
    private Element responseFor(String s, Element e) {
        String responseNamespace = (String)SOAPMessage.getURIMap().get("tkmsg");
        return e.addElement("m:"+s+"Response",responseNamespace);
    }
    
    
    /**
     * Returns a cashier record from the Cashier table
     * @param toHandle A SOAP <code>Body</code> element.
     * @return a list of cashier records.
     * @throws HandlerException if anything is wrong with the SOAP
     * message or if there is a problem with the database.
     */
    private void getCashier(Element toHandle, Element toAppend)
    throws HandlerException {
        
        int cashierId = HandlerUtils.getRequiredInteger(toHandle, "cashierId");
        
        String SQL = "SELECT Name, Password, EmailAddress FROM Cashier " +
                "WHERE ID = ?";
        
        Connection conn = HandlerUtils.connectTo(connectionURL);
        PreparedStatement stmt = null;
        ResultSet rs = null;
        String name = null, password = null, emailAddress = null;
        try {
            stmt = conn.prepareStatement(SQL);
            stmt.setInt(1, cashierId);
            rs = stmt.executeQuery();
            if (rs != null && rs.next()) {
                Element cashierEl = toAppend.addElement("cashier");
                name = rs.getString(1);
                password = rs.getString(2);
                emailAddress = rs.getString(3);
                cashierEl.addElement("name")
                .addText(HandlerUtils.nullToEmpty(name));
                cashierEl.addElement("password")
                .addText(HandlerUtils.nullToEmpty(password));
                cashierEl.addElement("emailAddress")
                .addText(HandlerUtils.nullToEmpty(emailAddress));
            }
        } catch (SQLException sqle) {
            logger.warn("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            if (rs != null) {
                try { rs.close(); } catch (SQLException sqle) { }
            }
            if (stmt != null) {
                try { stmt.close(); } catch (SQLException sqle) { }
            }
            if (conn != null) {
                try { conn.close(); } catch (SQLException sqle) { }
            }
        }
    }
    
    /**
     * Returns a list of customer records
     * @param toHandle A SOAP <code>Body</code> element.
     * @return a list of customer records.
     * @throws HandlerException if anything is wrong with the SOAP
     * message or if there is a problem with the database.
     */
    private void getCustomers(Element toHandle, Element toAppend)
    throws HandlerException {
        
        String accountNumber = HandlerUtils.getOptionalString(toHandle, "accountNumber");
        
        String SQL = "SELECT AccountNumber, FirstName, LastName, Company, " +
                "PhoneNumber, FaxNumber, AccountOpened, LastVisit, TotalVisits, " +
                "TotalSales, CurrentDiscount, CreditLimit, AccountBalance " +
                "FROM Customer";
        
        if (accountNumber != null) {
            SQL += " WHERE AccountNumber = ?";
        }
        
        
        Connection conn = HandlerUtils.connectTo(connectionURL);
        PreparedStatement stmt = null;
        ResultSet rs = null;
        String firstName = null, lastName = null, company = null,
                phoneNumber = null, faxNumber = null, accountOpened = null,
                lastVisit = null, totalVisits = null, totalSales = null,
                currentDiscount = null, creditLimit = null, accountBalance = null;
        
        try {
            stmt = conn.prepareStatement(SQL);
            if (accountNumber != null) {
                stmt.setString(1, accountNumber);
            }
            rs = stmt.executeQuery();
            Element customersEl = toAppend.addElement("customers");
            while (rs != null && rs.next()) {
                Element customerEl = customersEl.addElement("customer");
                accountNumber = rs.getString(1);
                firstName = rs.getString(2);
                lastName = rs.getString(3);
                company = rs.getString(4);
                phoneNumber = rs.getString(5);
                faxNumber = rs.getString(6);
                accountOpened = rs.getString(7);
                lastVisit = rs.getString(8);
                totalVisits = rs.getString(9);
                totalSales = rs.getString(10);
                currentDiscount = rs.getString(11);
                creditLimit = rs.getString(12);
                accountBalance = rs.getString(13);
                
                customerEl.addElement("accountNumber")
                .addText(HandlerUtils.nullToEmpty(accountNumber));
                customerEl.addElement("firstName")
                .addText(HandlerUtils.nullToEmpty(firstName));
                customerEl.addElement("lastName")
                .addText(HandlerUtils.nullToEmpty(lastName));
                customerEl.addElement("company")
                .addText(HandlerUtils.nullToEmpty(company));
                customerEl.addElement("phoneNumber")
                .addText(HandlerUtils.nullToEmpty(phoneNumber));
                customerEl.addElement("faxNumber")
                .addText(HandlerUtils.nullToEmpty(faxNumber));
                customerEl.addElement("accountOpened")
                .addText(HandlerUtils.nullToEmpty(accountOpened));
                customerEl.addElement("lastVisit")
                .addText(HandlerUtils.nullToEmpty(lastVisit));
                customerEl.addElement("totalVisits")
                .addText(HandlerUtils.nullToEmpty(totalVisits));
                customerEl.addElement("totalSales")
                .addText(HandlerUtils.nullToEmpty(totalSales));
                customerEl.addElement("currentDiscount")
                .addText(HandlerUtils.nullToEmpty(currentDiscount));
                customerEl.addElement("creditLimit")
                .addText(HandlerUtils.nullToEmpty(creditLimit));
                customerEl.addElement("accountBalance")
                .addText(HandlerUtils.nullToEmpty(accountBalance));
            }
        } catch (SQLException sqle) {
            logger.warn("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            if (rs != null) {
                try { rs.close(); } catch (SQLException sqle) { }
            }
            if (stmt != null) {
                try { stmt.close(); } catch (SQLException sqle) { }
            }
            if (conn != null) {
                try { conn.close(); } catch (SQLException sqle) { }
            }
        }
    }
    
    /**
     * Returns an item movement report.
     * @param toHandle A SOAP <code>Body</code> element.
     * @return a of item records by dates.
     * @throws HandlerException if anything is wrong with the SOAP
     * message or if there is a problem with the database.
     */
    private void getItemMovement(Element toHandle, Element toAppend)
    throws HandlerException {
        
        String startDate = HandlerUtils.getRequiredString(toHandle, "startDate");
        String endDate = HandlerUtils.getOptionalString(toHandle, "endDate");
        String supplier = HandlerUtils.getOptionalString(toHandle, "supplier");
        
        // Date format is 'YYYY-MM-DD HH:MM:SS.000'
        if (!startDate.matches("^\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\.000$")) {
            throw new HandlerException("startDate must be of the format 'YYYY-MM-DD HH:MM:SS.000");
        }
        
        if (endDate != null && !endDate.matches("^\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\.000$")) {
            throw new HandlerException("endDate must be of the format 'YYYY-MM-DD HH:MM:SS.000");
        }
        logger.debug("Retrieving Item Movement Report.");
        String SQL =
                "SELECT ITEMDESCRIPTION,SUM(QUANTITYSOLD),SUM(QUANTITY)"+
                "FROM VIEWITEMMOVEMENT "+
                "WHERE DATETRANSFERRED IS NOT NULL "+
                "  AND DATETRANSFERRED > ? AND DATETRANSFERRED < ? ";
        if (supplier != null) {
            SQL += "  AND SUPPLIERNAME = ? ";
        }
        SQL +=  "  GROUP BY ITEMDESCRIPTION ";
        
        Connection conn = HandlerUtils.connectTo(connectionURL);
        PreparedStatement stmt = null;
        ResultSet rs = null;
        
        logger.debug("Start Date: " + startDate);
        if (endDate == null) {
            // Since there's no end date, we'll set the end date to the EOD of start date.
            endDate = startDate.substring(0,1);         // Attach start date.
            endDate = endDate + " " + "23:59:59.000";   // Attach end time for start date.
            logger.debug("Create End Date of: " + endDate);
        } else {
            logger.debug("End Date: " + endDate);
        }
        
        try {
            stmt = conn.prepareStatement(SQL);
            
            stmt.setString(1, startDate);
            stmt.setString(2, endDate);
            if (supplier != null) {
                stmt.setString(3, supplier);
            }
            
            
            rs = stmt.executeQuery();
            while (rs != null && rs.next()) {
                Element itemEl = toAppend.addElement("item");
                itemEl.addElement("description").addText(HandlerUtils.nullToEmpty(rs.getString(1)));
                itemEl.addElement("sold").addText(HandlerUtils.nullToEmpty(rs.getString(2)));
                itemEl.addElement("quantity").addText(HandlerUtils.nullToEmpty(rs.getString(3)));
            }
        } catch (SQLException sqle) {
            logger.warn("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            if (rs != null) {
                try { rs.close(); } catch (SQLException sqle) { }
            }
            if (stmt != null) {
                try { stmt.close(); } catch (SQLException sqle) { }
            }
            if (conn != null) {
                try { conn.close(); } catch (SQLException sqle) { }
            }
        }
        
    }
    
    
    /**
     * Returns an account report.
     * @param toHandle A SOAP <code>Body</code> element.
     * @return a of item records by dates.
     * @throws HandlerException if anything is wrong with the SOAP
     * message or if there is a problem with the database.
     */
    private void getAccountReport(Element toHandle, Element toAppend)
    throws HandlerException {
        
        String startDate = HandlerUtils.getRequiredString(toHandle, "startDate");
        String endDate = HandlerUtils.getOptionalString(toHandle, "endDate");
        String account = HandlerUtils.getRequiredString(toHandle, "account");
        
        // Date format is 'YYYY-MM-DD HH:MM:SS.000'
        if (!startDate.matches("^\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\.000$")) {
            throw new HandlerException("startDate must be of the format 'YYYY-MM-DD HH:MM:SS.000");
        }
        
        if (endDate != null && !endDate.matches("^\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\.000$")) {
            throw new HandlerException("endDate must be of the format 'YYYY-MM-DD HH:MM:SS.000");
        }
        logger.debug("Retrieving Account Report.");
        String SQL =
                "    SELECT  Item.Description, [Transaction].Time, TransactionEntry.Quantity, TransactionEntry.Price"+
                "    FROM     TransactionEntry "+
                "    INNER JOIN [Transaction] WITH(NOLOCK) ON TransactionEntry.TransactionNumber = [Transaction].TransactionNumber "+
                "    LEFT JOIN   Item WITH(NOLOCK) ON TransactionEntry.ItemID = Item.ID "+
                "    LEFT JOIN   Customer WITH(NOLOCK) ON [Transaction].CustomerID = Customer.ID "+
                "    WHERE  Customer.AccountNumber= ?  AND [Transaction].Time > ? AND [Transaction].Time < ? "+
                " ORDER BY [Transaction].Time ";
        
        
        Connection conn = HandlerUtils.connectTo(connectionURL);
        PreparedStatement stmt = null;
        ResultSet rs = null;
        
        logger.debug("Start Date: " + startDate);
        if (endDate == null) {
            // Since there's no end date, we'll set the end date to the EOD of start date.
            endDate = startDate.substring(0,1);         // Attach start date.
            endDate = endDate + " " + "23:59:59.000";   // Attach end time for start date.
            logger.debug("Create End Date of: " + endDate);
        } else {
            logger.debug("End Date: " + endDate);
        }
        
        try {
            stmt = conn.prepareStatement(SQL);
            
            stmt.setString(1, account);
            stmt.setString(2, startDate);
            stmt.setString(3, endDate);
            
            
            logger.debug("Excuting Query");
            rs = stmt.executeQuery();
            logger.debug("Query Complete");
            
            while (rs != null && rs.next()) {
                Element itemEl = toAppend.addElement("item");
                itemEl.addElement("description").addText(HandlerUtils.nullToEmpty(rs.getString(1)));
                itemEl.addElement("date").addText(HandlerUtils.nullToEmpty(rs.getDate(2).toString()));
                itemEl.addElement("quantity").addText(HandlerUtils.nullToEmpty(rs.getString(3)));
                itemEl.addElement("price").addText(HandlerUtils.nullToEmpty(rs.getString(4)));
            }
            
            // Get account information about the customer
            stmt = conn.prepareStatement("SELECT CreditLimit,AccountBalance,Company FROM Customer WHERE AccountNumber=?");
            stmt.setString(1, account);
            rs = stmt.executeQuery();
            if (rs != null && rs.next()) {
                toAppend.addElement("company").addText(HandlerUtils.nullToEmpty(rs.getString(3)));
                toAppend.addElement("accountBalance").addText(HandlerUtils.nullToEmpty(rs.getString(2)));
                toAppend.addElement("creditLimit").addText(HandlerUtils.nullToEmpty(rs.getString(1)));
            } else {
                throw new HandlerException("Unable to lookup account number: "+account);
            }
            
        } catch (SQLException sqle) {
            logger.warn("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(rs);
            close(stmt);
            close(conn);
        }
        
    }
    
    
    /**
     * Returns a report on sales for a specific supplier over a period of time.
     * @param toHandle A SOAP <code>Body</code> element.
     * @return a of item records by dates.
     * @throws HandlerException if anything is wrong with the SOAP
     * message or if there is a problem with the database.
     */
    private void getSupplierSales(Element toHandle, Element toAppend)
    throws HandlerException {
        
        String startDate = HandlerUtils.getRequiredString(toHandle, "startDate");
        String endDate = HandlerUtils.getOptionalString(toHandle, "endDate");
        String supplier = HandlerUtils.getRequiredString(toHandle, "supplier");
        String customer = HandlerUtils.getOptionalString(toHandle, "customer");
        
        // Date format is 'YYYY-MM-DD HH:MM:SS.000'
        if (!startDate.matches("^\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\.000$")) {
            throw new HandlerException("startDate must be of the format 'YYYY-MM-DD HH:MM:SS.000");
        }
        
        if (endDate != null && !endDate.matches("^\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\.000$")) {
            throw new HandlerException("endDate must be of the format 'YYYY-MM-DD HH:MM:SS.000");
        }
        logger.debug("Retrieving Supplier Sales Report.");
        String SQL =
                " SELECT  Customer.AccountNumber, Item.Description, [Transaction].Time, TransactionEntry.Quantity "+
                " FROM     TransactionEntry INNER JOIN [Transaction] WITH(NOLOCK) "+
                "   ON TransactionEntry.TransactionNumber = [Transaction].TransactionNumber "+
                " LEFT JOIN   Item WITH(NOLOCK) ON TransactionEntry.ItemID = Item.ID "+
                " LEFT JOIN   Customer WITH(NOLOCK) ON [Transaction].CustomerID = Customer.ID"+
                " LEFT JOIN   Supplier WITH(NOLOCK) ON Item.SupplierID = Supplier.ID "+
                " WHERE Supplier.SupplierName = ?  AND [Transaction].Time > ? AND [Transaction].Time < ? ";
        
        
        
        
        
        if (customer != null) {
            SQL += " AND Customer.AccountNumber = ? ";
        }
        SQL +=  " ORDER BY Customer.AccountNumber, [Transaction].Time ";
        
        Connection conn = HandlerUtils.connectTo(connectionURL);
        PreparedStatement stmt = null;
        ResultSet rs = null;
        
        logger.debug("Start Date: " + startDate);
        if (endDate == null) {
            // Since there's no end date, we'll set the end date to the EOD of start date.
            endDate = startDate.substring(0,1);         // Attach start date.
            endDate = endDate + " " + "23:59:59.000";   // Attach end time for start date.
            logger.debug("Create End Date of: " + endDate);
        } else {
            logger.debug("End Date: " + endDate);
        }
        
        try {
            stmt = conn.prepareStatement(SQL);
            
            stmt.setString(1, supplier);
            stmt.setString(2, startDate);
            stmt.setString(3, endDate);
            
            if (customer != null) {
                stmt.setString(4,customer);
            }
            
            rs = stmt.executeQuery();
            while (rs != null && rs.next()) {
                Element itemEl = toAppend.addElement("sale");
                itemEl.addElement("customer").addText(HandlerUtils.nullToEmpty(rs.getString(1)));
                itemEl.addElement("item").addText(HandlerUtils.nullToEmpty(rs.getString(2)));
                itemEl.addElement("date").addText(HandlerUtils.nullToEmpty(rs.getString(3)));
                itemEl.addElement("quantity").addText(HandlerUtils.nullToEmpty(rs.getString(4)));
            }
        } catch (SQLException sqle) {
            logger.warn("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            if (rs != null) {
                try { rs.close(); } catch (SQLException sqle) { }
            }
            if (stmt != null) {
                try { stmt.close(); } catch (SQLException sqle) { }
            }
            if (conn != null) {
                try { conn.close(); } catch (SQLException sqle) { }
            }
        }
        
    }
    
    
    /**
     *  Places an order
     * @param toHandle A SOAP <code>Body</code> element.
     * @throws HandlerException if anything is wrong with the SOAP
     * message or if there is a problem with the database.
     */
    private void placeOrder_old(Element toHandle, Element toAppend)
    throws HandlerException {
        
        String customer = HandlerUtils.getRequiredString(toHandle, "customer");
        String itemId = HandlerUtils.getRequiredString(toHandle, "item");
        
        
        String create =
                " CREATE PROCEDURE sp_blank_trans"+
                " AS"+
                " BEGIN"+
                "    BEGIN TRANSACTION"+
                "	    INSERT [transaction] (customerId) VALUES ('"+customer+"')"+
                "	    INSERT transactionEntry (transactionNumber,itemId) VALUES"+
                "	    (SCOPE_IDENTITY(), '"+itemId+"')"+
                "    COMMIT TRANSACTION"+
                "    RETURN 0"+
                " END ";
        
        String execute = "sp_blank_trans";
        
        String cleanup =
                " DROP PROCEDURE sp_blank_trans ";
        
        Connection conn = HandlerUtils.connectTo(connectionURL);
        Statement stmt = null;
        ResultSet rs = null;
        
        try {
            stmt = conn.createStatement();
            
            stmt.execute(create);
            stmt.execute(execute);
            stmt.execute(cleanup);
            
        } catch (SQLException sqle) {
            logger.warn("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            if (rs != null) {
                try { rs.close(); } catch (SQLException sqle) { }
            }
            if (stmt != null) {
                try { stmt.close(); } catch (SQLException sqle) { }
            }
            if (conn != null) {
                try { conn.close(); } catch (SQLException sqle) { }
            }
        }
        
        toAppend.addElement("result").addText("complete");
        
    }
    
    /** Looks up the database key for the customer table ("ID") from an account number
     */
    private int lookupCustomerId(String customerAccount) throws HandlerException{
        
        if (null == customerAccount) {throw new NullPointerException("customerAccount is null"); }
        
        Connection conn = HandlerUtils.connectTo(connectionURL);
        PreparedStatement stmt = null;
        ResultSet rs = null;
        
        int result = -1;
        try {
            stmt = conn.prepareStatement("SELECT Id FROM customer WHERE accountNumber=?");
            stmt.setString(1,customerAccount);
            rs = stmt.executeQuery();
            
            if (rs != null && rs.next()) {
                result = rs.getInt(1);
            } else {
                throw new HandlerException("Unable to find account number: "+customerAccount);
            }
            
        } catch (SQLException sqle) {
            logger.warn("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(rs);
            close(stmt);
            close(conn);
        }
        return result;
    }
    
    /**  Demo Method
     *    Updates the on-hand quantity for the kitty demo when kegs are purchased.
     *  Checks to see if the ordered itemId matches a product in the mysql table, then
     *  updates the quantity if so.  Note, this is not a middleware method.
     *  @param itemId the Quicksell ID
     *  @param the quantity to order, must be non-zero
     */
    private void checkAndUpdateQty(int itemId, int quantity) throws HandlerException{
        
        if (quantity < 0) {throw new HandlerException("quantity argument to checkAndUpdateQty was < 0");}
        
        Connection conn = HandlerUtils.connectTo(mysqlConnectionURL);
        PreparedStatement stmt = null;
        ResultSet rs = null;
        
        try {
            
            String select = "SELECT id FROM product WHERE qid=?";
            String update = "UPDATE inventory SET qtyOnHand=qtyOnHand+? WHERE product=?";
            
            stmt = conn.prepareStatement(select);
            stmt.setInt(1,itemId);
            rs = stmt.executeQuery();
            if (rs.next()) {
                int pid = rs.getInt(1);
                logger.debug("Found updateable item in purchase order: QS#"+itemId+"/KH#"+pid+" Quan:"+quantity);
                close(rs);
                close(stmt);
                stmt = conn.prepareStatement(update);
                stmt.setInt(1,quantity);
                stmt.setInt(2, pid);
                stmt.executeUpdate();
            }
            
        } catch (SQLException sqle) {
            logger.warn("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
            close(rs);
            close(conn);
        }
        
    }
    
    /**
     *  Places an order
     *  <customer>1234</customer>
     *  <cashier>2</cashier>
     *  <total>45.23</total>
     *  <item>
     *    <itemId>99</itemId>
     *    <quantity>2</quantity>
     *  </item>
     *  <item>
     *    ...
     *  </item>
     * @param toHandle A SOAP <code>Body</code> element.
     * @throws HandlerException if anything is wrong with the SOAP
     * message or if there is a problem with the database.
     */
    private void placeOrder(Element toHandle, Element toAppend)
    throws HandlerException {
        
        // get top-level arguments
        String customerAccount = HandlerUtils.getRequiredString(toHandle, "customer");
        int cashierId = HandlerUtils.getRequiredInteger(toHandle, "cashier");
        String totalPrice = HandlerUtils.getRequiredString(toHandle, "total");
        // TODO: verify that totalPrice is a dollar amount
        
        
        int customerId = lookupCustomerId(customerAccount);
        Connection conn = HandlerUtils.connectTo(connectionURL);
        Statement stmt = null;
        ResultSet rs = null;
        PreparedStatement pstmt = null;
        
        // Prepare the SP to handle all the inserts
        StringBuffer createBuf = new StringBuffer(
                " CREATE PROCEDURE sp_blank_trans"+
                " AS"+
                " BEGIN"+
                "    BEGIN TRANSACTION"+
                "       DECLARE @transId AS int"+
                "	    INSERT [transaction] (customerId,cashierId,total) VALUES ("+customerId+","+cashierId+","+totalPrice+")"+
                "       SET @transId = SCOPE_IDENTITY()"
                );
        
        
        //Add an insert for each item in the order
        try {
            Iterator i = toHandle.elementIterator("item");
            while (i.hasNext()) {
                Element e = (Element) i.next();
                //item properties
                int itemId = HandlerUtils.getRequiredInteger(e,"itemId");
                int quantity = HandlerUtils.getRequiredInteger(e,"quantity");
                
                //update the auper-mysql database qtyinstock for this item
                checkAndUpdateQty(itemId,quantity);
                
                //lookup info about the item
                String select = "SELECT price, cost FROM item where id=?";
                PreparedStatement s2 = conn.prepareStatement(select);
                s2.setInt(1,itemId);
                rs = s2.executeQuery();
                
                if (rs == null) {
                    Element err = toAppend.addElement("error");
                    err.addElement("detail").addText("Unknown item: "+itemId);
                    err.addElement("item").addText(String.valueOf(itemId));
                    
                } else {
                    
                    while (rs != null && rs.next()) {
                        createBuf.append(
                                " INSERT transactionEntry (itemId,quantity,cost,price,fullprice,priceSource,transactionNumber) VALUES "+
                                " ("+itemId+","+quantity+","+rs.getString(2)+","+rs.getString(1)+","+rs.getString(1)+",1,@transId) "
                                );
                    }
                    close(s2);
                    close(rs);
                }
            }
            
            createBuf.append(
                    "    COMMIT TRANSACTION"+
                    "    RETURN 0"+
                    " END "
                    );
            
            String create = createBuf.toString();
            String execute = "sp_blank_trans";
            
            
            // Create, call, and remove the temporary SP
            logger.debug("Executing SP: "+create);
            stmt = conn.createStatement();
            stmt.execute(create);
            stmt.execute(execute);
            
            
        } catch (SQLException sqle) {
            logger.warn("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            try {
                String cleanup = " DROP PROCEDURE sp_blank_trans ";
                stmt.execute(cleanup);
            } catch (Exception e) {}
            close(rs);
            close(stmt);
            close(pstmt);
            close(conn);
        }
        
        toAppend.addElement("result").addText("complete");
        
    }
    
    private void getAllSuppliers(Element toHandle, Element toAppend)
    throws HandlerException {
        
        Connection conn = HandlerUtils.connectTo(connectionURL);
        PreparedStatement stmt = null;
        ResultSet rs = null;
        
        try {
            stmt = conn.prepareStatement("SELECT id,suppliername FROM supplier ORDER BY suppliername");
            rs = stmt.executeQuery();
            
            while (rs != null && rs.next()) {
                Element el = toAppend.addElement("supplier");
                el.addElement("name").addText(HandlerUtils.nullToEmpty(rs.getString(2)));
                el.addElement("id").addText(HandlerUtils.nullToEmpty(rs.getString(1)));
            }
            
        } catch (SQLException sqle) {
            logger.warn("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(rs);
            close(stmt);
            close(conn);
        }
        
    }
    
    
    private void getAllCustomers(Element toHandle, Element toAppend)
    throws HandlerException {
        
        Connection conn = HandlerUtils.connectTo(connectionURL);
        PreparedStatement stmt = null;
        ResultSet rs = null;
        
        try {
            stmt = conn.prepareStatement("SELECT accountNumber,company FROM customer ORDER BY company");
            rs = stmt.executeQuery();
            
            while (rs != null && rs.next()) {
                Element el = toAppend.addElement("customer");
                el.addElement("accountNumber").addText(HandlerUtils.nullToEmpty(rs.getString(1)));
                el.addElement("name").addText(HandlerUtils.nullToEmpty(rs.getString(2)));
            }
            
        } catch (SQLException sqle) {
            logger.warn("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(rs);
            close(stmt);
            close(conn);
        }
        
    }
    
    private void getItemCatalog(Element toHandle, Element toAppend)
    throws HandlerException {
        
        String account = HandlerUtils.getRequiredString(toHandle,"account");
        
        Connection conn = HandlerUtils.connectTo(connectionURL);
        PreparedStatement stmt = null;
        ResultSet rs = null;
        
        int priceLevel = 0;
        
        try {
            stmt = conn.prepareStatement("SELECT PriceLevel FROM customer WHERE AccountNumber=?");
            stmt.setString(1,account);
            rs = stmt.executeQuery();
            
            while (rs != null && rs.next()) {
                priceLevel = rs.getInt(1);
            }
            String priceField = "price";
            if (priceLevel == 1) {
                priceField = "priceA";
            } else if (priceLevel == 2) {
                priceField = "priceB";
            } else if (priceLevel == 3) {
                priceField = "priceC";
            }
            
            rs = stmt.executeQuery(" SELECT item.id,item.description,item."+priceField+", "+
                    "supplier.supplierName,item.price,item.tagAlongItem "+
                    " FROM item LEFT JOIN supplier ON supplier.id=item.supplierId"+
                    " ORDER BY supplier.supplierName,item.description");
            
            while (rs != null && rs.next()) {
                Element item =toAppend.addElement("item");
                item.addElement("id").addText(rs.getString(1));
                item.addElement("description").addText(rs.getString(2));
                String price = rs.getString(3);
                if ("0.0000".equals(price)) {
                    price = rs.getString(5);
                }
                item.addElement("price").addText(HandlerUtils.nullToEmpty(price));
                String supplier = "No Supplier";
                if (rs.getString(4) != null) {
                    supplier = rs.getString(4);
                }
                item.addElement("supplier").addText(supplier);
                String deposit = "0";
                if (rs.getString(6) != null) {
                    deposit = rs.getString(6);
                }
                item.addElement("depositId").addText(deposit);
            }
            
            // Add item history
            Element history = toAppend.addElement("history");
            stmt = conn.prepareStatement(
                    "SELECT DISTINCT item.id,item.description,item.priceA,supplier.supplierName,item.price,item.tagAlongItem "+
                    " FROM TransactionEntry INNER JOIN [Transaction] WITH(NOLOCK) "+
                    "      ON TransactionEntry.TransactionNumber = [Transaction].TransactionNumber "+
                    " LEFT JOIN item WITH (NOLOCK) ON TransactionEntry.ItemID = Item.ID "+
                    " INNER JOIN supplier ON supplier.id=item.supplierId "+
                    " LEFT JOIN Customer WITH (NOLOCK) ON [Transaction].CustomerId = Customer.ID "+
                    " WHERE Customer.AccountNumber = ? "+
                    " ORDER BY supplier.supplierName,item.description "
                    );
            stmt.setString(1,account);
            rs = stmt.executeQuery();
            while (rs != null && rs.next()) {
                Element item = history.addElement("item");
                item.addElement("id").addText(rs.getString(1));
                item.addElement("description").addText(rs.getString(2));
                String price = rs.getString(3);
                if ("0.0000".equals(price)) {
                    price = rs.getString(5);
                }
                item.addElement("price").addText(HandlerUtils.nullToEmpty(price));
                String supplier = "No Supplier";
                if (rs.getString(4) != null) {
                    supplier = rs.getString(4);
                }
                item.addElement("supplier").addText(supplier);
                item.addElement("depositId").addText(HandlerUtils.nullToEmpty(rs.getString(6)));
            }
            
            // Add deposit values
            Element deposits = toAppend.addElement("deposits");
            stmt = conn.prepareStatement("SELECT id,description,price FROM item WHERE id IN "+
                    "   ( SELECT DISTINCT tagAlongItem FROM ITEM )");
            rs = stmt.executeQuery();
            while (rs != null && rs.next()) {
                Element item = deposits.addElement("item");
                item.addElement("id").addText(rs.getString(1));
                item.addElement("description").addText(rs.getString(2));
                String price = rs.getString(3);
                if ("0.0000".equals(price)) {
                    price = rs.getString(5);
                }
                item.addElement("price").addText(HandlerUtils.nullToEmpty(price));
            }
            
            
        } catch (SQLException sqle) {
            logger.warn("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(rs);
            close(stmt);
            close(conn);
        }
        
    }
    
    private double kegToOz(double keg) {
        return keg * 1920;
    }
    
    private void processReading(Element toHandle, Element toAppend)
    throws HandlerException {
        
        //Get a fresh batch of readings.
        Set<AuperReading> readings = AuperManager.getReadings();
        
        //Connect to the db
        Connection conn = HandlerUtils.connectTo(connectionURL);
        PreparedStatement stmt = null;
        
        try {
            
            for (AuperReading r: readings) {
                AuperKegs difference = AuperReading.difference(r,AuperReading.mostRecent(r.getPid()));
                
                String insert =
                        "INSERT INTO readings (pid,qtyInStock)" +
                        "VALUES (?,?)";
                
                String update =
                        "UPDATE products SET " +
                        "qtyInStock=qtyInStock+?, " +
                        "qtyPoured=qtyPoured+? " +
                        "WHERE pid=?" ;
                
                //Insert a reading record
                stmt = conn.prepareStatement(insert);
                stmt.setInt(1,r.getPid());
                stmt.setDouble(2,r.getKegs().asKegs());
                stmt.executeUpdate();
                
                //Update the products records
                stmt = conn.prepareStatement(update);
                stmt.setDouble(1, difference.asKegs());
                stmt.setDouble(2, difference.asOz());
                stmt.setInt(3, r.getPid());
                stmt.executeUpdate();
                
            }
            
        } catch (SQLException sqle) {
            logger.warn("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
            close(conn);
        }
        
    }
    
    
}
