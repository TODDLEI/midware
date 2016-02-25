/**
 * StatusHandler.java
 *
 * @author Alex Astle
 * @version $$
 */
package net.terakeet.soapware.handlers;

// package imports.
import net.terakeet.soapware.Handler;
import net.terakeet.soapware.HandlerException;
import net.terakeet.soapware.SOAPMessage;
import net.terakeet.soapware.DatabaseConnectionManager;
import net.terakeet.soapware.RegisteredConnection;
import net.terakeet.util.MidwareLogger;
import java.sql.*;
import org.dom4j.Element;

public class StatusHandler implements Handler {
    static MidwareLogger logger = new MidwareLogger(StatusHandler.class.getName());

    private static final String connName = "auper";

    public StatusHandler() {
    }

    public void handle (Element toHandle, Element toAppend) throws HandlerException {
		String function = toHandle.getName();
		String responseNamespace = (String)SOAPMessage.getURIMap().get("tkmsg");

        RegisteredConnection conn = DatabaseConnectionManager.getNewConnection(connName,
                function+" (StatusHandler)");
		String success = "false";
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try {
			String select = "SELECT id FROM location WHERE id = 11";
			
			stmt = conn.prepareStatement(select);
			rs = stmt.executeQuery();
			success = "true";
		} catch (Exception e) {
			logger.midwareError("Status error: " + e.toString());
		} finally {
			if (null != rs) {
				try { rs.close(); } catch (Exception ignore) {}
			}
			if (null != stmt) {
				try { stmt.close(); } catch (Exception ignore) {}
			}
			if (null != conn) {
				conn.close();
			}
		}

		toAppend.addElement("m:"+function+"Response", responseNamespace)
			.addElement("success")
			.addText(success);
    }
}
