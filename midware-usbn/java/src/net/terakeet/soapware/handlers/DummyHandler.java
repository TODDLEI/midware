/**
 * DummyHandler.java
 *
 * @author Ben Ransford
 * @version $Id: DummyHandler.java,v 1.6 2006/04/20 16:25:32 anonymous Exp $
 */
package net.terakeet.soapware.handlers;

// package imports.
import net.terakeet.soapware.Handler;
import net.terakeet.soapware.HandlerException;
import net.terakeet.soapware.HandlerUtils;
import net.terakeet.soapware.SOAPMessage;
import net.terakeet.soapware.security.*;
import net.terakeet.util.MidwareLogger;
import org.apache.log4j.Logger;
import org.dom4j.Element;

public class DummyHandler implements Handler {
    private MidwareLogger logger; 

    public DummyHandler () throws HandlerException {
        logger = new MidwareLogger(DummyHandler.class.getName());
        HandlerUtils.initializeClientKeyManager();
	/* thunk */
    }

    public void handle (Element toHandle, Element toAppend) throws HandlerException {
	
        String function = toHandle.getName();
        String responseNamespace = (String)SOAPMessage.getURIMap().get("tkmsg");
        
        logger = new MidwareLogger(DummyHandler.class.getName(), function);
        logger.debug("DummyHandler processing method: "+function);
        logger.xml("request: " + toHandle.asXML());

	toAppend.addElement("m:"+function+"Response", responseNamespace)
	    .addElement("success")
	    .addText("true");
        
        String clientKey = HandlerUtils.getOptionalString(toHandle,"clientKey");
        SecureSession ss = ClientKeyManager.getSession(clientKey);
        
        if (clientKey != null && clientKey.length() > 0) {
             logger.debug("Loc# "+ss.getLocation()+"-"+ss.getClientId()+" Access: "+ss.getSecurityLevel());
        } else {
             logger.debug("No clientkey received");
        }
        
        String message = HandlerUtils.getOptionalString(toHandle,"message");
        if (message != null) {
            logger.debug("DummyHandler message: {"+message+"}");
        }

	logger.debug("DummyHandler finished.");
    }
}
