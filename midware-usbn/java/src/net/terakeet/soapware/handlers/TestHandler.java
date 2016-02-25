/**
 * TestHandler.java
 *
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


public class TestHandler implements Handler {
    private MidwareLogger logger;
    private SecureSession ss;


    public TestHandler() throws HandlerException {
        logger = new MidwareLogger(DummyHandler.class.getName());
        HandlerUtils.initializeClientKeyManager();
    }

    public void handle(Element toHandle, Element toAppend) throws HandlerException{
        
        String clientKey = HandlerUtils.getOptionalString(toHandle,"clientKey");
        ss = ClientKeyManager.getSession(clientKey);
        
        String function = toHandle.getName();
        logger = new MidwareLogger(TestHandler.class.getName(), function);
        logger.debug("TestHandler processing method: "+function);
        logger.xml("request: " + toHandle.asXML());
        
        try {
           if (ss.getLocation() == 0 && ss.getClientId() == 1 && ss.getSecurityLevel().canAdmin()) {
                if ("sleepTest".equals(function)) {
                    sleepTest(toHandle, responseFor(function,toAppend));
                }
           } else {
                // access violation
                addErrorDetail(toAppend, "Access violation: This method is not available with your client key");
                logger.portalAccessViolation("Tried to call '"+function+"' with key "+ss.toString());
            }
        } catch (Exception e) {
            if (e instanceof HandlerException) {
                throw (HandlerException) e;
            } else {
                logger.midwareError("Non-handler exception thrown in TestHandler: "+e.toString());
                logger.midwareError("XML: "+toHandle.asXML());
                throw new HandlerException(e);
            }
        }
        logger.xml("response: " + toAppend.asXML());
    }
    
    private void addErrorDetail(Element toAppend, String message) {
        toAppend.addElement("error").addElement("detail").addText(message);
    }
    
    private Element responseFor(String s, Element e) {
        String responseNamespace = (String)SOAPMessage.getURIMap().get("tkmsg");
        return e.addElement("m:"+s+"Response",responseNamespace);
    }
    
    private void sleepTest(Element toHandle, Element toAppend) throws HandlerException {
        long sleepTime = HandlerUtils.getRequiredLong(toHandle,"sleepTime");
        try {
            logger.debug("Going to sleep...");
            Thread.sleep(sleepTime);
            logger.debug("Woke up.");
        } catch (InterruptedException ie) {
            logger.debug("Sleep Interrupted");
        }
    }
    
}
