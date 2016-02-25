/*
 * AuperManager.java
 *
 * Created on August 18, 2005, 5:01 PM
 *
 * To change this template, choose Tools | Options and locate the template under
 * the Source Creation and Management node. Right-click the template and choose
 * Open. You can then make changes to the template in the Source Editor.
 */

package net.terakeet.soapware.handlers.auper;

import java.util.Set;
import java.util.HashSet;
import java.util.Iterator;
import org.dom4j.Element;
import org.dom4j.Document;
import org.dom4j.DocumentHelper;

/**
 *
 * @author Administrator
 */
public class AuperManager {
    
    /** Creates a new instance of AuperManager */
    public AuperManager() {
    }
    
 
    private void debugPrint(String s) {
        Util.debugPrint(s);
    }
    
    public static Set<AuperReading> getReadings() {
        
        Set<AuperReading> result = new HashSet<AuperReading>();
        try {
        
            AuperConnection conn = new AuperConnection();

            
            Command64 c64 = new Command64();
            boolean success = conn.sendCommand(c64);
            
            Document xmlResult = null;        

            Command65 c65 = new Command65();

            success = conn.sendCommand(c65);
            conn.close();
            if (success && c65.isSuccess()) {
                xmlResult = DocumentHelper.parseText(c65.getXmlString());
                if (xmlResult != null) {
                    Iterator i = xmlResult.getRootElement().elementIterator("Product");
                    while (i.hasNext()) {
                        Element el = (Element) i.next();
                        int pid = Integer.parseInt(el.attributeValue("index"));
                        double quantity = Double.parseDouble(el.element("QtyInStock").getText());
                        result.add(new AuperReading(pid,new AuperKegs(quantity)));
                    }
                }
            } 
        
        } catch (AuperException ae) {
            // Couldn't connect
            // TODO: Handle
        } catch (org.dom4j.DocumentException de) {
            // Bad XML
            // TODO: Handle
        }
        
        return result;
    }
    
}
