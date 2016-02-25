/**
 * SOAPMessage.java
 *
 * @author Ben Ransford
 * @version $Id: SOAPMessage.java,v 1.8 2011/04/19 16:28:15 sravindran Exp $
 */
package net.terakeet.soapware;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Writer;
import java.net.MalformedURLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import net.terakeet.util.MidwareLogger;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.DocumentFactory;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;
import org.dom4j.io.XMLWriter;

/**
 * Essentially a wrapper (with some SOAP-related utility functions)
 * around <code>org.dom4j.Document</code>.
 */
public class SOAPMessage {
    static MidwareLogger logger = new MidwareLogger(SOAPMessage.class.getName());
    private Document msgDoc;
    private /* static */ DocumentFactory factory = null;
    private static Map uriMap;
    private Iterator processThese;
    private Element bodyElement;

    /**
     * Default constructor for assembling a new SOAP message not based
     * on an existing document (e.g., a response message).
     */
    public SOAPMessage () {
	if (SOAPMessage.uriMap == null) {
	    uriMap = new HashMap();
	    synchronized (SOAPMessage.uriMap) {
		uriMap.put("SOAP-ENV", "http://schemas.xmlsoap.org/soap/envelope/");
		uriMap.put("tkmsg", "http://www.terakeet.net/schemas/usadMessage");
	    }
	}

	factory = new DocumentFactory();
	factory.setXPathNamespaceURIs(uriMap);

	msgDoc = factory.createDocument();
	this.bodyElement = msgDoc
	    .addElement("SOAP-ENV:Envelope", uriMap.get("SOAP-ENV").toString())
	    .addElement("SOAP-ENV:Body", uriMap.get("SOAP-ENV").toString());
    }

    /**
     * Default constructor for assembling a SOAP message based on an
     * existing <code>String</code>ified document.
     * @param docString An XML document (i.e., a SOAP message) as a String.
     * @throws MessageException if there is any problem building the
     * <code>Document</code> object, or if the document is malformed,
     * or if the document isn't a proper SOAP message.
     */
    public SOAPMessage (String docString) throws MessageException {
	Document doc;
	try {
	    doc = DocumentHelper.parseText(docString);
            
	} catch (Exception e) {
            
            throw new MessageException(e);
	}

	Element root = doc.getRootElement();
	if (root == null) {
            
            throw new MessageException("No root element available");
	}
        
	this.bodyElement = (Element)
	root.selectSingleNode("/SOAP-ENV:Envelope/SOAP-ENV:Body");
	if (bodyElement == null) {
            
	    throw new MessageException("No SOAP Body element available");
	}

	processThese = bodyElement.elementIterator();
	this.msgDoc = doc;
    }

    /**
     * Default constructor for assembling a SOAP message based on an
     * existing file.
     * @param docFile An XML document (i.e., a SOAP message) file.
     * @throws MessageException if there is any problem building the
     * <code>Document</code> object, or if the document is malformed,
     * or if the document isn't a proper SOAP message.
     */
    public static SOAPMessage instanceFromFile (File docFile) throws MessageException {
        FileReader in = null;
        BufferedReader reader = null;
        StringBuilder fullFile = new StringBuilder();
        try {
            in = new FileReader(docFile);
            reader = new BufferedReader(in);
            String line;
            while ((line = reader.readLine()) != null) {
                fullFile.append(line);
            }
        } catch (Exception e) {
            logger.dbError("Exception while reading "+docFile+": "+e);
        } finally {
            try {
                in.close();
                reader.close();
            } catch (Exception ignored) {}
        }
        return new SOAPMessage(fullFile.toString());
//	Document doc;
//	try {
//	    SAXReader reader = new SAXReader();
//	    doc = reader.read(docFile);
//	} catch (Exception e) {
//	    throw new MessageException("XML parser error: " + e.getMessage());
//	}
//
//	Element root = doc.getRootElement();
//	if (root == null) {
//	    throw new MessageException("No root element available");
//	}
//
//	this.bodyElement = (Element)
//	    root.selectSingleNode("/SOAP-ENV:Envelope/SOAP-ENV:Body");
//	if (bodyElement == null) {
//	    throw new MessageException("No SOAP Body element available");
//	}
//
//	processThese = bodyElement.elementIterator();
//	this.msgDoc = doc;
    }

    /**
     * Default constructor for assembling a SOAP message based on an
     * existing DOM <code>Document</code> object.
     * @param doc An XML document (i.e., a SOAP message).
     * @throws MessageException if there is any problem building the
     * <code>Document</code> object, or if the document is malformed,
     * or if the document isn't a proper SOAP message.
     */
    public SOAPMessage (Document doc) throws MessageException {
	Element root = doc.getRootElement();
	if (root == null) {
	    throw new MessageException("No root element available");
	}

	this.bodyElement = (Element)
	    root.selectSingleNode("/SOAP-ENV:Envelope/SOAP-ENV:Body");
	if (bodyElement == null) {
	    throw new MessageException("No SOAP Body element available");
	}

	processThese = bodyElement.elementIterator();
	this.msgDoc = doc;
    }

    /**
     * @return the next processable <code>Element</code> from the SOAP
     * message body.
     */
    public Element next () {
	if (! processThese.hasNext()) {
	    throw new NullPointerException("No more elements to process");
	}

	return (Element)processThese.next();
    }

    /**
     * @return <code>true</code> if there are more processable
     * <code>Element</code>s in the SOAP Body, <code>false</code>
     * otherwise.
     */
    public boolean hasNext () {
	return processThese.hasNext();
    }

    /**
     * @return the SOAP Body of this message.
     */
    public Element getBodyElement () {
	return this.bodyElement;
    }

    /**
     * @return an XML document in <code>String</code> form.
     */
    public String toString () {
	return msgDoc.asXML();
    }

    /**
     * @return a mapping from namespace prefixes to their namespace
     * URIs ("SOAP-ENV", etc.).
     */
    public static Map getURIMap () {
	return SOAPMessage.uriMap;
    }

    /**
     * Writes this <code>SOAPMessage</code>'s contents to a file.
     * Uses an <code>XMLWriter</code> wrapper around the provided
     * <code>Writer</code> because, for some reason,
     * <code>org.dom4j.Document.write(java.io.Writer)</code> didn't
     * work (created 0-byte files).
     * @param w a <code>Writer</code> (<code>FileWriter</code>, for
     * example) to which the XML version of this
     * <code>SOAPMessage</code> will be stored.
     */
    public void writeTo (Writer w) throws IOException {
		XMLWriter writer = null;
		try {
			writer = new XMLWriter(w);
			writer.write(msgDoc);
		} finally {
			if (null != writer) {
				try {
					writer.close();
				} catch (IOException ignore) {}
			}
		}
    }
}
