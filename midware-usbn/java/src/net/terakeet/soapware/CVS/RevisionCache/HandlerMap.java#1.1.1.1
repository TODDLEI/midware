/**
 * HandlerMap.java
 *
 * @author Ben Ransford
 * @version $Id: HandlerMap.java,v 1.1.1.1 2005/08/26 19:16:01 anonymous Exp $
 */
package net.terakeet.soapware;

import java.io.File;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.log4j.Logger;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

/**
 * Loads and provides a map of tag names (that might appear in SOAP
 * requests) to the class names of classes that handle the tags.  See
 * the listener.handlerMapPath property for the full path to the XML
 * file from which the handler map is loaded.
 *
 * A {@ref CleanerUpper} object periodically asks the HashMap to check
 * whether the on-disk mapping has changed; if so, this class reloads
 * its data.
 */
class HandlerMap {
    static Logger logger = Logger.getLogger(HandlerMap.class.getName());

    static final String HANDLER_PACKAGE = "net.terakeet.soapware.handlers";

    private String dataFilePath; // FQ path to an XML file
    private Map map;
    private SAXReader saxr;
    private ClassLoader classLoader;

    /**
     * Default constructor.  Loads the map from disk.
     * @param dataFilePath the fully-qualified path of an XML file
     * that describes a mapping from tags to their handlers.
     */
    HandlerMap (String dataFilePath) {
	this.dataFilePath = dataFilePath;
	this.map = new HashMap();
	this.saxr = new SAXReader();
	this.classLoader = this.getClass().getClassLoader();

	reloadData();
    }

    /**
     * Reloads the map from disk.  This method is synchronized so that
     * no other objects can get mapping information until the data
     * reload is finished.
     */
    public synchronized void reloadData () {
	logger.info("Reloading handler map.");

	File f = new File(dataFilePath);
	Document doc;
	try {
	    doc = saxr.read(f);
	} catch (Exception e) {
	    logger.warn("Handler map is unparsable.");
	    notifyAll();
	    return;
	}

	Element hm = doc.getRootElement();
	if ("handlerMap".equals(hm.getName())) {
	    Iterator i = hm.elementIterator("handler");
	    Element el;
	    String name, className;

	    map.clear();
	    while (i.hasNext()) {
		el = (Element)i.next();
		name = el.attributeValue("tag");
		className = el.getTextTrim();
		if (name == null) {
		    logger.warn("Encountered bad <handler> tag in handler map " +
				"(no valid 'tag' attribute).");
		    continue;
		}
		if ("".equals(className)) {
		    logger.warn("Encountered empty bad class name in handler map " +
				"(tag='" + name + "').");
		    continue;
		}
		map.put(name, className);
	    }

	} else {
	    logger.warn("Encountered unexpected tag (" + hm.getName() +
			") in handler map.");
	}

	logger.info("Loaded " + map.size() + " mappings.");
	notifyAll();
    }

    /**
     * Returns an instantiable <code>Class</code> object from the
     * handler map, given a tag name.
     * @param tagName the name of the tag to be handled.
     * @return A <code>Class</code> object the caller can instantiate
     * to handle the tag with the given name.
     * @throws HandlerException if there is no handler specified for
     * the given tag name.
     */
    public Class get (String tagName) throws HandlerException {
	if (tagName == null) {
	    throw new NullPointerException("Cannot get handler for a null tag name.");
	}
	if ("".equals(tagName)) {
	    throw new IllegalArgumentException("Cannot get handler for an empty tag name.");
	}

	String classToLoad;
	Object o = map.get(tagName);
	if ((o != null) && (o instanceof String)) {
	    classToLoad = HANDLER_PACKAGE + "." + (String)o;
	    try {
		return classLoader.loadClass(classToLoad);
	    } catch (ClassNotFoundException cnfe) {
		logger.warn("Missing handler class " + classToLoad +
			    " for (tag='" + tagName + "').");
		throw new HandlerException(cnfe);
	    }
	} else {
	    throw new HandlerException("No handler for this tag.");
	}
    }

}
