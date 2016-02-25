/**
 * Handler.java
 *
 * @author Ben Ransford
 * @version $Id: Handler.java,v 1.1.1.1 2005/08/26 19:16:01 anonymous Exp $
 */
package net.terakeet.soapware;

// package imports.
import org.dom4j.Element;

/**
 * Request handler to be dispatched by {@link
 * net.terakeet.soapware.MessageProcessor} objects when a SOAP request
 * arrives or is dequeued.
 */
public interface Handler {

    /**
     * Takes the child of a SOAP <code>&lt;Body&gt;</code> element (preferably)
     * containing a method name and parameters.  Does processing based
     * on the contents of the element and assembles its own Element to
     * be wrapped in a SOAP <code>&lt;Body&gt;</code> tag for
     * returning to the client.
     * @param toHandle a DOM element containing a SOAP method call and
     * its parameters.
     * @param toAppend a DOM element (i.e., a SOAP
     * <code>&lt;Body&gt;</code> element) underneath which this method
     * should place its response as a child node.
     */
    void handle (Element toHandle, Element toAppend) throws HandlerException;

}
