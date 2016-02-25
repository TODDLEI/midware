/**
 * TemplatedMessage.java
 *
 * @author Ben Ransford
 * @version $Id: TemplatedMessage.java,v 1.53 2015/08/05 18:23:06 sravindran Exp $
 */
package net.terakeet.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Date;
import java.util.Properties;
import net.terakeet.util.MailException;
import java.sql.SQLException;
import java.util.ArrayList;
import javax.mail.*;
import javax.mail.event.*;
import javax.mail.internet.*;
import javax.mail.internet.MimeBodyPart;
import net.terakeet.soapware.HandlerUtils;
import net.terakeet.util.MidwareLogger;
import javax.activation.DataHandler;
import javax.activation.DataSource;
import javax.activation.FileDataSource;

public class TemplatedMessage implements ConnectionListener, TransportListener {
    private Message msg = null;
    private String loadFrom = ".";
    private String plainContent = null, htmlContent = null;
    private Session session = null;
    private Transport transport = null;
    
    private static MidwareLogger logger = new MidwareLogger("net.terakeet.soapware.handlers.SQLGetHandler");
    
    public TemplatedMessage (String subject, String loadFrom, String templateName)
		throws MailException {
        logger.debug("--Initializing TemplatedMessage");
        
	if (loadFrom != null) {
	    this.loadFrom = loadFrom;
	}
        logger.debug("--Loading Template");
	try {
	    loadTemplate(templateName);
	} catch (IOException ioe) {
         
	    throw new MailException("Problem loading mail template: " +
				    ioe.getMessage());
	}

        String smtpHost = HandlerUtils.getSetting("mail.smtp.host");
        if (null == smtpHost) {
            smtpHost = HandlerUtils.getSetting("mail.server.smtp");
        }
        String smtpLocalHost = HandlerUtils.getSetting("mail.smtp.localhost");
        String smtpPort = HandlerUtils.getSetting("mail.smtp.port");
        String smtpStartTls = HandlerUtils.getSetting("mail.smtp.starttls.enable");
        String smtpAuth = HandlerUtils.getSetting("mail.smtp.auth");
        String smtpUser = HandlerUtils.getSetting("mail.smtp.user");
        String smtpPass = HandlerUtils.getSetting("mail.smtp.pass");
        String smtpSsl = HandlerUtils.getSetting("mail.smtp.ssl");
        
        Properties props = new Properties();
        Authenticator auth = null;
	props.put("mail.transport.protocol", "smtp");
	props.put("mail.smtp.host", smtpHost);
        if (null != smtpLocalHost) {
            props.put("mail.smtp.localhost", smtpLocalHost);
        }
        if (null != smtpPort) {
            props.put("mail.smtp.port", smtpPort);
        }
        if (null != smtpStartTls) {
            props.put("mail.smtp.starttls.enable", smtpStartTls);
        }
        if (null != smtpAuth) {
            props.put("mail.smtp.auth", smtpAuth);
        }
        if (null != smtpUser && null != smtpPass) {
            auth = new MessageAuthenticator(smtpUser, smtpPass);
        }
        if ("true".equalsIgnoreCase(smtpSsl)) {
            if (null != smtpPort) {
                props.put("mail.smtp.socketFactory.port", smtpPort);
            }
            props.put("mail.smtp.socketFactory.class",
                    "javax.net.ssl.SSLSocketFactory");
            props.put("mail.smtp.socketFactory.fallback", "false");
        }

        logger.debug("--Starting Session");
	try {
            logger.debug("--Getting Session Instance");
	    this.session = Session.getInstance(props, auth);
            logger.debug("--Initializing MimeMessage");
	    this.msg = new MimeMessage(session);
	    msg.setSubject((subject != null) ? subject : "");
	    msg.setHeader("X-Mailer", "TemplatedMessage.java");
	} catch (Throwable me) {
            logger.debug("--Exception raised: "+me.toString());
	    throw new MailException((Throwable)me);
	}
        logger.debug("--Initialization Complete");
    }

    public TemplatedMessage (String subject, String loadFrom, String templateName, String Billing)
		throws MailException {
        logger.debug("--Initializing TemplatedMessage");

	if (loadFrom != null) {
	    this.loadFrom = loadFrom;
	}
        logger.debug("--Loading Template");
	try {
	    loadTemplate(templateName);
	} catch (IOException ioe) {

	    throw new MailException("Problem loading mail template: " +
				    ioe.getMessage());
	}

        String smtpHost = HandlerUtils.getSetting("mail.smtp.host");
        if (null == smtpHost) {
            smtpHost = HandlerUtils.getSetting("mail.server.smtp");
        }
        String smtpLocalHost = HandlerUtils.getSetting("mail.smtp.localhost");
        String smtpPort = HandlerUtils.getSetting("mail.smtp.port");
        String smtpStartTls = HandlerUtils.getSetting("mail.smtp.starttls.enable");
        String smtpAuth = HandlerUtils.getSetting("mail.smtp.auth");
        String smtpUser = HandlerUtils.getSetting("billmail.smtp.user");
        String smtpPass = HandlerUtils.getSetting("billmail.smtp.pass");
        String smtpSsl = HandlerUtils.getSetting("mail.smtp.ssl");

        Properties props = new Properties();
        Authenticator auth = null;
	props.put("mail.transport.protocol", "smtp");
	props.put("mail.smtp.host", smtpHost);
        if (null != smtpLocalHost) {
            props.put("mail.smtp.localhost", smtpLocalHost);
        }
        if (null != smtpPort) {
            props.put("mail.smtp.port", smtpPort);
        }
        if (null != smtpStartTls) {
            props.put("mail.smtp.starttls.enable", smtpStartTls);
        }
        if (null != smtpAuth) {
            props.put("mail.smtp.auth", smtpAuth);
        }
        if (null != smtpUser && null != smtpPass) {
            auth = new MessageAuthenticator(smtpUser, smtpPass);
        }
        if ("true".equalsIgnoreCase(smtpSsl)) {
            if (null != smtpPort) {
                props.put("mail.smtp.socketFactory.port", smtpPort);
            }
            props.put("mail.smtp.socketFactory.class",
                    "javax.net.ssl.SSLSocketFactory");
            props.put("mail.smtp.socketFactory.fallback", "false");
        }

        logger.debug("--Starting Session");
	try {
            logger.debug("--Getting Session Instance");
	    this.session = Session.getInstance(props, auth);
            logger.debug("--Initializing MimeMessage");
	    this.msg = new MimeMessage(session);
	    msg.setSubject((subject != null) ? subject : "");
	    msg.setHeader("X-Mailer", "TemplatedMessage.java");
	} catch (Throwable me) {
            logger.debug("--Exception raised: "+me.toString());
	    throw new MailException((Throwable)me);
	}
        logger.debug("--Initialization Complete");
    }

    private void loadTemplate (String templateName) throws IOException {
	File plain = new File(loadFrom + File.separator + templateName + ".txt");
	File html = new File(loadFrom + File.separator + templateName + ".html");
        logger.debug(loadFrom + File.separator + templateName + ".txt");
        
	if (plain.canRead()) {
           
	    BufferedReader br = new BufferedReader(new FileReader(plain));
	    StringBuffer sb = new StringBuffer();
	    String str;
	    while ((str = br.readLine()) != null) {
		sb.append(str);
		sb.append("\n");
	    }
	    br.close();
           this.plainContent = sb.toString();
	}

	if (html.canRead()) {
          
	    BufferedReader htmlReader = new BufferedReader(new FileReader(html));
	    StringBuffer buf = new StringBuffer();
	    String s;
	    while ((s = htmlReader.readLine()) != null) {
		buf.append(s);
		buf.append("\n");
	    }
	    htmlReader.close();
	    this.htmlContent = buf.toString();

	    if (plainContent == null) {
		String tmp = htmlContent.replaceAll("<a href=\"([^\"]*)\">([^<]*)</a>",
						    "$2 [$1]");
		plainContent = tmp.replaceAll("<[^>]+>", "").trim();
	    }
	}
    }

    public void setRecipientBCC (String recip) throws MailException {
	try {
	    InternetAddress addr = new InternetAddress(recip);
	    msg.addRecipient(Message.RecipientType.BCC, addr);
	    this.transport = session.getTransport(addr);
	    transport.addConnectionListener(this);
	    transport.addTransportListener(this);
            logger.debug(recip);
	} catch (Exception e) {
	    throw new MailException((Throwable)e);
	}
    }

    public void setRecipientCC (String recip) throws MailException {
	try {
	    InternetAddress addr = new InternetAddress(recip);
	    msg.addRecipient(Message.RecipientType.CC, addr);
	    this.transport = session.getTransport(addr);
	    transport.addConnectionListener(this);
	    transport.addTransportListener(this);
            logger.debug(recip);
	} catch (Exception e) {
	    throw new MailException((Throwable)e);
	}
    }

    public void setRecipient (String recip) throws MailException {
	try {
	    InternetAddress addr = new InternetAddress(recip);
	    msg.addRecipient(Message.RecipientType.TO, addr);
	    this.transport = session.getTransport(addr);
	    transport.addConnectionListener(this);
	    transport.addTransportListener(this);
            logger.debug(recip);
	} catch (Exception e) {
	    throw new MailException((Throwable)e);
	}
    }

    public void setSender (String sender) throws MailException {
	try {
	    msg.setFrom(new InternetAddress(sender));
             
            logger.debug(sender);
             
	} catch (MessagingException me) {
	    throw new MailException((Throwable)me);
	}
    }

    public void setReplyTo (String addr) throws MailException {
	try {
	    Address[] addresses = { (new InternetAddress(addr)) };
	    msg.setReplyTo(addresses);
	} catch (MessagingException me) {
	    throw new MailException((Throwable)me);
	}
    }

    public void send () throws MailException {
	try {
	    if (htmlContent != null) {
		MimeBodyPart plainPart = new MimeBodyPart();
		plainPart.setContent(plainContent, "text/plain");
		MimeBodyPart htmlPart = new MimeBodyPart();
		htmlPart.setContent(htmlContent, "text/html");
		Multipart container = new MimeMultipart("alternative");
		container.addBodyPart(plainPart);
		container.addBodyPart(htmlPart);
		msg.setContent(container);
	    } else {
		msg.setContent(plainContent, "text/plain");
	    }
	    msg.setSentDate(new Date());
	    transport.send(msg);
	} catch (MessagingException me) {
	    throw new MailException((Throwable)me);
	}
    }
    
     public void sendWithAttachment(String filePath, String fileName) throws MailException {
	try {
	    if (htmlContent != null) {
		MimeBodyPart plainPart = new MimeBodyPart();
		plainPart.setContent(plainContent, "text/plain");
		MimeBodyPart htmlPart = new MimeBodyPart();
		htmlPart.setContent(htmlContent, "text/html");
		Multipart container = new MimeMultipart("alternative");
		container.addBodyPart(plainPart);
		container.addBodyPart(htmlPart);
                
                BodyPart messageBodyPart = new MimeBodyPart();
                DataSource source = new FileDataSource(filePath);
                messageBodyPart.setDataHandler(new DataHandler(source));
                messageBodyPart.setFileName(fileName);
                container.addBodyPart(messageBodyPart);
		msg.setContent(container);
	    } else {
               MimeBodyPart plainPart = new MimeBodyPart();
		plainPart.setContent(plainContent, "text/plain");
                Multipart container = new MimeMultipart("alternative");
		container.addBodyPart(plainPart);
		
                BodyPart messageBodyPart = new MimeBodyPart();
                DataSource source = new FileDataSource(filePath);
                messageBodyPart.setDataHandler(new DataHandler(source));
                messageBodyPart.setFileName(fileName);
                container.addBodyPart(messageBodyPart);
		msg.setContent(container);
	    }
	    msg.setSentDate(new Date());
	    transport.send(msg);
	} catch (MessagingException me) {
	    throw new MailException((Throwable)me);
	}
    }
     
     
     public void sendWithMultiAttachment(ArrayList<String> files) throws MailException {
	try {
	    if (htmlContent != null) {
		MimeBodyPart plainPart = new MimeBodyPart();
		plainPart.setContent(plainContent, "text/plain");
		MimeBodyPart htmlPart = new MimeBodyPart();
		htmlPart.setContent(htmlContent, "text/html");
		Multipart container = new MimeMultipart("alternative");
		container.addBodyPart(plainPart);
		container.addBodyPart(htmlPart);
                
                //BodyPart messageBodyPart = new MimeBodyPart();
                  for(int i=0;i<files.size();i++){
                    String locName          = files.get(i);
                    String fileName                 = "/home/midware/pdf/test/"+locName+".pdf";
                    MimeBodyPart messageBodyPart = new MimeBodyPart();
    DataSource source = new FileDataSource(fileName);
    messageBodyPart.setDataHandler(new DataHandler(source));
    messageBodyPart.setFileName(source.getName());
    container.addBodyPart(messageBodyPart);
               
                  }
               
		msg.setContent(container);
	    } else {
               MimeBodyPart plainPart = new MimeBodyPart();
		plainPart.setContent(plainContent, "text/plain");
                Multipart container = new MimeMultipart("alternative");
		container.addBodyPart(plainPart);
		
               // BodyPart messageBodyPart = new MimeBodyPart();
               for(int i=0;i<files.size();i++){
                    String locName          = files.get(i);
                    String fileName                 = "/home/midware/pdf/test/"+locName+".pdf";
                    MimeBodyPart messageBodyPart = new MimeBodyPart();
    DataSource source = new FileDataSource(fileName);
    messageBodyPart.setDataHandler(new DataHandler(source));
    messageBodyPart.setFileName(source.getName());
    container.addBodyPart(messageBodyPart);
                
               }
		msg.setContent(container);
	    }
	    msg.setSentDate(new Date());
	    transport.send(msg);
	} catch (MessagingException me) {
	    throw new MailException((Throwable)me);
	}
    }

    /**
     * Performs string substitution on a named "field" in a template.
     * <em>Note: any backslashes preceding a dollar sign in the
     * <code>value</code> parameter are removed.</em>
     * @param fieldName a "field name" to search for in the template;
     * this is a string of alphabetical characters enclosed in
     * double-'@' signs, for example "@@NAME@@".
     * @param value the value with which to replace all occurrences of
     * the named field in the template.
     */
    public void setField (String fieldName, String value) throws MailException {
 
        if (! fieldName.matches("^[a-zA-Z]\\w*")) {
	    throw new MailException("Malformed field name for template replacement.");
	}

	/* remove any backslashes before a dollar sign, then replace
	 * the whole shebang with a double backslash followed by an
	 * escaped dollar sign, which is eventually interpreted as a
	 * dollar sign.  so the end result is to leave the original
	 * pattern unmolested *EXCEPT* that backslashes before a
	 * dollar sign are removed. */
	String replaceMe = "(\\\\)*\\$";
      	String replaceWith = "\\\\\\$";
      	value = value.replaceAll(replaceMe, replaceWith);
    	plainContent = plainContent.replaceAll("@@"+fieldName+"@@", value);
      	htmlContent = htmlContent.replaceAll("@@"+fieldName+"@@", value);
       
    }
    
    class MessageAuthenticator extends Authenticator {
        String user;
        String pass;
        
        public MessageAuthenticator(String username, String password) {
            user = username;
            pass = password;
        }
        
        public PasswordAuthentication getPasswordAuthentication() {
            return new PasswordAuthentication(user, pass);
        }
    }

    

    /* stuff from listeners goes below here */
    public void messageDelivered (TransportEvent ev) { }
    public void messageNotDelivered (TransportEvent ev) { }
    public void messagePartiallyDelivered (TransportEvent ev) { }

    public void opened (ConnectionEvent ev) { }
    public void disconnected (ConnectionEvent ev) { }
    public void closed (ConnectionEvent ev) { }
}
