/**
 * ErrorMailer.java
 *
 * @author Alex Astle
 * @version $Id: ErrorMailer.java,v 1.10 2016/01/27 13:20:58 sravindran Exp $
 */
package net.terakeet.soapware;

// package imports
import net.terakeet.util.MidwareLogger;
import net.terakeet.util.TimedError;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.HashMap;
import java.util.Date;
import net.terakeet.util.TemplatedMessage;

/**
 * Maintains a static synchronized queue of errors and sends emails
 * when an error is queued.
 */
class ErrorMailer extends Thread {
    public static String EMAIL_TEMPLATE = "middlewareError";
    public static String ERROR_SENDER = "tech@beerboard.com";
    public static long RANDOM_SLEEP = 1000;

    public static long SLEEP_TIME = 9500;
    public static int NO_SPAM_SECONDS = 60;
    public static int SEND_LIMIT = 10;
    
    protected static MidwareLogger logger = new MidwareLogger(ErrorMailer.class.getName());

    protected static ConcurrentLinkedQueue<TimedError> errorQueue =
            new ConcurrentLinkedQueue<TimedError>();

    protected static HashMap<TimedError, Date> recentErrors =
            new HashMap<TimedError, Date>();
    
    public ErrorMailer() {
    }

    /**
     * Adds an error to the queue.
     */
    public static void addError(String error) {
        if (null != error) {
            errorQueue.add(new TimedError(error));
            logger.debug("Error queued for mailing.");
        }
    }
    
    /**
     * Check the queue for error messages and process them.
     */
    public void run () {
        String setting = null;
        long sleepTime = SLEEP_TIME;
        long ignoreMillis = NO_SPAM_SECONDS * 1000;
        int limit = SEND_LIMIT;
        try {
            setting = HandlerUtils.getSetting("errorMailer.sleepTime");
            if (null != setting && setting.length() > 0) {
                sleepTime = Long.valueOf(setting);
            }
            setting = HandlerUtils.getSetting("errorMailer.noSpamSeconds");
            if (null != setting && setting.length() > 0) {
                ignoreMillis = Long.valueOf(setting) * 1000;
            }
            setting = HandlerUtils.getSetting("errorMailer.sendLimit");
            if (null != setting && setting.length() > 0) {
                limit = Integer.valueOf(setting);
            }
        } catch (Exception e) {
            logger.midwareError("Invalid error emailer settings.");
        }
        String server = HandlerUtils.getSetting("server.name");
        if (server == null){
            server = "";
        }
        String emailTemplatePath = HandlerUtils.getSetting("email.templatePath");
        if ((emailTemplatePath == null) || "".equals(emailTemplatePath)) {
            emailTemplatePath = ".";
        }
        String recipientStr = HandlerUtils.getSetting("errorMailer.recipients");
        String[] recipients = null;
        if (recipientStr != null || recipientStr.trim().length() > 0) {
            recipients = recipientStr.trim().split("\\s*,\\s*");
        }
        if (recipients == null) {
            logger.generalWarning("No error email recipients");
        }
        
	while (recipients != null) {
            try {
                sleep(sleepTime + ((long)Math.rint(RANDOM_SLEEP)));
            } catch (InterruptedException ignored) {
                //ignore
            }
            int sent = 0;
            while (sent <= limit && !errorQueue.isEmpty()) {
                TimedError err = errorQueue.poll();
                // send an email unless this error has been seen recently
                if (!recentErrors.containsKey(err)) {
                    try {
                        TemplatedMessage errorEmail = new TemplatedMessage("[" +
                                server + "] Middleware Error", 
                                emailTemplatePath, EMAIL_TEMPLATE);
                        errorEmail.setSender(ERROR_SENDER);
                        for (String email : recipients) {
                            errorEmail.setRecipient(email);
                            errorEmail.setField("SERVER", server);
                            errorEmail.setField("TIME",
                                    err.getErrorTime().toString());
                            errorEmail.setField("ERROR", err.getErrorMessage());
                        }
                        errorEmail.send();
                    } catch (Exception e) {
                        // Debug email errors
                        logger.debug("Email failed: " + e.toString());
                    }
                    sent++;
                }
                recentErrors.put(err, err.getErrorTime());
            }
            if (sent > 0) {
                logger.debug(String.valueOf(sent) + " error email(s) sent.");
            }
            Date now = new Date();
            for (TimedError err : recentErrors.keySet()) {
                if (now.getTime() > err.getErrorTime().getTime() + ignoreMillis) {
                    recentErrors.remove(err);
                }
            }
        }
    }
}
