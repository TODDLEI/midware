/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package net.terakeet.util;

/**
 *
 * @author Sundar
 */

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.URL;
import java.net.HttpURLConnection;
import java.util.*;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.HttpResponse;
import org.apache.http.entity.StringEntity;
import org.apache.commons.codec.binary.Base64;
import org.apache.http.impl.client.HttpClientBuilder;
import java.nio.charset.Charset;


public class QuadGraphics {
    //private MidwareLogger logger;
    
    public void postMenu(String menu, MidwareLogger logger) {
       // logger                              = new MidwareLogger(QuadGraphics.class.getName());
        
        try {
            /*String prodUrl                  = "https://CS_BEVNET_SRV_:g75Psn4JQ@www.qconnect.com/bwwbevnet/api/Post/";

            String smsUrl                   = prodUrl;
            logger.debug("Step 1");
            
            URL url                         = new URL(smsUrl);
            HttpURLConnection conn          = (HttpURLConnection) url.openConnection();
            conn.setDoOutput(true);
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Content-Type", "application/json");
            conn.connect();
            logger.debug("Connection log: " + conn.toString());
            logger.debug("Step 2");
            
            String input                    = "";

            OutputStream os                 = conn.getOutputStream();
            os.write(input.getBytes());
            os.flush();
            logger.debug("Step 3");

            BufferedReader serverResponse   = new BufferedReader(new InputStreamReader(conn.getInputStream()));
            logger.debug(serverResponse.toString());
            String line;
            StringBuffer response           = new StringBuffer(); 
            while((line = serverResponse.readLine()) != null) {
                response.append(line);
                response.append('\r');
            }
            logger.debug("Step 4");

            serverResponse.close();*/
            
            //logger.debug("Http Post");
            String auth                     =new StringBuffer("CS_BEVNET_SRV_").append(":").append("g75Psn4JQ").toString();
            byte[] encodedAuth              = Base64.encodeBase64(auth.getBytes(Charset.forName("US-ASCII")));
            String authHeader               = "Basic " + new String(encodedAuth);
             //logger.debug("ecncoded value is " + new String(encodedAuth));
            HttpPost post                   = new HttpPost("https://www.qconnect.com/bwwbevnet/api/Post/");
            post.addHeader("Authorization", authHeader);
            post.addHeader("User-Agent","Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/45.0.2454.101 Safari/537.36");
            post.addHeader("content-type", "application/json; charset=UTF-8");
            post.addHeader("Accept", "/");
            post.addHeader("Accept-Language","en-US,en;q=0.8,ta;q=0.6");
            post.addHeader("Accept-Encoding","gzip, deflate");
            post.addHeader("Accept-Charset","ISO-8859-1,UTF-8;");

            StringEntity params             = new StringEntity(menu, "UTF-8");
	      
            post.setEntity(params);        
             //logger.debug("HttpClient Initialize" );
             
            HttpClient client               = HttpClientBuilder.create().build();
             //logger.debug("HttpClient Started" );
            HttpResponse response           = client.execute(post);
            //logger.debug("HttpClient Execute..." );
	      
	      int code                      = response.getStatusLine().getStatusCode();
	      //logger.debug("Response Code:"+code);
	      //System.out.println(response.getEntity().toString());
	      BufferedReader rd = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
	      String line = "";
              StringBuffer sb               = new StringBuffer();
	      while ((line = rd.readLine()) != null) {	        
                sb.append(line);
	      }
	      
            logger.debug("Response Code:"+code +" Reponse: " + sb.toString());
          } catch (Exception e) {
              logger.debug(e.getMessage());
              e.printStackTrace();
        }
    }
    
         
}
