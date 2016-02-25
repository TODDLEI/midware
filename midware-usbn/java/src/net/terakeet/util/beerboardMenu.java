/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package net.terakeet.util;


import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.*;
import java.text.BreakIterator;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Scanner;
import net.terakeet.soapware.DatabaseConnectionManager;
import net.terakeet.soapware.HandlerUtils;
import net.terakeet.soapware.RegisteredConnection;
import net.terakeet.soapware.handlers.BeerBoardHandler;
import net.terakeet.soapware.*;
import org.dom4j.Element;
import org.json.JSONObject;

/**
 *
 * @author suba
 */
public class beerboardMenu {
    
    private void close(Statement s) {
        if (s != null) {
            try { s.close(); } catch (SQLException sqle) { }
        }
    }
    private void close(ResultSet rs) {
        if (rs != null) {
            try { rs.close(); } catch (SQLException sqle) { }
        }
    }
    
    public void getBeerBoardMenuData(int location, int bar,Element toAppend, MidwareLogger logger, RegisteredConnection transconn) throws HandlerException {

         PreparedStatement stmt             = null;
         ResultSet rs                       = null;
         ArrayList<String> assetUrl         = new ArrayList<String>();
         
         try {
             String selectTemplate          ="SELECT DISTINCT bM.id,tile,lM.template,bM.template FROM  locationBeerBoardMap lM LEFT JOIN bbtvUserMac bM ON bM.location= lM.location WHERE bM.id IS NOT NULL AND  lM.location=?;";
             String selectDraftBeer         = "SELECT bS.majorStyle, pS.name, IF(cBN.name IS NULL, p.name, cBN.name), IF(pD.abv = 0.0, '', CONCAT(pD.abv,'%')),pD.origin,pD.ibu,pD.calorie, l.local, l.advertise, " +
                                            " (SELECT file FROM kegIcon WHERE (l.qtyOnHand*100) BETWEEN start AND end),  p.id, pD.category,  CONCAT(sL.logo,'.png'), "
                                            + " CONCAT(bL.logo, IF(bL.type = 0, '.png', IF(bL.type = 1, '.jpg', '.gif'))),pS1.addrCity FROM line l LEFT JOIN bar b ON b.id = l.bar LEFT JOIN product p ON p.id = l.product " +
                                            " LEFT JOIN productDescription pD ON pD.product = p.id " +
                                            " LEFT JOIN brewStyleMap bM ON bM.product = l.product LEFT JOIN productSet pS ON pS.id = bM.style LEFT JOIN productSet pS1 ON pS1.id = bM.brewery " +
                                            " LEFT JOIN beerStylesMap bSM ON bSM.productSet = bM.style LEFT JOIN beerStyles bS ON bS.id =  bSM.style " +                                            
                                            " LEFT JOIN customBeerName cBN ON cBN.product = p.id AND cBN.location = b.location "
                                            + " LEFT JOIN breweryLogo bL ON bL.productSet = bM.brewery  LEFT JOIN styleLogo sL ON sL.productSet = bM.style WHERE l.status = 'RUNNING' " +
                                            " AND l.advertise = 0 AND l.cask = 0 AND p.id NOT IN(4311,10661) AND b.location = ? GROUP BY l.product " +
                                            " ORDER BY p.name;";             
             String selectBottleBeer        = "SELECT  DISTINCT  bS.majorStyle, pS.name,IF(cBN.name IS NULL, p.name, cBN.name), IF(pD.abv = 0.0, '', CONCAT(pD.abv,'%')),pD.origin,pD.ibu,pD.calorie,pD.category, "
                                            + " price, pS1.addrCity,p.id FROM bottleBeer b LEFT JOIN product p ON p.id=b.product"
                                            + " LEFT JOIN productDescription pD ON pD.product = p.id "
                                            + "  LEFT JOIN brewStyleMap bM ON bM.product = b.product LEFT JOIN productSet pS ON pS.id = bM.style LEFT JOIN productSet pS1 ON pS1.id = bM.style "
                                            + " LEFT JOIN beerStylesMap bSM ON bSM.productSet = bM.style LEFT JOIN beerStyles bS ON bS.id =  bSM.style "                                            
                                            + " LEFT JOIN customBeerName cBN ON cBN.product = p.id AND cBN.location = b.location  WHERE b.location= ? AND p.id NOT IN(4311,10661) ORDER BY p.name;";
            
             String selectPrices            = "SELECT i.product, s.name, CONCAT('$', iP.value), s.showOnTV FROM inventoryPrices iP LEFT JOIN inventory i " +
                                            " ON i.id = iP.inventory LEFT JOIN beverageSize s ON s.id = iP.size WHERE i.location = ? AND s.id <> 'null' " +
                                            " GROUP BY i.product ORDER BY i.product, iP.value;";
             
             String selectPromotions        = "SELECT  type, sequence, file, location FROM locationPromotions WHERE location IN (?) ORDER BY sequence";
             String selectSocialMedia       = "SELECT DISTINCT bMU.avatar, CONCAT(bMU.username, ' LIKES ', p.name) msg,2, DATE(bMUR.date) AS d,  bMU.fbID FROM bbtvMobileUserRating bMUR " +
                                            " LEFT JOIN bbtvMobileUser bMU ON bMU.id = bMUR.user LEFT JOIN product p ON p.id = bMUR.product " +
                                            " WHERE bMUR.location = ? AND  bMU.username <> 'null' AND DateDiff(now(),bMUR.date) < 2 " +
                                            " UNION " +
                                            " SELECT DISTINCT bMU.avatar, CONCAT(bMU.username, ' Checked-In @ ', l.boardName) msg, 1,DATE(bMUC.date) AS d, bMU.fbID FROM bbtvMobileUserCheckin bMUC " +
                                            " LEFT JOIN bbtvMobileUser bMU ON bMU.id = bMUC.user LEFT JOIN location l ON l.id = bMUC.location " +
                                            " WHERE bMUC.location = ? AND  bMU.username <> 'null' AND DateDiff(now(),bMUC.date) < 2 " +
                                            " ORDER BY d DESC LIMIT 3;";
             String selectSpecials          = "SELECT  specials, sequence FROM locationSpecials WHERE location = ? ORDER BY sequence  ";
             String selectSponsors          = "SELECT  bSC.customer, bSCr.file, bSCr.type ,now(),bSCr.validity FROM bevSyncCampaign bSC " +
                                            " LEFT JOIN bevSyncCampaignLocations bSCL ON bSCL.campaign = bSC.id " +
                                            " LEFT JOIN bevSyncCampaignCreatives bSCC ON bSCC.campaign = bSC.id LEFT JOIN bevSyncCreatives bSCr ON bSCr.id = bSCC.creatives " +
                                            " WHERE bSCL.location = ? AND NOW() BETWEEN bSC.start AND bSC.end AND NOW() < bSCr.validity AND bSCr.type IN (1, 2);";   
             String selectLocationSchedule  = "SELECT sequence, type, startDate, endDate FROM bbtvSchedule WHERE location = ? ORDER BY sequence";             
             String selectNewBeer           = "SELECT DISTINCT bS.majorStyle, pS.name, IF(cBN.name IS NULL, p.name, cBN.name), IF(pD.abv = 0.0, '', CONCAT(pD.abv,'%')),pD.origin,pD.ibu,pD.calorie,  " +
                                            "   p.id, pD.category,  CONCAT(sL.logo,'.png'), CONCAT(bL.logo, IF(bL.type = 0, '.png', IF(bL.type = 1, '.jpg', '.gif'))), pS1.addrCity "
                                            + " FROM  lineUpdates lU LEFT JOIN line l ON l.id = lU.line  LEFT JOIN product p ON p.id = l.product " +
                                            " LEFT JOIN productDescription pD ON pD.product = p.id " +
                                            " LEFT JOIN brewStyleMap bM ON bM.product = l.product LEFT JOIN productSet pS ON pS.id = bM.style LEFT JOIN productSet pS1 ON pS1.id = bM.style " +
                                            " LEFT JOIN beerStylesMap bSM ON bSM.productSet = bM.style LEFT JOIN beerStyles bS ON bS.id =  bSM.style " +                                            
                                            " LEFT JOIN customBeerName cBN ON cBN.product = p.id AND cBN.location = lU.location "
                                            + " LEFT JOIN breweryLogo bL ON bL.productSet = bM.brewery  LEFT JOIN styleLogo sL ON sL.productSet = bM.style WHERE lU.location = ? " +
                                            "  AND p.id NOT IN(4311,10661) AND lU.date > SUBDATE(NOW(), INTERVAL 7 DAY) AND l.status = 'RUNNING' ORDER BY lU.date DESC, lU.id DESC LIMIT 2;";
             String selectOnDeck            = "SELECT DISTINCT bS.majorStyle, pS.name, IF(cBN.name IS NULL, p.name, cBN.name), IF(pD.abv = 0.0, '', CONCAT(pD.abv,'%')),pD.origin,pD.ibu,pD.calorie,  " +
                                            "   p.id, pD.category,  CONCAT(sL.logo,'.png'), CONCAT(bL.logo, IF(bL.type = 0, '.png', IF(bL.type = 1, '.jpg', '.gif'))) , pS1.addrCity FROM  comingSoonProducts c  LEFT JOIN product p ON p.id = c.product " +
                                            " LEFT JOIN productDescription pD ON pD.product = p.id " +
                                            " LEFT JOIN brewStyleMap bM ON bM.product = c.product LEFT JOIN productSet pS ON pS.id = bM.style LEFT JOIN productSet pS1 ON pS1.id = bM.style  " +
                                            " LEFT JOIN beerStylesMap bSM ON bSM.productSet = bM.style LEFT JOIN beerStyles bS ON bS.id =  bSM.style " +                                            
                                            " LEFT JOIN customBeerName cBN ON cBN.product = p.id AND cBN.location = c.location "
                                            + " LEFT JOIN breweryLogo bL ON bL.productSet = bM.brewery  LEFT JOIN styleLogo sL ON sL.productSet = bM.style WHERE c.location = ? " +
                                            "  AND p.id NOT IN(4311,10661) ORDER BY p.name;";
             
            HashMap<Integer, String> productPrices
                                            = new HashMap<Integer, String>();
            stmt                            = transconn.prepareStatement(selectPrices);
            stmt.setInt(1, location);
            rs                              = stmt.executeQuery();
            while (rs.next()) {
                int productId               = rs.getInt(1);
                int showOnTv                = rs.getInt(4);
                String price                = "";
                if(showOnTv > 0){
                    price                   = HandlerUtils.nullToEmpty(rs.getString(2)) + " - " + HandlerUtils.nullToEmpty(rs.getString(3));
                } else {
                    price                   = HandlerUtils.nullToEmpty(rs.getString(3));
                }
                productPrices.put(productId, price);
            }
            String selectlogo               = "Select logo FROM locationGraphics WHERE location = ?";
            String  logo                    = "USBNLogo.png";
            stmt                            = transconn.prepareStatement(selectlogo);
            stmt.setInt(1, location);
            rs                              = stmt.executeQuery();
            if (rs.next()) {
               logo                         = HandlerUtils.nullToString( rs.getString(1),logo).replaceAll(" ", "%20");
               assetUrl.add("http://bevmanager.net/Images/Location_logo/" + HandlerUtils.nullToString( rs.getString(1),logo).replaceAll(" ", "%20"));
            }
            toAppend.addElement("locationLogo").addText(logo);
            
            Element draftsEl                = toAppend.addElement("draftBeers");
            boolean isResponseObj           = false;
            stmt                            = transconn.prepareStatement(selectDraftBeer);
            stmt.setInt(1, location);
            rs                              = stmt.executeQuery();
            while (rs.next()) {
                isResponseObj               = true;
                String majorStyle           = HandlerUtils.nullToEmpty(rs.getString(1));
                String style                = HandlerUtils.nullToEmpty(rs.getString(2));
                String name                 = HandlerUtils.nullToEmpty(rs.getString(3));
                String abv                  = HandlerUtils.nullToEmpty(rs.getString(4));
                String origin               = HandlerUtils.nullToEmpty(rs.getString(5));                 
                int ibu                     = rs.getInt(6);                 
                int calorie                 = rs.getInt(7);                 
                int local                   = rs.getInt(8);                 
                int advertise               = rs.getInt(9);    
                String kegname              = HandlerUtils.nullToString(rs.getString(10),"keg.png");
                int product                 = rs.getInt(11);                                  
                int category                = rs.getInt(12);    
                String glass                = HandlerUtils.nullToEmpty(rs.getString(13));
                String brew                 = HandlerUtils.nullToEmpty(rs.getString(14));
                String city                 = HandlerUtils.nullToEmpty(rs.getString(15));
                String glassUrl             = "http://beerboard.tv/USBN.BeerBoard.UI/Images/glass/" + HandlerUtils.nullToEmpty(glass);
                String brewUrl              = "http://beerboard.tv/USBN.BeerBoard.UI/Images/logo/" + HandlerUtils.nullToEmpty(brew);
                assetUrl.add(glassUrl);
                assetUrl.add(brewUrl);
                String price                = HandlerUtils.nullToEmpty(productPrices.get(product));

                Element beerMenusEl         = draftsEl.addElement("draftBeer");
                beerMenusEl.addElement("majorStyle").addText(majorStyle);
                beerMenusEl.addElement("style").addText(style);
                beerMenusEl.addElement("name").addText(name);
                beerMenusEl.addElement("abv").addText(abv);
                beerMenusEl.addElement("origin").addText(origin);
                beerMenusEl.addElement("ibu").addText(String.valueOf(ibu));
                beerMenusEl.addElement("calorie").addText(String.valueOf(calorie));
                beerMenusEl.addElement("local").addText(String.valueOf(local));
                beerMenusEl.addElement("advertise").addText(String.valueOf(advertise));
                beerMenusEl.addElement("keg").addText(kegname);
                beerMenusEl.addElement("price").addText(price);
                beerMenusEl.addElement("category").addText(String.valueOf(category));
                beerMenusEl.addElement("brewLogo").addText(brew);
                beerMenusEl.addElement("glassLogo").addText(glass);
                beerMenusEl.addElement("city").addText(city);
                beerMenusEl.addElement("productId").addText(String.valueOf(product));
            }
            
            Element newBeersEl              = toAppend.addElement("newBeers");
            isResponseObj                   = false;
            stmt                            = transconn.prepareStatement(selectNewBeer);
            stmt.setInt(1, location);
            rs                              = stmt.executeQuery();
            while (rs.next()) {
                isResponseObj               =true;
                String majorStyle           = HandlerUtils.nullToEmpty(rs.getString(1));
                String style                = HandlerUtils.nullToEmpty(rs.getString(2));
                String name                 = HandlerUtils.nullToEmpty(rs.getString(3));
                String abv                  = HandlerUtils.nullToEmpty(rs.getString(4));
                String origin               = HandlerUtils.nullToEmpty(rs.getString(5));                 
                int ibu                     = rs.getInt(6);                 
                int calorie                 = rs.getInt(7);                                 
                int product                 = rs.getInt(8);                                  
                int category                = rs.getInt(9);    
                String glass                = HandlerUtils.nullToEmpty(rs.getString(10));
                String brew                 = HandlerUtils.nullToEmpty(rs.getString(11));
                String city                 = HandlerUtils.nullToEmpty(rs.getString(12));
                String glassUrl             = "http://beerboard.tv/USBN.BeerBoard.UI/Images/glass/" + HandlerUtils.nullToEmpty(glass);
                String brewUrl              = "http://beerboard.tv/USBN.BeerBoard.UI/Images/logo/" + HandlerUtils.nullToEmpty(brew);
                assetUrl.add(glassUrl);
                assetUrl.add(brewUrl);                

                Element beerMenusEl         = newBeersEl.addElement("newBeer");
                beerMenusEl.addElement("majorStyle").addText(majorStyle);
                beerMenusEl.addElement("style").addText(style);
                beerMenusEl.addElement("name").addText(name);
                beerMenusEl.addElement("abv").addText(abv);
                beerMenusEl.addElement("origin").addText(origin);
                beerMenusEl.addElement("ibu").addText(String.valueOf(ibu));
                beerMenusEl.addElement("calorie").addText(String.valueOf(calorie));                                
                beerMenusEl.addElement("category").addText(String.valueOf(category));
                beerMenusEl.addElement("brewLogo").addText(brew);
                beerMenusEl.addElement("glassLogo").addText(glass);
                beerMenusEl.addElement("city").addText(city);
                beerMenusEl.addElement("productId").addText(String.valueOf(product));
            }
            
            Element onDeckEl                = toAppend.addElement("onDeck");
            isResponseObj                   = false;
            stmt                            = transconn.prepareStatement(selectOnDeck);
            stmt.setInt(1, location);
            rs                              = stmt.executeQuery();
            while (rs.next()) {
                isResponseObj               = true;
                String majorStyle           = HandlerUtils.nullToEmpty(rs.getString(1));
                String style                = HandlerUtils.nullToEmpty(rs.getString(2));
                String name                 = HandlerUtils.nullToEmpty(rs.getString(3));
                String abv                  = HandlerUtils.nullToEmpty(rs.getString(4));
                String origin               = HandlerUtils.nullToEmpty(rs.getString(5));                 
                int ibu                     = rs.getInt(6);                 
                int calorie                 = rs.getInt(7);                                 
                int product                 = rs.getInt(8);                                  
                int category                = rs.getInt(9);    
                String glass                = HandlerUtils.nullToEmpty(rs.getString(10));
                String brew                 = HandlerUtils.nullToEmpty(rs.getString(11));
                String city                 = HandlerUtils.nullToEmpty(rs.getString(12));
                String glassUrl             = "http://beerboard.tv/USBN.BeerBoard.UI/Images/glass/" + HandlerUtils.nullToEmpty(glass);
                String brewUrl              = "http://beerboard.tv/USBN.BeerBoard.UI/Images/logo/" + HandlerUtils.nullToEmpty(brew);
                assetUrl.add(glassUrl);
                assetUrl.add(brewUrl);

                Element beerMenusEl         = onDeckEl.addElement("product");
                beerMenusEl.addElement("majorStyle").addText(majorStyle);
                beerMenusEl.addElement("style").addText(style);
                beerMenusEl.addElement("name").addText(name);
                beerMenusEl.addElement("abv").addText(abv);
                beerMenusEl.addElement("origin").addText(origin);
                beerMenusEl.addElement("ibu").addText(String.valueOf(ibu));
                beerMenusEl.addElement("calorie").addText(String.valueOf(calorie));                                
                beerMenusEl.addElement("category").addText(String.valueOf(category));
                beerMenusEl.addElement("brewLogo").addText(brew);
                beerMenusEl.addElement("glassLogo").addText(glass);
                beerMenusEl.addElement("city").addText(city);
                beerMenusEl.addElement("productId").addText(String.valueOf(product));
            }
            
            Element locationSpecialsEl      = toAppend.addElement("locationSpecials");
            isResponseObj                   = false; 
            stmt                            = transconn.prepareStatement(selectSpecials);
            stmt.setInt(1, location);
            rs                              = stmt.executeQuery();
            while (rs.next()) {
                isResponseObj               = true;
                 Element specialsEl         = locationSpecialsEl.addElement("locationSpecial");
                 specialsEl.addElement("specials").addText(rs.getString(1));
                 specialsEl.addElement("sequence").addText(String.valueOf(rs.getInt(2)));

            }
            
            Element locationPromotionsEl    = toAppend.addElement("locationPromotions");
            isResponseObj                   = false;  
            stmt                            = transconn.prepareStatement(selectPromotions);
            if(location==1082){
                stmt.setInt(1, 425);
            } else {
                stmt.setInt(1, location);
            }
            rs                              = stmt.executeQuery();
            while (rs.next()) {
                isResponseObj               = true;
                String creative             = rs.getString(3);                 
                String url                  = "http://beerboard.tv/USBN.BeerBoard.UI/Images/Customers/"+ String.valueOf(rs.getInt(4)) + "/" + creative.trim().replaceAll("\'", "%27").replaceAll(" ", "%20");
                assetUrl.add(url);
                Element promoEl             = locationPromotionsEl.addElement("locationPromotion");
                promoEl.addElement("creative").addText(creative);
                promoEl.addElement("type").addText(String.valueOf(rs.getInt(1)));
                promoEl.addElement("sequence").addText(String.valueOf(rs.getInt(2)));
            }
            
            Element locationSponsorsEl      = toAppend.addElement("locationSponsors");
            isResponseObj                   = false;  
            stmt                            = transconn.prepareStatement(selectSponsors);
            stmt.setInt(1, location);
            rs                              = stmt.executeQuery();
            while (rs.next()) {
                isResponseObj               = true;
                int customer                = rs.getInt(1);
                String creative             = rs.getString(3);
                String startTime            = rs.getString(4);
                String endTime              = rs.getString(5);
                assetUrl.add(HandlerUtils.nullToEmpty("http://beerboard.tv/USBN.BeerBoard.UI/bevSyncCustomers/" + customer + "/" + creative.trim().replaceAll("\'", "%27").replaceAll(" ", "%20")));                
                Element SponsorEl           = locationSponsorsEl.addElement("locationSponsor");
                SponsorEl.addElement("creative").addText(creative);
                SponsorEl.addElement("type").addText(String.valueOf(rs.getInt(2)));
                SponsorEl.addElement("startTime").addText(startTime);
                SponsorEl.addElement("endTime").addText(endTime);
            }
            
            Element bottleBeersEl           = toAppend.addElement("bottleBeers");
            isResponseObj                   = false;   
            stmt                            = transconn.prepareStatement(selectBottleBeer);
            stmt.setInt(1, location);
            rs                              = stmt.executeQuery();
            while (rs.next()) {
                isResponseObj               = true;
                String majorStyle           = HandlerUtils.nullToEmpty(rs.getString(1));
                String style                = HandlerUtils.nullToEmpty(rs.getString(2));
                String name                 = HandlerUtils.nullToEmpty(rs.getString(3));
                String abv                  = HandlerUtils.nullToEmpty(rs.getString(4));
                String origin               = HandlerUtils.nullToEmpty(rs.getString(5));                 
                int ibu                     = rs.getInt(6);                 
                int calorie                 = rs.getInt(7);                 
                int category                = rs.getInt(8);                                                                    
                String price                = HandlerUtils.nullToEmpty(rs.getString(9));
                String city                 = HandlerUtils.nullToEmpty(rs.getString(10));
                int product                 = rs.getInt(11);             

                Element beerMenusEl         = bottleBeersEl.addElement("bottleBeer");
                beerMenusEl.addElement("majorStyle").addText(majorStyle);
                beerMenusEl.addElement("style").addText(style);
                beerMenusEl.addElement("name").addText(name);
                beerMenusEl.addElement("abv").addText(abv);
                beerMenusEl.addElement("origin").addText(origin);
                beerMenusEl.addElement("ibu").addText(String.valueOf(ibu));
                beerMenusEl.addElement("calorie").addText(String.valueOf(calorie));                 
                beerMenusEl.addElement("price").addText(price);
                beerMenusEl.addElement("category").addText(String.valueOf(category));
                beerMenusEl.addElement("city").addText(city);
                beerMenusEl.addElement("productId").addText(String.valueOf(product));
            }
            
            Element socialConnectionsEl     = toAppend.addElement("SocialConnections");
            stmt                            = transconn.prepareStatement(selectSocialMedia);
            stmt.setInt(1, location);
            stmt.setInt(2, location);
            rs                              = stmt.executeQuery();
            if(rs.next()) {
                String avatar               = rs.getString(1);
                String fbId                 = rs.getString(5);
                //logger.debug("FbID:"+fbId);
                if(fbId!=null && !fbId.equals("") && !fbId.equals("null")) {
                        avatar              = getAvatar(fbId);
                        avatar              = avatar.replace("https", "http");
                }
                if (!httpFileExists(avatar)) {
                   avatar                   = "http://beerboard.tv/USBN.BeerBoard.UI/Images/Assets/Generic/profile-pic.png";
                }

                String[] avatarArray        = avatar.split("/");
                int arrayLength             = avatarArray.length;
                String[] avatarArray2       = avatarArray[avatarArray.length-1].split("\\?");
                String avatarFileName       = avatarArray[arrayLength - 1];
                if(avatar.contains("?")){
                    avatarFileName          = avatarArray2[0];
                }
                
                Element locationSMEl        = socialConnectionsEl.addElement("socialMedia");                
                locationSMEl.addElement("type").addText(String.valueOf(rs.getInt(3)));
                locationSMEl.addElement("avatar").addText(avatarFileName);
                locationSMEl.addElement("post").addText(HandlerUtils.nullToEmpty(rs.getString(2)));                
                assetUrl.add(avatar);
            } 
            
            Element locationSequenceEl      = toAppend.addElement("locationSequence");
            isResponseObj                   = false;   
            String open                     = "00:00:00";
            String close                    = "00:00:00";
            stmt                            = transconn.prepareStatement(selectLocationSchedule);
            stmt.setInt(1, location);
            rs                              = stmt.executeQuery();
            while (rs.next()) {
                isResponseObj               = true;
                Element bbtvScheduleEl      = locationSequenceEl.addElement("sequence");
                bbtvScheduleEl.addElement("sequence").addText(HandlerUtils.nullToEmpty(rs.getString(1)));
                bbtvScheduleEl.addElement("type").addText(HandlerUtils.nullToEmpty(rs.getString(2)));
                bbtvScheduleEl.addElement("startTime").addText(HandlerUtils.nullToEmpty(open));
                bbtvScheduleEl.addElement("endTime").addText(HandlerUtils.nullToEmpty(close));
            }
            
            if (!isResponseObj) {
                stmt                        = transconn.prepareStatement(selectLocationSchedule);
                stmt.setInt(1, 0);
                rs                          = stmt.executeQuery();
                while (rs.next()) {
                    isResponseObj           = true;
                    Element bbtvScheduleEl  = locationSequenceEl.addElement("sequence");
                    bbtvScheduleEl.addElement("sequence").addText(HandlerUtils.nullToEmpty(rs.getString(1)));
                    bbtvScheduleEl.addElement("type").addText(HandlerUtils.nullToEmpty(rs.getString(2)));
                    bbtvScheduleEl.addElement("startTime").addText(HandlerUtils.nullToEmpty(open));
                    bbtvScheduleEl.addElement("endTime").addText(HandlerUtils.nullToEmpty(close));
                }
            }
            
            assetUrl.add("http://beerboard.tv/USBN.BeerBoard.UI/Images/Assets/Generic/FacebookBar.png");
            assetUrl.add("http://beerboard.tv/USBN.BeerBoard.UI/Images/Assets/Generic/BeerBoardSocialMedia.png");
            assetUrl.add("http://beerboard.tv/USBN.BeerBoard.UI/Images/Assets/Generic/BeerBoardSocialMediaWithAd.jpg"); 
            assetUrl.add("http://beerboard.tv/USBN.BeerBoard.UI/Images/Assets/facebook.png");
            assetUrl.add("http://beerboard.tv/USBN.BeerBoard.UI/Images/Assets/twitter.png");
            assetUrl.add("http://beerboard.tv/USBN.BeerBoard.UI/Images/Assets/instagram.png");
            assetUrl.add("http://beerboard.tv/USBN.BeerBoard.UI/Images/Assets/bbwhiteboarderlogo.png");
            assetUrl.add("http://beerboard.tv/USBN.BeerBoard.UI/Images/Assets/likeChalk.png");
            assetUrl.add("http://beerboard.tv/USBN.BeerBoard.UI/Images/Assets/checkinChalk.png");            
            assetUrl.add("http://beerboard.tv/USBN.BeerBoard.UI/Images/Assets/bernard-mt-condense.ttf");             
            assetUrl.add("http://beerboard.tv/USBN.BeerBoard.UI/Images/Assets/sline.png");
            assetUrl.add("http://beerboard.tv/USBN.BeerBoard.UI/Images/Assets/Generic/keg.png");
            assetUrl.add("http://beerboard.tv/USBN.BeerBoard.UI/Images/Assets/Generic/Keg0.png");
            assetUrl.add("http://beerboard.tv/USBN.BeerBoard.UI/Images/Assets/Generic/Keg05.png");
            assetUrl.add("http://beerboard.tv/USBN.BeerBoard.UI/Images/Assets/Generic/Keg10.png");
            assetUrl.add("http://beerboard.tv/USBN.BeerBoard.UI/Images/Assets/Generic/Keg15.png");
            assetUrl.add("http://beerboard.tv/USBN.BeerBoard.UI/Images/Assets/Generic/Keg20.png");
            assetUrl.add("http://beerboard.tv/USBN.BeerBoard.UI/Images/Assets/Generic/Keg25.png");
            assetUrl.add("http://beerboard.tv/USBN.BeerBoard.UI/Images/Assets/Generic/Keg30.png");                        
            assetUrl.add("http://beerboard.tv/USBN.BeerBoard.UI/Images/Assets/Generic/Keg35.png");                        
            assetUrl.add("http://beerboard.tv/USBN.BeerBoard.UI/Images/Assets/Generic/Keg40.png");                        
            assetUrl.add("http://beerboard.tv/USBN.BeerBoard.UI/Images/Assets/Generic/Keg45.png");                        
            assetUrl.add("http://beerboard.tv/USBN.BeerBoard.UI/Images/Assets/Generic/Keg50.png");
            assetUrl.add("http://beerboard.tv/USBN.BeerBoard.UI/Images/Assets/Generic/Keg55.png");                        
            assetUrl.add("http://beerboard.tv/USBN.BeerBoard.UI/Images/Assets/Generic/Keg60.png");
            assetUrl.add("http://beerboard.tv/USBN.BeerBoard.UI/Images/Assets/Generic/Keg65.png");                        
            assetUrl.add("http://beerboard.tv/USBN.BeerBoard.UI/Images/Assets/Generic/Keg70.png");
            assetUrl.add("http://beerboard.tv/USBN.BeerBoard.UI/Images/Assets/Generic/Keg75.png");                        
            assetUrl.add("http://beerboard.tv/USBN.BeerBoard.UI/Images/Assets/Generic/Keg80.png");
            assetUrl.add("http://beerboard.tv/USBN.BeerBoard.UI/Images/Assets/Generic/Keg85.png");                        
            assetUrl.add("http://beerboard.tv/USBN.BeerBoard.UI/Images/Assets/Generic/Keg90.png");
            assetUrl.add("http://beerboard.tv/USBN.BeerBoard.UI/Images/Assets/Generic/Keg95.png");                        
            assetUrl.add("http://beerboard.tv/USBN.BeerBoard.UI/Images/Assets/Generic/Keg100.png");
            assetUrl.add("http://beerboard.tv/USBN.BeerBoard.UI/Images/Assets/Generic/Keg125.png");                        
            assetUrl.add("http://beerboard.tv/USBN.BeerBoard.UI/Images/Assets/Generic/Keg150.png");                        
            assetUrl.add("http://beerboard.tv/USBN.BeerBoard.UI/Images/Assets/Generic/Keg175.png");
            assetUrl.add("http://beerboard.tv/USBN.BeerBoard.UI/Images/Assets/Generic/Keg200.png");                        
            assetUrl.add("http://beerboard.tv/USBN.BeerBoard.UI/Images/Assets/Generic/Keg225.png");
            assetUrl.add("http://beerboard.tv/USBN.BeerBoard.UI/Images/Assets/Generic/Keg250.png");                        
            assetUrl.add("http://beerboard.tv/USBN.BeerBoard.UI/Images/Assets/Generic/Keg275.png");
            assetUrl.add("http://beerboard.tv/USBN.BeerBoard.UI/Images/Assets/Generic/Keg300.png");                        
            assetUrl.add("http://beerboard.tv/USBN.BeerBoard.UI/Images/Assets/Generic/Keg325.png");
            assetUrl.add("http://beerboard.tv/USBN.BeerBoard.UI/Images/Assets/Generic/Keg350.png");                        
            assetUrl.add("http://beerboard.tv/USBN.BeerBoard.UI/Images/Assets/Generic/Keg375.png");
            assetUrl.add("http://beerboard.tv/USBN.BeerBoard.UI/Images/Assets/Generic/Keg400.png"); 
             
            Element assetEl                 = toAppend.addElement("assets"); 
            for (String urlStr : assetUrl) {
                assetEl.addElement("url").addText(HandlerUtils.nullToEmpty(urlStr));
            } 
           
        } catch (SQLException sqle) {
            logger.dbError("Database error: "+sqle.toString());
            throw new HandlerException(sqle);
        } catch (Exception e){
             logger.debug(e.getMessage());
        }finally {
             close(rs);
            close(stmt);
        }

     }
    
   private String getAvatar(String fbid ){
         
        String avatar                       = "http://beerboard.tv/USBN.BeerBoard.UI/Images/Assets/Generic/profile-pic.png";
        try {
            if(fbid!= null && fbid.length() > 4) {
                String data                 = getData(new URL("http://graph.facebook.com/"+fbid+"/picture?type=normal&redirect=false"));      
                //logger.debug("data:"+data);
               if(data!= null && data.length() >6) {
                    JSONObject jdata        = new JSONObject(data);
                    if(jdata.has("data")){
                        jdata               = (JSONObject)jdata.get("data");
                        avatar              = jdata.get("url").toString();	
	                }
               /* data                        = data.replaceAll("/", "");
                avatar                      = data.substring(data.indexOf("url")+6,data.indexOf(",")-1);
                avatar                      = avatar.replaceAll("\\\\","/");
                logger.debug("Avatar:"+avatar);*/
                    
               }
            }
        } catch(Exception e) {
            
        }
        return avatar;
    }
    
    public String getData(URL urL) {
          //logger.debug("fBID"+urL);
        String graph                        = null;
        try {
            String inputLine;            
            HttpURLConnection conn         = (HttpURLConnection) urL.openConnection();            
            BufferedReader in               = new BufferedReader(new InputStreamReader(conn.getInputStream()));           
            StringBuffer  b                 = new StringBuffer();           
            while ((inputLine = in.readLine()) != null){          
                b.append(inputLine + "\n");


            }            
            in.close();           
            graph                           = b.toString();
            //logger.debug(graph);
        }catch (Exception e) {
           
        }
        return graph;

    }
    public static boolean httpFileExists(String URLName){
            try {
              HttpURLConnection.setFollowRedirects(false);
              HttpURLConnection con         = (HttpURLConnection) new URL(URLName).openConnection();
              con.setRequestMethod("HEAD");
              return (con.getResponseCode() == HttpURLConnection.HTTP_OK);
            }
            catch (Exception e) {
               e.printStackTrace();
               return false;
            }
        }
    
    public void getBWWTileMenuTemplate(int location,int bar, int fireStick, Element toAppend, MidwareLogger logger, RegisteredConnection transconn) throws HandlerException {
            
         ArrayList<String> htmlMenus        = new ArrayList<String>();
         ArrayList<String> menus            = new ArrayList<String>();
         ArrayList<String> specials         = new ArrayList<String>();
         ArrayList<String> assetUrl         = new ArrayList<String>();
         ArrayList<String> promotion        = new ArrayList<String>();
         ArrayList<String> localBeer        = new ArrayList<String>();
         ArrayList<String> featuredBeer     = new ArrayList<String>();
         ArrayList<String> specialArray     = new ArrayList<String>();
         String locationLogo                = "";
         PreparedStatement stmt             = null;
         ResultSet rs                       = null;

         try {
             
             String selectBeverageDescription
                                            = "SELECT DISTINCT p.id, IF(l.cask>0 ,'Cask', IFNULL(bS.style, 'Cask')), p.name, pD.abv, sPS.name, pD.origin, " +
                                            " (SELECT name FROM customBeerName WHERE location =b.location AND product = p.id),( SELECT name FROM customStyleName " +
                                            " WHERE location = b.location AND productSet = sPS.id), l.local, l.advertise, IFNULL(pS.addrCity,''), IFNULL(pS.addrState,'')  " +
                                            " FROM line l LEFT JOIN bar b ON b.id = l.bar LEFT JOIN product p ON p.id = l.product " +
                                            " LEFT JOIN productDescription pD ON pD.product = p.id LEFT JOIN brewStyleMap bSM ON bSM.product = l.product " +
                                            " LEFT JOIN productSet sPS ON sPS.id = bSM.style LEFT JOIN beerStylesMap sM ON sM.productSet = sPS.id " +
                                            " LEFT JOIN productSetDescription pSD ON pSD.productSet= sPS.id LEFT JOIN beerStyles bS ON bS.id =  sM.style LEFT JOIN styleLogo sL ON sL.style = sPS.name " +
                                            " LEFT JOIN productSet pS ON pS.id=bSM.brewery WHERE l.status = 'RUNNING' AND p.id NOT IN(4311,10661) ";
             
             if(bar >0){
                 selectBeverageDescription  += " AND b.id = ? AND sPS.productSetType = 9 AND sPS.id<> 4796 " +
                                            " GROUP BY l.product ORDER BY  p.name; ";
             } else {
                 selectBeverageDescription  += " AND b.location = ? AND sPS.productSetType = 9 AND sPS.id<> 4796 " +
                                            " GROUP BY l.product ORDER BY   p.name; ";
             }
              String selectLocationSpecials  = "SELECT id, specials, sequence FROM locationSpecials WHERE location = ? ORDER BY sequence  "; 
             String selectlogo              = "SELECT logo FROM locationGraphics WHERE location = ?";             
             String selectPrices            = "SELECT i.product, s.name, CONCAT('$', iP.value) FROM inventoryPrices iP LEFT JOIN inventory i " +
                                            " ON i.id = iP.inventory LEFT JOIN beverageSize s ON s.id = iP.size WHERE i.location = ? AND s.id <> 'null' " +
                                            " GROUP BY i.product ORDER BY i.product, iP.value;";
             String selectLocationPromotions= "SELECT id, type, visibleTime, file, sequence,location FROM locationPromotions WHERE location IN (?) ORDER BY sequence";
             
            
             StringBuilder prodDesc         = new StringBuilder();             
             
             String headColor               ="#FFD200";
             String color                   = "#FFFFFF";
             int productCount               = 1;
             int specialCount               = 0;
             String boarder                 =" border=\"0\" ";
             
             stmt                           = transconn.prepareStatement(selectLocationSpecials);
             stmt.setInt(1,location);             
             rs                             = stmt.executeQuery();
             while(rs.next()){
                 
                 if(specialCount==0){
                     prodDesc               = new StringBuilder();
                     prodDesc.append("<table  width=\"100%\"  valign=\"top\" align=\"center\"  border=\"0\"><tr><td height=\"5px\"> </td> </tr>"
                                            + "<tr><td  valign=\"top\" class=\"beer_list_header1\"  align=\"center\">Specials <br style=\"line-height:'150%'; margin-top:5px;\"/>"
                                            + "<img src=\"/mnt/sdcard/Images/sline.png\"/></td></tr>"
                                            + "<tr><td  align=\"center\" class=\"special_list\" height=\"50px\" ></td></tr>"
                                            + "<tr><td  align=\"center\" class=\"special_list\" width=\"350px\" >"+rs.getString(2).replace("|", "<br/>")+"</td></tr> ");
                 } else {
                 prodDesc.append("<tr><td  align=\"center\" class=\"special_list\" height=\"50px\" ></td></tr>");  
                 prodDesc.append("<tr><td  align=\"center\" class=\"special_list\" width=\"350px\" >"+rs.getString(2).replace("|", "<br/>")+"</td></tr>");
                 }
                 specialCount++;
                 if(specialCount>5){
                      prodDesc.append("</table>");
                     specialArray.add(prodDesc.toString());
                     specialCount           =0;
                 }
             }
             
             if(specialCount>0){
                 prodDesc.append("</table>");
                 specialArray.add(prodDesc.toString());
             }
             
             
          
             
             stmt                           = transconn.prepareStatement(selectlogo);
             stmt.setInt(1,location);
             String logo                    = "USBNLogo.png";
             rs                             = stmt.executeQuery();
             if(rs.next()) {
                 logo                       = HandlerUtils.nullToString(rs.getString(1), logo).replaceAll(" ", "%20");
             }
            assetUrl.add("http://bevmanager.net/Images/Location_logo/"+logo.trim().replaceAll("\'", "%27").replaceAll(" ", "%20"));
             locationLogo                   ="<table  border=\"0\"><tr><td align=\"center\" valign=\"middle\">"
                                            + "<img style=\"  max-height:70px;  max-width:100px;\"   src=\"/mnt/sdcard/Images/"+logo+"\"/></td></tr></table>";
             String logoHeight              =(fireStick > 0 ? "80px;" : "130px;");
             String logoWidth               =(fireStick > 0 ? "200px;" : "230px;");
             String lineHeight              =(fireStick > 0 ? "80px" : "100px");
             String title                   ="<table width=\"100%\" cellpadding=\"0\" cellspacing=\"0\" border=\"0\"><tr>"
                                            + "<td width='20%' valign=\"bottom\"><img style=\"  max-height:"+logoHeight+"  max-width:"+logoWidth+"\"   src=\"/mnt/sdcard/Images/BWW_3C.png\"/></td><td></td>"
                                            + "<td width='5%' align='center' valign='middle'><img height=\""+lineHeight+"\" width=\"3px\"   src=\"/mnt/sdcard/Images/whitehline.png\"/></td><td  valign=\"bottom\" class=\"beer_list_header\">DRAFT BEER</td></tr></table>";
             HashMap<Integer, String> productPrices 
                                            = new HashMap<Integer, String>();
             stmt                           = transconn.prepareStatement(selectPrices);
             stmt.setInt(1, location);             
             rs                             = stmt.executeQuery();
             while (rs.next()) {
                 int productId              = rs.getInt(1);
                 String price               = HandlerUtils.nullToEmpty(rs.getString(2)) + " - " + HandlerUtils.nullToEmpty(rs.getString(3));
                 productPrices.put(productId, price);
             }

             String glass                   = "", glassUrl = "", preStyle = "";
             int menuSteps                  =1, countLimit = 7;
             stmt                           = transconn.prepareStatement(selectBeverageDescription);
             if(bar >0){
                 stmt.setInt(1, bar);
             } else {
                 stmt.setInt(1, location);
             }             
             rs                             = stmt.executeQuery();
             boolean finishingLoop          = false;
             while (rs.next()) {
                 int prodId                 = rs.getInt(1);
                 String beerStyle           = HandlerUtils.nullToEmpty(rs.getString(2));
                 String productName         = HandlerUtils.nullToEmpty(rs.getString(3));
                 float abv                 = rs.getFloat(4);
                 String style               = HandlerUtils.nullToEmpty(rs.getString(5));
                 String origin              = HandlerUtils.nullToEmpty(rs.getString(6));           
                 productName                = HandlerUtils.nullToString(rs.getString(7),productName); 
                 style                      = HandlerUtils.nullToString(rs.getString(8),style);               
                 int local                  = rs.getInt(9);
                 int featured               = rs.getInt(10);
                 String city                = HandlerUtils.nullToEmpty(rs.getString(11));     
                 String state               = HandlerUtils.nullToEmpty(rs.getString(12));     
                 String addrState           = "";
                 
                 if(style.contains("Do Not Know")){
                     style                  ="";
                 }
                 if(origin.contains("Select")) {
                     origin                 = "";
                 }
                 String abvStr="";
                 if(abv>0){
                     abvStr                 ="ABV "+String.valueOf(abv)+"%";
                 }
                 
                 if(!city.equals("")){
                     addrState              =city;
                 }
                 if(!state.equals("")){
                     if(!addrState.equals("")){
                         addrState          +=", "+state;
                         
                     }
                 }
                 
                 if(addrState.equals("")){
                         addrState          =origin;
                         
                     }
                 String price               = productPrices.get(prodId);
                 
               
                 String productHtml         = "<table width=\"100%\" cellpadding=\"0\" cellspacing=\"0\"  border=\"0\"><tr><td colspan=\"5\" class=\"beer_list\">"+productName.toUpperCase()+"</td></tr>"
                                            + "<tr><td width=\"40%\" valign=\"bottom\" class=\"beer_list2\">"+addrState+"</td><td width=\"2%\">&nbsp;</td><td width=\"40%\" valign=\"bottom\"  class=\"beer_list2\">"+style+"</td><td width=\"2%\">&nbsp;</td><td width=\"15%\" valign=\"bottom\" class=\"beer_list2\">"+abvStr+"</td></tr></table>";
                 
                 if (productCount == 1) {
                     prodDesc               = new StringBuilder();
                     prodDesc.append("<table width=\"100%\" width=\"540px\" cellpadding=\"0\" cellspacing=\"0\" border=\"0\"><tr><td  height=\"5px\" colspan=\"6\"></td></tr><tr>"
                             + "<td width=\"26%\"></td><td colspan=\"5\">"+title+"</td></tr>"
                             + " <tr><td  height=\"3px\" colspan=\"6\"></td></tr> <tr> <td width=\"26%\"></td><td  height=\"3px\" align='center' colspan=\"5\" bgcolor=\"#FFD200\" ></td></tr>" //<img  height=\"3px\" width=\"650px\" src=\"/mnt/sdcard/Images/yellowline.png\"/>
                             + "<tr><td  height=\"3px\" colspan=\"6\"></td></tr><tr><td width=\"26%\"></td><td  colspan=\"5\" >"+productHtml+"</td></tr>");
                     
                     productCount++;
                     
                 } else {
                     if(productCount%2==0){
                         prodDesc.append("<tr><td  height=\"3px\" colspan=\"6\"></td></tr><tr><td bgcolor=\"#322f31\" width=\"26%\"></td><td bgcolor=\"#322f31\" colspan=\"5\">"+productHtml+"</td></tr>");
                     } else {
                         prodDesc.append("<tr><td  height=\"3px\" colspan=\"6\"></td></tr><tr><td    width=\"26%\"></td><td colspan=\"5\">"+productHtml+"</td></tr>");
                     }
                     productCount++;
                     String glassHeight     =(fireStick > 0 ? "540px" : "700px");
                     if(productCount>=countLimit) {
                         prodDesc.append("</table><div><img  height=\""+glassHeight+"\" src=\"/mnt/sdcard/Images/bwwglass1.png\"" +
                                            " style=\"position:absolute; float:left; left:0px; bottom:0px; z-index:2;\" /></div>");
                         menus.add(prodDesc.toString());
                         productCount       = 1;
                          if (finishingLoop) { rs.afterLast();}
                     }
                   
                 }
                
                 
                 if(!finishingLoop){
                     if(style.equals("")) {
                         style              = "-";
                     }
                     if(origin.equals("")) {
                         origin             = "-";
                     }
                     if(abv>0){
                         abvStr                 =String.valueOf(abv)+"%";
                         
                     }
                     
                     productHtml         = "<table align=\"left\" border=\"0\" align=\"center\" cellspacing=\"3\" cellpadding=\"4\" ><tr><td align=\"left\" class=\"feature_list\">"+productName+"</td></tr>"
                                             + "<tr><td align=\"center\" class=\"feature_list2\"></td></tr>"
                                            + "<tr><td align=\"center\" class=\"feature_list2\"> Style : "+style+"</td></tr>"
                                            + "<tr><td align=\"center\" class=\"feature_list2\"> Origin : "+origin+"</td></tr>"
                                            + "<tr><td align=\"center\" class=\"feature_list2\"> ABV : "+abvStr+"</td></tr></table>";
                     
                 }

                 if (rs.isLast() && (productCount > 0) && !finishingLoop) {
                    rs.beforeFirst();
                    finishingLoop           = true;
                    //logger.debug("Resetting Counter");
                 }
                 
             }
             if(productCount > 1){
                 prodDesc.append("</table><div><img height=\"540px\" src=\"/mnt/sdcard/Images/bwwglass1.png\"" +
                                            " style=\"position:absolute; float:left; left:0px; bottom:0px; z-index:2;\" /></div>");
                 menus.add(prodDesc.toString());
                 
             }
            
             
            
            String maxHeight                = "700px";
            String maxWidth                 = "1250px";
            if (fireStick == 1) {
                maxHeight                   = "540px";
                maxWidth                    = "960px";
            }
           stmt                             = transconn.prepareStatement(selectLocationPromotions);
           stmt.setInt(1, location);
           rs                               = stmt.executeQuery();
           while(rs.next()) {
                int type                    = rs.getInt(1);
                String creative             = rs.getString(4);
                switch (rs.getInt(2)) {
                    case 1:
                        String url          = "http://beerboard.tv/USBN.BeerBoard.UI/Images/Customers/"+ String.valueOf(rs.getInt(6)) + "/" + creative.trim().replaceAll("\'", "%27").replaceAll(" ", "%20");
                        prodDesc            = new StringBuilder();
                        prodDesc.append("<table width=\"100%\" height=\"100%\"   border=\"0\"> "
                                            + "  <tr><td  valign=\"middle\" align=\"center\">"
                                            + "<img align=\"center\"  style=\"  max-height: " + maxHeight + ";  max-width:" + maxWidth + ";\"   src=\"/mnt/sdcard/Images/"+creative+"\"/></td></tr>"
                                            + "</table>");
                        promotion.add(prodDesc.toString());
                        assetUrl.add(url);
                    case 2:
                        break;
                    default:
                        break;
                }
            }
            
            
            
            
            //logger.debug("Specials Size:"+specialArray.size());
            if(location!=425){
             if (promotion.size() > 0) {
                 for (String promoStr : promotion) {
                   for (String menuStr : menus) {
                       htmlMenus.add(menuStr);
                   }
                   /*for (String specialStr : specialArray) {
                       htmlMenus.add(specialStr);
                       //logger.debug(specialStr);
                   }*/
                   if(featuredBeer.size()>0){
                       for (String featuredStr : featuredBeer) {
                           htmlMenus.add(featuredStr);
                       }
                   }
                   if(localBeer.size()>0){
                       for (String localStr : localBeer) {
                           htmlMenus.add(localStr);
                       }
                   }
                   htmlMenus.add(promoStr);
                }
             } else {
                for (String menuStr : menus) {
                   htmlMenus.add(menuStr);
                }
                if(featuredBeer.size()>0){
                       for (String featuredStr : featuredBeer) {
                           htmlMenus.add(featuredStr);
                       }
                   }
                   if(localBeer.size()>0){
                       for (String localStr : localBeer) {
                           htmlMenus.add(localStr);
                       }
                   }
               /* for (String specialStr : specialArray) {
                       htmlMenus.add(specialStr);
                   }*/
             }
            } else {
              for (String menuStr : menus) {
                       htmlMenus.add(menuStr);
                   }
                if(localBeer.size()>0){
                    for (String localStr : localBeer) {
                       htmlMenus.add(localStr);
                   }
                }
                if(featuredBeer.size()>0){
                    for (String featuredStr : featuredBeer) {
                       htmlMenus.add(featuredStr);
                   }
                }
                
            }
            assetUrl.add("http://beerboard.tv/USBN.BeerBoard.UI/Images/Assets/font/tradegothic-boldcondtwenty.ttf");                    
            assetUrl.add("http://beerboard.tv/USBN.BeerBoard.UI/Images/Assets/MARIDAVID%20BOLD.ttf");            
            assetUrl.add("http://beerboard.tv/USBN.BeerBoard.UI/Images/Assets/SimonScript.ttf");            
            assetUrl.add("http://beerboard.tv/USBN.BeerBoard.UI/Images/Assets/font/AachenBoldPlain.ttf");
            assetUrl.add("http://beerboard.tv/USBN.BeerBoard.UI/Images/Assets/bbtv/BWW/bbwhiteboarderlogoop65.png");
            assetUrl.add("http://beerboard.tv/USBN.BeerBoard.UI/Images/Assets/bbblackbackground.png");                        
            assetUrl.add("http://beerboard.tv/USBN.BeerBoard.UI/Images/Assets/font/jotterscript.ttf");
            assetUrl.add("http://beerboard.tv/USBN.BeerBoard.UI/Images/Assets/bbtv/whitehline.png");
            assetUrl.add("http://beerboard.tv/USBN.BeerBoard.UI/Images/Assets/bbtv/yellowline.png");
            assetUrl.add("http://beerboard.tv/USBN.BeerBoard.UI/Images/Assets/bbtv/BWW/bwwglass1.png");
            assetUrl.add("http://beerboard.tv/USBN.BeerBoard.UI/Images/Assets/bbtv/BWW/BWW_3C.png");
            assetUrl.add("http://beerboard.tv/USBN.BeerBoard.UI/Images/Assets/bbtv/bbblackfirebackground3.png");
                    assetUrl.add("http://beerboard.tv/USBN.BeerBoard.UI/Images/Assets/bbtv/BWW/bbblackbackground9.png");
            
            
            
            
            //logger.debug("html size:" + htmlMenus.size());
            
            String cssHtml                  = getBWWMenuCSS(headColor, color, fireStick);
            
            
            Element layoutAssetEl           = null;
            for (int i = 0; i < htmlMenus.size(); i++) {  
                String beerListString       = htmlMenus.get(i);   
                if(i==0){
                    File file                       = new File("/home/midware/bbtv/temp28.html");
                    BufferedWriter bw               = new BufferedWriter(new FileWriter(file));
                    bw.write(beerListString);
                    bw.close();
                    
                    file                       = new File("/home/midware/bbtv/temp28.css");
                    bw               = new BufferedWriter(new FileWriter(file));
                    bw.write(cssHtml);
                    bw.close();
                }    
                               
                Element beerMenusEl         = toAppend.addElement("beerMenus");
                beerMenusEl.addElement("visibleTime").addText("12000");
                beerMenusEl.addElement("sequence").addText(HandlerUtils.nullToEmpty(String.valueOf(i)));
                beerMenusEl.addElement("beerMenu").addText(HandlerUtils.nullToEmpty(beerListString));
                beerMenusEl.addElement("type").addText(HandlerUtils.nullToEmpty(String.valueOf(6)));
                beerMenusEl.addElement("locationBackground").addText(HandlerUtils.nullToEmpty(""));
                beerMenusEl.addElement("locationAnimation").addText(HandlerUtils.nullToEmpty("0"));
                beerMenusEl.addElement("aniVisible").addText(String.valueOf((0)));                
                beerMenusEl.addElement("css").addText(HandlerUtils.nullToEmpty(cssHtml));

                Element menusLayoutEl       = beerMenusEl.addElement("menuLayout");
                menusLayoutEl.addElement("html").addText(HandlerUtils.nullToEmpty(""));

                layoutAssetEl               = menusLayoutEl.addElement("asset");
                layoutAssetEl.addElement("url").addText(HandlerUtils.nullToEmpty("http://beerboard.tv/USBN.BeerBoard.UI/Images/Assets/Chalkboard.ttf"));
                layoutAssetEl.addElement("url").addText(HandlerUtils.nullToEmpty("http://beerboard.tv/USBN.BeerBoard.UI/Images/Assets/ChalkboardNew.jpg"));
            }
            if(layoutAssetEl!=null) {
                for(int i=0;i<assetUrl.size();i++){
                    layoutAssetEl.addElement("url").addText(HandlerUtils.nullToEmpty(assetUrl.get(i)));                        
                }
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: "+sqle.toString());
            throw new HandlerException(sqle);
        } catch(Exception e){
            logger.debug(e.getMessage());
        }finally {            
            close(rs);
            close(stmt);
        }
     }
    
      private String getBWWMenuCSS( String headColor, String color, int fireStick) throws HandlerException {
           
           String beer_list_header          ="font-size:92px;";
           String beer_list                 ="font-size:36px;";
           String beer_list2                ="font-size:36px;";
           String price_list                ="font-size:22px;";
           String special_list              ="font-size:40px;";
           String feature_header            ="font-size:60px;";
           String feature_list              ="font-size:40px;";
           String feature_list2             ="font-size:35px;";
           String bgimage                   ="bbblackbackground9.png";
            if(fireStick > 0){                
                beer_list_header            = "font-size:72px;";
                beer_list                   = "font-size:26px;";
                beer_list2                  = "font-size:26px; ";
                price_list                  = "font-size:16px;";
                special_list                = "font-size:20px;";
                feature_header              = "font-size:40px;";
                feature_list                = "font-size:34px;";
                feature_list2               = "font-size:30px;";
                bgimage                     ="bbblackfirebackground3.png";
            }
          
           String css                      = "@charset 'utf-8'; " +
                                            " @font-face { font-family: 'jotterscript'; src: url('/mnt/sdcard/Images/jotterscript.ttf'); } " +
                                            " @font-face { font-family: 'tradegothic'; src: url('/mnt/sdcard/Images/tradegothic-boldcondtwenty.ttf'); } " +
                                            " @font-face { font-family: 'SimonScript'; src: url('/mnt/sdcard/Images/SimonScript.ttf'); } " +                                                                                                                                    
                                            " @font-face { font-family: 'AachenBold'; src: url('/mnt/sdcard/Images/AachenBoldPlain.ttf'); } " +                                            
                                            " body {  background-image: url('/mnt/sdcard/Images/"+bgimage+"'); background-position: left top; fieldset border-style:none; margin:0px; border-collapse:collapse;  }" +
                                            " .beer_list_header{ font-family:'AachenBold'; "+beer_list_header+" color:"+color+"; word-wrap:break-word; text-align:left; padding-top:\"0px\";  }" +
                                            " .beer_list_header1{ font-family:'SimonScript'; font-size:52px;  color:"+headColor+"; overflow:hidden;  white-space:nowrap; text-align:left; padding-top:\"0px\";  }" +
                                            " .beer_list{ font-family: \"tradegothic\";  "+beer_list+"  color: "+headColor+"; word-wrap:break-word;  white-space:normal;   text-align:left;  }" +
                                            " .beer_list2{ font-family: \"tradegothic\";  "+beer_list2+"  color: "+color+"; word-wrap:break-word;  white-space:normal;  text-align:left;  }" +
                                            " .price_list{ font-family: \"tradegothic\";  "+price_list+"  color: #F2EF9B; word-wrap:break-word;   text-align:center;  }" +
                                            " .special_list{ font-family: \"jotterscript\";  "+special_list+"  color: "+color+"; overflow:hidden;  word-wrap:break-word;   text-align:center;  }" +
                                            " .feature_header{ font-family: \"AachenBold\"; "+feature_header+"  color:"+headColor+"; overflow:hidden;  white-space:nowrap;  text-align:center; }" +
                                            " .feature_list{ font-family: \"Minion\"; "+feature_list+" color: "+color+"; word-wrap:break-word; text-align:left;  }" +
                                            " .feature_list2{ font-family: \"Minion\"; "+feature_list2+" color: "+headColor+"; word-wrap:break-word; text-align:left;  }" +
                                            " .cals{ font-family: \"Arial\";  font-size:16px; color: "+color+"; overflow:hidden;  white-space:nowrap;  text-align:center; }";

              
        return css;
     }
      
      
    
}
