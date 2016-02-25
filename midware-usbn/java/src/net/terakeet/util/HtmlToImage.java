/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package net.terakeet.util;
import java.awt.Container;
import java.awt.Graphics;
import java.awt.Insets;
import java.awt.image.BufferedImage;

import javax.swing.JEditorPane;
import javax.swing.SwingUtilities;
import javax.swing.text.Document;
import javax.swing.text.html.HTMLDocument;
import javax.swing.text.html.HTMLEditorKit;

/**
 *
 * @author suba
 */


public abstract class HtmlToImage
{
    public static BufferedImage create(String src, int width, int height)
    {
        BufferedImage image                 = null;
        JEditorPane pane                    = new JEditorPane();
        Kit kit                             = new Kit();
        pane.setEditorKit(kit);
        pane.setEditable(false);
        pane.setMargin(new Insets(0,0,0,0));
        try {
            pane.setPage(src);
            image                           = new BufferedImage(width, height, BufferedImage.TYPE_INT_RGB);
            Graphics g                      = image.createGraphics();   
            Container c                     = new Container();
            for(int i=0;i<800;i++) {
                SwingUtilities.paintComponent(g, pane, c, 0, 0, width, height);
            }

            g.dispose();
        } catch (Exception e) {
            System.out.println(e);
        }
        return image;
    }
    static class Kit extends HTMLEditorKit
    {
        public Document createDefaultDocument() {
            HTMLDocument doc                = (HTMLDocument) super.createDefaultDocument();
            doc.setTokenThreshold(Integer.MAX_VALUE);
            doc.setAsynchronousLoadPriority(-1);
            return doc;
        }
    }
}