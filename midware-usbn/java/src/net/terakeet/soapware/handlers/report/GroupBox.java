/*
 * GroupBox.java
 *
 * Created on January 2, 2007, 4:19 PM
 *
 * (c) 2007 Terakeet Corp.
 */

package net.terakeet.soapware.handlers.report;

import java.util.*;
import java.io.*;
import java.lang.*;

/**
 * This class defines the content for a single group, which contains a set of entries as well as their values.
 */
//public class GroupBox extends Hashtable<GroupingKey, ValueBox>{
public class GroupBox extends TreeMap<GroupingKey, ValueBox> {
    public GroupBox() {
    }
}
