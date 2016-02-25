/**
 * FileCopier.java
 *
 * @author Ben Ransford
 * @version $Id: FileCopier.java,v 1.1.1.1 2005/08/26 19:16:01 anonymous Exp $
 */
package net.terakeet.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;

/**
 * Contains methods that copy and move files.
 */
public class FileCopier {
    private static final int bufSize = 2048;

    /**
     * Copies a file over another (possibly non-existent) file.
     * @param src the source file
     * @param dest the destination file
     */
    public static void copy (File src, File dest) throws IOException {
        FileChannel srcChannel = new FileInputStream(src).getChannel();
        FileChannel dstChannel = new FileOutputStream(dest).getChannel();
	
        dstChannel.transferFrom(srcChannel, 0, srcChannel.size());
	
        srcChannel.close();
        dstChannel.close();
    }

    /**
     * Moves a file over another (possibly non-existent) file without
     * creating a temporary file.
     */
    public static void move (File src, File dest) throws IOException {
	move(src, dest, false);
    }

    /**
     * Moves a file over another (possibly non-existent) file.
     * @param src the source file
     * @param dest the destination file
     * @param tmpFile whether to create a temporary file while moving
     */
    public static void move (File src, File dest, boolean tmpFile)
	throws IOException {
	String finalDest = dest.getAbsolutePath();
	if (tmpFile) {
	    dest = new File(dest.getParent() + File.separator +
			    "." + dest.getName());
	}
	copy(src, dest);
	if (!tmpFile || dest.renameTo(new File(finalDest))) {
	    src.delete();
	} else {
	    move(dest, new File(finalDest), false);
	}
    }

}
