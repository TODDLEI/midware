/**
 * MD5.java
 *
 * @author Ben Ransford
 * @version $Id: MD5.java,v 1.1.1.1 2005/08/26 19:16:01 anonymous Exp $
 */

package net.terakeet.util;

/**
 * MD5 utility class (<a
 * href="http://www.faqs.org/rfcs/rfc1321.html">RFC1321</a>).
 */
public class MD5 {

    int A,B,C,D;
    int d[];
    int numwords;

     /**
      * Class constructor.
      * @param str The string on which we'll be performing the MD5 calculations.
      */
    public MD5 (String str) {
	byte[] bytes = new byte[str.length()];
	
	// Convert the input string to a byte array
	for (int i = 0; i < str.length(); i++)
	    bytes[i] = (byte)(str.charAt(i) & 0xff);
	
	// Do MD5 initialization on our new byte array
	mdinit(bytes);
    }
    
    /**
     * Class constructor.
     * @param bytes The byte-array on which we'll be performing the MD5 calculations.
     */
    public MD5 (byte[] bytes) {
	// Do MD5 initialization the input byte array
	mdinit(bytes);
    }
    
    /**
     * MD5 initialization.  Standard stuff.
     * @param bytes A byte array.
     */
    private void mdinit (byte[] bytes) {
	int newlen, endblklen, pad, i;
	long datalenbits;

	// Compute the number of bits in the byte array
	datalenbits = bytes.length  * 8;

	// b[] is basically bytes[] padded to a 64-byte boundary
	endblklen = bytes.length % 64;
	if (endblklen < 56)
	    pad = 64 - endblklen;
	else
	    pad = (64 - endblklen) + 64;
	newlen = bytes.length + pad;
	byte b[] = new byte[newlen];

	// Copy bytes[] to b[]
	for(i = 0; i < bytes.length; i++)
	    b[i] = bytes[i];

	// 0x80-terminate bytes[]'s content in b[]
	b[bytes.length] = (byte) 0x80;

	// Zero out the rest of b[]
	for (i = b.length + 1; i < (newlen - 8); i++)
	    b[i] = 0;
	for (i = 0; i < 8; i++) {
	    b[newlen - 8 + i] = (byte) (datalenbits & 0xff);
	    datalenbits >>= 8;
	}
	
	// Initialize A-D registers
	A = 0x67452301;
	B = 0xefcdab89;
	C = 0x98badcfe;
	D = 0x10325476;

	// Assume 16-bit words... hmm.  Oh, wait, this is Java. :-)
	this.numwords = newlen/4;
	this.d = new int[this.numwords];
	for (i = 0; i < newlen; i += 4)
	    this.d[i/4] = (b[i] & 0xff) + ((b[i+1] & 0xff) << 8) + 
		((b[i+2] & 0xff) << 16) + ((b[i+3] & 0xff) << 24);
    }
     
    private static int F (int x, int y, int z) {
	return ((x & y) | (~x & z));
    }
     
    private static int G (int x, int y, int z) {
	return ((x & z) | (y & ~z));
    }
     
    private static int H (int x, int y, int z) {
	return (x ^ y ^ z);
    }
     
    private static int I (int x, int y, int z) {
	return (y ^ (x | ~z));
    }
     
    /**
     * Gets a <code>String</code> representation of the current state of
     * the four MD5 registers.
     * @return A string containing the concatenation of hexified values
     *         of the four MD5 registers.
     */
    public String toString () {
	String s;
	return(tohex(A) + tohex(B) + tohex(C) + tohex(D));
    }

    /**
     * Actually <em>does</em> the MD5 calculation by calling rounds 1-4
     * of the MD5 machine.  Lots of math.
     * @return This instance of this class.
     */
    public MD5 calc () {
	int AA, BB, CC, DD, i;
	
	for(i = 0; i < numwords/16; i++) {
	    AA = A; BB = B; CC = C; DD = D;
	    round1(i);
	    round2(i);
	    round3(i);
	    round4(i);
	    A += AA; B+= BB; C+= CC; D+= DD;
	}

	return this;
    }
     
    /**
     * Leftward rotates an <code>int</code> by a given number of bits.
     *
     * @param val The integer to rotate.
     * @param numbits The number of bits to rotate the given integer.
     * @return The rotated integer.
     */
    static int rotintlft (int val, int numbits) {
	return ((val << numbits) | (val >>> (32 - numbits)));
    }
    
    /**
     * Converts an <code>int</code> to a hexadecimal <code>String</code>.
     *
     * @param i The integer to hexify.
     * @return A string containing the hexadecimal representation of the
     *         integer argument.
     */
    private static String tohex (int i) {
	int b;
	String tmpstr;
	
	tmpstr = "";
	for (b = 0; b < 4; b++) {
	    tmpstr += Integer.toString((i >> 4) & 0xf, 16) 
		+ Integer.toString(i & 0xf, 16);
	    i >>= 8;
	}
	return tmpstr;
    }
    
    /**
     * MD5 round 1.
     * @param blk The work block.
     */
    private void round1 (int blk) {
	A = rotintlft(A + F(B, C, D) + d[0 + 16 * blk] + 0xd76aa478, 7) + B;
	D = rotintlft(D + F(A, B, C) + d[1 + 16 * blk] + 0xe8c7b756, 12) + A;
	C = rotintlft(C + F(D, A, B) + d[2 + 16 * blk] + 0x242070db, 17) + D;
	B = rotintlft(B + F(C, D, A) + d[3 + 16 * blk] + 0xc1bdceee, 22) + C;
	
	A = rotintlft(A + F(B, C, D) + d[4 + 16 * blk] + 0xf57c0faf, 7) + B;
	D = rotintlft(D + F(A, B, C) + d[5 + 16 * blk] + 0x4787c62a, 12) + A;
	C = rotintlft(C + F(D, A, B) + d[6 + 16 * blk] + 0xa8304613, 17) + D;
	B = rotintlft(B + F(C, D, A) + d[7 + 16 * blk] + 0xfd469501, 22) + C;
	
	A = rotintlft(A + F(B, C, D) + d[8 + 16 * blk] + 0x698098d8, 7) + B;
	D = rotintlft(D + F(A, B, C) + d[9 + 16 * blk] + 0x8b44f7af, 12) + A;
	C = rotintlft(C + F(D, A, B) + d[10 + 16 * blk] + 0xffff5bb1, 17) + D;
	B = rotintlft(B + F(C, D, A) + d[11 + 16 * blk] + 0x895cd7be, 22) + C;
	
	A = rotintlft(A + F(B, C, D) + d[12 + 16 * blk] + 0x6b901122, 7) + B;
	D = rotintlft(D + F(A, B, C) + d[13 + 16 * blk] + 0xfd987193, 12) + A;
	C = rotintlft(C + F(D, A, B) + d[14 + 16 * blk] + 0xa679438e, 17) + D;
	B = rotintlft(B + F(C, D, A) + d[15 + 16 * blk] + 0x49b40821, 22) + C;
    }
    
    /**
     * MD5 round 2.
     * @param blk The work block.
     */
    private void round2 (int blk) {
	A = rotintlft(A + G(B, C, D) + d[1 + 16 * blk] + 0xf61e2562, 5) + B;
	D = rotintlft(D + G(A, B, C) + d[6 + 16 * blk] + 0xc040b340, 9) + A;
	C = rotintlft(C + G(D, A, B) + d[11 + 16 * blk] + 0x265e5a51, 14) + D;
	B = rotintlft(B + G(C, D, A) + d[0 + 16 * blk] + 0xe9b6c7aa, 20) + C;
	
	A = rotintlft(A + G(B, C, D) + d[5 + 16 * blk] + 0xd62f105d, 5) + B;
	D = rotintlft(D + G(A, B, C) + d[10 + 16 * blk] + 0x02441453, 9) + A;
	C = rotintlft(C + G(D, A, B) + d[15 + 16 * blk] + 0xd8a1e681, 14) + D;
	B = rotintlft(B + G(C, D, A) + d[4 + 16 * blk] + 0xe7d3fbc8, 20) + C;
	
	A = rotintlft(A + G(B, C, D) + d[9 + 16 * blk] + 0x21e1cde6, 5) + B;
	D = rotintlft(D + G(A, B, C) + d[14 + 16 * blk] + 0xc33707d6, 9) + A;
	C = rotintlft(C + G(D, A, B) + d[3 + 16 * blk] + 0xf4d50d87, 14) + D;
	B = rotintlft(B + G(C, D, A) + d[8 + 16 * blk] + 0x455a14ed, 20) + C;
	
	A = rotintlft(A + G(B, C, D) + d[13 + 16 * blk] + 0xa9e3e905, 5) + B;
	D = rotintlft(D + G(A, B, C) + d[2 + 16 * blk] + 0xfcefa3f8, 9) + A;
	C = rotintlft(C + G(D, A, B) + d[7 + 16 * blk] + 0x676f02d9, 14) + D;
	B = rotintlft(B + G(C, D, A) + d[12 + 16 * blk] + 0x8d2a4c8a, 20) + C;
    }

    /**
     * MD5 round 3.
     * @param blk The work block.
     */
    private void round3 (int blk) {
	A = rotintlft(A + H(B, C, D) + d[5 + 16 * blk] + 0xfffa3942, 4) + B;
	D = rotintlft(D + H(A, B, C) + d[8 + 16 * blk] + 0x8771f681, 11) + A;
	C = rotintlft(C + H(D, A, B) + d[11 + 16 * blk] + 0x6d9d6122, 16) + D;
	B = rotintlft(B + H(C, D, A) + d[14 + 16 * blk] + 0xfde5380c, 23) + C;
	
	A = rotintlft(A + H(B, C, D) + d[1 + 16 * blk] + 0xa4beea44, 4) + B;
	D = rotintlft(D + H(A, B, C) + d[4 + 16 * blk] + 0x4bdecfa9, 11) + A;
	C = rotintlft(C + H(D, A, B) + d[7 + 16 * blk] + 0xf6bb4b60, 16) + D;
	B = rotintlft(B + H(C, D, A) + d[10 + 16 * blk] + 0xbebfbc70, 23) + C;
	
	A = rotintlft(A + H(B, C, D) + d[13 + 16 * blk] + 0x289b7ec6, 4) + B;
	D = rotintlft(D + H(A, B, C) + d[0 + 16 * blk] + 0xeaa127fa, 11) + A;
	C = rotintlft(C + H(D, A, B) + d[3 + 16 * blk] + 0xd4ef3085, 16) + D;
	B = rotintlft(B + H(C, D, A) + d[6 + 16 * blk] + 0x04881d05, 23) + C;
	
	A = rotintlft(A + H(B, C, D) + d[9 + 16 * blk] + 0xd9d4d039, 4) + B;
	D = rotintlft(D + H(A, B, C) + d[12 + 16 * blk] + 0xe6db99e5, 11) + A;
	C = rotintlft(C + H(D, A, B) + d[15 + 16 * blk] + 0x1fa27cf8, 16) + D;
	B = rotintlft(B + H(C, D, A) + d[2 + 16 * blk] + 0xc4ac5665, 23) + C;
    }
    
    /**
     * MD5 round 4.
     * @param blk The work block.
     */
    private void round4 (int blk) {
	A = rotintlft(A + I(B, C, D) + d[0 + 16 * blk] + 0xf4292244, 6) + B;
	D = rotintlft(D + I(A, B, C) + d[7 + 16 * blk] + 0x432aff97, 10) + A;
	C = rotintlft(C + I(D, A, B) + d[14 + 16 * blk] + 0xab9423a7, 15) + D;
	B = rotintlft(B + I(C, D, A) + d[5 + 16 * blk] + 0xfc93a039, 21) + C;
	
	A = rotintlft(A + I(B, C, D) + d[12 + 16 * blk] + 0x655b59c3, 6) + B;
	D = rotintlft(D + I(A, B, C) + d[3 + 16 * blk] + 0x8f0ccc92, 10) + A;
	C = rotintlft(C + I(D, A, B) + d[10 + 16 * blk] + 0xffeff47d, 15) + D;
	B = rotintlft(B + I(C, D, A) + d[1 + 16 * blk] + 0x85845dd1, 21) + C;
	
	A = rotintlft(A + I(B, C, D) + d[8 + 16 * blk] + 0x6fa87e4f, 6) + B;
	D = rotintlft(D + I(A, B, C) + d[15 + 16 * blk] + 0xfe2ce6e0, 10) + A;
	C = rotintlft(C + I(D, A, B) + d[6 + 16 * blk] + 0xa3014314, 15) + D;
	B = rotintlft(B + I(C, D, A) + d[13 + 16 * blk] + 0x4e0811a1, 21) + C;
	
	A = rotintlft(A + I(B, C, D) + d[4 + 16 * blk] + 0xf7537e82, 6) + B;
	D = rotintlft(D + I(A, B, C) + d[11 + 16 * blk] + 0xbd3af235, 10) + A;
	C = rotintlft(C + I(D, A, B) + d[2 + 16 * blk] + 0x2ad7d2bb, 15) + D;
	B = rotintlft(B + I(C, D, A) + d[9 + 16 * blk] + 0xeb86d391, 21) + C;
    }
    
}
