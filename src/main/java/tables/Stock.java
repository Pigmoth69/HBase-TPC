package tables;

import misc.KeyBuilder;

import java.util.Arrays;

/**
 * Stock: S_I_ID, S_W_ID, S_QUANTITY, S_DIST_01, S_DIST_02, S_DIST_03, S_DIST_04, S_DIST_05, S_DIST_06, S_DIST_07, S_DIST_08, S_DIST_09, S_DIST_10, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DATA
 * Key: S_W_ID, S_I_ID
 */

public class Stock {

    public static byte[] TABLE = "Stock".getBytes();

    public static final byte[] S_I_ID = "S_I_ID".getBytes();
    public static final byte[] S_W_ID = "S_W_ID".getBytes();
    public static final byte[] S_QUANTITY = "S_QUANTITY".getBytes();
    public static final byte[] S_DIST_01 = "S_DIST_01".getBytes();
    public static final byte[] S_DIST_02 = "S_DIST_02".getBytes();
    public static final byte[] S_DIST_03 = "S_DIST_03".getBytes();
    public static final byte[] S_DIST_04 = "S_DIST_04".getBytes();
    public static final byte[] S_DIST_05 = "S_DIST_05".getBytes();
    public static final byte[] S_DIST_06 = "S_DIST_06".getBytes();
    public static final byte[] S_DIST_07 = "S_DIST_07".getBytes();
    public static final byte[] S_DIST_08 = "S_DIST_08".getBytes();
    public static final byte[] S_DIST_09 = "S_DIST_09".getBytes();
    public static final byte[] S_DIST_10 = "S_DIST_10".getBytes();
    public static final byte[] S_YTD = "S_YTD".getBytes();
    public static final byte[] S_ORDER_CNT = "S_ORDER_CNT".getBytes();
    public static final byte[] S_REMOTE_CNT = "S_REMOTE_CNT".getBytes();
    public static final byte[] S_DATA = "S_DATA".getBytes();

    public static byte[] getKey(String S_W_ID, String S_I_ID){
        return KeyBuilder.buildKey(Arrays.asList(S_W_ID, S_I_ID));
    }
}
