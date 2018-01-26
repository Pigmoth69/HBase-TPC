package tables;

import misc.KeyBuilder;

import java.util.Arrays;

/** History: H_C_ID, H_C_D_ID, H_C_W_ID, H_D_ID ,H_W_ID, H_DATE, H_AMOUNT, H_DATA
 * */

public class History {

    public static byte[] TABLE = "History".getBytes();

    public static final byte[] H_C_ID = "H_C_ID".getBytes();
    public static final byte[] H_C_D_ID = "H_C_D_ID".getBytes();
    public static final byte[] H_C_W_ID = "H_C_W_ID".getBytes();
    public static final byte[] H_D_ID = "H_D_ID".getBytes();
    public static final byte[] H_W_ID = "H_W_ID".getBytes();
    public static final byte[] H_DATE = "H_DATE".getBytes();
    public static final byte[] H_AMOUNT = "H_AMOUNT".getBytes();
    public static final byte[] H_DATA = "H_DATA".getBytes();

    public static byte[] getKey(String H_C_W_ID, String H_C_D_ID, String H_C_ID, String H_DATA){
        return KeyBuilder.buildKey(Arrays.asList(H_C_W_ID, H_C_D_ID, H_C_ID, H_DATA));
    }
}
