package tables;

import misc.KeyBuilder;

import java.util.Arrays;

/**
 * Order_line: OL_O_ID, O_D_ID, OL_W_ID, OL_NUMBER, OL_I_ID, OL_SUPPLY_W_ID, OL_DELIVERY_D, OL_QUANTITY, OL_AMOUNT, OL_DIST_INFO
 * Key: OL_W_ID, OL_D_ID, OL_O_ID, OL_NUMBER
 */

public class OrderLine {

    public static byte[] TABLE = "OrderLine".getBytes();

    public static final byte[] OL_O_ID = "OL_O_ID".getBytes();
    public static final byte[] OL_D_ID = "OL_D_ID".getBytes();
    public static final byte[] OL_W_ID = "OL_W_ID".getBytes();
    public static final byte[] OL_NUMBER = "OL_NUMBER".getBytes();
    public static final byte[] OL_I_ID = "OL_I_ID".getBytes();
    public static final byte[] OL_SUPPLY_W_ID = "OL_SUPPLY_W_ID".getBytes();
    public static final byte[] OL_DELIVERY_D = "OL_DELIVERY_D".getBytes();
    public static final byte[] OL_QUANTITY = "OL_QUANTITY".getBytes();
    public static final byte[] OL_AMOUNT = "OL_AMOUNT".getBytes();
    public static final byte[] OL_DIST_INFO = "OL_DIST_INFO".getBytes();

    public static byte[] getKey(String OL_W_ID, String OL_D_ID, String OL_O_ID, String OL_NUMBER){
        return KeyBuilder.buildKey(Arrays.asList(OL_W_ID, OL_D_ID, OL_O_ID, OL_NUMBER));
    }
}
