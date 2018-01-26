package tables;

import misc.KeyBuilder;

import java.util.Arrays;

/**
 * Orders: O_ID, O_D_ID, O_W_ID, O_C_ID, O_ENTRY_D, O_CARRIER_ID, O_OL_CNT,O_ALL_LOCAL
 * Key: O_W_ID, O_D_ID, O_ID*/

public class Orders {

    public static byte[] TABLE = "Orders".getBytes();

    public static final byte[] O_ID = "O_ID".getBytes();
    public static final byte[] O_D_ID = "O_D_ID".getBytes();
    public static final byte[] O_W_ID = "O_W_ID".getBytes();
    public static final byte[] O_C_ID = "O_C_ID".getBytes();
    public static final byte[] O_ENTRY_D = "O_ENTRY_D".getBytes();
    public static final byte[] O_CARRIER_ID = "O_CARRIER_ID".getBytes();
    public static final byte[] O_OL_CNT = "O_OL_CNT".getBytes();
    public static final byte[] O_ALL_LOCAL = "O_ALL_LOCAL".getBytes();

    public static byte[] getKey(String O_W_ID, String O_D_ID, String O_ID){
        return KeyBuilder.buildKey(Arrays.asList(O_W_ID, O_D_ID, O_ID));
    }
}
