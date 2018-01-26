package tables;

import misc.KeyBuilder;

import java.util.Arrays;

/**
 * New_order: NO_O_ID, NO_D_ID, NO_W_ID
 * Key: NO_W_ID, NO_D_ID, NO_O_ID*/

public class NewOrder {

    public static byte[] TABLE = "NewOrder".getBytes();

    public static final byte[] NO_O_ID = "NO_O_ID".getBytes();
    public static final byte[] NO_D_ID = "NO_D_ID".getBytes();
    public static final byte[] NO_W_ID = "NO_W_ID".getBytes();

    public static byte[] getKey(String NO_W_ID, String NO_D_ID, String NO_O_ID){
        return KeyBuilder.buildKey(Arrays.asList(NO_W_ID, NO_D_ID, NO_O_ID));
    }
}
