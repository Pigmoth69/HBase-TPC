package tables;

import misc.KeyBuilder;

import java.util.Arrays;

/**
 * District: D_ID, D_W_ID, D_NAME, D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP,D_TAX, D_YTD, D_NEXT_O_ID
 * key: D_W_ID, D_ID*/

public class District {

    public final byte[] TABLE = "District".getBytes();

    public final byte[] D_ID = "D_ID".getBytes();
    public final byte[] D_W_ID = "D_W_ID".getBytes();
    public final byte[] D_NAME = "D_NAME".getBytes();
    public final byte[] D_STREET_1 = "D_STREET_1".getBytes();
    public final byte[] D_STREET_2 = "D_STREET_2".getBytes();
    public final byte[] D_CITY = "D_CITY".getBytes();
    public final byte[] D_STATE = "D_STATE".getBytes();
    public final byte[] D_ZIP = "D_ZIP".getBytes();
    public final byte[] D_TAX = "D_TAX".getBytes();
    public final byte[] D_YTD = "D_YTD".getBytes();
    public final byte[] D_NEXT_O_ID = "D_NEXT_O_ID".getBytes();

    public static byte[] getKey(String D_ID, String D_W_ID){
        return KeyBuilder.buildKey(Arrays.asList(D_ID, D_W_ID));
    }
}
