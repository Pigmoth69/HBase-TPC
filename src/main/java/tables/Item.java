package tables;

/**
 * Item: I_ID, I_IM_ID, I_NAME, I_PRICE, I_DATA
 * Key: I_ID
 * */

public class Item {

    public final byte[] TABLE = "Item".getBytes();

    public final byte[] I_ID = "I_ID".getBytes();
    public final byte[] I_IM_ID = "I_IM_ID".getBytes();
    public final byte[] I_NAME = "I_NAME".getBytes();
    public final byte[] I_PRICE = "I_PRICE".getBytes();
    public final byte[] I_DATA = "I_DATA".getBytes();

    public static byte[] getKey(int I_ID){
        return String.format("%9d",I_ID).getBytes();
    }
}
