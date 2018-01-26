package tables;

/** History: H_C_ID, H_C_D_ID, H_C_W_ID, H_D_ID ,H_W_ID, H_DATE, H_AMOUNT, H_DATA
 * */

public class History {

    public final byte[] TABLE = "History".getBytes();

    public final byte[] H_C_ID = "H_C_ID".getBytes();
    public final byte[] H_C_D_ID = "H_C_D_ID".getBytes();
    public final byte[] H_C_W_ID = "H_C_W_ID".getBytes();
    public final byte[] H_D_ID = "H_D_ID".getBytes();
    public final byte[] H_W_ID = "H_W_ID".getBytes();
    public final byte[] H_DATE = "H_DATE".getBytes();
    public final byte[] H_AMOUNT = "H_AMOUNT".getBytes();
    public final byte[] H_DATA = "H_DATA".getBytes();
}
