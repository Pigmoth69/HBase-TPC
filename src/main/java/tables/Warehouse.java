package tables;
/**Table Schema:
 * Warehouse:
 *              W_ID, W_NAME, W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP, W_TAX, W_YTD
 *              Key: W_ID
 */
public class Warehouse {

    public final byte[] TABLE = "Warehouse".getBytes();

    public final byte[] W_ID = "W_ID".getBytes();
    public final byte[] W_NAME = "W_NAME".getBytes();
    public final byte[] W_STREET_1 = "W_STREET_1".getBytes();
    public final byte[] W_STREET_2 = "W_STREET_2".getBytes();
    public final byte[] W_CITY = "W_CITY".getBytes();
    public final byte[] W_STATE = "W_STATE".getBytes();
    public final byte[] W_ZIP = "W_ZIP".getBytes();
    public final byte[] W_TAX = "W_TAX".getBytes();
    public final byte[] W_YTD = "W_YTD".getBytes();

    public static byte[] getKey(int W_ID){
        return String.format("%9d",W_ID).getBytes();
    }
}
