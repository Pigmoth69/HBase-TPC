package tables;


/**Table Schema:
 * Warehouse:
 *              W_ID, W_NAME, W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP, W_TAX, W_YTD
 *              Key: W_ID
 */
public class Warehouse {

    public static byte[] TABLE = "Warehouse".getBytes();

    public static final byte[] W_ID = "W_ID".getBytes();
    public static final byte[] W_NAME = "W_NAME".getBytes();
    public static final byte[] W_STREET_1 = "W_STREET_1".getBytes();
    public static final byte[] W_STREET_2 = "W_STREET_2".getBytes();
    public static final byte[] W_CITY = "W_CITY".getBytes();
    public static final byte[] W_STATE = "W_STATE".getBytes();
    public static final byte[] W_ZIP = "W_ZIP".getBytes();
    public static final byte[] W_TAX = "W_TAX".getBytes();
    public static final byte[] W_YTD = "W_YTD".getBytes();
}
