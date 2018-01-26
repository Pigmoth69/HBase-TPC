package tables;

import misc.KeyBuilder;

import java.util.Arrays;

/**
 * Customer: C_ID, C_W_ID, C_D_ID, C_FIRST,C_MIDDLE, C_LAST, C_STREET_1,C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE , C_CREDIT, C_CREDITLIM,C_DISCOUNT, C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_DELIVERY_CNT,C_DATA
 * Key: C_W_ID, C_D_ID, C_ID
 * */

public class Customer {

    public final byte[] TABLE = "Customer".getBytes();

    public final byte[] C_ID = "C_ID".getBytes();
    public final byte[] C_W_ID = "C_W_ID".getBytes();
    public final byte[] C_D_ID = "C_D_ID".getBytes();
    public final byte[] C_FIRST = "C_FIRST".getBytes();
    public final byte[] C_MIDDLE = "C_MIDDLE".getBytes();
    public final byte[] C_LAST = "C_LAST".getBytes();
    public final byte[] C_STREET_1 = "C_STREET_1".getBytes();
    public final byte[] C_STREET_2 = "C_STREET_2".getBytes();
    public final byte[] C_CITY = "C_CITY".getBytes();
    public final byte[] C_STATE = "C_STATE".getBytes();
    public final byte[] C_ZIP = "C_ZIP".getBytes();
    public final byte[] C_PHONE = "C_PHONE".getBytes();
    public final byte[] C_SINCE = "C_SINCE".getBytes();
    public final byte[] C_CREDIT = "C_CREDIT".getBytes();
    public final byte[] C_CREDITLIM = "C_CREDITLIM".getBytes();
    public final byte[] C_DISCOUNT = "C_DISCOUNT".getBytes();
    public final byte[] C_BALANCE = "C_BALANCE".getBytes();
    public final byte[] C_YTD_PAYMENT = "C_YTD_PAYMENT".getBytes();
    public final byte[] C_PAYMENT_CNT = "C_PAYMENT_CNT".getBytes();
    public final byte[] C_DELIVERY_CNT = "C_DELIVERY_CNT".getBytes();
    public final byte[] C_DATA = "C_DATA".getBytes();

    public static byte[] getKey(String C_W_ID, String C_D_ID, String C_ID){
        return KeyBuilder.buildKey(Arrays.asList(C_W_ID, C_D_ID, C_ID));
    }
}
