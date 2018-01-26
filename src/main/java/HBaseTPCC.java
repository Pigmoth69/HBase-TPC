import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import tables.*;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;


public class HBaseTPCC {
    private Configuration config;
    private HBaseAdmin hBaseAdmin;

    /**Define the Table Column   Families*/
    private static final byte[] TABLE_FAMILY_ID = "ID".getBytes();
    private static final byte[] TABLE_FAMILY_TEXT = "TEXT".getBytes();
    private static final byte[] TABLE_FAMILY_NUMERIC = "NUMERIC".getBytes();

    /**Define a Logger to output errors to a logger file*/
    private static Logger logger = Logger.getLogger("MyHBaseLogger");
    private static FileHandler fh;
    private static DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");

    /**Define Variables*/
    private int MAX_VERSION = 10;
    public enum Status {INFO,WARNING,ERROR}
    private List<byte[]> TABLES = Arrays.asList(Customer.TABLE, District.TABLE, History.TABLE, Item.TABLE, NewOrder.TABLE,OrderLine.TABLE,Orders.TABLE,Stock.TABLE,Warehouse.TABLE);

    /**
     * The Constructor. Establishes the connection with HBase.
     * @param zkHost
     * @throws IOException
     */
    public HBaseTPCC(String zkHost) throws IOException {
        config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", zkHost.split(":")[0]);
        config.set("hbase.zookeeper.property.clientPort", zkHost.split(":")[1]);
        HBaseConfiguration.addHbaseResources(config);
        this.hBaseAdmin = new HBaseAdmin(config);
    }

    public void createTPCCTables() throws IOException {

        //Define all the table families and their max version
        HColumnDescriptor familyId = new HColumnDescriptor(TABLE_FAMILY_ID);
        familyId.setMaxVersions(MAX_VERSION);
        HColumnDescriptor familyText = new HColumnDescriptor(TABLE_FAMILY_TEXT);
        familyText.setMaxVersions(MAX_VERSION);
        HColumnDescriptor familyNumeric = new HColumnDescriptor(TABLE_FAMILY_NUMERIC);
        familyNumeric.setMaxVersions(MAX_VERSION);

        for(byte[] tmp : TABLES){
            String tableName = new String(tmp, StandardCharsets.UTF_8);
            if(!hBaseAdmin.tableExists(tmp)){
                HTableDescriptor tmpTableDescriptor = new HTableDescriptor(TableName.valueOf(tmp));
                tmpTableDescriptor.addFamily(familyId);
                tmpTableDescriptor.addFamily(familyText);
                tmpTableDescriptor.addFamily(familyNumeric);
                hBaseAdmin.createTable(tmpTableDescriptor);
                writeToLog("Table: " + tableName+ " created!",Status.INFO);
            }else{
                writeToLog("Table: " + tableName+ " was not created because already exists!",Status.WARNING);
            }
        }
    }

    public void loadTables(String folder)throws IOException{

        //Load the customers table
        try {
            BufferedReader bufferedReader = new BufferedReader(new FileReader(folder+"/customer.csv"));
            HTable table = new HTable(config, Customer.TABLE);
            String line;
            Put put;
            while ((line = bufferedReader.readLine()) != null) {
                String[] customer = line.split(",");
                put = createCustomerTable(customer[0],customer[1],customer[2],customer[3],customer[4],
                        customer[5],customer[6],customer[7],customer[8],customer[9],customer[10],
                        customer[11],customer[12],customer[13],customer[14],customer[15],customer[16],customer[17],
                        customer[18],customer[19],customer[20]);
                table.put(put);
            }
            table.close();
            bufferedReader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        //District Table Data Load.
        try {
            BufferedReader bufferedReader = new BufferedReader(new FileReader(folder+"/district.csv"));
            HTable table = new HTable(config, District.TABLE);
            String line;
            Put put;
            while ((line = bufferedReader.readLine()) != null) {
                String[] district = line.split(",");
                put = createDistrictTable(district[0],district[1],district[2],district[3],district[4],district[5],district[6],district[7],district[8],district[9],district[10]);
                table.put(put);
            }
            table.close();
            bufferedReader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        //History Table Data Load.
        try {
            BufferedReader bufferedReader = new BufferedReader(new FileReader(folder+"/history.csv"));
            HTable table = new HTable(config, History.TABLE);
            String line;
            Put put;
            while ((line = bufferedReader.readLine()) != null) {
                String[] history = line.split(",");
                put = createHistoryTable(history[0],history[1],history[2],history[3],history[4],history[5],history[6],history[7]);
                table.put(put);
            }

            table.close();
            bufferedReader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        //Item Table Data Load.
        try {
            BufferedReader bufferedReader = new BufferedReader(new FileReader(folder+"/item.csv"));
            HTable table = new HTable(config, Item.TABLE);
            String line;
            Put put;
            while ((line = bufferedReader.readLine()) != null) {
                String[] item = line.split(",");
                put = createItemTable(item[0],item[1],item[2],item[3],item[4]);
                table.put(put);
            }

            table.close();
            bufferedReader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        //NewOrder Table Data Load
        try {
            BufferedReader bufferedReader = new BufferedReader(new FileReader(folder+"/new_order.csv"));
            HTable table = new HTable(config, NewOrder.TABLE);
            String line;
            Put put;
            while ((line = bufferedReader.readLine()) != null) {
                String[] newOrder = line.split(",");
                put = createNewOrderTable(newOrder[0],newOrder[1],newOrder[2]);
                table.put(put);
            }
            table.close();
            bufferedReader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        //OrderLine Table Data Load
        try (BufferedReader bufferedReader = new BufferedReader(new FileReader(folder+"/order_line.csv"))) {
            HTable table = new HTable(config, OrderLine.TABLE);
            String line;
            Put put;
            while ((line = bufferedReader.readLine()) != null) {
                String[] orderLine = line.split(",");
                put = createOrderLineTable(orderLine[0],orderLine[1],orderLine[2],orderLine[3],
                        orderLine[4],orderLine[5],orderLine[6],orderLine[7],orderLine[8],orderLine[9]);
                table.put(put);
            }

            table.close();
            bufferedReader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        //Orders Table Data Load
        try {
            BufferedReader bufferedReader = new BufferedReader(new FileReader(folder+"/orders.csv"));
            HTable table = new HTable(config, Orders.TABLE);
            String line;
            Put put;
            while ((line = bufferedReader.readLine()) != null) {
                String[] orders = line.split(",");
                put = createOrdersTable(orders[0], orders[1], orders[2], orders[3], orders[4], orders[5], orders[6], orders[7]);
                table.put(put);
            }
            table.close();
            bufferedReader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        //Stock Table Data Load
        try (BufferedReader bufferedReader = new BufferedReader(new FileReader(folder+"/stock.csv"))) {
            HTable table = new HTable(config, Stock.TABLE);
            String line;
            Put put;
            while ((line = bufferedReader.readLine()) != null) {
                String[] stock = line.split(",");
                put = createStockTable(stock[0],stock[1],stock[2],
                        stock[3],stock[4],stock[5],stock[6],stock[7],stock[8],stock[9],stock[10],stock[11],stock[12],
                        stock[13],stock[14],stock[15],stock[16]);
                table.put(put);
            }

            table.close();
            bufferedReader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        //Warehouse Table Data Load
        try {
            BufferedReader bufferedReader = new BufferedReader(new FileReader(folder+"/warehouse.csv"));
            HTable table = new HTable(config, Warehouse.TABLE);
            String line;
            Put put;
            while ((line = bufferedReader.readLine()) != null) {
                String[] warehouse = line.split(",");
                put = createWarehouseTable(warehouse[0],warehouse[1],warehouse[2],warehouse[3],warehouse[4],warehouse[5],warehouse[6],warehouse[7],warehouse[8]);
                table.put(put);
            }

            table.close();
            bufferedReader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.exit(-1);
    }

    /**
     * This method generates the key
     * @param values The value of each column
     * @param keyTable The position of each value that is required to create the key in the array of values.
     * @return The encoded key to be inserted in HBase
     */
    private byte[] getKey(String[] values, int[] keyTable) {
        String keyString = "";
        for (int keyId : keyTable){
            keyString += values[keyId];
        }
        byte[] key = Bytes.toBytes(keyString);

        return key;
    }



    public List<String> query1(String warehouseId, String districtId, String startDate, String endDate) throws IOException {
        ResultScanner results;
        HTable ordersTable = new HTable(config, Orders.TABLE);

        List<String> customers = new ArrayList<>();
        FilterList filters = new FilterList(FilterList.Operator.MUST_PASS_ALL);

        filters.addFilter(new SingleColumnValueFilter(TABLE_FAMILY_NUMERIC,Orders.O_ENTRY_D, CompareFilter.CompareOp.GREATER_OR_EQUAL,stringToByteTimestamp(startDate)));
        filters.addFilter(new SingleColumnValueFilter(TABLE_FAMILY_NUMERIC,Orders.O_ENTRY_D, CompareFilter.CompareOp.LESS,stringToByteTimestamp(endDate)));
        filters.addFilter(new SingleColumnValueFilter(TABLE_FAMILY_ID,Orders.O_D_ID, CompareFilter.CompareOp.EQUAL,districtId.getBytes()));
        filters.addFilter(new SingleColumnValueFilter(TABLE_FAMILY_ID,Orders.O_W_ID, CompareFilter.CompareOp.EQUAL,warehouseId.getBytes()));

        Scan scan = new Scan();
        scan.setFilter(filters);

        results = ordersTable.getScanner(scan);
        Result res = results.next();
        while (res!=null && !res.isEmpty()){
            byte[] val = res.getValue(TABLE_FAMILY_TEXT, Orders.O_C_ID);
            customers.add(new String(val,"UTF-8"));
            res = results.next();
        }
        ordersTable.close();
        return customers;

    }

    public void query2(String warehouseId, String districtId, String customerId, String[] discounts) throws IOException {
        HTable table = new HTable(config, Customer.TABLE);

        if(discounts.length > 6) {
            System.out.println("Query2: Insert/update (up to 6 times) the discount for a given customer, warehouse and district.");
            System.exit(-1);
            return;
        }

        for(int i = 0; i < discounts.length; i++) {
            Put put = new Put(Customer.getKey(warehouseId, districtId, customerId));
            put.add(TABLE_FAMILY_NUMERIC, Customer.C_DISCOUNT, discounts[i].getBytes());
            table.put(put);
        }

        table.close();
        System.exit(-1);
    }

    public String[] query3(String warehouseId, String districtId, String customerId) throws IOException {

        HTable table = new HTable(config, Customer.TABLE);
        ArrayList<String> discounts = new ArrayList<>();

        Get get = new Get(Customer.getKey(warehouseId, districtId, customerId));
        get.setMaxVersions(4);
        get.addColumn(TABLE_FAMILY_NUMERIC, Customer.C_DISCOUNT);


        Result result = table.get(get);
        if (result != null && !result.isEmpty()){
            CellScanner scanner = result.cellScanner();
            while (scanner.advance() && discounts.size()<4) {
                Cell cell = scanner.current();
                byte[] value = CellUtil.cloneValue(cell);
                discounts.add(new String(value,"UTF-8"));
            }
        }
        return discounts.toArray(new String[0]);
    }

    public List<Integer>  query4(String warehouseId, String[] districtIds) throws IOException {
        ArrayList<Integer> users = new ArrayList<>();
        HTable table = new HTable(config, Customer.TABLE);

        for(int i=0; i<districtIds.length;i++) {
            byte[] startKey = buildStartKey(warehouseId,districtIds[i]);
            byte[] endKey = buildEndKey(warehouseId,districtIds[i]);
            Scan scan = new Scan(startKey,endKey);
            ResultScanner rs = table.getScanner(scan);
            Result res = rs.next();
            while (res!=null && !res.isEmpty()){
                users.add(Integer.parseInt(new String(res.getValue(TABLE_FAMILY_ID, Customer.C_ID),"UTF-8")));
                res = rs.next();
            }
        }
        table.close();
        return users;
    }

    public static void main(String[] args) throws IOException {
        setupLogger();
        logger.info("Starting Logger!! Time: "+ getCurrentTimeDate());
        if(args.length<2){
            System.out.println("Error: \n1)ZK_HOST:ZK_PORT, \n2)action [createTables, loadTables, query1, query2, query3, query4], \n3)Extra parameters for loadTables and queries:\n" +
                    "\ta) If loadTables: csvsFolder.\n " +
                    "\tb) If query1: warehouseId districtId startDate endData.\n  " +
                    "\tc) If query2: warehouseId districtId customerId listOfDiscounts.\n  " +
                    "\td) If query3: warehouseId districtId customerId.\n  " +
                    "\te) If query4: warehouseId listOfdistrictId.\n  ");
            System.exit(-1);
        }
        HBaseTPCC hBaseTPCC = new HBaseTPCC(args[0]);
        if(args[1].toUpperCase().equals("CREATETABLES")){
            hBaseTPCC.createTPCCTables();
        }
        else if(args[1].toUpperCase().equals("LOADTABLES")){
            if(args.length!=3){
                System.out.println("Error: 1) ZK_HOST:ZK_PORT, 2)action [createTables, loadTables], 3)csvsFolder");
                System.exit(-1);
            }
            else if(!(new File(args[2])).isDirectory()){
                System.out.println("Error: Folder "+args[2]+" does not exist.");
                System.exit(-2);
            }
            hBaseTPCC.loadTables(args[2]);
        }
        else if(args[1].toUpperCase().equals("QUERY1")){
            if(args.length!=6){
                System.out.println("Error: 1) ZK_HOST:ZK_PORT, 2)query1, " +
                        "3) warehouseId 4) districtId 5) startDate 6) endData");
                System.exit(-1);
            }

            List<String> customerIds = hBaseTPCC.query1(args[2], args[3], args[4], args[5]);
            System.out.println("There are "+customerIds.size()+" customers that order products from warehouse "+args[2]+" of district "+args[3]+" during after the "+args[4]+" and before the "+args[5]+".");
            System.out.println("The list of customers is: "+Arrays.toString(customerIds.toArray(new String[customerIds.size()])));
        }
        else if(args[1].toUpperCase().equals("QUERY2")){
            if(args.length!=6){
                System.out.println("Error: 1) ZK_HOST:ZK_PORT, 2)query2, " +
                        "3) warehouseId 4) districtId 5) customerId 6) listOfDiscounts");
                System.exit(-1);
            }
            hBaseTPCC.query2(args[2], args[3], args[4], args[5].split(","));
        }
        else if(args[1].toUpperCase().equals("QUERY3")){
            if(args.length!=5){
                System.out.println("Error: 1) ZK_HOST:ZK_PORT, 2) query3, " +
                        "3) warehouseId 4) districtId 5) customerId");
                System.exit(-1);
            }
            String[] discounts = hBaseTPCC.query3(args[2], args[3], args[4]);
            System.out.println("The last 4 discounts obtained from Customer "+args[4]+" of warehouse "+args[2]+" of district "+args[3]+" are: "+Arrays.toString(discounts));
        }
        else if(args[1].toUpperCase().equals("QUERY4")){
            if(args.length!=4){
                System.out.println("Error: 1) ZK_HOST:ZK_PORT, 2) query3, " +
                        "3) warehouseId 4) listOfDistrictIds");
                System.exit(-1);
            }
            System.out.println("There are "+hBaseTPCC.query4(args[2], args[3].split(","))+" customers that belong to warehouse "+args[2]+" of districts "+args[3]+".");
        }
        else{
            System.out.println("Error: 1) ZK_HOST:ZK_PORT, 2)action [createTables, loadTables, query1, query2, query3, query4], 3)Extra parameters for loadTables and queries:" +
                    "a) If loadTables: csvsFolder." +
                    "b) If query1: warehouseId districtId startDate endData" +
                    "c) If query2: warehouseId districtId customerId listOfDiscounts" +
                    "d) If query3: warehouseId districtId customerId " +
                    "e) If query4: warehouseId listOfdistrictId");
            System.exit(-1);
        }

    }

    private static void setupLogger() throws IOException {
        // This block configure the logger with handler and formatter
        fh = new FileHandler("MyHBaseFile.log",true);
        logger.addHandler(fh);
        SimpleFormatter formatter = new SimpleFormatter();
        fh.setFormatter(formatter);
        logger.setUseParentHandlers(false);
    }

    private static String getCurrentTimeDate(){
        return dateFormat.format(new Date());
    }

    private static byte[] stringToByteTimestamp(String time) {
        long inter = Timestamp.valueOf(time).getTime();
        return String.valueOf(inter).getBytes();
    }


    private byte[] buildEndKey(String C_W_ID, String C_D_ID) {
        return Customer.getKey(C_W_ID, C_D_ID, "90000");
    }

    private byte[] buildStartKey(String C_W_ID, String C_D_ID) {
        return Customer.getKey(C_W_ID, C_D_ID, "1");
    }
    private static void writeToLog(String s, Status info) {
        if(info == Status.INFO){
            logger.info(s+" - "+ getCurrentTimeDate());
        }else if(info == Status.WARNING){
            logger.warning(s+" - "+ getCurrentTimeDate());
        }else{
            logger.severe(s+" - "+ getCurrentTimeDate());
        }
    }

    public static Put createCustomerTable(String C_ID, String C_W_ID, String C_D_ID, String C_FIRST,
                                          String C_MIDDLE, String C_LAST, String C_STREET_1, String C_STREET_2,
                                          String C_CITY, String C_STATE, String C_ZIP, String C_PHONE,
                                          String C_SINCE , String C_CREDIT, String C_CREDITLIM, String C_DISCOUNT,
                                          String C_BALANCE, String C_YTD_PAYMENT, String C_PAYMENT_CNT, String C_DELIVERY_CNT, String C_DATA){

        Put put = new Put(Customer.getKey(C_W_ID, C_D_ID, C_ID));

        put.add(TABLE_FAMILY_ID, Customer.C_ID, C_ID.getBytes());
        put.add(TABLE_FAMILY_ID, Customer.C_W_ID, C_W_ID.getBytes());
        put.add(TABLE_FAMILY_ID, Customer.C_D_ID, C_D_ID.getBytes());

        put.add(TABLE_FAMILY_TEXT, Customer.C_FIRST, C_FIRST.getBytes());
        put.add(TABLE_FAMILY_TEXT, Customer.C_MIDDLE, C_MIDDLE.getBytes());
        put.add(TABLE_FAMILY_TEXT, Customer.C_LAST, C_LAST.getBytes());
        put.add(TABLE_FAMILY_TEXT, Customer.C_STREET_1, C_STREET_1.getBytes());
        put.add(TABLE_FAMILY_TEXT, Customer.C_STREET_2, C_STREET_2.getBytes());
        put.add(TABLE_FAMILY_TEXT, Customer.C_CITY, C_CITY.getBytes());
        put.add(TABLE_FAMILY_TEXT, Customer.C_STATE, C_STATE.getBytes());
        put.add(TABLE_FAMILY_TEXT, Customer.C_ZIP, C_ZIP.getBytes());
        put.add(TABLE_FAMILY_TEXT, Customer.C_PHONE, C_PHONE.getBytes());
        put.add(TABLE_FAMILY_TEXT, Customer.C_CREDIT, C_CREDIT.getBytes());
        put.add(TABLE_FAMILY_TEXT, Customer.C_DATA, C_DATA.getBytes());

        put.add(TABLE_FAMILY_NUMERIC, Customer.C_SINCE, stringToByteTimestamp(C_SINCE));
        put.add(TABLE_FAMILY_NUMERIC, Customer.C_CREDITLIM, C_CREDITLIM.getBytes());
        put.add(TABLE_FAMILY_NUMERIC, Customer.C_DISCOUNT, C_DISCOUNT.getBytes());
        put.add(TABLE_FAMILY_NUMERIC, Customer.C_BALANCE, C_BALANCE.getBytes());
        put.add(TABLE_FAMILY_NUMERIC, Customer.C_YTD_PAYMENT, C_YTD_PAYMENT.getBytes());
        put.add(TABLE_FAMILY_NUMERIC, Customer.C_PAYMENT_CNT, C_PAYMENT_CNT.getBytes());
        put.add(TABLE_FAMILY_NUMERIC, Customer.C_DELIVERY_CNT, C_DELIVERY_CNT.getBytes());


        return put;
    }

    public static Put createDistrictTable(String D_ID, String D_W_ID, String D_NAME, String D_STREET_1,
                                          String D_STREET_2, String D_CITY, String D_STATE, String D_ZIP,
                                          String D_TAX, String D_YTD, String D_NEXT_O_ID){

        Put put = new Put(District.getKey(D_ID, D_W_ID));
        put.add(TABLE_FAMILY_ID, District.D_ID, D_ID.getBytes());
        put.add(TABLE_FAMILY_ID, District.D_W_ID, D_W_ID.getBytes());
        put.add(TABLE_FAMILY_TEXT, District.D_NAME, D_NAME.getBytes());
        put.add(TABLE_FAMILY_TEXT, District.D_STREET_1, D_STREET_1.getBytes());
        put.add(TABLE_FAMILY_TEXT, District.D_STREET_2, D_STREET_2.getBytes());
        put.add(TABLE_FAMILY_TEXT, District.D_CITY, D_CITY.getBytes());
        put.add(TABLE_FAMILY_TEXT, District.D_STATE, D_STATE.getBytes());
        put.add(TABLE_FAMILY_TEXT, District.D_ZIP, D_ZIP.getBytes());
        put.add(TABLE_FAMILY_NUMERIC, District.D_TAX, D_TAX.getBytes());
        put.add(TABLE_FAMILY_NUMERIC, District.D_YTD, D_YTD.getBytes());
        put.add(TABLE_FAMILY_NUMERIC, District.D_NEXT_O_ID, D_NEXT_O_ID.getBytes());
        return put;
    }


    public static Put createHistoryTable(String H_C_ID, String H_C_D_ID, String H_C_W_ID, String H_D_ID, String H_W_ID, String H_DATE, String H_AMOUNT, String H_DATA){

        Put put = new Put(History.getKey(H_C_W_ID, H_C_D_ID, H_C_ID, H_DATA));

        put.add(TABLE_FAMILY_TEXT, History.H_C_ID, H_C_ID.getBytes());
        put.add(TABLE_FAMILY_TEXT, History.H_C_D_ID, H_C_D_ID.getBytes());
        put.add(TABLE_FAMILY_TEXT, History.H_C_W_ID, H_C_W_ID.getBytes());
        put.add(TABLE_FAMILY_TEXT, History.H_D_ID, H_D_ID.getBytes());
        put.add(TABLE_FAMILY_TEXT, History.H_W_ID, H_W_ID.getBytes());
        put.add(TABLE_FAMILY_TEXT, History.H_DATA, H_DATA.getBytes());

        put.add(TABLE_FAMILY_NUMERIC, History.H_DATE, stringToByteTimestamp(H_DATE));
        put.add(TABLE_FAMILY_NUMERIC, History.H_AMOUNT, H_AMOUNT.getBytes());

        return put;
    }

    public static Put createItemTable(String I_ID, String I_IM_ID, String I_NAME, String I_PRICE, String I_DATA){

        Put put = new Put(I_ID.getBytes());

        put.add(TABLE_FAMILY_ID, Item.I_ID, I_ID.getBytes());

        put.add(TABLE_FAMILY_TEXT, Item.I_IM_ID, I_IM_ID.getBytes());
        put.add(TABLE_FAMILY_TEXT, Item.I_NAME, I_NAME.getBytes());
        put.add(TABLE_FAMILY_TEXT, Item.I_DATA, I_DATA.getBytes());

        put.add(TABLE_FAMILY_NUMERIC, Item.I_PRICE, I_PRICE.getBytes());

        return put;
    }

    public static Put createNewOrderTable(String NO_O_ID, String NO_D_ID, String NO_W_ID){

        Put put = new Put(NewOrder.getKey(NO_W_ID, NO_D_ID, NO_O_ID));
        put.add(TABLE_FAMILY_ID, NewOrder.NO_O_ID, NO_O_ID.getBytes());
        put.add(TABLE_FAMILY_ID, NewOrder.NO_D_ID, NO_D_ID.getBytes());
        put.add(TABLE_FAMILY_ID, NewOrder.NO_W_ID, NO_W_ID.getBytes());

        return put;
    }

    public static Put createOrderLineTable(String OL_O_ID, String OL_D_ID, String OL_W_ID, String OL_NUMBER,
                                           String OL_I_ID, String OL_SUPPLY_W_ID, String OL_DELIVERY_D,
                                           String OL_QUANTITY, String OL_AMOUNT, String OL_DIST_INFO){

        Put put = new Put(OrderLine.getKey(OL_W_ID, OL_D_ID, OL_O_ID, OL_NUMBER));

        put.add(TABLE_FAMILY_ID, OrderLine.OL_O_ID, OL_O_ID.getBytes());
        put.add(TABLE_FAMILY_ID, OrderLine.OL_D_ID, OL_D_ID.getBytes());
        put.add(TABLE_FAMILY_ID, OrderLine.OL_W_ID, OL_W_ID.getBytes());
        put.add(TABLE_FAMILY_ID, OrderLine.OL_NUMBER, OL_NUMBER.getBytes());

        put.add(TABLE_FAMILY_TEXT, OrderLine.OL_I_ID, OL_I_ID.getBytes());
        put.add(TABLE_FAMILY_TEXT, OrderLine.OL_SUPPLY_W_ID, OL_SUPPLY_W_ID.getBytes());
        put.add(TABLE_FAMILY_TEXT, OrderLine.OL_DELIVERY_D, stringToByteTimestamp(OL_DELIVERY_D));
        put.add(TABLE_FAMILY_TEXT, OrderLine.OL_DIST_INFO, OL_DIST_INFO.getBytes());

        put.add(TABLE_FAMILY_NUMERIC, OrderLine.OL_QUANTITY, OL_QUANTITY.getBytes());
        put.add(TABLE_FAMILY_NUMERIC, OrderLine.OL_AMOUNT, OL_AMOUNT.getBytes());
        return put;
    }

    public static Put createOrdersTable(String O_ID, String O_D_ID, String O_W_ID, String O_C_ID, String O_ENTRY_D, String O_CARRIER_ID, String O_OL_CNT, String O_ALL_LOCAL){

        Put put = new Put(Orders.getKey(O_W_ID, O_D_ID, O_ID));

        put.add(TABLE_FAMILY_ID, Orders.O_ID, O_ID.getBytes());
        put.add(TABLE_FAMILY_ID, Orders.O_D_ID, O_D_ID.getBytes());
        put.add(TABLE_FAMILY_ID, Orders.O_W_ID, O_W_ID.getBytes());

        put.add(TABLE_FAMILY_TEXT, Orders.O_C_ID, O_C_ID.getBytes());
        put.add(TABLE_FAMILY_TEXT, Orders.O_CARRIER_ID, O_CARRIER_ID.getBytes());

        put.add(TABLE_FAMILY_NUMERIC, Orders.O_ENTRY_D, stringToByteTimestamp(O_ENTRY_D));
        put.add(TABLE_FAMILY_NUMERIC, Orders.O_OL_CNT, O_OL_CNT.getBytes());
        put.add(TABLE_FAMILY_NUMERIC, Orders.O_ALL_LOCAL, O_ALL_LOCAL.getBytes());

        return put;
    }

    public static Put createStockTable(String S_I_ID, String S_W_ID, String S_QUANTITY,
                                       String S_DIST_01, String S_DIST_02, String S_DIST_03, String S_DIST_04,
                                       String S_DIST_05, String S_DIST_06, String S_DIST_07, String S_DIST_08,
                                       String S_DIST_09, String S_DIST_10, String S_YTD, String S_ORDER_CNT, String S_REMOTE_CNT, String S_DATA){

        Put put = new Put(Stock.getKey(S_W_ID, S_I_ID));

        put.add(TABLE_FAMILY_ID, Stock.S_I_ID, S_I_ID.getBytes());
        put.add(TABLE_FAMILY_ID, Stock.S_W_ID, S_W_ID.getBytes());

        put.add(TABLE_FAMILY_TEXT, Stock.S_DIST_01, S_DIST_01.getBytes());
        put.add(TABLE_FAMILY_TEXT, Stock.S_DIST_02, S_DIST_02.getBytes());
        put.add(TABLE_FAMILY_TEXT, Stock.S_DIST_03, S_DIST_03.getBytes());
        put.add(TABLE_FAMILY_TEXT, Stock.S_DIST_04, S_DIST_04.getBytes());
        put.add(TABLE_FAMILY_TEXT, Stock.S_DIST_05, S_DIST_05.getBytes());
        put.add(TABLE_FAMILY_TEXT, Stock.S_DIST_06, S_DIST_06.getBytes());
        put.add(TABLE_FAMILY_TEXT, Stock.S_DIST_07, S_DIST_07.getBytes());
        put.add(TABLE_FAMILY_TEXT, Stock.S_DIST_08, S_DIST_08.getBytes());
        put.add(TABLE_FAMILY_TEXT, Stock.S_DIST_09, S_DIST_09.getBytes());
        put.add(TABLE_FAMILY_TEXT, Stock.S_DIST_10, S_DIST_10.getBytes());
        put.add(TABLE_FAMILY_TEXT, Stock.S_DATA, S_DATA.getBytes());

        put.add(TABLE_FAMILY_NUMERIC, Stock.S_YTD, S_YTD.getBytes());
        put.add(TABLE_FAMILY_NUMERIC, Stock.S_ORDER_CNT, S_ORDER_CNT.getBytes());
        put.add(TABLE_FAMILY_NUMERIC, Stock.S_REMOTE_CNT, S_REMOTE_CNT.getBytes());
        put.add(TABLE_FAMILY_NUMERIC, Stock.S_QUANTITY, S_QUANTITY.getBytes());

        return put;
    }

    public static Put createWarehouseTable(String W_ID, String W_NAME, String W_STREET_1, String W_STREET_2, String W_CITY, String W_STATE, String W_ZIP, String W_TAX, String W_YTD){
        Put put = new Put(W_ID.getBytes());

        put.add(TABLE_FAMILY_ID, Warehouse.W_ID, W_ID.getBytes());

        put.add(TABLE_FAMILY_TEXT, Warehouse.W_NAME, W_NAME.getBytes());
        put.add(TABLE_FAMILY_TEXT, Warehouse.W_STREET_1, W_STREET_1.getBytes());
        put.add(TABLE_FAMILY_TEXT, Warehouse.W_STREET_2, W_STREET_2.getBytes());
        put.add(TABLE_FAMILY_TEXT, Warehouse.W_CITY, W_CITY.getBytes());
        put.add(TABLE_FAMILY_TEXT, Warehouse.W_STATE, W_STATE.getBytes());
        put.add(TABLE_FAMILY_TEXT, Warehouse.W_ZIP, W_ZIP.getBytes());

        put.add(TABLE_FAMILY_NUMERIC, Warehouse.W_TAX, W_TAX.getBytes());
        put.add(TABLE_FAMILY_NUMERIC, Warehouse.W_YTD, W_YTD.getBytes());

        return put;
    }



}




























