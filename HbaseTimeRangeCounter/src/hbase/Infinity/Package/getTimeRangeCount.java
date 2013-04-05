package hbase.Infinity.Package;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
/**
 * Class to Find out the count of rows between the given time range.
 *
 * @author Shashwat Shriparv
 * @Email  dwivedishashwat@gmail.com
 */
public class getTimeRangeCount {
    public static Configuration conf = null;
    /**
     * Initialization
     */
    static String Zookeeper = null;
    static String port = null;

    /**
     * Then main function. Uses : ZookeeperAddress ZookeeperPort TableName
     * StartTime EndTime OptionForDataOrCount(data=1,count=2)
     *
     * @ZookeeperAddress : Address at which zookeeper is running
     * @ZookeeperPort : Port at which zookeeper is running
     * @TableName : Table name which has to be scaned
     * @StartTime : Start Time Interval
     * @EndTime : End Time Interval
     * @OptionForDataOrCount : Either 1 or 2. 1 for getting data, 2 for getting
     * count only
     */
    public static void main(String[] args) {
        // TODO Auto-generated method stub
        if (args.length < 6) {
            System.out.println("\nUses : ZookeeperAddress ZookeeperPort TableName StartTime EndTime OptionForDataOrCount(data=1,count=2)\n");
        } else {
            Zookeeper = args[0];
            port = args[1];
            String tableName = args[2];
            String sTimeLong = args[3];
            String eTimeLong = args[4];
            String optionDataOrCount = args[5];
            conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", Zookeeper);
            conf.set("hbase.zookeeper.property.clientPort", port);
            getRecordwithtimeRange(Long.parseLong(sTimeLong), Long.parseLong(eTimeLong), tableName, optionDataOrCount);
        }
            //getRecordwithtimeRange(Long.parseLong(args[1]),Long.parseLong(args[2]), "scores");
    }

     /**
     * Function to return the count of rows in time range.
     *
     * @param startTime : Start Time
     * @param endTime : End Time
     * @param tableName : Table name to scan
     * @param option : Option (1 or 2) data or count
     */
    public static void getRecordwithtimeRange(Long startTime, Long endTime,
            String tableName, String option) {
        int counter = 0;
        try {

            HTable table = new HTable(conf, tableName);
            Scan scan2 = new Scan();
            scan2.setTimeRange(startTime, endTime);
            ResultScanner scanner2 = table.getScanner(scan2);
            if (option.equals("2")) {
                for (Result result : scanner2) {
                    ++counter;
                    System.out.print(".");

                }
                System.out.println("Total Count of Records In Given Time Range is # " + counter);
            } else {
                System.out.println("The rows in the given time range :");
                for (Result result : scanner2) {

                    System.out.println(Bytes.toString(result.getRow()));

                }
            }


        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    /**
     * Scan Full Table and list data
     */
    public static void getAllRecord(String tableName) {
        try {
            HTable table = new HTable(conf, tableName);

            Scan s = new Scan();
            ResultScanner ss = table.getScanner(s);
            for (Result r : ss) {
                for (KeyValue kv : r.raw()) {
                    System.out.print(new String(kv.getRow()) + " ");
                    System.out.print(new String(kv.getFamily()) + ":");
                    System.out.print(new String(kv.getQualifier()) + " ");
                    System.out.print(kv.getTimestamp() + " ");
                    System.out.println(new String(kv.getValue()));
                    System.out.println(new String(String.valueOf(kv.getTimestamp())));

                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

   
}
