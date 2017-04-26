package Hbase;

import label.hbase.HBaseService;
import org.junit.Test;

import java.io.IOException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Map;

/**
 * 描述：TODO
 * author qiaobin   2017/4/14 15:14.
 */
public class HBaseTest {

    @Test
    public void test() {
        HBaseService service = new HBaseService("172.16.9.42", "2181");
        service.dropTable("qiaobin");
        service.createTable("qiaobin",  "default");
    }

    @Test
    public void insert() {
        HBaseService service = new HBaseService("172.16.9.42", "2181");
        try {
            service.deleteRow("versiontest", "1");
            service.deleteRow("versiontest", "2");
            long ts1 = System.currentTimeMillis();
            service.insertRow("versiontest", "2", "default", "a", "b", ts1);
            service.insertRow("versiontest", "2", "default", "version", "1", ts1);
            long tss = System.currentTimeMillis();
            service.insertRow("versiontest", "2", "default", "a", "b", tss);
            service.insertRow("versiontest", "2", "default", "version", "2", tss);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void search() {
        HBaseService service = new HBaseService("172.16.9.42", "2181");
        try {
            Map<String, String> result = service.getRowData("qiaobin", "bad", "default", Long.parseLong("20170418141512"));
            System.out.println(result);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void tsToString() {
        Timestamp ts = new Timestamp(Long.parseLong("1492224680375"));
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd HHmmss");
        try {
            String s = sdf.format(ts);
            System.out.println(s);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    //get 'qiaobin','17',{COLUMN=>'default:titile',VERSIONS=>10}     获取一个字段下的多版本数据
    //get 'qiaobin','994',{COLUMN=>'default:Tag',TIMESTAMP=>1492224680375}
}
