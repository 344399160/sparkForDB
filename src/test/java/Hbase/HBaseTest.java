package Hbase;

import label.combine.SearchConstructor;
import label.hbase.HBaseClient;
import label.model.LabelResult;
import label.utils.ConstUtil;
import label.utils.Json;
import org.junit.Test;

import java.io.IOException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

/**
 * 描述：TODO
 * author qiaobin   2017/4/14 15:14.
 */
public class HBaseTest {

    @Test
    public void test() {
        HBaseClient service = new HBaseClient("172.16.9.42", "2181");
        service.dropTable("qiaobin");
        service.createTable("qiaobin",  "default");
    }

    @Test
    public void insert() {
        HBaseClient service = new HBaseClient("172.16.9.42", "2181");
        try {
            long ts1 = System.currentTimeMillis();
            service.insertRow("qiaobin", "1", "default", "userid", "b", ts1);
            service.insertRow("qiaobin", "1", "default", "genres", "1", ts1);
            service.insertRow("qiaobin", "2", "default", "userid", "b", ts1);
            service.insertRow("qiaobin", "2", "default", "genres", "2", ts1);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void search() {
        HBaseClient service = new HBaseClient("172.16.9.42", "2181");
        try {
            List<String> list = new ArrayList<>();
            list.add("1");
            list.add("2");
            LabelResult rowData = service.getRowData("qiaobin", list, "default", 0);
            System.out.println(rowData);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    @Test
    public void search1() {
        HBaseClient service = new HBaseClient("172.16.9.42", "2181");
        try {
            SearchConstructor constructor = new SearchConstructor();
            constructor.setVersion(Long.parseLong("1493705671238"));
            LabelResult qiaobin = service.search("qiaobin", constructor, null);
            System.out.println(Json.toJsonString(qiaobin));
        } catch (Exception e) {
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

    @Test
    public void count() {
        long count;
        try {
            HBaseClient service = new HBaseClient("172.16.9.42", "2181");
            count = service.count("qiaobin", "title");
            System.out.println(count);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void get() {
        HBaseClient service = new HBaseClient("172.16.9.42", "2181");
        try {
            service.getRowData("qiaobin", "1", ConstUtil.COLUMNFAMILY_DEFAULT, 0);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }



    //get 'combine','1001',{COLUMN=>'default:userName',VERSIONS=>10}     获取一个字段下的多版本数据
    //get 'qiaobin','994',{COLUMN=>'default:Tag',TIMESTAMP=>1492224685}037
}
