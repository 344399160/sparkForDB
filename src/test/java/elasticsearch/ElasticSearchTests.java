package elasticsearch;

import com.fasterxml.jackson.core.JsonProcessingException;
import label.client.StoreClient;
import label.common.VersionGenerator;
import label.elasticsearch.ESClient;
import label.elasticsearch.ESQueryBuilders;
import label.elasticsearch.ESQueryConstructor;
import label.model.LabelResult;
import label.utils.Json;
import org.apache.commons.lang.time.DateUtils;
import org.junit.Test;

import java.util.*;

public class ElasticSearchTests {

    @Test
    public void test() {
        try {
            ESClient service = new ESClient("es", "192.168.40.128", 9300);
            List<String> list = new ArrayList<String>();
            list.add("AVqpSIK0r8R1pmANP8Ji");
            list.add("ididid1");
            service.bulkDeleteData("test", "test", list);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void search() {
        ESClient service = new ESClient("es", "192.168.40.128", 9300);
        ESQueryConstructor constructor = new ESQueryConstructor();
        constructor.setSize(15);  //查询返回条数，最大 10000
        constructor.setFrom(11);  //分页查询条目起始位置， 默认0
        constructor.must(new ESQueryBuilders().range("col", "1", "10").term("col2", "20"));
        constructor.setAsc("");
        LabelResult search = service.search("bank", constructor, "account");
        System.out.println(search);
    }

    @Test
    public void getTypes() {
        Date d = new Date();
        Date a = DateUtils.addDays(d, -1);
        Calendar cal = Calendar.getInstance();
        cal.setTime(a);
        cal.add(Calendar.WEEK_OF_YEAR, -1);// 一周
        cal.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY);

        Date now = new Date();
        Date before = cal.getTime();

        ESClient service = new ESClient("es", "192.168.40.128", 9300);
        List<String> list = service.getTypes("label", before.getTime(), now.getTime());
        for (String s : list) {
            System.out.println(s);
        }
    }

    @Test
    public void addType() {
        ESClient service = new ESClient("es", "172.16.2.3", 9300);
        service.createIndex("version");
    }

    @Test
    public void exist() {
        ESClient service = new ESClient("es", "172.16.2.3", 9300);
        boolean a = service.indexExist("qiaobin");
        boolean b = service.indexExist("test");
        System.out.println(a);
        System.out.println(b);
    }

    @Test
    public void getIndexDocumentCount() {
        ESClient service = new ESClient("es", "172.16.2.3", 9300);
        System.out.println(service.getTotalCount("qiaobin"));
    }

    @Test
    public void getIndexDocumentCount1() {
        ESClient service = new ESClient("es", "172.16.2.3", 9300);
        System.out.println(service.getTotalCount("qiaobin", "20170418-1"));
    }

    @Test
    public void insertData() {
//        ESClient service = new ESClient("es", "192.168.40.128", 9300);
        ESClient service = new ESClient("es", "172.16.2.3", 9300);
        Map<String, String> map = new HashMap<String, String>();
        map.put("name", "qb1");
        map.put("age", "231");
        map.put("sex", null);
        map.put("version", "2");

        try {
            service.insertData("label", "20170406-1", Json.toJsonString(map));
//            service.insertData("label", "20170405-2", Json.toJsonString(map));
//            service.insertData("label", "20170405-3", Json.toJsonString(map));
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void search1() {
//        ESClient service = new ESClient("es", "192.168.40.128", 9300);
        ESClient service = new ESClient("es", "172.16.2.3", 9300);
        ESQueryConstructor constructor = new ESQueryConstructor();
        constructor.must(new ESQueryBuilders().term("age", "265"));
        LabelResult label = service.search("label", constructor, "20170406-1");
    }

    @Test
    public void version() {

        System.out.println(VersionGenerator.generateVersion());
    }

    @Test
    public void disableIndex() {
        ESClient service = new ESClient("es", "192.168.40.128", 9300);
//        service.createIndex("dis");
        service.disabledIndex("dis");
    }

    @Test
    public void deleteIndex() {
        ESClient service = new ESClient("es", "192.168.40.128", 9300);
        service.deleteIndex("label");
        service.deleteIndex("label");
    }

    @Test
    public void addIndex() {
        ESClient service = new ESClient("es", "172.16.2.3", 9300);
        service.createIndex("tesssss");
    }

    @Test
    public void stats() {
        ESClient service = new ESClient("es", "172.16.2.3", 9300);
//        Map<Object, Object> map = service.statSearch(new ESQueryConstructor(), "version", "qiaobin", "20170418-1");
        LabelResult list = service.search("qiaobin", new ESQueryConstructor(), "20170418-1");
        System.out.println(list);
    }

    @Test
    public void getById() {
        ESClient service = new ESClient("es", "172.16.2.3", 9300);
        Map<String, Object> map = service.get("qiaobin", "20170425-1", "AVujTznZ2zpbozxYt-tK");
        System.out.println(map);
    }

    @Test
    public void percent() {
        try {
            StoreClient client = new StoreClient();
            client.init();
            client.labelCoverageRate("label", "sex");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}
