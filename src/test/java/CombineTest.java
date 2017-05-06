import label.client.StoreClient;
import label.combine.Builders;
import label.combine.SearchConstructor;
import label.hbase.HBaseClient;
import label.model.Entity;
import label.model.LabelEntity;
import label.model.LabelResult;
import org.apache.commons.collections.CollectionUtils;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * 描述：TODO
 * author qiaobin   2017/4/27 09:42.
 */
public class CombineTest {

    //新建对象
    @Test
    public void createObject() {
        try {
            StoreClient client = new StoreClient();
            client.init();
            client.createObject("qiaobin");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //删除对象
    @Test
    public void deleteObject() {
        try {
            StoreClient client = new StoreClient();
            client.init();
            client.deleteObject("qiaobin");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //插入实体 （测试执行两次，生成两个版本数据）
    @Test
    public void insertEntity() {
        try {
            StoreClient client = new StoreClient();
            client.init();
            Map<String, Object> entity = new HashMap<>();
            entity.put("userName", "bbbb");
            entity.put("age", 22);
            entity.put("addr", "青岛1");
            Map<String, Object> entity1 = new HashMap<>();
            entity1.put("userName", "qqq");
            entity1.put("age", 33);
            entity1.put("addr", "宁波1");
//            Map<String, Object> entity2 = new HashMap<>();
//            entity2.put("userName", "ff");
//            entity2.put("age", 19);
//            entity2.put("addr", "大连");
//            Map<String, Object> entity3 = new HashMap<>();
//            entity3.put("userName", "版本");
//            entity3.put("age", 11);
//            entity3.put("addr", "去去去");
            long timestamp = System.currentTimeMillis();
            client.insertEntity("combine", "1009", entity, timestamp);
            client.insertEntity("combine", "1010", entity1, timestamp);
//            client.insertEntity("combine", "1005", entity2, timestamp);
//            client.insertEntity("combine", "1008", entity3, timestamp);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //插入实体 （测试执行两次，生成两个版本数据）
    @Test
    public void bulkInsertEntity() {
        try {
            StoreClient client = new StoreClient();
            client.init();
            client.deleteObject("combine");
            client.createObject("combine");
            Map<String, Object> entity = new HashMap<>();
            entity.put("userName", "bbbb");
            entity.put("age", 22);
            entity.put("addr", "青岛1");
            Map<String, Object> entity1 = new HashMap<>();
            entity1.put("userName", "qqq");
            entity1.put("age", 33);
            entity1.put("addr", "宁波1");
            long timestamp = System.currentTimeMillis();
            LabelEntity l1 = new LabelEntity();
            l1.setRowKey("1");
            l1.setEntity(entity);
            LabelEntity l2 = new LabelEntity();
            l2.setRowKey("2");
            l2.setEntity(entity1);
            List<LabelEntity> list = new ArrayList<>();
            list.add(l1);
            list.add(l2);
            client.bulkInsertEntity("combine", list, timestamp);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    @Test
    public void updateEntity() {
        try {
            StoreClient client = new StoreClient();
            client.init();
            Map<String, Object> entity = new HashMap<>();
            entity.put("userName", "ttt");
            entity.put("age", 21);
            entity.put("addr", "沈阳");
        } catch (Exception e) {
        }
    }

    @Test
    public void deleteEntity() {
        try {
            StoreClient client = new StoreClient();
            client.init();
            client.deleteEntity("combine", "20170427-1", "1008", Long.parseLong("1493276032329"));
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @Test
    public void bulkDeleteEntity() {
        try {
            StoreClient client = new StoreClient();
            client.init();
            List<String> list = new ArrayList<>();
            list.add("1007");
            list.add("1006");
            client.bulkDeleteEntity("combine", "20170427-1", list, Long.parseLong("1493258394788"));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //条件查询
    @Test
    public void pageQuery() {
        try {
            StoreClient client = new StoreClient();
            client.init();
            SearchConstructor constructor = new SearchConstructor();
            constructor.setSize(2);
            constructor.add(new Builders().equal("userName", "qqq"));
            LabelResult labelResult = client.search("combine", constructor, Long.parseLong("1494037136327"));
            if (CollectionUtils.isNotEmpty(labelResult.getEntityList()))
            for (Entity entity : labelResult.getEntityList()) {
                System.out.println(entity);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //条件查询
    @Test
    public void query() {
        try {
            StoreClient client = new StoreClient();
            client.init();
            SearchConstructor constructor = new SearchConstructor();
            constructor.add(new Builders().equal("age", 27).equal("userName", "qb"));
            constructor.setSize(5);
            constructor.setFrom(0);
            constructor.setStartEntity("1001");
            LabelResult labelResult = client.search("combine", constructor, Long.parseLong("1493258472776"));
            if (CollectionUtils.isNotEmpty(labelResult.getEntityList())) {
                for (Entity entity : labelResult.getEntityList()) {
                    System.out.println(entity);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void searchById() {
        try {
            StoreClient client = new StoreClient();
            client.init();
            List<String> idList = new ArrayList<>();
            idList.add("1001");
            idList.add("1002");
            LabelResult result = client.search("combine", idList, Long.parseLong("1493258472776"));

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void count() {
        HBaseClient hBaseClient = new HBaseClient("172.16.9.42", "2181");
        try {
            hBaseClient.count("combine", Long.parseLong("1493258394788"), Long.parseLong("1493346844495"));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}
