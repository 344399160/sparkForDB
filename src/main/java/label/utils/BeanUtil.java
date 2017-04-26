package label.utils;

import org.apache.commons.lang.StringUtils;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.Map;

/**
 * 描述：实体工具类
 * author qiaobin   2016/10/11 16:25.
 */
public class BeanUtil {

    /**
      * 功能描述：对象转Map
      * @author qiaobin
      * @date 2016/10/11  16:26
      * @param obj 待转对象
      */
    public static Map<String, Object> objectToMap(Object obj) throws Exception {
        if(obj == null){
            return null;
        }
        Map<String, Object> map = new HashMap<String, Object>();
        Field[] declaredFields = obj.getClass().getDeclaredFields();
        Field[] superDeclaredFields =  obj.getClass().getSuperclass().getDeclaredFields();
        for (Field field : declaredFields) {
            field.setAccessible(true);
            map.put(field.getName(), field.get(obj));
        }
        for (Field field : superDeclaredFields) {
            field.setAccessible(true);
            map.put(field.getName(), field.get(obj));
        }
        return map;
    }

    /**
     * 功能描述：Map转对象
     * @author qiaobin
     * @date 2016/10/11  16:26
     * @param map
     * @param beanClass
     */
    public static Object mapToObject(Map<String, Object> map, Class<?> beanClass) throws Exception {
        if (map == null)
            return null;
        Object obj = beanClass.newInstance();
        Field[] fields = obj.getClass().getDeclaredFields();
        for (Field field : fields) {
            int mod = field.getModifiers();
            if(Modifier.isStatic(mod) || Modifier.isFinal(mod)){
                continue;
            }
            field.setAccessible(true);
            field.set(obj, map.get(field.getName()));
        }
        Field[] superFields = obj.getClass().getSuperclass().getDeclaredFields();
        for (Field field : superFields) {
            int mod = field.getModifiers();
            if(Modifier.isStatic(mod) || Modifier.isFinal(mod)){
                continue;
            }
            field.setAccessible(true);
            field.set(obj, map.get(field.getName()));
        }
        return obj;
    }

    /**
      * 功能描述：对象合并
      * @author qiaobin
      * @date 2016/10/11  16:30
      * @param combinedObj 被合并对象
      * @param obj 合并对象
      */
    public static Object combineObject(Object combinedObj, Object obj) throws Exception{
        Map<String, Object> combinedMap = objectToMap(combinedObj);
        Map<String, Object> objMap = objectToMap(obj);
        objMap.forEach((param1, param2) -> {
            if (combinedMap.containsKey(param1) && (param2 != null)) {
                //删除旧KEY
                combinedMap.remove(param1);
                combinedMap.put(param1, param2);
            }
        });
        return mapToObject(combinedMap, combinedObj.getClass());
    }

    /**
      * 功能描述：对象合并
      * @author qiaobin
      * @date 2016/10/11  16:30
      * @param combinedObj 被合并对象
      * @param objMap 合并对象
      */
    public static Object combineObject(Object combinedObj, Map<String, Object> objMap) throws Exception{
        Map<String, Object> combinedMap = objectToMap(combinedObj);
        objMap.forEach((param1, param2) -> {
            if (combinedMap.containsKey(param1) && (param2 != null && StringUtils.isNotEmpty(param2.toString()))) {
                //删除旧KEY
                combinedMap.remove(param1);
                combinedMap.put(param1, param2);
            }
        });
        return mapToObject(combinedMap, combinedObj.getClass());
    }

    /**
      * 功能描述：对象合并
      * @author qiaobin
      * @date 2016/10/11  16:30
      * @param combinedMap 被合并对象
      * @param objMap 合并对象
      */
    public static Map<String, Object> combineObject(Map<String, Object> combinedMap, Map<String, Object> objMap) throws Exception{
        objMap.forEach((param1, param2) -> {
            if (combinedMap.containsKey(param1) && (param2 != null && StringUtils.isNotEmpty(param2.toString()))) {
                //删除旧KEY
                combinedMap.remove(param1);
                combinedMap.put(param1, param2);
            }
        });
        return combinedMap;
    }



}
