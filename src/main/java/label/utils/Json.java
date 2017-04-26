package label.utils;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


/**
 * 描述：Json 转换
 * author qiaobin   2016/9/6 13:56.
 */
public class Json {

    /**
     * 功能描述：将对象转为JSON字符串
     * @author qiaobin
     * @date 2016/9/6  14:53
     * @param object
     */
    public static String toJsonString (Object object) throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.writeValueAsString(object);
    }
    
    /**
     * <p>将对象转换为JSON字符串，在转换失败时，返回空串。
     * <p>该方法用在不是很严格的场景，如日志输出。调用客户端不需要捕获异常。
     * 
     * @param object
     * @author suzj 2016-11-02
     * @return
     */
    public static String toUnstrictJsonString(Object object) {
    	try {
    		return toJsonString(object);
    	} catch (Exception ignore) {
    		return "";
    	}
    }
    
    /**
     * <p>将JSON字符串转换为List对象
     * 
     * @param json		JSON字符串
     * @param clazz		List中元素的类型
     * @return
     * @throws JsonParseException
     * @throws JsonMappingException
     * @throws IOException
     */
    public static <T> List<T> parseArray(String json, Class<T> clazz) 
    		throws JsonParseException, JsonMappingException, IOException {
    	if (StringUtils.isEmpty(json)) {
    		return null;
    	}
    	ObjectMapper mapper = new ObjectMapper();
    	JavaType javaType = mapper.getTypeFactory().constructParametricType(ArrayList.class, clazz);
        return mapper.readValue(json, javaType);
    }

    /**
     * 功能描述：将JSON转为List<Object>
     * @author qiaobin
     * @date 2016/9/8  17:58
     * @param jsonString
     * @param clazz
     */
    public static List toObjectList(String jsonString, Class<?> clazz) throws Exception{
        ObjectMapper mapper = new ObjectMapper();
        List lst = null;
        try {
            JavaType javaType = mapper.getTypeFactory().constructParametricType(ArrayList.class, clazz);
            lst = mapper.readValue(jsonString, javaType);
        } catch (IOException e) {
            throw e;
        }
        return lst;
    }

    /**
     * 功能描述：JSON转Object
     * @author qiaobin
     * @date 2016/9/6  14:53
     * @param clazz
     * @param jsonString
     */
    public static Object toObject (String jsonString, Class<?> clazz) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        Object obj = mapper.readValue(jsonString, clazz);
        return obj;
    }
}
