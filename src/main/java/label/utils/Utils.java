package label.utils;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.List;

/**
 * 描述：工具类
 * author qiaobin   2017/4/26 10:58.
 */
public class Utils {

    /**
     * 功能描述：list 转 map
     * @author qiaobin
     * @date 2017/4/26  11:01
     * @param list
     */
    public static String[] toArray(List<String> list) {
        if (list.size() > 0) {
            String[] arr = new String[list.size()];
            for (int i = 0; i < list.size(); i++) {
                arr[i] = list.get(i);
            }
            return arr;
        }
        return null;
    }

    /**
     * 功能描述：对象数组转String数组
     * @author qiaobin
     * @date 2017/5/2  10:09
     * @param objs
     */
    public static String[] toStringArray(Object[] objs) {
        String[] str = new String[objs.length];
        for (int i = 0; i < objs.length; i++) {
            str[i] = objs[i].toString();
        }
        return str;
    }

    /**
     * 功能描述：根据时间戳转换成es type
     * @author qiaobin
     * @date 2017/5/2  9:44
     * @param version 版本
     */
    public static String tsToDateType(long version) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        Timestamp ts = new Timestamp(version);
        String typePrefix = sdf.format(ts);
        return typePrefix;
    }

}
