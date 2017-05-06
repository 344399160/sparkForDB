package label.common;

/**
 * 描述：版本控制器
 * author qiaobin   2017/4/6 11:17.
 */
public class VersionGenerator {

    /**
     * 功能描述：生成当前时间作为当前版本
     * @author qiaobin
     * @param
     */
    public synchronized static long generateVersion() {
        return System.currentTimeMillis();
    }

}
