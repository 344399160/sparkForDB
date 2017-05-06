package label.utils;

import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

import java.io.File;
import java.util.Iterator;

/**
 * 描述：配置文件解析
 * author qiaobin   2017/4/28 14:18.
 */
public class DBParserUtil {

    /**
     * 功能描述：解析hbase-site.xml
     * @author qiaobin
     * @date 2017/4/28  14:21
     * @param file xml文件地址
     * @param name 要解析的名称
     */
    public static String parseHbaseXml(File file, String name) throws DocumentException{
        SAXReader saxReader = new SAXReader();
        Document document = saxReader.read(file);
        Element root = document.getRootElement();
        Iterator<?> it = root.elementIterator();
        while (it.hasNext()) {
            Element node = (Element) it.next();
            if (name.equals(node.element("name").getData().toString())) {
                return node.element("value").getData().toString();
            }
        }
        return null;
    }


}
