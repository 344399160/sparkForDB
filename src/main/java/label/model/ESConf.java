package label.model;

import org.apache.commons.lang.StringUtils;

/**
 * 描述：ES 配置
 * author qiaobin   2017/4/14 11:34.
 */
public class ESConf{

    //索引
    private String index;

    //ip
    private String nodes;

    //port
    private String clientPort = "9300";

    //port
    private String hadoopPort = "9200";

    //集群名
    private String clusterName;

    //检验字段是否有空
    public boolean check() {
        if (StringUtils.isNotEmpty(index) && StringUtils.isNotEmpty(nodes))
            return true;
        else
            return false;
    }

    public String getIndex() {
        return index;
    }

    public ESConf setIndex(String index) {
        this.index = index;
        return this;
    }

    public String getNodes() {
        return nodes;
    }

    public ESConf setNodes(String nodes) {
        this.nodes = nodes;
        return this;
    }

    public String getClusterName() {
        return clusterName;
    }

    public ESConf setClusterName(String clusterName) {
        this.clusterName = clusterName;
        return this;
    }

    public String getClientPort() {
        return clientPort;
    }

    public void setClientPort(String clientPort) {
        this.clientPort = clientPort;
    }

    public String getHadoopPort() {
        return hadoopPort;
    }

    public void setHadoopPort(String hadoopPort) {
        this.hadoopPort = hadoopPort;
    }
}
