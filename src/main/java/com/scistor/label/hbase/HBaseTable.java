package com.scistor.label.hbase;

import com.scistor.label.common.MessageException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

/**
 * Created by ctt on 2016/11/7.
 */
public class HBaseTable {
    protected Configuration configuration = null;
    protected Connection connect = null;
    protected String zookeeperQuorum = null;
    protected String zookeeperClientPort = null;
    protected String hbaseRootdir = null;

    public HBaseTable(){

    }
    public HBaseTable(String hbaseconf) {
        String[] hbaseconfString = hbaseconf.split(";");
        this.setZookeeperQuorum(hbaseconfString[0]);
        this.setZookeeperClientPort(hbaseconfString[1]);
        this.setHbaseRootdir(hbaseconfString[2]);
        try {
            configuration = HBaseConfiguration.create();
            configuration.set("hbase.zookeeper.quorum", zookeeperQuorum);
            configuration.set("hbase.zookeeper.property.clientPort", zookeeperClientPort);
            configuration.set("hbase.rootdir", hbaseRootdir);
            connect = ConnectionFactory.createConnection(configuration);
        } catch (Exception e) {
            throw new MessageException(e);
        }
    }

    public void setZookeeperQuorum(String zookeeperQuorum){ this.zookeeperQuorum = zookeeperQuorum;}

    public String getZookeeperQuorum() {return zookeeperQuorum;}

    public void setZookeeperClientPort(String zookeeperClientPort) {this.zookeeperClientPort = zookeeperClientPort;}

    public String getZookeeperClientPort() { return zookeeperClientPort;}

    public void setHbaseRootdir(String hbaseRootdir){ this.hbaseRootdir = hbaseRootdir;}

    public String getHbaseRootdir() {return hbaseRootdir;}

}
