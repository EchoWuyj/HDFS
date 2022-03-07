package com.wyj.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import sun.security.krb5.Config;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @ClassName HdfsClient
 * @Author wyj
 * @DateTime 2021-10-21 16:35
 * @Version 1.0
 *
 * 客户端代码有国际通用步骤
 * 1.创建客户端对象--创建
 * 2.使用客户端对象进行一些操作--操作
 * 3.关闭客户端对象--关闭
 * jdbc hdfs zookeeper kafka
 */
public class HdfsClient {
    public static void main(String[] args) throws Exception {
        //TODO 1.创建客户端对象  因为是一个文件系统 创建文件系统对象
        //参数第一个 uri
        //参数第二个 conf
        //参数第三个 user
        //创建uri(java.net)

        //以下两种方式都行 选择其一即可
        //URI uri = new URI("hdfs://hadoop102:8020");
        URI uri = URI.create("hdfs://hadoop102:8020");

        //URI和URL区别
        //URI = Universal Resource Identifier 统一资源 标志符
        //URI采用一种特定语法 标识一个资源的字符串 它包含URL和URN
        //通过URI找到资源是通过对名称进行标识 这个名称在某命名空间中 并不代表网络地址

        //URL = Universal Resource Locator 统一资源 定位符
        //URL唯一地标识一个资源在Internet上的位置 不管用什么方法表示 只要能定位一个资源 就叫URL

        //创建conf对象 configuration为配置文件的抽象对象
        //new一个configuration对象 即已经获取所有的配置文件(4个)
        Configuration conf = new Configuration();
        //FS是hadoop包下的类
        FileSystem fs = FileSystem.get(uri, conf, "atguigu");

        //查看其实现类 结果为DistributedFileSystem
        //通过配置文件中的路径判断 hdfs://hadoop102:8020 选择DistributedFileSystem
        //若是 file:/// 选择LocalFileSystem

        //String name = fs.getClass().getName();
        //System.out.println("name = " + name);

        //TODO 2.使用客户端对象进行一些操作 创建键文件夹
        //new Path 也是hadoop下的包 在分布式系统的根目录下创建的/java
        fs.mkdirs(new Path("/java"));

        //TODO 3.关闭客户端
        fs.close();
    }
}
