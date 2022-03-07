package com.wyj.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.theories.suppliers.TestedOn;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Arrays;

/**
 * @ClassName HdfsClientDemo
 * @Author wyj
 * @DateTime 2021-10-21 20:41
 * @Version 1.0
 * <p>
 * 对代码进行解耦
 */
public class HdfsClientDemo {

    private FileSystem fs;
    private Configuration conf;

    //初始化方法创建客户端对象
    @Before //在test方法运行之前 执行一次
    public void init() throws IOException, InterruptedException {
        URI uri = URI.create("hdfs://hadoop102:8020");
        conf = new Configuration();
        //设置副本数量 以conf设置副本数量为准
        conf.set("dfs.replication", "2");
        //conf.set方法得在FileSystem.get之前
        fs = FileSystem.get(uri, conf, "atguigu");
    }

    //关闭放方法 关闭客户端对象
    @After //在test方法运行之后运行一次
    public void close() throws IOException {
        fs.close();
    }

    @Test
    public void mkdirs() throws IOException {
        fs.mkdirs(new Path("/java_1"));
    }

    //上传方法
    @Test
    public void put() throws IOException {
        /* 参数解读
         * 1:boolean delSrc 是否删除源文件 本地文件 即window上的文件
         * 2:boolean overwrite 是否覆盖目标文件 即hdfs文件  将目标文件删除 用新的文件替换)
         * 3:Path src 源文件路径 本地文件
         * 4:Path dst 目标路径 hdfs路径
         * */

        //两个//和一个\的含义是一样的
//        fs.copyFromLocalFile(false, false,
//                new Path("D:\\All_Test_Data\\01_hadoop\\input\\word.txt"), new Path("/"));
//        fs.copyFromLocalFile(false, true,
//                new Path("D:\\All_Test_Data\\01_hadoop\\input\\word.txt"), new Path("/"));
        fs.copyFromLocalFile(false, true,
                new Path("D:\\All_Test_Data\\01_hadoop\\input\\word.txt"), new Path("/"));

        //关于参数的优先级总结
        //xxxx.default.xml < xxxx.site.xml < 在代码中直接设置

    }

    //下载方法
    @Test
    public void get() throws IOException {
        /**
         * 1.boolean delSrc 是否删除源文件 hdfs文件
         * 2.Path src  源文件路径 hdfs文件路径
         * 3.Path dst  目标路径 本地路径 window本机
         * 4.boolean useRawLocalFileSystem  本地和hdfs都会校验   两者只会有一个检验
         *                                  true  开了本地 hdfs就不校验 本地默认不检验  就没有crc校验文件
         *                                  false 没有开本地 hdfs校验  hdfs校验就会有crc文件
         */
        fs.copyToLocalFile(false, new Path("/word.txt"), new Path("D:\\"), true);
    }

    //hdfs文件更名和移动
    @Test
    public void mv() throws IOException {
        //文件更名(具体名字) 原名字 现名字
        //fs.rename(new Path("/word.txt"), new Path("/HelloWorld.txt"));
        //文件的移动(具体路径)
        //fs.rename(new Path("/HelloWorld.txt"), new Path("/sanguo"));
        //文件的更名且移动(路径+名字)
        //fs.rename(new Path("/sanguo/caocao.txt"), new Path("/java/caozei.txt"));

        //目录的更新
        fs.rename(new Path("/java"), new Path("/ja"));
        //目录的移动
        fs.rename(new Path("/java_1"), new Path("/ja"));
        //目录的更名且移动
        fs.rename(new Path("/ja"), new Path("/sanguo/java"));
    }

    //删除
    @Test
    public void rm() throws IOException {
        //new Path "/sanguo"  中路径得加上/ 否则是没法删除文件夹的
        //true表示递归删除
        //false表示只能删除一层目录
        fs.delete(new Path("/input2"), false);
    }

    //打印文件的详情信息(该方法只能看文件)
    @Test
    public void ls() throws IOException {
        //递归进行遍历
        //遍历迭代器 获取每一个值
        RemoteIterator<LocatedFileStatus> remoteIterator = fs.listFiles(new Path("/"), true);
        while (remoteIterator.hasNext()) {
            //获取文件的详细信息
            LocatedFileStatus fileStatus = remoteIterator.next();
            System.out.println("===============" + fileStatus.getPath() + "===============");
            System.out.println(fileStatus.getPermission());
            System.out.println(fileStatus.getOwner());
            System.out.println(fileStatus.getGroup());
            //获取文件大小
            System.out.println(fileStatus.getLen());
            //格式化时间 (大mm 小dd)
            //先定义格式化时间模板 再调用format方法 最后将时间参数传入进行格式化
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            System.out.println(simpleDateFormat.format(fileStatus.getModificationTime()));
            System.out.println(fileStatus.getReplication());
            //获取块大小
            System.out.println(fileStatus.getBlockSize());
            //获取文件名称(先获取路径 再在路径的基础上再获取文件名)
            System.out.println(fileStatus.getPath().getName());
            //查看块信息
            BlockLocation[] blockLocations = fileStatus.getBlockLocations();
            //数组对象不能直接打印 需要先进行toString的转换 否则打印的是其地址值
            //[0,22,hadoop102,hadoop104,hadoop103] 参数的含义 块起始位置 块的大小 文件所在节点    
            System.out.println(Arrays.toString(blockLocations));
        }
    }

    //判断是否是文件还是文件夹
    @Test
    public void fileOrDir() throws IOException {
        //获取文件状态数组
        FileStatus[] fileStatuses = fs.listStatus(new Path("/"));
        //循环数组
        for (FileStatus fileStatus : fileStatuses) {
            //判断是文件
            if (fileStatus.isFile()) {
                System.out.println("文件:" + fileStatus.getPath());
            } else {
                //这里不能直接递归 fileOrDir方法中需要形参传入
                System.out.println("目录:" + fileStatus.getPath());
            }
        }
    }

    //含有形参的方法不能直接运行 需要在调用时传入相应的实参 否则会报错
    //Method fileOrDir should have no parameters
    //被测试方法所调用 没有@Test注解
    public void fileOrDir(Path path) throws IOException {
        //相同的代码逻辑 同时listStatus中传入的是path 而不是new Path()对象
        FileStatus[] fileStatuses = fs.listStatus(path);
        //循环数组
        for (FileStatus fileStatus : fileStatuses) {
            if (fileStatus.isFile()) {
                System.out.println("文件:" + fileStatus.getPath());
            } else {
                System.out.println("目录:" + fileStatus.getPath());
                //获取目录的路径 将其传入到fileOrDir(path) 实现递归调用
                fileOrDir(fileStatus.getPath());
            }
        }
    }

    //测试fileOrDir(Path path)方法
    @Test
    public void testFileOrDir() throws IOException {
        fileOrDir(new Path("/"));
    }

    //基于io的上传
    @Test
    public void putByIO() throws IOException {
        //1.创建本地文件系统输入流 (输入到内存)
        FileInputStream fis = new FileInputStream("D:\\All_Test_Data\\01_hadoop\\input\\hello.txt");
        //2.创建hdfs文件输出流(从内存输出到hdfs)
        //保证输出路径不能已经存在 否则报错 同时也要将文件名写上hello.txt
        FSDataOutputStream fos = fs.create(new Path("/input/hello.txt"));
        //3.流的对拷
        //使用hadoop包下IOUtils工具类 调用封装好的方法
        IOUtils.copyBytes(fis, fos, conf);
        //4.流的关闭
        //先开后关 后开先关
        IOUtils.closeStreams(fos, fis);
    }

    //基于IO的下载
    @Test
    public void getByIO() throws IOException {
        //1.创建hdfs输入流
        //fs调用的为open方法
        FSDataInputStream fdis = fs.open(new Path("/hadoop-3.1.3.tar.gz"));
        //2.创建本地文件输出流
        FileOutputStream fos = new FileOutputStream("D:\\All_Test_Data\\01_hadoop\\input\\hadoop.tar.gz");
        //注意先是流的对拷再是关闭流
        //3.流的对拷
        IOUtils.copyBytes(fdis, fos, conf);
        //4.流的关闭
        IOUtils.closeStreams(fos,fdis);
    }
}
