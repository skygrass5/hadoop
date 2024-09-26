package cn.itcast.hdfsdemo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Before;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.io.IOException;

public class HDFS_CURD {
    FileSystem fs = null;

    @Before
    public void init() throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://hadoop01:9000");
        System.setProperty("HADOOP_USER_NAME", "root");
        fs = FileSystem.get(conf);
    }

    //注解Test用于在Junit单元测试框架中测试方法testAddFileToHdfs()
    @Test
    public void testAddFileToHdfs() throws IOException {
        // 指定本地文件系统上传的文件
        Path src = new Path("D:\\test.txt");
        // 指定将文件上传到HDFS的目录
        Path dst = new Path("/testFile");
        fs.copyFromLocalFile(src, dst);
        // 关闭资源
        fs.close();
    }

    @Test
    //下载到 downloadFile
    public void testDownloadFileToLocal() throws IllegalArgumentException,
            IOException {
        fs.copyToLocalFile(
                new Path("/testFile"),
                new Path("D:\\downloadFile"));
    }

    @Test
    //创建两个新目录 /a/b/c 和 /a2/b2/c2。
    //重命名现有目录 /a 为 /a3。
    public void testMkdirAndDeleteAndRename() throws Exception {
        fs.mkdirs(new Path("/a/b/c"));
        fs.mkdirs(new Path("/a2/b2/c2"));
        fs.rename(new Path("/a"), new Path("/a3"));
        fs.delete(new Path("/a2"), true);
    }

    @Test
    public void testListFiles() throws FileNotFoundException,
            IllegalArgumentException, IOException {
        RemoteIterator<LocatedFileStatus> listFiles =
                fs.listFiles(new Path("/"), true);
        while (listFiles.hasNext()) {
            LocatedFileStatus fileStatus = listFiles.next();
            System.out.println("文件名：" + fileStatus.getPath().getName());
            System.out.println("文件的副本数：" + fileStatus.getReplication());
            System.out.println("文件的权限：" + fileStatus.getPermission());
            System.out.println("文件大小：" + fileStatus.getLen() + "字节");
            BlockLocation[] blockLocations =
                    fileStatus.getBlockLocations();
            for (BlockLocation bl : blockLocations) {
                String[] hosts = bl.getHosts();
                System.out.println("文件的Block所在虚拟机的主机名：");
                for (String host : hosts) {
                    System.out.println(host);
                }
            }
            System.out.println("----------------------------");
        }
    }
}