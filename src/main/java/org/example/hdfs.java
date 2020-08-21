package org.example;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.net.URI;

//import org.junit.Testo;

public class hdfs {
    public static final  String HDFS_PATH = "hdfs://192.168.186.100:9000";
    FileSystem fileSystem = null;
    Configuration configuration = null;

    public void mkdir(String hdfs_path)throws Exception{
        fileSystem.mkdirs(new Path(hdfs_path));
    }

    public void setUp()throws Exception{
        configuration = new Configuration();
        fileSystem = FileSystem.get(new URI(HDFS_PATH), configuration, "root");
        System.out.println("hdfs.setUp");
    }

    public void tearDown()throws Exception{
        configuration = null;
        fileSystem = null;

        System.out.println("hdfs.tearDown");
    }

    public void createfile(String hdfs_path, String file_name, String content)throws Exception{
        FSDataOutputStream outputStream = fileSystem.create(new Path(hdfs_path + '/' + file_name));
        outputStream.write(content.getBytes());
        outputStream.flush();
        outputStream.close();
    }

    public void readfile(String hdfs_path, String file_name)throws Exception{
        FSDataInputStream inputStream = fileSystem.open(new Path(hdfs_path + '/' + file_name));
        IOUtils.copy(inputStream, System.out);
        System.out.println();
        inputStream.close();
    }

    public void deletefile(String hdfs_path, String file_name)throws Exception{
        fileSystem.delete(new Path(hdfs_path + '/' + file_name), true);
    }

    public void copylocalfile(String localFilePath, String hdfsFilePath)throws Exception{
        fileSystem.copyFromLocalFile(new Path(localFilePath), new Path(hdfsFilePath));
    }

    public static void main( String[] args ) throws Exception {
        hdfs HDFS = new hdfs();
        String hdfs_path = "/hdfs_file/test";
        String filename = "digit_test.txt";
        String localfilepath = "G:/digit_test.txt";
        String hdfsfilepath = hdfs_path + "/" + filename;
//        String content = "hi, there";
        HDFS.setUp();
//        HDFS.mkdir(hdfs_path);
//        HDFS.deletefile(hdfs_path, filename);
//        HDFS.createfile(hdfs_path, filename, content);
        HDFS.copylocalfile(localfilepath, hdfsfilepath);
        HDFS.readfile(hdfs_path, filename);
        HDFS.tearDown();
    }
}
