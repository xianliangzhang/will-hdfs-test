package sexy.kome.hadoop.test;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Created by Administrator on 2017/3/28.
 */
public class FileSystemTest {
    private static final URI HDFS_PREFIX_URI = URI.create("hdfs://192.168.140.138:9000");

    private static final Configuration CONFIGURATION = new Configuration(){{
    }};

    public static void readFromHDFS(String hdfs) throws Exception {
        URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());

        InputStream inputStream = new URL(StringUtils.isEmpty(hdfs) ? "hdfs://192.168.140.138:9000/" : hdfs).openStream();
        System.out.println(inputStream.available());
        IOUtils.copyBytes(inputStream, System.out, inputStream.available(), false);
        inputStream.close();
    }

    public static FSDataOutputStream createFileIfAbsent(String file) throws Exception {
        FileSystem fileSystem = FileSystem.get(HDFS_PREFIX_URI, CONFIGURATION);
        fileSystem.setReplication(new Path(file), (short) 1);

        if (!fileSystem.exists(new Path(file))) {
            return fileSystem.create(new Path(file));
        }
        return fileSystem.append(new Path(file));
    }

    public static void main(String[] args) throws Exception {
        FileSystem fileSystem = FileSystem.get(HDFS_PREFIX_URI, CONFIGURATION);
        FSDataOutputStream outputStream = fileSystem.create(new Path("/lab/xx.txt"));
        outputStream.close();

        FileSystem xx = FileSystem.get(HDFS_PREFIX_URI, CONFIGURATION);
        FSDataOutputStream xxx = xx.append(new Path("/lab/xx.txt"));
        xxx.write("GGG".getBytes());
        xxx.hsync();
        xxx.close();
    }
}
