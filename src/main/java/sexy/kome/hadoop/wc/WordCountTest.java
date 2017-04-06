package sexy.kome.hadoop.wc;

/**
 * Created by Administrator on 2017/3/27.
 */
public class WordCountTest {
    public static void main(String[] args) throws Exception {
//        WordCount.main("d://lab//test//Test.java", "d://xlogs//");
        WordCount.main("hdfs://192.168.140.150:9000/lab/dest.txt", "hdfs://192.168.140.150:9000/lab/dest");
    }
}
