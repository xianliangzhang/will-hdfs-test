package sexy.kome.hadoop.wc;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Created by Hack on 2016/11/16.
 */
public class WordCountMapper extends Mapper<Object, Text, Text, IntWritable> {
    private static final Logger RUN_LOG = Logger.getLogger(WordCountMapper.class);
    private static final IntWritable one = new IntWritable(1);

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer itr = new StringTokenizer(value.toString());
        RUN_LOG.info(String.format("  MAPPER -- %s", value.toString()));
        while (itr.hasMoreTokens()) {
            String token = itr.nextToken();
            context.write(new Text(token), one);
            RUN_LOG.info(String.format("  MAPPER -- %s", token));
        }
    }
}
