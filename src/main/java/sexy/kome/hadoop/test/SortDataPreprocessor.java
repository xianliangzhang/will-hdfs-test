package sexy.kome.hadoop.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import java.io.IOException;


/**
 * Created by ZXL on 2017/4/6.
 */
public class SortDataPreprocessor extends Configured implements Tool {
    private static final String DEFAULT_INPUT_PATH = "data/input/unsort.txt";
    private static final String DEFAULT_OUTPUT_PATH = "data/output";

    private static class SortMapper extends Mapper<Object, Text, Text, IntWritable> {

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        }
    }

    @Override
    public int run(String[] strings) throws Exception {
        return 0;
    }

    public static void main(String[] args) {

    }
}
