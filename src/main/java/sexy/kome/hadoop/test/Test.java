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
import org.apache.hadoop.util.ToolRunner;
import sexy.kome.hadoop.wc.WordCount;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

/**
 * Created by Administrator on 2017/3/28.
 */
public class Test extends Configured implements Tool {
    private static final String DEFAULT_INPUT_PATH = "lab/input/input.txt";
    private static final String DEFAULT_OUTPUT_PATH = "lab/output";

    enum WordState{
        INVALIDATE, VALIDATE
    }

    private static class CounterMapper extends Mapper<Object, Text, Text, IntWritable> {

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                String word = itr.nextToken();
                if (word.contentEquals("xx")) {
                    context.getCounter(WordState.INVALIDATE).increment(1L);
                } else {
                    context.getCounter(WordState.VALIDATE).increment(1L);
                }

                context.write(new Text("COUNTER"), new IntWritable(1));
            }
        }
    }

    private static class CounterReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int counter = 0;
            for (Iterator<IntWritable> iterator = values.iterator(); iterator.hasNext(); ) {
                counter += iterator.next().get();
            }
            context.write(key, new IntWritable(counter));
        }
    }

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "WordCount");
        job.setJarByClass(WordCount.class);

        job.setMapperClass(CounterMapper.class);
        job.setReducerClass(CounterReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(strings[0]));
        FileOutputFormat.setOutputPath(job, new Path(strings[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Test(), args.length == 0 ? new String[]{DEFAULT_INPUT_PATH, DEFAULT_OUTPUT_PATH} : args);
        System.exit(exitCode);
    }

}
