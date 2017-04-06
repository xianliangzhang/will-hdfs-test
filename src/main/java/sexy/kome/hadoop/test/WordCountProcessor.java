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

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

/**
 * Created by ZXL on 2017/4/6.
 */
public class WordCountProcessor  extends Configured implements Tool {
    private static final String DEFAULT_INPUT_PATH = "lab/input/word.txt";
    private static final String DEFAULT_OUTPUT_PATH = "lab/output";

    private static class WordCountMapper extends Mapper<Object, Text, Text, IntWritable> {

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer tokenizer = new StringTokenizer(value.toString());
            while (tokenizer.hasMoreTokens()) {
                String token = tokenizer.nextToken();
                context.write(new Text(token), new IntWritable(1));
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);
        }
    }

    private static class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
        }

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int counter = 0;
            for (Iterator<IntWritable> iterator = values.iterator(); iterator.hasNext();) {
                counter += iterator.next().get();
            }

            context.write(key, new IntWritable(counter));
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);
        }
    }

    @Override
    public int run(String[] strings) throws Exception {
        Job job = Job.getInstance(new Configuration(), "WordCount");

        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(strings[0]));
        FileOutputFormat.setOutputPath(job, new Path(strings[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new SortDataPreprocessor(), new String[]{DEFAULT_INPUT_PATH, DEFAULT_OUTPUT_PATH});
        System.exit(exitCode);
    }
}
