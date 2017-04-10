package sexy.kome.hadoop.homework;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * Created by ZXL on 2017/4/6.
 * 排序部分待实现
 */
public class SortData extends Configured implements Tool {
    static class SortingTemperatureMapper extends Mapper<Object, Text, IntWritable, Text> {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] temperatureArr = value.toString().split(",");
            Integer temperature = Integer.valueOf(temperatureArr[temperatureArr.length - 1].trim());
            System.out.println(" -- ".concat(String.valueOf(temperature)).concat(" - ").concat(value.toString()));
            context.write(new IntWritable(temperature), value);
        }
    }

    @Override
    public int run(String[] strings) throws Exception {
        Configuration configuration = new Configuration();

        Job job = Job.getInstance(configuration, "SortData");

        job.setMapperClass(SortingTemperatureMapper.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(0);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path("data/input/temperature.txt"));
        FileOutputFormat.setOutputPath(job, new Path("data/output"));
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new SortData(), args);
        System.exit(exitCode);
    }
}
