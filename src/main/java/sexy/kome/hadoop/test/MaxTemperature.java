package sexy.kome.hadoop.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import sexy.kome.hadoop.wc.WordCount;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by ZXL on 2017/4/6.
 */
public class MaxTemperature extends Configured implements Tool {
    private static final SimpleDateFormat SIMPLE_DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private static final Map<String, String> STATION_MAP = new HashMap<String, String>() {{
        put("011990-999999", "XianStation");
        put("012650-999999", "BeijingStation");
    }};

    private static class WeatherRecord {
        public String stationID;
        public String stationName;
        public Date recordTime;
        public int temperature;

        public static WeatherRecord parse(String value) {
            String[] splits = value.trim().split(",");

            try {
                WeatherRecord weatherRecord = new WeatherRecord();
                weatherRecord.stationID = splits[0].trim();
                weatherRecord.stationName = STATION_MAP.get(weatherRecord.stationID);
                weatherRecord.recordTime = SIMPLE_DATE_FORMAT.parse(splits[1]);
                weatherRecord.temperature = Integer.parseInt(splits[2].trim());
                return weatherRecord;
            } catch (Exception e) {
                return null;
            }
        }
    }

    private static class MaxTemperatureMapper extends Mapper<Object, Text, Text, IntWritable> {

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            WeatherRecord record = WeatherRecord.parse(value.toString());
            if (record != null) {
                context.write(new Text(record.stationName), new IntWritable(record.temperature));
            }
        }
    }

    private static class MaxTemperatureReducer extends Reducer<Text, IntWritable, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int minTemperature = Integer.MAX_VALUE;
            int maxTemperature = Integer.MIN_VALUE;

            for (Iterator<IntWritable> iterator = values.iterator(); iterator.hasNext();) {
                int currentTemperature = iterator.next().get();
                if (currentTemperature > maxTemperature) {
                    maxTemperature = currentTemperature;
                }
                if (currentTemperature < minTemperature) {
                    minTemperature = currentTemperature;
                }
            }

            context.write(key, new Text(String.valueOf(minTemperature).concat("~").concat(String.valueOf(maxTemperature))));
        }
    }

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "WordCount");

        job.setMapperClass(MaxTemperatureMapper.class);
        job.setReducerClass(MaxTemperatureReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(strings[0]));
        FileOutputFormat.setOutputPath(job, new Path(strings[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        String[] paths = new String[]{
                "data/input/temperature.txt",
                "data/output"
        };

        System.exit(ToolRunner.run(new MaxTemperature(), paths));
    }
}
