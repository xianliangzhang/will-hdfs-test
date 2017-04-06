package sexy.kome.hadoop.wc;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * Created by Hack on 2016/11/16.
 */
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private static final Logger RUN_LOG = Logger.getLogger(WordCountReducer.class);
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;

        RUN_LOG.info(String.format("  REDUCER -- %s - %d", key.toString(), sum));

        for (IntWritable value : values) {
            sum += value.get();
            RUN_LOG.info(String.format("    REDUCER ---- %s - %d", key.toString(), sum));
        }

        this.result.set(sum);
        context.write(key, this.result);
        RUN_LOG.info(String.format("  REDUCER -- %s - %d", key.toString(), sum));
    }
}
