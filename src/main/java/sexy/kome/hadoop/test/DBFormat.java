package sexy.kome.hadoop.test;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.lib.db.DBInputFormat;
import org.apache.hadoop.mapred.lib.db.DBWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import sexy.kome.hadoop.wc.WordCount;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;

/**
 * Created by Hack on 2017/4/9.
 */
public class DBFormat extends Configured implements Tool {

    static class Operator implements Writable, DBWritable {
        Long id;
        String name;
        String email;


        @Override
        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeLong(id);
            Text.writeString(dataOutput, name);
            Text.writeString(dataOutput, email);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            this.id = dataInput.readLong();
            this.name = Text.readString(dataInput);
            this.email = Text.readString(dataInput);
        }

        @Override
        public void write(PreparedStatement preparedStatement) throws SQLException {
            preparedStatement.setLong(1, id);
            preparedStatement.setString(2, name);
            preparedStatement.setString(3, email);
        }

        @Override
        public void readFields(ResultSet resultSet) throws SQLException {
            this.id = resultSet.getLong("id");
            this.name = resultSet.getString("name");
            this.email = resultSet.getString("email");
        }

        @Override
        public int hashCode() {
            int prime = 31;
            int result = 1;
            result = result * prime + ((id == 0) ? 0 : id.hashCode());
            result = result * prime + ((name == null) ? 0 : name.hashCode());
            result = result * prime + ((email == null) ? 0 : email.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object object) {
            if (object == null) {
                return false;
            }

            if (object.getClass().equals(getClass()) &&
                    ((Operator) object).name.equals(name) &&
                    ((Operator) object).email.equals(email)) {
                return true;
            }
            return false;
        }
    }

    static class DBFormatMapper extends Mapper<LongWritable, Operator, Text, Text> {

        @Override
        protected void map(LongWritable key, Operator value, Context context) throws IOException, InterruptedException {
            context.write(new Text(String.valueOf(value.id)), new Text(value.name.concat("_").concat(value.email)));
        }
    }

    static class DBFormatReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Iterator<Text> iterator = values.iterator(); iterator.hasNext(); ) {
                context.write(key, iterator.next());
            }
        }
    }

    @Override
    public int run(String[] strings) throws Exception {
        String dbDriver = "com.mysql.jdbc.Driver";
        String dbURL = "jdbc:mysql://localhost:3306/kome";

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "DBFormat");
        job.setJarByClass(WordCount.class);

        job.setMapperClass(DBFormatMapper.class);
        job.setReducerClass(DBFormatReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //FileInputFormat.addInputPath(job, new Path(strings[0]));
        FileOutputFormat.setOutputPath(job, new Path("data/output"));

        job.setInputFormatClass(DBInputFormat.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Test(), args);
        System.exit(exitCode);
    }

}
