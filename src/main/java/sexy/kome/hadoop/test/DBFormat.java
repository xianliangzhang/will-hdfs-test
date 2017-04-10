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
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
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

    static class DBFormatMapper extends Mapper<LongWritable, Operator, LongWritable, Text> {

        @Override
        protected void map(LongWritable key, Operator value, Context context) throws IOException, InterruptedException {
            context.write(new LongWritable(value.id), new Text(String.valueOf(value.id).concat(":").concat(value.name).concat(":").concat(value.email)));
        }
    }

    static class DBFormatReducer extends Reducer<LongWritable, Text, LongWritable, Text> {

        @Override
        protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Iterator<Text> iterator = values.iterator(); iterator.hasNext(); ) {
                context.write(key, iterator.next());
            }
        }
    }

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = new Configuration();
        conf.setStrings("mapreduce.jdbc.driver.class", "com.mysql.jdbc.Driver");
        conf.set("mapreduce.jdbc.url", "jdbc:mysql://10.146.16.208:8306/spider");
        conf.set("mapreduce.jdbc.username", "root");
        conf.set("mapreduce.jdbc.password", "123456");
        conf.set("mapreduce.jdbc.input.table.name", "operator");

        Job job = Job.getInstance(conf, "DBFormat");
        job.setJarByClass(WordCount.class);

        job.setMapperClass(DBFormatMapper.class);
        job.setReducerClass(DBFormatReducer.class);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job, new Path("data/output"));
        DBConfiguration.configureDB(conf, "com.mysql.jdbc.Driver", "jdbc:mysql://10.146.16.208:8306/spider", "root", "123456");
        DBInputFormat.setInput(job, Operator.class, "select id, name, email from operator", null);

        job.setInputFormatClass(DBInputFormat.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new DBFormat(), args);
        System.exit(exitCode);
    }

}
