import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.hbase.mapreduce.*;

import java.util.regex.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class LoadLogsMR extends Configured implements Tool {
    static final Pattern patt = Pattern.compile("^(\\S+) - - \\[(\\S+) [+-]\\d+\\] \"[A-Z]+ (\\S+) HTTP/\\d\\.\\d\" \\d+ (\\d+)$");
    static final SimpleDateFormat dateparse = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss");

    static public Put get_put(String line) throws ParseException {
        //put 'kdd2-logs', DIGEST, 'raw:line', LINE
        //grimnet23.idirect.com - - [01/Aug/1995:00:36:09 -0400] "GET /images/USA-logosmall.gif HTTP/1.0" 200 234
        byte[] rowkey = DigestUtils.md5(line);
        Put put = new Put(rowkey);
        put.addColumn(Bytes.toBytes("raw"), Bytes.toBytes("line"), Bytes.toBytes(line));
        
        Matcher m = patt.matcher(line);
        if(m.find()) {
            // HOST
            String host = m.group(1);
            put.addColumn(Bytes.toBytes("struct"), Bytes.toBytes("host"), Bytes.toBytes(host));
            // DATE
            Long date = dateparse.parse(m.group(2)).getTime();
            put.addColumn(Bytes.toBytes("struct"), Bytes.toBytes("date"), Bytes.toBytes(date));
            // PATH
            String path = m.group(3);
            put.addColumn(Bytes.toBytes("struct"), Bytes.toBytes("path"), Bytes.toBytes(path));
            // BYTES
            Long byte_count = new Long(m.group(4));
            put.addColumn(Bytes.toBytes("struct"), Bytes.toBytes("bytes"), Bytes.toBytes(byte_count));
        }

        return put;
    }

    public static class NASAReducer extends TableReducer<LongWritable, Text, Put> {

        @Override
        public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text val : values) {
                try{
                    Put p = get_put(val.toString());
                    context.write(p, p);
                } catch(ParseException e) {
                    // lol errors
                }
              }
        }
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new LoadLogsMR(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        Job job = Job.getInstance(conf, "Load Logs MR");
        job.setJarByClass(LoadLogsMR.class);

        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, new Path(args[0]));

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);
  
        TableMapReduceUtil.addDependencyJars(job);
        TableMapReduceUtil.initTableReducerJob(args[1], NASAReducer.class, job);
        job.setReducerClass(NASAReducer.class);
        job.setNumReduceTasks(3);

        return job.waitForCompletion(true) ? 0 : 1;
    }
}
