import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.*;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class CorrelateLogs extends Configured implements Tool {
    
    static public Scan get_scan() {
        Scan s = new Scan();
        s.addFamily(Bytes.toBytes("struct"));
        return s;
    }
    
    public static class NASAMapper extends TableMapper<Text, LongPairWritable>{

        public void map(ImmutableBytesWritable row, Result result, Context context) throws InterruptedException, IOException {
            Cell host_cell = result.getColumnLatestCell(Bytes.toBytes("struct"), Bytes.toBytes("host"));
            Text host = new Text(Bytes.toString(CellUtil.cloneValue(host_cell)));

            Cell numbytes_cell = result.getColumnLatestCell(Bytes.toBytes("struct"), Bytes.toBytes("bytes"));
            Long num_bytes = Bytes.toLong(CellUtil.cloneValue(numbytes_cell));

            LongPairWritable pair = new LongPairWritable(1, num_bytes);
            context.write(host, pair);
        }
        
    }

    public static class NASAReducer extends Reducer<Text, LongPairWritable, Text, LongPairWritable> {

        @Override
        public void reduce(Text key, Iterable<LongPairWritable> values, Context context) throws IOException, InterruptedException {
            long request_count =  0;
            long request_sum_bytes = 0;
            for (LongPairWritable val : values) {
              request_count += val.get_0();
              request_sum_bytes += val.get_1();
            }
            context.write(key, new LongPairWritable(request_count, request_sum_bytes));
        }
    }
    
    public class NASACalculationMapper extends Mapper<Text, LongPairWritable, Text, Text>{
        private Long n, Sx, Sy, Sxy;
        private double Sx2, Sy2;
        
        public void setup(Context context) throws java.io.IOException, java.lang.InterruptedException {
            n = new Long(0);
            Sx = new Long(0);
            Sx2 = 0.0;
            Sy = new Long(0);
            Sy2 = 0.0;
            Sxy = new Long(0);
        }
      
        public void map(Text key, LongPairWritable value, Context context) throws IOException, InterruptedException {
          Long x = value.get_0();
          Long y = value.get_1();
          
          Sx += x;
          Sx2 += Math.pow(x, 2);
          Sy += y;
          Sy2 += Math.pow(y, 2);
          Sxy = x * y;
        }
        
        public void cleanup(Context context) throws java.io.IOException, java.lang.InterruptedException {
            double r = ((n * Sxy) - (Sx * Sy)) / (Math.sqrt(n * Sx2 - Math.pow(Sx, 2)) * Math.sqrt(n * Sy2 - Math.pow(Sy, 2)));
            double r2 = Math.pow(r, 2);
            
            context.write(new Text("n"), new Text(n.toString()));
            context.write(new Text("Sx"), new Text(Sx.toString()));
            context.write(new Text("Sx2"), new Text(String.valueOf(Sx2)));
            context.write(new Text("Sy"), new Text(Sy.toString()));
            context.write(new Text("Sy2"), new Text(String.valueOf(Sy2)));
            context.write(new Text("Sxy"), new Text(Sxy.toString()));
            context.write(new Text("r"), new Text(String.valueOf(r)));
            context.write(new Text("r2"), new Text(String.valueOf(r2)));
        }
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new CorrelateLogs(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        Job job = Job.getInstance(conf, "Correlate Logs");
        job.setJarByClass(CorrelateLogs.class);
        TableMapReduceUtil.addDependencyJars(job);

        TableMapReduceUtil.initTableMapperJob(args[0], get_scan(), NASAMapper.class, Text.class, LongPairWritable.class, job);
        
        //job.setInputFormatClass(TextInputFormat.class);
        //TextInputFormat.addInputPath(job, new Path(args[0]));

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongPairWritable.class);
  
        job.setReducerClass(NASAReducer.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongPairWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path(args[1]));
        

        job.setNumReduceTasks(1);

        return job.waitForCompletion(true) ? 0 : 1;
    }
}
