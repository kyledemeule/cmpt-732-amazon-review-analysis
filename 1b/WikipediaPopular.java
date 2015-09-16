// adapted from https://hadoop.apache.org/docs/current/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class WikipediaPopular extends Configured implements Tool {

    // filters out invalid input
    // for good input returns a date/hour string and the count
    public static class TokenizerMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
          // filename is in format "pagecounts-20141204-100000.gz"
          String hour_string = ((FileSplit) context.getInputSplit()).getPath().getName();
          hour_string = hour_string.substring("pagecounts-".length(), hour_string.length() - "0000.gz".length());
          String[] tokens = value.toString().split("[ ]");
          // a line should have 4 components, if it does not ignore it
          if(tokens.length == 4) {
            String project_name = tokens[0], page_name = tokens[1], view_count = tokens[2], content_size = tokens[3];
            if (project_name.equals("en") && !page_name.equals("Main_Page") && !page_name.startsWith("Special:")) {
              context.write(new Text(hour_string), new IntWritable(Integer.parseInt(view_count)));
            }
          }
        }
    }

    // just find the maximum view count for this hour
    public static class MaxReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
          int max =  0;
          for (IntWritable val : values) {
              if(val.get() > max) {
                max = val.get();
              }
          }
          context.write(key, new IntWritable(max));
        }
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new WikipediaPopular(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        Job job = Job.getInstance(conf, "wikipedia popular");
        job.setJarByClass(WikipediaPopular.class);

        job.setInputFormatClass(TextInputFormat.class);

        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(MaxReducer.class);
        job.setReducerClass(MaxReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextInputFormat.addInputPath(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }
}
