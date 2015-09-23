// adapted from https://hadoop.apache.org/docs/current/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class RedditAverage extends Configured implements Tool {

    // Take JSON for comments and map to -> subreddit string, (number of comments, sum score)
    public static class CommentMapper extends Mapper<LongWritable, Text, Text, LongPairWritable>{

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
          ObjectMapper json_mapper = new ObjectMapper();
          JsonNode data = json_mapper.readValue(value.toString(), JsonNode.class);

          Text subreddit = new Text(data.get("subreddit").textValue());
          LongPairWritable pair = new LongPairWritable(1, data.get("score").longValue());

          context.write(subreddit, pair);
        }
    }

    public static class SubredditCombiner extends Reducer<Text, LongPairWritable, Text, LongPairWritable> {

        @Override
        public void reduce(Text key, Iterable<LongPairWritable> values, Context context) throws IOException, InterruptedException {
          long comment_count =  0;
          long comment_sum_score = 0;
          for (LongPairWritable val : values) {
            comment_count += val.get_0();
            comment_sum_score += val.get_1();
          }
          context.write(key, new LongPairWritable(comment_count, comment_sum_score));
        }
    }

    public static class SubredditReducer extends Reducer<Text, LongPairWritable, Text, DoubleWritable> {

        @Override
        public void reduce(Text key, Iterable<LongPairWritable> values, Context context) throws IOException, InterruptedException {
          long comment_count =  0;
          long comment_sum_score = 0;
          for (LongPairWritable val : values) {
            comment_count += val.get_0();
            comment_sum_score += val.get_1();
          }
          double average = (double)comment_sum_score / (double)comment_count;
          context.write(key, new DoubleWritable(average));
        }
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new RedditAverage(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        Job job = Job.getInstance(conf, "Reddit average");
        job.setJarByClass(RedditAverage.class);

        job.setInputFormatClass(MultiLineJSONInputFormat.class);

        job.setMapperClass(CommentMapper.class);
        job.setCombinerClass(SubredditCombiner.class);
        job.setReducerClass(SubredditReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongPairWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextInputFormat.addInputPath(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }
}
