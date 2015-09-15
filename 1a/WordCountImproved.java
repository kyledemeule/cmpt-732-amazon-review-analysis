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
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import java.util.Locale;
import java.text.BreakIterator;
import java.text.Normalizer;

public class WordCount extends Configured implements Tool {

    public static class TokenizerMapper
    extends Mapper<LongWritable, Text, Text, LongWritable>{

        private final static LongWritable one = new LongWritable(1);
        private String word = new String();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
          Locale locale = new Locale("en", "ca");
          BreakIterator breakiter = BreakIterator.getWordInstance(locale);

          int start = breakiter.first();
          String text_string = value.toString();
          breakiter.setText(text_string);
          for (int end = breakiter.next(); end != BreakIterator.DONE; start = end, end = breakiter.next()) {
            word = text_string.substring(start,end).trim().toLowerCase(locale);
            if ( word.length() > 0 ) {
              context.write(new Text(Normalizer.normalize(word, Normalizer.Form.NFD)), one);
            }
          }
        }
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new WordCount(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCount.class);

        job.setInputFormatClass(TextInputFormat.class);

        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(LongSumReducer.class);
        job.setReducerClass(LongSumReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextInputFormat.addInputPath(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }
}
