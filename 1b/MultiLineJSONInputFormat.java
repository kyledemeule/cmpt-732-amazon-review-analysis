import java.io.IOException;
 
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
 
import com.google.common.base.Charsets;
 
public class MultiLineJSONInputFormat extends TextInputFormat {
 
    public class MultiLineRecordReader extends RecordReader<LongWritable, Text> {
        LineRecordReader linereader;
        LongWritable current_key;
        Text current_value;
 
        public MultiLineRecordReader(byte[] recordDelimiterBytes) {
            linereader = new LineRecordReader(recordDelimiterBytes);
        }
 
        @Override
        public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException {
            linereader.initialize(genericSplit, context);
            current_key = null;
            current_value = null;
        }
 
        @Override
        public boolean nextKeyValue() throws IOException {
            //if this is the first time running this, run a nextKeyValue on linerunner
            if(linereader.getCurrentValue() == null) {
                if(!linereader.nextKeyValue()){
                    return false;
                }
            }
            //at this point we assume the linereader current value is pointing to the beginning of a json item
            StringBuilder temp_value = new StringBuilder();
            temp_value.append(linereader.getCurrentValue().toString());
            //current_key.set(linereader.getCurrentKey().get() + current_key.get());
            current_key = linereader.getCurrentKey();
            
            while(true) {
                // advance to the next line
                if(!linereader.nextKeyValue()){
                    break;
                }
                // check if we've hit the next json item
                if(linereader.getCurrentValue().toString().startsWith("{")){
                    break;
                } else {
                    temp_value.append(linereader.getCurrentValue().toString());
                    //current_key.set(linereader.getCurrentKey().get() + current_key.get());
                }
                
            }
            current_value = new Text(temp_value.toString());
            return true;
        }
 
        @Override
        public float getProgress() throws IOException {
            return linereader.getProgress();
        }
 
        @Override
        public LongWritable getCurrentKey() {
            // return current_key;
            return current_key;
        }
 
        @Override
        public Text getCurrentValue() {
            // return current_value;
            return current_value;
        }
 
        @Override
        public synchronized void close() throws IOException {
            linereader.close();
        }
    }
 
    // shouldn't have to change below here
 
    @Override
    public RecordReader<LongWritable, Text> 
    createRecordReader(InputSplit split,
            TaskAttemptContext context) {
        // same as TextInputFormat constructor, except return MultiLineRecordReader
        String delimiter = context.getConfiguration().get(
                "textinputformat.record.delimiter");
        byte[] recordDelimiterBytes = null;
        if (null != delimiter)
            recordDelimiterBytes = delimiter.getBytes(Charsets.UTF_8);
        return new MultiLineRecordReader(recordDelimiterBytes);
    }
 
    @Override
    protected boolean isSplitable(JobContext context, Path file) {
        // let's not worry about where to split within a file
        return false;
    }
}