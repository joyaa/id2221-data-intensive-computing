package sics;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.Reader;
import java.io.StringReader;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.jdom.Document;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.input.SAXBuilder;

public class ToptenCount {

  public static class TopTenMapper extends Mapper<Object, Text, NullWritable, Text> {
    // Stores a map of user reputation to the record
    private TreeMap<Integer, Text> repToRecordMap = new TreeMap<Integer, Text>(Collections.reverseOrder);

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      Map<String, String> parsed = new Map<String, String>();

      String xmlString = value.toString();

      SAXBuilder builder = new SAXBuilder();
      Reader in = new StringReader(xmlString);

      try {
          Document doc = builder.build(in);
          Element root = doc.getRootElement();

          String reputation = root.getChild("row").getAttribute("Reputation").getTextTrim() ;
          String userId = root.getChild("row").getAttribute("Id").getTextTrim();
          String row = root.getChild("row");

      } catch (JDOMException ex) {
          Logger.getLogger(MyParserMapper.class.getName()).log(Level.SEVERE, null, ex);
      } catch (IOException ex) {
          Logger.getLogger(MyParserMapper.class.getName()).log(Level.SEVERE, null, ex);
      }

      // Add this record to our map with the reputation as the key
      repToRecordMap.put(reputation, row)

      // If we have more than ten records, remove the one with the lowest reputation.
      if (repToRecordMap.size() > 10) {
        repToRecordMap.pollLastEntry();
      }
  }

    protected void cleanup(Context context) throws IOException, InterruptedException {
      // Output our ten records to the reducers with a null key
      for (Text t : repToRecordMap.values()) {
        context.write(NullWritable.get(), t);
      }
    }
  }

  public static class TopTenReducer extends Reducer<NullWritable, Text, NullWritable, Text> {
  // Stores a map of user reputation to the record
  // Overloads the comparator to order the reputations in descending order
  private TreeMap<Integer, Text> repToRecordMap = new TreeMap<Integer, Text>();

    public void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      for (Text value : values) {
        repToRecordMap.put(reputation, value);

        // If we have more than ten records, remove the one with the lowest reputation
        if (repToRecordMap.size() > 10) {
          repToRecordMap.pollLastEntry();
        }
      }

      for (Text t : repToRecordMap.descendingMap().values()) {
      // Output our ten records to the file system with a null key
        context.write(NullWritable.get(), t);
      }
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.set("xmlinput.start", "<row");
    conf.set("xmlinput.end", "/>");
    Job job = Job.getInstance(conf, "top ten counter");

    job.setNumReduceTasks(1); 
    job.setInputFormatClass(XmlInputFormat.class);

    job.setJarByClass(TopTenCount.class);

    job.setMapperClass(TopTenMapper.class);
    job.setCombinerClass(TopTenReducer.class);
    job.setReducerClass(TopTenReducer.class);

    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(Text.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
