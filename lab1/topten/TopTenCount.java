/**
 20160925
 By Joy Alam and Ibrahim Said.

 Added packages:
 hadoop - $HADOOP_HOME
 jdom - $JDOM_HOME
 mahout - $MAHOUT_HOME

 Run using:
 javac -classpath $HADOOP_HOME/share/hadoop/common/hadoop-common-2.6.4.jar:$HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-client-core-2.6.4.jar:$HADOOP_HOME/share/hadoop/common/lib/commons-cli-1.2.jar:$JDOM_HOME/build/jdom-1.1.3.jar:$MAHOUT_HOME/mahout-integration-0.12.2.jar -d toptencount_classes TopTenCount.java

 */

package sics;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.mahout.text.wikipedia.XmlInputFormat;
import org.jdom.Document;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.input.SAXBuilder;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.Collections;
import java.util.TreeMap;
import java.util.logging.Level;
import java.util.logging.Logger;

//import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class TopTenCount {

    public static class TopTenMapper extends Mapper<Object, Text, NullWritable, Text> {
        // Stores a map of user reputation to the record
        private TreeMap<Integer, Text> repToRecordMap = new TreeMap<Integer, Text>(Collections.reverseOrder());

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            //Map<String, String> parsed = new Map<String, String>();

            String xmlString = value.toString();

            SAXBuilder builder = new SAXBuilder();
            Reader in = new StringReader(xmlString);

            try {
                Document doc = builder.build(in);
                Element root = doc.getRootElement();

                String reputation = root.getAttribute("Reputation").getValue() ;
                String userId = root.getAttribute("Id").getValue();
                //String row = root.toString();

                // Add this record to our map with the reputation as the key
                repToRecordMap.put(Integer.valueOf(reputation), value);

            } catch (JDOMException ex) {
                Logger.getLogger(TopTenMapper.class.getName()).log(Level.SEVERE, null, ex);
            } catch (IOException ex) {
                Logger.getLogger(TopTenMapper.class.getName()).log(Level.SEVERE, null, ex);
            }

            // If we have more than ten records, remove the one with the lowest reputation.
            if (repToRecordMap.size() > 10) {
                repToRecordMap.pollLastEntry();
            }

        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            // Output our ten records to the reducers with a null key
            for (Text t : repToRecordMap.values()) {
                context.write(NullWritable.get(), t);
                //System.err.println("VEMMM MAP: " + t);
            }
        }
    }

    public static class TopTenReducer extends Reducer<NullWritable, Text, NullWritable, Text> {
        // Stores a map of user reputation to the record
        // Overloads the comparator to order the reputations in descending order
        private TreeMap<Integer, Text> repToRecordMap = new TreeMap<Integer, Text>(Collections.reverseOrder());

        @Override
        public void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            SAXBuilder builder = new SAXBuilder();
            //System.out.println("SIZEE1 = " + repToRecordMap.size());
            try {
                for (Text value : values) {
                    //System.err.println("Reducer INPUTS: " + value);
                    String xmlString = value.toString();
                    Reader in = new StringReader(xmlString);

                    Document doc = builder.build(in);
                    Element root = doc.getRootElement();

                    String reputation = root.getAttribute("Reputation").getValue() ;
                    String userId = root.getAttribute("Id").getValue();

                    // Add this record to our map with the reputation as the key
                    repToRecordMap.put(Integer.valueOf(reputation), new Text(value.toString())); //WHYYYYYY???

                    //System.err.println("Reducer added: " + value);

                    // If we have more than ten records, remove the one with the lowest reputation
                    if (repToRecordMap.size() > 10) {
                        repToRecordMap.pollLastEntry();
                        //System.err.println("Reducer polled: " + value);
                    }
                }
                //System.out.println("SIZEE2 = " + repToRecordMap.size());
                //System.out.println("FIRST VALUE " + repToRecordMap.firstEntry());
                //System.out.println("LAST VALUE " + repToRecordMap.lastEntry());
            } catch (JDOMException ex) {
                Logger.getLogger(TopTenMapper.class.getName()).log(Level.SEVERE, null, ex);
            } catch (IOException ex) {
                Logger.getLogger(TopTenMapper.class.getName()).log(Level.SEVERE, null, ex);
            }

            for (Text t : repToRecordMap.values()) {
                //System.err.println("VEMMM REDUCE: " + t);
                //System.err.println("VEMMM REDUCE text: " + t.toString());

                // Output our ten records to the file system with a null key
                context.write(NullWritable.get(), t);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("xmlinput.start", "<row");
        conf.set("xmlinput.end", "/>");
        conf.set("mapred.max.split.size", "1000000");
        Job job = Job.getInstance(conf, "top ten counter");

        job.setNumReduceTasks(1);

        job.setJarByClass(TopTenCount.class);

        job.setMapperClass(TopTenMapper.class);
       // job.setCombinerClass(TopTenReducer.class);
        job.setReducerClass(TopTenReducer.class);

        job.setInputFormatClass(XmlInputFormat.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileSystem.get(conf).delete(new Path(args[2]),true);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
