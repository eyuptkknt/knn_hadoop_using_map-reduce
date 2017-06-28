package example;


import java.util.*;

import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;

public class KNN {
  
  static void printUsage() {
    System.out.println ("KNN <database location> <points to classify> <output> <k> <num reducers>");
    System.exit(-1);
  }
  
  public static void main (String [] args) throws Exception {
    
    // if we have the wrong number of args, then exit
    if (args.length != 5) {
      printUsage ();
      //return 1; 
    }
    
    // Get the default configuration object
    Configuration conf = new Configuration ();
    
    // set the number of clusters
    conf.set ("k", (args[3]));
    
    // set the serializations
    conf.set ("io.serializations", "RecordKeySerialization,org.apache.hadoop.io.serializer.WritableSerialization");

    // remember the directory having the points to classify
    conf.set ("pointsToClassify", args[1]);
    
    // get the new job
    Job job = new Job(conf);
    job.setJobName ("KNN Classification");
    
    // all of the inputs and outputs are text
    job.setMapOutputKeyClass (RecordKey.class);
    job.setMapOutputValueClass (RecordKey.class);
    job.setOutputKeyClass (Text.class);
    job.setOutputValueClass (Text.class);
    
    // tell Hadoop what mapper and reducer to use
    job.setMapperClass (KNNMapper.class);
    job.setReducerClass (KNNReducer.class);
    
    // set the number of reducers
    try {
      job.setNumReduceTasks (Integer.parseInt (args[4]));
    } catch (Exception e) {
      printUsage ();
      //return 0;
    }
    
    // set the input and output format class... these tell Haoop how to read/write to HDFS
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    
    // set the various sorting/grouping/paritioning classes
    job.setGroupingComparatorClass (RecordKeyGroupingComparator.class);
    job.setPartitionerClass (RecordKeyPartitioner.class);
    job.setSortComparatorClass (RecordKeySortComparator.class);
    
    // set the input and output files
    TextInputFormat.setInputPaths (job, args[0]);
    TextOutputFormat.setOutputPath (job, new Path (args[2]));
    
    // force the split size to 4 megs (this is small!)
    TextInputFormat.setMinInputSplitSize (job, 4 * 1024 * 1024);
    TextInputFormat.setMaxInputSplitSize (job, 4 * 1024 * 1024);
    
    // set the jar file to run
    job.setJarByClass (KNN.class);
    
    int exitCode = job.waitForCompletion(true) ? 0 : 1;
    if (exitCode != 0) {
      System.out.println("Job Failed!!!");
      //return exitCode;
    }
    
   // return 0;
  }
}

