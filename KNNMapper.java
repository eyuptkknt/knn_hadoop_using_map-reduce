package example;
import java.io.IOException;
import java.util.*;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;

import java.io.IOException;
import java.util.*;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;

public class KNNMapper extends Mapper <LongWritable, Text, RecordKey, RecordKey> {
  
  // this is the list of points to classify
  private ArrayList <VectorizedObject> pointsToClassify = new ArrayList <VectorizedObject> (0);
  
  // this is called to set up the mapper... it basically just reads the clusters file into memory
  protected void setup (Context context) throws IOException, InterruptedException {
    
    // first we open up the file of points to classify
    Configuration conf = context.getConfiguration ();
    FileSystem dfs = FileSystem.get (conf);
    
    // if we can't find it in the configuration, then die
    if (conf.get ("pointsToClassify") == null)
      throw new RuntimeException ("no directory with the points to classify!");
    
    // now, list the files in that directory
    FileSystem fs = FileSystem.get (conf); 
    Path path = new Path (conf.get ("pointsToClassify")); 
    FileStatus fstatus[] = fs.listStatus (path);
    
    // load up all of the points to classify from the files in that directory 
    for (FileStatus f: fstatus) {
    
      // ignore files that start with an underscore, since they just describe Hadoop output
      if (f.getPath().toUri().getPath().contains ("/_"))
        continue;
      
      FSDataInputStream input = dfs.open (f.getPath ());
      BufferedReader myReader = new BufferedReader (new InputStreamReader (input));
      
      // and now we read in all of the points to classify from this file
      String cur = myReader.readLine (); 
      while (cur != null) {
        VectorizedObject temp = new VectorizedObject (cur);
        pointsToClassify.add (temp);
        cur = myReader.readLine (); 
      }
    }
  } 
  
  public void map (LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    
    // process the next data point
    String cur = value.toString();
    VectorizedObject temp = new VectorizedObject (cur);
    
    // now, compare it with each of the points we are trying to classify to get the distance 
    for (VectorizedObject i : pointsToClassify) {
      
      // and write the name of the point we are trying to classify, the distance, and the label
      RecordKey keyOut = new RecordKey (i.getKey (), i.getLocation ().distance (temp.getLocation ()));
      RecordKey valueOut = new RecordKey (temp.getValue (), i.getLocation ().distance (temp.getLocation ()));
      context.write (keyOut, valueOut);
    }
    
  }
  
}
