package example;
import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;

public class KNNReducer extends Reducer <RecordKey, RecordKey, Text, Text> {
  
  // the "k" to use in the KNN classification
  private int k;
  
  protected void setup (Context context) throws IOException, InterruptedException {
    
    // first we open up the clusters file
    Configuration conf = context.getConfiguration ();
    
    // if we can't find it in the configuration, then die
    if (conf.get ("k") == null)
      throw new RuntimeException ("no cluster file!");
    
    k = Integer.parseInt (conf.get ("k"));
  }
  
  // key is a (point to classify, distance) pair, and values is a set of (label, distance) pairs
  public void reduce (RecordKey key, Iterable <RecordKey> values, Context context) throws IOException, InterruptedException {
    
    // this will tell us how many of each class we have in the top k
    HashMap <String, Integer> myCounts = new HashMap <String, Integer> ();
    
    // look through all of the distances
    int counter = 0;
    for (RecordKey i : values) {
      
      // if we are at the kth one, get outta here
      if (counter == k)
        break;
      else
        counter++;
    
      // add the count for this class in
      if (myCounts.get (i.getKey ()) == null) {
        myCounts.put (i.getKey (), new Integer (1)); 
      } else {
        Integer temp = myCounts.get (i.getKey ());
        myCounts.put (i.getKey (), temp + 1);
      }
    }
    
    // this finds the class label with the highest count
    String bestString = null;
    int bestCount = -1;
    for (Map.Entry <String, Integer> i : myCounts.entrySet ()) {
      Integer curCount = i.getValue ();
      if (curCount > bestCount) {
        bestCount = curCount;
        bestString = i.getKey ();  
      }
    } 
    
    // and here we write the answer to the output
    Text keyOut = new Text ();
    Text valOut = new Text ();
    keyOut.set (key.getKey ());
    valOut.set (bestString);
    context.write (keyOut, valOut);
    
  }
  
}
