package example;

import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.io.Text;

/**
 * This little class partitions the KNN points to the various reducers
 */
class RecordKeyPartitioner extends Partitioner <RecordKey, RecordKey> {
  
  // the partitioning is just done based upon the key
  public int getPartition (RecordKey key, RecordKey value, int numReducers) {
    if (numReducers != 0)
      return (int) (key.getKey ().hashCode () % numReducers);
    else
      return 0;
  }
}
