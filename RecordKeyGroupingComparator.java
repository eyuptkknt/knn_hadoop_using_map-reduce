package example;
import org.apache.hadoop.io.RawComparator;

/**
 * This is used to group records so that all records in the same 
 * group are given to the same reducer.  In our case, this means
 * that everyone with the same key is put in the same group (that is,
 * two RecordKeys with the same key value cause compare to return a 0)
 */
class RecordKeyGroupingComparator implements RawComparator <RecordKey> {
  
  // this checks to see if the two point names are identical
  public int compare (byte [] keyLHS, int startLHS, int lenLHS, byte [] keyRHS, int startRHS, int lenRHS) {
    
    // get the length of the strings
    lenLHS = 0;
    for (int i = 0; i < 2; i++) {
      lenLHS += ((short) (keyLHS[i + startLHS] & 0xFF)) << ((1 - i) * 8);
    }
    lenRHS = 0;
    for (int i = 0; i < 2; i++) {
      lenRHS += ((short) (keyRHS[i + startRHS] & 0xFF)) << ((1 - i) * 8);
    }
    
    // if the lens are not the same, we are done
    if (lenLHS != lenRHS) {
      return lenLHS - lenRHS;
    }
    
    // if they are the same, then we compare the strings
    for (int i = 0; i < lenLHS; i++) {
      if (keyLHS[i + 2 + startLHS] != keyRHS[i + 2 + startRHS])
        return (keyLHS[i + 2 + startLHS] - keyRHS[i + 2 + startRHS]);
    }
    
    // if we got here, they are the same!
    return 0;
  }
  
  public int compare (RecordKey lhs, RecordKey rhs) {
    byte [] tempL = new byte [500], tempR = new byte [500];
    lhs.serialize (tempL);
    rhs.serialize (tempR);
    return compare (tempL, 0, 0, tempR, 0, 0);
  }
  
}

