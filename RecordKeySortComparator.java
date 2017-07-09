package example;
import org.apache.hadoop.io.RawComparator;

/**
 * This is used to sort records.  The sort order is as follows.  If the
 * key strings differ in length, then the smaller string is the "lesser" of the two.
 * If the key strings are the same lenth, then we loop through the key bytes and find
 * the first bytes that differ; the guy with the smaller byte value is the lesser of
 * the two.  If the two key strings are the same, then the one with the smaller distance
 * is the lesser of the two.
 */
class RecordKeySortComparator implements RawComparator <RecordKey> {
  
  // this checks to see if the two point names are identical
  public int compare(byte [] keyLHS, int startLHS, int lenLHS, byte [] keyRHS, int startRHS, int lenRHS) {
    
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
    
    // if we got here, the strings are the same, so we look at the distances
    long leftLenAsALong = 0, rightLenAsALong = 0;
    for (int i = 0; i < 8; i++) {
      leftLenAsALong += ((long) (keyLHS[i + 2 + lenLHS + startLHS] & 0xFF)) << ((7 - i) * 8);
      rightLenAsALong += ((long) (keyRHS[i + 2 + lenRHS + startRHS] & 0xFF)) << ((7 - i) * 8);
    }
    
    if (Double.longBitsToDouble (leftLenAsALong) < Double.longBitsToDouble (rightLenAsALong))
      return -1;
    
    if (Double.longBitsToDouble (leftLenAsALong) > Double.longBitsToDouble (rightLenAsALong))
      return 1;
    
    return 0;
  }
  
  public int compare (RecordKey lhs, RecordKey rhs) {
    byte [] tempL = new byte [500], tempR = new byte [500];
    lhs.serialize (tempL);
    rhs.serialize (tempR);
    return compare (tempL, 0, 0, tempR, 0, 0);
  }
    
}

