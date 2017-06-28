package example;
// This class is used to hold String (the "key") Double (the "distance") pairs
class RecordKey {
  
  private String key; 
  private Double distance;
  
  // return the key
  public String getKey () {
    return key;
  }
  
  // return the distance 
  public Double getDistance () {
    return distance;
  }
  
  // construtor
  public RecordKey (String keyIn, Double distanceIn) {
    key = keyIn;
    distance = distanceIn;
  }
  
  // serialize the record
  public void serialize (byte [] outputArray) {
    
    // write the length of the string out
    byte [] data = getKey ().getBytes ();
    for (int i = 0; i < 2; i++) {
      outputArray[i] = (byte) ((data.length >>> ((1 - i) * 8)) & 0xFF);
    }
    
    // write the key out
    for (int i = 0; i < data.length; i++) {
      outputArray[i + 2] = data[i];   
    }
    
    // now write the distance out
    long bits = Double.doubleToLongBits (getDistance ());
    for (int i = 0; i < 8; i++) {
      outputArray[i + 2 + data.length] = (byte) ((bits >>> ((7 - i) * 8)) & 0xFF);
    }
  }
}

