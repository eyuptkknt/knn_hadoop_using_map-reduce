package example;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.io.OutputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.BufferedOutputStream;
import java.io.InputStream;
import java.io.FileInputStream;
import java.io.BufferedInputStream;
import org.apache.hadoop.io.serializer.Serialization;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.hadoop.io.serializer.Deserializer;

/**
 * This is the class that serves as a factory for RecordKey serialization and desrialization
 */

class RecordKeySerialization implements Serialization <RecordKey> {
  
  // tells Hadoop that we generate serializers and desrializers for RecordKeys
  public boolean accept (Class <?> c) {
    return c.equals (RecordKey.class);
  }
  
  public RecordKeySerializer getSerializer (Class <RecordKey> c) {
    return new RecordKeySerializer ();
  }
  
  public RecordKeyDeserializer getDeserializer (Class <RecordKey> c) {
    return new RecordKeyDeserializer ();
  }
  
  // this class takes care of serializing a RecordKey
  class RecordKeySerializer implements Serializer <RecordKey> {
    
    private OutputStream writeToMe;
    
    public void close () throws IOException {
      if (writeToMe != null)
        writeToMe.close ();
    }
    
    public void open (OutputStream out) throws IOException {
      writeToMe = out; 
    }
    
    public void serialize (RecordKey serializeMe) throws IOException {
      
      // get the string of the key
      byte [] myBytes = serializeMe.getKey ().getBytes ();
      
      // write the length of the string out
      byte [] outputArray = new byte [2]; 
      for (int i = 0; i < 2; i++) {
        outputArray[i] = (byte) ((myBytes.length >>> ((1 - i) * 8)) & 0xFF);
      }
      writeToMe.write (outputArray);
      
      // write the key out
      writeToMe.write (myBytes);
      
      // now write the distance out
      outputArray = new byte [8];
      long bits = Double.doubleToLongBits (serializeMe.getDistance ());
      for (int i = 0; i < 8; i++) {
        outputArray[i] = (byte) ((bits >>> ((7 - i) * 8)) & 0xFF);
      }
      writeToMe.write (outputArray);
    }
    
  }
  
  // this class takes care of desrializing a RecordKey
  class RecordKeyDeserializer implements Deserializer <RecordKey> {
    
    // this is the stream we deserialize from
    private InputStream getFromMe;
    
    public void open (InputStream in) throws IOException {
      getFromMe = in;
    }
    
    public void close () throws IOException {
      if (getFromMe != null)
        getFromMe.close ();
    }
  
    public RecordKey deserialize (RecordKey input) throws IOException {
     
      // this is the buffer we read to
      byte [] dataHere = new byte [2];
      getFromMe.read (dataHere);
      
      // get the length of the string
      short len = 0;
      for (int i = 0; i < 2; i++) {
        len += ((long) (dataHere[i] & 0xFF)) << ((1 - i) * 8);
      }
      
      // a second buffer
      byte [] newBuf = new byte[len];
      getFromMe.read (newBuf);
      String key = new String (newBuf);
      
      // now get the double
      long distance = 0;
      dataHere = new byte [8];
      getFromMe.read (dataHere);
      for (int i = 0; i < 8; i++) {
        distance += ((long) (dataHere[i] & 0xFF)) << ((7 - i) * 8);
      }
      
      return new RecordKey (key, Double.longBitsToDouble (distance));
    }
  }
}