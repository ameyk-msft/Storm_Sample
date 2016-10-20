import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DfsDataStore implements IDataStore {
  private static final Logger logger = LoggerFactory.getLogger(DfsDataStore.class);

  private final String rootPathStr;
  private FileSystem fs;

  public DfsDataStore(String rootPathStr) {
    this.rootPathStr = rootPathStr;
  }

  @Override
  public boolean saveData(String pathStr, String data) {
    if(fs == null) {
      return false;
    }
    try {
      FSDataOutputStream stream = fs.create(new Path(pathStr));
      stream.writeUTF(data);
      stream.close();
      return true;
    }
    catch(IOException e) {
    	System.out.println("Exception in saveData " + e.getMessage());
      return false;
    }
  }

  @Override
  public String readData(String pathStr) {
    if(fs == null) {
    	System.out.println("FS is null");
    	return null;
    }
    try {
      FSDataInputStream stream = fs.open(new Path(pathStr));
      String ret = stream.readUTF();
      stream.close();
      return ret;
    }
    catch(IOException e) {
    	System.out.println("Exception in readData from " + pathStr + ", " + e.getMessage());
      return null;
    }
  }

  @Override
  public List<String> getChildren(String pathStr) {
    if(fs == null) {
      return null;
    }
    try {
      Path path = new Path(pathStr);
      List<String> ret = new ArrayList<String>();
      if(fs.isDirectory(path)) {
        FileStatus[] statuses = fs.listStatus(path);
        for(FileStatus status : statuses) {
          ret.add(status.getPath().getName().toString());
        }
      }
      return ret;
    } catch (IOException e) {
    	System.out.println("Exception in getChildren " + e.getMessage());
      return null;
    }
  }

  @Override
  public boolean delete(String pathStr) {
    try {
      if(exists(pathStr)) {
        fs.delete(new Path(pathStr), true);
      }
      return true;
    } catch (Exception e) {
    	System.out.println("Exception in delete " + e.getMessage());
      return false;
    }
  }

  @Override
  public boolean exists(String pathStr) throws Exception {
    return fs.exists(new Path(pathStr));
  }

  @Override
  public boolean open() {
    Configuration conf = new Configuration();
    Path p = new Path("/etc/hadoop/conf/core-site.xml");
    conf.addResource(p);
    Path path = new Path(rootPathStr);
    try {
      fs = path.getFileSystem(conf);
    }
    catch(IOException e) {
    	System.out.println("Exception in open() " + e.getMessage());
      e.printStackTrace();
      return false;
    }
    return true;
  }

  @Override
  public void close() {
    if(fs != null) {
      try {
        fs.close();
      }
      catch(IOException e) {
    	  System.out.println("Failed to close WASB " + e.getMessage());
      }
      fs = null;
    }
  }

}
