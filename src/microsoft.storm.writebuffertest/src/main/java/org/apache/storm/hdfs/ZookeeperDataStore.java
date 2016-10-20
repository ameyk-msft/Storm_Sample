import java.util.ArrayList;
import java.util.List;

import org.apache.zookeeper.data.Stat;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZookeeperDataStore implements IDataStore {
	  private static final Logger logger = LoggerFactory.getLogger(ZookeeperDataStore.class);

	  private final CuratorFramework curatorFramework;

	  public ZookeeperDataStore(String connectionString) {
	    this(connectionString, 3, 100);
	  }

	  public ZookeeperDataStore(String connectionString, int retries, int retryInterval) {
	    if (connectionString == null) {
	      connectionString = "localhost:2181";
	    }

	    RetryPolicy retryPolicy = new RetryNTimes(retries, retryInterval);
	    curatorFramework = CuratorFrameworkFactory.newClient(connectionString, retryPolicy);
	  }

	  @Override
	  public boolean open() {
	    curatorFramework.start();
	    return true;
	  }

	  @Override
	  public void close() {
	    curatorFramework.close();
	  }

 	  @Override
 	  public boolean saveData(String pathStr, String data) {
 	    data = data == null ? "" : data;
 	    byte[] bytes = data.getBytes();


 	    try {
 	      if (curatorFramework.checkExists().forPath(pathStr) == null) {
 	        curatorFramework.create().creatingParentsIfNeeded().forPath(pathStr, bytes);
 	      } else {
 	        curatorFramework.setData().forPath(pathStr, bytes);
 	      }
 	    } catch (Exception e) {
	      System.out.println("Exception in saveData() " + e.getMessage());
	      return false;
	    }
	    return true;
 	  }


	  @Override
	  public String readData(String pathStr) {
	    try {
	      if (curatorFramework.checkExists().forPath(pathStr) == null) {
	    	  System.out.println("Path does not exist: " + pathStr);
	        return null;
	      } else {
	        byte[] bytes = curatorFramework.getData().forPath(pathStr);
	        String data = new String(bytes);

	        return data;
	      }
	    } catch (Exception e) {
	    	System.out.println("Exception in readData() " + e.getMessage());
	      return null;
	    }
	  }

	  @Override
	  public List<String> getChildren(String pathStr) {
	    try {

	      List<String> relativePaths = curatorFramework.getChildren().forPath(pathStr);
	      return relativePaths;
	    }
	    catch (Exception e) {
	    	System.out.println("Exception in getChildren() " + e.getMessage());
        return null;
	    }
	  }

	  @Override
	  public boolean delete(String pathStr) {
	    try {
	      if(exists(pathStr)) {
	    	  System.out.println("PATH EXISTS: " + pathStr );
	          curatorFramework.delete().deletingChildrenIfNeeded().forPath(pathStr);
	      }
	      return true;
	    }
	    catch(Exception e) {
	    	System.out.println("Exception in delete() " + e.getMessage());
	      return false;
	    }
	  }

	  @Override
	  public boolean exists(String pathStr) throws Exception {
      if(curatorFramework.checkExists().forPath(pathStr) != null) {
        return true;
      }
      return false;
	  }
	}
