import java.util.List;

public interface IDataStore {
  /**
   * Save data in the "file" specified by the pathStr.
   * If the parents of pathStr does not exist, create them first.
   * @param pathStr
   * @param data
   * @return true for success, or false if the pathStr is not valid or is not
   * a valid "file"
   */
  boolean saveData(String pathStr, String data);

  /**
   * Read data from the "file" specified by the pathStr.
   * @param pathStr
   * @return stored data in pathStr, or null if pathStr is not valid or is
   * not a valid "file".
   */
  String readData(String pathStr);

  /**
   * Enumerate all children given the parent's pathStr.
   * @param pathStr
   * @return children's relative path to parent, or null when any error happens during
   * enumeration.
   */
  List<String> getChildren(String pathStr);

  /**
   * Remove all files and folders under pathStr
   * @param pathStr
   * @return true if succeeded, otherwise false
   */
  boolean delete(String pathStr);

  /**
   * Check if a path exists
   * @param pathStr
   * @return true if the path exists
   * @throws Exception when it failed to determine if a path exists
   */
  boolean exists(String pathStr) throws Exception;

  /**
   * Open connection to the target
   * @return true for success, or false.
   */
  boolean open();

  /**
   * Close connection to the target
   */
  void close();
}
