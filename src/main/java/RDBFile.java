public class RDBFile {

  public static String fileDir;
  public static String fileName;

  public static void createRDBFile(String[] args) {
    if(args.length > 0){
      fileDir = args[1];
      fileName = args[3];
    }
  }

  public static String getFileDir() {
    return fileDir;
  }

  public static String getFileName() {
    return fileName;
  }

}
