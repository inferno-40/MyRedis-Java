package resp;

import java.util.List;

public class RespConvertor {

  public final static String NULL_BULK_STRING = "$-1\r\n";

  public static String toBulkString(String input) {
    if (input == null) {
      return NULL_BULK_STRING;
    }
    return "$" + input.length() + "\r\n" + input + "\r\n";
  }


  public static String toRESPArray(List<String> input) {
    int len = input.size();
    StringBuilder output = new StringBuilder(String.format("*%d\r\n", len));
    for (String in : input) {
      output.append(toBulkString(in));
    }
    return output.toString();
  }

}
