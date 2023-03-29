package CSCI485ClassProject;

public class Cursor {
  public enum Mode {
    READ,
    READ_WRITE
  }


  // your code here
  String cursorTableName;
  Mode cursorMode;
  int index;
  Cursor(String cursorTableName, Mode cursorMode){
    this.cursorTableName = cursorTableName;
    this.cursorMode = cursorMode;
    index = 0;
  }
}
