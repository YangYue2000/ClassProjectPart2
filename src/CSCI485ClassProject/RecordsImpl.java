package CSCI485ClassProject;

import CSCI485ClassProject.models.AttributeType;
import CSCI485ClassProject.models.ComparisonOperator;
import CSCI485ClassProject.models.Record;
import CSCI485ClassProject.models.TableMetadata;
import com.apple.foundationdb.*;
import com.apple.foundationdb.async.AsyncIterable;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.directory.PathUtil;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;

import java.util.*;

public class RecordsImpl implements Records{

  private Database db;

  public RecordsImpl() {
    db = FDBHelper.initialization();
  }
  @Override
  public StatusCode insertRecord(String tableName, String[] primaryKeys, Object[] primaryKeysValues, String[] attrNames, Object[] attrValues) {
    Transaction tx = FDBHelper.openTransaction(db);
    // retrieve attributes of the table, check if attributes exists
    TableMetadataTransformer tblTransformer = new TableMetadataTransformer(tableName);
    List<String> tblAttributeDirPath = tblTransformer.getTableAttributeStorePath();
    List<FDBKVPair> kvPairs = FDBHelper.getAllKeyValuePairsOfSubdirectory(db, tx, tblAttributeDirPath);
    TableMetadata tblMetadata = tblTransformer.convertBackToTableMetadata(kvPairs);

    //check all primary keys exist
    List<String> pk = tblMetadata.getPrimaryKeys();
    if(!pk.equals(Arrays.asList(primaryKeys))){
      return StatusCode.DATA_RECORD_PRIMARY_KEYS_UNMATCHED;
    }
    //check all existing attrs are in row to insert
    HashMap<String, AttributeType> attr = tblMetadata.getAttributes();
    List<String> attrNameList = Arrays.asList(attrNames);
    for (Map.Entry<String,AttributeType> mapElement : attr.entrySet()) {
      String key = mapElement.getKey();
      if(!pk.contains(key)&&!attrNameList.contains(key)){
        return StatusCode.DATA_RECORD_CREATION_ATTRIBUTE_TYPE_UNMATCHED;
      }
    }
    //add new attributes to tablemeta, if any
    TableManagerImpl tableManager = new TableManagerImpl();
    for(int i=0; i<attrNames.length; i++){
      String attrName = attrNames[i];
      //if new attribute
      if(!attr.containsKey(attrName)){
        Object attrVal = attrValues[i];
        AttributeType attrType = AttributeType.NULL;
        if (attrVal instanceof Integer || attrVal instanceof Long) {
          attrType = AttributeType.INT;
        } else if (attrVal instanceof String) {
          attrType = AttributeType.VARCHAR;
        } else if (attrVal instanceof Double) {
          attrType = AttributeType.DOUBLE;
        }
        tableManager.addAttribute(tableName, attrName, attrType);
      }
    }

    //insert rows
    DirectorySubspace rootDirectory = DirectoryLayer.getDefault().createOrOpen(db,
            PathUtil.from("rootTable")).join();
    DirectorySubspace tableAttrValSpace = rootDirectory.createOrOpen(db, PathUtil.from(tableName)).join();

    //build key tuple
    Tuple keyTuple = new Tuple();
    for(String primarykey : primaryKeys){
      keyTuple.add(primarykey);
    }
    //build value tuple, which is a hashmap whose key is attrName and value is attrVal
    Tuple valueTuple = new Tuple();
    for(int i=0; i<primaryKeys.length; i++){
      valueTuple.add(new Tuple().add(primaryKeys[i]).addObject(primaryKeysValues[i]));
    }
    for(int i=0; i<attrNames.length; i++){
      valueTuple.add(new Tuple().add(attrNames[i]).addObject(attrValues[i]));
    }
    tx.set(tableAttrValSpace.pack(keyTuple), valueTuple.pack());
    FDBHelper.commitTransaction(tx);
    return StatusCode.SUCCESS;
  }

  @Override
  public Cursor openCursor(String tableName, String attrName, Object attrValue, ComparisonOperator operator, Cursor.Mode mode, boolean isUsingIndex) {
    return null;
  }

  @Override
  public Cursor openCursor(String tableName, Cursor.Mode mode) {
    return new Cursor(tableName, mode);
  }

  @Override
  public Record getFirst(Cursor cursor) {
    Transaction tx = FDBHelper.openTransaction(db);
    DirectorySubspace rootDirectory = DirectoryLayer.getDefault().createOrOpen(db,
            PathUtil.from("rootTable")).join();
    DirectorySubspace tableAttrValSpace = rootDirectory.createOrOpen(db, PathUtil.from(cursor.cursorTableName)).join();

    byte[] str  = new Subspace(new byte[]{(byte) 0x00}).getKey();
    byte[] firstKey = KeySelector.firstGreaterOrEqual(str).getKey();
    Tuple keyTuple = Tuple.fromBytes(firstKey);
    System.out.println("first key tuple: "+keyTuple);
    byte[] valBytes = tx.get(tableAttrValSpace.pack(keyTuple)).join();
    if (valBytes == null) {
      return null;
    }
    Tuple valueTuple = Tuple.fromBytes(valBytes);
    for(int i=0; i<valueTuple.size(); i++){
      Tuple AttrValPair = valueTuple.getNestedTuple(0);
      System.out.println("first value tuple: "+AttrValPair);
    }
    return null;
  }

  @Override
  public Record getLast(Cursor cursor) {
    return null;
  }

  @Override
  public Record getNext(Cursor cursor) {
    return null;
  }

  @Override
  public Record getPrevious(Cursor cursor) {
    return null;
  }

  @Override
  public StatusCode updateRecord(Cursor cursor, String[] attrNames, Object[] attrValues) {
    return null;
  }

  @Override
  public StatusCode deleteRecord(Cursor cursor) {
    return null;
  }

  @Override
  public StatusCode commitCursor(Cursor cursor) {
    return null;
  }

  @Override
  public StatusCode abortCursor(Cursor cursor) {
    return null;
  }

  @Override
  public StatusCode deleteDataRecord(String tableName, String[] attrNames, Object[] attrValues) {
    return null;
  }
}
