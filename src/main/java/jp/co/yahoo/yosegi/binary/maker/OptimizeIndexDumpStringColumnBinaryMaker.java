/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package jp.co.yahoo.yosegi.binary.maker;

import jp.co.yahoo.yosegi.binary.ColumnBinary;
import jp.co.yahoo.yosegi.binary.ColumnBinaryMakerConfig;
import jp.co.yahoo.yosegi.binary.ColumnBinaryMakerCustomConfigNode;
import jp.co.yahoo.yosegi.binary.CompressResultNode;
import jp.co.yahoo.yosegi.blockindex.BlockIndexNode;
import jp.co.yahoo.yosegi.blockindex.StringRangeBlockIndex;
import jp.co.yahoo.yosegi.compressor.FindCompressor;
import jp.co.yahoo.yosegi.compressor.ICompressor;
import jp.co.yahoo.yosegi.inmemory.IDictionaryLoader;
import jp.co.yahoo.yosegi.inmemory.ILoader;
import jp.co.yahoo.yosegi.inmemory.ISequentialLoader;
import jp.co.yahoo.yosegi.inmemory.LoadType;
import jp.co.yahoo.yosegi.message.objects.StringObj;
import jp.co.yahoo.yosegi.spread.analyzer.IColumnAnalizeResult;
import jp.co.yahoo.yosegi.spread.analyzer.StringColumnAnalizeResult;
import jp.co.yahoo.yosegi.spread.column.ColumnType;
import jp.co.yahoo.yosegi.spread.column.ICell;
import jp.co.yahoo.yosegi.spread.column.IColumn;
import jp.co.yahoo.yosegi.spread.column.PrimitiveCell;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class OptimizeIndexDumpStringColumnBinaryMaker implements IColumnBinaryMaker {

  /**
   * Determine the type of minimum value of Diff.
   */
  public static ColumnType getDiffColumnType( final int min , final int max ) {
    int diff = max - min;
    if ( diff < 0 ) {
      return ColumnType.INTEGER;
    }

    if ( diff <= Byte.MAX_VALUE ) {
      return ColumnType.BYTE;
    } else if ( diff <= Short.MAX_VALUE ) {
      return ColumnType.SHORT;
    }

    return ColumnType.INTEGER;
  }

  /**
   * Create the smallest ILengthMaker from the minimum and maximum length of the String.
   */
  public static ILengthMaker chooseLengthMaker( final int min , final int max ) {
    if ( min == max ) {
      return new FixedLengthMaker( min );
    }

    ColumnType diffType = getDiffColumnType( min , max );
    if ( max <= Byte.valueOf( Byte.MAX_VALUE ).intValue() ) {
      return new ByteLengthMaker();
    } else if ( diffType == ColumnType.BYTE ) {
      return new DiffByteLengthMaker( min );
    } else if ( max <= Short.valueOf( Short.MAX_VALUE ).intValue() ) {
      return new ShortLengthMaker();
    } else if ( diffType == ColumnType.SHORT ) {
      return new DiffShortLengthMaker( min );
    } else {
      return new IntLengthMaker();
    }
  }

  /**
   * Create the smallest IIndexMaker from the index
   * where the value first appears and the last index.
   */
  public static IIndexMaker chooseIndexMaker( final int startIndex , final int lastIndex ) {
    ColumnType diffType = getDiffColumnType( startIndex , lastIndex );
    if ( lastIndex < Byte.valueOf( Byte.MAX_VALUE ).intValue() ) {
      return new ByteIndexMaker();
    } else if ( diffType == ColumnType.BYTE ) {
      return new DiffByteIndexMaker( startIndex );
    } else if ( lastIndex < Short.valueOf( Short.MAX_VALUE ).intValue() ) {
      return new ShortIndexMaker();
    } else if ( diffType == ColumnType.SHORT ) {
      return new DiffShortIndexMaker( startIndex );
    } else {
      return new IntIndexMaker();
    }
  }

  public interface ILengthMaker {

    int calcBinarySize( final int columnSize );

    void create(
        final List<byte[]> objList , final ByteBuffer wrapBuffer ) throws IOException;

    int[] getLengthArray( final ByteBuffer wrapBuffer , final int size ) throws IOException;

  }

  public static class FixedLengthMaker implements ILengthMaker {

    private final int min;

    public FixedLengthMaker( final int min ) {
      this.min = min;
    }

    @Override
    public int calcBinarySize( final int columnSize ) {
      return 0;
    }

    @Override
    public void create(
        final List<byte[]> objList , final ByteBuffer wrapBuffer ) throws IOException {
      return;
    }

    @Override
    public int[] getLengthArray( final ByteBuffer wrapBuffer , final int size ) throws IOException {
      int[] result = new int[size];
      for ( int i = 0 ; i < result.length ; i++ ) {
        result[i] = min;
      }
      return result;
    }

  }

  public static class ByteLengthMaker implements ILengthMaker {

    @Override
    public int calcBinarySize( final int columnSize ) {
      return Byte.BYTES * columnSize;
    }

    @Override
    public void create(
        final List<byte[]> objList , final ByteBuffer wrapBuffer ) throws IOException {
      for ( byte[] obj : objList ) {
        wrapBuffer.put( (byte)obj.length );
      }
    }

    @Override
    public int[] getLengthArray( final ByteBuffer wrapBuffer , final int size ) throws IOException {
      int[] result = new int[size];
      for ( int i = 0 ; i < size ; i++ ) {
        result[i] = wrapBuffer.get();
      }
      return result;
    }

  }

  public static class DiffByteLengthMaker implements ILengthMaker {

    private final int min;

    public DiffByteLengthMaker( final int min ) {
      this.min = min;
    }

    @Override
    public int calcBinarySize( final int columnSize ) {
      return Byte.BYTES * columnSize;
    }

    @Override
    public void create(
        final List<byte[]> objList , final ByteBuffer wrapBuffer ) throws IOException {
      for ( byte[] obj : objList ) {
        wrapBuffer.put( (byte)( obj.length - min ) );
      }
    }

    @Override
    public int[] getLengthArray( final ByteBuffer wrapBuffer , final int size ) throws IOException {
      int[] result = new int[size];
      for ( int i = 0 ; i < size ; i++ ) {
        result[i] = wrapBuffer.get() + min;
      }
      return result;
    }

  }

  public static class ShortLengthMaker implements ILengthMaker {

    @Override
    public int calcBinarySize( final int columnSize ) {
      return Short.BYTES * columnSize;
    }

    @Override
    public void create(
        final List<byte[]> objList , final ByteBuffer wrapBuffer ) throws IOException {
      for ( byte[] obj : objList ) {
        wrapBuffer.putShort( (short)obj.length );
      }
    }

    @Override
    public int[] getLengthArray( final ByteBuffer wrapBuffer , final int size ) throws IOException {
      int[] result = new int[size];
      for ( int i = 0 ; i < size ; i++ ) {
        result[i] = wrapBuffer.getShort();
      }
      return result;
    }

  }

  public static class DiffShortLengthMaker implements ILengthMaker {

    private final int min;

    public DiffShortLengthMaker( final int min ) {
      this.min = min;
    }

    @Override
    public int calcBinarySize( final int columnSize ) {
      return Short.BYTES * columnSize;
    }

    @Override
    public void create(
        final List<byte[]> objList , final ByteBuffer wrapBuffer ) throws IOException {
      for ( byte[] obj : objList ) {
        wrapBuffer.putShort( (short)( obj.length - min ) );
      }
    }

    @Override
    public int[] getLengthArray( final ByteBuffer wrapBuffer , final int size ) throws IOException {
      int[] result = new int[size];
      for ( int i = 0 ; i < size ; i++ ) {
        result[i] = wrapBuffer.getShort() + min;
      }
      return result;
    }

  }

  public static class IntLengthMaker implements ILengthMaker {

    @Override
    public int calcBinarySize( final int columnSize ) {
      return Integer.BYTES * columnSize;
    }

    @Override
    public void create(
        final List<byte[]> objList , final ByteBuffer wrapBuffer ) throws IOException {
      for ( byte[] obj : objList ) {
        wrapBuffer.putInt( obj.length );
      }
    }

    @Override
    public int[] getLengthArray( final ByteBuffer wrapBuffer , final int size ) throws IOException {
      int[] result = new int[size];
      for ( int i = 0 ; i < size ; i++ ) {
        result[i] = wrapBuffer.getInt();
      }
      return result;
    }

  }

  public interface IIndexMaker {

    int calcBinarySize( final int size );

    void create(
        final List<Integer> indexList , final ByteBuffer wrapBuffer ) throws IOException;

    int[] getIndexArray( final int size , final ByteBuffer wrapBuffer ) throws IOException;

  }

  public static class ByteIndexMaker implements IIndexMaker {

    @Override
    public int calcBinarySize( final int size ) {
      return Byte.BYTES * size;
    }

    @Override
    public void create(
        final List<Integer> indexList , final ByteBuffer wrapBuffer ) throws IOException {
      for ( Integer index : indexList ) {
        wrapBuffer.put( index.byteValue() );
      }
    }

    @Override
    public int[] getIndexArray( final int size , final ByteBuffer wrapBuffer ) throws IOException {
      int[] result = new int[size];
      for ( int i = 0 ; i < size ; i++ ) {
        result[i] = (int)( wrapBuffer.get() );
      }
      return result;
    }

  }

  public static class DiffByteIndexMaker implements IIndexMaker {

    private final int min;

    public DiffByteIndexMaker( final int min ) {
      this.min = min;
    }

    @Override
    public int calcBinarySize( final int size ) {
      return Byte.BYTES * size;
    }

    @Override
    public void create(
        final List<Integer> indexList , final ByteBuffer wrapBuffer ) throws IOException {
      for ( Integer index : indexList ) {
        wrapBuffer.put( (byte)( index.intValue() - min ) );
      }
    }

    @Override
    public int[] getIndexArray( final int size , final ByteBuffer wrapBuffer ) throws IOException {
      int[] result = new int[size];
      for ( int i = 0 ; i < size ; i++ ) {
        result[i] = (int)( wrapBuffer.get() ) + min;
      }
      return result;
    }

  }

  public static class ShortIndexMaker implements IIndexMaker {

    @Override
    public int calcBinarySize( final int size ) {
      return Short.BYTES * size;
    }

    @Override
    public void create(
        final List<Integer> indexList , final ByteBuffer wrapBuffer ) throws IOException {
      for ( Integer index : indexList ) {
        wrapBuffer.putShort( index.shortValue() );
      }
    }

    @Override
    public int[] getIndexArray( final int size , final ByteBuffer wrapBuffer ) throws IOException {
      int[] result = new int[size];
      for ( int i = 0 ; i < size ; i++ ) {
        result[i] = (int)( wrapBuffer.getShort() );
      }
      return result;
    }

  }

  public static class DiffShortIndexMaker implements IIndexMaker {

    private final int min;

    public DiffShortIndexMaker( final int min ) {
      this.min = min;
    }

    @Override
    public int calcBinarySize( final int size ) {
      return Short.BYTES * size;
    }

    @Override
    public void create(
        final List<Integer> indexList , final ByteBuffer wrapBuffer ) throws IOException {
      for ( Integer index : indexList ) {
        wrapBuffer.putShort( (short)( index.intValue() - min ) );
      }
    }

    @Override
    public int[] getIndexArray( final int size , final ByteBuffer wrapBuffer ) throws IOException {
      int[] result = new int[size];
      for ( int i = 0 ; i < size ; i++ ) {
        result[i] = (int)( wrapBuffer.getShort() ) + min;
      }
      return result;
    }

  }

  public static class IntIndexMaker implements IIndexMaker {

    @Override
    public int calcBinarySize( final int size ) {
      return Integer.BYTES * size;
    }

    @Override
    public void create(
        final List<Integer> indexList , final ByteBuffer wrapBuffer ) throws IOException {
      for ( Integer index : indexList ) {
        wrapBuffer.putInt( index.intValue() );
      }
    }

    @Override
    public int[] getIndexArray( final int size , final ByteBuffer wrapBuffer ) throws IOException {
      int[] result = new int[size];
      for ( int i = 0 ; i < size ; i++ ) {
        result[i] = wrapBuffer.getInt();
      }
      return result;
    }

  }

  @Override
  public ColumnBinary toBinary(
      final ColumnBinaryMakerConfig commonConfig ,
      final ColumnBinaryMakerCustomConfigNode currentConfigNode ,
      final CompressResultNode compressResultNode ,
      final IColumn column ) throws IOException {
    if ( column.size() == 0 ) {
      return new UnsupportedColumnBinaryMaker()
          .toBinary( commonConfig , currentConfigNode , compressResultNode , column );
    }

    ColumnBinaryMakerConfig currentConfig = commonConfig;
    if ( currentConfigNode != null ) {
      currentConfig = currentConfigNode.getCurrentConfig();
    }

    List<Integer> indexList = new ArrayList<Integer>();
    List<byte[]> stringList = new ArrayList<byte[]>();

    int totalLength = 0;
    int logicalDataLength = 0;
    boolean hasNull = false;
    String min = null;
    String max = "";
    int minLength = Integer.MAX_VALUE;
    int maxLength = 0;
    int startIndex = -1;
    int lastIndex = 0;
    for ( int i = 0 ; i < column.size() ; i++ ) {
      ICell cell = column.get(i);
      if ( cell.getType() == ColumnType.NULL ) {
        hasNull = true;
        continue;
      }
      PrimitiveCell byteCell = (PrimitiveCell) cell;
      String strObj = byteCell.getRow().getString();
      if ( strObj == null ) {
        hasNull = true;
        continue;
      }
      if ( startIndex == -1 ) {
        startIndex = i;
      }
      lastIndex = i;
      byte[] obj = strObj.getBytes( "UTF-8" );
      indexList.add( i );
      stringList.add( obj );
      if ( maxLength < obj.length ) {
        maxLength = obj.length;
      }
      if ( obj.length < minLength ) {
        minLength = obj.length;
      }
      totalLength += obj.length;
      if ( max.compareTo( strObj ) < 0 ) {
        max = strObj;
      }
      if ( min == null || 0 < min.compareTo( strObj ) ) {
        min = strObj;
      }
      logicalDataLength += strObj.length() * Character.BYTES;
    }

    if ( ! hasNull && min.equals( max ) ) {
      return ConstantColumnBinaryMaker.createColumnBinary(
          new StringObj( min ) , column.getColumnName() , column.size() );
    }

    IIndexMaker indexMaker = chooseIndexMaker( startIndex , lastIndex );
    ILengthMaker lengthMaker = chooseLengthMaker( minLength , maxLength );

    int indexBinaryLength = indexMaker.calcBinarySize( indexList.size() );
    int lengthBinaryLength = lengthMaker.calcBinarySize( stringList.size() );

    int binaryLength = Integer.BYTES * 5 + indexBinaryLength + lengthBinaryLength + totalLength;
    byte[] binaryRaw = new byte[binaryLength];
    ByteBuffer wrapBuffer = ByteBuffer.wrap( binaryRaw , 0 , binaryRaw.length );
    wrapBuffer.putInt( minLength );
    wrapBuffer.putInt( maxLength );
    wrapBuffer.putInt( startIndex );
    wrapBuffer.putInt( lastIndex );
    wrapBuffer.putInt( indexList.size() );
    indexMaker.create( indexList , wrapBuffer );
    lengthMaker.create( stringList , wrapBuffer );
    for ( byte[] obj : stringList ) {
      wrapBuffer.put( obj );
    }
    byte[] compressBinaryRaw =
        currentConfig.compressorClass.compress( binaryRaw , 0 , binaryRaw.length );

    int minCharLength = Character.BYTES * min.length();
    int maxCharLength = Character.BYTES * max.length();
    int headerSize = Integer.BYTES + minCharLength + Integer.BYTES + maxCharLength;

    byte[] binary = new byte[headerSize + compressBinaryRaw.length];
    ByteBuffer binaryWrapBuffer = ByteBuffer.wrap( binary );
    binaryWrapBuffer.putInt( minCharLength );
    binaryWrapBuffer.asCharBuffer().put( min );
    binaryWrapBuffer.position( binaryWrapBuffer.position() + minCharLength );
    binaryWrapBuffer.putInt( maxCharLength );
    binaryWrapBuffer.asCharBuffer().put( max );
    binaryWrapBuffer.position( binaryWrapBuffer.position() + maxCharLength );
    binaryWrapBuffer.put( compressBinaryRaw );

    return new ColumnBinary(
        this.getClass().getName() ,
        currentConfig.compressorClass.getClass().getName() ,
        column.getColumnName() ,
        ColumnType.STRING ,
        column.size() ,
        binaryRaw.length ,
        logicalDataLength ,
        stringList.size() ,
        binary ,
        0 ,
        binary.length ,
        null );
  }

  @Override
  public int calcBinarySize( final IColumnAnalizeResult analizeResult ) {
    StringColumnAnalizeResult stringAnalizeResult = (StringColumnAnalizeResult)analizeResult;
    boolean hasNull = analizeResult.getNullCount() != 0;
    if ( ! hasNull && analizeResult.getUniqCount() == 1 ) {
      return stringAnalizeResult.getUniqUtf8ByteSize();
    }

    IIndexMaker indexMaker = chooseIndexMaker(
        stringAnalizeResult.getRowStart() , stringAnalizeResult.getRowEnd() );
    ILengthMaker lengthMaker = chooseLengthMaker(
        stringAnalizeResult.getMinUtf8Bytes() , stringAnalizeResult.getMaxUtf8Bytes() );

    int indexBinaryLength = indexMaker.calcBinarySize( stringAnalizeResult.getRowCount() );
    int lengthBinaryLength = lengthMaker.calcBinarySize( stringAnalizeResult.getRowCount() );

    return Integer.BYTES * 5
        + indexBinaryLength
        + lengthBinaryLength
        + stringAnalizeResult.getTotalUtf8ByteSize();
  }

  @Override
  public LoadType getLoadType(final ColumnBinary columnBinary , final int loadSize ) {
    if (columnBinary.isSetLoadSize) {
      return LoadType.DICTIONARY;
    }
    return LoadType.SEQUENTIAL;
  }

  private void loadFromColumnBinary(final ColumnBinary columnBinary, final ISequentialLoader loader)
      throws IOException {
    ByteBuffer headerWrapBuffer =
        ByteBuffer.wrap(columnBinary.binary, columnBinary.binaryStart, columnBinary.binaryLength);
    int minCharLength = headerWrapBuffer.getInt();
    headerWrapBuffer.position(headerWrapBuffer.position() + minCharLength);

    int maxCharLength = headerWrapBuffer.getInt();
    headerWrapBuffer.position(headerWrapBuffer.position() + maxCharLength);
    int headerSize = Integer.BYTES + minCharLength + Integer.BYTES + maxCharLength;

    ICompressor compressor = FindCompressor.get(columnBinary.compressorClassName);
    byte[] binary =
        compressor.decompress(
            columnBinary.binary,
            columnBinary.binaryStart + headerSize,
            columnBinary.binaryLength - headerSize);
    ByteBuffer wrapBuffer = ByteBuffer.wrap(binary, 0, binary.length);
    int minLength = wrapBuffer.getInt();
    int maxLength = wrapBuffer.getInt();
    int startIndex = wrapBuffer.getInt();
    int lastIndex = wrapBuffer.getInt();
    int indexSize = wrapBuffer.getInt();

    IIndexMaker indexMaker = chooseIndexMaker(startIndex, lastIndex);
    ILengthMaker lengthMaker = chooseLengthMaker(minLength, maxLength);

    int[] indexArray = indexMaker.getIndexArray(indexSize, wrapBuffer);
    int[] lengthArray = lengthMaker.getLengthArray(wrapBuffer, columnBinary.cardinality);

    int currentStart = wrapBuffer.position();

    int currentIndex = 0;
    for (int i = 0; i < indexArray.length; i++) {
      for (; currentIndex < indexArray[i]; currentIndex++) {
        loader.setNull(currentIndex);
      }
      loader.setBytes(currentIndex, binary, currentStart, lengthArray[i]);
      currentStart += lengthArray[i];
      currentIndex++;
    }
    for (int i = currentIndex; i < columnBinary.rowCount; i++) {
      loader.setNull(i);
    }
    // NOTE: null padding up to load size.
    for (int i = columnBinary.rowCount; i < loader.getLoadSize(); i++) {
      loader.setNull(i);
    }
  }

  private void loadFromExpandColumnBinary(
      final ColumnBinary columnBinary, final IDictionaryLoader loader) throws IOException {
    ByteBuffer headerWrapBuffer =
        ByteBuffer.wrap(columnBinary.binary, columnBinary.binaryStart, columnBinary.binaryLength);
    int minCharLength = headerWrapBuffer.getInt();
    headerWrapBuffer.position(headerWrapBuffer.position() + minCharLength);

    int maxCharLength = headerWrapBuffer.getInt();
    headerWrapBuffer.position(headerWrapBuffer.position() + maxCharLength);
    int headerSize = Integer.BYTES + minCharLength + Integer.BYTES + maxCharLength;

    ICompressor compressor = FindCompressor.get(columnBinary.compressorClassName);
    byte[] binary =
        compressor.decompress(
            columnBinary.binary,
            columnBinary.binaryStart + headerSize,
            columnBinary.binaryLength - headerSize);
    ByteBuffer wrapBuffer = ByteBuffer.wrap(binary, 0, binary.length);
    int minLength = wrapBuffer.getInt();
    int maxLength = wrapBuffer.getInt();
    int startIndex = wrapBuffer.getInt();
    int lastIndex = wrapBuffer.getInt();
    int indexSize = wrapBuffer.getInt();

    IIndexMaker indexMaker = chooseIndexMaker(startIndex, lastIndex);
    ILengthMaker lengthMaker = chooseLengthMaker(minLength, maxLength);

    int[] indexArray = indexMaker.getIndexArray(indexSize, wrapBuffer);
    int[] lengthArray = lengthMaker.getLengthArray(wrapBuffer, columnBinary.cardinality);

    // NOTE: Calculate dictionarySize
    int dictionarySize = 0;
    int lastIndexArrayOffset = indexArray.length - 1;
    int lastIndexArrayValue = indexArray[lastIndexArrayOffset];
    int indexArrayValue = indexArray[0];
    int indexArrayOffset = 1;
    for (int i = 0; i < columnBinary.repetitions.length; i++) {
      if (columnBinary.repetitions[i] < 0) {
        throw new IOException("Repetition must be equal to or greater than 0.");
      }
      if (i > lastIndexArrayValue) {
        continue;
      }
      if (i > indexArrayValue) {
        indexArrayValue = indexArray[indexArrayOffset];
        indexArrayOffset++;
      }
      if (columnBinary.repetitions[i] == 0 || i < indexArrayValue) {
        continue;
      }
      dictionarySize++;
    }
    loader.createDictionary(dictionarySize);

    // NOTE:
    //   Set value to dict: dictionaryIndex, value
    //   Set dictionaryIndex: currentIndex, dictionaryIndex
    int dictionaryIndex = 0;
    int currentIndex = 0;
    int lengthArrayValue = lengthArray[0];
    indexArrayValue = indexArray[0]; // NOTE: reset value
    indexArrayOffset = 1; // NOTE: reset value
    int nextStart = wrapBuffer.position();
    for (int i = 0; i < columnBinary.repetitions.length; i++) {
      if (i > indexArrayValue && indexArrayOffset < indexArray.length) {
        indexArrayValue = indexArray[indexArrayOffset];
        lengthArrayValue = lengthArray[indexArrayOffset];
        indexArrayOffset++;
      }
      if (columnBinary.repetitions[i] == 0) {
        if (i == indexArrayValue) {
          nextStart += lengthArrayValue;
        }
        continue;
      }
      if (i > lastIndexArrayValue) {
        for (int j = 0; j < columnBinary.repetitions[i]; j++) {
          loader.setNull(currentIndex);
          currentIndex++;
        }
        continue;
      }
      if (i == indexArrayValue) {
        int currentStart = nextStart;
        loader.setBytesToDic(dictionaryIndex, binary, currentStart, lengthArrayValue);
        for (int j = 0; j < columnBinary.repetitions[i]; j++) {
          loader.setDictionaryIndex(currentIndex, dictionaryIndex);
          currentIndex++;
        }
        nextStart += lengthArrayValue;
        dictionaryIndex++;
      } else {
        for (int j = 0; j < columnBinary.repetitions[i]; j++) {
          loader.setNull(currentIndex);
          currentIndex++;
        }
      }
    }
  }

  @Override
  public void load(final ColumnBinary columnBinary, final ILoader loader) throws IOException {
    if (columnBinary.isSetLoadSize) {
      if (loader.getLoaderType() != LoadType.DICTIONARY) {
        throw new IOException("Loader type is not DICTIONARY.");
      }
      loadFromExpandColumnBinary(columnBinary, (IDictionaryLoader) loader);
    } else {
      if (loader.getLoaderType() != LoadType.SEQUENTIAL) {
        throw new IOException("Loader type is not SEQUENTIAL.");
      }
      loadFromColumnBinary(columnBinary, (ISequentialLoader) loader);
    }
    loader.finish();
  }

  @Override
  public void setBlockIndexNode(
      final BlockIndexNode parentNode ,
      final ColumnBinary columnBinary ,
      final int spreadIndex ) throws IOException {
    ByteBuffer wrapBuffer = ByteBuffer.wrap(
        columnBinary.binary , columnBinary.binaryStart , columnBinary.binaryLength );
    int minLength = wrapBuffer.getInt();
    char[] minCharArray = new char[minLength / Character.BYTES];
    wrapBuffer.asCharBuffer().get( minCharArray );
    wrapBuffer.position( wrapBuffer.position() + minLength );

    int maxLength = wrapBuffer.getInt();
    char[] maxCharArray = new char[maxLength / Character.BYTES];
    wrapBuffer.asCharBuffer().get( maxCharArray );
    wrapBuffer.position( wrapBuffer.position() + maxLength );

    String min = new String( minCharArray );
    String max = new String( maxCharArray );

    BlockIndexNode currentNode = parentNode.getChildNode( columnBinary.columnName );
    currentNode.setBlockIndex( new StringRangeBlockIndex( min , max ) );
  }
}
