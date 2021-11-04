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
import jp.co.yahoo.yosegi.blockindex.FloatRangeBlockIndex;
import jp.co.yahoo.yosegi.compressor.CompressResult;
import jp.co.yahoo.yosegi.compressor.FindCompressor;
import jp.co.yahoo.yosegi.compressor.ICompressor;
import jp.co.yahoo.yosegi.inmemory.ILoader;
import jp.co.yahoo.yosegi.inmemory.ISequentialLoader;
import jp.co.yahoo.yosegi.inmemory.LoadType;
import jp.co.yahoo.yosegi.message.objects.FloatObj;
import jp.co.yahoo.yosegi.message.objects.PrimitiveObject;
import jp.co.yahoo.yosegi.spread.analyzer.FloatColumnAnalizeResult;
import jp.co.yahoo.yosegi.spread.analyzer.IColumnAnalizeResult;
import jp.co.yahoo.yosegi.spread.column.ColumnType;
import jp.co.yahoo.yosegi.spread.column.ICell;
import jp.co.yahoo.yosegi.spread.column.IColumn;
import jp.co.yahoo.yosegi.spread.column.PrimitiveCell;
import jp.co.yahoo.yosegi.util.io.IReadSupporter;
import jp.co.yahoo.yosegi.util.io.IWriteSupporter;
import jp.co.yahoo.yosegi.util.io.NumberToBinaryUtils;
import jp.co.yahoo.yosegi.util.io.unsafe.ByteBufferSupporterFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class UnsafeOptimizeFloatColumnBinaryMaker implements IColumnBinaryMaker {

  public static IDictionaryMaker chooseDictionaryMaker( final float min , final float max ) {
    return new FloatDictionaryMaker();
  }

  /**
   * Select the smallest numeric type in the range of Index.
   */
  public static IDictionaryIndexMaker chooseDictionaryIndexMaker( final int dicIndexLength ) {
    if ( dicIndexLength <= NumberToBinaryUtils.INT_BYTE_MAX_LENGTH ) {
      return new ByteDictionaryIndexMaker();
    } else if ( dicIndexLength <= NumberToBinaryUtils.INT_SHORT_MAX_LENGTH ) {
      return new ShortDictionaryIndexMaker();
    } else {
      return new IntDictionaryIndexMaker();
    }
  }

  public interface IDictionaryMaker {

    int getLogicalSize( final int indexLength );

    int calcBinarySize( final int dicSize );

    void create(
        final List<PrimitiveObject> objList ,
        final byte[] buffer ,
        final int start ,
        final int length ,
        final ByteOrder order ) throws IOException;

    PrimitiveObject[] getDicPrimitiveArray(
        final byte[] buffer ,
        final int start ,
        final int length ,
        final ByteOrder order ) throws IOException;

  }

  public static class FloatDictionaryMaker implements IDictionaryMaker {

    @Override
    public int getLogicalSize( final int indexLength ) {
      return Float.BYTES * indexLength;
    }

    @Override
    public int calcBinarySize( final int dicSize ) {
      return Float.BYTES * dicSize;
    }

    @Override
    public void create(
        final List<PrimitiveObject> objList ,
        final byte[] buffer ,
        final int start ,
        final int length ,
        final ByteOrder order ) throws IOException {
      IWriteSupporter wrapBuffer =
          ByteBufferSupporterFactory.createWriteSupporter( buffer , start , length , order );
      for ( PrimitiveObject obj : objList ) {
        wrapBuffer.putFloat( obj.getFloat() );
      }
    }

    @Override
    public PrimitiveObject[] getDicPrimitiveArray(
        final byte[] buffer ,
        final int start ,
        final int length ,
        final ByteOrder order ) throws IOException {
      int size = length / Float.BYTES;
      PrimitiveObject[] result = new PrimitiveObject[size];
      IReadSupporter wrapBuffer =
          ByteBufferSupporterFactory.createReadSupporter( buffer , start , length , order );
      wrapBuffer.getFloat();
      for ( int i = 1 ; i < size ; i++ ) {
        result[i] = new FloatObj( wrapBuffer.getFloat() );
      }

      return result;
    }

  }

  public interface IDictionaryIndexMaker {

    int calcBinarySize( final int indexLength );

    void create(
        final int[] dicIndexArray ,
        final byte[] buffer ,
        final int start ,
        final int length ,
        final ByteOrder order ) throws IOException;

    IntBuffer getIndexIntBuffer(
        final byte[] buffer ,
        final int start ,
        final int length ,
        final ByteOrder order ) throws IOException;

  }

  public static class ByteDictionaryIndexMaker implements IDictionaryIndexMaker {

    @Override
    public int calcBinarySize( final int indexLength ) {
      return Byte.BYTES * indexLength;
    }

    @Override
    public void create(
        final int[] dicIndexArray ,
        final byte[] buffer ,
        final int start ,
        final int length ,
        final ByteOrder order ) throws IOException {
      IWriteSupporter wrapBuffer =
          ByteBufferSupporterFactory.createWriteSupporter( buffer , start , length , order );
      for ( int index : dicIndexArray ) {
        wrapBuffer.putByte( (byte)index );
      }
    }

    @Override
    public IntBuffer getIndexIntBuffer(
        final byte[] buffer ,
        final int start ,
        final int length ,
        final ByteOrder order ) throws IOException {
      int size = length / Byte.BYTES;
      IReadSupporter wrapBuffer =
          ByteBufferSupporterFactory.createReadSupporter( buffer , start , length , order );
      IntBuffer result = IntBuffer.allocate( size );
      for ( int i = 0 ; i < size ; i++ ) {
        result.put( NumberToBinaryUtils.getUnsignedByteToInt( wrapBuffer.getByte() ) );
      }
      result.position( 0 );
      return result;
    }

  }

  public static class ShortDictionaryIndexMaker implements IDictionaryIndexMaker {

    @Override
    public int calcBinarySize( final int indexLength ) {
      return Short.BYTES * indexLength;
    }

    @Override
    public void create(
        final int[] dicIndexArray ,
        final byte[] buffer ,
        final int start ,
        final int length ,
        final ByteOrder order ) throws IOException {
      IWriteSupporter wrapBuffer =
          ByteBufferSupporterFactory.createWriteSupporter( buffer , start , length , order );
      for ( int index : dicIndexArray ) {
        wrapBuffer.putShort( (short)index );
      }
    }

    @Override
    public IntBuffer getIndexIntBuffer(
        final byte[] buffer ,
        final int start ,
        final int length ,
        final ByteOrder order ) throws IOException {
      int size = length / Short.BYTES;
      IReadSupporter wrapBuffer =
          ByteBufferSupporterFactory.createReadSupporter( buffer , start , length , order );
      IntBuffer result = IntBuffer.allocate( size );
      for ( int i = 0 ; i < size ; i++ ) {
        result.put( NumberToBinaryUtils.getUnsignedShortToInt( wrapBuffer.getShort() ) );
      }
      result.position( 0 );
      return result;
    }

  }

  public static class IntDictionaryIndexMaker implements IDictionaryIndexMaker {

    @Override
    public int calcBinarySize( final int indexLength ) {
      return Integer.BYTES * indexLength;
    }

    @Override
    public void create(
        final int[] dicIndexArray ,
        final byte[] buffer ,
        final int start ,
        final int length ,
        final ByteOrder order ) throws IOException {
      IWriteSupporter wrapBuffer =
          ByteBufferSupporterFactory.createWriteSupporter( buffer , start , length , order );
      for ( int index : dicIndexArray ) {
        wrapBuffer.putInt( index );
      }
    }

    @Override
    public IntBuffer getIndexIntBuffer(
        final byte[] buffer ,
        final int start ,
        final int length ,
        ByteOrder order ) throws IOException {
      int size = length / Integer.BYTES;
      IReadSupporter wrapBuffer =
          ByteBufferSupporterFactory.createReadSupporter( buffer , start , length , order );
      IntBuffer result = IntBuffer.allocate( size );
      for ( int i = 0 ; i < size ; i++ ) {
        result.put( wrapBuffer.getInt() );
      }
      result.position( 0 );
      return result;
    }

  }

  @Override
  public ColumnBinary toBinary(
      final ColumnBinaryMakerConfig commonConfig ,
      final ColumnBinaryMakerCustomConfigNode currentConfigNode ,
      final CompressResultNode compressResultNode ,
      final IColumn column ) throws IOException {
    ColumnBinaryMakerConfig currentConfig = commonConfig;
    if ( currentConfigNode != null ) {
      currentConfig = currentConfigNode.getCurrentConfig();
    }
    Map<Float,Integer> dicMap = new HashMap<Float,Integer>();
    List<PrimitiveObject> dicList = new ArrayList<PrimitiveObject>();
    int[] indexArray = new int[column.size()];

    dicMap.put( null , Integer.valueOf(0) );
    dicList.add( new FloatObj( (float)0 ) );

    Float min = Float.MAX_VALUE;
    Float max = Float.MIN_VALUE;
    int rowCount = 0;
    boolean hasNull = false;
    for ( int i = 0 ; i < column.size() ; i++ ) {
      ICell cell = column.get(i);
      PrimitiveObject primitiveObj = null;
      Float target = null;
      if ( cell.getType() == ColumnType.NULL ) {
        hasNull = true;
      } else {
        rowCount++;
        PrimitiveCell stringCell = (PrimitiveCell) cell;
        primitiveObj = stringCell.getRow();
        target = Float.valueOf( primitiveObj.getFloat() );
      }
      if ( ! dicMap.containsKey( target ) ) {
        if ( 0 < min.compareTo( target ) ) {
          min = Float.valueOf( target );
        }
        if ( max.compareTo( target ) < 0 ) {
          max = Float.valueOf( target );
        }
        dicMap.put( target , dicMap.size() );
        dicList.add( primitiveObj );
      }
      indexArray[i] = dicMap.get( target );
    }

    if ( ! hasNull && min.equals( max ) ) {
      return ConstantColumnBinaryMaker.createColumnBinary(
        new FloatObj( min ) , column.getColumnName() , column.size() );
    }

    ByteOrder order = ByteOrder.nativeOrder();

    IDictionaryIndexMaker indexMaker = chooseDictionaryIndexMaker( indexArray.length );
    IDictionaryMaker dicMaker = chooseDictionaryMaker( min.floatValue() , max.floatValue() );

    int indexLength = indexMaker.calcBinarySize( indexArray.length );
    int dicLength = dicMaker.calcBinarySize( dicList.size() );

    byte[] binaryRaw = new byte[ indexLength + dicLength ];
    indexMaker.create( indexArray , binaryRaw , 0 , indexLength , order );
    dicMaker.create( dicList , binaryRaw , indexLength , dicLength , order );

    CompressResult compressResult = compressResultNode.getCompressResult(
        this.getClass().getName() ,
        "c0"  ,
        currentConfig.compressionPolicy ,
        currentConfig.allowedRatio );
    byte[] compressBinary = currentConfig.compressorClass.compress(
        binaryRaw , 0 , binaryRaw.length , compressResult );

    byte[] binary = new byte[ Float.BYTES * 2 + Byte.BYTES + compressBinary.length ];

    byte byteOrderByte = order == ByteOrder.BIG_ENDIAN ? (byte)0 : (byte)1;
    ByteBuffer wrapBuffer = ByteBuffer.wrap( binary , 0 , binary.length );
    wrapBuffer.putFloat( min );
    wrapBuffer.putFloat( max );
    wrapBuffer.put( byteOrderByte );
    wrapBuffer.put( compressBinary );

    return new ColumnBinary(
        this.getClass().getName() ,
        currentConfig.compressorClass.getClass().getName() ,
        column.getColumnName() ,
        column.getColumnType() ,
        column.size() ,
        binary.length ,
        dicMaker.getLogicalSize( rowCount ) ,
        dicList.size() ,
        binary ,
        0 ,
        binary.length ,
        null );
  }

  @Override
  public int calcBinarySize( final IColumnAnalizeResult analizeResult ) {
    float min = ( (FloatColumnAnalizeResult) analizeResult ).getMin();
    float max = ( (FloatColumnAnalizeResult) analizeResult ).getMax();
    IDictionaryIndexMaker indexMaker = chooseDictionaryIndexMaker( analizeResult.getColumnSize() );
    IDictionaryMaker dicMaker = chooseDictionaryMaker( min , max );

    int indexLength = indexMaker.calcBinarySize( analizeResult.getColumnSize() );
    int dicLength = dicMaker.calcBinarySize( analizeResult.getUniqCount() );

    return indexLength + dicLength;
  }

  @Override
  public LoadType getLoadType(final ColumnBinary columnBinary, final int loadSize) {
    return LoadType.SEQUENTIAL;
  }

  private void loadFromColumnBinary(final ColumnBinary columnBinary, final ISequentialLoader loader)
      throws IOException {
    ByteBuffer wrapBuffer =
        ByteBuffer.wrap(columnBinary.binary, columnBinary.binaryStart, columnBinary.binaryLength);
    Float min = Float.valueOf(wrapBuffer.getFloat());
    Float max = Float.valueOf(wrapBuffer.getFloat());
    ByteOrder order = wrapBuffer.get() == (byte) 0 ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN;

    IDictionaryIndexMaker indexMaker = chooseDictionaryIndexMaker(columnBinary.rowCount);
    IDictionaryMaker dicMaker = chooseDictionaryMaker(min.floatValue(), max.floatValue());

    int start = columnBinary.binaryStart + (Float.BYTES * 2 + Byte.BYTES);
    int length = columnBinary.binaryLength - (Float.BYTES * 2 + Byte.BYTES);

    ICompressor compressor = FindCompressor.get(columnBinary.compressorClassName);
    byte[] binary = compressor.decompress(columnBinary.binary, start, length);

    int indexLength = indexMaker.calcBinarySize(columnBinary.rowCount);
    int dicLength = dicMaker.calcBinarySize(columnBinary.cardinality);

    IntBuffer indexIntBuffer = indexMaker.getIndexIntBuffer(binary, 0, indexLength, order);
    PrimitiveObject[] dicArray =
        dicMaker.getDicPrimitiveArray(binary, indexLength, dicLength, order);

    for (int i = 0; i < columnBinary.rowCount; i++) {
      int dicIndex = indexIntBuffer.get();
      if (dicIndex != 0) {
        loader.setFloat(i, dicArray[dicIndex].getFloat());
      } else {
        loader.setNull(i);
      }
    }
    // NOTE: null padding up to load size.
    for (int i = columnBinary.rowCount; i < loader.getLoadSize(); i++) {
      loader.setNull(i);
    }
  }

  private void loadFromExpandColumnBinary(
      final ColumnBinary columnBinary, final ISequentialLoader loader) throws IOException {
    ByteBuffer wrapBuffer =
        ByteBuffer.wrap(columnBinary.binary, columnBinary.binaryStart, columnBinary.binaryLength);
    Float min = Float.valueOf(wrapBuffer.getFloat());
    Float max = Float.valueOf(wrapBuffer.getFloat());
    ByteOrder order = wrapBuffer.get() == (byte) 0 ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN;

    IDictionaryIndexMaker indexMaker = chooseDictionaryIndexMaker(columnBinary.rowCount);
    IDictionaryMaker dicMaker = chooseDictionaryMaker(min.floatValue(), max.floatValue());

    int start = columnBinary.binaryStart + (Float.BYTES * 2 + Byte.BYTES);
    int length = columnBinary.binaryLength - (Float.BYTES * 2 + Byte.BYTES);

    ICompressor compressor = FindCompressor.get(columnBinary.compressorClassName);
    byte[] binary = compressor.decompress(columnBinary.binary, start, length);

    int indexLength = indexMaker.calcBinarySize(columnBinary.rowCount);
    int dicLength = dicMaker.calcBinarySize(columnBinary.cardinality);

    IntBuffer indexIntBuffer = indexMaker.getIndexIntBuffer(binary, 0, indexLength, order);
    PrimitiveObject[] dicArray =
        dicMaker.getDicPrimitiveArray(binary, indexLength, dicLength, order);

    // NOTE: repetitions check
    //   LoadSize is less than real size if repetitions include negative number.
    //   It is possible to be thrown ArrayIndexOutOfBoundsException.
    for (int i = 0; i < columnBinary.repetitions.length; i++) {
      if (columnBinary.repetitions[i] < 0) {
        throw new IOException("Repetition must be equal to or greater than 0.");
      }
    }

    // NOTE:
    //   Set: currentIndex, value
    int currentIndex = 0;
    int lastIndex = columnBinary.rowCount - 1;
    for (int i = 0; i < columnBinary.repetitions.length; i++) {
      if (columnBinary.repetitions[i] == 0) {
        continue;
      }
      if (i > lastIndex) {
        for (int j = 0; j < columnBinary.repetitions[i]; j++) {
          loader.setNull(currentIndex);
          currentIndex++;
        }
        continue;
      }
      int dicIndex = indexIntBuffer.get(i);
      if (dicIndex != 0) {
        float value = dicArray[dicIndex].getFloat();
        for (int j = 0; j < columnBinary.repetitions[i]; j++) {
          loader.setFloat(currentIndex, value);
          currentIndex++;
        }
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
    if (loader.getLoaderType() != LoadType.SEQUENTIAL) {
      throw new IOException("Loader type is not SEQUENTIAL");
    }
    if (columnBinary.isSetLoadSize) {
      loadFromExpandColumnBinary(columnBinary, (ISequentialLoader) loader);
    } else {
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
    Float min = Float.valueOf( wrapBuffer.getFloat() );
    Float max = Float.valueOf( wrapBuffer.getFloat() );
    BlockIndexNode currentNode = parentNode.getChildNode( columnBinary.columnName );
    currentNode.setBlockIndex( new FloatRangeBlockIndex( min , max ) );
  }
}
