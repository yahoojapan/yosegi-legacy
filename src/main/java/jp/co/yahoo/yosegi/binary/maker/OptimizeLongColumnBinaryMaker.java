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
import jp.co.yahoo.yosegi.blockindex.LongRangeBlockIndex;
import jp.co.yahoo.yosegi.compressor.FindCompressor;
import jp.co.yahoo.yosegi.compressor.ICompressor;
import jp.co.yahoo.yosegi.inmemory.IDictionaryLoader;
import jp.co.yahoo.yosegi.inmemory.ILoader;
import jp.co.yahoo.yosegi.inmemory.ISequentialLoader;
import jp.co.yahoo.yosegi.inmemory.LoadType;
import jp.co.yahoo.yosegi.message.objects.ByteObj;
import jp.co.yahoo.yosegi.message.objects.IntegerObj;
import jp.co.yahoo.yosegi.message.objects.LongObj;
import jp.co.yahoo.yosegi.message.objects.PrimitiveObject;
import jp.co.yahoo.yosegi.message.objects.ShortObj;
import jp.co.yahoo.yosegi.spread.analyzer.ByteColumnAnalizeResult;
import jp.co.yahoo.yosegi.spread.analyzer.IColumnAnalizeResult;
import jp.co.yahoo.yosegi.spread.analyzer.IntegerColumnAnalizeResult;
import jp.co.yahoo.yosegi.spread.analyzer.LongColumnAnalizeResult;
import jp.co.yahoo.yosegi.spread.analyzer.ShortColumnAnalizeResult;
import jp.co.yahoo.yosegi.spread.column.ColumnType;
import jp.co.yahoo.yosegi.spread.column.ICell;
import jp.co.yahoo.yosegi.spread.column.IColumn;
import jp.co.yahoo.yosegi.spread.column.PrimitiveCell;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OptimizeLongColumnBinaryMaker implements IColumnBinaryMaker {

  /**
   * Determine the type of difference.
   */
  public static ColumnType getDiffColumnType( final long min , final long max ) {
    long diff = max - min;
    if ( diff < 0 ) {
      return ColumnType.LONG;
    }

    if ( diff <= Byte.MAX_VALUE ) {
      return ColumnType.BYTE;
    } else if ( diff <= Short.MAX_VALUE ) {
      return ColumnType.SHORT;
    } else if ( diff <= Integer.MAX_VALUE ) {
      return ColumnType.INTEGER;
    }

    return ColumnType.LONG;
  }

  /**
   * Create a constant column.
   */
  public static PrimitiveObject createConstObject(
      final ColumnType type , final long num ) throws IOException {
    switch ( type ) {
      case BYTE:
        return new ByteObj( Long.valueOf( num ).byteValue() );
      case SHORT:
        return new ShortObj( Long.valueOf( num ).shortValue() );
      case INTEGER:
        return new IntegerObj( Long.valueOf( num ).intValue() );
      default:
        return new LongObj( num );
    }
  }

  /**
   * Create the smallest IDictionaryMaker from the minimum and maximum values.
   */
  public static IDictionaryMaker chooseDictionaryMaker( final long min , final long max ) {
    ColumnType diffType = getDiffColumnType( min , max );

    if ( Byte.valueOf( Byte.MIN_VALUE ).longValue() <= min
        && max <= Byte.valueOf( Byte.MAX_VALUE ).longValue() ) {
      return new ByteDictionaryMaker();
    } else if ( diffType == ColumnType.BYTE ) {
      return new DiffByteDictionaryMaker( min );
    } else if ( Short.valueOf( Short.MIN_VALUE ).longValue() <= min
        && max <= Short.valueOf( Short.MAX_VALUE ).longValue() ) {
      return new ShortDictionaryMaker();
    } else if ( diffType == ColumnType.SHORT ) {
      return new DiffShortDictionaryMaker( min );
    } else if ( Integer.valueOf( Integer.MIN_VALUE ).longValue() <= min
        && max <= Integer.valueOf( Integer.MAX_VALUE ).longValue() ) {
      return new IntDictionaryMaker();
    } else if ( diffType == ColumnType.INTEGER ) {
      return new DiffIntDictionaryMaker( min );
    } else {
      return new LongDictionaryMaker();
    }
  }

  /**
   * Create the smallest IDictionaryIndexMaker
   * from the maximum value of the index of the dictionary.
   */
  public static IDictionaryIndexMaker chooseDictionaryIndexMaker( final int dicIndexLength ) {
    if ( dicIndexLength < Byte.valueOf( Byte.MAX_VALUE ).intValue() ) {
      return new ByteDictionaryIndexMaker();
    } else if ( dicIndexLength < Short.valueOf( Short.MAX_VALUE ).intValue() ) {
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
        final int length ) throws IOException;

    PrimitiveObject[] getDicPrimitiveArray(
        final byte[] buffer , final int start , final int length ) throws IOException;

  }

  public static class ByteDictionaryMaker implements IDictionaryMaker {

    @Override
    public int getLogicalSize( final int indexLength ) {
      return Byte.BYTES * indexLength;
    }

    @Override
    public int calcBinarySize( final int dicSize ) {
      return Byte.BYTES * dicSize;
    }

    @Override
    public void create(
        final List<PrimitiveObject> objList ,
        final byte[] buffer ,
        final int start ,
        final int length ) throws IOException {
      ByteBuffer wrapBuffer = ByteBuffer.wrap( buffer , start , length );
      for ( PrimitiveObject obj : objList ) {
        wrapBuffer.put( obj.getByte() );
      }
    }

    @Override
    public PrimitiveObject[] getDicPrimitiveArray(
        final byte[] buffer , final int start , final int length ) throws IOException {
      int size = length / Byte.BYTES;
      PrimitiveObject[] result = new PrimitiveObject[size];
      ByteBuffer wrapBuffer = ByteBuffer.wrap( buffer , start , length );
      wrapBuffer.get();
      for ( int i = 1 ; i < size ; i++ ) {
        result[i] = new ByteObj( wrapBuffer.get() );
      }

      return result;
    }

  }

  public static class DiffByteDictionaryMaker implements IDictionaryMaker {

    private final long min;

    public DiffByteDictionaryMaker( final long min ) {
      this.min = min;
    }

    @Override
    public int getLogicalSize( final int indexLength ) {
      return Byte.BYTES * indexLength;
    }

    @Override
    public int calcBinarySize( final int dicSize ) {
      return Byte.BYTES * dicSize;
    }

    @Override
    public void create(
        final List<PrimitiveObject> objList ,
        final byte[] buffer ,
        final int start ,
        final int length ) throws IOException {
      ByteBuffer wrapBuffer = ByteBuffer.wrap( buffer , start , length );
      for ( PrimitiveObject obj : objList ) {
        wrapBuffer.put( (byte)( obj.getLong() - min ) );
      }
    }

    @Override
    public PrimitiveObject[] getDicPrimitiveArray(
        final byte[] buffer , final int start , final int length ) throws IOException {
      int size = length / Byte.BYTES;
      PrimitiveObject[] result = new PrimitiveObject[size];
      ByteBuffer wrapBuffer = ByteBuffer.wrap( buffer , start , length );
      wrapBuffer.get();
      for ( int i = 1 ; i < size ; i++ ) {
        result[i] = new LongObj( (long)wrapBuffer.get() + min );
      }

      return result;
    }

  }

  public static class ShortDictionaryMaker implements IDictionaryMaker {

    @Override
    public int getLogicalSize( final int indexLength ) {
      return Short.BYTES * indexLength;
    }

    @Override
    public int calcBinarySize( final int dicSize ) {
      return Short.BYTES * dicSize;
    }

    @Override
    public void create(
        final List<PrimitiveObject> objList ,
        final byte[] buffer ,
        final int start ,
        final int length ) throws IOException {
      ByteBuffer wrapBuffer = ByteBuffer.wrap( buffer , start , length );
      for ( PrimitiveObject obj : objList ) {
        wrapBuffer.putShort( obj.getShort() );
      }
    }

    @Override
    public PrimitiveObject[] getDicPrimitiveArray(
        final byte[] buffer , final int start , final int length ) throws IOException {
      int size = length / Short.BYTES;
      PrimitiveObject[] result = new PrimitiveObject[size];
      ByteBuffer wrapBuffer = ByteBuffer.wrap( buffer , start , length );
      wrapBuffer.getShort();
      for ( int i = 1 ; i < size ; i++ ) {
        result[i] = new ShortObj( wrapBuffer.getShort() );
      }

      return result;
    }

  }

  public static class DiffShortDictionaryMaker implements IDictionaryMaker {

    private final long min;

    public DiffShortDictionaryMaker( final long min ) {
      this.min = min;
    }

    @Override
    public int getLogicalSize( final int indexLength ) {
      return Short.BYTES * indexLength;
    }

    @Override
    public int calcBinarySize( final int dicSize ) {
      return Short.BYTES * dicSize;
    }

    @Override
    public void create(
        final List<PrimitiveObject> objList ,
        final byte[] buffer ,
        final int start ,
        final int length ) throws IOException {
      ByteBuffer wrapBuffer = ByteBuffer.wrap( buffer , start , length );
      for ( PrimitiveObject obj : objList ) {
        wrapBuffer.putShort( (short)( obj.getLong() - min ) );
      }
    }

    @Override
    public PrimitiveObject[] getDicPrimitiveArray(
        final byte[] buffer , final int start , final int length ) throws IOException {
      int size = length / Short.BYTES;
      PrimitiveObject[] result = new PrimitiveObject[size];
      ByteBuffer wrapBuffer = ByteBuffer.wrap( buffer , start , length );
      wrapBuffer.getShort();
      for ( int i = 1 ; i < size ; i++ ) {
        result[i] = new LongObj( (long)wrapBuffer.getShort() + min );
      }

      return result;
    }

  }

  public static class IntDictionaryMaker implements IDictionaryMaker {

    @Override
    public int getLogicalSize( final int indexLength ) {
      return Integer.BYTES * indexLength;
    }

    @Override
    public int calcBinarySize( final int dicSize ) {
      return Integer.BYTES * dicSize;
    }

    @Override
    public void create(
        final List<PrimitiveObject> objList ,
        final byte[] buffer ,
        final int start ,
        final int length ) throws IOException {
      ByteBuffer wrapBuffer = ByteBuffer.wrap( buffer , start , length );
      for ( PrimitiveObject obj : objList ) {
        wrapBuffer.putInt( obj.getInt() );
      }
    }

    @Override
    public PrimitiveObject[] getDicPrimitiveArray(
        final byte[] buffer , final int start , final int length ) throws IOException {
      int size = length / Integer.BYTES;
      PrimitiveObject[] result = new PrimitiveObject[size];
      ByteBuffer wrapBuffer = ByteBuffer.wrap( buffer , start , length );
      wrapBuffer.getInt();
      for ( int i = 1 ; i < size ; i++ ) {
        result[i] = new IntegerObj( wrapBuffer.getInt() );
      }

      return result;
    }

  }

  public static class DiffIntDictionaryMaker implements IDictionaryMaker {

    private final long min;

    public DiffIntDictionaryMaker( final long min ) {
      this.min = min;
    }

    @Override
    public int getLogicalSize( final int indexLength ) {
      return Integer.BYTES * indexLength;
    }

    @Override
    public int calcBinarySize( final int dicSize ) {
      return Integer.BYTES * dicSize;
    }

    @Override
    public void create(
        final List<PrimitiveObject> objList ,
        final byte[] buffer ,
        final int start ,
        final int length ) throws IOException {
      ByteBuffer wrapBuffer = ByteBuffer.wrap( buffer , start , length );
      for ( PrimitiveObject obj : objList ) {
        wrapBuffer.putInt( (int)( obj.getLong() - min ) );
      }
    }

    @Override
    public PrimitiveObject[] getDicPrimitiveArray(
        final byte[] buffer , final int start , final int length ) throws IOException {
      int size = length / Integer.BYTES;
      PrimitiveObject[] result = new PrimitiveObject[size];
      ByteBuffer wrapBuffer = ByteBuffer.wrap( buffer , start , length );
      wrapBuffer.getInt();
      for ( int i = 1 ; i < size ; i++ ) {
        result[i] = new LongObj( (long)wrapBuffer.getInt() + min );
      }

      return result;
    }

  }

  public static class LongDictionaryMaker implements IDictionaryMaker {

    @Override
    public int getLogicalSize( final int indexLength ) {
      return Long.BYTES * indexLength;
    }

    @Override
    public int calcBinarySize( final int dicSize ) {
      return Long.BYTES * dicSize;
    }

    @Override
    public void create(
        final List<PrimitiveObject> objList ,
        final byte[] buffer ,
        final int start ,
        final int length ) throws IOException {
      ByteBuffer wrapBuffer = ByteBuffer.wrap( buffer , start , length );
      for ( PrimitiveObject obj : objList ) {
        wrapBuffer.putLong( obj.getLong() );
      }
    }

    @Override
    public PrimitiveObject[] getDicPrimitiveArray(
        final byte[] buffer , final int start , final int length ) throws IOException {
      int size = length / Long.BYTES;
      PrimitiveObject[] result = new PrimitiveObject[size];
      ByteBuffer wrapBuffer = ByteBuffer.wrap( buffer , start , length );
      wrapBuffer.getLong();
      for ( int i = 1 ; i < size ; i++ ) {
        result[i] = new LongObj( wrapBuffer.getLong() );
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
        final int length ) throws IOException;

    IntBuffer getIndexIntBuffer(
        final byte[] buffer , final int start , final int length ) throws IOException;

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
        final int length ) throws IOException {
      ByteBuffer wrapBuffer = ByteBuffer.wrap( buffer , start , length );
      for ( int index : dicIndexArray ) {
        wrapBuffer.put( (byte)index );
      }
    }

    @Override
    public IntBuffer getIndexIntBuffer(
        final byte[] buffer , final int start , final int length ) throws IOException {
      int size = length / Byte.BYTES;
      ByteBuffer wrapBuffer = ByteBuffer.wrap( buffer , start , length );
      IntBuffer result = IntBuffer.allocate( size );
      for ( int i = 0 ; i < size ; i++ ) {
        result.put( (int)( wrapBuffer.get() ) );
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
        final int length ) throws IOException {
      ByteBuffer wrapBuffer = ByteBuffer.wrap( buffer , start , length );
      for ( int index : dicIndexArray ) {
        wrapBuffer.putShort( (short)index );
      }
    }

    @Override
    public IntBuffer getIndexIntBuffer(
        final byte[] buffer , final int start , final int length ) throws IOException {
      int size = length / Short.BYTES;
      ByteBuffer wrapBuffer = ByteBuffer.wrap( buffer , start , length );
      IntBuffer result = IntBuffer.allocate( size );
      for ( int i = 0 ; i < size ; i++ ) {
        result.put( (int)( wrapBuffer.getShort() ) );
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
        final int length ) throws IOException {
      ByteBuffer wrapBuffer = ByteBuffer.wrap( buffer , start , length );
      for ( int index : dicIndexArray ) {
        wrapBuffer.putInt( index );
      }
    }

    @Override
    public IntBuffer getIndexIntBuffer(
        final byte[] buffer ,
        final int start ,
        final int length ) throws IOException {
      return ByteBuffer.wrap( buffer , start , length ).asIntBuffer();
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
    Map<Long,Integer> dicMap = new HashMap<Long,Integer>();
    List<PrimitiveObject> dicList = new ArrayList<PrimitiveObject>();
    int[] indexArray = new int[column.size()];

    dicMap.put( null , Integer.valueOf(0) );
    dicList.add( new LongObj( 0L ) );

    Long min = Long.MAX_VALUE;
    Long max = Long.MIN_VALUE;
    int rowCount = 0;
    boolean hasNull = false;
    for ( int i = 0 ; i < column.size() ; i++ ) {
      ICell cell = column.get(i);
      PrimitiveObject primitiveObj = null;
      Long target = null;
      if ( cell.getType() == ColumnType.NULL ) {
        hasNull = true;
      } else {
        rowCount++;
        PrimitiveCell stringCell = (PrimitiveCell) cell;
        primitiveObj = stringCell.getRow();
        target = Long.valueOf( primitiveObj.getLong() );
      }
      if ( ! dicMap.containsKey( target ) ) {
        if ( 0 < min.compareTo( target ) ) {
          min = Long.valueOf( target );
        }
        if ( max.compareTo( target ) < 0 ) {
          max = Long.valueOf( target );
        }
        dicMap.put( target , dicMap.size() );
        dicList.add( primitiveObj );
      }
      indexArray[i] = dicMap.get( target );
    }

    if ( ! hasNull && min.equals( max ) ) {
      return ConstantColumnBinaryMaker.createColumnBinary( 
          createConstObject( column.getColumnType() , min ) ,
          column.getColumnName() ,
          column.size() );
    }

    IDictionaryIndexMaker indexMaker = chooseDictionaryIndexMaker( indexArray.length );
    IDictionaryMaker dicMaker = chooseDictionaryMaker( min.longValue() , max.longValue() );

    int indexLength = indexMaker.calcBinarySize( indexArray.length );
    int dicLength = dicMaker.calcBinarySize( dicList.size() );

    byte[] binaryRaw = new byte[ indexLength + dicLength ];
    indexMaker.create( indexArray , binaryRaw , 0 , indexLength );
    dicMaker.create( dicList , binaryRaw , indexLength , dicLength );

    byte[] compressBinary =
        currentConfig.compressorClass.compress( binaryRaw , 0 , binaryRaw.length );

    byte[] binary = new byte[ Long.BYTES * 2 + compressBinary.length ];

    ByteBuffer wrapBuffer = ByteBuffer.wrap( binary , 0 , binary.length );
    wrapBuffer.putLong( min );
    wrapBuffer.putLong( max );
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
    long min;
    long max;
    switch ( analizeResult.getColumnType() ) {
      case BYTE:
        min = (long)( (ByteColumnAnalizeResult) analizeResult ).getMin();
        max = (long)( (ByteColumnAnalizeResult) analizeResult ).getMax();
        break;
      case SHORT:
        min = (long)( (ShortColumnAnalizeResult) analizeResult ).getMin();
        max = (long)( (ShortColumnAnalizeResult) analizeResult ).getMax();
        break;
      case INTEGER:
        min = (long)( (IntegerColumnAnalizeResult) analizeResult ).getMin();
        max = (long)( (IntegerColumnAnalizeResult) analizeResult ).getMax();
        break;
      case LONG:
        min = ( (LongColumnAnalizeResult) analizeResult ).getMin();
        max = ( (LongColumnAnalizeResult) analizeResult ).getMax();
        break;
      default:
        min = Long.MIN_VALUE;
        max = Long.MAX_VALUE;
        break;
    }
    IDictionaryIndexMaker indexMaker = chooseDictionaryIndexMaker( analizeResult.getColumnSize() );
    IDictionaryMaker dicMaker = chooseDictionaryMaker( min , max );

    int indexLength = indexMaker.calcBinarySize( analizeResult.getColumnSize() );
    int dicLength = dicMaker.calcBinarySize( analizeResult.getUniqCount() );

    return indexLength + dicLength;
  }

  @Override
  public LoadType getLoadType(final ColumnBinary columnBinary, final int loadSize) {
    if (columnBinary.isSetLoadSize) {
      return LoadType.DICTIONARY;
    }
    return LoadType.SEQUENTIAL;
  }

  private void loadFromColumnBinary(final ColumnBinary columnBinary, final ISequentialLoader loader)
      throws IOException {
    ByteBuffer wrapBuffer =
        ByteBuffer.wrap(columnBinary.binary, columnBinary.binaryStart, columnBinary.binaryLength);
    Long min = Long.valueOf(wrapBuffer.getLong());
    Long max = Long.valueOf(wrapBuffer.getLong());

    IDictionaryIndexMaker indexMaker = chooseDictionaryIndexMaker(columnBinary.rowCount);
    IDictionaryMaker dicMaker = chooseDictionaryMaker(min.longValue(), max.longValue());

    int start = columnBinary.binaryStart + (Long.BYTES * 2);
    int length = columnBinary.binaryLength - (Long.BYTES * 2);

    ICompressor compressor = FindCompressor.get(columnBinary.compressorClassName);
    byte[] binary = compressor.decompress(columnBinary.binary, start, length);

    int indexLength = indexMaker.calcBinarySize(columnBinary.rowCount);
    int dicLength = dicMaker.calcBinarySize(columnBinary.cardinality);

    IntBuffer indexIntBuffer = indexMaker.getIndexIntBuffer(binary, 0, indexLength);
    PrimitiveObject[] dicArray = dicMaker.getDicPrimitiveArray(binary, indexLength, dicLength);

    int loopCount = indexIntBuffer.capacity();
    for (int i = 0; i < loopCount; i++) {
      int dicIndex = indexIntBuffer.get();
      if (dicIndex == 0) {
        loader.setNull(i);
      } else {
        loader.setLong(i, dicArray[dicIndex].getLong());
      }
    }
    // NOTE: null padding up to load size.
    for (int i = loopCount; i < loader.getLoadSize(); i++) {
      loader.setNull(i);
    }
  }

  private void loadFromExpandColumnBinary(
      final ColumnBinary columnBinary, final IDictionaryLoader loader) throws IOException {
    ByteBuffer wrapBuffer =
        ByteBuffer.wrap(columnBinary.binary, columnBinary.binaryStart, columnBinary.binaryLength);
    Long min = Long.valueOf(wrapBuffer.getLong());
    Long max = Long.valueOf(wrapBuffer.getLong());

    IDictionaryIndexMaker indexMaker = chooseDictionaryIndexMaker(columnBinary.rowCount);
    IDictionaryMaker dicMaker = chooseDictionaryMaker(min.longValue(), max.longValue());

    int start = columnBinary.binaryStart + (Long.BYTES * 2);
    int length = columnBinary.binaryLength - (Long.BYTES * 2);

    ICompressor compressor = FindCompressor.get(columnBinary.compressorClassName);
    byte[] binary = compressor.decompress(columnBinary.binary, start, length);

    int indexLength = indexMaker.calcBinarySize(columnBinary.rowCount);
    int dicLength = dicMaker.calcBinarySize(columnBinary.cardinality);

    IntBuffer indexIntBuffer = indexMaker.getIndexIntBuffer(binary, 0, indexLength);
    PrimitiveObject[] dicArray = dicMaker.getDicPrimitiveArray(binary, indexLength, dicLength);

    // NOTE: Calculate dictionarySize
    int dictionarySize = 0;
    int lastIndex = indexIntBuffer.capacity() - 1;
    for (int i = 0; i < columnBinary.repetitions.length; i++) {
      if (columnBinary.repetitions[i] < 0) {
        throw new IOException("Repetition must be equal to or greater than 0.");
      }
      if (i > lastIndex || indexIntBuffer.get() == 0 || columnBinary.repetitions[i] == 0) {
        continue;
      }
      dictionarySize++;
    }
    loader.createDictionary(dictionarySize);
    // NOTE: reset indexIntBuffer
    indexIntBuffer = indexMaker.getIndexIntBuffer(binary, 0, indexLength);

    // NOTE:
    //   Set value to dict: dictionaryIndex, value
    //   Set dictionaryIndex: currentIndex, dictionaryIndex
    int currentIndex = 0;
    int dictionaryIndex = 0;
    for (int i = 0; i < columnBinary.repetitions.length; i++) {
      if (columnBinary.repetitions[i] == 0) {
        // NOTE: read skip.
        if (i < indexIntBuffer.capacity()) {
          indexIntBuffer.get();
        }
        continue;
      }
      if (i > lastIndex) {
        for (int j = 0; j < columnBinary.repetitions[i]; j++) {
          loader.setNull(currentIndex);
          currentIndex++;
        }
        continue;
      }
      int dicIndex = indexIntBuffer.get();
      if (dicIndex == 0) {
        for (int j = 0; j < columnBinary.repetitions[i]; j++) {
          loader.setNull(currentIndex);
          currentIndex++;
        }
      } else {
        loader.setLongToDic(dictionaryIndex, dicArray[dicIndex].getLong());
        for (int j = 0; j < columnBinary.repetitions[i]; j++) {
          loader.setDictionaryIndex(currentIndex, dictionaryIndex);
          currentIndex++;
        }
        dictionaryIndex++;
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
    Long min = Long.valueOf( wrapBuffer.getLong() );
    Long max = Long.valueOf( wrapBuffer.getLong() );
    BlockIndexNode currentNode = parentNode.getChildNode( columnBinary.columnName );
    currentNode.setBlockIndex( new LongRangeBlockIndex( min , max ) );
  }
}
