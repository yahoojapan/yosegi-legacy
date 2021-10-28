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

public class OptimizeDumpLongColumnBinaryMaker implements IColumnBinaryMaker {

  /**
   * Determine the type of column.
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
   * The minimum type is determined from the value of the argument.
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
   * Create the smallest IBinaryMaker from the minimum and maximum.
   */
  public static IBinaryMaker chooseBinaryMaker( final long min , final long max ) {
    ColumnType diffType = getDiffColumnType( min , max );

    if ( Byte.valueOf( Byte.MIN_VALUE ).longValue() <= min
        && max <= Byte.valueOf( Byte.MAX_VALUE ).longValue() ) {
      return new ByteBinaryMaker();
    } else if ( diffType == ColumnType.BYTE ) {
      return new DiffByteBinaryMaker( min );
    } else if ( Short.valueOf( Short.MIN_VALUE ).longValue() <= min
        && max <= Short.valueOf( Short.MAX_VALUE ).longValue() ) {
      return new ShortBinaryMaker();
    } else if ( diffType == ColumnType.SHORT ) {
      return new DiffShortBinaryMaker( min );
    } else if ( Integer.valueOf( Integer.MIN_VALUE ).longValue() <= min
        && max <= Integer.valueOf( Integer.MAX_VALUE ).longValue() ) {
      return new IntBinaryMaker();
    } else if ( diffType == ColumnType.INTEGER ) {
      return new DiffIntBinaryMaker( min );
    } else {
      return new LongBinaryMaker();
    }
  }

  public interface IBinaryMaker {

    int getLogicalSize( final int columnSize );

    int calcBinarySize( final int columnSize );

    void create(
        final long[] longArray ,
        final byte[] buffer ,
        final int start ,
        final int length ) throws IOException;

    PrimitiveObject[] getPrimitiveArray(
        final byte[] buffer ,
        final int start ,
        final int length ) throws IOException;

    void setSequentialLoader(
        final byte[] buffer,
        final int start,
        final int length,
        final byte[] isNullArray,
        final int size,
        final ISequentialLoader loader)
        throws IOException;

    void setDictionaryLoader(
        final byte[] buffer,
        final int start,
        final int length,
        final byte[] isNullArray,
        final int size,
        final int[] repetitions,
        final IDictionaryLoader loader)
        throws IOException;
  }

  public static class ByteBinaryMaker implements IBinaryMaker {

    @Override
    public int getLogicalSize( final int columnSize ) {
      return Byte.BYTES * columnSize;
    }

    @Override
    public int calcBinarySize( final int columnSize ) {
      return Byte.BYTES * columnSize;
    }

    @Override
    public void create(
        final long[] longArray ,
        final byte[] buffer ,
        final int start ,
        final int length ) throws IOException {
      ByteBuffer wrapBuffer = ByteBuffer.wrap( buffer , start , length );
      for ( long value : longArray ) {
        wrapBuffer.put( Long.valueOf( value ).byteValue() );
      }
    }

    @Override
    public PrimitiveObject[] getPrimitiveArray(
        final byte[] buffer ,
        final int start ,
        final int length ) throws IOException {
      int size = length / Byte.BYTES;
      PrimitiveObject[] result = new PrimitiveObject[size];
      ByteBuffer wrapBuffer = ByteBuffer.wrap( buffer , start , length );
      for ( int i = 0 ; i < size ; i++ ) {
        result[i] = new ByteObj( wrapBuffer.get() );
      }
      return result;
    }

    @Override
    public void setSequentialLoader(
        final byte[] buffer,
        final int start,
        final int length,
        final byte[] isNullArray,
        final int size,
        final ISequentialLoader loader)
        throws IOException {
      ByteBuffer wrapBuffer = ByteBuffer.wrap(buffer, start, length);
      for (int i = 0; i < size; i++) {
        if (isNullArray[i] == 0) {
          loader.setByte(i, wrapBuffer.get());
        } else {
          wrapBuffer.position(wrapBuffer.position() + Byte.BYTES);
          loader.setNull(i);
        }
      }
      // NOTE: null padding up to load size.
      for (int i = size; i < loader.getLoadSize(); i++) {
        loader.setNull(i);
      }
    }

    @Override
    public void setDictionaryLoader(
        final byte[] buffer,
        final int start,
        final int length,
        final byte[] isNullArray,
        final int size,
        final int[] repetitions,
        final IDictionaryLoader loader)
        throws IOException {
      ByteBuffer wrapBuffer = ByteBuffer.wrap(buffer, start, length);

      // NOTE: Calculate dictionarySize
      int dictionarySize = 0;
      int lastIndex = size - 1;
      for (int i = 0; i < repetitions.length; i++) {
        if (repetitions[i] < 0) {
          throw new IOException("Repetition must be equal to or greater than 0.");
        }
        if (i > lastIndex || repetitions[i] == 0 || isNullArray[i] != 0) {
          continue;
        }
        dictionarySize++;
      }
      loader.createDictionary(dictionarySize);

      // NOTE:
      //   Set value to dict: dictionaryIndex, value
      //   Set dictionaryIndex: currentIndex, dictionaryIndex
      int currentIndex = 0;
      int dictionaryIndex = 0;
      for (int i = 0; i < repetitions.length; i++) {
        if (repetitions[i] == 0) {
          // NOTE: read skip
          if (i < size) {
            if (isNullArray[i] == 0) {
              wrapBuffer.get();
            } else {
              wrapBuffer.position(wrapBuffer.position() + Byte.BYTES);
            }
          }
          continue;
        }
        if (i > lastIndex) {
          for (int j = 0; j < repetitions[i]; j++) {
            loader.setNull(currentIndex);
            currentIndex++;
          }
          continue;
        }
        if (isNullArray[i] == 0) {
          loader.setByteToDic(dictionaryIndex, wrapBuffer.get());
          for (int j = 0; j < repetitions[i]; j++) {
            loader.setDictionaryIndex(currentIndex, dictionaryIndex);
            currentIndex++;
          }
          dictionaryIndex++;
        } else {
          wrapBuffer.position(wrapBuffer.position() + Byte.BYTES);
          for (int j = 0; j < repetitions[i]; j++) {
            loader.setNull(currentIndex);
            currentIndex++;
          }
        }
      }
    }
  }

  public static class DiffByteBinaryMaker implements IBinaryMaker {

    private final long min;

    public DiffByteBinaryMaker( final long min ) {
      this.min = min;
    }

    @Override
    public int getLogicalSize( final int columnSize ) {
      return Byte.BYTES * columnSize;
    }

    @Override
    public int calcBinarySize( final int columnSize ) {
      return Byte.BYTES * columnSize;
    }

    @Override
    public void create(
        final long[] longArray ,
        final byte[] buffer ,
        final int start ,
        final int length ) throws IOException {
      ByteBuffer wrapBuffer = ByteBuffer.wrap( buffer , start , length );
      for ( long value : longArray ) {
        byte castValue = (byte)( value - min );
        wrapBuffer.put( castValue );
      }
    }

    @Override
    public PrimitiveObject[] getPrimitiveArray(
        final byte[] buffer ,
        final int start ,
        final int length ) throws IOException {
      int size = length / Byte.BYTES;
      PrimitiveObject[] result = new PrimitiveObject[size];
      ByteBuffer wrapBuffer = ByteBuffer.wrap( buffer , start , length );
      for ( int i = 0 ; i < size ; i++ ) {
        result[i] = new LongObj( (long)( wrapBuffer.get() ) + min );
      }
      return result;
    }

    @Override
    public void setSequentialLoader(
        final byte[] buffer,
        final int start,
        final int length,
        final byte[] isNullArray,
        final int size,
        final ISequentialLoader loader)
        throws IOException {
      ByteBuffer wrapBuffer = ByteBuffer.wrap(buffer, start, length);
      for (int i = 0; i < size; i++) {
        if (isNullArray[i] == 0) {
          loader.setLong(i, (long) (wrapBuffer.get()) + min);
        } else {
          wrapBuffer.position(wrapBuffer.position() + Byte.BYTES);
          loader.setNull(i);
        }
      }
      // NOTE: null padding up to load size.
      for (int i = size; i < loader.getLoadSize(); i++) {
        loader.setNull(i);
      }
    }

    @Override
    public void setDictionaryLoader(
        final byte[] buffer,
        final int start,
        final int length,
        final byte[] isNullArray,
        final int size,
        final int[] repetitions,
        final IDictionaryLoader loader)
        throws IOException {
      ByteBuffer wrapBuffer = ByteBuffer.wrap(buffer, start, length);

      // NOTE: Calculate dictionarySize
      int dictionarySize = 0;
      int lastIndex = size - 1;
      for (int i = 0; i < repetitions.length; i++) {
        if (repetitions[i] < 0) {
          throw new IOException("Repetition must be equal to or greater than 0.");
        }
        if (i > lastIndex || repetitions[i] == 0 || isNullArray[i] != 0) {
          continue;
        }
        dictionarySize++;
      }
      loader.createDictionary(dictionarySize);

      // NOTE:
      //   Set value to dict: dictionaryIndex, value
      //   Set dictionaryIndex: currentIndex, dictionaryIndex
      int currentIndex = 0;
      int dictionaryIndex = 0;
      for (int i = 0; i < repetitions.length; i++) {
        if (repetitions[i] == 0) {
          // NOTE: read skip
          if (i < size) {
            if (isNullArray[i] == 0) {
              wrapBuffer.get();
            } else {
              wrapBuffer.position(wrapBuffer.position() + Byte.BYTES);
            }
          }
          continue;
        }
        if (i > lastIndex) {
          for (int j = 0; j < repetitions[i]; j++) {
            loader.setNull(currentIndex);
            currentIndex++;
          }
          continue;
        }
        if (isNullArray[i] == 0) {
          loader.setLongToDic(dictionaryIndex, (long) (wrapBuffer.get()) + min);
          for (int j = 0; j < repetitions[i]; j++) {
            loader.setDictionaryIndex(currentIndex, dictionaryIndex);
            currentIndex++;
          }
          dictionaryIndex++;
        } else {
          wrapBuffer.position(wrapBuffer.position() + Byte.BYTES);
          for (int j = 0; j < repetitions[i]; j++) {
            loader.setNull(currentIndex);
            currentIndex++;
          }
        }
      }
    }
  }

  public static class ShortBinaryMaker implements IBinaryMaker {

    @Override
    public int getLogicalSize( final int columnSize ) {
      return Short.BYTES * columnSize;
    }

    @Override
    public int calcBinarySize( final int columnSize ) {
      return Short.BYTES * columnSize;
    }

    @Override
    public void create(
        final long[] longArray ,
        final byte[] buffer ,
        final int start ,
        final int length ) throws IOException {
      ByteBuffer wrapBuffer = ByteBuffer.wrap( buffer , start , length );
      for ( long value : longArray ) {
        wrapBuffer.putShort( Long.valueOf( value ).shortValue() );
      }
    }

    @Override
    public PrimitiveObject[] getPrimitiveArray(
        final byte[] buffer , final int start , final int length ) throws IOException {
      int size = length / Short.BYTES;
      PrimitiveObject[] result = new PrimitiveObject[size];
      ByteBuffer wrapBuffer = ByteBuffer.wrap( buffer , start , length );
      for ( int i = 0 ; i < size ; i++ ) {
        result[i] = new ShortObj( wrapBuffer.getShort() );
      }
      return result;
    }

    @Override
    public void setSequentialLoader(
        final byte[] buffer,
        final int start,
        final int length,
        final byte[] isNullArray,
        final int size,
        final ISequentialLoader loader)
        throws IOException {
      ByteBuffer wrapBuffer = ByteBuffer.wrap(buffer, start, length);
      for (int i = 0; i < size; i++) {
        if (isNullArray[i] == 0) {
          loader.setShort(i, wrapBuffer.getShort());
        } else {
          wrapBuffer.position(wrapBuffer.position() + Short.BYTES);
          loader.setNull(i);
        }
      }
      // NOTE: null padding up to load size.
      for (int i = size; i < loader.getLoadSize(); i++) {
        loader.setNull(i);
      }
    }

    @Override
    public void setDictionaryLoader(
        final byte[] buffer,
        final int start,
        final int length,
        final byte[] isNullArray,
        final int size,
        final int[] repetitions,
        final IDictionaryLoader loader)
        throws IOException {
      ByteBuffer wrapBuffer = ByteBuffer.wrap(buffer, start, length);

      // NOTE: Calculate dictionarySize
      int dictionarySize = 0;
      int lastIndex = size - 1;
      for (int i = 0; i < repetitions.length; i++) {
        if (repetitions[i] < 0) {
          throw new IOException("Repetition must be equal to or greater than 0.");
        }
        if (i > lastIndex || repetitions[i] == 0 || isNullArray[i] != 0) {
          continue;
        }
        dictionarySize++;
      }
      loader.createDictionary(dictionarySize);

      // NOTE:
      //   Set value to dict: dictionaryIndex, value
      //   Set dictionaryIndex: currentIndex, dictionaryIndex
      int currentIndex = 0;
      int dictionaryIndex = 0;
      for (int i = 0; i < repetitions.length; i++) {
        if (repetitions[i] == 0) {
          // NOTE: read skip
          if (i < size) {
            if (isNullArray[i] == 0) {
              wrapBuffer.getShort();
            } else {
              wrapBuffer.position(wrapBuffer.position() + Short.BYTES);
            }
          }
          continue;
        }
        if (i > lastIndex) {
          for (int j = 0; j < repetitions[i]; j++) {
            loader.setNull(currentIndex);
            currentIndex++;
          }
          continue;
        }
        if (isNullArray[i] == 0) {
          loader.setShortToDic(dictionaryIndex, wrapBuffer.getShort());
          for (int j = 0; j < repetitions[i]; j++) {
            loader.setDictionaryIndex(currentIndex, dictionaryIndex);
            currentIndex++;
          }
          dictionaryIndex++;
        } else {
          wrapBuffer.position(wrapBuffer.position() + Short.BYTES);
          for (int j = 0; j < repetitions[i]; j++) {
            loader.setNull(currentIndex);
            currentIndex++;
          }
        }
      }
    }
  }

  public static class DiffShortBinaryMaker implements IBinaryMaker {

    private final long min;

    public DiffShortBinaryMaker( final long min ) {
      this.min = min;
    }

    @Override
    public int getLogicalSize( final int columnSize ) {
      return Short.BYTES * columnSize;
    }

    @Override
    public int calcBinarySize( final int columnSize ) {
      return Short.BYTES * columnSize;
    }

    @Override
    public void create(
        final long[] longArray ,
        final byte[] buffer ,
        final int start ,
        final int length ) throws IOException {
      ByteBuffer wrapBuffer = ByteBuffer.wrap( buffer , start , length );
      for ( long value : longArray ) {
        short castValue = (short)( value - min );
        wrapBuffer.putShort( castValue );
      }
    }

    @Override
    public PrimitiveObject[] getPrimitiveArray(
        final byte[] buffer ,
        final int start ,
        final int length ) throws IOException {
      int size = length / Short.BYTES;
      PrimitiveObject[] result = new PrimitiveObject[size];
      ByteBuffer wrapBuffer = ByteBuffer.wrap( buffer , start , length );
      for ( int i = 0 ; i < size ; i++ ) {
        result[i] = new LongObj( (long)( wrapBuffer.getShort() ) + min );
      }
      return result;
    }

    @Override
    public void setSequentialLoader(
        final byte[] buffer,
        final int start,
        final int length,
        final byte[] isNullArray,
        final int size,
        final ISequentialLoader loader)
        throws IOException {
      ByteBuffer wrapBuffer = ByteBuffer.wrap(buffer, start, length);
      for (int i = 0; i < size; i++) {
        if (isNullArray[i] == 0) {
          loader.setLong(i, (long) (wrapBuffer.getShort()) + min);
        } else {
          wrapBuffer.position(wrapBuffer.position() + Short.BYTES);
          loader.setNull(i);
        }
      }
      // NOTE: null padding up to load size.
      for (int i = size; i < loader.getLoadSize(); i++) {
        loader.setNull(i);
      }
    }

    @Override
    public void setDictionaryLoader(
        final byte[] buffer,
        final int start,
        final int length,
        final byte[] isNullArray,
        final int size,
        final int[] repetitions,
        final IDictionaryLoader loader)
        throws IOException {
      ByteBuffer wrapBuffer = ByteBuffer.wrap(buffer, start, length);

      // NOTE: Calculate dictionarySize
      int dictionarySize = 0;
      int lastIndex = size - 1;
      for (int i = 0; i < repetitions.length; i++) {
        if (repetitions[i] < 0) {
          throw new IOException("Repetition must be equal to or greater than 0.");
        }
        if (i > lastIndex || repetitions[i] == 0 || isNullArray[i] != 0) {
          continue;
        }
        dictionarySize++;
      }
      loader.createDictionary(dictionarySize);

      // NOTE:
      //   Set value to dict: dictionaryIndex, value
      //   Set dictionaryIndex: currentIndex, dictionaryIndex
      int currentIndex = 0;
      int dictionaryIndex = 0;
      for (int i = 0; i < repetitions.length; i++) {
        if (repetitions[i] == 0) {
          // NOTE: read skip
          if (i < size) {
            if (isNullArray[i] == 0) {
              wrapBuffer.getShort();
            } else {
              wrapBuffer.position(wrapBuffer.position() + Short.BYTES);
            }
          }
          continue;
        }
        if (i > lastIndex) {
          for (int j = 0; j < repetitions[i]; j++) {
            loader.setNull(currentIndex);
            currentIndex++;
          }
          continue;
        }
        if (isNullArray[i] == 0) {
          loader.setLongToDic(dictionaryIndex, (long) (wrapBuffer.getShort()) + min);
          for (int j = 0; j < repetitions[i]; j++) {
            loader.setDictionaryIndex(currentIndex, dictionaryIndex);
            currentIndex++;
          }
          dictionaryIndex++;
        } else {
          wrapBuffer.position(wrapBuffer.position() + Short.BYTES);
          for (int j = 0; j < repetitions[i]; j++) {
            loader.setNull(currentIndex);
            currentIndex++;
          }
        }
      }
    }
  }

  public static class IntBinaryMaker implements IBinaryMaker {

    @Override
    public int getLogicalSize( final int columnSize ) {
      return Integer.BYTES * columnSize;
    }

    @Override
    public int calcBinarySize( final int columnSize ) {
      return Integer.BYTES * columnSize;
    }

    @Override
    public void create(
        final long[] longArray ,
        final byte[] buffer ,
        final int start ,
        final int length ) throws IOException {
      ByteBuffer wrapBuffer = ByteBuffer.wrap( buffer , start , length );
      for ( long value : longArray ) {
        wrapBuffer.putInt( Long.valueOf( value ).intValue() );
      }
    }

    @Override
    public PrimitiveObject[] getPrimitiveArray(
        final byte[] buffer , final int start , final int length ) throws IOException {
      int size = length / Integer.BYTES;
      PrimitiveObject[] result = new PrimitiveObject[size];
      ByteBuffer wrapBuffer = ByteBuffer.wrap( buffer , start , length );
      for ( int i = 0 ; i < size ; i++ ) {
        result[i] = new IntegerObj( wrapBuffer.getInt() );
      }
      return result;
    }

    @Override
    public void setSequentialLoader(
        final byte[] buffer,
        final int start,
        final int length,
        final byte[] isNullArray,
        final int size,
        final ISequentialLoader loader)
        throws IOException {
      ByteBuffer wrapBuffer = ByteBuffer.wrap(buffer, start, length);
      for (int i = 0; i < size; i++) {
        if (isNullArray[i] == 0) {
          loader.setInteger(i, wrapBuffer.getInt());
        } else {
          wrapBuffer.position(wrapBuffer.position() + Integer.BYTES);
          loader.setNull(i);
        }
      }
      // NOTE: null padding up to load size.
      for (int i = size; i < loader.getLoadSize(); i++) {
        loader.setNull(i);
      }
    }

    @Override
    public void setDictionaryLoader(
        final byte[] buffer,
        final int start,
        final int length,
        final byte[] isNullArray,
        final int size,
        final int[] repetitions,
        final IDictionaryLoader loader)
        throws IOException {
      ByteBuffer wrapBuffer = ByteBuffer.wrap(buffer, start, length);

      // NOTE: Calculate dictionarySize
      int dictionarySize = 0;
      int lastIndex = size - 1;
      for (int i = 0; i < repetitions.length; i++) {
        if (repetitions[i] < 0) {
          throw new IOException("Repetition must be equal to or greater than 0.");
        }
        if (i > lastIndex || repetitions[i] == 0 || isNullArray[i] != 0) {
          continue;
        }
        dictionarySize++;
      }
      loader.createDictionary(dictionarySize);

      // NOTE:
      //   Set value to dict: dictionaryIndex, value
      //   Set dictionaryIndex: currentIndex, dictionaryIndex
      int currentIndex = 0;
      int dictionaryIndex = 0;
      for (int i = 0; i < repetitions.length; i++) {
        if (repetitions[i] == 0) {
          // NOTE: read skip
          if (i < size) {
            if (isNullArray[i] == 0) {
              wrapBuffer.getInt();
            } else {
              wrapBuffer.position(wrapBuffer.position() + Integer.BYTES);
            }
          }
          continue;
        }
        if (i > lastIndex) {
          for (int j = 0; j < repetitions[i]; j++) {
            loader.setNull(currentIndex);
            currentIndex++;
          }
          continue;
        }
        if (isNullArray[i] == 0) {
          loader.setIntegerToDic(dictionaryIndex, wrapBuffer.getInt());
          for (int j = 0; j < repetitions[i]; j++) {
            loader.setDictionaryIndex(currentIndex, dictionaryIndex);
            currentIndex++;
          }
          dictionaryIndex++;
        } else {
          wrapBuffer.position(wrapBuffer.position() + Integer.BYTES);
          for (int j = 0; j < repetitions[i]; j++) {
            loader.setNull(currentIndex);
            currentIndex++;
          }
        }
      }
    }
  }

  public static class DiffIntBinaryMaker implements IBinaryMaker {

    private final long min;

    public DiffIntBinaryMaker( final long min ) {
      this.min = min;
    }

    @Override
    public int getLogicalSize( final int columnSize ) {
      return Long.BYTES * columnSize;
    }

    @Override
    public int calcBinarySize( final int columnSize ) {
      return Integer.BYTES * columnSize;
    }

    @Override
    public void create(
        final long[] longArray ,
        final byte[] buffer ,
        final int start ,
        final int length ) throws IOException {
      ByteBuffer wrapBuffer = ByteBuffer.wrap( buffer , start , length );
      for ( long value : longArray ) {
        int castValue = (int)( value - min );
        wrapBuffer.putInt( castValue );
      }
    }

    @Override
    public PrimitiveObject[] getPrimitiveArray(
        final byte[] buffer ,
        final int start ,
        final int length ) throws IOException {
      int size = length / Integer.BYTES;
      PrimitiveObject[] result = new PrimitiveObject[size];
      ByteBuffer wrapBuffer = ByteBuffer.wrap( buffer , start , length );
      for ( int i = 0 ; i < size ; i++ ) {
        result[i] = new LongObj( (long)( wrapBuffer.getInt() ) + min );
      }
      return result;
    }

    @Override
    public void setSequentialLoader(
        final byte[] buffer,
        final int start,
        final int length,
        final byte[] isNullArray,
        final int size,
        final ISequentialLoader loader)
        throws IOException {
      ByteBuffer wrapBuffer = ByteBuffer.wrap(buffer, start, length);
      for (int i = 0; i < size; i++) {
        if (isNullArray[i] == 0) {
          loader.setLong(i, (long) (wrapBuffer.getInt()) + min);
        } else {
          wrapBuffer.position(wrapBuffer.position() + Integer.BYTES);
          loader.setNull(i);
        }
      }
      // NOTE: null padding up to load size.
      for (int i = size; i < loader.getLoadSize(); i++) {
        loader.setNull(i);
      }
    }

    @Override
    public void setDictionaryLoader(
        final byte[] buffer,
        final int start,
        final int length,
        final byte[] isNullArray,
        final int size,
        final int[] repetitions,
        final IDictionaryLoader loader)
        throws IOException {
      ByteBuffer wrapBuffer = ByteBuffer.wrap(buffer, start, length);

      // NOTE: Calculate dictionarySize
      int dictionarySize = 0;
      int lastIndex = size - 1;
      for (int i = 0; i < repetitions.length; i++) {
        if (repetitions[i] < 0) {
          throw new IOException("Repetition must be equal to or greater than 0.");
        }
        if (i > lastIndex || repetitions[i] == 0 || isNullArray[i] != 0) {
          continue;
        }
        dictionarySize++;
      }
      loader.createDictionary(dictionarySize);

      // NOTE:
      //   Set value to dict: dictionaryIndex, value
      //   Set dictionaryIndex: currentIndex, dictionaryIndex
      int currentIndex = 0;
      int dictionaryIndex = 0;
      for (int i = 0; i < repetitions.length; i++) {
        if (repetitions[i] == 0) {
          // NOTE: read skip
          if (i < size) {
            if (isNullArray[i] == 0) {
              wrapBuffer.getInt();
            } else {
              wrapBuffer.position(wrapBuffer.position() + Integer.BYTES);
            }
          }
          continue;
        }
        if (i > lastIndex) {
          for (int j = 0; j < repetitions[i]; j++) {
            loader.setNull(currentIndex);
            currentIndex++;
          }
          continue;
        }
        if (isNullArray[i] == 0) {
          loader.setLongToDic(dictionaryIndex, (long) (wrapBuffer.getInt()) + min);
          for (int j = 0; j < repetitions[i]; j++) {
            loader.setDictionaryIndex(currentIndex, dictionaryIndex);
            currentIndex++;
          }
          dictionaryIndex++;
        } else {
          wrapBuffer.position(wrapBuffer.position() + Integer.BYTES);
          for (int j = 0; j < repetitions[i]; j++) {
            loader.setNull(currentIndex);
            currentIndex++;
          }
        }
      }
    }
  }

  public static class LongBinaryMaker implements IBinaryMaker {

    @Override
    public int getLogicalSize( final int columnSize ) {
      return Long.BYTES * columnSize;
    }

    @Override
    public int calcBinarySize( final int columnSize ) {
      return Long.BYTES * columnSize;
    }

    @Override
    public void create(
        final long[] longArray ,
        final byte[] buffer ,
        final int start ,
        final int length ) throws IOException {
      ByteBuffer wrapBuffer = ByteBuffer.wrap( buffer , start , length );
      for ( long value : longArray ) {
        wrapBuffer.putLong( value );
      }
    }

    @Override
    public PrimitiveObject[] getPrimitiveArray(
        final byte[] buffer ,
        final int start ,
        final int length ) throws IOException {
      int size = length / Long.BYTES;
      PrimitiveObject[] result = new PrimitiveObject[size];
      ByteBuffer wrapBuffer = ByteBuffer.wrap( buffer , start , length );
      for ( int i = 0 ; i < size ; i++ ) {
        result[i] = new LongObj( wrapBuffer.getLong() );
      }
      return result;
    }

    @Override
    public void setSequentialLoader(
        final byte[] buffer,
        final int start,
        final int length,
        final byte[] isNullArray,
        final int size,
        final ISequentialLoader loader)
        throws IOException {
      ByteBuffer wrapBuffer = ByteBuffer.wrap(buffer, start, length);
      for (int i = 0; i < size; i++) {
        if (isNullArray[i] == 0) {
          loader.setLong(i, wrapBuffer.getLong());
        } else {
          wrapBuffer.position(wrapBuffer.position() + Long.BYTES);
          loader.setNull(i);
        }
      }
      // NOTE: null padding up to load size.
      for (int i = size; i < loader.getLoadSize(); i++) {
        loader.setNull(i);
      }
    }

    @Override
    public void setDictionaryLoader(
        final byte[] buffer,
        final int start,
        final int length,
        final byte[] isNullArray,
        final int size,
        final int[] repetitions,
        final IDictionaryLoader loader)
        throws IOException {
      ByteBuffer wrapBuffer = ByteBuffer.wrap(buffer, start, length);

      // NOTE: Calculate dictionarySize
      int dictionarySize = 0;
      int lastIndex = size - 1;
      for (int i = 0; i < repetitions.length; i++) {
        if (repetitions[i] < 0) {
          throw new IOException("Repetition must be equal to or greater than 0.");
        }
        if (i > lastIndex || repetitions[i] == 0 || isNullArray[i] != 0) {
          continue;
        }
        dictionarySize++;
      }
      loader.createDictionary(dictionarySize);

      // NOTE:
      //   Set value to dict: dictionaryIndex, value
      //   Set dictionaryIndex: currentIndex, dictionaryIndex
      int currentIndex = 0;
      int dictionaryIndex = 0;
      for (int i = 0; i < repetitions.length; i++) {
        if (repetitions[i] == 0) {
          // NOTE: read skip
          if (i < size) {
            if (isNullArray[i] == 0) {
              wrapBuffer.getLong();
            } else {
              wrapBuffer.position(wrapBuffer.position() + Long.BYTES);
            }
          }
          continue;
        }
        if (i > lastIndex) {
          for (int j = 0; j < repetitions[i]; j++) {
            loader.setNull(currentIndex);
            currentIndex++;
          }
          continue;
        }
        if (isNullArray[i] == 0) {
          loader.setLongToDic(dictionaryIndex, wrapBuffer.getLong());
          for (int j = 0; j < repetitions[i]; j++) {
            loader.setDictionaryIndex(currentIndex, dictionaryIndex);
            currentIndex++;
          }
          dictionaryIndex++;
        } else {
          wrapBuffer.position(wrapBuffer.position() + Long.BYTES);
          for (int j = 0; j < repetitions[i]; j++) {
            loader.setNull(currentIndex);
            currentIndex++;
          }
        }
      }
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
    long[] valueArray = new long[column.size()];
    byte[] isNullArray = new byte[column.size()];

    Long min = Long.MAX_VALUE;
    Long max = Long.MIN_VALUE;
    int rowCount = 0;
    boolean hasNull = false;
    for ( int i = 0 ; i < column.size() ; i++ ) {
      ICell cell = column.get(i);
      PrimitiveObject primitiveObj = null;
      if ( cell.getType() == ColumnType.NULL ) {
        hasNull = true;
        isNullArray[i] = 1;
      } else {
        rowCount++;
        PrimitiveCell stringCell = (PrimitiveCell) cell;
        primitiveObj = stringCell.getRow();
        valueArray[i] = primitiveObj.getLong();
        if ( 0 < min.compareTo( valueArray[i] ) ) {
          min = Long.valueOf( valueArray[i] );
        }
        if ( max.compareTo( valueArray[i] ) < 0 ) {
          max = Long.valueOf( valueArray[i] );
        }
      }
    }

    if ( ! hasNull && min.equals( max ) ) {
      return ConstantColumnBinaryMaker.createColumnBinary(
          createConstObject(
            column.getColumnType() ,
            min
          ) ,
          column.getColumnName() ,
          column.size() );
    }

    IBinaryMaker binaryMaker = chooseBinaryMaker( min.longValue() , max.longValue() );

    int nullBinaryLength = isNullArray.length;
    int valueLength = binaryMaker.calcBinarySize( valueArray.length );

    byte[] binaryRaw = new byte[ nullBinaryLength + valueLength ];
    ByteBuffer compressBinaryBuffer = ByteBuffer.wrap( binaryRaw , 0 , nullBinaryLength );
    compressBinaryBuffer.put( isNullArray );
    binaryMaker.create( valueArray , binaryRaw , nullBinaryLength , valueLength );

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
        binaryMaker.getLogicalSize( rowCount ) ,
        -1 ,
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
    IBinaryMaker binaryMaker = chooseBinaryMaker( min , max );
    int nullBinaryLength = analizeResult.getColumnSize();
    int valueLength = binaryMaker.calcBinarySize( analizeResult.getColumnSize() );
    return nullBinaryLength + valueLength;
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

    IBinaryMaker binaryMaker = chooseBinaryMaker(min.longValue(), max.longValue());

    int start = columnBinary.binaryStart + (Long.BYTES * 2);
    int length = columnBinary.binaryLength - (Long.BYTES * 2);

    ICompressor compressor = FindCompressor.get(columnBinary.compressorClassName);
    byte[] binary = compressor.decompress(columnBinary.binary, start, length);

    int isNullLength = columnBinary.rowCount;
    int binaryLength = binaryMaker.calcBinarySize(columnBinary.rowCount);

    binaryMaker.setSequentialLoader(
        binary, isNullLength, binaryLength, binary, columnBinary.rowCount, loader);
  }

  private void loadFromExpandColumnBinary(
      final ColumnBinary columnBinary, final IDictionaryLoader loader) throws IOException {
    ByteBuffer wrapBuffer =
        ByteBuffer.wrap(columnBinary.binary, columnBinary.binaryStart, columnBinary.binaryLength);
    Long min = Long.valueOf(wrapBuffer.getLong());
    Long max = Long.valueOf(wrapBuffer.getLong());

    IBinaryMaker binaryMaker = chooseBinaryMaker(min.longValue(), max.longValue());

    int start = columnBinary.binaryStart + (Long.BYTES * 2);
    int length = columnBinary.binaryLength - (Long.BYTES * 2);

    ICompressor compressor = FindCompressor.get(columnBinary.compressorClassName);
    byte[] binary = compressor.decompress(columnBinary.binary, start, length);

    int isNullLength = columnBinary.rowCount;
    int binaryLength = binaryMaker.calcBinarySize(columnBinary.rowCount);

    binaryMaker.setDictionaryLoader(
        binary,
        isNullLength,
        binaryLength,
        binary,
        columnBinary.rowCount,
        columnBinary.repetitions,
        loader);
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
