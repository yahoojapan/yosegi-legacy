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
import jp.co.yahoo.yosegi.blockindex.DoubleRangeBlockIndex;
import jp.co.yahoo.yosegi.compressor.CompressResult;
import jp.co.yahoo.yosegi.compressor.FindCompressor;
import jp.co.yahoo.yosegi.compressor.ICompressor;
import jp.co.yahoo.yosegi.inmemory.IDictionaryLoader;
import jp.co.yahoo.yosegi.inmemory.ILoader;
import jp.co.yahoo.yosegi.inmemory.ISequentialLoader;
import jp.co.yahoo.yosegi.inmemory.LoadType;
import jp.co.yahoo.yosegi.message.objects.DoubleObj;
import jp.co.yahoo.yosegi.spread.analyzer.IColumnAnalizeResult;
import jp.co.yahoo.yosegi.spread.column.ColumnType;
import jp.co.yahoo.yosegi.spread.column.ICell;
import jp.co.yahoo.yosegi.spread.column.IColumn;
import jp.co.yahoo.yosegi.spread.column.PrimitiveCell;
import jp.co.yahoo.yosegi.util.io.IReadSupporter;
import jp.co.yahoo.yosegi.util.io.IWriteSupporter;
import jp.co.yahoo.yosegi.util.io.unsafe.ByteBufferSupporterFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class UnsafeRangeDumpDoubleColumnBinaryMaker implements IColumnBinaryMaker {

  private static final int HEADER_SIZE = ( Double.BYTES * 2 ) + Byte.BYTES;

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
    byte[] parentsBinaryRaw =
        new byte[ Byte.BYTES + column.size() + ( column.size() * Double.BYTES ) ];
    ByteOrder order = ByteOrder.nativeOrder();

    IWriteSupporter nullSupporter = ByteBufferSupporterFactory.createWriteSupporter(
        parentsBinaryRaw , 0 , column.size() , order );
    IWriteSupporter doubleSupporter = ByteBufferSupporterFactory.createWriteSupporter(
        parentsBinaryRaw ,
        Byte.BYTES + column.size() ,
        column.size() * Double.BYTES ,
        order );

    int rowCount = 0;
    boolean hasNull = false;
    Double min = Double.MAX_VALUE;
    Double max = Double.MIN_VALUE;
    for ( int i = 0 ; i < column.size() ; i++ ) {
      ICell cell = column.get(i);
      if ( cell.getType() == ColumnType.NULL ) {
        nullSupporter.putByte( (byte)1 );
        hasNull = true;
      } else {
        rowCount++;
        PrimitiveCell byteCell = (PrimitiveCell) cell;
        nullSupporter.putByte( (byte)0 );
        Double target = Double.valueOf( byteCell.getRow().getDouble() );
        doubleSupporter.putDouble( target );
        if ( 0 < min.compareTo( target ) ) {
          min = Double.valueOf( target );
        }
        if ( max.compareTo( target ) < 0 ) {
          max = Double.valueOf( target );
        }
      }
    }

    if ( ! hasNull && min.equals( max ) ) {
      return ConstantColumnBinaryMaker.createColumnBinary(
          new DoubleObj( min ) , column.getColumnName() , column.size() );
    }

    int rawLength;
    byte byteOrderByte = order == ByteOrder.BIG_ENDIAN ? (byte)0 : (byte)1;
    parentsBinaryRaw[column.size()] = byteOrderByte;

    CompressResult compressResult = compressResultNode.getCompressResult(
        this.getClass().getName() ,
        "c0"  ,
        currentConfig.compressionPolicy ,
        currentConfig.allowedRatio );
    byte[] compressBinaryRaw;
    if ( hasNull ) {
      rawLength =  parentsBinaryRaw.length - ( Double.BYTES * ( column.size() - rowCount ) );
      compressBinaryRaw = currentConfig.compressorClass.compress(
          parentsBinaryRaw , 0 , rawLength , compressResult );
    } else {
      rawLength = Byte.BYTES + column.size() * Double.BYTES;
      compressBinaryRaw = currentConfig.compressorClass.compress(
          parentsBinaryRaw ,
          column.size() ,
          parentsBinaryRaw.length - column.size() ,
          compressResult );
    }
    byte[] binary = new byte[ HEADER_SIZE + compressBinaryRaw.length ];
    ByteBuffer wrapBuffer = ByteBuffer.wrap( binary );
    wrapBuffer.putDouble( min );
    wrapBuffer.putDouble( max );
    wrapBuffer.put( hasNull ? (byte)1 : (byte)0 );
    wrapBuffer.put( compressBinaryRaw );

    return new ColumnBinary(
        this.getClass().getName() ,
        currentConfig.compressorClass.getClass().getName() ,
        column.getColumnName() ,
        ColumnType.DOUBLE ,
        column.size() ,
        rawLength ,
        rowCount * Double.BYTES ,
        -1 ,
        binary ,
        0 ,
        binary.length ,
        null );
  }

  @Override
  public int calcBinarySize( final IColumnAnalizeResult analizeResult ) {
    if ( analizeResult.getNullCount() == 0 && analizeResult.getUniqCount() == 1 ) {
      return Double.BYTES;
    } else if ( analizeResult.getNullCount() == 0 ) {
      return analizeResult.getColumnSize() * Double.BYTES;
    } else {
      return analizeResult.getColumnSize()
          + ( analizeResult.getColumnSize() - analizeResult.getNullCount() ) * Double.BYTES;
    }
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
    byte type = columnBinary.binary[columnBinary.binaryStart + Double.BYTES * 2];
    ICompressor compressor = FindCompressor.get(columnBinary.compressorClassName);
    byte[] binary =
        compressor.decompress(
            columnBinary.binary,
            columnBinary.binaryStart + HEADER_SIZE,
            columnBinary.binaryLength - HEADER_SIZE);
    if (type == (byte) 1) {
      ByteOrder order =
          binary[columnBinary.rowCount] == (byte) 0
              ? ByteOrder.BIG_ENDIAN
              : ByteOrder.LITTLE_ENDIAN;
      IReadSupporter nullReader =
          ByteBufferSupporterFactory.createReadSupporter(binary, 0, columnBinary.rowCount, order);
      IReadSupporter doubleReader =
          ByteBufferSupporterFactory.createReadSupporter(
              binary,
              columnBinary.rowCount + Byte.BYTES,
              Double.BYTES * columnBinary.rowCount,
              order);
      for (int i = 0; i < columnBinary.rowCount; i++) {
        if (nullReader.getByte() == (byte) 0) {
          loader.setDouble(i, doubleReader.getDouble());
        } else {
          loader.setNull(i);
        }
      }
    } else {
      ByteOrder order = binary[0] == (byte) 0 ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN;
      IReadSupporter doubleReader =
          ByteBufferSupporterFactory.createReadSupporter(
              binary, Byte.BYTES, Double.BYTES * columnBinary.rowCount, order);
      for (int i = 0; i < columnBinary.rowCount; i++) {
        loader.setDouble(i, doubleReader.getDouble());
      }
    }
    // NOTE: null padding up to load size.
    for (int i = columnBinary.rowCount; i < loader.getLoadSize(); i++) {
      loader.setNull(i);
    }
  }

  private void loadFromExpandColumnBinary(
      final ColumnBinary columnBinary, final IDictionaryLoader loader) throws IOException {
    byte type = columnBinary.binary[columnBinary.binaryStart + Double.BYTES * 2];
    ICompressor compressor = FindCompressor.get(columnBinary.compressorClassName);
    byte[] binary =
        compressor.decompress(
            columnBinary.binary,
            columnBinary.binaryStart + HEADER_SIZE,
            columnBinary.binaryLength - HEADER_SIZE);
    if (type == (byte) 1) {
      ByteOrder order =
          binary[columnBinary.rowCount] == (byte) 0
              ? ByteOrder.BIG_ENDIAN
              : ByteOrder.LITTLE_ENDIAN;
      IReadSupporter nullReader =
          ByteBufferSupporterFactory.createReadSupporter(binary, 0, columnBinary.rowCount, order);
      IReadSupporter doubleReader =
          ByteBufferSupporterFactory.createReadSupporter(
              binary,
              columnBinary.rowCount + Byte.BYTES,
              Double.BYTES * columnBinary.rowCount,
              order);

      // NOTE: Calculate dictionarySize
      int dictionarySize = 0;
      int lastIndex = columnBinary.rowCount - 1;
      for (int i = 0; i < columnBinary.repetitions.length; i++) {
        if (columnBinary.repetitions[i] < 0) {
          throw new IOException("Repetition must be equal to or greater than 0.");
        }
        // FIXME: continue if null
        if (i > lastIndex || nullReader.getByte() == (byte) 0 || columnBinary.repetitions[i] == 0) {
          continue;
        }
        dictionarySize++;
      }
      loader.createDictionary(dictionarySize);
      // NOTE: reset nullReader
      nullReader =
          ByteBufferSupporterFactory.createReadSupporter(binary, 0, columnBinary.rowCount, order);

      // NOTE:
      //   Set value to dict: dictionaryIndex, value
      //   Set dictionaryIndex: currentIndex, dictionaryIndex
      int currentIndex = 0;
      int dictionaryIndex = 0;
      for (int i = 0; i < columnBinary.repetitions.length; i++) {
        if (columnBinary.repetitions[i] == 0) {
          if (i < columnBinary.rowCount) {
            // NOTE: read skip
            if (nullReader.getByte() == (byte) 0) {
              doubleReader.getDouble();
            }
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
        if (nullReader.getByte() == (byte) 0) {
          loader.setDoubleToDic(dictionaryIndex, doubleReader.getDouble());
          for (int j = 0; j < columnBinary.repetitions[i]; j++) {
            loader.setDictionaryIndex(currentIndex, dictionaryIndex);
            currentIndex++;
          }
          dictionaryIndex++;
        } else {
          for (int j = 0; j < columnBinary.repetitions[i]; j++) {
            loader.setNull(currentIndex);
            currentIndex++;
          }
        }
      }
    } else {
      ByteOrder order = binary[0] == (byte) 0 ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN;
      IReadSupporter doubleReader =
          ByteBufferSupporterFactory.createReadSupporter(
              binary, Byte.BYTES, Double.BYTES * columnBinary.rowCount, order);

      // NOTE: Calculate dictionarySize
      int dictionarySize = 0;
      int lastIndex = columnBinary.rowCount - 1;
      for (int i = 0; i < columnBinary.repetitions.length; i++) {
        if (columnBinary.repetitions[i] < 0) {
          throw new IOException("Repetition must be equal to or greater than 0.");
        }
        if (i > lastIndex || columnBinary.repetitions[i] == 0) {
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
      for (int i = 0; i < columnBinary.repetitions.length; i++) {
        if (columnBinary.repetitions[i] == 0) {
          if (i < columnBinary.rowCount) {
            // NOTE: read skip
            doubleReader.getDouble();
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
        loader.setDoubleToDic(dictionaryIndex, doubleReader.getDouble());
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
    Double min = Double.valueOf( wrapBuffer.getDouble() );
    Double max = Double.valueOf( wrapBuffer.getDouble() );
    BlockIndexNode currentNode = parentNode.getChildNode( columnBinary.columnName );
    currentNode.setBlockIndex( new DoubleRangeBlockIndex( min , max ) );
  }
}
