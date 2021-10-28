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
import jp.co.yahoo.yosegi.compressor.FindCompressor;
import jp.co.yahoo.yosegi.compressor.ICompressor;
import jp.co.yahoo.yosegi.inmemory.ILoader;
import jp.co.yahoo.yosegi.inmemory.ISequentialLoader;
import jp.co.yahoo.yosegi.inmemory.LoadType;
import jp.co.yahoo.yosegi.spread.analyzer.IColumnAnalizeResult;
import jp.co.yahoo.yosegi.spread.column.ColumnType;
import jp.co.yahoo.yosegi.spread.column.ICell;
import jp.co.yahoo.yosegi.spread.column.IColumn;
import jp.co.yahoo.yosegi.spread.column.PrimitiveCell;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.FloatBuffer;


public class DumpFloatColumnBinaryMaker implements IColumnBinaryMaker {

  private int getBinaryLength( final int columnSize ) {
    return ( Integer.BYTES * 2 ) + columnSize + ( columnSize * Float.BYTES );
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
    byte[] binaryRaw = new byte[ getBinaryLength( column.size() ) ];
    ByteBuffer lengthBuffer = ByteBuffer.wrap( binaryRaw );
    lengthBuffer.putInt( column.size() );
    lengthBuffer.putInt( column.size() * Float.BYTES );

    ByteBuffer nullFlagBuffer = ByteBuffer.wrap( binaryRaw , Integer.BYTES * 2 , column.size() );
    FloatBuffer floatBuffer = ByteBuffer.wrap(
        binaryRaw ,
        ( Integer.BYTES * 2 + column.size() ) ,
        ( column.size() * Float.BYTES ) ).asFloatBuffer();

    int rowCount = 0;
    for ( int i = 0 ; i < column.size() ; i++ ) {
      ICell cell = column.get(i);
      if ( cell.getType() == ColumnType.NULL ) {
        nullFlagBuffer.put( (byte)1 );
        floatBuffer.put( (float)0 );
      } else {
        rowCount++;
        PrimitiveCell byteCell = (PrimitiveCell) cell;
        nullFlagBuffer.put( (byte)0 );
        floatBuffer.put( byteCell.getRow().getFloat() );
      }
    }

    byte[] binary = currentConfig.compressorClass.compress( binaryRaw , 0 , binaryRaw.length );

    return new ColumnBinary(
        this.getClass().getName() ,
        currentConfig.compressorClass.getClass().getName() ,
        column.getColumnName() ,
        ColumnType.FLOAT ,
        rowCount ,
        binaryRaw.length ,
        rowCount * Float.BYTES ,
        -1 ,
        binary ,
        0 ,
        binary.length ,
        null );
  }

  @Override
  public int calcBinarySize( final IColumnAnalizeResult analizeResult ) {
    return getBinaryLength( analizeResult.getColumnSize() );
  }

  @Override
  public LoadType getLoadType(final ColumnBinary columnBinary, final int loadSize) {
    return LoadType.SEQUENTIAL;
  }

  private void loadFromColumnBinary(final ColumnBinary columnBinary, final ISequentialLoader loader)
      throws IOException {
    loadFromColumnBinary(columnBinary, columnBinary.binaryStart, columnBinary.binaryLength, loader);
  }

  protected void loadFromColumnBinary(
      final ColumnBinary columnBinary,
      final int start,
      final int length,
      final ISequentialLoader loader)
      throws IOException {
    ICompressor compressor = FindCompressor.get(columnBinary.compressorClassName);
    byte[] binary = compressor.decompress(columnBinary.binary, start, length);
    ByteBuffer wrapBuffer = ByteBuffer.wrap(binary);
    int nullFlagBinaryLength = wrapBuffer.getInt();
    int floatBinaryLength = wrapBuffer.getInt();
    int nullFlagBinaryStart = Integer.BYTES * 2;
    int floatBinaryStart = nullFlagBinaryStart + nullFlagBinaryLength;

    ByteBuffer nullFlagBuffer = ByteBuffer.wrap(binary, nullFlagBinaryStart, nullFlagBinaryLength);
    FloatBuffer floatBuffer =
        ByteBuffer.wrap(binary, floatBinaryStart, floatBinaryLength).asFloatBuffer();
    for (int i = 0; i < nullFlagBinaryLength; i++) {
      if (nullFlagBuffer.get() == (byte) 0) {
        loader.setFloat(i, floatBuffer.get(i));
      } else {
        loader.setNull(i);
      }
    }
    // NOTE: null padding up to load size
    for (int i = nullFlagBinaryLength; i < loader.getLoadSize(); i++) {
      loader.setNull(i);
    }
  }

  private void loadFromExpandColumnBinary(
      final ColumnBinary columnBinary, final ISequentialLoader loader) throws IOException {
    loadFromExpandColumnBinary(
        columnBinary, columnBinary.binaryStart, columnBinary.binaryLength, loader);
  }

  protected void loadFromExpandColumnBinary(
      final ColumnBinary columnBinary,
      final int start,
      final int length,
      final ISequentialLoader loader)
      throws IOException {
    ICompressor compressor = FindCompressor.get(columnBinary.compressorClassName);
    byte[] binary = compressor.decompress(columnBinary.binary, start, length);
    ByteBuffer wrapBuffer = ByteBuffer.wrap(binary);
    int nullFlagBinaryLength = wrapBuffer.getInt();
    int floatBinaryLength = wrapBuffer.getInt();
    int nullFlagBinaryStart = Integer.BYTES * 2;
    int floatBinaryStart = nullFlagBinaryStart + nullFlagBinaryLength;

    ByteBuffer nullFlagBuffer = ByteBuffer.wrap(binary, nullFlagBinaryStart, nullFlagBinaryLength);
    FloatBuffer floatBuffer =
        ByteBuffer.wrap(binary, floatBinaryStart, floatBinaryLength).asFloatBuffer();

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
    int lastIndex = nullFlagBinaryLength - 1;
    for (int i = 0; i < columnBinary.repetitions.length; i++) {
      if (columnBinary.repetitions[i] == 0) {
        if (i < nullFlagBinaryLength) {
          if (nullFlagBuffer.get() == (byte) 0) {
            floatBuffer.get();
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
      if (nullFlagBuffer.get() == (byte) 0) {
        float value = floatBuffer.get(i);
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
    parentNode.getChildNode( columnBinary.columnName ).disable();
  }
}
