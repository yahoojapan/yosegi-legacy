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
import jp.co.yahoo.yosegi.compressor.CompressResult;
import jp.co.yahoo.yosegi.compressor.FindCompressor;
import jp.co.yahoo.yosegi.compressor.ICompressor;
import jp.co.yahoo.yosegi.inmemory.IDictionaryLoader;
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
import java.nio.IntBuffer;
import java.util.ArrayList;
import java.util.List;

public class DumpBytesColumnBinaryMaker implements IColumnBinaryMaker {

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
    List<Integer> columnList = new ArrayList<Integer>();
    List<byte[]> objList = new ArrayList<byte[]>();
    objList.add( new byte[0] );
    int totalLength = 0;
    int rowCount = 0;
    for ( int i = 0 ; i < column.size() ; i++ ) {
      ICell cell = column.get(i);
      if ( cell.getType() == ColumnType.NULL ) {
        columnList.add( 0 );
        continue;
      }
      PrimitiveCell byteCell = (PrimitiveCell) cell;
      byte[] obj = byteCell.getRow().getBytes();
      if ( obj == null ) {
        columnList.add( 0 );
        continue;
      }
      rowCount++;
      totalLength += obj.length;
      objList.add( obj );
      columnList.add( objList.size() - 1 );
    }
    byte[] binaryRaw = convertBinary( columnList , objList , currentConfig , totalLength );
    CompressResult compressResult = compressResultNode.getCompressResult(
        this.getClass().getName() ,
        "c0"  ,
        currentConfig.compressionPolicy ,
        currentConfig.allowedRatio );
    byte[] binary = currentConfig.compressorClass.compress(
        binaryRaw , 0 , binaryRaw.length , compressResult );

    return new ColumnBinary(
        this.getClass().getName() ,
        currentConfig.compressorClass.getClass().getName() ,
        column.getColumnName() ,
        ColumnType.BYTES ,
        rowCount ,
        binaryRaw.length ,
        totalLength + Integer.BYTES * rowCount ,
        -1 ,
        binary ,
        0 ,
        binary.length ,
        null );
  }

  private byte[] convertBinary(
      final List<Integer> columnList ,
      final List<byte[]> objList ,
      final ColumnBinaryMakerConfig currentConfig ,
      final int totalLength ) throws IOException {
    int dicBinarySize = ( objList.size() * Integer.BYTES ) + totalLength;
    int binaryLength =
        ( Integer.BYTES * 2 ) + ( columnList.size() * Integer.BYTES ) + dicBinarySize;

    byte[] binaryRaw = new byte[binaryLength];
    ByteBuffer wrapBuffer = ByteBuffer.wrap( binaryRaw );
    wrapBuffer.putInt( columnList.size() );
    wrapBuffer.putInt( dicBinarySize );
    for ( Integer index : columnList ) {
      wrapBuffer.putInt( index );
    }

    for ( byte[] obj : objList ) {
      wrapBuffer.putInt( obj.length );
      wrapBuffer.put( obj );
    }

    return binaryRaw;
  }

  @Override
  public int calcBinarySize( final IColumnAnalizeResult analizeResult ) {
    int dicBinarySize = ( analizeResult.getRowCount() * Integer.BYTES )
        + analizeResult.getLogicalDataSize();
    return ( Integer.BYTES * 2 )
        + ( analizeResult.getColumnSize() * Integer.BYTES ) + dicBinarySize;
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
    int start = columnBinary.binaryStart;
    int length = columnBinary.binaryLength;

    ICompressor compressor = FindCompressor.get(columnBinary.compressorClassName);
    byte[] binary = compressor.decompress(columnBinary.binary, start, length);
    ByteBuffer wrapBuffer = ByteBuffer.wrap(binary);
    int indexListSize = wrapBuffer.getInt();
    int objBinaryLength = wrapBuffer.getInt();
    int indexBinaryStart = Integer.BYTES * 2;
    int indexBinaryLength = Integer.BYTES * indexListSize;
    int objBinaryStart = indexBinaryStart + indexBinaryLength;

    IntBuffer indexBuffer =
        ByteBuffer.wrap(binary, indexBinaryStart, indexBinaryLength).asIntBuffer();
    ByteBuffer dicBuffer = ByteBuffer.wrap(binary, objBinaryStart, objBinaryLength);
    int skipLength = dicBuffer.getInt();
    dicBuffer.position(dicBuffer.position() + skipLength);

    for (int i = 0; i < indexListSize; i++) {
      int index = indexBuffer.get();
      if (index != 0) {
        int objLength = dicBuffer.getInt();
        loader.setBytes(i, binary, dicBuffer.position(), objLength);
        dicBuffer.position(dicBuffer.position() + objLength);
      }
    }
    // NOTE: null padding up to load size.
    for (int i = indexListSize; i < loader.getLoadSize(); i++) {
      loader.setNull(i);
    }
  }

  private void loadFromExpandColumnBinary(
      final ColumnBinary columnBinary, final IDictionaryLoader loader) throws IOException {
    int start = columnBinary.binaryStart;
    int length = columnBinary.binaryLength;

    ICompressor compressor = FindCompressor.get(columnBinary.compressorClassName);
    byte[] binary = compressor.decompress(columnBinary.binary, start, length);
    ByteBuffer wrapBuffer = ByteBuffer.wrap(binary);
    int indexListSize = wrapBuffer.getInt();
    int objBinaryLength = wrapBuffer.getInt();
    int indexBinaryStart = Integer.BYTES * 2;
    int indexBinaryLength = Integer.BYTES * indexListSize;
    int objBinaryStart = indexBinaryStart + indexBinaryLength;

    IntBuffer indexBuffer =
        ByteBuffer.wrap(binary, indexBinaryStart, indexBinaryLength).asIntBuffer();
    ByteBuffer dicBuffer = ByteBuffer.wrap(binary, objBinaryStart, objBinaryLength);
    int skipLength = dicBuffer.getInt();
    dicBuffer.position(dicBuffer.position() + skipLength);

    // NOTE: Calculate dictionarySize
    int dictionarySize = 0;
    int lastIndex = indexListSize - 1;
    for (int i = 0; i < columnBinary.repetitions.length; i++) {
      if (columnBinary.repetitions[i] < 0) {
        throw new IOException("Repetition must be equal to or greater than 0.");
      }
      if (i > lastIndex || indexBuffer.get() == 0 || columnBinary.repetitions[i] == 0) {
        continue;
      }
      dictionarySize++;
    }
    loader.createDictionary(dictionarySize);
    // NOTE: reset indexBuffer
    indexBuffer = ByteBuffer.wrap(binary, indexBinaryStart, indexBinaryLength).asIntBuffer();

    // NOTE:
    //   Set value to dict: dictionaryIndex, value
    //   Set dictionaryIndex: currentIndex, dictionaryIndex
    int dictionaryIndex = 0;
    int currentIndex = 0;
    for (int i = 0; i < columnBinary.repetitions.length; i++) {
      if (columnBinary.repetitions[i] == 0) {
        if (i < indexListSize) {
          // NOTE: read skip
          int index = indexBuffer.get();
          if (index != 0) {
            int objLength = dicBuffer.getInt();
            dicBuffer.position(dicBuffer.position() + objLength);
          }
        }
        continue;
      }
      if (i > lastIndex) {
        for (int j = 0; j < columnBinary.repetitions[i]; j++) {
          loader.setNull(currentIndex);
          currentIndex++;
        }
      } else {
        int index = indexBuffer.get();
        if (index != 0) {
          int objLength = dicBuffer.getInt();
          loader.setBytesToDic(dictionaryIndex, binary, dicBuffer.position(), objLength);
          for (int j = 0; j < columnBinary.repetitions[i]; j++) {
            loader.setDictionaryIndex(currentIndex, dictionaryIndex);
            currentIndex++;
          }
          dicBuffer.position(dicBuffer.position() + objLength);
          dictionaryIndex++;
        } else {
          for (int j = 0; j < columnBinary.repetitions[i]; j++) {
            loader.setNull(currentIndex);
            currentIndex++;
          }
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
    parentNode.getChildNode( columnBinary.columnName ).disable();
  }
}
