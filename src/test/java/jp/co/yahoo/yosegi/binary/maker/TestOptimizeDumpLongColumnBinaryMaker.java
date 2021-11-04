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

import java.io.IOException;

import jp.co.yahoo.yosegi.inmemory.YosegiLoaderFactory;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

import jp.co.yahoo.yosegi.binary.ColumnBinaryMakerConfig;
import jp.co.yahoo.yosegi.binary.ColumnBinaryMakerCustomConfigNode;
import jp.co.yahoo.yosegi.binary.CompressResultNode;

import jp.co.yahoo.yosegi.message.objects.*;
import jp.co.yahoo.yosegi.message.objects.PrimitiveObject;

import jp.co.yahoo.yosegi.spread.column.IColumn;
import jp.co.yahoo.yosegi.spread.column.PrimitiveColumn;
import jp.co.yahoo.yosegi.spread.column.ColumnType;
import jp.co.yahoo.yosegi.binary.ColumnBinary;

public class TestOptimizeDumpLongColumnBinaryMaker {

  public IColumn toColumn(final ColumnBinary columnBinary) throws IOException {
    int loadCount = (columnBinary.isSetLoadSize) ? columnBinary.loadSize : columnBinary.rowCount;
    return new YosegiLoaderFactory().create(columnBinary, loadCount);
  }

  public int getLoadSize(final int[] repetitions) {
    if (repetitions == null) {
      return 0;
    }
    int loadSize = 0;
    for (int size : repetitions) {
      loadSize += size;
    }
    return loadSize;
  }

  @Test
  public void T_toBinary_1() throws IOException{
    IColumn column = new PrimitiveColumn( ColumnType.SHORT , "SHORT" );
    column.add( ColumnType.SHORT , new ShortObj( (short)1 ) , 0 );
    column.add( ColumnType.SHORT , new ShortObj( (short)2 ) , 1 );
    column.add( ColumnType.SHORT , new ShortObj( (short)6 ) , 5 );

    ColumnBinaryMakerConfig defaultConfig = new ColumnBinaryMakerConfig();
    ColumnBinaryMakerCustomConfigNode configNode = new ColumnBinaryMakerCustomConfigNode( "root" , defaultConfig );

    IColumnBinaryMaker maker = new OptimizeDumpLongColumnBinaryMaker();
    ColumnBinary columnBinary = maker.toBinary( defaultConfig , null , new CompressResultNode() , column );

    assertEquals( columnBinary.columnName , "SHORT" );
    assertEquals( columnBinary.rowCount , 6 );
    assertEquals( columnBinary.columnType , ColumnType.SHORT );

    IColumn decodeColumn = toColumn(columnBinary);
    assertEquals( decodeColumn.getColumnKeys().size() , 0 );
    assertEquals( decodeColumn.getColumnSize() , 0 );

    assertEquals( (short)1 , ( (PrimitiveObject)( decodeColumn.get(0).getRow() ) ).getShort() );
    assertEquals( (short)2 , ( (PrimitiveObject)( decodeColumn.get(1).getRow() ) ).getShort() );
    assertEquals( (short)6 , ( (PrimitiveObject)( decodeColumn.get(5).getRow() ) ).getShort() );

    assertEquals( decodeColumn.getColumnKeys().size() , 0 );
    assertEquals( decodeColumn.getColumnSize() , 0 );
  }

  @Test
  public void T_toBinary_2() throws IOException{
    IColumn column = new PrimitiveColumn( ColumnType.INTEGER , "C" );
    column.add( ColumnType.INTEGER , new IntegerObj( 1488553199 ) , 0 );
    column.add( ColumnType.INTEGER , new IntegerObj( 1488553299 ) , 1 );
    column.add( ColumnType.INTEGER , new IntegerObj( 1488653299 ) , 2 );
    column.add( ColumnType.INTEGER , new IntegerObj( 1488653299 ) , 5 );

    ColumnBinaryMakerConfig defaultConfig = new ColumnBinaryMakerConfig();
    ColumnBinaryMakerCustomConfigNode configNode = new ColumnBinaryMakerCustomConfigNode( "root" , defaultConfig );

    IColumnBinaryMaker maker = new OptimizeDumpLongColumnBinaryMaker();
    ColumnBinary columnBinary = maker.toBinary( defaultConfig , null , new CompressResultNode() , column );

    assertEquals( columnBinary.columnName , "C" );
    assertEquals( columnBinary.rowCount , 6 );
    assertEquals( columnBinary.columnType , ColumnType.INTEGER );

    IColumn decodeColumn = toColumn( columnBinary );
    assertEquals( decodeColumn.getColumnKeys().size() , 0 );
    assertEquals( decodeColumn.getColumnSize() , 0 );

    assertEquals( 1488553199 , ( (PrimitiveObject)( decodeColumn.get(0).getRow() ) ).getInt() );
    assertEquals( 1488553299 , ( (PrimitiveObject)( decodeColumn.get(1).getRow() ) ).getInt() );
    assertEquals( 1488653299 , ( (PrimitiveObject)( decodeColumn.get(2).getRow() ) ).getInt() );
    assertEquals( 1488653299 , ( (PrimitiveObject)( decodeColumn.get(5).getRow() ) ).getInt() );

    assertEquals( decodeColumn.getColumnKeys().size() , 0 );
    assertEquals( decodeColumn.getColumnSize() , 0 );
  }
}
