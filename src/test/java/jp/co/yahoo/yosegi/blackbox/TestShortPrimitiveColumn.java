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
package jp.co.yahoo.yosegi.blackbox;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import jp.co.yahoo.yosegi.inmemory.YosegiLoaderFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.Arguments;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;


import jp.co.yahoo.yosegi.config.Configuration;

import jp.co.yahoo.yosegi.message.objects.*;

import jp.co.yahoo.yosegi.spread.expression.*;
import jp.co.yahoo.yosegi.spread.column.filter.*;
import jp.co.yahoo.yosegi.spread.column.*;
import jp.co.yahoo.yosegi.binary.*;
import jp.co.yahoo.yosegi.binary.maker.*;

public class TestShortPrimitiveColumn {

  public static Stream<Arguments> data1() throws IOException{
    return Stream.of(
        arguments("jp.co.yahoo.yosegi.binary.maker.OptimizeDumpLongColumnBinaryMaker"),
        arguments("jp.co.yahoo.yosegi.binary.maker.OptimizeLongColumnBinaryMaker"),
        arguments("jp.co.yahoo.yosegi.binary.maker.UnsafeOptimizeDumpLongColumnBinaryMaker"),
        arguments("jp.co.yahoo.yosegi.binary.maker.UnsafeOptimizeLongColumnBinaryMaker"));
  }

  public static Stream<Arguments> D_shortColumnBinaryMaker() {
    return Stream.of(
        arguments("jp.co.yahoo.yosegi.binary.maker.OptimizeDumpLongColumnBinaryMaker"),
        arguments("jp.co.yahoo.yosegi.binary.maker.OptimizeLongColumnBinaryMaker"),
        arguments("jp.co.yahoo.yosegi.binary.maker.UnsafeOptimizeDumpLongColumnBinaryMaker"),
        arguments("jp.co.yahoo.yosegi.binary.maker.UnsafeOptimizeLongColumnBinaryMaker"));
  }

  public static IColumn toColumn(final ColumnBinary columnBinary) throws IOException {
    return toColumn(columnBinary, null);
  }

  public static IColumn toColumn(final ColumnBinary columnBinary, Integer loadCount) throws IOException {
    if (loadCount == null) {
      loadCount = (columnBinary.isSetLoadSize) ? columnBinary.loadSize : columnBinary.rowCount;
    }
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

  public IColumn createTestColumn(final String targetClassName, final short[] valueArray)
      throws IOException {
    return createTestColumn(targetClassName, valueArray, null, null);
  }

  public IColumn createTestColumn(
      final String targetClassName,
      final short[] valueArray,
      final int[] repetitions,
      final Integer loadSize)
      throws IOException {
    IColumn column = new PrimitiveColumn( ColumnType.SHORT , "column" );
    for ( int i = 0 ; i < valueArray.length ; i++ ) {
      column.add( ColumnType.SHORT , new ShortObj( valueArray[i] ) , i );
    }

    IColumnBinaryMaker maker = FindColumnBinaryMaker.get( targetClassName );
    ColumnBinaryMakerConfig defaultConfig = new ColumnBinaryMakerConfig();
    ColumnBinaryMakerCustomConfigNode configNode = new ColumnBinaryMakerCustomConfigNode( "root" , defaultConfig );
    ColumnBinary columnBinary = maker.toBinary( defaultConfig , null , new CompressResultNode() , column );
    if (repetitions == null) {
      return toColumn(columnBinary, loadSize);
    } else {
      columnBinary.setRepetitions(repetitions, loadSize);
      return toColumn(columnBinary);
    }
  }

  public static IColumn createNotNullColumn(final String targetClassName) throws IOException {
    return createNotNullColumn(targetClassName, null, null);
  }

  public static Short notNullColumnValue(int index) {
    final Short[] values =
        new Short[] {
          Short.MAX_VALUE,
          Short.MIN_VALUE,
          (short) -200,
          (short) -300,
          (short) -400,
          (short) -500,
          (short) -600,
          (short) 700,
          (short) 800,
          (short) 900,
          (short) 0
        };
    if (index < values.length) {
      return values[index];
    }
    return null;
  }

  public static IColumn createNotNullColumn(
      final String targetClassName, final int[] repetitions, final Integer loadSize)
      throws IOException {
    IColumn column = new PrimitiveColumn(ColumnType.SHORT, "column");
    for (int i = 0; i <= 10; i++) {
      column.add(ColumnType.SHORT, new ShortObj(notNullColumnValue(i)), i);
    }

    IColumnBinaryMaker maker = FindColumnBinaryMaker.get(targetClassName);
    ColumnBinaryMakerConfig defaultConfig = new ColumnBinaryMakerConfig();
    ColumnBinaryMakerCustomConfigNode configNode =
        new ColumnBinaryMakerCustomConfigNode("root", defaultConfig);
    ColumnBinary columnBinary =
        maker.toBinary(defaultConfig, null, new CompressResultNode(), column);
    if (repetitions == null) {
      return toColumn(columnBinary, loadSize);
    } else {
      columnBinary.setRepetitions(repetitions, loadSize);
      return toColumn(columnBinary);
    }
  }

  public static IColumn createNullColumn(final String targetClassName) throws IOException {
    return createNullColumn(targetClassName, null, null);
  }

  public static IColumn createNullColumn(
      final String targetClassName, final int[] repetitions, final Integer loadSize)
      throws IOException {
    IColumn column = new PrimitiveColumn( ColumnType.SHORT , "column" );

    IColumnBinaryMaker maker = FindColumnBinaryMaker.get( targetClassName );
    ColumnBinaryMakerConfig defaultConfig = new ColumnBinaryMakerConfig();
    ColumnBinaryMakerCustomConfigNode configNode = new ColumnBinaryMakerCustomConfigNode( "root" , defaultConfig );
    ColumnBinary columnBinary = maker.toBinary( defaultConfig , null , new CompressResultNode() , column );
    if (repetitions == null) {
      return toColumn(columnBinary, loadSize);
    } else {
      columnBinary.setRepetitions(repetitions, loadSize);
      return toColumn(columnBinary);
    }
  }

  public static IColumn createHasNullColumn(final String targetClassName) throws IOException {
    return createHasNullColumn(targetClassName, null, null);
  }

  public static Short hasNullColumnValue(int index) {
    final Map<Integer, Short> values =
        new HashMap<Integer, Short>() {
          {
            put(0, (short) 0);
            put(4, (short) 4);
            put(8, (short) 8);
          }
        };
    if (values.containsKey(index)) {
      return values.get(index);
    }
    return null;
  }

  public static IColumn createHasNullColumn(
      final String targetClassName, final int[] repetitions, final Integer loadSize)
      throws IOException {
    IColumn column = new PrimitiveColumn(ColumnType.SHORT, "column");
    for (int i : new int[] {0, 4, 8}) {
      column.add(ColumnType.SHORT, new ShortObj(hasNullColumnValue(i)), i);
    }

    IColumnBinaryMaker maker = FindColumnBinaryMaker.get(targetClassName);
    ColumnBinaryMakerConfig defaultConfig = new ColumnBinaryMakerConfig();
    ColumnBinaryMakerCustomConfigNode configNode =
        new ColumnBinaryMakerCustomConfigNode("root", defaultConfig);
    ColumnBinary columnBinary =
        maker.toBinary(defaultConfig, null, new CompressResultNode(), column);
    if (repetitions == null) {
      return toColumn(columnBinary, loadSize);
    } else {
      columnBinary.setRepetitions(repetitions, loadSize);
      return toColumn(columnBinary);
    }
  }

  public static IColumn createLastCellColumn(final String targetClassName) throws IOException {
    return createLastCellColumn(targetClassName, null, null);
  }

  public static Short lastCellColumnValue(int index) {
    final Map<Integer, Short> values =
        new HashMap<Integer, Short>() {
          {
            put(10000, Short.MAX_VALUE);
          }
        };
    if (values.containsKey(index)) {
      return values.get(index);
    }
    return null;
  }

  public static IColumn createLastCellColumn(
      final String targetClassName, final int[] repetitions, final Integer loadSize)
      throws IOException {
    IColumn column = new PrimitiveColumn(ColumnType.SHORT, "column");
    for (int i : new int[] {10000}) {
      column.add(ColumnType.SHORT, new ShortObj(lastCellColumnValue(i)), i);
    }

    IColumnBinaryMaker maker = FindColumnBinaryMaker.get(targetClassName);
    ColumnBinaryMakerConfig defaultConfig = new ColumnBinaryMakerConfig();
    ColumnBinaryMakerCustomConfigNode configNode =
        new ColumnBinaryMakerCustomConfigNode("root", defaultConfig);
    ColumnBinary columnBinary =
        maker.toBinary(defaultConfig, null, new CompressResultNode(), column);
    if (repetitions == null) {
      return toColumn(columnBinary, loadSize);
    } else {
      columnBinary.setRepetitions(repetitions, loadSize);
      return toColumn(columnBinary);
    }
  }
  public static Short fixedColumnValue(int index) {
    final Short[] values =
        new Short[] {
            Short.MAX_VALUE,
            Short.MAX_VALUE,
            Short.MAX_VALUE,
            Short.MAX_VALUE,
            Short.MAX_VALUE,
            Short.MAX_VALUE,
            Short.MAX_VALUE,
            Short.MAX_VALUE,
            Short.MAX_VALUE,
            Short.MAX_VALUE,
            Short.MAX_VALUE
        };
    if (index < values.length) {
      return values[index];
    }
    return null;
  }

  public static IColumn createFixedColumn(
      final String targetClassName, final int[] repetitions, final Integer loadSize)
      throws IOException {
    IColumn column = new PrimitiveColumn(ColumnType.SHORT, "column");
    for (int i = 0; i <= 10; i++) {
      column.add(ColumnType.SHORT, new ShortObj(fixedColumnValue(i)), i);
    }

    IColumnBinaryMaker maker = FindColumnBinaryMaker.get(targetClassName);
    ColumnBinaryMakerConfig defaultConfig = new ColumnBinaryMakerConfig();
    ColumnBinaryMakerCustomConfigNode configNode =
        new ColumnBinaryMakerCustomConfigNode("root", defaultConfig);
    ColumnBinary columnBinary =
        maker.toBinary(defaultConfig, null, new CompressResultNode(), column);
    if (repetitions == null) {
      return toColumn(columnBinary, loadSize);
    } else {
      columnBinary.setRepetitions(repetitions, loadSize);
      return toColumn(columnBinary);
    }
  }

  @ParameterizedTest
  @MethodSource( "data1" )
  public void T_notNull_1( final String targetClassName ) throws IOException{
    IColumn column = createNotNullColumn( targetClassName );
    assertEquals( ( (PrimitiveObject)( column.get(0).getRow() ) ).getShort() , Short.MAX_VALUE );
    assertEquals( ( (PrimitiveObject)( column.get(1).getRow() ) ).getShort() , Short.MIN_VALUE );
    assertEquals( ( (PrimitiveObject)( column.get(2).getRow() ) ).getShort() , (short)-200 );
    assertEquals( ( (PrimitiveObject)( column.get(3).getRow() ) ).getShort() , (short)-300 );
    assertEquals( ( (PrimitiveObject)( column.get(4).getRow() ) ).getShort() , (short)-400 );
    assertEquals( ( (PrimitiveObject)( column.get(5).getRow() ) ).getShort() , (short)-500 );
    assertEquals( ( (PrimitiveObject)( column.get(6).getRow() ) ).getShort() , (short)-600 );
    assertEquals( ( (PrimitiveObject)( column.get(7).getRow() ) ).getShort() , (short)700 );
    assertEquals( ( (PrimitiveObject)( column.get(8).getRow() ) ).getShort() , (short)800 );
    assertEquals( ( (PrimitiveObject)( column.get(9).getRow() ) ).getShort() , (short)900 );
    assertEquals( ( (PrimitiveObject)( column.get(10).getRow() ) ).getShort() , (short)0 );
  }

  public void assertNotNullColumn(
      final String targetClassName, final int[] repetitions, final int loadSize)
      throws IOException {
    IColumn column = createNotNullColumn(targetClassName, repetitions, loadSize);
    assertEquals(loadSize, column.size());
    int offset = 0;
    for (int i = 0; i < repetitions.length; i++) {
      Short expected = notNullColumnValue(i);
      for (int j = 0; j < repetitions[i]; j++) {
        if (expected == null) {
          assertEquals(ColumnType.NULL, column.get(offset).getType());
        } else {
          assertEquals(expected.shortValue(), ((PrimitiveObject) (column.get(offset).getRow())).getShort());
        }
        offset++;
      }
    }
  }

  @ParameterizedTest
  @MethodSource("D_shortColumnBinaryMaker")
  public void T_loadNotNullColumn_withAllLoadIndex(final String targetClassName)
      throws IOException {
    int[] repetitions = new int[] {1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1};
    assertNotNullColumn(targetClassName, repetitions, getLoadSize(repetitions));
  }

  @ParameterizedTest
  @MethodSource("D_shortColumnBinaryMaker")
  public void T_loadNotNullColumn_withOutOfBoundsLoadIndex(final String targetClassName)
      throws IOException {
    int[] repetitions = new int[] {1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1};
    assertNotNullColumn(targetClassName, repetitions, getLoadSize(repetitions));
  }

  @ParameterizedTest
  @MethodSource("D_shortColumnBinaryMaker")
  public void T_loadNotNullColumn_withHead5LoadIndex(final String targetClassName)
      throws IOException {
    int[] repetitions = new int[] {1, 1, 1, 1, 1};
    assertNotNullColumn(targetClassName, repetitions, getLoadSize(repetitions));
  }

  @ParameterizedTest
  @MethodSource("D_shortColumnBinaryMaker")
  public void T_loadNotNullColumn_withTail5LoadIndex(final String targetClassName)
      throws IOException {
    int[] repetitions = new int[] {0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1};
    assertNotNullColumn(targetClassName, repetitions, getLoadSize(repetitions));
  }

  @ParameterizedTest
  @MethodSource("D_shortColumnBinaryMaker")
  public void T_loadNotNullColumn_withOddNumberLoadIndex(final String targetClassName)
      throws IOException {
    int[] repetitions = new int[] {0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0};
    assertNotNullColumn(targetClassName, repetitions, getLoadSize(repetitions));
  }

  @ParameterizedTest
  @MethodSource("D_shortColumnBinaryMaker")
  public void T_loadNotNullColumn_withAllLoadIndexAndExpand(final String targetClassName)
      throws IOException {
    int[] repetitions = new int[] {2, 1, 2, 3, 1, 1, 2, 1, 1, 1, 3};
    assertNotNullColumn(targetClassName, repetitions, getLoadSize(repetitions));
  }

  @ParameterizedTest
  @MethodSource("D_shortColumnBinaryMaker")
  public void T_loadNotNullColumn_withOutOfBoundsLoadIndexAndExpand(final String targetClassName)
      throws IOException {
    int[] repetitions = new int[] {2, 1, 2, 3, 1, 1, 2, 1, 1, 1, 3, 0, 2, 1, 0, 1};
    assertNotNullColumn(targetClassName, repetitions, getLoadSize(repetitions));
  }

  @ParameterizedTest
  @MethodSource("D_shortColumnBinaryMaker")
  public void T_loadNotNullColumn_withHead5LoadIndexAndExpand(final String targetClassName)
      throws IOException {
    int[] repetitions = new int[] {2, 1, 2, 3, 1};
    assertNotNullColumn(targetClassName, repetitions, getLoadSize(repetitions));
  }

  @ParameterizedTest
  @MethodSource("D_shortColumnBinaryMaker")
  public void T_loadNotNullColumn_withTail5LoadIndexAndExpand(final String targetClassName)
      throws IOException {
    int[] repetitions = new int[] {0, 0, 0, 0, 0, 0, 2, 1, 1, 1, 3};
    assertNotNullColumn(targetClassName, repetitions, getLoadSize(repetitions));
  }

  @ParameterizedTest
  @MethodSource("D_shortColumnBinaryMaker")
  public void T_loadNotNullColumn_withOddNumberLoadIndexAndExpand(final String targetClassName)
      throws IOException {
    int[] repetitions = new int[] {0, 2, 0, 1, 0, 1, 0, 2, 0, 3, 0};
    assertNotNullColumn(targetClassName, repetitions, getLoadSize(repetitions));
  }

  @ParameterizedTest
  @MethodSource( "data1" )
  public void T_null_1( final String targetClassName ) throws IOException{
    IColumn column = createNullColumn( targetClassName );
    assertNull( column.get(0).getRow() );
    assertNull( column.get(1).getRow() );
  }

  @ParameterizedTest
  @MethodSource("D_shortColumnBinaryMaker")
  public void T_loadNullColumn_withAllLoadIndexAndExpand(final String targetClassName)
      throws IOException {
    int[] repetitions = new int[] {2, 1, 3, 1, 1, 1, 2, 3, 2};
    IColumn column = createNullColumn(targetClassName, repetitions, getLoadSize(repetitions));
    // TODO: NullColumn returns 0.
    if (column.getColumnType() == ColumnType.NULL) {
      assertEquals(0, column.size());
    } else {
      assertEquals(getLoadSize(repetitions), column.size());
    }
    int offset = 0;
    for (int repetition : repetitions) {
      for (int j = 0; j < repetition; j++) {
        assertEquals(ColumnType.NULL, column.get(offset).getType());
        offset++;
      }
    }
  }

  @ParameterizedTest
  @MethodSource( "data1" )
  public void T_hasNull_1( final String targetClassName ) throws IOException{
    IColumn column = createHasNullColumn( targetClassName );
    assertEquals( ( (PrimitiveObject)( column.get(0).getRow() ) ).getShort() , (short)0 );
    assertNull( column.get(1).getRow() );
    assertNull( column.get(2).getRow() );
    assertNull( column.get(3).getRow() );
    assertEquals( ( (PrimitiveObject)( column.get(4).getRow() ) ).getShort() , (short)4 );
    assertNull( column.get(5).getRow() );
    assertNull( column.get(6).getRow() );
    assertNull( column.get(7).getRow() );
    assertEquals( ( (PrimitiveObject)( column.get(8).getRow() ) ).getShort() , (short)8 );
  }

  public void assertHasNullColumn(
      final String targetClassName, final int[] repetitions, final int loadSize)
      throws IOException {
    IColumn column = createHasNullColumn(targetClassName, repetitions, loadSize);
    assertEquals(loadSize, column.size());
    int offset = 0;
    for (int i = 0; i < repetitions.length; i++) {
      Short expected = hasNullColumnValue(i);
      for (int j = 0; j < repetitions[i]; j++) {
        if (expected == null) {
          assertEquals(ColumnType.NULL, column.get(offset).getType());
        } else {
          assertEquals(expected.shortValue(), ((PrimitiveObject) (column.get(offset).getRow())).getShort());
        }
        offset++;
      }
    }
  }

  @ParameterizedTest
  @MethodSource("D_shortColumnBinaryMaker")
  public void T_loadHasNullColumn_withAllLoadIndex(final String targetClassName)
      throws IOException {
    int[] repetitions = new int[] {1, 1, 1, 1, 1, 1, 1, 1, 1};
    assertHasNullColumn(targetClassName, repetitions, getLoadSize(repetitions));
  }

  @ParameterizedTest
  @MethodSource("D_shortColumnBinaryMaker")
  public void T_loadHasNullColumn_withOutOfBoundsLoadIndex(final String targetClassName)
      throws IOException {
    int[] repetitions = new int[] {1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1};
    assertHasNullColumn(targetClassName, repetitions, getLoadSize(repetitions));
  }

  @ParameterizedTest
  @MethodSource("D_shortColumnBinaryMaker")
  public void T_loadHasNullColumn_withHead5LoadIndex(final String targetClassName)
      throws IOException {
    int[] repetitions = new int[] {1, 1, 1, 1, 1};
    assertHasNullColumn(targetClassName, repetitions, getLoadSize(repetitions));
  }

  @ParameterizedTest
  @MethodSource("D_shortColumnBinaryMaker")
  public void T_loadHasNullColumn_withTail5LoadIndex(final String targetClassName)
      throws IOException {
    int[] repetitions = new int[] {0, 0, 0, 0, 1, 1, 1, 1, 1};
    assertHasNullColumn(targetClassName, repetitions, getLoadSize(repetitions));
  }

  @ParameterizedTest
  @MethodSource("D_shortColumnBinaryMaker")
  public void T_loadHasNullColumn_withOddNumberLoadIndex(final String targetClassName)
      throws IOException {
    int[] repetitions = new int[] {0, 1, 0, 1, 0, 1, 0, 1, 0};
    assertHasNullColumn(targetClassName, repetitions, getLoadSize(repetitions));
  }

  @ParameterizedTest
  @MethodSource("D_shortColumnBinaryMaker")
  public void T_loadHasNullColumn_withAllLoadIndexAndExpand(final String targetClassName)
      throws IOException {
    int[] repetitions = new int[] {2, 1, 3, 1, 1, 1, 2, 1, 3};
    assertHasNullColumn(targetClassName, repetitions, getLoadSize(repetitions));
  }

  @ParameterizedTest
  @MethodSource("D_shortColumnBinaryMaker")
  public void T_loadHasNullColumn_withOutOfBoundsLoadIndexAndExpand(final String targetClassName)
      throws IOException {
    int[] repetitions = new int[] {2, 1, 2, 3, 1, 1, 2, 1, 1, 1, 3, 0, 2, 1, 0, 1};
    assertHasNullColumn(targetClassName, repetitions, getLoadSize(repetitions));
  }

  @ParameterizedTest
  @MethodSource("D_shortColumnBinaryMaker")
  public void T_loadHasNullColumn_withHead5LoadIndexAndExpand(final String targetClassName)
      throws IOException {
    int[] repetitions = new int[] {2, 1, 3, 1, 4};
    assertHasNullColumn(targetClassName, repetitions, getLoadSize(repetitions));
  }

  @ParameterizedTest
  @MethodSource("D_shortColumnBinaryMaker")
  public void T_loadHasNullColumn_withTail5LoadIndexAndExpand(final String targetClassName)
      throws IOException {
    int[] repetitions = new int[] {0, 0, 0, 0, 2, 1, 3, 1, 1};
    assertHasNullColumn(targetClassName, repetitions, getLoadSize(repetitions));
  }

  @ParameterizedTest
  @MethodSource("D_shortColumnBinaryMaker")
  public void T_loadHasNullColumn_withOddNumberLoadIndexAndExpand(final String targetClassName)
      throws IOException {
    int[] repetitions = new int[] {0, 2, 0, 1, 0, 1, 0, 3, 0};
    assertHasNullColumn(targetClassName, repetitions, getLoadSize(repetitions));
  }

  @ParameterizedTest
  @MethodSource( "data1" )
  public void T_lastCell_1( final String targetClassName ) throws IOException{
    IColumn column = createLastCellColumn( targetClassName );
    for( int i = 0 ; i < 10000 ; i++ ){
      assertNull( column.get(i).getRow() );
    }
    assertEquals( ( (PrimitiveObject)( column.get(10000).getRow() ) ).getShort() , Short.MAX_VALUE );
  }

  public void assertLastCellColumn(
      final String targetClassName, final int[] repetitions, final int loadSize)
      throws IOException {
    IColumn column = createLastCellColumn(targetClassName, repetitions, loadSize);
    assertEquals(loadSize, column.size());
    int offset = 0;
    for (int i = 0; i < repetitions.length; i++) {
      Short expected = lastCellColumnValue(i);
      for (int j = 0; j < repetitions[i]; j++) {
        if (expected == null) {
          assertEquals(ColumnType.NULL, column.get(offset).getType());
        } else {
          assertEquals(expected.shortValue(), ((PrimitiveObject) (column.get(offset).getRow())).getShort());
        }
        offset++;
      }
    }
  }

  @ParameterizedTest
  @MethodSource("D_shortColumnBinaryMaker")
  public void T_loadLastCellColumn_withAllLoadIndex(final String targetClassName)
      throws IOException {
    int lastIndex = 10000;
    int[] repetitions = new int[lastIndex + 1];
    Arrays.fill(repetitions, 1);
    assertLastCellColumn(targetClassName, repetitions, getLoadSize(repetitions));
  }

  @ParameterizedTest
  @MethodSource("D_shortColumnBinaryMaker")
  public void T_loadLastCellColumn_withOutOfBoundsLoadIndex(final String targetClassName)
      throws IOException {
    int lastIndex = 10001;
    int[] repetitions = new int[lastIndex + 1];
    Arrays.fill(repetitions, 1);
    assertLastCellColumn(targetClassName, repetitions, getLoadSize(repetitions));
  }

  @ParameterizedTest
  @MethodSource("D_shortColumnBinaryMaker")
  public void T_loadLastCellColumn_withHead5LoadIndex(final String targetClassName)
      throws IOException {
    int[] repetitions = new int[] {1, 1, 1, 1, 1};
    assertLastCellColumn(targetClassName, repetitions, getLoadSize(repetitions));
  }

  @ParameterizedTest
  @MethodSource("D_shortColumnBinaryMaker")
  public void T_loadLastCellColumn_withTail5LoadIndex(final String targetClassName)
      throws IOException {
    int lastIndex = 10000;
    int[] repetitions = new int[lastIndex + 1];
    for (int i = 0; i < repetitions.length; i++) {
      repetitions[i] = ((lastIndex - i) < 5) ? 1 : 0;
    }
    assertLastCellColumn(targetClassName, repetitions, getLoadSize(repetitions));
  }

  @ParameterizedTest
  @MethodSource("D_shortColumnBinaryMaker")
  public void T_loadLastCellColumn_withOddNumberLoadIndex(final String targetClassName)
      throws IOException {
    int lastIndex = 10000;
    int[] repetitions = new int[lastIndex + 1];
    for (int i = 0; i < repetitions.length; i++) {
      repetitions[i] = i % 2;
    }
    assertLastCellColumn(targetClassName, repetitions, getLoadSize(repetitions));
  }

  @ParameterizedTest
  @MethodSource("D_shortColumnBinaryMaker")
  public void T_loadLastCellColumn_withAllLoadIndexAndExpand(final String targetClassName)
      throws IOException {
    int lastIndex = 10000;
    int[] repetitions = new int[lastIndex + 1];
    for (int i = 0; i < repetitions.length; i++) {
      repetitions[i] = 3 - (i % 3);
    }
    assertLastCellColumn(targetClassName, repetitions, getLoadSize(repetitions));
  }

  @ParameterizedTest
  @MethodSource("D_shortColumnBinaryMaker")
  public void T_loadLastCellColumn_withOutOfBoundsLoadIndexAndExpand(final String targetClassName)
      throws IOException {
    int lastIndex = 10003;
    int[] repetitions = new int[lastIndex + 1];
    for (int i = 0; i < repetitions.length; i++) {
      repetitions[i] = 3 - (i % 3);
    }
    assertLastCellColumn(targetClassName, repetitions, getLoadSize(repetitions));
  }

  @ParameterizedTest
  @MethodSource("D_shortColumnBinaryMaker")
  public void T_loadLastCellColumn_withHead5LoadIndexAndExpand(final String targetClassName)
      throws IOException {
    int[] repetitions = new int[] {2, 1, 1, 2, 3};
    assertLastCellColumn(targetClassName, repetitions, getLoadSize(repetitions));
  }

  @ParameterizedTest
  @MethodSource("D_shortColumnBinaryMaker")
  public void T_loadLastCellColumn_withTail5LoadIndexAndExpand(final String targetClassName)
      throws IOException {
    final Map<Integer, Integer> values =
        new HashMap<Integer, Integer>() {
          {
            put(9996, 2);
            put(9997, 1);
            put(9998, 1);
            put(9999, 2);
            put(10000, 3);
          }
        };
    int lastIndex = 10000;
    int[] repetitions = new int[lastIndex + 1];
    for (int i = 0; i < repetitions.length; i++) {
      repetitions[i] = values.getOrDefault(i, 0);
    }
    assertLastCellColumn(targetClassName, repetitions, getLoadSize(repetitions));
  }

  @ParameterizedTest
  @MethodSource("D_shortColumnBinaryMaker")
  public void T_loadLastCellColumn_withOddNumberLoadIndexAndExpand(final String targetClassName)
      throws IOException {
    int lastIndex = 10000;
    int[] repetitions = new int[lastIndex + 1];
    for (int i = 0; i < repetitions.length; i++) {
      int odd = i % 2;
      repetitions[i] = (odd == 1) ? 3 - (i % 3) : 0;
    }
    assertLastCellColumn(targetClassName, repetitions, getLoadSize(repetitions));
  }

  public void assertTestColumn(final String targetClassName, final short[] valueArray)
          throws IOException {
    assertTestColumn(targetClassName, valueArray, null, null);
  }

  public void assertTestColumn(
      final String targetClassName,
      final short[] valueArray,
      final int[] repetitions,
      final Integer loadSize)
      throws IOException {
    IColumn column = createTestColumn(targetClassName, valueArray, repetitions, loadSize);
    if (repetitions == null) {
      assertEquals(valueArray.length, column.size());
      for (int i = 0; i < valueArray.length; i++) {
        assertEquals(valueArray[i], ((PrimitiveObject) column.get(i).getRow()).getShort());
      }
    } else {
      assertEquals(loadSize.intValue(), column.size());
      int offset = 0;
      for (int i = 0; i < repetitions.length; i++) {
        for (int j = 0; j < repetitions[i]; j++) {
          assertEquals(valueArray[i], ((PrimitiveObject) column.get(offset).getRow()).getShort());
          offset++;
        }
      }
    }
  }

  public int[] testColumnRepetitions(final short[] valueArray) {
    final int[] repetitions = new int[valueArray.length];
    for (int i = 0; i < valueArray.length; i++) {
      repetitions[i] = 2;
    }
    return repetitions;
  }

  public short[] bit0() {
    short[] valueArray = new short[10];
    for (int i = 0; i < valueArray.length; i++) {
      valueArray[i] = (short) 0;
    }
    return valueArray;
  }

  @ParameterizedTest
  @MethodSource("data1")
  public void T_encodeAndDecode_equalsSetValue_withIntBit0(final String targetClassName)
      throws IOException {
    short[] valueArray = bit0();
    assertTestColumn(targetClassName, valueArray);
  }

  @ParameterizedTest
  @MethodSource("D_shortColumnBinaryMaker")
  public void T_loadTestColumn_withAllLoadIndexAndExpand_bit0(final String targetClassName)
      throws IOException {
    final short[] valueArray = bit0();
    final int[] repetitions = testColumnRepetitions(valueArray);
    assertTestColumn(targetClassName, valueArray, repetitions, getLoadSize(repetitions));
  }

  public short[] int1() {
    short[] valueArray =
        new short[] {
          (short) 0, (short) 0, (short) 1, (short) 1, (short) 0, (short) 0, (short) 1, (short) 1,
          (short) 0, (short) 0
        };
    return valueArray;
  }

  @ParameterizedTest
  @MethodSource("data1")
  public void T_encodeAndDecode_equalsSetValue_withInt1(final String targetClassName)
      throws IOException {
    short[] valueArray = int1();
    assertTestColumn(targetClassName, valueArray);
  }

  @ParameterizedTest
  @MethodSource("D_shortColumnBinaryMaker")
  public void T_loadTestColumn_withAllLoadIndexAndExpand_int1(final String targetClassName)
      throws IOException {
    final short[] valueArray = int1();
    final int[] repetitions = testColumnRepetitions(valueArray);
    assertTestColumn(targetClassName, valueArray, repetitions, getLoadSize(repetitions));
  }

  public short[] int2() {
    short[] valueArray =
        new short[] {
          (short) 0, (short) 0, (short) 1, (short) 1, (short) 2, (short) 2, (short) 3, (short) 3,
          (short) 0, (short) 0
        };
    return valueArray;
  }

  @ParameterizedTest
  @MethodSource("data1")
  public void T_encodeAndDecode_equalsSetValue_withInt2(final String targetClassName)
      throws IOException {
    short[] valueArray = int2();
    assertTestColumn(targetClassName, valueArray);
  }

  @ParameterizedTest
  @MethodSource("D_shortColumnBinaryMaker")
  public void T_loadTestColumn_withAllLoadIndexAndExpand_int2(final String targetClassName)
      throws IOException {
    final short[] valueArray = int2();
    final int[] repetitions = testColumnRepetitions(valueArray);
    assertTestColumn(targetClassName, valueArray, repetitions, getLoadSize(repetitions));
  }

  public short[] int4() {
    short[] valueArray =
        new short[] {
          (short) 0,
          (short) 0,
          (short) 8,
          (short) 8,
          (short) 15,
          (short) 15,
          (short) 1,
          (short) 2,
          (short) 3,
          (short) 4
        };
    return valueArray;
  }

  @ParameterizedTest
  @MethodSource("data1")
  public void T_encodeAndDecode_equalsSetValue_withInt4(final String targetClassName)
      throws IOException {
    short[] valueArray = int4();
    assertTestColumn(targetClassName, valueArray);
  }

  @ParameterizedTest
  @MethodSource("D_shortColumnBinaryMaker")
  public void T_loadTestColumn_withAllLoadIndexAndExpand_int4(final String targetClassName)
      throws IOException {
    final short[] valueArray = int4();
    final int[] repetitions = testColumnRepetitions(valueArray);
    assertTestColumn(targetClassName, valueArray, repetitions, getLoadSize(repetitions));
  }

  public short[] int8() {
    short[] valueArray =
        new short[] {
          (short) Byte.MAX_VALUE,
          (short) Byte.MIN_VALUE,
          (short) 0,
          (short) 0,
          (short) 64,
          (short) -64,
          (short) 32,
          (short) -32,
          (short) 16,
          (short) -16
        };
    return valueArray;
  }

  @ParameterizedTest
  @MethodSource("data1")
  public void T_encodeAndDecode_equalsSetValue_withInt8(final String targetClassName)
      throws IOException {
    short[] valueArray = int8();
    assertTestColumn(targetClassName, valueArray);
  }

  @ParameterizedTest
  @MethodSource("D_shortColumnBinaryMaker")
  public void T_loadTestColumn_withAllLoadIndexAndExpand_int8(final String targetClassName)
      throws IOException {
    final short[] valueArray = int8();
    final int[] repetitions = testColumnRepetitions(valueArray);
    assertTestColumn(targetClassName, valueArray, repetitions, getLoadSize(repetitions));
  }

  public short[] int16() {
    short[] valueArray =
        new short[] {
          Short.MAX_VALUE,
          Short.MIN_VALUE,
          (short) Byte.MAX_VALUE,
          (short) Byte.MIN_VALUE,
          (short) 1,
          (short) 1,
          (short) -1,
          (short) -1,
          (short) 2,
          (short) 2
        };
    return valueArray;
  }

  @ParameterizedTest
  @MethodSource("data1")
  public void T_encodeAndDecode_equalsSetValue_withInt16(final String targetClassName)
      throws IOException {
    short[] valueArray = int16();
    assertTestColumn(targetClassName, valueArray);
  }

  @ParameterizedTest
  @MethodSource("D_shortColumnBinaryMaker")
  public void T_loadTestColumn_withAllLoadIndexAndExpand_int16(final String targetClassName) throws IOException {
    final short[] valueArray = int16();
    final int[] repetitions = testColumnRepetitions(valueArray);
    assertTestColumn(targetClassName, valueArray, repetitions, getLoadSize(repetitions));
  }

  @ParameterizedTest
  @MethodSource("D_shortColumnBinaryMaker")
  public void T_load_exception_withLessThan0LoadIndex(final String targetClassName) {
    int[] repetitions = new int[] {-1, 0, 1, 2};
    assertThrows(
        IOException.class,
        () -> {
          IColumn column =
              createNotNullColumn(targetClassName, repetitions, getLoadSize(repetitions));
        });
  }

  @ParameterizedTest
  @MethodSource("D_shortColumnBinaryMaker")
  public void T_load_exception_withLessThan0LoadIndex_withOutOfBoundsLoadIndexAndExpand(
      final String targetClassName) {
    int[] repetitions = new int[] {1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, -1};
    assertThrows(
        IOException.class,
        () -> {
          IColumn column =
              createNotNullColumn(targetClassName, repetitions, getLoadSize(repetitions));
        });
  }

  @ParameterizedTest
  @MethodSource("D_shortColumnBinaryMaker")
  public void T_loadFixedColumn_exception_withLessThan0LoadIndex(
      final String targetClassName) {
    int[] repetitions = new int[] {1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, -1};
    assertThrows(
        IOException.class,
        () -> {
          IColumn column =
              createFixedColumn(targetClassName, repetitions, getLoadSize(repetitions));
        });
  }
}
