package org.pathirage.calcite.tutorial.utils;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.DataContext;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.TableFactory;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.Map;

public class ShippingTableFactory implements TableFactory<Table> {
  @Override
  public Table create(SchemaPlus schema, String name, Map<String, Object> operand, RelDataType rowType) {
    final Object[][] rows = {
        {1, 5, "shanghai"},
        {2, 3, "newyork"},
        {3, 4, "tokyo"},
        {4, 2, "hongkong"}
    };
    return new ShippingTable(ImmutableList.copyOf(rows));
  }

  public static class ShippingTable implements ScannableTable {
    protected final RelProtoDataType protoRowType = new RelProtoDataType() {
      public RelDataType apply(RelDataTypeFactory a0) {
        return a0.builder()
            .add("id", SqlTypeName.INTEGER)
            .add("orderId", SqlTypeName.INTEGER)
            .add("destination", SqlTypeName.VARCHAR, 10)
            .build();
      }
    };

    private final ImmutableList<Object[]> rows;

    public ShippingTable(ImmutableList<Object[]> rows) {
      this.rows = rows;
    }

    public Enumerable<Object[]> scan(DataContext root) {
      return Linq4j.asEnumerable(rows);
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      return protoRowType.apply(typeFactory);
    }

    @Override
    public Statistic getStatistic() {
      return Statistics.UNKNOWN;
    }

    @Override
    public Schema.TableType getJdbcTableType() {
      return Schema.TableType.TABLE;
    }

    @Override
    public boolean isRolledUp(String column) {
      return false;
    }

    @Override
    public boolean rolledUpColumnValidInsideAgg(String column, SqlCall call, SqlNode parent, CalciteConnectionConfig config) {
      return false;
    }
  }
}
