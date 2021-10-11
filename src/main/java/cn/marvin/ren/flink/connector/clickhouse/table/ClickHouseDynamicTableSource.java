package cn.marvin.ren.flink.connector.clickhouse.table;

import cn.marvin.ren.flink.connector.clickhouse.dialect.ClickHouseDialect;
import org.apache.flink.connector.jdbc.internal.options.JdbcOptions;
import org.apache.flink.connector.jdbc.table.JdbcRowDataInputFormat;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.InputFormatProvider;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.types.logical.RowType;

public class ClickHouseDynamicTableSource implements ScanTableSource {

    private final JdbcOptions options;
    private final ResolvedSchema resolvedSchema;

    public ClickHouseDynamicTableSource(JdbcOptions options, ResolvedSchema resolvedSchema) {
        this.options = options;
        this.resolvedSchema = resolvedSchema;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext scanContext) {

        ClickHouseDialect dialect = (ClickHouseDialect)options.getDialect();
        String query = dialect.getSelectFromStatement(options.getTableName(), resolvedSchema.getColumnNames().stream().toArray(String[]::new), new String[0]);

        RowType rowType = (RowType)resolvedSchema.toSourceRowDataType().getLogicalType();
        JdbcRowDataInputFormat build = JdbcRowDataInputFormat.builder()
                .setDrivername(options.getDialect().defaultDriverName().get())
                .setDBUrl(options.getDbURL())
                .setUsername(options.getUsername().orElse(null))
                .setPassword(options.getPassword().orElse(null))
                .setQuery(query)
                .setRowConverter(dialect.getRowConverter(rowType))
                .setRowDataTypeInfo(scanContext.createTypeInformation(resolvedSchema.toSourceRowDataType()))
                .build();
        return InputFormatProvider.of(build);
    }

    @Override
    public DynamicTableSource copy() {
        return new ClickHouseDynamicTableSource(options, resolvedSchema);
    }

    @Override
    public String asSummaryString() {
        return "ClickHouse Table Source";
    }



}
