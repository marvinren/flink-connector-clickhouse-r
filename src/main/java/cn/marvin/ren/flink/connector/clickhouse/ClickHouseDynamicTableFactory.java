package cn.marvin.ren.flink.connector.clickhouse;

import cn.marvin.ren.flink.connector.clickhouse.dialect.ClickHouseDialect;
import cn.marvin.ren.flink.connector.clickhouse.table.ClickHouseDynamicTableSource;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.jdbc.internal.options.JdbcOptions;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;

import java.util.HashSet;
import java.util.Set;

public class ClickHouseDynamicTableFactory implements DynamicTableSourceFactory {

    // connector name
    public static final String IDENTIFIER = "clickhouse-r";

    // clickhouse driver
    private static final String DRIVER_NAME = "ru.yandex.clickhouse.ClickHouseDriver";

    // connector parameters
    public static final ConfigOption<String> URL = ConfigOptions.key("url")
            .stringType()
            .noDefaultValue()
            .withDeprecatedKeys("the ClickHouse url in format `clickhouse://<host>:<port>`.");
    public static final ConfigOption<String> USERNAME = ConfigOptions.key("username")
            .stringType()
            .noDefaultValue()
            .withDescription("the ClickHouse username.");

    public static final ConfigOption<String> PASSWORD = ConfigOptions.key("password")
            .stringType()
            .noDefaultValue()
            .withDescription("the ClickHouse password.");

    public static final ConfigOption<String> DATABASE_NAME = ConfigOptions.key("database-name")
            .stringType()
            .defaultValue("default")
            .withDescription("the ClickHouse database name. Default to `default`.");

    public static final ConfigOption<String> TABLE_NAME = ConfigOptions.key("table-name")
            .stringType()
            .noDefaultValue()
            .withDescription("the ClickHouse table name.");


    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {

        // use the flink's validate logic(helper)
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);

        final ReadableConfig config = helper.getOptions();

        // validate all options
        helper.validate();

        // get jdbc options
        JdbcOptions jdbcOptions = getJdbcOptions(config);

        // get resovled table schema
        ResolvedSchema resolvedSchema = context.getCatalogTable().getResolvedSchema();

        // create the table source
        return new ClickHouseDynamicTableSource(jdbcOptions, resolvedSchema);

    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> requiredOptions = new HashSet<>();
        requiredOptions.add(URL);
        requiredOptions.add(TABLE_NAME);
        requiredOptions.add(USERNAME);
        requiredOptions.add(PASSWORD);
        return requiredOptions;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> optionalOptions = new HashSet<>();
        return optionalOptions;
    }

    private JdbcOptions getJdbcOptions(ReadableConfig readableConfig) {
        final String url = readableConfig.get(URL);
        final JdbcOptions.Builder builder = JdbcOptions.builder()
                .setDriverName(DRIVER_NAME)
                .setDBUrl(url)
                .setTableName(readableConfig.get(TABLE_NAME))
                .setDialect(new ClickHouseDialect());

        readableConfig.getOptional(USERNAME).ifPresent(builder::setUsername);
        readableConfig.getOptional(PASSWORD).ifPresent(builder::setPassword);
        return builder.build();
    }
}
