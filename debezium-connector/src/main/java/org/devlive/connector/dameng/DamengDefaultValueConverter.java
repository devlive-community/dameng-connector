/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.devlive.connector.dameng;

import io.debezium.annotation.Immutable;
import io.debezium.annotation.ThreadSafe;
import io.debezium.relational.Column;
import io.debezium.relational.DefaultValueConverter;
import io.debezium.relational.ValueConverter;
import io.debezium.util.Strings;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Types;
import java.util.Optional;

/**
 * Converts the string default value of a column (as reported by the catalog or parsed from DDL)
 * into a Java object recognized by the {@link DamengValueConverters value converters}.
 * <p>
 * Without such a converter Debezium falls back to {@link DefaultValueConverter#passthrough()},
 * which hands the raw expression string straight to {@code SchemaBuilder.defaultValue(...)}. For any
 * non-string column that leads to a Kafka Connect validation error, e.g. a {@code TIMESTAMP} column
 * with {@code DEFAULT SYSDATE} fails with
 * {@code Invalid Java object for schema "io.debezium.time.Timestamp" with type INT64: class java.lang.String}.
 * <p>
 * Non-literal expressions (function calls such as {@code SYSDATE}) cannot be evaluated statically, so
 * for those we simply drop the schema default rather than fail the connector.
 *
 * @see io.debezium.connector.oracle.OracleDefaultValueConverter
 */
@ThreadSafe
@Immutable
public class DamengDefaultValueConverter
        implements DefaultValueConverter
{
    private static final Logger LOGGER = LoggerFactory.getLogger(DamengDefaultValueConverter.class);

    private final DamengValueConverters valueConverters;

    public DamengDefaultValueConverter(DamengValueConverters valueConverters)
    {
        this.valueConverters = valueConverters;
    }

    @Override
    public Optional<Object> parseDefaultValue(Column column, String defaultValueExpression)
    {
        if (defaultValueExpression == null) {
            return Optional.empty();
        }

        final String value = defaultValueExpression.trim();
        if (value.isEmpty() || "NULL".equalsIgnoreCase(value)) {
            return Optional.empty();
        }

        try {
            final int dataType = column.jdbcType();

            // Non-literal temporal expressions (SYSDATE, CURRENT_TIMESTAMP, ...) cannot be evaluated
            // statically. Dropping the default is preferable to crashing the connector.
            if (isTemporalType(dataType) && !isParsableTemporalLiteral(value)) {
                LOGGER.warn("Cannot evaluate default value expression '{}' for temporal column '{}'; the schema default will be omitted.",
                        value, column.name());
                return Optional.empty();
            }

            final Object rawDefaultValue = unquoteIfNeeded(column, value);
            final Object convertedDefaultValue = convertDefaultValue(rawDefaultValue, column);
            if (convertedDefaultValue instanceof Struct) {
                // A Struct cannot be used as a Kafka Connect schema default value (see KAFKA-12694).
                LOGGER.warn("Struct can't be used as default value for column '{}', will use null instead.", column.name());
                return Optional.empty();
            }
            return Optional.ofNullable(convertedDefaultValue);
        }
        catch (Exception e) {
            LOGGER.warn("Cannot parse column default value '{}' to type '{}' for column '{}'. Expression evaluation is not supported; the schema default will be omitted.",
                    value, column.jdbcType(), column.name());
            LOGGER.debug("Parsing failed due to error", e);
            return Optional.empty();
        }
    }

    /**
     * Converts the raw default value into the Java type expected by the column's schema using the
     * shared {@link DamengValueConverters}.
     */
    private Object convertDefaultValue(Object defaultValue, Column column)
    {
        if (valueConverters != null && defaultValue != null) {
            final SchemaBuilder schemaBuilder = valueConverters.schemaBuilder(column);
            if (schemaBuilder != null) {
                final Schema schema = schemaBuilder.build();
                // The field index is never used when converting a default value, so any value is fine.
                final Field field = new Field(column.name(), -1, schema);
                final ValueConverter valueConverter = valueConverters.converter(column, field);

                Object result = valueConverter.convert(defaultValue);
                if (result instanceof BigDecimal && column.scale().isPresent()
                        && column.scale().get() > ((BigDecimal) result).scale()) {
                    // Increasing the scale is purely cosmetic; align it with the column definition.
                    result = ((BigDecimal) result).setScale(column.scale().get(), RoundingMode.HALF_EVEN);
                }
                return result;
            }
        }
        return defaultValue;
    }

    private Object unquoteIfNeeded(Column column, String value)
    {
        switch (column.jdbcType()) {
            case Types.CHAR:
            case Types.NCHAR:
                return Strings.pad(unquote(value), column.length(), ' ');
            case Types.VARCHAR:
            case Types.NVARCHAR:
            case Types.CLOB:
                return unquote(value);
            default:
                return value;
        }
    }

    private static boolean isTemporalType(int jdbcType)
    {
        return jdbcType == Types.DATE
                || jdbcType == Types.TIME
                || jdbcType == Types.TIMESTAMP
                || jdbcType == Types.TIME_WITH_TIMEZONE
                || jdbcType == Types.TIMESTAMP_WITH_TIMEZONE;
    }

    /**
     * Only default expressions that {@link DamengValueConverters} is able to resolve are considered
     * parsable. Everything else (typically function calls like {@code SYSDATE}) is treated as a
     * non-evaluable expression and the default is dropped.
     */
    private static boolean isParsableTemporalLiteral(String value)
    {
        final String upper = value.toUpperCase();
        return upper.startsWith("TO_TIMESTAMP")
                || upper.startsWith("TO_DATE")
                || upper.startsWith("TIMESTAMP'")
                || upper.startsWith("TIMESTAMP '");
    }

    private static String unquote(String value)
    {
        if (value.startsWith("('") && value.endsWith("')")) {
            return value.substring(2, value.length() - 2);
        }
        if (value.startsWith("'") && value.endsWith("'")) {
            return value.substring(1, value.length() - 1);
        }
        return value;
    }
}
