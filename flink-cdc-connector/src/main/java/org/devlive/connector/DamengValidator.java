package org.devlive.connector;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.flink.cdc.debezium.Validator;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.ValidationException;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

@SuppressFBWarnings(value = {"EI_EXPOSE_REP2"})
public class DamengValidator
        implements Validator
{
    private final Properties properties;
    private static final List<Integer> SUPPORT_VERSIONS = Collections.singletonList(8);

    public DamengValidator(Properties properties)
    {
        this.properties = properties;
    }

    @Override
    public void validate()
    {
        try (Connection connection = openConnection(properties)) {
            DatabaseMetaData metaData = connection.getMetaData();
            if (!SUPPORT_VERSIONS.contains(metaData.getDatabaseMajorVersion())) {
                throw new ValidationException(
                        String.format(
                                "Currently Flink Dameng CDC connector only supports Dameng "
                                        + "whose version is either %s but actual is %d.%d.",
                                SUPPORT_VERSIONS,
                                metaData.getDatabaseMajorVersion(),
                                metaData.getDatabaseMinorVersion()));
            }
        }
        catch (SQLException ex) {
            throw new TableException(
                    "Unexpected error while connecting to Oracle and validating", ex);
        }
    }

    public static Connection openConnection(Properties properties)
            throws SQLException
    {
        DriverManager.registerDriver(new dm.jdbc.driver.DmDriver());
        String url = JdbcUrlUtils.getConnectionUrlWithSid(properties);
        String userName = properties.getProperty("database.user");
        String userpwd = properties.getProperty("database.password");
        return DriverManager.getConnection(url, userName, userpwd);
    }
}
