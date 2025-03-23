package org.devlive.connector;

import java.util.Properties;

public class JdbcUrlUtils
{
    private JdbcUrlUtils() {}

    public static String getConnectionUrlWithSid(Properties properties)
    {
        String url;
        if (properties.containsKey("database.url")) {
            url = properties.getProperty("database.url");
        }
        else {
            String hostname = properties.getProperty("database.hostname");
            String port = properties.getProperty("database.port");
            String dbname = properties.getProperty("database.dbname");
            url = "jdbc:dm://" + hostname + ":" + port + "/" + dbname;
        }
        return url;
    }
}
