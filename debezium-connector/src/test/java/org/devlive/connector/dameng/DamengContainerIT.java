/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.devlive.connector.dameng;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeTrue;

/**
 * Smoke integration test that boots a real Dameng database via Testcontainers and verifies basic
 * JDBC connectivity and DDL/DML. It is the foundation for regression tests that need a live database
 * (supplemental logging, DDL capture, snapshot/streaming).
 * <p>
 * Runs only under the {@code integration} profile ({@code mvn -Pintegration verify}); it is skipped
 * automatically when Docker is not available.
 */
@SuppressFBWarnings(value = {"DMI_CONSTANT_DB_PASSWORD", "HARD_CODE_PASSWORD"},
        justification = "Well-known default password of a throwaway test container")
public class DamengContainerIT
{
    private static final DockerImageName IMAGE = DockerImageName.parse("xuxuclassmate/dameng:latest");
    private static final int DM_PORT = 5236;
    private static final String USER = "SYSDBA";
    private static final String PASSWORD = "SYSDBA001";

    private GenericContainer<?> dameng;

    @Before
    public void setUp()
    {
        assumeTrue("Docker is not available; skipping integration test", DockerClientFactory.instance().isDockerAvailable());
        dameng = new GenericContainer<>(IMAGE)
                .withExposedPorts(DM_PORT)
                .waitingFor(Wait.forLogMessage(".*SYSTEM IS READY.*", 1)
                        .withStartupTimeout(Duration.ofMinutes(5)));
        dameng.start();
    }

    @After
    public void tearDown()
    {
        if (dameng != null) {
            dameng.stop();
        }
    }

    @Test
    public void connectsAndRunsBasicSql()
            throws Exception
    {
        final String url = String.format("jdbc:dm://%s:%d", dameng.getHost(), dameng.getMappedPort(DM_PORT));
        Class.forName("dm.jdbc.driver.DmDriver");
        try (Connection connection = DriverManager.getConnection(url, USER, PASSWORD);
                Statement statement = connection.createStatement()) {
            statement.execute("CREATE TABLE SYSDBA.IT_SMOKE (ID INT PRIMARY KEY, NAME VARCHAR(50))");
            statement.execute("INSERT INTO SYSDBA.IT_SMOKE VALUES (1, 'hello')");
            try (ResultSet rs = statement.executeQuery("SELECT NAME FROM SYSDBA.IT_SMOKE WHERE ID = 1")) {
                assertEquals(true, rs.next());
                assertEquals("hello", rs.getString(1));
            }
        }
    }
}
