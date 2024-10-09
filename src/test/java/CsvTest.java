import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.UUID;
import java.util.logging.Logger;

public class CsvTest {
    final static Logger LOGGER = Logger.getLogger(CsvTest.class.getName());
    private final static String EXTRACTION_PATH =
            System.getProperty("java.io.tmpdir") + File.separator + UUID.randomUUID();
    private final static String DDL_STR =
            "CREATE TABLE sales (\n" +
                    "    salesid          INTEGER         NOT NULL PRIMARY KEY\n" +
                    "    , listid         INTEGER         NOT NULL\n" +
                    "    , sellerid       INTEGER         NOT NULL\n" +
                    "    , buyerid        INTEGER         NOT NULL\n" +
                    "    , eventid        INTEGER         NOT NULL\n" +
                    "    , dateid         SMALLINT        NOT NULL\n" +
                    "    , qtysold        SMALLINT        NOT NULL\n" +
                    "    , pricepaid      DECIMAL (8,2)\n" +
                    "    , commission     DECIMAL (8,2)\n" +
                    "    , saletime       TIMESTAMP\n" +
                    ")\n" +
                    ";";
    private static File csvFile;
    private static Connection connDuck;

    @BeforeAll
    // setting up a TEST Database according to
    // https://docs.aws.amazon.com/redshift/latest/dg/c_sampledb.html
    static synchronized void init()
            throws SQLException, IOException {

        File extractionPathFolder = new File(EXTRACTION_PATH);
        boolean mkdirs = extractionPathFolder.mkdirs();

        File fileDuckDB =
                new File(EXTRACTION_PATH, CsvTest.class.getSimpleName() + ".duckdb");

        csvFile = new File(EXTRACTION_PATH, "sales_tab.txt");

        Properties info = new Properties();
        info.put("old_implicit_casting", "true");
        info.put("default_null_order", "NULLS FIRST");
        info.put("default_order", "ASC");
        info.put("memory_limit", "1GB");
        connDuck = DriverManager.getConnection("jdbc:duckdb:" + fileDuckDB.getAbsolutePath(), info);


        LOGGER.info("Create the DuckDB Table with Indices");
        try (Statement st = connDuck.createStatement()) {
            st.execute(DDL_STR);
        }

        LOGGER.info("Copy the original CSV File to the Temp folder");
        try (FileOutputStream outputStream = new FileOutputStream(csvFile)) {
            IOUtils.copy(Objects.requireNonNull(CsvTest.class.getResourceAsStream("sales_tab.txt")), outputStream);
        }

        LOGGER.info("remove some silly '\\N' entries since");
        try {
            List<String> lines =
                    Files.readAllLines(csvFile.toPath(), StandardCharsets.UTF_8);
            lines.replaceAll(s -> s.replace("\\N", ""));
            Files.write(csvFile.toPath(), lines, StandardCharsets.UTF_8,
                    StandardOpenOption.TRUNCATE_EXISTING);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @AfterAll
    static void cleanUp() {
        File extractionPathFolder = new File(EXTRACTION_PATH);
        if (extractionPathFolder.exists()) {
            extractionPathFolder.delete();
        }
    }

    // This test shows that CSV Autodetect Delimiter FAILS for COPY FROM
    // --> Invalid Input Error: Mismatch between the number of columns (1) in the CSV file and what is expected in the scanner (10).
    // The test fails with Duck DB 1.1.1 but works with 1.0.0
    @Test
    void testCopyAutoDetectDoesNotWork() throws SQLException {
        LOGGER.info("COPY FROM Csv File");
        String copyCommand = "COPY sales FROM '"
                + csvFile
                + "' (FORMAT CSV, AUTO_DETECT true, TIMESTAMPFORMAT '%m/%d/%Y %I:%M:%S', IGNORE_ERRORS true);";
        try (Statement st = connDuck.createStatement()) {
            st.executeUpdate("TRUNCATE TABLE sales");

            LOGGER.info("execute: " + copyCommand);
            st.execute(copyCommand);
        }
    }

    // This test shows that setting the Delimiter explicitly WORK for COPY FROM,
    // although it will FAIL with a duplication error (while there is not duplicate in the CSV source):
    // --> Constraint Error: PRIMARY KEY or UNIQUE constraint violated: duplicate key "125681"

    // The test fails with Duck DB 1.1.1 but works with 1.0.0
    @Test
    void testCopySetDelimiterWorks() throws SQLException {
        LOGGER.info("COPY FROM Csv File");
        String copyCommand = "COPY sales FROM '"
                + csvFile
                + "' (FORMAT CSV, DELIMITER '\t', TIMESTAMPFORMAT '%m/%d/%Y %I:%M:%S', IGNORE_ERRORS true);";
        try (Statement st = connDuck.createStatement()) {
            st.executeUpdate("TRUNCATE TABLE sales");

            LOGGER.info("execute: " + copyCommand);
            st.execute(copyCommand);
        }
    }

    // This test shows that CSV Autodetect Delimiter WORKS for INSERT INTO .... SELECT FROM
    // The test WORKS with Duck DB 1.1.1
    @Test
    void testInsertIntoAutoDetectWorks() throws SQLException {
        LOGGER.info("COPY FROM Csv File");
        String copyCommand = "INSERT INTO sales SELECT * FROM read_csv('"
                + csvFile
                + "', TimestampFormat='%m/%d/%Y %I:%M:%S');";
        try (Statement st = connDuck.createStatement()) {
            st.executeUpdate("TRUNCATE TABLE sales");

            LOGGER.info("execute: " + copyCommand);
            st.execute(copyCommand);
        }
    }

}
