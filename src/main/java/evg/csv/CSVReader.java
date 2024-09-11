/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package evg.csv;

/**
 * @author eshahov
 */

import cn.hutool.core.io.FileUtil;
import cn.hutool.core.io.file.PathUtil;
import cn.hutool.core.text.csv.*;

import static evg.csv.Utils.*;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Class for read csv
 */

public class CSVReader {

    @SuppressWarnings("empty-statement")
    public static void generate_sql(String table_name, String csvFile, char fieldSeparator, char textDelimiter, long headerLineNo, long beginDataLineNo, String schema) throws Exception {
//        String csvFile = "C:\\temp\\IKAR-16902.csv";        
        CsvRow csv_line = null;
        String cvsSplitBy = ",";
        String str_for_create_table = "CREATE TABLE \"" + schema + "\".\"" + table_name + "\" ";
        StringBuilder str_create_table = new StringBuilder("(");
        StringBuilder str_values_table = new StringBuilder("(");
        int num_cols;
        String final_sql_create_table;

        CsvReadConfig csvReadConfig = CsvReadConfig.defaultConfig()
                .setFieldSeparator(fieldSeparator)
                .setTextDelimiter(textDelimiter)
                .setHeaderLineNo(headerLineNo);

        try (CsvParser csvParser = new CsvParser(FileUtil.getBOMReader(FileUtil.file(csvFile)), csvReadConfig)) {

            csv_line = csvParser.nextRow();
            num_cols = csv_line.size();
            Map<String, String> fieldMap = csv_line.getFieldMap();
            String[] headers = fieldMap.keySet().toArray(new String[]{});

            str_create_table.append("LINE_NO$ INTEGER,");
            str_values_table.append("LINE_NO$,");

            for (int i = 0; i < num_cols; i++) {
                str_create_table.append("\"" + headers[i]).append("\" ").append("VARCHAR2(500)");
                str_values_table.append("\"" + headers[i] + "\"");

                if (i < num_cols - 1) {
                    str_create_table.append(",");
                    str_values_table.append(",");
                }

                if (i == num_cols - 1) {
                    str_create_table.append(")");
                    str_values_table.append(")");
                }
            }
            // final string for create table
            final_sql_create_table = str_for_create_table + str_create_table;
            System.err.println(final_sql_create_table);
            // execute create table 
            try {

                Connection connection = ConnectBean.getInstance().getConnection();
                Statement stmt = connection.createStatement();
                stmt.executeUpdate(final_sql_create_table);
                // stmt.executeUpdate(final_sql_create_table);
                connection.commit();
            } catch (Exception e) {
                System.err.println(e);
            }
            Connection connection = ConnectBean.getInstance().getConnection();

            String final_insert = buildFinalInsert(schema, table_name, str_values_table.toString(), num_cols, 100);

            System.err.println(final_insert);


            PreparedStatement ps = connection.prepareStatement(final_insert);
            int commitCount = 1;

            List<CsvRow> temp = new ArrayList<>(100);
            temp.add(csv_line);

            do {
                while (temp.size() < 100 && (csv_line = csvParser.nextRow()) != null) {
                    commitCount++;
                    temp.add(csv_line);
                }

                if (temp.size() < 100) {
                    ps.executeBatch();
                    ps.clearBatch();
                    connection.commit();

                    final_insert = buildFinalInsert(schema, table_name, str_values_table.toString(), num_cols, temp.size());
                    System.err.println(final_insert);
                    ps = connection.prepareStatement(final_insert);
                }
                // int index = 0;
                for (int i = 0; i < temp.size(); i++) {
                    CsvRow row = temp.get(i);
                    ps.setLong(row.size() * i + 1 + i, row.getOriginalLineNumber());
                    for (int j = 0; j < row.size(); j++) {
                        ps.setString(row.size() * i + 2 + i + j, row.get(j));
                    }
                }

                ps.addBatch();
                temp.clear();

                ps.executeBatch();
                ps.clearBatch();

                if (commitCount % 1000 == 0) {
                    System.err.println(commitCount + " raws processed");
                    if (commitCount % 2000 == 0) {
                        connection.commit();
                    }
                }


            } while (csv_line != null);


            ps.executeBatch();
            ps.clearBatch();
            connection.commit();
            System.err.println(commitCount + " raws processed");
        } catch (IOException e) {
            System.err.println(e);
        }
    }


    static String buildFinalInsert(String schema, String table_name, String str_values_table, int num_cols, int rowCount) {

        StringBuilder str_insert_table = new StringBuilder();
        for (int j = 0; j < rowCount; j++) {
            str_insert_table.append("SELECT ");
            str_insert_table.append("?,");
            for (int i = 0; i < num_cols; i++) {
                if (i > 0 && i <= num_cols - 1) {
                    str_insert_table.append(",");
                }
                str_insert_table.append("?");
            }
            str_insert_table.append(" FROM DUAL ");
            if (j < (rowCount - 1)) {
                str_insert_table.append(" UNION ALL \n");
            }
        }

        return "INSERT /*+ append */ INTO \"" + schema + "\".\"" + table_name + "\" " + str_values_table + str_insert_table;
    }


}
