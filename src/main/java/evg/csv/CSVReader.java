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
import de.siegmar.fastcsv.reader.CloseableIterator;
import de.siegmar.fastcsv.reader.CsvRecord;
import de.siegmar.fastcsv.reader.NamedCsvRecord;

import static evg.csv.Utils.*;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.sql.*;
import java.util.*;

/**
 * Class for read csv
 */

public class CSVReader {

    @SuppressWarnings("empty-statement")
    public static void generate_sql(String table_name, String csvFile, char fieldSeparator, char textDelimiter, long headerLineNo, long beginDataLineNo, String schema,String charset) throws Exception {
//        String csvFile = "C:\\temp\\IKAR-16902.csv";        
//         CsvRow csv_line = null;
        NamedCsvRecord  csv_line = null;
        String cvsSplitBy = ",";
        String str_for_create_table = "CREATE TABLE \"" + schema + "\".\"" + table_name + "\" ";
        StringBuilder str_create_table = new StringBuilder("(");
        StringBuilder str_values_table = new StringBuilder("(");
        int num_cols;
        String final_sql_create_table;


        try (de.siegmar.fastcsv.reader.CsvReader<NamedCsvRecord> csvr = de.siegmar.fastcsv.reader.CsvReader
                .builder()
                .fieldSeparator(fieldSeparator)
                .quoteCharacter(textDelimiter)
                .ofNamedCsvRecord(Paths.get(csvFile), Charset.forName(charset));
             CloseableIterator<NamedCsvRecord> iterator = csvr.iterator();) {

            csv_line = iterator.next();
            num_cols = csv_line.getFieldCount();
            Map<String, String> fieldMap = csv_line.getFieldsAsMap();
            String[] headers = fieldMap.keySet().toArray(new String[]{});

            str_create_table.append("LINE_NO$ INTEGER,");
            str_values_table.append("LINE_NO$,");

            for (int i = 0; i < num_cols; i++) {
                str_create_table.append("\"" + headers[i]).append("\" ").append("VARCHAR2(1000)");
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

            String final_insert = buildFinalInsert(schema, table_name, str_values_table.toString(), num_cols, 1);

            System.err.println(final_insert);


            PreparedStatement ps = connection.prepareStatement(final_insert);
            int commitCount = 1;
            // Savepoint savepoint = null;
            ps.setLong(1, csv_line.getStartingLineNumber()-1);
            for (int j = 0; j < csv_line.getFieldCount(); j++) {
                ps.setString( 2 + j, csv_line.getField(j));
            }
            try{
                while (iterator.hasNext()){
                    commitCount++;
                    csv_line = iterator.next();

                    ps.setLong(1, csv_line.getStartingLineNumber()-1);
                    for (int j = 0; j < csv_line.getFieldCount(); j++) {
                        ps.setString( 2 + j, csv_line.getField(j));
                    }
                    ps.addBatch();
                    ps.clearParameters();
                    if (commitCount % 1000 == 0) {
                        int[] results = ps.executeBatch();
                        ps.clearBatch();
                        // savepoint = connection.setSavepoint();
                        System.err.println(commitCount + " raws processed");
                        if (commitCount % 10000 == 0) {
                            connection.commit();
                        }
                    }
                }
            } catch (SQLException e) {
                connection.rollback();
                connection.commit();
                throw new RuntimeException("line:"+ commitCount,e);
            }finally {

            }


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
        /*for (int j = 0; j < rowCount; j++) {
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
        }*/
        str_insert_table.append("(");
        str_insert_table.append("?");
        for (int i = 0; i < num_cols; i++) {
            if (/*i > 0 &&*/ i <= num_cols - 1) {
                str_insert_table.append(",");
            }
            str_insert_table.append("?");
        }
        str_insert_table.append(")");
        return "INSERT  INTO \"" + schema + "\".\"" + table_name + "\" " + str_values_table +" VALUES "+ str_insert_table;
    }


}
