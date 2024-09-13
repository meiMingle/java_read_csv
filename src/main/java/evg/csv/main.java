package evg.csv;

import cn.hutool.core.io.FileUtil;
import cn.hutool.core.text.csv.CsvReadConfig;
import cn.hutool.core.text.csv.CsvReader;
import cn.hutool.core.text.csv.CsvUtil;
import cn.hutool.core.text.csv.CsvWriter;
import de.siegmar.fastcsv.reader.CsvRecord;
import de.siegmar.fastcsv.writer.QuoteStrategies;
import de.siegmar.fastcsv.writer.QuoteStrategy;

import javax.swing.*;
import javax.swing.filechooser.FileNameExtensionFilter;
import javax.swing.text.*;
import java.io.File;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Objects;
import java.util.logging.Level;
import java.util.logging.Logger;

public class main extends JFrame {
    String full_path;

    public static class JTextFieldLimit extends PlainDocument {

        private final int limit;  // 限制的长度

        public JTextFieldLimit(int limit) {
            super(); // 调用父类构造
            this.limit = limit;
        }

        public void insertString(int offset, String str, AttributeSet attr) throws BadLocationException {
            if (str == null) return;

            // 下面的判断条件改为自己想要限制的条件即可，这里为限制输入的长度
            if ((getLength() + str.length()) <= limit) {
                super.insertString(offset, str, attr);// 调用父类方法
            }
        }

    }


    public main() {
        super("CSV to Database import");
        setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        JPanel panel = new JPanel();
        panel.setLayout(new BoxLayout(panel, BoxLayout.Y_AXIS));

        final JLabel jdbc_url = new JLabel("JDBC Url");
        final JTextField jdbc_url_label = new JTextField(15);
        jdbc_url_label.setText("jdbc:oracle:thin:@192.168.1.102:1521:ORCL?rewriteBatchedStatements=true");

        final JLabel user_name = new JLabel("User Name");
        final JTextField user_name_label = new JTextField(15);
        user_name_label.setText("datachange");

        final JLabel password = new JLabel("Password");
        final JTextField password_label = new JTextField(15);
        password_label.setText("drgs2019");

        final JLabel label_schema = new JLabel("Schema/User");
        JComboBox<String> jcb = new JComboBox<>();

        JButton test_button = new JButton("Test Connection");
        test_button.addActionListener(e -> {
            try (Connection connection = DriverManager.getConnection(jdbc_url_label.getText().trim(), user_name_label.getText().trim(), password_label.getText().trim())) {
                JOptionPane.showMessageDialog(null, "###连接成功###");
                ResultSet schemas = connection.getMetaData().getSchemas();
                ArrayList<String> list = new ArrayList<>();
                while (schemas.next()) {
                    list.add(schemas.getString(1));
                }
                String[] excludeSchemas = {"ANONYMOUS", "APPQOSSYS", "AUDSYS", "CTXSYS", "DBSFWUSER",
                        "DBSNMP", "DIP", "DVF", "DVSYS", "ENGINE", "GGSYS", "GSMADMIN_INTERNAL", "GSMCATUSER",
                        "GSMUSER", "HR", "LBACSYS", "MDDATA", "MDSYS", "OJVMSYS", "OLAPSYS", "ORACLE_OCM",
                        "ORDDATA", "ORDPLUGINS", "ORDSYS", "OUTLN", "REMOTE_SCHEDULER_AGENT", "SI_INFORMTN_SCHEMA",
                        "SPATIAL_CSW_ADMIN_USR", "SYS", "SYS$UMF", "SYSBACKUP", "SYSDG", "SYSKM", "SYSRAC", "SYSTEM",
                        "WMSYS", "XDB", "XS$NULL"};
                String[] array = list.stream().filter(a -> {
                    for (int i = 0; i < excludeSchemas.length; i++) {
                        if (excludeSchemas[i].equals(a)) {
                            return false;
                        }
                    }
                    return true;
                }).toArray(String[]::new);
                jcb.setModel(new DefaultComboBoxModel<>(array));
            } catch (Exception exception) {
                System.err.println(exception);
                JOptionPane.showMessageDialog(null, "！！！连接失败！！！:\n" + exception.getMessage());
            }

        });

        final JLabel label_table = new JLabel("Table name");
        final JTextField table_name_label = new JTextField(15);


        final JLabel fieldSeparator = new JLabel("Field Separator(CSV)");
        final JTextField fieldSeparator_label = new JTextField(5);
        fieldSeparator_label.setDocument(new JTextFieldLimit(1));
        fieldSeparator_label.setText(",");

        final JLabel textDelimiter = new JLabel("Text Delimiter(CSV)");
        final JTextField textDelimiter_label = new JTextField(15);
        textDelimiter_label.setDocument(new JTextFieldLimit(1));
        textDelimiter_label.setText("\"");

        // 编码
        JComboBox<String> encodeBox = new JComboBox<>();
        encodeBox.setModel(new DefaultComboBoxModel<>(new String[]{"UTF-8", "GB2312", "GBK"}));
        encodeBox.setSelectedItem("UTF-8");
        final JLabel headerLineNo = new JLabel("Header Line No");
        final JTextField headerLineNo_label = new JTextField(15);
        ((AbstractDocument) headerLineNo_label.getDocument()).setDocumentFilter(new DocumentFilter() {
            @Override
            public void insertString(FilterBypass fb, int offset, String string, AttributeSet attr) throws BadLocationException {
                // super.insertString(fb, offset, string, attr);
                fb.insertString(offset, string.replaceAll("[^0-9]", ""), attr);
            }

            @Override
            public void replace(FilterBypass fb, int offset, int length, String text, AttributeSet attrs) throws BadLocationException {
                // super.replace(fb, offset, length, text, attrs);
                fb.replace(offset, length, text.replaceAll("[^0-9]", ""), attrs);
            }
        });
        headerLineNo_label.setText("0");

        final JLabel beginDataLineNo = new JLabel("First Data Line No");
        final JTextField firstDataLineNo_label = new JTextField(15);
        ((AbstractDocument) firstDataLineNo_label.getDocument()).setDocumentFilter(new DocumentFilter() {
            @Override
            public void insertString(FilterBypass fb, int offset, String string, AttributeSet attr) throws BadLocationException {
                // super.insertString(fb, offset, string, attr);
                fb.insertString(offset, string.replaceAll("[^0-9]", ""), attr);
            }

            @Override
            public void replace(FilterBypass fb, int offset, int length, String text, AttributeSet attrs) throws BadLocationException {
                // super.replace(fb, offset, length, text, attrs);
                fb.replace(offset, length, text.replaceAll("[^0-9]", ""), attrs);
            }
        });
        firstDataLineNo_label.setText("1");


        final JLabel label = new JLabel("Selected CSV/Excel");

        JButton button = new JButton("Select File");

        button.addActionListener(e -> {
            JFileChooser fileopen = new JFileChooser();
            fileopen.setFileFilter(new FileNameExtensionFilter("csv,xlsx)", "csv", "xlsx"));
            int ret = fileopen.showDialog(null, "Open");
            if (ret == JFileChooser.APPROVE_OPTION) {
                File file = fileopen.getSelectedFile();
                table_name_label.setText(file.getName().substring(0, file.getName().lastIndexOf('.')));
                label.setText(file.getName());
                full_path = file.getAbsolutePath();
            }
        });

        JButton button_start = new JButton("Import CSV to DB");
        button_start.addActionListener(e -> {
            System.err.println(table_name_label.getText());
            System.err.println(full_path);
            try {
                System.err.println("Importing \"" + full_path + "\"");
                CSVReader.generate_sql(table_name_label.getText(), full_path,
                        fieldSeparator_label.getText().trim().charAt(0),
                        textDelimiter_label.getText().trim().charAt(0),
                        Long.parseLong(headerLineNo_label.getText()),
                        Long.parseLong(firstDataLineNo_label.getText()),
                        (String) jcb.getSelectedItem()
                );
                System.err.println("Import \"" + full_path + "\"  completed");
            } catch (Exception ex) {
                Logger.getLogger(main.class.getName()).log(Level.SEVERE, null, ex);
                JOptionPane.showMessageDialog(null, "！！！处理CSV导入数据库失败！！！:\n" + ex.getMessage());
            }
        });

        JButton standardize_button_hutoolCsvReader = new JButton("Standardize_HutoolCsvReader");
        standardize_button_hutoolCsvReader.addActionListener(e -> {
            CsvReadConfig csvReadConfig = CsvReadConfig.defaultConfig()
                    .setFieldSeparator(fieldSeparator_label.getText().trim().charAt(0))
                    .setTextDelimiter(textDelimiter_label.getText().trim().charAt(0))
                    // .setHeaderLineNo(Long.parseLong(headerLineNo_label.getText()))
                    .setTrimField(true);

            try (CsvReader csvReader = CsvUtil.getReader(FileUtil.getBOMReader(FileUtil.file(full_path)), csvReadConfig)) {
                String out_path = full_path.substring(0, full_path.lastIndexOf('.')) + "_hu.csv";
                try (CsvWriter csvWriter = CsvUtil.getWriter(out_path, Charset.forName((String) Objects.requireNonNull(encodeBox.getSelectedItem())))) {
                    csvReader.forEach(a -> {
                        long ol = a.getOriginalLineNumber();
                        if (ol < 100 && ol % 10 == 0) {
                            System.err.println(ol + " lines processed");
                        } else if (ol < 10000 && ol % 100 == 0) {
                            System.err.println(ol + " lines processed");
                        } else if (ol < 1000000 && ol % 10000 == 0) {
                            System.err.println(ol + " lines processed");
                        } else if (ol % 100000 == 0) {
                            System.err.println(ol + " lines processed");
                        }
                        csvWriter.writeLine(a.toArray(new String[0]));
                    });
                    JOptionPane.showMessageDialog(null, "！！！标化CSV处理成功！！！");
                } catch (Exception ex) {
                    Logger.getLogger(main.class.getName()).log(Level.SEVERE, null, ex);
                    JOptionPane.showMessageDialog(null, "！！！标化CSV时写入CSV文件失败！！！:\n" + ex.getMessage());
                }
            } catch (Exception ex) {
                Logger.getLogger(main.class.getName()).log(Level.SEVERE, null, ex);
                JOptionPane.showMessageDialog(null, "！！！标化CSV时读取CSV文件失败！！！:\n" + ex.getMessage());
            }
        });

        JButton standardize_button_FastCsv = new JButton("Standardize_FastCsv");
        standardize_button_FastCsv.addActionListener(e -> {
            Path file = Paths.get(full_path);
            try (de.siegmar.fastcsv.reader.CsvReader<CsvRecord> csvr = de.siegmar.fastcsv.reader.CsvReader
                    .builder()
                    .fieldSeparator(fieldSeparator_label.getText().trim().charAt(0))
                    .quoteCharacter(textDelimiter_label.getText().trim().charAt(0))
                    .ofCsvRecord(file, Charset.forName((String) Objects.requireNonNull(encodeBox.getSelectedItem())))) {
                String out_path = full_path.substring(0, full_path.lastIndexOf('.')) + "_fast.csv";
                Path ofile = Paths.get(out_path);
                try (de.siegmar.fastcsv.writer.CsvWriter csvw = de.siegmar.fastcsv.writer.CsvWriter.builder()
                        .quoteStrategy(QuoteStrategies.EMPTY)
                        .build(ofile)) {
                    csvr.forEach(a -> {
                        long ol = a.getStartingLineNumber();
                        if (ol < 100 && ol % 10 == 0) {
                            System.err.println(ol + " lines processed");
                        } else if (ol < 10000 && ol % 100 == 0) {
                            System.err.println(ol + " lines processed");
                        } else if (ol < 1000000 && ol % 10000 == 0) {
                            System.err.println(ol + " lines processed");
                        } else if (ol % 100000 == 0) {
                            System.err.println(ol + " lines processed");
                        }
                        csvw.writeRecord(a.getFields());
                    });
                    JOptionPane.showMessageDialog(null, "！！！标化CSV处理成功！！！");
                } catch (Exception ex) {
                    Logger.getLogger(main.class.getName()).log(Level.SEVERE, null, ex);
                    JOptionPane.showMessageDialog(null, "！！！标化CSV时写入CSV文件失败！！！:\n" + ex.getMessage());
                }
            } catch (Exception ex) {
                Logger.getLogger(main.class.getName()).log(Level.SEVERE, null, ex);
                JOptionPane.showMessageDialog(null, "！！！标化CSV时读取CSV文件失败！！！:\n" + ex.getMessage());
            }
        });

        // CSVParser.parse()

        panel.add(jdbc_url);
        panel.add(jdbc_url_label);
        panel.add(user_name);
        panel.add(user_name_label);
        panel.add(password);
        panel.add(password_label);
        panel.add(test_button);


        panel.add(label_table);
        panel.add(table_name_label);

        panel.add(label_schema);
        panel.add(jcb);

        panel.add(fieldSeparator);
        panel.add(fieldSeparator_label);

        panel.add(textDelimiter);
        panel.add(textDelimiter_label);
        panel.add(encodeBox);

        panel.add(headerLineNo);
        panel.add(headerLineNo_label);

        panel.add(beginDataLineNo);
        panel.add(firstDataLineNo_label);

        panel.add(label);
        panel.add(button);
        panel.add(button_start);
        panel.add(standardize_button_hutoolCsvReader);
        panel.add(standardize_button_FastCsv);

        getContentPane().add(panel);
        pack();
        setLocationRelativeTo(null);
        setVisible(true);
    }

    public static void main(String[] args) {
        javax.swing.SwingUtilities.invokeLater(new Runnable() {
            public void run() {
                JFrame.setDefaultLookAndFeelDecorated(true);
                JDialog.setDefaultLookAndFeelDecorated(true);
                new main();
            }
        });
    }
}