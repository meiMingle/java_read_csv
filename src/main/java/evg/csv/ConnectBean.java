package evg.csv;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class ConnectBean {
    private Connection con;
    private static ConnectBean instance;
    private ConnectBean() throws Exception {
        try {           
            String driver = "oracle.jdbc.driver.OracleDriver";
            Class.forName(driver).newInstance();
            // String url = "jdbc:sqlite:C:\\Projects\\JAVA\\jsp_app\\database.sqlite3";
            String url = "jdbc:oracle:thin:@192.168.1.102:1521:ORCL?rewriteBatchedStatements=true";
            con = DriverManager.getConnection(url,"datachange","drgs2019");
            con.setAutoCommit(false);
        } 
        catch (ClassNotFoundException e) {
            throw new Exception(e);
        }
        catch (SQLException e) {
            throw new Exception(e);
        }
    }
    public static synchronized  ConnectBean getInstance () throws Exception {
        if (instance == null) {
            instance = new ConnectBean();
        }
        return instance;         
    }
    public Connection getConnection() {
        return con;
    }
}