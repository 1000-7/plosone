import java.sql.*;

public abstract class PlosOne implements Runnable {
    public static final String URL = "jdbc:mysql://202.114.70.31/plosonewangxin";
    public static final String USER = "root";
    public static final String PASSWD = "irlab2013";
    public static final String DRIVERNAME = "com.mysql.jdbc.Driver";


    protected Connection conn;
    protected PreparedStatement pstmt;
    protected Statement stmt;
    protected ResultSet rs;

    public PlosOne() {
        conn = initDB();
    }

    public Connection initDB() {
        try {
            Class.forName(DRIVERNAME);
            Connection connection = DriverManager.getConnection(URL, USER, PASSWD);
            connection.setAutoCommit(false);
            return connection;
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    public PreparedStatement createptmt(Connection conn, String sql) {
        try {
            return conn.prepareStatement(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    public Statement createstmt(Connection conn) {
        try {
            return conn.createStatement();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    public ResultSet query(Statement stmt, String QUERYSQL) {
        try {
            System.out.println(QUERYSQL);
            return stmt.executeQuery(QUERYSQL);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    protected ResultSet query(Statement stmt, String querysql, String begin, String end) {
        querysql = querysql + " where id >= " + begin + " and id <" + end;
        try {
            System.out.println(querysql);
            ResultSet rs = stmt.executeQuery(querysql);
            return rs;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }
}
