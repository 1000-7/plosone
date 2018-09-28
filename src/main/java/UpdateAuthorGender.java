
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.sql.SQLException;
import java.util.concurrent.*;

public class UpdateAuthorGender extends PlosOne {
    public static final String QUERYSQL = "Select * from author_gender ";
    public static final String INSERTSQL = "update author_gender set author_abbr = ? where id = ?";

    @Override
    public void run() {
        conn = initDB();
        try {
            stmt = conn.createStatement();
            rs = stmt.executeQuery(QUERYSQL);
            pstmt = conn.prepareStatement(INSERTSQL);
            int num = 0;
            while (rs.next()) {
                int id = rs.getInt("id");
                String author_abbr = rs.getString("author_abbr");
                pstmt.setString(1, rejectLowerChar(author_abbr));
                pstmt.setInt(2, id);
                pstmt.addBatch();
                num++;
                if (num % 1000 == 0) {
                    System.out.println(Thread.currentThread().getName() + " " + num);
                    pstmt.executeBatch();
                    conn.commit();
                    pstmt.clearBatch();
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    public static String rejectLowerChar(String s) {
        return s.replaceAll("[a-z]+", "");

    }

    public static void main(String[] args) {
        ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("updateAuthorGender-%d").build();
        ExecutorService pool = new ThreadPoolExecutor(10, 20, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(1024), threadFactory);
        pool.execute(new UpdateAuthorGender());
        pool.shutdown();

    }
}
