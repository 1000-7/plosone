import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.sql.SQLException;
import java.util.concurrent.*;
import java.util.stream.Stream;

public class PlosOneAuthor extends PlosOne {
    public static final String QUERYSQL = "Select id,authors from plosonepapersnotnull";
    public static final String INSERTSQL = "insert into author_copy1(paper_id,author,author_abbr,gender) values(?,?,?,?)";
    protected String begin;
    protected String end;

    public PlosOneAuthor() {
    }

    public PlosOneAuthor(String begin, String end) {
        super();
        stmt = createstmt(conn);
        rs = query(stmt, QUERYSQL, begin, end);
        pstmt = createptmt(conn, INSERTSQL);
    }


    @Override
    public void run() {
        try {
            int id = 0;
            while (rs.next()) {
                int paperid = rs.getInt(1);
                String authors = rs.getString(2);
                Stream.of(authors.split(", ")).forEach(w -> {
                    try {
//                        System.out.println(w);
                        pstmt.setInt(1, paperid);
                        pstmt.setString(2, w);
                        pstmt.setString(3, abbr(w));
                        pstmt.setString(4, "male");
                        pstmt.executeUpdate();
                        conn.commit();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                });
                id++;
                if (id % 500 == 0)
                    System.out.println(Thread.currentThread().getName() + "  " + paperid);


            }
            stmt.close();
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private String abbr(String w) {
        String[] names = w.split("[ -]");
        StringBuffer sb = new StringBuffer();
        for (String name : names) {
            if (name.toCharArray().length >= 1) {
                String a = name.substring(0, 1);
                sb.append(a);
            } else {
                continue;
            }

        }
        return String.valueOf(sb).replaceAll("[a-z]+", "");
    }


    public static void main(String[] args) {
        ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("plosone-%d").build();
        ExecutorService pool = new ThreadPoolExecutor(10, 20, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(1024), threadFactory);
        for (int i = 0; i < 8; i++) {
            PlosOne po = new PlosOneAuthor(String.valueOf(i * 25060), String.valueOf((i + 1) * 25060));
            pool.execute(po);
        }
        pool.shutdown();
    }
}
