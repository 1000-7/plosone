import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.sql.SQLException;
import java.util.concurrent.*;
import java.util.stream.Stream;

public class PlosOneContributions extends PlosOne {
    public static final String QUERYSQL = "Select id,contributions from plosonepapersnotnull";
    public static final String INSERTSQL = "insert into contribution(paper_id,contribution,abbr) values(?,?,?)";
    protected String begin;
    protected String end;

    public PlosOneContributions() {
    }

    public PlosOneContributions(String begin, String end) {
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
                String contributions = rs.getString(2);
                System.out.println(contributions);
                for (String w : contributions.split("\\.+\\s?")) {
//
                    if (contributions.contains(":")) {
                        String[] cname = w.split(": ");

                        if (cname.length > 1) {
                            Stream.of(cname[1].replaceAll("-", "").split(" ")).forEach(abbr -> {
                                try {
                                    pstmt.setInt(1, paperid);
                                    pstmt.setString(2, cname[0]);
                                    pstmt.setString(3, abbr);
                                    pstmt.addBatch();
                                } catch (SQLException e) {
                                    e.printStackTrace();
                                }
                            });

                        } else break;
                    } else {
                        break;
                    }
                    pstmt.executeBatch();
                    conn.commit();
                    pstmt.clearBatch();
                }
                id++;
                if (id % 1 == 0)
                    System.out.println(Thread.currentThread().getName() + "  " + paperid);
//                break;
            }
            stmt.close();
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("plosone-%d").build();
        ExecutorService pool = new ThreadPoolExecutor(10, 20, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(1024), threadFactory);
        for (int i = 0; i < 8; i++) {
            PlosOne po = new PlosOneContributions(String.valueOf(i * 25060), String.valueOf((i + 1) * 25060));
            pool.execute(po);
        }
        pool.shutdown();
    }
}
