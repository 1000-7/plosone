import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.concurrent.*;

public class PaperContributionGender extends PlosOne {
    public static final String QUERYID = "Select id from plosonepapersnotnull";
    public static final String QUERYSQL = "Select paper_id,abbr,class from paper_contribution where paper_id = ? and abbr = ?";
    public static final String QUERYSQLGENDER = "Select author_abbr,f1 from author_gender where paper_id = ? ";
    public static final String INSERTSQL = "insert into acgMust(paper_id,author_contribution_gender) values(?,?)";
    public static final String QUERYABBR = "Select DISTINCT abbr from paper_contribution where paper_id = ?";
    protected ResultSet rsId;
    protected PreparedStatement psContribution;
    protected PreparedStatement psGender;
    protected ResultSet rsContribution;
    protected ResultSet rsGender;
    protected PreparedStatement psABBR;
    protected ResultSet rsABBR;
    protected String begin;
    protected String end;

    public PaperContributionGender() {
        super();
    }

    public PaperContributionGender(String begin, String end) {
        super();
        this.begin = begin;
        this.end = end;
        conn = initDB();
        stmt = createstmt(conn);
        rsId = query(stmt, QUERYID, begin, end);
        psContribution = createptmt(conn, QUERYSQL);
        psGender = createptmt(conn, QUERYSQLGENDER);
        pstmt = createptmt(conn, INSERTSQL);
        psABBR = createptmt(conn, QUERYABBR);
    }

    @Override
    public void run() {
        try {
            System.out.println(Thread.currentThread().getName());
            int num = 0;
            while (rsId.next()) {
                num++;
                int id = Integer.parseInt(rsId.getString("id"));
                psGender.setInt(1, id);
                rsGender = psGender.executeQuery();
                pstmt.setInt(1, id);
                StringBuffer sb = new StringBuffer();
                HashSet<String> nameabbrs = new HashSet<>();
                while (rsGender.next()) {
                    String abbr = rsGender.getString("author_abbr");

                    sb.append(abbr + "#");
                    int gender = gender2int(rsGender.getString("f1"));
                    sb.append(gender + "$");
                    psContribution.setInt(1, id);
                    psContribution.setString(2, abbr);
                    rsContribution = psContribution.executeQuery();

                    StringBuffer subSb = new StringBuffer();
                    if (!rsContribution.next()) {
                        psABBR.setInt(1, id);
                        rsABBR = psABBR.executeQuery();
                        while (rsABBR.next()) {
                            nameabbrs.add(rsABBR.getString("abbr"));
                        }
                        String bestAbbr = calmSet(abbr, nameabbrs);
                        psContribution.setInt(1, id);
                        psContribution.setString(2, bestAbbr);
                        rsContribution = psContribution.executeQuery();
                        while (rsContribution.next()) {
                            int contribution = rsContribution.getInt("class");
                            subSb.append(contribution + "%");
                        }
                    } else {
                        while (rsContribution.next()) {
                            int contribution = rsContribution.getInt("class");
                            subSb.append(contribution + "%");
                        }
                    }
                    sb.append(subSb);
                    sb.append("*");

                }
                System.out.println(id + "\t" + sb.toString());
                pstmt.setString(2, sb.toString());
                pstmt.addBatch();
                if (num % 1000 == 0 || num + 1000 > Integer.parseInt(end)) {
                    System.out.println(Thread.currentThread().getName() + "  " + id + "  " + num);
                    pstmt.executeBatch();
                    conn.commit();
                    pstmt.clearBatch();
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public int gender2int(String gender) {
        if ("female".equals(gender) || "mostly_female".equals(gender)) {
            return 2;
        } else if ("male".equals(gender) || "mostly_male".equals(gender)) {
            return 1;
        } else {
            return 0;
        }
    }


    public static String rejectLowerChar(String s) {
        return s.replaceAll("[a-z]+", "");

    }

    public static int calmed(String a, int i, String b, int j) {
        if (i == 0) return j;
        if (j == 0) return i;
        if (a.charAt(i - 1) == b.charAt(j - 1)) {
            return calmed(a, i - 1, b, j - 1);
        } else {
            int n1 = calmed(a, i - 1, b, j) + 1;
            int n2 = calmed(a, i, b, j - 1) + 1;
            int n3 = calmed(a, i - 1, b, j - 1) + 1;
            return Math.min(Math.min(n1, n2), n3);
        }
    }

    public static String calmSet(String a, HashSet<String> nameabbrs) {
        String result = "";
        int min = 100000;
        for (String s : nameabbrs) {
            int num = calmed(a, a.length(), s, s.length());
            if (num < min) {
                min = num;
                result = s;
            }
        }
        return result;
    }

    public static void main(String[] args) {
        ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("agc-%d").build();
        ExecutorService pool = new ThreadPoolExecutor(10, 20, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(1024), threadFactory);
        for (int i = 0; i < 8; i++) {
            PaperContributionGender po = new PaperContributionGender(String.valueOf(i * 25056), String.valueOf(i * 25056 + 25056));
            pool.execute(po);
        }
        pool.shutdown();
    }


}
