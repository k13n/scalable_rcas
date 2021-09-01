/**
 * Execute with:
 * java -cp postgresql-42.2.19.jar BtreeQueryPerformance.java
 **/

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Stream;
import java.util.regex.Pattern;

public class BtreeQueryPerformance {
  public static class CASQuery {
    String path;
    long low;
    long high;

    @Override
    public String toString() {
      return path + ";" + low + ";" + high;
    }

    public String getQueryPath() {
      String p = path;
      // encode descendant axis **
      p = p.replaceAll("\\*\\*/", "(.*/)?");
      p = p.replaceAll("\\*\\*", ".*");
      // encode label wildcard * (matches everything until the next path separator
      var matcher = Pattern.compile("([^.])\\*").matcher(p);
      if (matcher.find()) {
        p = matcher.replaceAll(matcher.group(1) + "[^/]*");
      }
      return "^" + p + "$";
    }
  }

  public static class Result {
    long runtimeMs;
    long nrMatches;

    @Override
    public String toString() {
      return runtimeMs + ";" + nrMatches;
    }
  }

  private static final boolean DEBUG = false;
  private final String table;
  private final ArrayList<CASQuery> queries;
  private final int nrRepetitions;
  private ArrayList<Result> results = new ArrayList<>();


  BtreeQueryPerformance(String table, ArrayList<CASQuery> queries, int nrRepetitions) {
    this.table = table;
    this.queries = queries;
    this.nrRepetitions = nrRepetitions;
  }


  void configureConnection(Connection con, boolean enableIndexVP, boolean enableIndexPV) {
    try {
      Statement st = con.createStatement();
      st.execute("SET enable_seqscan = false;");
      st.execute("UPDATE pg_index SET indisvalid = "+enableIndexVP+" WHERE indexrelid = '"+table+"_idx_vp'::regclass;");
      st.execute("UPDATE pg_index SET indisvalid = "+enableIndexPV+" WHERE indexrelid = '"+table+"_idx_pv'::regclass;");
      st.close();
    } catch (SQLException ex) {
      ex.printStackTrace();
    }
    printConnectionConfig(con);
  }


  void resetConnection(Connection con) {
    try {
      Statement st = con.createStatement();
      st.execute("UPDATE pg_index SET indisvalid = true WHERE indexrelid = '"+table+"_idx_pv'::regclass;");
      st.execute("UPDATE pg_index SET indisvalid = true WHERE indexrelid = '"+table+"_idx_vp'::regclass;");
      st.execute("SET enable_seqscan = true;");
      st.close();
    } catch (SQLException ex) {
      ex.printStackTrace();
    }
    printConnectionConfig(con);
  }


  void printConnectionConfig(Connection con) {
    System.out.println("Configuration:");
    try {
      Statement st = con.createStatement();
      ResultSet resultSet = st.executeQuery("SHOW enable_seqscan");
      if (resultSet.next()) {
        System.out.printf("enable_seqscan: %s\n", resultSet.getString(1));
      }
      resultSet = st.executeQuery("SELECT indisvalid FROM pg_index WHERE indexrelid = '"+table+"_idx_vp'::regclass;");
      if (resultSet.next()) {
        System.out.printf(""+table+"_idx_vp indisvalid: %s\n", resultSet.getString(1));
      }
      resultSet = st.executeQuery("SELECT indisvalid FROM pg_index WHERE indexrelid = '"+table+"_idx_pv'::regclass;");
      if (resultSet.next()) {
        System.out.printf(""+table+"_idx_pv indisvalid: %s\n", resultSet.getString(1));
      }
      st.close();
    } catch (SQLException ex) {
      ex.printStackTrace();
    }
    System.out.println();
  }

  interface QueryExecutor {
    Result call(CASQuery query, String table, Connection con) throws SQLException;
  }

  Result executeCount(CASQuery query, String table, Connection con) throws SQLException {
    Result result = new Result();
    PreparedStatement pst = con.prepareStatement(
        "SELECT COUNT(*) FROM "+table+" WHERE kpath ~ ? AND kvalue BETWEEN ? AND ?");
    pst.setString(1, query.getQueryPath());
    pst.setLong(2, query.low);
    pst.setLong(3, query.high);
    long tStart = System.currentTimeMillis();
    ResultSet rs = pst.executeQuery();
    if (rs.next()) {
      result.nrMatches = rs.getLong(1);
    }
    result.runtimeMs = System.currentTimeMillis() - tStart;
    pst.close();
    return result;
  }


  Result executeSelect(CASQuery query, String table, Connection con) throws SQLException {
    Result result = new Result();
    PreparedStatement pst = con.prepareStatement(
        "SELECT revision FROM "+table+" WHERE kpath ~ ? AND kvalue BETWEEN ? AND ?",
        ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
    pst.setString(1, query.getQueryPath());
    pst.setLong(2, query.low);
    pst.setLong(3, query.high);
    long tStart = System.currentTimeMillis();
    ResultSet rs = pst.executeQuery();
    if (rs.next()) {
      rs.last();
      result.nrMatches = rs.getRow();
    }
    result.runtimeMs = System.currentTimeMillis() - tStart;
    pst.close();
    return result;
  }


  void execute(QueryExecutor executor, boolean enableIndexVP, boolean enableIndexPV) {
    String url = "jdbc:postgresql://localhost:5400/rcas";
    String user = "wellenzohn";
    String password = "";

    try (Connection con = DriverManager.getConnection(url, user, password)) {
      configureConnection(con, enableIndexVP, enableIndexPV);
      for (int i = 0; i < nrRepetitions; ++i) {
        int counter = 0;
        for (var query : queries) {
          var result = executor.call(query, table, con);
          results.add(result);
          System.out.printf("Q%d;%d;%d\n", counter, result.nrMatches, result.runtimeMs);
          if (DEBUG) {
            System.out.println(query);
            System.out.println(result);
            if (counter % 10 == 0) {
              System.out.println("Queries completed: " + counter);
            }
          }
          ++counter;
        }
        System.out.println();
      }
      resetConnection(con);
    } catch (SQLException ex) {
      ex.printStackTrace();
    }

    printStats();
  }


  private void printStats() {
    long runtimeMs = 0;
    long nrMatches = 0;
    for (var result : results) {
      runtimeMs += result.runtimeMs;
      nrMatches += result.nrMatches;
    }
    double runtimeS = runtimeMs / 1000.0d;

    double avgRuntimeMs = runtimeMs / (double)results.size();
    double avgRuntimeS  = runtimeS  / (double)results.size();
    double avgNrMatches = nrMatches / (double)results.size();

    System.out.println("Totals:");
    System.out.printf("runtimeMs: %d\n", runtimeMs);
    System.out.printf("runtimeS:  %f\n", runtimeS);
    System.out.printf("nrMatches: %d\n\n", nrMatches);

    System.out.println("Averages:");
    System.out.printf("runtimeMs: %f\n", avgRuntimeMs);
    System.out.printf("runtimeS:  %f\n", avgRuntimeS);
    System.out.printf("nrMatches: %f\n\n", avgNrMatches);
  }


  public static ArrayList<BtreeQueryPerformance.CASQuery> readQueries(String filename) {
    ArrayList<BtreeQueryPerformance.CASQuery> queries = new ArrayList<>();
    try (Stream<String> stream = Files.lines(Paths.get(filename))) {
      stream.forEach((line) -> {
        BtreeQueryPerformance.CASQuery query = new BtreeQueryPerformance.CASQuery();
        String[] parts = line.split(";");
        query.path = parts[0];
        query.low  = Long.parseLong(parts[1]);
        query.high = Long.parseLong(parts[2]);
        queries.add(query);
      });
    } catch (Exception e) {
      e.printStackTrace();
    }
    return queries;
  }


  public static void main(String[] args) {
    var queries = readQueries(args[0]);
    var table = args[1];
    var compositeIndex = args[2];
    int nrRepetitions = 1;
    boolean enableIndexVP;
    boolean enableIndexPV;
    if (compositeIndex.equals("vp")) {
      enableIndexVP = true;
      enableIndexPV = false;
    } else if (compositeIndex.equals("pv")) {
      enableIndexPV = true;
      enableIndexVP = false;
    } else {
      throw new IllegalArgumentException("composite index must be one of {vp,pv}");
    }
    var exp = new BtreeQueryPerformance(table, queries, nrRepetitions);
    QueryExecutor count  = (q, t, c) -> { return exp.executeCount(q, t, c);  };
    QueryExecutor select = (q, t, c) -> { return exp.executeSelect(q, t, c); };
    // exp.execute(count, enableIndexVP, enableIndexPV);
    exp.execute(select, enableIndexVP, enableIndexPV);
  }
}
