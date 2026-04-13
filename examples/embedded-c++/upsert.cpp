#include "duckdb.hpp"
#include <iostream>

using namespace std;
using namespace duckdb;

int main(int argc, char* argv[]) {
  DuckDB db(argv[1]);
  Connection con(db);

  con.Query("CREATE TABLE integers(k UINTEGER PRIMARY KEY, v UINTEGER)");
  con.Query("CREATE TEMP TABLE inserts(k UINTEGER, v UINTEGER)");

  Appender appender(con, "integers");
  Appender appender1(con, "inserts");


  appender.AppendRow(10, 230);
  appender.AppendRow(2, 30);
  appender.AppendRow(5, 23);
  appender.AppendRow(4, 2230);
  appender.AppendRow(15, 2350);
  appender.AppendRow(1, 2130);
  appender.AppendRow(110, 2830);
  appender.AppendRow(200, 2305);
  appender.AppendRow(3, 2130);
  appender.AppendRow(9,230);  
  appender.Close();

  appender1.AppendRow(300, 5);
  appender1.AppendRow(3, 52);
  appender1.AppendRow(500, 54);
  appender1.AppendRow(2, 25);
  appender1.AppendRow(15, 59);
  appender1.AppendRow(5, 67);
  appender1.Close();

  auto start = std::chrono::steady_clock::now();
  //con.Query("PRAGMA explain_output = 'all';");
  //auto plan = con.Query("EXPLAIN (FORMAT HTML) INSERT INTO integers (k,v) SELECT k,v FROM inserts ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.v");
  // plan->Print();
  cout << "Before Upsert" << endl;
  con.Query("INSERT INTO integers (k,v) SELECT k,v FROM inserts ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.v");
  auto end = std::chrono::steady_clock::now();

  // Calculate the duration and cast it to nanoseconds
  auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start);

  std::cout << "Upsert Time: " << duration.count() << " nanoseconds" << std::endl;
  auto result = con.Query("SELECT * FROM integers");
  result->Print();
  return 0;
}

