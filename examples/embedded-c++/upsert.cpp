#include <iostream>
#include <random>
#include <chrono>
#include <unordered_set>
#include "duckdb.hpp"

using namespace std;
using namespace duckdb;

int main(int argc, char* argv[]) {
  DuckDB db(argv[1]);
  uint32_t initial_num_records = stoi(argv[2]);
  uint32_t upsert_num_records = stoi(argv[3]);
  Connection con(db);

  con.Query("CREATE TABLE integers(k UINTEGER PRIMARY KEY, v UINTEGER)");
  con.Query("CREATE TEMP TABLE inserts(k UINTEGER, v UINTEGER)");

  Appender appender(con, "integers");
  Appender appender1(con, "inserts");

  // Obtain a non-deterministic seed value from the operating system.
  std::random_device rd; //
  // Seed a Mersenne Twister 32-bit pseudo-random number engine.
  std::mt19937 gen(rd());
  for (uint32_t idx = 0; idx < initial_num_records; ++idx) {
    appender.BeginRow();
    appender.Append<uint32_t>(idx);
    uint32_t random_value = gen();
    appender.Append<uint32_t>(random_value);
    appender.EndRow();
  }

  appender.Close();

  unordered_set<uint32_t> upsert_keys;
  for (uint32_t idx = 0; idx < upsert_num_records; ++idx) {
    // Define the desired range (optional). For the full 32-bit unsigned range, 
    //    no explicit distribution is needed, as gen() already produces values 
    //    between 0 and 4,294,967,295.
    //    If a specific range is needed, use std::uniform_int_distribution.

    // Generate the random 32-bit unsigned integer.
    uint32_t random_key = gen();
    auto it = upsert_keys.find(random_key);
    if (it == upsert_keys.end()) {
      upsert_keys.insert(random_key);
    } else {
      cout << "duplicate random key" << endl;
      continue;
    }
    uint32_t random_value = gen();
    appender1.BeginRow();
    appender1.Append<uint32_t>(random_key);
    appender1.Append<uint32_t>(random_value);
    appender1.EndRow();
  }

  appender1.Close();
  auto start = std::chrono::steady_clock::now();
  con.Query("INSERT INTO integers (k,v) SELECT k,v FROM inserts ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.v");
  auto end = std::chrono::steady_clock::now();

  // Calculate the duration and cast it to nanoseconds
  auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start);

  std::cout << "Upsert Time: " << duration.count() << " nanoseconds" << std::endl;
  return 0;
}
