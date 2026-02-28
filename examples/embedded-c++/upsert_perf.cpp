#include <iostream>
#include <random>
#include <chrono>
#include <unordered_set>
#include "duckdb/main/appender.hpp"
#include "duckdb.hpp"

using namespace std;
using namespace duckdb;

int main(int argc, char* argv[]) {
  DuckDB db(argv[1]);
  uint32_t initial_num_records = stoi(argv[2]);
  uint32_t upsert_num_records = stoi(argv[3]);
  Connection con(db);

  con.Query("CREATE TABLE key_values(k UINTEGER PRIMARY KEY, v VARCHAR)");
  con.Query("CREATE TEMP TABLE inserts(k UINTEGER, v VARCHAR)");

  Appender appender(con, "key_values");
  Appender appender1(con, "inserts");

  // Obtain a non-deterministic seed value from the operating system.
  std::random_device rd; //
  // Seed a Mersenne Twister 32-bit pseudo-random number engine.
  std::mt19937 gen(rd());
  std::uniform_int_distribution<int> dist_int1(1, 64);
  std::uniform_int_distribution<int> dist_int2(45, 64);

  auto randomStr = [&](uint8_t len) -> string {
    string ret;

    for (int i = 0; i < len; ++i) {
      uint8_t c = dist_int2(gen);
      ret += c;
    }
    return ret;
  };

  for (uint32_t idx = 0; idx < initial_num_records; ++idx) {
    int strl = dist_int1(gen);
    string rstr = randomStr(strl);
    appender.AppendRow(idx, rstr.c_str());
  }

  appender.Close();
  
  unordered_set<uint32_t> upsert_keys;
  for (uint32_t idx = 0; idx < upsert_num_records; ++idx) {
    uint32_t random_key = gen();
    auto it = upsert_keys.find(random_key);
    if (it == upsert_keys.end()) {
      upsert_keys.insert(random_key);
    } else {
      cout << "duplicate random key" << endl;
      continue;
    }
    int strl = dist_int1(gen);
    string rStr = randomStr(strl);

    appender1.AppendRow(random_key, rStr.c_str());
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
