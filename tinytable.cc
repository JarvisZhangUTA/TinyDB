#include <assert.h>
#include <vector>
#include <iostream>
#include <sstream>
#include <string>
#include <boost/algorithm/string.hpp>
#include "leveldb/db.h"
#include "leveldb/filter_policy.h"
#include "leveldb/write_batch.h"

using std::string;
using std::vector;
using std::cin;
using std::cout;
using std::cerr;
using std::endl;
using std::stringstream;
using leveldb::ReadOptions;
using leveldb::Options;
using leveldb::Status;
using leveldb::WriteBatch;
using leveldb::WriteOptions;
using leveldb::DB;
using leveldb::NewBloomFilterPolicy;

struct TinyTable {
  DB * lldb; // low-level LevelDB
  vector<string> columns; // column names
};

// low-level GET
  static bool
lldb_get(struct TinyTable * const tt, string &key, string &value)
{
  assert(tt->lldb);
  ReadOptions ropt;
  Status s = tt->lldb->Get(ropt, key, &value);
  assert(s.ok());
  if (s.IsNotFound()) {
    return false;
  } else {
    return true;
  }
}

// low-level SET
  static void
lldb_set(struct TinyTable * const tt, string &key, string &value)
{
  WriteBatch wb;
  Status s;
  WriteOptions wopt;
  wb.Put(key, value);
  s = tt->lldb->Write(wopt, &wb);
  assert(s.ok());
}

// low-level DELETE
  static void
lldb_del(struct TinyTable * const tt, string &key)
{
  WriteOptions wopt;
  Status s;
  s = tt->lldb->Delete(wopt, key);
  assert(s.ok());
}

  static string
combine_key(string &rowname, string &colname)
{
  stringstream skey;
  skey << rowname << "\1" << colname;
  string key = skey.str();
  return key;
}

/**
 * command syntax:
 *  table <column-1> <column-2> ... <column-n>
 */
  static void
cmd_table(struct TinyTable * const tt, vector<string> &params)
{
  if (params.size() > 0) {
    tt->columns = params;
    cout << "Table scheme defined as : |";
  } else if (tt->columns.size() > 0) {
    cout << "Current table scheme: |";
  } else {
    cout << "Table scheme not defined.";
  }
  for (vector<string>::iterator it = tt->columns.begin(); it != tt->columns.end(); it++) {
    cout << (*it) << "|";
  }
  cout << endl;
}

/**
 * Get row(s) from TinyTable
 * command syntax:
 *   getrow <row-name> <row-name> ... <row-name>
 */
  static void
cmd_getrow(struct TinyTable * const tt, vector<string> &params)
{
  // (1) Check if table scheme has been defined.
  if (tt->columns.size() == 0) {
    cerr << "[getrow] table not defined." << endl;
    return;
  }

  // (2) Check parameters
  if (params.size() == 0) {
    cerr << "[getrow] need row name(s)." << endl;
    return;
  }

  // (3) Do getr
  // (4) print results to cout

  // row iterator
  for(vector<string>::iterator ir = params.begin();ir != params.end(); ir++){
    stringstream output;
    output << " [getrow] " << *ir << " ";

    // column iterator
    for (vector<string>::iterator ic = tt->columns.begin();ic != tt->columns.end(); ic++) {
      string key = combine_key(*ir, *ic);
      string value;
      const bool found = lldb_get(tt, key, value);
      if (found) {
        output << value << " ";
      } else {
        output << "[miss]" << " ";
      }
    }
    
    cout << output.str() << endl;
  }
}

/**
 * Insert or overwrite a row to TinyTable.
 * command syntax:
 *   setrow <row-name> <value-1> <value-2> ... <value-N>
 */
  static void
cmd_setrow(struct TinyTable * const tt, vector<string> &params)
{
  if (tt->columns.size() == 0) {
    cerr << "[setrow] table not defined." << endl;
    return;
  }
  if (params.size() != (tt->columns.size() + 1)) {
    cerr << "[setrow] need a row name and " << tt->columns.size() << " values." << endl;
    return;
  }
  vector<string>::iterator it = params.begin();
  string rowname = *it;
  it++;
  for (vector<string>::iterator ic = tt->columns.begin();ic != tt->columns.end(); ic++, it++) {
    string value = *it;
    string key = combine_key(rowname, *ic);
    lldb_set(tt, key, value);
  }
}

/**
 * Delete (multiple) row(s) from TinyTable.
 * command syntax:
 *   delr <row-name> <row-name> .. <row-name>
 */
  static void
cmd_delrow(struct TinyTable * const tt, vector<string> &params)
{
  // (1) Check if table scheme has been defined.
  if (tt->columns.size() == 0) {
    cerr << "[delrow] table not defined." << endl;
    return;
  }
  // (2) Check parameters
  if (params.size() == 0) {
    cerr << "[delrow] Need row name" << endl;
    return;
  }
  // (3) do delrow
  // row iterator
  for(vector<string>::iterator ir = params.begin();ir != params.end(); ir++){
    // column iterator
    for (vector<string>::iterator ic = tt->columns.begin();ic != tt->columns.end(); ic++) {
      string key = combine_key(*ir, *ic);
      lldb_del(tt, key);
    }

    cout << "[delrow] " << *ir << " deleted" << endl;
  }
}

/**
 * Get an element from TinyTable
 * command syntax:
 *   gete <row-name> <column-name>
 */
  static void
cmd_getelem(struct TinyTable * const tt, vector<string> &params)
{
  if (tt->columns.size() == 0) {
    cerr << "[getelem] Table not defined." << endl;
    return;
  }
  if (params.size() != 2) {
    cerr << "[getelem] Need exactly 2 parameters" << endl;
    return;
  }
  string key = combine_key(params[0], params[1]);
  string value;
  const bool found = lldb_get(tt, key, value);
  if (found) {
    cout << "[getelem] Found [" << params[0] << ", " << params[1] << "] value:" << value << endl;
  } else {
    cout << "[getelem] Not found." << endl;
  }
}

/**
 * Set an element at TinyTable
 * command syntax:
 *   sete <row-name> <column-name> <value>
 */
  static void
cmd_setelem(struct TinyTable * const tt, vector<string> &params)
{
  // (1) Check if table scheme has been defined.
  if (tt->columns.size() == 0) {
    cerr << "[setelem] table not defined." << endl;
    return;
  }

  // (2) Check parameters
  if (params.size() != 3) {
    cerr << "[setelem] Need exactly 3 parameters" << endl;
    return;
  }

  // (3) Do set element
  string key = combine_key(params[0], params[1]);
  string value = params[2];
  lldb_set(tt, key, value);
}

/**
 * Delete an element from TinyTable
 * command syntax:
 *   dele <row-name> <column-name>
 */
  static void
cmd_delelem(struct TinyTable * const tt, vector<string> &params)
{
  // (1) Check if table has been defined.
  if (tt->columns.size() == 0) {
    cerr << "[setelem] table not defined." << endl;
    return;
  }
  // (2) Check parameters
  if (params.size() != 2) {
    cerr << "[setelem] Need exactly 2 parameters" << endl;
    return;
  }

  // (3) Do delete element
  string key = combine_key(params[0], params[1]);
  lldb_del(tt, key);

  cout << "[delelem] row " << params[0] << " col " << params[1] << " deleted" << endl;
}

  static bool
next_cmd(string &op, vector<string> &params)
{
  char buf[4096];
  cout << "tinytable > ";
  cout.flush();
  if (cin.eof() == false) {
    cin.getline(buf, sizeof(buf));
    string line = buf;
    vector<string> tokens;
    boost::split(tokens, line, boost::is_any_of(" "), boost::token_compress_on);

    // get 'op'
    op.clear();
    vector<string>::iterator it = tokens.begin();
    if (it != tokens.end()) {
      op = *it;
      it++;
    }

    // get 'params'
    params.clear();
    for (; it!=tokens.end(); ++it) {
      if (it->length() > 0) {
        params.push_back(*it);
      }
    }

    // return false on "exit"
    if (op.compare("exit") == 0) {
      return false;
    } else {
      return true;
    }
  } else {
    return false;
  }
}

  static void
dispatch_cmd(struct TinyTable * const tt, string &op, vector<string> &params)
{
  if (op.empty()) {
    return;
  }

  cout << "Command: " << op;
  for (vector<string>::iterator it = params.begin(); it != params.end(); it++) {
    cout << " " << *it;
  }
  cout << endl;
  if (op == "table") {
    cmd_table(tt, params);
  } else if (op == "setrow") {
    cmd_setrow(tt, params);
  } else if (op == "getrow") {
    cmd_getrow(tt, params);
  } else if (op == "delrow") {
    cmd_delrow(tt, params);
  } else if (op == "setelem") {
    cmd_setelem(tt, params);
  } else if (op == "getelem") {
    cmd_getelem(tt, params);
  } else if (op == "delelem") {
    cmd_delelem(tt, params);
  } else {
    cerr << "Unknown command \"" << op << "\", ignored." << endl;
    return;
  }
}

  static bool
initial_tinytable(struct TinyTable * const tt)
{
  Options options;
  options.create_if_missing = true;
  options.filter_policy = NewBloomFilterPolicy(10);
  options.write_buffer_size = 1u << 24;
  Status s = DB::Open(options, "ttdir", &(tt->lldb));
  if (tt->lldb == NULL) {
    return false;
  }
  tt->columns.clear();
  return true;
}

  static void
save_table_scheme(struct TinyTable * const tt)
{
  // Save it in tt->lldb.
  // NEVER erase any data stored by user. Be careful of the corner cases.
  // TODO: Your code here:

  stringstream stream;
  for (vector<string>::iterator ic = tt->columns.begin();ic != tt->columns.end(); ic++) {
    stream << *ic << "\n";
  }
	string value = stream.str();
  string key = "init_table_scheme";

  lldb_set(tt, key, value);
}

  static void
load_table_scheme(struct TinyTable * const tt)
{
  // load previously stored schemes from tt->lldb
  // TODO: Your code here:

  string key = "init_table_scheme";
  string value;
  const bool found = lldb_get(tt, key, value);
  if (found) {
      string divider = "\n";
      string::size_type pos1, pos2;
      pos2 = value.find(divider);
      pos1 = 0;
      while(string::npos != pos2)
      {
        tt->columns.push_back(value.substr(pos1, pos2-pos1));
        pos1 = pos2 + divider.size();
        pos2 = value.find(divider, pos1);
      }
      if(pos1 != value.length())
        tt->columns.push_back(value.substr(pos1));
      
      cout << "Load scheme" << endl;
  } 
}

  int
main(int argc, char ** argv)
{
  string op;
  vector<string> params;
  struct TinyTable tt = {NULL};
  if (false == initial_tinytable(&tt)) {
    cerr << "Initial TinyTable failed!" << endl;
    exit(1);
  }

  load_table_scheme(&tt);

  cout << "Welcome to TinyTable Console!" << endl;
  cout << "Commands:" << endl;
  cout << " Define THE table scheme  $ table <column-1> <column-2> ..." << endl;
  cout << " Print current scheme     $ table" << endl;
  cout << " Get row(s) by row-key(s) $ getrow  <row-key-1> <row-key-2> ..." << endl;
  cout << " Set one row              $ setrow  <row-key> <value-1> <value-2> ..." << endl;
  cout << " Delete row(s)            $ delrow  <row-key-1> <row-key-2> ..." << endl;
  cout << " Get an element           $ getelem <row-key> <column-key>" << endl;
  cout << " Set an element           $ setelem <row-key> <column-key> <value>" << endl;
  cout << " Delete an element        $ delelem <row-key> <column-key>" << endl;
  cout << " Exit                     $ exit" << endl;
  cout << " ========" << endl;

  // loop until 'exit' or EOF
  while(true == next_cmd(op, params)) {
    dispatch_cmd(&tt, op, params);
  }

  save_table_scheme(&tt);
  if (tt.lldb) delete tt.lldb;
  cout << "Bye." << endl;
  exit(0);
}
