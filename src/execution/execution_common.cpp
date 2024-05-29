#include "execution/execution_common.h"
#include "catalog/catalog.h"
#include "common/config.h"
#include "common/macros.h"
#include "concurrency/transaction_manager.h"
#include "fmt/core.h"
#include "storage/table/table_heap.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

auto ReconstructTuple(const Schema *schema, const Tuple &base_tuple, const TupleMeta &base_meta,
                      const std::vector<UndoLog> &undo_logs) -> std::optional<Tuple> {

  std::vector<Value> values(schema->GetColumnCount());
  bool is_deleted = base_meta.is_deleted_;

  for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {
    values[i] = base_tuple.GetValue(schema, i);
  }

  for (const auto &undo : undo_logs) {
    if ((is_deleted = undo.is_deleted_)) {
      continue;
    }

    std::vector<uint32_t> modified_field_indexes;
    for (size_t i = 0; i < undo.modified_fields_.size(); i++) {
      if (undo.modified_fields_[i]) {
        modified_field_indexes.push_back(i);
      }
    }

    if (!modified_field_indexes.empty()) {
      std::vector<Column>  columns;
      for (size_t i = 0; i < modified_field_indexes.size(); i++) {
        columns.push_back(schema->GetColumn(modified_field_indexes[i]));
      }
      Schema partial_schema(columns);
      for (size_t i = 0; i < modified_field_indexes.size(); i++) {
        const auto idx = modified_field_indexes[i];
        values[idx] = undo.tuple_.GetValue(&partial_schema, i);
      }
    }
  }

  return is_deleted ? std::nullopt : std::make_optional<Tuple>(values, schema);
}

void TxnMgrDbg(const std::string &info, TransactionManager *txn_mgr, const TableInfo *table_info,
               TableHeap *table_heap) {
  // always use stderr for printing logs...
  fmt::println(stderr, "debug_hook: {}", info);

  fmt::println(
      stderr,
      "You see this line of text because you have not implemented `TxnMgrDbg`. You should do this once you have "
      "finished task 2. Implementing this helper function will save you a lot of time for debugging in later tasks.");

  // We recommend implementing this function as traversing the table heap and print the version chain. An example output
  // of our reference solution:
  //
  // debug_hook: before verify scan
  // RID=0/0 ts=txn8 tuple=(1, <NULL>, <NULL>)
  //   txn8@0 (2, _, _) ts=1
  // RID=0/1 ts=3 tuple=(3, <NULL>, <NULL>)
  //   txn5@0 <del> ts=2
  //   txn3@0 (4, <NULL>, <NULL>) ts=1
  // RID=0/2 ts=4 <del marker> tuple=(<NULL>, <NULL>, <NULL>)
  //   txn7@0 (5, <NULL>, <NULL>) ts=3
  // RID=0/3 ts=txn6 <del marker> tuple=(<NULL>, <NULL>, <NULL>)
  //   txn6@0 (6, <NULL>, <NULL>) ts=2
  //   txn3@1 (7, _, _) ts=1
}

}  // namespace bustub
