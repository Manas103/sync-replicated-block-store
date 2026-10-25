#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace syncrepl {

// One recorded client operation. Recorded by the test driver from the
// client's own observed wall-clock start/end, not by the nodes, so the
// checker is independent of the implementation under test: it only ever
// sees what the client actually asked for and actually got back.
struct Op {
    uint32_t key = 0;
    bool is_write = false;
    bool success = false;  // ok=true in the RPC reply; a failed (fenced) op never happened and is excluded
    std::string value;      // the value written (is_write) or read back (!is_write)
    uint64_t start_ns = 0;
    uint64_t end_ns = 0;
};

struct CheckResult {
    bool linearizable = true;
    int checked_keys = 0;
    int skipped_keys = 0;  // keys with more touching ops than max_ops_per_key
    int total_ops_checked = 0;
    std::string violation_detail;
};

// An independent, brute-force (memoized, bounded) linearizability checker
// for single-key read/write registers, in the same spirit as the
// portfolio's raft-sharded-kv-store checker: real-time-ordering-pruned
// search over which not-yet-applied operation could go next, memoized on
// (bitmask, current value) rather than bitmask alone, since two different
// orderings of the same operation set can leave the register at different
// values (bitmask-only memoization is unsound for a general register; see
// README).
class LinearizabilityChecker {
public:
    explicit LinearizabilityChecker(int max_ops_per_key = 20) : max_ops_per_key_(max_ops_per_key) {}
    CheckResult check(const std::vector<Op>& history) const;

private:
    bool check_key(const std::vector<Op>& ops, std::string* violation) const;
    int max_ops_per_key_;
};

}  // namespace syncrepl
