#include "linearizability_checker.h"

#include <algorithm>
#include <functional>
#include <map>
#include <sstream>
#include <unordered_set>

namespace syncrepl {

bool LinearizabilityChecker::check_key(const std::vector<Op>& ops, std::string* violation) const {
    int n = static_cast<int>(ops.size());
    std::unordered_set<std::string> dead;  // memoized (bitmask, value) states known to fail

    std::function<bool(uint32_t, const std::string&)> dfs = [&](uint32_t mask, const std::string& value) -> bool {
        if (mask == (1u << n) - 1u) return true;
        std::string state_key = std::to_string(mask) + "|" + value;
        if (dead.count(state_key)) return false;

        for (int i = 0; i < n; ++i) {
            if (mask & (1u << i)) continue;
            // Real-time ordering: op i cannot go next if some other
            // not-yet-applied op j is known to have completed strictly
            // before i started.
            bool blocked = false;
            for (int j = 0; j < n; ++j) {
                if (j == i || (mask & (1u << j))) continue;
                if (ops[j].end_ns <= ops[i].start_ns) {
                    blocked = true;
                    break;
                }
            }
            if (blocked) continue;

            if (ops[i].is_write) {
                if (dfs(mask | (1u << i), ops[i].value)) return true;
            } else if (ops[i].value == value) {
                if (dfs(mask | (1u << i), value)) return true;
            }
        }
        dead.insert(state_key);
        return false;
    };

    bool ok = dfs(0, "");
    if (!ok && violation) {
        std::ostringstream os;
        os << "no linearization found for " << n << " operations on this key";
        *violation = os.str();
    }
    return ok;
}

CheckResult LinearizabilityChecker::check(const std::vector<Op>& history) const {
    CheckResult result;
    std::map<uint32_t, std::vector<Op>> by_key;
    for (const auto& op : history) {
        if (!op.success) continue;  // a failed op never happened, from the client's perspective
        by_key[op.key].push_back(op);
    }

    for (auto& [key, ops] : by_key) {
        std::sort(ops.begin(), ops.end(), [](const Op& a, const Op& b) { return a.start_ns < b.start_ns; });
        if (static_cast<int>(ops.size()) > max_ops_per_key_) {
            result.skipped_keys++;
            continue;
        }
        std::string violation;
        if (!check_key(ops, &violation)) {
            result.linearizable = false;
            result.violation_detail = "key " + std::to_string(key) + ": " + violation;
            result.checked_keys++;
            result.total_ops_checked += static_cast<int>(ops.size());
            return result;  // first violation is enough to report
        }
        result.checked_keys++;
        result.total_ops_checked += static_cast<int>(ops.size());
    }
    return result;
}

}  // namespace syncrepl
