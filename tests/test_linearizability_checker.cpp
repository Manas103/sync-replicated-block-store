#include <gtest/gtest.h>

#include "linearizability_checker.h"

using namespace syncrepl;

namespace {
Op make_write(uint32_t key, std::string value, uint64_t start, uint64_t end) {
    Op op;
    op.key = key;
    op.is_write = true;
    op.success = true;
    op.value = std::move(value);
    op.start_ns = start;
    op.end_ns = end;
    return op;
}
Op make_read(uint32_t key, std::string value, uint64_t start, uint64_t end) {
    Op op;
    op.key = key;
    op.is_write = false;
    op.success = true;
    op.value = std::move(value);
    op.start_ns = start;
    op.end_ns = end;
    return op;
}
}  // namespace

TEST(LinearizabilityChecker, SequentialHistoryIsLinearizable) {
    std::vector<Op> history = {
        make_write(1, "A", 0, 10),
        make_read(1, "A", 20, 30),
        make_write(1, "B", 40, 50),
        make_read(1, "B", 60, 70),
    };
    LinearizabilityChecker checker;
    CheckResult result = checker.check(history);
    EXPECT_TRUE(result.linearizable);
    EXPECT_EQ(result.checked_keys, 1);
}

TEST(LinearizabilityChecker, ReadOverlappingAWriteMayReturnEitherValue) {
    // Write "B" overlaps the read in real time, so the read returning
    // either the old value "A" or the new value "B" is linearizable.
    std::vector<Op> history = {
        make_write(1, "A", 0, 10),
        make_write(1, "B", 20, 60),
        make_read(1, "A", 30, 40),  // starts after B begins but ends before it commits: still valid as "before B"
    };
    LinearizabilityChecker checker;
    CheckResult result = checker.check(history);
    EXPECT_TRUE(result.linearizable);
}

TEST(LinearizabilityChecker, StaleReadAfterANonOverlappingWriteIsAViolation) {
    // The write to "B" fully completes (end=50) strictly before the read
    // starts (start=60), so real-time ordering forces the read to observe
    // "B"; observing stale "A" is a genuine violation.
    std::vector<Op> history = {
        make_write(1, "A", 0, 10),
        make_write(1, "B", 20, 50),
        make_read(1, "A", 60, 70),
    };
    LinearizabilityChecker checker;
    CheckResult result = checker.check(history);
    EXPECT_FALSE(result.linearizable);
}

TEST(LinearizabilityChecker, ReadOfNeverWrittenValueIsAViolation) {
    std::vector<Op> history = {
        make_write(1, "A", 0, 10),
        make_read(1, "Z", 20, 30),
    };
    LinearizabilityChecker checker;
    CheckResult result = checker.check(history);
    EXPECT_FALSE(result.linearizable);
}

TEST(LinearizabilityChecker, KeysAreCheckedIndependently) {
    // Key 2's history is a violation; key 1's is fine. The checker must
    // still catch the key 2 violation even though key 1's ops interleave
    // with it in the overall history.
    std::vector<Op> history = {
        make_write(1, "A", 0, 10),
        make_write(2, "X", 5, 15),
        make_read(1, "A", 20, 30),
        make_read(2, "nope", 25, 35),
    };
    LinearizabilityChecker checker;
    CheckResult result = checker.check(history);
    EXPECT_FALSE(result.linearizable);
}

TEST(LinearizabilityChecker, FailedOpsAreExcludedFromTheHistory) {
    Op failed_write = make_write(1, "never-applied", 0, 10);
    failed_write.success = false;
    std::vector<Op> history = {
        failed_write,
        make_write(1, "A", 20, 30),
        make_read(1, "A", 40, 50),
    };
    LinearizabilityChecker checker;
    CheckResult result = checker.check(history);
    EXPECT_TRUE(result.linearizable);
}
