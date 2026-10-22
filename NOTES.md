This describes the use of two tables in Postgres for safely tracking points:

- One table records each ledger entry (award or reversal of points) with a unique constraint to prevent duplicates.
- The other table tracks each member's points balance per month, including a version number for optimistic locking.

To perform an award or reversal, a transaction is used so everything stays consistent even with concurrent operations:
1. The new ledger entry is inserted; if it's a duplicate (same payment and kind), the operation fails.
2. The current points balance and version for the member/month is fetched with a lock to prevent conflicting updates.
3. The balance is updated and the version incremented—but only if the version still matches what was just read, ensuring no conflict (optimistic concurrency control).
4. The transaction is committed, or rolled back if errors occur.

This approach ensures correctness and consistency even when multiple replicas are processing events concurrently, thanks to unique constraints and transactional versioning.

-- Why safe for highly concurrent replicas?
-- - Unique constraint on ledger_entries prevents duplicate awards/reversals.
-- - Optimistic lock/version on member_balances allows safe concurrent updates; only one succeeds, losing tx retries.
-- - Both tables always updated in same transaction, keeping data consistent and correct even with parallel delivery.