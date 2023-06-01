package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;

import java.util.Arrays;
import java.util.List;

/**
 * LockUtil is a declarative layer which simplifies multigranularity lock
 * acquisition for the user (you, in the last task of Part 2). Generally
 * speaking, you should use LockUtil for lock acquisition instead of calling
 * LockContext methods directly.
 */
public class LockUtil {
    /**
     * Ensure that the current transaction can perform actions requiring
     * `requestType` on `lockContext`.
     *
     * `requestType` is guaranteed to be one of: S, X, NL.
     *
     * This method should promote/escalate/acquire as needed, but should only
     * grant the least permissive set of locks needed. We recommend that you
     * think about what to do in each of the following cases:
     * - The current lock type can effectively substitute the requested type
     * - The current lock type is IX and the requested lock is S
     * - The current lock type is an intent lock
     * - None of the above: In this case, consider what values the explicit
     *   lock type can be, and think about how ancestor looks will need to be
     *   acquired or changed.
     *
     * You may find it useful to create a helper method that ensures you have
     * the appropriate locks on all ancestors.
     */
    public static void ensureSufficientLockHeld(LockContext lockContext, LockType requestType) {
        // requestType must be S, X, or NL
        assert (requestType == LockType.S || requestType == LockType.X || requestType == LockType.NL);

        // Do nothing if the transaction or lockContext is null
        TransactionContext transaction = TransactionContext.getTransaction();
        if (transaction == null || lockContext == null) return;

        // You may find these variables useful
        LockContext parentContext = lockContext.parentContext();
        LockType effectiveLockType = lockContext.getEffectiveLockType(transaction);
        LockType explicitLockType = lockContext.getExplicitLockType(transaction);

        if (LockType.substitutable(effectiveLockType, requestType)) {
            return;
        } else if (explicitLockType.equals(LockType.IX) && requestType.equals(LockType.S)) {
            lockContext.promote(transaction, LockType.SIX);
        } else if (explicitLockType.isIntent()) {
            if (!requestType.equals(LockType.NL)) {
                lockContext.escalate(transaction);
                explicitLockType = lockContext.getExplicitLockType(transaction);
                if (explicitLockType.equals(LockType.S) && requestType.equals(LockType.X)) {
                    lockContext.promote(transaction, LockType.X);
                }
            }
        } else {
            ensureAncestorLocks(parentContext, requestType);
            if (explicitLockType.equals(LockType.NL)) {
                lockContext.acquire(transaction, requestType);
            } else {
                lockContext.promote(transaction, requestType);
            }
        }

        // TODO(proj4_part2): implement
        return;
    }

    private static void  ensureAncestorLocks(LockContext parentContext, LockType currentType) {
        TransactionContext transaction = TransactionContext.getTransaction();
        if (transaction == null || parentContext == null) return;

        if (currentType.equals(LockType.NL)) {
            return;
        } else {
            List<LockType> needIX = Arrays.asList(LockType.X, LockType.IX, LockType.SIX);
            LockType parentLockType = parentContext.getExplicitLockType(transaction);
            if (!LockType.canBeParentLock(parentLockType, currentType)) {
                LockType tmp = needIX.contains(currentType) ? LockType.IX : LockType.IS;
                ensureAncestorLocks(parentContext.parent, tmp);
                if (parentLockType.equals(LockType.NL)) {
                    parentContext.acquire(transaction, tmp);
                } else {
                    parentContext.promote(transaction, tmp);
                }

                parentContext = parentContext.parent;
                currentType = tmp;
                }
        }
    }

    // TODO(proj4_part2) add any helper methods you want
}
