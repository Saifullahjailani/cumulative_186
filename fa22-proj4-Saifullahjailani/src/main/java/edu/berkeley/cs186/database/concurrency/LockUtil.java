package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;

import java.lang.invoke.SwitchPoint;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Stack;

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
        if(requestType == LockType.NL){
            return;
        }
        // Do nothing if the transaction or lockContext is null
        TransactionContext transaction = TransactionContext.getTransaction();
        if (transaction == null || lockContext == null) return;

        // You may find these variables useful
        LockContext parentContext = lockContext.parentContext();
        LockType effectiveLockType = lockContext.getEffectiveLockType(transaction);
        LockType explicitLockType = lockContext.getExplicitLockType(transaction);

        // TODO(proj4_part2): implement
        // The current lock type can effectively substitute the requested type
        if(LockType.substitutable(effectiveLockType, requestType)){
            return;
        }
        // The current lock type is IX and the requested lock is S
        // The current lock type is an intent lock
        // None of the above: In this case, consider what values the explicit
        //  * lock type can be, and think about how ancestor looks will need to be
        //  * acquired or changed.
        // requestType is always S or X at this point

        if(requestType == LockType.S){

           handleSLock(explicitLockType, lockContext, transaction);

        }else {
            handleXLock(explicitLockType, lockContext, transaction);
        }

    }

    // TODO(proj4_part2) add any helper methods you want
    private static Stack<LockContext> path(LockContext ctx){
        Stack<LockContext> path = new Stack<>();
        LockContext par = ctx.parent;
        while(par != null){
            path.add(par);
            par = par.parent;
        }
        return path;
    }

    private static void processParentSLock(LockContext lockContext, TransactionContext transaction){
        Stack<LockContext> parents = path(lockContext);
        while (!parents.isEmpty()) {
            LockContext ctx = parents.pop();
            LockType parentLockType = ctx.getExplicitLockType(transaction);
            if (parentLockType.equals(LockType.NL)) {
                ctx.acquire(transaction, LockType.IS);
            }
        }
    }
    private static void handleSLock(LockType explicitLockType, LockContext lockContext, TransactionContext transaction){
        processParentSLock(lockContext,  transaction);
        switch (explicitLockType){
            case NL:
                lockContext.acquire(transaction, LockType.S);
                break;
            case IS:
                lockContext.escalate(transaction);
                break;
            default:
                lockContext.promote(transaction, LockType.SIX);
        }
    }

    private static void processParentXLock(LockContext lockContext, TransactionContext transaction){
        Stack<LockContext> parents = path(lockContext);
        while (!parents.isEmpty()) {
            LockContext ctx = parents.pop();
            LockType parentLockType = ctx.getExplicitLockType(transaction);
            switch (parentLockType){
                case NL:
                    ctx.acquire(transaction, LockType.IX);
                    break;
                case IS:
                    ctx.promote(transaction, LockType.IX);
                    break;
                case S:
                    ctx.promote(transaction, LockType.SIX);
                    break;
            }

        }
    }
    private static void handleXLock(LockType explicitLockType, LockContext lockContext, TransactionContext transaction) {
       processParentXLock(lockContext, transaction);
       // process the lock it self
       switch (explicitLockType){
           case IS:
               lockContext.escalate(transaction);
               lockContext.promote(transaction, LockType.X);
               break;
           case NL:
               lockContext.acquire(transaction, LockType.X);
               break;
           case S:
               lockContext.promote(transaction, LockType.X);
               break;
           default:
               lockContext.escalate(transaction);
       }

    }
}
