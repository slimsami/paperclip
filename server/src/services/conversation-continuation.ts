import { hasRequiredWorkspaceRecovery } from "./workspace-restore-recovery-state.js";
import { and, desc, eq, inArray, isNotNull, or, sql } from "drizzle-orm";
import { environmentLeases, heartbeatRunEvents, heartbeatRuns, issueRecoveryActions, type Db } from "@paperclipai/db";
import { readProcessStartedAt } from "./hot-restart.js";
import { hasRemoteTerminationReceipt } from "./remote-execution-termination.js";

// These adapters accept a conversation turn. Retrying a process or webhook can
// replay the action itself, so those adapters retain their recovery contract.
export const CONVERSATION_ADAPTER_TYPES = [
  "claude_local", "codex_local", "cursor", "gemini_local", "opencode_local",
  "pi_local", "grok_local", "kimi_local", "hermes_local",
] as const;

export function isConversationAdapter(adapterType: string): boolean {
  return (CONVERSATION_ADAPTER_TYPES as readonly string[]).includes(adapterType);
}

export const CONVERSATION_CONTINUATION_POLICY = "continue_conversation_v1";

export function hasConversationContinuationPolicy(result: Record<string, unknown> | null | undefined): boolean {
  return result?.workspaceRestoreFailure !== "restore_unsafe_archive" && !hasRequiredWorkspaceRecovery(result) && result?.conversationContinuation === CONVERSATION_CONTINUATION_POLICY;
}

/** Persisted by the server when it claims the run, before remote provisioning. */
export function claimedAdapterType(run: Pick<typeof heartbeatRuns.$inferSelect, "runnerProfileJson">): string | null {
  const dispatch = run.runnerProfileJson?.adapterDispatch as Record<string, unknown> | undefined;
  return typeof dispatch?.adapterType === "string" ? dispatch.adapterType : null;
}

function conversationRunPredicate() {
  return or(
    inArray(sql`${heartbeatRuns.runnerProfileJson}->'adapterDispatch'->>'adapterType'`, [...CONVERSATION_ADAPTER_TYPES]),
    sql`${heartbeatRuns.resultJson}->>'conversationContinuation' = ${CONVERSATION_CONTINUATION_POLICY}`,
    sql`exists (
      select 1 from ${heartbeatRunEvents}
      where ${heartbeatRunEvents.companyId} = ${heartbeatRuns.companyId}
        and ${heartbeatRunEvents.runId} = ${heartbeatRuns.id}
        and ${heartbeatRunEvents.eventType} = 'adapter.invoke'
        and ${inArray(sql`${heartbeatRunEvents.payload}->>'adapterType'`, [...CONVERSATION_ADAPTER_TYPES])}
    )`,
  );
}

/** Recovery must not infer the old adapter from the agent's mutable settings. */
export async function historicalAdapterType(db: Db, run: typeof heartbeatRuns.$inferSelect): Promise<string | null> {
  const selected = claimedAdapterType(run);
  if (selected) return selected;
  const [invocation] = await db.select({ payload: heartbeatRunEvents.payload }).from(heartbeatRunEvents)
    .where(and(eq(heartbeatRunEvents.companyId, run.companyId), eq(heartbeatRunEvents.runId, run.id),
      eq(heartbeatRunEvents.eventType, "adapter.invoke")))
    .orderBy(desc(heartbeatRunEvents.seq)).limit(1);
  const adapterType = invocation?.payload?.adapterType;
  return typeof adapterType === "string" ? adapterType : null;
}

export async function runUsedConversationAdapter(db: Db, run: typeof heartbeatRuns.$inferSelect): Promise<boolean> {
  if (run.resultJson?.workspaceRestoreFailure === "restore_unsafe_archive" || hasRequiredWorkspaceRecovery(run.resultJson)) return false;
  if (hasConversationContinuationPolicy(run.resultJson)) return true;
  const adapterType = await historicalAdapterType(db, run);
  return adapterType !== null && isConversationAdapter(adapterType);
}

/** Only immutable run evidence can retire a historical conversation hold.
 * An agent's current adapter can differ from the one that executed this run.
 * Missing evidence retains the hold; the current agent is never a fallback.
 */
export function conversationRecoveryActionPredicate() {
  return and(
    eq(issueRecoveryActions.cause, "legacy_execution_requires_reconciliation"),
    sql`exists (
      select 1 from ${heartbeatRuns}
      where ${heartbeatRuns.companyId} = ${issueRecoveryActions.companyId}
        and ${heartbeatRuns.id}::text = ${issueRecoveryActions.evidence}->>'runId'
        and coalesce(${heartbeatRuns.nativeIssueId}::text, ${heartbeatRuns.contextSnapshot}->>'issueId') = ${issueRecoveryActions.sourceIssueId}::text
        and ${heartbeatRuns.runtimeMode} = 'legacy'
        and coalesce(${heartbeatRuns.resultJson}->>'workspaceRestoreFailure', '') <> 'restore_unsafe_archive'
        and coalesce(${heartbeatRuns.resultJson}->'workspaceRestoreRecovery'->>'schema', '') <> 'paperclip.workspace-restore-recovery.v1'
        and ${inArray(heartbeatRuns.status, ['failed', 'timed_out', 'interrupted', 'cancelled'])}
        and ${conversationRunPredicate()}
        and ${or(
          sql`${heartbeatRuns.resultJson}->>'conversationContinuation' = ${CONVERSATION_CONTINUATION_POLICY}`,
          eq(heartbeatRuns.status, "interrupted"),
          inArray(heartbeatRuns.errorCode, ["process_lost", "server_shutdown_interrupted", "execution_reconciliation_required"]),
          and(eq(heartbeatRuns.status, "cancelled"), sql`${heartbeatRuns.resultJson}->'executionCancellation'->>'state' = 'acknowledged'`),
        )}
    )`,
  );
}

/** OS liveness probes do not signal or stop the process. Unknown ownership holds. */
function processMayBeAlive(pid: number): boolean {
  try {
    process.kill(pid, 0);
    return true;
  } catch (error) {
    return (error as NodeJS.ErrnoException).code !== "ESRCH";
  }
}

/** A run cancelled by an issue update (reassignment, interrupt) keeps execution
 * ownership until its process exits and its environment lease is released,
 * which lands shortly after the update commits. Wakes dispatched by that update
 * in the meantime would see the cancelled run as the active owner and be
 * recorded as skipped, with nothing to replay them. Waits until `runId` no
 * longer owns the issue's execution; false when it still does at the deadline.
 */
export async function waitForConversationOwnershipRelease(db: Db, input: {
  companyId: string;
  issueId: string;
  runId: string;
  timeoutMs?: number;
  intervalMs?: number;
}): Promise<boolean> {
  const deadline = Date.now() + (input.timeoutMs ?? 30_000);
  for (;;) {
    const blocker = await getConversationOwnershipBlocker(db, input.companyId, input.issueId);
    if (!blocker || blocker.runId !== input.runId) return true;
    if (Date.now() >= deadline) return false;
    await new Promise((resolve) => setTimeout(resolve, input.intervalMs ?? 250));
  }
}

/** A terminal conversation row does not prove that its execution authority ended.
 * Other adapters keep their existing bootstrap and ownership protocols.
 */
export async function getConversationOwnershipBlocker(db: Db, companyId: string, issueId: string) {
  const activeLease = sql`exists (select 1 from ${environmentLeases}
    where ${environmentLeases.companyId} = "heartbeat_runs"."company_id"
      and ${environmentLeases.heartbeatRunId} = "heartbeat_runs"."id"
      and (${environmentLeases.releasedAt} is null
        or ${environmentLeases.status} = 'pending_cleanup'
        or ${environmentLeases.cleanupStatus} = 'failed'))`;
  const candidates = await db.select({ run: heartbeatRuns, activeLease }).from(heartbeatRuns)
    .where(and(
      eq(heartbeatRuns.companyId, companyId), eq(heartbeatRuns.runtimeMode, "legacy"),
      conversationRunPredicate(),
      sql`coalesce(${heartbeatRuns.nativeIssueId}::text, ${heartbeatRuns.contextSnapshot}->>'issueId') = ${issueId}`,
      inArray(heartbeatRuns.status, ["failed", "timed_out", "interrupted", "cancelled"]),
      or(isNotNull(heartbeatRuns.processPid), isNotNull(heartbeatRuns.processGroupId), activeLease,
        sql`${heartbeatRuns.resultJson}->'workspaceRestoreRecovery'->>'schema' = 'paperclip.workspace-restore-recovery.v1'`),
    )).orderBy(desc(heartbeatRuns.createdAt), desc(heartbeatRuns.id));
  for (const { run, activeLease: leaseHeld } of candidates) {
    if (hasRequiredWorkspaceRecovery(run.resultJson)) {
      const leases = await db.select().from(environmentLeases).where(and(
        eq(environmentLeases.companyId, companyId), eq(environmentLeases.heartbeatRunId, run.id),
      ));
      const retainedIds = (run.resultJson!.workspaceRestoreRecovery as { leaseIds?: unknown }).leaseIds;
      const remoteLeases = leases.filter(lease => lease.provider !== "local");
      const remoteStopped = Array.isArray(retainedIds) && retainedIds.length > 0
        && retainedIds.every(id => typeof id === "string" && remoteLeases.some(lease =>
          lease.id === id && hasRemoteTerminationReceipt(lease)))
        && remoteLeases.every(hasRemoteTerminationReceipt);
      if (!remoteStopped) return {
        runId: run.id, agentId: run.agentId, cause: "execution_owner_active",
        nextAction: "The retained workspace environment has not confirmed that it stopped. Wait for cleanup before continuing this task.",
      };
      // These PIDs came from the sandbox. A matching process on this host is
      // unrelated and must not hide the repair action or block a repaired task.
      // A separate local lease keeps the existing host process checks below.
      if (leases.every(lease => lease.provider !== "local")) continue;
    }
    let pidAlive = run.processPid !== null && processMayBeAlive(run.processPid);
    if (pidAlive && run.processStartedAt) {
      // A recycled PID cannot keep an old task blocked. An unreadable identity
      // stays conservative; the original process may still own execution.
      const observed = await readProcessStartedAt(run.processPid!).catch(() => null);
      if (observed && new Date(observed).getTime() !== run.processStartedAt.getTime()) pidAlive = false;
    }
    const groupAlive = run.processGroupId !== null && processMayBeAlive(-run.processGroupId);
    if (pidAlive || groupAlive || leaseHeld) {
      return {
        runId: run.id,
        agentId: run.agentId,
        cause: "execution_owner_active",
        nextAction: pidAlive || groupAlive
          ? "The previous provider process is still running. Stop it before continuing this task."
          : "The previous execution has not released its environment lease. Wait for cleanup before continuing this task.",
      };
    }
  }
  return null;
}
