import { randomUUID } from "node:crypto";
import { eq } from "drizzle-orm";
import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest";
import {
  agents,
  companies,
  createDb,
  environmentLeases,
  environments,
  heartbeatRuns,
  issues,
} from "@paperclipai/db";
import {
  getEmbeddedPostgresTestSupport,
  startEmbeddedPostgresTestDatabase,
} from "./helpers/embedded-postgres.js";
import {
  getConversationOwnershipBlocker,
  waitForConversationOwnershipRelease,
} from "../services/conversation-continuation.js";

const embeddedPostgresSupport = await getEmbeddedPostgresTestSupport();
const describeEmbeddedPostgres = embeddedPostgresSupport.supported ? describe : describe.skip;

if (!embeddedPostgresSupport.supported) {
  console.warn(
    `Skipping embedded Postgres conversation ownership release tests on this host: ${
      embeddedPostgresSupport.reason ?? "unsupported environment"
    }`,
  );
}

// An issue update that hands the issue to another agent cancels the run that
// made the update; that run's environment lease is released a moment later.
// Until then the cancelled run still owns the issue's execution.
describeEmbeddedPostgres("waitForConversationOwnershipRelease", () => {
  let db!: ReturnType<typeof createDb>;
  let tempDb: Awaited<ReturnType<typeof startEmbeddedPostgresTestDatabase>> | null = null;

  beforeAll(async () => {
    tempDb = await startEmbeddedPostgresTestDatabase("paperclip-ownership-release-");
    db = createDb(tempDb.connectionString);
  }, 20_000);

  afterEach(async () => {
    await db.delete(environmentLeases);
    await db.delete(environments);
    await db.delete(issues);
    await db.delete(heartbeatRuns);
    await db.delete(agents);
    await db.delete(companies);
  });

  afterAll(async () => {
    await tempDb?.cleanup();
  });

  async function seedCancelledRunHoldingLease() {
    const companyId = randomUUID();
    const agentId = randomUUID();
    const issueId = randomUUID();
    const runId = randomUUID();
    await db.insert(companies).values({
      id: companyId,
      name: "Paperclip",
      issuePrefix: `T${companyId.replace(/-/g, "").slice(0, 6).toUpperCase()}`,
      requireBoardApprovalForNewAgents: false,
    });
    await db.insert(agents).values({
      id: agentId,
      companyId,
      name: "Author",
      role: "engineer",
      status: "active",
      adapterType: "opencode_local",
      adapterConfig: {},
      runtimeConfig: {},
      permissions: {},
    });
    await db.insert(issues).values({
      id: issueId,
      companyId,
      title: "Handed to review",
      status: "in_review",
      priority: "medium",
    });
    await db.insert(heartbeatRuns).values({
      id: runId,
      companyId,
      agentId,
      status: "cancelled",
      errorCode: "issue_reassigned",
      invocationSource: "assignment",
      startedAt: new Date(),
      finishedAt: new Date(),
      runtimeMode: "legacy",
      contextSnapshot: { issueId },
      runnerProfileJson: { adapterDispatch: { adapterType: "opencode_local" } },
    });
    const [environment] = await db.insert(environments)
      .values({ name: `lease-${runId}`, driver: "sandbox" }).returning();
    const [lease] = await db.insert(environmentLeases).values({
      companyId,
      environmentId: environment.id,
      heartbeatRunId: runId,
      agentId,
      provider: "local",
      status: "active",
      leasePolicy: "ephemeral",
    }).returning();
    return { companyId, issueId, runId, leaseId: lease.id };
  }

  async function releaseLease(leaseId: string) {
    await db.update(environmentLeases)
      .set({ status: "released", releasedAt: new Date() })
      .where(eq(environmentLeases.id, leaseId));
  }

  it("returns once the cancelled run's lease is released", async () => {
    const { companyId, issueId, runId, leaseId } = await seedCancelledRunHoldingLease();
    // This is the state a wake dispatched right after the update would see.
    const blocker = await getConversationOwnershipBlocker(db, companyId, issueId);
    expect(blocker).toMatchObject({ runId, cause: "execution_owner_active" });

    const waiting = waitForConversationOwnershipRelease(db, {
      companyId, issueId, runId, timeoutMs: 5_000, intervalMs: 20,
    });
    await new Promise((resolve) => setTimeout(resolve, 150));
    await releaseLease(leaseId);

    await expect(waiting).resolves.toBe(true);
    expect(await getConversationOwnershipBlocker(db, companyId, issueId)).toBeNull();
  });

  it("returns false when the run still owns execution at the deadline", async () => {
    const { companyId, issueId, runId } = await seedCancelledRunHoldingLease();
    const startedAt = Date.now();
    await expect(waitForConversationOwnershipRelease(db, {
      companyId, issueId, runId, timeoutMs: 200, intervalMs: 20,
    })).resolves.toBe(false);
    expect(Date.now() - startedAt).toBeGreaterThanOrEqual(200);
  });

  it("does not wait for a run that does not own the issue's execution", async () => {
    const { companyId, issueId } = await seedCancelledRunHoldingLease();
    await expect(waitForConversationOwnershipRelease(db, {
      companyId, issueId, runId: randomUUID(), timeoutMs: 5_000, intervalMs: 20,
    })).resolves.toBe(true);
  });
});
