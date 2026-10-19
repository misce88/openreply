import { NextRequest, NextResponse } from "next/server";
import { z } from "zod";
import { getCurrentWorkspaceId } from "@/lib/auth";
import { prisma } from "@/lib/db/client";
import { getFacebookPageCommentsSince } from "@/lib/meta/client";
import { decryptToken } from "@/lib/meta/oauth";
import { getDMQueue } from "@/lib/queue/client";
import { matchKeywords } from "@/lib/utils/keyword-matcher";

export const dynamic = "force-dynamic";
export const maxDuration = 60;

const bodySchema = z.object({
  instagramAccountId: z.string().min(1),
  days: z.number().int().min(1).max(7).default(7),
  dryRun: z.boolean().default(true),
});

/**
 * One-off recovery for Facebook Page comments left while no campaign was on.
 * A private reply is still allowed up to 7 days after the comment. Each
 * recovered comment goes through the normal comment job, so the campaign's
 * texts, public reply and opening DM apply, and anything already answered is
 * skipped by the worker's own log check.
 */
export async function POST(request: NextRequest) {
  const workspaceId = await getCurrentWorkspaceId();
  if (!workspaceId) {
    return NextResponse.json({ success: false, error: "Unauthorized" }, { status: 401 });
  }
  const parsed = bodySchema.safeParse(await request.json().catch(() => ({})));
  if (!parsed.success) {
    return NextResponse.json({ success: false, error: "Invalid input" }, { status: 400 });
  }
  const { instagramAccountId, days, dryRun } = parsed.data;

  const account = await prisma.instagramAccount.findFirst({
    where: { id: instagramAccountId, workspaceId },
    select: { id: true, instagramId: true, accessToken: true },
  });
  if (!account) {
    return NextResponse.json({ success: false, error: "Account not found" }, { status: 404 });
  }

  const automations = await prisma.automation.findMany({
    where: {
      instagramAccountId: account.id,
      isActive: true,
      facebookEnabled: true,
      commentTriggerEnabled: true,
    },
    orderBy: { createdAt: "asc" },
  });

  const since = new Date(Date.now() - days * 24 * 60 * 60 * 1000);
  const { pageId, targets, comments, debug } = await getFacebookPageCommentsSince(
    decryptToken(account.accessToken),
    since
  );

  const rows: Array<Record<string, unknown>> = [];
  const queue = dryRun ? null : getDMQueue();
  let enqueued = 0;

  for (const c of comments) {
    if (c.authorId === pageId) continue; // the Page's own public replies
    const matching = automations.filter(
      (a) => a.matchAnyWord || matchKeywords(c.text, a.keywords, a.wholeWordMatch).matched
    );
    const automation = matching[matching.length - 1]; // newest wins, as in the worker
    if (!automation) continue;

    const suffix = c.id.split("_").pop() ?? c.id;
    const logged = await prisma.dmLog.findFirst({
      where: {
        instagramAccountId: account.id,
        OR: [{ commentId: c.id }, { commentId: { endsWith: `_${suffix}` } }, { commentId: suffix }],
      },
      select: { id: true },
    });

    rows.push({
      comment: c.text.slice(0, 60),
      author: c.authorName,
      when: c.createdTime,
      campaign: automation.name,
      alreadyAnswered: Boolean(logged),
    });

    if (queue && !logged) {
      await queue.add(
        "process-comment",
        {
          instagramAccountId: account.instagramId,
          accountConnectionId: account.id,
          commentId: c.id,
          commentText: c.text,
          commenterId: c.authorId,
          commenterName: c.authorName,
          mediaId: c.postId,
          source: "WEBHOOK",
          platform: "FACEBOOK",
        },
        { jobId: `comment_${account.instagramId}_fb_${c.id}` }
      );
      enqueued++;
    }
  }

  return NextResponse.json({
    success: true,
    data: {
      dryRun,
      postsChecked: targets,
      debug,
      scanned: comments.length,
      matched: rows.length,
      toSend: rows.filter((r) => !r.alreadyAnswered).length,
      enqueued,
      rows,
    },
  });
}
