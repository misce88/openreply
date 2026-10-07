import { prisma } from '@/lib/db/client';
import { getDMQueue, MESSAGE_JOB_NAME, POSTBACK_JOB_NAME } from '@/lib/queue/client';
import { parseCommentEvents, parseMessageEvents, parsePostbackEvents, parseReadEvents, parseFacebookPageCommentEvents } from '@/lib/meta/webhook';
import { resolvePageIdForToken } from '@/lib/meta/client';
import { decryptToken } from '@/lib/meta/oauth';
import { Prisma, type InstagramProvider } from '@/app/generated/prisma/client';

const OPENING_DM_READ_FALLBACK_DELAY_MS = 5 * 60 * 1000;
type InstagramPayload = Parameters<typeof parseCommentEvents>[0];

export async function processInstagramWebhook({ payload: incoming, provider, workspaceId }: {
  payload: InstagramPayload; provider: InstagramProvider; workspaceId?: string;
}) {
  if ((incoming.object as string) === 'page' && Array.isArray(incoming.entry)) {
    await processFacebookPageWebhook(incoming, provider);
    return;
  }
  if (incoming.object !== 'instagram' || !Array.isArray(incoming.entry)) return;
  const accounts = await prisma.instagramAccount.findMany({
    where: { instagramId: { in: incoming.entry.map(e => e.id) }, provider, ...(workspaceId ? { workspaceId } : {}) },
    select: { id: true, instagramId: true, workspaceId: true },
  });
  const accountMap = new Map(accounts.map(a => [a.instagramId, a]));
  const allowed = new Set(accountMap.keys());
  const payload = { ...incoming, entry: incoming.entry.filter(e => allowed.has(e.id)) };
  if (!payload.entry.length) return;
  const webhookEvent = await prisma.webhookEvent.create({
    data: {
      object:
        typeof payload === "object" && payload && "object" in payload
          ? String(payload.object)
          : null,
      payload: payload as unknown as Prisma.InputJsonValue,
      ...(workspaceId ? { workspaceId } : {}),
      status: "PENDING",
    },
  });

  try {
    const commentEvents = parseCommentEvents(
      payload as Parameters<typeof parseCommentEvents>[0]
    );
    const queue = getDMQueue();

    for (const event of commentEvents) {
      const account = accountMap.get(event.instagramAccountId);
      if (!account) continue;

      await queue.add(
        "process-comment",
        {
          instagramAccountId: event.instagramAccountId,
          accountConnectionId: accountMap.get(event.instagramAccountId)?.id,
          commentId: event.commentId,
          commentText: event.commentText,
          commenterId: event.commenterId,
          commenterName: event.commenterName,
          mediaId: event.mediaId,
          originalMediaId: event.originalMediaId,
          source: "WEBHOOK",
        },
        {
          jobId: `comment_${event.instagramAccountId}_${event.commentId}`,
        }
      );

      if (account) {
        await prisma.webhookEvent.update({
          where: { id: webhookEvent.id },
          data: { workspaceId: account.workspaceId },
        });
      }
    }

    // Button taps from opening DMs → deliver the reveal message.
    const postbackEvents = parsePostbackEvents(
      payload as Parameters<typeof parsePostbackEvents>[0]
    );

    for (const event of postbackEvents) {
      await queue.add(
        POSTBACK_JOB_NAME,
        {
          instagramAccountId: event.instagramAccountId,
          accountConnectionId: accountMap.get(event.instagramAccountId)?.id,
          userId: event.userId,
          payload: event.payload,
          mid: event.mid,
        },
        {
          // BullMQ forbids ":" in custom job ids, and the payload is
          // "reveal:<id>", so build with underscores and strip any colons.
          jobId: `postback_${event.instagramAccountId}_${event.userId}_${(
            event.mid ?? event.payload
          ).replace(/:/g, "_")}`,
        }
      );
    }

    // Inbound DMs → keyword-triggered autoreply.
    const messageEvents = parseMessageEvents(
      payload as Parameters<typeof parseMessageEvents>[0]
    );

    for (const event of messageEvents) {
      const account = accountMap.get(event.instagramAccountId);
      if (!account) continue;

      await queue.add(
        MESSAGE_JOB_NAME,
        {
          instagramAccountId: event.instagramAccountId,
          accountConnectionId: accountMap.get(event.instagramAccountId)?.id,
          messageId: event.messageId,
          messageText: event.messageText,
          senderId: event.senderId,
        },
        {
          // Message ids can contain characters BullMQ rejects in a job id (":"
          // in particular). base64url encodes into exactly the allowed alphabet
          // and stays injective — substituting invalid characters would let two
          // distinct mids collapse onto one job id, silently dropping a reply.
          jobId: `message_${event.instagramAccountId}_${Buffer.from(
            event.messageId
          ).toString("base64url")}`,
        }
      );

      if (account) {
        await prisma.webhookEvent.update({
          where: { id: webhookEvent.id },
          data: { workspaceId: account.workspaceId },
        });
      }
    }

    // If a user reads the opening DM and never taps the button, deliver the
    // same next-step DM after five minutes. The worker no-ops this delayed job
    // if a real button tap has already delivered the reveal.
    const readEvents = parseReadEvents(
      payload as Parameters<typeof parseReadEvents>[0]
    );

    for (const event of readEvents) {
      const openingLogs = await prisma.dmLog.findMany({
        where: {
          commenterId: event.userId,
          status: "SENT",
          automation: {
            isActive: true,
            openingDmEnabled: true,
            instagramAccount: {
              instagramId: event.instagramAccountId,
            },
          },
        },
        select: {
          automation: {
            select: {
              id: true,
            },
          },
        },
      });

      const scheduledAutomationIds = new Set<string>();
      for (const log of openingLogs) {
        const automation = log.automation;
        if (scheduledAutomationIds.has(automation.id)) continue;
        scheduledAutomationIds.add(automation.id);

        await queue.add(
          POSTBACK_JOB_NAME,
          {
            instagramAccountId: event.instagramAccountId,
          accountConnectionId: accountMap.get(event.instagramAccountId)?.id,
            userId: event.userId,
            payload: `reveal:${automation.id}`,
            fallback: true,
          },
          {
            delay: OPENING_DM_READ_FALLBACK_DELAY_MS,
            jobId: `read_fallback_${event.instagramAccountId}_${event.userId}_${automation.id}`,
          }
        );
      }
    }

    await prisma.webhookEvent.update({
      where: { id: webhookEvent.id },
      data: {
        status: "PROCESSED",
        processedAt: new Date(),
      },
    });

    return;
  } catch (error) {
    const message = error instanceof Error ? error.message : "Unknown error";
    await prisma.webhookEvent.update({
      where: { id: webhookEvent.id },
      data: {
        status: "FAILED",
        errorMessage: message,
        processedAt: new Date(),
      },
    });

    throw error;
  }
}

async function processFacebookPageWebhook(incoming: InstagramPayload, provider: InstagramProvider) {
  const events = parseFacebookPageCommentEvents(incoming as Parameters<typeof parseFacebookPageCommentEvents>[0]);
  if (!events.length) return;

  const accounts = await prisma.instagramAccount.findMany({
    where: { provider },
    select: { id: true, instagramId: true, workspaceId: true, accessToken: true },
  });
  const byPage = new Map<string, (typeof accounts)[number]>();
  for (const account of accounts) {
    try {
      byPage.set(await resolvePageIdForToken(decryptToken(account.accessToken)), account);
    } catch {
      // Not a Page token, or an expired one: this account cannot serve Page comments.
    }
  }

  const matched = events.filter((event) => byPage.has(event.pageId));
  if (!matched.length) return;

  const webhookEvent = await prisma.webhookEvent.create({
    data: {
      object: 'page',
      payload: incoming as unknown as Prisma.InputJsonValue,
      workspaceId: byPage.get(matched[0].pageId)!.workspaceId,
      status: 'PENDING',
    },
  });

  try {
    const queue = getDMQueue();
    for (const event of matched) {
      const account = byPage.get(event.pageId)!;
      await queue.add(
        'process-comment',
        {
          instagramAccountId: account.instagramId,
          accountConnectionId: account.id,
          commentId: event.commentId,
          commentText: event.commentText,
          commenterId: event.commenterId,
          commenterName: event.commenterName,
          mediaId: event.postId,
          source: 'WEBHOOK',
          platform: 'FACEBOOK',
        },
        { jobId: `comment_${account.instagramId}_fb_${event.commentId}` }
      );
    }
    await prisma.webhookEvent.update({
      where: { id: webhookEvent.id },
      data: { status: 'PROCESSED', processedAt: new Date() },
    });
  } catch (error) {
    await prisma.webhookEvent.update({
      where: { id: webhookEvent.id },
      data: {
        status: 'FAILED',
        errorMessage: error instanceof Error ? error.message : 'Unknown error',
        processedAt: new Date(),
      },
    });
    throw error;
  }
}
