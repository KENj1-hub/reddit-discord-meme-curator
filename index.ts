import { CronJob } from "cron";
import { createClient } from "@libsql/client";

const db = createClient({
    url: process.env.DB_URL!,
    authToken: process.env.DB_TOKEN!,
});

const SUBREDDIT_WEIGHTS = {
    memes: 1.2,
    dankmemes: 1.3,
    wholesomememes: 1.0,
    me_irl: 1.2,
    MemeEconomy: 1.1,
    AdviceAnimals: 1.0,
    funny: 0.8,
} as const;

const CONFIG = {
    SUBREDDITS: Object.keys(SUBREDDIT_WEIGHTS) as Array<
        keyof typeof SUBREDDIT_WEIGHTS
    >,
    FETCH_LIMIT: 50,
    MIN_UPVOTES: 100,
    MAX_TITLE_LENGTH: 200,
    MIN_UPVOTE_RATIO: 0.7,
    WEBHOOK_URL: process.env.DISCORD_WEB_URL ?? "",
    REQUEST_TIMEOUT_MS: 10_000,
    MAX_RETRIES: 3,
    MAX_TOP_CANDIDATES: 5,
    AGE_PENALTY_DIVISOR: 8,
    MAX_AGE_PENALTY: 25,
    MAX_ENGAGEMENT_BONUS: 30,
    CLEANUP_DAYS: 365,
    ROTATION_ENABLED: true,
    ROTATION_LOOKBACK_POSTS: 3,
} as const;

type RedditListing = {
    kind: "Listing";
    data: {
        children: { data: RedditPost }[];
    };
};

type RedditPost = {
    id: string;
    title: string;
    permalink: string;
    ups: number;
    num_comments: number;
    over_18: boolean;
    post_hint?: string;
    url: string;
    url_overridden_by_dest?: string;
    is_video: boolean;
    created_utc: number;
    upvote_ratio: number;
    author: string;
    subreddit: string;
    crosspost_parent?: string;
    crosspost_parent_list?: any[];
};

type HealthStatus = {
    status: "healthy" | "degraded" | "unhealthy";
    lastPost: number | null;
    hoursSinceLastPost: number | null;
    dbConnected: boolean;
};

async function initDb() {
    await db.execute(`
    CREATE TABLE IF NOT EXISTS posted_memes (
      id TEXT PRIMARY KEY,
      subreddit TEXT NOT NULL,
      posted_at INTEGER NOT NULL
    );
  `);

    // Create index for cleanup queries
    await db.execute(`
    CREATE INDEX IF NOT EXISTS idx_posted_at ON posted_memes(posted_at);
  `);

    // Create index for subreddit rotation queries
    await db.execute(`
    CREATE INDEX IF NOT EXISTS idx_subreddit_posted_at ON posted_memes(subreddit, posted_at DESC);
  `);

    console.log("✅ Database initialized");
}

async function getPostedIds(): Promise<Set<string>> {
    const result = await db.execute("SELECT id FROM posted_memes");
    const postedIds = new Set(result.rows.map((r) => String(r.id)));
    console.log(`📦 Loaded ${postedIds.size} posted IDs`);
    return postedIds;
}

async function getRecentSubreddits(limit: number): Promise<string[]> {
    const result = await db.execute({
        sql: `
      SELECT DISTINCT subreddit
      FROM posted_memes
      ORDER BY posted_at DESC
      LIMIT ?
    `,
        args: [limit],
    });

    const recentSubreddits = result.rows.map((r) =>
        String(r.subreddit).toLowerCase(),
    );
    console.log(
        `📊 Recent subreddits (last ${limit}): ${recentSubreddits.join(", ")}`,
    );
    return recentSubreddits;
}

async function getSubredditUsageStats(): Promise<Map<string, number>> {
    // Get usage count for each subreddit in the last 24 hours
    const oneDayAgo = Date.now() - 24 * 60 * 60 * 1000;

    const result = await db.execute({
        sql: `
      SELECT subreddit, COUNT(*) as count
      FROM posted_memes
      WHERE posted_at > ?
      GROUP BY subreddit
    `,
        args: [oneDayAgo],
    });

    const stats = new Map<string, number>();
    for (const row of result.rows) {
        stats.set(String(row.subreddit).toLowerCase(), Number(row.count));
    }

    return stats;
}

async function storePosted(post: RedditPost) {
    await db.execute({
        sql: `
      INSERT OR IGNORE INTO posted_memes (id, subreddit, posted_at)
      VALUES (?, ?, ?)
    `,
        args: [post.id, post.subreddit, Date.now()],
    });
}

async function cleanupOldPosts() {
    const cutoffTime = Date.now() - CONFIG.CLEANUP_DAYS * 24 * 60 * 60 * 1000;

    const result = await db.execute({
        sql: "DELETE FROM posted_memes WHERE posted_at < ?",
        args: [cutoffTime],
    });

    if (result.rowsAffected > 0) {
        console.log(`🧹 Cleaned up ${result.rowsAffected} old posts`);
    }
}

async function safeFetch(
    url: string,
    options: RequestInit = {},
    attempt = 0,
): Promise<Response> {
    const controller = new AbortController();
    const timeout = setTimeout(
        () => controller.abort(),
        CONFIG.REQUEST_TIMEOUT_MS,
    );

    try {
        const res = await fetch(url, {
            ...options,
            signal: controller.signal,
            headers: {
                "User-Agent": "meme-bot-by-anish",
                ...(options.headers || {}),
            },
        });

        if (!res.ok) {
            const text = await res.text();
            console.error("Reddit error:", res.status, text);
            throw new Error(`HTTP ${res.status}`);
        }

        return res;
    } catch (err) {
    if (attempt < CONFIG.MAX_RETRIES) {
        const delayMs = 5000 * (attempt + 1);

        console.warn(
            `⚠️ Fetch failed. Retrying in ${delayMs / 1000}s (attempt ${attempt + 1}/${CONFIG.MAX_RETRIES})...`,
        );

        await new Promise((r) => setTimeout(r, delayMs));

        return safeFetch(url, options, attempt + 1);
    }

    throw err;
          } finally {
        clearTimeout(timeout);
    }
}

type MemeApiPost = {
    postLink: string;
    subreddit: string;
    title: string;
    url: string;
    nsfw: boolean;
    spoiler: boolean;
    author: string;
    ups: number;
    preview?: string[];
};

type MemeApiResponse = {
    count: number;
    memes: MemeApiPost[];
};

async function fetchPosts(): Promise<RedditPost[]> {
    const posts: RedditPost[] = [];

    const requests = CONFIG.SUBREDDITS.map(async (subreddit) => {
        try {
            const url = `https://meme-api.com/gimme/${subreddit}/10`;
            const response = await safeFetch(url);

            const json = (await response.json()) as MemeApiResponse;

            if (!json.memes || !Array.isArray(json.memes)) {
                console.warn(`⚠️ No memes returned for r/${subreddit}`);
                return;
            }

            for (const meme of json.memes) {
                const id =
                    meme.postLink.split("/").filter(Boolean).pop() ??
                    crypto.randomUUID();

                posts.push({
                    id,
                    title: meme.title,
                    permalink: new URL(meme.postLink).pathname,
                    ups: meme.ups ?? 0,
                    num_comments: 0,
                    over_18: meme.nsfw ?? false,
                    post_hint: "image",
                    url: meme.url,
                    url_overridden_by_dest: meme.url,
                    is_video: false,
                    created_utc: Math.floor(Date.now() / 1000),
                    upvote_ratio: 1,
                    author: meme.author ?? "unknown",
                    subreddit: meme.subreddit ?? subreddit,
                });
            }

            console.log(
                `✅ Fetched ${json.memes.length} memes from r/${subreddit}`,
            );
        } catch (err) {
            console.error(`❌ Failed to fetch r/${subreddit}:`, err);
        }
    });

    await Promise.allSettled(requests);

    console.log(`📥 Fetched ${posts.length} memes via Meme API`);

    return posts;
}
function isValidImage(post: RedditPost, postedIds: Set<string>): boolean {
    // Basic filters
    if (post.over_18) return false;
    if (post.is_video) return false;
    if (post.ups < CONFIG.MIN_UPVOTES) return false;
    if (post.upvote_ratio < CONFIG.MIN_UPVOTE_RATIO) return false;

    // Check if already posted
    if (postedIds.has(post.id)) return false;

    // Filter crossposts - check if the original was already posted
    if (post.crosspost_parent) {
        const parentId = post.crosspost_parent.split("_")[1]; // Format: t3_xxxxx
        if (parentId && postedIds.has(parentId)) {
            return false;
        }
    }

    // Validate image URL
    const imageUrl = post.url_overridden_by_dest ?? post.url;
    return /\.(jpg|jpeg|png|gif|webp|gifv)$/i.test(imageUrl);
}

function scorePost(
    post: RedditPost,
    recentSubreddits: string[],
    usageStats: Map<string, number>,
): number {
    let score = 0;

    // Upvote score (logarithmic scaling) - 0 to ~300 points
    score += Math.log10(post.ups + 1) * 100;

    // Upvote ratio bonus - 0 to 50 points
    score += post.upvote_ratio * 50;

    // Engagement ratio (comments/upvotes) - 0 to 30 points
    const engagementRatio = post.num_comments / (post.ups + 1);
    score += Math.min(engagementRatio * 20, CONFIG.MAX_ENGAGEMENT_BONUS);

    // Freshness bonus/penalty based on age
    const ageHours = (Date.now() / 1000 - post.created_utc) / 3600;

    // Posts 0-2 hours old get bonus
    if (ageHours < 2) {
        score += (2 - ageHours) * 10; // Up to +20 for very fresh posts
    } else {
        // Older posts get penalty
        score -= Math.min(
            (ageHours - 2) / CONFIG.AGE_PENALTY_DIVISOR,
            CONFIG.MAX_AGE_PENALTY,
        );
    }

    // Subreddit quality weight
    const subredditWeight =
        SUBREDDIT_WEIGHTS[
        post.subreddit.toLowerCase() as keyof typeof SUBREDDIT_WEIGHTS
        ] ?? 1.0;
    score *= subredditWeight;

    // Penalty for crossposts (prefer original content)
    if (post.crosspost_parent) {
        score *= 0.8;
    }

    // Title quality heuristics
    const titleLower = post.title.toLowerCase();

    // Slight penalty for clickbait-y titles
    if (
        titleLower.includes("upvote") ||
        titleLower.includes("updoot") ||
        titleLower.includes("award")
    ) {
        score *= 0.9;
    }

    // Bonus for OC
    if (titleLower.includes("[oc]") || titleLower.includes("(oc)")) {
        score *= 1.1;
    }

    // ROTATION LOGIC: Penalize recently used subreddits
    if (CONFIG.ROTATION_ENABLED) {
        const subredditLower = post.subreddit.toLowerCase();

        // Heavy penalty for subreddits used very recently
        const recentIndex = recentSubreddits.indexOf(subredditLower);
        if (recentIndex !== -1) {
            // The more recent, the heavier the penalty
            // Last post: 50% penalty, 2nd last: 30%, 3rd last: 15%
            const penaltyFactor = 1 - 0.5 / (recentIndex + 1);
            score *= penaltyFactor;
            console.log(
                `  ⚖️  Rotation penalty for r/${post.subreddit}: ${((1 - penaltyFactor) * 100).toFixed(0)}%`,
            );
        }

        // Additional penalty based on 24h usage
        const usageCount = usageStats.get(subredditLower) ?? 0;
        if (usageCount > 0) {
            // Each post in last 24h reduces score by 10%
            const usagePenalty = Math.pow(0.9, usageCount);
            score *= usagePenalty;
            console.log(
                `  📊 Usage penalty for r/${post.subreddit} (${usageCount} posts in 24h): ${((1 - usagePenalty) * 100).toFixed(0)}%`,
            );
        }
    }

    return score;
}

function selectBestPost(
    posts: RedditPost[],
    postedIds: Set<string>,
    recentSubreddits: string[],
    usageStats: Map<string, number>,
): RedditPost | null {
    const candidates = posts.filter((p) => isValidImage(p, postedIds));

    if (!candidates.length) {
        console.log("❌ No valid candidates found");
        return null;
    }

    console.log(`✅ Found ${candidates.length} valid candidates`);

    // Group candidates by subreddit for diversity check
    const bySubreddit = new Map<string, RedditPost[]>();
    for (const post of candidates) {
        const sub = post.subreddit.toLowerCase();
        if (!bySubreddit.has(sub)) {
            bySubreddit.set(sub, []);
        }
        bySubreddit.get(sub)!.push(post);
    }

    console.log(`📂 Candidates from ${bySubreddit.size} different subreddits`);

    // Score and sort all candidates
    const scored = candidates.map((post) => ({
        post,
        score: scorePost(post, recentSubreddits, usageStats),
    }));

    scored.sort((a, b) => b.score - a.score);

    // Log top candidates for debugging
    console.log("\n🏆 Top candidates:");
    scored.slice(0, 10).forEach((item, i) => {
        console.log(
            `  ${i + 1}. [${item.score.toFixed(1)}] r/${item.post.subreddit} - ${item.post.title.slice(0, 50)}...`,
        );
    });

    // Select randomly from top N to add variety while respecting rotation
    const topCandidates = scored.slice(
        0,
        Math.min(CONFIG.MAX_TOP_CANDIDATES, scored.length),
    );
    const selected =
        topCandidates[Math.floor(Math.random() * topCandidates.length)];

    return selected?.post ?? null;
}

function formatNumber(num: number): string {
    return new Intl.NumberFormat("en-US", {
        notation: "compact",
        compactDisplay: "short",
        maximumFractionDigits: 1,
    }).format(num);
}

async function sendToDiscord(post: RedditPost) {
    const imageUrl = post.url_overridden_by_dest ?? post.url;
    const redditUrl = `https://reddit.com${post.permalink}`;

    let title = post.title;
    if (title.length > CONFIG.MAX_TITLE_LENGTH) {
        title = title.slice(0, CONFIG.MAX_TITLE_LENGTH - 3) + "...";
    }

    const payload = {
        content: `Posted on <t:${post.created_utc}:F> (<t:${post.created_utc}:R>)`, // Discord relative timestamp
        embeds: [
            {
                title,
                url: redditUrl,
                image: { url: imageUrl },
                color: 0x57F287,
                footer: {
                    text: `r/${post.subreddit} • 👍 ${formatNumber(post.ups)} | 💬 ${formatNumber(post.num_comments)} | 📊 ${Math.round(post.upvote_ratio * 100)}%`,
                },
                author: {
                    name: `u/${post.author}`,
                    url: `https://reddit.com/user/${post.author}`,
                },
            },
        ],
    };

    try {
        await safeFetch(CONFIG.WEBHOOK_URL, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(payload),
        });

        await storePosted(post);
        console.log("✅ Posted to Discord successfully!");
    } catch (err) {
        console.error("❌ Failed to post to Discord:", err);

        // Log to a dead letter queue or retry later
        // For now, just throw to be caught by the run() error handler
        throw new Error(`Discord webhook failed: ${err}`);
    }
}

async function healthCheck(): Promise<HealthStatus> {
    let dbConnected = false;
    let lastPost: number | null = null;

    try {
        const result = await db.execute(
            "SELECT MAX(posted_at) as last FROM posted_memes",
        );
        dbConnected = true;
        lastPost = (result.rows[0]?.last as number | null) ?? null;
    } catch (err) {
        console.error("❌ Health check DB query failed:", err);
    }

    const hoursSinceLastPost = lastPost
        ? (Date.now() - lastPost) / (1000 * 60 * 60)
        : null;

    const status: HealthStatus["status"] =
        !dbConnected || (hoursSinceLastPost !== null && hoursSinceLastPost > 6)
            ? "unhealthy"
            : hoursSinceLastPost !== null && hoursSinceLastPost > 2
                ? "degraded"
                : "healthy";

    return {
        status,
        lastPost,
        hoursSinceLastPost,
        dbConnected,
    };
}

async function run() {
    if (!CONFIG.WEBHOOK_URL) {
        console.error("❌ WEBHOOK_URL missing");
        return;
    }

    try {
        console.log(`\n🚀 Run started at ${new Date().toISOString()}`);

        const postedIds = await getPostedIds();
        const recentSubreddits = await getRecentSubreddits(
            CONFIG.ROTATION_LOOKBACK_POSTS,
        );
        const usageStats = await getSubredditUsageStats();

        const posts = await fetchPosts();
        const meme = selectBestPost(posts, postedIds, recentSubreddits, usageStats);

        if (!meme) {
            console.log("⏭️  No suitable meme found this run");
            return;
        }

        console.log(`\n📤 Posting: "${meme.title}"`);
        console.log(
            `   From: r/${meme.subreddit} by u/${meme.author} (${formatNumber(meme.ups)} upvotes)`,
        );

        await sendToDiscord(meme);

        // Periodic cleanup
        if (Math.random() < 0.1) {
            // 10% chance each run
            await cleanupOldPosts();
        }

        // Health check
        const health = await healthCheck();
        console.log(
            `💚 Health: ${health.status} (last post ${health.hoursSinceLastPost?.toFixed(1)}h ago)`,
        );
    } catch (err) {
        console.error("💥 Run failed:", err);

        // Alert or log to monitoring service here
        // For critical failures, you might want to send a Discord alert
    }
}

// Graceful shutdown
process.on("SIGTERM", async () => {
    console.log("📴 Shutting down gracefully...");
    process.exit(0);
});

process.on("SIGINT", async () => {
    console.log("📴 Shutting down gracefully...");
    process.exit(0);
});

(async () => {
    try {
        await initDb();
        console.log("🤖 Meme bot running...");
        console.log(`📡 Monitoring: ${CONFIG.SUBREDDITS.length} subreddits`);
        console.log(
            `🔄 Rotation: ${CONFIG.ROTATION_ENABLED ? "ENABLED" : "DISABLED"} (lookback: ${CONFIG.ROTATION_LOOKBACK_POSTS} posts)\n`,
        );

        // Initial run immediately on startup
        await run();
    } catch (err) {
        console.error("💥 Failed to start bot:", err);
        process.exit(1);
    }
})();
