import Parser from "rss-parser";
import axios from "axios";
import { createClient } from "@supabase/supabase-js";
import dotenv from "dotenv";

dotenv.config();

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_ANON_KEY
);

const rssParser = new Parser({
  customFields: {
    item: [
      ["media:content", "media"],
      ["content:encoded", "fullContent"],
    ],
  },
});

const DECISION_MATRIX = {
  priorityKeywords: [
    "Sony", "Microsoft", "Nintendo", "Take-Two", "Ubisoft", "Square Enix", "Capcom",
    "Bandai Namco", "Activision", "Rockstar", "Bethesda", "SEGA", "Konami", "Embracer",
    "Devolver", "FromSoftware", "Obsidian", "Remedy", "Naughty Dog", "Insomniac",
    "Bungie", "Epic", "Valve", "Blizzard", "Annapurna", "Raw Fury", "Team17",
    "acquisition", "acquired", "buyout", "merger", "layoffs", "restructuring",
    "shutdown", "closure", "IPO", "going public", "funding round", "partnership",
    "collaboration", "launch", "release", "announcement", "reveals", "resignation",
    "CEO change", "new CEO", "new president", "founded", "game sales", "sales figures",
    "GTA", "Grand Theft Auto", "Elder Scrolls", "Final Fantasy", "Call of Duty",
    "Assassin's Creed", "Legend of Zelda", "Mario", "Pokémon", "Fortnite",
    "Elden Ring", "Baldur's Gate", "Starfield", "Cyberpunk", "Diablo",
    "Overwatch", "World of Warcraft",
  ],
  exclusionKeywords: [
    "esports", "tournament", "esports league", "LAN event", "competitive gaming",
    "Twitch", "streamer", "Kick", "YouTube Gaming", "live streaming", "stream",
    "iOS game", "Android exclusive", "mobile gaming", "app store",
    "roguelike difficulty", "roguelike design", "difficulty settings", "soulslike",
    "YouTuber", "streamer feud", "influencer beef", "gaming debate",
    "fandom discourse", "console wars", "PC vs console",
    "review", "rated", "review score", "rating", "hardware review",
    "gaming chair", "gaming peripherals", "gaming setup", "patch notes",
    "mod showcase", "community creation",
  ],
  blacklist: ["EA", "Electronic Arts"],
  priorityRegions: ["US", "EU", "Japan", "South Korea", "Canada", "Australia"],
};

function passesStage1Filter(title, snippet) {
  const text = `${title} ${snippet}`.toLowerCase();
  const hasPriority = DECISION_MATRIX.priorityKeywords.some((keyword) =>
    text.includes(keyword.toLowerCase())
  );
  if (!hasPriority) return false;
  const hasExclusion = DECISION_MATRIX.exclusionKeywords.some((keyword) =>
    text.includes(keyword.toLowerCase())
  );
  if (hasExclusion) return false;
  const isBlacklisted = DECISION_MATRIX.blacklist.some((company) =>
    text.includes(company.toLowerCase())
  );
  if (isBlacklisted) return false;
  return true;
}

async function processWithGroq(title, summary, url) {
  try {
    const prompt = `You are a gaming industry analyst. Extract the following from this gaming news article:

Article Title: ${title}
Article Summary: ${summary}

Extract and return ONLY valid JSON (no markdown, no preamble):
{
  "companies": ["Company1", "Company2"],
  "company_tiers": {"Company1": "aaa", "Company2": "mid_tier"},
  "category": "acquisition",
  "sentiment": "positive",
  "impact_score": 8,
  "summary": "1-2 sentence summary"
}

Categories: layoff, acquisition, release, business, trend, leadership
Company tiers: aaa, mid_tier, indie_publisher
Sentiment: positive, neutral, negative
Impact score: 1-10`;

    const requestBody = {
      model: "mixtral-8x7b-32768",
      messages: [{ role: "user", content: prompt }],
      max_tokens: 500,
    };

    console.log("📤 Sending request to Groq...");
    const response = await axios.post(
      "https://api.groq.com/openai/v1/chat/completions",
      requestBody,
      {
        headers: {
          Authorization: `Bearer ${process.env.GROQ_API_KEY}`,
          "Content-Type": "application/json",
        },
      }
    );

    const responseText = response.data.choices[0].message.content;
    const jsonMatch = responseText.match(/\{[\s\S]*\}/);
    if (!jsonMatch) throw new Error("No JSON found in response");

    const parsed = JSON.parse(jsonMatch[0]);

    return {
      companies: parsed.companies || [],
      company_tiers: parsed.company_tiers || {},
      category: parsed.category || "trend",
      sentiment: parsed.sentiment || "neutral",
      impact_score: Math.min(Math.max(parsed.impact_score || 5, 1), 10),
      summary: parsed.summary || "",
      success: true,
    };
  } catch (error) {
    console.error("❌ GROQ ERROR DETAILS:");
    console.error("Status Code:", error.response?.status);
    console.error("Status Text:", error.response?.statusText);
    console.error("Error Message:", error.message);
    console.error("Full Response:", JSON.stringify(error.response?.data, null, 2));
    console.error("Request Headers:", error.config?.headers);
    return { success: false, error: error.message };
  }
}

function calculateDigestScore(article) {
  let score = article.impact_score || 5;
  const aaaCount = Object.values(article.company_tiers || {}).filter(
    (tier) => tier === "aaa"
  ).length;
  if (aaaCount >= 2) score += 2;
  const ageHours =
    (Date.now() - new Date(article.published_at)) / (1000 * 60 * 60);
  if (ageHours < 4) score += 0.5;
  const indieCount = Object.values(article.company_tiers || {}).filter(
    (tier) => tier === "indie_publisher"
  ).length;
  if (indieCount > 0 && article.companies.length === 1) score -= 2;
  return Math.min(score, 10);
}

async function scrapeRSSFeeds() {
  console.log("🔄 Starting RSS feed scrape...");
  const { data: sources } = await supabase
    .from("feed_sources")
    .select("*")
    .eq("is_active", true)
    .eq("source_type", "rss");

  if (!sources) {
    console.error("Failed to fetch feed sources");
    return [];
  }

  let newArticles = [];

  for (const source of sources) {
    try {
      console.log(`📰 Scraping ${source.name}...`);
      const feed = await rssParser.parseURL(source.url);

      for (const item of feed.items.slice(0, 10)) {
        const { data: existing } = await supabase
          .from("articles")
          .select("id")
          .eq("url", item.link)
          .single();

        if (existing) continue;

        const snippet = item.contentSnippet || item.summary || "";
        if (!passesStage1Filter(item.title, snippet)) continue;

        const { data: article, error } = await supabase
          .from("articles")
          .insert({
            title: item.title,
            url: item.link,
            source: source.name,
            published_at: new Date(item.pubDate),
            summary: snippet.slice(0, 500),
          })
          .select()
          .single();

        if (error) {
          console.error(`Error inserting article from ${source.name}:`, error);
          continue;
        }

        const groqResult = await processWithGroq(
          item.title,
          snippet,
          item.link
        );

        if (groqResult.success) {
          await supabase
            .from("articles")
            .update({
              companies: groqResult.companies,
              company_tiers: groqResult.company_tiers,
              category: groqResult.category,
              sentiment: groqResult.sentiment,
              impact_score: groqResult.impact_score,
              summary: groqResult.summary,
              digest_score: calculateDigestScore(groqResult),
              include_in_feed: groqResult.impact_score >= 4,
            })
            .eq("id", article.id);

          newArticles.push(article);
          console.log(`✅ Processed: ${item.title.slice(0, 50)}...`);
        } else {
          console.log(`⚠️ Failed to process: ${groqResult.error}`);
        }
      }

      await supabase
        .from("feed_sources")
        .update({ last_fetched_at: new Date() })
        .eq("id", source.id);
    } catch (error) {
      console.error(`Error scraping ${source.name}:`, error.message);
    }
  }

  return newArticles;
}

async function scrapeReddit(subreddit) {
  try {
    console.log(`📱 Scraping r/${subreddit}...`);
    const response = await axios.get(
      `https://www.reddit.com/r/${subreddit}/.json?limit=25`,
      { headers: { "User-Agent": "Gaming-News-Aggregator/1.0" } }
    );

    const posts = response.data.data.children;
    let newArticles = [];

    for (const post of posts) {
      const { title, url, created_utc, selftext } = post.data;

      const { data: existing } = await supabase
        .from("articles")
        .select("id")
        .eq("url", url)
        .single();

      if (existing) continue;

      if (!passesStage1Filter(title, selftext)) continue;

      const { data: article, error } = await supabase
        .from("articles")
        .insert({
          title,
          url,
          source: `r/${subreddit}`,
          published_at: new Date(created_utc * 1000),
          summary: selftext.slice(0, 500),
        })
        .select()
        .single();

      if (error) continue;

      const groqResult = await processWithGroq(title, selftext, url);

      if (groqResult.success) {
        await supabase
          .from("articles")
          .update({
            companies: groqResult.companies,
            company_tiers: groqResult.company_tiers,
            category: groqResult.category,
            sentiment: groqResult.sentiment,
            impact_score: groqResult.impact_score,
            summary: groqResult.summary,
            digest_score: calculateDigestScore(groqResult),
            include_in_feed: groqResult.impact_score >= 4,
          })
          .eq("id", article.id);

        newArticles.push(article);
        console.log(`✅ Processed Reddit: ${title.slice(0, 50)}...`);
      }
    }

    return newArticles;
  } catch (error) {
    console.error(`Error scraping r/${subreddit}:`, error.message);
    return [];
  }
}

async function runAggregation() {
  console.log("\n🎮 Gaming News Aggregator - Starting polling cycle...");
  console.log(`⏰ ${new Date().toISOString()}`);

  try {
    const rssArticles = await scrapeRSSFeeds();
    console.log(`\n📊 RSS: Found ${rssArticles.length} new articles`);

    const redditGames = await scrapeReddit("Games");
    const redditPC = await scrapeReddit("pcgaming");
    console.log(
      `\n📊 Reddit: Found ${redditGames.length + redditPC.length} new articles`
    );

    const total = rssArticles.length + redditGames.length + redditPC.length;
    console.log(`\n✨ Total new articles processed: ${total}`);
  } catch (error) {
    console.error("Fatal error in aggregation:", error);
  }

  console.log("✅ Polling cycle complete\n");
}

async function start() {
  console.log("🚀 Gaming News Aggregator Backend Starting...");
  console.log("📍 Environment:", process.env.NODE_ENV || "development");
  console.log("🔑 API Key present:", !!process.env.GROQ_API_KEY);

  try {
    console.log("🔍 Testing Groq API connection...");
    const testBody = {
      model: "mixtral-8x7b-32768",
      messages: [{ role: "user", content: "Say: success" }],
      max_tokens: 10,
    };

    const testResponse = await axios.post(
      "https://api.groq.com/openai/v1/chat/completions",
      testBody,
      {
        headers: {
          Authorization: `Bearer ${process.env.GROQ_API_KEY}`,
          "Content-Type": "application/json",
        },
      }
    );
    console.log("✅ Groq API connected successfully");
  } catch (error) {
    console.error("❌ GROQ TEST FAILED - ERROR DETAILS:");
    console.error("Status Code:", error.response?.status);
    console.error("Status Text:", error.response?.statusText);
    console.error("Error Message:", error.message);
    console.error("Full Response:", JSON.stringify(error.response?.data, null, 2));
    process.exit(1);
  }

  try {
    const { data } = await supabase
      .from("articles")
      .select("count()")
      .single();
    console.log("✅ Supabase connected");
  } catch (error) {
    console.error("❌ Supabase error:", error.message);
    process.exit(1);
  }

  await runAggregation();

  setInterval(runAggregation, 30 * 60 * 1000);
  console.log("🔄 Polling every 30 minutes");
}

start().catch((error) => {
  console.error("Startup error:", error);
  process.exit(1);
});

export { runAggregation };
