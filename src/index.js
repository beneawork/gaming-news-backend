import Groq from 'groq-sdk';
import Parser from 'rss-parser';
import axios from 'axios';
import { createClient } from '@supabase/supabase-js';
import dotenv from 'dotenv';

dotenv.config();

// Initialize clients - Fixed Groq import
const GroqClient = Groq.default || Groq;
const groq = new GroqClient({
  apiKey: process.env.GROQ_API_KEY,
});

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_ANON_KEY
);

const rssParser = new Parser({
  customFields: {
    item: [['media:content', 'media'], ['content:encoded', 'fullContent']],
  },
});

// ============================================
// CONFIGURATION & DECISION MATRIX
// ============================================

const DECISION_MATRIX = {
  // Priority news keywords (Stage 1)
  priorityKeywords: [
    // Studio/company signals
    'Sony', 'Microsoft', 'Nintendo', 'Take-Two', 'Ubisoft', 'Square Enix', 'Capcom',
    'Bandai Namco', 'Activision', 'Rockstar', 'Bethesda', 'SEGA', 'Konami', 'Embracer',
    'Devolver', 'FromSoftware', 'Obsidian', 'Remedy', 'Naughty Dog', 'Insomniac',
    'Bungie', 'Epic', 'Valve', 'Blizzard', 'Annapurna', 'Raw Fury', 'Team17',
    
    // News type keywords
    'acquisition', 'acquired', 'buyout', 'merger', 'layoffs', 'restructuring',
    'shutdown', 'closure', 'IPO', 'going public', 'funding round', 'partnership',
    'collaboration', 'launch', 'release', 'announcement', 'reveals', 'resignation',
    'CEO change', 'new CEO', 'new president', 'founded', 'game sales', 'sales figures',
    
    // Major game titles
    'GTA', 'Grand Theft Auto', 'Elder Scrolls', 'Final Fantasy', 'Call of Duty',
    'Assassin\'s Creed', 'Legend of Zelda', 'Mario', 'Pokémon', 'Fortnite',
    'Elden Ring', 'Baldur\'s Gate', 'Starfield', 'Cyberpunk', 'Diablo',
    'Overwatch', 'World of Warcraft'
  ],

  // Exclusion keywords (Stage 2)
  exclusionKeywords: [
    // Topics to skip
    'esports', 'tournament', 'esports league', 'LAN event', 'competitive gaming',
    'Twitch', 'streamer', 'Kick', 'YouTube Gaming', 'live streaming', 'stream',
    'iOS game', 'Android exclusive', 'mobile gaming', 'app store',
    'roguelike difficulty', 'roguelike design', 'difficulty settings', 'soulslike',
    'YouTuber', 'streamer feud', 'influencer beef', 'gaming debate',
    'fandom discourse', 'console wars', 'PC vs console',
    'review', 'rated', 'review score', 'rating', 'hardware review',
    'gaming chair', 'gaming peripherals', 'gaming setup', 'patch notes',
    'mod showcase', 'community creation'
  ],

  // Company blacklist
  blacklist: ['EA', 'Electronic Arts'],

  // Geographic focus (for filtering)
  priorityRegions: ['US', 'EU', 'Japan', 'South Korea', 'Canada', 'Australia'],
};

// ============================================
// STAGE 1: KEYWORD PRE-FILTER
// ============================================

function passesStage1Filter(title, snippet) {
  const text = `${title} ${snippet}`.toLowerCase();
  
  // Check if contains priority keywords
  const hasPriority = DECISION_MATRIX.priorityKeywords.some(keyword =>
    text.includes(keyword.toLowerCase())
  );
  
  if (!hasPriority) return false;

  // Check if contains exclusion keywords
  const hasExclusion = DECISION_MATRIX.exclusionKeywords.some(keyword =>
    text.includes(keyword.toLowerCase())
  );
  
  if (hasExclusion) return false;

  // Check if blacklisted company
  const isBlacklisted = DECISION_MATRIX.blacklist.some(company =>
    text.includes(company.toLowerCase())
  );
  
  if (isBlacklisted) return false;

  return true;
}

// ============================================
// STAGE 2: GROQ PROCESSING
// ============================================

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
Impact score: 1-10 (9-10 major M&A/studio shutdown, 7-8 significant news, 5-6 notable, 3-4 niche)`;

    const message = await groq.messages.create({
      model: 'mixtral-8x7b-32768',
      max_tokens: 500,
      messages: [{ role: 'user', content: prompt }],
    });

    const responseText = message.content[0].type === 'text' ? message.content[0].text : '';
    
    // Parse JSON from response
    const jsonMatch = responseText.match(/\{[\s\S]*\}/);
    if (!jsonMatch) throw new Error('No JSON found in response');
    
    const parsed = JSON.parse(jsonMatch[0]);
    
    return {
      companies: parsed.companies || [],
      company_tiers: parsed.company_tiers || {},
      category: parsed.category || 'trend',
      sentiment: parsed.sentiment || 'neutral',
      impact_score: Math.min(Math.max(parsed.impact_score || 5, 1), 10),
      summary: parsed.summary || '',
      success: true
    };
  } catch (error) {
    console.error('Groq processing error:', error.message);
    return { success: false, error: error.message };
  }
}

// ============================================
// STAGE 3: RELEVANCE SCORING
// ============================================

function calculateDigestScore(article) {
  let score = article.impact_score || 5;

  // Add points for multiple AAA studios
  const aaaCount = Object.values(article.company_tiers || {})
    .filter(tier => tier === 'aaa').length;
  if (aaaCount >= 2) score += 2;

  // Add points for freshness (within 4 hours)
  const ageHours = (Date.now() - new Date(article.published_at)) / (1000 * 60 * 60);
  if (ageHours < 4) score += 0.5;

  // Subtract points for single indie projects
  const indieCount = Object.values(article.company_tiers || {})
    .filter(tier => tier === 'indie_publisher').length;
  if (indieCount > 0 && article.companies.length === 1) score -= 2;

  return Math.min(score, 10);
}

// ============================================
// RSS FEED SCRAPING
// ============================================

async function scrapeRSSFeeds() {
  console.log('🔄 Starting RSS feed scrape...');

  const { data: sources } = await supabase
    .from('feed_sources')
    .select('*')
    .eq('is_active', true)
    .eq('source_type', 'rss');

  if (!sources) {
    console.error('Failed to fetch feed sources');
    return [];
  }

  let newArticles = [];

  for (const source of sources) {
    try {
      console.log(`📰 Scraping ${source.name}...`);
      const feed = await rssParser.parseURL(source.url);

      for (const item of feed.items.slice(0, 10)) {
        // Skip if article already exists
        const { data: existing } = await supabase
          .from('articles')
          .select('id')
          .eq('url', item.link)
          .single();

        if (existing) continue;

        // Stage 1: Keyword filter
        const snippet = item.contentSnippet || item.summary || '';
        if (!passesStage1Filter(item.title, snippet)) continue;

        // Create article record
        const { data: article, error } = await supabase
          .from('articles')
          .insert({
            title: item.title,
            url: item.link,
            source: source.name,
            published_at: new Date(item.pubDate),
            summary: snippet.slice(0, 500)
          })
          .select()
          .single();

        if (error) {
          console.error(`Error inserting article from ${source.name}:`, error);
          continue;
        }

        // Stage 2: Groq processing
        const groqResult = await processWithGroq(
          item.title,
          snippet,
          item.link
        );

        if (groqResult.success) {
          // Update article with Groq data
          await supabase
            .from('articles')
            .update({
              companies: groqResult.companies,
              company_tiers: groqResult.company_tiers,
              category: groqResult.category,
              sentiment: groqResult.sentiment,
              impact_score: groqResult.impact_score,
              summary: groqResult.summary,
              digest_score: calculateDigestScore(groqResult),
              include_in_feed: groqResult.impact_score >= 4
            })
            .eq('id', article.id);

          newArticles.push(article);
          console.log(`✅ Processed: ${item.title.slice(0, 50)}...`);
        } else {
          console.log(`⚠️ Failed to process: ${groqResult.error}`);
        }
      }

      // Update last fetched time
      await supabase
        .from('feed_sources')
        .update({ last_fetched_at: new Date() })
        .eq('id', source.id);

    } catch (error) {
      console.error(`Error scraping ${source.name}:`, error.message);
    }
  }

  return newArticles;
}

// ============================================
// REDDIT SCRAPING
// ============================================

async function scrapeReddit(subreddit) {
  try {
    console.log(`📱 Scraping r/${subreddit}...`);
    const response = await axios.get(
      `https://www.reddit.com/r/${subreddit}/.json?limit=25`,
      {
        headers: { 'User-Agent': 'Gaming-News-Aggregator/1.0' }
      }
    );

    const posts = response.data.data.children;
    let newArticles = [];

    for (const post of posts) {
      const { title, url, created_utc, selftext } = post.data;

      // Skip if already exists
      const { data: existing } = await supabase
        .from('articles')
        .select('id')
        .eq('url', url)
        .single();

      if (existing) continue;

      // Stage 1: Keyword filter
      if (!passesStage1Filter(title, selftext)) continue;

      // Create article record
      const { data: article, error } = await supabase
        .from('articles')
        .insert({
          title,
          url,
          source: `r/${subreddit}`,
          published_at: new Date(created_utc * 1000),
          summary: selftext.slice(0, 500)
        })
        .select()
        .single();

      if (error) continue;

      // Stage 2: Groq processing
      const groqResult = await processWithGroq(title, selftext, url);

      if (groqResult.success) {
        await supabase
          .from('articles')
          .update({
            companies: groqResult.companies,
            company_tiers: groqResult.company_tiers,
            category: groqResult.category,
            sentiment: groqResult.sentiment,
            impact_score: groqResult.impact_score,
            summary: groqResult.summary,
            digest_score: calculateDigestScore(groqResult),
            include_in_feed: groqResult.impact_score >= 4
          })
          .eq('id', article.id);

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

// ============================================
// MAIN AGGREGATION LOOP
// ============================================

async function runAggregation() {
  console.log('\n🎮 Gaming News Aggregator - Starting polling cycle...');
  console.log(`⏰ ${new Date().toISOString()}`);

  try {
    // RSS feeds
    const rssArticles = await scrapeRSSFeeds();
    console.log(`\n📊 RSS: Found ${rssArticles.length} new articles`);

    // Reddit
    const redditGames = await scrapeReddit('Games');
    const redditPC = await scrapeReddit('pcgaming');
    console.log(`\n📊 Reddit: Found ${redditGames.length + redditPC.length} new articles`);

    const total = rssArticles.length + redditGames.length + redditPC.length;
    console.log(`\n✨ Total new articles processed: ${total}`);
  } catch (error) {
    console.error('Fatal error in aggregation:', error);
  }

  console.log('✅ Polling cycle complete\n');
}

// ============================================
// STARTUP & SCHEDULING
// ============================================

async function start() {
  console.log('🚀 Gaming News Aggregator Backend Starting...');
  console.log('📍 Environment:', process.env.NODE_ENV || 'development');

  // Test Groq connection
  try {
    const testMessage = await groq.messages.create({
      model: 'mixtral-8x7b-32768',
      max_tokens: 10,
      messages: [{ role: 'user', content: 'Test' }],
    });
    console.log('✅ Groq API connected');
  } catch (error) {
    console.error('❌ Groq API error:', error.message);
    process.exit(1);
  }

  // Test Supabase connection
  try {
    const { data } = await supabase.from('articles').select('count()').single();
    console.log('✅ Supabase connected');
  } catch (error) {
    console.error('❌ Supabase error:', error.message);
    process.exit(1);
  }

  // Run initial aggregation
  await runAggregation();

  // Poll every 30 minutes
  setInterval(runAggregation, 30 * 60 * 1000);
  console.log('🔄 Polling every 30 minutes');
}

start().catch(error => {
  console.error('Startup error:', error);
  process.exit(1);
});

export { runAggregation };
