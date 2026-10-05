'use strict';

const Redis = require('ioredis');

const config = {
  redisUrl: process.env.REDIS_URL || 'redis://127.0.0.1:6379',
  criticalTokens: Number(process.env.CRITICAL_TOKENS || 10),
  agingIntervalMs: Number(process.env.AGING_INTERVAL_MS || 1000),
};

const CHANNELS = {
  scores: 'alioth:svi',
  operatorFree: 'alioth:operator_free',
  silentOverride: 'alioth:silent_override',
  dispatch: 'alioth:dispatch',
};

const CURSOR_KEY = 'alioth:token_cursor';
const TIER_INDEX_KEY = 'alioth:case_tier';

const TIERS = [
  { name: 'critical', floor: 85, ceiling: 100, agingBoost: 0 },
  { name: 'high', floor: 60, ceiling: 84.99, agingBoost: 0.4 },
  { name: 'moderate', floor: 35, ceiling: 59.99, agingBoost: 0.3 },
  { name: 'low', floor: 0, ceiling: 34.99, agingBoost: 0.2 },
];

const AGING_SCRIPT = `
local members = redis.call('ZRANGE', KEYS[1], 0, -1, 'WITHSCORES')
local boost = tonumber(ARGV[1])
local ceiling = tonumber(ARGV[2])
local updated = 0
for i = 1, #members, 2 do
  local score = tonumber(members[i + 1])
  local boosted = math.min(score + boost, ceiling)
  if boosted > score then
    redis.call('ZADD', KEYS[1], boosted, members[i])
    updated = updated + 1
  end
end
return updated
`;

function queueKey(tierName) {
  return `alioth:queue:${tierName}`;
}

function tierByName(name) {
  return TIERS.find((tier) => tier.name === name);
}

function tierRank(name) {
  return TIERS.findIndex((tier) => tier.name === name);
}

function tierForScore(svi) {
  return TIERS.find((tier) => svi >= tier.floor) || TIERS[TIERS.length - 1];
}

function clampToTier(svi, tier) {
  return Math.min(Math.max(svi, tier.floor), tier.ceiling);
}

function buildTokenSequence() {
  const criticalSlots = Array(config.criticalTokens).fill(TIERS[0]);
  return [...criticalSlots, TIERS[1], TIERS[2], TIERS[3]];
}

function buildTierOrder(cursor) {
  const sequence = buildTokenSequence();
  const preferred = sequence[(cursor - 1) % sequence.length];
  const fallback = TIERS.filter((tier) => tier.name !== preferred.name);
  return [preferred, ...fallback];
}

function registerCommands(redis) {
  redis.defineCommand('ageTier', { numberOfKeys: 1, lua: AGING_SCRIPT });
}

async function enqueueCase(redis, { sessionId, svi }) {
  const target = tierForScore(svi);
  const current = await redis.hget(TIER_INDEX_KEY, sessionId);
  if (current && tierRank(current) < tierRank(target.name)) {
    return { sessionId, tier: current, kept: true };
  }
  const pipeline = redis.multi();
  TIERS.filter((tier) => tier.name !== target.name).forEach((tier) =>
    pipeline.zrem(queueKey(tier.name), sessionId)
  );
  pipeline.zadd(queueKey(target.name), 'GT', clampToTier(svi, target), sessionId);
  pipeline.hset(TIER_INDEX_KEY, sessionId, target.name);
  await pipeline.exec();
  return { sessionId, tier: target.name, kept: false };
}

async function forceCritical(redis, sessionId) {
  return enqueueCase(redis, { sessionId, svi: TIERS[0].ceiling });
}

async function ageTier(redis, tier) {
  if (tier.agingBoost <= 0) {
    return 0;
  }
  return redis.ageTier(queueKey(tier.name), tier.agingBoost, tier.ceiling);
}

async function ageAllTiers(redis) {
  const results = await Promise.all(TIERS.map((tier) => ageTier(redis, tier)));
  return results.reduce((sum, count) => sum + count, 0);
}

async function popFromTier(redis, tier) {
  const popped = await redis.zpopmax(queueKey(tier.name));
  if (popped.length === 0) {
    return null;
  }
  return { sessionId: popped[0], score: Number(popped[1]), tier: tier.name };
}

async function popByTokenOrder(redis, order) {
  for (const tier of order) {
    const entry = await popFromTier(redis, tier);
    if (entry) {
      return entry;
    }
  }
  return null;
}

async function dispatchNext(redis) {
  const cursor = await redis.incr(CURSOR_KEY);
  const entry = await popByTokenOrder(redis, buildTierOrder(cursor));
  if (!entry) {
    return null;
  }
  await redis.hdel(TIER_INDEX_KEY, entry.sessionId);
  await redis.publish(CHANNELS.dispatch, JSON.stringify(entry));
  return entry;
}

async function queueDepths(redis) {
  const sizes = await Promise.all(TIERS.map((tier) => redis.zcard(queueKey(tier.name))));
  return Object.fromEntries(TIERS.map((tier, index) => [tier.name, sizes[index]]));
}

function parseMessage(raw) {
  try {
    return JSON.parse(raw);
  } catch (error) {
    return null;
  }
}

async function handleScore(redis, raw) {
  const payload = parseMessage(raw);
  if (!payload || payload.session_id === undefined || typeof payload.svi !== 'number') {
    return;
  }
  await enqueueCase(redis, { sessionId: String(payload.session_id), svi: payload.svi });
}

async function handleSilentOverride(redis, raw) {
  const payload = parseMessage(raw);
  if (!payload || payload.session_id === undefined) {
    return;
  }
  await forceCritical(redis, String(payload.session_id));
}

function routeMessage(redis, channel, raw) {
  if (channel === CHANNELS.scores) {
    return handleScore(redis, raw);
  }
  if (channel === CHANNELS.silentOverride) {
    return handleSilentOverride(redis, raw);
  }
  if (channel === CHANNELS.operatorFree) {
    return dispatchNext(redis);
  }
  return Promise.resolve();
}

function attachSubscriber(subscriber, redis) {
  subscriber.on('message', (channel, raw) => {
    routeMessage(redis, channel, raw).catch((error) => {
      console.error(`handler failure on ${channel}:`, error.message);
    });
  });
  return subscriber.subscribe(
    CHANNELS.scores,
    CHANNELS.operatorFree,
    CHANNELS.silentOverride
  );
}

function startAgingLoop(redis) {
  return setInterval(() => {
    ageAllTiers(redis).catch((error) => {
      console.error('aging failure:', error.message);
    });
  }, config.agingIntervalMs);
}

async function shutdown(redis, subscriber, timer) {
  clearInterval(timer);
  await subscriber.quit();
  await redis.quit();
}

async function start() {
  const redis = new Redis(config.redisUrl);
  const subscriber = new Redis(config.redisUrl);
  registerCommands(redis);
  await attachSubscriber(subscriber, redis);
  const timer = startAgingLoop(redis);
  const stop = () => shutdown(redis, subscriber, timer).then(() => process.exit(0));
  process.on('SIGINT', stop);
  process.on('SIGTERM', stop);
  return { redis, subscriber, timer };
}

module.exports = {
  TIERS,
  tierForScore,
  clampToTier,
  buildTierOrder,
  enqueueCase,
  forceCritical,
  ageAllTiers,
  dispatchNext,
  queueDepths,
  start,
};

if (require.main === module) {
  start().catch((error) => {
    console.error('startup failure:', error.message);
    process.exit(1);
  });
}
