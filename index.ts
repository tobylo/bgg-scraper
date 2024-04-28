import { BggClient } from "boardgamegeekclient";
import type { BggThingDto } from "boardgamegeekclient/dist/esm/dto";
import type { BggPollDto } from "boardgamegeekclient/dist/esm/dto/concrete/subdto";
import Breadroll, { Dataframe } from "breadroll";
import { decode } from "html-entities";
import { parseArgs } from "util";
import { mkdir } from "node:fs/promises";

let debug: boolean = false;
let skipBatchCount: number | undefined;
let batchSize: number = 40;
let path = "boardgames_ranks_2024-04-26.csv";

const { values } = parseArgs({
  args: Bun.argv,
  options: {
    debug: {
      type: "boolean",
      short: "d",
    },
    skip: {
      type: "string",
      short: "s",
    },
    batchsize: {
      type: "string",
      short: "b",
    },
    path: {
      type: "string",
      short: "p",
    },
  },
  strict: true,
  allowPositionals: true,
});

if (values.debug) {
  debug = true;
  console.log("Debug mode enabled, will only run for one batch");
}
if (values.skip) {
  skipBatchCount = Number.parseInt(values.skip);
  console.log(`Skipping to batch ${skipBatchCount + 1}`);
}
if (values.batchsize) {
  batchSize = Number.parseInt(values.batchsize);
  console.log(`Batch size set to ${batchSize}`);
}
if (values.path) {
  path = values.path;
  console.log(`Path set to ${path}`);
}

interface BatchError {
  ids: number[];
  error: string;
}

interface Stats {
  start: Date;
  end?: Date;
  batchSize: number;
  availableBatches: number;
  startingBatch: number;
  batchesProcessed: number;
  errors: BatchError[];
  averageTimePerBatch: number;
}

interface Recommendation {
  value: string | number;
  score: number;
}

interface PollResult {
  score: number;
  votes: number;
}

interface Game {
  id: number;
  thumbnail: string;
  image: string;
  name: string;
  description: string;
  yearPublished: number;
  categories: string[];
  mechanics: string[];
  complexity?: number;
  rating: {
    average: number;
    bayesian: number;
    stddev: number;
  };
  playTimeMinutes: {
    rated: number;
    min: number;
    max: number;
  };
  age: {
    rated: number;
    suggested?: string | number;
    pollResults: Recommendation[] | undefined;
  };
  playerCount: {
    min: number;
    max: number;
    suggested?: number;
    pollResults: Recommendation[] | undefined;
  };
}

function calculateRecommendedAgeScore(
  polls?: BggPollDto[]
): Recommendation[] | undefined {
  const poll = polls?.find((p) => p.name == "suggested_playerage");
  if (poll === undefined || poll === null) {
    return;
  }
  return parsePlayerAgePoll(poll);
}

function parsePlayerAgePoll(
  pollResult: BggPollDto
): Recommendation[] | undefined {
  if (
    pollResult.results === undefined ||
    pollResult.results === null ||
    pollResult.results.length === 0
  ) {
    return;
  }

  const totalVotes = Number.parseInt(nullGuard(pollResult.totalvotes, "0"));
  if (totalVotes === 0) {
    return;
  }

  const result: Recommendation[] = [];
  pollResult.results.forEach((item) => {
    item.resultItemList.forEach((ageVote) => {
      const numOfVotes = nullGuard(ageVote.numvotes, 0);
      if (numOfVotes === 0) {
        return;
      }
      result.push({
        value: ageVote.value,
        score: Math.round((numOfVotes / totalVotes) * 100),
      });
    });
  });

  return result.length === 0 ? undefined : result;
}

function calculateRecommendedPlayerCountScore(
  polls?: BggPollDto[]
): Recommendation[] | undefined {
  const poll = polls?.find((p) => p.name == "suggested_numplayers");
  if (poll === undefined || poll === null) {
    return;
  }
  return parseNumPlayersPoll(poll);
}

function parseNumPlayersPoll(
  pollResult: BggPollDto
): Recommendation[] | undefined {
  if (
    pollResult.results === undefined ||
    pollResult.results === null ||
    pollResult.results.length === 0
  ) {
    console.trace("No poll results found for player count recommendation");
    return;
  }

  const result: Recommendation[] = [];
  pollResult.results.forEach((item) => {
    if (
      item.numplayers === undefined ||
      item.numplayers === null ||
      item.resultItemList === undefined
    ) {
      return;
    }

    const summary = item.resultItemList.reduce<PollResult>(
      (acc, item) => {
        const numOfVotes = nullGuard(item.numvotes, 0);
        if (numOfVotes === 0) {
          return acc;
        }

        if (item.value == "Best") {
          acc.score += numOfVotes;
          acc.votes += numOfVotes;
        } else if (item.value == "Recommended") {
          acc.score += 0.5 * numOfVotes;
          acc.votes += numOfVotes;
        } else if (item.value == "Not Recommended") {
          acc.score -= 0.5 * numOfVotes;
          acc.votes += numOfVotes;
        }
        return acc;
      },
      { score: 0, votes: 0 }
    );

    if (summary.votes !== 0) {
      result.push({
        value: item.numplayers,
        score: Math.max(0, Math.round((summary.score / summary.votes) * 100)),
      });
    }
  });

  return result.length === 0 ? undefined : result;
}

function getTopRecommendation(recommendations: Recommendation[] | undefined) {
  if (
    recommendations === undefined ||
    recommendations === null ||
    recommendations.length === 0 ||
    recommendations.every((item) => item.score === 0)
  ) {
    return;
  }
  return recommendations.reduce(
    (acc, item) => (item.score > acc.score ? item : acc),
    recommendations[0]
  );
}

function nullGuard<T>(value: T | undefined | null, defaultValue: T): T {
  if (value === undefined || value === null) {
    return defaultValue;
  }
  return value;
}

function parseGame(thing: BggThingDto): Game {
  const playerCountPoll = calculateRecommendedPlayerCountScore(thing.polls);
  const agePoll = calculateRecommendedAgeScore(thing.polls);

  return {
    id: thing.id,
    thumbnail: thing.thumbnail,
    image: thing.image,
    name: thing.name,
    description: decode(decode(thing.description, { level: "html5" }), {
      level: "html4",
    })
      .replace(/&#(\d+)(;)/g, " ")
      .replace(/\n/g, " ")
      .replace(/\s\s+/g, " "),
    yearPublished: thing.yearpublished,
    rating: {
      average: thing.statistics.ratings.average,
      bayesian: thing.statistics.ratings.bayesaverage,
      stddev: thing.statistics.ratings.stddev,
    },
    complexity: thing.statistics.ratings.averageweight,
    categories:
      thing.links
        ?.filter((link) => link.type == "boardgamecategory")
        .map((link) => link.value) ?? [],
    mechanics:
      thing.links
        ?.filter((link) => link.type == "boardgamemechanic")
        .map((link) => link.value) ?? [],
    playTimeMinutes: {
      rated: thing.playingtime,
      min: thing.minplaytime,
      max: thing.maxplaytime,
    },
    playerCount: {
      min: thing.minplayers,
      max: thing.maxplayers,
      suggested: getTopRecommendation(playerCountPoll)?.value,
      pollResults: playerCountPoll,
    },
    age: {
      rated: thing.minage,
      suggested: getTopRecommendation(agePoll)?.value,
      pollResults: agePoll,
    },
  } as Game;
}

// Assert output directory exists
await mkdir(`./${debug ? "debug" : "output"}`, { recursive: true });

let client = BggClient.Create();
const targetPrefix = "batch_";
const csv: Breadroll = new Breadroll({ header: true, delimiter: "," });
const df: Dataframe = await csv.open.local(path);
const sleepTime = 5000;
const timeBetweenRequests = 5000;

const totalBatchCount = Math.ceil(df.value.length / batchSize);
console.log(`Total batch count: ${totalBatchCount}`);

let currentBatchCount = 0;
let batch: Record<string, unknown>[] = [];

if (skipBatchCount !== undefined) {
  console.log(`Skipping to batch ${skipBatchCount + 1}`);
  df.value.splice(0, skipBatchCount * batchSize);
  currentBatchCount = skipBatchCount;
}

batch = df.value.splice(0, batchSize);
currentBatchCount++;

const statsFilePath = `./${
  debug ? "debug" : "output"
}/_stats_${new Date().getTime()}.json`;
const statsFile = Bun.file(statsFilePath, { type: "json", endings: "native" });
const writer = statsFile.writer();
const stats: Stats = {
  start: new Date(),
  end: undefined,
  batchSize: batchSize,
  availableBatches: totalBatchCount,
  startingBatch: currentBatchCount,
  batchesProcessed: 0,
  averageTimePerBatch: 0,
  errors: [],
};

process.on("exit", () => {
  stats.end = new Date();
  writer.write(JSON.stringify(stats));
  writer.end();
});

while (batch.length > 0) {
  const start = performance.now();
  const ids = batch.map((row) => row.id as number);
  console.log(`Batch #${currentBatchCount}: ${ids}`);
  let things: BggThingDto[];
  try {
    things = await client.thing.query({
      id: ids,
      videos: 0,
      comments: 0,
      marketplace: 0,
      stats: 1,
      type: "boardgame",
    });
    if (things === undefined || things === null || things.length === 0) {
      console.error("No data returned from BGG API, skipping batch...");
      stats.errors.push({ ids, error: "No data returned from BGG API" });
      things = [];
    }
  } catch (e) {
    const error = `Error fetching batch ${currentBatchCount}, recreating client and retrying after ${
      sleepTime / 1000
    }sec...`;
    console.log(error);
    stats.errors.push({ ids, error });
    console.error(e);
    client = BggClient.Create();
    await Bun.sleep(sleepTime);
    continue;
  }
  await Bun.write(
    `./${debug ? "debug" : "output"}/${targetPrefix}${currentBatchCount}.json`,
    JSON.stringify(things.map(parseGame))
  );
  batch = df.value.splice(0, batchSize);
  stats.batchesProcessed++;
  const timeTaken = performance.now() - start;
  stats.averageTimePerBatch =
    (stats.averageTimePerBatch * (stats.batchesProcessed - 1) + timeTaken) /
    stats.batchesProcessed;
  console.log(
    `Batch ${currentBatchCount}/${totalBatchCount} (${(
      (currentBatchCount / totalBatchCount) *
      100
    ).toFixed(2)}%) took ${(timeTaken / 1000).toFixed(
      2
    )}sec | Remaining Time: ${(
      (stats.averageTimePerBatch * (totalBatchCount - currentBatchCount)) /
      1000 /
      60
    ).toFixed(2)}min (average: ${(stats.averageTimePerBatch / 1000).toFixed(
      2
    )}s)`
  );
  if (debug) {
    await Bun.write(
      `./debug/${targetPrefix}${currentBatchCount}-raw.json`,
      JSON.stringify(things)
    );
    console.log("Single batch run complete in debug mode. Exiting.");
    process.exit(0);
  }
  if (timeTaken < timeBetweenRequests) {
    await Bun.sleep(timeBetweenRequests - timeTaken);
  }
  currentBatchCount++;
}
