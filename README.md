# BoardGameGeek Scraper
A small CLI to scrape data from the BoardGameGeek XMLAPI2, written in typescript and Bun.
Outputs the data in formatted json files.

### Getting started

First you need to get the latest available CSV dump from BoardGameGeeks: https://boardgamegeek.com/data_dumps/bg_ranks. This CSV is used to get the game ID:s to fetch information about.
Then to install dependencies:

```bash
bun install
```

To run:

```bash
bun run index.ts
```

See optional parameters below.

After a run, a stats file is generated named **\_stats\_[epochtime].json** in the output folder

### Parameters

**--path [path]**: Set a path to a CSV data dump to use

**--debug**: Does a sigle run, outputting the result plus the raw data (from boardgamegeekclient) into a ./debug folder

**--skip [number]**: Skips over the passed number of batches, starting at batch #[number]+1

**--batchsize [number]**: Set the batch size (default: 20)
