# nba_etl

This is a [Dagster](https://dagster.io/) project scaffolded with [`dagster project scaffold`](https://docs.dagster.io/guides/build/projects/creating-a-new-project).

## Getting started

First, install your Dagster code location as a Python package. By using the --editable flag, pip will install your Python package in ["editable mode"](https://pip.pypa.io/en/latest/topics/local-project-installs/#editable-installs) so that as you develop, local code changes will automatically apply.

```bash
pip install -e ".[dev]"
```

Then, start the Dagster UI web server:

```bash
dagster dev
```

Open http://localhost:3000 with your browser to see the project.

Key components added:

- Assets: `all_players`, `all_teams`, `league_player_stats`, `player_profiles`, `player_embeddings_openai`
- Job: `moneyball_nba` (profiles from league stats + static metadata)
- IO Managers: Postgres (tables) and Mongo (embeddings). Optional: Qdrant later.

Quickstart (Moneyball pipeline):

1) Install editable package and extras

```bash
pip install -e ".[dev]"
```

2) Configure environment in `nba_etl/.env` or shell:

```bash
# Postgres
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=nba

# Mongo (optional for embeddings)
MONGO_HOST=localhost
MONGO_PORT=27017
MONGO_DB=nba
MONGO_COLLECTION=player_embeddings

# OpenAI (optional for embeddings asset)
OPENAI_API_KEY=sk-...
```

3) Run Dagster locally

```bash
dagster dev
```

4) In the UI, materialize the job `moneyball_nba` to build `league_player_stats` and `player_profiles`.

Optional run config example (limit API usage during dev):

```yaml
ops:
	league_player_stats:
		config:
			season: "2024-25"
			max_players: 150
```

Notes:

- `league_player_stats` uses `nba_api` game logs for season="2024-25". Adjust as needed.
- `player_embeddings_openai` writes to Mongo; set the API key to enable.

## Development

### Adding new Python dependencies

You can specify new Python dependencies in `pyproject.toml`.

### Unit testing

Tests are in the `nba_etl_tests` directory and you can run tests using `pytest`:

```bash
pytest nba_etl_tests
```

### Schedules and sensors

If you want to enable Dagster [Schedules](https://docs.dagster.io/guides/automate/schedules/) or [Sensors](https://docs.dagster.io/guides/automate/sensors/) for your jobs, the [Dagster Daemon](https://docs.dagster.io/guides/deploy/execution/dagster-daemon) process must be running. This is done automatically when you run `dagster dev`.

Once your Dagster Daemon is running, you can start turning on schedules and sensors for your jobs.

## Deploy on Dagster+

The easiest way to deploy your Dagster project is to use Dagster+.

Check out the [Dagster+ documentation](https://docs.dagster.io/dagster-plus/) to learn more.
