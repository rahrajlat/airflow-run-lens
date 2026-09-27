<div align="center">

<img src="media/runlens-logo.png" alt="RunLens logo" width="120" />

# RunLens for Apache Airflow

### Compare DAG runs, catch regressions, and understand task-level drift inside the Airflow UI.

[![Apache Airflow](https://img.shields.io/badge/Apache_Airflow-3.x-017CEE?logo=apacheairflow&logoColor=white)](https://airflow.apache.org/)
[![Python](https://img.shields.io/badge/Python-3.10%2B-3776AB?logo=python&logoColor=white)](https://www.python.org/)
[![FastAPI](https://img.shields.io/badge/FastAPI-Backend-009688?logo=fastapi&logoColor=white)](https://fastapi.tiangolo.com/)
[![React](https://img.shields.io/badge/React-19-61DAFB?logo=react&logoColor=0B1020)](https://react.dev/)
[![TypeScript](https://img.shields.io/badge/TypeScript-5-3178C6?logo=typescript&logoColor=white)](https://www.typescriptlang.org/)
[![pnpm](https://img.shields.io/badge/pnpm-10-F69220?logo=pnpm&logoColor=white)](https://pnpm.io/)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker&logoColor=white)](https://www.docker.com/)
[![License](https://img.shields.io/badge/License-Apache_2.0-D22128?logo=apache&logoColor=white)](LICENSE)

[Apache Airflow](https://airflow.apache.org/) · [Airflow Plugins](https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/plugins.html) · [FastAPI](https://fastapi.tiangolo.com/) · [React](https://react.dev/)

</div>

RunLens is an Apache Airflow 3 plugin for comparing DAG runs, finding runtime regressions, and understanding how a DAG behaves over time.

It adds a native DAG-level RunLens page inside the Airflow UI. Pick two DAG runs, compare task duration/state/retry deltas, inspect missing/new tasks as DAGs evolve, and review recent historical performance for the current DAG.

The page has two core sections:

- **Run Compare**: compare two selected DAG runs task-by-task.
- **Historical View**: review the current DAG over a default 10-day window, including run duration trends and latest-vs-median task drift.

## Table of Contents

- [Features](#features)
- [Plugin Layout](#plugin-layout)
- [Install In An Existing Airflow 3 Environment](#install-in-an-existing-airflow-3-environment)
- [Local Development With This Repo](#local-development-with-this-repo)
- [Demo DAG](#demo-dag)
- [Historical View](#historical-view)
- [API Endpoints](#api-endpoints)
- [Development Notes](#development-notes)
- [Requirements](#requirements)
- [License](#license)

## Features

- Compare two runs of the current DAG
- See total duration, succeeded tasks, failed tasks, and retry deltas
- Compare task-level duration, state, and retries
- Filter by task and by task presence:
  - in both runs
  - base only
  - compare only
  - missing in either run
- Handle evolving DAGs where tasks exist in one run but not the other
- View historical DAG run duration over a configurable window, defaulting to the last 10 days
- Compare a selected task's latest duration against its historical median
- Spot recent task-level drift without manually picking a second run
- Native Airflow plugin route:

```text
/dags/<dag_id>/plugin/runlens
```

## Plugin Layout

The runtime plugin lives in:

```text
airflow_docker/plugins/runlens/
  __init__.py
  run_lens_backend_plugin.py
  runlens.js
```

`run_lens_backend_plugin.py` registers:

- FastAPI backend at `/runlens/api`
- React app bundle at `/runlens/api/static/runlens.js`
- DAG-level Airflow plugin page at `/dags/<dag_id>/plugin/runlens`

The React source lives in:

```text
airflow_docker/widgets/runlens-ui/
```

`pnpm run build` writes the compiled bundle directly into:

```text
airflow_docker/plugins/runlens/runlens.js
```

## Install In An Existing Airflow 3 Environment

Build the frontend bundle:

```bash
cd airflow_docker/widgets/runlens-ui
pnpm install
pnpm run build
```

Copy the plugin package into your Airflow plugins folder:

```bash
cp -R airflow_docker/plugins/runlens "$AIRFLOW_HOME/plugins/"
```

Your Airflow plugins folder should then contain:

```text
$AIRFLOW_HOME/plugins/runlens/
  __init__.py
  run_lens_backend_plugin.py
  runlens.js
```

Restart the Airflow API server/webserver components so plugins reload. For Docker Compose setups this is usually:

```bash
docker compose restart airflow-apiserver
```

If the plugin route does not appear, do a full recreate:

```bash
docker compose down
docker compose up -d
```

Open a DAG and visit:

```text
http://localhost:8080/dags/<dag_id>/plugin/runlens
```

## Local Development With This Repo

Build the Airflow image and initialize Airflow:

```bash
cd airflow_docker
docker compose build airflow-init
docker compose up airflow-init
docker compose up -d
```

Build the RunLens frontend after making UI changes:

```bash
cd airflow_docker/widgets/runlens-ui
pnpm install
pnpm run build
```

Restart Airflow after backend/plugin Python changes:

```bash
cd airflow_docker
docker compose restart airflow-apiserver
```

For plugin path or mount changes, prefer a full recreate:

```bash
docker compose down
docker compose up -d
```

## Demo DAG

This repo includes a demo DAG:

```text
airflow_docker/dags/run_lens_random_sleep_demo.py
```

It creates several normal Airflow `PythonOperator` tasks with random sleep durations. Trigger it multiple times to generate runs that are useful for testing RunLens comparisons and historical views.

## Historical View

RunLens includes a historical section below the pairwise comparison view. It is scoped to the current DAG and defaults to the last 10 days.

The historical section currently shows:

- **Run Duration Trend**: each recent DAG run as a duration bar, with final run state shown beside it.
- **Latest vs Median**: a selected task's latest duration compared with its median duration across the historical window.
- **Window selector**: switch between 10, 30, 60, and 90 day windows.
- **Task selector**: choose which task to inspect in the latest-vs-median chart.

This view answers a different question from pairwise compare. Pairwise compare asks, “what changed between these two runs?” Historical view asks, “is the latest behavior unusual for this DAG?”

## API Endpoints

RunLens exposes these backend endpoints under `/runlens/api`:

```text
GET /health
GET /dags
GET /dags/{dag_id}/runs
GET /dags/{dag_id}/runs/{run_id}/tasks
GET /dags/{dag_id}/compare?base_run_id=...&compare_run_id=...
GET /dags/{dag_id}/history?days=10
```

## Development Notes

- Airflow serves the built React bundle from the plugin package itself.
- The frontend bundle intentionally exposes `globalThis.AirflowPlugin`, which is how Airflow mounts the React plugin.
- The Vite build is configured with `emptyOutDir: false` so it does not delete the Python plugin files when writing `runlens.js`.
- `runlens.js` is committed because Airflow needs it at runtime. The source app and lockfile are also committed so the bundle can be rebuilt.

## Requirements

- Apache Airflow 3.x
- Python dependencies available in the Airflow image:
  - `fastapi`
  - Airflow metadata models
- Frontend build dependencies:
  - Node.js
  - pnpm

## License

Apache-2.0. See [LICENSE](LICENSE).
