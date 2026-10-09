# Deploys the Task 3.3 DAG + its sibling modules into the Airflow containers.
#
# The base docker-compose.yml mounts ./starter_files -> /opt/airflow/dags, so
# anything written into the container's dags folder would land in the host's
# starter_files/ (a bind mount is transparent both ways). To keep starter_files/
# untouched, this script recreates the two Airflow containers with the
# docker-compose.airflow.yml override next to this script, which replaces that
# mount and bind-mounts the DAG + modules read-only from this submission folder.
# Because they are live mounts, later edits need no redeploy (Airflow re-parses
# the dags folder on its own); re-run only if the containers were recreated
# from the base file alone.
#
# Run from the repo root:  .\solutions\submissions\anil_vaikuntam\03_big_data\deploy_dag.ps1

$ErrorActionPreference = "Stop"
$OVERRIDE = "solutions\submissions\anil_vaikuntam\03_big_data\docker-compose.airflow.yml"

Write-Host "Recreating Airflow webserver + scheduler with the submission DAG mounts..."
# docker compose writes progress to stderr, which Windows PowerShell 5.1 turns
# into a terminating error under "Stop" — rely on the exit code instead.
$ErrorActionPreference = "Continue"
docker compose -f docker-compose.yml -f $OVERRIDE up -d --no-deps --force-recreate airflow-webserver airflow-scheduler
if ($LASTEXITCODE -ne 0) { throw "docker compose up failed (exit $LASTEXITCODE)" }

Write-Host "Done. Open http://localhost:8081 (admin/admin) and look for 'presight_etl_pipeline'."
