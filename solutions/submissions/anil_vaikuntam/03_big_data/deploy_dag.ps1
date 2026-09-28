# Deploys the Task 3.3 DAG + its sibling modules into the Airflow container's
# dags folder, following the README's documented docker exec workflow.
#
# NOTE: docker-compose.yml mounts ./starter_files -> /opt/airflow/dags on
# both the webserver and scheduler. That means files written into the
# container's /opt/airflow/dags land in the host's starter_files/ folder too
# (a bind mount is transparent both ways) -- this is inherent to the fixed
# infrastructure this assessment ships with, not a choice to develop inside
# starter_files/. The authoritative source for every file below still lives
# under solutions/submissions/anil_vaikuntam/; this script only *deploys* copies.
#
# Run from the repo root:  .\solutions\submissions\anil_vaikuntam\03_big_data\deploy_dag.ps1

$ErrorActionPreference = "Stop"
$SRC = "solutions\submissions\anil_vaikuntam"

Write-Host "Removing starter DAG + pycache from both Airflow containers..."
docker exec presight-airflow-webserver bash -c "rm -f /opt/airflow/dags/airflow_dag_starter.py && rm -rf /opt/airflow/dags/__pycache__"
docker exec presight-airflow-scheduler bash -c "rm -f /opt/airflow/dags/airflow_dag_starter.py && rm -rf /opt/airflow/dags/__pycache__"

Write-Host "Copying sibling modules + DAG into both containers..."
foreach ($container in @("presight-airflow-webserver", "presight-airflow-scheduler")) {
    docker cp "$SRC\01_foundations\etl_pipeline.py"      "${container}:/opt/airflow/dags/etl_pipeline.py"
    docker cp "$SRC\02_sql_and_viz\etl_full.py"           "${container}:/opt/airflow/dags/etl_full.py"
    docker cp "$SRC\04_infrastructure\dq_framework.py"    "${container}:/opt/airflow/dags/dq_framework.py"
    docker cp "$SRC\03_big_data\airflow_dag.py"           "${container}:/opt/airflow/dags/presight_etl_pipeline.py"
}

Write-Host "Restarting Airflow services to pick up the new DAG..."
docker compose restart airflow-webserver airflow-scheduler

Write-Host "Done. Open http://localhost:8081 (admin/admin) and look for 'presight_etl_pipeline'."
