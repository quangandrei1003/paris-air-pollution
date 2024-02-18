from prefect.deployments import Deployment
from prefect.infrastructure.docker import DockerContainer
from prefect.orion.schemas.schedules import CronSchedule
from current_parameterized_flow import etl_current_data_main_flow, etl_gcs_to_bq

docker_block = DockerContainer.load("airpollution")

hourly_dep = Deployment.build_from_flow(
    flow=etl_current_data_main_flow,
    name="current-flow",
    infrastructure=docker_block,
    # schedule=(CronSchedule(cron="*/2 * * * *", timezone="Europe/Paris")),
)

bq_dep = Deployment.build_from_flow(
    flow=etl_gcs_to_bq,
    name="current-bg-flow",
    infrastructure=docker_block,
)

if __name__ == "__main__":
    hourly_dep.apply()
    bq_dep.apply()

