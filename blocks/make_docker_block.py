from prefect.docker import (
    DockerContainer,
)

# alternative to creating DockerContainer block in the UI
docker_block = DockerContainer(
    image="quangcongnguyen1003/quangnc-air-pollution:lastest",  # insert your image here
    image_pull_policy="ALWAYS",
    auto_remove=True,
)

docker_block.save("airpollution", overwrite=True)
