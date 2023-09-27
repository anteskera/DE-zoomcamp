from prefect.infrastructure.container import DockerContainer

docker_block = DockerContainer(
    image="discdiver/prefect:zoom",
    image_pull_policy="ALWAYS",
    auto_remove=True
)

docker_block.save("zoom", overwrite=True)

docker_container_block = DockerContainer.load("zoom")