from pathlib import Path


def test_docker_assets_exist():
    files = [
        Path("Dockerfile"),
        Path("docker-compose.yml"),
        Path(".dockerignore"),
    ]
    for path in files:
        assert path.exists(), path
