import tomllib


def get_configs(path: str) -> dict:
    with open(path, "rb") as toml:
        confs = tomllib.load(toml)
    return confs
