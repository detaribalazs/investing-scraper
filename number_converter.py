import yaml

cache_file = "./cache/mc.yaml"
if __name__ == "__main__":
    with open(cache_file, "r") as f:
        yaml_data = yaml.safe_load(f)

    