import yaml


def test_library_refs():
    yaml_file = open("repositories.yaml", "r")
    repositories = yaml.safe_load(yaml_file)
    yaml_file.close()
    libs = set(library["library_name"] for library in repositories["libraries"])
    for app in repositories["applications"]:
        if missing_libs := set(app["dependencies"]) - libs:
            raise KeyError(
                f'application {app["app_name"]} contains invalid library references: {missing_libs}'
            )
