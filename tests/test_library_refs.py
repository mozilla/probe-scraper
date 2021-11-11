import yaml


def test_library_refs():
    yaml_file = open("repositories.yaml", "r")
    repositories = yaml.safe_load(yaml_file)
    yaml_file.close()
    dependency_list = []
    for library in repositories["libraries"]:
        for variant in library["variants"]:
            dependency_list.append(variant["dependency_name"])
    libs = set(dependency_list)
    for app in repositories["applications"]:
        missing_libs = set(app["dependencies"]) - libs
        if missing_libs:
            raise KeyError(
                f'application {app["app_name"]} contains invalid library references: {missing_libs}'
            )
