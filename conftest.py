import pytest


def pytest_addoption(parser):
    parser.addoption(
        "--run-web-tests",
        action="store_true",
        default=False,
        help="Run tests that require a web connection",
    )


def pytest_collection_modifyitems(config, items):
    if config.getoption("--run-web-tests"):
        return
    skip_web = pytest.mark.skip(reason="Need --run-web-tests option to run")
    for item in items:
        if "web_dependency" in item.keywords:
            item.add_marker(skip_web)
