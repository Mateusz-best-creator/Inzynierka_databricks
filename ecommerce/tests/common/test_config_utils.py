import os
import pytest

from ecommerce.common_functions.config_utils import (
    get_config_path,
    get_default_config_path,
)


def test_get_config_path() -> None:
    # Verify environment-specific config path construction
    root = "/mock/root"
    layer = "silver"
    env = "dev"
    filename = "users.json"

    expected = os.path.join(root, "ecommerce", "table_properties", env, layer, filename)
    actual = get_config_path(root, layer, env, filename)

    assert actual == expected


def test_get_default_config_path() -> None:
    # Verify default config path construction
    root = "/mock/root"
    filename = "default_properties.json"

    expected = os.path.join(root, "ecommerce", "table_properties", filename)
    actual = get_default_config_path(root, filename)

    assert actual == expected
