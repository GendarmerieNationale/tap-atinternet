"""Tests standard tap features using the built-in SDK tests library."""

import json
from pathlib import Path

import jsonschema
import pytest
from singer_sdk.testing import get_standard_tap_tests

from tap_atinternet.tap import TapATInternet

with open(Path(__file__).parents[2] / ".secrets/config.gn.json") as f:
    SAMPLE_CONFIG_GN = json.load(f)
with open(Path(__file__).parents[2] / ".secrets/config.sp.json") as f:
    SAMPLE_CONFIG_SP = json.load(f)
with open(Path(__file__).parent / "incremental_catalog.json") as f:
    # you can obtain this file by configuring the tap to incremental sync, and running
    # `meltano elt tap-atinternet target-jsonl --dump catalog`
    INCREMENTAL_CATALOG = json.load(f)


@pytest.fixture
def configs():
    return [SAMPLE_CONFIG_GN, SAMPLE_CONFIG_SP]


# Run standard built-in tap tests from the SDK:
def test_standard_tap_tests(configs):
    """Run standard tap tests from the SDK."""
    for config in configs:
        tests = get_standard_tap_tests(TapATInternet, config=config)
        for test in tests:
            test()


# Additional tests
def test_incremental_sync(configs):
    for config in configs:
        tap = TapATInternet(config=config, catalog=INCREMENTAL_CATALOG)
        tap.run_connection_test()


def test_record_schema(configs):
    """
    Validate the tap records against the provided schema.

    This should part of the SDK standard tests (above), but this not the case yet
    (see https://gitlab.com/meltano/sdk/-/issues/228).

    Note: During ELT pipelines, target can be configured to validate the input records against
    the schema, but this is not an obligation.
    """
    for config in configs:
        tap = TapATInternet(config=config)
        tap.run_connection_test()  # start with this to initialize stream_state

        streams = tap.discover_streams()
        for stream in streams:
            records_iterator = stream.get_records(context=None)
            first_record = next(records_iterator)
            print('Record:\n', first_record)
            print('Schema:\n', stream.schema)
            jsonschema.validate(instance=first_record, schema=stream.schema)
