"""Tests standard tap features using the built-in SDK tests library."""

import json
from pathlib import Path

import jsonschema
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


# Run standard built-in tap tests from the SDK:
def test_standard_tap_tests_gn_config():
    """Run standard tap tests from the SDK."""
    tests = get_standard_tap_tests(TapATInternet, config=SAMPLE_CONFIG_GN)
    for test in tests:
        test()


def test_standard_tap_tests_sp_config():
    """Run standard tap tests from the SDK."""
    tests = get_standard_tap_tests(TapATInternet, config=SAMPLE_CONFIG_SP)
    for test in tests:
        test()


def validate_record_schema(config):
    """
    Validate the tap records against the provided schema.

    This should part of the SDK standard tests (above), but this not the case yet
    (see https://gitlab.com/meltano/sdk/-/issues/228).

    Note: During ELT pipelines, target can be configured to validate the input records against
    the schema, but this is not an obligation.
    """
    tap = TapATInternet(config=config)
    streams = tap.discover_streams()

    for stream in streams:
        records_iterator = stream.get_records(context=None)
        first_record = next(records_iterator)
        print(first_record)
        print(stream.schema)
        try:
            jsonschema.validate(instance=first_record, schema=stream.schema)
        except AssertionError:
            raise AssertionError(
                f"Could not validate schema for stream {stream}.\n"
                f"Stream schema:\n"
                f"{stream.schema}\n"
                f"First record:\n"
                f"{first_record}"
            )


def test_record_schema_gn():
    validate_record_schema(SAMPLE_CONFIG_GN)


def test_record_schema_sp():
    validate_record_schema(SAMPLE_CONFIG_SP)


def run_incremental_sync(config):
    tap = TapATInternet(config=config, catalog=INCREMENTAL_CATALOG)
    tap.run_connection_test()


def test_incremental_sync_gn():
    run_incremental_sync(SAMPLE_CONFIG_GN)


def test_incremental_sync_sp():
    run_incremental_sync(SAMPLE_CONFIG_SP)


if __name__ == "__main__":
    test_incremental_sync_gn()
