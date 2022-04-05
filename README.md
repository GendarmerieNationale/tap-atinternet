# tap-atinternet

`tap-atinternet` is a Singer tap for the [AT Internet Reporting API v3](https://developers.atinternet-solutions.com/data-api-en/reporting-api-v3/getting-started/how-does-it-work).

Built with the [Meltano Tap SDK](https://sdk.meltano.com) for Singer Taps.

## Installation

You can install the Singer tap with:

```bash
pipx install git+https://github.com/GendarmerieNationale/tap-atinternet.git
```

## Configuration

| Setting     | Required | Default | Description                                                                              |
|:------------|:--------:|:-------:|:-----------------------------------------------------------------------------------------|
| api_key     |   True   |  None   | AT Internet API key                                                                      |
| secret_key  |   True   |  None   | AT Internet secret key                                                                   |
| site_id     |   True   |  None   | Site ID (can be queried at https://dataquery.atinternet-solutions.com/)                  |
| start_date  |   True   |  None   | Start syncing data from that date                                                        |
| max_results |  False   |  5000   | Max number of results per page (up to 10000)                                             |
| filter_str  |  False   |   ""    | If not empty, filter and extract only the pages with this string in the 'page_full_name' |

A full list of supported settings and capabilities for this tap is available by running:

```bash
tap-atinternet --about
```

It can be used with both `FULL_TABLE` or `INCREMENTAL` replication methods, to be specified in the tap metadata 
([how to do it with meltano](https://docs.meltano.com/concepts/plugins#metadata-extra)).

**Note: ⚠️ in incremental mode, the tap will produce duplicate records.**
With incremental sync (and `date` as the `replication_key`), the tap will use the last state value instead of the 
`start_date` specified in the config, and will extract data for that date even though it has already been extracted last time.

Example:
The last state will typically something like 
```json
{
  "bookmarks": {
    "visits": {
      "replication_key": "date",
      "replication_key_value": "2022-04-05"
    }
  }
}
```
if last time you extracted data up to 2022-04-05 (_included_). Next time you call the tap in incremental mode 
(and with the same `job_id` if using meltano), the extraction will start from 2022-04-05 (_included_), which will 
likely lead to duplicate records for this date.

According to the meltano team (see [issue](https://gitlab.com/meltano/meltano/-/issues/2504)), 
this is expected, and you can deal with it by removing the potential duplicates in the Transform layer, e.g. with dbt:
> Singer does support incremental replication that append instead of upsert - but the promise is that each
> row arrives "at least once" and not necessarily "exactly once".
> I've dealt with this in the past at the dbt layer with much success.

## Usage

You can easily run `tap-atinternet` by itself or in a pipeline using [Meltano](https://meltano.com/).

### Executing the Tap Directly

```bash
tap-atinternet --version
tap-atinternet --help
tap-atinternet --config CONFIG --discover > ./catalog.json
```

## Developer Resources

### Initialize your Development Environment

```bash
pipx install poetry
poetry install
```

### Create and Run Tests

Create tests within the `tap_atinternet/tests` subfolder and
  then run:

```bash
poetry run pytest
```

You can also test the `tap-atinternet` CLI interface directly using `poetry run`:

```bash
poetry run tap-atinternet --help
```

### Testing with [Meltano](https://www.meltano.com)

_**Note:** This tap will work in any Singer environment and does not require Meltano.
Examples here are for convenience and to streamline end-to-end orchestration scenarios._

Your project comes with a custom `meltano.yml` project file already created. Open the `meltano.yml` and follow any _"TODO"_ items listed in
the file.

Next, install Meltano (if you haven't already) and any needed plugins:

```bash
# Install meltano
pipx install meltano
# Initialize meltano within this directory
cd tap-atinternet
meltano install
```

Now you can test and orchestrate using Meltano:

```bash
# Test invocation:
meltano invoke tap-atinternet --version
# OR run a test `elt` pipeline:
meltano elt tap-atinternet target-jsonl
```

### SDK Dev Guide

See the [dev guide](https://sdk.meltano.com/en/latest/dev_guide.html) for more instructions on how to use the SDK to 
develop your own taps and targets.


## Contact
Gendarmerie Nationale, cyberimpact@gendarmerie.interieur.gouv.fr

This was developed as part of the [CyberImpact EIG project](https://eig.etalab.gouv.fr/defis/cyberimp-ct/).
