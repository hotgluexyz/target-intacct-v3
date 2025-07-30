# target-intacct-v3

**target-intacct-v3** is a Singer Target for writing data to Sage Intacct.
**target-intacct-v3** can be run on [hotglue](https://hotglue.com), an embedded integration platform for running Singer Taps and Targets.

## Installation

```bash
pipx install target-intacct-v3
```

## Configuration

### Accepted Config Options

target is available by running:

```bash
target-intacct-v3 --about
```

### Config file example


```json
  { 
    "start_date": "2000-01-01T00:00:00.000Z",
    "company_id": "company",
    "sender_id": "sender",
    "sender_password": "**************",
    "user_id": "user",
    "user_password": "************",
    "input_path": "../.secrets" 
  }
```


## Usage

You can easily run `target-intacct-v3` by itself or in a pipeline using [Meltano](https://meltano.com/).

### Executing the Target Directly

```bash
target-intacct-v3 --version
target-intacct-v3 --help
# Test using the "Carbon Intensity" sample:
tap-carbon-intensity | target-intacct-v3 --config /path/to/target-intacct-v3-config.json
```

### Initialize your Development Environment

```bash
pipx install poetry
poetry install
```

### Create and Run Tests

Create tests within the `target_intacct_v3/tests` subfolder and
  then run:

```bash
poetry run pytest
```

You can also test the `target-intacct-v3` CLI interface directly using `poetry run`:

```bash
poetry run target-intacct-v3 --help
```

### SDK Dev Guide

See the [dev guide](https://sdk.meltano.com/en/latest/dev_guide.html) for more instructions on how to use the Meltano SDK to
develop your own Singer taps and targets.
