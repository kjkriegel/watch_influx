Export TRBOnet Watch metrics to InfluxDB
===

## Requirements
* TRBOnet Watch 3.x (4.0 with small changes)
* Python >= 3.7
* pyodbc
* InfluxDB 2.x

## Installation

```sh
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## Run
1. Edit `config.default.json` and rename to `config.json`
2. Run `python3 ./exporter.py`
