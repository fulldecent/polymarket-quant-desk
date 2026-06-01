# Metadata file specification

The raw_data and derived_data folders in this project generate parquet.data files comprising a dataset.

Following is a description of the metadata.json files that accompany each parquet.data file.

## Notes

The metadata file is a small dictionary of informaton on the dataset to support supply chain traceability of each generated file.

## Shape

The JSON is a single object with the following fields:

- `dataset`: dataset name, including version (e.g. `polygon_contract_events_v3`)
- `created_at`: UTC timestamp in ISO 8601 with Z suffix
- `source_script`: script that generated the file
- `git_commit`: commit hash used at generation time
- `row_count`: number of rows in the parquet file
- `file_size_bytes`: parquet file size in bytes
- `content_hash`: sha256 hash string, prefixed like sha256:...
- `input_hashes`: map of input file path to sha256 hash (required for derived datasets)
- `parameters`: optional run parameters, such as min and max block

## Rules

- Empty partitions (i.e. row count is zero) must still have metadata.json
- No data files nor metadata shall be generated if git working directory is dirty (`git diff --quiet` must return status 0)

## Example

```json
{
  "dataset": "polygon_contract_events_v3",
  "created_at": "2026-02-23T15:10:00Z",
  "source_script": "aggregate_trader_markets_10k.py",
  "git_commit": "a1b2c3d4e5f6",
  "row_count": 124837,
  "file_size_bytes": 2847392,
  "content_hash": "sha256:...",
  "input_hashes": {
    "path/to/input.parquet": "sha256:..."
  },
  "parameters": {
    "min_block": 82880000,
    "max_block": 82889999
  }
}
```
