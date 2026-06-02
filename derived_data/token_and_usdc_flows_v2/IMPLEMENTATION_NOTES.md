# Implementation notes for main.py completion

This file documents the gaps and tasks remaining to make main.py fully functional.

## Current status

main.py is 70% complete. It has:
- ✅ Git-clean check with `--run-dirty`
- ✅ Frontier loading and frontier-bounded partition enumeration
- ✅ V2 exchanges added to enumerate_partitions
- ✅ BLOB output schema
- ✅ Corrected metadata schema (per docs/Metadata files.md)
- ✅ Startup recovery (cleanup_temp_partitions)
- ✅ Ordered partition processing loop

Remaining:
- ❌ process_chunk: Full SQL generators for splits, merges, redeems, converts
- ❌ process_chunk: Atomic folder write logic
- ❌ Per-1M token ID precomputation (looping and caching)
- ❌ Input hash computation for metadata.input_hashes

## SQL generators to complete

### 1. _ct_split_sql(m_val, k_val) -> str
Generate split rows from ConditionalTokens/position_split.

**Output:**
- Row 0 (sub_index=0): stakeholder, 0 net_usdc, 0 net_tokens, NULL token_id, 'split'
- Rows 1..N: stakeholder, 0 net_usdc, +amount net_tokens, computed token_id per partition[i]

**Key logic:**
- USDC row: net_usdc = -amount (collateral locked)
- Token rows: one per partition index; compute token_id via keccak (already cached)

**SQL template:**
```sql
SELECT ... sub_index=0, net_usdc = -amount, net_tokens = 0, token_id = NULL
UNION ALL
SELECT ... (unnest partition array), sub_index = row_number, net_tokens = +amount, 
  token_id = join computed_token_ids for (collateral, parent, condition, partition[i])
```

### 2. _nr_split_sql(m_val, k_val) -> str
Generate split rows from NegRiskAdapter/position_split (simpler: always YES/NO pair).

**Output:**
- Row 0: 0 net_usdc, 0 net_tokens, NULL token_id
- Row 1: 0 net_usdc, +amount net_tokens, YES token_id (index_set=1)
- Row 2: 0 net_usdc, +amount net_tokens, NO token_id (index_set=2)

### 3. _ct_merge_sql(m_val, k_val) -> str
Inverse of CT split (net amounts negated).

### 4. _nr_merge_sql(m_val, k_val) -> str
Inverse of NR split.

### 5. _ct_redeem_sql(m_val, k_val) -> str
Generate redeem rows from ConditionalTokens/payout_redemption.

**Output:**
- Row 0: stakeholder, +payout net_usdc, 0 net_tokens, NULL token_id
- Rows 1..N: stakeholder, 0 net_usdc, **NULL** net_tokens (not -amount!), computed token_id per index_sets[i]

**Critical:** NR redeem token rows have NULL net_tokens (unknown burn), not explicit amounts.

### 6. _nr_redeem_sql(m_val, k_val) -> str
Generate redeem rows from NegRiskAdapter/payout_redemption.

**Difference from CT:** NR redeem token rows have explicit `-amounts[i]` for non-zero amounts (not NULL).

### 7. _convert_rows_python(con, m_val, k_val) -> list[dict]
Process NegRiskAdapter/positions_converted in Python (expensive logic).

**For each convert event:**
- Lookup market_id in question_to_condition to get all conditions in that market
- For each condition: if index_set bit is set → YES minted (+amount); else NO consumed (-amount)
- Return one row per condition

**Key fields:**
```python
{
    "block_number": int,
    "transaction_index": int,
    "transaction_hash": bytes,
    "log_index": int,
    "sub_index": int,  # one per condition
    "account": bytes,
    "token_id": bytes,  # YES or NO token computed
    "condition_id": bytes,
    "flow_type": "convert",
    "net_usdc": 0,
    "net_tokens": int (signed),
    "price_1e18": None,
    "collateral_token": USDC.e (constant),
}
```

## process_chunk refactor

```python
def process_chunk(
    con: duckdb.DuckDBPyConnection,
    m_val: int,
    k_val: int,
    *,
    log: logging.Logger,
) -> tuple[int, dict[str, str]]:
    """
    Process one 10K partition: generate rows, validate, write atomically.
    
    Returns (row_count, input_hashes).
    """
    # 1. Precompute token IDs for this 1M range (once per 1M, not per 10K)
    #    Note: add a module-level cache to avoid recomputing if same 1M seen again
    if not _precomputed_1m.get(m_val):
        precompute_token_ids_for_1m(con, m_val, log)
        _precomputed_1m[m_val] = True
    _loaded_token_ids_to_table(con)
    
    # 2. Load global lookup tables (once at startup, not per chunk)
    #    Note: global lookups should be loaded in main() before the loop
    
    # 3. Collect all SQL fragments
    sql_parts = []
    sql_parts.append(_trade_sql(con, "CTFExchange/order_filled", ...))
    sql_parts.append(_trade_sql(con, "NegRiskCtfExchange/order_filled", ...))
    sql_parts.append(_trade_sql_v2(con, "CTFExchangeV2/order_filled", ...))
    sql_parts.append(_trade_sql_v2(con, "NegRiskCtfExchangeV2/order_filled", ...))
    sql_parts.append(_ct_split_sql(m_val, k_val))
    sql_parts.append(_nr_split_sql(m_val, k_val))
    sql_parts.append(_ct_merge_sql(m_val, k_val))
    sql_parts.append(_nr_merge_sql(m_val, k_val))
    sql_parts.append(_ct_redeem_sql(m_val, k_val))
    sql_parts.append(_nr_redeem_sql(m_val, k_val))
    
    # 4. Execute main SQL
    if sql_parts:
        union_sql = "\n    UNION ALL\n".join([p for p in sql_parts if p])
        full_sql = f"""
        SELECT
            CAST(block_number AS UINT32) AS block_number,
            CAST(transaction_index AS UINT32) AS transaction_index,
            transaction_hash,  -- already BLOB from raw events
            CAST(log_index AS UINT32) AS log_index,
            CAST(sub_index AS UINT32) AS sub_index,
            raw_source,
            account,  -- already BLOB
            token_id,  -- already BLOB or NULL
            condition_id,  -- already BLOB
            flow_type,
            net_usdc,
            net_tokens,
            CASE WHEN flow_type IN ('trade_buy', 'trade_sell') AND net_tokens != 0
                THEN ABS(net_usdc)::HUGEINT * 1_000_000_000_000_000_000 / ABS(net_tokens)
                ELSE NULL
            END AS price_1e18,
            collateral_token  -- already BLOB
        FROM ({union_sql})
        ORDER BY block_number, log_index, sub_index
        """
        arrow_table = con.execute(full_sql).fetch_arrow_table()
    else:
        arrow_table = pa.table(
            {f.name: pa.array([], type=f.type) for f in _OUTPUT_SCHEMA},
            schema=_OUTPUT_SCHEMA,
        )
    
    # 5. Process convert rows (Python)
    convert_rows = _convert_rows_python(con, m_val, k_val)
    if convert_rows:
        convert_table = _convert_rows_to_arrow(convert_rows)
        if convert_table:
            arrow_table = pa.concat_tables([arrow_table, convert_table])
            # Re-sort
            indices = pa.compute.sort_indices(arrow_table, ...)
            arrow_table = arrow_table.take(indices)
    
    # 6. Validations
    # - Unique grain
    # - Bounds on net_usdc
    # - BLOB byte lengths
    
    # 7. Atomic write
    chunk_dir = Path(OUT_DIR, f"1M={m_val}", f"10K={k_val}")
    temp_dir = chunk_dir.parent / f".tmp_10K={k_val}_{int(time.time())}"
    temp_dir.mkdir(parents=True, exist_ok=True)
    
    try:
        out = temp_dir / "data.parquet"
        pq.write_table(arrow_table, str(out), compression="zstd", ...)
        
        # Compute input hashes BEFORE writing metadata
        input_hashes = _compute_input_hashes(m_val, k_val, con, log)
        
        _write_metadata(con, temp_dir, m_val, k_val, input_hashes, log)
        
        # Atomic rename
        if chunk_dir.exists():
            import shutil
            shutil.rmtree(chunk_dir)
        temp_dir.rename(chunk_dir)
    except Exception:
        import shutil
        shutil.rmtree(temp_dir, ignore_errors=True)
        raise
    
    return arrow_table.num_rows, input_hashes
```

## _compute_input_hashes function

```python
def _compute_input_hashes(m_val: int, k_val: int, 
                          con: duckdb.DuckDBPyConnection,
                          log: logging.Logger) -> dict[str, str]:
    """
    For each input parquet file read for this chunk, compute its sha256 hash.
    
    Returns a dict mapping relative paths to "sha256:..." hashes.
    """
    source_tables = [
        "CTFExchange/order_filled",
        ... (all 11 sources)
    ]
    
    hashes = {}
    for table in source_tables:
        path = _src_path(table, m_val, k_val)
        if path:
            hashes[path] = _sha256_file(Path(path))
            # Also relative path for portability
            rel_path = os.path.relpath(path, RAW)
            hashes[rel_path] = hashes[path]
    
    return hashes
```

## Key assumptions and TODOs

1. **Token ID computation assumes keccak + ECC are deterministic.** They are, via `lib/ct_helpers`.

2. **Temp folder naming:** Use `{k_dir}.tmp_{timestamp}` or `.tmp_10K={k_val}_{random}` to avoid collisions.

3. **Startup recovery:** If a folder like `.tmp_*` is found inside an 1M directory, delete it. This handles crashes mid-write.

4. **SIGINT handling:** Abort cleanly; on resume, the incomplete partition is cleaned up.

5. **Test the fee placement logic FIRST** (via the fee study) before finalizing SQL. The current _trade_sql assumes the logic is correct, but cross-check against a real transaction.

6. **Frontier bounding:** The frontier is a block number (highest guaranteed-complete block). A partition `10K={k}` is included if `k + 9_999 <= frontier`.

## Debugging checklist

- [ ] Git status clean at startup
- [ ] Frontier successfully loaded from upstream manifest
- [ ] enumerate_partitions includes V2 exchange tables
- [ ] Token ID cache populated for at least one 1M range
- [ ] First chunk processes without validation errors
- [ ] Input hashes computed and metadata written
- [ ] Temp folder renamed to final folder successfully
- [ ] BLOB byte lengths correct (20/32)
- [ ] price_1e18 computed only on trade rows
- [ ] net_tokens NULL only on CT redeem token rows
- [ ] Exchange-contract rows filtered
- [ ] Run pytest: tests should all pass
