def test_run_handles_empty_tpn_list(monkeypatch, tmp_path):
    dummy_logger = type("L", (), {"info": lambda self, msg: None})()
    monkeypatch.setattr(utils, "setup_logger", lambda *a, **k: dummy_logger)

    gd_basic_path = tmp_path / "BasicTable.xlsx"
    df = pd.DataFrame({
        "start_date": [pd.Timestamp("2024-01-01")],
        "TPN": [pd.NA],
        "store_list": [123]
    })
    with pd.ExcelWriter(gd_basic_path) as writer:
        df.to_excel(writer, sheet_name="phase1", index=False)
    pd.ExcelWriter(gd_basic_path).save()
    monkeypatch.setattr(pd, "ExcelFile", lambda path: pd.ExcelFile(gd_basic_path))
    monkeypatch.setattr(pd, "read_excel", lambda *a, **k: df)

    called_queries = []
    monkeypatch.setattr(database, "run_query", lambda q: called_queries.append(q) or pd.DataFrame(columns=[
        'Phase','Country','Store Nr','Store Name','Store Format','Fiscal Week','Date','Day','Department','TPN','Product Description','Sales','Scan Margin','Scan Margin Cons','Sold Unit','Stock Unit','Stock Value'
    ]))

    monkeypatch.setattr(paths, "HDP_PATH", str(tmp_path))

    run()

    assert len(called_queries) == 1
    # empty tpn list should still produce well-formed query without double commas or syntax errors
    assert "IN ()" not in called_queries[0]