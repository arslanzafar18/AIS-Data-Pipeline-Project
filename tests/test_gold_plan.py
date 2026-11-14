import os, time
from datetime import date, datetime, timezone
from spark import gold_plan as gp

D = date(2026, 11, 12)


def s(y, m, d, h=0, mi=0):
    return int(datetime(y, m, d, h, mi, tzinfo=timezone.utc).timestamp())


def test_plan_start_recent_days_only():
    mt = {date(2026, 11, d): 100.0 for d in range(1, 13)}
    assert gp.plan_start(mt, D, date(2026, 11, 1), 2, last_success_epoch=1000.0) == date(2026, 11, 11)


def test_plan_start_includes_older_changed_day():
    mt = {date(2026, 11, d): 100.0 for d in range(1, 13)}
    mt[date(2026, 11, 4)] = 5000.0                                  # backfill landed on 4 Nov
    assert gp.plan_start(mt, D, date(2026, 11, 1), 2, last_success_epoch=1000.0) == date(2026, 11, 4)


def test_plan_start_ignores_days_outside_window_and_first_run_takes_all():
    mt = {date(2026, 10, 1): 9e9, date(2026, 11, 5): 1.0, D: 1.0}
    assert gp.plan_start(mt, D, date(2026, 11, 3), 2, last_success_epoch=1000.0) == D      # 1 Oct is outside
    assert gp.plan_start(mt, D, date(2026, 11, 3), 2, last_success_epoch=None) == date(2026, 11, 5)


def _mk(path, name="part.parquet"):
    os.makedirs(path, exist_ok=True)
    open(os.path.join(path, name), "w").write("x")


def test_silver_day_mtimes_uses_newest_data_file(tmp_path):
    _mk(tmp_path / "dt=2026-11-11" / "hour=3")
    p = tmp_path / "dt=2026-11-12" / "hour=1"; _mk(p)
    os.utime(p / "part.parquet", (2000, 2000))
    _mk(tmp_path / "dt=2026-11-12" / "hour=2", "_SUCCESS")         # ignored
    mt = gp.silver_day_mtimes(str(tmp_path))
    assert set(mt) == {date(2026, 11, 11), D} and mt[D] == 2000


def test_swap_partitions_replaces_only_given_days(tmp_path):
    live, stage = tmp_path / "live", tmp_path / "stage"
    for d in ("2026-11-10", "2026-11-11", "2026-11-12"):
        _mk(live / f"dt={d}", "old")
    _mk(stage / "dt=2026-11-12", "new")
    gp.swap_partitions(str(stage), str(live), [date(2026, 11, 11), D])
    assert sorted(os.listdir(live)) == ["dt=2026-11-10", "dt=2026-11-12"]   # 11th had no rows any more
    assert os.listdir(live / "dt=2026-11-12") == ["new"] and os.listdir(live / "dt=2026-11-10") == ["old"]


def test_drop_partitions_before_and_is_partitioned(tmp_path):
    for d in ("2026-08-01", "2026-11-12"):
        _mk(tmp_path / f"dt={d}")
    assert gp.is_partitioned(str(tmp_path))
    assert gp.drop_partitions_before(str(tmp_path), date(2026, 8, 15)) == ["dt=2026-08-01"]
    assert os.listdir(tmp_path) == ["dt=2026-11-12"]


def test_swap_dir_replaces_whole_table(tmp_path):
    _mk(tmp_path / "t", "old"); _mk(tmp_path / "new", "fresh")
    gp.swap_dir(str(tmp_path / "new"), str(tmp_path / "t"))
    assert os.listdir(tmp_path / "t") == ["fresh"] and not (tmp_path / "_prev_t").exists()
