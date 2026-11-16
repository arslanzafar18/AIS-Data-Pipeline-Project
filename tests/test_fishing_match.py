"""Fishing register in Gold: matched on call sign, confirmed by name or length, one row per vessel."""
from spark.gold_logic import match_fishing_register, no_fishing_register

VESSELS = "mmsi long, name string, call_sign string, length_m int"
REGISTER = "vessel_id string, call_sign string, name string, length_m string"


def _match(spark, vessels, register, tol=3.0):
    out = match_fishing_register(spark.createDataFrame(vessels, VESSELS), spark.createDataFrame(register, REGISTER), tol)
    return {r["mmsi"]: (r["fishing_match"], r["fishing_vessel_id"]) for r in out.collect()}


def test_call_sign_confirmed_by_name_or_length(spark):
    reg = [("R1", "LK-2345", "KYSTFISK", "18.2"), ("R2", "LM9999", "HAVBRIS", "30.0"), ("R3", "LN1111", "SJØGUTTEN", "12.0")]
    got = _match(spark, [
        (1, "KYSTFISK", "lk 2345", 0),      # name agrees, length unknown            -> strong
        (2, "HAV BRIS II", "LM9999", 31),   # name differs, length within 3 m       -> strong
        (3, "SJOGUTT", "LN1111", 40),       # name differs, length off by 28 m      -> weak
        (4, "SUPPLY ONE", "LAXX", 80),      # not in the register                   -> none
        (5, "NO SIGN", None, 10)], reg)     # no call sign in AIS                    -> none
    assert got == {1: ("strong", "R1"), 2: ("strong", "R2"), 3: ("weak", "R3"), 4: (None, None), 5: (None, None)}


def test_one_row_per_vessel_when_a_call_sign_is_listed_twice(spark):
    reg = [("R1", "LK2345", "OLD BOAT", "40.0"), ("R2", "LK2345", "KYSTFISK", "18.0")]
    got = _match(spark, [(1, "KYSTFISK", "LK2345", 18)], reg)
    assert got == {1: ("strong", "R2")}                 # the register row that agrees wins; no duplicate vessel


def test_empty_register_call_signs_never_match(spark):
    got = _match(spark, [(1, "X", "", 10)], [("R1", "", "X", "10")])
    assert got == {1: (None, None)}


def test_without_register_the_columns_still_exist(spark):
    out = no_fishing_register(spark.createDataFrame([(1, "X", "LK1", 10)], VESSELS))
    assert {"fishing_match", "fishing_vessel_id"} <= set(out.columns) and out.collect()[0]["fishing_match"] is None

