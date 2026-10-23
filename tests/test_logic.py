from spark.common import haversine_py

def test_haversine_known_distance():
    assert 150_000 < haversine_py(60.3913, 5.3221, 58.9700, 5.7331) < 165_000

def test_zero_distance():
    assert haversine_py(60, 5, 60, 5) == 0
