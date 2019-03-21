from . import ESImputation

def test_ESImputation():
    assert ESImputation.apply("Jane") == "hello Jane"
