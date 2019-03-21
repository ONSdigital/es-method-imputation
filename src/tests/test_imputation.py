from src import es_imputation

class TestESImputation:

    def test_imputation(self):
        assert es_imputation.apply("Jane") == "hello Jane"
