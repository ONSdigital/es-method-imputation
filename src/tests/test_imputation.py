"""  This Module is for the testing of the imputation module. """
import os
from unittest import TestCase, mock
from src import imputation


class ImputationTestCase(TestCase):
    """ This is a class for the imputation testing. """

    def _get_test_data_fh(self, filename):
        """
        Open a file under the fixtures directory.
        Opens the file in read mode and returns the file object.
        :param filename: file url.
        :return: file object.
        """
        filename = os.path.abspath(
            os.path.join(
                os.path.dirname(__file__),
                'fixtures',
                filename
            )
        )
        return open(filename, 'r')

    @mock.patch('src.imputation.client')
    def test_successful_imputation(self, mock_client):
        """
        Mocking the connection to the client.
        :param mock_client:
        :return:
        """
        test_fh = self._get_test_data_fh("test_data_imputation.csv")
        mock_client.file.return_value.getFile.return_value = test_fh
        result = imputation.apply(
            {"s3Pointer": "es-algo-poc/luke2.csv"}
        )

        print(str(result))
        assert "success" in result
        assert result["success"] is True
