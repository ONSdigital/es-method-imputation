import traceback
import Algorithmia
import pandas as pd
import os

client = Algorithmia.client()


def _get_fh(data_url):
    """
    Opens Algorithmia data urls and returns a file object.
    :param data_url: data url to open.
    :return: file object.
    :raises: AlgorithmException.
    """
    try:
        fh = client.file(data_url).getFile()
    except Algorithmia.errors.DataApiError as exc:
        raise Algorithmia.errors.AlgorithmException(
            "Unable to get datafile {}: {}".format(
                data_url, ''.join(exc.args)
            )
        )
    return fh


def _get_traceback(exception):
    """
    Given an exception, returns the traceback as a string.
    :param exception: Exception object.
    :return: String.
    """
    return ''.join(
        traceback.format_exception(
            etype=type(exception), value=exception, tb=exception.__traceback__
        )
    )


def apply(input):

    # API calls will begin at the apply() method, with the request body passed as 'input'
    # For more details, see algorithmia.com/developers/algorithm-development/languages

    try:
        imp_df = do_imputation(_get_fh("s3+input://es-algo-poc/enrichment_output.csv"))
    except Algorithmia.errors.AlgorithmException as exc:
        return {
            "success": False,
            "error": _get_traceback(exc)
        }
    except Exception as exc:
        return {
            "success": False,
            "error": "Unexpected exception {}".format(_get_traceback(exc))
        }

    output = imp_df.sum().reset_index().to_csv(index=False)
    Algorithmia.client().file("s3+input://es-algo-poc/imputation_output.csv").put(str(output))

    return {
            "success": True,
            "data": output  # .to_json(orient='records')[1:-1].replace('}, {','},{')
            }


def do_imputation(input_df):
    period = 201809
    # cwd = os.path.dirname(__file__)
    # imported_data = pd.read_csv('{}/tests/fixtures/test_data_imputation.csv'.format(cwd), index_col=False)
    responder_data = check_non_response(input_df, period)

    return responder_data  # .to_json(orient='records')


def check_non_response(data, period):
    # Create a dataframe where the response column value is set as 1 i.e non responders
    non_response_filter = (data["response_type"] == 1) & (data["period"] == period)
    filtered_non_responders = data[non_response_filter]
    # check the length of the dataset - if there are rows then run imputation...
    response_check = len(filtered_non_responders.index)
    if response_check > 0:
        # Ensure that only responder_ids with a response type of 2 (returned) get
        # picked up
        data = data[data['response_type'] == 2]
        # Select and order the columns required for imputation.
        ordered_data = data[['responder_id', 'land_or_marine', 'region', 'strata', 'Q601_asphalting_sand',
                             'Q602_building_soft_sand', 'Q603_concreting_sand', 'Q604_bituminous_gravel',
                             'Q605_concreting_gravel', 'Q606_other_gravel',
                             'Q607_constructional_fill', 'Q608_total', 'period'

                             ]]
        return ordered_data
    else:
        print('There are no non responders!')
        print('Skipping Imputation!')
        return

# if __name__ == "__main__":
    # apply("z")
