import traceback
import boto3
import pandas as pd
from io import StringIO

client = boto3.client('s3')
client_out = boto3.resource('s3')
bucket_name = 'es-algo-poc'
input_csv = 'enrichment_output.csv'
output_csv = 'imputation_output.csv'


def _get_traceback(exception):
    """
    Given an exception, returns the traceback as a string.
    :param exception: Exception object
    :return: string
    """
    return ''.join(
        traceback.format_exception(
            etype=type(exception), value=exception, tb=exception.__traceback__
        )
    )


def lambda_handler(event, context):
    try:
        # Not sure where period will be picked up from - hard coded for now. - DF
        period = 201809

        input_obj = client.get_object(Bucket=bucket_name, Key=input_csv)
        input_body = input_obj['Body']
        input_string = input_body.read().decode('utf-8')

        input_df = pd.read_csv(StringIO(input_string))

        imputed_df = do_imputation(input_df, period)

    except Exception as exc:
        return {
            "success": False,
            "error": "Unexpected exception {}".format(_get_traceback(exc))
        }

    csv_buffer = StringIO()
    _buffering = imputed_df.to_csv(csv_buffer)

    client_out.Object(bucket_name, output_csv).put(Body=csv_buffer.getvalue())

    lambda_retval = {"success": True}

    return lambda_retval


def local_handler():
    try:
        import os
        period = 201809
        cwd = os.path.dirname(__file__)
        input_df = pd.read_csv('{}/tests/resources/impute_input_data.csv'.format(cwd), index_col=False)

        check_output = do_imputation(input_df, period)

    except Exception as exc:
        return {
            "success": False,
            "error": "Unexpected exception {}".format(_get_traceback(exc))
        }

    local_retval = {"success": True}
    print(str(check_output))

    return local_retval


def do_imputation(input_df, period):
    # Do non-response
    responder_data = check_non_response(input_df, period)
    # Do something else

    # Do the last thing

    return responder_data  # .to_json(orient='records')


def check_non_response(data, period):
    # Create a dataframe where the response column value is set as 1 i.e non responders
    filtered_non_responders = data.loc[(data['response_type'] == 1)
                                      &(data['period'] == period)]

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
        output = {
            "success": False,
            "status": "Imputation skipped"
        }
        return output


if __name__ == "__main__":
    retval = local_handler()
    print(str(retval))
