if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


def get_column_rename_map():
    #q5
    return {
        'VendorID': 'vendor_id',
        'RatecodeID': 'record_id',
        'PULocationID': 'pickup_location_id',
        'DOLocationID': 'dropoff_location_id',
        'lpep_pickup_datetime': 'pickup_datetime',
        'lpep_dropoff_datetime': 'dropoff_datetime',
    }

@transformer
def transform(data, *args, **kwargs):
    """
    Template code for a transformer block.

    Add more parameters to this function if this block has multiple parent blocks.
    There should be one parameter for each output variable from each parent block.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    data.rename(columns=get_column_rename_map(), inplace=True)
    data = data[(data['passenger_count'] > 0) & (data['trip_distance'] > 0)]
    print('q2', data.shape)
    # q3
    data['pickup_date'] = data['pickup_datetime'].dt.date
    print('q4', data['vendor_id'].unique().tolist())

    return data


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
    assert 'vendor_id' in output.columns, 'vendor_id column not found'
    assert len(output[output['passenger_count'] == 0]) == 0, 'found invalid rows which passenger_count is zero'
    assert len(output[output['trip_distance'] == 0]) == 0, 'found invalid rows which trip_distance is zero' 
