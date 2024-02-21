from mage_ai.orchestration.run_status_checker import check_status

if 'sensor' not in globals():
    from mage_ai.data_preparation.decorators import sensor


@sensor
def check_condition(*args, **kwargs) -> bool:
    """
    Template code for checking if block or pipeline run completed.
    """
    if kwargs['url'] is None:
        print('you should provide the url for extracting data')
        return False
    
    if kwargs['tbl_name'] is None:
        print('you should provide the tbl_name for loading data')
        return False

    return True