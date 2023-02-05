from typing import Optional

from prefect import flow

from project.data_ingestion.base_etl import etl as base_etl


@flow()
def etl_flow(
        years: Optional[list[int]] = None,
        months: Optional[list[int]] = None,
        days: Optional[list[int]] = None,
        hours: Optional[list[int]] = None
):
    if years is None:
        years = [2023]

    if months is None:
        months = range(1, 13)

    if days is None:
        days = range(1, 32)

    if hours is None:
        hours = range(24)

    for year in years:
        for month in months:
            for day in days:
                for hour in hours:
                    try:
                        base_etl(year, month, day, hour)
                    except Exception as e:
                        print(e)


if __name__ == '__main__':
    etl_flow([2023], [1], [1, 2, 3], [5])
