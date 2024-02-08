import psycopg2
if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data(data, *args, **kwargs):
    conn = psycopg2.connect(
        dbname="postgres",
        user="root",
        password="root",
        host="de-zoomcamp-week3-postgres",
        port="5432"
    )

    # Create a cursor object
    cur = conn.cursor()

    # Execute a SQL command
    cur.execute(
        """CREATE TABLE green_taxi_monthly_clustered (LIKE green_taxi_data INCLUDING ALL)
        PARTITION BY RANGE (date_trunc('day', lpep_pickup_datetime));""")

    for month in range(1, 13):
        year = 2022
        first_day = f"{year}-{month:02d}-01"
        last_day = f"{year}-{(month+1):02d}-01"
        if month == 12:
            last_day = "2023-01-01"
        cur.execute(f"""CREATE TABLE green_taxi_monthly_clustered_{month} PARTITION OF postgres.public.green_taxi_monthly_clustered
        FOR VALUES FROM ('{first_day}') TO ('{last_day}');""")

        cur.execute(f"""ALTER TABLE green_taxi_monthly_clustered_{month} ADD CONSTRAINT green_taxi_monthly_clustered_{month}_check
        CHECK (lpep_pickup_datetime >= '{first_day}' AND lpep_pickup_datetime < '{last_day}');""")

    conn.commit()

    cur.execute(
        """INSERT INTO postgres.public.green_taxi_monthly_clustered
            SELECT *
            FROM (select *
                from postgres.public.green_taxi_data
                where lpep_pickup_datetime >= '2022-01-01'
                    and lpep_pickup_datetime < '2023-01-01') as "cleaned_data";""")

    conn.commit()

    # Close the cursor and connection
    cur.close()
    conn.close()


