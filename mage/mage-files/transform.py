import pandas as pd

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(df, *args, **kwargs):
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
    # Specify your transformation logic here
    df['tpep_pickup_datetime']= pd.to_datetime(df['tpep_pickup_datetime'])
    df['tpep_dropoff_datetime']= pd.to_datetime(df['tpep_dropoff_datetime'])

    datetime_table = df[['tpep_pickup_datetime','tpep_dropoff_datetime']].drop_duplicates().reset_index(drop=True)  
    datetime_table['pick_year']=datetime_table['tpep_pickup_datetime'].dt.year          
    datetime_table['pick_month']=datetime_table['tpep_pickup_datetime'].dt.month
    datetime_table['pick_day']=datetime_table['tpep_pickup_datetime'].dt.day
    datetime_table['pick_weekday']=datetime_table['tpep_pickup_datetime'].dt.weekday
    datetime_table['pick_hour']=datetime_table['tpep_pickup_datetime'].dt.hour

    datetime_table['drop_year']=datetime_table['tpep_dropoff_datetime'].dt.year
    datetime_table['drop_month']=datetime_table['tpep_dropoff_datetime'].dt.month
    datetime_table['drop_day']=datetime_table['tpep_dropoff_datetime'].dt.day
    datetime_table['drop_weekday']=datetime_table['tpep_dropoff_datetime'].dt.weekday
    datetime_table['drop_hour']=datetime_table['tpep_dropoff_datetime'].dt.hour

    datetime_table['datetime_id'] = datetime_table.index 
    datetime_table = datetime_table[['datetime_id','tpep_pickup_datetime','pick_year','pick_month','pick_day','pick_weekday','pick_hour',
                     'tpep_dropoff_datetime','drop_year','drop_month','drop_day','drop_weekday','drop_hour']]


    trip_distance_table = df[['trip_distance']].drop_duplicates().reset_index(drop=True)
    trip_distance_table['trip_distance_id'] = trip_distance_table.index
    trip_distance_table = trip_distance_table[['trip_distance_id','trip_distance']]
    
    passenger_count_table = df[['passenger_count']].drop_duplicates().reset_index(drop=True)
    passenger_count_table['passenger_count_id'] = passenger_count_table.index
    passenger_count_table = passenger_count_table[['passenger_count_id','passenger_count']]

    payment_type_code = {
        1:"Credit card",
        2:"Cash",
        3:"No charge",
        4:"Dispute",
        5:"Unknown",
        6:"Voided trip"
    }

    payment_type_table = df[['payment_type']].drop_duplicates().reset_index(drop=True)
    payment_type_table ['payment_type_id'] = payment_type_table.index
    payment_type_table ['payment_type_name'] = payment_type_table ['payment_type'].map(payment_type_code)
    payment_type_table = payment_type_table[['payment_type_id','payment_type','payment_type_name']]
 
    
    rate_code_type = {
        1:"Standard rate",
        2:"JFK",
        3:"Newark",
        4:"Nassau or Westchester",
        5:"Negotiated fare",
        6:"Group ride"
    }

    rate_code_table = df[['RatecodeID']].drop_duplicates().reset_index(drop=True)
    rate_code_table['rate_code_id'] = rate_code_table.index
    rate_code_table['rate_code_name'] = rate_code_table['RatecodeID'].map(rate_code_type)
    rate_code_table = rate_code_table[['rate_code_id','RatecodeID','rate_code_name']]

    pickup_location_table = df[['pickup_longitude','pickup_latitude']].drop_duplicates().reset_index(drop=True)
    pickup_location_table ['pickup_location_id'] = pickup_location_table.index
    pickup_location_table = pickup_location_table [['pickup_location_id','pickup_longitude','pickup_latitude']]

    dropoff_location_table = df[['dropoff_longitude','dropoff_latitude']].drop_duplicates().reset_index(drop=True)
    dropoff_location_table ['dropoff_location_id'] = dropoff_location_table.index
    dropoff_location_table = dropoff_location_table [['dropoff_location_id','dropoff_longitude','dropoff_latitude']]
    
    fact_table = df.merge(passenger_count_table, on='passenger_count') \
             .merge(trip_distance_table, on='trip_distance') \
             .merge(rate_code_table, on='RatecodeID') \
             .merge(pickup_location_table, on=['pickup_longitude', 'pickup_latitude']) \
             .merge(dropoff_location_table, on=['dropoff_longitude', 'dropoff_latitude'])\
             .merge(datetime_table, on=['tpep_pickup_datetime','tpep_dropoff_datetime']) \
             .merge(payment_type_table, on='payment_type') \
             [['VendorID', 'datetime_id', 'pickup_location_id','dropoff_location_id','trip_distance_id',
               'rate_code_id','passenger_count_id','payment_type_id','store_and_fwd_flag','fare_amount', 
               'extra', 'mta_tax', 'tip_amount', 'tolls_amount','improvement_surcharge', 'total_amount']]
 
    return {"datetime_table":datetime_table.to_dict(orient="dict"),
    "passenger_count_table":passenger_count_table.to_dict(orient="dict"),
    "trip_distance_table":trip_distance_table.to_dict(orient="dict"),
    "rate_code_table":rate_code_table.to_dict(orient="dict"),
    "pickup_location_table":pickup_location_table.to_dict(orient="dict"),
    "dropoff_location_table":dropoff_location_table.to_dict(orient="dict"),
    "payment_type_table":payment_type_table.to_dict(orient="dict"),
    "fact_table":fact_table.to_dict(orient="dict")}


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
