import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as sf
from pyspark.sql import Window
from pyspark.sql.types import IntegerType, DecimalType, StringType, DateType, LongType, FloatType

SHUFFLE_PARTITIONS = sys.argv[1]
INPUT_LOC = sys.argv[2]
OUTPUT_LOC = sys.argv[3]

spark = SparkSession \
    .builder \
    .appName("TPC-DS") \
    .config("spark.sql.shuffle.partitions", SHUFFLE_PARTITIONS) \
    .enableHiveSupport() \
    .getOrCreate()

def fill_na(df, fk_cols):
    # given fk_columns find numeric, string and date to fill with default NA values
    num_cols = [sf.name for sf in df.schema.fields if sf.name not in fk_cols and (isinstance(sf.dataType, IntegerType) or isinstance(sf.dataType, LongType))]
    double_cols = [sf.name for sf in df.schema.fields if sf.name not in fk_cols and (isinstance(sf.dataType, DecimalType) or isinstance(sf.dataType, FloatType))]
    str_cols = [sf.name for sf in df.schema.fields if sf.name not in fk_cols and isinstance(sf.dataType, StringType)]
    date_cols = [sf.name for sf in df.schema.fields if sf.name not in fk_cols and isinstance(sf.dataType, DateType)]
    # combine all na mappings
    na_mapping = {
        **{k:-1 for k in fk_cols}, 
        **{k:0 for k in num_cols}, 
        **{k:0.0 for k in double_cols}, 
        **{k:'UNKNOWN' for k in str_cols},
        **{k:"1990-01-01 00:00:00" for k in date_cols}}
    return df.na.fill(na_mapping)

def get_valid_dim_rows(dim_df, fact_df, dim_col, fact_col):
    # take only records that are in dimension and fact table
    return dim_df.join(fact_df.select(sf.col(fact_col).alias(dim_col)).distinct(), dim_col, "right_outer")

def get_invalid_dim_rows(dim_df, fact_df, dim_col, fact_col):
    # take only records that are not_referenced in fact table
    return dim_df.join(fact_df.select(sf.col(fact_col).alias(dim_col)).distinct(), dim_col, "left_anti")

def pk_check_log_and_exit(table):
    print(f"Table {table} doesn't have unique FK. Exiting.")

def write_as_parquet(df, loc, part_by = None, coalesce = 1):
    if part_by:
        df.coalesce(coalesce).write.partitionBy(part_by).mode("overwrite").parquet(loc)
    else:
        df.coalesce(coalesce).write.mode("overwrite").parquet(loc)
        
    
# read the data
customer_raw = spark.read.parquet(f'{INPUT_LOC}/customer/')
date_dim_raw = spark.read.parquet(f'{INPUT_LOC}/date_dim/')
item_raw = spark.read.parquet(f'{INPUT_LOC}/item/')
store_raw = spark.read.parquet(f'{INPUT_LOC}/store/')
store_sales_raw = spark.read.parquet(f'{INPUT_LOC}/store_sales/')

# check pk is unique
if customer_raw.select('c_customer_sk').distinct().count() != customer_raw.count():
    pk_check_log_and_exit(customer_raw)
if date_dim_raw.select('d_date_id').distinct().count() != date_dim_raw.count():
    pk_check_log_and_exit(date_dim_raw)
if item_raw.select('i_item_sk').distinct().count() != item_raw.count():
    pk_check_log_and_exit(item_raw)
if store_raw.select('s_store_sk').distinct().count() != store_raw.count():
    pk_check_log_and_exit(store_raw)

## BASIC FACT AND DIMENSION CLEANUP

# fill null values in facts first - we need ids when joining with dimensions
store_sales = fill_na(store_sales_raw, ['ss_customer_sk', 'ss_sold_date_sk', 'ss_item_sk', 'ss_store_sk'])

# find valid dimensions - those that are in the fact table - including missing with -1
customer_filtr = get_valid_dim_rows(customer_raw, store_sales, "c_customer_sk", "ss_customer_sk")
date_dim_filtr = get_valid_dim_rows(date_dim_raw, store_sales, "d_date_sk", "ss_sold_date_sk")
item_filtr = get_valid_dim_rows(item_raw, store_sales, "i_item_sk", "ss_item_sk")
store_filtr = get_valid_dim_rows(store_raw, store_sales, "s_store_sk", "ss_store_sk")

# find dimensions that are not in fact table for auditing
customer_invalid = get_invalid_dim_rows(customer_raw, store_sales, "c_customer_sk", "ss_customer_sk")
date_dim_invalid = get_invalid_dim_rows(date_dim_raw, store_sales, "d_date_sk", "ss_sold_date_sk")
item_invalid = get_invalid_dim_rows(item_raw, store_sales, "i_item_sk", "ss_item_sk")
store_invalid = get_invalid_dim_rows(store_raw, store_sales, "s_store_sk", "ss_store_sk")

# fill null values in dimensions - since there might be -1 we need to do this after joining
customer = fill_na(customer_filtr, ['c_customer_sk'])
date_dim = fill_na(date_dim_filtr, ['d_date_sk'])
item = fill_na(item_filtr, ['i_item_sk'])
store = fill_na(store_filtr, ['s_store_sk'])

# remove unnecessery keys
store_sales = store_sales.drop('ss_sold_time_sk', 'ss_cdemo_sk', 'ss_hdemo_sk', 'ss_addr_sk', 'ss_promo_sk')
customer = customer.drop('c_current_cdemo_sk', 'c_current_hdemo_sk', 'c_current_addr_sk')

# custom dimensions
def make_simple_dimension(source_df, on_column, as_column, as_column_id):
    return source_df \
        .select(sf.col(on_column).alias(as_column)) \
        .distinct() \
        .withColumn(as_column_id,  sf.monotonically_increasing_id()) \
        .withColumn(as_column_id,  sf.when(sf.col(as_column) == 'UNKNOWN', -1).otherwise(sf.col(as_column_id)))

item_category = make_simple_dimension(item, 'i_category', 'ic_category', 'ic_category_sk')
store_city = make_simple_dimension(store, 's_city', 'sc_city', 'sc_city_sk')
store_country = make_simple_dimension(store, 's_country', 'sc_country', 'sc_country_sk')

# join dimensions to extract additonal data to fact table
# get d_date for partitioning
store_sales = store_sales.join(date_dim.select(sf.col('d_date_sk').alias('ss_sold_date_sk'), sf.col('d_date').alias('ss_d_date')), 'ss_sold_date_sk', 'left')
item = item.join(item_category.select(sf.col('ic_category').alias('i_category'), sf.col('ic_category_sk').alias('i_category_sk')), 'i_category', 'left')
store = store.join(store_city.select(sf.col('sc_city').alias('s_city'), sf.col('sc_city_sk').alias('s_city_sk')), 's_city', 'left')
store = store.join(store_country.select(sf.col('sc_country').alias('s_country'), sf.col('sc_country_sk').alias('s_country_sk')), 's_country', 'left')
# adding item_category id
store_sales = store_sales.join(item.select(sf.col('i_item_sk').alias('ss_item_sk'), sf.col('i_category_sk').alias('ss_item_category_sk')), 'ss_item_sk', 'left')
# adding store_city id and store_country id
store_sales = store_sales.join(store.select(
        sf.col('s_store_sk').alias('ss_store_sk'), 
        sf.col('s_city_sk').alias('ss_store_city_sk'), 
        sf.col('s_country_sk').alias('ss_store_country_sk')), 'ss_store_sk', 'left')

## AGGREGATIONS

def calc_a_store_sales_store_lvl():
    # a_store_sales_store_lvl
    ## group facts by date and store
    ## gives information about
    ## - items sold, distinct items sold
    ## - how many different customers per day
    ## - how many purchases per day
    ## - sum and avg of different prices
    groupBy = ['ss_d_date', 'ss_store_sk']
    cntFields = ['ss_item_sk']
    cntDistinctFields = ['ss_item_sk', 'ss_customer_sk', 'ss_ticket_number']
    sumFields = ['ss_item_sk', 'ss_quantity', 'ss_wholesale_cost', 'ss_list_price', 'ss_sales_price', 'ss_ext_discount_amt', 'ss_ext_list_price', 'ss_ext_tax', 'ss_coupon_amt', 'ss_net_paid' ,'ss_net_paid_inc_tax', 'ss_net_profit']
    avgFields = ['ss_item_sk', 'ss_quantity', 'ss_wholesale_cost', 'ss_list_price', 'ss_sales_price', 'ss_ext_discount_amt', 'ss_ext_list_price', 'ss_ext_tax', 'ss_coupon_amt', 'ss_net_paid' ,'ss_net_paid_inc_tax', 'ss_net_profit']
    aggregations = [sf.count(x).alias(f'count_{x}') for x in cntFields]
    aggregations = aggregations + [sf.countDistinct(x).alias(f'cnt_dist_{x}') for x in cntDistinctFields]
    aggregations = aggregations + [sf.sum(x).alias(f'sum_{x}') for x in sumFields]
    aggregations = aggregations + [sf.avg(x).alias(f'avg_{x}') for x in avgFields]

    return store_sales.groupBy(groupBy).agg(*aggregations)

def calc_a_store_sales_customer_lvl():
    # a_store_sales_customer_lvl
    ## group facts by date and customer
    ## gives information about
    ## - items bought, distinct items bought
    ## - how many stores is he using
    ## - number of purchases per day
    ## - sum and avg of different prices
    groupBy = ['ss_d_date', 'ss_customer_sk']
    cntFields = ['ss_item_sk']
    cntDistinctFields = ['ss_item_sk', 'ss_store_sk', 'ss_ticket_number']
    sumFields = ['ss_item_sk', 'ss_quantity', 'ss_wholesale_cost', 'ss_list_price', 'ss_sales_price', 'ss_ext_discount_amt', 'ss_ext_list_price', 'ss_ext_tax', 'ss_coupon_amt', 'ss_net_paid' ,'ss_net_paid_inc_tax', 'ss_net_profit']
    avgFields = ['ss_item_sk', 'ss_quantity', 'ss_wholesale_cost', 'ss_list_price', 'ss_sales_price', 'ss_ext_discount_amt', 'ss_ext_list_price', 'ss_ext_tax', 'ss_coupon_amt', 'ss_net_paid' ,'ss_net_paid_inc_tax', 'ss_net_profit']
    aggregations = [sf.count(x).alias(f'count_{x}') for x in cntFields]
    aggregations = aggregations + [sf.countDistinct(x).alias(f'cnt_dist_{x}') for x in cntDistinctFields]
    aggregations = aggregations + [sf.sum(x).alias(f'sum_{x}') for x in sumFields]
    aggregations = aggregations + [sf.avg(x).alias(f'avg_{x}') for x in avgFields]

    return store_sales.groupBy(groupBy).agg(*aggregations)

def calc_a_store_sales_item_lvl():
    # a_store_sales_item_lvl
    ## group facts by date and item
    ## gives information about
    ## - items sold per day
    ## - how many different customers per day
    ## - how many purchases per day
    ## - sum and avg of different prices
    groupBy = ['ss_d_date', 'ss_item_sk']
    cntFields = ['ss_item_sk']
    cntDistinctFields = ['ss_store_sk', 'ss_customer_sk', 'ss_ticket_number']
    sumFields = ['ss_item_sk', 'ss_quantity', 'ss_wholesale_cost', 'ss_list_price', 'ss_sales_price', 'ss_ext_discount_amt', 'ss_ext_list_price', 'ss_ext_tax', 'ss_coupon_amt', 'ss_net_paid' ,'ss_net_paid_inc_tax', 'ss_net_profit']
    avgFields = ['ss_item_sk', 'ss_quantity', 'ss_wholesale_cost', 'ss_list_price', 'ss_sales_price', 'ss_ext_discount_amt', 'ss_ext_list_price', 'ss_ext_tax', 'ss_coupon_amt', 'ss_net_paid' ,'ss_net_paid_inc_tax', 'ss_net_profit']
    aggregations = [sf.count(x).alias(f'count_{x}') for x in cntFields]
    aggregations = aggregations + [sf.countDistinct(x).alias(f'cnt_dist_{x}') for x in cntDistinctFields]
    aggregations = aggregations + [sf.sum(x).alias(f'sum_{x}') for x in sumFields]
    aggregations = aggregations + [sf.avg(x).alias(f'avg_{x}') for x in avgFields]

    return store_sales.groupBy(groupBy).agg(*aggregations)

a_store_sales_store_lvl = calc_a_store_sales_store_lvl()
a_store_sales_customer_lvl = calc_a_store_sales_customer_lvl()
a_store_sales_item_lvl = calc_a_store_sales_item_lvl()

## WINDOWING

def calc_a_store_sales_store_quantity_ma():
    # a_store_sales_store_quantity_ma
    ## calculates moving average of each store for last 7, 30, 90 days

    # find min and max date for each store
    min_max_date_store = a_store_sales_store_lvl.groupBy('ss_store_sk').agg(sf.min('ss_d_date').alias('min_d_date'), sf.max('ss_d_date').alias('max_d_date'))
    # cross join will all dates
    min_max_date_store = min_max_date_store.crossJoin(date_dim_raw.select(sf.col('d_date').alias('ss_d_date')))
    # filter lower than min and higher than max 
    min_max_date_store = min_max_date_store.filter((sf.col('ss_d_date') >= sf.col('min_d_date')) & (sf.col('ss_d_date') <= sf.col('max_d_date')))
    # take only store and date cols
    min_max_date_store = min_max_date_store.select('ss_store_sk', 'ss_d_date')

    # join facts will all valid dates - fill missing values with 0
    a_store_sales_store_quantity_ma = min_max_date_store \
        .join(a_store_sales_store_lvl, ['ss_store_sk', 'ss_d_date'], 'left_outer') \
        .fillna(0) \
        .select('ss_d_date', 'ss_store_sk', 'avg_ss_quantity')

    # get 7d MA
    a_store_sales_store_quantity_ma = a_store_sales_store_quantity_ma \
        .withColumn("7d_ma_quantity", 
                    sf.avg("avg_ss_quantity").over(Window.partitionBy('ss_store_sk').orderBy('ss_d_date').rowsBetween(-7, Window.currentRow)))
    # get 1m MA
    a_store_sales_store_quantity_ma = a_store_sales_store_quantity_ma \
        .withColumn("1m_ma_quantity", 
                    sf.avg("avg_ss_quantity").over(Window.partitionBy('ss_store_sk').orderBy('ss_d_date').rowsBetween(-30, Window.currentRow)))
    # get 1y MA
    a_store_sales_store_quantity_ma = a_store_sales_store_quantity_ma \
        .withColumn("1q_ma_quantity", 
                    sf.avg("avg_ss_quantity").over(Window.partitionBy('ss_store_sk').orderBy('ss_d_date').rowsBetween(-90, Window.currentRow)))
    return a_store_sales_store_quantity_ma

a_store_sales_store_quantity_ma = calc_a_store_sales_store_quantity_ma()


## OUTPUT

# fact and dimensions
write_as_parquet(customer, f'{OUTPUT_LOC}/d_customer/')
write_as_parquet(date_dim, f'{OUTPUT_LOC}/d_date_dim/')
write_as_parquet(item, f'{OUTPUT_LOC}/d_item/')
write_as_parquet(store, f'{OUTPUT_LOC}/d_store/')
write_as_parquet(item_category, f'{OUTPUT_LOC}/d_item_category/')
write_as_parquet(store_city, f'{OUTPUT_LOC}/d_store_city/')
write_as_parquet(store_country, f'{OUTPUT_LOC}/d_store_country/')
write_as_parquet(store_sales, f'{OUTPUT_LOC}/f_store_sales/')
# given full dataset it would make sense to partition by d_date for example
#write_as_parquet(store_sales, 'tpcds-dwh/store_sales_part/', 'd_date')

# audit
write_as_parquet(customer_invalid, f'{OUTPUT_LOC}/d_customer_invalid/')
write_as_parquet(date_dim_invalid, f'{OUTPUT_LOC}/d_date_dim_invalid/')
write_as_parquet(item_invalid, f'{OUTPUT_LOC}/d_item_invalid/')
write_as_parquet(store_invalid, f'{OUTPUT_LOC}/d_store_invalid/')

# aggregations
write_as_parquet(a_store_sales_store_lvl, f'{OUTPUT_LOC}/a_store_sales_store_lvl/')
write_as_parquet(a_store_sales_customer_lvl, f'{OUTPUT_LOC}/a_store_sales_customer_lvl/')
write_as_parquet(a_store_sales_item_lvl, f'{OUTPUT_LOC}/a_store_sales_item_lvl/')

# windowing
write_as_parquet(a_store_sales_store_quantity_ma, f'{OUTPUT_LOC}/a_store_sales_store_quantity_ma/')