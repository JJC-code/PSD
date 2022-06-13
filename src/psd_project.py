import findspark
import datetime
import os
findspark.init('E:\BigData\Spark')

from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import mean, col, pandas_udf 
from pyspark.sql.types import StructType,StructField, StringType, FloatType
import pandas as pd
from pyspark.sql.window import Window
import pyspark.sql.functions as F

import file_operations as fo

DATA_FILE_PATHNAME = '\\'.join(os.path.dirname(__file__).split('\\')[:-1])+'\\'+'data'+'\\'+'yields_with_rounded_numbers_full_dataset_ONE_MILION.txt'
# DATA_FILE_PATHNAME = '\\'.join(os.path.dirname(__file__).split('\\')[:-1])+'\\'+'data'+'\\'+'yields_with_rounded_numbers_part_dataset_copy.txt'

METRICS_FILE_PATHNAME = '\\'.join(os.path.dirname(__file__).split('\\')[:-1])+'\\'+'data'+'\\'+'metrics.txt'
METRICS_OVR_FILE_PATHNAME = '\\'.join(os.path.dirname(__file__).split('\\')[:-1])+'\\'+'data'+'\\'+'metrics_ovr.txt'

ASSETS_NUM = 6
NUM_OF_RECORDS_PER_ASSET = 1000000
NUM_OF_DATA_IN_FULL_PORTFOLIO = 6*NUM_OF_RECORDS_PER_ASSET
WINDOW_SIZE = 30


def mapper(line):
    fields = line.split()
    return Row(ID=int(fields[0]),asset_1 = float(fields[1]), asset_2 = float(fields[2]), asset_3 = float(fields[3]),asset_4 = float(fields[4]), asset_5 = float(fields[5]), asset_6 = float(fields[6]), grouper = 1)

def transp(assets_df, which_assets):
    assets_pd_df = assets_df.toPandas()[which_assets]
    transp_assets_pd_df = assets_pd_df.transpose()
    return transp_assets_pd_df

def union_win_columns(spark, assets_df: pd.DataFrame, cols_num: int):
    sparkDF = spark.createDataFrame(assets_df) 
    list_of_cols = []
    schema = StructType([
        StructField('full_portfolio', FloatType(), True)])  
    columns = spark.createDataFrame([],schema)
    for i in range(0, cols_num):
        list_of_cols.append(sparkDF.select(sparkDF.columns[i]))
    for i in range(0, cols_num):
        columns = columns.unionAll(list_of_cols[i])
    return columns

def union_columns(assets_df, which_assets):
    list_of_cols = []
    schema = StructType([
        StructField('full_portfolio', FloatType(), True)])  
    for i in range(1,7):
        list_of_cols.append(assets_df.select(assets_df.columns[i]))
    columns = spark.createDataFrame([],schema)
    for i in range(0,6):
        columns = columns.unionAll(list_of_cols[i])
    return columns

def calculate_means_of_assets(assets_df, which_assets):
    means_per_columns = assets_df.groupBy("grouper")\
            .agg(mean("asset_1").alias("mean_asset_1"), \
                mean("asset_2").alias("mean_asset_2"), \
                mean("asset_3").alias("mean_asset_3"), \
                mean("asset_4").alias("mean_asset_4"), \
                mean("asset_5").alias("mean_asset_5"), \
                mean("asset_6").alias("mean_asset_6"), \
            )
    means_per_columns_list = means_per_columns.collect()
    means_per_columns_dict = {asset: mean_val for asset, mean_val in zip(which_assets, means_per_columns_list[0][1:])}
    return means_per_columns_dict

def full_ds_stat_extractor(full_ds_stats_df):
    full_ds_stats_pd_df = full_ds_stats_df.toPandas()
    df = full_ds_stats_pd_df[full_ds_stats_pd_df.index % 6 == 0] 
    df = df[df[df.columns[0]]!=-100].reset_index()
    df = df[df.columns[1]]
    return df

def calculate_mean_of_portfolio(means_of_assets_df):
    temp_mean = 0
    for mean_val in means_of_assets_df.values():
        temp_mean +=mean_val
    mean_of_portfolio = temp_mean/ASSETS_NUM 
    return {'mean_full_portfolio': mean_of_portfolio}

def calculate_medians_of_assets(assets_df, which_assets, num_of_records):
    center_of_dataset =  num_of_records//2
    next_percentile = (center_of_dataset+1)/num_of_records
    percentiles = assets_df.approxQuantile(which_assets,[0.5, next_percentile], 0)
    medians_of_assets = {}
    seq_of_assets = [seq for seq in range(len(which_assets))]
    for asset, seq in zip(which_assets, seq_of_assets):
        medians_of_assets['median_' + asset] = round((percentiles[seq][1]+ percentiles[seq][0])/2, 4) 
    return medians_of_assets

def calculate_one_tenth_percentile(assets_df, which_assets):
    dec_percentile = assets_df.approxQuantile(which_assets,[0.1], 0)
    assets_dec_percentile = {'quant_' + asset: percentile[0] for asset, percentile in zip(which_assets, dec_percentile)}
    return assets_dec_percentile

def calculate_mean_of_min_ten_percent(assets_df, which_assets, num_of_records):
    ten_percent_idx = int(0.1*num_of_records)
    if len(which_assets)>1:
        sorted_asset_1_df = assets_df.sort(col(which_assets[0]).asc()).collect()
        sorted_asset_2_df = assets_df.sort(col(which_assets[1]).asc()).collect()
        sorted_asset_3_df = assets_df.sort(col(which_assets[2]).asc()).collect()
        sorted_asset_4_df = assets_df.sort(col(which_assets[3]).asc()).collect()
        sorted_asset_5_df = assets_df.sort(col(which_assets[4]).asc()).collect()
        sorted_asset_6_df = assets_df.sort(col(which_assets[5]).asc()).collect()
        top_sorted_asset_1 = []
        top_sorted_asset_2 = []
        top_sorted_asset_3 = []
        top_sorted_asset_4 = []
        top_sorted_asset_5 = []
        top_sorted_asset_6 = []
        for elem in sorted_asset_1_df[:ten_percent_idx]:
            top_sorted_asset_1.append(elem.asset_1)
        for elem in sorted_asset_2_df[:ten_percent_idx]:
            top_sorted_asset_2.append(elem.asset_2)
        for elem in sorted_asset_3_df[:ten_percent_idx]:
            top_sorted_asset_3.append(elem.asset_3)
        for elem in sorted_asset_4_df[:ten_percent_idx]:
            top_sorted_asset_4.append(elem.asset_4)
        for elem in sorted_asset_5_df[:ten_percent_idx]:
            top_sorted_asset_5.append(elem.asset_5)
        for elem in sorted_asset_6_df[:ten_percent_idx]:
            top_sorted_asset_6.append(elem.asset_6)
        mean_of_min_ten_percent_asset_1 = sum(top_sorted_asset_1)/ten_percent_idx
        mean_of_min_ten_percent_asset_2 = sum(top_sorted_asset_2)/ten_percent_idx
        mean_of_min_ten_percent_asset_3 = sum(top_sorted_asset_3)/ten_percent_idx
        mean_of_min_ten_percent_asset_4 = sum(top_sorted_asset_4)/ten_percent_idx
        mean_of_min_ten_percent_asset_5 = sum(top_sorted_asset_5)/ten_percent_idx
        mean_of_min_ten_percent_asset_6 = sum(top_sorted_asset_6)/ten_percent_idx
        return {'mean_of_min_ten_percent_asset_1': mean_of_min_ten_percent_asset_1, 
            'mean_of_min_ten_percent_asset_2': mean_of_min_ten_percent_asset_2, 
            'mean_of_min_ten_percent_asset_3': mean_of_min_ten_percent_asset_3, 
            'mean_of_min_ten_percent_asset_4': mean_of_min_ten_percent_asset_4,
            'mean_of_min_ten_percent_asset_5': mean_of_min_ten_percent_asset_5, 
            'mean_of_min_ten_percent_asset_6': mean_of_min_ten_percent_asset_6}
    else:
        # for portfolio
        sorted_assets_df = assets_df.sort(col(which_assets[0]).asc()).collect()
        top_sorted_assets = []
        for elem in sorted_assets_df[:ten_percent_idx]:
            top_sorted_assets.append(elem.full_portfolio)
        mean_of_min_ten_percent = sum(top_sorted_assets)/ten_percent_idx
        return {'mean_of_min_ten_percent_for_portfolio': mean_of_min_ten_percent}
    
def safety_measure_on_average_deviation(assets_df, which_assets, num_of_records, means_per_assets):
    df = assets_df.toPandas()
    if len(which_assets)>1:
        dict_of_assets_df = {'safety_measure_of_' + which_assets[asset_idx]:    \
            mean_of_asset - df[which_assets[asset_idx]].sub(mean_of_asset).abs().sum() / (2*NUM_OF_RECORDS_PER_ASSET) \
            for asset_idx, mean_of_asset in zip(range(0,len(which_assets)),means_per_assets.values())}
        return dict_of_assets_df
    else:
        subtraction = df.sub(means_per_assets).abs().sum().div(2*NUM_OF_DATA_IN_FULL_PORTFOLIO)
        safety_measure = means_per_assets - subtraction
    return {'safety_measure_of_portfolio': safety_measure['full_portfolio']}


def turn_stat_dict_into_asset_dict(stat_dict):
    asset_1_dict = {}
    asset_2_dict = {}
    asset_3_dict = {}
    asset_4_dict = {}
    asset_5_dict = {}
    asset_6_dict = {}
    full_portfolio_dict = {}
    for dict_ in stat_dict:
        asset_1_dict[list(dict_.keys())[0]] = list(dict_.values())[0]
        asset_2_dict[list(dict_.keys())[1]] = list(dict_.values())[1]
        asset_3_dict[list(dict_.keys())[2]] = list(dict_.values())[2]
        asset_4_dict[list(dict_.keys())[3]] = list(dict_.values())[3]
        asset_5_dict[list(dict_.keys())[4]] = list(dict_.values())[4]
        asset_6_dict[list(dict_.keys())[5]] = list(dict_.values())[5]
        full_portfolio_dict[list(dict_.keys())[6]] = list(dict_.values())[6]
    asset_dict = [asset_1_dict, \
                    asset_2_dict, \
                    asset_3_dict, \
                    asset_4_dict, \
                    asset_5_dict, \
                    asset_6_dict, \
                    full_portfolio_dict]
    return asset_dict


def full_portfolio_batch_data_executor(assets_with_yields_df, all_in_one_col_data):
    # MEANS
    means_per_assets = calculate_means_of_assets(assets_with_yields_df, ['mean_asset_1','mean_asset_2','mean_asset_3','mean_asset_4','mean_asset_5','mean_asset_6'])
    mean_of_portfolio = calculate_mean_of_portfolio(means_per_assets)
    means_per_assets.update(mean_of_portfolio)
    # # MEDIANS
    medians_per_assets = calculate_medians_of_assets(assets_with_yields_df, ['asset_1','asset_2','asset_3','asset_4','asset_5','asset_6'], NUM_OF_RECORDS_PER_ASSET)
    median_of_portfolio = calculate_medians_of_assets(all_in_one_col_data, ['full_portfolio'], NUM_OF_DATA_IN_FULL_PORTFOLIO)
    medians_per_assets.update(median_of_portfolio)
    # # QUANT
    dec_percentile_per_assets = calculate_one_tenth_percentile(assets_with_yields_df, ['asset_1','asset_2','asset_3','asset_4','asset_5','asset_6'])
    dec_percentile_of_portfolio = calculate_one_tenth_percentile(all_in_one_col_data, ['full_portfolio'])
    dec_percentile_per_assets.update(dec_percentile_of_portfolio)
    # # MEAN_OF_MIN_TEN_PERCENT
    mean_of_min_ten_percent_of_assets = calculate_mean_of_min_ten_percent(assets_with_yields_df, ['asset_1','asset_2','asset_3','asset_4','asset_5','asset_6'], NUM_OF_RECORDS_PER_ASSET)
    mean_of_min_ten_percent_of_portfolio = calculate_mean_of_min_ten_percent(all_in_one_col_data, ['full_portfolio'], NUM_OF_DATA_IN_FULL_PORTFOLIO)
    mean_of_min_ten_percent_of_assets.update(mean_of_min_ten_percent_of_portfolio)
    # # SAFETY MEASURES
    safety_measure_on_average_deviation_of_assets = safety_measure_on_average_deviation(assets_with_yields_df, ['asset_1','asset_2','asset_3','asset_4','asset_5','asset_6'], NUM_OF_RECORDS_PER_ASSET, means_per_assets)
    safety_measure_on_average_deviation_of_portfolio = safety_measure_on_average_deviation(all_in_one_col_data, ['full_portfolio'], NUM_OF_DATA_IN_FULL_PORTFOLIO, mean_of_portfolio['mean_full_portfolio'])
    safety_measure_on_average_deviation_of_assets.update(safety_measure_on_average_deviation_of_portfolio)

    # CHANGE DICTS ORIENTATION
    batch_data_results_per_stat = [means_per_assets, \
                    medians_per_assets, \
                    dec_percentile_per_assets, \
                    mean_of_min_ten_percent_of_assets, \
                    safety_measure_on_average_deviation_of_assets]
    batch_data_results_per_asset = turn_stat_dict_into_asset_dict(batch_data_results_per_stat)

    # CREATE DF TO CONCAT WITH WINDOW STATS
    all_in_one_stats_dict = {}
    for dict_ in batch_data_results_per_asset:
        all_in_one_stats_dict.update(dict_)
    all_in_one_stats_list_dict = [all_in_one_stats_dict]
    all_in_one_stats_df = pd.DataFrame(list(all_in_one_stats_list_dict), index = ['full_dataset_stat'])

    # SAVE TO FILE
    fo.save_to_file(METRICS_FILE_PATHNAME, 'FULL_PORTFOLIO_BATCH_DATA_PER_STAT', batch_data_results_per_stat)
    fo.save_to_file(METRICS_FILE_PATHNAME, 'FULL_PORTFOLIO_BATCH_DATA_PER_ASSET', batch_data_results_per_asset)
    return all_in_one_stats_df

def make_list(df):
    l = []
    for elem in df:
        l.append(elem[0])
    return l

def calculate_overrun(win_stat, ds_stat):
    ovr = (ds_stat-win_stat)/(1+ds_stat)
    ovr_exceeded = 0
    if ovr >= 0.01:
        ovr_exceeded=1
    else:
        ovr_exceeded=0
    return ovr_exceeded

def insert_row(df, my_row):
    df.loc[len(df)] = my_row

def calculate_windows_overrun(stats_for_windows, stats_for_full_ds):
    # ovr_summary = [] 
    for ind in stats_for_windows.index:
        a1_mean_ovr = calculate_overrun(stats_for_windows['mean_of_asset_1_in_win'][ind], stats_for_full_ds['mean_asset_1'][0])
        a2_mean_ovr = calculate_overrun(stats_for_windows['mean_of_asset_2_in_win'][ind], stats_for_full_ds['mean_asset_2'][0])
        a3_mean_ovr = calculate_overrun(stats_for_windows['mean_of_asset_3_in_win'][ind], stats_for_full_ds['mean_asset_3'][0])
        a4_mean_ovr = calculate_overrun(stats_for_windows['mean_of_asset_4_in_win'][ind], stats_for_full_ds['mean_asset_4'][0])
        a5_mean_ovr = calculate_overrun(stats_for_windows['mean_of_asset_5_in_win'][ind], stats_for_full_ds['mean_asset_5'][0])
        a6_mean_ovr = calculate_overrun(stats_for_windows['mean_of_asset_6_in_win'][ind], stats_for_full_ds['mean_asset_6'][0])
        full_portfolio_mean_ovr = calculate_overrun(stats_for_windows['mean_of_full_portfolio_in_win'][ind], stats_for_full_ds['mean_full_portfolio'][0])

        a1_median_ovr = calculate_overrun(stats_for_windows['median_of_asset_1_in_win'][ind], stats_for_full_ds['median_asset_1'][0])
        a2_median_ovr = calculate_overrun(stats_for_windows['median_of_asset_2_in_win'][ind], stats_for_full_ds['median_asset_2'][0])
        a3_median_ovr = calculate_overrun(stats_for_windows['median_of_asset_3_in_win'][ind], stats_for_full_ds['median_asset_3'][0])
        a4_median_ovr = calculate_overrun(stats_for_windows['median_of_asset_4_in_win'][ind], stats_for_full_ds['median_asset_4'][0])
        a5_median_ovr = calculate_overrun(stats_for_windows['median_of_asset_5_in_win'][ind], stats_for_full_ds['median_asset_5'][0])
        a6_median_ovr = calculate_overrun(stats_for_windows['median_of_asset_6_in_win'][ind], stats_for_full_ds['median_asset_6'][0])
        full_portfolio_median_ovr = calculate_overrun(stats_for_windows['median_of_full_portfolio_in_win'][ind], stats_for_full_ds['median_full_portfolio'][0])

        a1_one_of_tenth_ovr = calculate_overrun(stats_for_windows['one_of_tenth_1_in_win'][ind], stats_for_full_ds['quant_asset_1'][0])
        a2_one_of_tenth_ovr = calculate_overrun(stats_for_windows['one_of_tenth_2_in_win'][ind], stats_for_full_ds['quant_asset_2'][0])
        a3_one_of_tenth_ovr = calculate_overrun(stats_for_windows['one_of_tenth_3_in_win'][ind], stats_for_full_ds['quant_asset_3'][0])
        a4_one_of_tenth_ovr = calculate_overrun(stats_for_windows['one_of_tenth_4_in_win'][ind], stats_for_full_ds['quant_asset_4'][0])
        a5_one_of_tenth_ovr = calculate_overrun(stats_for_windows['one_of_tenth_5_in_win'][ind], stats_for_full_ds['quant_asset_5'][0])
        a6_one_of_tenth_ovr = calculate_overrun(stats_for_windows['one_of_tenth_6_in_win'][ind], stats_for_full_ds['quant_asset_6'][0])
        full_portfolio_one_of_tenth_ovr = calculate_overrun(stats_for_windows['one_of_tenth_full_portfolio_in_win'][ind], stats_for_full_ds['quant_full_portfolio'][0])

        a1_mean_of_min_ten_percent_ovr = calculate_overrun(stats_for_windows['mean_of_min_ten_percent_1_in_win'][ind], stats_for_full_ds['mean_of_min_ten_percent_asset_1'][0])
        a2_mean_of_min_ten_percent_ovr = calculate_overrun(stats_for_windows['mean_of_min_ten_percent_2_in_win'][ind], stats_for_full_ds['mean_of_min_ten_percent_asset_2'][0])
        a3_mean_of_min_ten_percent_ovr = calculate_overrun(stats_for_windows['mean_of_min_ten_percent_3_in_win'][ind], stats_for_full_ds['mean_of_min_ten_percent_asset_3'][0])
        a4_mean_of_min_ten_percent_ovr = calculate_overrun(stats_for_windows['mean_of_min_ten_percent_4_in_win'][ind], stats_for_full_ds['mean_of_min_ten_percent_asset_4'][0])
        a5_mean_of_min_ten_percent_ovr = calculate_overrun(stats_for_windows['mean_of_min_ten_percent_5_in_win'][ind], stats_for_full_ds['mean_of_min_ten_percent_asset_5'][0])
        a6_mean_of_min_ten_percent_ovr = calculate_overrun(stats_for_windows['mean_of_min_ten_percent_6_in_win'][ind], stats_for_full_ds['mean_of_min_ten_percent_asset_6'][0])
        full_portfolio_mean_of_min_ten_percent_ovr = calculate_overrun(stats_for_windows['mean_of_min_ten_percent_full_portfolio_in_win'][ind], stats_for_full_ds['mean_of_min_ten_percent_for_portfolio'][0])

        a1_safety_measure_on_avgdev_ovr = calculate_overrun(stats_for_windows['safety_measure_on_avgdev_1_in_win'][ind], stats_for_full_ds['safety_measure_of_asset_1'][0])
        a2_safety_measure_on_avgdev_ovr = calculate_overrun(stats_for_windows['safety_measure_on_avgdev_2_in_win'][ind], stats_for_full_ds['safety_measure_of_asset_2'][0])
        a3_safety_measure_on_avgdev_ovr = calculate_overrun(stats_for_windows['safety_measure_on_avgdev_3_in_win'][ind], stats_for_full_ds['safety_measure_of_asset_3'][0])
        a4_safety_measure_on_avgdev_ovr = calculate_overrun(stats_for_windows['safety_measure_on_avgdev_4_in_win'][ind], stats_for_full_ds['safety_measure_of_asset_4'][0])
        a5_safety_measure_on_avgdev_ovr = calculate_overrun(stats_for_windows['safety_measure_on_avgdev_5_in_win'][ind], stats_for_full_ds['safety_measure_of_asset_5'][0])
        a6_safety_measure_on_avgdev_ovr = calculate_overrun(stats_for_windows['safety_measure_on_avgdev_6_in_win'][ind], stats_for_full_ds['safety_measure_of_asset_6'][0])
        full_portfolio_safety_measure_ovr = calculate_overrun(stats_for_windows['safety_measure_on_avgdev_full_portfolio_in_win'][ind], stats_for_full_ds['safety_measure_of_portfolio'][0])

        with open(METRICS_OVR_FILE_PATHNAME, 'a', encoding='utf-8') as f:
            f.write(str(ind) +' ')
            f.write(str([a1_mean_ovr, a2_mean_ovr, a3_mean_ovr, a4_mean_ovr, a5_mean_ovr, a6_mean_ovr,full_portfolio_mean_ovr,\
                    a1_median_ovr, a2_median_ovr, a3_median_ovr, a4_median_ovr, a5_median_ovr, a6_median_ovr,full_portfolio_median_ovr,\
                    a1_one_of_tenth_ovr, a2_one_of_tenth_ovr, a3_one_of_tenth_ovr, a4_one_of_tenth_ovr, a5_one_of_tenth_ovr, a6_one_of_tenth_ovr,full_portfolio_one_of_tenth_ovr,\
                    a1_mean_of_min_ten_percent_ovr,a2_mean_of_min_ten_percent_ovr, a3_mean_of_min_ten_percent_ovr, a4_mean_of_min_ten_percent_ovr, a5_mean_of_min_ten_percent_ovr, a6_mean_of_min_ten_percent_ovr,full_portfolio_mean_of_min_ten_percent_ovr,\
                    a1_safety_measure_on_avgdev_ovr, a2_safety_measure_on_avgdev_ovr, a3_safety_measure_on_avgdev_ovr, a4_safety_measure_on_avgdev_ovr, a5_safety_measure_on_avgdev_ovr, a6_safety_measure_on_avgdev_ovr,full_portfolio_safety_measure_ovr]))
            f.write('\n')


if __name__ == "__main__":

    st = datetime.datetime.now()

    # Spark config
    spark = SparkSession.builder.config('spark.driver.memory','40g')\
                                .config('spark.serializer', 'org.apache.spark.serializer.KryoSerializer')\
                                .appName("Portfolio Monitoring").getOrCreate()                        

    lines = spark.sparkContext.textFile(DATA_FILE_PATHNAME, minPartitions = 2)
    # map values for columns 
    assets_with_yields_rdd = lines.map(mapper)                  # PythonRDD[2] at RDD at PythonRDD.scala:53 - to be able transform into dataframe, tylko wtedy kiedy mamy Dataset of Rows
    assets_with_yields_df = spark.createDataFrame(assets_with_yields_rdd).cache()
    all_in_one_col_data = union_columns(assets_with_yields_df, ['asset_1','asset_2','asset_3','asset_4','asset_5','asset_6'])
    win_seq = 0

    # Create an empty RDD
    emp_RDD = spark.sparkContext.emptyRDD()
    # Create empty schema
    columns = StructType([
        StructField('name_of_stat', StringType(), True),   \
        StructField('value', FloatType(), True)]            \
        )  

    # --------- User defined windows' functions
    @pandas_udf("double")
    def mean_of_assets_in_win(asset: pd.Series) -> float:
        singleton_val = -100
        if asset.size == WINDOW_SIZE or asset.size == 6*WINDOW_SIZE:
            asset_mean = asset.mean()
        else:
            asset_mean = singleton_val
        return asset_mean

    @pandas_udf("double")
    def calculate_medians_of_assets_in_win(asset: pd.Series) -> float:
        center_of_dataset=0
        if asset.size == WINDOW_SIZE:
            center_of_dataset =  WINDOW_SIZE//2
        elif  asset.size == WINDOW_SIZE*6:
            center_of_dataset =  (WINDOW_SIZE*6)//2
        singleton_val = -100
        next_percentile = (center_of_dataset+1)
        srt_asset = asset.sort_values(ascending=True)
        list_of_values = srt_asset.to_list()
        if len(list_of_values) == WINDOW_SIZE or len(list_of_values) == (WINDOW_SIZE*6):
            median = (list_of_values[center_of_dataset]+list_of_values[next_percentile])/2
        else:
            median = singleton_val
        return median

    @pandas_udf("double")   
    def calculate_one_tenth_percentile_in_win(asset: pd.Series) -> float:
        one_tenth_of_dataset= float(0)
        if asset.size == WINDOW_SIZE:
            one_tenth_of_dataset = 0.1*WINDOW_SIZE
        elif  asset.size == WINDOW_SIZE*6:
            one_tenth_of_dataset = WINDOW_SIZE*0.6
        singleton_val = -100
        srt_asset = asset.sort_values(ascending=True)
        list_of_values = srt_asset.to_list()
        if len(list_of_values) == WINDOW_SIZE or len(list_of_values) == (WINDOW_SIZE*6):
            one_tenth = list_of_values[int(one_tenth_of_dataset)]
        else:
            one_tenth = singleton_val
        return one_tenth 

    @pandas_udf("double")    
    def calculate_mean_of_min_ten_percent_in_win(asset: pd.Series) -> float:
        one_tenth_of_dataset = float(0)
        if asset.size == WINDOW_SIZE:
            one_tenth_of_dataset = 0.1*WINDOW_SIZE
        elif asset.size == WINDOW_SIZE*6:
            one_tenth_of_dataset = WINDOW_SIZE*0.6
        singleton_val = -100
        top_sorted_asset = []
        srt_asset = asset.sort_values(ascending=True)
        list_of_values = srt_asset.to_list()
        if len(list_of_values) == WINDOW_SIZE or len(list_of_values) == (WINDOW_SIZE*6):
            for elem in list_of_values[:int(one_tenth_of_dataset)]:
                top_sorted_asset.append(elem)
            mean_of_min_ten_percent_asset = sum(top_sorted_asset)/one_tenth_of_dataset
        else:
            mean_of_min_ten_percent_asset = singleton_val
        return mean_of_min_ten_percent_asset

    @pandas_udf("double")           
    def calculate_safety_measure_on_average_deviation(asset: pd.Series) -> float:
        singleton_val = -100
        if asset.size == WINDOW_SIZE:
            mean = asset.mean()
            subtraction = asset.sub(mean).abs().sum() /(2*NUM_OF_RECORDS_PER_ASSET)
            safety_measure = mean - subtraction
        elif asset.size == WINDOW_SIZE*6:
            mean = asset.mean()
            subtraction = asset.sub(mean).abs().sum() /(2*NUM_OF_DATA_IN_FULL_PORTFOLIO)
            safety_measure = mean - subtraction
        else:
            safety_measure = singleton_val
        return safety_measure


    #---------------- FULL DATASET
    full_dataset_stats_df = full_portfolio_batch_data_executor(assets_with_yields_df, all_in_one_col_data)

    #---------------- WINDOWING
    # sliding - window settings
    sliding_window = Window.orderBy(F.col("ID")).rowsBetween(Window.currentRow, WINDOW_SIZE-1)
    sliding_window_full = Window.orderBy(F.col("ID")).rowsBetween(Window.currentRow, (6*WINDOW_SIZE)-1)

    
    transp_assets_with_yields_pd_df = transp(assets_with_yields_df, ['asset_1','asset_2','asset_3','asset_4','asset_5','asset_6'])
    all_in_one_col_data_win = union_win_columns(spark, transp_assets_with_yields_pd_df, NUM_OF_RECORDS_PER_ASSET)
    all_in_one_col_id_data_win = all_in_one_col_data_win.withColumn('ID', monotonically_increasing_id())
    
    #---- MEANS
    a1_mean_win = assets_with_yields_df.select(mean_of_assets_in_win('asset_1').over(sliding_window).alias('mean_of_asset_1_in_win'))
    a2_mean_win = assets_with_yields_df.select(mean_of_assets_in_win('asset_2').over(sliding_window).alias('mean_of_asset_2_in_win'))
    a3_mean_win = assets_with_yields_df.select(mean_of_assets_in_win('asset_3').over(sliding_window).alias('mean_of_asset_3_in_win'))
    a4_mean_win = assets_with_yields_df.select(mean_of_assets_in_win('asset_4').over(sliding_window).alias('mean_of_asset_4_in_win'))
    a5_mean_win = assets_with_yields_df.select(mean_of_assets_in_win('asset_5').over(sliding_window).alias('mean_of_asset_5_in_win'))
    a6_mean_win = assets_with_yields_df.select(mean_of_assets_in_win('asset_6').over(sliding_window).alias('mean_of_asset_6_in_win'))
    full_portfolio_mean_win = all_in_one_col_id_data_win.select(mean_of_assets_in_win('full_portfolio').over(sliding_window_full).alias('mean_of_full_portfolio_in_win'))
    a1_mean_win_pd = a1_mean_win.toPandas()
    a2_mean_win_pd = a2_mean_win.toPandas()
    a3_mean_win_pd = a3_mean_win.toPandas()
    a4_mean_win_pd = a4_mean_win.toPandas()
    a5_mean_win_pd = a5_mean_win.toPandas()
    a6_mean_win_pd = a6_mean_win.toPandas()
    full_portfolio_mean_win_pd = full_ds_stat_extractor(full_portfolio_mean_win)

    # #---- MEDIANS
    a1_median_win = assets_with_yields_df.select(calculate_medians_of_assets_in_win('asset_1').over(sliding_window).alias('median_of_asset_1_in_win')) 
    a2_median_win = assets_with_yields_df.select(calculate_medians_of_assets_in_win('asset_2').over(sliding_window).alias('median_of_asset_2_in_win')) 
    a3_median_win = assets_with_yields_df.select(calculate_medians_of_assets_in_win('asset_3').over(sliding_window).alias('median_of_asset_3_in_win'))  
    a4_median_win = assets_with_yields_df.select(calculate_medians_of_assets_in_win('asset_4').over(sliding_window).alias('median_of_asset_4_in_win')) 
    a5_median_win = assets_with_yields_df.select(calculate_medians_of_assets_in_win('asset_5').over(sliding_window).alias('median_of_asset_5_in_win'))  
    a6_median_win = assets_with_yields_df.select(calculate_medians_of_assets_in_win('asset_6').over(sliding_window).alias('median_of_asset_6_in_win')) 
    full_portfolio_median_win = all_in_one_col_id_data_win.select(calculate_medians_of_assets_in_win('full_portfolio').over(sliding_window_full).alias('median_of_full_portfolio_in_win'))
    a1_median_win_pd = a1_median_win.toPandas()
    a2_median_win_pd = a2_median_win.toPandas()
    a3_median_win_pd = a3_median_win.toPandas()
    a4_median_win_pd = a4_median_win.toPandas()
    a5_median_win_pd = a5_median_win.toPandas()
    a6_median_win_pd = a6_median_win.toPandas()
    full_portfolio_median_win_pd = full_ds_stat_extractor(full_portfolio_median_win)

    # #----
    a1_one_tenth_win = assets_with_yields_df.select(calculate_one_tenth_percentile_in_win('asset_1').over(sliding_window).alias('one_of_tenth_1_in_win')) 
    a2_one_tenth_win = assets_with_yields_df.select(calculate_one_tenth_percentile_in_win('asset_2').over(sliding_window).alias('one_of_tenth_2_in_win'))
    a3_one_tenth_win = assets_with_yields_df.select(calculate_one_tenth_percentile_in_win('asset_3').over(sliding_window).alias('one_of_tenth_3_in_win'))
    a4_one_tenth_win = assets_with_yields_df.select(calculate_one_tenth_percentile_in_win('asset_4').over(sliding_window).alias('one_of_tenth_4_in_win'))
    a5_one_tenth_win = assets_with_yields_df.select(calculate_one_tenth_percentile_in_win('asset_5').over(sliding_window).alias('one_of_tenth_5_in_win')) 
    a6_one_tenth_win = assets_with_yields_df.select(calculate_one_tenth_percentile_in_win('asset_6').over(sliding_window).alias('one_of_tenth_6_in_win'))
    full_portfolio_one_tenth_win = all_in_one_col_id_data_win.select(calculate_one_tenth_percentile_in_win('full_portfolio').over(sliding_window_full).alias('one_of_tenth_full_portfolio_in_win'))

    a1_one_tenth_win_pd = a1_one_tenth_win.toPandas()
    a2_one_tenth_win_pd = a2_one_tenth_win.toPandas()
    a3_one_tenth_win_pd = a3_one_tenth_win.toPandas()
    a4_one_tenth_win_pd = a4_one_tenth_win.toPandas()
    a5_one_tenth_win_pd = a5_one_tenth_win.toPandas()
    a6_one_tenth_win_pd = a6_one_tenth_win.toPandas()
    full_portfolio_one_tenth_win_pd = full_ds_stat_extractor(full_portfolio_one_tenth_win)

    #----
    a1_mean_of_min_ten_percent_win = assets_with_yields_df.select(calculate_mean_of_min_ten_percent_in_win('asset_1').over(sliding_window).alias('mean_of_min_ten_percent_1_in_win'))
    a2_mean_of_min_ten_percent_win = assets_with_yields_df.select(calculate_mean_of_min_ten_percent_in_win('asset_2').over(sliding_window).alias('mean_of_min_ten_percent_2_in_win'))
    a3_mean_of_min_ten_percent_win = assets_with_yields_df.select(calculate_mean_of_min_ten_percent_in_win('asset_3').over(sliding_window).alias('mean_of_min_ten_percent_3_in_win'))
    a4_mean_of_min_ten_percent_win = assets_with_yields_df.select(calculate_mean_of_min_ten_percent_in_win('asset_4').over(sliding_window).alias('mean_of_min_ten_percent_4_in_win'))
    a5_mean_of_min_ten_percent_win = assets_with_yields_df.select(calculate_mean_of_min_ten_percent_in_win('asset_5').over(sliding_window).alias('mean_of_min_ten_percent_5_in_win'))
    a6_mean_of_min_ten_percent_win = assets_with_yields_df.select(calculate_mean_of_min_ten_percent_in_win('asset_6').over(sliding_window).alias('mean_of_min_ten_percent_6_in_win'))
    full_portfolio_mean_of_min_ten_percent_win = all_in_one_col_id_data_win.select(calculate_mean_of_min_ten_percent_in_win('full_portfolio').over(sliding_window_full).alias('mean_of_min_ten_percent_full_portfolio_in_win'))

    a1_mean_of_min_ten_percent_win_pd = a1_mean_of_min_ten_percent_win.toPandas()
    a2_mean_of_min_ten_percent_win_pd = a2_mean_of_min_ten_percent_win.toPandas()
    a3_mean_of_min_ten_percent_win_pd = a3_mean_of_min_ten_percent_win.toPandas()
    a4_mean_of_min_ten_percent_win_pd = a4_mean_of_min_ten_percent_win.toPandas()
    a5_mean_of_min_ten_percent_win_pd = a5_mean_of_min_ten_percent_win.toPandas()
    a6_mean_of_min_ten_percent_win_pd = a6_mean_of_min_ten_percent_win.toPandas()
    full_portfolio_mean_of_min_ten_percent_win_pd = full_ds_stat_extractor(full_portfolio_mean_of_min_ten_percent_win)

    # #----
    a1_safety_measure_on_avgdev_win = assets_with_yields_df.select(calculate_safety_measure_on_average_deviation('asset_1').over(sliding_window).alias('safety_measure_on_avgdev_1_in_win'))
    a2_safety_measure_on_avgdev_win = assets_with_yields_df.select(calculate_safety_measure_on_average_deviation('asset_2').over(sliding_window).alias('safety_measure_on_avgdev_2_in_win'))
    a3_safety_measure_on_avgdev_win = assets_with_yields_df.select(calculate_safety_measure_on_average_deviation('asset_3').over(sliding_window).alias('safety_measure_on_avgdev_3_in_win'))
    a4_safety_measure_on_avgdev_win = assets_with_yields_df.select(calculate_safety_measure_on_average_deviation('asset_4').over(sliding_window).alias('safety_measure_on_avgdev_4_in_win'))
    a5_safety_measure_on_avgdev_win = assets_with_yields_df.select(calculate_safety_measure_on_average_deviation('asset_5').over(sliding_window).alias('safety_measure_on_avgdev_5_in_win'))
    a6_safety_measure_on_avgdev_win = assets_with_yields_df.select(calculate_safety_measure_on_average_deviation('asset_6').over(sliding_window).alias('safety_measure_on_avgdev_6_in_win'))
    full_portfolio_safety_measure_on_avgdev_win = all_in_one_col_id_data_win.select(calculate_safety_measure_on_average_deviation('full_portfolio').over(sliding_window_full).alias('safety_measure_on_avgdev_full_portfolio_in_win'))

    a1_safety_measure_on_avgdev_win_pd = a1_safety_measure_on_avgdev_win.toPandas()
    a2_safety_measure_on_avgdev_win_pd = a2_safety_measure_on_avgdev_win.toPandas()
    a3_safety_measure_on_avgdev_win_pd = a3_safety_measure_on_avgdev_win.toPandas()
    a4_safety_measure_on_avgdev_win_pd = a4_safety_measure_on_avgdev_win.toPandas()
    a5_safety_measure_on_avgdev_win_pd = a5_safety_measure_on_avgdev_win.toPandas()
    a6_safety_measure_on_avgdev_win_pd = a6_safety_measure_on_avgdev_win.toPandas()
    full_portfolio_safety_measure_on_avgdev_win_pd = full_ds_stat_extractor(full_portfolio_safety_measure_on_avgdev_win)
    

    # Collect all stats for all windows
    c = pd.concat([a1_mean_win_pd, a2_mean_win_pd, a3_mean_win_pd, a4_mean_win_pd, a5_mean_win_pd, a6_mean_win_pd, full_portfolio_mean_win_pd,\
                a1_median_win_pd, a2_median_win_pd, a3_median_win_pd, a4_median_win_pd, a5_median_win_pd, a6_median_win_pd, full_portfolio_median_win_pd,\
                a1_one_tenth_win_pd, a2_one_tenth_win_pd, a3_one_tenth_win_pd, a4_one_tenth_win_pd, a5_one_tenth_win_pd, a6_one_tenth_win_pd, full_portfolio_one_tenth_win_pd,\
                a1_mean_of_min_ten_percent_win_pd, a2_mean_of_min_ten_percent_win_pd, a3_mean_of_min_ten_percent_win_pd, a4_mean_of_min_ten_percent_win_pd, a5_mean_of_min_ten_percent_win_pd, a6_mean_of_min_ten_percent_win_pd, full_portfolio_mean_of_min_ten_percent_win_pd,\
                a1_safety_measure_on_avgdev_win_pd, a2_safety_measure_on_avgdev_win_pd, a3_safety_measure_on_avgdev_win_pd, a4_safety_measure_on_avgdev_win_pd, a5_safety_measure_on_avgdev_win_pd, a6_safety_measure_on_avgdev_win_pd, full_portfolio_safety_measure_on_avgdev_win_pd \
    ], axis=1)
    all_windows_stats_df = c[c['mean_of_asset_1_in_win'] != -100]

    calculate_windows_overrun(all_windows_stats_df, full_dataset_stats_df)

    et = datetime.datetime.now()
    elapsed_time = et - st
    print(elapsed_time)
    spark.stop()
