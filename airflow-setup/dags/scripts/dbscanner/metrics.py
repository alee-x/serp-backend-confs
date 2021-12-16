from sqlalchemy import create_engine, inspect
import pandas as pd
import numpy as np

def get_metrics(**context):
    db_platform = context['dag_run'].conf['db_platform']
    db_name = context['dag_run'].conf['database_name']
    schema_name = context['dag_run'].conf['schema_name']
    table_name = context['dag_run'].conf['table_name']

    connection_str = context['ti'].xcom_pull(key='connection_str')
    engine = create_engine(connection_str)

    row_count = int(pd.read_sql('SELECT COUNT(*) FROM "{0}"."{1}"'.format(schema_name,table_name), engine).values)

    if row_count == 0:
        metrics_result = {"row_count" : 0}

    if row_count > 0:
        chunksize = 100000
        
        summary_num = []
        summary_char = []
        summary_dt = []

        # predefine dataframes to avoid null error after loop?
        df_num = pd.DataFrame()
        df_num = pd.DataFrame()
        df_dt = pd.DataFrame()

        batches = int(row_count / chunksize) + 1
        for i in range(batches):
            query = 'SELECT * FROM "{0}"."{1}" ORDER BY 1 OFFSET {2} ROWS FETCH NEXT {3} ROWS ONLY'.format(schema_name,table_name,i*chunksize,chunksize)
            df = pd.read_sql_query(query, con=engine)

            # include: number
            try:  
                df_num = df.describe(include=[np.number]).loc[['count','mean','std','min','max'],:]      
                summary_num.append(df_num)
            except ValueError as e:        # 'No objects to concatenate'
                if (e.args[0]=="No objects to concatenate"):
                    pass

            # exclude: number, date
            try:        
                df_char = df.describe(exclude=[np.number, np.datetime64])
                summary_char.append(df_char)
            except ValueError as e:        # 'No objects to concatenate'
                if (e.args[0]=="No objects to concatenate"):
                    df_char = []
                    pass

            # include date
            try:
                df_dt = df.describe(include=[np.datetime64])
                summary_dt.append(df_dt)   
            except ValueError as e:        # 'No objects to concatenate'
                if (e.args[0]=="No objects to concatenate"):
                    pass

        num_columns = []
        for col in df_num.columns.values:
            agg_count = int(sum([summary_num[i].loc['count'][col] for i in range(len(summary_num)) if (col in summary_num[i]) and (summary_num[i].loc['count'][col] > 0)]))        
            agg_min = min([summary_num[i].loc['min'][col] for i in range(len(summary_num)) if (col in summary_num[i]) and (summary_num[i].loc['count'][col] > 0)])
            agg_max = max([summary_num[i].loc['max'][col] for i in range(len(summary_num)) if (col in summary_num[i]) and (summary_num[i].loc['count'][col] > 0)])
            agg_mean = sum([summary_num[i].loc['mean'][col]*summary_num[i].loc['count'][col] for i in range(len(summary_num)) if (col in summary_num[i]) and (summary_num[i].loc['count'][col] > 0)])/agg_count
            agg_std = round(np.sqrt(sum([np.square(summary_num[i].loc['std'][col])*summary_num[i].loc['count'][col] for i in range(len(summary_num)) if (col in summary_num[i]) and (summary_num[i].loc['count'][col] > 0)])/agg_count),6)
            c = { "name": col, "count": agg_count, "min": str(agg_min), "max": str(agg_max), "avg": str(agg_mean), "stddev": str(agg_std)}
            num_columns.append(c)

        char_columns = []
        if len(df_char)!=0:
            for col in df_char.columns.values:
                topList = {}
                agg_count = int(sum([summary_char[i].loc['count'][col] for i in range(len(summary_num))]))  
                try:
                    agg_unique_per = round(sum([summary_char[i].loc['unique'][col] / summary_char[i].loc['count'][col] for i in range(len(summary_num)) if summary_char[i].loc['count'][col] > 0]) / len(summary_num),6)
                except ZeroDivisionError:
                    agg_unique_per = 0
                for i in range(len(summary_char)):
                    topList[summary_char[i].loc['top'][col]] = topList.get(summary_char[i].loc['top'][col], 0) + summary_char[i].loc['freq'][col]
                #print(foo)
                sorted_top = sorted(topList.items(), key = lambda kv:(kv[1], kv[0]), reverse=True)
                bool_series = pd.notnull(sorted_top[0][1])
                if sorted_top and bool_series and int(sorted_top[0][1]) > 10:
                    c = { "name": col, "count": agg_count, "unique_per": str(agg_unique_per), "most_freq_val" : str(sorted_top[0][0]), "freq" : int(sorted_top[0][1])}
                else:
                    c = { "name": col, "count": agg_count, "unique_per": str(agg_unique_per)}
                char_columns.append(c)

        dt_columns = []
        for col in df_dt.columns.values:
            topList = {}
            agg_count = int(sum([summary_dt[i].loc['count'][col] for i in range(len(summary_dt))])) 
            try:
                agg_unique_per = round(sum([summary_dt[i].loc['unique'][col] / summary_dt[i].loc['count'][col] for i in range(len(summary_dt)) if summary_dt[i].loc['count'][col] > 0]) / len(summary_num),6)
            except ZeroDivisionError:
                agg_unique_per = 0
            agg_first = min([summary_dt[i].loc['first'][col] for i in range(len(summary_dt))])
            agg_last = max([summary_dt[i].loc['last'][col] for i in range(len(summary_dt))])
            for i in range(len(summary_dt)):
                topList[summary_dt[i].loc['top'][col]] = topList.get(summary_dt[i].loc['top'][col], 0) + summary_dt[i].loc['freq'][col]
            #print(foo)
            sorted_top = sorted(topList.items(), key = lambda kv:(kv[1], kv[0]), reverse=True)
            if sorted_top and int(sorted_top[0][1]) > 10:
                c = { "name": col, "count": agg_count, "unique_per": str(agg_unique_per),               "min" : str(agg_first), "max" : str(agg_last),               "most_freq_val" : str(sorted_top[0][0]), "freq" : int(sorted_top[0][1])}
            else:
                c = { "name": col, "count": agg_count, "unique_per": str(agg_unique_per),               "min" : str(agg_first), "max" : str(agg_last)}
            dt_columns.append(c)

        metrics_result = {"row_count" : row_count,  "num_metrics" : num_columns, "char_metrics" : char_columns , "dt_metrics" : dt_columns}
        context['ti'].xcom_push(key='metrics_result',value=metrics_result)