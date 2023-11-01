import os
import json
from mysql.connector import connect
from mysql import connector


def GetConnection():
    conn = None
    conn = connector.connect(
        host=os.environ.get("TLOG_DB_HOST"),
        user=os.environ.get("TLOG_DB_USER"),
        passwd=os.environ.get("TLOG_DB_PASS"),
        port=3306,
        db="tlog_dashboard_db")
    return conn


def GetAverageBasketSizeForWeek(cursor, TransactionDate, storeNumber, ReportType):
    proc = ""
    result = []
    proc = "tlog_dashboard_db.sp_BasketSize_getWeekly_read"
    args = (storeNumber, TransactionDate, ReportType)
    result = executeGet(proc, args, cursor)
    return result


def GetAverageSizeForDay(cursor, TransactionDate, storeNumber, ReportType):
    proc = ""
    result = []

    proc = "tlog_dashboard_db.sp_get_TotalAverageBasketSize_read"
    args = (TransactionDate, storeNumber, ReportType)

    result = executeGet(proc, args, cursor)
    print(result)
    return result


def GetHourlyAverageSizeForDay(cursor, TransactionDate, storeNumber, ReportType):
    proc = ""
    result = []

    proc = 'tlog_dashboard_db.sp_hourlyBasketSize_get_read'
    args = (storeNumber, TransactionDate, ReportType)

    result = executeGet(proc, args, cursor)

    return result


def GetAverageSizeForDepartment(cursor, TransactionDate, storeNumber):
    proc = ""
    result = []
    response = []

    proc = "tlog_dashboard_db.sp_DeptAvgBasketSize_get_read"

    args = (storeNumber, TransactionDate)
    print(args)

    result = executeGet(proc, args, cursor)
    print(result)

    return result


def GetAverageSizeDepartmentWise(cursor, TransactionDate, storeNumber):
    proc = ""
    result = []
    response = []

    proc = "tlog_dashboard_db.sp_DeptWiseBasketSize_get_read"

    args = (storeNumber, TransactionDate)

    result = executeGet(proc, args, cursor)

    return result


def executeGet(proc, args, cursor):
    try:
        result = []
        print("78", proc)
        cursor.callproc(str(proc), args)
        for result in cursor.stored_results():
            dataset = result.fetchall()
            columns = result.description
        result = []
        print("83", dataset)
        for value in dataset:

            tmp = {}
            for (index, column) in enumerate(value):
                tmp[columns[index][0]] = column
            result.append(tmp)

    except connector.Error as e:
        result = e

    return result


def GetTotalAvgBasketSize1(cursor, TransactionDate, param, Filter):
    proc = ""
    result = []
    if Filter == 'Day':
        proc = 'tlog_dashboard_db.sp_get_TotalAverageBasketSize_read'
    if Filter == 'Week':
        proc = 'tlog_dashboard_db.sp_get_TotalAvgBasketSizeWeekly_read'
    if Filter == 'Month':
        proc = 'tlog_dashboard_db.sp_get_TotalAvgBasketSizeMonthly_read'
    args = (param, TransactionDate)
    result = executeGet(proc, args, cursor)
    return result


def GetTotalAvgBasketSizeBanner(cursor, TransactionDate, param, Filter):
    proc = ""
    result = []
    if Filter == 'Day':
        proc = 'tlog_dashboard_db.sp_get_TotalAvgBasketSizeForBanner_read'
    if Filter == 'Week':
        proc = 'tlog_dashboard_db.sp_get_TotalBasketSizeWeeklyBanner_read'
    if Filter == 'Month':
        proc = 'tlog_dashboard_db.sp_get_TotalBasketSizeMonthlyBanner_read'
    # proc =spName
    args = (param, TransactionDate)
    result = executeGet(proc, args, cursor)
    return result


def main(event, context):
    conn = GetConnection()
    cursor = conn.cursor()
    Result = []
    GrossSales = []
    HourlySales = []

    if event is not None:
        TransactionDate = event["TransactionDate"]
        storeNumber = event["StoreNumber"]
        ReportType = event["ReportType"]
        Type = event["Type"]
        param = storeNumber
        AverageBasketSize = []

        Filter = event["Filter"]
        spName = ''
        if (Filter == 'Department'):
            AverageBasketSize = GetAverageSizeForDepartment(cursor, TransactionDate, storeNumber)
            DepartmentWiseBasketSize = GetAverageSizeDepartmentWise(cursor, TransactionDate, storeNumber)

            Result.append({"AvgBasketSize": AverageBasketSize,
                           "DepartmentWiseBasketSize": DepartmentWiseBasketSize
                           })

        if (Type == 'Total'):
            if (Filter == 'Day'):
                if storeNumber is not None and storeNumber != '':
                    # spName = 'tlog_dashboard_db.sp_get_TotalAverageBasketSize_read'
                    try:
                        AverageBasketSize = GetTotalAvgBasketSize1(cursor, TransactionDate, param, Filter)
                    except Exception as e:
                        return e
                else:
                    if "Banner" in event:
                        param = event["Banner"]
                        AverageBasketSize = GetTotalAvgBasketSizeBanner(cursor, TransactionDate, param, Filter)

            if (Filter == 'Week'):
                if storeNumber is not None and storeNumber != '':
                    AverageBasketSize = GetTotalAvgBasketSize1(cursor, TransactionDate, param, Filter)
                else:
                    if "Banner" in event and event["Banner"] is not None:
                        param = event["Banner"]
                        AverageBasketSize = GetTotalAvgBasketSizeBanner(cursor, TransactionDate, param, Filter)

            if (Filter == 'Month'):
                if storeNumber is not None and storeNumber != '':
                    AverageBasketSize = GetTotalAvgBasketSize1(cursor, TransactionDate, param, Filter)
                else:
                    if "Banner" in event and event["Banner"] is not None:
                        param = event["Banner"]
                        AverageBasketSize = GetTotalAvgBasketSizeBanner(cursor, TransactionDate, param, Filter)

            Result.append({"AvgBasketSize": AverageBasketSize
                           })
        else:
            if (Filter == 'Day'):
                AverageBasketSize = GetAverageSizeForDay(cursor, TransactionDate, storeNumber, ReportType)
                HourlyBasketSize = GetHourlyAverageSizeForDay(cursor, TransactionDate, storeNumber, ReportType)

                Result.append({"AvgBasketSize": AverageBasketSize,
                               "HourlyBasketSize": HourlyBasketSize
                               })
            if (Filter == 'Week'):
                AverageBasketSize = GetAverageBasketSizeForWeek(cursor, TransactionDate, storeNumber, ReportType)
                DepartmentWiseBasketSize = GetAverageSizeDepartmentWise(cursor, TransactionDate, storeNumber)

                Result.append({"AvgBasketSize": AverageBasketSize,
                               "DepartmentWiseBasketSize": DepartmentWiseBasketSize
                               })

    conn.close()

    # TODO implement
    return {
        'statusCode': 200,
        'Result': Result
    }
