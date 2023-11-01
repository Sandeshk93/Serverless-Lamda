import json
from mysql.connector import connect
from mysql import connector
from omni.core.cache import get_config, get_service_token, get_fine_grain
from omni.core.utils import check_cold, keep_warm


def GetConnection(host, port, user, passwd, db):
    conn = None
    conn = connector.connect(host=host,
                             port=port, user=user, passwd=passwd,
                             db=db)
    return conn


def GetUserDetails(cursor, UserEmailAddress, config):
    proc = ""
    result = []

    proc = config["sp_get_userdetails"]
    args = ('', UserEmailAddress)

    result = executeGet(proc, args, cursor)
    return result


def executeGet(proc, args, cursor):
    try:
        result = []
        cursor.callproc(proc, args)
        for result in cursor.stored_results():
            dataset = result.fetchall()
            columns = result.description
        result = []
        for value in dataset:

            tmp = {}
            for (index, column) in enumerate(value):
                tmp[columns[index][0]] = column
            result.append(tmp)

    except connector.Error as e:
        result = e

    return result


def main(event, context):
    # Cold start / keep warm
    check_cold()
    if keep_warm(event):
        return
    Result = []
    GrossSales = []
    HourlySales = []
    error_return_code = '1'

    try:
        # Get config
        config = get_config(
            function_name=context.function_name,
            banner="all")

    except Exception as e:
        print(f"Error: Unable to load config - {e}")
        return {
            "return_code": error_return_code,
            "return_message": f"Error: Unable to load config - {e}"
        }
    else:
        try:
            # Fine grain authorization
            get_fine_grain(
                client_id=event["oauth_client_name"],
                host_name=event["host_name"],
                url_fragment=event["url_fragment"],
                http_method=event["http_method"],
                retries=config["fine_grain_retry_times"],
                backoff=config["fine_grain_backoff_delay"]
            )
        except Exception as e:
            # Return system and user errors
            print("Error: Fine grain auth exception - ", e)
            return {
                "return_code": error_return_code,
                "return_message": "Error: An error occurred during fine grain authentication."
            }

    conn = GetConnection(host=config["tlog_replica_host"], port=config["tlog_replica_port"],
                         user=config["tlog_replica_username"], passwd=config["tlog_replica_password"],
                         db=config["tlog_replica_database"])
    cursor = conn.cursor()
    if event is not None:
        UserEmailAddress = event["UserEmailAddress"]
        UserPassword = event["UserPassword"]

    UserDetails = GetUserDetails(cursor, UserEmailAddress, config)

    Result.append({"UserDetails": UserDetails
                   })

    conn.close()

    # TODO implement
    return {
        'statusCode': 200,
        'Result': Result
    }
