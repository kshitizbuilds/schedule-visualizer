
# ====================================================================
# GLOBAL FILTER PARAMETER
# ====================================================================

# Status value to exclude from the results (e.g., 'P' for Pending/Processing)
STATUS_TO_EXCLUDE = 'P'

# ====================================================================
# QUERY DEFINITIONS
# ====================================================================

SCHEDULING_QUERIES = [
    {
        "name": "RAADW-SCHED",
        "description": "Scheduled/Recurring Jobs",
        "schema": "SCHED_BIPLATFORM",
        "filter_status_excluded": STATUS_TO_EXCLUDE,
        "sql": f"""
            SELECT 
                'RAADW-SCHED' APPLICATION_SERVER,
                'ADW' APPLICAITON,
                XMLP.SCHEDULE_DESCRIPTION FREQUENCY,
                TRIM(XMLP.USER_JOB_NAME) JOB_NAME,
                TO_CHAR(START_DATE, 'HH24:MI:SS') START_TIME,
                XMLP.REPORT_URL,
                XMLP.ISSUER,
                XMLP.OWNER,
                XMLP.JOB_ID JOB_ID,
                XMLP.CREATED,
                XMLP.LAST_UPDATED,
                XMLP.START_DATE,
                XMLP.END_DATE,
                XMLP.JOB_GROUP,
                XMLP.SCHEDULE_DESCRIPTION,
                XMLP.USER_DESCRIPTION,
                XMLP.DELIVERY_DESCRIPTION
            FROM SCHED_BIPLATFORM.QRTZ_JOB_DETAILS QRTZ
            INNER JOIN SCHED_BIPLATFORM.XMLP_SCHED_JOB XMLP
                ON QRTZ.JOB_NAME = XMLP.JOB_ID
            WHERE 1 = 1
            AND XMLP.STATUS <> '{STATUS_TO_EXCLUDE}'
            ORDER BY 1,2,3
        """
    },
    {
        "name": "RAADW-ADHOC",
        "description": "Ad-Hoc Jobs",
        "schema": "ADHOC_BIPLATFORM",
        "filter_status_excluded": STATUS_TO_EXCLUDE,
        "sql": f"""
            SELECT 
                'RAADW-ADHOC' APPLICATION_SERVER,
                'ADW' APPLICAITON,
                XMLP.SCHEDULE_DESCRIPTION FREQUENCY,
                TRIM(XMLP.USER_JOB_NAME) JOB_NAME,
                TO_CHAR(START_DATE, 'HH24:MI:SS') START_TIME,
                XMLP.REPORT_URL,
                XMLP.ISSUER,
                XMLP.OWNER,
                XMLP.JOB_ID JOB_ID,
                XMLP.CREATED,
                XMLP.LAST_UPDATED,
                XMLP.START_DATE,
                XMLP.END_DATE,
                XMLP.JOB_GROUP,
                XMLP.SCHEDULE_DESCRIPTION,
                XMLP.USER_DESCRIPTION,
                XMLP.DELIVERY_DESCRIPTION
            FROM ADHOC_BIPLATFORM.QRTZ_JOB_DETAILS QRTZ
            INNER JOIN ADHOC_BIPLATFORM.XMLP_SCHED_JOB XMLP
                ON QRTZ.JOB_NAME = XMLP.JOB_ID
            WHERE 1 = 1
            AND XMLP.STATUS <> '{STATUS_TO_EXCLUDE}'
            ORDER BY 1,2,3
        """
    },
    {
        "name": "RAADW-APP",
        "description": "Application & Production Jobs (UNION of two schemas)",
        "schema": "RAAPP_BIPLATFORM, PRD_BIPLATFORM",
        "filter_status_excluded": STATUS_TO_EXCLUDE,
        "sql": f"""
            SELECT 
                'RAADW-APP' APPLICATION_SERVER,
                'ADW' APPLICAITON,
                XMLP.SCHEDULE_DESCRIPTION FREQUENCY,
                TRIM(XMLP.USER_JOB_NAME) JOB_NAME,
                TO_CHAR(START_DATE, 'HH24:MI:SS') START_TIME,
                XMLP.REPORT_URL,
                XMLP.ISSUER,
                XMLP.OWNER,
                XMLP.JOB_ID JOB_ID,
                XMLP.CREATED,
                XMLP.LAST_UPDATED,
                XMLP.START_DATE,
                XMLP.END_DATE
            FROM RAAPP_BIPLATFORM.QRTZ_JOB_DETAILS QRTZ
            INNER JOIN RAAPP_BIPLATFORM.XMLP_SCHED_JOB XMLP
                ON QRTZ.JOB_NAME = XMLP.JOB_ID
            WHERE 1 = 1
            AND XMLP.STATUS <> '{STATUS_TO_EXCLUDE}'
            UNION 
            SELECT 
                'RAADW-APP' APPLICATION_SERVER,
                'ADW' APPLICAITON,
                XMLP.SCHEDULE_DESCRIPTION FREQUENCY,
                TRIM(XMLP.USER_JOB_NAME) JOB_NAME,
                TO_CHAR(START_DATE, 'HH24:MI:SS') START_TIME,
                XMLP.REPORT_URL,
                XMLP.ISSUER,
                XMLP.OWNER,
                XMLP.JOB_ID JOB_ID,
                XMLP.CREATED,
                XMLP.LAST_UPDATED,
                XMLP.START_DATE,
                XMLP.END_DATE,   
                XMLP.JOB_GROUP,
                XMLP.SCHEDULE_DESCRIPTION,
                XMLP.USER_DESCRIPTION,
                XMLP.DELIVERY_DESCRIPTION
            FROM PRD_BIPLATFORM.QRTZ_JOB_DETAILS QRTZ
            INNER JOIN PRD_BIPLATFORM.XMLP_SCHED_JOB XMLP
                ON QRTZ.JOB_NAME = XMLP.JOB_ID
            WHERE 1 = 1
            AND XMLP.STATUS <> '{STATUS_TO_EXCLUDE}'
            ORDER BY 1,2,3
        """
    },
   
]



