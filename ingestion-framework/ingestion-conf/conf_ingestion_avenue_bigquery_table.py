config = {
    "project_id": "avenue",
    "service" : "bigquery",
    "type" : "googleanalytics",
    "instance" : "ga4",
    "database" : "analytics",
    'modification_date' : '3 days',
    "sec" : {
        "host" : 'data-prod-295413'
    },
    "tables": {
        "events" : {
            "endpoint" : "events",
            "extract_mode" : "update",
            "dataset" : "analytics_328012517",
            "primary_key"  : "event_date, event_timestamp, event_name,event_params",
            "changed_range" : None,
            "changed_date" : None,
            "parent_columns" : [],
            "exclude_columns" :[],
            "col_escape_char" : None,
            "struct_fields" : [
                'event_params',
                'privacy_info',
                'user_properties',
                'user_ltv',
                'device',
                'geo',
                'traffic_source',
                'ecommerce',
                'items',
                'collected_traffic_source'
                ],
            "integer_fields" :['event_date'],
            "float_fields" :["event_value_in_usd"],
            "timestamp_fields":[]
        },

        "user" : {
            "endpoint" : "pseudonymous_users",
            "extract_mode" : "update",
            "dataset" : "analytics_328012517",
            "primary_key"  : "pseudo_user_id, stream_id, user_info",
            "changed_range" : None,
            "changed_date" : None,
            "parent_columns" : [],
            "exclude_columns" :[],
            "col_escape_char" : None,
            "struct_fields" : [
                'user_info',
                'device',
                'user_properties',
                'geo',
                'audiences',
                'user_ltv',
                'predictions',
                'privacy_info'
                ],
            "integer_fields" :["occurrence_date", "last_updated_date"],
            "float_fields" :[],
            "timestamp_fields":[]
        }
    }
}
