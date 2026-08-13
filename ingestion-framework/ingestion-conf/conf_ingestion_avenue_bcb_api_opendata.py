config = {
    "project_id": "avenue",
    "service" : "bcb",
    "type" : "opendata",
    "instance" : "api",
    "database" : "opendata",
    "sec" : {
        "host" : "https://api.bcb.gov.br"
    },
    "tables": {
        "selic_daily" : {
            "endpoint" : "/dados/serie/bcdata.sgs.{id}/dados?formato={type_file}",
            "extract_mode" : "update",
            "id" : "11",
            "type_file" : "json",
            "primary_key"  : 'data',
            "changed_range" : '7 days',
            "changed_date" : 'data',
            "exclude_columns" :[],
            "schema" : [
                {"field" : "data","type" : "DATE"},
                {"field" : "valor","type" : "DOUBLE"}
            ]
        },
         "igpm_monthly" : {
            "endpoint" : "/dados/serie/bcdata.sgs.{id}/dados?formato={type_file}",
            "extract_mode" : "update",
            "id" : "189",
            "type_file" : "json",
            "primary_key"  : 'data',
            "changed_range" : '12 Months',
            "changed_date" : 'data',
            "exclude_columns" :[],
            "schema" : [
                {"field" : "data","type" : "DATE"},
                {"field" : "valor","type" : "DOUBLE"}
            ]
        },
         "inpc_monthly" : {
            "endpoint" : "/dados/serie/bcdata.sgs.{id}/dados?formato={type_file}",
            "extract_mode" : "update",
            "id" : "188",
            "type_file" : "json",
            "primary_key"  : 'data',
            "changed_range" : '12 Months',
            "changed_date" : 'data',
            "exclude_columns" :[],
            "schema" : [
                {"field" : "data","type" : "DATE"},
                {"field" : "valor","type" : "DOUBLE"}
            ]
        },
         "ipca_monthly" : {
            "endpoint" : "/dados/serie/bcdata.sgs.{id}/dados?formato={type_file}",
            "extract_mode" : "update",
            "id" : "433",
            "type_file" : "json",
            "primary_key"  : 'data',
            "changed_range" : '12 Months',
            "changed_date" : 'data',
            "exclude_columns" :[],
            "schema" : [
                {"field" : "data","type" : "DATE"},
                {"field" : "valor","type" : "DOUBLE"}
        ]
        },
        "selic_monthly" : {
            "endpoint" : "/dados/serie/bcdata.sgs.{id}/dados?formato={type_file}",
            "extract_mode" : "update",
            "id" : "4390",
            "type_file" : "json",
            "primary_key"  : 'data',
            "changed_range" : '12 Months',
            "changed_date" : 'data',
            "exclude_columns" :[],
            "schema" : [
                {"field" : "data","type" : "DATE"},
                {"field" : "valor","type" : "DOUBLE"}
        ]
        }
    }
}
