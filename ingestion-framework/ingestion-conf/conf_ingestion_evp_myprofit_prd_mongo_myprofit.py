config = {
    "project_id": "evp-myprofit-prd",
    "service" : "mongodb",
    "type" : "mongodb",
    "instance" : "evp-myprofit-prd",
    "database" : "MyProfitEVP",
    "sec" : {
        "sm_user_secret_id" :"user-evp-myprofit-prd-MyProfitEVP",
        "sm_pwd_secret_id" :"pwd-evp-myprofit-prd-MyProfitEVP"
    },
    "tables" : {
        "CustomerAccount" :
        {
            "endpoint" : "CustomerAccount",
            "extract_mode" : "update",
            "primary_key" : "MovimentId",
            "changed_date" : "PositionDate",
            "changed_range" : "7 days",
            "export_date_included": False,
            "date_fields" : ["PositionDate"],
            "string_fields" : ['AccountNumber'],
            "double_fields" : ["PositionValue",
                               "CashValue",
                               "Provents",
                               "Purchases",
                               "PurchasesCrypto",
                               "Deposits",
                               "Sales",
                               "SalesCrypto",
                               "Withdrawals"],
            "binary_fields" : ['MovimentId',
                               'CustomerId',
                               'id']
        }
    }
}
