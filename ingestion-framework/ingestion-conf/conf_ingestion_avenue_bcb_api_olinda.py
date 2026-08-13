config = {
    "project_id": "avenue",
    "service" : "bcb",
    "type" : "olinda",
    "instance" : "api",
    "database" : "olinda",
    "sec" : {
        "host" : "https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata/CotacaoMoedaPeriodo(moeda=@moeda,dataInicial=@dataInicial,dataFinalCotacao=@dataFinalCotacao)"
    },
    "tables": {
        "eur_ptax" : {
            "endpoint" : "?@moeda='EUR'&@dataInicial='{incial}'&@dataFinalCotacao='{final}'&$filter=tipoBoletim%20eq%20'Fechamento'&$format={format}",
            "extract_mode" : "update",
            "id": "",
            "type_file" : "json",
            "primary_key"  : 'dataHoraCotacao',
            "changed_range" : '7 days',
            "changed_date" : 'dataHoraCotacao',
            "exclude_columns" :[],
            "schema" : [
                {"field" : "paridadeCompra","type" : "DOUBLE"},
                {"field" : "paridadeVenda","type" : "DOUBLE"},
                {"field" : "cotacaoCompra","type" : "DOUBLE"},
                {"field" : "cotacaoVenda","type" : "DOUBLE"},
                {"field" : "dataHoraCotacao","type" : "TIMESTAMP"},
                {"field" : "tipoBoletim","type" : "STRING"}
            ]
        }
    }
}
