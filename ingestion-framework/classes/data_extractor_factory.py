class DataExtractorFactory:

    @staticmethod
    def get_extractor(ingestion_config):
        if ingestion_config['service'] == 's3':

            return S3DataExtractor(ingestion_config['sec']['sm_user_secret_id'],
                                   ingestion_config['sec']['sm_pwd_secret_id'],
                                   ingestion_config['sec']['instance']
                                   )

        if ingestion_config['service'] == 'jira':
            return JiraDataExtractor(ingestion_config['sec']['sm_user_secret_id'],
                                     ingestion_config['sec']['sm_token_secret_id'],
                                     ingestion_config['sec']['instance']
                                   )
        if ingestion_config['service'] == 'salesforce':
            return SalesforceDataExtractor(ingestion_config['sec']['sm_user_secret_id'],
                                           ingestion_config['sec']['sm_pwd_secret_id'],
                                           ingestion_config['sec']['sm_token_secret_id'],
                                           ingestion_config['sec']['instance'],
                                           ingestion_config['sec']['client_id'],
                                           ingestion_config['sec']['version']
                                           )
        if ingestion_config['service'] == 'cloudsql':
            return CloudSQLDataExtractor(ingestion_config['sec']['sm_user_secret_id'],
                                         ingestion_config['sec']['sm_pwd_secret_id'],
                                         ingestion_config['sec']['host'],
                                         ingestion_config['sec']['db_name'],
                                         ingestion_config['type'],
                                         ingestion_config['sec']['port']

                                           )
        if ingestion_config['service'] == 'hiplatform':
            return HiChatDataExtractor(ingestion_config['sec']['sm_user_secret_id'],
                                       ingestion_config['sec']['sm_pwd_secret_id'],
                                       ingestion_config['sec']['sm_token_secret_id'],
                                       ingestion_config['sec']['host']
                                       )
        if ingestion_config['service'] == 'facebook':
            return FacebookAdsDataExtractor(ingestion_config['sec']['sm_user_secret_id'],
                                            ingestion_config['sec']['sm_pwd_secret_id'],
                                            ingestion_config['sec']['sm_token_secret_id']
                                            )
        if ingestion_config['service'] == 'gcs':
            if ('mult_files' in ingestion_config and ingestion_config['mult_files'] == True):
                return GcsDataExtractorMultFiles(ingestion_config['sec']['instance'],
                                                 ingestion_config['type'],
                                                 ingestion_config['service'],
                                                 ingestion_config['project_id'],
                                                 ingestion_config['instance'],
                                                 ingestion_config['database']
                                                 )
            else:
                return GcsDataExtractor(ingestion_config['sec']['instance'],
                                        ingestion_config['type']
                                        )
        if ingestion_config['service'] == 'myprofit':
            return MyProfitDataExtractor(ingestion_config['sec']['sm_user_secret_id'],
                                         ingestion_config['sec']['sm_pwd_secret_id'],
                                         ingestion_config['sec']['host']
                                        )
        if ingestion_config['service'] == 'nasdaq':
            return NasdaqDataExtractor(ingestion_config['sec']['sm_token_secret_id'],
                                         ingestion_config['sec']['host']
                                        )
        if ingestion_config['service'] == 'apex':
            return ApexAPIDataExtractor(ingestion_config['sec']['sm_user_secret_id'],
                                        ingestion_config['sec']['sm_pwd_secret_id'],
                                        ingestion_config['sec']['host']
                                       )
        if ingestion_config['service'] == 'datastore':
            return DatastoreDataExtractor(ingestion_config['project_id'],ingestion_config['namespace'])

        if ingestion_config['service'] == 'ftp' and ingestion_config['instance'] == 'apex' :
            return FtpApexDataExtractor(ingestion_config['sec']['sm_user_secret_id'],
                                        ingestion_config['sec']['sm_pwd_secret_id'],
                                        ingestion_config['sec']['sm_pwd_zipfile_secret_id'],
                                        ingestion_config['sec']['host'],
                                        ingestion_config['type']
                                       )

        # if ingestion_config['service'] == 'worthix':
        #     return WorthixDataExtractor(ingestion_config['sec']['sm_user_secret_id'],
        #                                 ingestion_config['sec']['sm_token_secret_id'],
        #                                 ingestion_config['sec']['host']
        #                                )
        # if ingestion_config['service'] == 'qulturerocks':
        #     return QultureRocksDataExtractor(ingestion_config['sec']['sm_user_secret_id'],
        #                                      ingestion_config['sec']['sm_token_secret_id'],
        #                                      ingestion_config['sec']['host']
        #                                     )

        if ingestion_config['service'] == 'ftp' and ingestion_config['instance'] == 'thales' :
            return FtpThalesDataExtractor(ingestion_config['sec']['sm_user_secret_id'],
                                          ingestion_config['sec']['sm_pwd_secret_id'],
                                          ingestion_config['sec']['sm_token_secret_id'],
                                          ingestion_config['sec']['host'],
                                          ingestion_config['sec']['ssh_port']
                                         )

        if ingestion_config['service'] == 'sharepoint':
            return SharepointDataExtractor(ingestion_config['sec']['sm_user_secret_id'],
                                          ingestion_config['sec']['sm_pwd_secret_id'],
                                          ingestion_config['sec']['site_url']
                                         )
        if ingestion_config['service'] == 'gupy':
            return GupyDataExtractor(ingestion_config['sec']['sm_token_secret_id'],
                                    ingestion_config['sec']['host']
                                         )
        if ingestion_config['service'] == 'cognito':
            return CognitoDataExtractor(ingestion_config['sec']['sm_user_secret_id'],
                                    ingestion_config['sec']['sm_pwd_secret_id'],
                                    ingestion_config['sec']['user_pool_id']
                                         )
        if ingestion_config['service'] == 'mongodb':
            return MongoDBDataExtractor(ingestion_config['sec']['sm_user_secret_id'],
                                    ingestion_config['sec']['sm_pwd_secret_id'],
                                    ingestion_config['database'],
                                    ingestion_config['instance'],
                                    ingestion_config['service'],
                                    ingestion_config['type']
                                         )
        if ingestion_config['service'] == 'bcb':
            return BCBDataExtractor(ingestion_config['sec']['host'])

        if ingestion_config['service'] == 'fedfunds':
            return FEDFundsDataExtractor(ingestion_config['sec']['host'],
                                        ingestion_config['sec']['sm_token_secret_id'])

        if ingestion_config['service'] == 'bigquery':
            return BigQueryDataExtractor(ingestion_config['sec']['host'],
                                        ingestion_config['type'])

        if ingestion_config['service'] == 'appsflyer':
            return AppsFlyerDataExtractor(ingestion_config['sec']['host'],
                                        ingestion_config['sec']['sm_token_secret_id'])

        if ingestion_config['service'] == 'galileo':
            return GalileoDataExtractor(ingestion_config['sec']['instance'],
                                        ingestion_config['sec']['project_id'],
                                        ingestion_config['sec']['sm_pwd_secret_id'],
                                        ingestion_config['sec']['sm_private_key']
                                        )

        if ingestion_config['service'] == 'ftp' and ingestion_config['instance'] == 'marketingcloud':
            return MarketingCloudFTPDataExtractor(
                                                ingestion_config['sec']['host'],
                                                ingestion_config['sec']['port'],
                                                ingestion_config['sec']['sm_user_secret_id'],
                                                ingestion_config['sec']['sm_pwd_secret_id']
                                            )
