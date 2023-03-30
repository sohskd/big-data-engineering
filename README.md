# Data Ingestion

1. Go to ```https://console.cloud.google.com/storage/browser/mock_raw_ebd_2023/des_raw_csv?project=ebd2023&pageState=(%22StorageObjectListTable%22:(%22f%22:%22%255B%255D%22))&prefix=&forceOnObjectsSortingFiltering=false``` and upload files in raw zone.
2. run ```gcloud auth application-default login ```, find ```application_default_credentials.json``` in your local and paste under resources folder as ```gcp-account-file.json```
3. Start spring server.
4. file should be ingested in ```https://console.cloud.google.com/storage/browser/zone_landing_ebd_2023/des_raw_csv?pageState=(%22StorageObjectListTable%22:(%22f%22:%22%255B%255D%22))&project=ebd2023&prefix=&forceOnObjectsSortingFiltering=false```
5. create sub in https://console.cloud.google.com/cloudpubsub/topic/detail/test-topic?project=ebd2023
