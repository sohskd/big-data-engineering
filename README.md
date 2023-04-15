# Data Ingestion

1. Go to ```https://console.cloud.google.com/storage/browser/mock_raw_ebd_2023/des_raw_csv?project=ebd2023&pageState=(%22StorageObjectListTable%22:(%22f%22:%22%255B%255D%22))&prefix=&forceOnObjectsSortingFiltering=false``` and upload files in raw zone.
2. run ```gcloud auth application-default login ```, spring should read the ```application_default_credentials.json``` automatically.
3. Start spring server.
4. file should be ingested in ```https://console.cloud.google.com/storage/browser/zone_landing_ebd_2023/des_raw_csv?pageState=(%22StorageObjectListTable%22:(%22f%22:%22%255B%255D%22))&project=ebd2023&prefix=&forceOnObjectsSortingFiltering=false```


# Executing application require the following args as Spark support with JDK 17 and Springboot is not mature

mvn spring-boot:run -Dspring-boot.run.jvmArguments="--add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/sun.nio.cs=ALL-UNNAMED --add-opens=java.base/sun.security.action=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED --add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED"