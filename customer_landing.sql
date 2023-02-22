CREATE EXTERNAL TABLE IF NOT EXISTS `kerry-stedi-lakehouse`.`customer_landing` (
  `serialnumber` string,
  `sharewithpublicasofdate` bigint,
  `birthday` date,
  `registrationdate` bigint,
  `sharewithresearchasofdate` bigint,
  `customername` string,
  `email` string,
  `lastupdatedate` bigint,
  `phone` string,
  `sharewithfriendsasofdate` bigint
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'ignore.malformed.json' = 'FALSE',
  'dots.in.keys' = 'FALSE',
  'case.insensitive' = 'TRUE',
  'mapping' = 'TRUE'
)
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://kerry-pj3-stedi-lakehouse/customer/landing/'
TBLPROPERTIES ('classification' = 'json');