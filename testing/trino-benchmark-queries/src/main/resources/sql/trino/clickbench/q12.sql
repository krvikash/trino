SELECT MobilePhone, MobilePhoneModel, COUNT(DISTINCT UserID) AS u FROM ${database}.${schema}.hits WHERE MobilePhoneModel <> '' GROUP BY MobilePhone, MobilePhoneModel ORDER BY u DESC LIMIT 10;
