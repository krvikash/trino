SELECT MobilePhoneModel, COUNT(DISTINCT UserID) AS u FROM ${database}.${schema}.hits WHERE MobilePhoneModel <> '' GROUP BY MobilePhoneModel ORDER BY u DESC LIMIT 10;
