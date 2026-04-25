SELECT substr(Cast(AVG(UserID) as varchar), 1, 14) FROM ${database}.${schema}.hits;
