# Redshift Connector

# External tables support setup

By impersonating as `arn:aws:iam::888469412714:role/role_for_lake_formation_admin` in Lake Formation, 
the role `arn:aws:iam::888469412714:role/redshift-cicd` was given permissions ot create databases.



Add to the role `arn:aws:iam::888469412714:role/redshift-cicd`

https://us-east-1.console.aws.amazon.com/iam/home?region=us-east-2#/roles/details/redshift-cicd?section=permissions

the following inline permissions



`manage-redshift-external-tables`

```
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "lakeformation:GetDataAccess",
                "glue:CreateDatabase",
                "glue:DeleteDatabase",
                "glue:GetDatabase",
                "glue:GetDatabases",
                "glue:CreateTable",
                "glue:UpdateTable",
                "glue:GetTable",
                "glue:GetTables",
                "glue:GetPartition",
                "glue:GetPartitions",
                "glue:CreatePartition"
            ],
            "Resource": "*"
        }
    ]
}
```

In order for Redshift to interact with Hive & Iceberg tables over the Glue Data Catalog governed 
by Lake Formation, we're making use of the user

```
arn:aws:iam::888469412714:user/redshift-assume-role-connector-test-user
```

which can assume the role

`arn:aws:iam::888469412714:role/redshift-cicd`

A corresponding trust relationship has been added on `arn:aws:iam::888469412714:role/redshift-cicd` role

```
        {
            "Effect": "Allow",
            "Principal": {
                "AWS": "arn:aws:iam::888469412714:user/redshift-assume-role-connector-test-user"
            },
            "Action": "sts:AssumeRole"
        }
```
