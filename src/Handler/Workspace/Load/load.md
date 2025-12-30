| ID | LOAD TYPE | TYPED SRC TABLE | DATA CASTING | COLUMN NAME MAPPING | PK SET |
|----|-----------|-----------------|--------------|---------------------|--------|
|  1 | INC       | NO              | YES          | YES                 | YES    | STAGING + INC IMPORTER
|  2 | INC       | NO              | YES          | YES                 | NO     | STAGING + INC IMPORTER
|  3 | INC       | NO              | YES          | NO                  | YES    | STAGING + INC IMPORTER
|  4 | INC       | NO              | YES          | NO                  | NO     | STAGING + INC IMPORTER
|  5 | INC       | NO              | NO           | YES                 | YES    | STAGING + INC IMPORTER
|  6 | INC       | NO              | NO           | YES                 | NO     | STAGING + INC IMPORTER
|  7 | INC       | NO              | NO           | NO                  | YES    | STAGING + INC IMPORTER
|  8 | INC       | NO              | NO           | NO                  | NO     | STAGING + INC IMPORTER
|  9 | INC       | YES             | YES          | YES                 | YES    | X NOT USECASE - cannot rename column when typed source
| 10 | INC       | YES             | YES          | YES                 | NO     | X NOT USECASE - cannot rename column when typed source
| 11 | INC       | YES             | YES          | NO                  | YES    | X NOT USECASE - columns have to identical when typed source
| 12 | INC       | YES             | YES          | NO                  | NO     | X NOT USECASE - columns have to identical when typed source
| 13 | INC       | YES             | NO           | YES                 | YES    | X NOT USECASE - cannot rename column when typed source
| 14 | INC       | YES             | NO           | YES                 | NO     | X NOT USECASE - cannot rename column when typed source
| 15 | INC       | YES             | NO           | NO                  | YES    | STAGING + INC IMPORTER
| 16 | INC       | YES             | NO           | NO                  | NO     | STAGING + INC IMPORTER
| 17 | FULL      | NO              | YES          | YES                 | YES    | STAGING + FULL IMPORTER
| 18 | FULL      | NO              | YES          | YES                 | NO     | STAGING + FULL IMPORTER
| 19 | FULL      | NO              | YES          | NO                  | YES    | FULL IMPORTER - no need for data mapping but casting yes
| 20 | FULL      | NO              | YES          | NO                  | NO     | FULL IMPORTER - no need for data mapping but casting yes
| 21 | FULL      | NO              | NO           | YES                 | YES    | STAGING IMPORTER - no need for data casting, but mapping yes
| 22 | FULL      | NO              | NO           | YES                 | NO     | STAGING IMPORTER - no need for data casting, but mapping yes
| 23 | FULL      | NO              | NO           | NO                  | YES    | FULL IMPORTER
| 24 | FULL      | NO              | NO           | NO                  | NO     | FULL IMPORTER
| 25 | FULL      | YES             | YES          | YES                 | YES    | X NOT USECASE - cannot rename column when typed source
| 26 | FULL      | YES             | YES          | YES                 | NO     | X NOT USECASE - cannot rename column when typed source
| 27 | FULL      | YES             | YES          | NO                  | YES    | X NOT USECASE - columns have to identical when typed source
| 28 | FULL      | YES             | YES          | NO                  | NO     | X NOT USECASE - columns have to identical when typed source
| 29 | FULL      | YES             | NO           | YES                 | YES    | X NOT USECASE - cannot rename column when typed source
| 30 | FULL      | YES             | NO           | YES                 | NO     | X NOT USECASE - cannot rename column when typed source
| 31 | FULL      | YES             | NO           | NO                  | YES    | FULL IMPORTER
| 32 | FULL      | YES             | NO           | NO                  | NO     | FULL IMPORTER


notes: 
Staging - enables column name mapping
Full importer - enables data casting

DEDUP TYPE is not important because it is being set by PKs in Connection
PK SET = DEDUP TYPE so it means TRUE = NO  ; FALSE = YES
timestamp - with COPY operation in connection, it is not possible to create table with _timestamp in WS. It should be possible with `clone`
