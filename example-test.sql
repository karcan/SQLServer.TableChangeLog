-- INSERT EXAMPLE
INSERT INTO TestTable (test_bigint, test_bit, test_date, test_datetime, test_datetime2, test_decimal, test_float, 
						test_guid, test_int, test_numeric, test_smallint, test_time, test_tinyint)
SELECT 
	 test_bigint		= ABS(CHECKSUM(NEWID()) % 9000000000000000)
	,test_bit		= CRYPT_GEN_RANDOM(1) % 2
	,test_date		= GETDATE()
	,test_datetime		= GETDATE()
	,test_datetime2		= GETDATE()
	,test_decimal		= ROUND(RAND(CHECKSUM(NEWID())) * (2100000000), 4)
	,test_float		= RAND(CHECKSUM(NEWID())) * (2100000000)
	,test_guid		= NEWID()
	,test_int		= ABS(CHECKSUM(NEWID()) % 2100000000)
	,test_numeric		= ROUND(RAND(CHECKSUM(NEWID())) * (2100000000), 4)
	,test_smallint		= ABS(CHECKSUM(NEWID()) % 32767)
	,test_time		= GETDATE()
	,test_tinyint		= ABS(CHECKSUM(NEWID()) % 255)


GO
-- UPDATE EXAMPLE
UPDATE 
	TestTable
	SET
		 test_bigint		= ABS(CHECKSUM(NEWID()) % 9000000000000000)
		,test_bit		= CRYPT_GEN_RANDOM(1) % 2
		,test_date		= GETDATE()
		,test_datetime		= GETDATE()
		,test_datetime2		= GETDATE()
		,test_decimal		= ROUND(RAND(CHECKSUM(NEWID())) * (2100000000), 4)
		,test_float		= RAND(CHECKSUM(NEWID())) * (2100000000)
		,test_guid		= NEWID()
		,test_int		= ABS(CHECKSUM(NEWID()) % 2100000000)
		,test_numeric		= ROUND(RAND(CHECKSUM(NEWID())) * (2100000000), 4)
		,test_smallint		= ABS(CHECKSUM(NEWID()) % 32767)
		,test_time		= GETDATE()
		,test_tinyint		= ABS(CHECKSUM(NEWID()) % 255)
WHERE
	ID = 1

GO
-- DELETE EXAMPLE
DELETE
FROM
	TestTable
WHERE
	ID = 1

SELECT * FROM ChangeLog
SELECT * FROM ChangeLogRawData
