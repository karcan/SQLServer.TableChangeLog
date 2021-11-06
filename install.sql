CREATE TABLE dbo.TestTable
(
	ID				INT					IDENTITY(1,1)	PRIMARY KEY		NOT NULL,
	test_int		INT					NULL,
	test_bigint		BIGINT				NULL,
	test_smallint	SMALLINT			NULL,
	test_tinyint	TINYINT				NULL,
	test_bit		BIT					NULL,
	test_date		DATE				NULL,
	test_datetime	DATETIME			NULL,
	test_datetime2	DATETIME2(7)		NULL,
	test_time		TIME(7)				NULL,
	test_decimal	DECIMAL(18, 4)		NULL,
	test_numeric	DECIMAL(18, 4)		NULL,
	test_float		FLOAT				NULL,
	test_guid		UNIQUEIDENTIFIER	NULL
)
GO
CREATE TABLE [dbo].[ChangeLog]
(
	[ID]			BIGINT IDENTITY(1,1) NOT NULL,
	[SchemaName]	NVARCHAR(128) NULL,
	[ObjectName]	NVARCHAR(128) NULL,
	[ActionType]	VARCHAR(6) NULL,
	[RowID]			BIGINT NULL,
	[CreateDate]	DATETIME NULL,
	[CreateUserID]	SMALLINT NULL, 
    CONSTRAINT [PK_ChangeLog] PRIMARY KEY ([ID]),
)

GO
CREATE TABLE [dbo].[ChangeLogRawData]
(
	[ChangeLogID]	BIGINT NULL,
	[Name]			NVARCHAR(128) NULL,
	[Type]			NVARCHAR(128) NULL,
	[Length]		SMALLINT NULL,
	[DeletedValue]	NVARCHAR(MAX),
	[InsertedValue]	NVARCHAR(MAX)
)

GO
CREATE FUNCTION [dbo].[FN_DynamicColumnConvertSQL]
(
	@pColumn		VARCHAR(128),
	@pColumnType	TINYINT
)
RETURNS NVARCHAR(MAX)
AS
BEGIN
	DECLARE 
		@vResult NVARCHAR(MAX);

	SET @vResult =	CASE
						-- datetime
						WHEN @pColumnType = 61
							THEN 'FORMAT('+ @pColumn +', ''yyyy-MM-dd hh:mm:ss.fff'')'
						-- datetime2
						WHEN @pColumnType = 42
							THEN 'FORMAT('+ @pColumn +', ''yyyy-MM-dd hh:mm:ss.fffffff'')'
						-- date
						WHEN @pColumnType = 40
							THEN 'FORMAT('+ @pColumn +', ''yyyy-MM-dd'')'
						-- time
						WHEN @pColumnType = 41
							THEN 'CONVERT(VARCHAR, ' + @pColumn + ', 14)'
						-- bit
						WHEN @pColumnType = 104
							THEN 'CASE ' +@pColumn + ' WHEN 1 THEN ''true'' WHEN 0 THEN ''false'' END '
						-- tinyint & smallint & int & bigint
						WHEN @pColumnType IN (48,52,56,127)
							THEN @pColumn
						-- float
						WHEN @pColumnType = 62
							THEN 'CONVERT(VARCHAR,' + @pColumn + ', 128)'
						-- numeric & decimal
						WHEN @pColumnType IN (106,108)
							THEN @pColumn
						-- uniqueidentifier
						WHEN @pColumnType IN (36)
							THEN 'CAST(' + @pColumn + ' AS VARCHAR(36))'
						ELSE @pColumn
					END


	SET @vResult = 'CAST(' + @vResult + ' AS VARCHAR) AS ' + @pColumn
	RETURN @vResult
END
GO
CREATE PROCEDURE [dbo].[SP_TableTriggerChangeLogInsert]			@pObjectID		INT,
																@pObjectName	VARCHAR(128),
																@pSchemaID		INT,
																@pSchemaName	VARCHAR(128),
																@pID			NVARCHAR(MAX),
																@pActionType	TINYINT
AS
BEGIN

	SET NOCOUNT ON;

	DECLARE	@vSQL			NVARCHAR(MAX),
			@vData			NVARCHAR(MAX),
			@vRow			NVARCHAR(MAX),
			@vChanges		NVARCHAR(MAX)

	DECLARE @vColumnChanges TABLE (
				_IDName		NVARCHAR(128),
				_IDValue	NVARCHAR(16),
				ColumnID INT, 
				ColumnName VARCHAR(128), 
				TypeID TINYINT, 
				TypeName VARCHAR(128), 
				Length SMALLINT, 
				DeletedValue NVARCHAR(MAX), 
				InsertedValue NVARCHAR(MAX)
			);

	DECLARE @vChangeLog TABLE (
		ID		BIGINT,
		RowID	BIGINT
	)

	-- Get Columns with Changes
	BEGIN
		SET @vSQL = 
		CONCAT(
			'
			WITH CTE_Rows AS (
				SELECT ', @pID, ' FROM #vInserted
				UNION
				SELECT ', @pID, ' FROM #vDeleted
			)
			SELECT
				''', @pID, ''' AS _IDName
				,cr.', @pID, ' AS _IDValue
				,c.ColumnID
				,c.ColumnName
				,c.TypeID
				,c.TypeName
				,c.Length
				,d.DeletedValue
				,i.InsertedValue
			FROM 
				CTE_Rows AS cr
				CROSS JOIN (SELECT * FROM #vColumn) AS c
				OUTER APPLY (
								SELECT
									pvt.InsertedName,
									pvt.InsertedValue
								FROM
									(
										SELECT
											'+ (SELECT STRING_AGG(dbo.FN_DynamicColumnConvertSQL(ColumnName, TypeID),',') FROM #vColumn) + '
										FROM
											#vInserted AS i
										WHERE
											i.', @pID, ' = cr.', @pID, '
									) AS i
								UNPIVOT(
										InsertedValue
										FOR InsertedName IN ('+ (SELECT STRING_AGG(ColumnName,',') FROM #vColumn) + ')
									) AS pvt
								WHERE
									pvt.InsertedName = c.ColumnName
							) AS i
				OUTER APPLY (
								SELECT
									pvt.DeletedName,
									pvt.DeletedValue
								FROM
									(
										SELECT
											'+ (SELECT STRING_AGG(dbo.FN_DynamicColumnConvertSQL(ColumnName, TypeID),',') FROM #vColumn) + '
										FROM
											#vDeleted AS d
										WHERE
											d.', @pID, ' = cr.', @pID, '
									) AS d
								UNPIVOT(
										DeletedValue
										FOR DeletedName IN ('+ (SELECT STRING_AGG(ColumnName,',') FROM #vColumn) + ')
									) AS pvt
								WHERE
									pvt.DeletedName = c.ColumnName
							) AS d
				WHERE 
					ISNULL(i.InsertedValue,'''') != ISNULL(d.DeletedValue,'''')
					OR c.ColumnName IN (''ModifyUserID'', ''CreateUserID'')
				ORDER BY
					_IDValue, c.ColumnID
		'
		)

		INSERT INTO @vColumnChanges
		EXEC sp_executesql @vSQL
	END

	-- if action type is insert and no changes then break;
	IF	@pActionType = 1
		AND NOT EXISTS (SELECT TOP(1) 1 FROM @vColumnChanges WHERE ColumnName != @pID)
		RETURN;

	-- Insert Change Log With/Without Bulk
	BEGIN
		INSERT INTO dbo.ChangeLog(SchemaName,ObjectName, ActionType, RowID, CreateDate,CreateUserID)
		OUTPUT 
			inserted.ID, 
			inserted.RowID 
		INTO 
			@vChangeLog
		SELECT 
			SchemaName		= @pSchemaName, 
			ObjectName		= @pObjectName, 
			ActionType		= CASE @pActionType WHEN 1 THEN 'Update' WHEN 2 THEN 'Insert' WHEN 3 THEN 'Delete' END,
			RowID			= _IDValue, 
			CreateDate		= GETDATE(), 
			CreateUserID	= NULL
		FROM 
			@vColumnChanges AS vc
		WHERE
			@pActionType IN (2,3)
			OR (@pActionType = 1 AND vc.ColumnName != @pID )
		GROUP BY 
			_IDValue
	END

	-- Insert Change Log Raw Data With/Without Bulk
	BEGIN
		INSERT INTO dbo.ChangeLogRawData(ChangeLogID, Name, Type, Length, DeletedValue, InsertedValue)
		SELECT
			ChangeLogID		= vl.ID
			,Name			= vc.ColumnName
			,Type			= vc.TypeName
			,Length			= vc.Length
			,DeletedValue	= vc.DeletedValue
			,InsertedValue	= vc.InsertedValue
		FROM
			@vColumnChanges AS vc
			INNER JOIN @vChangeLog AS vl ON vl.RowID = vc._IDValue
		WHERE
			@pActionType IN (2,3)
			OR (@pActionType = 1 AND vc.ColumnName != @pID )
		ORDER BY 
			vl.ID, vc.ColumnID
	END
END

CREATE TRIGGER [dbo].[TR_TestTable_Log]
ON [dbo].[TestTable]
AFTER INSERT,UPDATE,DELETE
AS
BEGIN
	SET NOCOUNT ON;
	DECLARE	@pActionType	SMALLINT,
			@vObjectID		INT,
			@vObjectName	NVARCHAR(128),
			@vSchemaID		INT,
			@vSchemaName	NVARCHAR(128),
			@vID			NVARCHAR(128) = 'ID'

	-- Get action type ex: 
	BEGIN
		SET @pActionType =	CASE
								/*UPDATE*/
								WHEN
									EXISTS(SELECT * FROM INSERTED)
									AND EXISTS(SELECT * FROM DELETED)
								THEN 1
								/*INSERT*/
								WHEN
									EXISTS(SELECT * FROM INSERTED)
								THEN 2
								/*DELETE*/
								WHEN
									EXISTS(SELECT * FROM DELETED)
								THEN 3
							END
	END

	-- if Action Type is unknown then finish trigger.
	IF @pActionType IS NULL
		RETURN;
	
	-- Get Object Detail
	BEGIN
		SELECT
			@vObjectID		= parent_id,
			@vObjectName	= stbl.name,
			@vSchemaID		= ssch.schema_id,
			@vSchemaName	= ssch.name
		FROM
			sys.triggers AS strg
			INNER JOIN sys.tables stbl ON stbl.object_id = strg.parent_id
			INNER JOIN sys.schemas AS ssch ON ssch.schema_id = stbl.schema_id
		WHERE
			strg.object_id = @@PROCID
	END

	-- Get Updated Columns
	BEGIN
		SELECT
			column_id AS ColumnID,
			sc.name AS ColumnName,
			sty.system_type_id AS TypeID,
			sty.name AS TypeName,
			sc.max_length AS Length
		INTO
			#vColumn
		FROM
			sys.columns AS sc
			INNER JOIN sys.types AS sty ON sty.user_type_id = sc.user_type_id
		WHERE
			object_id = @vObjectID
			AND (
					(SUBSTRING(COLUMNS_UPDATED(), (column_id - 1) / 8 + 1, 1)) & POWER(2, (column_id - 1) % 8) = POWER(2, (column_id - 1) % 8)
					OR (
						@pActionType = 3
						OR sc.name in (SELECT * FROM STRING_SPLIT(@vID,','))
						)
				)
			
	END
	
	-- Keep Inserted and Deleted Data in Temp Tables	
	SELECT *, ROW_NUMBER() OVER(ORDER BY (SELECT NULL)) AS _Order INTO #vInserted FROM INSERTED
	SELECT *, ROW_NUMBER() OVER(ORDER BY (SELECT NULL)) AS _Order INTO #vDeleted FROM DELETED
	
	EXEC [dbo].[SP_TableTriggerChangeLogInsert]		@pObjectID		= @vObjectID,
															@pObjectName	= @vObjectName,
															@pSchemaID		= @vSchemaID,
															@pSchemaName	= @vSchemaName,
															@pID			= @vID,
															@pActionType	= @pActionType
END
GO
