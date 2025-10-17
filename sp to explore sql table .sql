ALTER PROCEDURE uspExploreTable  @schemaName nvarchar(max), @tableName nvarchar(max)
-- =============================================
-- Author: Lukasz Balcerzak
-- Create date: 2023-11-20
-- Description: This stored procedure returns basic diagnostic information about values in a table like count, nulls, distinct, max, min.
-- =============================================
AS 
BEGIN

	DECLARE @sqlQuery NVARCHAR(MAX) = '';

	-- build your query
	SELECT @sqlQuery += 'SELECT ''' + COLUMN_NAME + ''' AS ColumnName, 
						COUNT(1) AS AllRows,
						COUNT(1) - COUNT(' + COLUMN_NAME + ') as Nulls,
						COUNT(' + COLUMN_NAME + ') AS ColumnCount, 
						COUNT(DISTINCT ' + COLUMN_NAME + ') AS DistinctCount,
						IIF(COUNT(' + COLUMN_NAME + ') = COUNT(DISTINCT ' + COLUMN_NAME + '), 1,0) AS UniqueRows,
						CONVERT(NVARCHAR, MIN(' + COLUMN_NAME + ')) AS MinColumn,
						CONVERT(NVARCHAR, MAX(' + COLUMN_NAME + ')) AS MaxColumn
						FROM ' + @schemaName + '.' + @tableName + ' UNION ALL '
	FROM INFORMATION_SCHEMA.COLUMNS
	WHERE
	TABLE_SCHEMA = @schemaName
	and TABLE_NAME = @tableName;


	-- Remove the trailing 'UNION ALL'
	SET @sqlQuery = LEFT(@sqlQuery, LEN(@sqlQuery) - 10);

	-- Execute the dynamic SQL
	EXEC sp_executesql @sqlQuery;

END;