DECLARE @SchemaName VARCHAR(50) = 'Data';
DECLARE @TableName VARCHAR(100) = 'Table';
DECLARE @BatchSize INT = 50000;

-- Define your column mappings
DECLARE @ColumnMappings TABLE (
    ColumnName VARCHAR(100),
    TargetType VARCHAR(50),
    Precision INT NULL,
    Scale INT NULL
);

INSERT INTO @ColumnMappings VALUES
    ('reportingDate', 'DATE', NULL, NULL),
    ('tenor', 'SMALLINT', NULL, NULL),
    ('deal_value', 'DECIMAL', 12, 4);
    -- Add all your columns here...

-- Generate CREATE TABLE statement
DECLARE @CreateTableSQL NVARCHAR(MAX) = 
    'DROP TABLE IF EXISTS [' + @SchemaName + '].[t_' + @TableName + '__old];' + CHAR(13) +
    'EXEC sp_rename ''[' + @SchemaName + '].[' + @TableName + ']'', ''t_' + @TableName + '__old'';' + CHAR(13) +
    'CREATE TABLE [' + @SchemaName + '].[t_' + @TableName + '] (' + CHAR(13);

SELECT @CreateTableSQL = @CreateTableSQL + '    ' + ColumnName + ' ' + 
    TargetType + 
    CASE 
        WHEN TargetType = 'DECIMAL' THEN '(' + CAST(Precision AS VARCHAR) + ',' + CAST(Scale AS VARCHAR) + ')'
        ELSE ''
    END + ',' + CHAR(13)
FROM @ColumnMappings;

SET @CreateTableSQL = LEFT(@CreateTableSQL, LEN(@CreateTableSQL) - 2) + CHAR(13) + ');';

-- Generate INSERT SELECT statement
DECLARE @SelectList NVARCHAR(MAX) = '';
SELECT @SelectList = @SelectList + '        CONVERT(' + 
    CASE 
        WHEN TargetType = 'DECIMAL' THEN TargetType + '(' + CAST(Precision AS VARCHAR) + ',' + CAST(Scale AS VARCHAR) + '), NULLIF(LTRIM(RTRIM([' + ColumnName + '])), '''')),' + CHAR(13)
        WHEN TargetType LIKE '%INT' THEN TargetType + ', CONVERT(DECIMAL(38,0)' + ', NULLIF(LTRIM(RTRIM([' + ColumnName + '])), ''''))),' + CHAR(13)
        ELSE TargetType + ', NULLIF(LTRIM(RTRIM([' + ColumnName + '])), '''')),' + CHAR(13)
    END
FROM @ColumnMappings;

SET @SelectList = LEFT(@SelectList, LEN(@SelectList) - 2);

DECLARE @ColumnList NVARCHAR(MAX);
SELECT @ColumnList = STRING_AGG(ColumnName, ', ')
FROM @ColumnMappings;

DECLARE @MigrationSQL NVARCHAR(MAX) = 
    'DECLARE @rows INT = 1;' + CHAR(13) +
    'WHILE @rows > 0' + CHAR(13) +
    'BEGIN' + CHAR(13) +
    '    INSERT INTO [' + @SchemaName + '].[t_' + @TableName + '] (' + @ColumnList + ')' + CHAR(13) +
    '    SELECT TOP (' + CAST(@BatchSize AS VARCHAR) + ')' + CHAR(13) +
    @SelectList + CHAR(13) +
    '    FROM [' + @SchemaName + '].[t_' + @TableName + '__old] WITH (READPAST);' + CHAR(13) +
    '    SET @rows = @@ROWCOUNT;' + CHAR(13) +
    'END' + CHAR(13) +
    'DROP TABLE IF EXISTS [' + @SchemaName + '].[t_' + @TableName + '__old];';

-- Print or execute
PRINT @CreateTableSQL;
PRINT @MigrationSQL;

-- Uncomment to execute:
-- EXEC sp_executesql @CreateTableSQL;
-- EXEC sp_executesql @MigrationSQL;
