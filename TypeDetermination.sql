-- =============================================
-- Automatic T-SQL Type Detection and Migration Generator
-- =============================================
-- This script analyzes VARCHAR columns and determines optimal data types
-- Outputs: CREATE TABLE statement OR column mapping tuples for migration
-- =============================================

SET NOCOUNT ON;

-- ============= CONFIGURATION =============
DECLARE @SchemaName SYSNAME = 'Data';
DECLARE @TableName SYSNAME = 'Table';
DECLARE @SampleSize INT = 100000;  -- Number of rows to sample (NULL = all rows)
DECLARE @OutputMode VARCHAR(20) = 'MAPPINGS';  -- 'CREATE_TABLE' or 'MAPPINGS'
DECLARE @IncludeNonVarcharColumns BIT = 1;  -- Include columns already properly typed
DECLARE @MinDecimalPrecision INT = 10;  -- Minimum precision for decimals
DECLARE @MaxDecimalPrecision INT = 18;  -- Maximum precision for decimals
DECLARE @DateFormats TABLE (FormatName VARCHAR(50));  -- Add custom date formats if needed

-- ============= RESULTS TABLE =============
DECLARE @ColumnAnalysis TABLE (
    ColumnName SYSNAME,
    CurrentType VARCHAR(100),
    SampleCount INT,
    NullCount INT,
    EmptyCount INT,
    NonEmptyCount INT,
    IsNumeric BIT,
    IsInteger BIT,
    IsBit BIT,
    IsDate BIT,
    IsDateTime BIT,
    MinLength INT,
    MaxLength INT,
    MinValue DECIMAL(38,10),
    MaxValue DECIMAL(38,10),
    DecimalPlaces INT,
    RecommendedType VARCHAR(100),
    RecommendedPrecision INT,
    RecommendedScale INT,
    Reasoning VARCHAR(500)
);

-- ============= GET COLUMN LIST =============
DECLARE @Columns TABLE (
    ColumnName SYSNAME,
    DataType VARCHAR(50),
    MaxLength INT,
    IsVarchar BIT
);

INSERT INTO @Columns
SELECT 
    c.name,
    t.name,
    c.max_length,
    CASE WHEN t.name IN ('varchar', 'nvarchar', 'char', 'nchar', 'text', 'ntext') THEN 1 ELSE 0 END
FROM sys.columns c
JOIN sys.types t ON c.user_type_id = t.user_type_id
JOIN sys.tables tbl ON c.object_id = tbl.object_id
JOIN sys.schemas s ON tbl.schema_id = s.schema_id
WHERE s.name = @SchemaName 
    AND tbl.name = @TableName
    AND (@IncludeNonVarcharColumns = 1 OR t.name IN ('varchar', 'nvarchar', 'char', 'nchar', 'text', 'ntext'));

-- ============= ANALYZE EACH COLUMN =============
DECLARE @CurrentColumn SYSNAME;
DECLARE @CurrentDataType VARCHAR(50);
DECLARE @IsVarchar BIT;
DECLARE @SQL NVARCHAR(MAX);

DECLARE column_cursor CURSOR FOR 
SELECT ColumnName, DataType, IsVarchar FROM @Columns;

OPEN column_cursor;
FETCH NEXT FROM column_cursor INTO @CurrentColumn, @CurrentDataType, @IsVarchar;

WHILE @@FETCH_STATUS = 0
BEGIN
    -- Skip analysis for non-varchar if they're already properly typed
    IF @IsVarchar = 0 AND @IncludeNonVarcharColumns = 1
    BEGIN
        INSERT INTO @ColumnAnalysis (ColumnName, CurrentType, RecommendedType, Reasoning)
        VALUES (@CurrentColumn, @CurrentDataType, @CurrentDataType, 'Already properly typed');
        
        FETCH NEXT FROM column_cursor INTO @CurrentColumn, @CurrentDataType, @IsVarchar;
        CONTINUE;
    END

    -- Build dynamic SQL to analyze the column
    SET @SQL = N'
    DECLARE @SampleCount INT,
            @NullCount INT,
            @EmptyCount INT,
            @NonEmptyCount INT,
            @IsNumeric BIT = 1,
            @IsInteger BIT = 1,
            @IsBit BIT = 1,
            @IsDate BIT = 1,
            @IsDateTime BIT = 1,
            @MinLength INT,
            @MaxLength INT,
            @MinValue DECIMAL(38,10),
            @MaxValue DECIMAL(38,10),
            @DecimalPlaces INT = 0;

    -- Sample the data
    SELECT ' + 
    CASE WHEN @SampleSize IS NULL THEN '' 
         ELSE 'TOP (' + CAST(@SampleSize AS VARCHAR) + ') ' END + '
        @SampleCount = COUNT(*),
        @NullCount = SUM(CASE WHEN [' + @CurrentColumn + '] IS NULL THEN 1 ELSE 0 END),
        @EmptyCount = SUM(CASE WHEN LTRIM(RTRIM([' + @CurrentColumn + '])) = '''' THEN 1 ELSE 0 END),
        @NonEmptyCount = SUM(CASE WHEN LTRIM(RTRIM([' + @CurrentColumn + '])) != '''' AND [' + @CurrentColumn + '] IS NOT NULL THEN 1 ELSE 0 END),
        @MinLength = MIN(LEN(LTRIM(RTRIM([' + @CurrentColumn + '])))),
        @MaxLength = MAX(LEN(LTRIM(RTRIM([' + @CurrentColumn + ']))))
    FROM [' + @SchemaName + '].[' + @TableName + '];

    -- Check if all non-empty values are numeric
    IF EXISTS (
        SELECT 1 FROM [' + @SchemaName + '].[' + @TableName + ']
        WHERE LTRIM(RTRIM([' + @CurrentColumn + '])) != ''''
            AND [' + @CurrentColumn + '] IS NOT NULL
            AND ISNUMERIC(LTRIM(RTRIM([' + @CurrentColumn + ']))) = 0
    )
        SET @IsNumeric = 0;

    -- If numeric, check if integer
    IF @IsNumeric = 1 AND EXISTS (
        SELECT 1 FROM [' + @SchemaName + '].[' + @TableName + ']
        WHERE LTRIM(RTRIM([' + @CurrentColumn + '])) != ''''
            AND [' + @CurrentColumn + '] IS NOT NULL
            AND LTRIM(RTRIM([' + @CurrentColumn + '])) LIKE ''%.%''
    )
        SET @IsInteger = 0;

    -- If numeric, get min/max values
    IF @IsNumeric = 1
    BEGIN
        SELECT 
            @MinValue = MIN(TRY_CONVERT(DECIMAL(38,10), LTRIM(RTRIM([' + @CurrentColumn + '])))),
            @MaxValue = MAX(TRY_CONVERT(DECIMAL(38,10), LTRIM(RTRIM([' + @CurrentColumn + '])))),
            @DecimalPlaces = MAX(
                CASE 
                    WHEN CHARINDEX(''.'', LTRIM(RTRIM([' + @CurrentColumn + ']))) > 0 
                    THEN LEN(LTRIM(RTRIM([' + @CurrentColumn + ']))) - CHARINDEX(''.'', LTRIM(RTRIM([' + @CurrentColumn + '])))
                    ELSE 0 
                END
            )
        FROM [' + @SchemaName + '].[' + @TableName + ']
        WHERE LTRIM(RTRIM([' + @CurrentColumn + '])) != ''''
            AND [' + @CurrentColumn + '] IS NOT NULL;
    END
    ELSE
    BEGIN
        SET @IsInteger = 0;
    END

    -- Check if values are only 0 or 1 (BIT)
    IF @IsInteger = 1 AND EXISTS (
        SELECT 1 FROM [' + @SchemaName + '].[' + @TableName + ']
        WHERE LTRIM(RTRIM([' + @CurrentColumn + '])) != ''''
            AND [' + @CurrentColumn + '] IS NOT NULL
            AND LTRIM(RTRIM([' + @CurrentColumn + '])) NOT IN (''0'', ''1'')
    )
        SET @IsBit = 0;

    -- Check if all non-empty values are dates
    IF EXISTS (
        SELECT 1 FROM [' + @SchemaName + '].[' + @TableName + ']
        WHERE LTRIM(RTRIM([' + @CurrentColumn + '])) != ''''
            AND [' + @CurrentColumn + '] IS NOT NULL
            AND TRY_CONVERT(DATE, LTRIM(RTRIM([' + @CurrentColumn + ']))) IS NULL
    )
        SET @IsDate = 0;

    -- Check if any dates have time components
    IF @IsDate = 1 AND EXISTS (
        SELECT 1 FROM [' + @SchemaName + '].[' + @TableName + ']
        WHERE LTRIM(RTRIM([' + @CurrentColumn + '])) != ''''
            AND [' + @CurrentColumn + '] IS NOT NULL
            AND CONVERT(VARCHAR, TRY_CONVERT(DATETIME, LTRIM(RTRIM([' + @CurrentColumn + ']))), 108) != ''00:00:00''
    )
        SET @IsDateTime = 1;

    -- Determine recommended type
    DECLARE @RecommendedType VARCHAR(100),
            @RecommendedPrecision INT,
            @RecommendedScale INT,
            @Reasoning VARCHAR(500);

    -- Type determination logic
    IF @NonEmptyCount = 0
    BEGIN
        SET @RecommendedType = ''VARCHAR'';
        SET @RecommendedPrecision = 50;
        SET @Reasoning = ''All values are NULL or empty'';
    END
    ELSE IF @IsDate = 1
    BEGIN
        IF @IsDateTime = 1
        BEGIN
            SET @RecommendedType = ''DATETIME2'';
            SET @RecommendedScale = 0;
            SET @Reasoning = ''Contains valid dates with time components'';
        END
        ELSE
        BEGIN
            SET @RecommendedType = ''DATE'';
            SET @Reasoning = ''Contains valid dates without time'';
        END
    END
    ELSE IF @IsBit = 1
    BEGIN
        SET @RecommendedType = ''BIT'';
        SET @Reasoning = ''Only contains 0 and 1 values'';
    END
    ELSE IF @IsInteger = 1
    BEGIN
        IF @MinValue >= -128 AND @MaxValue <= 127
        BEGIN
            SET @RecommendedType = ''TINYINT'';
            SET @Reasoning = ''Integer range: '' + CAST(@MinValue AS VARCHAR) + '' to '' + CAST(@MaxValue AS VARCHAR);
        END
        ELSE IF @MinValue >= -32768 AND @MaxValue <= 32767
        BEGIN
            SET @RecommendedType = ''SMALLINT'';
            SET @Reasoning = ''Integer range: '' + CAST(@MinValue AS VARCHAR) + '' to '' + CAST(@MaxValue AS VARCHAR);
        END
        ELSE IF @MinValue >= -2147483648 AND @MaxValue <= 2147483647
        BEGIN
            SET @RecommendedType = ''INT'';
            SET @Reasoning = ''Integer range: '' + CAST(@MinValue AS VARCHAR) + '' to '' + CAST(@MaxValue AS VARCHAR);
        END
        ELSE
        BEGIN
            SET @RecommendedType = ''BIGINT'';
            SET @Reasoning = ''Integer range: '' + CAST(@MinValue AS VARCHAR) + '' to '' + CAST(@MaxValue AS VARCHAR);
        END
    END
    ELSE IF @IsNumeric = 1
    BEGIN
        DECLARE @IntDigits INT = LEN(CAST(CAST(ABS(@MaxValue) AS BIGINT) AS VARCHAR));
        DECLARE @TotalPrecision INT = @IntDigits + @DecimalPlaces;
        
        IF @TotalPrecision < ' + CAST(@MinDecimalPrecision AS VARCHAR) + '
            SET @TotalPrecision = ' + CAST(@MinDecimalPrecision AS VARCHAR) + ';
        IF @TotalPrecision > ' + CAST(@MaxDecimalPrecision AS VARCHAR) + '
            SET @TotalPrecision = ' + CAST(@MaxDecimalPrecision AS VARCHAR) + ';
            
        SET @RecommendedType = ''DECIMAL'';
        SET @RecommendedPrecision = @TotalPrecision;
        SET @RecommendedScale = CASE WHEN @DecimalPlaces > 10 THEN 10 ELSE @DecimalPlaces END;
        SET @Reasoning = ''Numeric with '' + CAST(@DecimalPlaces AS VARCHAR) + '' decimal places, range: '' + 
                         CAST(@MinValue AS VARCHAR) + '' to '' + CAST(@MaxValue AS VARCHAR);
    END
    ELSE
    BEGIN
        -- Keep as VARCHAR but optimize length
        SET @RecommendedType = ''VARCHAR'';
        SET @RecommendedPrecision = CASE 
            WHEN @MaxLength <= 10 THEN 10
            WHEN @MaxLength <= 50 THEN 50
            WHEN @MaxLength <= 100 THEN 100
            WHEN @MaxLength <= 255 THEN 255
            WHEN @MaxLength <= 500 THEN 500
            ELSE 1000
        END;
        SET @Reasoning = ''Non-numeric text, max length: '' + CAST(@MaxLength AS VARCHAR);
    END

    -- Insert results
    INSERT INTO @ColumnAnalysis (
        ColumnName, CurrentType, SampleCount, NullCount, EmptyCount, NonEmptyCount,
        IsNumeric, IsInteger, IsBit, IsDate, IsDateTime,
        MinLength, MaxLength, MinValue, MaxValue, DecimalPlaces,
        RecommendedType, RecommendedPrecision, RecommendedScale, Reasoning
    )
    VALUES (
        ''' + @CurrentColumn + ''', ''' + @CurrentDataType + ''', @SampleCount, @NullCount, @EmptyCount, @NonEmptyCount,
        @IsNumeric, @IsInteger, @IsBit, @IsDate, @IsDateTime,
        @MinLength, @MaxLength, @MinValue, @MaxValue, @DecimalPlaces,
        @RecommendedType, @RecommendedPrecision, @RecommendedScale, @Reasoning
    );';

    -- Execute the analysis
    BEGIN TRY
        EXEC sp_executesql @SQL;
    END TRY
    BEGIN CATCH
        INSERT INTO @ColumnAnalysis (ColumnName, CurrentType, RecommendedType, Reasoning)
        VALUES (@CurrentColumn, @CurrentDataType, 'ERROR', 'Analysis failed: ' + ERROR_MESSAGE());
    END CATCH

    FETCH NEXT FROM column_cursor INTO @CurrentColumn, @CurrentDataType, @IsVarchar;
END

CLOSE column_cursor;
DEALLOCATE column_cursor;

-- ============= OUTPUT RESULTS =============
PRINT '-- ============================================='
PRINT '-- Type Detection Analysis'
PRINT '-- Schema: ' + @SchemaName
PRINT '-- Table: ' + @TableName
PRINT '-- Sample Size: ' + ISNULL(CAST(@SampleSize AS VARCHAR), 'ALL ROWS')
PRINT '-- ============================================='
PRINT ''

-- Display analysis summary
SELECT 
    ColumnName,
    CurrentType,
    RecommendedType + 
    CASE 
        WHEN RecommendedType = 'DECIMAL' THEN '(' + CAST(RecommendedPrecision AS VARCHAR) + ',' + CAST(RecommendedScale AS VARCHAR) + ')'
        WHEN RecommendedType = 'VARCHAR' THEN '(' + CAST(RecommendedPrecision AS VARCHAR) + ')'
        ELSE ''
    END AS RecommendedType,
    NonEmptyCount,
    Reasoning
FROM @ColumnAnalysis
ORDER BY ColumnName;

PRINT ''
PRINT '-- ============================================='
PRINT '-- Generated Output (' + @OutputMode + ')'
PRINT '-- ============================================='
PRINT ''

IF @OutputMode = 'CREATE_TABLE'
BEGIN
    -- Generate CREATE TABLE statement
    DECLARE @CreateTableSQL NVARCHAR(MAX) = 'CREATE TABLE [' + @SchemaName + '].[t_' + @TableName + '] (' + CHAR(13);
    
    SELECT @CreateTableSQL = @CreateTableSQL + '    [' + ColumnName + '] ' + 
        RecommendedType + 
        CASE 
            WHEN RecommendedType = 'DECIMAL' THEN '(' + CAST(RecommendedPrecision AS VARCHAR) + ',' + CAST(RecommendedScale AS VARCHAR) + ')'
            WHEN RecommendedType = 'VARCHAR' THEN '(' + CAST(RecommendedPrecision AS VARCHAR) + ')'
            WHEN RecommendedType = 'DATETIME2' THEN '(' + CAST(ISNULL(RecommendedScale, 0) AS VARCHAR) + ')'
            ELSE ''
        END + 
        ' NULL,' + CHAR(13)
    FROM @ColumnAnalysis
    ORDER BY ColumnName;
    
    SET @CreateTableSQL = LEFT(@CreateTableSQL, LEN(@CreateTableSQL) - 2) + CHAR(13) + ');';
    
    PRINT @CreateTableSQL;
END
ELSE IF @OutputMode = 'MAPPINGS'
BEGIN
    -- Generate INSERT statements for column mappings
    PRINT 'INSERT INTO @ColumnMappings (ColumnName, TargetType, Precision, Scale) VALUES'
    
    DECLARE @MappingSQL NVARCHAR(MAX) = '';
    
    SELECT @MappingSQL = @MappingSQL + 
        '    (''' + ColumnName + ''', ''' + RecommendedType + ''', ' + 
        ISNULL(CAST(RecommendedPrecision AS VARCHAR), 'NULL') + ', ' + 
        ISNULL(CAST(RecommendedScale AS VARCHAR), 'NULL') + '),' + CHAR(13)
    FROM @ColumnAnalysis
    ORDER BY ColumnName;
    
    SET @MappingSQL = LEFT(@MappingSQL, LEN(@MappingSQL) - 2) + ';';
    
    PRINT @MappingSQL;
END

PRINT ''
PRINT '-- ============================================='
PRINT '-- Analysis Complete'
PRINT '-- Total Columns: ' + CAST((SELECT COUNT(*) FROM @ColumnAnalysis) AS VARCHAR)
PRINT '-- Columns Needing Conversion: ' + CAST((SELECT COUNT(*) FROM @ColumnAnalysis WHERE CurrentType != RecommendedType OR (RecommendedType = CurrentType AND CurrentType LIKE '%VARCHAR%')) AS VARCHAR)
PRINT '-- ============================================='
