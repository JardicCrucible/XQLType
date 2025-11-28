/* DATA TYPE ANALYZER & MAPPER
    ---------------------------------------------------------
    This script scans a specific table, profiles the data, 
    and generates the @ColumnMappings INSERT statements 
    for your migration script.
*/

-- 1. CONFIGURATION
DECLARE @SchemaName NVARCHAR(128) = 'Data';
DECLARE @TableName NVARCHAR(128)  = 'Table';
DECLARE @VarcharCushionPct INT    = 20; -- Add 20% to max string length found
DECLARE @MinVarcharSize INT       = 24; -- Minimum size for VARCHAR
DECLARE @AnalyzeSampleRows INT    = NULL; -- Set to NULL to scan ALL rows (Slow but accurate), or a number (e.g., 100000) for speed.

-- 2. SETUP
DECLARE @Sql NVARCHAR(MAX);
DECLARE @ColName NVARCHAR(128);
DECLARE @DataType NVARCHAR(128);
DECLARE @AnalysisTable TABLE (
    ColumnName NVARCHAR(128),
    SuggestedType NVARCHAR(50),
    SuggestedPrecision INT,
    SuggestedScale INT,
    MaxLenFound INT,
    MaxIntFound BIGINT,
    MaxDecPrecisionFound INT,
    MaxDecScaleFound INT,
    IsAllDate BIT,
    IsAllInt BIT,
    IsAllNumeric BIT
);

-- Cursor to loop through columns
DECLARE col_cursor CURSOR FOR 
SELECT c.name, t.name
FROM sys.columns c
JOIN sys.types t ON c.user_type_id = t.user_type_id
JOIN sys.tables tb ON c.object_id = tb.object_id
JOIN sys.schemas s ON tb.schema_id = s.schema_id
WHERE s.name = @SchemaName 
  AND tb.name = @TableName
  AND t.name NOT IN ('text', 'ntext', 'image', 'timestamp'); -- Skip legacy/binary types

OPEN col_cursor;
FETCH NEXT FROM col_cursor INTO @ColName, @DataType;

-- 3. ANALYSIS LOOP
WHILE @@FETCH_STATUS = 0
BEGIN
    -- Build Dynamic SQL to profile this specific column
    SET @Sql = '
    SELECT 
        @ColName,
        -- Check if valid Date
        MIN(CASE WHEN TRY_CAST(Val AS DATE) IS NOT NULL OR Val IS NULL THEN 1 ELSE 0 END) as IsDate,
        
        -- Check if Numeric
        MIN(CASE WHEN TRY_CAST(Val AS FLOAT) IS NOT NULL OR Val IS NULL THEN 1 ELSE 0 END) as IsNumber,

        -- Check if Integer (Numeric AND no decimal value remainder)
        MIN(CASE 
            WHEN TRY_CAST(Val AS FLOAT) IS NULL AND Val IS NOT NULL THEN 0 
            WHEN TRY_CAST(Val AS FLOAT) = FLOOR(TRY_CAST(Val AS FLOAT)) THEN 1 
            ELSE 0 
        END) as IsInt,

        -- Max String Length
        MAX(LEN(Val)) as MaxLen,
        
        -- Max Integer Value (for sizing TINYINT vs BIGINT)
        MAX(TRY_CAST(Val AS BIGINT)) as MaxIntVal,

        -- Max Decimal Scale (digits after dot)
        MAX(CASE 
            WHEN CHARINDEX(''.'', Val) > 0 THEN LEN(Val) - CHARINDEX(''.'', Val)
            ELSE 0 
        END) as MaxScale
    FROM (
        SELECT ' + CASE WHEN @AnalyzeSampleRows IS NOT NULL THEN 'TOP (' + CAST(@AnalyzeSampleRows AS VARCHAR) + ') ' ELSE '' END + '
            LTRIM(RTRIM(CAST([' + @ColName + '] AS VARCHAR(MAX)))) as Val 
        FROM [' + @SchemaName + '].[' + @TableName + ']
        WHERE [' + @ColName + '] IS NOT NULL
    ) t';

    DECLARE @IsDate BIT, @IsNum BIT, @IsInt BIT, @MaxLen INT, @MaxIntVal BIGINT, @MaxScale INT;

    -- Execute the profiling
    EXEC sp_executesql @Sql, 
        N'@ColName NVARCHAR(128), @IsDate BIT OUTPUT, @IsNum BIT OUTPUT, @IsInt BIT OUTPUT, @MaxLen INT OUTPUT, @MaxIntVal BIGINT OUTPUT, @MaxScale INT OUTPUT',
        @ColName, @IsDate OUTPUT, @IsNum OUTPUT, @IsInt OUTPUT, @MaxLen OUTPUT, @MaxIntVal OUTPUT, @MaxScale OUTPUT;

    -- 4. TYPE LOGIC
    DECLARE @FinalType VARCHAR(50) = 'VARCHAR';
    DECLARE @FinalPrec INT = NULL;
    DECLARE @FinalScale INT = NULL;

    -- A. DATE LOGIC
    IF @IsDate = 1 AND @MaxLen > 0 
    BEGIN
        -- If it looks like a date, matches date format, and isn't just numbers (like 2023)
        IF @IsNum = 0 OR (@IsNum = 1 AND @MaxLen >= 8) -- prevent '2020' integer being cast as date
            SET @FinalType = 'DATE';
    END

    -- B. NUMERIC LOGIC
    IF @FinalType = 'VARCHAR' AND @IsNum = 1 AND @MaxLen > 0
    BEGIN
        IF @IsInt = 1
        BEGIN
            -- It is an integer (even if stored as 120.00)
            SET @FinalType = CASE 
                WHEN @MaxIntVal BETWEEN 0 AND 255 THEN 'TINYINT'
                WHEN @MaxIntVal BETWEEN -32768 AND 32767 THEN 'SMALLINT'
                WHEN @MaxIntVal BETWEEN -2147483648 AND 2147483647 THEN 'INT'
                ELSE 'BIGINT'
            END;
        END
        ELSE
        BEGIN
            -- It is a Decimal
            SET @FinalType = 'DECIMAL';
            SET @FinalScale = @MaxScale;
            -- Calculate Precision: Integer Part Length + Scale
            DECLARE @IntPartLen INT = LEN(CAST(@MaxIntVal AS VARCHAR(50)));
            SET @FinalPrec = @IntPartLen + @FinalScale;
            
            -- Add Cushion to Precision (growth room)
            SET @FinalPrec = @FinalPrec + 2; 
            IF @FinalPrec > 38 SET @FinalPrec = 38;
        END
    END

    -- C. STRING LOGIC (Default)
    IF @FinalType = 'VARCHAR'
    BEGIN
        SET @FinalPrec = CEILING(@MaxLen * (1.0 + (@VarcharCushionPct / 100.0)));
        IF @FinalPrec < @MinVarcharSize SET @FinalPrec = @MinVarcharSize;
        IF @FinalPrec > 8000 SET @FinalPrec = -1; -- logic for MAX
    END

    INSERT INTO @AnalysisTable VALUES (@ColName, @FinalType, @FinalPrec, @FinalScale, @MaxLen, @MaxIntVal, NULL, NULL, @IsDate, @IsInt, @IsNum);

    FETCH NEXT FROM col_cursor INTO @ColName, @DataType;
END

CLOSE col_cursor;
DEALLOCATE col_cursor;

-- 5. OUTPUT GENERATION
PRINT '---------------------------------------------------';
PRINT '-- Generated Mappings for table: ' + @TableName;
PRINT '---------------------------------------------------';
PRINT '';

SELECT 'INSERT INTO @ColumnMappings VALUES (''' + ColumnName + ''', ''' + SuggestedType + ''', ' + 
       CASE 
            WHEN SuggestedType = 'DECIMAL' THEN CAST(SuggestedPrecision AS VARCHAR) 
            WHEN SuggestedType = 'VARCHAR' AND SuggestedPrecision = -1 THEN '''MAX''' 
            WHEN SuggestedType = 'VARCHAR' THEN CAST(SuggestedPrecision AS VARCHAR) 
            ELSE 'NULL' 
       END + ', ' + 
       CASE 
            WHEN SuggestedType = 'DECIMAL' THEN CAST(SuggestedScale AS VARCHAR) 
            ELSE 'NULL' 
       END + ');' 
FROM @AnalysisTable;
