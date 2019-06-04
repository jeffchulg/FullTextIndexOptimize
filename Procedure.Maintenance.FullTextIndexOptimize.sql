/*requires Schema.Maintenance.sql*/

DECLARE @ProcedureSchema NVARCHAR(256);
DECLARE @ProcedureName   NVARCHAR(256);

SET @ProcedureSchema = 'Maintenance' ;
SET @ProcedureName = 'FullTextIndexOptimize' ;

RAISERROR('-----------------------------------------------------------------------------------------------------------------',0,1);
RAISERROR('PROCEDURE [%s].[%s]',0,1,@ProcedureSchema,@ProcedureName);

IF  NOT EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[' + @ProcedureSchema + N'].[' + @ProcedureName +  N']') AND type in (N'P'))
BEGIN
    BEGIN TRY
        EXECUTE ('CREATE Procedure [' + @ProcedureSchema + '].[' + @ProcedureName +  '] ( ' +
                ' @ServerName    varchar(512), ' +
                ' @DbName    varchar(50) ' +
                ') ' +
                'AS ' +
                'BEGIN ' +
                '   SELECT ''Not implemented'' ' +
                'END')
    END TRY
    BEGIN CATCH
        PRINT '   Error while trying to create procedure'
        RETURN
    END CATCH

    PRINT '   PROCEDURE created.'
END
GO

ALTER PROCEDURE [Maintenance].[FullTextIndexOptimize] (
    @MaintenanceMode                        VARCHAR(16)     = 'CATALOG', -- possible values: CATALOG or INDEX
    @DatabaseName                           VARCHAR(256)    = NULL,
    @FragmentationPctThresh4IndexRebuild    INT             = 10,
    @FragmentationMinSizeMb4IndexRebuild    INT             = 50,
    @FragmentsCountThresh4IndexRebuild      INT             = 30, 
    @MinIndexSizeMb                         FLOAT           = 1.0, -- only for INDEX maintenance mode
    @UseParameterTable                      BIT             = 0,
    @WithLog                                BIT             = 1,
    @ReportOnly                             BIT             = 0,
    @TestOnly                               BIT             = 0,
    @_BypassIndexRebuildThresholds          BIT             = 0,
    @Debug                                  SMALLINT        = 0
)
AS
/*
  ===================================================================================
    DESCRIPTION:
        Performs a maintenance of every Full-Text indexes either in the specified 
        database (@DatabaseName parameter) or in every user databases.
    
    PARAMETERS:
    
        @Debug      Tells the stored procedure whether to be more talkative or not.
                    0 means no debug output
                    1 means some debug output (but no T-SQL statement)
                    2 or more means everything

    REQUIREMENTS:

    EXAMPLE USAGE :
    
		-- Run with defaults
        EXEC [Maintenance].[FullTextIndexOptimize] @Debug = 1 ;


		-- Run with defaults but only report what you saw for all databases
        EXEC [Maintenance].[FullTextIndexOptimize] 
            @Debug = 0,
            @ReportOnly = 1
        ;

		-- Run with defaults but only report what you saw for Test_FT_Maintenance database
		EXEC [Maintenance].[FullTextIndexOptimize] 
            @Debug = 1,
            @ReportOnly = 1
            @DatabaseName = 'Test_FT_Maintenance'
        ;

		-- Run INDEX maintenance mode on given database
        EXEC [Maintenance].[FullTextIndexOptimize] 
                    @MaintenanceMode    = 'INDEX',
                    @DatabaseName       = 'Test_FT_Maintenance',
                    @Debug              = 2
        ;

		-- Run INDEX maintenance mode but only report what you saw in DEBUG mode
        EXEC [Maintenance].[FullTextIndexOptimize] 
                    @MaintenanceMode    = 'INDEX',
                    --@DatabaseName       = 'Test_FT_Maintenance',
                    @Debug              = 2,
                    @ReportOnly         = 1
        ;

		-- Run INDEX maintenance mode but only report what you saw in DEBUG mode
        EXEC [Maintenance].[FullTextIndexOptimize] 
                    @MaintenanceMode    = 'INDEX',
                    @Debug              = 2,
                    @ReportOnly         = 1
        ;

		-- Run Catalog maintenance mode with custom thresholds
		EXEC [Maintenance].[FullTextIndexOptimize] 
			@FragmentationPctThresh4IndexRebuild = 5,
			@FragmentationMinSizeMb4IndexRebuild = 100,
			@FragmentsCountThresh4IndexRebuild   = 3,
            @Debug = 0,
            @ReportOnly = 1
        ;

        -- Run for all indices:
        EXEC [Maintenance].[FullTextIndexOptimize] @Debug = 3, @MaintenanceMode = 'Index' , @TestOnly = 1, @_BypassIndexRebuildThresholds = 1

	-- Helpful queries for checks
        select * From sys.fulltext_indexes
        SELECT * From Common.CommandLog;
        

  ===================================================================================
*/
BEGIN
    SET NOCOUNT ON;
    DECLARE @tsql               nvarchar(max);
    DECLARE @tmpStr             nvarchar(max);
    DECLARE @LineFeed           CHAR(2);
    DECLARE @ProcedureName      VARCHAR(1024);
    DECLARE @ExecRet            INT;
    DECLARE @InnerDebug         BIT;
    DECLARE @Outcome            VARCHAR(16);
    DECLARE @CurEntryId         INT;
    DECLARE @CurObjectId        INT;
    DECLARE @CurIndexId         INT;
    DECLARE @LogMsg             VARCHAR(MAX);
    DECLARE @CurDbName          VARCHAR(256);
    DECLARE @CurObjectName      VARCHAR(1024);
    DECLARE @CurCatalogName     VARCHAR(256);
    DECLARE @FTColumnNames      NVARCHAR(MAX);
    DECLARE @Statement4Drop     NVARCHAR(MAX);
    DECLARE @Statement4Create   NVARCHAR(MAX);
    DECLARE @Log2Tbl            CHAR(1);

    -- error handling (TRY..CATCH)
    DECLARE @ErrorNumber    INT             ;
    DECLARE @ErrorLine      INT             ;
    DECLARE @ErrorMessage   NVARCHAR(4000)  ;
    DECLARE @ErrorSeverity  INT             ;
    DECLARE @ErrorState     INT             ;

    SELECT
        @ProcedureName      = QUOTENAME(OBJECT_SCHEMA_NAME(@@PROCID)) + '.' + QUOTENAME(OBJECT_NAME(@@PROCID)),
        @tsql               = '',
        @LineFeed           = CHAR(13) + CHAR(10),
        @Debug              = CASE WHEN @Debug > 2 THEN 2 ELSE @Debug END,
        @InnerDebug         = CASE WHEN @Debug > 0 THEN 1 ELSE 0 END,
        @MaintenanceMode    = CASE WHEN LEN(LTRIM(RTRIM(@MaintenanceMode))) = 0 THEN NULL ELSE UPPER(@MaintenanceMode) END,
        @Log2Tbl            = CASE WHEN @WithLog = 1 THEN 'Y' ELSE 'N' END,
        @DatabaseName       = CASE WHEN LEN(LTRIM(RTRIM(@DatabaseName))) = 0 THEN NULL ELSE @DatabaseName END
    ;

    if (@Debug >= 1)
    BEGIN
        RAISERROR('-- -----------------------------------------------------------------------------------------------------------------',0,1);
        RAISERROR('-- Now running %s stored procedure.',0,1,@ProcedureName);
        RAISERROR('-- -----------------------------------------------------------------------------------------------------------------',0,1);
    END;

    IF(@MaintenanceMode NOT IN ('CATALOG','INDEX'))
    BEGIN
        RAISERROR('Invalid parameter value [%s] for @MaintenanceMode',12,1,@MaintenanceMode);
        RETURN;
    END;

    IF(@DatabaseName IS NOT NULL AND DB_ID(@DatabaseName) IS NULL)
    BEGIN
        RAISERROR('No database found with name [%s] on server',12,1,@DatabaseName);
        RETURN;
    END;
    
    /* TODO: remove it */
    IF(@UseParameterTable = 1)
    BEGIN
        RAISERROR('Parameter table as thresholds source not yet implemented',12,1);
        RETURN;
    END;
    
    IF(@FragmentationPctThresh4IndexRebuild < 1)
    BEGIN
        RAISERROR('Value for % fragmentation Threshold should be higher than 0',12,1);
        RETURN;
    END;
    
    IF(@FragmentsCountThresh4IndexRebuild < 2)
    BEGIN
        RAISERROR('Value for index fragments count should be higher than 1',12,1);
        RETURN;
    END;
    
    IF(@FragmentationMinSizeMb4IndexRebuild < 5)
    BEGIN
        RAISERROR('Value for fragmentation size should be higher than 5 Mb',12,1);
        RETURN;
    END;
    
    IF(@Debug >= 1)
    BEGIN
        RAISERROR('Collecting FullText Indexes details...',0,1);
    END;
    
    IF(OBJECT_ID('tempdb..#FTidx') IS NOT NULL)
    BEGIN
        EXEC sp_executesql N'DROP TABLE #FTidx';
    END;
    
    CREATE TABLE #FTidx (
        EntryId                     INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        DatabaseName                VARCHAR(256) NOT NULL,
        CatalogId                   INT NOT NULL,
        CatalogName                 VARCHAR(256) NOT NULL,
        BaseObjectId                INT,
        BaseObjectName              VARCHAR(1024),
        BaseIndexId                 INT,
        TotalSizeMb                 FLOAT NOT NULL,
        FragmentsCount              INT,
        LargestFragmentSizeMb       FLOAT,
        FragmentationSpaceMb        FLOAT,
        FragmentationPct            FLOAT,
        MarkedForIndexMaintenance   BIT DEFAULT 0,
        StatementForDrop            AS 'USE ' + QUOTENAME(DatabaseName) + ';' + CHAR(13) + CHAR(10) +
                                       'DROP FULLTEXT INDEX ON ' + BaseObjectName + ';',
        MaintenanceOutcome          VARCHAR(32),
        MaintenanceLog              VARCHAR(MAX)
    );
    
    SET @tsql = 'WITH FragmentationDetails' + @LineFeed +
                'AS (' + @LineFeed +
                '    SELECT ' + @LineFeed +
                '        table_id,' + @LineFeed +
                '        COUNT(*) AS FragmentsCount,' + @LineFeed +
                '        CONVERT(DECIMAL(9,2), SUM(data_size/(1024.*1024.))) AS IndexSizeMb,' + @LineFeed +
                '        CONVERT(DECIMAL(9,2), MAX(data_size/(1024.*1024.))) AS largest_fragment_mb' + @LineFeed +
                '    FROM sys.fulltext_index_fragments' + @LineFeed +
                '    GROUP BY table_id' + @LineFeed +
                ')' + @LineFeed +
                'INSERT INTO #FTidx (' + @LineFeed +
                '    DatabaseName,CatalogId,CatalogName,BaseObjectId,BaseObjectName,BaseIndexId,TotalSizeMb,FragmentsCount,LargestFragmentSizeMb,FragmentationSpaceMb,FragmentationPct' + @LineFeed +
                ')' + @LineFeed +
                'SELECT ' + @LineFeed +
                '    DB_NAME()              AS DatabaseName,' + @LineFeed +
                '    ftc.fulltext_catalog_id AS CatalogId, ' + @LineFeed +
                '    ftc.[name]             AS CatalogName, ' + @LineFeed +
                --'    fti.change_tracking_state AS ChangeTrackingState,' + @LineFeed +
                '    fti.object_id              AS BaseObjectId, ' + @LineFeed +
                '    QUOTENAME(OBJECT_SCHEMA_NAME(fti.object_id)) + ''.'' + QUOTENAME(OBJECT_NAME(fti.object_id)) AS BaseObjectName,' + @LineFeed +
                '    unique_index_id, ' + @LineFeed +
                '    f.IndexSizeMb          AS IndexSizeMb, ' + @LineFeed +
                '    f.FragmentsCount       AS FragmentsCount, ' + @LineFeed +
                '    f.largest_fragment_mb   AS IndexLargestFragmentMb,' + @LineFeed +
                '    f.IndexSizeMb - f.largest_fragment_mb AS IndexFragmentationSpaceMb,' + @LineFeed +
                '    CASE' + @LineFeed +
                '        WHEN f.IndexSizeMb = 0 THEN 0' + @LineFeed +
                '        ELSE ' + @LineFeed +
                '            100.0 * (f.IndexSizeMb - f.largest_fragment_mb) / f.IndexSizeMb' + @LineFeed +
                '    END AS IndexFragmentationPct' + @LineFeed +
                'FROM ' + @LineFeed +
                '    sys.fulltext_catalogs ftc' + @LineFeed +
                'JOIN ' + @LineFeed +
                '    sys.fulltext_indexes fti' + @LineFeed +
                'ON ' + @LineFeed +
                '    fti.fulltext_catalog_id = ftc.fulltext_catalog_id' + @LineFeed +
                'JOIN FragmentationDetails f' + @LineFeed +
                '    ON f.table_id = fti.object_id' + @LineFeed +
                ';' + @LineFeed 
                ;
                
    EXEC @ExecRet = [Common].[RunQueryAcrossDatabases] 
                @QueryTxt               = @tsql,
                @IncludeSystemDatabases = 0,
                @DbName_equals          = @DatabaseName,
                @Debug                  = @InnerDebug
    ;

        
    IF(@ExecRet <> 0)
    BEGIN
        RAISERROR('Error while collecting fulltext indexes',12,1);
        RETURN;
    END;
    
    IF(@Debug >= 1)
    BEGIN
        RAISERROR('Setting maintenance actions to take at index level',0,1);
    END;    
    
    UPDATE #FTidx 
    SET MarkedForIndexMaintenance = 1
    WHERE @_BypassIndexRebuildThresholds = 1
    OR (
        TotalSizeMb >= @MinIndexSizeMb
        AND (
                FragmentsCount              >= @FragmentsCountThresh4IndexRebuild
            OR  FragmentationPct            >= @FragmentationPctThresh4IndexRebuild
            OR  FragmentationSpaceMb        >= @FragmentationMinSizeMb4IndexRebuild
        )
    );
    

    IF(@MaintenanceMode = 'INDEX')
    BEGIN
        IF(@ReportOnly = 1)
        BEGIN
            UPDATE #FTidx 
            SET MaintenanceOutcome = 'SKIPPED'
            WHERE MarkedForIndexMaintenance = 1
            ;

            SELECT *
            FROM #FTidx
            ;

            DROP TABLE #FTidx;

            RETURN;
        END;

        IF(@Debug >= 1)
        BEGIN
            RAISERROR('Index Maintenance Mode Start',0,1);
        END;

        /*
            In this mode, we will loop on records in #FTidx that has MarkedForIndexMaintenance column with value 1.
        */
        -- In order to enter loop condition
        SELECT @CurEntryId = MIN(EntryId)
        FROM #FTidx
        WHERE MarkedForIndexMaintenance = 1
        ;

        WHILE (@CurEntryId IS NOT NULL)
        BEGIN
            
            SELECT 
                @Statement4Create   = NULL,
                @tmpStr             = NULL,
                @tsql               = '',
                @Statement4Drop     = StatementForDrop,
                @CurObjectId        = BaseObjectId,
                @CurIndexId         = BaseIndexId,
                @CurDbName          = DatabaseName,
                @CurObjectName      = BaseObjectName,
                @CurCatalogName     = CatalogName
            FROM #FTidx
            WHERE EntryId = @CurEntryId
            ;
             
            IF(@Debug >= 1)
            BEGIN
                SET @LogMsg = 'Now taking care of FT index on ' + @CurObjectName + @LineFeed + 
                              '    in ' + @CurDbName + ' database' + @LineFeed + 
                              '    in ' + @CurCatalogName + ' FT catalog'
                              ;
                RAISERROR(@LogMsg,0,1);
            END;
                
            IF(@Debug > 1)
            BEGIN 
                RAISERROR('    > Generating CREATE statement',0,1);
            END;
            
            -- easy part of the statement
            SET @Statement4Create = 'USE ' + QUOTENAME(@CurDbName) + ';' + @LineFeed +
                                    'CREATE FULLTEXT INDEX ON ' + @CurObjectName + '(' + @LineFeed 
                                    ;
            /*                        
            IF(@Debug > 1)
            BEGIN
                RAISERROR('================',0,1);
                RAISERROR('Current version of creation statement:',0,1);
                RAISERROR(@Statement4Create,0,1);
                RAISERROR('================',0,1);
            END;
            */  

            IF(@Debug > 1)
            BEGIN 
                RAISERROR('        Generating the list of columns included in the FT index',0,1);
            END;

            SET @tsql = 'USE ' + QUOTENAME(@CurDbName) + ';' + @LineFeed +
                        'SELECT @tsql = CASE WHEN LEN(@tsql) = 0 OR @tsql IS NULL THEN '''' ELSE @tsql + '','' + @LineFeed END + 
                                        ''    '' + QUOTENAME(c.Name) + CASE WHEN refc.name IS NOT NULL THEN '' TYPE COLUMN '' + refc.name + '' ''  ELSE '''' END +
                                                          + '' Language '' + CONVERT(VARCHAR(10),language_id) + '' '' +
                                                            CASE WHEN fic.statistical_semantics = 1 THEN ''STATISTICAL_SEMANTICS '' ELSE '''' END' + @LineFeed +
                        'FROM sys.fulltext_index_columns fic' + @LineFeed +
                        'INNER JOIN sys.columns as c' + @LineFeed +
                        'ON c.object_id  = fic.object_id' + @LineFeed +
                        'AND c.column_id = fic.column_id' + @LineFeed +
                        'LEFT JOIN sys.columns as refc' + @LineFeed +
                        'ON refc.object_id  = fic.object_id' + @LineFeed +
                        'AND refc.column_id = fic.type_column_id' + @LineFeed +
                        'WHERE fic.object_id = @CurObjectId' + @LineFeed +
                        ';'
                        ;
            BEGIN TRY
                IF(@Debug > 1)
                BEGIN
                    RAISERROR('================',0,1);
                    RAISERROR('Next query: (Used to get back the columns implied in FT index)',0,1);
                    RAISERROR(@tsql,0,1);
                    RAISERROR('----------------', 0,1);
                    RAISERROR('Parameters:', 0,1);
                    RAISERROR('    %d', 0,1,@CurObjectId);
                    RAISERROR('================',0,1);
                END;

                EXEC @ExecRet = sp_executesql 
                                    @tsql,
                                    N'@tsql NVARCHAR(MAX) OUTPUT,@LineFeed CHAR(2),@CurObjectId INT',
                                    @tsql           = @tmpStr OUTPUT,
                                    @LineFeed       = @LineFeed,
                                    @CurObjectId    = @CurObjectId
                ;

                IF(@ExecRet <> 0)
                BEGIN
                    SET @LogMsg = 'Unable to get back list of columns for fulltext index created on object ' + @CurObjectName +
                                  'in database ' + QUOTENAME(@CurDbName)
                                  ;
                    RAISERROR(@LogMsg,12,1);
                END;
                
                IF(@Debug > 0)
                BEGIN
                    RAISERROR('Got back following columns list:' , 0,1);
                    RAISERROR('    "%s"',0,1,@tmpStr);
                END;

                IF(@Debug > 1)
                BEGIN 
                    RAISERROR('        Appending column listing',0,1);
                END;

                SET  @Statement4Create = @Statement4Create + @tmpStr + @LineFeed +
                                        ')' + @LineFeed 
                                        ;
                SET @tmpStr = NULL;

                
                IF(@Debug > 1)
                BEGIN
                    RAISERROR('================',0,1);
                    RAISERROR('Current version of creation statement:',0,1);
                    RAISERROR(@Statement4Create,0,1);
                    RAISERROR('================',0,1);
                END;
                

                IF(@Debug > 1)
                BEGIN 
                    RAISERROR('        Getting back FT index options',0,1);
                END;
                                                
                SET @tsql = 'USE ' + QUOTENAME(@CurDbName) + ';' + @LineFeed +
                            'SELECT @tsql = ''KEY INDEX ''  + QUOTENAME(idx.Name) + @LineFeed +' + @LineFeed +
                            '               ''ON '' + QUOTENAME(@CatalogName) + @LineFeed +' + @LineFeed +
                            '               ''WITH CHANGE_TRACKING '' + fi.change_tracking_state_desc + @LineFeed +' + @LineFeed +
                            '               '';''' + @LineFeed +
                            'FROM sys.indexes idx' + @LineFeed +
                            'JOIN sys.fulltext_indexes fi ' + @LineFeed +
                            'ON idx.object_id = fi.object_id' + @LineFeed +
                            'WHERE idx.object_id = @ObjectId' + @LineFeed +
                            'AND index_id        = @IndexId' + @LineFeed + 
                            ';'
                            ;
                                            
                IF(@Debug > 1)
                BEGIN
                    RAISERROR('================',0,1);
                    RAISERROR('Next query: (Used to defined FT index options)',0,1);
                    RAISERROR(@tsql,0,1);
                    RAISERROR('================',0,1);
                END;

                EXEC @ExecRet = sp_executesql 
                                        @tsql,
                                        N'@tsql NVARCHAR(MAX) OUTPUT,@LineFeed CHAR(2),@ObjectId INT,@IndexId INT,@CatalogName VARCHAR(256)',
                                        @tsql        = @tmpStr OUTPUT ,
                                        @LineFeed    = @LineFeed , 
                                        @ObjectId    = @CurObjectId , 
                                        @IndexId     = @CurIndexId,
                                        @CatalogName = @CurCatalogName
                ;

                IF(@ExecRet <> 0)
                BEGIN
                    SET @LogMsg = 'Unable to generate FT index creation option on table:' + @CurObjectName +
                                  'in database ' + QUOTENAME(@CurDbName)
                                  ;
                    RAISERROR(@LogMsg,12,1);
                END;            
                
                SET @Statement4Create = @Statement4Create + @tmpStr ;
                
                IF(@Debug >= 1)
                BEGIN
                    SET @LogMsg = '    > Dropping the FullText index ON ' + QUOTENAME(@CurDbName) + '.' + @CurObjectName ;
                    RAISERROR(@LogMsg,0,1);
                END;

                IF(@Debug > 1)
                BEGIN
                    RAISERROR('================',0,1);
                    RAISERROR('> Drop statement:' ,0,1);
                    RAISERROR(@Statement4Drop,0,1);
                    RAISERROR('================',0,1);

                END;
                
                IF(@TestOnly = 0)
                BEGIN
                    EXEC @ExecRet = Common.CommandExecute 
                            @Command        = @Statement4Drop, 
                            @CommandType    = 'DROP_FULLTEXT_INDEX',
                            @RunMode        = 1 , 
                            @LogToTable     = @Log2Tbl, 
                            @RunCommand     = 'Y',
                            @Comments       = 'Part of an index maintenance'
                    ;

                    IF(@ExecRet <> 0)
                    BEGIN
                        RAISERROR('An error occurred during FT index removal',12,1);
                    END;
                END;
                
                IF(@Debug >= 1)
                BEGIN
                    SET @LogMsg = '    > Re-creating the FullText index ON ' + QUOTENAME(@CurDbName) + '.' + @CurObjectName ;
                    RAISERROR(@LogMsg,0,1);
                END;
                
                IF(@Debug > 1)
                BEGIN
                    RAISERROR('================',0,1);
                    RAISERROR('> Creation statement:' ,0,1);
                    RAISERROR(@Statement4Create,0,1);
                    RAISERROR('================',0,1);
                END;
                
                IF(@TestOnly = 0)
                BEGIN
                    EXEC @ExecRet = Common.CommandExecute 
                            @Command        = @Statement4Create, 
                            @CommandType    = 'CREATE_FULLTEXT_INDEX',
                            @RunMode        = 1 , 
                            @LogToTable     = @Log2Tbl, 
                            @RunCommand     = 'Y',
                            @Comments       = 'Part of an index maintenance'
                    ;                
                    
                    IF(@ExecRet <> 0)
                    BEGIN
                        RAISERROR('An error occurred during FT index creation',12,1);
                    END;
                END;

                SET @Outcome = 'SUCCESS';
                SET @LogMsg  = NULL;
            END TRY
            BEGIN CATCH

                SELECT 
                    @ErrorNumber    = ERROR_NUMBER(),
                    @ErrorLine      = ERROR_LINE(),
                    @ErrorMessage   = ERROR_MESSAGE(),
                    @ErrorSeverity  = ERROR_SEVERITY(),
                    @ErrorState     = ERROR_STATE()
                ;
    
                SET @LogMsg = 'Caught error #' + CAST(@ErrorNumber AS VARCHAR(10)) + /*'during XXX' + */ @LineFeed +
                              'At line #' + CAST(@ErrorLine AS VARCHAR(10)) + @LineFeed +
                              'With Severity ' + CAST(@ErrorSeverity AS VARCHAR(10)) + ' State ' + CAST(@ErrorState AS VARCHAR(10)) + @LineFeed +
                              @LineFeed +
                              'Message:' + @LineFeed +
                              '-------' + @LineFeed +
                              @ErrorMessage 
                              ;
                
                SET @Outcome = 'ERROR';

            END CATCH

            UPDATE #FTidx
            SET MaintenanceOutcome = @Outcome, MaintenanceLog = @LogMsg
            WHERE EntryId = @CurEntryId
            ;

            -- prepare for next iteration
            SET @CurEntryId = NULL;
            SELECT TOP 1 @CurEntryId = EntryId
            FROM #FTidx
            WHERE MarkedForIndexMaintenance = 1
            AND MaintenanceOutcome IS NULL
            ;
        END;

        SELECT @Outcome = CASE WHEN COUNT(*) > 0 THEN 'ERROR' ELSE 'SUCCESS' END
        FROM #FTidx 
        WHERE MaintenanceOutcome <> 'SUCCESS' AND MarkedForIndexMaintenance = 1
        ;

        SELECT * FROM #FTidx;
    END;
    ELSE -- CATALOG mode
    BEGIN

        IF(@Debug >= 1)
        BEGIN
            RAISERROR('Getting back catalog details based on collected Full-Text indexes',0,1);
        END;
                
        IF(OBJECT_ID('tempdb..#FTcatalog') IS NOT NULL)
        BEGIN
            EXEC sp_executesql N'DROP TABLE #FTcatalog';
        END;

        CREATE TABLE #FTcatalog (
            RecordId                        INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
            DatabaseName                    VARCHAR(256) NOT NULL,
            CatalogName                     VARCHAR(256) NOT NULL,
            CatalogId                       INT,
            BaseObjectsCount                INT,
            IndexesNeedingMaintenance       INT,
            TotalSizeMb                     FLOAT,
            TotalFragmentsCount             INT,
            TotalFragmentationSpaceMb       FLOAT,
            MarkedForCatalogMaintenance     BIT DEFAULT 0,
            MaintenanceOutcome              VARCHAR(32),
            MaintenanceLog                  VARCHAR(MAX),
            ReorganizeStatement             AS CAST(
                                                'USE ' + QUOTENAME(DatabaseName) + ';' + CHAR(13) + CHAR(10) +
                                                'ALTER FULLTEXT CATALOG ' + QUOTENAME(CatalogName) + ' REORGANIZE;' 
                                                AS NVARCHAR(4000)
                                            )
        );
        
        WITH CatalogsDetails
        AS (
            SELECT 
                DatabaseName,
                CatalogName,
                CatalogId,
                sum(idx.TotalSizeMb) as TotalSizeMb,
                COUNT(*) as IndexesCount,
                SUM(FragmentsCount) as FragmentsCount,
                SUM(FragmentationSpaceMb) AS FragmentationSpaceMb
            FROM #FTidx idx
            group by DatabaseName,CatalogName,CatalogId
        ),        
        CatalogsWithMaintenanceNeeded 
        AS (
            SELECT 
                DatabaseName,
                CatalogId,
                count(*) as IndexesCount
            FROM #FTidx idx
            WHERE MarkedForIndexMaintenance = 1
            GROUP BY DatabaseName,CatalogId
        )
        INSERT INTO #FTcatalog (
            DatabaseName,CatalogName,CatalogId,BaseObjectsCount,IndexesNeedingMaintenance,TotalSizeMb,TotalFragmentsCount,TotalFragmentationSpaceMb,MarkedForCatalogMaintenance
        )
        SELECT 
            cd.DatabaseName,
            cd.CatalogName,
            cd.CatalogId,
            cd.IndexesCount,
            ISNULL(cm.IndexesCount,0),
            cd.TotalSizeMb,
            cd.FragmentsCount,
            cd.FragmentationSpaceMb,
            CASE WHEN cm.IndexesCount IS NULL THEN 0 ELSE 1 END
        FROM CatalogsDetails cd
        LEFT JOIN CatalogsWithMaintenanceNeeded cm
        ON 
            cd.DatabaseName = cm.DatabaseName
        AND cd.CatalogId    = cm.CatalogId
        ;
        
        /*
            Here we use the same thresholds as indexes but for catalog (not discussed in documentation but it's just to be sure
        */
        UPDATE #FTcatalog
        SET MarkedForCatalogMaintenance = 1
        WHERE 
            MarkedForCatalogMaintenance = 0
        AND TotalSizeMb >= @MinIndexSizeMb
        AND (
                TotalFragmentsCount - BaseObjectsCount >= @FragmentsCountThresh4IndexRebuild
            OR  TotalFragmentationSpaceMb >= @FragmentationPctThresh4IndexRebuild
            OR  100.0 * TotalFragmentationSpaceMb / TotalSizeMb >= @FragmentationPctThresh4IndexRebuild
        );

        IF(@ReportOnly = 1)
        BEGIN
            UPDATE #FTcatalog 
            SET MaintenanceOutcome = 'SKIPPED'
            WHERE MarkedForCatalogMaintenance = 1
            ;

            SELECT *
            FROM #FTcatalog
            ;

            DROP TABLE #FTidx;
            DROP TABLE #FTcatalog;

            RETURN;
        END;

        IF(@Debug >= 1)
        BEGIN
            RAISERROR('Catalog Maintenance Mode Start',0,1);
        END;

        SET @CurEntryId = NULL;
        SET @tsql       = NULL;

        SELECT TOP 1
            @CurEntryId = RecordId,
            @tsql       = ReorganizeStatement
        FROM #FTcatalog
        WHERE MarkedForCatalogMaintenance = 1
        AND MaintenanceOutcome IS NULL
        ;
        
        WHILE(@tsql IS NOT NULL)
        BEGIN
            
            BEGIN TRY
                                
                EXEC @ExecRet = Common.CommandExecute 
                        @Command        = @tsql, 
                        @CommandType    = 'REORGANIZE_FULLTEXT_CATALOG',
                        @RunMode        = 1 , 
                        @LogToTable     = @Log2Tbl, 
                        @RunCommand     = 'Y',
                        @Comments       = 'Part of an index maintenance'
                ;   
            
                IF(@ExecRet <> 0)
                BEGIN
                    RAISERROR('An error occurred during FT index creation',12,1);
                END;

                SET @Outcome = 'SUCCESS';
                SET @LogMsg  = NULL;
            END TRY
            BEGIN CATCH
                
                SELECT 
                    @ErrorNumber    = ERROR_NUMBER(),
                    @ErrorLine      = ERROR_LINE(),
                    @ErrorMessage   = ERROR_MESSAGE(),
                    @ErrorSeverity  = ERROR_SEVERITY(),
                    @ErrorState     = ERROR_STATE()
                ;
    
                SET @LogMsg = 'Caught error #' + CAST(@ErrorNumber AS VARCHAR(10)) + /*'during XXX' + */ @LineFeed +
                              'At line #' + CAST(@ErrorLine AS VARCHAR(10)) + @LineFeed +
                              'With Severity ' + CAST(@ErrorSeverity AS VARCHAR(10)) + ' State ' + CAST(@ErrorState AS VARCHAR(10)) + @LineFeed +
                              @LineFeed +
                              'Message:' + @LineFeed +
                              '-------' + @LineFeed +
                              @ErrorMessage 
                              ;
                SET @Outcome = 'ERROR';

            END CATCH
            
            UPDATE #FTcatalog
            SET MaintenanceOutcome = @Outcome, MaintenanceLog = @LogMsg
            WHERE RecordId = @CurEntryId
            ;
            
            
            -- next in the loop
            SET @tsql       = NULL;
			SET @CurEntryId = NULL;
            SELECT TOP 1
                @CurEntryId = RecordId,
                @tsql       = ReorganizeStatement
            FROM #FTcatalog
            WHERE MarkedForCatalogMaintenance = 1
            AND MaintenanceOutcome IS NULL
            ;
        END;

        SELECT @Outcome = CASE WHEN COUNT(*) > 0 THEN 'ERROR' ELSE 'SUCCESS' END
        FROM #FTcatalog
        WHERE MaintenanceOutcome <> 'SUCCESS' AND MarkedForCatalogMaintenance = 1
        ;

        SELECT *
        FROM #FTcatalog;
    END

    -- cleanups
    IF(OBJECT_ID('tempdb..#FTidx') IS NOT NULL)
    BEGIN
        EXEC sp_executesql N'DROP TABLE #FTidx';
    END;
    
    IF(OBJECT_ID('tempdb..#FTcatalog') IS NOT NULL)
    BEGIN
        EXEC sp_executesql N'DROP TABLE #FTcatalog';
    END;
    
    if (@Debug >= 1)
    BEGIN
        RAISERROR('-- -----------------------------------------------------------------------------------------------------------------',0,1);
        RAISERROR('-- Execution of %s completed.',0,1,@ProcedureName);
        RAISERROR('-- -----------------------------------------------------------------------------------------------------------------',0,1);
    END;

    IF(@Outcome <> 'SUCCESS')
    BEGIN
        RAISERROR('An error occurred during process',12,1);
    END;

END;
GO


IF (@@ERROR = 0)
BEGIN
    PRINT '   PROCEDURE altered.';
END;
ELSE
BEGIN
    PRINT '   Error while trying to alter procedure';
    RETURN
END;
GO

RAISERROR('-----------------------------------------------------------------------------------------------------------------',0,1);
RAISERROR('',0,1);
GO