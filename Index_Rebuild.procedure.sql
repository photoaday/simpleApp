CREATE OR ALTER PROCEDURE Index_Rebuild
    @Index_Frag_PCT int = 30
    , @Debug          bit = 1

AS

--  ======================================================================================================  
--  PURPOSE: Rebuild indexes in tables with indexes that have fragmentation greater than 
--             provided/default percentage.
--  ------------------------------------------------------------------------------------------------------
--  DATE        AUTHOR          Modifiction  
--  
--  ====================================================================================================== 


SET NOCOUNT ON;

    ---- Remove for debugging
    --DECLARE @Index_Frag_PCT int = 20
    --DECLARE @Debug          bit = 1

    --  ------------------------------------------------------------------------------------------------------------
    --  Set default values
    --  ------------------------------------------------------------------------------------------------------------

    DECLARE @DB_NAME    VARCHAR(120) 
    SELECT  @DB_NAME = DB_NAME()

    --  ------------------------------------------------------------------------------------------------------------
    --  This section finds indexes with that have fregmenation greater than provided percent
    --  ------------------------------------------------------------------------------------------------------------

    IF EXISTS ( Select TOP 1 * From tempdb.INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE '#TEMP_Results%')
        BEGIN
            DROP TABLE #TEMP_Results         
        END

    CREATE TABLE #TEMP_Results (
         [Table_Name] varchar(125)
         , [Table_Schema] varchar(50) NULL
         , [index_id] int NULL
         , [index_name] varchar(125) NULL
         , [avg_fragmentation_in_percent] float NULL
         , [fragment_count] bigint NULL
         , [avg_fragment_size_in_pages] float NULL )
    INSERT  #TEMP_Results (
            [Table_Name]
            , [Table_Schema]
            , [index_id]
            , [index_name]
            , [avg_fragmentation_in_percent]
            , [fragment_count]
            , [avg_fragment_size_in_pages] )
    SELECT  o.name as Table_Name
            , SCHEMA_NAME(O.schema_id) AS Table_Schema
            , a.index_id
            , b.name as index_name
            , avg_fragmentation_in_percent
            , fragment_count
            , avg_fragment_size_in_pages
    FROM    sys.dm_db_index_physical_stats (DB_ID(@DB_NAME), NULL , NULL, NULL, NULL) as a
            join sys.indexes as b on a.object_id = b.object_id and a.index_id = b.index_id
            join sys.objects as o on o.object_id = b.object_id
    WHERE   a.avg_fragmentation_in_percent > @Index_Frag_PCT
            and a.index_id > 0

    SELECT  ' ** INDEXES THAT HAVE FRAGMENTATION > ' + CAST(@Index_Frag_PCT AS VARCHAR(20) ) + ' Percent '
    SELECT  * 
    FROM    #TEMP_Results
  
    --  ------------------------------------------------------------------------------------------------------------
    --  This section builds a distinct list of tables to rebuild all indexes
    --  ------------------------------------------------------------------------------------------------------------

    IF EXISTS ( Select TOP 1 * From tempdb.INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE '#TEMP_Table_List%')
        BEGIN
            DROP TABLE #TEMP_Table_List         
        END

    CREATE TABLE #TEMP_Table_List (
        ID              INT IDENTITY(1,1)
        , [Table_Schema] varchar(50) NULL
        , [Table_Name]  varchar(125) )
        
    INSERT #TEMP_Table_List(
        [Table_Schema]
        , [Table_Name] )
    SELECT  DISTINCT 
            [Table_Schema]
            , table_name
    FROM    #TEMP_Results

    --  ------------------------------------------------------------------------------------------------------------
    --  This section loops through #TEMP_Table_List and rebuild all index in the table
    --  ------------------------------------------------------------------------------------------------------------

    DECLARE @CNT            INT
    DECLARE @Query_Template VARCHAR(1000) = 'ALTER INDEX ALL ON [##Table_Schema##].[##table_name##] REBUILD ; '
    DECLARE @Query          VARCHAR(1000) = ''
    DECLARE @Table_Schema   VARCHAR(125) = ''
    DECLARE @Table_Name     VARCHAR(125) = ''

    SELECT  @CNT = MIN(ID)
    FROM    #TEMP_Table_List

    IF @Debug = 1
        BEGIN
            SELECT  ' *** DEBUG MODE *** '
            SELECT  ' Rebuilding indexes in table: [' + @Table_Schema + '].[' + @Table_Name + ']'
        END
    ELSE
        BEGIN
            SELECT  'Index Rebuild Commands'
        END

    WHILE @CNT IS NOT NULL
        BEGIN
            SELECT  @Table_Schema = [Table_Schema]
                    , @Table_Name= [Table_Name]
            FROM    #TEMP_Table_List
            WHERE   ID = @CNT
            
            SELECT  @Query = REPLACE(@Query_Template, '##table_schema##', @Table_Schema )
            SELECT  @Query = REPLACE(@Query, '##table_name##', @Table_Name )

            IF @Debug = 0
                BEGIN
                    EXEC (@Query)
                END
            ELSE
                BEGIN
                    SELECT  @Query
                END

            -- get next record.                            
            SELECT  @CNT = MIN(ID)
            FROM    #TEMP_Table_List
            WHERE   ID > @CNT

        END 