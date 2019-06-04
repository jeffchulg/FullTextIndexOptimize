PRINT '--------------------------------------------------------------------------------------------------------------'
PRINT 'SCHEMA [Maintenance] CREATION'

IF  NOT EXISTS (SELECT * FROM sys.schemas WHERE name = N'Maintenance')
BEGIN	
    DECLARE @SQL VARCHAR(MAX);
    SET @SQL = 'CREATE SCHEMA [Maintenance] AUTHORIZATION [dbo]'
    EXEC (@SQL)
    
	PRINT '   SCHEMA [Maintenance] created.'
END
ELSE
	PRINT '   SCHEMA [Maintenance] already exists.'
GO

PRINT '--------------------------------------------------------------------------------------------------------------'
PRINT '' 