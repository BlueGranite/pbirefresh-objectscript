/****** Object:  Schema [ETL]    Script Date: 6/17/2022 2:08:25 PM ******/
CREATE SCHEMA [ETL]
GO
/****** Object:  Table [ETL].[PBI_Object]    Script Date: 6/17/2022 2:08:25 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [ETL].[PBI_Object](
	[pbi_object_key] [int] IDENTITY(1,1) NOT NULL,
	[workspace_id] [uniqueidentifier] NOT NULL,
	[workspace_name] [nvarchar](200) NOT NULL,
	[object_id] [uniqueidentifier] NOT NULL,
	[object_name] [nvarchar](500) NOT NULL,
	[object_type] [nvarchar](200) NOT NULL,
 CONSTRAINT [pkPbiObject] PRIMARY KEY CLUSTERED 
(
	[pbi_object_key] ASC
)WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  View [ETL].[PBI_Object_v]    Script Date: 6/17/2022 2:08:25 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE VIEW [ETL].[PBI_Object_v] AS
SELECT [pbi_object_key]
      ,[workspace_id]
      ,[workspace_name] AS Workspace
      ,[object_id]
      ,[object_name] AS [Object Name]
      ,[object_type] AS [Object Type]
  FROM [ETL].[PBI_Object]
GO
/****** Object:  Table [ETL].[PBI_Refresh_History]    Script Date: 6/17/2022 2:08:25 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [ETL].[PBI_Refresh_History](
	[pbi_object_key] [int] NOT NULL,
	[request_id] [uniqueidentifier] NULL,
	[id] [nvarchar](200) NOT NULL,
	[refresh_type] [nvarchar](200) NOT NULL,
	[start_time] [datetime2](3) NOT NULL,
	[end_time] [datetime2](3) NULL,
	[service_exception_json] [nvarchar](max) NULL,
	[status] [nvarchar](200) NOT NULL,
	[LastUpdatedDateTimeUtc] [datetime2](3) NOT NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  View [ETL].[PBI_Refresh_History_v]    Script Date: 6/17/2022 2:08:25 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE VIEW [ETL].[PBI_Refresh_History_v] AS
SELECT [pbi_object_key]
      ,[request_id]
      ,[id]
      ,[refresh_type] AS [Refresh Type]
      ,[start_time]
      ,[end_time]
	  ,CAST( [start_time] AT TIME ZONE 'UTC' AT TIME ZONE 'Eastern Standard Time' AS DATE ) AS start_date_eastern
	  ,LEFT(CAST( [start_time] AT TIME ZONE 'UTC' AT TIME ZONE 'Eastern Standard Time' AS TIME( 0 ) ),5) AS start_time_eastern
      -- Set end_date_eastern and end_time_eastern keys to current date/time for refreshes in progress so that their duration at query time can be determined
      ,CASE WHEN [end_time] IS NULL
            THEN CAST( GETUTCDATE() AT TIME ZONE 'UTC' AT TIME ZONE 'Eastern Standard Time' AS DATE )
            ELSE CAST( [end_time] AT TIME ZONE 'UTC' AT TIME ZONE 'Eastern Standard Time' AS DATE ) END AS end_date_eastern
	  ,CASE WHEN [end_time] IS NULL
            THEN LEFT( CAST( GETUTCDATE() AT TIME ZONE 'UTC' AT TIME ZONE 'Eastern Standard Time' AS TIME( 0 ) ),5)
            ELSE LEFT( CAST( [end_time] AT TIME ZONE 'UTC' AT TIME ZONE 'Eastern Standard Time' AS TIME( 0 ) ),5) END AS end_time_eastern
      ,CASE WHEN [status] = 'Failed' AND service_exception_json IS NULL THEN '{"errorCode":"Unknown or not available. Check the refresh history in the Power BI service for more details."}'
	        ELSE service_exception_json END AS [Service Exception]
	  ,DATEDIFF ( minute , [start_time] , [end_time] ) AS duration_minutes
	  ,CASE WHEN [status] = 'Success' THEN 'Completed' ELSE [status] END AS [Refresh Status]
      ,LastUpdatedDateTimeUtc AS [Last Updated Datetime UTC]
  FROM [ETL].[PBI_Refresh_History]
GO
ALTER TABLE [ETL].[PBI_Refresh_History] ADD  CONSTRAINT [dfPbiRefreshHistory_LastUpdatedDateTimeUtc]  DEFAULT (getutcdate()) FOR [LastUpdatedDateTimeUtc]
GO
/****** Object:  StoredProcedure [ETL].[uspLoad_PBI_Refresh_History]    Script Date: 6/17/2022 2:08:25 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [ETL].[uspLoad_PBI_Refresh_History]
   @workspaceId uniqueidentifier
  ,@objectId uniqueidentifier
  ,@refreshHistory nvarchar(max)
AS  
BEGIN

SET NOCOUNT ON;

WITH refreshData AS (
SELECT
  COALESCE(o.pbi_object_key, -1) AS pbi_object_key
 ,requestId AS request_id
 ,id
 ,refreshType AS refresh_type
 ,startTime AS start_time
 ,endTime AS end_time
 ,serviceExceptionJson AS service_exception_json
 ,[status]
FROM OpenJson(@refreshHistory, '$.value')
CROSS APPLY OPENJSON([value])
WITH (
	  requestId            uniqueidentifier '$.requestId'
	 ,id                   nvarchar(200)    '$.id'
	 ,refreshType          nvarchar(200)    '$.refreshType'
	 ,startTime            datetime2(3)     '$.startTime'
	 ,endTime              datetime2(3)     '$.endTime'
	 ,serviceExceptionJson nvarchar(MAX)    '$.serviceExceptionJson'
	 ,[status]             nvarchar(200)    '$.status'
	 )
LEFT JOIN ETL.PBI_Object AS o
  ON @workspaceId = o.workspace_id
  AND @objectId = o.[object_id]
)

MERGE ETL.PBI_Refresh_History AS tgt  
USING (SELECT * FROM refreshData) as src
ON (tgt.pbi_object_key = src.pbi_object_key
AND tgt.id = src.id)
WHEN MATCHED and tgt.[status] IN ('Unknown', 'InProgress', 'Cancelling') THEN
    UPDATE SET tgt.end_time = src.end_time
	          ,tgt.service_exception_json = src.service_exception_json
	          ,tgt.[status] = src.[status]
              ,tgt.LastUpdatedDateTimeUtc = GETUTCDATE()
WHEN NOT MATCHED THEN  
    INSERT (pbi_object_key
           ,request_id
           ,id
           ,refresh_type
           ,start_time
           ,end_time
           ,[status])  
    VALUES (src.pbi_object_key
           ,src.request_id
           ,src.id
           ,src.refresh_type
           ,src.start_time
           ,src.end_time
           ,src.[status]);

END
GO
