SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[CollSearchDetail](
	[hit_num] [int] NULL,
	[num_proc] [int] NULL,
	[service_search_time] [int] NULL,
	[kernel_search_time] [int] NULL,
	[collection] [nvarchar](100) NULL,
	[k2collsearch_id] [int] NOT NULL,
	[id] [int] IDENTITY(1,1) NOT NULL,
 CONSTRAINT [PK_CollSearchDetail] PRIMARY KEY CLUSTERED
(
	[id] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
) ON [PRIMARY]

GO

CREATE TABLE [dbo].[CollSearchFields](
	[name] [nvarchar](100) NULL,
	[id] [int] IDENTITY(1,1) NOT NULL,
	[k2collsearch_id] [int] NOT NULL,
 CONSTRAINT [PK_CollSearchFields] PRIMARY KEY CLUSTERED
(
	[id] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
) ON [PRIMARY]

GO

CREATE TABLE [dbo].[K2AssistCollections](
	[name] [nvarchar](100) NULL,
	[id] [int] IDENTITY(1,1) NOT NULL,
	[k2assistsuggest_id] [int] NOT NULL,
 CONSTRAINT [PK_K2AssistCollections] PRIMARY KEY CLUSTERED
(
	[id] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
) ON [PRIMARY]

GO

CREATE TABLE [dbo].[K2AssistSuggest](
	[client] [nvarchar](100) NULL,
	[time] [datetime] NULL,
	[suggest_time] [int] NULL,
	[query] [text] NULL,
	[id] [int] IDENTITY(1,1) NOT NULL,
 CONSTRAINT [PK_K2AssistSuggest] PRIMARY KEY CLUSTERED
(
	[id] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]

GO

CREATE TABLE [dbo].[K2AssistSuggestions](
	[id] [int] IDENTITY(1,1) NOT NULL,
	[query_suggestion] [text] NOT NULL,
	[k2assistsuggest_id] [int] NOT NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]

GO

CREATE TABLE [dbo].[K2CollSearch](
	[time] [datetime] NULL,
	[search_time] [int] NULL,
	[service_search_time] [int] NULL,
	[hit_num] [int] NULL,
	[total_docs] [int] NULL,
	[query] [nvarchar](4000) NULL,
	[source_query] [ntext] NULL,
	[client] [nvarchar](100) NULL,
	[from_cache] [int] NULL,
	[id] [int] IDENTITY(1,1) NOT NULL,
	[query_parser] [nvarchar](4000) NULL,
 CONSTRAINT [PK_K2CollSearch] PRIMARY KEY CLUSTERED
(
	[id] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]

GO

CREATE TABLE [dbo].[K2DocStream](
	[time] [datetime] NULL,
	[dockey] [nvarchar](100) NOT NULL,
	[query] [text] NULL,
	[client] [nvarchar](100) NULL,
	[id] [int] IDENTITY(1,1) NOT NULL,
 CONSTRAINT [PK_K2DocStream] PRIMARY KEY CLUSTERED
(
	[id] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]

GO

CREATE TABLE [dbo].[K2DocStreamFields](
	[name] [nvarchar](100) NULL,
	[id] [int] IDENTITY(1,1) NOT NULL,
	[k2docstream_id] [int] NOT NULL,
 CONSTRAINT [PK_K2DocStreamFields] PRIMARY KEY CLUSTERED
(
	[id] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
) ON [PRIMARY]

GO
