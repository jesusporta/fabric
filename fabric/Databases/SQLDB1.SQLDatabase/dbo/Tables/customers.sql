CREATE TABLE [dbo].[customers] (
    [CustomerID]  INT           NOT NULL,
    [FirstName]   VARCHAR (50)  NOT NULL,
    [LastName]    VARCHAR (50)  NOT NULL,
    [Email]       VARCHAR (100) NULL,
    [Age]         INT           NULL,
    [City]        VARCHAR (50)  NULL,
    [CreatedDate] DATE          NULL,
    PRIMARY KEY CLUSTERED ([CustomerID] ASC)
);


GO

