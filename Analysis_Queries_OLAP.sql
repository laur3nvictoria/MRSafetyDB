USE CMI_MRSafetyDB_OLAP
GO

/**** NUMBER OF JOBS PER SITE ****/

SELECT
	ds.SiteLongName,
	dd.Year,
	dd.Quarter,
	COUNT(fj.JobId) AS JobCount
FROM FactJob fj
INNER JOIN DimSite ds ON fj.SiteKey = ds.SiteKey
INNER JOIN DimDate dd ON fj.DateJobLoggedKey = dd.DateKey
WHERE dd.Year = '2024'
GROUP BY ds.SiteLongName, dd.Year, dd.Quarter
ORDER BY ds.SiteLongName, dd.Quarter;

/**** PIVOT QUERY FOR JOBS PER SITE PER QUARTER ****/

SELECT
	ds.SiteLongName AS Site,
	SUM	(CASE WHEN dd.Quarter = 1 THEN 1 ELSE 0 END) AS Q1,
	SUM (CASE WHEN dd.Quarter = 2 THEN 1 ELSE 0 END) AS Q2, 
	SUM (CASE WHEN dd.Quarter = 3 THEN 1 ELSE 0 END) AS Q3,
	SUM (CASE WHEN dd.Quarter = 4 THEN 1 ELSE 0 END) AS Q4, 
	COUNT (fj.JobKey) AS Total
FROM FactJob fj
INNER JOIN DimSite ds ON fj.SiteKey = ds.SiteKey
INNER JOIN DimDate dd ON fj.DateQueryReceivedKey = dd.DateKey
WHERE dd.Year = 2024
GROUP BY ds.SiteLongName
ORDER BY ds.SiteLongName;