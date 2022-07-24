### **prerequisites**
---
- Create the database "naverland" with mysql
~~~sql
CREATE database databaseName DEFAULT CHARACTER SET utf8;
~~~
- Create tables below in the database
~~~sql
CREATE TABLE city (cityNo VARCHAR(10), city VARCHAR(30));
~~~

~~~sql
CREATE table gu (guNo VARCHAR(10), guName VARCHAR(30), cityNo VARCHAR(10));
~~~

~~~sql
CREATE TABLE dong (dongNo VARCHAR(10), dongName VARCHAR(30), guNo VARCHAR(10));
~~~

~~~sql
CREATE TABLE complex (complexNo VARCHAR(10), name VARCHAR(100), dongNo VARCHAR(10), realEstateTypeCode VARCHAR(10), cortarAddress VARCHAR(100), detailAddress VARCHAR(100), totalHouseholdCount INTEGER, totalBuildingCount INTEGER, highFloor INTEGER, lowFloor INTEGER, useApproveYmd TIMESTAMP)
~~~

~~~sql
CREATE TABLE article (articleNo VARCHAR(10), articleName  VARCHAR(100), complexNo VARCHAR(10))
~~~

~~~sql
CREATE TABLE articleInfo (articleNo VARCHAR(10), articleName VARCHAR(100), hscpNo VARCHAR(10), ptpNo VARCHAR(10), ptpName VARCHAR(100), exposeStartYMD TIMESTAMP, exposeEndYMD TIMESTAMP, articleConfirmYMD TIMESTAMP, aptName VARCHAR(100), aptHouseholdCount INTEGER, aptConstructionCompanyName VARCHAR(100), aptUseApproveYmd TIMESTAMP, totalDongCount INTEGER, realestateTypeCode VARCHAR(10), tradeTypeName VARCHAR(100), verificationTypeCode VARCHAR(10), cityName VARCHAR(100), divisionName VARCHAR(100), sectionName VARCHAR(100), householdCountByPtp INTEGER, walkingTimeToNearSubway INTEGER, detailAddress VARCHAR(100), roomCount INTEGER, bathroomCount INTEGER, moveInTypeCode VARCHAR(10), moveInDiscussionPossibleYN VARCHAR(10), monthlyManagementCost INTEGER, monthlyManagementCostIncludeItemName VARCHAR(100), buildingName VARCHAR(100), articleFeatureDescription VARCHAR(100), detailDescription VARCHAR(200), floorLayerName VARCHAR(100), floorInfo VARCHAR(100), priceChangeState VARCHAR(10), dealOrWarrantPrc VARCHAR(10), direction TEXT, latitude INTEGER, longitude INTEGER, entranceTypeName VARCHAR(100), rentPrice INTEGER, dealPrice INTEGER, warrantPrice INTEGER, allWarrantPrice INTEGER, financePrice INTEGER, premiumPrice INTEGER, isalePrice INTEGER, allRentPrice INTEGER, priceBySpace INTEGER, bondPrice INTEGER, middlePayment INTEGER, realtorName VARCHAR(100), representativeName VARCHAR(100), address VARCHAR(100), representativeTelNo VARCHAR(10), cellPhoneNo TEXT, supplySpace REAL, exclusiveSpace REAL, exclusiveRate FLOAT, tagList TEXT)
~~~

~~~sql
CREATE TABLE complexPrice (complexNo VARCHAR(10), ptpNo VARCHAR(10), date TIMESTAMP, price INTEGER, pct_change FLOAT);
~~~