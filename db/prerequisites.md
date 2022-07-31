### **prerequisites**
---
- Create the database "naverland" with mysql
~~~sql
CREATE database naverland DEFAULT CHARACTER SET utf8mb4;
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
CREATE TABLE complex (complexNo VARCHAR(10), name VARCHAR(100), dongNo VARCHAR(10), realEstateTypeCode VARCHAR(10), cortarAddress VARCHAR(100), detailAddress VARCHAR(100), totalHouseholdCount INTEGER, totalBuildingCount INTEGER, highFloor INTEGER, lowFloor INTEGER, useApproveYmd DATE);
~~~

~~~sql
CREATE TABLE article (articleNo VARCHAR(10), articleName  VARCHAR(100), complexNo VARCHAR(10));
~~~

~~~sql
CREATE TABLE articleInfo (articleNo VARCHAR(10), articleName VARCHAR(100), hscpNo VARCHAR(10), ptpNo VARCHAR(10), ptpName VARCHAR(100), exposeStartYMD DATE, exposeEndYMD DATE, articleConfirmYMD DATE, aptName VARCHAR(100), aptHouseholdCount INTEGER, aptConstructionCompanyName VARCHAR(100), aptUseApproveYmd DATE, totalDongCount INTEGER, realestateTypeCode VARCHAR(10), tradeTypeName VARCHAR(100), verificationTypeCode VARCHAR(10), cityName VARCHAR(100), divisionName VARCHAR(100), sectionName VARCHAR(100), householdCountByPtp INTEGER, walkingTimeToNearSubway INTEGER, detailAddress VARCHAR(100), roomCount INTEGER, bathroomCount INTEGER, moveInTypeCode VARCHAR(10), moveInDiscussionPossibleYN VARCHAR(10), monthlyManagementCost INTEGER, monthlyManagementCostIncludeItemName VARCHAR(100), buildingName VARCHAR(100), articleFeatureDescription TEXT, detailDescription TEXT, floorLayerName VARCHAR(100), floorInfo VARCHAR(100), priceChangeState VARCHAR(10), dealOrWarrantPrc VARCHAR(10), direction VARCHAR(10), latitude INTEGER, longitude INTEGER, entranceTypeName VARCHAR(100), rentPrice INTEGER, dealPrice INTEGER, warrantPrice INTEGER, allWarrantPrice INTEGER, financePrice INTEGER, premiumPrice INTEGER, isalePrice INTEGER, allRentPrice INTEGER, priceBySpace INTEGER, bondPrice INTEGER, middlePayment INTEGER, realtorName VARCHAR(100), representativeName VARCHAR(100), address VARCHAR(100), representativeTelNo VARCHAR(50), cellPhoneNo VARCHAR(30), supplySpace REAL, exclusiveSpace REAL, exclusiveRate FLOAT, tagList TEXT);
~~~

~~~sql
CREATE TABLE complexPrice (num int(10) NOT NULL AUTO_INCREMENT PRIMARY KEY, complexNo VARCHAR(10), ptpNo VARCHAR(10), date DATE, price INT);
~~~