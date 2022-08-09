import pymysql
from datetime import datetime
from pydantic import BaseModel, validator
from datetime import datetime

class returnModel(BaseModel):

    city : str
    guName: str
    dongName: str
    complexName : str 
    address: str 
    complexNo: str
    ptpNo: str
    first_date: str
    last_date: str
    first_price: int
    last_price: int
    cul_return: str
    cagr: str
    dealPrice = int
    totalHouseholdCount: int 
    totalBuildingCount: int
    useApproveYmd: str
    supplySpace: float
    exclusiveSpace: float
    roomCount: str
    bathroomCount : str 
    entranceType: str

    @validator('*', pre=True, always=True)
    def none_(cls, v):
        if not v:
            return '-'
        return v

    @validator('first_date', 'last_date', 'useApproveYmd', pre=True, always=True)
    def datetime_(cls, v):
        return datetime(v.year, v.month, v.day).strftime(format='%Y-%m-%d')

    @validator('cul_return', 'cagr', pre=True, always=True)
    def symbol_(cls, v):
        return v[:-1]

class Query:

    def __init__(self, db):
        self.db = db

    def get_return(self, start='2022-01-01', end=None, where=None):
        if not end:
            end = datetime.now().strftime(format='%Y-%m-%d')
        
        grouped = f'''
                with 
                grouped as
                (
                select max(num) as mnum, complexNo, ptpNo, date
                from complexprice
                where date > '{start}' and date < '{end}'
                group by complexNo, ptpNo, date
                )
                '''

        laged = f'''
                laged as
                (
                select 
                    origin.num, origin.complexNo, origin.ptpNo, origin.date, 
                    lag(origin.date, 1) over (PARTITION BY complexNo, ptpNo order by num) as pre_date, 
                    origin.price, 
                    lag(origin.price, 1) over (PARTITION BY complexNo, ptpNo order by num) as pre_price
                from complexprice as origin
                right join
                grouped
                on origin.num = grouped.mnum
                )
                '''

        cul =f'''
                cul as
                (
                select laged.num, laged.complexNo, laged.ptpNo, laged.date, laged.pre_date, 
                DATEDIFF(laged.date, laged.pre_date) as term,
                laged.price, laged.pre_price,
                round((laged.price/laged.pre_price-1)*100, 2) as pct_change,
                power(10, SUM(LOG10(laged.price/laged.pre_price)) OVER (PARTITION BY complexNo, ptpNo ORDER BY laged.date)) as cul_return
                from laged
                )
                '''

        cul_ = f'''
                cul_ as
                (
                select cul.num, cul.complexNo, cul.ptpNo, cul.date, cul.pre_date, cul.term,
                        sum(term) over (PARTITION BY complexNo, ptpNo ORDER BY cul.date) as cul_term,
                        cul.price, cul.pre_price, cul.pct_change, cul.cul_return as cul_return
                from cul
                ),

                final as
                (
                select 
                    cul_.num, cul_.complexNo, cul_.ptpNo, cul_.date, cul_.pre_date, cul_.term, cul_.cul_term, cul_.price, cul_.pre_price, cul_.pct_change, round((cul_.cul_return-1)*100,2) as cul_return,
                    round((power(power(cul_.cul_return, (1/cul_.cul_term)),365)-1)*100,2) as cagr
                from cul_
                )'''

        final_grouped = f'''
            final_grouped as
            (
            select max(final.num) as maxnum, min(final.num) as minnum, final.complexNo, final.ptpNo
            from final
            group by final.complexNo, final.ptpNo
            )
            '''

        maxnum_final_grouped = f'''
            maxnum_final_grouped as
            (
            select 
                origin_final.num, origin_final.complexNo, origin_final.ptpNo, origin_final.date, 
                origin_final.price, origin_final.cul_return, origin_final.cagr
            from final as origin_final
            right join final_grouped
            on origin_final.num = final_grouped.maxnum
            )'''

        minnum_final_grouped = f'''
            minnum_final_grouped as
            (
            select 
                origin_final.num, origin_final.complexNo, origin_final.ptpNo, origin_final.date, 
                origin_final.price, origin_final.cul_return, origin_final.cagr
            from final as origin_final
            right join final_grouped
            on origin_final.num = final_grouped.minnum
            )'''

        cagr_return = f'''
            cagr_return as
            (
            select 
                maxnum_final_grouped.num, maxnum_final_grouped.complexNo, maxnum_final_grouped.ptpNo, 
                minnum_final_grouped.date as first_date, maxnum_final_grouped.date as last_date,  
                minnum_final_grouped.price as first_price, maxnum_final_grouped.price as last_price,  
                maxnum_final_grouped.cul_return, maxnum_final_grouped.cagr
            from maxnum_final_grouped
            right join minnum_final_grouped 
            on maxnum_final_grouped.complexNo = minnum_final_grouped.complexNo and maxnum_final_grouped.ptpNo = minnum_final_grouped.ptpNo
            where maxnum_final_grouped.cul_return is not null
            order by maxnum_final_grouped.num
            )'''

        complex_info = f'''
            complex_info as
            (
            select
            cagr_return.num, 
            complex.dongNo, 
            complex.name as complexName,
            concat(complex.cortarAddress, " " ,complex.detailAddress) as address, 
            cagr_return.complexNo, 
            cagr_return.ptpNo, 
            cagr_return.first_date, 
            cagr_return.last_date,  
            cagr_return.first_price, 
            cagr_return.last_price,  
            cagr_return.cul_return, 
            cagr_return.cagr,
            complex.totalHouseholdCount, 
            complex.totalBuildingCount, 
            complex.highFloor, 
            complex.lowFloor, 
            complex.useApproveYmd
            from cagr_return
            left join complex
            on cagr_return.complexNo = complex.complexNo
            )'''

        dong_complex_info = f'''
            dong_complex_info as 
            (
            select
            dong.guNo, dong.dongName,
            complex_info.*
            from complex_info
            left join dong
            on complex_info.dongNo = dong.dongNo
            )'''

        gu_dong_complex_info = f'''
            gu_dong_complex_info as
            (
            select
            gu.cityNo, gu.guName, 
            dong_complex_info.*
            from dong_complex_info
            left join gu
            on dong_complex_info.guNo = gu.guNo
            )'''

        city_gu_dong_complex_info = f'''
            city_gu_dong_complex_info as
            (
            select
            city.city,
            gu_dong_complex_info.num,
            gu_dong_complex_info.guName, 
            gu_dong_complex_info.dongName, 
            gu_dong_complex_info.complexName, 
            gu_dong_complex_info.address, 
            gu_dong_complex_info.complexNo, 
            gu_dong_complex_info.ptpNo, 
            gu_dong_complex_info.first_date, 
            gu_dong_complex_info.last_date, 
            format(gu_dong_complex_info.first_price, '#,#') as first_price,
            format(gu_dong_complex_info.last_price, '#,#') as last_price, 
            concat(coalesce(gu_dong_complex_info.cul_return, '-'),'%') as cul_return,
            concat(coalesce(gu_dong_complex_info.cagr, '-'), '%') as cagr, 
            format(gu_dong_complex_info.totalHouseholdCount, '#,#') as totalHouseholdCount, 
            format(gu_dong_complex_info.totalBuildingCount, '#,#') as totalBuildingCount,
            format(gu_dong_complex_info.highFloor, '#,#') as highFloor,
            format(gu_dong_complex_info.lowFloor, '#,#') as lowFloor,
            gu_dong_complex_info.useApproveYmd
            from gu_dong_complex_info
            left join city
            on gu_dong_complex_info.cityNo = city.cityNo
            )'''

        min_dealPrice = f'''
            min_dealPrice as
            (
            select hscpNo, ptpNo, min(dealPrice) as minPrice
            from articleinfo
            group by hscpNo, ptpNo
            )'''


        min_no = f'''
            min_no as
            (
            select articleinfo.hscpNo, articleinfo.ptpNo, min(articleinfo.articleNo) as minNo
            from articleinfo
            right join min_dealPrice
            on articleinfo.hscpNo = min_dealPrice.hscpNo and articleinfo.ptpNo = min_dealPrice.ptpNo and articleinfo.dealPrice = min_dealPrice.minPrice
            group by hscpNo, ptpNo
            )'''


        article_info = f'''
            article_info as
            (
            select 
            b.minNo, 
            a.hscpNo, 
            a.ptpNo, 
            a.dealPrice,
            a.supplySpace,
            a.exclusiveSpace,
            a.roomCount,
            a.bathroomCount,
            a.entranceTypeName
            from articleinfo as a
            right join min_no as b
            on a.articleNo = b.minNo
            )'''

        select_join = f'''
            select
            city_gu_dong_complex_info.city,
            city_gu_dong_complex_info.guName, 
            city_gu_dong_complex_info.dongName, 
            city_gu_dong_complex_info.complexName, 
            city_gu_dong_complex_info.address, 
            city_gu_dong_complex_info.complexNo, 
            city_gu_dong_complex_info.ptpNo, 
            city_gu_dong_complex_info.first_date, 
            city_gu_dong_complex_info.last_date, 
            format(city_gu_dong_complex_info.first_price, '#,#') as first_price,
            format(city_gu_dong_complex_info.last_price, '#,#') as last_price, 
            concat(coalesce(city_gu_dong_complex_info.cul_return, '-'),'%') as cul_return,
            concat(coalesce(city_gu_dong_complex_info.cagr, '-'), '%') as cagr,
            article_info.dealPrice,
            format(city_gu_dong_complex_info.totalHouseholdCount, '#,#') as totalHouseholdCount, 
            format(city_gu_dong_complex_info.totalBuildingCount, '#,#') as totalBuildingCount,
            city_gu_dong_complex_info.useApproveYmd,
            article_info.supplySpace,
            article_info.exclusiveSpace,
            article_info.roomCount,
            article_info.bathroomCount,
            coalesce(article_info.entranceTypeName, '-') as entranceType
            from city_gu_dong_complex_info
            join article_info
            on city_gu_dong_complex_info.complexNo = article_info.hscpNo and city_gu_dong_complex_info.ptpNo = article_info.ptpNo'''

        if not where:
            where = ''

        order = f'''
            order by city_gu_dong_complex_info.num'''

        with_clause = ','.join([grouped,laged, cul, cul_, final_grouped, maxnum_final_grouped, minnum_final_grouped, cagr_return, complex_info, dong_complex_info, gu_dong_complex_info, city_gu_dong_complex_info, min_dealPrice, min_no ,article_info])
        sql = ' '.join([with_clause, select_join, where, order]) + ';'
        print(sql)
        c = self.db.cursor()
        c.execute(sql)
        field = [column[0] for column in c.description]
        return (returnModel(**dict(zip(field, row))) for row in c.fetchall())



class Return:

    def __init__(self):
        self.db = pymysql.connect(host='192.168.35.243', port=3306, user='ajcltm', passwd='2642805Ab!', db='naverland', charset='utf8mb4')
        self.query = Query(self.db)