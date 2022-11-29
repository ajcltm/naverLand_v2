from db import cityDumper, guDumper, dongDumper, complexDumper, articleDumper, articleInfoDumper, complexPriceDumper
from scrap import config

class NaverLandDumper:

    def execute(self):
        project_folder_path = config.main_path
        dbname = 'naverland'
        # cityDumper.CityDumper().execute()
        # guDumper.GuDumper(folder_path=project_folder_path.joinpath('0. gu'), db_name=dbname).execute()
        # dongDumper.DongDumper(folder_path=project_folder_path.joinpath('1. dong'), db_name=dbname).execute()
        # complexDumper.ComplexDumper(folder_path=project_folder_path.joinpath('2. complex'), db_name=dbname).execute()
        # articleDumper.ArticleDumper(folder_path=project_folder_path.joinpath('3. article'), db_name=dbname).execute()
        articleInfoDumper.ArticleInfoDumper(folder_path=project_folder_path.joinpath('4. articleInfo'), db_name=dbname, start_file_name='articleNo_2241250174.pickle').execute()
        complexPriceDumper.ComplexPriceDumper(folder_path=project_folder_path.joinpath('5. complexPrice'), db_name=dbname).execute()


if __name__ == '__main__':
    NaverLandDumper().execute()