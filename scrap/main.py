from scrap import config, guScraper, dongScraper, complexScraper, articleScrpaer, articleInfo, complexPriceScraper 
import os
import shutil

class WorkSpace:

    def __init__(self, subFolderList):
        self.subFolderList = subFolderList

    def create_workSpace(self):
        if not os.path.exists(config.main_path):
            os.makedirs(config.main_path)
        for i in self.subFolderList:
            if not os.path.exists(config.main_path.joinpath(i)):
                os.makedirs(config.main_path.joinpath(i))

class CopyAndPaste:

    def get_subFolderList(self, num):
        project_folder_name = os.listdir(config.parent_path)[num]
        sub_folders_names = ['0. gu', '1. dong', '2. complex']
        return[config.parent_path.joinpath(project_folder_name, i) for i in sub_folders_names]
        
    def copyAndPaste(self):
        copy_target_list = self.get_subFolderList(-2)
        paste_target_list = self.get_subFolderList(-1)

        for i, item in enumerate(copy_target_list):
            if not os.path.exists(paste_target_list[i]):
                shutil.copytree(item, paste_target_list[i])

class FullScraper:

    def execute(self):

        subFolderList = ['0. gu', '1. dong', '2. complex', '3. article', '4. articleInfo', '5. complexPrice']
        WorkSpace(subFolderList).create_workSpace()

        guScraper.GuScraper().execute()
        dongScraper.DongScraper().execute()
        complexScraper.ComplexScraper().execute()
        articleScrpaer.ArticleScraper().execute()
        articleInfo.ArticleInfoScraper().execute()
        complexPriceScraper.ComplexPriceScraper().execute()

class PartScraper:

    def execute(self):
        subFolderList = ['3. article', '4. articleInfo', '5. complexPrice']
        WorkSpace(subFolderList).create_workSpace()

        CopyAndPaste().copyAndPaste()

        articleScrpaer.ArticleScraper().execute()
        articleInfo.ArticleInfoScraper().execute()
        complexPriceScraper.ComplexPriceScraper().execute()


if __name__ == '__main__':
    PartScraper().execute()
