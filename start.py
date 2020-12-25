import glob
import json
import logging
import os
import random
import time
import requests
from lxml import html


logging.basicConfig(filename='parser.log', level=logging.INFO, filemode='w',
                    format='%(asctime)s %(levelname)s %(message)s')
logger=logging.getLogger(__name__)

class Parser:

    URL = 'https://www.imdb.com'

    UA = 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.121 Safari/537.36'

    DUMP_DIR = 'html'

    # Количество попыток если ошибка
    MAX_ATTEMPT = 3
    # Пауза между ошибками (сек)
    WAIT_FOR_ERROR = 5
    # Таймаут
    HTTP_TIMEOUT = 30

    # Диапазон случайной задержки
    WAIT_MIN = 1
    WAIT_MAX = 3

    def __init__(self, max_films):
        """Инициируем все внутренние переменные для начала"""
        logging.info('Init parser, max_films=%d' % max_films)
        self.session = requests.Session()
        self.referer = 'https://www.google.com'
        self.headers_get = {
            'User-Agent': self.UA}
        self.max_films = max_films
        self.data = []
        self.request_counter = 1
        self.dump_clear()

    def dump_clear(self):
        """Очистка папки с HTML"""
        logging.info('Clear html dir '+self.DUMP_DIR)
        files = glob.glob(self.DUMP_DIR + '/*')
        for f in files:
            os.remove(f)



    def send_get(self, url=None, request_name=''):
        """Обёртка для requests.get"""
        logging.info('send_get url=' + url)

        # Манипуляции с referer, чтобы выдать себя за переходы в браузере
        self.headers_get['referer'] = self.referer

        success = False
        attempt = 0
        response = None

        # Пока не получим ответ, запрашиваем (но не больше MAX_ATTEMPT раз)
        while not success and attempt < self.MAX_ATTEMPT:
            attempt += 1
            try:
                response = self.session.get(url, headers=self.headers_get, allow_redirects=True,
                                            timeout=self.HTTP_TIMEOUT)
                success = True
            except:
                logging.info('send_get error. wait and retry')
                time.sleep(self.WAIT_FOR_ERROR)
            finally:
                logging.info('send_get result=%s, attempt=%d' % (str(success), attempt))
                self.sleep()

        # Созраняем полученный файл
        if response: self._dump(response.content, request_name)
        # Если так и не получили ответ, генериркем исключение
        if not success:
            raise ConnectionError
        return response


    def run(self, list_page):
        """Основная логика перебора страниц IMDB"""
        next_page = list_page

        while next_page:
            # Запрашиваем страницу со списком фильмов
            response = self.send_get(next_page, request_name='filmlist')
            tree = html.fromstring(response.content)
            # Парсим список фильмов(только ссылки на фильмы)
            film_links = self.parse_film_links(tree)
            # Парсим ссылку на следующую страницу со списком фильмов
            next_url = self.parse_next_url(tree)
            self.referer = next_page
            # Перебираем ссылки на фильмы
            for link in film_links:
                try:
                    # Получаем данные о фильме
                    film = self.get_film_info(self.URL+link)
                    self.data.append(film)
                except Exception as ex:
                    logging.error(ex, exc_info=True)
                if len(self.data) >= self.max_films:
                    # Если всё уперлись в заданный лимит, прекращаем просмотр каталога
                    return
            # Формируем полную ссылку на следующую страницу каталога
            next_page = self.URL+next_url if next_url else None

    def get_film_info(self, film_page):
        """Разбор данных о фильме
        film_page - Полный url на страницу фильма
        """
        logging.info('get_film_info')
        res = dict(imdb_url=film_page)
        # Запрос страницы
        response = self.send_get(film_page, request_name='filmpage')
        tree = html.fromstring(response.content)
        res['name'] = tree.xpath("//div[@class='title_wrapper']/h1/text()")[0].replace(u'\xa0', ' ').rstrip()
        res['genres'] = tree.xpath("//div[@class='subtext']/a[contains(@href,'genres')]/text()")
        res['rating'] = tree.xpath("//span[@itemprop='ratingValue']/text()")[0]
        res['stars'] = tree.xpath("//div[@class='credit_summary_item' and h4[text()='Stars:']]/a[contains(@href, '/name/nm')]/text()")

        res['details'] = {}
        sitelinks = tree.xpath("//div[@class='txt-block' and h4[text()='Official Sites:']]/a")
        # Если есть данные об официальных страницах, то приходится обходить их, чтобы получить конкретный url
        if sitelinks:
            sites = {}
            for a in sitelinks:
                logging.info('looking for external links')
                try:
                    # Многие сайты уже недоступны
                    r = self.send_get(self.URL+a.attrib.get('href'), request_name='external')
                    sites[a.text] = r.url
                except ConnectionError as ex:
                    logging.error(ex, exc_info=True)
            self.dict_set(res['details'], 'sites', sites)

        # Просто выдергиваем данные через XPath выражения
        self.dict_set(res['details'], 'country', tree.xpath("//div[@class='txt-block' and h4[text()='Country:']]/a/text()"))
        self.dict_set(res['details'], 'language', tree.xpath("//div[@class='txt-block' and h4[text()='Language:']]/a/text()"))
        self.dict_set(res['details'], 'release date', "".join(tree.xpath("//div[@class='txt-block' and h4[text()='Release Date:']]/text()")).strip())
        self.dict_set(res['details'], 'other name', "".join(tree.xpath("//div[@class='txt-block' and h4[text()='Also Known As:']]/text()")).strip())
        self.dict_set(res['details'], 'locations', self.get_xvalue(tree, "//div[@class='txt-block' and h4[text()='Filming Locations:']]/a/text()"))

        res['box office'] = {}
        self.dict_set(res['box office'], 'Budget', "".join(tree.xpath("//div[@class='txt-block' and h4[text()='Budget:']]/text()")).strip())
        self.dict_set(res['box office'], 'Opening Weekend USA', "".join(tree.xpath("//div[@class='txt-block' and h4[text()='Opening Weekend USA:']]/text()")).strip().strip(','))
        self.dict_set(res['box office'], 'Gross USA', "".join(tree.xpath("//div[@class='txt-block' and h4[text()='Gross USA:']]/text()")).strip())
        self.dict_set(res['box office'], 'Cumulative Worldwide Gross', "".join(tree.xpath("//div[@class='txt-block' and h4[text()='Cumulative Worldwide Gross:']]/text()")).strip())
        if res['box office'] == {}:
            del res['box office']

        res['technical specs'] = {}
        self.dict_set(res['technical specs'], 'Runtime', "".join(tree.xpath("//div[@class='txt-block' and h4[text()='Runtime:']]/time/text()")).strip())
        self.dict_set(res['technical specs'], 'Sound Mix', "".join(tree.xpath("//div[@class='txt-block' and h4[text()='Sound Mix:']]/descendant-or-self::*[text()!='Sound Mix:']/text()")).replace('\n','').replace('  ','')).replace(u'\xa0', ' ')
        self.dict_set(res['technical specs'], 'Color', "".join(tree.xpath("//div[@class='txt-block' and h4[text()='Color:']]/descendant-or-self::*[text()!='Color:']/text()")).replace('\n','').replace('  ',''))
        self.dict_set(res['technical specs'], 'Aspect Ratio', "".join(tree.xpath("//div[@class='txt-block' and h4[text()='Aspect Ratio:']]/text()")).strip())
        if res['technical specs'] == {}:
            del res['technical specs']

        logging.info(res)
        return res

    def dict_set(self, dict_var, key, value):
        """Обертка для присвоения: присваивает значение справочнику, только если значение заполнено. """
        if value:
            dict_var[key] = value

    def parse_next_url(self, tree):
        """Парсим ссылку на следующую страницу спика фильмов"""
        res = None
        links = tree.xpath("//a[text()='Next »']/@href")
        if links:
            res = links[0]
        return res


    def parse_film_links(self, tree):
        """Парсим список ссылок на фильмы"""
        res = tree.xpath("//div/h3/a[contains(@href, '/title/')]/@href")
        return res


    def _dump(self, text, request_name=''):
        """Запись данных в файл. Используется для того чтобы сохранять все полученные файлы"""
        f = open(self.DUMP_DIR + '/' + str(self.request_counter).rjust(5, '0') + '-' + request_name + '.html', 'wb')
        self.request_counter += 1
        f.write(text)
        f.close()

    def sleep(self):
        """Случайная задержка"""
        time.sleep(random.randint(self.WAIT_MIN, self.WAIT_MAX))

    def get_xvalue(self, tree, xpath, n=0):
        """Получить один элемент из списка по xpath"""
        tags = tree.xpath(xpath)
        return tags[n] if tags else ''


    def save(self, filename):
        """Сохраняем все данные, как json"""
        logging.info('Save to file %s, film count: %d' % (filename, len(self.data)))
        with open(filename, 'w') as fp:
            json.dump(self.data, fp)



first_page = 'https://www.imdb.com/search/title/?title_type=feature&release_date=2000-02-25,2020-05-28&user_rating=4.0,10.0&genres=comedy&countries=us'

parser = Parser(1000)
parser.run(first_page)
parser.save('output.json')