import csv
import logging
import sys
import time
from io import StringIO
from itertools import count
from pathlib import Path

import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.common.exceptions import ElementClickInterceptedException
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from webdriver_manager.chrome import ChromeDriverManager


class YachtPartsParserError(Exception):
    pass


class ProductLinksNotFound(YachtPartsParserError):
    def __init__(self, link):
        self.link = link
        super().__init__(
            f'Не удалось получить ссылки на товары со страницы {link}')


class YachtPartsParser:
    """
    Позволяет получить данные о товарах сайта https://yacht-parts.ru/.
    """

    def __init__(self,
                 timeout=5,
                 headless=False,
                 log_to_file=True,
                 fields_order = ('article', 'name', 'brand', 'options',
                                 'price', 'category',
                                 'preview_description', 'main_description',
                                 'image_links', 'product_link')
                 ):
        """

        :param timeout: Время паузы между запросами к сайту
        :param headless: Если True, браузер будет работать без GUI
        :param log_to_file: Если True, записи журнала будут сохраняться в файл
        :param fields_order: Порядок полей в csv файле
        """
        self.base_link = 'https://yacht-parts.ru/'
        self.timeout = timeout
        self.data_save_path = Path('yacht-parts.ru_catalog.csv')
        self.collected_links_path = Path('collected_links.txt')
        self.fields_order = fields_order

        self.logger = logging.getLogger(__name__)
        # Предотвратит дублирование сообщений в корневом журнале
        self.logger.propagate = False
        self.logger.setLevel(logging.INFO)
        # Создание обработчика для сохранения сообщений журнала в файл
        if log_to_file:
            fh = logging.FileHandler(filename='logs.txt',
                                     mode='a', encoding='utf-8')
            fh.setLevel(logging.INFO)
            formatter = logging.Formatter(
                fmt='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                datefmt='%H:%M:%S'
            )
            fh.setFormatter(formatter)
            self.logger.addHandler(fh)
        # Создание объекта для передачи настроек драйверу
        options = Options()
        if headless:
            user_agent = ('Mozilla/5.0 (X11; Linux x86_64) '
                          'AppleWebKit/537.36 (KHTML, like Gecko) '
                          'Chrome/60.0.3112.50 Safari/537.36')
            options.add_argument(f'user-agent={user_agent}')
            # режим без графического интерфейса
            options.add_argument('--headless')
            # необходимо для запуска на сервере через root
            options.add_argument('--no-sandbox')
            options.add_argument('--disable-dev-shm-usage')
        self.driver = webdriver.Chrome(
            service=Service(ChromeDriverManager().install()), options=options)
        # Открытие браузера во весь экран
        self.driver.maximize_window()
        # Создание объекта для выполнения действий на странице
        self.actions = ActionChains(self.driver)

    def load_page(self, link:str) -> None:
        """
        Позволяет загрузить страницу целиком.

        :link: Ссылка на веб-страницу
        :return: None
        """
        # Отправляем запрос
        self.driver.get(link)
        # Пауза между запросами
        time.sleep(self.timeout)
        # Загружаем страницу полностью
        try:
            # Выбираем тело страницы
            html = self.driver.find_element(By.TAG_NAME, 'html')
            # Прокручиваем страницу в конец
            html.send_keys(Keys.END)
        except NoSuchElementException as e:
            self.logger.warning(f'Не возможно получить html-текст страницы'
                                f' {self.driver.current_url}:\n'
                                f'{e}')

    def get_page_soup(self, link:str) -> BeautifulSoup:
        """
        Загружает страницу и возвращает объект ``BeautifulSoup`` на основе её
        содержимого.

        :param link: Ссылка на веб-страницу
        :return: Объект ``BeautifulSoup``
        """
        self.load_page(link)
        return self.get_current_soup()

    def get_current_soup(self):
        """
        Создает и возвращает объект ``BeautifulSoup`` на основе содержимого
        текущей открытой веб-страницы.

        :return: Объект ``BeautifulSoup``
        """
        src = self.driver.page_source
        return BeautifulSoup(src, 'html.parser')

    def get_absolute_link(self, relevant_link:str) -> str:
        """
        Возвращает абсолютную ссылку на веб-страницу.

        :param relevant_link: Относительная ссылка
        :return: Абсолютная ссылка
        """
        if self.base_link[-1] == '/' and relevant_link[0] == '/':
            relevant_link = relevant_link[1:]
        return self.base_link + relevant_link

    def get_categories(self, catalog_link:str) -> dict[str, str]:
        """
        Позволяет получить имена категорий и подкатегорий и относительные
        ссылки на подкатегории.

        :param catalog_link: Ссылка на страницу каталога
        :return: Словарь вида 'категория; подкатегория: ссылка на подкатегорию'
        """
        # Получаем объект BeautifulSoup для поиска
        soup = self.get_page_soup(catalog_link)
        # Поиск тега со списком категорий
        catalog_section = soup.find('div',
                                    {'class': 'catalog_section_list'})
        # Если тег со списком категорий не найден
        if not catalog_section:
            return {}
        # Поиск основных категорий
        main_categories = catalog_section.find_all(
            'div', {'class': 'section_item'})
        # Если не нашлось ни одного тега с данными о категории
        if not main_categories:
            return {}
        # Брендовые категории, которые будут обработаны в последнюю очередь
        categories_to_extract = ('/catalog/other_brands/',
                                 '/catalog/osculati_catalogue/')
        ordered_main_categories = []
        extracted_categories = []
        # Сортировка основных категорий
        for category in main_categories:
            try:
                _ = category.find('li', {'class': 'name'})
                _ = _.find('a')
                link = _.attrs.get('href')
                if not link in categories_to_extract:
                    ordered_main_categories.append(category)
                else:
                    extracted_categories.append(category)
            except AttributeError as e:
                self.logger.error(f'Ошибка при сортировке основных категорий, '
                                  f'не удалось найти ссылку на категорию\n{e}')

        ordered_main_categories += extracted_categories

        categories_dict = {}  # Словарь для данных о категориях

        for category in ordered_main_categories:
            try:
                # Поиск названия основной категории
                _ = category.find('li', {'class': 'name'})
                main_name = _.get_text(strip=True)
            except AttributeError as e:
                self.logger.error(f'Не удалось получить название основной'
                                  f'категории товаров\n{e}')
                main_name = ''

            # Находим все подкатегории
            sub_categories = category.find_all('li',
                                              {'class': 'sect'})
            # Если не нашлось ни одного тега с данными о подкатегориях
            if not sub_categories:
                return {}

            for sub_category in sub_categories:
                # Поиск тега с данными о подкатегории
                category_data = sub_category.find('a')
                if not category_data:
                    continue
                # Получаем относительную ссылку на подкатегорию
                rel_link = category_data.attrs.get('href')
                # Получаем название подкатегории
                name = category_data.get_text(strip=True)
                categories_dict['; '.join([main_name, name])] = rel_link

        return categories_dict

    @staticmethod
    def get_pagination_page_link(link:str, page_num:int) -> str:
        """
        Позволяет получить страницу с номером ``page_num`` категории,
        доступной по ссылке ``link``.

        :param link: Ссылка на товары категории
        :param page_num: Номер страницы
        :return: Ссылка на страницу категории
        """
        return link + f'?PAGEN_1={page_num}'

    def get_product_links(self, link) -> tuple[list[str], bool]:
        """
        Позволяет получить ссылки на товары со страницы пагинатора. Возвращает
        список ссылок и индикатор, принимающий значение True, если страница
        последняя, иначе False.

        :param link: Ссылка на страницу пагинатора
        :return: Список ссылок на товары и индикатор последней страницы
        """
        # Получаем объект BeautifulSoup для поиска
        soup = self.get_page_soup(link)
        # Ищем тег со списком товаров
        product_list = soup.find('div', {'class': 'display_list'})
        if not product_list:
            # Применяем дополнительную стратегию поиска
            product_list = soup.find('table',
                                     {'class': 'module_products_list'})
        if not product_list:
            raise ProductLinksNotFound(link)
        # Ищем теги с названиями товаров
        product_titles = product_list.find_all('div',
                                               {'class': 'item-title'})
        if not product_titles:
            product_titles = product_list.find_all(
                'td', {'class': 'item-name-cell'})
        if not product_titles:
            raise ProductLinksNotFound(link)

        product_links = []  # список ссылок на товары
        # Для каждого тега с названием товара
        for title in product_titles:
            # Ищем тег, содержащий ссылку на товар
            link_tag = title.find('a')
            try:
                # Извлекаем ссылку
                product_link = link_tag.attrs.get('href')
                # Добавляем ссылку в список
                product_links.append(product_link)
            except AttributeError as e:
                self.logger.warning(f'Не удалось получить ссылку на товар:\n'
                                    f'"{title.get_text(strip=True)}"\n'
                                    f'на странице {link}:\n'
                                    f'{e}')

        # Получаем тег с пагинатором
        paginator = soup.find('div', {'class': 'module-pagination'})
        if not paginator:
            # Если тег с пагинатором не найден, то страница считается последней
            is_last_page = True
        else:
            # Если страница последняя, на ней нет кнопки с именем класса
            # 'flex-nav-next', вместо неё появляется 'flex-nav-next  disabled'
            next_btn = paginator.find('li', {'class': 'flex-nav-next'})
            if 'disabled' in next_btn.attrs.get('class'):
                is_last_page = True  # True, если страница последняя
            else:
                is_last_page = False

        return product_links, is_last_page



    def iterate_options(self, options_groups, level:int=0) -> list[str]:
        """
        Генератор. Выбирает набор опций на странице товара и возвращает
        названия выбранных опций. Рекурсивно перебирает всевозможные комбинации
        опций.

        :param options_groups: Список тегов, содержащих опции
        :param level: Номер группы опций
        :return: Список названий выбранных опций
        """
        # Ищем кнопки выбора опций на уровне level
        options_btns = options_groups[level].find_elements(
            By.TAG_NAME, 'li')
        # Удаляем скрытые элементы
        options_btns = [btn for btn in options_btns
                        if not btn.get_attribute('style') == 'display: none;']
        # Для каждой кнопки
        for btn in options_btns:
            try:
                # Переход к элементу
                self.actions.move_to_element(btn).perform()
                # Очистка предыдущих действий
                self.actions.reset_actions()
                # Нажатие на кнопку
                btn.click()
            except NoSuchElementException as e:
                self.logger.warning(f'Не удалось найти кнопку, чтобы выбрать '
                                    f'опцию для товара на странице '
                                    f'{self.driver.current_url}:\n{e}')
            except ElementClickInterceptedException as e:
                self.logger.warning(f'Не удалось нажать на кнопку, чтобы '
                                    f'выбрать опцию для товара на странице '
                                    f'{self.driver.current_url}:\n{e}')
            # Если для каждой опции выбрано значение
            if level == len(options_groups) - 1:
                # Создаем список с перечислением названий выбранных опций
                buttons = []
                # Находим все доступные кнопки
                for group in options_groups:
                    buttons += group.find_elements(By.TAG_NAME, 'li')
                # Находим названия всех активных кнопок;
                # в названии содержится имя опции и выбранное значение
                selected_options = [item.get_attribute('title')
                                    for item in buttons
                                    if item.get_attribute('class') == 'active']
                yield selected_options
            else:
                # Иначе перебираем значения следующей опции
                for selected_options in self.iterate_options(options_groups,
                                                             level + 1):
                    pass

    def get_product_page_data(self, link:str) -> list[dict[str, str | None]]:
        """
        Возвращает список с данными о каждом артикуле на странице товара.

        :param link: Ссылка на страницу товара
        :return: Список с данными о каждом артикуле на странице товара
        """
        # Загружаем страницу
        self.load_page(link)
        page_data = []  # список для сохранения словарей с данными о товарах
        # Находим блок с кнопками выбора опций товара
        try:
            options_block = self.driver.find_element(By.CLASS_NAME,
                                                     'buy_block.iblock')
            product_options = options_block.find_element(By.CLASS_NAME,
                                                        'sku_props')
            # Поиск групп опций товара
            options_groups = product_options.find_elements(
                By.CLASS_NAME,
                'bx_item_detail_size')
        except NoSuchElementException:
            options_groups = None

        # Получаем список тегов с доступными опциями, каждый тег содержит
        # кнопки для выбора значения соответствующей опции

        general_data = self.get_general_data()
        if not general_data:
            return [{}]

        # Если найдены группы опций
        if options_groups:
            # Загружаем страницу для всех возможных комбинаций опций
            for selected_options in self.iterate_options(options_groups):
                # Получаем данные о товаре с конкретным сочетанием опций
                specific_data = self.get_specific_data()
                # Добавляем общие данные
                data = dict(general_data, **specific_data)
                # Добавляем к описанию выбранные опции
                data['options'] = ','.join(selected_options)
                # Добавляем словарь к списку
                page_data.append(data)
        else:
            # Если никаких опций не доступно
            specific_data = self.get_specific_data()
            data = dict(general_data, **specific_data)
            data['options'] = None
            page_data.append(data)

        return page_data

    def get_general_data(self) -> dict[str, str | None]:
        """
        Возвращает данные общие для всех артикулов на текущей открытой странице
        товара.

        :return: Общие данные для всех атрикулов на странице товара
        """
        # Создаем объект BeautifulSoup для поиска данных о товаре на основе
        # текущей открытой страницы
        soup = self.get_current_soup()
        # Поиск тегов с нужными данными
        try:
            main_info = soup.find('div',
                                  {'class': 'item_main_info'})
            middle_info = main_info.find('div',
                                         {'class': 'middle_info wrap_md'})
            top_info = main_info.find('div', {'class': 'top_info'})
        except AttributeError as e:
            self.logger.warning(f'Не найден товар по ссылке '
                                f'{self.driver.current_url}\n'
                                f'{e}')
            return {}
        # Поиск названия бренда
        brand_img = top_info.find('img')
        if brand_img:
            brand = brand_img.attrs.get('title')
        else:
            link_parts = self.driver.current_url.split('/')
            try:
                brand = link_parts[link_parts.index('other_brands') + 1]
            except ValueError as e:
                self.logger.info(f'Не удалось найти название бренда в адресной'
                                 f' строке для товара по ссылке '
                                 f'{self.driver.current_url}\n'
                                 f'{e}')
                brand = None

        if not brand:
            self.logger.warning(f'Не удалось найти название бренда для товара '
                                f'по ссылке: {self.driver.current_url}')

        # Поиск названия товара
        name_tag = soup.find('h1', {'id': 'pagetitle'})
        if name_tag:
            name = name_tag.get_text(strip=True)
        else:
            name = None

        if not name:
            self.logger.warning(f'Не удалось найти название товара по ссылке: '
                                f'{self.driver.current_url}')

        # Поиск краткого описания
        preview_description_tag = middle_info.find('div',
                                                   {'class': 'preview_text'})
        if preview_description_tag:
            preview_description = preview_description_tag.get_text(strip=True)
        else:
            preview_description = None

        if not preview_description:
            self.logger.warning(f'Не удалось найти краткое описание товара по '
                                f'ссылке: {self.driver.current_url}')

        # Поиск основного описания
        main_description_tag = soup.find('div',
                                         {'class': 'detail_text'})
        if not main_description_tag:
            self.logger.warning(f'Не удалось найти тег с описанием товара по '
                                f'ссылке: {self.driver.current_url}')
            main_description = None
        else:
            # Основное описание может содержать таблицы, которые необходимо
            # обработать отдельно, поэтому тег с описанием обрабатывается по
            # частям
            main_description_parts = []
            # Для каждой части описания
            for tag in main_description_tag:
                if tag.name == 'table':
                    try:
                        # Загружаем таблицу в датафрейм
                        df = pd.read_html(StringIO(str(tag)))[0]
                        # Получаем отформатированную таблицу в виде строки
                        main_description_parts.append(
                            df.to_string(index=False))
                    except ValueError as e:
                        self.logger.warning(f'Формат таблицы\n'
                                            f'{str(tag)}\n'
                                            f'полученной на странице'
                                            f'"{self.driver.current_url}" не '
                                            f'поддерживается:\n'
                                            f'{e}')
                    except IndexError as e:
                        self.logger.warning(f'Не удалось форматировать таблицу'
                                            f'\n{str(tag)}\n'
                                            f'полученную на странице'
                                            f'"{self.driver.current_url}"\n'
                                            f'{e}')
                else:
                    main_description_parts.append(tag.text)
            # Удаляем пустые строки
            main_description_parts_strip = [s for s in main_description_parts
                                            if s and s!= '\n']
            # Соединяем части основного описания
            main_description = '\n'.join(main_description_parts_strip)

            if not main_description:
                self.logger.warning(f'Не удалось найти основное описание '
                                    f'товара по ссылке: '
                                    f'{self.driver.current_url}')

        data = {
            'brand': brand,
            'name': name,
            'main_description': main_description,
            'preview_description': preview_description,
            'product_link': self.driver.current_url
        }

        return data

    def get_specific_data(self) -> dict[str, str | None]:
        """
        Возвращает данные для конкретного артикула на текущей открытой странице
        товара.

        :return: Данные конкретного артикула на странице товара
        """
        # Создаем объект BeautifulSoup для поиска данных о товаре на основе
        # текущей открытой страницы
        soup = self.get_current_soup()
        # Поиск тегов с нужными данными
        main_info = soup.find('div', {'class': 'item_main_info'})
        middle_info = main_info.find('div', {'class': 'middle_info wrap_md'})
        top_info = main_info.find('div', {'class': 'top_info'})

        # Поиск артикула
        _ = top_info.find('div', {'class': 'article iblock'})
        try:
            article_tag = _.find('span', {'class': 'value'})
            article = article_tag.get_text(strip=True)
        except AttributeError as e:
            self.logger.warning(f'Не найден артикул для товара по ссылке: '
                                f'{self.driver.current_url}\n'
                                f'{e}')
            article = None

        # Поиск цены
        price_tag = middle_info.find('div', {'class': 'price'})
        if price_tag:
            price = price_tag.get_text(strip=True)
            price = price.replace(' ', '')
            price = price.replace('.', '')
            price = price.replace('руб', '')
        else:
            price = None

        if not price:
            self.logger.warning(f'Не найдена цена для товара по ссылке:'
                                f' {self.driver.current_url}')

        # Поиск ссылок на изображения
        image_links = []

        img_wrapper = main_info.find('div', {'class': 'img_wrapper'})
        if img_wrapper:
            wrapp_thumbs = img_wrapper.find('div', {'class': 'slides'})
            if wrapp_thumbs:
                image_thumbs = wrapp_thumbs.find_all('a')

                for image in image_thumbs:
                    rel_link = image.attrs.get('href')
                    if not rel_link:
                        continue
                    link = self.get_absolute_link(rel_link)
                    image_links.append(link)

        image_links = ','.join(image_links)

        if not image_links:
            self.logger.warning(f'Не найдены изображения для товара по ссылке:'
                                f' {self.driver.current_url}')

        data = {
            'article': article,
            'price': price,
            'image_links': image_links  # через запятую
         }

        return data

    @staticmethod
    def set_max_csv_size(size: int) -> None:
        """
        Позволяет установить максимальный размер csv файла. Если указанный
        размер установить невозможно, будет установлен максимальный возможный
        размер.

        :param size: Размер csv файла
        :return: None
        """

        decrement = True

        while decrement:
            decrement = False
            try:
                csv.field_size_limit(size)
            except OverflowError:
                size = int(size / 10)
                decrement = True

    def add_collected_link(self, links_set:set[str], link) -> None:
        """
        Добавляет переданную ссылку в файл, а так же в переданную коллекцию.

        :param links_set: Коллекция для добавления ссылки
        :param link: Ссылка
        :return: None
        """
        with self.collected_links_path.open('a', encoding='utf-8') as f:
            f.write(link + '\n')
        links_set.add(link)

    def get_all_data(self) -> None:
        """
        Извлекает все необходимые данные.

        :return: None
        """
        # Устанавливаем максимальный размер csv файла
        self.set_max_csv_size(sys.maxsize)
        collected_links = set() # множество ранее обработанных ссылок
        # Открываем или создаем файл для сохранения ссылок на уже обработанные
        # категории
        with self.collected_links_path.open('a+', encoding='utf-8') as f:
            f.seek(0)  # установка каретки в начало файла
            for line in f:
                collected_links.add(line.rstrip())

        # Получаем абсолютную ссылку на страницу каталога
        catalog_link = self.get_absolute_link('catalog/')
        # Получаем относительные ссылки на страницы категорий
        categories = self.get_categories(catalog_link)

        # Для каждой категории
        for category_name, rel_category_link in categories.items():
            # Получение абсолютной ссылки на категорию
            category_link = self.get_absolute_link(rel_category_link)
            if category_link in collected_links:
                continue
            # Для каждой страницы категории
            for page_num in count(start=1, step=1):
                # Получение ссылки на страницу в категории
                page_link = self.get_pagination_page_link(category_link,
                                                          page_num)
                if page_link in collected_links:
                    continue
                # Получение ссылок на товары категории с одной страницы, если
                # страница последняя, is_last_page принимает значение True
                product_links, is_last_page = self.get_product_links(
                    page_link)
                # Для каждого товара на странице
                for rel_product_link in product_links:
                    # Если данные уже получены - ссылка пропускается
                    if rel_product_link in collected_links:
                        continue
                    # Получение абсолютной ссылки на страницу товара
                    product_link = self.get_absolute_link(rel_product_link)
                    # Получение всех необходимых данных со страницы товара в
                    # виде списка словарей
                    page_data = self.get_product_page_data(product_link)
                    # Если на странице товара нет никаких данных
                    if all(not data_dict for data_dict in page_data):
                        self.logger.error(f'Не удалось получить никакие данные'
                                          f' о товаре по ссылке '
                                          f'{product_link}')
                        continue
                    # Для каждого артикула на странице товара
                    for data_dict in page_data:
                        # Указываем имя категории
                        data_dict['category'] = category_name
                    # Определение существования csv файла для сохранения данных
                    saving_file_exists = self.data_save_path.is_file()
                    # Открываем или создаем csv файл для сохранения данных
                    with self.data_save_path.open('a',
                              newline='', encoding='utf-8') as f:
                        # Создаем объект для записи данных в csv файл
                        w = csv.DictWriter(f, self.fields_order)
                        # Если файл ранее не был создан, записываем заголовки
                        if not saving_file_exists:
                            w = csv.DictWriter(f, self.fields_order)
                            w.writeheader()
                        # Записываем данные каждого словаря в отдельную строку
                        w.writerows(page_data)
                    # Сохраняем ссылку на обработанный товар
                    self.add_collected_link(collected_links, rel_product_link)
                # Сохраняем ссылку на обработанную страницу категории
                self.add_collected_link(collected_links, page_link)
                # Если обработана последняя страница категории
                if is_last_page:
                    break
            # Сохраняем ссылку на обработанную категорию
            self.add_collected_link(collected_links, category_link)


if __name__ == '__main__':
    pass
