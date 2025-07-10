import requests
import time
import random
from django.utils.text import slugify
from shop.models import Product, Category, ProductImage
from django.core.files.base import ContentFile
import logging
from DjangoAdCrawler.models import ImportProgress
from django.utils import timezone
from celery import shared_task
import csv
import os
from django.urls import reverse
from django.shortcuts import redirect
from django.contrib import messages
from django.contrib import admin
from django.db import models
from django.template.response import TemplateResponse

# Настройка логгера для импорта
logger = logging.getLogger('import_logger')
file_handler = logging.FileHandler('import.log', encoding='utf-8')
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
file_handler.setFormatter(formatter)
if not logger.hasHandlers():
    logger.addHandler(file_handler)
logger.setLevel(logging.INFO)

PAUSE_MINUTES = 5

CSV_PATH = os.path.join('file', 'import.csv')

IMPORT_FIELDS = [
    ('name', 'Название'),
    ('price', 'Цена'),
    ('description', 'Описание'),
    ('avito_id', 'ID объявления'),
    ('images', 'Изображения (ссылки)'),
    ('category', 'Категория (если есть в файле)'),
]

class CSVImportStub(models.Model):
    """
    Заглушка-модель для отображения импорта CSV в админке.
    Не содержит полей, используется только для интерфейса импорта.
    """
    class Meta:
        verbose_name = "Импорт CSV"
        verbose_name_plural = "Импорт CSV"
        app_label = "DjangoAdCrawler"

@admin.register(CSVImportStub)
class CSVImportAdmin(admin.ModelAdmin):
    """
    Админ-класс для импорта товаров из CSV-файла.
    Позволяет предпросматривать, маппить столбцы и импортировать
    товары с изображениями.
    """
    def changelist_view(self, request, extra_context=None):
        preview = None
        columns = None
        error = None
        encoding_used = None
        mapping = None
        show_mapping = False
        categories = Category.objects.all()
        selected_category_id = None
        if request.method == 'POST' and 'preview' in request.POST:
            try:
                try:
                    with open(CSV_PATH, encoding='utf-8') as f:
                        reader = csv.reader(f, delimiter=';')
                        rows = list(reader)
                    encoding_used = 'utf-8'
                except UnicodeDecodeError:
                    with open(CSV_PATH, encoding='cp1251') as f:
                        reader = csv.reader(f, delimiter=';')
                        rows = list(reader)
                    encoding_used = 'cp1251'
                columns = rows[0] if rows else []
                preview = rows[1:6] if len(rows) > 1 else []
                request.session['avito_csv_data'] = rows
                request.session['avito_csv_columns'] = columns
                show_mapping = True
                messages.info(
                    request,
                    (
                        'Проверьте предпросмотр. Для импорта выберите соответствие '
                        'столбцов и категорию, затем нажмите "Импортировать". '
                        f'(Кодировка файла: {encoding_used})'
                    )
                )
            except Exception as e:
                error = f'Ошибка чтения файла: {e}'
        elif request.method == 'POST' and ('import' in request.POST or 'start' in request.POST):
            rows = request.session.get('avito_csv_data')
            columns = request.session.get('avito_csv_columns')
            mapping = {}
            for field, _ in IMPORT_FIELDS:
                mapping[field] = request.POST.get(f'col_{field}')
            selected_category_id = request.POST.get('category_id')
            if not rows or not columns or not mapping:
                messages.error(
                    request,
                    'Нет данных для импорта или не выбраны столбцы.'
                )
                return redirect(
                    reverse('admin:DjangoAdCrawler_csvimportstub_changelist')
                )
            user = request.user if request.user.is_authenticated else None
            progress_obj = None
            if user:
                progress_obj = ImportProgress.objects.filter(user=user).order_by('-id').first()
                if not progress_obj:
                    progress_obj = ImportProgress.objects.create(user=user)
                last_success_row = progress_obj.last_success_row
            else:
                last_success_row = request.session.get('avito_import_last_row', 0)
            if 'import' in request.POST:
                last_success_row = 0
                if progress_obj:
                    progress_obj.last_success_row = 0
                    progress_obj.images_downloaded = 0
                    progress_obj.status = 'running'
                    progress_obj.pause_until = None
                    progress_obj.pause_minutes = PAUSE_MINUTES
                    progress_obj.extra_delay_after_429 = 0
                    progress_obj.save()
            if 'stop' in request.POST:
                if progress_obj:
                    progress_obj.status = 'stopped'
                    progress_obj.save()
                request.session['avito_import_last_row'] = 0
                messages.info(request, 'Импорт остановлен и прогресс сброшен.')
                return redirect(reverse('admin:DjangoAdCrawler_csvimportstub_changelist'))
            result = import_products_from_csv(
                rows,
                columns,
                mapping,
                selected_category_id=selected_category_id,
                request=request,
                start_row=last_success_row + 1,
                user=user
            )
            skipped = result.get('skipped_duplicates', 0)
            if progress_obj:
                progress_obj.last_success_row = result['last_success_row']
                progress_obj.save()
            request.session['avito_import_last_row'] = result['last_success_row']
            if result['status'] == 'paused':
                msg = (
                    'Импорт приостановлен из-за ограничения (429). '
                    f'Импортировано товаров: {result["imported"]}. '
                )
                if skipped:
                    msg += f'Пропущено дубликатов: {skipped}. '
                msg += (
                    f'Можно продолжить с позиции '
                    f'{result["last_success_row"] + 1}.'
                )
                messages.warning(request, msg)
            elif result['status'] == 'waiting':
                wait_until = result.get('wait_until')
                msg = (
                    f'Импорт на паузе до {wait_until.strftime("%H:%M:%S")}. '
                    f'Импортировано товаров: {result["imported"]}. '
                )
                if skipped:
                    msg += f'Пропущено дубликатов: {skipped}. '
                msg += (
                    f'Можно продолжить с позиции '
                    f'{result["last_success_row"] + 1}.'
                )
                messages.info(request, msg)
            else:
                msg = (
                    'Импорт завершён. '
                    f'Импортировано товаров: {result["imported"]}. '
                )
                if skipped:
                    msg += f'Пропущено дубликатов: {skipped}.'
                messages.success(request, msg)
            if result['status'] == 'completed':
                request.session['avito_import_last_row'] = 0
                del request.session['avito_csv_data']
                del request.session['avito_csv_columns']
            return redirect(
                reverse('admin:DjangoAdCrawler_csvimportstub_changelist')
            )
        elif request.method == 'POST' and 'pause' in request.POST:
            messages.info(request, 'Импорт приостановлен пользователем.')
            return redirect(reverse('admin:DjangoAdCrawler_csvimportstub_changelist'))
        else:
            columns = request.session.get('avito_csv_columns')
            preview = None
            rows = request.session.get('avito_csv_data')
            if columns and rows:
                preview = rows[1:6] if len(rows) > 1 else []
                show_mapping = True
        return TemplateResponse(
            request,
            'admin/DjangoAdCrawler/avitoimportlog/import_csv.html',
            {
                'preview': preview,
                'columns': columns,
                'error': error,
                'import_fields': IMPORT_FIELDS,
                'show_mapping': show_mapping,
                'categories': categories,
                'selected_category_id': selected_category_id,
                'opts': CSVImportStub._meta,
                'app_label': CSVImportStub._meta.app_label,
                'has_add_permission': False,
                'has_change_permission': True,
                'has_view_permission': True,
                'has_delete_permission': False,
                'cl': None,
                'title': 'Импорт товаров из CSV',
            }
        )

@shared_task(bind=True)
def import_products_from_csv_task(self,
    rows, columns, mapping, selected_category_id=None, user_id=None,
    preview_limit=3, start_row=1, stop_on_429=True
):
    """
    Celery-задача для импорта товаров из CSV с поддержкой пауз и автопродолжения.
    """
    from django.contrib.auth import get_user_model
    from DjangoAdCrawler.models import ImportProgress
    from shop.models import Product, Category, ProductImage
    from django.core.files.base import ContentFile
    from django.utils.text import slugify
    from django.utils import timezone
    import requests
    import time
    import random
    import logging
    logger = logging.getLogger('import_logger')
    imported = 0
    last_success_row = start_row - 1
    status = 'completed'
    images_downloaded = 0
    pause_minutes = 5
    extra_delay_after_429 = 0
    progress_obj = None
    user = get_user_model().objects.filter(id=user_id).first() if user_id else None
    if user:
        progress_obj = ImportProgress.objects.filter(user=user)\
            .order_by('-id').first()
        if not progress_obj:
            progress_obj = ImportProgress.objects.create(
                user=user,
                last_success_row=last_success_row,
                images_downloaded=0,
                status='running',
                pause_until=None,
                pause_minutes=pause_minutes,
                extra_delay_after_429=0,
                total_rows=len(rows) - 1 if rows else 0,
            )
        last_success_row = progress_obj.last_success_row
        images_downloaded = progress_obj.images_downloaded
        pause_minutes = progress_obj.pause_minutes or 4
        extra_delay_after_429 = progress_obj.extra_delay_after_429 or 0
        if progress_obj.total_rows == 0 and rows:
            progress_obj.total_rows = len(rows) - 1
            progress_obj.save(update_fields=['total_rows'])
        if progress_obj.status in ['paused', 'waiting'] and progress_obj.pause_until:
            now = timezone.now()
            if now < progress_obj.pause_until:
                # Автоматическая пауза через Celery retry (или return)
                seconds_left = (progress_obj.pause_until - now).total_seconds()
                progress_obj.status = 'waiting'
                progress_obj.save(update_fields=['status'])
                try:
                    raise self.retry(countdown=int(seconds_left))
                except NameError:
                    return {
                        'imported': imported,
                        'last_success_row': last_success_row,
                        'status': 'waiting',
                        'wait_until': progress_obj.pause_until,
                    }
            else:
                progress_obj.status = 'running'
                progress_obj.save(update_fields=['status'])
    for i, row in enumerate(rows[start_row:], start=start_row):
        data = dict(zip(columns, row))
        name = data.get(mapping['name'])
        price = data.get(mapping['price'])
        description = data.get(mapping['description'])
        avito_id = data.get(mapping['avito_id'])
        images_raw = data.get(mapping['images'])
        category_value = mapping['category'] and data.get(mapping['category'])
        category = None
        if category_value:
            category, _ = Category.objects.get_or_create(name=category_value)
        elif selected_category_id:
            try:
                category = Category.objects.get(id=selected_category_id)
            except Category.DoesNotExist:
                category = None
        if not name or not avito_id or not category:
            logger.warning(
                f'Skip row: name={name}, avito_id={avito_id}, category={category}'
            )
            continue
        if Product.objects.filter(avito_id=avito_id).exists():
            logger.info(f'Skip duplicate avito_id={avito_id}')
            continue
        slug = slugify(f"{name}-{avito_id}")
        product = Product.objects.create(
            name=name,
            price=price or 0,
            description=description or '',
            avito_id=avito_id,
            available=True,
            category=category,
            slug=slug,
        )
        logger.info(
            f'Created product: name={name}, avito_id={avito_id}, category={category}'
        )
        # --- Импорт изображений ---
        if images_raw:
            for sep in ['|', ';', ',']:
                if sep in images_raw:
                    img_urls = [
                        img_url.strip() for img_url in images_raw.split(sep)
                        if img_url.strip()
                    ]
                    break
            else:
                img_urls = [images_raw.strip()] if images_raw.strip() else []
            logger.info(f'Image URLs for {name}: {img_urls}')
            headers = {
                'User-Agent': (
                    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                    'AppleWebKit/537.36 (KHTML, like Gecko) '
                    'Chrome/122.0.0.0 Safari/537.36'
                ),
                'Accept': (
                    'text/html,application/xhtml+xml,application/xml;q=0.9,'
                    'image/webp,*/*;q=0.8'
                ),
                'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
                'Accept-Encoding': 'gzip, deflate, br',
                'Referer': 'https://www.avito.ru/',
                'Cookie': (
                    '__ai_fp_uuid=2f37c7c10901ab6a%3A1; '
                    '__upin=TGhfBOJEiPX8/2CZ2k1wmw; '
                    '_buzz_aidata=JTdCJTIydWZwJTIyJTNBJTIyVEdoZkJPSkVpUFg4JTJGMkNaMmsxd213JTIyJTJDJTIyYnJvd3NlclZlcnNpb24lMjIlM0ElMjIyNS42JTIyJTJDJTIydHNDcmVhdGVkJTIyJTNBMTc1MTgxMDE0MTAwMSU3RA==; '
                    '_buzz_mtsa=JTdCJTIydWZwJTIyJTNBJTIyYjljZTNhZGZjYTZiN2YyYzg4NjNmYzRkZmE4ZGVjNTYlMjIlMkMlMjJicm93c2VyVmVyc2lvbiUyMiUzQSUyMjI1LjYlMjIlMkMlMjJ0c0NyZWF0ZWQlMjIlM0ExNzUxODEwMTQxMTM0JTdE; '
                    '_ga=GA1.1.1215029419.1751810141; '
                    '_ga_M29JC28873=GS2.1.s1751810140$o1$g0$t1751810140$j60$l0$h0; '
                    '_gcl_au=1.1.1460471293.1751810140; '
                    '_ym_d=1751810141; _ym_isad=2; _ym_uid=1751810141202569353; '
                    '_ym_visorc=b; sx=2; abp=0'
                )
            }
            for idx, img_url in enumerate(img_urls):
                img_content = None
                final_url = None
                try:
                    resp = requests.get(
                        img_url, timeout=10, headers=headers, allow_redirects=True
                    )
                    final_url = resp.url
                    logger.info(
                        f'Download image: {img_url} -> {final_url}, '
                        f'status={resp.status_code}'
                    )
                    if resp.status_code == 429:
                        status = 'paused'
                        logger.warning(
                            f'Paused import at row {i} due to 429 for image {img_url}'
                        )
                        if progress_obj:
                            progress_obj.last_success_row = last_success_row
                            progress_obj.images_downloaded = images_downloaded
                            progress_obj.status = 'paused'
                            progress_obj.pause_minutes = pause_minutes + 3
                            progress_obj.extra_delay_after_429 = (
                                extra_delay_after_429 + 3
                            )
                            progress_obj.pause_until = timezone.now() + timezone.timedelta(
                                minutes=pause_minutes
                            )
                            progress_obj.save()
                        return {
                            'imported': imported,
                            'last_success_row': last_success_row,
                            'status': status,
                            'wait_until': timezone.now() + timezone.timedelta(
                                minutes=pause_minutes
                            ),
                        }
                    if (
                        'avito.st/image/' in final_url and resp.status_code == 200
                    ):
                        img_content = resp.content
                    else:
                        img_content = None
                    # --- Логика паузы после 10 изображений ---
                    images_downloaded += 1
                    if images_downloaded % 10 == 0:
                        logger.info(
                            f'Pause {pause_minutes} min after {images_downloaded} images'
                        )
                        if progress_obj:
                            progress_obj.last_success_row = i
                            progress_obj.images_downloaded = images_downloaded
                            progress_obj.status = 'waiting'
                            progress_obj.pause_minutes = pause_minutes
                            progress_obj.pause_until = timezone.now() + timezone.timedelta(
                                minutes=pause_minutes
                            )
                            progress_obj.save()
                        time.sleep(pause_minutes * 60)
                    # --- Доп. задержка после 429 ---
                    delay = random.uniform(3, 7) + extra_delay_after_429 * 60
                    logger.info(f'Delay between requests: {delay:.2f} sec.')
                    time.sleep(delay)
                    if img_content:
                        img_name = f"{product.slug}-{idx}.jpg"
                        if idx == 0:
                            product.image.save(
                                img_name, ContentFile(img_content), save=True
                            )
                            logger.info(f'Saved main image {img_name} for {name}')
                        else:
                            ProductImage.objects.create(
                                product=product,
                                image=ContentFile(img_content, name=img_name),
                            )
                            logger.info(f'Added gallery image {img_name} for {name}')
                    else:
                        logger.warning(f'Failed to get image content for {img_url}')
                except Exception as e:
                    logger.error(f'Error downloading image {img_url}: {e}')
        imported += 1
        last_success_row = i
        if progress_obj:
            progress_obj.last_success_row = last_success_row
            progress_obj.images_downloaded = images_downloaded
            progress_obj.status = 'running'
            progress_obj.save()
        logger.info(f'Imported product: {name} (avito_id={avito_id})')
    if progress_obj:
        progress_obj.status = 'completed'
        progress_obj.save()
    return {
        'imported': imported,
        'last_success_row': last_success_row,
        'status': status
    }

def import_products_from_csv(
    rows, columns, mapping, selected_category_id=None, request=None,
    preview_limit=3, start_row=1, stop_on_429=True, user=None
):
    """
    Импортирует товары из CSV-таблицы с поддержкой изображений и категорий.
    :param rows: Список строк CSV (list of lists)
    :param columns: Список названий столбцов
    :param mapping: dict соответствия полей (name, price, description, avito_id,
        images, category)
    :param selected_category_id: id выбранной категории (если не указана в файле)
    :param request: (опционально) объект запроса для сообщений в админке
    :param preview_limit: сколько товаров логировать подробно
    :param start_row: с какой строки начинать импорт (по умолчанию 1 — после заголовка)
    :param stop_on_429: останавливать ли импорт при получении 429 (по умолчанию True)
    :param user: пользователь, инициировавший импорт (для ImportProgress)
    :return: dict с количеством импортированных, позицией, статусом
    """
    logger.info(f"=== START IMPORT === start_row={start_row}, rows={len(rows) if rows else 0}, columns={columns}, mapping={mapping}, selected_category_id={selected_category_id}, user={user}")
    imported = 0
    last_success_row = start_row - 1
    status = 'completed'
    images_downloaded = 0
    pause_minutes = 4
    extra_delay_after_429 = 0
    progress_obj = None
    skipped_duplicates = 0
    if user:
        progress_obj = ImportProgress.objects.filter(user=user)\
            .order_by('-id').first()
        if not progress_obj:
            progress_obj = ImportProgress.objects.create(
                user=user,
                last_success_row=last_success_row,
                images_downloaded=0,
                status='running',
                pause_until=None,
                pause_minutes=pause_minutes,
                extra_delay_after_429=0,
                total_rows=len(rows) - 1 if rows else 0,
            )
        last_success_row = progress_obj.last_success_row
        images_downloaded = progress_obj.images_downloaded
        pause_minutes = progress_obj.pause_minutes or 4
        extra_delay_after_429 = progress_obj.extra_delay_after_429 or 0
        if progress_obj.total_rows == 0 and rows:
            progress_obj.total_rows = len(rows) - 1
            progress_obj.save(update_fields=['total_rows'])
        if progress_obj.status in ['paused', 'waiting'] and progress_obj.pause_until:
            now = timezone.now()
            if now < progress_obj.pause_until:
                return {
                    'imported': imported,
                    'last_success_row': last_success_row,
                    'status': 'waiting',
                    'wait_until': progress_obj.pause_until,
                }
            else:
                progress_obj.status = 'running'
                progress_obj.save(update_fields=['status'])
    for i, row in enumerate(rows[start_row:], start=start_row):
        data = dict(zip(columns, row))
        name = data.get(mapping['name'])
        price = data.get(mapping['price'])
        description = data.get(mapping['description'])
        avito_id = data.get(mapping['avito_id'])
        images_raw = data.get(mapping['images'])
        category_value = mapping['category'] and data.get(mapping['category'])
        category = None
        if category_value:
            category, _ = Category.objects.get_or_create(name=category_value)
        elif selected_category_id:
            try:
                category = Category.objects.get(id=selected_category_id)
            except Category.DoesNotExist:
                category = None
        if not name or not avito_id or not category:
            logger.warning(
                f'Skip row: name={name}, avito_id={avito_id}, category={category}'
            )
            continue
        if Product.objects.filter(avito_id=avito_id).exists():
            logger.info(f'Skip duplicate avito_id={avito_id}')
            skipped_duplicates += 1
            continue
        slug = slugify(f"{name}-{avito_id}")
        product = Product.objects.create(
            name=name,
            price=price or 0,
            description=description or '',
            avito_id=avito_id,
            available=True,
            category=category,
            slug=slug,
        )
        logger.info(
            f'Created product: name={name}, avito_id={avito_id}, category={category}'
        )
        # --- Импорт изображений ---
        if images_raw:
            for sep in ['|', ';', ',']:
                if sep in images_raw:
                    img_urls = [
                        img_url.strip() for img_url in images_raw.split(sep)
                        if img_url.strip()
                    ]
                    break
            else:
                img_urls = [images_raw.strip()] if images_raw.strip() else []
            logger.info(f'Image URLs for {name}: {img_urls}')
            headers = {
                'User-Agent': (
                    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                    'AppleWebKit/537.36 (KHTML, like Gecko) '
                    'Chrome/122.0.0.0 Safari/537.36'
                ),
                'Accept': (
                    'text/html,application/xhtml+xml,application/xml;q=0.9,'
                    'image/webp,*/*;q=0.8'
                ),
                'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
                'Accept-Encoding': 'gzip, deflate, br',
                'Referer': 'https://www.avito.ru/',
                'Cookie': (
                    '__ai_fp_uuid=2f37c7c10901ab6a%3A1; '
                    '__upin=TGhfBOJEiPX8/2CZ2k1wmw; '
                    '_buzz_aidata=JTdCJTIydWZwJTIyJTNBJTIyVEdoZkJPSkVpUFg4JTJGMkNaMmsxd213JTIyJTJDJTIyYnJvd3NlclZlcnNpb24lMjIlM0ElMjIyNS42JTIyJTJDJTIydHNDcmVhdGVkJTIyJTNBMTc1MTgxMDE0MTAwMSU3RA==; '
                    '_buzz_mtsa=JTdCJTIydWZwJTIyJTNBJTIyYjljZTNhZGZjYTZiN2YyYzg4NjNmYzRkZmE4ZGVjNTYlMjIlMkMlMjJicm93c2VyVmVyc2lvbiUyMiUzQSUyMjI1LjYlMjIlMkMlMjJ0c0NyZWF0ZWQlMjIlM0ExNzUxODEwMTQxMTM0JTdE; '
                    '_ga=GA1.1.1215029419.1751810141; '
                    '_ga_M29JC28873=GS2.1.s1751810140$o1$g0$t1751810140$j60$l0$h0; '
                    '_gcl_au=1.1.1460471293.1751810140; '
                    '_ym_d=1751810141; _ym_isad=2; _ym_uid=1751810141202569353; '
                    '_ym_visorc=b; sx=2; abp=0'
                )
            }
            for idx, img_url in enumerate(img_urls):
                img_content = None
                final_url = None
                try:
                    resp = requests.get(
                        img_url, timeout=10, headers=headers, allow_redirects=True
                    )
                    final_url = resp.url
                    logger.info(
                        f'Download image: {img_url} -> {final_url}, '
                        f'status={resp.status_code}'
                    )
                    if resp.status_code == 429:
                        status = 'paused'
                        logger.warning(
                            f'Paused import at row {i} due to 429 for image {img_url}'
                        )
                        if progress_obj:
                            progress_obj.last_success_row = last_success_row
                            progress_obj.images_downloaded = images_downloaded
                            progress_obj.status = 'paused'
                            progress_obj.pause_minutes = pause_minutes + 3
                            progress_obj.extra_delay_after_429 = (
                                extra_delay_after_429 + 3
                            )
                            progress_obj.pause_until = timezone.now() + timezone.timedelta(
                                minutes=pause_minutes
                            )
                            progress_obj.save()
                        return {
                            'imported': imported,
                            'last_success_row': last_success_row,
                            'status': status,
                            'wait_until': timezone.now() + timezone.timedelta(
                                minutes=pause_minutes
                            ),
                        }
                    if (
                        'avito.st/image/' in final_url and resp.status_code == 200
                    ):
                        img_content = resp.content
                    else:
                        img_content = None
                    # --- Логика паузы после 10 изображений ---
                    images_downloaded += 1
                    if images_downloaded % 10 == 0:
                        logger.info(
                            f'Pause {pause_minutes} min after {images_downloaded} images'
                        )
                        if progress_obj:
                            progress_obj.last_success_row = i
                            progress_obj.images_downloaded = images_downloaded
                            progress_obj.status = 'waiting'
                            progress_obj.pause_minutes = pause_minutes
                            progress_obj.pause_until = timezone.now() + timezone.timedelta(
                                minutes=pause_minutes
                            )
                            progress_obj.save()
                        time.sleep(pause_minutes * 60)
                    # --- Доп. задержка после 429 ---
                    delay = random.uniform(3, 7) + extra_delay_after_429 * 60
                    logger.info(f'Delay between requests: {delay:.2f} sec.')
                    time.sleep(delay)
                    if img_content:
                        img_name = f"{product.slug}-{idx}.jpg"
                        if idx == 0:
                            product.image.save(
                                img_name, ContentFile(img_content), save=True
                            )
                            logger.info(f'Saved main image {img_name} for {name}')
                        else:
                            ProductImage.objects.create(
                                product=product,
                                image=ContentFile(img_content, name=img_name),
                            )
                            logger.info(f'Added gallery image {img_name} for {name}')
                    else:
                        logger.warning(f'Failed to get image content for {img_url}')
                except Exception as e:
                    logger.error(f'Error downloading image {img_url}: {e}')
        imported += 1
        last_success_row = i
        if progress_obj:
            progress_obj.last_success_row = last_success_row
            progress_obj.images_downloaded = images_downloaded
            progress_obj.status = 'running'
            progress_obj.save()
        logger.info(f'Imported product: {name} (avito_id={avito_id})')
    if progress_obj:
        progress_obj.status = 'completed'
        progress_obj.save()
    return {
        'imported': imported,
        'last_success_row': last_success_row,
        'status': status,
        'skipped_duplicates': skipped_duplicates
    } 