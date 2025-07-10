from django.apps import AppConfig


class DjangoAdCrawlerConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'DjangoAdCrawler'
    verbose_name = 'DjangoAdCrawler'

    def ready(self):
        import django.conf
        from django.conf import settings
        if hasattr(settings, 'TEMPLATES'):
            for tpl in settings.TEMPLATES:
                if tpl['BACKEND'].endswith('DjangoTemplates'):
                    tpl.setdefault('APP_DIRS', True)
