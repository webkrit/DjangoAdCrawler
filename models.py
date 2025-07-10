from django.db import models
from django.contrib.auth import get_user_model
from django.utils import timezone

# Create your models here.


class ImportProgress(models.Model):
    STATUS_CHOICES = [
        ('idle', 'Ожидание'),
        ('running', 'Выполняется'),
        ('paused', 'Пауза'),
        ('waiting', 'Ожидание паузы'),
        ('stopped', 'Остановлено'),
        ('error', 'Ошибка'),
        ('completed', 'Завершено'),
    ]
    user = models.OneToOneField(
        get_user_model(), on_delete=models.SET_NULL, null=True, blank=True
    )
    started_at = models.DateTimeField(default=timezone.now)
    updated_at = models.DateTimeField(auto_now=True)
    last_success_row = models.IntegerField(default=0)
    images_downloaded = models.IntegerField(default=0)
    total_rows = models.IntegerField(default=0)
    status = models.CharField(
        max_length=16, choices=STATUS_CHOICES, default='idle'
    )
    pause_until = models.DateTimeField(null=True, blank=True)
    pause_minutes = models.IntegerField(default=10)  # в минутах
    extra_delay_after_429 = models.IntegerField(default=0)  # в минутах
    last_message = models.TextField(blank=True, default='')
    stopped_by_user = models.BooleanField(default=False)

    def __str__(self):
        return f"Импорт {self.pk} ({self.get_status_display()})"

    class Meta:
        app_label = 'DjangoAdCrawler'
