from django.http import JsonResponse
from DjangoAdCrawler.models import ImportProgress


def DjangoAdCrawler_import_progress_status(request):
    user = request.user if request.user.is_authenticated else None
    data = {
        'status': 'idle',
        'current': 0,
        'total': 0,
        'error': '',
    }
    if user:
        progress = ImportProgress.objects.filter(user=user)
        progress = progress.order_by('-updated_at').first()
        if progress:
            data['status'] = progress.status
            data['current'] = progress.last_success_row
            data['total'] = progress.total_rows or 0
            if progress.status == 'error':
                data['error'] = progress.error or 'Ошибка импорта'
    return JsonResponse(data)
