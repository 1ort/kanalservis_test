# kanalservis_test
Python test task for Kanalservis

## Ссылка на таблицу:
https://docs.google.com/spreadsheets/d/1NEOgEBntRbH_2Mdfky4INddCwh-zxZszAAgeVjCvbq8/

## Запуск:
```
git clone https://github.com/1ort/kanalservis_test.git
cd kanalservis_test
docker-compose up -d --build
```

Перед первым запуском необходимо указать в файле config.ini Токен телеграм бота и ID чата, в который будут приходить уведомления. Получить свой ID можно в @userinfobot.
Скрипт и база данных запустятся в Docker-контейнерах.
Чтобы подключить скрипт к существующей базе данных, необходимо указать данные в файле config.ini

Уведомления в telegram будут приходить каждый день в 12:00, так как в таблице хранится только дата, без времени.
