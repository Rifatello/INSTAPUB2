# AI Runbook: Корректный запуск сервиса

Этот проект считается "поднят", когда работают 4 узла:
- `Content API` на `127.0.0.1:8002`
- `Publish API` на `127.0.0.1:8010`
- `Media HTTP` (раздача видео) на `127.0.0.1:8080`
- `Public tunnel` до `8080` (через `localhost.run`)

## 1) Что поднять в первую очередь

1. `Media HTTP` (порт `8080`)
2. `Tunnel` до `8080`
3. `Content API` (порт `8002`)
4. `Publish API` (порт `8010`)

Порядок важен: `publish` должен отдавать внешнюю ссылку на видео, значит сначала нужен `8080 + tunnel`.

## 2) Команды запуска (локально)

Рабочая директория:
`/Users/rifatello/Desktop/content-factory copy 2`

### 2.1 Media HTTP (файлы из `~/my_videos`)
```bash
python3 -m http.server 8080 --bind 127.0.0.1
```
Запускать из директории:
`/Users/rifatello/my_videos`

### 2.2 Tunnel (localhost.run)
```bash
ssh -o StrictHostKeyChecking=no -o ServerAliveInterval=30 -R 80:localhost:8080 nokey@localhost.run
```
Из вывода взять публичный URL вида:
`https://<subdomain>.lhr.life`

Сохранить URL в файл:
`/Users/rifatello/my_videos/public_base_url.txt`

### 2.3 Content API
```bash
/Applications/Xcode.app/Contents/Developer/Library/Frameworks/Python3.framework/Versions/3.9/Resources/Python.app/Contents/MacOS/Python -m uvicorn api.content_api:app --host 127.0.0.1 --port 8002
```

### 2.4 Publish API
```bash
/Applications/Xcode.app/Contents/Developer/Library/Frameworks/Python3.framework/Versions/3.9/Resources/Python.app/Contents/MacOS/Python -m uvicorn app.main:app --host 127.0.0.1 --port 8010
```

## 3) Обязательные проверки после запуска

### 3.1 Health-check
```bash
curl -sS http://127.0.0.1:8002/health
curl -sS http://127.0.0.1:8010/health
```
Ожидается: `{"status":"ok"}`

### 3.2 Проверка внешней раздачи видео
```bash
URL=$(cat /Users/rifatello/my_videos/public_base_url.txt)
FILE=$(ls /Users/rifatello/my_videos/*.mp4 | head -n 1 | xargs basename)
curl -I "$URL/$FILE"
```
Ожидается: `HTTP/1.0 200 OK` и `content-type: video/mp4`

### 3.3 Проверка генерации preview
```bash
curl -sS -X POST http://127.0.0.1:8002/generate-preview \
  -H 'Content-Type: application/json' \
  -d '{"account_id":"account_01"}'
```

### 3.4 Проверка публикации в GeeLark
```bash
curl -sS -X POST http://127.0.0.1:8010/publish \
  -H 'Content-Type: application/json' \
  -d '{"video_path":"storage/processing/<file>.mp4","caption":"test","cloud_phone_id":"615335260552429723"}'
```
Проверить, что в ответе `success=true`, а в логе `flowId=616420599023010067`.

## 4) Критические env-переменные

В `.env` должны быть:
- `GEELARK_PUBLISH_PATH=616420599023010067`
- `VIDEO_REMOTE_DIR=~/my_videos`
- `VIDEO_PUBLIC_BASE_URL=<текущий tunnel URL>`
- `VIDEO_PUBLIC_BASE_URL_FILE=/Users/rifatello/my_videos/public_base_url.txt`
- `PUBLISH_API_BASE_URL=http://127.0.0.1:8010`
- `CONTENT_API_BASE_URL=http://127.0.0.1:8002`

Примечание: `VIDEO_PUBLIC_BASE_URL_FILE` приоритетнее статического URL.

## 5) Типовые проблемы и быстрые решения

### Ошибка `Connection refused 127.0.0.1:8002`
`Content API` не запущен. Поднять `uvicorn api.content_api:app --port 8002`.

### Ошибка `Connection refused 127.0.0.1:8010`
`Publish API` не запущен. Поднять `uvicorn app.main:app --port 8010`.

### GeeLark зависает на загрузке видео
Проверь, что ссылка на видео не `ngrok` с interstitial. Использовать только прямой `localhost.run` URL.

### `ERR_NGROK_3200`
Игнорировать ngrok в этом проекте. Используется `localhost.run`.

## 6) Важно для macOS launchd

`launchd` может не иметь доступа к проекту в `Desktop` (`Operation not permitted`).
Если сервисы падают в launchd:
- запускать `Content API`/`Publish API` вручную (как выше), или
- перенести проект из `Desktop` в директорию без ограничений доступа.
