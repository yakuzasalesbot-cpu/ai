# Signal Group Ingest

Мінімальний ingestion-сервіс для читання нових повідомлень з **однієї дозволеної Signal-групи** через офіційний `signal-cli` daemon і передачі нормалізованих подій у downstream webhook.

Сервіс не змінює існуючу логіку звітів. Якщо downstream тимчасово недоступний або ще не заданий, повідомлення залишаються в локальному SQLite outbox і не губляться.

## Архітектура

```text
Signal group
   ↓
signal-cli daemon (localhost HTTP + SSE)
   ↓
signal_ingest.py
   ├─ exact group_id filter
   ├─ normalize
   ├─ dedup by message_id
   ├─ SQLite outbox
   └─ retry/reconnect
   ↓
DOWNSTREAM_WEBHOOK_URL
   ↓
existing parser / report pipeline
```

## Передумови

- Python 3.11+.
- Актуальний `signal-cli`.
- Signal-акаунт має бути легітимно зареєстрований/прив'язаний до `signal-cli` і бути учасником потрібної групи.
- HTTP daemon `signal-cli` тримати тільки на localhost або в іншому довіреному приватному сегменті. Не виставляти його напряму в Інтернет.

## 1. Встановити залежності

```bash
python -m venv .venv

# Linux/macOS
source .venv/bin/activate

# Windows PowerShell
# .\.venv\Scripts\Activate.ps1

pip install -r requirements.txt
```

## 2. Запустити signal-cli daemon

Для одного акаунта:

```bash
signal-cli -a +380XXXXXXXXX daemon --http=127.0.0.1:8080
```

Для multi-account режиму можна запустити daemon без `-a`, а потрібний акаунт задати через `SIGNAL_ACCOUNT`.

Перевірка daemon:

```bash
curl http://127.0.0.1:8080/api/v1/check
```

## 3. Знайти internal ID групи

`SIGNAL_GROUP_ID` на цьому кроці ще не потрібен.

```bash
export SIGNAL_CLI_URL=http://127.0.0.1:8080
export SIGNAL_ACCOUNT=+380XXXXXXXXX
python signal_ingest.py --list-groups
```

PowerShell:

```powershell
$env:SIGNAL_CLI_URL = "http://127.0.0.1:8080"
$env:SIGNAL_ACCOUNT = "+380XXXXXXXXX"
python .\signal_ingest.py --list-groups
```

Команда виведе:

```text
<group-id>    <group-name>
```

У production фільтрація виконується **за internal `group_id`**, а не за назвою групи.

## 4. Налаштувати environment

Скопіювати `.env.example` у локальний `.env` або задати змінні через середовище/secret manager. Сам `.env` з реальними значеннями не комітити.

Обов'язково для ingestion:

```text
SIGNAL_CLI_URL=http://127.0.0.1:8080
SIGNAL_ACCOUNT=+380XXXXXXXXX
SIGNAL_GROUP_ID=<group-id>
```

Для автоматичної передачі далі:

```text
DOWNSTREAM_WEBHOOK_URL=https://...
DOWNSTREAM_BEARER_TOKEN=...
```

Якщо `DOWNSTREAM_WEBHOOK_URL` порожній, ingest продовжує приймати й дедуплікувати події та складає їх у SQLite outbox.

## 5. Запустити ingestion

```bash
python signal_ingest.py
```

Очікувані логи:

```text
Connected to Signal event stream
Queued Signal message id=...
Delivered id=...
```

Текст повідомлень у лог не пишеться.

## Формат downstream payload

```json
{
  "message_id": "sha256...",
  "source": "signal",
  "account": "+380...",
  "group_id": "...",
  "group_name": "...",
  "sender": "+380...",
  "sender_name": "...",
  "sender_uuid": "...",
  "timestamp": 1725148800000,
  "text": "повідомлення",
  "attachments": [],
  "is_edit": false
}
```

`attachments` зараз передаються як metadata з event. Файли автоматично не завантажуються.

## Надійність

- Дублікати блокуються SQLite primary key `message_id`.
- Якщо webhook впав, подія лишається `pending`.
- Повторні доставки мають exponential backoff до 300 секунд.
- SSE stream автоматично reconnect'иться з backoff до 30 секунд.
- Успішна доставка переводить event у `delivered`.

## Тести

```bash
pytest -q
```

Покрито:

- target group filtering;
- rejection чужої групи;
- JSON-RPC `receive` wrapper;
- attachment-only message;
- empty event rejection;
- SQLite dedup + delivered state.

## Межа цієї ітерації

Signal ingestion готовий як незалежний adapter. Остання production-прив'язка залежить від фактичного endpoint існуючого парсера/бота: значення `DOWNSTREAM_WEBHOOK_URL` і, за потреби, контракт payload мають відповідати тому endpoint без зміни логіки звітності.
