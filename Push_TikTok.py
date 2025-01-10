import requests
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# Thông tin Google Service Account
SERVICE_ACCOUNT_INFO = {
    "type": "service_account",
    "project_id": "emall-445202",
    "private_key_id": "cabfc059b43f4d4300f32356e2027c3352be1244",
    "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQDxprj8LoGx7Mza\nuwiQulME8AfJintW4DDJfssCYROltL2CpAyELxm//yz3Ya/+nFvelhCbs4HrXMkD\nOYGOZjj4OZME6RBKNw7Kw/R48jj+rQm+ghyiGjKKwgoo4lDa5wAa7sHe8aQZnaLp\nvPg8yMqroZImWJKvQ7zc8pVtJ+xGlCW9BOVKFlXvhzayHVAlz1dA6g6rR+gbhkJv\nBM+Ys3tQU4tj9K+yW88gzm7TLTCAhsPfP8EfmLexI7I7K64ainsSFqaubv42t4aJ\nSOW7B51RW4oVKsQMll6AB9Jdkuk2Fmduqc27VbuANDoWQdw3WJHnxOKAoVY7KlDH\n99erUOb7AgMBAAECggEAH6IGhySDX8oyytM+/MsXQQc2zcAuAyJX4JiTdJ1GmCN/\nx1E4Pjl4MnTx6vRDuJ25PPsB84/dCvVbbuuhNQXWoU/QGg10T3LDofFi2E5E/bmj\nE+guXV32jcl20QFHOlCQ5/WnNUsGgz/qapSNHk/ej/cXjaN8mCrS101+GAro2Bgd\nb3a+5/jK507RuVDCAfzipCeN2F5VGWLS7cAZ2OkrQdtltlWTqFeaNz1rjelQ6WnC\nNJxRhRN2WRxrZxakCtcKnRZJ+aSpVISDpoL86HiZld/QWB+9ZHhhvmJJ9m5//B+A\neZyyeMbWwuHTEZiJ7VE9yazpHnEWHH4qgFo9kKXW0QKBgQD/+RLteZx1Miq2lqpU\nN90EVWh6N7kaK59iUSOaBCo1Hy2qArWfEixhA2WEczhk6Yq5ldezg28gYtGzxYn9\nAiMjkO+YuvDbLQUdZrml1IIWJ7qJ0BslBpXBslWp0RspDyvWFSG4iGyQOmTG24fE\nhCFjL52V9qWCFdrPM8O/aT99uQKBgQDxrULao5NOOIVC5pZsu8Ikk6aFrskZM+kD\nupmiaE/5zM4pqT4M9TFLHe2P1FZZOE+iXyybfSiu5HvQlGzuIl4q1GjwpmVuDRLR\n2Ad+V7DTF5BIsa1fGWfDmlb8ZLTpKfIKMxeVFI3P0grVc36x3ACSVCrqcYIu33I1\n6xOKnAZEUwKBgDQbYvP+po/g53sFzqSYPqCsNjly8HZYXPippcKriGAJ+cS7CnfY\nPjp9c4Pz5I3+UKQY0bEUV2HTW/bphn5/meGalnuTyoYDcyAaEj6ktNqHudxmBbCS\n15d3kyKfk5TXJshMwvGKq/bsPCmS9N9XOmizf7PQYr7JbiJx2i4z4Z3ZAoGAEDX7\nAgdu3hRUfARTmr+Naj6tMNJkktg0jRd7LrWoh60BNzIvA1d/EvbS9I7dq+ZnEREp\nQNTIYdLZq0gJdn+/qUfOMjY3H4ao+IJxHjxrk/1EpFumsXZWy3wS0aM/r1Qs9Box\nPsK+MG56Y473tJ55O1eB+W/bt6EtXAiEqwuEMBkCgYBOIRlU/o8jvXGdeGiWfI8G\n03lo4GbXk56T+z5tN1B98esR6Zc2IZmQgBwnYc9xZ09cvFho4SGe7wlEj7p/MxEv\n8FTBpedL4brSWT5nT2gdR0N/BYI4OsZSnm8HZpY4SATaXvpJWANDmQKXnCkD+738\nVzPUkQwJ6BB8/odB87yFBw==\n-----END PRIVATE KEY-----\n",
    "client_email": "basevn@emall-445202.iam.gserviceaccount.com",
    "client_id": "106497851641442608167",
    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
    "token_uri": "https://oauth2.googleapis.com/token",
    "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
    "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/basevn%40emall-445202.iam.gserviceaccount.com",
}

SPREADSHEET_ID = "1oGt5NbjzXPixKAhpurcyNiFogppHDdUcrUNa2Pk9epo"
GOAL_SHEET_NAME = "Follower"

# Danh sách các Table ID
table_ids = [
    "tblD1FTQzgkmSdTz", "tblfwnCVQP5lgPPe", "tblm2bO32uqjOtNm", "tbljLvzdWwE5k7iO",
    "tblc3BNrvYDuxune", "tbl9AYXIlAicaQhp", "tblx2iNdu0qdXwiC", "tblODgW2bMktLGyp",
    "tbl9TkNg1rBPmJOs", "tblYaGbtVts2PhTY", "tblhpQR342lX7A4l", "tbl5gQKVwHcJzHCY",
    "tblzPaCBjf72jaDo", "tblC4vCiVzdpc9wH", "tblOgIIUQtEd3c7r", "tblcuxiffdRMl8xE",
    "tblsW7rNMzRNvWgw", "tblmN0i1T6kUiK4O", "tbltYRNsu3T8Kly6", "tblwXWYD4ZcigNdw",
    "tblGhmCTvqYA03nf", "tbla4Llyawa1UZjL", "tblpA574EtjL2qm2", "tbluEPfUkP56lTHj",
    "tbl1qezErTt1YiIA", "tbl9wYmp0BVlActD", "tblRXnWMv4xpn84j", "tblNfJQQpbcb1w79",
    "tblAeelLGawhKrge", "tblFa1VsfApMu3nB", "tblJPKnVWsKUPiOu", "tblTjbXgI5PfcTg8",
    "tbleSB74unoECUrl","tblv7rkz8gLtQXpp","tbl0jc8ebi11UpNw","tblE2pJEZuIfZzFB",
    "tblh1vZWPxeDymjh","tbl9seojVP3tayzZ","tblH7DTNnLuZGXFT","tbl5OZiYzwEPGr9v",
    "tblWU0mCsemgxFIU","tblHHitEe0lm3qQm","tblYRCBddpAkPJ1L","tblQK54HI8GgTdUL",
    "tblSOrYAay5H1VHI","tblnTbFgTdWj68cy","tbl0I05tqYzhEO55","tblBNdwW6Pti7VhJ",
    "tbl7HxD62n3YCKHr","tbl0FZtk7X5LzJTw","tbldP6R1IMz90Tqp","tblqThBUiiRqNUdk",
    "tblNwgo2nzZAPj6t","tbl9Tk2STUZsOSYW"
]

def get_access_token(app_id, app_secret):
    """
    Lấy access_token từ Lark bằng App ID và App Secret.
    """
    url = "https://open.larksuite.com/open-apis/auth/v3/app_access_token/internal"
    payload = {
        "app_id": app_id,
        "app_secret": app_secret
    }
    response = requests.post(url, json=payload)
    response.raise_for_status()
    data = response.json()
    return data.get("app_access_token")


def get_table_data(access_token, app_token, table_id):
    """
    Lấy thông tin bảng từ Base bằng access_token.
    """
    url = f"https://open.larksuite.com/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records"
    headers = {"Authorization": f"Bearer {access_token}"}
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json().get("data", {}).get("items", [])


def clear_sheet_except_header(sheet):
    """
    Xóa tất cả các hàng trừ hàng đầu tiên (header) trong Google Sheets.
    """
    rows = len(sheet.get_all_values())
    if rows > 0:
        sheet.delete_rows(1, rows)  # Xóa từ hàng thứ 2 đến cuối


def write_to_sheet(sheet, table_id, records):
    """
    Ghi dữ liệu vào Google Sheets.
    """
    for record in records:
        sheet.append_row([
            table_id,
            record.get("id", ""),
            record.get("fields", {}).get("Follower", ""),
            record.get("fields", {}).get("Like", ""),
            record.get("fields", {}).get("comment", ""),
            record.get("fields", {}).get("Ngày", ""),
            record.get("fields", {}).get("CH", "")
        ])


# Google Sheets Authentication
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_dict(SERVICE_ACCOUNT_INFO, scope)
client = gspread.authorize(creds)
spreadsheet = client.open_by_key(SPREADSHEET_ID)
try:
    sheet = spreadsheet.worksheet(GOAL_SHEET_NAME)
except gspread.exceptions.WorksheetNotFound:
    sheet = spreadsheet.add_worksheet(title=GOAL_SHEET_NAME, rows="100", cols="20")

# Main script
app_id = "cli_a7f4f2fcd53a502f"
app_secret = "uSN21Ih65KhVZv62Uh8sxdICvMRUg1tt"
app_token = "MgW2bhlf5aBQnGsZ6t8l7Kv6g6d"

access_token = get_access_token(app_id, app_secret)
if access_token:
    clear_sheet_except_header(sheet)  # Xóa dữ liệu cũ, chỉ giữ tiêu đề
    for table_id in table_ids:
        records = get_table_data(access_token, app_token, table_id)
        write_to_sheet(sheet, table_id, records)

print("Hoàn thành ghi dữ liệu vào Google Sheets!")
