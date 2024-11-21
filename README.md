# rtg-automotive-frontend

Commands:

uvicorn app.api.mock:app --host 0.0.0.0 --port 8000 --reload


curl -X GET "https://tsybspea31.execute-api.eu-west-2.amazonaws.com/dev/items/?table_name=supplier_stock&filters=%7B%22supplier%22%3A%22SupplierA%22%7D&limit=10"