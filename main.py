import json
import sqlalchemy
from sqlalchemy.orm import sessionmaker
from tabulate import tabulate
from models import create_tables, Publisher, Shop, Book, Stock, Sale

login = "postgres"
password = "123456"
db_name = "bookmarket"

DSN = f"postgresql://{login}:{password}@localhost:5432/{db_name}"
engine = sqlalchemy.create_engine(DSN)

create_tables(engine)

Session = sessionmaker(bind=engine)
session = Session()

# Импорт данных в базу
with open("tests_data.json", "r") as fd:
    data = json.load(fd)


for record in data:
    model = {
        "publisher": Publisher,
        "shop": Shop,
        "book": Book,
        "stock": Stock,
        "sale": Sale,
    }[record.get("model")]
    session.add(model(id=record.get("pk"), **record.get("fields")))
session.commit()


publ = input('Введите название или id издательства: ')

# Первый способ
query = session.query(Sale, Stock, Shop, Book, Publisher)
query = query.join(Book, Book.id_publisher == Publisher.id)
query = query.join(Stock, Stock.id_book == Book.id)
query = query.join(Shop, Shop.id == Stock.id_shop)
query = query.join(Sale, Sale.id_stock == Stock.id)

if publ.isdigit():
    records = query.filter(Publisher.id == publ)
    table = []
    table_str = []
    for sale, stock, shop, book, publisher in records:
        # table_str.append(publisher.name)
        table_str.append(book.title)
        table_str.append(shop.name)
        table_str.append(sale.price * sale.count)
        table_str.append(sale.date_sale.date().strftime('%d-%m-%Y'))
        table.append(table_str)
        table_str = []
else:
    records = query.filter(Publisher.name == publ)
    table = []
    table_str = []
    for sale, stock, shop, book, publisher in records:
        # table_str.append(publisher.name)
        table_str.append(book.title)
        table_str.append(shop.name)
        table_str.append(sale.price * sale.count)
        table_str.append(sale.date_sale.date().strftime('%d-%m-%Y'))
        table.append(table_str)
        table_str = []

print(tabulate(table, tablefmt='orgtbl'))

# # Второй способ. В принципе результат тот же, если оформить. Какой способ предпочтительней?
# q = (
#     session.query(Publisher, Book, Stock, Shop, Sale)
#     .filter(
#         Stock.id == Sale.id_stock,
#     )
#     .filter(
#         Shop.id == Stock.id_shop,
#     )
#     .filter(
#         Book.id == Stock.id_book,
#     )
#     .filter(
#         Book.id_publisher == Publisher.id,
#     )
#     .filter(
#         Publisher.name == publ,
#     )
#     .all()
# )

# for publisher, book, stock, shop, sale in q:
#     print(book.title, shop.name, sale.price*sale.count, sale.date_sale.date().strftime('%d-%m-%Y'))


session.close()
